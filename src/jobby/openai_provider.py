"""OpenAI Responses API integration with provenance and citation capture."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .audit import redact_text
from .config import AppConfig, SecretStore
from .enums import ApprovalState
from .models import AICacheEntry, AIRun, Citation
from .normalization import normalize_url


T = TypeVar("T", bound=BaseModel)
MAX_AI_INPUT_CHARS = 200_000
MAX_WEB_QUERY_CHARS = 8_000
MAX_SYSTEM_PROMPT_CHARS = 50_000
OPENAI_TIMEOUT_SECONDS = 30.0
SAFE_READ_RETRIES = 1
# Only purposes whose output is stable for identical text/schema inputs may opt
# into local reuse.  Web results and user-facing composition are intentionally
# absent even if a caller accidentally requests caching.
CACHEABLE_PURPOSES = frozenset(
    {
        "classification",
        "extraction",
        "job_classification",
        "job_enrichment_extraction",
        "job_evaluation_enrichment",
    }
)


class ProviderUnavailable(RuntimeError):
    pass


class ModelUnavailable(RuntimeError):
    pass


class ProviderRequestError(RuntimeError):
    """A bounded, credential-redacted provider failure safe for UI display."""


class WebCitation(BaseModel):
    url: str = Field(min_length=1, max_length=4_000)
    title: str | None = Field(default=None, max_length=1_000)
    quoted_text: str | None = Field(default=None, max_length=10_000)
    start_index: int | None = Field(default=None, ge=0)
    end_index: int | None = Field(default=None, ge=0)


class WebSearchResponse(BaseModel):
    text: str
    citations: list[WebCitation] = Field(default_factory=list)
    model: str
    ai_run_id: str | None = None


class GateEnrichment(BaseModel):
    name: str
    outcome: str
    evidence: str
    confidence: float = Field(ge=0, le=1)


class AIJobEnrichment(BaseModel):
    summary: str
    gates: list[GateEnrichment] = Field(default_factory=list)
    workload_evidence: list[str] = Field(default_factory=list)
    strategic_options: list[str] = Field(default_factory=list)
    missing_data: list[str] = Field(default_factory=list)


@dataclass(slots=True)
class StructuredResult(Generic[T]):
    value: T
    ai_run: AIRun


class OpenAIProvider:
    """No model fallback is performed: configured IDs either validate or fail."""

    def __init__(
        self,
        config: AppConfig,
        *,
        secret_store: SecretStore | None = None,
        client: Any | None = None,
    ):
        self.config = config
        self.secret_store = secret_store or SecretStore()
        self._ensure_enabled()
        if client is not None:
            self.client = client
            return
        key = self.secret_store.get("openai_api_key")
        if not key:
            raise ProviderUnavailable(
                "OpenAI is not configured; add an API key to the OS keyring"
            )
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - declared dependency
            raise ProviderUnavailable("the openai package is not installed") from exc
        # Billable Responses calls are not automatically retried: a lost
        # response does not prove the provider failed to perform the request.
        # Read-only model probes use one explicit bounded retry below.
        self.client = OpenAI(
            api_key=key,
            timeout=OPENAI_TIMEOUT_SECONDS,
            max_retries=0,
        )

    @property
    def configured_models(self) -> dict[str, str]:
        return {
            "fast": self.config.models.fast,
            "quality": self.config.models.quality,
            "premium": self.config.models.premium,
        }

    def validate_models(self) -> dict[str, str | None]:
        self._ensure_enabled()
        results: dict[str, str | None] = {}
        for tier, model_id in self.configured_models.items():
            for attempt in range(SAFE_READ_RETRIES + 1):
                try:
                    model = self.client.models.retrieve(model_id)
                    returned_id = getattr(model, "id", model_id)
                    if returned_id != model_id:
                        results[tier] = (
                            f"provider returned unexpected model {returned_id}"
                        )
                    else:
                        results[tier] = None
                    break
                except Exception as exc:
                    if attempt < SAFE_READ_RETRIES and _is_retryable_read_error(exc):
                        continue
                    results[tier] = _safe_error(exc)
                    break
        return results

    def require_models(self) -> None:
        self._ensure_enabled()
        failures = {
            tier: error for tier, error in self.validate_models().items() if error
        }
        if failures:
            detail = "; ".join(
                f"{tier}: {message}" for tier, message in failures.items()
            )
            raise ModelUnavailable(
                f"configured OpenAI model validation failed: {detail}"
            )

    def parse(
        self,
        *,
        purpose: str,
        text: str,
        output_type: type[T],
        prompt_version: str,
        system: str = "Extract only facts supported by the supplied text. Mark missing information explicitly.",
        tier: str = "fast",
        session: Session | None = None,
        max_output_tokens: int | None = None,
        use_cache: bool = False,
        bypass_cache: bool = False,
    ) -> T:
        return self.structured(
            purpose=purpose,
            text=text,
            output_type=output_type,
            prompt_version=prompt_version,
            system=system,
            tier=tier,
            session=session,
            max_output_tokens=max_output_tokens,
            use_cache=use_cache,
            bypass_cache=bypass_cache,
        ).value

    def structured(
        self,
        *,
        purpose: str,
        text: str,
        output_type: type[T],
        prompt_version: str,
        system: str,
        tier: str = "fast",
        session: Session | None = None,
        max_output_tokens: int | None = None,
        use_cache: bool = False,
        bypass_cache: bool = False,
    ) -> StructuredResult[T]:
        self._ensure_enabled()
        purpose = _required_text("purpose", purpose, maximum=200)
        prompt_version = _required_text("prompt version", prompt_version, maximum=200)
        system = _required_text(
            "system prompt", system, maximum=MAX_SYSTEM_PROMPT_CHARS
        )
        text = _required_text("AI input", text, maximum=MAX_AI_INPUT_CHARS)
        model = self._model_for_tier(tier)
        max_output_tokens = _optional_output_limit(max_output_tokens)
        schema = json.dumps(
            output_type.model_json_schema(), sort_keys=True, separators=(",", ":")
        )
        input_hash = _input_hash(purpose, prompt_version, system, text, schema)
        cache_identity = _cache_identity(
            provider="openai",
            model=model,
            purpose=purpose,
            prompt_version=prompt_version,
            system=system,
            text=text,
            schema=schema,
            max_output_tokens=max_output_tokens,
        )
        cache_allowed = (
            use_cache
            and not bypass_cache
            and session is not None
            and tier != "premium"
            and purpose in CACHEABLE_PURPOSES
        )
        if cache_allowed:
            cached = _read_cached_result(
                session,
                output_type=output_type,
                purpose=purpose,
                model=model,
                prompt_version=prompt_version,
                input_hash=input_hash,
                identity=cache_identity,
            )
            if cached is not None:
                return cached
        run = AIRun(
            purpose=purpose,
            provider="openai",
            model=model,
            prompt_version=prompt_version,
            input_hash=input_hash,
            approval_state=ApprovalState.PENDING,
            output_json={},
        )
        try:
            request: dict[str, Any] = {
                "model": model,
                "input": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": text},
                ],
                "text_format": output_type,
            }
            if max_output_tokens is not None:
                request["max_output_tokens"] = max_output_tokens
            response = self.client.responses.parse(
                **request,
            )
            value = response.output_parsed
            if value is None:
                raise ValueError("the model returned no parsed output")
            if not isinstance(value, output_type):
                value = output_type.model_validate(value)
            run.output_json = value.model_dump(mode="json")
            _apply_usage(run, getattr(response, "usage", None))
        except Exception as exc:
            run.error = _safe_error(exc)
            run.approval_state = ApprovalState.REJECTED
            if session is not None:
                session.add(run)
                session.flush()
            raise ProviderRequestError(run.error) from None
        if session is not None:
            session.add(run)
            session.flush()
            if cache_allowed:
                _write_cached_result(
                    session,
                    run=run,
                    value=value,
                    identity=cache_identity,
                    ttl_days=self.config.ai_cache_ttl_days,
                )
        return StructuredResult(value=value, ai_run=run)

    def search(
        self,
        query: str,
        *,
        prompt_version: str = "web-search-v1",
        session: Session | None = None,
        max_output_tokens: int | None = None,
    ) -> WebSearchResponse:
        self._ensure_enabled()
        query = _required_text("search query", query, maximum=MAX_WEB_QUERY_CHARS)
        prompt_version = _required_text("prompt version", prompt_version, maximum=200)
        model = self.config.models.fast
        max_output_tokens = _optional_output_limit(max_output_tokens)
        run = AIRun(
            purpose="job_web_search",
            provider="openai",
            model=model,
            prompt_version=prompt_version,
            input_hash=_input_hash("job_web_search", prompt_version, "", query),
            approval_state=ApprovalState.PENDING,
            output_json={},
        )
        try:
            request: dict[str, Any] = {
                "model": model,
                "tools": [{"type": "web_search"}],
                "input": query,
            }
            if max_output_tokens is not None:
                request["max_output_tokens"] = max_output_tokens
            response = self.client.responses.create(**request)
            text = str(getattr(response, "output_text", "") or "")
            citations = _extract_citations(response)
            run.output_json = {
                "text": text,
                "citations": [item.model_dump(mode="json") for item in citations],
            }
            _apply_usage(run, getattr(response, "usage", None))
            if not text.strip():
                raise ValueError("OpenAI web search returned no text")
        except Exception as exc:
            run.error = _safe_error(exc)
            run.approval_state = ApprovalState.REJECTED
            if session is not None:
                session.add(run)
                session.flush()
            raise ProviderRequestError(run.error) from None
        if session is not None:
            session.add(run)
            session.flush()
            for item in citations:
                session.add(
                    Citation(
                        ai_run_id=run.id,
                        url=item.url,
                        title=item.title,
                        quoted_text=item.quoted_text,
                        start_index=item.start_index,
                        end_index=item.end_index,
                    )
                )
        return WebSearchResponse(
            text=text,
            citations=citations,
            model=model,
            ai_run_id=run.id if session is not None else None,
        )

    def enrich_job(
        self,
        description: str,
        *,
        session: Session | None = None,
        premium: bool = False,
        bypass_cache: bool = False,
    ) -> StructuredResult[AIJobEnrichment]:
        self._ensure_enabled()
        tier = "premium" if premium else "quality"
        return self.structured(
            purpose="job_evaluation_enrichment",
            text=description,
            output_type=AIJobEnrichment,
            prompt_version="job-evaluation-v1",
            system=(
                "Analyze this job posting. Use only quoted or directly paraphrased posting evidence. "
                "Identify gates, workload signals, strategic options, and missing data; do not invent requirements."
            ),
            tier=tier,
            session=session,
            use_cache=not premium,
            bypass_cache=bypass_cache,
        )

    def _model_for_tier(self, tier: str) -> str:
        try:
            return self.configured_models[tier]
        except KeyError as exc:
            raise ValueError(f"unknown model tier: {tier}") from exc

    def _ensure_enabled(self) -> None:
        """Fail closed even when a client was injected or config later changed."""

        if not self.config.openai_enabled:
            raise ProviderUnavailable(
                "OpenAI is disabled; set openai_enabled=true explicitly before using it"
            )


def _input_hash(*parts: str) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _CacheIdentity:
    key: str
    output_schema_hash: str
    request_hash: str
    max_output_tokens: int


def _cache_identity(
    *,
    provider: str,
    model: str,
    purpose: str,
    prompt_version: str,
    system: str,
    text: str,
    schema: str,
    max_output_tokens: int | None,
) -> _CacheIdentity:
    output_schema_hash = _input_hash(schema)
    # Validation has already stripped insignificant outer whitespace.  Internal
    # text and punctuation remain exact so semantically distinct requests never
    # collide.
    request_hash = _input_hash(system, text)
    output_limit = max_output_tokens or 0
    return _CacheIdentity(
        key=_input_hash(
            provider,
            model,
            purpose,
            prompt_version,
            output_schema_hash,
            request_hash,
            str(output_limit),
        ),
        output_schema_hash=output_schema_hash,
        request_hash=request_hash,
        max_output_tokens=output_limit,
    )


def _read_cached_result(
    session: Session,
    *,
    output_type: type[T],
    purpose: str,
    model: str,
    prompt_version: str,
    input_hash: str,
    identity: _CacheIdentity,
) -> StructuredResult[T] | None:
    now = datetime.now(timezone.utc)
    entry = session.scalar(
        select(AICacheEntry)
        .where(
            AICacheEntry.cache_key == identity.key,
            AICacheEntry.active.is_(True),
            AICacheEntry.expires_at > now,
        )
        .order_by(AICacheEntry.created_at.desc(), AICacheEntry.id.desc())
        .limit(1)
    )
    if entry is None:
        return None
    source_run = (
        session.get(AIRun, entry.source_ai_run_id)
        if entry.source_ai_run_id is not None
        else None
    )
    identity_matches = (
        entry.provider == "openai"
        and entry.model == model
        and entry.purpose == purpose
        and entry.prompt_version == prompt_version
        and entry.output_schema_hash == identity.output_schema_hash
        and entry.request_hash == identity.request_hash
        and entry.max_output_tokens == identity.max_output_tokens
    )
    source_matches = (
        source_run is not None
        and source_run.provider == entry.provider
        and source_run.model == entry.model
        and source_run.purpose == entry.purpose
        and source_run.prompt_version == entry.prompt_version
        and source_run.input_hash == input_hash
        and source_run.output_json == entry.output_json
    )
    if (
        not identity_matches
        or not source_matches
        or source_run is None
        or source_run.error is not None
        or source_run.approval_state == ApprovalState.REJECTED
    ):
        _invalidate_cache_entry(entry, now=now)
        session.flush()
        return None
    try:
        value = output_type.model_validate(entry.output_json)
    except (TypeError, ValueError, ValidationError):
        _invalidate_cache_entry(entry, now=now)
        session.flush()
        return None
    run = AIRun(
        purpose=purpose,
        provider="openai",
        model=model,
        prompt_version=prompt_version,
        input_hash=input_hash,
        input_tokens=0,
        output_tokens=0,
        cached_tokens=None,
        output_json=value.model_dump(mode="json"),
        approval_state=ApprovalState.PENDING,
        cache_hit=True,
        cache_entry_id=entry.id,
        source_ai_run_id=source_run.id,
    )
    session.add(run)
    session.flush()
    return StructuredResult(value=value, ai_run=run)


def _write_cached_result(
    session: Session,
    *,
    run: AIRun,
    value: BaseModel,
    identity: _CacheIdentity,
    ttl_days: int,
) -> AICacheEntry:
    now = datetime.now(timezone.utc)
    entry = AICacheEntry(
        provider=run.provider,
        model=run.model,
        purpose=run.purpose,
        prompt_version=run.prompt_version,
        output_schema_hash=identity.output_schema_hash,
        request_hash=identity.request_hash,
        max_output_tokens=identity.max_output_tokens,
        cache_key=identity.key,
        output_json=value.model_dump(mode="json"),
        source_ai_run_id=run.id,
        expires_at=now + timedelta(days=ttl_days),
        active=True,
    )
    try:
        # A savepoint contains the expected race between two identical calls;
        # unrelated work already present in the caller's transaction survives.
        with session.begin_nested():
            session.add(entry)
            session.flush()
        return entry
    except IntegrityError:
        existing = session.scalar(
            select(AICacheEntry).where(AICacheEntry.cache_key == identity.key)
        )
        if existing is None:  # pragma: no cover - defensive database anomaly
            raise
        source = (
            session.get(AIRun, existing.source_ai_run_id)
            if existing.source_ai_run_id is not None
            else None
        )
        winner_is_reusable = (
            existing.active
            and existing.expires_at > now
            and source is not None
            and source.error is None
            and source.approval_state != ApprovalState.REJECTED
            and existing.provider == run.provider
            and existing.model == run.model
            and existing.purpose == run.purpose
            and existing.prompt_version == run.prompt_version
            and existing.output_schema_hash == identity.output_schema_hash
            and existing.request_hash == identity.request_hash
            and existing.max_output_tokens == identity.max_output_tokens
            and source.input_hash == run.input_hash
            and source.output_json == existing.output_json
        )
        if not winner_is_reusable:
            # A unique identity is intentionally stable over time.  Refresh its
            # payload in place after expiry/invalidation so stale rows cannot
            # turn into permanent misses.
            existing.output_json = value.model_dump(mode="json")
            existing.provider = run.provider
            existing.model = run.model
            existing.purpose = run.purpose
            existing.prompt_version = run.prompt_version
            existing.output_schema_hash = identity.output_schema_hash
            existing.request_hash = identity.request_hash
            existing.max_output_tokens = identity.max_output_tokens
            existing.source_ai_run_id = run.id
            existing.expires_at = now + timedelta(days=ttl_days)
            existing.active = True
            existing.invalidated_at = None
            session.flush()
        return existing


def _invalidate_cache_entry(
    entry: AICacheEntry, *, now: datetime | None = None
) -> None:
    entry.active = False
    entry.invalidated_at = now or datetime.now(timezone.utc)


def _apply_usage(run: AIRun, usage: Any) -> None:
    if usage is None:
        return
    run.input_tokens = _nonnegative_int(_get(usage, "input_tokens"))
    run.output_tokens = _nonnegative_int(_get(usage, "output_tokens"))
    details = _get(usage, "input_tokens_details")
    run.cached_tokens = (
        _nonnegative_int(_get(details, "cached_tokens"))
        if details is not None
        else None
    )


def _get(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _as_dict(value: Any, *, _depth: int = 0, _seen: set[int] | None = None) -> Any:
    if _depth > 32:
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    seen = _seen if _seen is not None else set()
    identity = id(value)
    if identity in seen:
        return None
    seen.add(identity)
    if isinstance(value, dict):
        return {
            key: _as_dict(item, _depth=_depth + 1, _seen=seen)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_as_dict(item, _depth=_depth + 1, _seen=seen) for item in value]
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raw = getattr(value, "__dict__", str(value))
    return _as_dict(raw, _depth=_depth + 1, _seen=seen)


def _extract_citations(response: Any) -> list[WebCitation]:
    payload = _as_dict(response)
    found: list[WebCitation] = []
    seen: set[tuple[str, int | None, int | None]] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("type") == "url_citation":
                raw = (
                    value.get("url_citation")
                    if isinstance(value.get("url_citation"), dict)
                    else value
                )
                url = normalize_url(raw.get("url"))
                if url:
                    try:
                        citation = WebCitation(
                            url=url,
                            title=_truncated(raw.get("title"), 1_000),
                            quoted_text=_truncated(
                                raw.get("text") or raw.get("quoted_text"), 10_000
                            ),
                            start_index=raw.get("start_index"),
                            end_index=raw.get("end_index"),
                        )
                    except ValidationError:
                        citation = None
                    if citation is None:
                        return
                    key = (citation.url, citation.start_index, citation.end_index)
                    if key not in seen:
                        seen.add(key)
                        found.append(citation)
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload)
    return found


def _required_text(name: str, value: str, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{name} must not be blank")
    if len(normalized) > maximum:
        raise ValueError(f"{name} exceeds the {maximum:,}-character limit")
    return normalized


def _nonnegative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _optional_output_limit(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("max_output_tokens must be an integer")
    if not 256 <= value <= 100_000:
        raise ValueError("max_output_tokens must be between 256 and 100,000")
    return value


def _is_retryable_read_error(exc: Exception) -> bool:
    """Return true only for transient failures of a read-only provider call."""

    status = getattr(exc, "status_code", None)
    if status is None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status, int):
        return status in {408, 409, 429} or status >= 500
    return exc.__class__.__name__ in {
        "APITimeoutError",
        "APIConnectionError",
        "ConnectError",
        "ConnectTimeout",
        "ReadTimeout",
    }


def _truncated(value: Any, maximum: int) -> str | None:
    if value is None:
        return None
    return str(value)[:maximum]


def _safe_error(exc: Exception) -> str:
    message = str(exc) or exc.__class__.__name__
    patterns = (
        (r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED_API_KEY]"),
        (
            r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+",
            r"\1[REDACTED]",
        ),
        (r"(?i)(api[_-]?key\s*[:=]\s*)[^\s,;]+", r"\1[REDACTED]"),
    )
    for pattern, replacement in patterns:
        message = re.sub(pattern, replacement, message)
    return redact_text(message)[:4_000]
