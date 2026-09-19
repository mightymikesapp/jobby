"""Bounded, two-phase manual job capture.

The preview phase may perform one SSRF-safe static HTML read and database reads
for duplicate suggestions, but it never writes.  :func:`save_capture` is the
only write boundary; it persists the exact preview the user approved in one
transaction and never updates an existing job.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
import hashlib
import html
from html.parser import HTMLParser
import json
import re
import time
from typing import Annotated, Protocol, TypeAlias, runtime_checkable

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import case, or_, select
from sqlalchemy.exc import IntegrityError

from .audit import record_audit
from .db import Database
from .dedup import token_jaccard
from .enums import JobStatus
from .models import Company, Job, JobSourceState, Location, SourceObservation, utc_now
from .normalization import (
    CompensationExtraction,
    MAX_PERSISTED_SALARY,
    NormalizedSalary,
    RemoteStatus,
    SalaryPeriod,
    content_hash,
    extract_compensation,
    is_public_http_url,
    normalize_company,
    normalize_location,
    normalize_remote,
    normalize_salary,
    normalize_title,
    normalize_url,
)
from .sources.browser import PortalConfig, PublicPortalSource


MAX_URL_LENGTH = 8_000
MAX_COMPANY_LENGTH = 300
MAX_TITLE_LENGTH = 500
MAX_LOCATION_LENGTH = 500
MAX_DESCRIPTION_LENGTH = 500_000
MAX_COMPENSATION_LENGTH = 10_000
MAX_STATIC_HTML_LENGTH = 2_000_000
MAX_DUPLICATE_CANDIDATES = 20
MAX_DUPLICATE_ROWS = 1_000

_SPACE_RE = re.compile(r"\s+")
_RESTRICTED_PAGE_RE = re.compile(
    r"\b(?:captcha|verify you are human|access denied|sign in to continue|"
    r"log in to continue|authentication required)\b",
    re.IGNORECASE,
)
_JS_ONLY_RE = re.compile(
    r"\b(?:enable javascript|javascript (?:is )?required|requires javascript)\b",
    re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_FETCH_ERROR_CODES = frozenset(
    {
        "dns_error",
        "http_error",
        "invalid_redirect",
        "malformed_html",
        "network_error",
        "parse_error",
        "redirect_limit",
        "redirect_loop",
        "response_too_large",
        "restricted_page",
        "static_content_unavailable",
        "timeout",
        "unsafe_redirect",
        "unsafe_target",
        "unsupported_content",
    }
)
_AUTHORITATIVE_FIELD_ORDER = (
    "url",
    "company",
    "title",
    "location",
    "description",
    "compensation",
)

BoundedWarning = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1_000)
]


@runtime_checkable
class CancellationSignal(Protocol):
    """Structural type accepted for ``threading.Event``-style cancellation."""

    def is_set(self) -> bool: ...


CancelCheck: TypeAlias = Callable[[], bool] | CancellationSignal


class CaptureError(RuntimeError):
    """Base class for capture service errors."""


class CaptureCancelledError(CaptureError):
    """Raised before a preview or save crosses its next side-effect boundary."""


class CaptureNotReadyError(CaptureError):
    """Raised when a preview does not contain the required company and title."""


class CaptureConflictError(CaptureError):
    """Raised instead of overwriting a job already represented in storage."""


class CaptureFetchError(CaptureError):
    """A bounded static-page failure suitable for presenting in a preview."""

    def __init__(self, code: str, message: str) -> None:
        normalized = re.sub(r"[^a-z0-9_.-]+", "_", code.casefold())[:80]
        self.code = normalized if normalized in _FETCH_ERROR_CODES else "network_error"
        self.message = _bounded_single_line(message, 500)
        super().__init__(self.message)


class CaptureDraft(BaseModel):
    """User-entered capture fields before static enrichment.

    Blank strings become ``None``.  The bounds are deliberately the same as
    the durable/source contracts so neither preview nor persistence needs to
    truncate user input silently.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    url: str | None = Field(
        default=None,
        max_length=MAX_URL_LENGTH,
        validation_alias=AliasChoices("url", "launch_url"),
    )
    company: str | None = Field(default=None, max_length=MAX_COMPANY_LENGTH)
    title: str | None = Field(default=None, max_length=MAX_TITLE_LENGTH)
    location: str | None = Field(default=None, max_length=MAX_LOCATION_LENGTH)
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    compensation: str | None = Field(
        default=None,
        max_length=MAX_COMPENSATION_LENGTH,
        validation_alias=AliasChoices(
            "compensation", "compensation_text", "salary_text"
        ),
    )

    @field_validator("url", mode="before")
    @classmethod
    def validate_url(cls, value: object) -> str | None:
        if value is None:
            return None
        raw = str(value)
        if any(character in raw for character in ("\r", "\n", "\x00")):
            raise ValueError("capture URL contains control characters")
        normalized = raw.strip()
        if not normalized:
            return None
        if not is_public_http_url(normalized) or not normalize_url(normalized):
            raise ValueError(
                "capture URL must target a credential-free public HTTP(S) host"
            )
        return normalized

    @field_validator("company", "title", "location", mode="before")
    @classmethod
    def normalize_short_text(cls, value: object) -> str | None:
        return _optional_text(value, collapse=True)

    @field_validator("description", "compensation", mode="before")
    @classmethod
    def normalize_long_text(cls, value: object) -> str | None:
        return _optional_text(value, collapse=False)

    @model_validator(mode="after")
    def require_some_input(self) -> CaptureDraft:
        if not any(
            getattr(self, field)
            for field in (
                "url",
                "company",
                "title",
                "location",
                "description",
                "compensation",
            )
        ):
            raise ValueError("capture needs a URL or at least one pasted field")
        return self


class StaticPage(BaseModel):
    """Bounded result returned by an injectable static page fetcher."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    html: str = Field(max_length=MAX_STATIC_HTML_LENGTH)
    final_url: str = Field(max_length=MAX_URL_LENGTH)
    http_status: int = Field(ge=100, le=599)

    @field_validator("html")
    @classmethod
    def validate_html_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > MAX_STATIC_HTML_LENGTH:
            raise ValueError("static HTML exceeds the 2 MB capture safety limit")
        return value

    @field_validator("final_url")
    @classmethod
    def validate_final_url(cls, value: str) -> str:
        value = value.strip()
        if not is_public_http_url(value):
            raise ValueError("fetched page final URL must remain public HTTP(S)")
        return value


StaticPageFetcher: TypeAlias = Callable[
    [str], StaticPage | tuple[str, str, int] | Mapping[str, object]
]


class FetchedCaptureFields(BaseModel):
    """Useful bounded fields parsed from a static HTML response."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    final_url: str = Field(max_length=MAX_URL_LENGTH)
    http_status: int = Field(ge=100, le=599)
    company: str | None = Field(default=None, max_length=MAX_COMPANY_LENGTH)
    title: str | None = Field(default=None, max_length=MAX_TITLE_LENGTH)
    location: str | None = Field(default=None, max_length=MAX_LOCATION_LENGTH)
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    compensation: str | None = Field(default=None, max_length=MAX_COMPENSATION_LENGTH)


class DuplicateCandidate(BaseModel):
    """Detached duplicate suggestion; constructing it has no persistence effects."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str = Field(min_length=1, max_length=36)
    company: str = Field(max_length=MAX_COMPANY_LENGTH)
    title: str = Field(max_length=MAX_TITLE_LENGTH)
    location: str | None = Field(default=None, max_length=MAX_LOCATION_LENGTH)
    launch_url: str | None = Field(default=None, max_length=MAX_URL_LENGTH)
    reason: str = Field(min_length=1, max_length=80)
    similarity: float = Field(ge=0, le=1, allow_inf_nan=False)
    exact: bool = False


class CapturePreview(BaseModel):
    """Immutable, normalized value the user can explicitly approve for saving."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    company: str | None = Field(default=None, max_length=MAX_COMPANY_LENGTH)
    title: str | None = Field(default=None, max_length=MAX_TITLE_LENGTH)
    location: str | None = Field(default=None, max_length=MAX_LOCATION_LENGTH)
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    compensation: str | None = Field(default=None, max_length=MAX_COMPENSATION_LENGTH)
    launch_url: str | None = Field(default=None, max_length=MAX_URL_LENGTH)
    comparison_url: str | None = Field(default=None, max_length=MAX_URL_LENGTH)
    authoritative_fields: tuple[str, ...] = Field(max_length=6)
    fetched: FetchedCaptureFields | None = None
    fetch_error: str | None = Field(default=None, max_length=80)
    salary_min: int | None = Field(default=None, ge=0, le=MAX_PERSISTED_SALARY)
    salary_max: int | None = Field(default=None, ge=0, le=MAX_PERSISTED_SALARY)
    salary_currency: str = Field(default="UNK", min_length=3, max_length=3)
    compensation_period: SalaryPeriod = SalaryPeriod.UNKNOWN
    compensation_confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    compensation_evidence: str | None = Field(default=None, max_length=2_000)
    duplicate_candidates: tuple[DuplicateCandidate, ...] = Field(
        default_factory=tuple, max_length=MAX_DUPLICATE_CANDIDATES
    )
    missing_required_fields: tuple[str, ...] = Field(
        default_factory=tuple, max_length=2
    )
    warnings: tuple[BoundedWarning, ...] = Field(default_factory=tuple, max_length=100)
    fingerprint: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    @property
    def ready_to_save(self) -> bool:
        return not self.missing_required_fields


def preview_capture(
    database: Database,
    draft: CaptureDraft | Mapping[str, object] | None = None,
    *,
    url: str | None = None,
    company: str | None = None,
    title: str | None = None,
    location: str | None = None,
    description: str | None = None,
    compensation: str | None = None,
    fetcher: StaticPageFetcher | None = None,
    cancel: CancelCheck | None = None,
) -> CapturePreview:
    """Build a read-only capture preview.

    Callers may pass a :class:`CaptureDraft`, a compatible mapping, or keyword
    fields.  The URL is validated before a fetcher is invoked.  Static-fetch
    failure is represented in the preview so pasted input remains usable.
    """

    _raise_if_cancelled(cancel)
    provided = (url, company, title, location, description, compensation)
    if draft is not None and any(value is not None for value in provided):
        raise ValueError("pass either a draft or capture field keywords, not both")
    if draft is None:
        draft_value = CaptureDraft(
            url=url,
            company=company,
            title=title,
            location=location,
            description=description,
            compensation=compensation,
        )
    elif isinstance(draft, CaptureDraft):
        draft_value = draft
    else:
        draft_value = CaptureDraft.model_validate(draft)

    fetched: FetchedCaptureFields | None = None
    fetch_error: str | None = None
    warnings: list[str] = []
    if draft_value.url:
        try:
            page = _coerce_static_page(
                fetcher(draft_value.url)
                if fetcher is not None
                else fetch_static_page(draft_value.url, cancel=cancel)
            )
            _raise_if_cancelled(cancel)
            fetched = _parse_static_page(page)
        except CaptureCancelledError:
            raise
        except Exception as exc:
            fetch_error = _fetch_error_code(exc)
            warnings.append(_fetch_warning(fetch_error))

    _raise_if_cancelled(cancel)
    merged_company = draft_value.company or (fetched.company if fetched else None)
    merged_title = draft_value.title or (fetched.title if fetched else None)
    merged_location = draft_value.location or (fetched.location if fetched else None)
    merged_description = draft_value.description or (
        fetched.description if fetched else None
    )
    merged_compensation = draft_value.compensation or (
        fetched.compensation if fetched else None
    )

    compensation_result = _capture_compensation(merged_compensation, merged_description)
    salary = compensation_result.salary
    if merged_compensation is None and compensation_result.evidence:
        merged_compensation = compensation_result.evidence
    warnings.extend(compensation_result.warnings)
    if salary is None:
        warnings.append(
            "No explicit compensation range was found; verify compensation manually."
        )

    salary_min, salary_max = _stored_salary_bounds(salary)
    launch_url = draft_value.url
    comparison_url = (normalize_url(launch_url) or None) if launch_url else None
    authoritative_fields = tuple(
        field for field in _AUTHORITATIVE_FIELD_ORDER if getattr(draft_value, field)
    )
    missing = tuple(
        field
        for field, value, normalized in (
            ("company", merged_company, normalize_company(merged_company)),
            ("title", merged_title, normalize_title(merged_title)),
        )
        if not value or not normalized
    )
    if missing:
        warnings.append(
            "Company and title are required before saving; add the missing "
            "pasted fields."
        )

    partial = {
        "company": merged_company,
        "title": merged_title,
        "location": merged_location,
        "description": merged_description,
        "compensation": merged_compensation,
        "launch_url": launch_url,
        "comparison_url": comparison_url,
        "authoritative_fields": authoritative_fields,
        "fetched": fetched,
        "fetch_error": fetch_error,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": salary.currency if salary else "UNK",
        "compensation_period": salary.period if salary else SalaryPeriod.UNKNOWN,
        "compensation_confidence": salary.confidence if salary else 0.0,
        "compensation_evidence": compensation_result.evidence,
        "missing_required_fields": missing,
    }
    fingerprint = _preview_fingerprint(partial)
    duplicates = _find_duplicate_candidates(database, partial)
    _raise_if_cancelled(cancel)
    if duplicates:
        warnings.append(
            f"{len(duplicates)} possible duplicate candidate(s) require review "
            "before saving."
        )
    return CapturePreview(
        **partial,
        duplicate_candidates=duplicates,
        warnings=tuple(dict.fromkeys(warnings)),
        fingerprint=fingerprint,
    )


# A descriptive alias for callers that prefer a noun-led service name.
build_capture_preview = preview_capture


def save_capture(
    database: Database,
    preview: CapturePreview,
    *,
    cancel: CancelCheck | None = None,
) -> Job:
    """Atomically persist an explicitly approved preview as a new manual job.

    Existing company and location rows may be referenced, but no existing row
    is modified.  Exact URL/source identity conflicts fail closed instead of
    overwriting richer or user-maintained data.
    """

    if not isinstance(preview, CapturePreview):
        preview = CapturePreview.model_validate(preview)
    _validate_preview(preview)
    if not preview.ready_to_save or not preview.company or not preview.title:
        missing = ", ".join(preview.missing_required_fields) or "company, title"
        raise CaptureNotReadyError(
            f"capture preview is missing required fields: {missing}"
        )
    _raise_if_cancelled(cancel)

    source_id = _manual_source_id(preview)
    now = utc_now()
    try:
        with database.session() as session:
            existing = session.scalar(
                select(Job.id).where(
                    Job.source_primary == "manual", Job.source_id == source_id
                )
            )
            if existing is None and preview.comparison_url:
                existing = session.scalar(
                    select(Job.id).where(
                        or_(
                            Job.comparison_url == preview.comparison_url,
                            Job.canonical_url == preview.comparison_url,
                        )
                    )
                )
            if existing is not None:
                raise CaptureConflictError(
                    f"capture matches existing job {existing}; no records were "
                    "overwritten"
                )

            company = session.scalar(
                select(Company).where(
                    Company.normalized_name == normalize_company(preview.company)
                )
            )
            if company is None:
                company = Company(
                    name=preview.company,
                    normalized_name=normalize_company(preview.company),
                )
                session.add(company)
                session.flush()

            job_location: Location | None = None
            if preview.location:
                location_key = normalize_location(preview.location)
                job_location = session.scalar(
                    select(Location).where(Location.normalized_key == location_key)
                )
                if job_location is None:
                    remote = normalize_remote(location=preview.location)
                    job_location = Location(
                        display_name=preview.location,
                        normalized_key=location_key,
                        remote=remote == RemoteStatus.REMOTE,
                    )
                    session.add(job_location)
                    session.flush()

            _raise_if_cancelled(cancel)
            remote_status = normalize_remote(location=preview.location).value
            job = Job(
                company_id=company.id,
                location_id=job_location.id if job_location else None,
                title=preview.title,
                normalized_title=normalize_title(preview.title),
                canonical_url=preview.comparison_url,
                launch_url=preview.launch_url,
                comparison_url=preview.comparison_url,
                source_primary="manual",
                source_id=source_id,
                description=preview.description,
                description_hash=content_hash(preview.description) or None,
                status=JobStatus.DISCOVERED,
                remote_status=remote_status,
                compensation_text=preview.compensation,
                salary_min=preview.salary_min,
                salary_max=preview.salary_max,
                salary_currency=preview.salary_currency,
                compensation_period=preview.compensation_period.value,
                compensation_confidence=preview.compensation_confidence,
                compensation_evidence=preview.compensation_evidence,
                authoritative_fields=list(preview.authoritative_fields),
                discovered_at=now,
                last_seen_at=now,
                liveness_known=False,
            )
            session.add(job)
            session.flush()
            _raise_if_cancelled(cancel)

            payload = _observation_payload(preview)
            snapshot_hash = _json_hash(payload)
            observation = SourceObservation(
                job_id=job.id,
                import_key=_manual_import_key(source_id),
                scan_run_id=None,
                source="manual",
                source_job_id=source_id,
                source_url=preview.launch_url,
                title_snapshot=preview.title,
                company_snapshot=preview.company,
                location_snapshot=preview.location,
                observed_at=now,
                is_live=None,
                content_hash=content_hash(preview.description) or None,
                raw_payload=payload,
                snapshot_hash=snapshot_hash,
                observation_kind="manual_capture",
            )
            session.add(observation)
            session.add(
                JobSourceState(
                    job_id=job.id,
                    source="manual",
                    source_job_id=source_id,
                    first_seen_at=now,
                    last_seen_at=now,
                    seen_count=1,
                    last_content_hash=content_hash(preview.description) or None,
                    last_snapshot_hash=snapshot_hash,
                    last_source_run_id=None,
                    is_live=None,
                )
            )
            record_audit(
                session,
                action="job.capture_saved",
                entity_type="job",
                entity_id=job.id,
                actor="user",
                after={
                    "source": "manual",
                    "source_id": source_id,
                    "authoritative_fields": preview.authoritative_fields,
                    "preview_fingerprint": preview.fingerprint,
                },
            )
            session.flush()
            _raise_if_cancelled(cancel)
            return job
    except IntegrityError as exc:
        raise CaptureConflictError(
            "capture conflicts with an existing record; no records were overwritten"
        ) from exc


def fetch_static_page(
    url: str,
    *,
    timeout_ms: int = 20_000,
    cancel: CancelCheck | None = None,
) -> StaticPage:
    """Fetch one public static HTML page through the existing SSRF-safe reader.

    Redirects are revalidated and DNS is pinned by ``PublicPortalSource``.  No
    JavaScript, authentication, CAPTCHA bypass, or browser runtime is used.
    """

    value = CaptureDraft(url=url)
    assert value.url is not None
    _raise_if_cancelled(cancel)
    source = PublicPortalSource(
        PortalConfig(name="Manual capture", url=value.url, timeout_ms=timeout_ms)
    )
    source.configure_runtime(
        deadline_at=time.monotonic() + timeout_ms / 1_000,
        cancelled=lambda: _is_cancelled(cancel),
    )
    try:
        page_html, final_url, status = source._fetch_html()  # noqa: SLF001
    except InterruptedError as exc:
        raise CaptureCancelledError("capture was cancelled") from exc
    except Exception as exc:
        raise CaptureFetchError(
            _fetch_error_code(exc), _fetch_warning(_fetch_error_code(exc))
        ) from exc
    return StaticPage(html=page_html, final_url=final_url, http_status=status)


def _parse_static_page(page: StaticPage) -> FetchedCaptureFields:
    parser = _StaticJobParser()
    try:
        parser.feed(page.html)
        parser.close()
    except Exception as exc:
        raise CaptureFetchError(
            "malformed_html", "The static page could not be parsed safely."
        ) from exc
    body = _collapse(" ".join(parser.visible_text))
    if _RESTRICTED_PAGE_RE.search(body):
        raise CaptureFetchError(
            "restricted_page",
            "The page requires authentication or a challenge; Jobby will not "
            "bypass it.",
        )

    posting = _first_job_posting(parser.json_documents)
    company = _posting_company(posting) or _meta_first(
        parser.meta, "og:site_name", "application-name", "author"
    )
    metadata_title = _meta_first(parser.meta, "og:title", "twitter:title")
    title = _posting_text(posting, "title") or metadata_title
    metadata_description = _meta_first(
        parser.meta, "description", "og:description", "twitter:description"
    )
    if (
        _JS_ONLY_RE.search(body)
        and not posting
        and not any((metadata_title, metadata_description, company))
    ):
        raise CaptureFetchError(
            "static_content_unavailable",
            "No useful static job fields were available; JavaScript is not supported.",
        )
    if not title:
        title = _collapse(" ".join(parser.title_text)) or None
    if title:
        title = _clean_page_title(title, company)
    location = _posting_location(posting) or _meta_first(
        parser.meta, "job:location", "location", "og:locality"
    )
    description = _posting_text(posting, "description") or metadata_description
    if description:
        description = _plain_text(description, MAX_DESCRIPTION_LENGTH)
    elif body:
        description = body[:MAX_DESCRIPTION_LENGTH]
    compensation = _posting_compensation(posting)
    if not compensation and description:
        compensation = extract_compensation(description).evidence

    if not any((company, title, location, description, compensation)):
        code = (
            "static_content_unavailable" if _JS_ONLY_RE.search(body) else "parse_error"
        )
        raise CaptureFetchError(
            code,
            "No useful static job fields were available; JavaScript and "
            "authentication are not supported.",
        )
    return FetchedCaptureFields(
        final_url=page.final_url,
        http_status=page.http_status,
        company=_bounded_optional(company, MAX_COMPANY_LENGTH),
        title=_bounded_optional(title, MAX_TITLE_LENGTH),
        location=_bounded_optional(location, MAX_LOCATION_LENGTH),
        description=_bounded_optional(description, MAX_DESCRIPTION_LENGTH),
        compensation=_bounded_optional(compensation, MAX_COMPENSATION_LENGTH),
    )


class _StaticJobParser(HTMLParser):
    """Collect only bounded metadata, visible text, and JSON-LD."""

    _ignored = frozenset({"style", "svg", "template", "noscript"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.meta: dict[str, str] = {}
        self.title_text: list[str] = []
        self.visible_text: list[str] = []
        self.json_documents: list[str] = []
        self._ignored_depth = 0
        self._in_title = False
        self._in_json_ld = False
        self._json_parts: list[str] = []
        self._visible_length = 0
        self._json_length = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        attributes = {str(key).casefold(): str(value or "") for key, value in attrs}
        if tag == "script":
            content_type = (
                attributes.get("type", "").split(";", 1)[0].strip().casefold()
            )
            self._in_json_ld = content_type == "application/ld+json"
            self._json_parts = []
            if not self._in_json_ld:
                self._ignored_depth += 1
            return
        if tag in self._ignored:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if tag == "title":
            self._in_title = True
        elif tag == "meta":
            key = (
                (
                    attributes.get("property")
                    or attributes.get("name")
                    or attributes.get("itemprop")
                    or ""
                )
                .strip()
                .casefold()
            )
            value = attributes.get("content", "").strip()
            if key and value and key not in self.meta and len(self.meta) < 500:
                self.meta[key[:200]] = value[:MAX_DESCRIPTION_LENGTH]

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag == "script":
            if self._in_json_ld and self._json_parts and len(self.json_documents) < 20:
                self.json_documents.append("".join(self._json_parts)[:200_000])
            elif not self._in_json_ld and self._ignored_depth:
                self._ignored_depth -= 1
            self._in_json_ld = False
            self._json_parts = []
        elif tag in self._ignored and self._ignored_depth:
            self._ignored_depth -= 1
        elif tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_json_ld:
            remaining = 200_000 - self._json_length
            if remaining > 0:
                part = data[:remaining]
                self._json_parts.append(part)
                self._json_length += len(part)
            return
        if self._ignored_depth:
            return
        if self._in_title:
            self.title_text.append(data[:10_000])
        remaining = MAX_DESCRIPTION_LENGTH - self._visible_length
        if remaining > 0:
            part = data[:remaining]
            self.visible_text.append(part)
            self._visible_length += len(part)


def _first_job_posting(documents: list[str]) -> Mapping[str, object]:
    for document in documents[:20]:
        try:
            value = json.loads(document)
        except (json.JSONDecodeError, RecursionError, TypeError, ValueError):
            continue
        pending: list[tuple[object, int]] = [(value, 0)]
        visited = 0
        while pending and visited < 1_000:
            item, depth = pending.pop()
            visited += 1
            if isinstance(item, Mapping):
                item_type = item.get("@type")
                types = item_type if isinstance(item_type, list) else [item_type]
                if any(str(value).casefold() == "jobposting" for value in types):
                    return {str(key): value for key, value in item.items()}
                if depth < 20:
                    pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list) and depth < 20:
                pending.extend((child, depth + 1) for child in item[:1_000])
    return {}


def _posting_text(posting: Mapping[str, object], key: str) -> str | None:
    value = posting.get(key)
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return str(value).strip() or None
    return None


def _posting_company(posting: Mapping[str, object]) -> str | None:
    organization = posting.get("hiringOrganization")
    if isinstance(organization, Mapping):
        value = organization.get("name")
        if isinstance(value, str):
            return _collapse(value) or None
    return None


def _posting_location(posting: Mapping[str, object]) -> str | None:
    location_type = str(posting.get("jobLocationType") or "").casefold()
    if "telecommute" in location_type or "remote" in location_type:
        return "Remote"
    locations = posting.get("jobLocation")
    candidates = locations if isinstance(locations, list) else [locations]
    for candidate in candidates[:20]:
        if not isinstance(candidate, Mapping):
            continue
        address = candidate.get("address", candidate)
        if isinstance(address, str):
            value = _collapse(address)
            if value:
                return value
        if isinstance(address, Mapping):
            parts = []
            for key in (
                "addressLocality",
                "addressRegion",
                "addressCountry",
            ):
                part = address.get(key)
                if isinstance(part, Mapping):
                    part = part.get("name")
                if part is not None and (text := _collapse(str(part))):
                    parts.append(text)
            if parts:
                return ", ".join(dict.fromkeys(parts))
    return None


def _posting_compensation(posting: Mapping[str, object]) -> str | None:
    salary = posting.get("baseSalary") or posting.get("estimatedSalary")
    if not isinstance(salary, Mapping):
        return None
    currency = str(salary.get("currency") or "").upper().strip()
    value = salary.get("value", salary)
    minimum: object = None
    maximum: object = None
    period: object = None
    if isinstance(value, Mapping):
        minimum = value.get("minValue", value.get("value"))
        maximum = value.get("maxValue")
        period = value.get("unitText") or value.get("unitCode")
    elif isinstance(value, (str, int, float)) and not isinstance(value, bool):
        minimum = value
    normalized = normalize_salary(
        {
            "min": minimum,
            "max": maximum,
            "currency": currency or None,
            "period": period,
        }
    )
    if normalized is None:
        return None
    bounds = (
        f"{_decimal_text(normalized.minimum)} - {_decimal_text(normalized.maximum)}"
        if normalized.minimum is not None and normalized.maximum is not None
        else _decimal_text(normalized.minimum or normalized.maximum)
    )
    period_text = (
        f" per {normalized.period.value}"
        if normalized.period != SalaryPeriod.UNKNOWN
        else ""
    )
    return f"{normalized.currency} {bounds}{period_text}".strip()


def _capture_compensation(
    compensation: str | None, description: str | None
) -> CompensationExtraction:
    if compensation:
        extracted = extract_compensation(compensation)
        if extracted.salary is not None:
            return extracted
        salary = normalize_salary(compensation)
        if salary is not None:
            warnings = _salary_warnings(salary)
            return CompensationExtraction(salary, salary.evidence, warnings)
    if description:
        extracted = extract_compensation(description)
        if extracted.salary is not None:
            return extracted
    return CompensationExtraction(None, None)


def _salary_warnings(salary: NormalizedSalary) -> tuple[str, ...]:
    warnings: list[str] = []
    if salary.currency != "USD":
        warnings.append(
            "Compensation is non-USD or its currency is unknown; salary floors "
            "were not applied."
        )
    if salary.period == SalaryPeriod.UNKNOWN:
        warnings.append(
            "Compensation period is unknown; the amount was not annualized or "
            "used as a rejection gate."
        )
    elif salary.confidence < 0.8:
        warnings.append(
            "Compensation normalization confidence is low; verify the range manually."
        )
    return tuple(warnings)


def _stored_salary_bounds(
    salary: NormalizedSalary | None,
) -> tuple[int | None, int | None]:
    if salary is None:
        return None, None
    minimum = (
        salary.annual_minimum if salary.annualization_confident else salary.minimum
    )
    maximum = (
        salary.annual_maximum if salary.annualization_confident else salary.maximum
    )
    return _decimal_int(minimum), _decimal_int(maximum)


def _find_duplicate_candidates(
    database: Database, values: Mapping[str, object]
) -> tuple[DuplicateCandidate, ...]:
    company = str(values.get("company") or "")
    title = str(values.get("title") or "")
    location = str(values.get("location") or "")
    description = str(values.get("description") or "")
    comparison_url = str(values.get("comparison_url") or "")
    normalized_company = normalize_company(company)
    normalized_title = normalize_title(title)
    normalized_location = normalize_location(location)
    description_digest = content_hash(description)
    conditions = []
    if comparison_url:
        conditions.extend(
            (Job.comparison_url == comparison_url, Job.canonical_url == comparison_url)
        )
    if normalized_company:
        conditions.append(Company.normalized_name == normalized_company)
    if description_digest:
        conditions.append(Job.description_hash == description_digest)
    if not conditions:
        return ()

    with database.session() as session:
        statement = (
            select(
                Job.id,
                Job.title,
                Job.normalized_title,
                Job.launch_url,
                Job.canonical_url,
                Job.comparison_url,
                Job.description_hash,
                Company.name,
                Company.normalized_name,
                Location.display_name,
                Location.normalized_key,
            )
            .join(Company, Company.id == Job.company_id)
            .outerjoin(Location, Location.id == Job.location_id)
            .where(or_(*conditions))
        )
        priority = []
        if comparison_url:
            priority.append(
                case(
                    (
                        or_(
                            Job.comparison_url == comparison_url,
                            Job.canonical_url == comparison_url,
                        ),
                        0,
                    ),
                    else_=1,
                )
            )
        if description_digest:
            priority.append(
                case((Job.description_hash == description_digest, 0), else_=1)
            )
        rows = session.execute(
            statement.order_by(*priority, Job.discovered_at.desc(), Job.id).limit(
                MAX_DUPLICATE_ROWS
            )
        ).all()

    candidates: list[DuplicateCandidate] = []
    for row in rows:
        reason: str | None = None
        similarity = 0.0
        exact = False
        row_url = row.comparison_url or row.canonical_url or ""
        if comparison_url and row_url == comparison_url:
            reason, similarity, exact = "comparison_url", 1.0, True
        elif description_digest and row.description_hash == description_digest:
            reason, similarity, exact = "content_hash", 1.0, True
        elif (
            normalized_company
            and row.normalized_name == normalized_company
            and normalized_title
            and row.normalized_title == normalized_title
            and (row.normalized_key or "") == normalized_location
        ):
            reason, similarity, exact = "normalized_fields", 1.0, True
        elif normalized_company and row.normalized_name == normalized_company:
            left_identity = f"{normalized_title} {normalized_location}".strip()
            right_identity = (
                f"{row.normalized_title} {row.normalized_key or ''}".strip()
            )
            similarity = token_jaccard(left_identity, right_identity)
            if similarity >= 0.8:
                reason = "fuzzy"
        if reason is None:
            continue
        candidates.append(
            DuplicateCandidate(
                job_id=row.id,
                company=row.name,
                title=row.title,
                location=row.display_name,
                launch_url=row.launch_url or row.canonical_url,
                reason=reason,
                similarity=similarity,
                exact=exact,
            )
        )
    reason_order = {
        "comparison_url": 0,
        "content_hash": 1,
        "normalized_fields": 2,
        "fuzzy": 3,
    }
    candidates.sort(
        key=lambda item: (
            reason_order.get(item.reason, 99),
            -item.similarity,
            item.company.casefold(),
            item.title.casefold(),
            item.job_id,
        )
    )
    return tuple(candidates[:MAX_DUPLICATE_CANDIDATES])


def _validate_preview(preview: CapturePreview) -> None:
    values = {
        "company": preview.company,
        "title": preview.title,
        "location": preview.location,
        "description": preview.description,
        "compensation": preview.compensation,
        "launch_url": preview.launch_url,
        "comparison_url": preview.comparison_url,
        "authoritative_fields": preview.authoritative_fields,
        "fetched": preview.fetched,
        "fetch_error": preview.fetch_error,
        "salary_min": preview.salary_min,
        "salary_max": preview.salary_max,
        "salary_currency": preview.salary_currency,
        "compensation_period": preview.compensation_period,
        "compensation_confidence": preview.compensation_confidence,
        "compensation_evidence": preview.compensation_evidence,
        "missing_required_fields": preview.missing_required_fields,
    }
    if _preview_fingerprint(values) != preview.fingerprint:
        raise ValueError(
            "capture preview fingerprint does not match its approved fields"
        )
    if preview.launch_url:
        if not is_public_http_url(preview.launch_url):
            raise ValueError("capture preview launch URL is no longer valid")
        if normalize_url(preview.launch_url) != preview.comparison_url:
            raise ValueError("capture preview URL identities are inconsistent")
    elif preview.comparison_url is not None:
        raise ValueError("capture preview has a comparison URL without a launch URL")


def _preview_fingerprint(values: Mapping[str, object]) -> str:
    payload = {
        "company": values.get("company"),
        "title": values.get("title"),
        "location": values.get("location"),
        "description": values.get("description"),
        "compensation": values.get("compensation"),
        "launch_url": values.get("launch_url"),
        "comparison_url": values.get("comparison_url"),
        "authoritative_fields": _sequence_list(values.get("authoritative_fields")),
        "fetched": _model_dump(values.get("fetched")),
        "fetch_error": values.get("fetch_error"),
        "salary_min": values.get("salary_min"),
        "salary_max": values.get("salary_max"),
        "salary_currency": values.get("salary_currency"),
        "compensation_period": _enum_value(values.get("compensation_period")),
        "compensation_confidence": values.get("compensation_confidence"),
        "compensation_evidence": values.get("compensation_evidence"),
        "missing_required_fields": _sequence_list(
            values.get("missing_required_fields")
        ),
    }
    return _json_hash(payload)


def _manual_source_id(preview: CapturePreview) -> str:
    identity = preview.comparison_url or "\x1f".join(
        (
            normalize_company(preview.company),
            normalize_title(preview.title),
            normalize_location(preview.location),
            content_hash(preview.description),
            content_hash(preview.compensation),
        )
    )
    return hashlib.sha256(f"jobby:manual-capture:v1:{identity}".encode()).hexdigest()


def _manual_import_key(source_id: str) -> str:
    return hashlib.sha256(
        f"jobby:manual-observation:v1:{source_id}".encode()
    ).hexdigest()


def _observation_payload(preview: CapturePreview) -> dict[str, object]:
    return {
        "capture_version": 1,
        "preview_fingerprint": preview.fingerprint,
        "authoritative_fields": list(preview.authoritative_fields),
        "company": preview.company,
        "title": preview.title,
        "location": preview.location,
        "description": preview.description,
        "compensation": preview.compensation,
        "launch_url": preview.launch_url,
        "comparison_url": preview.comparison_url,
        "fetched": preview.fetched.model_dump(mode="json") if preview.fetched else None,
        "fetch_error": preview.fetch_error,
        "normalization": {
            "salary_min": preview.salary_min,
            "salary_max": preview.salary_max,
            "currency": preview.salary_currency,
            "period": preview.compensation_period.value,
            "confidence": preview.compensation_confidence,
            "evidence": preview.compensation_evidence,
        },
    }


def _coerce_static_page(
    value: StaticPage | tuple[str, str, int] | Mapping[str, object],
) -> StaticPage:
    if isinstance(value, StaticPage):
        return value
    if isinstance(value, tuple) and len(value) == 3:
        page_html, final_url, http_status = value
        return StaticPage(
            html=str(page_html),
            final_url=str(final_url),
            http_status=http_status,
        )
    return StaticPage.model_validate(value)


def _fetch_error_code(exc: Exception) -> str:
    if isinstance(exc, CaptureFetchError):
        return exc.code
    code = str(getattr(exc, "code", "")).casefold()
    if code in _FETCH_ERROR_CODES:
        return code
    name = type(exc).__name__.casefold()
    if "timeout" in name or "deadline" in name:
        return "timeout"
    return "network_error"


def _fetch_warning(code: str) -> str:
    messages = {
        "restricted_page": (
            "Static fetch was blocked by authentication or a challenge; Jobby "
            "did not bypass it."
        ),
        "response_too_large": "Static page exceeded the 2 MB capture safety limit.",
        "unsupported_content": "The URL did not return a supported static HTML page.",
        "unsafe_target": "Static fetch was blocked because the target was not public.",
        "unsafe_redirect": (
            "Static fetch was blocked because a redirect was not public."
        ),
        "timeout": "Static fetch timed out; pasted fields remain available.",
        "static_content_unavailable": (
            "No useful static fields were available; JavaScript pages are not "
            "supported."
        ),
        "parse_error": "No useful static job fields could be extracted.",
        "malformed_html": "The static page could not be parsed safely.",
    }
    return messages.get(code, "Static fetch failed; pasted fields remain available.")


def _raise_if_cancelled(cancel: CancelCheck | None) -> None:
    if _is_cancelled(cancel):
        raise CaptureCancelledError("capture was cancelled")


def _is_cancelled(cancel: CancelCheck | None) -> bool:
    if cancel is None:
        return False
    return bool(cancel.is_set() if isinstance(cancel, CancellationSignal) else cancel())


def _optional_text(value: object, *, collapse: bool) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "�").strip()
    if collapse:
        text = _collapse(text)
    return text or None


def _bounded_optional(value: object, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "�").strip()
    return text[:limit] or None


def _bounded_single_line(value: object, limit: int) -> str:
    return _collapse(str(value).replace("\x00", "�"))[:limit]


def _collapse(value: str) -> str:
    return _SPACE_RE.sub(" ", html.unescape(value)).strip()


def _plain_text(value: object, limit: int) -> str:
    return _collapse(_HTML_TAG_RE.sub(" ", str(value or "")))[:limit]


def _meta_first(meta: Mapping[str, str], *keys: str) -> str | None:
    for key in keys:
        if value := meta.get(key.casefold()):
            return _collapse(value) or None
    return None


def _clean_page_title(value: str, company: str | None) -> str:
    title = _collapse(value)
    if company:
        escaped = re.escape(_collapse(company))
        title = re.sub(
            rf"\s+(?:at|[-|–—:])\s*{escaped}(?:\s+(?:careers?|jobs?))?$",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip()
    parts = [part.strip() for part in re.split(r"\s+[|–—]\s+", title) if part.strip()]
    if len(parts) > 1:
        useful = [
            part
            for part in parts
            if part.casefold() not in {"careers", "jobs", "job opportunities"}
            and (not company or normalize_company(part) != normalize_company(company))
        ]
        if useful:
            title = useful[0]
    return title[:MAX_TITLE_LENGTH]


def _decimal_text(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.normalize(), "f")


def _decimal_int(value: Decimal | None) -> int | None:
    if value is None or not value.is_finite() or not 0 <= value <= MAX_PERSISTED_SALARY:
        return None
    return int(value)


def _model_dump(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return value


def _sequence_list(value: object) -> list[object]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _enum_value(value: object) -> object:
    return value.value if isinstance(value, SalaryPeriod) else value


def _json_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "CaptureCancelledError",
    "CaptureConflictError",
    "CaptureDraft",
    "CaptureError",
    "CaptureFetchError",
    "CaptureNotReadyError",
    "CapturePreview",
    "DuplicateCandidate",
    "FetchedCaptureFields",
    "StaticPage",
    "build_capture_preview",
    "fetch_static_page",
    "preview_capture",
    "save_capture",
]
