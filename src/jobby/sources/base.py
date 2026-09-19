"""Typed discovery source contracts and shared adapter behavior."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum, StrEnum
import hashlib
from itertools import islice
import re
import time
from typing import Any, ClassVar, cast

import httpx

from jobby.normalization import (
    NormalizedSalary,
    RemoteStatus,
    content_hash,
    is_public_http_url,
    normalize_company,
    normalize_location,
    normalize_title,
    normalize_url,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


DEFAULT_MAX_RESPONSE_BYTES = 25 * 1024 * 1024
MAX_RESULT_ITEMS = 100_000
TRANSIENT_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})


def bounded_source_key(provider: str, identity: str) -> str:
    """Retain readable provider identity without violating durable 80-char keys."""

    raw = f"{provider.strip().casefold()}:{identity.strip().casefold()}"
    if len(raw) <= 80:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    prefix = raw[: 80 - len(digest) - 1].rstrip(":-_.")
    return f"{prefix}:{digest}"


class SourceDeadlineExceeded(TimeoutError):
    """A cooperative source deadline expired between safe read operations."""


class ScanStatus(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SourceError:
    """A recoverable, persistable source or item failure."""

    code: str
    message: str
    retryable: bool = False
    item_index: int | None = None
    http_status: int | None = None
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        code = re.sub(r"[^a-z0-9_.-]+", "_", str(self.code or "").casefold())[:80]
        if not code:
            raise ValueError("source error code is required")
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "message", sanitize_error_message(self.message))
        if self.item_index is not None and self.item_index < 0:
            raise ValueError("item_index cannot be negative")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("http_status must be between 100 and 599")
        if self.retry_after_seconds is not None and not (
            0 <= self.retry_after_seconds <= 120
        ):
            raise ValueError("retry_after_seconds must be between zero and 120")


@dataclass(frozen=True, slots=True)
class ScanItem:
    """A source observation before it is merged into a canonical job."""

    source: str
    source_id: str
    company: str
    title: str
    url: str
    location: str = ""
    description: str = ""
    salary_text: str = ""
    salary: NormalizedSalary | None = None
    remote: RemoteStatus = RemoteStatus.UNKNOWN
    posted_at: datetime | None = None
    deadline: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        raw_url = str(self.url or "")
        if len(raw_url.strip()) > 8_000:
            raise ValueError("url must be at most 8,000 characters")
        if any(ord(character) < 32 or ord(character) == 127 for character in raw_url):
            raise ValueError("url must not contain control characters")
        limits = {
            "source": 80,
            "source_id": 500,
            "company": 300,
            "title": 500,
            "url": 8_000,
            "location": 500,
            "description": 500_000,
            "salary_text": 10_000,
        }
        for attribute, limit in limits.items():
            value = (
                str(getattr(self, attribute) or "").replace("\x00", "�").strip()[:limit]
            )
            object.__setattr__(self, attribute, value)
        if not self.source:
            raise ValueError("source is required")
        if not self.source_id:
            raise ValueError("source_id is required")
        if not self.company:
            raise ValueError("company is required")
        if not self.title:
            raise ValueError("title is required")
        if not is_public_http_url(self.url):
            raise ValueError("url must be a public HTTP(S) URL")
        if not isinstance(self.remote, RemoteStatus):
            object.__setattr__(self, "remote", RemoteStatus(str(self.remote)))
        object.__setattr__(self, "metadata", _bounded_metadata(self.metadata))

    @property
    def canonical_url(self) -> str:
        """Compatibility alias for the conservative comparison URL."""

        return normalize_url(self.url)

    @property
    def launch_url(self) -> str:
        return self.url

    @property
    def comparison_url(self) -> str:
        return normalize_url(self.url)

    @property
    def normalized_company(self) -> str:
        return normalize_company(self.company)

    @property
    def normalized_title(self) -> str:
        return normalize_title(self.title)

    @property
    def normalized_location(self) -> str:
        return normalize_location(self.location)

    @property
    def description_hash(self) -> str:
        return content_hash(self.description)

    @property
    def published_at(self) -> datetime | None:
        """Compatibility alias for source APIs that call this a publish date."""

        return self.posted_at

    @property
    def closes_at(self) -> datetime | None:
        """Compatibility alias for source APIs that call this a close date."""

        return self.deadline


@dataclass(frozen=True, slots=True)
class SourceResult:
    """The explicit outcome of scanning one configured source."""

    source: str
    status: ScanStatus
    items: tuple[ScanItem, ...] = ()
    errors: tuple[SourceError, ...] = ()
    started_at: datetime = field(default_factory=utc_now)
    finished_at: datetime = field(default_factory=utc_now)
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        source = str(self.source or "").strip()[:80]
        if not source:
            raise ValueError("result source is required")
        object.__setattr__(self, "source", source)
        items = tuple(islice(self.items, MAX_RESULT_ITEMS + 1))
        errors = tuple(islice(self.errors, 1_000))
        if len(items) > MAX_RESULT_ITEMS:
            items = items[:MAX_RESULT_ITEMS]
            errors += (
                SourceError(
                    code="result_limit",
                    message=(
                        "Source returned more than 100,000 items; the result was "
                        "truncated safely."
                    ),
                ),
            )
            object.__setattr__(
                self,
                "status",
                ScanStatus.PARTIAL if items else ScanStatus.FAILED,
            )
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "errors", errors)
        object.__setattr__(self, "metadata", _bounded_metadata(self.metadata))
        if self.finished_at < self.started_at:
            raise ValueError("finished_at cannot precede started_at")
        if self.status is ScanStatus.SUCCEEDED and self.errors:
            raise ValueError("a successful result cannot contain errors")
        if self.status is ScanStatus.PARTIAL and (not self.items or not self.errors):
            raise ValueError("a partial result needs both items and errors")
        if self.status is ScanStatus.FAILED and not self.errors:
            raise ValueError("a failed result needs at least one error")

    @property
    def ok(self) -> bool:
        return self.status in {ScanStatus.SUCCEEDED, ScanStatus.PARTIAL}

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def completed_at(self) -> datetime:
        return self.finished_at


class JobSource(ABC):
    """Interface implemented by every independent discovery provider."""

    name: ClassVar[str]

    def __init__(
        self,
        client: httpx.Client,
        *,
        source_key: str,
        concurrency_key: str | None = None,
    ) -> None:
        self.client = client
        self.source_key = source_key
        # Multiple configured boards often share one upstream API host.  The
        # scanner uses this stable key to bound aggregate pressure on that
        # provider while retaining a distinct ``source_key`` for persistence.
        self.concurrency_key = (concurrency_key or source_key).strip().casefold()
        self.max_response_bytes = DEFAULT_MAX_RESPONSE_BYTES
        self.hydration_workers = 1
        self.inventory_metadata_only = False
        self._deadline_at: float | None = None
        self._cancelled: Callable[[], bool] = lambda: False

    def configure_runtime(
        self,
        *,
        deadline_at: float | None,
        cancelled: Callable[[], bool],
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        hydration_workers: int = 1,
        inventory_metadata_only: bool = False,
    ) -> None:
        """Bind per-run, read-only safety controls before a scan starts."""

        if deadline_at is not None and deadline_at <= 0:
            raise ValueError("source deadline must be a positive monotonic timestamp")
        if not 1_000_000 <= max_response_bytes <= 100 * 1024 * 1024:
            raise ValueError("source response limit must be between 1 MB and 100 MB")
        if not 1 <= hydration_workers <= 8:
            raise ValueError("source hydration workers must be between one and eight")
        self._deadline_at = deadline_at
        self._cancelled = cancelled
        self.max_response_bytes = max_response_bytes
        self.hydration_workers = hydration_workers
        self.inventory_metadata_only = bool(inventory_metadata_only)

    def checkpoint(self) -> None:
        """Cooperatively stop between pages without interrupting an HTTP write."""

        if self._cancelled():
            raise InterruptedError("discovery scan cancelled")
        if self._deadline_at is not None and time.monotonic() >= self._deadline_at:
            raise SourceDeadlineExceeded("source deadline exceeded")

    def remaining_seconds(self) -> float | None:
        if self._deadline_at is None:
            return None
        return max(0.0, self._deadline_at - time.monotonic())

    def request_timeout(self, maximum_seconds: float = 20.0) -> float:
        """Bound one blocking HTTP operation by the cooperative source deadline."""

        if maximum_seconds <= 0:
            raise ValueError("request timeout must be positive")
        self.checkpoint()
        remaining = self.remaining_seconds()
        if remaining is None:
            return maximum_seconds
        # ``checkpoint`` above rejects an already-expired deadline. Keep a tiny
        # positive floor for clocks that advance between that check and httpx's
        # timeout validation.
        return max(0.001, min(maximum_seconds, remaining))

    @abstractmethod
    def scan(self, query: str | None = None) -> SourceResult:
        """Discover current public postings without taking external actions."""

    def hydrate(self, item: ScanItem) -> ScanItem:
        """Fetch optional detail for one listing; adapters may keep metadata as-is."""

        self.checkpoint()
        return item


def sanitize_error_message(value: object, *, limit: int = 2_000) -> str:
    """Bound diagnostics and redact credential-shaped values before storage."""

    text = (str(value) or value.__class__.__name__).replace("\x00", "�")
    text = re.sub(r"(?i)\bsk-[a-z0-9_-]{12,}\b", "[REDACTED]", text)
    text = re.sub(
        r"(?i)\b(authorization\s*:\s*bearer\s+|bearer\s+)[^\s,;]+",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)\b(api[_ -]?key|x-api-key|authorization(?:[_ -]?key)?|"
        r"bearer|token|client[_ -]?secret|secret|password)"
        r"([\s:=\"']+)([^\s,;\"']+)",
        r"\1\2[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)(https?://)[^/@\s:]+:[^/@\s]+@", r"\1[REDACTED]@", text)
    return text[:limit]


def _bounded_metadata(value: object, *, budget: int = 50_000) -> dict[str, Any]:
    remaining = [budget]

    def convert(item: object, depth: int) -> Any:
        if remaining[0] <= 0:
            return "[truncated]"
        if item is None or isinstance(item, (bool, int, float)):
            remaining[0] -= 1
            return item
        if isinstance(item, Enum):
            return convert(item.value, depth)
        if isinstance(item, (date, datetime)):
            return convert(item.isoformat(), depth)
        if isinstance(item, str):
            text = item.replace("\x00", "�")[: min(4_000, remaining[0])]
            remaining[0] -= len(text)
            return text
        if depth >= 4:
            return str(item)[:200]
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for key, nested in islice(item.items(), 100):
                key_text = str(key).replace("\x00", "�")[:100]
                remaining[0] -= len(key_text)
                result[key_text] = convert(nested, depth + 1)
                if remaining[0] <= 0:
                    break
            return result
        if isinstance(item, Iterable) and not isinstance(item, (bytes, bytearray)):
            return [convert(nested, depth + 1) for nested in islice(item, 100)]
        if isinstance(item, (bytes, bytearray)):
            return bytes(item[:1_000]).hex()
        text = str(item).replace("\x00", "�")[: min(1_000, remaining[0])]
        remaining[0] -= len(text)
        return text

    converted = convert(value, 0)
    return converted if isinstance(converted, dict) else {"value": converted}


class StructuredJobSource(JobSource, ABC):
    """Shared scan lifecycle for ATS APIs that return a collection of records."""

    @abstractmethod
    def _fetch_records(
        self, query: str | None
    ) -> tuple[Iterable[object], Mapping[str, Any]]:
        """Fetch raw records and source-level metadata."""

    @abstractmethod
    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        """Convert one raw record into a typed observation."""

    def scan(self, query: str | None = None) -> SourceResult:
        started_at = utc_now()
        try:
            self.checkpoint()
            records, metadata = self._fetch_records(query)
            if isinstance(records, (str, bytes, Mapping)) or not isinstance(
                records, Iterable
            ):
                raise ValueError("source response did not contain a job collection")
        except InterruptedError:
            raise
        except (
            Exception
        ) as exc:  # adapters turn source failures into data, not false empty scans
            return self._failure(exc, started_at)

        items: list[ScanItem] = []
        errors: list[SourceError] = []
        for index, record in enumerate(records):
            if index >= MAX_RESULT_ITEMS:
                errors.append(
                    SourceError(
                        code="result_limit",
                        message=(
                            "Source returned more than 100,000 records; remaining "
                            "records were not processed."
                        ),
                    )
                )
                break
            if not isinstance(record, Mapping):
                errors.append(
                    SourceError(
                        code="malformed_item",
                        message=f"item {index} is not an object",
                        item_index=index,
                    )
                )
                continue
            try:
                items.append(self._parse_record(cast(Mapping[str, Any], record), index))
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(
                    SourceError(
                        code="malformed_item",
                        message=f"item {index}: {exc}",
                        item_index=index,
                    )
                )

        if metadata.get("truncated"):
            errors.append(
                SourceError(
                    code=str(metadata.get("truncation_code") or "result_truncated"),
                    message=str(
                        metadata.get("truncation_reason")
                        or (
                            "The source reported more matching jobs than the "
                            "configured safety cap allowed; this result is incomplete."
                        )
                    ),
                    retryable=bool(metadata.get("truncation_retryable", False)),
                    http_status=_optional_http_status(
                        metadata.get("truncation_http_status")
                    ),
                    retry_after_seconds=_optional_retry_after(
                        metadata.get("truncation_retry_after_seconds")
                    ),
                )
            )

        if errors and items:
            status = ScanStatus.PARTIAL
        elif errors:
            status = ScanStatus.FAILED
        else:
            status = ScanStatus.SUCCEEDED
        return SourceResult(
            source=self.source_key,
            status=status,
            items=tuple(items),
            errors=tuple(errors),
            started_at=started_at,
            finished_at=utc_now(),
            metadata=metadata,
        )

    def _failure(self, exc: Exception, started_at: datetime) -> SourceResult:
        error = source_error_from_exception(exc)
        return SourceResult(
            source=self.source_key,
            status=ScanStatus.FAILED,
            errors=(error,),
            started_at=started_at,
            finished_at=utc_now(),
        )


def source_error_from_exception(exc: Exception) -> SourceError:
    """Classify only explicitly safe transient read failures as retryable."""

    http_status: int | None = None
    retryable = False
    code = "source_error"
    retry_after_seconds: float | None = None
    if isinstance(exc, httpx.HTTPStatusError):
        http_status = exc.response.status_code
        retryable = http_status in TRANSIENT_HTTP_STATUSES
        code = "http_error"
        if retryable:
            retry_after_seconds = _retry_after_seconds(
                exc.response.headers.get("Retry-After")
            )
    elif isinstance(exc, httpx.TimeoutException):
        retryable = True
        code = "timeout"
    elif isinstance(exc, httpx.NetworkError):
        code = "network_error"
    elif isinstance(exc, SourceDeadlineExceeded):
        code = "source_deadline"
    elif isinstance(exc, ValueError):
        code = "malformed_response"
    return SourceError(
        code=code,
        message=str(exc) or exc.__class__.__name__,
        retryable=retryable,
        http_status=http_status,
        retry_after_seconds=retry_after_seconds,
    )


def _optional_http_status(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        status = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return status if 100 <= status <= 599 else None


def _optional_retry_after(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return min(120.0, max(0.0, seconds))


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            seconds = (target - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return min(120.0, max(0.0, seconds))


__all__ = [
    "DEFAULT_MAX_RESPONSE_BYTES",
    "JobSource",
    "MAX_RESULT_ITEMS",
    "ScanItem",
    "ScanStatus",
    "SourceError",
    "SourceDeadlineExceeded",
    "SourceResult",
    "StructuredJobSource",
    "TRANSIENT_HTTP_STATUSES",
    "bounded_source_key",
    "sanitize_error_message",
    "source_error_from_exception",
    "utc_now",
]
