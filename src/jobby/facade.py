"""The validated, headless application boundary used by CLI and MCP.

The facade intentionally deals in plain dictionaries and Pydantic values.  It
owns sessions and commits, keeps ORM objects inside the service boundary, and
puts the same approval and audit rules behind both interfaces.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select, update

from .analytics import analytics_report
from .application_workspace import create_contact, create_task
from .audit import record_audit
from .capture import CapturePreview, preview_capture, save_capture
from .config import AppConfig, JobbyPaths, SecretStore, resolve_paths
from .db import Database
from .documents import DocumentService
from .enums import (
    ApplicationStage,
    ApprovalState,
    DocumentStatus,
    ImportReviewStatus,
    JobStatus,
    TaskStatus,
)
from .models import (
    Application,
    AuditEvent,
    Company,
    CompanyCandidate,
    CompanyWatchlistEntry,
    Contact,
    DocumentVersion,
    DuplicateRelationship,
    Evaluation,
    ExternalSuggestion,
    ImportReview,
    Interview,
    InterviewAnswer,
    InterviewQuestion,
    InterviewSession,
    Job,
    Location,
    MutationApproval,
    OperationRun,
    Offer,
    ScanRun,
    SourceHealth,
    StageEvent,
    Task,
    utc_now,
)
from .pipeline import (
    compare_persisted_offers,
    create_application,
    create_interview,
    create_offer,
    set_offer_decision,
    transition_application,
    apply_external_suggestion,
)
from .review_queues import (
    confirm_duplicate,
    dismiss_duplicate,
    list_alert_inbox,
)
from .discovery_service import run_discovery_scan
from .job_queries import JobListFilters, JobSort, query_jobs_page
from .sources.base import sanitize_error_message
from .facade_serialization import (
    MAX_RESPONSE_BYTES,
    enforce_response_budget,
    model_dto,
)


MAX_PAGE = 200
MAX_DESCRIPTION = 100_000
DEFAULT_DESCRIPTION = 3_000
MAX_TEXT = 500_000
MAX_NESTING_DEPTH = 8
MAX_LIST_ITEMS = 200
VALID_FACETS = frozenset({"source", "status", "category", "company", "location"})
APPROVAL_TTL_SECONDS = 600
APPROVAL_REQUIRED_ACTORS = frozenset({"mcp_client", "agent", "scheduler"})
APPROVAL_ACTIONS = frozenset(
    {
        "application.create",
        "application.transition",
        "task.create",
        "contact.create",
        "interview.create",
        "interview_question.create",
        "interview_session.create",
        "offer.create",
        "offer.decision",
        "company.watch",
        "company.unwatch",
    }
)
MUTATION_POLICY: dict[str, dict[str, object]] = {
    action: {"approval_required_for_non_human": True, "cas": True}
    for action in APPROVAL_ACTIONS
}
MUTATION_POLICY.update(
    {
        "review.approve": {"approval_required_for_non_human": False, "cas": True},
        "review.dismiss": {"approval_required_for_non_human": False, "cas": True},
        "activity.log": {"approval_required_for_non_human": False, "cas": False},
    }
)


class FacadeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SearchInput(FacadeInput):
    query: str | None = Field(default=None, max_length=300)
    company: str | None = Field(default=None, max_length=200)
    location: str | None = Field(default=None, max_length=200)
    category: str | None = Field(default=None, max_length=100)
    source: str | None = Field(default=None, max_length=100)
    statuses: tuple[JobStatus, ...] = ()
    sort: JobSort = JobSort.SCORE_HIGH
    limit: int = Field(default=50, ge=1, le=MAX_PAGE)
    offset: int = Field(default=0, ge=0, le=1_000_000)
    cursor: str | None = Field(default=None, max_length=4_000)
    full_content: bool = False

    @field_validator("query", "company", "location", "category", "source")
    @classmethod
    def clean_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        return normalized or None


class CaptureInput(FacadeInput):
    url: str | None = Field(default=None, max_length=8_000)
    company: str | None = None
    title: str | None = None
    location: str | None = None
    description: str | None = Field(default=None, max_length=MAX_TEXT)
    compensation: str | None = Field(default=None, max_length=10_000)


class FacadeError(ValueError):
    """A client-safe validation error at the headless boundary."""


def _cursor_token(
    kind: str,
    identity: str,
    offset: int,
    *,
    after: Mapping[str, Any] | None = None,
) -> str:
    payload = {"v": 1, "kind": kind, "identity": identity, "offset": offset}
    if after is not None:
        payload["after"] = dict(after)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(encoded).decode().rstrip("=")


def _cursor_offset(cursor: str | None, *, kind: str, identity: str) -> int | None:
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not 1 <= len(cursor) <= 4_000:
        raise ValueError("cursor is invalid or too long")
    try:
        encoded = cursor.encode()
        encoded += b"=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded))
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("v") != 1
        or payload.get("kind") != kind
        or payload.get("identity") != identity
        or isinstance(payload.get("offset"), bool)
        or not isinstance(payload.get("offset"), int)
        or not 0 <= payload["offset"] <= 1_000_000
    ):
        raise ValueError("cursor does not match current filters or sort")
    return payload["offset"]


def _cursor_after(
    cursor: str | None, *, kind: str, identity: str
) -> dict[str, Any] | None:
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not 1 <= len(cursor) <= 4_000:
        raise ValueError("cursor is invalid or too long")
    try:
        encoded = cursor.encode()
        encoded += b"=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded))
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    after = payload.get("after") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("v") != 1
        or payload.get("kind") != kind
        or payload.get("identity") != identity
        or isinstance(payload.get("offset"), bool)
        or not isinstance(payload.get("offset"), int)
        or not 0 <= payload["offset"] <= 1_000_000
    ):
        raise ValueError("cursor does not match current filters or sort")
    if after is None:
        # Offset cursors remain accepted as a compatibility path. New cursors
        # carry a keyset position and callers prefer that position when present.
        return None
    if not isinstance(after, dict):
        raise ValueError("cursor does not match current filters or sort")
    return after


def _json(value: Any, *, depth: int = 0) -> Any:
    """Convert detached values to bounded JSON-compatible data."""

    if depth >= MAX_NESTING_DEPTH:
        return "[maximum depth exceeded]"
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Enum):
        try:
            return value.value
        except Exception:
            pass
    if isinstance(value, Mapping):
        return {
            str(key): _json(item, depth=depth + 1)
            for key, item in list(value.items())[:MAX_LIST_ITEMS]
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json(item, depth=depth + 1) for item in list(value)[:MAX_LIST_ITEMS]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and len(value) > MAX_TEXT:
            return (
                value[:MAX_TEXT]
                + f"…[truncated; {len(value) - MAX_TEXT} chars omitted]"
            )
        return value
    return str(value)


def _truncate(value: str | None, *, full: bool = False) -> str | None:
    if value is None:
        return None
    limit = MAX_DESCRIPTION if full else DEFAULT_DESCRIPTION
    if len(value) <= limit:
        return value
    return value[:limit] + f"…[truncated; {len(value) - limit} chars omitted]"


def _clean_list(values: Iterable[str] | None, *, max_items: int = 50) -> list[str]:
    if values is None:
        return []
    result: list[str] = []
    for value in values:
        text = " ".join(str(value).split())
        if text and text.casefold() not in {item.casefold() for item in result}:
            result.append(text[:500])
        if len(result) >= max_items:
            break
    return result


def _bounded_limit(limit: int, *, label: str) -> int:
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAX_PAGE
    ):
        raise ValueError(f"{label} pagination is bounded to 1..{MAX_PAGE} rows")
    return limit


def _matches_candidate_filter(
    candidate: Mapping[str, Any],
    expected: str | None,
    keys: tuple[str, ...],
) -> bool:
    if not expected:
        return True
    needle = expected.casefold()
    values: list[Any] = []
    for key in keys:
        value = candidate.get(key)
        if isinstance(value, (list, tuple, set, frozenset)):
            values.extend(value)
        elif value is not None:
            values.append(value)
    return bool(values) and any(needle in str(value).casefold() for value in values)


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _json(value), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    ).hexdigest()


def _cursor_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    from datetime import date

    if isinstance(value, date):
        return value.isoformat()
    return str(value)


_REVIEW_FIELDS: dict[str, tuple[str, ...]] = {
    "duplicate": (
        "id",
        "job_id",
        "duplicate_job_id",
        "rule",
        "similarity",
        "confirmed",
        "resolution",
        "comparison_identity",
        "resolved_at",
        "canonical_group_id",
        "created_at",
        "updated_at",
    ),
    "import": (
        "id",
        "workspace_root",
        "source_path",
        "record_key",
        "reason",
        "raw_excerpt",
        "proposed_json",
        "status",
        "created_at",
        "updated_at",
    ),
    "company_candidate": (
        "id",
        "company_id",
        "name",
        "website",
        "source",
        "role_filter",
        "location_filter",
        "industry_filter",
        "evidence",
        "score",
        "decision",
        "discovered_at",
        "created_at",
        "updated_at",
    ),
    "suggestion": (
        "id",
        "kind",
        "application_id",
        "email_message_id",
        "external_event_id",
        "payload",
        "confidence",
        "approval_state",
        "applied_at",
        "created_at",
        "updated_at",
    ),
}


def _review_record(review_type: str, row: object) -> dict[str, Any]:
    fields = _REVIEW_FIELDS.get(review_type)
    if fields is None:
        raise ValueError("unsupported review type")
    return model_dto(
        row,
        fields,
        limits={"workspace_root": 500, "source_path": 2_000, "raw_excerpt": 5_000},
    )


def _review_hash(review_type: str, row: object) -> str:
    """Hash the review snapshot used by compare-and-set review mutations."""

    return _hash_payload(
        {"review_type": review_type, "record": _review_record(review_type, row)}
    )


class ApplicationFacade:
    """Single-user headless API for local Jobby operations."""

    def __init__(
        self,
        database: Database | None = None,
        *,
        config: AppConfig | None = None,
        paths: JobbyPaths | None = None,
        secrets: SecretStore | None = None,
        actor: str = "user",
    ) -> None:
        self.paths = paths or (
            database.paths if database is not None else resolve_paths()
        )
        self.config = config or AppConfig()
        self.secrets = secrets or SecretStore()
        self.database = database or Database(paths=self.paths)
        self.database.initialize()
        self.actor = actor
        self._operation_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="jobby-operation"
        )

    def close(self) -> None:
        self._operation_executor.shutdown(wait=True, cancel_futures=False)
        self.database.dispose()

    def __enter__(self) -> "ApplicationFacade":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def prepare_mutation(
        self,
        action: str,
        payload: Mapping[str, Any],
        *,
        ttl_seconds: int = APPROVAL_TTL_SECONDS,
    ) -> dict[str, Any]:
        action = str(action).strip()
        if action not in APPROVAL_ACTIONS:
            raise ValueError("unsupported approval action")
        if not 30 <= ttl_seconds <= 3_600:
            raise ValueError("approval TTL must be between 30 and 3,600 seconds")
        payload_hash = _hash_payload(payload)
        with self.database.session() as session:
            row = MutationApproval(
                action=action,
                payload_hash=payload_hash,
                actor=self.actor,
                status="pending",
                expires_at=utc_now() + timedelta(seconds=ttl_seconds),
            )
            session.add(row)
            session.flush()
            event = record_audit(
                session,
                action="mutation_approval.prepared",
                entity_type="mutation_approval",
                entity_id=row.id,
                actor=self.actor,
                after={
                    "action": action,
                    "payload_hash": payload_hash,
                    "expires_at": row.expires_at,
                },
            )
            return {
                "approval_id": row.id,
                "action": action,
                "payload_hash": payload_hash,
                "expires_at": row.expires_at,
                "state": row.status,
                "audit_id": event.id,
            }

    def approve_mutation(
        self, approval_id: str, *, expected_hash: str
    ) -> dict[str, Any]:
        with self.database.session() as session:
            row = session.get(MutationApproval, approval_id)
            if row is None:
                raise LookupError("approval intent not found")
            if row.actor != self.actor:
                raise PermissionError("approval intent belongs to another actor")
            if row.status != "pending":
                raise ValueError("approval intent has already been reviewed")
            if row.expires_at <= utc_now():
                row.status = "expired"
                raise ValueError("approval intent has expired")
            if row.payload_hash != expected_hash:
                raise ValueError("approval intent hash does not match")
            row.status = "approved"
            row.approved_at = utc_now()
            event = record_audit(
                session,
                action="mutation_approval.approved",
                entity_type="mutation_approval",
                entity_id=row.id,
                actor=self.actor,
                after={"action": row.action, "payload_hash": row.payload_hash},
            )
            return {
                "approval_id": row.id,
                "action": row.action,
                "payload_hash": row.payload_hash,
                "state": row.status,
                "audit_id": event.id,
            }

    def _consume_approval(
        self,
        session: Any,
        *,
        approval_id: str | None,
        action: str,
        payload: Mapping[str, Any],
    ) -> str | None:
        if self.actor not in APPROVAL_REQUIRED_ACTORS:
            return None
        if not approval_id:
            raise PermissionError("this mutation requires an approved approval_id")
        payload_hash = _hash_payload(payload)
        now = utc_now()
        # A read followed by an ORM assignment permits two concurrent callers
        # to observe the same approval.  The conditional UPDATE is the
        # compare-and-set: exactly one transaction can move approved ->
        # consumed, and a later mutation cannot consume it again.
        result = session.execute(
            update(MutationApproval)
            .where(
                MutationApproval.id == approval_id,
                MutationApproval.actor == self.actor,
                MutationApproval.action == action,
                MutationApproval.payload_hash == payload_hash,
                MutationApproval.status == "approved",
                MutationApproval.expires_at > now,
            )
            .values(status="consumed", consumed_at=now)
        )
        if result.rowcount == 1:
            return approval_id
        row = session.get(MutationApproval, approval_id)
        if row is None:
            raise LookupError("approval intent not found")
        if row.actor != self.actor:
            raise PermissionError("approval intent belongs to another actor")
        if row.action != action or row.payload_hash != payload_hash:
            raise ValueError("approval intent does not match this mutation")
        if row.expires_at <= now:
            raise ValueError("approval intent has expired")
        raise ValueError("approval intent is not approved")

    def _audit_id(
        self, session: Any, *, entity_id: str | None = None, action: str | None = None
    ) -> str | None:
        statement = (
            select(AuditEvent)
            .order_by(AuditEvent.occurred_at.desc(), AuditEvent.id.desc())
            .limit(1)
        )
        if entity_id is not None:
            statement = statement.where(AuditEvent.entity_id == entity_id)
        if action is not None:
            statement = statement.where(AuditEvent.action == action)
        row = session.scalar(statement)
        return row.id if row is not None else None

    def _result(
        self,
        payload: Mapping[str, Any],
        *,
        session: Any | None = None,
        entity_id: str | None = None,
        action: str | None = None,
    ) -> dict[str, Any]:
        result = dict(_json(payload))
        if session is not None:
            result.setdefault(
                "audit_id", self._audit_id(session, entity_id=entity_id, action=action)
            )
        return enforce_response_budget(result, budget=MAX_RESPONSE_BYTES)

    # Read-only operations -------------------------------------------------

    def get_search_facets(self) -> dict[str, Any]:
        with self.database.session() as session:
            company = [
                value
                for value in session.scalars(
                    select(Company.name).distinct().order_by(Company.name).limit(500)
                )
            ]
            location = [
                value
                for value in session.scalars(
                    select(Location.display_name)
                    .distinct()
                    .order_by(Location.display_name)
                    .limit(500)
                )
            ]
            source = [
                value
                for value in session.scalars(
                    select(Job.source_primary)
                    .where(Job.source_primary.is_not(None))
                    .distinct()
                    .order_by(Job.source_primary)
                    .limit(500)
                )
            ]
            category = [
                value
                for value in session.scalars(
                    select(Job.category)
                    .where(Job.category.is_not(None))
                    .distinct()
                    .order_by(Job.category)
                    .limit(500)
                )
            ]
            return enforce_response_budget(
                {
                    "facets": {
                        "company": company,
                        "location": location,
                        "source": source,
                        "category": category,
                        "status": [item.value for item in JobStatus],
                    },
                    "valid_facets": sorted(VALID_FACETS),
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def search_jobs(
        self, request: SearchInput | Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        request = (
            request
            if isinstance(request, SearchInput)
            else SearchInput.model_validate(request or {})
        )
        statuses = frozenset(request.statuses) if request.statuses else None
        identity = _hash_payload(
            {
                "query": request.query,
                "company": request.company,
                "location": request.location,
                "category": request.category,
                "source": request.source,
                "statuses": sorted(item.value for item in statuses or ()),
                "sort": request.sort,
                "full_content": request.full_content,
            }
        )
        cursor_offset = _cursor_offset(request.cursor, kind="jobs", identity=identity)
        cursor_after = _cursor_after(request.cursor, kind="jobs", identity=identity)
        offset = (
            0
            if cursor_after is not None
            else (request.offset if cursor_offset is None else cursor_offset)
        )
        filters = JobListFilters(
            query=request.query,
            company=request.company,
            location=request.location,
            category=request.category,
            source=request.source,
            statuses=statuses,
            sort=request.sort,
            limit=request.limit + (1 if cursor_after is not None else 0),
            offset=offset,
            after=cursor_after,
        )
        with self.database.session() as session:
            page = query_jobs_page(session, filters)
            items = []
            page_items = page.items
            has_extra_keyset_item = (
                cursor_after is not None and len(page_items) > request.limit
            )
            if has_extra_keyset_item:
                page_items = page_items[: request.limit]
            for item in page_items:
                items.append(
                    {
                        "id": item.id,
                        "title": item.title,
                        "company": item.company_name,
                        "location": item.location_name,
                        "description": _truncate(
                            item.description, full=request.full_content
                        ),
                        "category": item.category,
                        "url": item.canonical_url,
                        "source": item.source_primary,
                        "status": item.status,
                        "score": item.latest_score,
                        "deadline": item.deadline,
                        "discovered_at": item.discovered_at,
                        "last_seen_at": item.last_seen_at,
                        "salary_min": item.salary_min,
                        "salary_max": item.salary_max,
                    }
                )
            has_more = (
                has_extra_keyset_item
                if cursor_after is not None
                else offset + len(items) < page.total_count
            )
            last_item = items[-1] if items else None
            after = (
                {
                    "id": last_item["id"],
                    "score": last_item["score"],
                    "discovered_at": _cursor_datetime(last_item["discovered_at"]),
                    "deadline": _cursor_datetime(last_item["deadline"]),
                    "company": last_item["company"],
                    "title": last_item["title"],
                }
                if last_item is not None
                else None
            )
            return enforce_response_budget(
                {
                    "items": items,
                    "total_count": page.total_count,
                    "offset": offset,
                    "limit": request.limit,
                    "has_more": has_more,
                    "next_cursor": (
                        _cursor_token(
                            "jobs",
                            identity,
                            offset + len(items),
                            after=after,
                        )
                        if has_more
                        else None
                    ),
                    "provenance": {
                        "query": request.query,
                        "sort": request.sort,
                        "filter_identity": identity,
                    },
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def get_job(self, job_id: str, *, full_content: bool = False) -> dict[str, Any]:
        with self.database.session() as session:
            row = session.execute(
                select(Job, Company, Location)
                .join(Company, Company.id == Job.company_id)
                .outerjoin(Location, Location.id == Job.location_id)
                .where(Job.id == job_id)
            ).first()
            if row is None:
                raise LookupError("job not found")
            job, company, location = row
            from .models import SourceObservation

            observations = list(
                session.scalars(
                    select(SourceObservation)
                    .where(SourceObservation.job_id == job.id)
                    .order_by(
                        SourceObservation.observed_at.desc(), SourceObservation.id
                    )
                    .limit(20)
                )
            )
            evaluation = session.scalar(
                select(Evaluation)
                .where(Evaluation.job_id == job.id, Evaluation.is_current.is_(True))
                .order_by(Evaluation.updated_at.desc())
                .limit(1)
            )
            return enforce_response_budget(
                {
                    "id": job.id,
                    "title": job.title,
                    "company": {
                        "id": company.id,
                        "name": company.name,
                        "website": company.website,
                    },
                    "location": (
                        {
                            "id": location.id,
                            "display_name": location.display_name,
                            "remote": location.remote,
                        }
                        if location
                        else None
                    ),
                    "url": job.launch_url or job.canonical_url,
                    "comparison_url": job.comparison_url,
                    "source": job.source_primary,
                    "source_id": job.source_id,
                    "description": _truncate(job.description, full=full_content),
                    "status": job.status,
                    "score": job.latest_score,
                    "compensation": job.compensation_text,
                    "salary_min": job.salary_min,
                    "salary_max": job.salary_max,
                    "posted_at": job.posted_at,
                    "deadline": job.deadline,
                    "discovered_at": job.discovered_at,
                    "last_seen_at": job.last_seen_at,
                    "liveness": {
                        "known": job.liveness_known,
                        "consecutive_misses": job.consecutive_misses,
                    },
                    "evaluation": (
                        _json(evaluation.components)
                        | {
                            "score": evaluation.score,
                            "explanation": evaluation.explanation,
                            "warnings": evaluation.warnings,
                        }
                        if evaluation
                        else None
                    ),
                    "provenance": [
                        {
                            "source": item.source,
                            "source_id": item.source_job_id,
                            "url": item.source_url,
                            "observed_at": item.observed_at,
                        }
                        for item in observations
                    ],
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def get_company(self, company_id: str) -> dict[str, Any]:
        with self.database.session() as session:
            company = session.get(Company, company_id)
            if company is None:
                raise LookupError("company not found")
            jobs = list(
                session.execute(
                    select(Job.id, Job.title, Job.status, Job.latest_score)
                    .where(Job.company_id == company.id)
                    .order_by(Job.updated_at.desc())
                    .limit(MAX_PAGE)
                )
            )
            watch = session.scalar(
                select(CompanyWatchlistEntry).where(
                    CompanyWatchlistEntry.company_id == company.id
                )
            )
            return enforce_response_budget(
                {
                    "id": company.id,
                    "name": company.name,
                    "website": company.website,
                    "notes": company.notes,
                    "jobs": [
                        {
                            "id": item.id,
                            "title": item.title,
                            "status": item.status,
                            "score": item.latest_score,
                        }
                        for item in jobs
                    ],
                    "watchlist": (
                        model_dto(
                            watch,
                            (
                                "id",
                                "company_id",
                                "criteria",
                                "cadence_days",
                                "enabled",
                                "last_scanned_at",
                                "created_at",
                                "updated_at",
                            ),
                        )
                        if watch
                        else None
                    ),
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def get_latest_scan(self) -> dict[str, Any] | None:
        with self.database.session() as session:
            row = session.scalar(
                select(ScanRun)
                .order_by(ScanRun.created_at.desc(), ScanRun.id.desc())
                .limit(1)
            )
            if row is None:
                return None
            return enforce_response_budget(
                model_dto(
                    row,
                    (
                        "id",
                        "status",
                        "query",
                        "requested_sources",
                        "started_at",
                        "finished_at",
                        "discovered_count",
                        "error_summary",
                        "source_results",
                        "created_at",
                    ),
                    limits={"error_summary": 2_000},
                ),
                budget=MAX_RESPONSE_BYTES,
            )

    def get_source_health(self) -> list[dict[str, Any]]:
        with self.database.session() as session:
            return enforce_response_budget(
                [
                    model_dto(
                        row,
                        (
                            "id",
                            "source",
                            "last_attempt_at",
                            "last_success_at",
                            "last_complete_at",
                            "last_result_count",
                            "last_reported_total",
                            "failure_streak",
                            "last_failure_class",
                            "anomaly_state",
                        ),
                        limits={"last_failure_class": 200},
                    )
                    for row in session.scalars(
                        select(SourceHealth)
                        .order_by(SourceHealth.source)
                        .limit(MAX_PAGE)
                    )
                ],
                budget=MAX_RESPONSE_BYTES,
            )

    def get_market_fit(self, job_id: str) -> dict[str, Any]:
        with self.database.session() as session:
            evaluation = session.scalar(
                select(Evaluation)
                .where(Evaluation.job_id == job_id, Evaluation.is_current.is_(True))
                .order_by(Evaluation.updated_at.desc())
                .limit(1)
            )
            if evaluation is None:
                raise LookupError("market-fit evaluation not found")
            return enforce_response_budget(
                {
                    "job_id": job_id,
                    "score": evaluation.score,
                    "components": _json(evaluation.components),
                    "gates": _json(evaluation.gates),
                    "evidence": _json(evaluation.evidence),
                    "warnings": _json(evaluation.warnings),
                    "explanation": evaluation.explanation,
                    "provenance": {
                        "ranker_version": evaluation.ranker_version,
                        "evaluation_id": evaluation.id,
                        "created_at": evaluation.created_at,
                    },
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def list_pipeline(
        self,
        *,
        stage: ApplicationStage | str | None = None,
        limit: int = MAX_PAGE,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= MAX_PAGE or offset < 0:
            raise ValueError("pipeline pagination is bounded to 1..200 rows")
        identity = _hash_payload({"stage": str(stage) if stage is not None else None})
        cursor_offset = _cursor_offset(cursor, kind="applications", identity=identity)
        cursor_after = _cursor_after(cursor, kind="applications", identity=identity)
        if cursor_after is not None:
            offset = 0
        elif cursor_offset is not None:
            offset = cursor_offset
        with self.database.session() as session:
            statement = (
                select(Application, Job, Company)
                .join(Job, Job.id == Application.job_id)
                .join(Company, Company.id == Job.company_id)
            )
            if stage is not None:
                statement = statement.where(
                    Application.current_stage == ApplicationStage(stage)
                )
            total = int(
                session.scalar(select(func.count()).select_from(statement.subquery()))
                or 0
            )
            if cursor_after is not None:
                last_updated = datetime.fromisoformat(str(cursor_after["updated_at"]))
                statement = statement.where(
                    (Application.updated_at < last_updated)
                    | (
                        (Application.updated_at == last_updated)
                        & (Application.id > str(cursor_after["id"]))
                    )
                )
            rows = session.execute(
                statement.order_by(Application.updated_at.desc(), Application.id)
                .offset(offset)
                .limit(limit + (1 if cursor_after is not None else 0))
            ).all()
            has_extra_keyset_row = cursor_after is not None and len(rows) > limit
            if has_extra_keyset_row:
                rows = rows[:limit]
            has_more = (
                has_extra_keyset_row
                if cursor_after is not None
                else offset + len(rows) < total
            )
            return enforce_response_budget(
                {
                    "items": [
                        {
                            "id": app.id,
                            "job_id": job.id,
                            "company": company.name,
                            "title": job.title,
                            "stage": app.current_stage,
                            "submitted_at": app.submitted_at,
                            "follow_up_at": app.follow_up_at,
                            "updated_at": app.updated_at,
                        }
                        for app, job, company in rows
                    ],
                    "total_count": total,
                    "offset": offset,
                    "limit": limit,
                    "has_more": has_more,
                    "next_cursor": (
                        _cursor_token(
                            "applications",
                            identity,
                            offset + len(rows),
                            after=(
                                {
                                    "id": rows[-1][0].id,
                                    "updated_at": _cursor_datetime(
                                        rows[-1][0].updated_at
                                    ),
                                }
                                if rows
                                else None
                            ),
                        )
                        if has_more
                        else None
                    ),
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def get_application(self, application_id: str) -> dict[str, Any]:
        with self.database.session() as session:
            row = session.execute(
                select(Application, Job, Company)
                .join(Job, Job.id == Application.job_id)
                .join(Company, Company.id == Job.company_id)
                .where(Application.id == application_id)
            ).first()
            if row is None:
                raise LookupError("application not found")
            app, job, company = row
            return enforce_response_budget(
                {
                    "id": app.id,
                    "job_id": job.id,
                    "job": {
                        "title": job.title,
                        "company": company.name,
                        "url": job.launch_url or job.canonical_url,
                    },
                    "stage": app.current_stage,
                    "submission_channel": app.submission_channel,
                    "submitted_at": app.submitted_at,
                    "follow_up_at": app.follow_up_at,
                    "notes": app.notes,
                    "rejection_reason": app.rejection_reason,
                    "provenance": {
                        "score": app.applied_score,
                        "ranker_version": app.applied_ranker_version,
                        "sources": app.applied_sources,
                    },
                    "history": [
                        model_dto(
                            event,
                            (
                                "id",
                                "from_stage",
                                "to_stage",
                                "occurred_at",
                                "reason",
                                "actor",
                                "source",
                            ),
                            limits={"reason": 2_000, "actor": 100, "source": 100},
                        )
                        for event in session.scalars(
                            select(StageEvent)
                            .where(StageEvent.application_id == app.id)
                            .order_by(StageEvent.occurred_at, StageEvent.id)
                            .limit(MAX_LIST_ITEMS)
                        )
                    ],
                    "tasks": [
                        model_dto(
                            task,
                            (
                                "id",
                                "title",
                                "description",
                                "status",
                                "due_at",
                                "completed_at",
                                "job_id",
                                "application_id",
                                "created_at",
                                "updated_at",
                            ),
                            limits={"title": 500, "description": DEFAULT_DESCRIPTION},
                        )
                        for task in session.scalars(
                            select(Task)
                            .where(Task.application_id == app.id)
                            .order_by(Task.due_at, Task.id)
                            .limit(MAX_LIST_ITEMS)
                        )
                    ],
                    "interviews": [
                        model_dto(
                            item,
                            (
                                "id",
                                "starts_at",
                                "ends_at",
                                "interview_type",
                                "location_or_link",
                                "contact_id",
                                "notes",
                                "created_at",
                                "updated_at",
                            ),
                            limits={
                                "location_or_link": 2_000,
                                "notes": DEFAULT_DESCRIPTION,
                            },
                        )
                        for item in session.scalars(
                            select(Interview)
                            .where(Interview.application_id == app.id)
                            .order_by(Interview.starts_at, Interview.id)
                            .limit(MAX_LIST_ITEMS)
                        )
                    ],
                    "interview_sessions": [
                        model_dto(
                            item,
                            (
                                "id",
                                "interview_id",
                                "application_id",
                                "session_type",
                                "started_at",
                                "ended_at",
                                "role_focus",
                                "notes",
                                "retrospective",
                                "outcome",
                                "created_at",
                                "updated_at",
                            ),
                            limits={
                                "notes": DEFAULT_DESCRIPTION,
                                "retrospective": DEFAULT_DESCRIPTION,
                            },
                        )
                        for item in session.scalars(
                            select(InterviewSession)
                            .where(InterviewSession.application_id == app.id)
                            .order_by(InterviewSession.started_at, InterviewSession.id)
                            .limit(MAX_LIST_ITEMS)
                        )
                    ],
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def list_tasks(
        self,
        *,
        status: TaskStatus | str | None = None,
        limit: int = MAX_PAGE,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= MAX_PAGE or offset < 0:
            raise ValueError("task pagination is bounded to 1..200 rows")
        identity = _hash_payload(
            {"status": str(status) if status is not None else None}
        )
        cursor_offset = _cursor_offset(cursor, kind="tasks", identity=identity)
        cursor_after = _cursor_after(cursor, kind="tasks", identity=identity)
        if cursor_after is not None:
            offset = 0
        elif cursor_offset is not None:
            offset = cursor_offset
        with self.database.session() as session:
            statement = select(Task)
            if status is not None:
                statement = statement.where(Task.status == TaskStatus(status))
            total = int(
                session.scalar(select(func.count()).select_from(statement.subquery()))
                or 0
            )
            if cursor_after is not None:
                last_id = str(cursor_after["id"])
                last_created = datetime.fromisoformat(str(cursor_after["created_at"]))
                last_due = cursor_after.get("due_at")
                if last_due is None:
                    statement = statement.where(
                        Task.due_at.is_(None)
                        & (
                            (Task.created_at > last_created)
                            | ((Task.created_at == last_created) & (Task.id > last_id))
                        )
                    )
                else:
                    due = datetime.fromisoformat(str(last_due))
                    statement = statement.where(
                        (Task.due_at > due)
                        | (
                            (Task.due_at == due)
                            & (
                                (Task.created_at > last_created)
                                | (
                                    (Task.created_at == last_created)
                                    & (Task.id > last_id)
                                )
                            )
                        )
                    )
            rows = list(
                session.scalars(
                    statement.order_by(Task.due_at, Task.created_at, Task.id)
                    .offset(offset)
                    .limit(limit + (1 if cursor_after is not None else 0))
                )
            )
            has_extra_keyset_row = cursor_after is not None and len(rows) > limit
            if has_extra_keyset_row:
                rows = rows[:limit]
            return enforce_response_budget(
                {
                    "items": [
                        model_dto(
                            row,
                            (
                                "id",
                                "title",
                                "description",
                                "status",
                                "due_at",
                                "completed_at",
                                "job_id",
                                "application_id",
                                "automation_key",
                                "created_at",
                                "updated_at",
                            ),
                            limits={"title": 500, "description": DEFAULT_DESCRIPTION},
                        )
                        for row in rows
                    ],
                    "limit": limit,
                    "offset": offset,
                    "has_more": (
                        has_extra_keyset_row
                        if cursor_after is not None
                        else offset + len(rows) < total
                    ),
                    "next_cursor": (
                        _cursor_token(
                            "tasks",
                            identity,
                            offset + len(rows),
                            after=(
                                {
                                    "id": rows[-1].id,
                                    "due_at": _cursor_datetime(rows[-1].due_at),
                                    "created_at": _cursor_datetime(rows[-1].created_at),
                                }
                                if rows
                                else None
                            ),
                        )
                        if (
                            has_extra_keyset_row
                            if cursor_after is not None
                            else offset + len(rows) < total
                        )
                        else None
                    ),
                    "total_count": total,
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def list_contacts(
        self, *, company_id: str | None = None, limit: int = MAX_PAGE
    ) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, label="contact")
        with self.database.session() as session:
            statement = select(Contact)
            if company_id:
                statement = statement.where(Contact.company_id == company_id)
            return enforce_response_budget(
                [
                    model_dto(
                        row,
                        (
                            "id",
                            "company_id",
                            "name",
                            "email",
                            "title",
                            "linkedin_url",
                            "notes",
                            "created_at",
                            "updated_at",
                        ),
                        limits={
                            "name": 300,
                            "email": 500,
                            "title": 300,
                            "linkedin_url": 2_000,
                            "notes": DEFAULT_DESCRIPTION,
                        },
                    )
                    for row in session.scalars(
                        statement.order_by(Contact.name).limit(min(limit, MAX_PAGE))
                    )
                ],
                budget=MAX_RESPONSE_BYTES,
            )

    def create_contact(
        self,
        *,
        name: str,
        company_id: str | None = None,
        email: str | None = None,
        title: str | None = None,
        linkedin_url: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="contact.create",
                payload={
                    "name": name,
                    "company_id": company_id,
                    "email": email,
                    "title": title,
                    "linkedin_url": linkedin_url,
                    "notes": notes,
                },
            )
            row = create_contact(
                session,
                name=name,
                company_id=company_id,
                email=email,
                title=title,
                linkedin_url=linkedin_url,
                notes=notes,
                actor=self.actor,
            )
            session.flush()
            return self._result(
                {"contact_id": row.id, "name": row.name},
                session=session,
                entity_id=row.id,
                action="contact.created",
            )

    def list_alerts(
        self, *, limit: int = MAX_PAGE, unread_only: bool = True
    ) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, label="alert")
        with self.database.session() as session:
            return enforce_response_budget(
                [
                    model_dto(
                        row,
                        (
                            "id",
                            "severity",
                            "title",
                            "message",
                            "job_id",
                            "application_id",
                            "acknowledged_at",
                            "created_at",
                            "updated_at",
                            "fingerprint",
                            "recurrence_count",
                            "last_recurred_at",
                            "snoozed_until",
                            "resolved_at",
                            "resolution_reason",
                            "entity_type",
                            "entity_id",
                        ),
                        limits={
                            "title": 500,
                            "message": 5_000,
                            "resolution_reason": 2_000,
                        },
                    )
                    for row in list_alert_inbox(
                        session, unread_only=unread_only, limit=min(limit, MAX_PAGE)
                    )
                ],
                budget=MAX_RESPONSE_BYTES,
            )

    def list_pending_reviews(self, *, limit: int = MAX_PAGE) -> list[dict[str, Any]]:
        return self.list_pending_reviews_page(limit=limit)["items"]

    def list_pending_reviews_page(
        self, *, limit: int = MAX_PAGE, cursor: str | None = None
    ) -> dict[str, Any]:
        if not 1 <= limit <= MAX_PAGE:
            raise ValueError("review pagination is bounded to 1..200 rows")
        identity = _hash_payload({"queue": "pending_reviews"})
        cursor_offset = _cursor_offset(cursor, kind="reviews", identity=identity)
        cursor_after = _cursor_after(cursor, kind="reviews", identity=identity)
        offset = 0 if cursor_after is not None else (cursor_offset or 0)
        fetch_limit = (
            MAX_PAGE + 1 if cursor_after is not None else min(MAX_PAGE, offset + limit)
        )
        with self.database.session() as session:
            after_timestamp = (
                datetime.fromisoformat(str(cursor_after["timestamp"]))
                if cursor_after is not None
                else None
            )
            after_id = str(cursor_after["id"]) if cursor_after is not None else None
            duplicate_statement = select(DuplicateRelationship).where(
                DuplicateRelationship.resolution == "pending"
            )
            import_statement = select(ImportReview).where(
                ImportReview.status == "pending"
            )
            candidate_statement = select(CompanyCandidate).where(
                CompanyCandidate.decision == "pending"
            )
            suggestion_statement = select(ExternalSuggestion).where(
                ExternalSuggestion.approval_state == ApprovalState.PENDING
            )
            if after_timestamp is not None and after_id is not None:
                duplicate_statement = duplicate_statement.where(
                    (DuplicateRelationship.created_at > after_timestamp)
                    | (
                        (DuplicateRelationship.created_at == after_timestamp)
                        & (DuplicateRelationship.id > after_id)
                    )
                )
                import_statement = import_statement.where(
                    (ImportReview.created_at > after_timestamp)
                    | (
                        (ImportReview.created_at == after_timestamp)
                        & (ImportReview.id > after_id)
                    )
                )
                candidate_statement = candidate_statement.where(
                    (CompanyCandidate.discovered_at > after_timestamp)
                    | (
                        (CompanyCandidate.discovered_at == after_timestamp)
                        & (CompanyCandidate.id > after_id)
                    )
                )
                suggestion_statement = suggestion_statement.where(
                    (ExternalSuggestion.created_at > after_timestamp)
                    | (
                        (ExternalSuggestion.created_at == after_timestamp)
                        & (ExternalSuggestion.id > after_id)
                    )
                )
            duplicates = list(
                session.scalars(
                    duplicate_statement.order_by(
                        DuplicateRelationship.created_at, DuplicateRelationship.id
                    ).limit(fetch_limit)
                )
            )
            imports = list(
                session.scalars(
                    import_statement.order_by(
                        ImportReview.created_at, ImportReview.id
                    ).limit(fetch_limit)
                )
            )
            candidates = list(
                session.scalars(
                    candidate_statement.order_by(
                        CompanyCandidate.discovered_at, CompanyCandidate.id
                    ).limit(fetch_limit)
                )
            )
            suggestions = list(
                session.scalars(
                    suggestion_statement.order_by(
                        ExternalSuggestion.created_at, ExternalSuggestion.id
                    ).limit(fetch_limit)
                )
            )
            items = (
                [
                    {
                        "review_type": "duplicate",
                        **_review_record("duplicate", row),
                        "review_hash": _review_hash("duplicate", row),
                    }
                    for row in duplicates
                ]
                + [
                    {
                        "review_type": "import",
                        **_review_record("import", row),
                        "review_hash": _review_hash("import", row),
                    }
                    for row in imports
                ]
                + [
                    {
                        "review_type": "company_candidate",
                        **_review_record("company_candidate", row),
                        "review_hash": _review_hash("company_candidate", row),
                    }
                    for row in candidates
                ]
                + [
                    {
                        "review_type": "suggestion",
                        **_review_record("suggestion", row),
                        "review_hash": _review_hash("suggestion", row),
                    }
                    for row in suggestions
                ]
            )
            # Fetching ``limit`` from each queue and merging by timestamp is
            # deterministic and avoids a type-order bias in the first page.
            items.sort(
                key=lambda item: (
                    str(
                        item.get("created_at")
                        or item.get("discovered_at")
                        or item.get("updated_at")
                        or ""
                    ),
                    str(item.get("id") or ""),
                )
            )
            if cursor_after is not None:
                after_timestamp = str(cursor_after.get("timestamp") or "")
                after_id = str(cursor_after.get("id") or "")
                items = [
                    item
                    for item in items
                    if (
                        str(
                            item.get("created_at")
                            or item.get("discovered_at")
                            or item.get("updated_at")
                            or ""
                        ),
                        str(item.get("id") or ""),
                    )
                    > (after_timestamp, after_id)
                ]
            counts = {
                "duplicate": session.scalar(
                    select(func.count())
                    .select_from(DuplicateRelationship)
                    .where(DuplicateRelationship.resolution == "pending")
                ),
                "import": session.scalar(
                    select(func.count())
                    .select_from(ImportReview)
                    .where(ImportReview.status == "pending")
                ),
                "company_candidate": session.scalar(
                    select(func.count())
                    .select_from(CompanyCandidate)
                    .where(CompanyCandidate.decision == "pending")
                ),
                "suggestion": session.scalar(
                    select(func.count())
                    .select_from(ExternalSuggestion)
                    .where(ExternalSuggestion.approval_state == ApprovalState.PENDING)
                ),
            }
            total_count = sum(int(value or 0) for value in counts.values())
            page_items = items[offset : offset + limit]
            has_more = (
                len(items) > offset + limit
                if cursor_after is not None
                else offset + len(page_items) < total_count
            )
            return enforce_response_budget(
                {
                    "items": page_items,
                    "has_more": has_more,
                    "next_cursor": (
                        _cursor_token(
                            "reviews",
                            identity,
                            offset + len(page_items),
                            after=(
                                {
                                    "id": page_items[-1]["id"],
                                    "timestamp": str(
                                        page_items[-1].get("created_at")
                                        or page_items[-1].get("discovered_at")
                                        or page_items[-1].get("updated_at")
                                        or ""
                                    ),
                                }
                                if page_items
                                else None
                            ),
                        )
                        if has_more
                        else None
                    ),
                    "omission_counts": {
                        key: max(
                            0,
                            int(value or 0)
                            - sum(
                                1
                                for item in page_items
                                if item.get("review_type") == key
                            ),
                        )
                        for key, value in counts.items()
                    },
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def list_documents(
        self,
        *,
        status: DocumentStatus | str | None = None,
        limit: int = MAX_PAGE,
        full_content: bool = False,
    ) -> list[dict[str, Any]]:
        return self.list_documents_page(
            status=status, limit=limit, full_content=full_content
        )["items"]

    def get_document(
        self, document_id: str, *, full_content: bool = False
    ) -> dict[str, Any]:
        """Retrieve one document by its indexed primary key."""

        with self.database.session() as session:
            row = session.get(DocumentVersion, document_id)
            if row is None:
                raise LookupError("document not found")
            return enforce_response_budget(
                self._document_dto(row, full_content=full_content),
                budget=MAX_RESPONSE_BYTES,
            )

    @staticmethod
    def _document_dto(row: DocumentVersion, *, full_content: bool) -> dict[str, Any]:
        payload = model_dto(
            row,
            (
                "id",
                "kind",
                "name",
                "version",
                "parent_id",
                "job_id",
                "status",
                "approval_state",
                "is_canonical",
                "content_hash",
                "provenance",
                "diff_data",
                "validation",
                "created_at",
                "updated_at",
            ),
            limits={"name": 500},
        )
        content = _truncate(row.content_markdown, full=full_content)
        payload["content_markdown"] = content
        payload["content_truncated"] = content != row.content_markdown
        if content != row.content_markdown:
            payload["content_omitted_chars"] = max(
                0, len(row.content_markdown) - len(content or "")
            )
        return payload

    def list_documents_page(
        self,
        *,
        status: DocumentStatus | str | None = None,
        limit: int = MAX_PAGE,
        full_content: bool = False,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        limit = _bounded_limit(limit, label="document")
        identity = _hash_payload(
            {
                "status": str(status) if status is not None else None,
                "full_content": full_content,
            }
        )
        cursor_offset = _cursor_offset(cursor, kind="documents", identity=identity)
        cursor_after = _cursor_after(cursor, kind="documents", identity=identity)
        offset = 0 if cursor_after is not None else (cursor_offset or 0)
        with self.database.session() as session:
            statement = select(DocumentVersion)
            if status is not None:
                statement = statement.where(
                    DocumentVersion.status == DocumentStatus(status)
                )
            total = int(
                session.scalar(select(func.count()).select_from(statement.subquery()))
                or 0
            )
            if cursor_after is not None:
                last_updated = datetime.fromisoformat(str(cursor_after["updated_at"]))
                statement = statement.where(
                    (DocumentVersion.updated_at < last_updated)
                    | (
                        (DocumentVersion.updated_at == last_updated)
                        & (DocumentVersion.id > str(cursor_after["id"]))
                    )
                )
            result: list[dict[str, Any]] = []
            for row in session.scalars(
                statement.order_by(
                    DocumentVersion.updated_at.desc(), DocumentVersion.id
                )
                .offset(offset)
                .limit(limit + (1 if cursor_after is not None else 0))
            ):
                result.append(self._document_dto(row, full_content=full_content))
            has_extra_keyset_item = cursor_after is not None and len(result) > limit
            if has_extra_keyset_item:
                result = result[:limit]
            has_more = (
                has_extra_keyset_item
                if cursor_after is not None
                else offset + len(result) < total
            )
            return enforce_response_budget(
                {
                    "items": result,
                    "has_more": has_more,
                    "next_cursor": (
                        _cursor_token(
                            "documents",
                            identity,
                            offset + len(result),
                            after=(
                                {
                                    "id": result[-1]["id"],
                                    "updated_at": _cursor_datetime(
                                        result[-1]["updated_at"]
                                    ),
                                }
                                if result
                                else None
                            ),
                        )
                        if has_more
                        else None
                    ),
                    "omitted_count": max(0, total - offset - len(result)),
                },
                budget=MAX_RESPONSE_BYTES,
            )

    def get_analytics(self) -> dict[str, Any]:
        with self.database.session() as session:
            return enforce_response_budget(
                _json(analytics_report(session)), budget=MAX_RESPONSE_BYTES
            )

    def list_questions(
        self, *, role_focus: str | None = None, limit: int = MAX_PAGE
    ) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, label="question")
        with self.database.session() as session:
            statement = select(InterviewQuestion).where(
                InterviewQuestion.active.is_(True)
            )
            if role_focus:
                statement = statement.where(InterviewQuestion.role_focus == role_focus)
            return enforce_response_budget(
                [
                    model_dto(
                        row,
                        (
                            "id",
                            "prompt",
                            "role_focus",
                            "tags",
                            "skills",
                            "evidence_keys",
                            "active",
                            "created_at",
                            "updated_at",
                        ),
                        limits={"prompt": 5_000, "role_focus": 300},
                    )
                    for row in session.scalars(
                        statement.order_by(InterviewQuestion.updated_at.desc()).limit(
                            min(limit, MAX_PAGE)
                        )
                    )
                ],
                budget=MAX_RESPONSE_BYTES,
            )

    def list_interviews(
        self, *, application_id: str | None = None, limit: int = MAX_PAGE
    ) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, label="interview")
        with self.database.session() as session:
            statement = select(Interview)
            if application_id:
                statement = statement.where(Interview.application_id == application_id)
            interviews = [
                {
                    "record_type": "interview",
                    **model_dto(
                        row,
                        (
                            "id",
                            "application_id",
                            "starts_at",
                            "ends_at",
                            "interview_type",
                            "location_or_link",
                            "contact_id",
                            "notes",
                            "calendar_event_id",
                            "created_at",
                            "updated_at",
                        ),
                        limits={
                            "location_or_link": 2_000,
                            "notes": DEFAULT_DESCRIPTION,
                        },
                    ),
                }
                for row in session.scalars(
                    statement.order_by(Interview.starts_at).limit(limit)
                )
            ]
            session_statement = select(InterviewSession)
            if application_id:
                session_statement = session_statement.where(
                    InterviewSession.application_id == application_id
                )
            sessions = [
                {
                    "record_type": "session",
                    **model_dto(
                        row,
                        (
                            "id",
                            "application_id",
                            "interview_id",
                            "session_type",
                            "role_focus",
                            "started_at",
                            "notes",
                            "retrospective",
                            "outcome",
                            "follow_up_task_id",
                            "created_at",
                            "updated_at",
                        ),
                        limits={
                            "notes": DEFAULT_DESCRIPTION,
                            "retrospective": DEFAULT_DESCRIPTION,
                        },
                    ),
                }
                for row in session.scalars(
                    session_statement.order_by(InterviewSession.started_at).limit(limit)
                )
            ]
            combined = interviews + sessions
            combined.sort(
                key=lambda item: str(
                    item.get("starts_at") or item.get("started_at") or ""
                )
            )
            return enforce_response_budget(combined[:limit], budget=MAX_RESPONSE_BYTES)

    def create_interview(
        self,
        *,
        application_id: str,
        starts_at: datetime,
        ends_at: datetime | None = None,
        interview_type: str | None = None,
        location_or_link: str | None = None,
        contact_id: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(starts_at, datetime):
            raise ValueError("starts_at must be an ISO datetime")
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="interview.create",
                payload={
                    "application_id": application_id,
                    "starts_at": starts_at,
                    "ends_at": ends_at,
                    "interview_type": interview_type,
                    "location_or_link": location_or_link,
                    "contact_id": contact_id,
                    "notes": notes,
                },
            )
            row = create_interview(
                session,
                application_id,
                starts_at,
                ends_at=ends_at,
                interview_type=interview_type,
                location_or_link=location_or_link,
                contact_id=contact_id,
                notes=notes,
                actor=self.actor,
            )
            session.flush()
            return self._result(
                {
                    "interview_id": row.id,
                    "application_id": row.application_id,
                    "starts_at": row.starts_at,
                },
                session=session,
                entity_id=row.id,
                action="interview.created",
            )

    def list_offers(
        self, *, application_id: str | None = None, limit: int = MAX_PAGE
    ) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, label="offer")
        with self.database.session() as session:
            statement = select(Offer)
            if application_id:
                statement = statement.where(Offer.application_id == application_id)
            return enforce_response_budget(
                [
                    model_dto(
                        row,
                        (
                            "id",
                            "application_id",
                            "base_salary",
                            "annual_bonus",
                            "annualized_equity",
                            "currency",
                            "cost_of_living_index",
                            "stress_score",
                            "terms",
                            "decision",
                            "offered_at",
                            "created_at",
                            "updated_at",
                        ),
                    )
                    for row in session.scalars(
                        statement.order_by(Offer.offered_at).limit(limit)
                    )
                ],
                budget=MAX_RESPONSE_BYTES,
            )

    # Controlled actions ---------------------------------------------------

    def _run_scan_sync(
        self, *, source: str = "all", query: str | None = None
    ) -> dict[str, Any]:
        run = run_discovery_scan(
            self.database,
            self.config,
            secrets=self.secrets,
            source_selector=source,
            query=query,
            allow_paid_web=source.casefold() == "web",
        )
        with self.database.session() as session:
            return self._result(
                {
                    "scan_run_id": run.id,
                    "status": run.status,
                    "observations": run.discovered_count,
                    "sources": run.source_results,
                    "error": run.error_summary,
                },
                session=session,
                entity_id=run.id,
                action="scan.completed",
            )

    def _run_agent_sync(
        self, *, include_web: bool | None = None, mode: str | None = None
    ) -> dict[str, Any]:
        from .agent import DailyAgent

        run = DailyAgent(self.database, self.config, secrets=self.secrets).run(
            include_web=include_web, mode=mode
        )
        with self.database.session() as session:
            return self._result(
                {"run_id": run.id, "status": run.status, "summary": run.summary},
                session=session,
                entity_id=run.id,
            )

    def _execute_operation(
        self, operation_id: str, kind: str, request: Mapping[str, Any]
    ) -> None:
        with self.database.session() as session:
            operation = session.get(OperationRun, operation_id)
            if operation is None:
                return
            operation.status = "running"
            operation.started_at = utc_now()
        try:
            if kind == "scan":
                result = self._run_scan_sync(
                    source=str(request.get("source", "all")),
                    query=request.get("query"),
                )
            elif kind == "agent":
                result = self._run_agent_sync(
                    include_web=request.get("include_web"),
                    mode=request.get("mode"),
                )
            else:  # pragma: no cover - guarded by _start_operation
                raise ValueError(f"unsupported operation kind: {kind}")
            status = str(result.get("status", "succeeded"))
            with self.database.session() as session:
                operation = session.get(OperationRun, operation_id)
                if operation is not None:
                    operation.status = "partial" if status == "partial" else "succeeded"
                    operation.result_json = _json(result)
                    operation.finished_at = utc_now()
        except Exception as exc:
            with self.database.session() as session:
                operation = session.get(OperationRun, operation_id)
                if operation is not None:
                    operation.status = "failed"
                    operation.error = sanitize_error_message(exc)
                    operation.finished_at = utc_now()

    def _start_operation(self, kind: str, request: Mapping[str, Any]) -> dict[str, Any]:
        if kind not in {"scan", "agent"}:
            raise ValueError("unsupported operation kind")
        request_json = _json(request)
        with self.database.session() as session:
            operation = OperationRun(
                kind=kind, status="queued", request_json=request_json
            )
            session.add(operation)
            session.flush()
            event = record_audit(
                session,
                action="operation.queued",
                entity_type="operation_run",
                entity_id=operation.id,
                actor=self.actor,
                after={"kind": kind, "request": request_json},
            )
            operation_id = operation.id
        self._operation_executor.submit(
            self._execute_operation, operation_id, kind, request_json
        )
        return {
            "operation_id": operation_id,
            "kind": kind,
            "status": "queued",
            "audit_id": event.id,
        }

    def run_scan(
        self,
        *,
        source: str = "all",
        query: str | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        if background:
            return self._start_operation("scan", {"source": source, "query": query})
        return self._run_scan_sync(source=source, query=query)

    def run_agent(
        self,
        *,
        include_web: bool | None = None,
        mode: str | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        if background:
            return self._start_operation(
                "agent", {"include_web": include_web, "mode": mode}
            )
        return self._run_agent_sync(include_web=include_web, mode=mode)

    def get_operation_status(self, operation_id: str) -> dict[str, Any]:
        with self.database.session() as session:
            row = session.get(OperationRun, operation_id)
            if row is None:
                raise LookupError("operation not found")
            return enforce_response_budget(
                model_dto(
                    row,
                    (
                        "id",
                        "kind",
                        "status",
                        "request_json",
                        "result_json",
                        "error",
                        "started_at",
                        "finished_at",
                        "created_at",
                        "updated_at",
                    ),
                    limits={"error": 2_000},
                ),
                budget=MAX_RESPONSE_BYTES,
            )

    def preview_capture(
        self, request: CaptureInput | Mapping[str, Any]
    ) -> dict[str, Any]:
        request = (
            request
            if isinstance(request, CaptureInput)
            else CaptureInput.model_validate(request)
        )
        preview = preview_capture(self.database, **request.model_dump())
        return enforce_response_budget(
            _json(preview.model_dump(mode="json")), budget=MAX_RESPONSE_BYTES
        )

    def save_captured_job(
        self, preview: CapturePreview | Mapping[str, Any]
    ) -> dict[str, Any]:
        parsed = (
            preview
            if isinstance(preview, CapturePreview)
            else CapturePreview.model_validate(preview)
        )
        job = save_capture(self.database, parsed)
        with self.database.session() as session:
            return self._result(
                {
                    "job_id": job.id,
                    "state": "saved",
                    "provenance": {
                        "source": job.source_primary,
                        "source_id": job.source_id,
                        "url": job.launch_url or job.canonical_url,
                    },
                },
                session=session,
                entity_id=job.id,
                action="job.capture_saved",
            )

    def create_application(
        self,
        job_id: str,
        *,
        submission_channel: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="application.create",
                payload={
                    "job_id": job_id,
                    "submission_channel": submission_channel,
                    "notes": notes,
                },
            )
            row = create_application(
                session,
                job_id,
                submission_channel=submission_channel,
                notes=notes,
                actor=self.actor,
            )
            session.flush()
            return self._result(
                {
                    "application_id": row.id,
                    "stage": row.current_stage,
                    "job_id": row.job_id,
                },
                session=session,
                entity_id=row.id,
                action="application.created",
            )

    def transition_application(
        self,
        application_id: str,
        to_stage: ApplicationStage | str,
        *,
        reason: str | None = None,
        expected_stage: ApplicationStage | str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="application.transition",
                payload={
                    "application_id": application_id,
                    "to_stage": str(to_stage),
                    "reason": reason,
                    "expected_stage": str(expected_stage)
                    if expected_stage is not None
                    else None,
                },
            )
            application = session.get(Application, application_id)
            if application is None:
                raise LookupError("application not found")
            if expected_stage is not None and ApplicationStage(
                application.current_stage
            ) != ApplicationStage(expected_stage):
                raise ValueError(
                    "application stage has changed; refresh before transitioning"
                )
            event = transition_application(
                session, application, to_stage, reason=reason, actor=self.actor
            )
            session.flush()
            return self._result(
                {
                    "application_id": application.id,
                    "stage": application.current_stage,
                    "event_id": event.id,
                },
                session=session,
                entity_id=application.id,
                action="application.stage_changed",
            )

    def create_task(
        self,
        *,
        title: str,
        description: str | None = None,
        due_at: datetime | None = None,
        job_id: str | None = None,
        application_id: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="task.create",
                payload={
                    "title": title,
                    "description": description,
                    "due_at": due_at,
                    "job_id": job_id,
                    "application_id": application_id,
                },
            )
            row = create_task(
                session,
                title=title,
                description=description,
                due_at=due_at,
                job_id=job_id,
                application=application_id,
                actor=self.actor,
            )
            session.flush()
            return self._result(
                {"task_id": row.id, "status": row.status},
                session=session,
                entity_id=row.id,
                action="task.created",
            )

    def log_activity(
        self,
        *,
        action: str,
        entity_type: str,
        entity_id: str | None = None,
        detail: str | None = None,
    ) -> dict[str, Any]:
        for field, value, limit in (
            ("action", action, 200),
            ("entity_type", entity_type, 100),
            ("entity_id", entity_id, 100),
            ("detail", detail, 20_000),
        ):
            if value is None and field in {"entity_id", "detail"}:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a non-empty string")
            if len(value) > limit or any(ord(char) < 32 for char in value):
                raise ValueError(f"{field} exceeds its safety limit")
        with self.database.session() as session:
            event = record_audit(
                session,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                actor=self.actor,
                detail=detail,
            )
            session.flush()
            return enforce_response_budget(
                {"audit_id": event.id, "state": "recorded"},
                budget=MAX_RESPONSE_BYTES,
            )

    def create_document_draft(
        self,
        *,
        base_version_id: str,
        content_markdown: str,
        job_id: str | None = None,
        provenance: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if len(content_markdown.encode("utf-8")) > MAX_TEXT:
            raise ValueError("document content exceeds the safety limit")
        service = DocumentService(self.database, paths=self.paths)
        row = service.propose(
            base_version_id=base_version_id,
            content_markdown=content_markdown,
            job_id=job_id,
            provenance=provenance or [],
            actor=self.actor,
        )
        with self.database.session() as session:
            return self._result(
                {
                    "document_id": row.id,
                    "status": row.status,
                    "approval_state": row.approval_state,
                    "content_hash": row.content_hash,
                    "version": row.version,
                },
                session=session,
                entity_id=row.id,
                action="document.proposed",
            )

    def approve_document(
        self, document_id: str, *, expected_hash: str, edited_content: str | None = None
    ) -> dict[str, Any]:
        row = DocumentService(self.database, paths=self.paths).approve(
            document_id,
            edited_content=edited_content,
            expected_hash=expected_hash,
            actor=self.actor,
        )
        with self.database.session() as session:
            return self._result(
                {
                    "document_id": row.id,
                    "status": row.status,
                    "approval_state": row.approval_state,
                    "content_hash": row.content_hash,
                },
                session=session,
                entity_id=row.id,
                action="document.approved",
            )

    def reject_document(
        self, document_id: str, *, expected_hash: str
    ) -> dict[str, Any]:
        row = DocumentService(self.database, paths=self.paths).reject(
            document_id, expected_hash=expected_hash, actor=self.actor
        )
        with self.database.session() as session:
            return self._result(
                {
                    "document_id": row.id,
                    "status": row.status,
                    "approval_state": row.approval_state,
                },
                session=session,
                entity_id=row.id,
                action="document.rejected",
            )

    def approve_review(
        self,
        review_id: str,
        *,
        review_type: str = "duplicate",
        canonical_job_id: str | None = None,
        expected_hash: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            if review_type == "duplicate":
                current = session.get(DuplicateRelationship, review_id)
                if current is None:
                    raise LookupError("review not found")
                self._assert_review_hash(review_type, current, expected_hash)
                row = confirm_duplicate(
                    session, review_id, canonical_job_id=canonical_job_id
                )
                event = record_audit(
                    session,
                    action="review.approved",
                    entity_type="duplicate_relationship",
                    entity_id=review_id,
                    actor=self.actor,
                    after={"group_id": row.id},
                )
                session.flush()
                return {
                    "review_id": review_id,
                    "state": "approved",
                    "canonical_group_id": row.id,
                    "audit_id": event.id,
                }
            if review_type == "import":
                row = session.get(ImportReview, review_id)
                if row is None:
                    raise LookupError("review not found")
                self._assert_review_hash(review_type, row, expected_hash)
                row.status = ImportReviewStatus.RESOLVED
                event = record_audit(
                    session,
                    action="review.approved",
                    entity_type="import_review",
                    entity_id=review_id,
                    actor=self.actor,
                )
                session.flush()
                return {
                    "review_id": review_id,
                    "state": "approved",
                    "audit_id": event.id,
                }
            if review_type == "company_candidate":
                candidate = session.get(CompanyCandidate, review_id)
                if candidate is None:
                    raise LookupError("company candidate not found")
                self._assert_review_hash(review_type, candidate, expected_hash)
                if candidate.decision != "pending":
                    raise ValueError("company candidate has already been reviewed")
                company = session.scalar(
                    select(Company).where(
                        Company.normalized_name == candidate.name.casefold()
                    )
                )
                if company is None:
                    company = Company(
                        name=candidate.name,
                        normalized_name=candidate.name.casefold(),
                        website=candidate.website,
                    )
                    session.add(company)
                    session.flush()
                candidate.company_id = company.id
                candidate.decision = "approved"
                event = record_audit(
                    session,
                    action="review.approved",
                    entity_type="company_candidate",
                    entity_id=review_id,
                    actor=self.actor,
                    after={"company_id": company.id},
                )
                session.flush()
                return {
                    "review_id": review_id,
                    "state": "approved",
                    "company_id": company.id,
                    "audit_id": event.id,
                }
            if review_type == "suggestion":
                suggestion = session.get(ExternalSuggestion, review_id)
                if suggestion is None:
                    raise LookupError("suggestion not found")
                self._assert_review_hash(review_type, suggestion, expected_hash)
                result = apply_external_suggestion(
                    session, review_id, approved=True, actor=self.actor
                )
                event = record_audit(
                    session,
                    action="review.approved",
                    entity_type="external_suggestion",
                    entity_id=review_id,
                    actor=self.actor,
                )
                session.flush()
                return {
                    "review_id": review_id,
                    "state": "approved",
                    "effect": _json(result),
                    "audit_id": event.id,
                }
            raise ValueError(
                "review_type must be duplicate, import, company_candidate, or suggestion"
            )

    def dismiss_review(
        self,
        review_id: str,
        *,
        review_type: str = "duplicate",
        reason: str | None = None,
        expected_hash: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            if review_type == "duplicate":
                current = session.get(DuplicateRelationship, review_id)
                if current is None:
                    raise LookupError("review not found")
                self._assert_review_hash(review_type, current, expected_hash)
                row = dismiss_duplicate(session, review_id)
                entity_type = "duplicate_relationship"
            elif review_type == "import":
                row = session.get(ImportReview, review_id)
                if row is None:
                    raise LookupError("review not found")
                self._assert_review_hash(review_type, row, expected_hash)
                row.status = ImportReviewStatus.DISMISSED
                entity_type = "import_review"
            elif review_type == "company_candidate":
                row = session.get(CompanyCandidate, review_id)
                if row is None:
                    raise LookupError("company candidate not found")
                self._assert_review_hash(review_type, row, expected_hash)
                if row.decision != "pending":
                    raise ValueError("company candidate has already been reviewed")
                row.decision = "dismissed"
                entity_type = "company_candidate"
            elif review_type == "suggestion":
                current = session.get(ExternalSuggestion, review_id)
                if current is None:
                    raise LookupError("suggestion not found")
                self._assert_review_hash(review_type, current, expected_hash)
                apply_external_suggestion(
                    session, review_id, approved=False, actor=self.actor
                )
                row = session.get(ExternalSuggestion, review_id)
                entity_type = "external_suggestion"
            else:
                raise ValueError(
                    "review_type must be duplicate, import, company_candidate, or suggestion"
                )
            event = record_audit(
                session,
                action="review.dismissed",
                entity_type=entity_type,
                entity_id=review_id,
                actor=self.actor,
                detail=reason,
            )
            session.flush()
            return {"review_id": review_id, "state": "dismissed", "audit_id": event.id}

    @staticmethod
    def _assert_review_hash(
        review_type: str, row: object, expected_hash: str | None
    ) -> None:
        if expected_hash is None:
            return
        actual = _review_hash(review_type, row)
        if actual != expected_hash:
            raise ValueError("review is stale; refresh before applying this mutation")

    def create_interview_question(
        self,
        *,
        prompt: str,
        role_focus: str | None = None,
        tags: Iterable[str] = (),
        skills: Iterable[str] = (),
        evidence_keys: Iterable[str] = (),
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        if not prompt.strip() or len(prompt) > 10_000:
            raise ValueError(
                "question prompt must be non-blank and at most 10,000 characters"
            )
        tag_values = list(tags)
        skill_values = list(skills)
        evidence_values = list(evidence_keys)
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="interview_question.create",
                payload={
                    "prompt": prompt,
                    "role_focus": role_focus,
                    "tags": tag_values,
                    "skills": skill_values,
                    "evidence_keys": evidence_values,
                },
            )
            row = InterviewQuestion(
                prompt=prompt.strip(),
                role_focus=role_focus,
                tags=_clean_list(tag_values),
                skills=_clean_list(skill_values),
                evidence_keys=_clean_list(evidence_values),
            )
            session.add(row)
            session.flush()
            event = record_audit(
                session,
                action="interview_question.created",
                entity_type="interview_question",
                entity_id=row.id,
                actor=self.actor,
            )
            session.flush()
            return {"question_id": row.id, "state": "active", "audit_id": event.id}

    def create_interview_session(
        self,
        *,
        application_id: str,
        session_type: str = "preparation",
        interview_id: str | None = None,
        role_focus: str | None = None,
        notes: str | None = None,
        answers: Iterable[Mapping[str, Any]] = (),
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        answer_values = list(answers)[:MAX_PAGE]
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="interview_session.create",
                payload={
                    "application_id": application_id,
                    "session_type": session_type,
                    "interview_id": interview_id,
                    "role_focus": role_focus,
                    "notes": notes,
                    "answers": answer_values,
                },
            )
            if session.get(Application, application_id) is None:
                raise LookupError("application not found")
            row = InterviewSession(
                application_id=application_id,
                interview_id=interview_id,
                session_type=session_type.strip()[:80] or "preparation",
                role_focus=role_focus,
                started_at=utc_now(),
                notes=notes,
            )
            session.add(row)
            session.flush()
            answer_ids = []
            for answer in answer_values:
                text = str(answer.get("answer", "")).strip()
                if not text:
                    raise ValueError("interview answers must contain text")
                rating = answer.get("rating")
                if rating is not None and (
                    isinstance(rating, bool)
                    or not isinstance(rating, int)
                    or not 1 <= rating <= 5
                ):
                    raise ValueError(
                        "interview answer rating must be an integer from 1 to 5"
                    )
                question_id = answer.get("question_id")
                if (
                    question_id is not None
                    and session.get(InterviewQuestion, question_id) is None
                ):
                    raise LookupError("interview question not found")
                item = InterviewAnswer(
                    session_id=row.id,
                    question_id=question_id,
                    answer=text[:MAX_TEXT],
                    evidence_refs=_clean_list(answer.get("evidence_refs", ())),
                    tags=_clean_list(answer.get("tags", ())),
                    rating=rating,
                )
                session.add(item)
                session.flush()
                answer_ids.append(item.id)
            event = record_audit(
                session,
                action="interview_session.created",
                entity_type="interview_session",
                entity_id=row.id,
                actor=self.actor,
                after={"answer_ids": answer_ids},
            )
            session.flush()
            return {
                "session_id": row.id,
                "answer_ids": answer_ids,
                "audit_id": event.id,
            }

    def record_interview_review(
        self,
        session_id: str,
        *,
        retrospective: str,
        outcome: str | None = None,
        follow_up_task: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.database.session() as session:
            row = session.get(InterviewSession, session_id)
            if row is None:
                raise LookupError("interview session not found")
            row.retrospective = retrospective[:MAX_TEXT]
            row.outcome = outcome.strip()[:100] if outcome else None
            task_id = None
            if follow_up_task:
                due_at = follow_up_task.get("due_at")
                if isinstance(due_at, str):
                    due_at = datetime.fromisoformat(due_at.replace("Z", "+00:00"))
                task = create_task(
                    session,
                    title=str(follow_up_task.get("title", "Interview follow-up")),
                    description=follow_up_task.get("description"),
                    due_at=due_at,
                    application=row.application_id,
                    actor=self.actor,
                )
                row.follow_up_task_id = task.id
                task_id = task.id
            event = record_audit(
                session,
                action="interview_session.reviewed",
                entity_type="interview_session",
                entity_id=row.id,
                actor=self.actor,
                after={"outcome": row.outcome, "follow_up_task_id": task_id},
            )
            session.flush()
            return {
                "session_id": row.id,
                "outcome": row.outcome,
                "follow_up_task_id": task_id,
                "audit_id": event.id,
            }

    def compare_offers(
        self, *, application_id: str | None = None
    ) -> list[dict[str, Any]]:
        with self.database.session() as session:
            return enforce_response_budget(
                _json(compare_persisted_offers(session, application_id=application_id)),
                budget=MAX_RESPONSE_BYTES,
            )

    def create_offer(
        self, application_id: str, *, approval_id: str | None = None, **values: Any
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="offer.create",
                payload={"application_id": application_id, **values},
            )
            row = create_offer(session, application_id, actor=self.actor, **values)
            session.flush()
            return self._result(
                {
                    "offer_id": row.id,
                    "application_id": row.application_id,
                    "decision": row.decision,
                },
                session=session,
                entity_id=row.id,
                action="offer.created",
            )

    def set_offer_decision(
        self, offer_id: str, decision: str | None, *, approval_id: str | None = None
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="offer.decision",
                payload={"offer_id": offer_id, "decision": decision},
            )
            row = set_offer_decision(session, offer_id, decision, actor=self.actor)
            session.flush()
            return self._result(
                {"offer_id": row.id, "decision": row.decision},
                session=session,
                entity_id=row.id,
                action="offer.decision_changed",
            )

    def discover_companies(
        self,
        candidates: Iterable[Mapping[str, Any]] | None = None,
        *,
        role: str | None = None,
        location: str | None = None,
        industry: str | None = None,
    ) -> dict[str, Any]:
        source_candidates = list(candidates or [])[:MAX_PAGE]
        if not source_candidates:
            for provider, boards in (
                ("greenhouse", self.config.sources.greenhouse),
                ("lever", self.config.sources.lever),
                ("workday", self.config.sources.workday),
                ("eightfold", self.config.sources.eightfold),
                ("oracle_hcm", self.config.sources.oracle_hcm),
                ("rippling", self.config.sources.rippling),
                ("paylocity", self.config.sources.paylocity),
                ("freehire", self.config.sources.freehire),
            ):
                if provider == "freehire":
                    boards = {
                        key: value
                        for key, value in boards.items()
                        if isinstance(getattr(value, "credential_name", None), str)
                        and self.secrets.get(getattr(value, "credential_name"))
                    }
                for key, value in list(boards.items())[:MAX_PAGE]:
                    name = (
                        value if isinstance(value, str) else getattr(value, "name", key)
                    )
                    source_candidates.append(
                        {
                            "name": name or key,
                            "source": provider,
                            "evidence": [{"board": key}],
                        }
                    )
        created: list[str] = []
        audit_ids: list[str] = []
        filtered = 0
        with self.database.session() as session:
            for candidate in source_candidates:
                if not _matches_candidate_filter(
                    candidate, role, ("role", "roles", "title", "job_title")
                ):
                    filtered += 1
                    continue
                if not _matches_candidate_filter(
                    candidate, location, ("location", "locations", "city", "region")
                ):
                    filtered += 1
                    continue
                if not _matches_candidate_filter(
                    candidate, industry, ("industry", "industries", "sector")
                ):
                    filtered += 1
                    continue
                name = " ".join(str(candidate.get("name", "")).split())
                if not name:
                    continue
                existing = session.scalar(
                    select(CompanyCandidate).where(
                        func.lower(CompanyCandidate.name) == name.casefold(),
                        CompanyCandidate.decision == "pending",
                    )
                )
                if existing:
                    created.append(existing.id)
                    continue
                row = CompanyCandidate(
                    name=name[:300],
                    website=str(candidate.get("website"))[:2_000]
                    if candidate.get("website")
                    else None,
                    source=str(candidate.get("source", "configured_ats"))[:100],
                    role_filter=role,
                    location_filter=location,
                    industry_filter=industry,
                    evidence=list(candidate.get("evidence", ()))[:50],
                    score=float(candidate["score"])
                    if candidate.get("score") is not None
                    else None,
                )
                session.add(row)
                session.flush()
                event = record_audit(
                    session,
                    action="company_candidate.discovered",
                    entity_type="company_candidate",
                    entity_id=row.id,
                    actor=self.actor,
                )
                created.append(row.id)
                audit_ids.append(event.id)
            return {
                "candidate_ids": created,
                "count": len(created),
                "filtered_count": filtered,
                "audit_ids": audit_ids,
                "state": "pending_review",
            }

    def watch_company(
        self,
        company_id: str,
        *,
        criteria: Mapping[str, Any] | None = None,
        cadence_days: int = 7,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= cadence_days <= 365:
            raise ValueError("cadence_days must be between 1 and 365")
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="company.watch",
                payload={
                    "company_id": company_id,
                    "criteria": dict(criteria or {}),
                    "cadence_days": cadence_days,
                },
            )
            if session.get(Company, company_id) is None:
                raise LookupError("company not found")
            row = session.scalar(
                select(CompanyWatchlistEntry).where(
                    CompanyWatchlistEntry.company_id == company_id
                )
            )
            if row is None:
                row = CompanyWatchlistEntry(
                    company_id=company_id,
                    criteria=dict(criteria or {}),
                    cadence_days=cadence_days,
                    enabled=True,
                )
                session.add(row)
                session.flush()
                action = "company.watchlisted"
            else:
                row.criteria = dict(criteria or row.criteria or {})
                row.cadence_days = cadence_days
                row.enabled = True
                action = "company.watchlist_updated"
            event = record_audit(
                session,
                action=action,
                entity_type="company_watchlist",
                entity_id=row.id,
                actor=self.actor,
                after={"company_id": company_id, "criteria": row.criteria},
            )
            session.flush()
            return {
                "watchlist_id": row.id,
                "company_id": company_id,
                "enabled": row.enabled,
                "audit_id": event.id,
            }

    def unwatch_company(
        self, company_id: str, *, approval_id: str | None = None
    ) -> dict[str, Any]:
        with self.database.session() as session:
            self._consume_approval(
                session,
                approval_id=approval_id,
                action="company.unwatch",
                payload={"company_id": company_id},
            )
            row = session.scalar(
                select(CompanyWatchlistEntry).where(
                    CompanyWatchlistEntry.company_id == company_id
                )
            )
            if row is None:
                raise LookupError("company is not on the watchlist")
            row.enabled = False
            event = record_audit(
                session,
                action="company.unwatchlisted",
                entity_type="company_watchlist",
                entity_id=row.id,
                actor=self.actor,
                after={"company_id": company_id, "enabled": False},
            )
            session.flush()
            return {
                "watchlist_id": row.id,
                "company_id": company_id,
                "enabled": False,
                "audit_id": event.id,
            }


__all__ = ["ApplicationFacade", "CaptureInput", "FacadeError", "SearchInput"]
