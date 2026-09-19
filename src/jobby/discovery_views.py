"""Named Discovery filters and durable per-view review cursors."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .audit import record_audit
from .job_queries import JobListFilters, JobSort
from .models import DiscoveryReviewCursor, Job, SavedDiscoveryView


class SavedDiscoveryViewRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    filters: JobListFilters
    reviewed_through: datetime | None = None
    reviewed_job_id: str | None = None
    created_at: datetime
    updated_at: datetime


def list_saved_views(session: Session) -> tuple[SavedDiscoveryViewRecord, ...]:
    rows = session.execute(
        select(SavedDiscoveryView, DiscoveryReviewCursor)
        .outerjoin(
            DiscoveryReviewCursor,
            DiscoveryReviewCursor.view_id == SavedDiscoveryView.id,
        )
        .order_by(func.lower(SavedDiscoveryView.name), SavedDiscoveryView.id)
    )
    return tuple(_record(view, cursor) for view, cursor in rows)


def get_saved_view(session: Session, identity: str) -> SavedDiscoveryViewRecord:
    view = _load_view(session, identity)
    cursor = session.scalar(
        select(DiscoveryReviewCursor).where(DiscoveryReviewCursor.view_id == view.id)
    )
    return _record(view, cursor)


def create_saved_view(
    session: Session,
    *,
    name: str,
    filters: JobListFilters,
) -> SavedDiscoveryViewRecord:
    normalized = _name(name)
    if session.scalar(
        select(SavedDiscoveryView.id).where(
            func.lower(SavedDiscoveryView.name) == normalized.casefold()
        )
    ):
        raise ValueError(f"saved Discovery view already exists: {normalized}")
    view = SavedDiscoveryView(
        name=normalized,
        filters_json=_stored_filters(filters),
        sort_key=filters.sort.value,
    )
    session.add(view)
    session.flush()
    record_audit(
        session,
        action="discovery_view.created",
        entity_type="saved_discovery_view",
        entity_id=view.id,
        after={"name": view.name, "filters": view.filters_json, "sort": view.sort_key},
    )
    return _record(view, None)


def update_saved_view(
    session: Session,
    identity: str,
    *,
    name: str | None = None,
    filters: JobListFilters | None = None,
) -> SavedDiscoveryViewRecord:
    if name is None and filters is None:
        raise ValueError("saved Discovery view update has no changes")
    view = _load_view(session, identity)
    before = {"name": view.name, "filters": view.filters_json, "sort": view.sort_key}
    if name is not None:
        normalized = _name(name)
        duplicate = session.scalar(
            select(SavedDiscoveryView.id).where(
                func.lower(SavedDiscoveryView.name) == normalized.casefold(),
                SavedDiscoveryView.id != view.id,
            )
        )
        if duplicate:
            raise ValueError(f"saved Discovery view already exists: {normalized}")
        view.name = normalized
    if filters is not None:
        view.filters_json = _stored_filters(filters)
        view.sort_key = filters.sort.value
    session.flush()
    record_audit(
        session,
        action="discovery_view.updated",
        entity_type="saved_discovery_view",
        entity_id=view.id,
        before=before,
        after={"name": view.name, "filters": view.filters_json, "sort": view.sort_key},
    )
    cursor = session.scalar(
        select(DiscoveryReviewCursor).where(DiscoveryReviewCursor.view_id == view.id)
    )
    return _record(view, cursor)


def delete_saved_view(session: Session, identity: str) -> str:
    view = _load_view(session, identity)
    identifier = view.id
    record_audit(
        session,
        action="discovery_view.deleted",
        entity_type="saved_discovery_view",
        entity_id=view.id,
        before={"name": view.name, "filters": view.filters_json, "sort": view.sort_key},
    )
    session.delete(view)
    session.flush()
    return identifier


def mark_view_reviewed(
    session: Session,
    identity: str,
    *,
    job_id: str | None = None,
    reviewed_through: datetime | None = None,
) -> SavedDiscoveryViewRecord:
    view = _load_view(session, identity)
    if job_id is not None:
        job = session.get(Job, job_id)
        if job is None:
            raise LookupError(f"job not found: {job_id}")
        point = job.discovered_at
    else:
        point = reviewed_through or datetime.now(timezone.utc)
    point = _utc(point)
    # A conditional database update, rather than an ORM read/compare/write,
    # keeps the cursor monotonic even when two sessions commit out of order.
    session.execute(
        sqlite_insert(DiscoveryReviewCursor)
        .values(view_id=view.id)
        .on_conflict_do_nothing(index_elements=[DiscoveryReviewCursor.view_id])
    )
    session.execute(
        update(DiscoveryReviewCursor)
        .where(
            DiscoveryReviewCursor.view_id == view.id,
            or_(
                DiscoveryReviewCursor.reviewed_through.is_(None),
                DiscoveryReviewCursor.reviewed_through < point,
                and_(
                    DiscoveryReviewCursor.reviewed_through == point,
                    func.coalesce(DiscoveryReviewCursor.reviewed_job_id, "")
                    <= (job_id or ""),
                ),
            ),
        )
        .values(reviewed_through=point, reviewed_job_id=job_id)
    )
    cursor = session.scalar(
        select(DiscoveryReviewCursor)
        .where(DiscoveryReviewCursor.view_id == view.id)
        .execution_options(populate_existing=True)
    )
    if cursor is None:  # pragma: no cover - guarded by the insert above
        raise RuntimeError("discovery review cursor was not persisted")
    record_audit(
        session,
        action="discovery_view.reviewed",
        entity_type="saved_discovery_view",
        entity_id=view.id,
        after={
            "reviewed_through": cursor.reviewed_through,
            "reviewed_job_id": cursor.reviewed_job_id,
        },
    )
    return _record(view, cursor)


def filters_for_view(
    session: Session,
    identity: str,
    *,
    new_since_last_review: bool = False,
    limit: int = 200,
    offset: int = 0,
) -> JobListFilters:
    record = get_saved_view(session, identity)
    updates: dict[str, Any] = {"limit": limit, "offset": offset}
    if new_since_last_review and record.reviewed_through is not None:
        updates.update(
            discovered_after=record.reviewed_through,
            discovered_after_job_id=record.reviewed_job_id,
        )
    return record.filters.model_copy(update=updates)


def _stored_filters(filters: JobListFilters) -> dict[str, Any]:
    payload = filters.model_copy(
        update={
            "limit": JobListFilters.model_fields["limit"].default,
            "offset": 0,
            "discovered_after": None,
            "discovered_after_job_id": None,
        }
    ).model_dump(mode="json")
    payload.pop("sort", None)
    payload.pop("limit", None)
    payload.pop("offset", None)
    payload.pop("discovered_after", None)
    payload.pop("discovered_after_job_id", None)
    return payload


def _record(
    view: SavedDiscoveryView, cursor: DiscoveryReviewCursor | None
) -> SavedDiscoveryViewRecord:
    try:
        sort = JobSort(view.sort_key)
        filters = JobListFilters.model_validate({**view.filters_json, "sort": sort})
    except (TypeError, ValueError) as exc:
        raise ValueError(f"saved Discovery view {view.id} is invalid") from exc
    return SavedDiscoveryViewRecord(
        id=view.id,
        name=view.name,
        filters=filters,
        reviewed_through=cursor.reviewed_through if cursor else None,
        reviewed_job_id=cursor.reviewed_job_id if cursor else None,
        created_at=view.created_at,
        updated_at=view.updated_at,
    )


def _load_view(session: Session, identity: str) -> SavedDiscoveryView:
    normalized = _name(identity)
    view = session.get(SavedDiscoveryView, normalized)
    if view is None:
        view = session.scalar(
            select(SavedDiscoveryView).where(
                func.lower(SavedDiscoveryView.name) == normalized.casefold()
            )
        )
    if view is None:
        raise LookupError(f"saved Discovery view not found: {identity}")
    return view


def _name(value: str) -> str:
    normalized = " ".join(value.replace("\x00", "�").split())
    if not normalized:
        raise ValueError("saved Discovery view name must not be blank")
    if len(normalized) > 300:
        raise ValueError("saved Discovery view name must be at most 300 characters")
    return normalized


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("review cursor timestamps must include a timezone")
    return value.astimezone(timezone.utc)


__all__ = [
    "SavedDiscoveryViewRecord",
    "create_saved_view",
    "delete_saved_view",
    "filters_for_view",
    "get_saved_view",
    "list_saved_views",
    "mark_view_reviewed",
    "update_saved_view",
]
