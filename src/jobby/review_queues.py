"""Non-destructive duplicate review queues and the alert inbox.

The functions in this module deliberately accept an existing synchronous
SQLAlchemy session.  They flush changes needed to maintain invariants, but
leave commit and rollback control with the caller.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import re

from sqlalchemy import case, func, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .enums import AlertSeverity
from .models import (
    Alert,
    Application,
    CanonicalJobGroup,
    CanonicalJobMember,
    DuplicateRelationship,
    Job,
    new_id,
    utc_now,
)


COMPARISON_IDENTITY_VERSION = "duplicate-comparison-v1"
ALERT_FINGERPRINT_VERSION = "alert-fingerprint-v1"
_SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}")


class ReviewQueueError(RuntimeError):
    """Raised when a requested review transition would violate queue state."""


@dataclass(frozen=True)
class DuplicateConflictWarning:
    """A workflow conflict that should be shown before group-level actions."""

    code: str
    message: str
    group_id: str
    job_ids: tuple[str, ...]
    application_ids: tuple[str, ...]


def comparison_identity(left: Job, right: Job) -> str:
    """Return an order-independent identity for the material comparison state."""

    if not left.id or not right.id:
        raise ValueError("jobs must be persisted before comparison")
    if left.id == right.id:
        raise ValueError("a job cannot be compared with itself")
    jobs = sorted(
        (_job_comparison_payload(left), _job_comparison_payload(right)),
        key=lambda value: value["id"],
    )
    payload = {
        "version": COMPARISON_IDENTITY_VERSION,
        "jobs": jobs,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def suggest_duplicate(
    session: Session,
    left: Job | str,
    right: Job | str,
    *,
    rule: str,
    similarity: float | None = None,
) -> DuplicateRelationship | None:
    """Create or refresh a suggestion, suppressing unchanged dismissals.

    ``None`` means the pair was previously dismissed and its material
    comparison identity has not changed.  A changed dismissed pair reuses its
    relationship row and returns it to ``pending``.
    """

    left_job, right_job = _load_pair(session, left, right)
    normalized_rule = rule.strip()
    if not normalized_rule:
        raise ValueError("duplicate rule must not be empty")
    if len(normalized_rule) > 80:
        raise ValueError("duplicate rule must not exceed 80 characters")
    if similarity is not None and not 0.0 <= similarity <= 1.0:
        raise ValueError("duplicate similarity must be between zero and one")

    identity = comparison_identity(left_job, right_job)
    relationship = _relationship_for_pair(session, left_job.id, right_job.id)
    if relationship is None:
        first, second = sorted((left_job.id, right_job.id))
        relationship = DuplicateRelationship(
            job_id=first,
            duplicate_job_id=second,
            rule=normalized_rule,
            similarity=similarity,
            confirmed=False,
            resolution="pending",
            comparison_identity=identity,
        )
        session.add(relationship)
        session.flush()
        return relationship

    if relationship.resolution == "confirmed" or relationship.confirmed:
        return relationship
    if (
        relationship.resolution == "dismissed"
        and relationship.comparison_identity == identity
    ):
        return None

    relationship.rule = normalized_rule
    relationship.similarity = similarity
    relationship.comparison_identity = identity
    relationship.confirmed = False
    relationship.resolution = "pending"
    relationship.resolved_at = None
    relationship.canonical_group_id = None
    session.flush()
    return relationship


def dismiss_duplicate(
    session: Session,
    relationship: DuplicateRelationship | str,
    *,
    now: datetime | None = None,
) -> DuplicateRelationship:
    """Dismiss a pending pair without deleting either job or any provenance."""

    row = _load_relationship(session, relationship)
    if row.resolution == "confirmed" or row.confirmed:
        raise ReviewQueueError(
            "confirmed duplicate groups cannot be dismantled by dismissing a pair"
        )
    row.resolution = "dismissed"
    row.confirmed = False
    row.resolved_at = _as_utc(now)
    row.canonical_group_id = None
    session.flush()
    return row


def confirm_duplicate(
    session: Session,
    relationship: DuplicateRelationship | str,
    *,
    canonical_job_id: str | None = None,
    now: datetime | None = None,
) -> CanonicalJobGroup:
    """Confirm a pair and create, extend, or merge its canonical group.

    Group maintenance only changes review metadata.  Jobs and their existing
    applications, tasks, contacts, materials, and source rows are never moved
    or deleted.
    """

    row = _load_relationship(session, relationship)
    left_job, right_job = _load_pair(session, row.job_id, row.duplicate_job_id)
    pair_ids = {left_job.id, right_job.id}
    memberships = list(
        session.scalars(
            select(CanonicalJobMember)
            .where(CanonicalJobMember.job_id.in_(pair_ids))
            .order_by(CanonicalJobMember.group_id, CanonicalJobMember.job_id)
        )
    )
    group_ids = sorted({member.group_id for member in memberships})
    groups = [
        group
        for group_id in group_ids
        if (group := session.get(CanonicalJobGroup, group_id)) is not None
    ]

    group_memberships: list[CanonicalJobMember] = []
    existing_member_ids = set(pair_ids)
    if group_ids:
        group_memberships = list(
            session.scalars(
                select(CanonicalJobMember).where(
                    CanonicalJobMember.group_id.in_(group_ids)
                )
            )
        )
        existing_member_ids.update(member.job_id for member in group_memberships)
    if canonical_job_id is not None and canonical_job_id not in existing_member_ids:
        raise ReviewQueueError(
            "the canonical job must be a member of the resulting group"
        )

    if groups:
        target = _choose_target_group(groups, group_memberships, canonical_job_id)
        _merge_groups(session, target, groups)
    else:
        selected = canonical_job_id or min(pair_ids)
        target = CanonicalJobGroup(canonical_job_id=selected)
        session.add(target)
        session.flush()

    current_members = {
        member.job_id: member
        for member in session.scalars(
            select(CanonicalJobMember).where(CanonicalJobMember.group_id == target.id)
        )
    }
    for job_id in sorted(pair_ids):
        if job_id not in current_members:
            member = CanonicalJobMember(
                group_id=target.id,
                job_id=job_id,
                is_canonical=False,
                hidden_by_default=True,
            )
            session.add(member)
            current_members[job_id] = member
    session.flush()

    selected = canonical_job_id or target.canonical_job_id
    _set_canonical(session, target, selected)
    row.confirmed = True
    row.resolution = "confirmed"
    row.comparison_identity = comparison_identity(left_job, right_job)
    row.resolved_at = _as_utc(now)
    row.canonical_group_id = target.id
    session.flush()
    return target


def promote_canonical(
    session: Session,
    group: CanonicalJobGroup | str,
    job: Job | str,
) -> CanonicalJobGroup:
    """Promote an existing group member for all future workflow records."""

    group_row = _load_group(session, group)
    job_id = job.id if isinstance(job, Job) else job
    if session.get(Job, job_id) is None:
        raise LookupError(f"job not found: {job_id}")
    _set_canonical(session, group_row, job_id)
    session.flush()
    return group_row


def workflow_target_job_id(session: Session, job: Job | str) -> str:
    """Return the canonical target for a new workflow record."""

    job_id = job.id if isinstance(job, Job) else job
    if session.get(Job, job_id) is None:
        raise LookupError(f"job not found: {job_id}")
    member = session.scalar(
        select(CanonicalJobMember).where(CanonicalJobMember.job_id == job_id)
    )
    if member is None:
        return job_id
    group = session.get(CanonicalJobGroup, member.group_id)
    if group is None:
        raise ReviewQueueError("canonical membership refers to a missing group")
    return group.canonical_job_id


def workflow_group_job_ids(session: Session, job: Job | str) -> tuple[str, ...]:
    """Return every job identity sharing workflow context with ``job``."""

    job_id = job.id if isinstance(job, Job) else job
    if session.get(Job, job_id) is None:
        raise LookupError(f"job not found: {job_id}")
    group_id = session.scalar(
        select(CanonicalJobMember.group_id).where(CanonicalJobMember.job_id == job_id)
    )
    if group_id is None:
        return (job_id,)
    job_ids = tuple(
        session.scalars(
            select(CanonicalJobMember.job_id)
            .where(CanonicalJobMember.group_id == group_id)
            .order_by(CanonicalJobMember.job_id)
        )
    )
    if not job_ids:  # pragma: no cover - foreign-key/group invariant guard
        raise ReviewQueueError("canonical group has no members")
    return job_ids


def list_group_members(
    session: Session,
    group: CanonicalJobGroup | str,
    *,
    include_hidden: bool = False,
) -> tuple[CanonicalJobMember, ...]:
    """List group membership, hiding noncanonical members by default."""

    group_id = group.id if isinstance(group, CanonicalJobGroup) else group
    if session.get(CanonicalJobGroup, group_id) is None:
        raise LookupError(f"canonical job group not found: {group_id}")
    statement = select(CanonicalJobMember).where(
        CanonicalJobMember.group_id == group_id
    )
    if not include_hidden:
        statement = statement.where(CanonicalJobMember.hidden_by_default.is_(False))
    statement = statement.order_by(
        CanonicalJobMember.is_canonical.desc(), CanonicalJobMember.job_id
    )
    return tuple(session.scalars(statement))


def list_hidden_group_members(
    session: Session,
    group: CanonicalJobGroup | str,
) -> tuple[CanonicalJobMember, ...]:
    """List only members suppressed from default job and group views."""

    group_id = group.id if isinstance(group, CanonicalJobGroup) else group
    if session.get(CanonicalJobGroup, group_id) is None:
        raise LookupError(f"canonical job group not found: {group_id}")
    return tuple(
        session.scalars(
            select(CanonicalJobMember)
            .where(
                CanonicalJobMember.group_id == group_id,
                CanonicalJobMember.hidden_by_default.is_(True),
            )
            .order_by(CanonicalJobMember.job_id)
        )
    )


def list_review_jobs(
    session: Session,
    *,
    include_hidden: bool = False,
) -> tuple[Job, ...]:
    """List jobs for review, with noncanonical group members hidden by default."""

    statement = select(Job).outerjoin(
        CanonicalJobMember, CanonicalJobMember.job_id == Job.id
    )
    if not include_hidden:
        statement = statement.where(
            or_(
                CanonicalJobMember.id.is_(None),
                CanonicalJobMember.hidden_by_default.is_(False),
            )
        )
    return tuple(session.scalars(statement.order_by(Job.id)))


def duplicate_conflict_warnings(
    session: Session,
    group: CanonicalJobGroup | str,
) -> tuple[DuplicateConflictWarning, ...]:
    """Report workflow conflicts without modifying any application."""

    group_id = group.id if isinstance(group, CanonicalJobGroup) else group
    if session.get(CanonicalJobGroup, group_id) is None:
        raise LookupError(f"canonical job group not found: {group_id}")
    applications = list(
        session.execute(
            select(Application.id, Application.job_id)
            .join(
                CanonicalJobMember,
                CanonicalJobMember.job_id == Application.job_id,
            )
            .where(CanonicalJobMember.group_id == group_id)
            .order_by(Application.id)
        )
    )
    if len(applications) < 2:
        return ()
    job_ids = tuple(sorted({row.job_id for row in applications}))
    application_ids = tuple(row.id for row in applications)
    warning = DuplicateConflictWarning(
        code="multiple_applications",
        message=(
            f"Canonical group has {len(application_ids)} applications across "
            f"{len(job_ids)} job record(s); existing workflows remain attached "
            "to their original jobs."
        ),
        group_id=group_id,
        job_ids=job_ids,
        application_ids=application_ids,
    )
    return (warning,)


def alert_fingerprint(
    *,
    title: str,
    job_id: str | None = None,
    application_id: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    deduplication_key: str | None = None,
) -> str:
    """Build a stable recurrence fingerprint independent of mutable wording."""

    normalized_title = _normalize_text(title)
    normalized_key = _normalize_text(deduplication_key)
    if not normalized_title:
        raise ValueError("alert title must not be empty")
    if (entity_type is None) != (entity_id is None):
        raise ValueError("alert entity_type and entity_id must be provided together")
    payload = {
        "version": ALERT_FINGERPRINT_VERSION,
        "key": normalized_key or None,
        "title": normalized_title if not normalized_key else None,
        # A caller-supplied key identifies the recurring condition. Entity
        # links remain mutable context and update to the latest occurrence.
        "job_id": None if normalized_key else job_id,
        "application_id": None if normalized_key else application_id,
        "entity_type": (
            None if normalized_key else _normalize_text(entity_type) or None
        ),
        "entity_id": None if normalized_key else entity_id,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def create_or_recur_alert(
    session: Session,
    *,
    severity: AlertSeverity | str,
    title: str,
    message: str,
    job_id: str | None = None,
    application_id: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    deduplication_key: str | None = None,
    fingerprint: str | None = None,
    now: datetime | None = None,
) -> Alert:
    """Create one alert per fingerprint or recur and reopen the existing row."""

    clean_title = title.strip()
    clean_message = message.strip()
    if not clean_title:
        raise ValueError("alert title must not be empty")
    if len(clean_title) > 500:
        raise ValueError("alert title must not exceed 500 characters")
    if not clean_message:
        raise ValueError("alert message must not be empty")
    if fingerprint is not None and deduplication_key is not None:
        raise ValueError("provide fingerprint or deduplication_key, not both")
    resolved_entity_type, resolved_entity_id = _resolve_entity(
        job_id=job_id,
        application_id=application_id,
        entity_type=entity_type,
        entity_id=entity_id,
    )
    identity = (
        _external_fingerprint(fingerprint)
        if fingerprint is not None
        else alert_fingerprint(
            title=clean_title,
            job_id=job_id,
            application_id=application_id,
            entity_type=resolved_entity_type,
            entity_id=resolved_entity_id,
            deduplication_key=deduplication_key,
        )
    )
    severity_value = _coerce_severity(severity)
    occurred_at = _as_utc(now)
    latest_occurrence = occurred_at >= func.coalesce(
        Alert.last_recurred_at, Alert.created_at
    )
    # A single SQLite upsert closes the check-then-insert race between scanner,
    # maintenance and headless sessions. Recurrence context always represents the
    # latest occurrence, including an intentional clearing of old deep links.
    alert_id = session.scalar(
        sqlite_insert(Alert)
        .values(
            id=new_id(),
            severity=severity_value,
            title=clean_title,
            message=clean_message,
            job_id=job_id,
            application_id=application_id,
            fingerprint=identity,
            recurrence_count=1,
            entity_type=resolved_entity_type,
            entity_id=resolved_entity_id,
            created_at=occurred_at,
            updated_at=occurred_at,
        )
        .on_conflict_do_update(
            index_elements=[Alert.fingerprint],
            set_={
                "severity": case(
                    (latest_occurrence, severity_value), else_=Alert.severity
                ),
                "title": case((latest_occurrence, clean_title), else_=Alert.title),
                "message": case(
                    (latest_occurrence, clean_message), else_=Alert.message
                ),
                "job_id": case((latest_occurrence, job_id), else_=Alert.job_id),
                "application_id": case(
                    (latest_occurrence, application_id),
                    else_=Alert.application_id,
                ),
                "entity_type": case(
                    (latest_occurrence, resolved_entity_type),
                    else_=Alert.entity_type,
                ),
                "entity_id": case(
                    (latest_occurrence, resolved_entity_id),
                    else_=Alert.entity_id,
                ),
                "recurrence_count": case(
                    (Alert.recurrence_count < 1, 2),
                    else_=Alert.recurrence_count + 1,
                ),
                "last_recurred_at": case(
                    (latest_occurrence, occurred_at),
                    else_=Alert.last_recurred_at,
                ),
                "acknowledged_at": case(
                    (latest_occurrence, None), else_=Alert.acknowledged_at
                ),
                "snoozed_until": case(
                    (latest_occurrence, None), else_=Alert.snoozed_until
                ),
                "resolved_at": case((latest_occurrence, None), else_=Alert.resolved_at),
                "resolution_reason": case(
                    (latest_occurrence, None), else_=Alert.resolution_reason
                ),
                "updated_at": case(
                    (latest_occurrence, occurred_at), else_=Alert.updated_at
                ),
            },
        )
        .returning(Alert.id)
    )
    alert = session.scalar(
        select(Alert)
        .where(Alert.id == alert_id)
        .execution_options(populate_existing=True)
    )
    if alert is None:  # pragma: no cover - guarded by RETURNING above
        raise RuntimeError("alert upsert did not return a persisted row")
    return alert


def list_alert_inbox(
    session: Session,
    *,
    unread_only: bool = True,
    include_snoozed: bool = False,
    include_resolved: bool = False,
    severities: Iterable[AlertSeverity | str] | AlertSeverity | str | None = None,
    now: datetime | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[Alert, ...]:
    """List alert inbox rows, defaulting to active, unsnoozed, unread alerts."""

    if offset < 0:
        raise ValueError("alert inbox offset must not be negative")
    current = _as_utc(now)
    statement = select(Alert)
    if unread_only:
        statement = statement.where(Alert.acknowledged_at.is_(None))
    if not include_snoozed:
        statement = statement.where(
            or_(Alert.snoozed_until.is_(None), Alert.snoozed_until <= current)
        )
    if not include_resolved:
        statement = statement.where(Alert.resolved_at.is_(None))
    severity_values = _coerce_severities(severities)
    if severity_values is not None:
        if not severity_values:
            return ()
        statement = statement.where(Alert.severity.in_(severity_values))
    severity_order = case(
        (Alert.severity == AlertSeverity.URGENT, 0),
        (Alert.severity == AlertSeverity.WARNING, 1),
        else_=2,
    )
    statement = statement.order_by(
        severity_order,
        func.coalesce(Alert.last_recurred_at, Alert.created_at).desc(),
        Alert.id,
    )
    if limit is not None:
        if limit < 0:
            raise ValueError("alert inbox limit must not be negative")
        if limit == 0:
            return ()
        statement = statement.limit(limit)
    if offset:
        statement = statement.offset(offset)
    return tuple(session.scalars(statement))


def acknowledge_alert(
    session: Session,
    alert: Alert | str,
    *,
    now: datetime | None = None,
) -> Alert:
    """Acknowledge an alert while retaining it in history."""

    row = _load_alert(session, alert)
    if row.acknowledged_at is None:
        acknowledged_at = _as_utc(now)
        row.acknowledged_at = acknowledged_at
        row.updated_at = acknowledged_at
        session.flush()
    return row


def snooze_alert(
    session: Session,
    alert: Alert | str,
    *,
    until: datetime,
    now: datetime | None = None,
) -> Alert:
    """Hide an unresolved alert until a future UTC-normalized instant."""

    row = _load_alert(session, alert)
    current = _as_utc(now)
    wake_at = _as_utc(until)
    if wake_at <= current:
        raise ValueError("alert snooze must end in the future")
    if row.resolved_at is not None:
        raise ReviewQueueError("a resolved alert cannot be snoozed")
    row.snoozed_until = wake_at
    row.updated_at = current
    session.flush()
    return row


def resolve_alert(
    session: Session,
    alert: Alert | str,
    *,
    reason: str | None = None,
    now: datetime | None = None,
) -> Alert:
    """Resolve an alert non-destructively; recurrence can reopen it later."""

    row = _load_alert(session, alert)
    clean_reason = reason.strip() if reason else None
    if clean_reason and "\x00" in clean_reason:
        raise ValueError("alert resolution reason must not contain NUL characters")
    if clean_reason and len(clean_reason) > 2_000:
        raise ValueError("alert resolution reason must not exceed 2000 characters")
    resolved_at = _as_utc(now)
    if row.resolved_at is None:
        row.resolved_at = resolved_at
        row.resolution_reason = clean_reason
    elif clean_reason is not None:
        row.resolution_reason = clean_reason
    row.snoozed_until = None
    row.updated_at = resolved_at
    session.flush()
    return row


def _job_comparison_payload(job: Job) -> dict[str, str | None]:
    description = _normalize_text(job.description)
    return {
        "id": job.id,
        "company_id": job.company_id,
        "location_id": job.location_id,
        "title": _normalize_text(job.normalized_title or job.title),
        "comparison_url": (job.comparison_url or job.canonical_url or "").strip()
        or None,
        "description_hash": job.description_hash,
        "description_content_hash": (
            sha256(description.encode("utf-8")).hexdigest() if description else None
        ),
        "remote_status": _normalize_text(job.remote_status) or None,
    }


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").casefold().split())


def _load_job(session: Session, job: Job | str) -> Job:
    if isinstance(job, Job):
        if not job.id:
            raise ValueError("jobs must be persisted before review")
        row = session.get(Job, job.id)
    else:
        row = session.get(Job, job)
    if row is None:
        identifier = job.id if isinstance(job, Job) else job
        raise LookupError(f"job not found: {identifier}")
    return row


def _load_pair(
    session: Session,
    left: Job | str,
    right: Job | str,
) -> tuple[Job, Job]:
    left_job = _load_job(session, left)
    right_job = _load_job(session, right)
    if left_job.id == right_job.id:
        raise ValueError("a job cannot be compared with itself")
    return left_job, right_job


def _relationship_for_pair(
    session: Session,
    left_id: str,
    right_id: str,
) -> DuplicateRelationship | None:
    rows = list(
        session.scalars(
            select(DuplicateRelationship)
            .where(
                or_(
                    (
                        (DuplicateRelationship.job_id == left_id)
                        & (DuplicateRelationship.duplicate_job_id == right_id)
                    ),
                    (
                        (DuplicateRelationship.job_id == right_id)
                        & (DuplicateRelationship.duplicate_job_id == left_id)
                    ),
                )
            )
            .order_by(DuplicateRelationship.id)
        )
    )
    if len(rows) > 1:
        raise ReviewQueueError("duplicate pair is stored in both orientations")
    return rows[0] if rows else None


def _load_relationship(
    session: Session,
    relationship: DuplicateRelationship | str,
) -> DuplicateRelationship:
    identifier = (
        relationship.id
        if isinstance(relationship, DuplicateRelationship)
        else relationship
    )
    row = session.get(DuplicateRelationship, identifier)
    if row is None:
        raise LookupError(f"duplicate relationship not found: {identifier}")
    return row


def _load_group(
    session: Session,
    group: CanonicalJobGroup | str,
) -> CanonicalJobGroup:
    identifier = group.id if isinstance(group, CanonicalJobGroup) else group
    row = session.get(CanonicalJobGroup, identifier)
    if row is None:
        raise LookupError(f"canonical job group not found: {identifier}")
    return row


def _choose_target_group(
    groups: list[CanonicalJobGroup],
    memberships: list[CanonicalJobMember],
    canonical_job_id: str | None,
) -> CanonicalJobGroup:
    if canonical_job_id is not None:
        preferred_group_id = next(
            (
                member.group_id
                for member in memberships
                if member.job_id == canonical_job_id
            ),
            None,
        )
        if preferred_group_id is not None:
            for group in groups:
                if group.id == preferred_group_id:
                    return group
    return min(groups, key=lambda group: group.id)


def _merge_groups(
    session: Session,
    target: CanonicalJobGroup,
    groups: list[CanonicalJobGroup],
) -> None:
    for source in sorted(groups, key=lambda group: group.id):
        if source.id == target.id:
            continue
        for member in session.scalars(
            select(CanonicalJobMember).where(CanonicalJobMember.group_id == source.id)
        ):
            member.group_id = target.id
        for relationship in session.scalars(
            select(DuplicateRelationship).where(
                DuplicateRelationship.canonical_group_id == source.id
            )
        ):
            relationship.canonical_group_id = target.id
        session.flush()
        session.delete(source)
        session.flush()


def _set_canonical(
    session: Session,
    group: CanonicalJobGroup,
    canonical_job_id: str,
) -> None:
    members = list(
        session.scalars(
            select(CanonicalJobMember).where(CanonicalJobMember.group_id == group.id)
        )
    )
    if canonical_job_id not in {member.job_id for member in members}:
        raise ReviewQueueError("the canonical job must already belong to the group")
    group.canonical_job_id = canonical_job_id
    for member in members:
        member.is_canonical = member.job_id == canonical_job_id
        member.hidden_by_default = member.job_id != canonical_job_id


def _resolve_entity(
    *,
    job_id: str | None,
    application_id: str | None,
    entity_type: str | None,
    entity_id: str | None,
) -> tuple[str | None, str | None]:
    if (entity_type is None) != (entity_id is None):
        raise ValueError("alert entity_type and entity_id must be provided together")
    if entity_type is not None:
        clean_type = _normalize_text(entity_type)
        if not clean_type:
            raise ValueError("alert entity_type must not be empty")
        clean_id = (entity_id or "").strip()
        if not clean_id:
            raise ValueError("alert entity_id must not be empty")
        return clean_type, clean_id
    if application_id is not None:
        return "application", application_id
    if job_id is not None:
        return "job", job_id
    return None, None


def _external_fingerprint(value: str) -> str:
    clean = value.strip()
    if not clean:
        raise ValueError("alert fingerprint must not be empty")
    if _SHA256_PATTERN.fullmatch(clean):
        return clean.casefold()
    return sha256(f"external-alert-key:{clean}".encode()).hexdigest()


def _coerce_severity(value: AlertSeverity | str) -> AlertSeverity:
    try:
        return value if isinstance(value, AlertSeverity) else AlertSeverity(value)
    except ValueError as exc:
        raise ValueError(f"unknown alert severity: {value}") from exc


def _coerce_severities(
    values: Iterable[AlertSeverity | str] | AlertSeverity | str | None,
) -> tuple[AlertSeverity, ...] | None:
    if values is None:
        return None
    if isinstance(values, (AlertSeverity, str)):
        values = (values,)
    return tuple(dict.fromkeys(_coerce_severity(value) for value in values))


def _load_alert(session: Session, alert: Alert | str) -> Alert:
    identifier = alert.id if isinstance(alert, Alert) else alert
    row = session.get(Alert, identifier)
    if row is None:
        raise LookupError(f"alert not found: {identifier}")
    return row


def _as_utc(value: datetime | None) -> datetime:
    result = value or utc_now()
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return result.astimezone(timezone.utc)
