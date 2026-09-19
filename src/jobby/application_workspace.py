"""Transactional application-workspace services.

These helpers accept an existing synchronous SQLAlchemy session, flush changes
needed to enforce invariants, and leave commit/rollback control with callers.
Application creation and stage history remain in :mod:`jobby.pipeline`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any, Final

from sqlalchemy import and_, exists, func, literal, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .audit import record_audit
from .enums import ApplicationStage, TaskStatus
from .models import (
    Application,
    ApplicationContact,
    AuditEvent,
    CanonicalJobMember,
    Company,
    Contact,
    Evaluation,
    Interview,
    Job,
    SourceObservation,
    Task,
    new_id,
    utc_now,
)
from .review_queues import workflow_target_job_id


DEFAULT_FOLLOW_UP_DAYS = 7
FOLLOW_UP_AUTOMATION_PREFIX = "application-follow-up:"
_UNSET: Final[object] = object()


def route_new_application_job(session: Session, job_id: str) -> str:
    """Route a new workflow record to its group's canonical member."""

    return workflow_target_job_id(session, job_id)


def get_application(session: Session, application: Application | str) -> Application:
    if isinstance(application, Application):
        return application
    row = session.get(Application, application)
    if row is None:
        raise LookupError("application not found")
    return row


def get_task(session: Session, task: Task | str) -> Task:
    if isinstance(task, Task):
        return task
    row = session.get(Task, task)
    if row is None:
        raise LookupError("task not found")
    return row


def get_contact(session: Session, contact: Contact | str) -> Contact:
    if isinstance(contact, Contact):
        return contact
    row = session.get(Contact, contact)
    if row is None:
        raise LookupError("contact not found")
    return row


def get_interview(session: Session, interview: Interview | str) -> Interview:
    if isinstance(interview, Interview):
        return interview
    row = session.get(Interview, interview)
    if row is None:
        raise LookupError("interview not found")
    return row


def snapshot_application_provenance(
    session: Session,
    application: Application | str,
    *,
    actor: str = "user",
) -> Application:
    """Freeze evaluation and acquisition provenance at application time."""

    row = get_application(session, application)
    if ApplicationStage(row.current_stage) != ApplicationStage.APPLIED:
        raise ValueError("application provenance can only be snapshotted in Applied")
    already_snapshotted = session.scalar(
        select(AuditEvent.id)
        .where(
            AuditEvent.action == "application.provenance_snapshotted",
            AuditEvent.entity_type == "application",
            AuditEvent.entity_id == row.id,
        )
        .limit(1)
    )
    if already_snapshotted is not None:
        return row
    evaluation = session.scalar(
        select(Evaluation)
        .where(Evaluation.job_id == row.job_id, Evaluation.is_current.is_(True))
        .order_by(Evaluation.created_at.desc(), Evaluation.id.desc())
        .limit(1)
    )
    sources = _acquisition_sources(session, row.job_id)
    row.applied_evaluation_id = evaluation.id if evaluation is not None else None
    row.applied_score = float(evaluation.score) if evaluation is not None else None
    row.applied_ranker_version = (
        evaluation.ranker_version if evaluation is not None else None
    )
    row.applied_sources = sources
    record_audit(
        session,
        action="application.provenance_snapshotted",
        entity_type="application",
        entity_id=row.id,
        actor=actor,
        after={
            "evaluation_id": row.applied_evaluation_id,
            "score": row.applied_score,
            "ranker_version": row.applied_ranker_version,
            "sources": sources,
        },
    )
    return row


def _acquisition_sources(
    session: Session, canonical_job_id: str
) -> list[dict[str, str]]:
    member = session.scalar(
        select(CanonicalJobMember).where(CanonicalJobMember.job_id == canonical_job_id)
    )
    if member is None:
        job_ids = [canonical_job_id]
    else:
        job_ids = list(
            session.scalars(
                select(CanonicalJobMember.job_id)
                .where(CanonicalJobMember.group_id == member.group_id)
                .order_by(CanonicalJobMember.job_id)
            )
        )

    entries: dict[tuple[str, str, str, str, str], dict[str, str]] = {}
    observations = list(
        session.scalars(
            select(SourceObservation)
            .where(SourceObservation.job_id.in_(job_ids))
            .order_by(
                SourceObservation.job_id,
                SourceObservation.source,
                SourceObservation.observed_at.desc(),
                SourceObservation.id.desc(),
            )
        )
    )
    for observation in observations:
        key = (
            observation.job_id,
            observation.source,
            observation.source_account or "",
            observation.source_job_id or "",
            observation.source_url or "",
        )
        if key in entries:
            continue
        entries[key] = _without_none(
            {
                "job_id": observation.job_id,
                "source": observation.source,
                "source_account": observation.source_account,
                "source_job_id": observation.source_job_id,
                "source_url": observation.source_url,
            }
        )

    jobs = list(
        session.scalars(select(Job).where(Job.id.in_(job_ids)).order_by(Job.id))
    )
    for job in jobs:
        if not job.source_primary:
            continue
        source_url = job.launch_url or job.canonical_url
        key = (
            job.id,
            job.source_primary,
            "",
            job.source_id or "",
            source_url or "",
        )
        entries.setdefault(
            key,
            _without_none(
                {
                    "job_id": job.id,
                    "source": job.source_primary,
                    "source_job_id": job.source_id,
                    "source_url": source_url,
                }
            ),
        )
    return [entries[key] for key in sorted(entries)]


def _without_none(values: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in values.items() if value is not None}


def ensure_application_follow_up_task(
    session: Session,
    application: Application | str,
    *,
    occurred_at: datetime | None = None,
    due_at: datetime | None = None,
    follow_up_days: int = DEFAULT_FOLLOW_UP_DAYS,
    actor: str = "user",
) -> Task:
    """Create exactly one linked follow-up without resetting later edits."""

    row = get_application(session, application)
    if ApplicationStage(row.current_stage) != ApplicationStage.APPLIED:
        raise ValueError("application follow-up tasks require the Applied stage")
    validate_application_follow_up_request(
        session,
        row,
        occurred_at=occurred_at,
        due_at=due_at,
        follow_up_days=follow_up_days,
    )
    base = _as_utc(occurred_at or utc_now(), field="occurred_at")
    requested_due = (
        _as_utc(due_at, field="due_at")
        if due_at is not None
        else (
            _as_utc(row.follow_up_at, field="follow_up_at")
            if row.follow_up_at is not None
            else base + timedelta(days=follow_up_days)
        )
    )
    automation_key = follow_up_automation_key(row.id)
    existing = session.scalar(select(Task).where(Task.automation_key == automation_key))
    if existing is not None:
        _validate_follow_up_task(existing, row)
        if row.follow_up_at is None:
            row.follow_up_at = existing.due_at
        return existing

    now = utc_now()
    inserted_id = session.scalar(
        sqlite_insert(Task)
        .values(
            id=new_id(),
            title="Follow up on application",
            description="Automatically created when the application entered Applied.",
            status=TaskStatus.PENDING,
            due_at=requested_due,
            completed_at=None,
            job_id=row.job_id,
            application_id=row.id,
            automation_key=automation_key,
            created_at=now,
            updated_at=now,
        )
        .on_conflict_do_nothing(index_elements=[Task.automation_key])
        .returning(Task.id)
    )
    session.flush()
    task = session.scalar(select(Task).where(Task.automation_key == automation_key))
    if task is None:  # pragma: no cover - database invariant guard
        raise RuntimeError("follow-up task could not be loaded after creation")
    _validate_follow_up_task(task, row)
    row.follow_up_at = task.due_at
    if inserted_id is not None:
        record_audit(
            session,
            action="task.follow_up_created",
            entity_type="task",
            entity_id=task.id,
            actor=actor,
            after={
                "application_id": row.id,
                "job_id": row.job_id,
                "due_at": task.due_at,
                "automation_key": automation_key,
            },
        )
    return task


def follow_up_automation_key(application_id: str) -> str:
    return f"{FOLLOW_UP_AUTOMATION_PREFIX}{application_id}"


def validate_application_follow_up_request(
    session: Session,
    application: Application | str,
    *,
    occurred_at: datetime | None,
    due_at: datetime | None,
    follow_up_days: int,
) -> None:
    """Validate an Applied-transition follow-up before any stage mutation."""

    row = get_application(session, application)
    if (
        isinstance(follow_up_days, bool)
        or not isinstance(follow_up_days, int)
        or not 0 <= follow_up_days <= 365
    ):
        raise ValueError("follow-up days must be between zero and 365")
    if occurred_at is not None:
        _as_utc(occurred_at, field="occurred_at")
    if due_at is not None:
        _as_utc(due_at, field="due_at")
    existing = session.scalar(
        select(Task).where(Task.automation_key == follow_up_automation_key(row.id))
    )
    if existing is not None:
        _validate_follow_up_task(existing, row)


def _validate_follow_up_task(task: Task, application: Application) -> None:
    if (
        task.automation_key != follow_up_automation_key(application.id)
        or task.application_id != application.id
        or task.job_id != application.job_id
    ):
        raise RuntimeError("follow-up automation key is linked to another record")


def _is_reserved_follow_up_key(value: str | None) -> bool:
    return value is not None and value.startswith(FOLLOW_UP_AUTOMATION_PREFIX)


def _is_application_follow_up_task(task: Task) -> bool:
    return (
        task.application_id is not None
        and task.automation_key == follow_up_automation_key(task.application_id)
    )


def normalize_application_create_fields(
    *, submission_channel: Any, notes: Any
) -> tuple[str | None, str | None]:
    """Apply the same metadata contract used by the application editor."""

    return (
        _optional_text(submission_channel, field="submission channel", max_length=200),
        _optional_text(notes, field="application notes", collapse=False),
    )


def update_application(
    session: Session,
    application: Application | str,
    *,
    submission_channel: Any = _UNSET,
    submitted_at: Any = _UNSET,
    follow_up_at: Any = _UNSET,
    rejection_reason: Any = _UNSET,
    notes: Any = _UNSET,
    import_key: Any = _UNSET,
    actor: str = "user",
) -> Application:
    """Edit user-owned application metadata; stage and snapshots stay guarded."""

    row = get_application(session, application)
    before = _application_editable_state(row)
    new_submission_channel = (
        row.submission_channel
        if submission_channel is _UNSET
        else _optional_text(
            submission_channel, field="submission channel", max_length=200
        )
    )
    new_submitted_at = (
        row.submitted_at
        if submitted_at is _UNSET
        else _optional_datetime(submitted_at, field="submitted_at")
    )
    new_follow_up_at = (
        row.follow_up_at
        if follow_up_at is _UNSET
        else _optional_datetime(follow_up_at, field="follow_up_at")
    )
    new_rejection_reason = (
        row.rejection_reason
        if rejection_reason is _UNSET
        else _optional_text(rejection_reason, field="rejection reason", collapse=False)
    )
    new_notes = (
        row.notes
        if notes is _UNSET
        else _optional_text(notes, field="application notes", collapse=False)
    )
    normalized_key = (
        row.import_key
        if import_key is _UNSET
        else _optional_text(import_key, field="application import key", max_length=64)
    )
    if normalized_key is not None and normalized_key != row.import_key:
        duplicate = session.scalar(
            select(Application.id).where(
                Application.import_key == normalized_key,
                Application.id != row.id,
            )
        )
        if duplicate is not None:
            raise ValueError("application import key is already in use")

    follow_up_task: Task | None = None
    if follow_up_at is not _UNSET:
        follow_up_task = session.scalar(
            select(Task).where(Task.automation_key == follow_up_automation_key(row.id))
        )
        if follow_up_task is not None:
            _validate_follow_up_task(follow_up_task, row)

    row.submission_channel = new_submission_channel
    row.submitted_at = new_submitted_at
    row.follow_up_at = new_follow_up_at
    row.rejection_reason = new_rejection_reason
    row.notes = new_notes
    row.import_key = normalized_key
    if follow_up_task is not None:
        follow_up_task.due_at = row.follow_up_at
    _record_update(
        session,
        action="application.updated",
        entity_type="application",
        entity_id=row.id,
        actor=actor,
        before=before,
        after=_application_editable_state(row),
    )
    return row


def _application_editable_state(row: Application) -> dict[str, Any]:
    return {
        "submission_channel": row.submission_channel,
        "submitted_at": row.submitted_at,
        "follow_up_at": row.follow_up_at,
        "rejection_reason": row.rejection_reason,
        "notes": row.notes,
        "import_key": row.import_key,
    }


def delete_application(
    session: Session,
    application: Application | str,
    *,
    confirm: bool = False,
    actor: str = "user",
) -> None:
    row = get_application(session, application)
    if not confirm:
        raise PermissionError("application deletion requires explicit confirmation")
    record_audit(
        session,
        action="application.deleted",
        entity_type="application",
        entity_id=row.id,
        actor=actor,
        before={
            "job_id": row.job_id,
            "stage": row.current_stage,
            **_application_editable_state(row),
        },
    )
    session.delete(row)
    session.flush()


def create_task(
    session: Session,
    *,
    title: str,
    description: str | None = None,
    status: TaskStatus | str = TaskStatus.PENDING,
    due_at: datetime | None = None,
    completed_at: datetime | None = None,
    job_id: str | None = None,
    application: Application | str | None = None,
    automation_key: str | None = None,
    actor: str = "user",
) -> Task:
    normalized_title = _required_text(title, field="task title", max_length=500)
    normalized_description = _optional_text(
        description, field="task description", collapse=False
    )
    normalized_due = _optional_datetime(due_at, field="due_at")
    normalized_key = _optional_text(
        automation_key, field="automation key", max_length=200
    )
    if _is_reserved_follow_up_key(normalized_key):
        raise ValueError("application follow-up automation keys are reserved")
    application_row = (
        get_application(session, application) if application is not None else None
    )
    if application_row is None and job_id is not None:
        job_id = route_new_application_job(session, job_id)
    resolved_job_id = _resolve_task_job_id(
        session, job_id=job_id, application=application_row
    )
    normalized_status = TaskStatus(status)
    normalized_completed = _optional_datetime(completed_at, field="completed_at")
    if normalized_status == TaskStatus.COMPLETED and normalized_completed is None:
        normalized_completed = utc_now()
    if normalized_status != TaskStatus.COMPLETED and normalized_completed is not None:
        raise ValueError("only completed tasks may have completed_at")
    if normalized_key is not None:
        existing = session.scalar(
            select(Task).where(Task.automation_key == normalized_key)
        )
        if existing is not None:
            expected_application_id = (
                application_row.id if application_row is not None else None
            )
            if (
                existing.application_id != expected_application_id
                or existing.job_id != resolved_job_id
            ):
                raise ValueError("task automation key is linked to another record")
            return existing
    task = Task(
        title=normalized_title,
        description=normalized_description,
        status=normalized_status,
        due_at=normalized_due,
        completed_at=normalized_completed,
        job_id=resolved_job_id,
        application_id=application_row.id if application_row is not None else None,
        automation_key=normalized_key,
    )
    session.add(task)
    session.flush()
    record_audit(
        session,
        action="task.created",
        entity_type="task",
        entity_id=task.id,
        actor=actor,
        after=_task_state(task),
    )
    return task


def update_task(
    session: Session,
    task: Task | str,
    *,
    title: Any = _UNSET,
    description: Any = _UNSET,
    status: Any = _UNSET,
    due_at: Any = _UNSET,
    completed_at: Any = _UNSET,
    job_id: Any = _UNSET,
    application: Any = _UNSET,
    automation_key: Any = _UNSET,
    actor: str = "user",
) -> Task:
    row = get_task(session, task)
    before = _task_state(row)
    application_row = (
        get_application(session, application)
        if application is not _UNSET and application is not None
        else (
            None
            if application is None
            else (
                get_application(session, row.application_id)
                if row.application_id is not None
                else None
            )
        )
    )
    requested_job_id = row.job_id if job_id is _UNSET else job_id
    resolved_job_id = _resolve_task_job_id(
        session, job_id=requested_job_id, application=application_row
    )
    new_title = (
        row.title
        if title is _UNSET
        else _required_text(title, field="task title", max_length=500)
    )
    new_description = (
        row.description
        if description is _UNSET
        else _optional_text(description, field="task description", collapse=False)
    )
    new_due_at = (
        row.due_at if due_at is _UNSET else _optional_datetime(due_at, field="due_at")
    )
    normalized_status = TaskStatus(row.status if status is _UNSET else status)
    normalized_completed = (
        row.completed_at
        if completed_at is _UNSET
        else _optional_datetime(completed_at, field="completed_at")
    )
    if normalized_status == TaskStatus.COMPLETED and normalized_completed is None:
        normalized_completed = utc_now()
    if normalized_status != TaskStatus.COMPLETED:
        if completed_at is not _UNSET and normalized_completed is not None:
            raise ValueError("only completed tasks may have completed_at")
        normalized_completed = None
    normalized_key = (
        row.automation_key
        if automation_key is _UNSET
        else _optional_text(automation_key, field="automation key", max_length=200)
    )
    requested_application_id = (
        application_row.id if application_row is not None else None
    )
    if _is_application_follow_up_task(row):
        linked_application_id = row.application_id
        if linked_application_id is None:  # pragma: no cover - predicate invariant
            raise RuntimeError("follow-up task is missing its application")
        linked_application = get_application(session, linked_application_id)
        _validate_follow_up_task(row, linked_application)
        if (
            normalized_key != row.automation_key
            or requested_application_id != row.application_id
            or resolved_job_id != row.job_id
        ):
            raise ValueError(
                "automated follow-up task identity fields cannot be changed"
            )
    elif _is_reserved_follow_up_key(normalized_key):
        raise ValueError("application follow-up automation keys are reserved")
    if normalized_key is not None and normalized_key != row.automation_key:
        duplicate = session.scalar(
            select(Task.id).where(
                Task.automation_key == normalized_key, Task.id != row.id
            )
        )
        if duplicate is not None:
            raise ValueError("task automation key is already in use")

    row.title = new_title
    row.description = new_description
    row.due_at = new_due_at
    row.status = normalized_status
    row.completed_at = normalized_completed
    row.job_id = resolved_job_id
    row.application_id = requested_application_id
    row.automation_key = normalized_key
    if _is_application_follow_up_task(row):
        linked_application_id = row.application_id
        if linked_application_id is None:  # pragma: no cover - predicate invariant
            raise RuntimeError("follow-up task is missing its application")
        linked_application = get_application(session, linked_application_id)
        linked_application.follow_up_at = row.due_at
    _record_update(
        session,
        action="task.updated",
        entity_type="task",
        entity_id=row.id,
        actor=actor,
        before=before,
        after=_task_state(row),
    )
    return row


def _resolve_task_job_id(
    session: Session,
    *,
    job_id: str | None,
    application: Application | None,
) -> str | None:
    if job_id is None and application is not None:
        return application.job_id
    if job_id is not None and session.get(Job, job_id) is None:
        raise LookupError("job not found")
    if application is not None and job_id != application.job_id:
        raise ValueError("task job must match its linked application")
    return job_id


def _task_state(row: Task) -> dict[str, Any]:
    return {
        "title": row.title,
        "description": row.description,
        "status": row.status,
        "due_at": row.due_at,
        "completed_at": row.completed_at,
        "job_id": row.job_id,
        "application_id": row.application_id,
        "automation_key": row.automation_key,
    }


def delete_task(
    session: Session,
    task: Task | str,
    *,
    confirm: bool = False,
    actor: str = "user",
) -> None:
    row = get_task(session, task)
    if not confirm:
        raise PermissionError("task deletion requires explicit confirmation")
    state = _task_state(row)
    record_audit(
        session,
        action="task.deleted",
        entity_type="task",
        entity_id=row.id,
        actor=actor,
        before=state,
    )
    if (
        row.application_id is not None
        and row.automation_key == follow_up_automation_key(row.application_id)
    ):
        application = session.get(Application, row.application_id)
        if application is not None:
            application.follow_up_at = None
    session.delete(row)
    session.flush()


def create_contact(
    session: Session,
    *,
    name: str,
    company_id: str | None = None,
    email: str | None = None,
    title: str | None = None,
    linkedin_url: str | None = None,
    notes: str | None = None,
    actor: str = "user",
) -> Contact:
    normalized_name = _required_text(name, field="contact name", max_length=300)
    normalized_email = _normalize_email(email)
    normalized_title = _optional_text(title, field="contact title", max_length=300)
    normalized_linkedin = _optional_text(
        linkedin_url, field="LinkedIn URL", max_length=2_000
    )
    normalized_notes = _optional_text(notes, field="contact notes", collapse=False)
    _require_company(session, company_id)
    candidate_id = new_id()
    now = utc_now()
    identity_filter = _contact_identity_filter(
        name=normalized_name,
        email=normalized_email,
        company_id=company_id,
    )
    candidate = select(
        literal(candidate_id),
        literal(company_id),
        literal(normalized_name),
        literal(normalized_email),
        literal(normalized_title),
        literal(normalized_linkedin),
        literal(normalized_notes),
        literal(now),
        literal(now),
    ).where(~exists(select(Contact.id).where(identity_filter)))
    inserted_id = session.scalar(
        sqlite_insert(Contact)
        .from_select(
            [
                Contact.id,
                Contact.company_id,
                Contact.name,
                Contact.email,
                Contact.title,
                Contact.linkedin_url,
                Contact.notes,
                Contact.created_at,
                Contact.updated_at,
            ],
            candidate,
        )
        .returning(Contact.id)
    )
    session.flush()
    row = (
        session.get(Contact, inserted_id)
        if inserted_id is not None
        else _matching_contact(
            session,
            name=normalized_name,
            email=normalized_email,
            company_id=company_id,
        )
    )
    if row is None:  # pragma: no cover - database invariant guard
        raise RuntimeError("contact could not be loaded after creation")
    if inserted_id is not None:
        record_audit(
            session,
            action="contact.created",
            entity_type="contact",
            entity_id=row.id,
            actor=actor,
            after=_contact_state(row),
        )
    else:
        _enrich_contact_blanks(
            session,
            row,
            company_id=company_id,
            email=normalized_email,
            title=normalized_title,
            linkedin_url=normalized_linkedin,
            notes=normalized_notes,
            actor=actor,
        )
    return row


def update_contact(
    session: Session,
    contact: Contact | str,
    *,
    company_id: Any = _UNSET,
    name: Any = _UNSET,
    email: Any = _UNSET,
    title: Any = _UNSET,
    linkedin_url: Any = _UNSET,
    notes: Any = _UNSET,
    actor: str = "user",
) -> Contact:
    row = get_contact(session, contact)
    before = _contact_state(row)
    new_company_id = row.company_id if company_id is _UNSET else company_id
    _require_company(session, new_company_id)
    new_name = (
        row.name
        if name is _UNSET
        else _required_text(name, field="contact name", max_length=300)
    )
    new_email = row.email if email is _UNSET else _normalize_email(email)
    new_title = (
        row.title
        if title is _UNSET
        else _optional_text(title, field="contact title", max_length=300)
    )
    new_linkedin = (
        row.linkedin_url
        if linkedin_url is _UNSET
        else _optional_text(linkedin_url, field="LinkedIn URL", max_length=2_000)
    )
    new_notes = (
        row.notes
        if notes is _UNSET
        else _optional_text(notes, field="contact notes", collapse=False)
    )
    duplicate = _matching_contact(
        session,
        name=new_name,
        email=new_email,
        company_id=new_company_id,
        exclude_id=row.id,
    )
    if duplicate is not None:
        raise ValueError("contact identity is already in use")
    row.company_id = new_company_id
    row.name = new_name
    row.email = new_email
    row.title = new_title
    row.linkedin_url = new_linkedin
    row.notes = new_notes
    _record_update(
        session,
        action="contact.updated",
        entity_type="contact",
        entity_id=row.id,
        actor=actor,
        before=before,
        after=_contact_state(row),
    )
    return row


def _matching_contact(
    session: Session,
    *,
    name: str,
    email: str | None,
    company_id: str | None,
    exclude_id: str | None = None,
) -> Contact | None:
    exclusion = Contact.id != exclude_id if exclude_id is not None else None
    if email is not None:
        statement = select(Contact).where(func.lower(Contact.email) == email.casefold())
        if exclusion is not None:
            statement = statement.where(exclusion)
        exact = session.scalar(
            statement.order_by(Contact.created_at, Contact.id).limit(1)
        )
        if exact is not None:
            return exact
    if company_id is not None:
        statement = select(Contact).where(
            Contact.company_id == company_id,
            func.lower(Contact.name) == name.casefold(),
        )
        if email is not None:
            statement = statement.where(Contact.email.is_(None))
        if exclusion is not None:
            statement = statement.where(exclusion)
        return session.scalar(
            statement.order_by(Contact.created_at, Contact.id).limit(1)
        )
    return None


def _contact_identity_filter(
    *, name: str, email: str | None, company_id: str | None
) -> Any:
    identities: list[Any] = []
    if email is not None:
        identities.append(func.lower(Contact.email) == email.casefold())
    if company_id is not None:
        same_company_name = and_(
            Contact.company_id == company_id,
            func.lower(Contact.name) == name.casefold(),
        )
        identities.append(
            same_company_name
            if email is None
            else and_(same_company_name, Contact.email.is_(None))
        )
    return or_(*identities) if identities else literal(False)


def _enrich_contact_blanks(
    session: Session,
    row: Contact,
    *,
    company_id: str | None,
    email: str | None,
    title: str | None,
    linkedin_url: str | None,
    notes: str | None,
    actor: str,
) -> None:
    before = _contact_state(row)
    if row.company_id is None and company_id is not None:
        row.company_id = company_id
    if row.email is None and email is not None:
        row.email = email
    if row.title is None and title is not None:
        row.title = title
    if row.linkedin_url is None and linkedin_url is not None:
        row.linkedin_url = linkedin_url
    if row.notes is None and notes is not None:
        row.notes = notes
    _record_update(
        session,
        action="contact.enriched",
        entity_type="contact",
        entity_id=row.id,
        actor=actor,
        before=before,
        after=_contact_state(row),
    )


def _contact_state(row: Contact) -> dict[str, Any]:
    return {
        "company_id": row.company_id,
        "name": row.name,
        "email": row.email,
        "title": row.title,
        "linkedin_url": row.linkedin_url,
        "notes": row.notes,
    }


def delete_contact(
    session: Session,
    contact: Contact | str,
    *,
    confirm: bool = False,
    actor: str = "user",
) -> None:
    row = get_contact(session, contact)
    if not confirm:
        raise PermissionError("contact deletion requires explicit confirmation")
    record_audit(
        session,
        action="contact.deleted",
        entity_type="contact",
        entity_id=row.id,
        actor=actor,
        before=_contact_state(row),
    )
    session.delete(row)
    session.flush()


def associate_application_contact(
    session: Session,
    application: Application | str,
    contact: Contact | str,
    *,
    role: str | None = None,
    actor: str = "user",
) -> ApplicationContact:
    application_row = get_application(session, application)
    contact_row = get_contact(session, contact)
    normalized_role = _optional_text(
        role, field="application contact role", max_length=200
    )
    now = utc_now()
    inserted_id = session.scalar(
        sqlite_insert(ApplicationContact)
        .values(
            id=new_id(),
            application_id=application_row.id,
            contact_id=contact_row.id,
            role=normalized_role,
            created_at=now,
            updated_at=now,
        )
        .on_conflict_do_nothing(
            index_elements=[
                ApplicationContact.application_id,
                ApplicationContact.contact_id,
            ]
        )
        .returning(ApplicationContact.id)
    )
    session.flush()
    link = session.scalar(
        select(ApplicationContact).where(
            ApplicationContact.application_id == application_row.id,
            ApplicationContact.contact_id == contact_row.id,
        )
    )
    if link is None:  # pragma: no cover - database invariant guard
        raise RuntimeError("application contact link could not be loaded")
    if inserted_id is not None:
        record_audit(
            session,
            action="application.contact_associated",
            entity_type="application_contact",
            entity_id=link.id,
            actor=actor,
            after={
                "application_id": application_row.id,
                "contact_id": contact_row.id,
                "role": normalized_role,
            },
        )
    elif normalized_role is not None and normalized_role != link.role:
        before = link.role
        link.role = normalized_role
        record_audit(
            session,
            action="application.contact_role_updated",
            entity_type="application_contact",
            entity_id=link.id,
            actor=actor,
            before={"role": before},
            after={"role": normalized_role},
        )
    return link


def create_and_associate_contact(
    session: Session,
    application: Application | str,
    *,
    name: str,
    company_id: str | None = None,
    email: str | None = None,
    title: str | None = None,
    linkedin_url: str | None = None,
    notes: str | None = None,
    role: str | None = None,
    actor: str = "user",
) -> tuple[Contact, ApplicationContact]:
    application_row = get_application(session, application)
    if company_id is None:
        job = session.get(Job, application_row.job_id)
        if job is None:  # pragma: no cover - foreign-key invariant
            raise LookupError("application job not found")
        company_id = job.company_id
    contact = create_contact(
        session,
        name=name,
        company_id=company_id,
        email=email,
        title=title,
        linkedin_url=linkedin_url,
        notes=notes,
        actor=actor,
    )
    link = associate_application_contact(
        session, application_row, contact, role=role, actor=actor
    )
    return contact, link


def update_application_contact(
    session: Session,
    link: ApplicationContact | str,
    *,
    role: str | None,
    actor: str = "user",
) -> ApplicationContact:
    row = _get_application_contact(session, link)
    before = row.role
    row.role = _optional_text(role, field="application contact role", max_length=200)
    _record_update(
        session,
        action="application.contact_role_updated",
        entity_type="application_contact",
        entity_id=row.id,
        actor=actor,
        before={"role": before},
        after={"role": row.role},
    )
    return row


def remove_application_contact(
    session: Session,
    link: ApplicationContact | str,
    *,
    actor: str = "user",
) -> None:
    row = _get_application_contact(session, link)
    record_audit(
        session,
        action="application.contact_removed",
        entity_type="application_contact",
        entity_id=row.id,
        actor=actor,
        before={
            "application_id": row.application_id,
            "contact_id": row.contact_id,
            "role": row.role,
        },
    )
    session.delete(row)
    session.flush()


def _get_application_contact(
    session: Session, link: ApplicationContact | str
) -> ApplicationContact:
    if isinstance(link, ApplicationContact):
        return link
    row = session.get(ApplicationContact, link)
    if row is None:
        raise LookupError("application contact association not found")
    return row


def create_interview(
    session: Session,
    application: Application | str,
    starts_at: datetime,
    *,
    ends_at: datetime | None = None,
    interview_type: str | None = None,
    location_or_link: str | None = None,
    contact: Contact | str | None = None,
    notes: str | None = None,
    calendar_event_id: str | None = None,
    actor: str = "user",
) -> Interview:
    application_row = get_application(session, application)
    contact_row = get_contact(session, contact) if contact is not None else None
    start, end = _interview_times(starts_at, ends_at)
    normalized_type = _optional_text(
        interview_type, field="interview type", max_length=100
    )
    normalized_location = _optional_text(
        location_or_link, field="interview location", max_length=2_000
    )
    normalized_notes = _optional_text(notes, field="interview notes", collapse=False)
    event_id = _optional_text(
        calendar_event_id, field="calendar event ID", max_length=500
    )
    if event_id is not None:
        existing = session.scalar(
            select(Interview).where(Interview.calendar_event_id == event_id)
        )
        if existing is not None:
            if existing.application_id != application_row.id:
                raise ValueError("calendar event is linked to another application")
            return existing
    row = Interview(
        application_id=application_row.id,
        starts_at=start,
        ends_at=end,
        interview_type=normalized_type,
        location_or_link=normalized_location,
        contact_id=contact_row.id if contact_row is not None else None,
        notes=normalized_notes,
        calendar_event_id=event_id,
    )
    session.add(row)
    session.flush()
    record_audit(
        session,
        action="interview.created",
        entity_type="interview",
        entity_id=row.id,
        actor=actor,
        after=_interview_state(row),
    )
    return row


def update_interview(
    session: Session,
    interview: Interview | str,
    *,
    application: Any = _UNSET,
    starts_at: Any = _UNSET,
    ends_at: Any = _UNSET,
    interview_type: Any = _UNSET,
    location_or_link: Any = _UNSET,
    contact: Any = _UNSET,
    notes: Any = _UNSET,
    calendar_event_id: Any = _UNSET,
    actor: str = "user",
) -> Interview:
    row = get_interview(session, interview)
    before = _interview_state(row)
    application_row = (
        get_application(session, application)
        if application is not _UNSET
        else get_application(session, row.application_id)
    )
    contact_row = (
        get_contact(session, contact)
        if contact is not _UNSET and contact is not None
        else (
            None
            if contact is None
            else (
                get_contact(session, row.contact_id)
                if row.contact_id is not None
                else None
            )
        )
    )
    start_value = row.starts_at if starts_at is _UNSET else starts_at
    end_value = row.ends_at if ends_at is _UNSET else ends_at
    start, end = _interview_times(start_value, end_value)
    new_type = (
        row.interview_type
        if interview_type is _UNSET
        else _optional_text(interview_type, field="interview type", max_length=100)
    )
    new_location = (
        row.location_or_link
        if location_or_link is _UNSET
        else _optional_text(
            location_or_link, field="interview location", max_length=2_000
        )
    )
    new_notes = (
        row.notes
        if notes is _UNSET
        else _optional_text(notes, field="interview notes", collapse=False)
    )
    event_id = (
        row.calendar_event_id
        if calendar_event_id is _UNSET
        else _optional_text(
            calendar_event_id, field="calendar event ID", max_length=500
        )
    )
    if event_id is not None:
        duplicate = session.scalar(
            select(Interview.id).where(
                Interview.calendar_event_id == event_id,
                Interview.id != row.id,
            )
        )
        if duplicate is not None:
            raise ValueError("calendar event ID is already in use")

    row.application_id = application_row.id
    row.starts_at = start
    row.ends_at = end
    row.contact_id = contact_row.id if contact_row is not None else None
    row.interview_type = new_type
    row.location_or_link = new_location
    row.notes = new_notes
    row.calendar_event_id = event_id
    _record_update(
        session,
        action="interview.updated",
        entity_type="interview",
        entity_id=row.id,
        actor=actor,
        before=before,
        after=_interview_state(row),
    )
    return row


def _interview_times(
    starts_at: datetime, ends_at: datetime | None
) -> tuple[datetime, datetime | None]:
    start = _as_utc(starts_at, field="starts_at")
    end = _as_utc(ends_at, field="ends_at") if ends_at is not None else None
    if end is not None and end <= start:
        raise ValueError("interview end must be after its start")
    return start, end


def _interview_state(row: Interview) -> dict[str, Any]:
    return {
        "application_id": row.application_id,
        "starts_at": row.starts_at,
        "ends_at": row.ends_at,
        "interview_type": row.interview_type,
        "location_or_link": row.location_or_link,
        "contact_id": row.contact_id,
        "notes": row.notes,
        "calendar_event_id": row.calendar_event_id,
    }


def delete_interview(
    session: Session,
    interview: Interview | str,
    *,
    confirm: bool = False,
    actor: str = "user",
) -> None:
    row = get_interview(session, interview)
    if not confirm:
        raise PermissionError("interview deletion requires explicit confirmation")
    record_audit(
        session,
        action="interview.deleted",
        entity_type="interview",
        entity_id=row.id,
        actor=actor,
        before=_interview_state(row),
    )
    session.delete(row)
    session.flush()


def list_application_tasks(session: Session, application_id: str) -> tuple[Task, ...]:
    get_application(session, application_id)
    return tuple(
        session.scalars(
            select(Task)
            .where(Task.application_id == application_id)
            .order_by(Task.due_at, Task.created_at, Task.id)
        )
    )


def list_application_contacts(
    session: Session, application_id: str
) -> tuple[tuple[ApplicationContact, Contact], ...]:
    get_application(session, application_id)
    rows = session.execute(
        select(ApplicationContact, Contact)
        .join(Contact, Contact.id == ApplicationContact.contact_id)
        .where(ApplicationContact.application_id == application_id)
        .order_by(Contact.name, Contact.id)
    )
    return tuple((link, contact) for link, contact in rows)


def list_application_interviews(
    session: Session, application_id: str
) -> tuple[Interview, ...]:
    get_application(session, application_id)
    return tuple(
        session.scalars(
            select(Interview)
            .where(Interview.application_id == application_id)
            .order_by(Interview.starts_at, Interview.id)
        )
    )


def _require_company(session: Session, company_id: str | None) -> None:
    if company_id is not None and session.get(Company, company_id) is None:
        raise LookupError("company not found")


def _normalize_email(value: str | None) -> str | None:
    normalized = _optional_text(value, field="contact email", max_length=500)
    if normalized is not None and (
        "@" not in normalized or normalized.startswith("@") or normalized.endswith("@")
    ):
        raise ValueError("contact email is invalid")
    return normalized


def _required_text(value: Any, *, field: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    if len(normalized) > max_length:
        raise ValueError(f"{field} must be {max_length} characters or fewer")
    return normalized


def _optional_text(
    value: Any,
    *,
    field: str,
    max_length: int | None = None,
    collapse: bool = True,
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    normalized = re.sub(r"\s+", " ", value).strip() if collapse else value.strip()
    if not normalized:
        return None
    if max_length is not None and len(normalized) > max_length:
        raise ValueError(f"{field} must be {max_length} characters or fewer")
    return normalized


def _optional_datetime(value: Any, *, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    return _as_utc(value, field=field)


def _as_utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _record_update(
    session: Session,
    *,
    action: str,
    entity_type: str,
    entity_id: str,
    actor: str,
    before: dict[str, Any],
    after: dict[str, Any],
) -> None:
    if before == after:
        return
    record_audit(
        session,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        actor=actor,
        before=before,
        after=after,
    )


__all__ = [
    "DEFAULT_FOLLOW_UP_DAYS",
    "FOLLOW_UP_AUTOMATION_PREFIX",
    "associate_application_contact",
    "create_and_associate_contact",
    "create_contact",
    "create_interview",
    "create_task",
    "delete_application",
    "delete_contact",
    "delete_interview",
    "delete_task",
    "ensure_application_follow_up_task",
    "follow_up_automation_key",
    "get_application",
    "get_contact",
    "get_interview",
    "get_task",
    "list_application_contacts",
    "list_application_interviews",
    "list_application_tasks",
    "normalize_application_create_fields",
    "remove_application_contact",
    "route_new_application_job",
    "snapshot_application_provenance",
    "update_application",
    "update_application_contact",
    "update_contact",
    "update_interview",
    "update_task",
    "validate_application_follow_up_request",
]
