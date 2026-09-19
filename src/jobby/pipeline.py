"""Application lifecycle, contacts, interviews, and approval-gated suggestions."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import math
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import select
from sqlalchemy.orm import Session

from .application_workspace import (
    DEFAULT_FOLLOW_UP_DAYS,
    create_interview as create_workspace_interview,
    ensure_application_follow_up_task,
    normalize_application_create_fields,
    route_new_application_job,
    snapshot_application_provenance,
    update_application,
    validate_application_follow_up_request,
)
from .audit import record_audit
from .enums import ApplicationStage, ApprovalState, DocumentStatus, SuggestionKind
from .models import (
    Application,
    ApplicationMaterial,
    DocumentVersion,
    ExternalSuggestion,
    Interview,
    Offer,
    StageEvent,
)


ALLOWED_TRANSITIONS: dict[ApplicationStage, set[ApplicationStage]] = {
    ApplicationStage.PLANNED: {
        ApplicationStage.APPLIED,
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
    },
    ApplicationStage.APPLIED: {
        ApplicationStage.SCREENING,
        ApplicationStage.INTERVIEW,
        ApplicationStage.ASSESSMENT,
        ApplicationStage.OFFER,
        ApplicationStage.REJECTED,
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
    },
    ApplicationStage.SCREENING: {
        ApplicationStage.INTERVIEW,
        ApplicationStage.ASSESSMENT,
        ApplicationStage.OFFER,
        ApplicationStage.REJECTED,
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
    },
    ApplicationStage.INTERVIEW: {
        ApplicationStage.INTERVIEW,
        ApplicationStage.ASSESSMENT,
        ApplicationStage.OFFER,
        ApplicationStage.REJECTED,
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
    },
    ApplicationStage.ASSESSMENT: {
        ApplicationStage.INTERVIEW,
        ApplicationStage.OFFER,
        ApplicationStage.REJECTED,
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
    },
    ApplicationStage.OFFER: {
        ApplicationStage.WITHDRAWN,
        ApplicationStage.ARCHIVED,
        ApplicationStage.REJECTED,
    },
    ApplicationStage.REJECTED: {ApplicationStage.ARCHIVED},
    ApplicationStage.WITHDRAWN: {ApplicationStage.ARCHIVED},
    ApplicationStage.ARCHIVED: set(),
}


SUGGESTION_TO_STAGE: dict[SuggestionKind, ApplicationStage] = {
    SuggestionKind.APPLIED: ApplicationStage.APPLIED,
    SuggestionKind.INTERVIEW_REQUESTED: ApplicationStage.INTERVIEW,
    SuggestionKind.REJECTED: ApplicationStage.REJECTED,
    SuggestionKind.POSITION_CLOSED: ApplicationStage.REJECTED,
}
MAX_OFFER_TERMS_BYTES = 1_000_000


def _require_application(
    session: Session, application: Application | str
) -> Application:
    if not isinstance(application, str):
        return application
    resolved = session.get(Application, application)
    if resolved is None:
        raise LookupError("application not found")
    return resolved


def _require_document_version(
    session: Session, document_version: DocumentVersion | str
) -> DocumentVersion:
    if not isinstance(document_version, str):
        return document_version
    resolved = session.get(DocumentVersion, document_version)
    if resolved is None:
        raise LookupError("document version not found")
    return resolved


def _require_external_suggestion(
    session: Session, suggestion: ExternalSuggestion | str
) -> ExternalSuggestion:
    if not isinstance(suggestion, str):
        return suggestion
    resolved = session.get(ExternalSuggestion, suggestion)
    if resolved is None:
        raise LookupError("suggestion not found")
    return resolved


def _require_offer(session: Session, offer: Offer | str) -> Offer:
    if not isinstance(offer, str):
        return offer
    resolved = session.get(Offer, offer)
    if resolved is None:
        raise LookupError("offer not found")
    return resolved


def create_application(
    session: Session,
    job_id: str,
    *,
    submission_channel: str | None = None,
    notes: str | None = None,
    actor: str = "user",
    occurred_at: datetime | None = None,
) -> Application:
    normalized_channel, normalized_notes = normalize_application_create_fields(
        submission_channel=submission_channel,
        notes=notes,
    )
    if occurred_at is not None and not isinstance(occurred_at, datetime):
        raise ValueError("occurred_at must be a datetime")
    created_at = _as_utc(occurred_at or datetime.now(timezone.utc))
    requested_job_id = job_id
    job_id = route_new_application_job(session, job_id)
    application = Application(
        job_id=job_id,
        current_stage=ApplicationStage.PLANNED,
        submission_channel=normalized_channel,
        notes=normalized_notes,
    )
    session.add(application)
    session.flush()
    session.add(
        StageEvent(
            application_id=application.id,
            from_stage=None,
            to_stage=ApplicationStage.PLANNED,
            occurred_at=created_at,
            actor=actor,
            source="manual",
            reason="Application created",
        )
    )
    record_audit(
        session,
        action="application.created",
        entity_type="application",
        entity_id=application.id,
        actor=actor,
        after={
            "job_id": job_id,
            "requested_job_id": requested_job_id,
            "stage": ApplicationStage.PLANNED,
        },
    )
    return application


def transition_application(
    session: Session,
    application: Application | str,
    to_stage: ApplicationStage | str,
    *,
    reason: str | None = None,
    actor: str = "user",
    source: str = "manual",
    explicit_approval: bool = False,
    occurred_at: datetime | None = None,
    create_follow_up_task: bool = True,
    follow_up_days: int = DEFAULT_FOLLOW_UP_DAYS,
    follow_up_due_at: datetime | None = None,
) -> StageEvent:
    resolved_application = _require_application(session, application)
    to_stage = ApplicationStage(to_stage)
    from_stage = ApplicationStage(resolved_application.current_stage)
    if source != "manual" and not explicit_approval:
        raise PermissionError("external suggestions require explicit approval")
    if to_stage not in ALLOWED_TRANSITIONS[from_stage]:
        raise ValueError(
            f"invalid application transition: {from_stage.value} -> {to_stage.value}"
        )
    if occurred_at is not None and not isinstance(occurred_at, datetime):
        raise ValueError("occurred_at must be a datetime")
    occurred_at = _as_utc(occurred_at or datetime.now(timezone.utc))
    if to_stage == ApplicationStage.APPLIED:
        if not isinstance(create_follow_up_task, bool):
            raise ValueError("create_follow_up_task must be true or false")
        validate_application_follow_up_request(
            session,
            resolved_application,
            occurred_at=occurred_at,
            due_at=follow_up_due_at,
            follow_up_days=follow_up_days,
        )
    latest = session.scalar(
        select(StageEvent.occurred_at)
        .where(StageEvent.application_id == resolved_application.id)
        .order_by(StageEvent.occurred_at.desc())
        .limit(1)
    )
    if latest is not None:
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)
        if occurred_at < latest:
            raise ValueError("application transition cannot predate existing history")
    event = StageEvent(
        application_id=resolved_application.id,
        from_stage=from_stage,
        to_stage=to_stage,
        occurred_at=occurred_at,
        reason=reason,
        actor=actor,
        source=source,
    )
    session.add(event)
    resolved_application.current_stage = to_stage
    if to_stage == ApplicationStage.APPLIED:
        snapshot_application_provenance(session, resolved_application, actor=actor)
        if resolved_application.submitted_at is None:
            resolved_application.submitted_at = event.occurred_at
        if create_follow_up_task:
            ensure_application_follow_up_task(
                session,
                resolved_application,
                occurred_at=event.occurred_at,
                due_at=follow_up_due_at,
                follow_up_days=follow_up_days,
                actor=actor,
            )
    record_audit(
        session,
        action="application.stage_changed",
        entity_type="application",
        entity_id=resolved_application.id,
        actor=actor,
        before={"stage": from_stage},
        after={"stage": to_stage},
        detail=reason,
    )
    return event


def application_history(session: Session, application_id: str) -> list[StageEvent]:
    return list(
        session.scalars(
            select(StageEvent)
            .where(StageEvent.application_id == application_id)
            .order_by(StageEvent.occurred_at, StageEvent.id)
        )
    )


def attach_application_material(
    session: Session,
    application: Application | str,
    document_version: DocumentVersion | str,
    *,
    purpose: str,
    used_at: datetime | None = None,
    actor: str = "user",
) -> ApplicationMaterial:
    """Record exactly which approved material was used without duplicating links."""
    resolved_application = _require_application(session, application)
    resolved_document = _require_document_version(session, document_version)
    if (
        resolved_document.approval_state != ApprovalState.APPROVED
        or resolved_document.status
        not in {DocumentStatus.APPROVED, DocumentStatus.READY}
    ):
        raise PermissionError(
            "application materials must be approved document versions in approved or ready status"
        )
    purpose = re.sub(r"\s+", " ", purpose).strip().casefold()[:100]
    if not purpose:
        raise ValueError("material purpose must not be blank")
    if used_at is not None and not isinstance(used_at, datetime):
        raise ValueError("material used_at must be a datetime")
    normalized_used_at = _as_utc(used_at or datetime.now(timezone.utc))
    existing = session.scalar(
        select(ApplicationMaterial).where(
            ApplicationMaterial.application_id == resolved_application.id,
            ApplicationMaterial.document_version_id == resolved_document.id,
            ApplicationMaterial.purpose == purpose,
        )
    )
    if existing is not None:
        return existing
    material = ApplicationMaterial(
        application_id=resolved_application.id,
        document_version_id=resolved_document.id,
        purpose=purpose,
        used_at=normalized_used_at,
    )
    session.add(material)
    session.flush()
    record_audit(
        session,
        action="application.material_attached",
        entity_type="application_material",
        entity_id=material.id,
        actor=actor,
        after={
            "application_id": resolved_application.id,
            "document_version_id": resolved_document.id,
            "purpose": purpose,
            "used_at": material.used_at,
        },
    )
    return material


def apply_external_suggestion(
    session: Session,
    suggestion: ExternalSuggestion | str,
    *,
    approved: bool,
    actor: str = "user",
) -> StageEvent | Interview | None:
    """Review a suggestion and apply only its explicit, local effect.

    Google integrations are read-only. Approval can update Jobby's local database,
    but this function never calls an external provider. Suggestions that cannot be
    applied deterministically remain pending and raise a validation error.
    """
    resolved_suggestion = _require_external_suggestion(session, suggestion)
    if resolved_suggestion.approval_state != ApprovalState.PENDING:
        raise ValueError("suggestion has already been reviewed")
    if not approved:
        resolved_suggestion.approval_state = ApprovalState.REJECTED
        record_audit(
            session,
            action="suggestion.rejected",
            entity_type="external_suggestion",
            entity_id=resolved_suggestion.id,
            actor=actor,
        )
        return None
    if resolved_suggestion.application_id is None:
        raise ValueError("suggestion is not linked to an application")
    application = session.get(Application, resolved_suggestion.application_id)
    if application is None:
        raise LookupError("linked application not found")

    kind = SuggestionKind(resolved_suggestion.kind)
    if kind == SuggestionKind.CALENDAR_INTERVIEW:
        calendar_cancelled = bool(
            _suggestion_payload(resolved_suggestion).get("cancelled")
        )
        interview = _apply_calendar_interview(
            session, resolved_suggestion, application, actor
        )
        effect: dict[str, Any] = {
            "interview_id": interview.id,
            "external_event_id": interview.calendar_event_id,
        }
        if calendar_cancelled:
            effect["cancelled"] = True
        _mark_suggestion_approved(
            session,
            resolved_suggestion,
            actor=actor,
            effect=effect,
        )
        return interview
    if kind == SuggestionKind.FOLLOW_UP_NEEDED:
        follow_up_at = _apply_follow_up(
            session, resolved_suggestion, application, actor
        )
        _mark_suggestion_approved(
            session,
            resolved_suggestion,
            actor=actor,
            effect={"follow_up_at": follow_up_at},
        )
        return None

    target = SUGGESTION_TO_STAGE.get(kind)
    if target is None:
        raise ValueError(f"unsupported suggestion kind: {kind.value}")
    event = transition_application(
        session,
        application,
        target,
        reason=f"Approved {resolved_suggestion.kind.value} suggestion",
        actor=actor,
        source="approved_external_suggestion",
        explicit_approval=True,
    )
    _mark_suggestion_approved(
        session,
        resolved_suggestion,
        actor=actor,
        effect={"stage": target},
    )
    return event


def _mark_suggestion_approved(
    session: Session,
    suggestion: ExternalSuggestion,
    *,
    actor: str,
    effect: Mapping[str, Any],
) -> None:
    suggestion.approval_state = ApprovalState.APPROVED
    suggestion.applied_at = datetime.now(timezone.utc)
    record_audit(
        session,
        action="suggestion.approved",
        entity_type="external_suggestion",
        entity_id=suggestion.id,
        actor=actor,
        after={
            "suggestion_id": suggestion.id,
            "kind": suggestion.kind,
            "application_id": suggestion.application_id,
            **effect,
        },
    )


def _apply_calendar_interview(
    session: Session,
    suggestion: ExternalSuggestion,
    application: Application,
    actor: str,
) -> Interview:
    payload = _suggestion_payload(suggestion)
    external_event_id = suggestion.external_event_id
    if not isinstance(external_event_id, str) or not external_event_id.strip():
        raise ValueError("calendar interview requires an external event ID")
    external_event_id = external_event_id.strip()
    if len(external_event_id) > 500:
        raise ValueError("calendar interview event ID is too long")

    existing = session.scalar(
        select(Interview).where(Interview.calendar_event_id == external_event_id)
    )
    if payload.get("cancelled") is True:
        if existing is None:
            raise ValueError("cancelled calendar event has no linked local interview")
        if existing.application_id != application.id:
            raise ValueError(
                "calendar event is linked to a different local application"
            )
        before = {
            "application_id": existing.application_id,
            "starts_at": existing.starts_at,
            "ends_at": existing.ends_at,
            "interview_type": existing.interview_type,
            "location_or_link": existing.location_or_link,
            "calendar_event_id": existing.calendar_event_id,
        }
        record_audit(
            session,
            action="interview.calendar_cancellation_applied",
            entity_type="interview",
            entity_id=existing.id,
            actor=actor,
            before=before,
            after={"cancelled": True, "external_event_id": external_event_id},
            detail="Approved read-only Calendar cancellation preview",
        )
        session.delete(existing)
        return existing

    starts_at = _parse_calendar_event_datetime(payload.get("start"), field="start")
    raw_end = payload.get("end")
    ends_at = (
        _parse_calendar_event_datetime(raw_end, field="end")
        if raw_end is not None
        else None
    )
    if ends_at is not None and ends_at <= starts_at:
        raise ValueError("calendar interview end must be after its start")

    summary = payload.get("summary")
    if summary is not None and not isinstance(summary, str):
        raise ValueError("calendar interview summary must be text")
    interview_type = re.sub(r"\s+", " ", summary or "Calendar interview").strip()
    interview_type = interview_type[:100] or "Calendar interview"
    location = payload.get("location")
    if location is not None and not isinstance(location, str):
        raise ValueError("calendar interview location must be text")
    location = location.strip() if location else None

    if existing is not None:
        existing_start = _as_utc(existing.starts_at)
        existing_end = _as_utc(existing.ends_at) if existing.ends_at else None
        if existing.application_id != application.id:
            raise ValueError(
                "calendar event is linked to a different local application"
            )
        before = {
            "starts_at": existing_start,
            "ends_at": existing_end,
            "interview_type": existing.interview_type,
            "location_or_link": existing.location_or_link,
        }
        changed = (
            existing_start != starts_at
            or existing_end != ends_at
            or existing.interview_type != interview_type
            or existing.location_or_link != location
        )
        if changed:
            existing.starts_at = starts_at
            existing.ends_at = ends_at
            existing.interview_type = interview_type
            existing.location_or_link = location
            record_audit(
                session,
                action="interview.rescheduled_from_calendar",
                entity_type="interview",
                entity_id=existing.id,
                actor=actor,
                before=before,
                after={
                    "starts_at": starts_at,
                    "ends_at": ends_at,
                    "interview_type": interview_type,
                    "location_or_link": location,
                },
                detail="Approved read-only Calendar revision preview",
            )
        return existing

    return create_interview(
        session,
        application.id,
        starts_at,
        ends_at=ends_at,
        interview_type=interview_type,
        location_or_link=location,
        notes="Created from an explicitly approved read-only Calendar suggestion.",
        calendar_event_id=external_event_id,
        actor=actor,
    )


def _apply_follow_up(
    session: Session,
    suggestion: ExternalSuggestion,
    application: Application,
    actor: str,
) -> datetime:
    payload = _suggestion_payload(suggestion)
    raw_follow_up = payload.get("follow_up_at")
    if not isinstance(raw_follow_up, str) or not raw_follow_up.strip():
        raise ValueError(
            "follow-up suggestion requires an explicit follow_up_at timestamp; "
            "Jobby will not invent a deadline"
        )
    follow_up_at = _parse_iso_datetime(raw_follow_up, field="follow_up_at")
    before = application.follow_up_at
    if before != follow_up_at:
        update_application(
            session,
            application,
            follow_up_at=follow_up_at,
            actor=actor,
        )
        record_audit(
            session,
            action="application.follow_up_scheduled",
            entity_type="application",
            entity_id=application.id,
            actor=actor,
            before={"follow_up_at": before},
            after={"follow_up_at": follow_up_at},
            detail="Approved external follow-up suggestion",
        )
    return follow_up_at


def _suggestion_payload(suggestion: ExternalSuggestion) -> Mapping[str, Any]:
    if not isinstance(suggestion.payload, Mapping):
        raise ValueError("suggestion payload must be an object")
    return suggestion.payload


def _parse_calendar_event_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, Mapping):
        raise ValueError(f"calendar interview {field} must be an event-time object")
    raw = value.get("dateTime")
    if not isinstance(raw, str) or not raw.strip():
        if value.get("date"):
            raise ValueError(
                f"calendar interview {field} must include a time, not an all-day date"
            )
        raise ValueError(f"calendar interview {field} is missing dateTime")
    time_zone = value.get("timeZone")
    if time_zone is not None and not isinstance(time_zone, str):
        raise ValueError(f"calendar interview {field} timeZone must be text")
    return _parse_iso_datetime(raw, field=field, time_zone=time_zone)


def _parse_iso_datetime(
    value: str,
    *,
    field: str,
    time_zone: str | None = None,
) -> datetime:
    normalized = value.strip()
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        if not time_zone:
            raise ValueError(f"{field} timestamp must include a timezone")
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(time_zone))
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"{field} contains an unknown timezone") from exc
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def create_interview(
    session: Session,
    application_id: str,
    starts_at: datetime,
    *,
    ends_at: datetime | None = None,
    interview_type: str | None = None,
    location_or_link: str | None = None,
    contact_id: str | None = None,
    notes: str | None = None,
    calendar_event_id: str | None = None,
    actor: str = "user",
) -> Interview:
    return create_workspace_interview(
        session,
        application_id,
        starts_at,
        ends_at=ends_at,
        interview_type=interview_type,
        location_or_link=location_or_link,
        contact=contact_id,
        notes=notes,
        calendar_event_id=calendar_event_id,
        actor=actor,
    )


def offer_comparison(offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize user-entered offers into a transparent, sortable comparison."""
    result: list[dict[str, Any]] = []
    for offer in offers:
        if not isinstance(offer, Mapping):
            raise ValueError("each offer must be an object")
        base = _offer_number(offer.get("base_salary"), default=0, name="base salary")
        bonus = _offer_number(offer.get("annual_bonus"), default=0, name="annual bonus")
        equity = _offer_number(
            offer.get("annualized_equity"), default=0, name="annualized equity"
        )
        if any(value < 0 for value in (base, bonus, equity)):
            raise ValueError(
                "offer compensation values must be finite and non-negative"
            )
        col_index = _offer_number(
            offer.get("cost_of_living_index"),
            default=100,
            name="cost-of-living index",
        )
        if col_index <= 0:
            raise ValueError("cost-of-living index must be finite and positive")
        stress = _offer_number(
            offer.get("stress_score"), default=3, name="stress score"
        )
        if not 1 <= stress <= 5:
            raise ValueError("stress score must be finite and between 1 and 5")
        total = base + bonus + equity
        adjusted = total * 100 / col_index
        if not math.isfinite(total) or not math.isfinite(adjusted):
            raise ValueError("offer derived compensation must be finite")
        result.append(
            {
                **offer,
                "total_compensation": round(total, 2),
                "col_adjusted_compensation": round(adjusted, 2),
                "stress_adjusted_value": round(adjusted * (6 - stress) / 5, 2),
            }
        )
    return sorted(result, key=lambda row: row["stress_adjusted_value"], reverse=True)


def _offer_number(value: Any, *, default: float, name: str) -> float:
    if value is None or value == "":
        return float(default)
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be a finite number")
    return parsed


def create_offer(
    session: Session,
    application: Application | str,
    *,
    base_salary: float = 0,
    annual_bonus: float = 0,
    annualized_equity: float = 0,
    currency: str = "USD",
    cost_of_living_index: float = 100,
    stress_score: float = 3,
    terms: dict[str, Any] | None = None,
    decision: str | None = None,
    offered_at: datetime | None = None,
    actor: str = "user",
) -> Offer:
    resolved_application = _require_application(session, application)
    amounts = {
        "base_salary": _offer_number(base_salary, default=0, name="base salary"),
        "annual_bonus": _offer_number(annual_bonus, default=0, name="annual bonus"),
        "annualized_equity": _offer_number(
            annualized_equity, default=0, name="annualized equity"
        ),
    }
    if any(not math.isfinite(value) or value < 0 for value in amounts.values()):
        raise ValueError("offer compensation values must be finite and non-negative")
    cost_of_living_index = _offer_number(
        cost_of_living_index, default=100, name="cost-of-living index"
    )
    stress_score = _offer_number(stress_score, default=3, name="stress score")
    if not math.isfinite(cost_of_living_index) or cost_of_living_index <= 0:
        raise ValueError("cost-of-living index must be finite and positive")
    if not math.isfinite(stress_score) or not 1 <= stress_score <= 5:
        raise ValueError("stress score must be finite and between 1 and 5")
    total = sum(amounts.values())
    if not math.isfinite(total) or not math.isfinite(
        total * 100 / cost_of_living_index
    ):
        raise ValueError("offer derived compensation must be finite")
    if not isinstance(currency, str):
        raise ValueError("currency must be a three-letter code")
    currency = currency.strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise ValueError("currency must be a three-letter code")
    decision = _offer_decision(decision)
    if offered_at is not None and not isinstance(offered_at, datetime):
        raise ValueError("offered_at must be a datetime")
    normalized_terms = _offer_terms(terms)
    offer = Offer(
        application_id=resolved_application.id,
        **amounts,
        currency=currency,
        cost_of_living_index=cost_of_living_index,
        stress_score=stress_score,
        terms=normalized_terms,
        decision=decision,
        offered_at=_as_utc(offered_at or datetime.now(timezone.utc)),
    )
    session.add(offer)
    session.flush()
    record_audit(
        session,
        action="offer.created",
        entity_type="offer",
        entity_id=offer.id,
        actor=actor,
        after={
            "application_id": resolved_application.id,
            **amounts,
            "currency": currency,
            "cost_of_living_index": cost_of_living_index,
            "stress_score": stress_score,
            "decision": decision,
        },
    )
    return offer


def set_offer_decision(
    session: Session,
    offer: Offer | str,
    decision: str | None,
    *,
    actor: str = "user",
) -> Offer:
    resolved_offer = _require_offer(session, offer)
    normalized = _offer_decision(decision)
    before = resolved_offer.decision
    if normalized == before:
        return resolved_offer
    resolved_offer.decision = normalized
    record_audit(
        session,
        action="offer.decision_changed",
        entity_type="offer",
        entity_id=resolved_offer.id,
        actor=actor,
        before={"decision": before},
        after={"decision": normalized},
    )
    return resolved_offer


def _offer_decision(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("offer decision must be text")
    normalized = re.sub(r"\s+", " ", value).strip().casefold()
    if not normalized:
        return None
    if len(normalized) > 100:
        raise ValueError("offer decision must be 100 characters or fewer")
    return normalized


def _offer_terms(value: dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("offer terms must be an object")
    try:
        normalized = dict(value)
        serialized = json.dumps(
            normalized,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "offer terms must contain finite JSON-compatible data"
        ) from exc
    if len(serialized.encode("utf-8")) > MAX_OFFER_TERMS_BYTES:
        raise ValueError(f"offer terms exceed the {MAX_OFFER_TERMS_BYTES:,}-byte limit")
    return normalized


def compare_persisted_offers(
    session: Session,
    *,
    application_id: str | None = None,
) -> list[dict[str, Any]]:
    statement = select(Offer)
    if application_id is not None:
        statement = statement.where(Offer.application_id == application_id)
    offers = list(session.scalars(statement.order_by(Offer.offered_at, Offer.id)))
    return offer_comparison(
        [
            {
                "offer_id": offer.id,
                "application_id": offer.application_id,
                "base_salary": offer.base_salary,
                "annual_bonus": offer.annual_bonus,
                "annualized_equity": offer.annualized_equity,
                "currency": offer.currency,
                "cost_of_living_index": offer.cost_of_living_index,
                "stress_score": offer.stress_score,
                "terms": dict(offer.terms or {}),
                "decision": offer.decision,
                "offered_at": offer.offered_at,
            }
            for offer in offers
        ]
    )
