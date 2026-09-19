"""Explainable daily plan and weekly 60/20/15/5 allocation."""

from __future__ import annotations

import math
import re
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from .enums import JobStatus, TaskStatus
from .models import Application, CanonicalJobMember, Interview, Job, Task


ALLOCATION = {
    "legal_ai": 0.60,
    "ai_ip_policy": 0.20,
    "federal": 0.15,
    "lottery_ticket": 0.05,
}


class PlanItem(BaseModel):
    kind: str
    entity_type: str
    entity_id: str
    title: str
    reason: str
    priority: float = Field(ge=0)
    job_id: str | None = None
    application_id: str | None = None
    due_at: datetime | None = None


class DailyPlan(BaseModel):
    generated_at: datetime
    allocation_targets: dict[str, int]
    allocation_current: dict[str, int]
    items: list[PlanItem]


def allocation_targets(total: int) -> dict[str, int]:
    if isinstance(total, bool) or not isinstance(total, int):
        raise TypeError("weekly target must be an integer")
    if total < 0:
        raise ValueError("weekly target cannot be negative")
    raw = {key: total * weight for key, weight in ALLOCATION.items()}
    result = {key: math.floor(value) for key, value in raw.items()}
    remaining = total - sum(result.values())
    # On an exact remainder tie, preserve representation for the smaller
    # strategic bucket (notably the 5% lottery-ticket allocation at target 10).
    for key in sorted(
        raw,
        key=lambda item: (raw[item] - result[item], -ALLOCATION[item]),
        reverse=True,
    )[:remaining]:
        result[key] += 1
    return result


def classify_job(job: Job) -> str:
    text = f"{job.title} {job.description or ''}".casefold()
    if re.search(
        r"\b(?:uspto|copyright office|ftc|fcc|ntia|ferc|federal|gs-\d|government)\b",
        text,
    ):
        return "federal"
    if re.search(
        r"\b(?:senior|director|principal|head of|vice president|\bvp\b)\b", text
    ):
        return "lottery_ticket"
    if re.search(
        r"\b(?:ai policy|governance|intellectual property|patent|copyright|legal specialist)\b",
        text,
    ):
        return "ai_ip_policy"
    return "legal_ai"


def build_daily_plan(
    session: Session,
    *,
    weekly_target: int = 10,
    now: datetime | None = None,
    timezone_name: str = "UTC",
) -> DailyPlan:
    now = _aware(now or datetime.now(timezone.utc))
    local_zone = ZoneInfo(timezone_name)
    local_now = now.astimezone(local_zone)
    local_week_start = (local_now - timedelta(days=local_now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start = local_week_start.astimezone(timezone.utc)
    targets = allocation_targets(weekly_target)
    current = {key: 0 for key in ALLOCATION}
    applications = list(session.scalars(select(Application)))
    jobs_by_id = {job.id: job for job in session.scalars(select(Job))}
    for application in applications:
        submitted = application.submitted_at
        if submitted and submitted.tzinfo is None:
            submitted = submitted.replace(tzinfo=timezone.utc)
        if submitted and submitted >= week_start and application.job_id in jobs_by_id:
            current[classify_job(jobs_by_id[application.job_id])] += 1

    items: list[PlanItem] = []
    for task in session.scalars(
        select(Task).where(Task.status == TaskStatus.PENDING).order_by(Task.due_at)
    ):
        overdue = bool(task.due_at and _aware(task.due_at) < now)
        priority = (
            100
            if overdue
            else 85
            if task.due_at and _aware(task.due_at) < now + timedelta(days=2)
            else 55
        )
        items.append(
            PlanItem(
                kind="task",
                entity_type="task",
                entity_id=task.id,
                title=task.title,
                reason="Overdue" if overdue else "Pending task",
                priority=priority,
                job_id=task.job_id,
                application_id=task.application_id,
                due_at=_aware(task.due_at) if task.due_at else None,
            )
        )

    for interview in session.scalars(
        select(Interview)
        .where(
            Interview.starts_at >= now, Interview.starts_at <= now + timedelta(days=7)
        )
        .order_by(Interview.starts_at)
    ):
        items.append(
            PlanItem(
                kind="interview",
                entity_type="interview",
                entity_id=interview.id,
                title=f"Prepare for {interview.interview_type or 'interview'}",
                reason="Upcoming interview",
                priority=95,
                application_id=interview.application_id,
                due_at=_aware(interview.starts_at),
            )
        )

    memberships = list(
        session.execute(select(CanonicalJobMember.group_id, CanonicalJobMember.job_id))
    )
    group_for_job = {job_id: group_id for group_id, job_id in memberships}
    members_by_group: dict[str, set[str]] = {}
    for group_id, job_id in memberships:
        members_by_group.setdefault(group_id, set()).add(job_id)
    applied_job_ids: set[str] = set()
    for application in applications:
        group_id = group_for_job.get(application.job_id)
        if group_id is None:
            applied_job_ids.add(application.job_id)
        else:
            applied_job_ids.update(members_by_group[group_id])
    hidden_member = (
        select(CanonicalJobMember.id)
        .where(
            CanonicalJobMember.job_id == Job.id,
            CanonicalJobMember.hidden_by_default.is_(True),
        )
        .correlate(Job)
        .exists()
    )
    candidates = list(
        session.scalars(
            select(Job).where(
                Job.id.not_in(applied_job_ids)
                if applied_job_ids
                else Job.id.is_not(None),
                Job.status.not_in(
                    [JobStatus.IGNORED, JobStatus.STALE, JobStatus.CLOSED]
                ),
                ~hidden_member,
            )
        )
    )
    for job in candidates:
        if job.deadline and job.deadline < local_now.date():
            continue
        category = classify_job(job)
        deficit = max(targets[category] - current[category], 0)
        deadline_bonus = 0
        if job.deadline:
            days = (job.deadline - local_now.date()).days
            deadline_bonus = 40 if days <= 2 else 25 if days <= 7 else 0
        score = float(job.latest_score or 0)
        # Keep candidate ranking within its tier: overdue work (100) and
        # interview preparation (95) must remain ahead of applications.
        priority = min(score * 10 + deficit * 8 + deadline_bonus, 90)
        if priority <= 0:
            continue
        items.append(
            PlanItem(
                kind="application",
                entity_type="job",
                entity_id=job.id,
                title=f"Apply: {job.title}",
                reason=f"{category.replace('_', ' ')} allocation deficit {deficit}; score {score:.1f}",
                priority=priority,
                job_id=job.id,
                due_at=datetime.combine(job.deadline, time.max, tzinfo=local_zone)
                if job.deadline
                else None,
            )
        )
    items.sort(
        key=lambda item: (
            -item.priority,
            item.due_at or datetime.max.replace(tzinfo=timezone.utc),
        )
    )
    return DailyPlan(
        generated_at=now,
        allocation_targets=targets,
        allocation_current=current,
        items=items[:25],
    )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


__all__ = [
    "ALLOCATION",
    "DailyPlan",
    "PlanItem",
    "allocation_targets",
    "build_daily_plan",
    "classify_job",
]
