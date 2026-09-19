from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.enums import JobStatus
from jobby.models import (
    CanonicalJobGroup,
    CanonicalJobMember,
    Company,
    Interview,
    Job,
    Task,
)
from jobby.pipeline import create_application
from jobby.planner import allocation_targets, build_daily_plan


def make_database(tmp_path: Path) -> Database:
    root = tmp_path / "jobby"
    paths = JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    )
    database = Database(paths=paths)
    database.initialize()
    return database


@pytest.mark.parametrize("value", [True, False, 10.0, "10"])
def test_allocation_target_requires_an_actual_integer(value: object) -> None:
    with pytest.raises(TypeError, match="must be an integer"):
        allocation_targets(value)  # type: ignore[arg-type]


def test_daily_plan_excludes_stale_and_past_deadline_roles(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    now = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        session.add_all(
            [
                Job(
                    company_id=company.id,
                    title="Current Legal AI Role",
                    normalized_title="current legal ai role",
                    status=JobStatus.DISCOVERED,
                    deadline=date(2026, 7, 14),
                    latest_score=3.5,
                ),
                Job(
                    company_id=company.id,
                    title="Expired Legal AI Role",
                    normalized_title="expired legal ai role",
                    status=JobStatus.DISCOVERED,
                    deadline=now.date() - timedelta(days=1),
                    latest_score=5.0,
                ),
                Job(
                    company_id=company.id,
                    title="Stale Legal AI Role",
                    normalized_title="stale legal ai role",
                    status=JobStatus.STALE,
                    latest_score=5.0,
                ),
            ]
        )

    with database.session() as session:
        plan = build_daily_plan(session, now=now)

    application_titles = [
        item.title for item in plan.items if item.kind == "application"
    ]
    assert application_titles == ["Apply: Current Legal AI Role"]
    database.dispose()


def test_deadline_remains_actionable_through_end_of_configured_local_day(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    deadline = date(2026, 7, 12)
    with database.session() as session:
        company = Company(name="Pacific Co", normalized_name="pacific co")
        session.add(company)
        session.flush()
        session.add(
            Job(
                company_id=company.id,
                title="Pacific Deadline Role",
                normalized_title="pacific deadline role",
                status=JobStatus.DISCOVERED,
                deadline=deadline,
                latest_score=4.0,
            )
        )

    # 00:30 UTC is still 17:30 on July 12 in Los Angeles.
    with database.session() as session:
        plan = build_daily_plan(
            session,
            now=datetime(2026, 7, 13, 0, 30, tzinfo=timezone.utc),
            timezone_name="America/Los_Angeles",
        )
    item = next(item for item in plan.items if item.kind == "application")
    assert item.due_at is not None
    assert item.due_at.date() == deadline
    assert item.due_at.astimezone(timezone.utc) == datetime(
        2026, 7, 13, 6, 59, 59, 999999, tzinfo=timezone.utc
    )

    with database.session() as session:
        expired = build_daily_plan(
            session,
            now=datetime(2026, 7, 13, 7, 0, tzinfo=timezone.utc),
            timezone_name="America/Los_Angeles",
        )
    assert not [item for item in expired.items if item.kind == "application"]
    database.dispose()


def test_plan_items_preserve_exact_entity_ids_when_visible_fields_collide(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    with database.session() as session:
        company = Company(name="Same Fields", normalized_name="same fields")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Policy Counsel",
            normalized_title="policy counsel",
            status=JobStatus.DISCOVERED,
        )
        session.add(job)
        session.flush()
        application = create_application(session, job.id, occurred_at=now)
        tasks = [
            Task(
                title="Prepare packet",
                due_at=now + timedelta(days=1),
                application_id=application.id,
            )
            for _ in range(2)
        ]
        interviews = [
            Interview(
                application_id=application.id,
                starts_at=now + timedelta(days=2),
                interview_type="Panel",
            )
            for _ in range(2)
        ]
        session.add_all([*tasks, *interviews])
        session.flush()
        expected_task_ids = {task.id for task in tasks}
        expected_interview_ids = {interview.id for interview in interviews}

    with database.session() as session:
        plan = build_daily_plan(session, now=now)

    assert {
        item.entity_id for item in plan.items if item.entity_type == "task"
    } == expected_task_ids
    assert {
        item.entity_id for item in plan.items if item.entity_type == "interview"
    } == expected_interview_ids
    database.dispose()


def test_daily_plan_does_not_recommend_hidden_or_already_applied_group_members(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
    with database.session() as session:
        company = Company(name="Grouped Co", normalized_name="grouped co")
        session.add(company)
        session.flush()
        hidden = Job(
            company_id=company.id,
            title="Policy Counsel old",
            normalized_title="policy counsel",
            status=JobStatus.DISCOVERED,
            latest_score=4.8,
        )
        canonical = Job(
            company_id=company.id,
            title="Policy Counsel",
            normalized_title="policy counsel",
            status=JobStatus.DISCOVERED,
            latest_score=4.9,
        )
        session.add_all([hidden, canonical])
        session.flush()
        create_application(session, hidden.id, occurred_at=now)
        group = CanonicalJobGroup(canonical_job_id=canonical.id)
        session.add(group)
        session.flush()
        session.add_all(
            [
                CanonicalJobMember(
                    group_id=group.id,
                    job_id=hidden.id,
                    is_canonical=False,
                    hidden_by_default=True,
                ),
                CanonicalJobMember(
                    group_id=group.id,
                    job_id=canonical.id,
                    is_canonical=True,
                    hidden_by_default=False,
                ),
            ]
        )

    with database.session() as session:
        plan = build_daily_plan(session, now=now)

    assert not [item for item in plan.items if item.kind == "application"]
    database.dispose()
