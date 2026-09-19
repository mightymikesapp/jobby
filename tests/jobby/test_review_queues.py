from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from jobby.enums import AlertSeverity, ApplicationStage
from jobby.models import (
    Alert,
    Application,
    CanonicalJobGroup,
    CanonicalJobMember,
    Company,
    DuplicateRelationship,
    Job,
    SourceObservation,
    Task,
    Base,
)
from jobby.review_queues import (
    ReviewQueueError,
    acknowledge_alert,
    alert_fingerprint,
    comparison_identity,
    confirm_duplicate,
    create_or_recur_alert,
    dismiss_duplicate,
    duplicate_conflict_warnings,
    list_alert_inbox,
    list_group_members,
    list_hidden_group_members,
    list_review_jobs,
    promote_canonical,
    resolve_alert,
    snooze_alert,
    suggest_duplicate,
    workflow_group_job_ids,
    workflow_target_job_id,
)


NOW = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as database_session:
        yield database_session
        database_session.rollback()
    engine.dispose()


def _jobs(session: Session, count: int) -> list[Job]:
    company = Company(name="Acme", normalized_name="acme")
    session.add(company)
    session.flush()
    jobs = [
        Job(
            company_id=company.id,
            title=f"Counsel {index}",
            normalized_title=f"counsel {index}",
            source_primary="manual",
            source_id=str(index),
            comparison_url=f"https://jobs.example.test/{index}",
            description=f"Legal policy role {index}",
        )
        for index in range(count)
    ]
    session.add_all(jobs)
    session.flush()
    return jobs


def test_comparison_identity_is_order_independent_and_materially_sensitive(
    session: Session,
) -> None:
    left, right = _jobs(session, 2)

    original = comparison_identity(left, right)

    assert original == comparison_identity(right, left)
    assert len(original) == 64
    right.description = "A materially expanded legal policy role"
    assert comparison_identity(left, right) != original


def test_dismissed_pair_is_suppressed_until_its_identity_changes(
    session: Session,
) -> None:
    left, right = _jobs(session, 2)
    relationship = suggest_duplicate(
        session, left, right, rule="normalized_fields", similarity=0.91
    )
    assert relationship is not None
    relationship_id = relationship.id

    dismiss_duplicate(session, relationship, now=NOW)
    assert relationship.resolution == "dismissed"
    assert (
        suggest_duplicate(
            session, right, left, rule="normalized_fields", similarity=0.92
        )
        is None
    )
    assert session.scalar(select(func.count(DuplicateRelationship.id))) == 1

    right.normalized_title = "senior counsel"
    reopened = suggest_duplicate(
        session, left, right, rule="normalized_fields", similarity=0.88
    )

    assert reopened is relationship
    assert reopened.id == relationship_id
    assert reopened.resolution == "pending"
    assert reopened.resolved_at is None
    assert session.scalar(select(func.count(DuplicateRelationship.id))) == 1


def test_confirmation_preserves_workflow_and_source_rows_and_routes_new_work(
    session: Session,
) -> None:
    canonical, duplicate = _jobs(session, 2)
    relationship = suggest_duplicate(
        session, canonical, duplicate, rule="comparison_url"
    )
    assert relationship is not None
    application = Application(
        job_id=duplicate.id,
        current_stage=ApplicationStage.APPLIED,
    )
    observation = SourceObservation(
        job_id=duplicate.id,
        source="manual",
        source_job_id="duplicate-source-row",
        raw_payload={"preserve": True},
    )
    session.add_all([application, observation])
    session.flush()
    task = Task(
        title="Follow up",
        job_id=duplicate.id,
        application_id=application.id,
    )
    session.add(task)
    session.flush()

    group = confirm_duplicate(
        session,
        relationship,
        canonical_job_id=canonical.id,
        now=NOW,
    )

    assert relationship.resolution == "confirmed"
    assert relationship.confirmed is True
    assert relationship.canonical_group_id == group.id
    assert workflow_target_job_id(session, duplicate) == canonical.id
    assert workflow_target_job_id(session, canonical) == canonical.id
    assert workflow_group_job_ids(session, duplicate) == tuple(
        sorted((canonical.id, duplicate.id))
    )
    assert session.get(Application, application.id).job_id == duplicate.id
    assert session.get(Task, task.id).job_id == duplicate.id
    assert session.get(SourceObservation, observation.id).job_id == duplicate.id
    assert session.scalar(select(func.count(Job.id))) == 2
    assert session.scalar(select(func.count(SourceObservation.id))) == 1


def test_overlapping_confirmations_merge_groups_and_allow_promotion(
    session: Session,
) -> None:
    first, second, third, fourth = _jobs(session, 4)
    first_pair = suggest_duplicate(session, first, second, rule="title")
    second_pair = suggest_duplicate(session, third, fourth, rule="title")
    assert first_pair is not None and second_pair is not None
    first_group = confirm_duplicate(
        session, first_pair, canonical_job_id=first.id, now=NOW
    )
    second_group = confirm_duplicate(
        session, second_pair, canonical_job_id=fourth.id, now=NOW
    )
    bridge = suggest_duplicate(session, second, third, rule="description")
    assert bridge is not None

    merged = confirm_duplicate(session, bridge, canonical_job_id=fourth.id, now=NOW)

    assert merged.id == second_group.id
    assert session.get(CanonicalJobGroup, first_group.id) is None
    assert session.scalar(select(func.count(CanonicalJobGroup.id))) == 1
    assert {
        member.job_id
        for member in list_group_members(session, merged, include_hidden=True)
    } == {first.id, second.id, third.id, fourth.id}
    assert first_pair.canonical_group_id == merged.id
    promote_canonical(session, merged, second)
    assert merged.canonical_job_id == second.id
    assert workflow_target_job_id(session, fourth) == second.id


def test_group_views_hide_noncanonical_members_and_surface_application_conflicts(
    session: Session,
) -> None:
    canonical, duplicate, unrelated = _jobs(session, 3)
    relationship = suggest_duplicate(session, canonical, duplicate, rule="title")
    assert relationship is not None
    group = confirm_duplicate(
        session, relationship, canonical_job_id=canonical.id, now=NOW
    )
    applications = [
        Application(job_id=canonical.id, current_stage=ApplicationStage.APPLIED),
        Application(job_id=duplicate.id, current_stage=ApplicationStage.INTERVIEW),
    ]
    session.add_all(applications)
    session.flush()

    assert [member.job_id for member in list_group_members(session, group)] == [
        canonical.id
    ]
    assert [member.job_id for member in list_hidden_group_members(session, group)] == [
        duplicate.id
    ]
    assert {job.id for job in list_review_jobs(session)} == {
        canonical.id,
        unrelated.id,
    }
    assert {job.id for job in list_review_jobs(session, include_hidden=True)} == {
        canonical.id,
        duplicate.id,
        unrelated.id,
    }
    warnings = duplicate_conflict_warnings(session, group)
    assert len(warnings) == 1
    assert warnings[0].code == "multiple_applications"
    assert warnings[0].job_ids == tuple(sorted((canonical.id, duplicate.id)))
    assert warnings[0].application_ids == tuple(
        sorted(application.id for application in applications)
    )


def test_confirmed_pair_cannot_be_silently_dismissed(session: Session) -> None:
    left, right = _jobs(session, 2)
    relationship = suggest_duplicate(session, left, right, rule="title")
    assert relationship is not None
    confirm_duplicate(session, relationship, now=NOW)

    with pytest.raises(ReviewQueueError, match="cannot be dismantled"):
        dismiss_duplicate(session, relationship, now=NOW + timedelta(minutes=1))

    assert relationship.resolution == "confirmed"


def test_alert_fingerprint_is_normalized_and_entity_specific() -> None:
    first = alert_fingerprint(
        title="  Source   Failure ",
        entity_type="SOURCE",
        entity_id="greenhouse:acme",
    )

    assert first == alert_fingerprint(
        title="source failure",
        entity_type="source",
        entity_id="greenhouse:acme",
    )
    assert first != alert_fingerprint(
        title="source failure",
        entity_type="source",
        entity_id="lever:acme",
    )


def test_alert_recurrence_reopens_and_increments_one_fingerprinted_row(
    session: Session,
) -> None:
    alert = create_or_recur_alert(
        session,
        severity=AlertSeverity.WARNING,
        title="Source failure",
        message="First failure",
        deduplication_key="source:acme:failure",
        entity_type="source_run",
        entity_id="run-one",
        now=NOW,
    )
    alert_id = alert.id
    acknowledge_alert(session, alert, now=NOW + timedelta(minutes=1))
    snooze_alert(
        session,
        alert,
        until=NOW + timedelta(hours=2),
        now=NOW + timedelta(minutes=1),
    )
    resolve_alert(
        session,
        alert,
        reason="source recovered",
        now=NOW + timedelta(minutes=2),
    )

    recurring = create_or_recur_alert(
        session,
        severity=AlertSeverity.URGENT,
        title="Source failure escalated",
        message="Second failure",
        deduplication_key="source:acme:failure",
        entity_type="source_run",
        entity_id="run-two",
        now=NOW + timedelta(days=1),
    )

    assert recurring is alert
    assert recurring.id == alert_id
    assert recurring.recurrence_count == 2
    assert recurring.last_recurred_at == NOW + timedelta(days=1)
    assert recurring.severity == AlertSeverity.URGENT
    assert recurring.acknowledged_at is None
    assert recurring.snoozed_until is None
    assert recurring.resolved_at is None
    assert recurring.resolution_reason is None
    assert (recurring.entity_type, recurring.entity_id) == ("source_run", "run-two")
    assert session.scalar(select(func.count(Alert.id))) == 1
    assert list_alert_inbox(session, now=NOW + timedelta(days=1)) == (alert,)


def test_alert_recurrence_clears_stale_entity_links(session: Session) -> None:
    job = _jobs(session, 1)[0]
    alert = create_or_recur_alert(
        session,
        severity="warning",
        title="Source condition",
        message="Linked occurrence",
        deduplication_key="source-condition",
        job_id=job.id,
        now=NOW,
    )

    recurring = create_or_recur_alert(
        session,
        severity="info",
        title="Source condition",
        message="Workspace-wide occurrence",
        deduplication_key="source-condition",
        now=NOW + timedelta(minutes=1),
    )

    assert recurring is alert
    assert recurring.job_id is None
    assert recurring.application_id is None
    assert recurring.entity_type is None
    assert recurring.entity_id is None


def test_out_of_order_alert_occurrence_cannot_replace_newer_context(
    session: Session,
) -> None:
    alert = create_or_recur_alert(
        session,
        severity="warning",
        title="Initial",
        message="First",
        deduplication_key="ordered-condition",
        entity_type="source_run",
        entity_id="run-one",
        now=NOW,
    )
    create_or_recur_alert(
        session,
        severity="urgent",
        title="Newest",
        message="Latest context",
        deduplication_key="ordered-condition",
        entity_type="source_run",
        entity_id="run-three",
        now=NOW + timedelta(days=3),
    )
    acknowledge_alert(session, alert, now=NOW + timedelta(days=4))

    stale = create_or_recur_alert(
        session,
        severity="info",
        title="Late delivery of old event",
        message="Stale context",
        deduplication_key="ordered-condition",
        entity_type="source_run",
        entity_id="run-two",
        now=NOW + timedelta(days=2),
    )

    assert stale is alert
    assert stale.recurrence_count == 3
    assert stale.last_recurred_at == NOW + timedelta(days=3)
    assert stale.title == "Newest"
    assert (stale.entity_type, stale.entity_id) == ("source_run", "run-three")
    assert stale.acknowledged_at == NOW + timedelta(days=4)


def test_alert_inbox_defaults_honor_acknowledgement_snooze_and_resolution(
    session: Session,
) -> None:
    urgent = create_or_recur_alert(
        session,
        severity="urgent",
        title="Urgent",
        message="Act now",
        now=NOW,
    )
    warning = create_or_recur_alert(
        session,
        severity="warning",
        title="Warning",
        message="Review this",
        now=NOW,
    )
    acknowledged = create_or_recur_alert(
        session,
        severity="info",
        title="Acknowledged",
        message="Already read",
        now=NOW,
    )
    resolved = create_or_recur_alert(
        session,
        severity="urgent",
        title="Resolved",
        message="No longer active",
        now=NOW,
    )
    acknowledge_alert(session, acknowledged, now=NOW)
    snooze_alert(
        session,
        warning,
        until=NOW + timedelta(hours=1),
        now=NOW,
    )
    resolve_alert(session, resolved, reason="automatic recovery", now=NOW)

    assert list_alert_inbox(session, now=NOW) == (urgent,)
    assert (
        list_alert_inbox(
            session,
            now=NOW,
            severities={AlertSeverity.WARNING},
        )
        == ()
    )
    assert list_alert_inbox(
        session,
        now=NOW + timedelta(hours=2),
        severities="warning",
    ) == (warning,)
    assert set(
        list_alert_inbox(
            session,
            unread_only=False,
            include_snoozed=True,
            include_resolved=True,
            now=NOW,
        )
    ) == {urgent, warning, acknowledged, resolved}


def test_alert_actions_are_idempotent_and_validate_temporal_state(
    session: Session,
) -> None:
    alert = create_or_recur_alert(
        session,
        severity="info",
        title="Maintenance",
        message="Run optimize",
        fingerprint="maintenance-optimize",
        now=NOW,
    )
    acknowledge_alert(session, alert, now=NOW)
    acknowledge_alert(session, alert, now=NOW + timedelta(hours=1))
    assert alert.acknowledged_at == NOW

    with pytest.raises(ValueError, match="future"):
        snooze_alert(session, alert, until=NOW, now=NOW)
    with pytest.raises(ValueError, match="timezone"):
        snooze_alert(
            session,
            alert,
            until=datetime(2026, 7, 15),
            now=NOW,
        )

    resolve_alert(session, alert, reason=" done ", now=NOW)
    resolve_alert(session, alert, reason="done", now=NOW + timedelta(hours=1))
    resolve_alert(session, alert, now=NOW + timedelta(hours=2))
    assert alert.resolved_at == NOW
    assert alert.resolution_reason == "done"
    with pytest.raises(ReviewQueueError, match="resolved"):
        snooze_alert(
            session,
            alert,
            until=NOW + timedelta(days=1),
            now=NOW,
        )
    with pytest.raises(ValueError, match="NUL"):
        resolve_alert(session, alert, reason="unsafe\x00reason", now=NOW)
    with pytest.raises(ValueError, match="2000"):
        resolve_alert(session, alert, reason="x" * 2_001, now=NOW)


def test_alert_severity_and_pagination_inputs_are_validated(session: Session) -> None:
    with pytest.raises(ValueError, match="severity"):
        create_or_recur_alert(
            session,
            severity="catastrophic",
            title="Bad severity",
            message="invalid",
            now=NOW,
        )
    with pytest.raises(ValueError, match="not be negative"):
        list_alert_inbox(session, limit=-1, now=NOW)
    with pytest.raises(ValueError, match="offset"):
        list_alert_inbox(session, offset=-1, now=NOW)
    assert list_alert_inbox(session, limit=0, now=NOW) == ()


def test_alert_inbox_offset_returns_stable_pages(session: Session) -> None:
    rows = [
        create_or_recur_alert(
            session,
            severity="info",
            title=f"Alert {index}",
            message="Page me",
            now=NOW + timedelta(minutes=index),
        )
        for index in range(3)
    ]

    assert list_alert_inbox(session, now=NOW, limit=2) == (rows[2], rows[1])
    assert list_alert_inbox(session, now=NOW, limit=2, offset=2) == (rows[0],)


def test_concurrent_alert_upserts_converge_on_one_recurrence_row(
    tmp_path: Path,
) -> None:
    engine = create_engine(
        f"sqlite+pysqlite:///{tmp_path / 'alerts.sqlite3'}",
        connect_args={"check_same_thread": False, "timeout": 10},
    )
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
    Base.metadata.create_all(engine)
    barrier = Barrier(2)

    def recur(index: int) -> str:
        with Session(engine) as worker_session:
            barrier.wait()
            alert = create_or_recur_alert(
                worker_session,
                severity="warning",
                title="Concurrent condition",
                message=f"Occurrence {index}",
                deduplication_key="concurrent-condition",
                now=NOW + timedelta(seconds=index),
            )
            worker_session.commit()
            return alert.id

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            ids = set(executor.map(recur, range(2)))
        with Session(engine) as verification:
            alert = verification.scalar(select(Alert))
            assert alert is not None
            assert ids == {alert.id}
            assert alert.recurrence_count == 2
            assert verification.scalar(select(func.count(Alert.id))) == 1
    finally:
        engine.dispose()


def test_canonical_promotion_requires_group_membership(session: Session) -> None:
    first, second, outside = _jobs(session, 3)
    relationship = suggest_duplicate(session, first, second, rule="title")
    assert relationship is not None
    group = confirm_duplicate(session, relationship, now=NOW)

    with pytest.raises(ReviewQueueError, match="already belong"):
        promote_canonical(session, group, outside)

    assert (
        session.scalar(
            select(func.count(CanonicalJobMember.id)).where(
                CanonicalJobMember.group_id == group.id
            )
        )
        == 2
    )
