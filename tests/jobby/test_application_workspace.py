from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import threading

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from jobby.application_workspace import (
    associate_application_contact,
    create_and_associate_contact,
    create_contact,
    create_interview,
    create_task,
    delete_application,
    delete_contact,
    delete_interview,
    delete_task,
    ensure_application_follow_up_task,
    follow_up_automation_key,
    get_application,
    get_contact,
    get_interview,
    get_task,
    list_application_contacts,
    list_application_interviews,
    list_application_tasks,
    remove_application_contact,
    snapshot_application_provenance,
    update_application,
    update_application_contact,
    update_contact,
    update_interview,
    update_task,
)
from jobby.db import Database
from jobby.enums import ApplicationStage, SuggestionKind, TaskStatus
from jobby.models import (
    Application,
    ApplicationContact,
    AuditEvent,
    Base,
    Company,
    Contact,
    Evaluation,
    ExternalSuggestion,
    Interview,
    Job,
    SourceObservation,
    Task,
)
from jobby.pipeline import (
    application_history,
    apply_external_suggestion,
    create_application,
    transition_application,
)
from jobby.review_queues import confirm_duplicate, suggest_duplicate


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


def _jobs(session: Session, count: int = 2) -> list[Job]:
    company = Company(name="Acme", normalized_name="acme")
    session.add(company)
    session.flush()
    jobs = [
        Job(
            company_id=company.id,
            title=f"Policy Counsel {index}",
            normalized_title=f"policy counsel {index}",
            source_primary=f"primary-{index}",
            source_id=f"source-id-{index}",
            launch_url=f"https://jobs.example.test/{index}?apply=true",
        )
        for index in range(count)
    ]
    session.add_all(jobs)
    session.flush()
    return jobs


def test_canonical_application_snapshots_provenance_and_one_editable_follow_up(
    session: Session,
) -> None:
    canonical, duplicate = _jobs(session)
    relationship = suggest_duplicate(session, canonical, duplicate, rule="same role")
    assert relationship is not None
    confirm_duplicate(session, relationship, canonical_job_id=canonical.id, now=NOW)
    evaluation = Evaluation(
        job_id=canonical.id,
        score=4.75,
        ranker_version="semantic-v4",
        is_current=True,
    )
    session.add_all(
        [
            evaluation,
            SourceObservation(
                job_id=canonical.id,
                source="greenhouse",
                source_account="acme",
                source_job_id="gh-1",
                source_url="https://boards.example.test/gh-1",
                observed_at=NOW,
            ),
            SourceObservation(
                job_id=duplicate.id,
                source="workday",
                source_account="acme",
                source_job_id="wd-2",
                source_url="https://boards.example.test/wd-2",
                observed_at=NOW,
            ),
        ]
    )
    session.flush()

    application = create_application(
        session, duplicate.id, occurred_at=NOW - timedelta(seconds=1)
    )
    assert application.job_id == canonical.id
    event = transition_application(
        session, application, ApplicationStage.APPLIED, occurred_at=NOW
    )
    session.flush()

    assert event.to_stage == ApplicationStage.APPLIED
    assert application.applied_evaluation_id == evaluation.id
    assert application.applied_score == 4.75
    assert application.applied_ranker_version == "semantic-v4"
    assert {source["source"] for source in application.applied_sources} == {
        "greenhouse",
        "primary-0",
        "primary-1",
        "workday",
    }
    assert {source["job_id"] for source in application.applied_sources} == {
        canonical.id,
        duplicate.id,
    }

    tasks = list_application_tasks(session, application.id)
    assert len(tasks) == 1
    follow_up = tasks[0]
    assert follow_up.application_id == application.id
    assert follow_up.job_id == canonical.id
    assert follow_up.automation_key == follow_up_automation_key(application.id)
    assert follow_up.due_at == NOW + timedelta(days=7)
    assert application.follow_up_at == follow_up.due_at

    edited_due = NOW + timedelta(days=10)
    update_task(session, follow_up, due_at=edited_due)
    same = ensure_application_follow_up_task(
        session, application, occurred_at=NOW + timedelta(hours=1)
    )
    assert same.id == follow_up.id
    assert same.due_at == edited_due
    assert application.follow_up_at == edited_due
    assert (
        session.scalar(
            select(func.count(Task.id)).where(
                Task.automation_key == follow_up_automation_key(application.id)
            )
        )
        == 1
    )


def test_applied_transition_allows_immediate_follow_up_opt_out_and_custom_due_date(
    session: Session,
) -> None:
    first, second = _jobs(session)
    opted_out = create_application(
        session, first.id, occurred_at=NOW - timedelta(seconds=1)
    )

    transition_application(
        session,
        opted_out,
        ApplicationStage.APPLIED,
        occurred_at=NOW,
        create_follow_up_task=False,
    )

    assert opted_out.submitted_at == NOW
    assert list_application_tasks(session, opted_out.id) == ()
    assert opted_out.follow_up_at is None

    custom_due = NOW + timedelta(days=3, hours=2)
    customized = create_application(
        session, second.id, occurred_at=NOW - timedelta(seconds=1)
    )
    transition_application(
        session,
        customized,
        ApplicationStage.APPLIED,
        occurred_at=NOW,
        follow_up_due_at=custom_due,
    )

    assert list_application_tasks(session, customized.id)[0].due_at == custom_due
    assert customized.follow_up_at == custom_due


def test_workspace_crud_covers_application_task_contact_and_interview_fields(
    session: Session,
) -> None:
    job, other_job = _jobs(session)
    application = create_application(
        session, job.id, submission_channel="portal", occurred_at=NOW
    )
    submitted_at = NOW + timedelta(minutes=1)
    follow_up_at = NOW + timedelta(days=4)
    update_application(
        session,
        application,
        submission_channel="referral",
        submitted_at=submitted_at,
        follow_up_at=follow_up_at,
        rejection_reason="Position paused",
        notes="Recruiter requested a writing sample.",
        import_key="application-import-1",
    )
    assert get_application(session, application.id) is application
    assert application.submission_channel == "referral"
    assert application.submitted_at == submitted_at
    assert application.follow_up_at == follow_up_at
    assert application.rejection_reason == "Position paused"
    assert application.notes == "Recruiter requested a writing sample."
    assert application.import_key == "application-import-1"

    task = create_task(
        session,
        title="Prepare writing sample",
        description="Select the strongest policy memo.",
        status=TaskStatus.PENDING,
        due_at=NOW + timedelta(days=1),
        application=application,
        automation_key="workspace-task-1",
    )
    update_task(
        session,
        task,
        title="Send writing sample",
        description="Attach the approved PDF.",
        status=TaskStatus.COMPLETED,
        due_at=NOW + timedelta(days=2),
        completed_at=NOW + timedelta(hours=2),
        job_id=job.id,
        application=application.id,
        automation_key="workspace-task-2",
    )
    assert get_task(session, task.id) is task
    assert task.title == "Send writing sample"
    assert task.description == "Attach the approved PDF."
    assert task.status == TaskStatus.COMPLETED
    assert task.completed_at == NOW + timedelta(hours=2)
    assert task.job_id == job.id
    assert task.application_id == application.id
    assert task.automation_key == "workspace-task-2"
    update_task(session, task, status=TaskStatus.PENDING)
    assert task.completed_at is None

    contact = create_contact(
        session,
        name="Alex Recruiter",
        company_id=job.company_id,
        email="Alex@Example.test",
        title="Senior Recruiter",
        linkedin_url="https://www.linkedin.com/in/alex",
        notes="Met at a policy conference.",
    )
    same_contact = create_contact(
        session,
        name="Alex R.",
        company_id=other_job.company_id,
        email="alex@example.test",
    )
    assert same_contact.id == contact.id
    link = associate_application_contact(
        session, application, contact, role="recruiter"
    )
    same_link = associate_application_contact(
        session, application, contact, role="hiring contact"
    )
    assert same_link.id == link.id
    assert same_link.role == "hiring contact"
    update_application_contact(session, link, role=None)
    assert link.role is None
    update_contact(
        session,
        contact,
        company_id=job.company_id,
        name="Alexandra Recruiter",
        email="alexandra@example.test",
        title="Lead Recruiter",
        linkedin_url="https://www.linkedin.com/in/alexandra",
        notes="Primary hiring contact.",
    )
    assert get_contact(session, contact.id) is contact
    assert contact.name == "Alexandra Recruiter"
    assert contact.email == "alexandra@example.test"
    assert contact.title == "Lead Recruiter"
    assert contact.linkedin_url == "https://www.linkedin.com/in/alexandra"
    assert contact.notes == "Primary hiring contact."

    deduplicated, deduplicated_link = create_and_associate_contact(
        session,
        application,
        name="Alexandra Recruiter",
        email="ALEXANDRA@example.test",
        role="recruiter",
    )
    assert deduplicated.id == contact.id
    assert deduplicated_link.id == link.id
    assert len(list_application_contacts(session, application.id)) == 1

    interview = create_interview(
        session,
        application,
        NOW + timedelta(days=2),
        ends_at=NOW + timedelta(days=2, hours=1),
        interview_type="screen",
        location_or_link="https://meet.example.test/one",
        contact=contact,
        notes="Prepare policy examples.",
        calendar_event_id="calendar-1",
    )
    update_interview(
        session,
        interview,
        application=application.id,
        starts_at=NOW + timedelta(days=3),
        ends_at=NOW + timedelta(days=3, hours=2),
        interview_type="panel",
        location_or_link="HQ conference room",
        contact=contact.id,
        notes="Four-person panel.",
        calendar_event_id="calendar-2",
    )
    assert get_interview(session, interview.id) is interview
    assert interview.application_id == application.id
    assert interview.starts_at == NOW + timedelta(days=3)
    assert interview.ends_at == NOW + timedelta(days=3, hours=2)
    assert interview.interview_type == "panel"
    assert interview.location_or_link == "HQ conference room"
    assert interview.contact_id == contact.id
    assert interview.notes == "Four-person panel."
    assert interview.calendar_event_id == "calendar-2"
    assert list_application_interviews(session, application.id) == (interview,)

    with pytest.raises(PermissionError, match="confirmation"):
        delete_task(session, task)
    with pytest.raises(PermissionError, match="confirmation"):
        delete_interview(session, interview)
    with pytest.raises(PermissionError, match="confirmation"):
        delete_contact(session, contact)
    with pytest.raises(PermissionError, match="confirmation"):
        delete_application(session, application)

    remove_application_contact(session, link)
    delete_interview(session, interview, confirm=True)
    delete_task(session, task, confirm=True)
    delete_contact(session, contact, confirm=True)
    assert session.get(ApplicationContact, link.id) is None
    assert session.get(Interview, interview.id) is None
    assert session.get(Task, task.id) is None
    assert session.get(Contact, contact.id) is None
    delete_application(session, application, confirm=True)
    assert session.get(Application, application.id) is None
    assert (
        session.scalar(
            select(func.count(AuditEvent.id)).where(
                AuditEvent.action.in_(
                    {
                        "application.updated",
                        "task.updated",
                        "contact.updated",
                        "interview.updated",
                    }
                )
            )
        )
        == 5
    )


def test_workspace_validation_rejects_cross_links_and_invalid_edits(
    session: Session,
) -> None:
    first, second = _jobs(session)
    application = create_application(session, first.id, occurred_at=NOW)
    other_application = create_application(session, second.id, occurred_at=NOW)

    with pytest.raises(ValueError, match="must match"):
        create_task(
            session,
            title="Mismatched",
            job_id=second.id,
            application=application,
        )
    with pytest.raises(ValueError, match="end must be after"):
        create_interview(
            session,
            application,
            NOW + timedelta(hours=1),
            ends_at=NOW,
        )
    with pytest.raises(ValueError, match="email is invalid"):
        create_contact(session, name="Invalid", email="not-an-email")

    update_application(session, application, import_key="unique-import")
    with pytest.raises(ValueError, match="already in use"):
        update_application(session, other_application, import_key="unique-import")

    with pytest.raises(LookupError, match="not found"):
        get_task(session, "missing-task")
    with pytest.raises(LookupError, match="not found"):
        get_contact(session, "missing-contact")
    with pytest.raises(LookupError, match="not found"):
        get_interview(session, "missing-interview")


def test_applied_transition_validation_and_provenance_fail_without_side_effects(
    session: Session,
) -> None:
    job = _jobs(session, 1)[0]
    application = create_application(session, job.id, occurred_at=NOW)
    session.flush()

    with pytest.raises(ValueError, match="only be snapshotted"):
        snapshot_application_provenance(session, application)
    with pytest.raises(ValueError, match="follow-up days"):
        transition_application(
            session,
            application,
            ApplicationStage.APPLIED,
            occurred_at=NOW + timedelta(minutes=1),
            follow_up_days=1.5,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="due_at must be a datetime"):
        transition_application(
            session,
            application,
            ApplicationStage.APPLIED,
            occurred_at=NOW + timedelta(minutes=1),
            follow_up_due_at="tomorrow",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="true or false"):
        transition_application(
            session,
            application,
            ApplicationStage.APPLIED,
            occurred_at=NOW + timedelta(minutes=1),
            create_follow_up_task="yes",  # type: ignore[arg-type]
        )

    assert application.current_stage == ApplicationStage.PLANNED
    assert application.submitted_at is None
    assert application.applied_evaluation_id is None
    assert application.applied_sources == []
    assert len(application_history(session, application.id)) == 1
    assert list_application_tasks(session, application.id) == ()
    assert (
        session.scalar(
            select(func.count(AuditEvent.id)).where(
                AuditEvent.entity_id == application.id,
                AuditEvent.action.in_(
                    {
                        "application.provenance_snapshotted",
                        "application.stage_changed",
                    }
                ),
            )
        )
        == 0
    )


def test_follow_up_namespace_and_identity_are_protected_and_due_stays_editable(
    session: Session,
) -> None:
    job, other_job = _jobs(session)
    application = create_application(session, job.id, occurred_at=NOW)
    reserved_key = follow_up_automation_key(application.id)

    with pytest.raises(ValueError, match="reserved"):
        create_task(
            session,
            title="Not the real follow-up",
            application=application,
            automation_key=reserved_key,
            status=TaskStatus.COMPLETED,
        )

    transition_application(
        session,
        application,
        ApplicationStage.APPLIED,
        occurred_at=NOW + timedelta(minutes=1),
    )
    task = list_application_tasks(session, application.id)[0]
    original_identity = (task.application_id, task.job_id, task.automation_key)

    with pytest.raises(ValueError, match="identity fields"):
        update_task(session, task, automation_key="replacement")
    with pytest.raises(ValueError, match="identity fields"):
        update_task(session, task, application=None)
    with pytest.raises(ValueError, match="must match|identity fields"):
        update_task(session, task, job_id=other_job.id)

    edited_due = NOW + timedelta(days=20)
    update_task(session, task, title="Check application status", due_at=edited_due)
    assert (task.application_id, task.job_id, task.automation_key) == original_identity
    assert task.title == "Check application status"
    assert task.due_at == edited_due
    assert application.follow_up_at == edited_due
    assert ensure_application_follow_up_task(session, application).id == task.id
    assert len(list_application_tasks(session, application.id)) == 1


def test_standalone_tasks_route_to_the_canonical_job(session: Session) -> None:
    canonical, duplicate = _jobs(session)
    relationship = suggest_duplicate(session, canonical, duplicate, rule="same role")
    assert relationship is not None
    confirm_duplicate(session, relationship, canonical_job_id=canonical.id, now=NOW)

    task = create_task(session, title="Research role", job_id=duplicate.id)

    assert task.job_id == canonical.id


def test_failed_multi_field_edits_are_atomic(session: Session) -> None:
    first, second = _jobs(session)
    application = create_application(
        session, first.id, submission_channel="portal", occurred_at=NOW
    )
    other_application = create_application(session, second.id, occurred_at=NOW)
    update_application(session, other_application, import_key="occupied")
    task = create_task(session, title="Original task", application=application)
    contact = create_contact(
        session,
        name="Original Contact",
        company_id=first.company_id,
        title="Recruiter",
    )
    first_interview = create_interview(
        session,
        application,
        NOW + timedelta(days=1),
        calendar_event_id="calendar-original",
    )
    create_interview(
        session,
        application,
        NOW + timedelta(days=2),
        calendar_event_id="calendar-occupied",
    )

    with pytest.raises(ValueError, match="already in use"):
        update_application(
            session,
            application,
            submission_channel="changed",
            import_key="occupied",
        )
    with pytest.raises(ValueError, match="only completed"):
        update_task(
            session,
            task,
            title="Changed task",
            status=TaskStatus.PENDING,
            completed_at=NOW,
        )
    with pytest.raises(ValueError, match="300 characters"):
        update_contact(
            session,
            contact,
            name="Changed Contact",
            title="x" * 301,
        )
    with pytest.raises(ValueError, match="already in use"):
        update_interview(
            session,
            first_interview,
            starts_at=NOW + timedelta(days=5),
            calendar_event_id="calendar-occupied",
        )

    assert application.submission_channel == "portal"
    assert application.import_key is None
    assert task.title == "Original task"
    assert task.completed_at is None
    assert contact.name == "Original Contact"
    assert contact.title == "Recruiter"
    assert first_interview.starts_at == NOW + timedelta(days=1)
    assert first_interview.calendar_event_id == "calendar-original"


def test_contact_creation_enriches_blanks_without_overwriting_or_duplicates(
    session: Session,
) -> None:
    company = _jobs(session, 1)[0].company_id
    blank_first = create_contact(
        session,
        name="Alex Recruiter",
        company_id=company,
    )
    enriched = create_contact(
        session,
        name="Alex Recruiter",
        company_id=company,
        email="alex@example.test",
        title="Lead Recruiter",
    )
    assert enriched.id == blank_first.id
    assert blank_first.email == "alex@example.test"
    assert blank_first.title == "Lead Recruiter"

    email_first = create_contact(
        session,
        name="Jordan Recruiter",
        company_id=company,
        email="jordan@example.test",
        title="Original title",
    )
    no_email_second = create_contact(
        session,
        name="Jordan Recruiter",
        company_id=company,
        title="Replacement title",
    )
    same_email_again = create_contact(
        session,
        name="J. Recruiter",
        company_id=company,
        email="JORDAN@example.test",
        title="Replacement title",
    )
    assert no_email_second.id == email_first.id == same_email_again.id
    assert email_first.title == "Original title"
    assert session.scalar(select(func.count(Contact.id))) == 2


def test_external_follow_up_edit_synchronizes_the_automated_task(
    session: Session,
) -> None:
    job = _jobs(session, 1)[0]
    application = create_application(session, job.id, occurred_at=NOW)
    transition_application(
        session,
        application,
        ApplicationStage.APPLIED,
        occurred_at=NOW + timedelta(minutes=1),
    )
    task = list_application_tasks(session, application.id)[0]
    suggestion = ExternalSuggestion(
        kind=SuggestionKind.FOLLOW_UP_NEEDED,
        application_id=application.id,
        payload={"follow_up_at": "2026-07-30T09:00:00-07:00"},
    )
    session.add(suggestion)
    session.flush()

    assert apply_external_suggestion(session, suggestion, approved=True) is None

    expected = datetime(2026, 7, 30, 16, tzinfo=timezone.utc)
    assert application.follow_up_at == expected
    assert task.due_at == expected


@pytest.mark.parametrize(
    ("submission_channel", "notes", "message"),
    [
        ("x" * 201, None, "200 characters"),
        (123, None, "must be text"),
        (None, object(), "must be text"),
    ],
)
def test_application_creation_validates_before_persisting(
    session: Session,
    submission_channel: object,
    notes: object,
    message: str,
) -> None:
    job = _jobs(session, 1)[0]
    before = session.scalar(select(func.count(Application.id)))

    with pytest.raises(ValueError, match=message):
        create_application(
            session,
            job.id,
            submission_channel=submission_channel,  # type: ignore[arg-type]
            notes=notes,  # type: ignore[arg-type]
            occurred_at=NOW,
        )

    assert session.scalar(select(func.count(Application.id))) == before


def test_application_creation_normalizes_optional_text(session: Session) -> None:
    job = _jobs(session, 1)[0]

    application = create_application(
        session,
        job.id,
        submission_channel="  employee   referral  ",
        notes="  Ask about the policy team.  ",
        occurred_at=NOW,
    )
    blank = create_application(
        session,
        job.id,
        submission_channel="   ",
        notes="  ",
        occurred_at=NOW,
    )

    assert application.submission_channel == "employee referral"
    assert application.notes == "Ask about the policy team."
    assert blank.submission_channel is None
    assert blank.notes is None


def test_concurrent_contact_and_association_creation_converge(tmp_path) -> None:
    database = Database(tmp_path / "contact-convergence.sqlite3")
    database.initialize()
    with database.session() as session:
        job = _jobs(session, 1)[0]
        application = create_application(session, job.id, occurred_at=NOW)
        company_id = job.company_id
        application_id = application.id

    emails = [None, "concurrent@example.test"] * 4
    contact_barrier = threading.Barrier(len(emails))

    def create_one(email: str | None) -> str:
        with database.session() as session:
            contact_barrier.wait(timeout=5)
            return create_contact(
                session,
                name="Concurrent Recruiter",
                company_id=company_id,
                email=email,
            ).id

    with ThreadPoolExecutor(max_workers=len(emails)) as executor:
        contact_ids = list(executor.map(create_one, emails))

    assert len(set(contact_ids)) == 1
    contact_id = contact_ids[0]
    association_workers = 8
    association_barrier = threading.Barrier(association_workers)

    def associate_one(_: int) -> str:
        with database.session() as session:
            association_barrier.wait(timeout=5)
            return associate_application_contact(
                session,
                application_id,
                contact_id,
                role="recruiter",
            ).id

    with ThreadPoolExecutor(max_workers=association_workers) as executor:
        link_ids = list(executor.map(associate_one, range(association_workers)))

    assert len(set(link_ids)) == 1
    with database.session() as session:
        contact = session.get(Contact, contact_id)
        assert contact is not None
        assert contact.email == "concurrent@example.test"
        assert session.scalar(select(func.count(Contact.id))) == 1
        assert session.scalar(select(func.count(ApplicationContact.id))) == 1
        assert (
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "contact.created"
                )
            )
            == 1
        )
    database.dispose()
