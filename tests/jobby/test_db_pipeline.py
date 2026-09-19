"""Offline integration checks for SQLite durability and application history."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import re

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import IntegrityError

from jobby.audit import record_audit
from jobby.db import Database, DatabaseUpgradeRequiredError, table_names
from jobby.enums import (
    ApplicationStage,
    ApprovalState,
    ArtifactKind,
    DocumentStatus,
    SuggestionKind,
)
from jobby.models import (
    Application,
    ApplicationMaterial,
    AIRun,
    Artifact,
    AuditEvent,
    Base,
    Company,
    DocumentVersion,
    Evaluation,
    ExternalSuggestion,
    Interview,
    Job,
    Offer,
    StageEvent,
)
from jobby.pipeline import (
    application_history,
    apply_external_suggestion,
    attach_application_material,
    compare_persisted_offers,
    create_interview,
    create_offer,
    create_application,
    offer_comparison,
    set_offer_decision,
    transition_application,
)
from jobby.upgrade import apply_upgrade


def _database(tmp_path) -> Database:
    database = Database(tmp_path / "jobby.sqlite3")
    database.initialize()
    return database


def _job(session, *, suffix: str = "") -> Job:
    company = Company(name=f"Example{suffix}", normalized_name=f"example{suffix}")
    session.add(company)
    session.flush()
    job = Job(
        company_id=company.id,
        title="Policy Analyst",
        normalized_title="policy analyst",
    )
    session.add(job)
    session.flush()
    return job


def test_initialize_runs_alembic_head_and_creates_the_complete_schema(tmp_path):
    database = Database(tmp_path / "schema.sqlite3")
    database.initialize()
    database.initialize()  # The same Database object may be initialized repeatedly.

    assert table_names(database.engine) == set(Base.metadata.tables) | {
        "alembic_version"
    }
    with database.engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0013_operation_runs"
        )
    assert database.integrity_check() == (True, "ok")
    database.dispose()


def test_migration_head_is_frozen_and_matches_declared_model_schema(tmp_path):
    from alembic import command
    from alembic.config import Config

    migration_root = (
        Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"
    )
    initial_source = (migration_root / "versions" / "0001_initial.py").read_text(
        encoding="utf-8"
    )
    assert "from jobby.models import Base" not in initial_source
    assert "baseline_v0001.json" in initial_source

    migrated_path = tmp_path / "migrated.sqlite3"
    declared_path = tmp_path / "declared.sqlite3"
    config = Config()
    config.set_main_option("script_location", str(migration_root))
    config.set_main_option("sqlalchemy.url", f"sqlite+pysqlite:///{migrated_path}")
    command.upgrade(config, "head")
    declared_engine = create_engine(f"sqlite+pysqlite:///{declared_path}")
    Base.metadata.create_all(declared_engine)
    declared_engine.dispose()

    def signature(path: Path) -> set[tuple[str, str, str, str]]:
        engine = create_engine(f"sqlite+pysqlite:///{path}")
        try:
            with engine.connect() as connection:
                rows = connection.execute(
                    text(
                        """
                        SELECT type, name, tbl_name, sql
                        FROM sqlite_master
                        WHERE sql IS NOT NULL
                          AND name NOT LIKE 'sqlite_%'
                          AND name != 'alembic_version'
                        """
                    )
                ).all()
            return {
                (kind, name, table, re.sub(r"\s+", " ", sql).strip().casefold())
                for kind, name, table, sql in rows
            }
        finally:
            engine.dispose()

    assert signature(migrated_path) == signature(declared_path)


def test_0002_upgrades_a_database_previously_stamped_at_0001(tmp_path):
    from alembic import command
    from alembic.config import Config

    path = tmp_path / "upgrade.sqlite3"
    database = Database(path)
    database.initialize()
    database.dispose()

    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"),
    )
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+pysqlite:///{path}".replace("%", "%%")
    )
    command.downgrade(config, "0001_initial")

    pre_upgrade = Database(path)
    assert "application_materials" not in table_names(pre_upgrade.engine)
    assert "offers" not in table_names(pre_upgrade.engine)
    with pre_upgrade.engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0001_initial"
        )
    pre_upgrade.dispose()

    upgraded = Database(path)
    with pytest.raises(DatabaseUpgradeRequiredError):
        upgraded.initialize()
    upgraded.dispose()
    apply_upgrade(path, backup_dir=tmp_path / "upgrade-backups")
    upgraded = Database(path)
    upgraded.initialize()
    assert {"application_materials", "offers"} <= table_names(upgraded.engine)
    with upgraded.engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0013_operation_runs"
        )
    upgraded.dispose()


def test_0003_repairs_duplicate_current_evaluations_before_enforcing_uniqueness(
    tmp_path,
):
    from alembic import command
    from alembic.config import Config

    path = tmp_path / "duplicate-evaluations.sqlite3"
    database = Database(path)
    database.initialize()
    with database.session() as session:
        job_id = _job(session).id
    database.dispose()

    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"),
    )
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+pysqlite:///{path}".replace("%", "%%")
    )
    command.downgrade(config, "0002_materials_offers")

    now = datetime.now(timezone.utc).isoformat()
    with create_engine(f"sqlite+pysqlite:///{path}").begin() as connection:
        for evaluation_id, score in (("legacy-eval-1", 3.0), ("legacy-eval-2", 4.0)):
            connection.execute(
                text(
                    """
                    INSERT INTO evaluations(
                        job_id, score, components, gates, evidence, warnings,
                        confidence, automatic_skip, manual_override, locked,
                        is_current, id, created_at, updated_at
                    ) VALUES (
                        :job_id, :score, '{}', '[]', '[]', '[]',
                        1.0, 0, 0, 0, 1, :id, :now, :now
                    )
                    """
                ),
                {"job_id": job_id, "score": score, "id": evaluation_id, "now": now},
            )

    upgraded = Database(path)
    with pytest.raises(DatabaseUpgradeRequiredError):
        upgraded.initialize()
    upgraded.dispose()
    apply_upgrade(path, backup_dir=tmp_path / "evaluation-backups")
    upgraded = Database(path)
    upgraded.initialize()
    with upgraded.session() as session:
        evaluations = list(
            session.scalars(
                select(Evaluation)
                .where(Evaluation.job_id == job_id)
                .order_by(Evaluation.created_at, Evaluation.id)
            )
        )
        assert sum(item.is_current for item in evaluations) == 1
        assert session.get(Job, job_id).latest_score == next(
            item.score for item in evaluations if item.is_current
        )
    with pytest.raises(IntegrityError):
        with upgraded.session() as session:
            session.add(Evaluation(job_id=job_id, score=5.0, is_current=True))
    upgraded.dispose()


def test_0004_preserves_existing_ai_runs_and_incoming_foreign_keys(tmp_path):
    from alembic import command
    from alembic.config import Config

    path = tmp_path / "ai-cache-upgrade.sqlite3"
    database = Database(path)
    database.initialize()
    database.dispose()
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"),
    )
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+pysqlite:///{path}".replace("%", "%%")
    )
    command.downgrade(config, "0003_current_eval")

    now = datetime.now(timezone.utc).isoformat()
    with create_engine(f"sqlite+pysqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ai_runs(
                    purpose, provider, model, prompt_version, input_hash,
                    output_json, approval_state, id, created_at, updated_at
                ) VALUES (
                    'job_enrichment', 'openai', 'test-model', 'v1', :input_hash,
                    '{}', 'pending', 'old-ai-run', :now, :now
                )
                """
            ),
            {"input_hash": "a" * 64, "now": now},
        )
        connection.execute(
            text(
                """
                INSERT INTO citations(id, ai_run_id, url, created_at)
                VALUES ('old-citation', 'old-ai-run', 'https://example.test', :now)
                """
            ),
            {"now": now},
        )

    upgraded = Database(path)
    with pytest.raises(DatabaseUpgradeRequiredError):
        upgraded.initialize()
    upgraded.dispose()
    apply_upgrade(path, backup_dir=tmp_path / "ai-backups")
    upgraded = Database(path)
    upgraded.initialize()
    with upgraded.session() as session:
        run = session.get(AIRun, "old-ai-run")
        assert run is not None
        assert run.cache_hit is False
        assert run.cache_entry_id is None
        assert run.source_ai_run_id is None
    assert upgraded.integrity_check() == (True, "ok")
    upgraded.dispose()


def test_every_connection_enables_wal_and_foreign_key_enforcement(tmp_path):
    database = _database(tmp_path)
    with database.engine.connect() as connection:
        assert (
            connection.exec_driver_sql("PRAGMA journal_mode").scalar_one().casefold()
            == "wal"
        )
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 30_000

    with pytest.raises(IntegrityError):
        with database.session() as session:
            session.add(
                Job(
                    company_id="company-does-not-exist",
                    title="Invalid",
                    normalized_title="invalid",
                )
            )
    assert database.integrity_check()[0]
    database.dispose()


def test_event_history_is_append_only_at_the_orm_and_sqlite_layers(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        transition_application(session, application, ApplicationStage.APPLIED)
        application_id = application.id

    with database.session() as session:
        event = application_history(session, application_id)[0]
        event.reason = "rewritten"
        with pytest.raises(ValueError, match="immutable"):
            session.flush()
        session.rollback()

    with database.session() as session:
        event = application_history(session, application_id)[0]
        session.delete(event)
        with pytest.raises(ValueError, match="immutable"):
            session.flush()
        session.rollback()

    with database.engine.begin() as connection:
        event_id = connection.execute(
            text(
                "SELECT id FROM stage_events WHERE application_id = :application_id LIMIT 1"
            ),
            {"application_id": application_id},
        ).scalar_one()
    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text("UPDATE stage_events SET reason = 'raw rewrite' WHERE id = :id"),
                {"id": event_id},
            )
    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM stage_events WHERE id = :id"), {"id": event_id}
            )
    database.dispose()


def test_imported_source_identity_is_immutable_but_links_remain_enrichable(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        artifact = Artifact(
            kind=ArtifactKind.SOURCE_REPORT,
            workspace_root="/legacy",
            source_path="new_jobs.md",
            stored_path="/archive/abc.md",
            content_hash="a" * 64,
            size_bytes=12,
            source_immutable=True,
            metadata_json={"original_absolute_path": "/legacy/new_jobs.md"},
        )
        session.add(artifact)
        session.flush()
        artifact_id = artifact.id

    with database.session() as session:
        artifact = session.get(Artifact, artifact_id)
        artifact.content_hash = "b" * 64
        with pytest.raises(ValueError, match="immutable"):
            session.flush()
        session.rollback()

    with database.session() as session:
        artifact = session.get(Artifact, artifact_id)
        job = _job(session, suffix="-link")
        artifact.job_id = job.id
        session.flush()

    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text("UPDATE artifacts SET source_path = 'other.md' WHERE id = :id"),
                {"id": artifact_id},
            )
    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM artifacts WHERE id = :id"), {"id": artifact_id}
            )
    database.dispose()


def test_pipeline_requires_approval_for_external_changes_and_keeps_ordered_history(
    tmp_path,
):
    database = _database(tmp_path)
    applied_at = datetime(2026, 3, 3, 12, tzinfo=timezone.utc)
    with database.session() as session:
        job = _job(session)
        application = create_application(
            session,
            job.id,
            occurred_at=applied_at - timedelta(microseconds=1),
        )
        with pytest.raises(PermissionError, match="explicit approval"):
            transition_application(
                session,
                application,
                ApplicationStage.APPLIED,
                source="email",
                occurred_at=applied_at,
            )
        event = transition_application(
            session,
            application,
            ApplicationStage.APPLIED,
            source="approved_email",
            explicit_approval=True,
            occurred_at=applied_at,
        )
        assert event.occurred_at == applied_at
        application_id = application.id

    with database.session() as session:
        application = session.get(Application, application_id)
        assert application.current_stage == ApplicationStage.APPLIED
        assert application.submitted_at == applied_at
        assert [
            event.to_stage for event in application_history(session, application_id)
        ] == [
            ApplicationStage.PLANNED,
            ApplicationStage.APPLIED,
        ]
        with pytest.raises(ValueError, match="invalid application transition"):
            transition_application(session, application, ApplicationStage.PLANNED)

        audit = record_audit(
            session,
            action="test.recorded",
            entity_type="application",
            entity_id=application.id,
            after={"stage": application.current_stage, "at": applied_at},
        )
        session.flush()
        assert audit.after_json == {"stage": "applied", "at": applied_at.isoformat()}

    with database.session() as session:
        assert (
            session.scalar(
                select(AuditEvent).where(AuditEvent.action == "test.recorded")
            )
            is not None
        )
        assert (
            session.scalar(
                select(StageEvent).where(StageEvent.application_id == application_id)
            )
            is not None
        )
    database.dispose()


def test_application_materials_are_idempotently_linked_and_audited(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        document = DocumentVersion(
            kind=ArtifactKind.RESUME,
            name="Policy resume",
            content_markdown="# Resume",
            content_hash="d" * 64,
            status=DocumentStatus.APPROVED,
            approval_state=ApprovalState.APPROVED,
        )
        session.add(document)
        session.flush()

        first = attach_application_material(
            session,
            application,
            document,
            purpose="resume",
        )
        second = attach_application_material(
            session,
            application.id,
            document.id,
            purpose=" resume ",
        )
        assert second.id == first.id
        application_id = application.id

    with database.session() as session:
        materials = list(
            session.scalars(
                select(ApplicationMaterial).where(
                    ApplicationMaterial.application_id == application_id
                )
            )
        )
        assert len(materials) == 1
        assert materials[0].purpose == "resume"
        assert (
            session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "application.material_attached")
            )
            == 1
        )
    database.dispose()


def test_application_materials_reject_unapproved_versions_and_bound_purpose(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        pending = DocumentVersion(
            kind=ArtifactKind.RESUME,
            name="Pending resume",
            content_markdown="# Pending",
            content_hash="p" * 64,
            status=DocumentStatus.PROPOSED,
            approval_state=ApprovalState.PENDING,
        )
        rejected = DocumentVersion(
            kind=ArtifactKind.COVER_LETTER,
            name="Rejected letter",
            content_markdown="# Rejected",
            content_hash="r" * 64,
            status=DocumentStatus.REJECTED,
            approval_state=ApprovalState.REJECTED,
        )
        approved = DocumentVersion(
            kind=ArtifactKind.RESUME,
            name="Approved resume",
            content_markdown="# Approved",
            content_hash="a" * 64,
            status=DocumentStatus.READY,
            approval_state=ApprovalState.APPROVED,
        )
        session.add_all([pending, rejected, approved])
        session.flush()
        with pytest.raises(PermissionError, match="approved document"):
            attach_application_material(session, application, pending, purpose="resume")
        with pytest.raises(PermissionError, match="approved document"):
            attach_application_material(
                session, application, rejected, purpose="cover letter"
            )
        material = attach_application_material(
            session,
            application,
            approved,
            purpose="  " + "X" * 150 + "  ",
            used_at=datetime(2026, 7, 14, 9),
        )
        assert material.purpose == "x" * 100
        assert material.used_at == datetime(2026, 7, 14, 9, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="used_at must be a datetime"):
            attach_application_material(
                session,
                application,
                approved,
                purpose="cover letter",
                used_at="yesterday",
            )
    database.dispose()


def test_offers_are_persisted_compared_and_decisions_are_audited(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        first_job = _job(session, suffix="-first")
        second_job = _job(session, suffix="-second")
        first_application = create_application(session, first_job.id)
        second_application = create_application(session, second_job.id)
        first = create_offer(
            session,
            first_application,
            base_salary=150_000,
            annual_bonus=15_000,
            annualized_equity=10_000,
            cost_of_living_index=125,
            stress_score=2,
            terms={"remote": True},
        )
        create_offer(
            session,
            second_application.id,
            base_salary=145_000,
            annual_bonus=5_000,
            cost_of_living_index=100,
            stress_score=3,
        )
        set_offer_decision(session, first, "accepted")

    with database.session() as session:
        offers = list(session.scalars(select(Offer)))
        comparison = compare_persisted_offers(session)
        decisions = list(
            session.scalars(
                select(AuditEvent).where(AuditEvent.action == "offer.decision_changed")
            )
        )
    assert len(offers) == 2
    assert offers[0].currency == "USD"
    assert {offer.decision for offer in offers} == {"accepted", None}
    assert (
        comparison[0]["stress_adjusted_value"] >= comparison[1]["stress_adjusted_value"]
    )
    assert {row["offer_id"] for row in comparison} == {offer.id for offer in offers}
    assert len(decisions) == 1
    database.dispose()


def test_interview_ranges_and_offer_numbers_must_be_finite_and_ordered(tmp_path):
    database = _database(tmp_path)
    starts_at = datetime(2026, 7, 12, 12, tzinfo=timezone.utc)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        with pytest.raises(ValueError, match="after its start"):
            create_interview(
                session,
                application.id,
                starts_at,
                ends_at=starts_at,
            )
        with pytest.raises(ValueError, match="finite"):
            create_offer(session, application, base_salary=float("nan"))
        with pytest.raises(ValueError, match="finite"):
            create_offer(
                session,
                application,
                cost_of_living_index=float("inf"),
            )
        with pytest.raises(ValueError, match="finite"):
            create_offer(session, application, stress_score=float("nan"))

        with pytest.raises(ValueError, match="finite"):
            create_offer(session, application, base_salary=True)
        with pytest.raises(ValueError, match="derived compensation"):
            create_offer(session, application, base_salary=1e308, annual_bonus=1e308)
        with pytest.raises(ValueError, match="three-letter"):
            create_offer(session, application, currency=None)
        with pytest.raises(ValueError, match="decision must be text"):
            create_offer(session, application, decision=True)
        with pytest.raises(ValueError, match="offered_at must be a datetime"):
            create_offer(session, application, offered_at="tomorrow")
        with pytest.raises(ValueError, match="terms must be an object"):
            create_offer(session, application, terms=["remote"])
        with pytest.raises(ValueError, match="finite JSON-compatible"):
            create_offer(session, application, terms={"signing_bonus": float("nan")})
    database.dispose()


@pytest.mark.parametrize(
    "offer",
    [
        {"base_salary": float("nan")},
        {"annual_bonus": -1},
        {"cost_of_living_index": 0},
        {"stress_score": 6},
        {"annualized_equity": False},
    ],
)
def test_offer_comparison_rejects_invalid_values(offer) -> None:
    with pytest.raises(ValueError):
        offer_comparison([offer])


def test_offer_comparison_rejects_non_finite_derived_totals() -> None:
    with pytest.raises(ValueError, match="derived compensation"):
        offer_comparison([{"base_salary": 1e308, "annual_bonus": 1e308}])


def test_approved_external_suggestion_has_a_sanitized_distinct_audit_event(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        suggestion = ExternalSuggestion(
            kind=SuggestionKind.APPLIED,
            application_id=application.id,
            payload={"raw_body": "private email body", "confidence_reason": "explicit"},
            confidence=0.99,
        )
        session.add(suggestion)
        session.flush()
        event = apply_external_suggestion(session, suggestion, approved=True)
        suggestion_id = suggestion.id
        assert event is not None

    with database.session() as session:
        suggestion = session.get(ExternalSuggestion, suggestion_id)
        audits = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "suggestion.approved",
                    AuditEvent.entity_id == suggestion_id,
                )
            )
        )
    assert suggestion.approval_state == ApprovalState.APPROVED
    assert len(audits) == 1
    assert audits[0].after_json == {
        "suggestion_id": suggestion_id,
        "kind": "applied",
        "application_id": suggestion.application_id,
        "stage": "applied",
    }
    assert "raw_body" not in str(audits[0].after_json)
    database.dispose()


def test_approved_calendar_suggestion_creates_one_local_interview_and_audits_it(
    tmp_path,
):
    database = _database(tmp_path)
    with database.session() as session:
        job = _job(session)
        application = create_application(session, job.id)
        suggestion = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="calendar-event-1",
            payload={
                "summary": "  Recruiter   screen — Example  ",
                "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
                "end": {"dateTime": "2026-07-14T10:45:00-07:00"},
                "location": "https://meet.example/interview",
                "preview_only": True,
                "description": "must not be copied or audited",
            },
        )
        session.add(suggestion)
        session.flush()

        interview = apply_external_suggestion(session, suggestion, approved=True)
        suggestion_id = suggestion.id
        application_id = application.id
        assert isinstance(interview, Interview)
        interview_id = interview.id
        assert application.current_stage == ApplicationStage.PLANNED

    with database.session() as session:
        suggestion = session.get(ExternalSuggestion, suggestion_id)
        interview = session.get(Interview, interview_id)
        audits = list(
            session.scalars(
                select(AuditEvent)
                .where(
                    AuditEvent.action.in_(["interview.created", "suggestion.approved"])
                )
                .order_by(AuditEvent.occurred_at, AuditEvent.id)
            )
        )

        assert suggestion.approval_state == ApprovalState.APPROVED
        assert suggestion.applied_at is not None
        assert interview.application_id == application_id
        assert interview.calendar_event_id == "calendar-event-1"
        assert interview.starts_at == datetime(2026, 7, 14, 17, tzinfo=timezone.utc)
        assert interview.ends_at == datetime(2026, 7, 14, 17, 45, tzinfo=timezone.utc)
        assert interview.interview_type == "Recruiter screen — Example"
        assert interview.location_or_link == "https://meet.example/interview"
        assert len(application_history(session, application_id)) == 1
        assert [audit.action for audit in audits] == [
            "interview.created",
            "suggestion.approved",
        ]
        assert audits[1].after_json == {
            "suggestion_id": suggestion_id,
            "kind": "calendar_interview",
            "application_id": application_id,
            "interview_id": interview_id,
            "external_event_id": "calendar-event-1",
        }
        assert "description" not in str(audits)
    database.dispose()


@pytest.mark.parametrize(
    ("external_event_id", "payload", "message"),
    [
        (
            None,
            {"start": {"dateTime": "2026-07-14T10:00:00-07:00"}},
            "external event ID",
        ),
        ("event-1", {}, "start must be an event-time object"),
        (
            "event-1",
            {"start": {"date": "2026-07-14"}},
            "not an all-day date",
        ),
        (
            "event-1",
            {"start": {"dateTime": "2026-07-14T10:00:00"}},
            "must include a timezone",
        ),
        (
            "event-1",
            {
                "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
                "end": {"dateTime": "2026-07-14T09:00:00-07:00"},
            },
            "end must be after",
        ),
    ],
)
def test_calendar_suggestion_rejects_malformed_metadata_without_local_effect(
    tmp_path,
    external_event_id,
    payload,
    message,
):
    database = _database(tmp_path)
    with database.session() as session:
        application = create_application(session, _job(session).id)
        suggestion = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id=external_event_id,
            payload=payload,
        )
        session.add(suggestion)
        session.flush()
        suggestion_id = suggestion.id

        with pytest.raises(ValueError, match=message):
            apply_external_suggestion(session, suggestion, approved=True)

    with database.session() as session:
        suggestion = session.get(ExternalSuggestion, suggestion_id)
        assert suggestion.approval_state == ApprovalState.PENDING
        assert suggestion.applied_at is None
        assert session.scalar(select(func.count()).select_from(Interview)) == 0
        assert (
            session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "suggestion.approved")
            )
            == 0
        )
    database.dispose()


def test_unlinked_suggestion_cannot_be_approved_but_can_be_rejected_once(tmp_path):
    database = _database(tmp_path)
    with database.session() as session:
        suggestion = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            external_event_id="calendar-event-unlinked",
            payload={"start": {"dateTime": "2026-07-14T10:00:00Z"}},
        )
        session.add(suggestion)
        session.flush()
        suggestion_id = suggestion.id

        with pytest.raises(ValueError, match="not linked"):
            apply_external_suggestion(session, suggestion, approved=True)
        assert suggestion.approval_state == ApprovalState.PENDING
        assert apply_external_suggestion(session, suggestion, approved=False) is None
        with pytest.raises(ValueError, match="already been reviewed"):
            apply_external_suggestion(session, suggestion, approved=False)

    with database.session() as session:
        suggestion = session.get(ExternalSuggestion, suggestion_id)
        assert suggestion.approval_state == ApprovalState.REJECTED
        assert suggestion.applied_at is None
        assert session.scalar(select(func.count()).select_from(Interview)) == 0
        rejection_audits = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "suggestion.rejected",
                    AuditEvent.entity_id == suggestion_id,
                )
            )
        )
        assert len(rejection_audits) == 1
        assert rejection_audits[0].after_json is None
    database.dispose()


def test_calendar_suggestion_review_and_event_identity_are_idempotent(tmp_path):
    database = _database(tmp_path)
    payload = {
        "summary": "Hiring manager interview",
        "start": {"dateTime": "2026-07-14T10:00:00", "timeZone": "US/Pacific"},
        "end": {"dateTime": "2026-07-14T10:30:00", "timeZone": "US/Pacific"},
    }
    with database.session() as session:
        application = create_application(session, _job(session).id)
        first = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="same-calendar-event",
            payload=payload,
        )
        second = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="same-calendar-event",
            payload=payload,
        )
        session.add_all([first, second])
        session.flush()

        first_interview = apply_external_suggestion(session, first, approved=True)
        with pytest.raises(ValueError, match="already been reviewed"):
            apply_external_suggestion(session, first, approved=True)
        second_interview = apply_external_suggestion(session, second, approved=True)

        assert isinstance(first_interview, Interview)
        assert isinstance(second_interview, Interview)
        assert second_interview.id == first_interview.id

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Interview)) == 1
        assert (
            session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "interview.created")
            )
            == 1
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "suggestion.approved")
            )
            == 2
        )
    database.dispose()


def test_approved_calendar_revision_reschedules_or_cancels_only_local_interview(
    tmp_path,
):
    database = _database(tmp_path)
    with database.session() as session:
        application = create_application(session, _job(session).id)
        initial = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="calendar-revision",
            payload={
                "summary": "Recruiter interview",
                "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
                "end": {"dateTime": "2026-07-14T10:30:00-07:00"},
            },
        )
        session.add(initial)
        session.flush()
        interview = apply_external_suggestion(session, initial, approved=True)
        assert isinstance(interview, Interview)

        revised = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="calendar-revision",
            payload={
                "summary": "Hiring manager interview",
                "start": {"dateTime": "2026-07-15T11:00:00-07:00"},
                "end": {"dateTime": "2026-07-15T12:00:00-07:00"},
                "location": "https://meet.example/revised",
            },
        )
        session.add(revised)
        session.flush()
        same_interview = apply_external_suggestion(session, revised, approved=True)
        assert isinstance(same_interview, Interview)
        assert same_interview.id == interview.id
        assert same_interview.starts_at == datetime(
            2026, 7, 15, 18, tzinfo=timezone.utc
        )
        assert same_interview.location_or_link == "https://meet.example/revised"

        cancelled = ExternalSuggestion(
            kind=SuggestionKind.CALENDAR_INTERVIEW,
            application_id=application.id,
            external_event_id="calendar-revision",
            payload={"cancelled": True},
        )
        session.add(cancelled)
        session.flush()
        removed = apply_external_suggestion(session, cancelled, approved=True)
        assert isinstance(removed, Interview)

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Interview)) == 0
        actions = set(
            session.scalars(
                select(AuditEvent.action).where(
                    AuditEvent.action.in_(
                        {
                            "interview.rescheduled_from_calendar",
                            "interview.calendar_cancellation_applied",
                        }
                    )
                )
            )
        )
        assert actions == {
            "interview.rescheduled_from_calendar",
            "interview.calendar_cancellation_applied",
        }
    database.dispose()


def test_follow_up_requires_an_explicit_timestamp_then_updates_only_local_state(
    tmp_path,
):
    database = _database(tmp_path)
    due_at = datetime(2026, 7, 20, 16, tzinfo=timezone.utc)
    with database.session() as session:
        application = create_application(session, _job(session).id)
        ambiguous = ExternalSuggestion(
            kind=SuggestionKind.FOLLOW_UP_NEEDED,
            application_id=application.id,
            payload={"evidence": "we will be in touch", "preview_only": True},
        )
        explicit = ExternalSuggestion(
            kind=SuggestionKind.FOLLOW_UP_NEEDED,
            application_id=application.id,
            payload={"follow_up_at": "2026-07-20T09:00:00-07:00"},
        )
        session.add_all([ambiguous, explicit])
        session.flush()
        ambiguous_id = ambiguous.id
        explicit_id = explicit.id
        application_id = application.id

        with pytest.raises(ValueError, match="will not invent a deadline"):
            apply_external_suggestion(session, ambiguous, approved=True)
        assert ambiguous.approval_state == ApprovalState.PENDING
        assert application.follow_up_at is None

        assert apply_external_suggestion(session, explicit, approved=True) is None
        assert application.follow_up_at == due_at
        with pytest.raises(ValueError, match="already been reviewed"):
            apply_external_suggestion(session, explicit, approved=True)

    with database.session() as session:
        ambiguous = session.get(ExternalSuggestion, ambiguous_id)
        explicit = session.get(ExternalSuggestion, explicit_id)
        application = session.get(Application, application_id)
        assert ambiguous.approval_state == ApprovalState.PENDING
        assert explicit.approval_state == ApprovalState.APPROVED
        assert application.follow_up_at == due_at
        follow_up_audits = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "application.follow_up_scheduled",
                    AuditEvent.entity_id == application_id,
                )
            )
        )
        approval_audits = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "suggestion.approved",
                    AuditEvent.entity_id == explicit_id,
                )
            )
        )
        assert len(follow_up_audits) == 1
        assert follow_up_audits[0].before_json == {"follow_up_at": None}
        assert follow_up_audits[0].after_json == {"follow_up_at": due_at.isoformat()}
        assert len(approval_audits) == 1
        assert approval_audits[0].after_json["follow_up_at"] == due_at.isoformat()
    database.dispose()
