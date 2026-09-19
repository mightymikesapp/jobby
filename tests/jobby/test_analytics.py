from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import event, select

from jobby.analytics import (
    analytics_report,
    funnel_summary,
    render_analytics_markdown,
    score_calibration,
    source_yield,
    stage_duration_summary,
    time_in_stage,
)
from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.enums import ApplicationStage
from jobby.models import Application, Company, Evaluation, Job, StageEvent


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


def test_terminal_application_retains_historical_funnel_progression(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime.now(timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            source_primary="greenhouse",
            source_id="1",
        )
        session.add(job)
        session.flush()
        application = Application(
            job_id=job.id,
            current_stage=ApplicationStage.REJECTED,
            rejection_reason="role filled",
        )
        evaluation = Evaluation(job_id=job.id, score=4.2, is_current=True)
        session.add_all([application, evaluation])
        session.flush()
        session.add_all(
            [
                StageEvent(
                    application_id=application.id,
                    from_stage=ApplicationStage.APPLIED,
                    to_stage=ApplicationStage.INTERVIEW,
                    occurred_at=now - timedelta(days=5),
                ),
                StageEvent(
                    application_id=application.id,
                    from_stage=ApplicationStage.INTERVIEW,
                    to_stage=ApplicationStage.REJECTED,
                    occurred_at=now - timedelta(days=2),
                ),
            ]
        )

    with database.session() as session:
        assert funnel_summary(session)["response_rate"] == 1.0
        assert source_yield(session) == [
            {
                "source": "unknown",
                "applications": 1,
                "responses": 1,
                "response_rate": 1.0,
            }
        ]
        assert score_calibration(session) == []
    database.dispose()


def test_time_in_stage_uses_one_event_query_for_all_applications(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime.now(timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        for index in range(3):
            job = Job(
                company_id=company.id,
                title=f"Role {index}",
                normalized_title=f"role {index}",
                source_primary="manual",
                source_id=str(index),
            )
            session.add(job)
            session.flush()
            application = Application(
                job_id=job.id, current_stage=ApplicationStage.INTERVIEW
            )
            session.add(application)
            session.flush()
            session.add(
                StageEvent(
                    application_id=application.id,
                    from_stage=ApplicationStage.APPLIED,
                    to_stage=ApplicationStage.INTERVIEW,
                    occurred_at=now - timedelta(days=index + 1),
                )
            )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _many) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(database.engine, "before_cursor_execute", capture)
    try:
        with database.session() as session:
            durations = time_in_stage(session)
    finally:
        event.remove(database.engine, "before_cursor_execute", capture)

    assert ApplicationStage.INTERVIEW.value in durations
    assert len(statements) == 1
    database.dispose()


def test_application_time_score_and_all_frozen_sources_drive_analytics(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime.now(timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            source_primary="greenhouse",
            source_id="frozen",
        )
        session.add(job)
        session.flush()
        current = Evaluation(job_id=job.id, score=5.0, is_current=True)
        session.add(current)
        session.flush()
        application = Application(
            job_id=job.id,
            current_stage=ApplicationStage.INTERVIEW,
            applied_evaluation_id=current.id,
            applied_score=2.4,
            applied_ranker_version="deterministic-v2",
            applied_sources=[
                {"source": "greenhouse", "source_job_id": "one"},
                {"source": "workday", "source_job_id": "two"},
                {"source": "workday", "source_job_id": "two"},
            ],
        )
        session.add(application)
        session.flush()
        session.add(
            StageEvent(
                application_id=application.id,
                from_stage=ApplicationStage.APPLIED,
                to_stage=ApplicationStage.INTERVIEW,
                occurred_at=now,
            )
        )

    with database.session() as session:
        assert [row["source"] for row in source_yield(session)] == [
            "greenhouse",
            "workday",
        ]
        assert score_calibration(session) == [
            {"score_bucket": 2, "applications": 1, "progression_rate": 1.0}
        ]
        report = analytics_report(session)
        assert report["sample"] == {
            "applications": 1,
            "minimum": 10,
            "status": "insufficient_sample",
            "snapshot_scores": 1,
            "missing_score_snapshots": 0,
            "score_snapshot_coverage": 1.0,
            "snapshot_sources": 1,
            "missing_source_snapshots": 0,
            "source_snapshot_coverage": 1.0,
        }
    database.dispose()


def test_legacy_analytics_never_uses_mutable_job_or_evaluation_fallbacks(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            source_primary="greenhouse",
            source_id="legacy",
        )
        session.add(job)
        session.flush()
        session.add_all(
            [
                Application(
                    job_id=job.id,
                    current_stage=ApplicationStage.INTERVIEW,
                ),
                Evaluation(job_id=job.id, score=4.9, is_current=True),
            ]
        )

    with database.session() as session:
        report = analytics_report(session)
        assert source_yield(session) == [
            {
                "source": "unknown",
                "applications": 1,
                "responses": 1,
                "response_rate": 1.0,
            }
        ]
        assert score_calibration(session) == []
        assert report["sample"]["snapshot_scores"] == 0
        assert report["sample"]["missing_score_snapshots"] == 1
        assert report["sample"]["score_snapshot_coverage"] == 0.0
        assert report["sample"]["snapshot_sources"] == 0
        assert report["sample"]["missing_source_snapshots"] == 1
        assert report["sample"]["source_snapshot_coverage"] == 0.0
        assert "legacy_score_fallbacks" not in report["sample"]

        job = session.scalar(select(Job))
        evaluation = session.scalar(select(Evaluation))
        assert job is not None
        assert evaluation is not None
        job.source_primary = "workday"
        evaluation.score = 1.1

    with database.session() as session:
        assert [row["source"] for row in source_yield(session)] == ["unknown"]
        assert score_calibration(session) == []
    database.dispose()


def test_stage_percentiles_exclude_right_censored_open_intervals(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
        )
        session.add(job)
        session.flush()
        application = Application(
            job_id=job.id,
            current_stage=ApplicationStage.INTERVIEW,
            created_at=now - timedelta(days=11),
        )
        session.add(application)
        session.flush()
        session.add_all(
            [
                StageEvent(
                    application_id=application.id,
                    from_stage=ApplicationStage.PLANNED,
                    to_stage=ApplicationStage.APPLIED,
                    occurred_at=now - timedelta(days=10),
                ),
                StageEvent(
                    application_id=application.id,
                    from_stage=ApplicationStage.APPLIED,
                    to_stage=ApplicationStage.INTERVIEW,
                    occurred_at=now - timedelta(days=4),
                ),
            ]
        )

    with database.session() as session:
        rows = {row["stage"]: row for row in stage_duration_summary(session, now=now)}
    assert rows["applied"]["median_days"] == 6.0
    assert rows["applied"]["censored"] == 0
    assert rows["interview"]["completed"] == 0
    assert rows["interview"]["censored"] == 1
    assert rows["interview"]["median_days"] is None
    assert rows["interview"]["longest_open_days"] == 4.0
    database.dispose()


def test_report_hides_small_samples_and_renders_tables_at_ten_applications(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            source_primary="manual",
        )
        session.add(job)
        session.flush()
        session.add_all(Application(job_id=job.id) for _index in range(9))

    with database.session() as session:
        report = analytics_report(session)
        content = render_analytics_markdown(report)
    assert report["sample"]["status"] == "insufficient_sample"
    assert "Insufficient sample: 9 of 10" in content
    assert "```json" not in content

    with database.session() as session:
        job_id = session.scalar(select(Job.id))
        session.add(Application(job_id=job_id))
    with database.session() as session:
        content = render_analytics_markdown(analytics_report(session))
    assert "| Stage | Applications | Distribution |" in content
    assert "Open intervals are right-censored" in content
    assert "```json" not in content
    database.dispose()


def test_source_labels_aggregate_case_insensitively_and_markdown_cells_are_safe(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
        )
        session.add(job)
        session.flush()
        for index in range(10):
            session.add(
                Application(
                    job_id=job.id,
                    current_stage=ApplicationStage.REJECTED,
                    rejection_reason="agency | mismatch\nreview",
                    applied_sources=[
                        {
                            "source": "WorkDay" if index % 2 else "workday",
                            "source_job_id": str(index),
                        }
                    ],
                )
            )

    with database.session() as session:
        sources = source_yield(session)
        content = render_analytics_markdown(analytics_report(session))

    assert len(sources) == 1
    assert sources[0]["source"].casefold() == "workday"
    assert sources[0]["applications"] == 10
    assert "agency \\| mismatch review" in content
    assert "agency | mismatch\nreview" not in content
    database.dispose()
