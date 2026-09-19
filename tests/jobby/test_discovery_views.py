from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.discovery_views import (
    create_saved_view,
    delete_saved_view,
    filters_for_view,
    list_saved_views,
    mark_view_reviewed,
    update_saved_view,
)
from jobby.enums import JobStatus
from jobby.job_queries import JobListFilters, JobSort, query_jobs_page
from jobby.models import Company, DiscoveryReviewCursor, Job, SavedDiscoveryView


def _database(root: Path) -> Database:
    paths = JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()
    value = Database(paths=paths, acquire_lock=False)
    value.initialize()
    return value


def test_saved_view_round_trip_and_new_since_cursor(tmp_path: Path) -> None:
    database = _database(tmp_path)
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    try:
        with database.session() as session:
            company = Company(name="Example", normalized_name="example")
            session.add(company)
            session.flush()
            jobs = []
            for index, discovered in enumerate(
                (base, base + timedelta(days=1), base + timedelta(days=2))
            ):
                job = Job(
                    company_id=company.id,
                    title=f"Policy counsel {index}",
                    normalized_title=f"policy counsel {index}",
                    source_primary="manual",
                    source_id=str(index),
                    status=JobStatus.DISCOVERED,
                    discovered_at=discovered,
                )
                session.add(job)
                jobs.append(job)
            session.flush()
            view = create_saved_view(
                session,
                name="Policy inbox",
                filters=JobListFilters(
                    query="policy", sort=JobSort.NEWEST, limit=50, offset=20
                ),
            )
            assert view.filters.limit == 500
            mark_view_reviewed(session, view.id, job_id=jobs[1].id)

        with database.session() as session:
            current = filters_for_view(
                session,
                "POLICY INBOX",
                new_since_last_review=True,
                limit=25,
            )
            page = query_jobs_page(session, current)
            assert [row.title for row in page.items] == ["Policy counsel 2"]
            assert current.sort is JobSort.NEWEST
            assert len(list_saved_views(session)) == 1
            updated = update_saved_view(
                session,
                view.id,
                name="Policy review",
                filters=JobListFilters(company="Example", sort=JobSort.COMPANY),
            )
            assert updated.reviewed_job_id == jobs[1].id
            assert delete_saved_view(session, updated.id) == updated.id

        with database.session() as session:
            assert session.scalar(select(SavedDiscoveryView.id)) is None
            assert session.scalar(select(DiscoveryReviewCursor.id)) is None
    finally:
        database.dispose()


def test_cursor_never_moves_backwards(tmp_path: Path) -> None:
    database = _database(tmp_path)
    try:
        with database.session() as session:
            view = create_saved_view(session, name="All", filters=JobListFilters())
            later = datetime(2026, 7, 10, tzinfo=timezone.utc)
            mark_view_reviewed(session, view.id, reviewed_through=later)
            record = mark_view_reviewed(
                session,
                view.id,
                reviewed_through=later - timedelta(days=2),
            )
            assert record.reviewed_through == later
    finally:
        database.dispose()


def test_stale_session_cannot_overwrite_a_newer_review_cursor(tmp_path: Path) -> None:
    database = _database(tmp_path)
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    stale = None
    try:
        with database.session() as session:
            view = create_saved_view(session, name="All", filters=JobListFilters())
            mark_view_reviewed(session, view.id, reviewed_through=base)

        stale = database.Session()
        stale_record = list_saved_views(stale)[0]
        assert stale_record.reviewed_through == base
        # End the read transaction but deliberately retain the stale identity
        # map, matching a long-lived headless service/session boundary.
        stale.commit()

        later = base + timedelta(days=10)
        with database.session() as current:
            mark_view_reviewed(current, view.id, reviewed_through=later)

        record = mark_view_reviewed(
            stale,
            view.id,
            reviewed_through=base + timedelta(days=1),
        )
        stale.commit()
        assert record.reviewed_through == later

        with database.session() as verification:
            persisted = list_saved_views(verification)[0]
            assert persisted.reviewed_through == later
    finally:
        if stale is not None:
            stale.close()
        database.dispose()
