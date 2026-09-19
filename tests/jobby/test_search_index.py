"""Derived trigram search-index rebuild and synchronization coverage."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

from sqlalchemy import func, select

from jobby import cli
from jobby.config import AppConfig, JobbyPaths
from jobby.db import Database
from jobby.doctor import run_doctor
from jobby.job_queries import JobListFilters, query_jobs
from jobby.models import Company, Job, SearchIndexChange
from jobby.search_index import SearchIndex, invalidate_search_index


def _database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "jobby.sqlite3")
    database.initialize()
    return database


def _add_job(
    database: Database,
    job_id: str,
    *,
    company: str = "Example",
    title: str = "Policy Counsel",
    description: str = "Responsible technology governance",
    category: str = "Legal Technology",
) -> None:
    with database.session() as session:
        normalized = company.casefold()
        record = session.scalar(
            select(Company).where(Company.normalized_name == normalized)
        )
        if record is None:
            record = Company(name=company, normalized_name=normalized)
            session.add(record)
            session.flush()
        session.add(
            Job(
                id=job_id,
                company_id=record.id,
                title=title,
                normalized_title=title.casefold(),
                description=description,
                category=category,
            )
        )


def _ids(database: Database, query: str) -> list[str]:
    with database.session() as session:
        return [item.id for item in query_jobs(session, JobListFilters(query=query))]


def test_full_rebuild_and_incremental_insert_update_delete(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", description="healthcare contracts")
    index = SearchIndex(database)

    status = index.rebuild()
    assert status.indexed_jobs == 1
    assert status.pending_changes == 0
    assert _ids(database, "healthcare") == ["one"]

    _add_job(database, "two", description="renewable energy markets")
    assert _ids(database, "renewable") == ["two"]
    with database.session() as session:
        session.get(Job, "one").description = "copyright licensing"
    assert _ids(database, "healthcare") == []
    assert _ids(database, "copyright") == ["one"]

    with database.session() as session:
        session.delete(session.get(Job, "two"))
    assert _ids(database, "renewable") == []
    assert index.status().indexed_jobs == 1
    database.dispose()


def test_company_rename_is_queued_and_reindexed(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", company="Old Harbor", description="general counsel")
    assert _ids(database, "Old Harbor") == ["one"]

    with database.session() as session:
        company = session.scalar(select(Company).where(Company.name == "Old Harbor"))
        company.name = "New Harbor"

    assert _ids(database, "Old Harbor") == []
    assert _ids(database, "New Harbor") == ["one"]
    database.dispose()


def test_interrupted_queue_cleanup_is_idempotent(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", description="first description")
    index = SearchIndex(database)
    index.rebuild()
    with database.session() as session:
        session.get(Job, "one").description = "second description"

    with database.session() as session:
        retained = session.scalar(select(SearchIndexChange))
        assert retained is not None
        retained_job_id = retained.job_id
        retained_generation = retained.generation
    index.synchronize()
    # Simulate process loss after the durable cache commit but before queue
    # acknowledgement by restoring the exact retained generation.
    with database.session() as session:
        session.add(
            SearchIndexChange(
                job_id=retained_job_id,
                generation=retained_generation,
            )
        )
        assert session.scalar(select(func.count()).select_from(SearchIndexChange)) == 1

    assert _ids(database, "second") == ["one"]
    assert index.status().pending_changes == 0
    database.dispose()


def test_corrupt_cache_recovers_from_operational_truth(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", description="antitrust compliance")
    index = SearchIndex(database)
    index.rebuild()
    index.path.write_bytes(b"not a sqlite database")

    assert _ids(database, "antitrust") == ["one"]
    assert index.status().integrity_ok is True
    database.dispose()


def test_index_lock_serializes_cache_writers(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one")
    index = SearchIndex(database)
    index.rebuild()
    finished = threading.Event()

    def synchronize() -> None:
        SearchIndex(database).synchronize()
        finished.set()

    with index._lock():
        worker = threading.Thread(target=synchronize)
        worker.start()
        assert finished.wait(timeout=0.1) is False
    worker.join(timeout=5)

    assert finished.is_set()
    assert index.status().synchronized is True
    database.dispose()


def test_generation_mismatch_without_queue_forces_rebuild(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", description="privacy compliance")
    index = SearchIndex(database)
    index.rebuild()
    with sqlite3.connect(index.path) as connection:
        connection.execute(
            "UPDATE search_metadata SET value = '0' WHERE key = 'applied_generation'"
        )

    assert index.status().synchronized is False
    assert _ids(database, "privacy") == ["one"]
    assert index.status().synchronized is True
    database.dispose()


def test_short_punctuation_unicode_and_injection_terms_remain_literal(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    _add_job(
        database,
        "one",
        title="C++ AI 政策分析 Counsel",
        description="Percent%Works underscore_value",
    )

    assert _ids(database, "AI") == ["one"]  # short-term LIKE fallback
    assert _ids(database, "C++") == ["one"]
    assert _ids(database, "政策分析") == ["one"]
    assert _ids(database, "Percent%") == ["one"]
    assert _ids(database, '" OR 1=1 --') == []
    database.dispose()


def test_fts_prefilter_preserves_literal_unicode_case_semantics(
    tmp_path: Path, monkeypatch
) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", company="CAFÉ GROUP", description="general counsel")

    # SQLite's long-standing lower()+LIKE behavior is ASCII-only. The optional
    # Unicode-aware trigram prefilter must not alter that result set.
    assert _ids(database, "café") == []
    monkeypatch.setattr(
        SearchIndex,
        "fts5_available",
        staticmethod(lambda: (False, "disabled for comparison")),
    )
    assert _ids(database, "café") == []
    database.dispose()


def test_unavailable_or_excessive_fts_falls_back(tmp_path: Path, monkeypatch) -> None:
    database = _database(tmp_path)
    _add_job(database, "one", description="shared governance")
    _add_job(database, "two", description="shared governance")
    monkeypatch.setattr(
        SearchIndex,
        "fts5_available",
        staticmethod(lambda: (False, "disabled for test")),
    )
    assert set(_ids(database, "governance")) == {"one", "two"}
    monkeypatch.undo()

    with database.session() as session:
        assert (
            SearchIndex(database, excessive_match_limit=1).candidate_job_ids(
                session, query_terms=("governance",)
            )
            is None
        )
    database.dispose()


def test_cli_search_index_status_and_rebuild(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = tmp_path / "home"
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
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)

    assert cli.main(["search-index", "status"]) == 0
    assert '"exists": false' in capsys.readouterr().out
    assert cli.main(["search-index", "rebuild"]) == 0
    rebuilt = capsys.readouterr().out
    assert '"integrity_ok": true' in rebuilt
    assert '"indexed_jobs": 0' in rebuilt


def test_doctor_reports_fts_integrity_count_and_pending_changes(tmp_path: Path) -> None:
    root = tmp_path / "doctor-home"
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
    database = Database(paths=paths)
    database.initialize()
    _add_job(database, "one")
    SearchIndex(database).rebuild()

    class MissingSecrets:
        def status(self, _name: str):
            return SimpleNamespace(state="missing")

        def get(self, _name: str):
            return None

    checks = run_doctor(
        database,
        AppConfig(openai_enabled=False, google_enabled=False),
        paths,
        secrets=MissingSecrets(),
        check_network=False,
    )
    search_checks = {
        check.name: check for check in checks if check.name.startswith("search_index_")
    }
    assert set(search_checks) == {
        "search_index_fts",
        "search_index_integrity",
        "search_index_count",
        "search_index_pending",
    }
    assert all(check.status == "pass" for check in search_checks.values())
    database.dispose()


def test_restore_invalidation_helper_removes_cache_and_sidecars(tmp_path: Path) -> None:
    root = tmp_path / "restore-home"
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
    cache = paths.cache_dir / "search-index.sqlite3"
    candidates = [cache, Path(f"{cache}-wal"), Path(f"{cache}-shm")]
    for candidate in candidates:
        candidate.write_bytes(b"derived")

    invalidate_search_index(paths)

    assert all(not candidate.exists() for candidate in candidates)
