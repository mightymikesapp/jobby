from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import threading
import time
import zipfile

import pytest

import jobby.doctor as doctor_module
from jobby.config import AppConfig, JobbyPaths, SourceSettings
from jobby.db import Database
from jobby.doctor import _database_growth_check, _operational_checks
from jobby.enums import AgentRunStatus
from jobby.models import AgentRun, BackupRecord, SourceConfig, SourceHealth


def _paths(root: Path) -> JobbyPaths:
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()


@pytest.fixture
def database(tmp_path: Path) -> Database:
    value = Database(paths=_paths(tmp_path / "home"), acquire_lock=False)
    value.initialize()
    yield value
    value.dispose()


def _minimal_sources() -> SourceSettings:
    return SourceSettings(
        greenhouse={"acme": "Acme"},
        lever={},
        ashby={},
        workable={},
        workday={},
        usajobs_locations=[],
    )


def test_doctor_reports_configured_sources_that_have_never_been_attempted(
    database: Database,
) -> None:
    now = datetime.now(timezone.utc)
    config = AppConfig(sources=_minimal_sources())
    with database.session() as session:
        session.add_all(
            [
                SourceHealth(
                    source="greenhouse:acme",
                    last_attempt_at=now,
                    last_success_at=now,
                    anomaly_state="healthy",
                ),
                SourceConfig(
                    provider="static_http",
                    name="Acme Careers",
                    enabled=True,
                    config_json={"careers_url": "https://jobs.example.test/"},
                ),
            ]
        )

    by_name = {
        check.name: check
        for check in _operational_checks(database, config, database.paths)
    }
    assert by_name["source_attempts"].status == "warn"
    assert "portal:acme-careers" in by_name["source_attempts"].message
    assert "greenhouse:acme" not in by_name["source_attempts"].message

    with database.session() as session:
        session.add(
            SourceHealth(
                source="portal:acme-careers",
                last_attempt_at=now,
                last_success_at=now,
                anomaly_state="healthy",
            )
        )
    by_name = {
        check.name: check
        for check in _operational_checks(database, config, database.paths)
    }
    assert by_name["source_attempts"].status == "pass"
    assert by_name["source_attempts"].message == (
        "0 configured source(s) never attempted"
    )


def test_doctor_reports_each_installed_scheduler_cadence_and_staleness(
    database: Database,
) -> None:
    now = datetime.now(timezone.utc)
    with database.session() as session:
        session.add_all(
            [
                AgentRun(
                    status=AgentRunStatus.SUCCEEDED,
                    started_at=now - timedelta(hours=1),
                    finished_at=now - timedelta(minutes=59),
                    summary={
                        "run_mode": "focused",
                        "recovery": {"attempted": True},
                    },
                ),
                AgentRun(
                    status=AgentRunStatus.SUCCEEDED,
                    started_at=now - timedelta(days=9),
                    finished_at=now - timedelta(days=9),
                    summary={
                        "run_mode": "inventory",
                        "recovery": {"attempted": True},
                    },
                ),
            ]
        )

    checks = _operational_checks(
        database,
        AppConfig(sources=_minimal_sources()),
        database.paths,
        scheduler_enabled=True,
    )
    scheduler = next(check for check in checks if check.name == "scheduler_runs")
    assert scheduler.status == "warn"
    assert "focused: succeeded" in scheduler.message
    assert "inventory: succeeded, 9 day(s) ago (stale)" in scheduler.message
    assert set(json.loads(json.dumps(asdict(scheduler)))) == {
        "name",
        "status",
        "message",
    }


def _backup_archive(path: Path, database_size: int) -> tuple[int, str]:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("database/jobby.sqlite3", b"x" * database_size)
    payload = path.read_bytes()
    return len(payload), hashlib.sha256(payload).hexdigest()


def test_doctor_database_growth_uses_only_unchanged_verified_backup_records(
    database: Database,
) -> None:
    now = datetime.now(timezone.utc)
    old_path = database.paths.backups_dir / "old.zip"
    new_path = database.paths.backups_dir / "new.zip"
    old_size, old_hash = _backup_archive(old_path, 1_024)
    new_size, new_hash = _backup_archive(new_path, 4_096)
    with database.session() as session:
        session.add_all(
            [
                BackupRecord(
                    backup_kind="scheduled_local",
                    path=str(old_path),
                    plaintext_sha256=old_hash,
                    size_bytes=old_size,
                    verified_at=now - timedelta(days=7),
                    external=False,
                ),
                BackupRecord(
                    backup_kind="scheduled_local",
                    path=str(new_path),
                    plaintext_sha256=new_hash,
                    size_bytes=new_size,
                    verified_at=now,
                    external=False,
                ),
            ]
        )

    trend = _database_growth_check(database)
    assert trend.status == "pass"
    assert "grew by 3,072 bytes over 7 day(s)" in trend.message

    old_path.write_bytes(old_path.read_bytes() + b"changed")
    unavailable = _database_growth_check(database)
    assert unavailable.status == "warn"
    assert "fewer than two unchanged verified" in unavailable.message


def test_doctor_bounds_a_keyring_lookup_that_waits_for_ui(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()

    class BlockingSecrets:
        def status(self, _name: str) -> object:
            release.wait(timeout=5)
            return object()

    monkeypatch.setattr(doctor_module, "KEYRING_STATUS_TIMEOUT_SECONDS", 0.01)
    started = time.perf_counter()
    try:
        assert doctor_module._secret_status(BlockingSecrets(), "test") == "unavailable"
        assert time.perf_counter() - started < 0.5
    finally:
        release.set()
