from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

from jobby import cli
from jobby.config import AppConfig, JobbyPaths
from jobby.db import Database
from jobby.enums import AgentRunStatus, ApprovalState
from jobby.maintenance import (
    clean_expired_cache,
    maintenance_status,
    optimize_database,
    recover_stale_runs,
)
import jobby.maintenance as maintenance_module
from jobby.models import AICacheEntry, AIRun, AgentRun, MaintenanceRun, ScanRun


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
def database(tmp_path) -> Database:
    value = Database(tmp_path / "jobby.sqlite3")
    value.initialize()
    yield value
    value.dispose()


def test_status_reports_every_table_without_mutating(database: Database) -> None:
    before = database.path.stat().st_size
    status = maintenance_status(database)

    assert status.database_path == database.path
    assert status.page_count > 0 and status.page_size > 0
    assert {item.table for item in status.tables} >= {
        "jobs",
        "source_observations",
        "evaluation_compaction_ledger",
    }
    assert status.database_bytes == before


def test_recover_stale_runs_preserves_rows_and_audits_result(
    database: Database,
) -> None:
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    with database.session() as session:
        session.add_all(
            [
                ScanRun(
                    status=AgentRunStatus.RUNNING,
                    started_at=now - timedelta(hours=5),
                ),
                AgentRun(
                    status=AgentRunStatus.RUNNING,
                    started_at=now - timedelta(hours=5),
                ),
            ]
        )

    result = recover_stale_runs(
        database,
        stale_after=timedelta(hours=3),
        now=now,
    )

    assert result.changed == 2
    with database.session() as session:
        assert session.scalar(select(ScanRun)).status is AgentRunStatus.FAILED
        assert session.scalar(select(AgentRun)).status is AgentRunStatus.FAILED
        maintenance = session.scalar(select(MaintenanceRun))
        assert maintenance is not None
        assert maintenance.result_json == {"scan_runs": 1, "agent_runs": 1}


def test_expired_cache_is_deactivated_not_deleted(database: Database) -> None:
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    with database.session() as session:
        source = AIRun(
            purpose="extract",
            provider="openai",
            model="test-model",
            prompt_version="v1",
            input_hash="a" * 64,
            approval_state=ApprovalState.APPROVED,
        )
        session.add(source)
        session.flush()
        entry = AICacheEntry(
            provider="openai",
            model="test-model",
            purpose="extract",
            prompt_version="v1",
            output_schema_hash="b" * 64,
            request_hash="c" * 64,
            max_output_tokens=0,
            cache_key="d" * 64,
            output_json={"preserve": True},
            source_ai_run_id=source.id,
            expires_at=now - timedelta(seconds=1),
            active=True,
        )
        session.add(entry)
        session.flush()
        entry_id = entry.id

    result = clean_expired_cache(database, now=now)

    assert result.changed == 1
    with database.session() as session:
        entry = session.get(AICacheEntry, entry_id)
        assert entry is not None
        assert entry.active is False
        assert entry.invalidated_at == now
        assert entry.output_json == {"preserve": True}


def test_optimize_refuses_busy_wal_checkpoint_without_recording_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path / "home")
    paths.database.write_bytes(b"placeholder")
    optimized = False

    class Result:
        def one(self) -> tuple[int, int, int]:
            return (1, 10, 5)

    class Connection:
        def execution_options(self, **_kwargs):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def exec_driver_sql(self, statement: str):
            nonlocal optimized
            if statement == "PRAGMA wal_checkpoint(TRUNCATE)":
                return Result()
            optimized = True
            return None

    class FakeDatabase:
        def __init__(self, *_args, **_kwargs):
            self.engine = self

        def initialize(self) -> None:
            pass

        def connect(self) -> Connection:
            return Connection()

        def dispose(self) -> None:
            pass

    monkeypatch.setattr(maintenance_module, "Database", FakeDatabase)

    with pytest.raises(RuntimeError, match="checkpoint did not complete"):
        optimize_database(paths.database, paths=paths)

    assert optimized is False


def test_mutating_maintenance_cli_requires_exclusive_lock_and_preserves_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _paths(tmp_path / "home")
    database = Database(paths=paths)
    database.initialize()
    old = datetime(2000, 1, 1, tzinfo=timezone.utc)
    with database.session() as session:
        scan = ScanRun(status=AgentRunStatus.RUNNING, started_at=old)
        source = AIRun(
            purpose="extract",
            provider="openai",
            model="test-model",
            prompt_version="v1",
            input_hash="a" * 64,
            approval_state=ApprovalState.APPROVED,
        )
        session.add_all([scan, source])
        session.flush()
        cache = AICacheEntry(
            provider="openai",
            model="test-model",
            purpose="extract",
            prompt_version="v1",
            output_schema_hash="b" * 64,
            request_hash="c" * 64,
            max_output_tokens=0,
            cache_key="d" * 64,
            output_json={"preserve": True},
            source_ai_run_id=source.id,
            expires_at=old,
            active=True,
        )
        session.add(cache)
        session.flush()
        scan_id, cache_id = scan.id, cache.id

    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    monkeypatch.setattr(cli, "load_config", lambda _paths: AppConfig())
    recover_command = [
        "maintenance",
        "recover-stale-runs",
        "--older-than-minutes",
        "1",
        "--lock-timeout",
        "0",
    ]
    clean_command = [
        "maintenance",
        "clean-expired-cache",
        "--lock-timeout",
        "0",
    ]

    assert cli.main(recover_command) == 1
    assert cli.main(clean_command) == 1
    assert capsys.readouterr().err.count("storage is busy") == 2
    with database.session() as session:
        assert session.get(ScanRun, scan_id).status is AgentRunStatus.RUNNING
        assert session.get(AICacheEntry, cache_id).active is True
        assert session.scalar(select(MaintenanceRun)) is None
    database.dispose()

    assert cli.main(recover_command) == 0
    assert cli.main(clean_command) == 0
    verification = Database(paths=paths)
    verification.initialize()
    try:
        with verification.session() as session:
            assert session.get(ScanRun, scan_id).status is AgentRunStatus.FAILED
            entry = session.get(AICacheEntry, cache_id)
            assert entry is not None
            assert entry.active is False
            assert entry.output_json == {"preserve": True}
            assert session.scalar(select(AIRun.id).where(AIRun.id == source.id))
            assert len(tuple(session.scalars(select(MaintenanceRun)))) == 2
    finally:
        verification.dispose()
