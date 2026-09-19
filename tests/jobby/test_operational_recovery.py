from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import threading

import pytest
from sqlalchemy import func, select

from jobby.agent import DailyAgent
from jobby.backup import create_backup
from jobby.config import AppConfig, JobbyPaths
from jobby.db import Database
from jobby.doctor import _operational_checks
from jobby.encrypted_backup import MIN_SCRYPT_LOG_N, _HEADER
from jobby.enums import AgentRunStatus
from jobby.models import AgentRun, Alert, BackupRecord, MaintenanceRun, ScanRun
from jobby.operational_recovery import run_scheduled_recovery
import jobby.operational_recovery as recovery_module


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
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database(paths=_paths(tmp_path / "home"), acquire_lock=False)
    value.initialize()
    yield value
    value.dispose()


class MappingSecrets:
    def __init__(self, values: dict[str, str] | None = None):
        self.values = values or {}

    def get(self, name: str) -> str | None:
        return self.values.get(name)


def test_scheduled_focused_backup_is_verified_rotated_and_rehearsed(
    database: Database,
) -> None:
    now = datetime(2026, 7, 14, 14, tzinfo=timezone.utc)

    result = run_scheduled_recovery(
        database,
        AppConfig(),
        secrets=MappingSecrets(),  # type: ignore[arg-type]
        mode="focused",
        succeeded=True,
        now=now,
    )

    assert result.attempted is True
    assert result.local_backup is not None and result.local_backup.exists()
    assert result.encrypted_backup is None
    assert result.restore_rehearsal == "succeeded"
    with database.session() as session:
        backup = session.scalar(select(BackupRecord))
        assert backup is not None
        assert backup.backup_kind == "scheduled_local"
        assert backup.external is False
        rehearsal = session.scalar(
            select(MaintenanceRun).where(
                MaintenanceRun.kind == "monthly_restore_rehearsal"
            )
        )
        assert rehearsal is not None and rehearsal.status == "succeeded"
        assert rehearsal.result_json["temporary_home_removed"] is True
    doctor = {
        check.name: check
        for check in _operational_checks(
            database,
            AppConfig(),
            database.paths,
            now=now,
        )
    }
    assert doctor["restore_rehearsal"].status == "pass"


def test_missing_external_media_reuses_one_alert_and_keeps_local_replacements(
    database: Database,
    tmp_path: Path,
) -> None:
    missing_media = tmp_path / "not-mounted"
    config = AppConfig(external_backup_destination=missing_media)
    sunday = datetime(2026, 7, 12, 14, tzinfo=timezone.utc)

    first = run_scheduled_recovery(
        database,
        config,
        secrets=MappingSecrets(),  # type: ignore[arg-type]
        mode="inventory",
        succeeded=True,
        now=sunday,
    )
    second = run_scheduled_recovery(
        database,
        config,
        secrets=MappingSecrets(),  # type: ignore[arg-type]
        mode="inventory",
        succeeded=True,
        now=sunday,
    )

    assert not missing_media.exists()
    assert first.local_backup is not None
    assert second.local_backup is not None and second.local_backup.exists()
    assert first.encrypted_backup is None and second.encrypted_backup is None
    with database.session() as session:
        alerts = tuple(session.scalars(select(Alert)))
        assert len(alerts) == 1
        assert alerts[0].recurrence_count == 2
        assert alerts[0].resolved_at is None
        assert (
            session.scalar(
                select(func.count(BackupRecord.id)).where(
                    BackupRecord.external.is_(False)
                )
            )
            == 2
        )


def test_inventory_catch_up_encrypts_to_present_media_regardless_of_weekday(
    database: Database,
    tmp_path: Path,
) -> None:
    media = tmp_path / "mounted-media"
    media.mkdir()
    config = AppConfig(
        external_backup_destination=media,
        external_backup_scrypt_n=16_384,
    )
    # Tuesday is not the configured Sunday inventory day. An explicit
    # inventory invocation is authoritative because it may be a catch-up run.
    catch_up = datetime(2026, 7, 14, 14, tzinfo=timezone.utc)

    result = run_scheduled_recovery(
        database,
        config,
        secrets=MappingSecrets(
            {"external_backup_passphrase": "retained-test-passphrase"}
        ),  # type: ignore[arg-type]
        mode="inventory",
        succeeded=True,
        now=catch_up,
    )

    assert result.encrypted_backup is not None
    assert result.encrypted_backup.parent == media
    assert result.encrypted_backup.exists()
    with result.encrypted_backup.open("rb") as handle:
        fields = _HEADER.unpack(handle.read(_HEADER.size))
    assert fields[3] == MIN_SCRYPT_LOG_N
    with database.session() as session:
        external = session.scalar(
            select(BackupRecord).where(BackupRecord.external.is_(True))
        )
        assert external is not None
        assert external.recovery_tested_at == catch_up
        assert external.backup_kind == "scheduled_encrypted_external"
    doctor = {
        check.name: check
        for check in _operational_checks(
            database,
            config,
            database.paths,
            secrets=MappingSecrets(
                {"external_backup_passphrase": "retained-test-passphrase"}
            ),  # type: ignore[arg-type]
            now=catch_up,
        )
    }
    assert doctor["external_backup"].status == "pass"

    tampered = bytearray(result.encrypted_backup.read_bytes())
    tampered[-1] ^= 1
    result.encrypted_backup.write_bytes(tampered)
    after_tamper = {
        check.name: check
        for check in _operational_checks(
            database,
            config,
            database.paths,
            secrets=MappingSecrets(
                {"external_backup_passphrase": "retained-test-passphrase"}
            ),  # type: ignore[arg-type]
            now=catch_up,
        )
    }
    assert after_tamper["external_backup"].status == "fail"
    assert "recovery verification failed" in after_tamper["external_backup"].message


def test_scheduled_external_backup_bounds_blocking_keyring_lookup(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    media = tmp_path / "mounted-media"
    media.mkdir()
    release = threading.Event()

    class BlockingSecrets:
        def get(self, _name: str) -> None:
            release.wait(2)
            return None

    monkeypatch.setattr(recovery_module, "SCHEDULED_KEYRING_TIMEOUT_SECONDS", 0.01)
    try:
        result = run_scheduled_recovery(
            database,
            AppConfig(external_backup_destination=media),
            secrets=BlockingSecrets(),  # type: ignore[arg-type]
            mode="inventory",
            succeeded=True,
            now=datetime(2026, 7, 14, 14, tzinfo=timezone.utc),
        )
    finally:
        release.set()

    assert result.local_backup is not None and result.local_backup.exists()
    assert result.encrypted_backup is None
    assert len(result.warnings) == 1
    assert "lookup failed" in result.warnings[0]
    assert "TimeoutError" in result.warnings[0]
    with database.session() as session:
        alert = session.scalar(
            select(Alert).where(Alert.title == "External backup keyring is unavailable")
        )
        assert alert is not None
        assert "keyring" in alert.title.casefold()


def test_unsuccessful_scheduled_run_never_writes_a_backup(
    database: Database,
) -> None:
    result = run_scheduled_recovery(
        database,
        AppConfig(),
        secrets=MappingSecrets(),  # type: ignore[arg-type]
        mode="focused",
        succeeded=False,
    )

    assert result.attempted is False
    assert tuple(database.paths.backups_dir.iterdir()) == ()
    with database.session() as session:
        assert session.scalar(select(func.count(BackupRecord.id))) == 0


def test_doctor_backup_age_rejects_fresh_decoys_and_verifies_the_archive(
    database: Database,
) -> None:
    marker = database.paths.backups_dir / "recent-success.txt"
    marker.write_text("not a backup", encoding="utf-8")
    corrupt = database.paths.backups_dir / "jobby-backup-corrupt.zip"
    corrupt.write_bytes(b"this is not a ZIP archive")

    before = {
        check.name: check
        for check in _operational_checks(database, AppConfig(), database.paths)
    }
    assert before["backup_age"].status == "warn"
    assert "no verified local backup found" in before["backup_age"].message
    assert "invalid ZIP candidate" in before["backup_age"].message

    verified = create_backup(database)
    # A newer invalid candidate must not displace the usable archive, and a
    # symlink must never be followed as backup evidence.
    future = datetime.now(timezone.utc).timestamp() + 30
    os.utime(corrupt, (future, future))
    (database.paths.backups_dir / "alias.zip").symlink_to(verified)

    after = {
        check.name: check
        for check in _operational_checks(database, AppConfig(), database.paths)
    }
    assert after["backup_age"].status == "pass"
    assert "newest verified local backup" in after["backup_age"].message
    assert "newer invalid candidate" in after["backup_age"].message
    assert "unsafe ZIP candidate" in after["backup_age"].message


def test_doctor_fails_latest_failed_agent_and_stale_scan_runs_with_json_shape(
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
                ),
                AgentRun(
                    status=AgentRunStatus.FAILED,
                    started_at=now - timedelta(minutes=1),
                    finished_at=now,
                    error="scheduler failed after the previous success",
                ),
                ScanRun(
                    status=AgentRunStatus.RUNNING,
                    requested_sources=["adversarial-source"],
                    source_results={},
                    started_at=now - timedelta(minutes=16),
                ),
            ]
        )

    checks = _operational_checks(
        database,
        AppConfig(agent_stale_after_minutes=15),
        database.paths,
    )
    by_name = {check.name: check for check in checks}
    assert by_name["agent_runs"].status == "fail"
    assert by_name["agent_runs"].message == "last result: failed"
    assert by_name["scan_runs"].status == "fail"
    assert by_name["scan_runs"].message == "1 stale running scan record(s)"

    # Doctor's CLI JSON path uses dataclass serialization. Keep every new
    # operational check inside the existing name/status/message contract.
    payload = json.loads(json.dumps([asdict(check) for check in checks]))
    assert {"name", "status", "message"} == set(payload[0])
    assert next(item for item in payload if item["name"] == "scan_runs") == {
        "name": "scan_runs",
        "status": "fail",
        "message": "1 stale running scan record(s)",
    }


def test_installed_scheduler_environment_runs_post_cycle_recovery(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JOBBY_AGENT_MODE", "focused")

    run = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),  # type: ignore[arg-type]
    ).run(include_web=False)

    assert run.summary["run_mode"] == "focused"
    assert run.summary["recovery"]["attempted"] is True
    local_backup = Path(str(run.summary["recovery"]["local_backup"]))
    assert local_backup.exists()
