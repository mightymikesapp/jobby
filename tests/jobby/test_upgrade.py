from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

import jobby.upgrade as upgrade_module
from jobby.db import StorageBusyError, StorageLock
from jobby.upgrade import (
    UPGRADE_JOURNAL_FILENAME,
    UpgradeError,
    apply_upgrade,
    plan_upgrade,
    recover_upgrade,
    upgrade_status,
)


MIGRATIONS = Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"
PRE_UPGRADE_REVISION = "0004_search_ai_cache"


def _config(path: Path) -> Config:
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS))
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+pysqlite:///{path}".replace("%", "%%")
    )
    return config


def _revisions() -> tuple[str, str]:
    scripts = ScriptDirectory.from_config(_config(Path("unused.sqlite3")))
    head = scripts.get_current_head()
    assert head is not None
    pending = tuple(scripts.iterate_revisions(head, PRE_UPGRADE_REVISION))
    assert pending, "upgrade tests require a release after the pre-upgrade baseline"
    return head, PRE_UPGRADE_REVISION


def _database_at(path: Path, revision: str) -> None:
    command.upgrade(_config(path), revision)


def _revision(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(row[0])


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_marker(path: Path, marker: str = "upgrade-marker") -> None:
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO companies(id, name, normalized_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (marker, "Upgrade Marker", marker, now, now),
        )


def _marker_exists(path: Path, marker: str = "upgrade-marker") -> bool:
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT 1 FROM companies WHERE id = ?", (marker,)
        ).fetchone()
    return row == (1,)


def test_status_and_plan_are_read_only_for_fresh_current_old_and_future(
    tmp_path: Path,
) -> None:
    head, predecessor = _revisions()
    missing = tmp_path / "missing.sqlite3"

    fresh = upgrade_status(missing)
    assert fresh.state == "fresh"
    assert fresh.current_revision is None
    assert fresh.target_revision == head
    assert plan_upgrade(missing).action == "bootstrap"
    assert not missing.exists()

    empty = tmp_path / "empty.sqlite3"
    empty.write_bytes(b"")
    assert upgrade_status(empty).state == "fresh"
    assert empty.read_bytes() == b""

    current_path = tmp_path / "current.sqlite3"
    _database_at(current_path, head)
    current_hash = _sha256(current_path)
    current = upgrade_status(current_path)
    assert current.state == "current"
    assert current.current_revision == head
    assert plan_upgrade(current_path).action == "none"
    assert _sha256(current_path) == current_hash

    old_path = tmp_path / "old.sqlite3"
    _database_at(old_path, predecessor)
    old_hash = _sha256(old_path)
    old = upgrade_status(old_path)
    assert old.state == "upgrade_required"
    assert old.current_revision == predecessor
    assert old.pending_revisions[-1] == head
    old_plan = plan_upgrade(old_path)
    assert old_plan.action == "upgrade"
    assert old_plan.snapshot_required is True
    assert old_plan.rehearsal_required is True
    assert _sha256(old_path) == old_hash

    future_path = tmp_path / "future.sqlite3"
    _database_at(future_path, head)
    with sqlite3.connect(future_path) as connection:
        connection.execute(
            "UPDATE alembic_version SET version_num = '9999_future_release'"
        )
    future_hash = _sha256(future_path)
    future = upgrade_status(future_path)
    assert future.state == "future"
    assert future.current_revision == "9999_future_release"
    assert plan_upgrade(future_path).action == "blocked"
    assert _sha256(future_path) == future_hash


def test_existing_unversioned_database_is_invalid_not_fresh(tmp_path: Path) -> None:
    path = tmp_path / "unversioned.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE personal_data(value TEXT)")
        connection.execute("INSERT INTO personal_data VALUES ('preserve me')")
    before = _sha256(path)

    status = upgrade_status(path)

    assert status.state == "invalid"
    assert "no Alembic revision" in status.detail
    assert plan_upgrade(path).action == "blocked"
    assert _sha256(path) == before


def test_apply_snapshots_rehearses_journals_verifies_and_can_recover_completed(
    tmp_path: Path,
) -> None:
    head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    backups = tmp_path / "verified-backups"
    journal_path = tmp_path / UPGRADE_JOURNAL_FILENAME
    _database_at(path, predecessor)
    _seed_marker(path)
    phases: list[str] = []

    def observe_phase(phase: str, journal: Mapping[str, object]) -> None:
        phases.append(phase)
        persisted = json.loads(journal_path.read_text(encoding="utf-8"))
        assert persisted["phase"] == phase
        assert journal.get("phase") == phase
        if phase in {"snapshot_verified", "rehearsal_verified", "applying"}:
            assert _revision(path) == predecessor
        if phase == "snapshot_verified":
            snapshot = Path(persisted["snapshot_path"])
            assert snapshot.is_file()
            assert _revision(snapshot) == predecessor
            assert persisted["snapshot_sha256"] == _sha256(snapshot)

    result = apply_upgrade(path, backup_dir=backups, phase_hook=observe_phase)

    assert result.applied is True
    assert phases == [
        "snapshot_verified",
        "rehearsal_verified",
        "applying",
        "live_migrated",
        "verified",
        "complete",
    ]
    assert _revision(path) == head
    assert _marker_exists(path)
    assert result.snapshot_path is not None and result.snapshot_path.is_file()
    assert result.snapshot_path.parent == backups
    assert result.snapshot_sha256 == _sha256(result.snapshot_path)
    assert result.rehearsal_schema_sha256 == result.live_schema_sha256
    assert result.plan.status.state == "current"
    assert result.plan.status.journal_phase == "complete"
    assert result.plan.status.recovery_required is False
    if os.name == "posix":
        assert result.snapshot_path.stat().st_mode & 0o777 == 0o600
        assert journal_path.stat().st_mode & 0o777 == 0o600

    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    assert journal["phase"] == "complete"
    assert journal["rehearsal_schema_sha256"] == journal["live_schema_sha256"]
    _seed_marker(path, "post-upgrade-marker")

    recovered = recover_upgrade(path)

    assert recovered.recovered is True
    assert recovered.restored_revision == predecessor
    assert recovered.snapshot_path == result.snapshot_path
    assert result.snapshot_path.is_file(), "verified rollback snapshots are retained"
    assert recovered.rescue_snapshot_path is not None
    assert recovered.rescue_snapshot_path.is_file()
    assert recovered.rescue_snapshot_path.parent == backups
    assert recovered.rescue_snapshot_sha256 == _sha256(recovered.rescue_snapshot_path)
    assert _revision(recovered.rescue_snapshot_path) == head
    assert _marker_exists(recovered.rescue_snapshot_path, "post-upgrade-marker")
    assert _revision(path) == predecessor
    assert _marker_exists(path)
    assert not _marker_exists(path, "post-upgrade-marker")
    assert not journal_path.exists()
    assert upgrade_status(path).state == "upgrade_required"


def test_failure_after_live_migration_requires_and_supports_recovery(
    tmp_path: Path,
) -> None:
    head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    journal_path = tmp_path / UPGRADE_JOURNAL_FILENAME
    _database_at(path, predecessor)
    _seed_marker(path)

    def fail_after_live_migration(phase: str, _journal: Mapping[str, object]) -> None:
        if phase == "live_migrated":
            raise RuntimeError("injected interruption after live migration")

    with pytest.raises(RuntimeError, match="injected interruption"):
        apply_upgrade(path, phase_hook=fail_after_live_migration)

    assert _revision(path) == head
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    assert journal["phase"] == "live_migrated"
    status = upgrade_status(path)
    assert status.state == "current"
    assert status.recovery_required is True
    assert plan_upgrade(path).action == "recover"

    result = recover_upgrade(path)

    assert result.recovered is True
    assert _revision(path) == predecessor
    assert _marker_exists(path)
    assert not journal_path.exists()


def test_rehearsal_interruption_never_mutates_live_database(tmp_path: Path) -> None:
    _head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    journal_path = tmp_path / UPGRADE_JOURNAL_FILENAME
    _database_at(path, predecessor)
    _seed_marker(path)
    live_before = _sha256(path)

    def stop_after_rehearsal(phase: str, _journal: Mapping[str, object]) -> None:
        if phase == "rehearsal_verified":
            raise RuntimeError("injected stop after rehearsal")

    with pytest.raises(RuntimeError, match="stop after rehearsal"):
        apply_upgrade(path, phase_hook=stop_after_rehearsal)

    assert _sha256(path) == live_before
    assert _revision(path) == predecessor
    assert json.loads(journal_path.read_text())["phase"] == "rehearsal_verified"
    assert plan_upgrade(path).action == "recover"
    assert recover_upgrade(path).recovered is True
    assert _revision(path) == predecessor


def test_status_does_not_open_sqlite_while_recovery_is_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    _database_at(path, predecessor)

    def stop_after_snapshot(phase: str, _journal: Mapping[str, object]) -> None:
        if phase == "snapshot_verified":
            raise RuntimeError("injected stop after snapshot")

    with pytest.raises(RuntimeError, match="stop after snapshot"):
        apply_upgrade(path, phase_hook=stop_after_snapshot)

    def fail_if_database_is_opened(_path: Path) -> object:
        raise AssertionError("status opened SQLite while recovery was required")

    monkeypatch.setattr(
        upgrade_module, "_read_database_state", fail_if_database_is_opened
    )

    status = upgrade_status(path)

    assert status.current_revision == predecessor
    assert status.recovery_required is True
    assert plan_upgrade(path).action == "recover"


def test_recovery_rescue_snapshot_is_journaled_before_database_replacement(
    tmp_path: Path,
) -> None:
    head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    journal_path = tmp_path / UPGRADE_JOURNAL_FILENAME
    _database_at(path, predecessor)
    result = apply_upgrade(path)
    _seed_marker(path, "post-upgrade-marker")
    live_before = _sha256(path)

    def stop_after_rescue(phase: str, _journal: Mapping[str, object]) -> None:
        if phase == "recovery_backup_verified":
            raise RuntimeError("injected stop after recovery backup")

    with pytest.raises(RuntimeError, match="stop after recovery backup"):
        recover_upgrade(path, phase_hook=stop_after_rescue)

    persisted = json.loads(journal_path.read_text(encoding="utf-8"))
    rescue_path = Path(persisted["rescue_snapshot_path"])
    assert persisted["phase"] == "recovery_backup_verified"
    assert persisted["rescue_snapshot_sha256"] == _sha256(rescue_path)
    assert _sha256(path) == live_before
    assert _revision(path) == head
    assert _marker_exists(rescue_path, "post-upgrade-marker")
    assert plan_upgrade(path).action == "recover"

    recovered = recover_upgrade(path)

    assert recovered.rescue_snapshot_path == rescue_path
    assert _revision(path) == predecessor
    assert result.snapshot_path is not None and result.snapshot_path.is_file()


def test_recovery_retries_safely_after_database_replacement_interruption(
    tmp_path: Path,
) -> None:
    _head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    journal_path = tmp_path / UPGRADE_JOURNAL_FILENAME
    _database_at(path, predecessor)
    apply_upgrade(path)

    def stop_after_replacement(phase: str, _journal: Mapping[str, object]) -> None:
        if phase == "database_replaced":
            raise RuntimeError("injected stop after database replacement")

    with pytest.raises(RuntimeError, match="stop after database replacement"):
        recover_upgrade(path, phase_hook=stop_after_replacement)

    persisted = json.loads(journal_path.read_text(encoding="utf-8"))
    assert persisted["phase"] == "database_replaced"
    assert plan_upgrade(path).action == "recover"

    recovered = recover_upgrade(path)

    assert recovered.recovered is True
    assert _revision(path) == predecessor
    assert not journal_path.exists()


def test_apply_refuses_invalid_snapshot_and_does_not_run_migration(
    tmp_path: Path,
) -> None:
    _head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    _database_at(path, predecessor)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute(
            """
            INSERT INTO applications(
                job_id, current_stage, id, created_at, updated_at
            ) VALUES ('missing-job', 'planned', 'orphan-app', ?, ?)
            """,
            (now, now),
        )

    with pytest.raises(UpgradeError, match="foreign-key check failed"):
        apply_upgrade(path)

    assert _revision(path) == predecessor
    assert not (tmp_path / UPGRADE_JOURNAL_FILENAME).exists()


def test_apply_requires_exclusive_storage_lock(tmp_path: Path) -> None:
    _head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    _database_at(path, predecessor)

    with StorageLock(tmp_path / ".jobby.lock", exclusive=False):
        with pytest.raises(StorageBusyError, match="storage is busy"):
            apply_upgrade(path)

    assert _revision(path) == predecessor
    assert not (tmp_path / UPGRADE_JOURNAL_FILENAME).exists()


def test_recovery_rejects_a_tampered_snapshot_and_preserves_current_database(
    tmp_path: Path,
) -> None:
    head, predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    _database_at(path, predecessor)
    result = apply_upgrade(path)
    assert result.snapshot_path is not None
    result.snapshot_path.write_bytes(result.snapshot_path.read_bytes() + b"tampered")
    current_hash = _sha256(path)

    with pytest.raises(UpgradeError, match="checksum mismatch"):
        recover_upgrade(path)

    assert _revision(path) == head
    assert _sha256(path) == current_hash
    assert (tmp_path / UPGRADE_JOURNAL_FILENAME).exists()


def test_current_apply_is_an_idempotent_noop_and_recover_without_journal_is_safe(
    tmp_path: Path,
) -> None:
    head, _predecessor = _revisions()
    path = tmp_path / "jobby.sqlite3"
    _database_at(path, head)
    before = _sha256(path)

    result = apply_upgrade(path)

    assert result.applied is False
    assert result.snapshot_path is None
    assert _sha256(path) == before
    assert not (tmp_path / "backups").exists()
    recovery = recover_upgrade(path)
    assert recovery.recovered is False
    assert _sha256(path) == before
