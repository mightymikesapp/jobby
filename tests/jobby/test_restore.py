"""Restore safety, relocation, locking, and rollback regression tests."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select

from jobby.backup import create_backup, verify_backup
from jobby.config import AppConfig, JobbyPaths, load_config, save_config
from jobby.db import Database, StorageBusyError
from jobby.enums import ArtifactKind
from jobby.models import Artifact, AuditEvent, Company, Job
from jobby.restore import (
    ConfigPolicy,
    RestoreError,
    preflight_restore,
    recover_interrupted_restore,
    restore_backup,
)
import jobby.restore as restore_module


def _paths(root: Path, *, ensure: bool = True) -> JobbyPaths:
    data = root / "data"
    config = root / "config"
    cache = root / "cache"
    paths = JobbyPaths(
        data_dir=data,
        config_dir=config,
        cache_dir=cache,
        database=data / "jobby.sqlite3",
        artifacts_dir=data / "artifacts",
        backups_dir=data / "backups",
        logs_dir=data / "logs",
        config_file=config / "config.toml",
    )
    return paths.ensure() if ensure else paths


def _database(paths: JobbyPaths, *, company_name: str) -> Database:
    database = Database(paths=paths)
    database.initialize()
    with database.session() as session:
        company = Company(
            name=company_name,
            normalized_name=company_name.casefold(),
        )
        session.add(company)
        session.flush()
        session.add(
            Job(
                company_id=company.id,
                title="Counsel",
                normalized_title="counsel",
            )
        )
    return database


def _archive(
    root: Path,
    *,
    company_name: str = "Source Co",
    with_artifact: bool = True,
    with_config: bool = True,
) -> tuple[Path, JobbyPaths]:
    paths = _paths(root / "source")
    database = _database(paths, company_name=company_name)
    if with_artifact:
        content = b"immutable source artifact\n"
        artifact_path = paths.artifacts_dir / "source.md"
        artifact_path.write_bytes(content)
        with database.session() as session:
            session.add(
                Artifact(
                    kind=ArtifactKind.SOURCE_REPORT,
                    stored_path=str(artifact_path),
                    content_hash=hashlib.sha256(content).hexdigest(),
                    size_bytes=len(content),
                    source_immutable=True,
                )
            )
    if with_config:
        save_config(AppConfig(salary_floor=123_000), paths)
    output = root / f"{company_name.replace(' ', '-')}.zip"
    try:
        create_backup(database, output=output, paths=paths)
    finally:
        database.dispose()
    assert verify_backup(output) == (True, "ok")
    return output, paths


def _company_names(paths: JobbyPaths) -> list[str]:
    database = Database(paths=paths)
    database.initialize()
    try:
        with database.session() as session:
            return list(session.scalars(select(Company.name).order_by(Company.name)))
    finally:
        database.dispose()


def _rewrite_archive(source: Path, destination: Path, transform) -> Path:
    with zipfile.ZipFile(source) as archive:
        members = {info.filename: archive.read(info) for info in archive.infolist()}
    transform(members)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return destination


def test_preflight_is_default_and_does_not_create_target_home(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)

    result = restore_backup(archive, paths=target)

    assert result.applied is False
    assert result.plan.artifact_count == 1
    assert result.plan.source_revision == result.plan.target_revision
    assert result.plan.config_action == "preserve"
    assert not target.data_dir.exists()
    assert not target.config_dir.exists()


def test_restore_to_new_home_rehomes_artifacts_and_records_audit(
    tmp_path: Path,
) -> None:
    archive, source_paths = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)

    result = restore_backup(
        archive,
        paths=target,
        apply=True,
        config_policy=ConfigPolicy.RESTORE_IF_MISSING,
    )

    assert result.applied is True
    assert result.restored_artifacts == 1
    assert result.pre_restore_backup is None
    assert _company_names(target) == ["Source Co"]
    assert load_config(target).salary_floor == 123_000
    database = Database(paths=target)
    database.initialize()
    try:
        with database.session() as session:
            artifact = session.scalar(select(Artifact))
            audit = session.scalar(
                select(AuditEvent).where(AuditEvent.action == "restore.applied")
            )
        assert artifact is not None
        restored_path = Path(artifact.stored_path)
        assert restored_path.is_relative_to(target.artifacts_dir)
        assert not restored_path.is_relative_to(source_paths.artifacts_dir)
        assert restored_path.read_bytes() == b"immutable source artifact\n"
        assert audit is not None
    finally:
        database.dispose()
    assert stat.S_IMODE(target.database.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.config_file.stat().st_mode) == 0o600


def test_existing_database_requires_explicit_replace_and_gets_verified_backup(
    tmp_path: Path,
) -> None:
    archive, _ = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, company_name="Original Co")
    save_config(AppConfig(salary_floor=77_000), target)
    current.dispose()

    with pytest.raises(RestoreError, match="replace_current=True"):
        restore_backup(archive, paths=target, apply=True)

    result = restore_backup(
        archive,
        paths=target,
        apply=True,
        replace_current=True,
    )

    assert _company_names(target) == ["Replacement Co"]
    assert load_config(target).salary_floor == 77_000
    assert result.pre_restore_backup is not None
    assert verify_backup(result.pre_restore_backup) == (True, "ok")


def test_restore_refuses_to_run_while_a_database_client_is_open(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target")
    current = _database(target, company_name="Busy Co")
    try:
        with pytest.raises(StorageBusyError, match="close other Jobby processes"):
            restore_backup(
                archive,
                paths=target,
                apply=True,
                replace_current=True,
            )
    finally:
        current.dispose()
    assert _company_names(target) == ["Busy Co"]


def test_backup_creation_refuses_to_publish_incomplete_backup(tmp_path: Path) -> None:
    paths = _paths(tmp_path / "source")
    database = _database(paths, company_name="Incomplete Co")
    outside = tmp_path / "outside.md"
    outside.write_text("not managed", encoding="utf-8")
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.SOURCE_REPORT,
                stored_path=str(outside),
                content_hash=hashlib.sha256(outside.read_bytes()).hexdigest(),
                size_bytes=outside.stat().st_size,
            )
        )
    archive = tmp_path / "incomplete.zip"
    try:
        with pytest.raises(
            RuntimeError,
            match=r"backup verification failed: backup is incomplete: 1 managed artifact",
        ):
            create_backup(database, output=archive, paths=paths)
    finally:
        database.dispose()
    assert not archive.exists()


def test_restore_tolerates_only_external_generated_export_omissions(
    tmp_path: Path,
) -> None:
    source = _paths(tmp_path / "source")
    database = _database(source, company_name="Portable Co")
    external = tmp_path / "portable.json"
    content = b'{"schema":"jobby-export-v1"}\n'
    external.write_bytes(content)
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.GENERATED_EXPORT,
                stored_path=str(external),
                content_hash=hashlib.sha256(content).hexdigest(),
                size_bytes=len(content),
                source_immutable=False,
            )
        )
    archive = tmp_path / "portable-backup.zip"
    try:
        create_backup(database, output=archive, paths=source)
    finally:
        database.dispose()

    assert verify_backup(archive) == (True, "ok")
    target = _paths(tmp_path / "target", ensure=False)
    plan = preflight_restore(archive, paths=target)
    assert plan.artifact_count == 0
    assert plan.warnings == (
        "1 external generated export(s) were intentionally excluded",
    )

    restore_backup(archive, paths=target, apply=True)
    restored = Database(paths=target)
    restored.initialize()
    try:
        with restored.session() as session:
            artifact = session.scalar(
                select(Artifact).where(Artifact.kind == ArtifactKind.GENERATED_EXPORT)
            )
        assert artifact is not None
        assert artifact.stored_path is None
    finally:
        restored.dispose()


def test_preflight_cross_checks_manifest_against_database(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path)

    def alter(members: dict[str, bytes]) -> None:
        manifest = json.loads(members["manifest.json"])
        manifest["artifacts"][0]["kind"] = "resume"
        members["manifest.json"] = json.dumps(manifest).encode()

    tampered = _rewrite_archive(archive, tmp_path / "cross-check.zip", alter)
    assert verify_backup(tampered) == (True, "ok")

    with pytest.raises(RestoreError, match="artifact kind mismatch"):
        preflight_restore(tampered, paths=_paths(tmp_path / "target", ensure=False))


def test_preflight_rejects_unknown_future_database_revision(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path, with_artifact=False)

    def alter(members: dict[str, bytes]) -> None:
        database_path = tmp_path / "future.sqlite3"
        database_path.write_bytes(members["database/jobby.sqlite3"])
        with sqlite3.connect(database_path) as connection:
            connection.execute("UPDATE alembic_version SET version_num = '9999_future'")
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        payload = database_path.read_bytes()
        members["database/jobby.sqlite3"] = payload
        manifest = json.loads(members["manifest.json"])
        manifest["database_sha256"] = hashlib.sha256(payload).hexdigest()
        members["manifest.json"] = json.dumps(manifest).encode()

    future = _rewrite_archive(archive, tmp_path / "future.zip", alter)
    assert verify_backup(future) == (True, "ok")

    with pytest.raises(RestoreError, match="newer or unsupported"):
        preflight_restore(future, paths=_paths(tmp_path / "target", ensure=False))


def test_preflight_rehearses_and_apply_migrates_supported_old_database(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path / "source")
    database = _database(paths, company_name="Old Schema Co")
    migration_config = Config()
    migration_config.set_main_option(
        "script_location", str(Path(__file__).parents[2] / "src/jobby/migrations")
    )
    with database.engine.connect() as connection:
        migration_config.attributes["connection"] = connection
        command.downgrade(migration_config, "0002_materials_offers")
    archive = tmp_path / "old-schema.zip"
    try:
        create_backup(database, output=archive, paths=paths)
    finally:
        database.dispose()

    target = _paths(tmp_path / "target", ensure=False)
    plan = preflight_restore(archive, paths=target)
    result = restore_backup(archive, paths=target, apply=True)

    assert plan.source_revision == "0002_materials_offers"
    assert plan.target_revision == "0013_operation_runs"
    assert plan.migration_required is True
    assert result.applied is True
    assert _company_names(target) == ["Old Schema Co"]


def test_config_replace_is_explicit(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target")
    save_config(AppConfig(salary_floor=10), target)

    preserve = preflight_restore(archive, paths=target)
    replace = preflight_restore(
        archive, paths=target, config_policy=ConfigPolicy.REPLACE
    )

    assert preserve.config_action == "preserve"
    assert preserve.warnings
    assert replace.config_action == "replace"


def test_restore_if_missing_rechecks_config_after_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    real_preflight = restore_module.preflight_restore

    def config_appears(*args, **kwargs):
        plan = real_preflight(*args, **kwargs)
        save_config(AppConfig(salary_floor=222_000), target)
        return plan

    monkeypatch.setattr(restore_module, "preflight_restore", config_appears)

    result = restore_backup(
        archive,
        paths=target,
        apply=True,
        config_policy=ConfigPolicy.RESTORE_IF_MISSING,
    )

    assert result.plan.config_action == "preserve"
    assert "appeared after preflight" in " ".join(result.plan.warnings)
    assert load_config(target).salary_floor == 222_000
    assert _company_names(target) == ["Source Co"]


def test_restore_if_missing_never_clobbers_config_racing_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    real_install = restore_module._install_config
    raced = False

    def race_install(source: Path, destination: Path, *, replace_existing: bool = True):
        nonlocal raced
        if not replace_existing and not raced:
            raced = True
            save_config(AppConfig(salary_floor=333_000), target)
        return real_install(source, destination, replace_existing=replace_existing)

    monkeypatch.setattr(restore_module, "_install_config", race_install)

    with pytest.raises(RestoreError, match="appeared during restore"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            config_policy=ConfigPolicy.RESTORE_IF_MISSING,
        )

    assert raced is True
    assert load_config(target).salary_floor == 333_000
    assert not target.database.exists()
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()


def test_restore_recovery_rolls_back_config_if_process_dies_before_completion_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    real_write = restore_module._write_journal
    crashed = False

    def crash_after_config_install(path: Path, journal: dict[str, object]) -> None:
        nonlocal crashed
        if journal.get("config_installed") is True and not crashed:
            crashed = True
            raise KeyboardInterrupt("simulated process death")
        real_write(path, journal)

    monkeypatch.setattr(restore_module, "_write_journal", crash_after_config_install)

    with pytest.raises(KeyboardInterrupt, match="simulated process death"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            config_policy=ConfigPolicy.RESTORE_IF_MISSING,
        )

    journal = target.data_dir / ".jobby-restore-journal.json"
    assert crashed is True
    assert target.config_file.exists()
    assert journal.exists()

    assert recover_interrupted_restore(paths=target) is True
    assert not target.config_file.exists()
    assert not target.database.exists()
    assert not journal.exists()


def test_preflight_rejects_backup_dated_in_the_future(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path)

    def alter(members: dict[str, bytes]) -> None:
        manifest = json.loads(members["manifest.json"])
        manifest["created_at"] = (
            datetime.now(timezone.utc) + timedelta(days=1)
        ).isoformat()
        members["manifest.json"] = json.dumps(manifest).encode()

    future = _rewrite_archive(archive, tmp_path / "future-date.zip", alter)
    assert verify_backup(future) == (True, "ok")

    with pytest.raises(RestoreError, match="creation time is in the future"):
        preflight_restore(future, paths=_paths(tmp_path / "target", ensure=False))


def test_activation_failure_rolls_back_database_and_installed_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, company_name="Original Co")
    current.dispose()
    real_fsync = restore_module._fsync_file
    failed = False

    def fail_new_database(path: Path) -> None:
        nonlocal failed
        if path == target.database and not failed:
            failed = True
            raise OSError("simulated activation sync failure")
        real_fsync(path)

    monkeypatch.setattr(restore_module, "_fsync_file", fail_new_database)

    with pytest.raises(OSError, match="simulated activation"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            replace_current=True,
        )

    assert _company_names(target) == ["Original Co"]
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()
    assert not list(target.data_dir.glob(".restore-stage-*"))


def test_artifact_activation_failure_removes_write_ahead_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    real_fsync = restore_module._fsync_file

    def fail_artifact_sync(path: Path) -> None:
        real_fsync(path)
        if path.is_relative_to(target.artifacts_dir):
            raise OSError("simulated artifact sync failure")

    monkeypatch.setattr(restore_module, "_fsync_file", fail_artifact_sync)

    with pytest.raises(OSError, match="artifact sync"):
        restore_backup(archive, paths=target, apply=True)

    assert not list(target.artifacts_dir.rglob("*.md"))
    assert not target.database.exists()
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()


def test_racing_artifact_is_preserved_and_never_journaled_as_restore_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    real_link = restore_module.os.link
    raced: list[Path] = []

    def publish_after_competing_writer(
        source: Path | str,
        destination: Path | str,
        *,
        follow_symlinks: bool = True,
    ) -> None:
        destination_path = Path(destination)
        if destination_path.is_relative_to(target.artifacts_dir) and not raced:
            destination_path.write_bytes(b"racing user content")
            raced.append(destination_path)
        real_link(source, destination, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(restore_module.os, "link", publish_after_competing_writer)

    with pytest.raises(RestoreError, match="appeared.*different data"):
        restore_backup(archive, paths=target, apply=True)

    assert len(raced) == 1
    assert raced[0].read_bytes() == b"racing user content"
    assert not target.database.exists()
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()


def test_committed_restore_journal_is_finalized_without_rolling_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, company_name="Original Co")
    current.dispose()
    real_remove = restore_module._remove_path

    def crash_after_commit(path: Path | None) -> None:
        if path is not None and ".rollback-" in path.name:
            raise KeyboardInterrupt("simulated process loss after commit")
        real_remove(path)

    monkeypatch.setattr(restore_module, "_remove_path", crash_after_commit)
    with pytest.raises(KeyboardInterrupt, match="simulated process loss"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            replace_current=True,
        )
    journal = target.data_dir / ".jobby-restore-journal.json"
    assert journal.exists()
    with pytest.raises(StorageBusyError, match="interrupted restore"):
        Database(paths=target).initialize()

    monkeypatch.setattr(restore_module, "_remove_path", real_remove)
    assert recover_interrupted_restore(paths=target) is True
    assert not journal.exists()
    assert _company_names(target) == ["Replacement Co"]
    assert recover_interrupted_restore(paths=target) is False


def test_recovery_rolls_back_unverified_database_active_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _ = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, company_name="Original Co")
    current.dispose()
    real_remove = restore_module._remove_path

    def crash_after_commit(path: Path | None) -> None:
        if path is not None and ".rollback-" in path.name:
            raise KeyboardInterrupt("simulated process loss after commit")
        real_remove(path)

    monkeypatch.setattr(restore_module, "_remove_path", crash_after_commit)
    with pytest.raises(KeyboardInterrupt, match="simulated process loss"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            replace_current=True,
        )

    with sqlite3.connect(target.database) as connection:
        connection.execute(
            "UPDATE companies SET name = 'Unverified Co', "
            "normalized_name = 'unverified co'"
        )
        connection.commit()

    monkeypatch.setattr(restore_module, "_remove_path", real_remove)
    assert recover_interrupted_restore(paths=target) is True
    assert _company_names(target) == ["Original Co"]


def test_restore_rejects_symlinked_artifact_directory_components(
    tmp_path: Path,
) -> None:
    archive, _ = _archive(tmp_path)
    target = _paths(tmp_path / "target")
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("unchanged", encoding="utf-8")
    (target.artifacts_dir / "restored").symlink_to(outside, target_is_directory=True)

    with pytest.raises(RestoreError, match="symbolic link"):
        restore_backup(archive, paths=target, apply=True)

    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not target.database.exists()
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()
