"""Verified, relocatable, and crash-recoverable Jobby backup restoration."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import tomllib
import uuid
import zipfile
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory

from .audit import record_audit
from .backup import (
    BACKUP_SCHEMA,
    EXTERNAL_GENERATED_EXPORT_REASON,
    MAX_ARTIFACT_BYTES,
    MAX_ARTIFACTS,
    MAX_DATABASE_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_TOTAL_UNCOMPRESSED_BYTES,
    SAFE_ID_RE,
    create_backup,
    verify_backup,
)
from .config import AppConfig, JobbyPaths, MAX_CONFIG_BYTES, resolve_paths
from .db import Database, RESTORE_JOURNAL_FILENAME, StorageLock


RESTORE_JOURNAL_SCHEMA = "jobby-restore-journal-v1"
MAX_ARCHIVE_BYTES = MAX_TOTAL_UNCOMPRESSED_BYTES
FUTURE_CLOCK_TOLERANCE = timedelta(minutes=5)
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
ARTIFACT_UPDATE_TRIGGER = "artifacts_source_immutable_update"
JOURNAL_NAME = RESTORE_JOURNAL_FILENAME


class RestoreError(ValueError):
    """A backup cannot be safely restored as requested."""


class ConfigPolicy(StrEnum):
    """How the non-secret TOML member is handled during restoration."""

    PRESERVE = "preserve"
    RESTORE_IF_MISSING = "restore-if-missing"
    REPLACE = "replace"


@dataclass(frozen=True)
class RestorePlan:
    archive: Path
    archive_sha256: str
    created_at: datetime
    backup_schema: str
    source_revision: str
    target_revision: str
    migration_required: bool
    artifact_count: int
    artifact_bytes: int
    database_bytes: int
    target_database: Path
    target_artifacts: Path
    replaces_existing_database: bool
    config_in_archive: bool
    config_policy: ConfigPolicy
    config_action: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class RestoreResult:
    plan: RestorePlan
    applied: bool
    restored_artifacts: int = 0
    reused_artifacts: int = 0
    pre_restore_backup: Path | None = None
    audit_recorded: bool = False


@dataclass
class _PreparedRestore:
    activation_database: Path
    artifact_files: list[tuple[Path, Path, str]]
    config_file: Path | None
    activation_database_sha256: str


def preflight_restore(
    archive: Path | str,
    *,
    paths: JobbyPaths | None = None,
    config_policy: ConfigPolicy | str = ConfigPolicy.PRESERVE,
) -> RestorePlan:
    """Validate a backup and rehearse its migration without changing Jobby data."""

    target_paths = paths or resolve_paths()
    policy = _config_policy(config_policy)
    source = _validated_source_path(archive)
    with tempfile.TemporaryDirectory(prefix="jobby-restore-preflight-") as temp_name:
        snapshot = Path(temp_name) / "backup.zip"
        archive_hash = _snapshot_archive(source, snapshot)
        return _inspect_snapshot(
            snapshot,
            original_archive=source,
            archive_hash=archive_hash,
            paths=target_paths,
            config_policy=policy,
        )


def restore_backup(
    archive: Path | str,
    *,
    paths: JobbyPaths | None = None,
    apply: bool = False,
    replace_current: bool = False,
    config_policy: ConfigPolicy | str = ConfigPolicy.PRESERVE,
    lock_timeout: float = 0.0,
) -> RestoreResult:
    """Plan or apply a verified restore.

    The safe default is a read-only preflight. Callers must pass ``apply=True``;
    replacing an existing database additionally requires ``replace_current=True``.
    """

    target_paths = paths or resolve_paths()
    policy = _config_policy(config_policy)
    plan = preflight_restore(archive, paths=target_paths, config_policy=policy)
    if not apply:
        return RestoreResult(plan=plan, applied=False)
    if plan.replaces_existing_database and not replace_current:
        raise RestoreError(
            "target database already exists; pass replace_current=True "
            "(CLI: --replace) to replace it"
        )

    _ensure_restore_directories(target_paths)
    lock = StorageLock(
        target_paths.database.absolute().parent / ".jobby.lock",
        exclusive=True,
        timeout=lock_timeout,
    )
    with lock:
        _recover_interrupted_restore(target_paths)
        existing_database = _regular_file_exists(
            target_paths.database, label="target database"
        )
        if existing_database and not replace_current:
            raise RestoreError(
                "target database appeared after preflight; replacement was not "
                "authorized (CLI: --replace)"
            )
        if (
            policy is ConfigPolicy.RESTORE_IF_MISSING
            and plan.config_action == "restore"
            and _regular_file_exists(
                target_paths.config_file, label="target configuration"
            )
        ):
            plan = replace(
                plan,
                config_action="preserve",
                warnings=plan.warnings
                + (
                    "local configuration appeared after preflight and will be preserved",
                ),
            )
        source = _validated_source_path(archive)
        transaction_id = uuid.uuid4().hex
        stage_root = target_paths.data_dir / f".restore-stage-{transaction_id}"
        stage_root.mkdir(mode=0o700)
        stage_root.chmod(0o700)
        pinned_archive = stage_root / "backup.zip"
        actual_hash = _snapshot_archive(source, pinned_archive)
        if actual_hash != plan.archive_sha256:
            _remove_tree(stage_root)
            raise RestoreError("backup changed after preflight; restore was cancelled")

        pre_restore_backup: Path | None = None
        rollback_database: Path | None = None
        try:
            if existing_database:
                current = Database(paths=target_paths, acquire_lock=False)
                try:
                    current.initialize()
                    pre_restore_backup = create_backup(
                        current,
                        output=target_paths.backups_dir
                        / f"pre-restore-{datetime.now().astimezone():%Y%m%d-%H%M%S}-{transaction_id[:8]}.zip",
                        paths=target_paths,
                    )
                    verified, detail = verify_backup(pre_restore_backup)
                    if not verified:
                        raise RestoreError(
                            f"automatic pre-restore backup failed verification: {detail}"
                        )
                    if not _backup_is_complete(pre_restore_backup):
                        raise RestoreError(
                            "automatic pre-restore backup is incomplete; replacement was cancelled"
                        )
                    rollback_database = (
                        target_paths.database.parent
                        / f".{target_paths.database.name}.rollback-{transaction_id}"
                    )
                    current.backup_to(rollback_database)
                finally:
                    current.dispose()
        except Exception:
            _remove_path(rollback_database)
            _remove_tree(stage_root)
            raise

        try:
            prepared = _prepare_restore(
                pinned_archive,
                plan=plan,
                paths=target_paths,
                stage_root=stage_root,
                pre_restore_backup=pre_restore_backup,
            )
        except Exception:
            _remove_path(rollback_database)
            _remove_tree(stage_root)
            raise
        journal_path = target_paths.data_dir / JOURNAL_NAME
        journal: dict[str, Any] = {
            "schema": RESTORE_JOURNAL_SCHEMA,
            "transaction_id": transaction_id,
            "state": "prepared",
            "stage_root": str(stage_root),
            "target_database": str(target_paths.database.absolute()),
            "activation_database": str(prepared.activation_database),
            "activation_database_sha256": prepared.activation_database_sha256,
            "database_existed": existing_database,
            "rollback_database": str(rollback_database) if rollback_database else None,
            "pre_restore_backup": (
                str(pre_restore_backup) if pre_restore_backup else None
            ),
            "installed_artifacts": [],
            "config_action": plan.config_action,
            "config_existed": target_paths.config_file.exists(),
            "config_backup": None,
            "config_install_intent": False,
            "config_expected_sha256": (
                _hash_regular_file(prepared.config_file)
                if prepared.config_file is not None
                else None
            ),
            "config_installed": False,
        }
        try:
            if target_paths.config_file.exists() and plan.config_action in {
                "replace",
                "restore",
            }:
                config_backup = stage_root / "original-config.toml"
                _copy_regular_file(target_paths.config_file, config_backup)
                journal["config_backup"] = str(config_backup)
            _write_journal(journal_path, journal)
        except Exception:
            _remove_path(rollback_database)
            _remove_tree(stage_root)
            journal_path.unlink(missing_ok=True)
            raise

        restored = 0
        reused = 0
        try:
            for staged, destination, expected_hash in prepared.artifact_files:
                _ensure_managed_child_directory(
                    target_paths.artifacts_dir, destination.parent
                )
                if destination.exists() or destination.is_symlink():
                    if destination.is_symlink() or not destination.is_file():
                        raise RestoreError(
                            f"artifact destination is not a regular file: {destination}"
                        )
                    if _hash_regular_file(destination) != expected_hash:
                        raise RestoreError(
                            f"artifact destination contains different data: {destination}"
                        )
                    reused += 1
                    continue
                temporary = destination.with_name(
                    f".{destination.name}.restore-{transaction_id}.tmp"
                )
                try:
                    _copy_regular_file(staged, temporary, expected_hash=expected_hash)
                    try:
                        os.link(temporary, destination, follow_symlinks=False)
                    except FileExistsError:
                        if destination.is_symlink() or not destination.is_file():
                            raise RestoreError(
                                "artifact destination appeared during restore and is unsafe: "
                                f"{destination}"
                            ) from None
                        if _hash_regular_file(destination) == expected_hash:
                            reused += 1
                            continue
                        raise RestoreError(
                            "artifact destination appeared during restore with different "
                            f"data: {destination}"
                        ) from None
                    # Journal only after our no-clobber publication succeeds. A
                    # racing pre-existing file is therefore never deleted by
                    # interrupted-restore recovery.
                    installed = list(journal["installed_artifacts"])
                    installed.append(str(destination))
                    journal["installed_artifacts"] = installed
                    _write_journal(journal_path, journal)
                    _fsync_file(destination)
                    _fsync_directory(destination.parent)
                    restored += 1
                finally:
                    temporary.unlink(missing_ok=True)

            if prepared.config_file is not None:
                # Journal intent before mutating config. Recovery can distinguish
                # our no-clobber install from a racing user-created file by hash.
                journal["config_install_intent"] = True
                _write_journal(journal_path, journal)
                _install_config(
                    prepared.config_file,
                    target_paths.config_file,
                    replace_existing=plan.config_action == "replace",
                )
                journal["config_installed"] = True
                _write_journal(journal_path, journal)
            journal["state"] = "files_installed"
            _write_journal(journal_path, journal)

            activation_near_target = target_paths.database.parent / (
                f".{target_paths.database.name}.restore-{transaction_id}.tmp"
            )
            _copy_regular_file(
                prepared.activation_database,
                activation_near_target,
                expected_hash=prepared.activation_database_sha256,
            )
            journal["activation_database"] = str(activation_near_target)
            journal["state"] = "database_activating"
            _write_journal(journal_path, journal)
            _remove_sqlite_sidecars(target_paths.database)
            if target_paths.database.is_symlink():
                raise RestoreError("target database became a symbolic link")
            os.replace(activation_near_target, target_paths.database)
            target_paths.database.chmod(0o600)
            _fsync_file(target_paths.database)
            _fsync_directory(target_paths.database.parent)
            activated = Database(paths=target_paths, acquire_lock=False)
            try:
                activated.initialize()
                healthy, detail = activated.integrity_check()
                if not healthy:
                    raise RestoreError(
                        f"restored database failed final checks: {detail}"
                    )
            finally:
                activated.dispose()
            from .search_index import invalidate_search_index

            invalidate_search_index(target_paths)
            journal["activation_database_sha256"] = _hash_regular_file(
                target_paths.database
            )
            journal["state"] = "database_active"
            _write_journal(journal_path, journal)
            _remove_path(rollback_database)
            _remove_tree(stage_root)
            journal_path.unlink(missing_ok=True)
            _fsync_directory(journal_path.parent)
        except Exception:
            _recover_interrupted_restore(target_paths)
            raise

        return RestoreResult(
            plan=plan,
            applied=True,
            restored_artifacts=restored,
            reused_artifacts=reused,
            pre_restore_backup=pre_restore_backup,
            audit_recorded=True,
        )


def recover_interrupted_restore(
    *, paths: JobbyPaths | None = None, lock_timeout: float = 0.0
) -> bool:
    """Recover or finalize a journaled restore without requiring its archive."""

    target_paths = paths or resolve_paths()
    journal_path = target_paths.data_dir / JOURNAL_NAME
    if not journal_path.exists() and not journal_path.is_symlink():
        return False
    _ensure_restore_directories(target_paths)
    with StorageLock(
        target_paths.database.absolute().parent / ".jobby.lock",
        exclusive=True,
        timeout=lock_timeout,
    ):
        _recover_interrupted_restore(target_paths)
    return True


def _inspect_snapshot(
    snapshot: Path,
    *,
    original_archive: Path,
    archive_hash: str,
    paths: JobbyPaths,
    config_policy: ConfigPolicy,
) -> RestorePlan:
    verified, detail = verify_backup(snapshot)
    if not verified:
        raise RestoreError(f"backup verification failed: {detail}")
    with zipfile.ZipFile(snapshot) as archive:
        manifest = _read_manifest(archive)
        created_at = _backup_created_at(manifest)
        skipped = manifest.get("skipped_artifacts", [])
        if not isinstance(skipped, list):
            raise RestoreError("backup skipped-artifact manifest is invalid")
        if skipped:
            raise RestoreError(
                f"backup is incomplete: {len(skipped)} managed artifact(s) were skipped"
            )
        artifacts = _artifact_manifest(manifest)
        excluded_artifacts = _excluded_artifact_manifest(manifest)
        config_in_archive = isinstance(manifest.get("config"), dict)
        config_action, warnings = _config_action(
            config_policy,
            config_in_archive=config_in_archive,
            config_exists=paths.config_file.exists(),
        )
        if excluded_artifacts:
            warnings.append(
                f"{len(excluded_artifacts)} external generated export(s) "
                "were intentionally excluded"
            )
        if config_in_archive:
            _validate_archived_config(archive)
        elif config_policy is ConfigPolicy.REPLACE:
            raise RestoreError("backup contains no configuration to replace")
        with tempfile.TemporaryDirectory(prefix="jobby-restore-db-") as temp_name:
            database_path = Path(temp_name) / "jobby.sqlite3"
            _copy_zip_member(
                archive,
                "database/jobby.sqlite3",
                database_path,
                expected_hash=str(manifest["database_sha256"]),
                maximum_size=MAX_DATABASE_BYTES,
            )
            source_revision, target_revision, migration_required = (
                _validate_and_rehearse_database(
                    database_path,
                    manifest=manifest,
                    artifacts=artifacts,
                    excluded_artifacts=excluded_artifacts,
                    target_paths=paths,
                )
            )
        artifact_bytes = sum(int(item["size_bytes"]) for item in artifacts)
        database_bytes = archive.getinfo("database/jobby.sqlite3").file_size
    return RestorePlan(
        archive=original_archive,
        archive_sha256=archive_hash,
        created_at=created_at,
        backup_schema=BACKUP_SCHEMA,
        source_revision=source_revision,
        target_revision=target_revision,
        migration_required=migration_required,
        artifact_count=len(artifacts),
        artifact_bytes=artifact_bytes,
        database_bytes=database_bytes,
        target_database=paths.database.absolute(),
        target_artifacts=paths.artifacts_dir.absolute(),
        replaces_existing_database=_target_database_exists(paths.database),
        config_in_archive=config_in_archive,
        config_policy=config_policy,
        config_action=config_action,
        warnings=tuple(warnings),
    )


def _prepare_restore(
    snapshot: Path,
    *,
    plan: RestorePlan,
    paths: JobbyPaths,
    stage_root: Path,
    pre_restore_backup: Path | None,
) -> _PreparedRestore:
    with zipfile.ZipFile(snapshot) as archive:
        manifest = _read_manifest(archive)
        artifacts = _artifact_manifest(manifest)
        excluded_artifacts = _excluded_artifact_manifest(manifest)
        work_database = stage_root / "work.sqlite3"
        _copy_zip_member(
            archive,
            "database/jobby.sqlite3",
            work_database,
            expected_hash=str(manifest["database_sha256"]),
            maximum_size=MAX_DATABASE_BYTES,
        )
        _cross_check_database_manifest(
            work_database, artifacts, excluded_artifacts=excluded_artifacts
        )
        database = Database(
            work_database,
            paths=_temporary_paths(stage_root, work_database),
            acquire_lock=False,
        )
        try:
            _migrate_extracted_database(work_database, plan.target_revision)
            database.initialize()
            destinations = {
                str(item["id"]): _artifact_destination(paths, manifest, item)
                for item in artifacts
            }
            with database.engine.begin() as connection:
                connection.exec_driver_sql(
                    f'DROP TRIGGER IF EXISTS "{ARTIFACT_UPDATE_TRIGGER}"'
                )
                for artifact_id, destination in destinations.items():
                    connection.exec_driver_sql(
                        "UPDATE artifacts SET stored_path = ? WHERE id = ?",
                        (str(destination), artifact_id),
                    )
                for item in excluded_artifacts:
                    connection.exec_driver_sql(
                        "UPDATE artifacts SET stored_path = NULL WHERE id = ?",
                        (str(item["id"]),),
                    )
            database._install_immutability_triggers()
            with database.session() as session:
                record_audit(
                    session,
                    action="restore.applied",
                    entity_type="backup",
                    entity_id=plan.archive_sha256[:32],
                    actor="user",
                    after={
                        "backup_schema": plan.backup_schema,
                        "archive_sha256": plan.archive_sha256,
                        "source_revision": plan.source_revision,
                        "target_revision": plan.target_revision,
                        "artifact_count": plan.artifact_count,
                        "excluded_external_exports": len(excluded_artifacts),
                        "config_action": plan.config_action,
                        "pre_restore_backup": (
                            str(pre_restore_backup) if pre_restore_backup else None
                        ),
                    },
                    detail="Verified local backup restored with explicit approval.",
                )
            healthy, detail = database.integrity_check()
            if not healthy:
                raise RestoreError(f"staged database failed validation: {detail}")
            activation_database = stage_root / "activation.sqlite3"
            database.backup_to(activation_database)
        finally:
            database.dispose()

        artifact_files: list[tuple[Path, Path, str]] = []
        payload_root = stage_root / "artifact-payload"
        payload_root.mkdir(mode=0o700)
        for index, item in enumerate(artifacts):
            member = str(item["path"])
            destination = _artifact_destination(paths, manifest, item)
            staged = payload_root / f"{index:06d}.artifact"
            _copy_zip_member(
                archive,
                member,
                staged,
                expected_hash=str(item["sha256"]),
                maximum_size=MAX_ARTIFACT_BYTES,
            )
            artifact_files.append((staged, destination, str(item["sha256"])))

        config_file: Path | None = None
        if plan.config_action in {"restore", "replace"}:
            config_file = stage_root / "config.toml"
            config_manifest = manifest["config"]
            _copy_zip_member(
                archive,
                "config/config.toml",
                config_file,
                expected_hash=str(config_manifest["sha256"]),
                maximum_size=MAX_CONFIG_BYTES,
            )
            _validate_config_file(config_file)

    activation_hash = _hash_regular_file(activation_database)
    return _PreparedRestore(
        activation_database=activation_database,
        artifact_files=artifact_files,
        config_file=config_file,
        activation_database_sha256=activation_hash,
    )


def _validate_and_rehearse_database(
    database_path: Path,
    *,
    manifest: dict[str, Any],
    artifacts: list[dict[str, Any]],
    excluded_artifacts: list[dict[str, Any]],
    target_paths: JobbyPaths,
) -> tuple[str, str, bool]:
    _cross_check_database_manifest(
        database_path, artifacts, excluded_artifacts=excluded_artifacts
    )
    source_revision = _database_revision(database_path)
    target_revision, supported_revisions = _migration_revisions()
    if source_revision not in supported_revisions:
        raise RestoreError(
            f"backup database revision {source_revision!r} is newer or unsupported"
        )
    trial_paths = _temporary_paths(database_path.parent, database_path)
    database = Database(database_path, paths=trial_paths, acquire_lock=False)
    try:
        _migrate_extracted_database(database_path, target_revision)
        database.initialize()
        healthy, detail = database.integrity_check()
        if not healthy:
            raise RestoreError(f"migrated backup database is invalid: {detail}")
    finally:
        database.dispose()
    migrated_revision = _database_revision(database_path)
    if migrated_revision != target_revision:
        raise RestoreError(
            f"backup migration stopped at {migrated_revision!r}, expected {target_revision!r}"
        )
    for item in artifacts:
        _artifact_destination(target_paths, manifest, item)
    return source_revision, target_revision, source_revision != target_revision


def _migrate_extracted_database(database_path: Path, target_revision: str) -> None:
    """Migrate only the isolated copy extracted from a verified backup.

    Operational databases remain subject to ``jobby upgrade apply``. Restore
    preflight and staging work on disposable private copies, so they can safely
    rehearse the same Alembic path without weakening that boundary.
    """

    if _database_revision(database_path) == target_revision:
        return
    from .upgrade import _run_alembic_upgrade

    _run_alembic_upgrade(database_path, target_revision)


def _cross_check_database_manifest(
    database_path: Path,
    artifacts: list[dict[str, Any]],
    *,
    excluded_artifacts: list[dict[str, Any]] | None = None,
) -> None:
    connection = sqlite3.connect(database_path)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if not {"alembic_version", "artifacts"}.issubset(tables):
            raise RestoreError(
                "backup database is incomplete or is not a Jobby database"
            )
        rows = connection.execute(
            "SELECT id, kind, stored_path, content_hash, size_bytes FROM artifacts"
        ).fetchall()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        foreign_key_error = connection.execute("PRAGMA foreign_key_check").fetchone()
    except sqlite3.DatabaseError as exc:
        raise RestoreError(f"backup database cannot be inspected: {exc}") from exc
    finally:
        connection.close()
    if not integrity or integrity[0] != "ok" or foreign_key_error is not None:
        raise RestoreError("backup database failed integrity or foreign-key checks")

    database_rows = {str(row[0]): row for row in rows}
    manifest_rows = {str(item["id"]): item for item in artifacts}
    excluded_rows = {str(item["id"]): item for item in (excluded_artifacts or [])}
    accounted_ids = set(manifest_rows) | set(excluded_rows)
    if set(database_rows) != accounted_ids:
        missing = len(set(database_rows) - accounted_ids)
        extra = len(accounted_ids - set(database_rows))
        raise RestoreError(
            "backup artifact manifest does not match its database "
            f"({missing} missing, {extra} unexpected)"
        )
    for artifact_id, item in manifest_rows.items():
        row = database_rows[artifact_id]
        if not row[2]:
            raise RestoreError(f"artifact stored path is missing for {artifact_id}")
        if str(row[1]) != item["kind"]:
            raise RestoreError(f"artifact kind mismatch for {artifact_id}")
        if str(row[3]) != item["sha256"]:
            raise RestoreError(f"artifact hash mismatch for {artifact_id}")
        if int(row[4]) != int(item["size_bytes"]):
            raise RestoreError(f"artifact size mismatch for {artifact_id}")
    for artifact_id, item in excluded_rows.items():
        row = database_rows[artifact_id]
        if (
            not row[2]
            or str(row[1]) != "generated_export"
            or item.get("kind") != "generated_export"
            or item.get("reason") != EXTERNAL_GENERATED_EXPORT_REASON
        ):
            raise RestoreError(
                f"excluded artifact is not an external generated export: {artifact_id}"
            )


def _read_manifest(archive: zipfile.ZipFile) -> dict[str, Any]:
    info = archive.getinfo("manifest.json")
    if info.file_size > MAX_MANIFEST_BYTES:
        raise RestoreError("backup manifest exceeds its safety limit")
    try:
        value = json.loads(archive.read(info))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RestoreError("backup manifest is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict) or value.get("schema") != BACKUP_SCHEMA:
        raise RestoreError("backup schema is newer or unsupported")
    database_hash = value.get("database_sha256")
    if not isinstance(database_hash, str) or not HASH_RE.fullmatch(database_hash):
        raise RestoreError("backup database hash is invalid")
    return value


def _backup_is_complete(path: Path) -> bool:
    with zipfile.ZipFile(path) as archive:
        manifest = _read_manifest(archive)
    skipped = manifest.get("skipped_artifacts", [])
    return isinstance(skipped, list) and not skipped


def _artifact_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    raw = manifest.get("artifacts")
    if not isinstance(raw, list):
        raise RestoreError("backup artifact manifest is invalid")
    result: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for value in raw:
        if not isinstance(value, dict):
            raise RestoreError("backup artifact entry is invalid")
        artifact_id = value.get("id")
        kind = value.get("kind")
        member = value.get("path")
        digest = value.get("sha256")
        size = value.get("size_bytes")
        if (
            not isinstance(artifact_id, str)
            or not artifact_id
            or artifact_id in seen_ids
        ):
            raise RestoreError("backup artifact IDs are invalid or duplicated")
        if not isinstance(kind, str) or not kind:
            raise RestoreError(f"artifact kind is invalid for {artifact_id}")
        if not isinstance(member, str) or not isinstance(digest, str):
            raise RestoreError(f"artifact metadata is invalid for {artifact_id}")
        if not HASH_RE.fullmatch(digest):
            raise RestoreError(f"artifact hash is invalid for {artifact_id}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise RestoreError(f"artifact size is invalid for {artifact_id}")
        safe_id = (
            artifact_id
            if SAFE_ID_RE.fullmatch(artifact_id)
            else hashlib.sha256(artifact_id.encode()).hexdigest()[:32]
        )
        parts = PurePosixPath(member).parts
        if (
            len(parts) != 3
            or parts[0] != "artifacts"
            or parts[1] != safe_id
            or parts[2] in {"", ".", ".."}
        ):
            raise RestoreError(f"artifact archive path is invalid for {artifact_id}")
        seen_ids.add(artifact_id)
        result.append(value)
    return result


def _excluded_artifact_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    raw = manifest.get("excluded_artifacts", [])
    if not isinstance(raw, list) or len(raw) > MAX_ARTIFACTS:
        raise RestoreError("backup excluded-artifact manifest is invalid")
    result: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for value in raw:
        if not isinstance(value, dict):
            raise RestoreError("backup excluded-artifact entry is invalid")
        artifact_id = value.get("id")
        if (
            not isinstance(artifact_id, str)
            or not artifact_id
            or artifact_id in seen_ids
            or value.get("kind") != "generated_export"
            or value.get("reason") != EXTERNAL_GENERATED_EXPORT_REASON
        ):
            raise RestoreError("backup excluded-artifact entry is invalid")
        seen_ids.add(artifact_id)
        result.append(value)
    included_ids = {
        str(value.get("id"))
        for value in manifest.get("artifacts", [])
        if isinstance(value, dict)
    }
    if seen_ids & included_ids:
        raise RestoreError("backup artifact IDs overlap manifest categories")
    return result


def _artifact_destination(
    paths: JobbyPaths, manifest: dict[str, Any], item: dict[str, Any]
) -> Path:
    database_hash = str(manifest["database_sha256"])
    artifact_id = str(item["id"])
    safe_id = (
        artifact_id
        if SAFE_ID_RE.fullmatch(artifact_id)
        else hashlib.sha256(artifact_id.encode()).hexdigest()[:32]
    )
    filename = PurePosixPath(str(item["path"])).name
    destination = (
        paths.artifacts_dir.absolute()
        / "restored"
        / database_hash[:16]
        / safe_id
        / filename
    )
    if not _is_relative_to(destination, paths.artifacts_dir.absolute()):
        raise RestoreError("artifact destination escapes managed storage")
    return destination


def _backup_created_at(manifest: dict[str, Any]) -> datetime:
    raw = manifest.get("created_at")
    if not isinstance(raw, str):
        raise RestoreError("backup creation time is missing")
    try:
        created_at = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RestoreError("backup creation time is invalid") from exc
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise RestoreError("backup creation time must include a timezone")
    if (
        created_at.astimezone(timezone.utc)
        > datetime.now(timezone.utc) + FUTURE_CLOCK_TOLERANCE
    ):
        raise RestoreError("backup creation time is in the future")
    return created_at


def _migration_revisions() -> tuple[str, set[str]]:
    config = Config()
    config.set_main_option(
        "script_location", str(Path(__file__).with_name("migrations"))
    )
    scripts = ScriptDirectory.from_config(config)
    heads = scripts.get_heads()
    if len(heads) != 1:
        raise RuntimeError("Jobby restore requires a single migration head")
    revisions = {revision.revision for revision in scripts.walk_revisions()}
    return heads[0], revisions


def _database_revision(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        rows = connection.execute("SELECT version_num FROM alembic_version").fetchall()
    except sqlite3.DatabaseError as exc:
        raise RestoreError(
            "backup database has no supported migration revision"
        ) from exc
    finally:
        connection.close()
    if len(rows) != 1 or not isinstance(rows[0][0], str):
        raise RestoreError("backup database migration revision is invalid")
    return rows[0][0]


def _config_policy(value: ConfigPolicy | str) -> ConfigPolicy:
    try:
        return value if isinstance(value, ConfigPolicy) else ConfigPolicy(value)
    except ValueError as exc:
        choices = ", ".join(item.value for item in ConfigPolicy)
        raise RestoreError(f"config_policy must be one of: {choices}") from exc


def _config_action(
    policy: ConfigPolicy, *, config_in_archive: bool, config_exists: bool
) -> tuple[str, list[str]]:
    warnings: list[str] = []
    if policy is ConfigPolicy.PRESERVE:
        if config_in_archive:
            warnings.append("archived configuration will not replace local settings")
        return "preserve", warnings
    if policy is ConfigPolicy.RESTORE_IF_MISSING:
        if config_exists:
            warnings.append("local configuration exists and will be preserved")
            return "preserve", warnings
        return ("restore" if config_in_archive else "none"), warnings
    return "replace", warnings


def _validate_archived_config(archive: zipfile.ZipFile) -> None:
    if archive.getinfo("config/config.toml").file_size > MAX_CONFIG_BYTES:
        raise RestoreError("archived configuration exceeds its safety limit")
    raw = archive.read("config/config.toml")
    try:
        AppConfig.model_validate(tomllib.loads(raw.decode("utf-8")))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValueError) as exc:
        raise RestoreError(f"archived configuration is invalid: {exc}") from exc


def _validate_config_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            AppConfig.model_validate(tomllib.load(handle))
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        raise RestoreError(f"archived configuration is invalid: {exc}") from exc


def _install_config(
    source: Path, destination: Path, *, replace_existing: bool = True
) -> None:
    _ensure_private_directory(destination.parent)
    if destination.is_symlink():
        raise RestoreError("configuration destination must not be a symbolic link")
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.restore-", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(name)
    try:
        _copy_regular_file(source, temporary)
        if replace_existing:
            os.replace(temporary, destination)
        else:
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError as exc:
                raise RestoreError(
                    "local configuration appeared during restore; it was preserved, "
                    "so retry the restore"
                ) from exc
        destination.chmod(0o600)
        _fsync_file(destination)
        _fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _recover_interrupted_restore(paths: JobbyPaths) -> None:
    journal_path = paths.data_dir / JOURNAL_NAME
    if not journal_path.exists() and not journal_path.is_symlink():
        return
    if journal_path.is_symlink() or not journal_path.is_file():
        raise RestoreError("restore journal is not a regular file")
    if journal_path.stat().st_size > MAX_MANIFEST_BYTES:
        raise RestoreError("restore journal exceeds its safety limit")
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RestoreError(
            "restore journal is unreadable; manual recovery is required"
        ) from exc
    if not isinstance(journal, dict) or journal.get("schema") != RESTORE_JOURNAL_SCHEMA:
        raise RestoreError("restore journal has an unsupported schema")
    transaction_id = journal.get("transaction_id")
    if not isinstance(transaction_id, str) or not re.fullmatch(
        r"[0-9a-f]{32}", transaction_id
    ):
        raise RestoreError("restore journal transaction ID is invalid")
    target_database = _journal_path(
        journal.get("target_database"), paths.database.parent, "target database"
    )
    if target_database != paths.database.absolute():
        raise RestoreError("restore journal targets a different database")
    stage_root = _journal_path(journal.get("stage_root"), paths.data_dir, "stage root")
    if (
        stage_root.parent != paths.data_dir.absolute()
        or stage_root.name != f".restore-stage-{transaction_id}"
    ):
        raise RestoreError("restore journal staging directory is invalid")
    rollback_raw = journal.get("rollback_database")
    rollback_database = (
        _journal_path(rollback_raw, paths.database.parent, "rollback database")
        if rollback_raw
        else None
    )
    if rollback_database is not None and rollback_database.name != (
        f".{paths.database.name}.rollback-{transaction_id}"
    ):
        raise RestoreError("restore journal rollback database name is invalid")
    state = journal.get("state")
    if state not in {
        "prepared",
        "files_installed",
        "database_activating",
        "database_active",
    }:
        raise RestoreError("restore journal state is invalid")
    database_existed = journal.get("database_existed")
    if not isinstance(database_existed, bool):
        raise RestoreError("restore journal database state is invalid")
    expected_new_hash = journal.get("activation_database_sha256")
    if not isinstance(expected_new_hash, str) or not HASH_RE.fullmatch(
        expected_new_hash
    ):
        raise RestoreError("restore journal database hash is invalid")
    if state == "database_active" and target_database.exists():
        if target_database.is_symlink() or not target_database.is_file():
            raise RestoreError("activated restore database is unsafe")
        if _hash_regular_file(
            target_database
        ) == expected_new_hash and not _sqlite_sidecars_exist(target_database):
            _remove_path(rollback_database)
            _remove_tree(stage_root)
            journal_path.unlink(missing_ok=True)
            _fsync_directory(journal_path.parent)
            from .search_index import invalidate_search_index

            invalidate_search_index(paths)
            return

    if (
        state in {"database_activating", "database_active"}
        and database_existed
        and (rollback_database is None or not rollback_database.exists())
    ):
        raise RestoreError(
            "activated database could not be verified and the restore rollback "
            "database is missing; manual recovery is required"
        )
    if rollback_database is not None and rollback_database.exists():
        if rollback_database.is_symlink() or not rollback_database.is_file():
            raise RestoreError("restore rollback database is unsafe")
        _remove_sqlite_sidecars(target_database)
        os.replace(rollback_database, target_database)
        target_database.chmod(0o600)
        _fsync_file(target_database)
    elif not database_existed:
        _remove_sqlite_sidecars(target_database)
        target_database.unlink(missing_ok=True)

    config_installed = journal.get("config_installed")
    if not isinstance(config_installed, bool):
        raise RestoreError("restore journal configuration state is invalid")
    config_action = journal.get("config_action")
    if config_action not in {"none", "preserve", "replace", "restore"}:
        raise RestoreError("restore journal configuration action is invalid")
    config_install_intent = journal.get("config_install_intent", config_installed)
    if not isinstance(config_install_intent, bool):
        raise RestoreError("restore journal configuration intent is invalid")
    expected_config_hash = journal.get("config_expected_sha256")
    if expected_config_hash is not None and (
        not isinstance(expected_config_hash, str)
        or not HASH_RE.fullmatch(expected_config_hash)
    ):
        raise RestoreError("restore journal configuration hash is invalid")
    if config_installed or config_install_intent or config_action == "replace":
        config_backup_raw = journal.get("config_backup")
        if config_backup_raw:
            config_backup = _journal_path(
                config_backup_raw, stage_root, "configuration backup"
            )
            if config_backup.exists():
                _install_config(config_backup, paths.config_file)
            elif bool(journal.get("config_existed")):
                raise RestoreError(
                    "restore configuration rollback is missing; manual recovery is required"
                )
        elif not bool(journal.get("config_existed")):
            if paths.config_file.exists() or paths.config_file.is_symlink():
                if paths.config_file.is_symlink() or not paths.config_file.is_file():
                    raise RestoreError(
                        "restore configuration rollback target is unsafe"
                    )
                if (
                    expected_config_hash is not None
                    and _hash_regular_file(paths.config_file) != expected_config_hash
                ):
                    # RESTORE_IF_MISSING uses a no-clobber link. A different
                    # hash means another process won the race; preserve it.
                    if config_action == "restore" and not config_installed:
                        pass
                    else:
                        raise RestoreError(
                            "restore configuration changed during recovery; manual recovery is required"
                        )
                else:
                    paths.config_file.unlink(missing_ok=True)
                    _fsync_directory(paths.config_file.parent)

    installed = journal.get("installed_artifacts", [])
    if not isinstance(installed, list):
        raise RestoreError("restore journal artifact list is invalid")
    for raw in installed:
        artifact = _journal_path(raw, paths.artifacts_dir, "installed artifact")
        _reject_symlink_ancestors(artifact.parent, paths.artifacts_dir)
        artifact.unlink(missing_ok=True)
    activation_raw = journal.get("activation_database")
    if activation_raw:
        if not isinstance(activation_raw, str):
            raise RestoreError("restore journal activation database path is invalid")
        activation = Path(activation_raw).absolute()
        valid_staged = (
            activation == stage_root / "activation.sqlite3"
            and _is_relative_to(activation, stage_root)
        )
        valid_near_target = activation == paths.database.parent.absolute() / (
            f".{paths.database.name}.restore-{transaction_id}.tmp"
        )
        if not (valid_staged or valid_near_target):
            raise RestoreError("restore journal activation database path is invalid")
        activation.unlink(missing_ok=True)
    _remove_tree(stage_root)
    journal_path.unlink(missing_ok=True)
    _fsync_directory(journal_path.parent)
    from .search_index import invalidate_search_index

    invalidate_search_index(paths)


def _journal_path(value: object, root: Path, label: str) -> Path:
    if not isinstance(value, str):
        raise RestoreError(f"restore journal {label} path is invalid")
    candidate = Path(value)
    if not candidate.is_absolute():
        raise RestoreError(f"restore journal {label} path is not absolute")
    candidate = candidate.absolute()
    if not _is_relative_to(candidate, root.absolute()):
        raise RestoreError(f"restore journal {label} escapes managed storage")
    return candidate


def _write_journal(path: Path, value: dict[str, Any]) -> None:
    if path.is_symlink():
        raise RestoreError("restore journal must not be a symbolic link")
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")
    if len(payload) > MAX_MANIFEST_BYTES:
        raise RestoreError("restore journal exceeds its safety limit")
    descriptor, name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise


def _validated_source_path(value: Path | str) -> Path:
    path = Path(value).expanduser().absolute()
    if path.is_symlink():
        raise RestoreError("backup archive must not be a symbolic link")
    try:
        metadata = path.stat()
    except OSError as exc:
        raise RestoreError(f"backup archive cannot be read: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise RestoreError("backup archive must be a regular file")
    if metadata.st_size > MAX_ARCHIVE_BYTES:
        raise RestoreError("backup archive exceeds its safety limit")
    return path


def _snapshot_archive(source: Path, destination: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    digest = hashlib.sha256()
    size = 0
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RestoreError("backup archive must be a regular file")
        if before.st_size > MAX_ARCHIVE_BYTES:
            raise RestoreError("backup archive exceeds its safety limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(descriptor, "rb", closefd=False) as input_handle:
            with destination.open("xb") as output_handle:
                destination.chmod(0o600)
                for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                    size += len(chunk)
                    if size > MAX_ARCHIVE_BYTES:
                        raise RestoreError("backup archive exceeds its safety limit")
                    digest.update(chunk)
                    output_handle.write(chunk)
                output_handle.flush()
                os.fsync(output_handle.fileno())
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RestoreError("backup archive changed while it was being read")
        if size != before.st_size:
            raise RestoreError("backup archive size changed while it was being read")
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _copy_zip_member(
    archive: zipfile.ZipFile,
    name: str,
    destination: Path,
    *,
    expected_hash: str,
    maximum_size: int,
) -> None:
    if not HASH_RE.fullmatch(expected_hash):
        raise RestoreError(f"invalid expected hash for {name}")
    info = archive.getinfo(name)
    if info.file_size > maximum_size:
        raise RestoreError(f"backup member exceeds its size limit: {name}")
    mode = (info.external_attr >> 16) & 0xFFFF
    pure = PurePosixPath(name)
    if pure.is_absolute() or ".." in pure.parts or "\\" in name:
        raise RestoreError(f"unsafe backup member path: {name}")
    if info.is_dir() or stat.S_ISLNK(mode) or info.flag_bits & 0x1:
        raise RestoreError(f"unsupported backup member: {name}")
    digest = hashlib.sha256()
    size = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with archive.open(info) as input_handle:
        with destination.open("xb") as output_handle:
            destination.chmod(0o600)
            for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                size += len(chunk)
                if size > maximum_size:
                    raise RestoreError(f"backup member exceeds its size limit: {name}")
                digest.update(chunk)
                output_handle.write(chunk)
            output_handle.flush()
            os.fsync(output_handle.fileno())
    if size != info.file_size or digest.hexdigest() != expected_hash:
        destination.unlink(missing_ok=True)
        raise RestoreError(f"backup member failed size or hash validation: {name}")


def _copy_regular_file(
    source: Path, destination: Path, *, expected_hash: str | None = None
) -> None:
    if source.is_symlink() or not source.is_file():
        raise RestoreError(f"restore source is not a regular file: {source}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    output_descriptor: int | None = None
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RestoreError(f"restore source is not a regular file: {source}")
        output_flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_TRUNC
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        output_descriptor = os.open(destination, output_flags, 0o600)
        output_metadata = os.fstat(output_descriptor)
        if not stat.S_ISREG(output_metadata.st_mode):
            raise RestoreError(
                f"restore destination is not a regular file: {destination}"
            )
        os.fchmod(output_descriptor, 0o600)
        with os.fdopen(descriptor, "rb", closefd=False) as input_handle:
            with os.fdopen(output_descriptor, "wb", closefd=False) as output_handle:
                for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                    output_handle.write(chunk)
                output_handle.flush()
                os.fsync(output_handle.fileno())
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RestoreError(f"restore source changed while being read: {source}")
    finally:
        if output_descriptor is not None:
            os.close(output_descriptor)
        os.close(descriptor)
    if expected_hash is not None and digest.hexdigest() != expected_hash:
        destination.unlink(missing_ok=True)
        raise RestoreError(f"restore source hash mismatch: {source.name}")


def _hash_regular_file(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise RestoreError(f"expected a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RestoreError(f"file changed while being hashed: {path}")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _ensure_restore_directories(paths: JobbyPaths) -> None:
    for directory in (
        paths.data_dir,
        paths.config_dir,
        paths.cache_dir,
        paths.artifacts_dir,
        paths.backups_dir,
        paths.logs_dir,
        paths.database.parent,
    ):
        _ensure_private_directory(directory)


def _ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise RestoreError(f"managed directory must not be a symbolic link: {path}")
    path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise RestoreError(f"managed path is not a directory: {path}")
    path.chmod(0o700)


def _ensure_managed_child_directory(root: Path, target: Path) -> None:
    root = root.absolute()
    target = target.absolute()
    if not _is_relative_to(target, root):
        raise RestoreError("managed directory escapes its storage root")
    _ensure_private_directory(root)
    current = root
    for part in target.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise RestoreError(
                f"managed directory component must not be a symbolic link: {current}"
            )
        current.mkdir(mode=0o700, exist_ok=True)
        if current.is_symlink() or not current.is_dir():
            raise RestoreError(f"managed path is not a directory: {current}")
        current.chmod(0o700)


def _reject_symlink_ancestors(target: Path, root: Path) -> None:
    root = root.absolute()
    target = target.absolute()
    if not _is_relative_to(target, root):
        raise RestoreError("managed path escapes its storage root")
    current = root
    if current.is_symlink():
        raise RestoreError(f"managed directory is a symbolic link: {current}")
    for part in target.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise RestoreError(f"managed directory is a symbolic link: {current}")


def _regular_file_exists(path: Path, *, label: str) -> bool:
    if path.is_symlink():
        raise RestoreError(f"{label} must not be a symbolic link")
    if not path.exists():
        return False
    if not path.is_file():
        raise RestoreError(f"{label} must be a regular file")
    return True


def _target_database_exists(path: Path) -> bool:
    return _regular_file_exists(path, label="target database")


def _temporary_paths(root: Path, database: Path) -> JobbyPaths:
    return JobbyPaths(
        data_dir=root,
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=database,
        artifacts_dir=root / "artifacts",
        backups_dir=root / "backups",
        logs_dir=root / "logs",
        config_file=root / "config" / "config.toml",
    )


def _remove_sqlite_sidecars(database: Path) -> None:
    for path in (Path(f"{database}-wal"), Path(f"{database}-shm")):
        if path.is_symlink():
            raise RestoreError(f"SQLite sidecar must not be a symbolic link: {path}")
        if path.exists() and not path.is_file():
            raise RestoreError(f"SQLite sidecar must be a regular file: {path}")
        path.unlink(missing_ok=True)


def _sqlite_sidecars_exist(database: Path) -> bool:
    found = False
    for path in (Path(f"{database}-wal"), Path(f"{database}-shm")):
        if path.is_symlink():
            raise RestoreError(f"SQLite sidecar must not be a symbolic link: {path}")
        if path.exists() and not path.is_file():
            raise RestoreError(f"SQLite sidecar must be a regular file: {path}")
        found = found or path.exists()
    return found


def _remove_tree(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    if path.is_symlink() or not path.is_dir():
        raise RestoreError(f"refusing to remove unsafe restore staging path: {path}")
    shutil.rmtree(path)


def _remove_path(path: Path | None) -> None:
    if path is None:
        return
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise RestoreError(f"refusing to remove unsafe restore file: {path}")
    path.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise RestoreError(f"restore sync target is not a regular file: {path}")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


__all__ = [
    "ConfigPolicy",
    "RestoreError",
    "RestorePlan",
    "RestoreResult",
    "preflight_restore",
    "recover_interrupted_restore",
    "restore_backup",
]
