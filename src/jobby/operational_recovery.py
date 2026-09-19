"""Scheduled, verification-gated backup and restore-rehearsal operations.

The scheduler is the only automatic caller.  Every filesystem publication is
still handled by the existing no-clobber backup/encryption primitives, and a
configured external destination is treated as removable media: Jobby never
creates a missing mount point or prompts for a passphrase in the background.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select

from .audit import record_audit
from .backup import create_backup, verify_backup
from .backup_rotation import rotate_backups
from .config import AppConfig, SecretStore, resolve_paths
from .db import Database
from .encrypted_backup import decrypt_backup, encrypt_backup
from .enums import AlertSeverity
from .models import Alert, BackupRecord, MaintenanceRun
from .restore import restore_backup
from .review_queues import alert_fingerprint, create_or_recur_alert
from .sources.base import sanitize_error_message


EXTERNAL_MEDIA_ALERT_KEY = "scheduled-external-backup-media"
EXTERNAL_BACKUP_ALERT_KEY = "scheduled-external-backup-failure"
LOCAL_BACKUP_ALERT_KEY = "scheduled-local-backup-failure"
RESTORE_REHEARSAL_ALERT_KEY = "monthly-restore-rehearsal-failure"
SCHEDULED_KEYRING_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ScheduledRecoveryResult:
    """Non-secret summary suitable for an ``AgentRun.summary`` payload."""

    attempted: bool
    local_backup: Path | None = None
    encrypted_backup: Path | None = None
    rotation_pruned: tuple[Path, ...] = ()
    restore_rehearsal: str = "not_due"
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def as_summary(self) -> dict[str, object]:
        return {
            "attempted": self.attempted,
            "local_backup": str(self.local_backup) if self.local_backup else None,
            "encrypted_backup": (
                str(self.encrypted_backup) if self.encrypted_backup else None
            ),
            "rotation_pruned": [str(path) for path in self.rotation_pruned],
            "restore_rehearsal": self.restore_rehearsal,
            "warnings": list(self.warnings),
        }


def run_scheduled_recovery(
    database: Database,
    config: AppConfig,
    *,
    secrets: SecretStore,
    mode: str,
    succeeded: bool,
    now: datetime | None = None,
) -> ScheduledRecoveryResult:
    """Run post-cycle recovery work for an installed scheduler invocation.

    A successful focused cycle always produces a verified rotating local
    backup.  A successful weekly inventory also makes a local snapshot so the
    encrypted copy necessarily includes that inventory.  External publication
    happens after every successful inventory invocation, including a catch-up
    run on another weekday, and only when its destination directory and
    keyring passphrase are already present.

    Operational failures are persisted as deduplicated alerts and maintenance
    records.  They do not rewrite the already-committed discovery result.
    """

    current = _aware(now or datetime.now(timezone.utc))
    normalized_mode = str(mode).strip().casefold()
    if not succeeded or normalized_mode not in {"focused", "inventory"}:
        return ScheduledRecoveryResult(attempted=False)

    warnings: list[str] = []
    try:
        local_backup, pruned = _create_rotated_local_backup(database, config, current)
        _resolve_operational_alert(database, LOCAL_BACKUP_ALERT_KEY, now=current)
    except Exception as exc:
        detail = sanitize_error_message(exc)
        _record_failed_operation(
            database,
            kind="scheduled_local_backup",
            alert_key=LOCAL_BACKUP_ALERT_KEY,
            title="Scheduled local backup failed",
            detail=detail,
            now=current,
        )
        return ScheduledRecoveryResult(
            attempted=True,
            warnings=(f"local backup failed: {detail}",),
        )

    encrypted: Path | None = None
    if (
        normalized_mode == "inventory"
        and config.external_backup_destination is not None
    ):
        encrypted, external_warning = _scheduled_external_backup(
            database,
            config,
            secrets=secrets,
            local_backup=local_backup,
            now=current,
        )
        if external_warning:
            warnings.append(external_warning)

    rehearsal = _run_monthly_restore_rehearsal_if_due(
        database,
        local_backup,
        now=current,
        timezone_name=config.timezone,
    )
    if rehearsal.startswith("failed:"):
        warnings.append(rehearsal)

    return ScheduledRecoveryResult(
        attempted=True,
        local_backup=local_backup,
        encrypted_backup=encrypted,
        rotation_pruned=pruned,
        restore_rehearsal=rehearsal,
        warnings=tuple(warnings),
    )


def run_monthly_restore_rehearsal(
    database: Database,
    backup: Path | str,
    *,
    now: datetime | None = None,
) -> MaintenanceRun:
    """Restore one verified backup into an isolated temporary Jobby home."""

    current = _aware(now or datetime.now(timezone.utc))
    archive = Path(backup).expanduser().absolute()
    verified, detail = verify_backup(archive)
    if not verified:
        raise RuntimeError(f"restore rehearsal backup is invalid: {detail}")
    started = current
    try:
        with tempfile.TemporaryDirectory(
            prefix="jobby-restore-rehearsal-", dir=database.paths.cache_dir
        ) as temporary_name:
            rehearsal_paths = resolve_paths({"JOBBY_HOME": temporary_name}).ensure()
            result = restore_backup(
                archive,
                paths=rehearsal_paths,
                apply=True,
                replace_current=False,
            )
            if not result.applied:
                raise RuntimeError("restore rehearsal did not apply its verified plan")
            restored = Database(paths=rehearsal_paths)
            try:
                restored.initialize()
                healthy, restored_detail = restored.integrity_check()
                if not healthy:
                    raise RuntimeError(
                        f"rehearsed database failed verification: {restored_detail}"
                    )
            finally:
                restored.dispose()
        finished = current
        with database.session() as session:
            run = MaintenanceRun(
                kind="monthly_restore_rehearsal",
                status="succeeded",
                started_at=started,
                finished_at=finished,
                result_json={
                    "backup_path": str(archive),
                    "backup_sha256": _hash_file(archive),
                    "target_revision": result.plan.target_revision,
                    "temporary_home_removed": True,
                },
            )
            session.add(run)
            session.flush()
            record_audit(
                session,
                action="maintenance.restore_rehearsal_succeeded",
                entity_type="maintenance_run",
                entity_id=run.id,
                actor="agent",
                after=run.result_json,
            )
            _resolve_alert_in_session(session, RESTORE_REHEARSAL_ALERT_KEY, now=current)
            return run
    except Exception as exc:
        detail = sanitize_error_message(exc)
        _record_failed_operation(
            database,
            kind="monthly_restore_rehearsal",
            alert_key=RESTORE_REHEARSAL_ALERT_KEY,
            title="Monthly restore rehearsal failed",
            detail=detail,
            now=current,
        )
        raise


def _create_rotated_local_backup(
    database: Database,
    config: AppConfig,
    now: datetime,
) -> tuple[Path, tuple[Path, ...]]:
    backup = create_backup(database, paths=database.paths)
    verified, detail = verify_backup(backup)
    if not verified:
        raise RuntimeError(f"new local backup failed verification: {detail}")
    digest = _hash_file(backup)
    rotation = rotate_backups(
        database.paths.backups_dir,
        backup,
        apply=True,
        daily_retention=config.backup_daily_retention,
        weekly_retention=config.backup_weekly_retention,
        monthly_retention=config.backup_monthly_retention,
    )
    with database.session() as session:
        record = BackupRecord(
            backup_kind="scheduled_local",
            path=str(backup),
            plaintext_sha256=digest,
            size_bytes=backup.stat().st_size,
            verified_at=now,
            external=False,
        )
        session.add(record)
        session.flush()
        record_audit(
            session,
            action="backup.scheduled_local_created",
            entity_type="backup_record",
            entity_id=record.id,
            actor="agent",
            after={
                "path": str(backup),
                "plaintext_sha256": digest,
                "rotation_pruned": [str(path) for path in rotation.pruned],
            },
        )
    return backup, tuple(rotation.pruned)


def _scheduled_external_backup(
    database: Database,
    config: AppConfig,
    *,
    secrets: SecretStore,
    local_backup: Path,
    now: datetime,
) -> tuple[Path | None, str | None]:
    destination = Path(config.external_backup_destination or "").expanduser().absolute()
    destination_identity = hashlib.sha256(str(destination).encode()).hexdigest()[:32]
    if not destination.exists() or destination.is_symlink() or not destination.is_dir():
        detail = (
            "configured external backup media is absent; no external path was "
            "created and the verified local replacement was retained"
        )
        _record_external_warning(
            database,
            alert_key=EXTERNAL_MEDIA_ALERT_KEY,
            title="External backup media is unavailable",
            detail=detail,
            entity_id=destination_identity,
            now=now,
        )
        return None, detail

    try:
        passphrase = _bounded_secret_get(secrets, "external_backup_passphrase")
    except Exception as exc:
        detail = (
            "external-backup passphrase lookup failed; scheduled encryption was "
            f"skipped ({exc.__class__.__name__})"
        )
        _record_external_warning(
            database,
            alert_key=EXTERNAL_BACKUP_ALERT_KEY,
            title="External backup keyring is unavailable",
            detail=detail,
            entity_id=destination_identity,
            now=now,
        )
        return None, detail
    if not passphrase:
        detail = (
            "external-backup passphrase is not available in the OS keyring; "
            "scheduled encryption was skipped"
        )
        _record_external_warning(
            database,
            alert_key=EXTERNAL_BACKUP_ALERT_KEY,
            title="External backup needs a stored passphrase",
            detail=detail,
            entity_id=destination_identity,
            now=now,
        )
        return None, detail

    name = f"{local_backup.stem}.jobbyenc"
    encrypted = destination / name
    try:
        encrypt_backup(
            local_backup,
            encrypted,
            passphrase,
            scrypt_n=config.external_backup_scrypt_n,
        )
        with tempfile.TemporaryDirectory(
            prefix=".jobby-external-recovery-test-",
            dir=database.paths.cache_dir,
        ) as temporary_name:
            recovered = Path(temporary_name) / "recovered.zip"
            decrypt_backup(encrypted, recovered, passphrase)
            recovered_ok, recovered_detail = verify_backup(recovered)
            if not recovered_ok:
                raise RuntimeError(
                    f"encrypted recovery test failed: {recovered_detail}"
                )
            if _hash_file(recovered) != _hash_file(local_backup):
                raise RuntimeError("encrypted recovery test checksum mismatch")
        with database.session() as session:
            record = BackupRecord(
                backup_kind="scheduled_encrypted_external",
                path=str(encrypted),
                plaintext_sha256=_hash_file(local_backup),
                size_bytes=encrypted.stat().st_size,
                verified_at=now,
                external=True,
                recovery_tested_at=now,
            )
            session.add(record)
            session.flush()
            record_audit(
                session,
                action="backup.scheduled_external_created",
                entity_type="backup_record",
                entity_id=record.id,
                actor="agent",
                after={
                    "path": str(encrypted),
                    "plaintext_sha256": record.plaintext_sha256,
                    "recovery_tested": True,
                    "scrypt_n": config.external_backup_scrypt_n,
                },
            )
            _resolve_alert_in_session(
                session,
                EXTERNAL_MEDIA_ALERT_KEY,
                entity_id=destination_identity,
                now=now,
            )
            _resolve_alert_in_session(
                session,
                EXTERNAL_BACKUP_ALERT_KEY,
                entity_id=destination_identity,
                now=now,
            )
        return encrypted, None
    except Exception as exc:
        detail = sanitize_error_message(exc)
        _record_external_warning(
            database,
            alert_key=EXTERNAL_BACKUP_ALERT_KEY,
            title="Scheduled external backup failed",
            detail=detail,
            entity_id=destination_identity,
            now=now,
        )
        return None, f"external backup failed: {detail}"


def _bounded_secret_get(secrets: SecretStore, name: str) -> str | None:
    """Prevent a background schedule from hanging on an OS keyring prompt."""

    values: list[str | None] = []
    errors: list[Exception] = []

    def lookup() -> None:
        try:
            values.append(secrets.get(name))
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(
        target=lookup,
        name="jobby-scheduled-keyring",
        daemon=True,
    )
    worker.start()
    worker.join(SCHEDULED_KEYRING_TIMEOUT_SECONDS)
    if worker.is_alive():
        raise TimeoutError("OS keyring lookup timed out")
    if errors:
        raise errors[0]
    return values[0] if values else None


def _run_monthly_restore_rehearsal_if_due(
    database: Database,
    backup: Path,
    *,
    now: datetime,
    timezone_name: str,
) -> str:
    local_now = now.astimezone(ZoneInfo(timezone_name))
    month_start = local_now.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)
    with database.session() as session:
        completed = session.scalar(
            select(MaintenanceRun.id)
            .where(
                MaintenanceRun.kind == "monthly_restore_rehearsal",
                MaintenanceRun.status == "succeeded",
                MaintenanceRun.started_at >= month_start,
            )
            .limit(1)
        )
    if completed is not None:
        return "already_completed"
    try:
        run_monthly_restore_rehearsal(database, backup, now=now)
    except Exception as exc:
        return f"failed: {sanitize_error_message(exc)}"
    return "succeeded"


def _record_failed_operation(
    database: Database,
    *,
    kind: str,
    alert_key: str,
    title: str,
    detail: str,
    now: datetime,
) -> None:
    with database.session() as session:
        run = MaintenanceRun(
            kind=kind,
            status="failed",
            started_at=now,
            finished_at=now,
            result_json={},
            error=detail,
        )
        session.add(run)
        session.flush()
        create_or_recur_alert(
            session,
            severity=AlertSeverity.WARNING,
            title=title,
            message=detail,
            deduplication_key=alert_key,
            now=now,
        )
        record_audit(
            session,
            action="maintenance.failed",
            entity_type="maintenance_run",
            entity_id=run.id,
            actor="agent",
            after={"kind": kind, "status": "failed"},
            detail=detail,
        )


def _record_external_warning(
    database: Database,
    *,
    alert_key: str,
    title: str,
    detail: str,
    entity_id: str,
    now: datetime,
) -> None:
    with database.session() as session:
        create_or_recur_alert(
            session,
            severity=AlertSeverity.WARNING,
            title=title,
            message=detail,
            deduplication_key=alert_key,
            entity_type="backup_destination",
            entity_id=entity_id,
            now=now,
        )


def _resolve_operational_alert(
    database: Database,
    key: str,
    *,
    now: datetime,
) -> None:
    with database.session() as session:
        _resolve_alert_in_session(session, key, now=now)


def _resolve_alert_in_session(
    session,
    key: str,
    *,
    entity_id: str | None = None,
    now: datetime,
) -> None:
    entity_type = "backup_destination" if entity_id is not None else None
    fingerprint = alert_fingerprint(
        title="operational alert",
        entity_type=entity_type,
        entity_id=entity_id,
        deduplication_key=key,
    )
    alert = session.scalar(select(Alert).where(Alert.fingerprint == fingerprint))
    if alert is not None and alert.resolved_at is None:
        alert.resolved_at = now
        alert.resolution_reason = "A subsequent scheduled operation succeeded."


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


__all__ = [
    "ScheduledRecoveryResult",
    "run_monthly_restore_rehearsal",
    "run_scheduled_recovery",
]
