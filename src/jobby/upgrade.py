"""Explicit, backup-first control for upgrading an existing Jobby database.

This module deliberately does not use :meth:`jobby.db.Database.initialize`.
That method is the fresh-database bootstrap boundary; calling it for an older
database would run Alembic before an upgrade snapshot could be taken.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.script.revision import RevisionError
from alembic.util.exc import CommandError
from sqlalchemy import URL, create_engine, event

from .db import StorageLock


UPGRADE_JOURNAL_FILENAME = ".jobby-upgrade-journal.json"
UPGRADE_JOURNAL_SCHEMA = "jobby-upgrade-journal-v1"
MAX_JOURNAL_BYTES = 1_000_000
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
TERMINAL_JOURNAL_PHASES = frozenset({"complete", "recovered"})

UpgradeState = Literal["fresh", "current", "upgrade_required", "future", "invalid"]
UpgradeAction = Literal["bootstrap", "none", "upgrade", "recover", "blocked"]
PhaseHook = Callable[[str, Mapping[str, Any]], None]


class UpgradeError(RuntimeError):
    """An explicit database upgrade could not be completed safely."""


@dataclass(frozen=True, slots=True)
class UpgradeStatus:
    database_path: Path
    state: UpgradeState
    current_revision: str | None
    target_revision: str
    pending_revisions: tuple[str, ...] = ()
    journal_phase: str | None = None
    detail: str = ""

    @property
    def requires_upgrade(self) -> bool:
        return self.state == "upgrade_required"

    @property
    def recovery_available(self) -> bool:
        return self.journal_phase is not None

    @property
    def recovery_required(self) -> bool:
        if self.journal_phase is None:
            return False
        if self.journal_phase not in TERMINAL_JOURNAL_PHASES:
            return True
        return self.state in {"fresh", "invalid"}


@dataclass(frozen=True, slots=True)
class UpgradePlan:
    status: UpgradeStatus
    action: UpgradeAction
    source_revision: str | None
    target_revision: str
    revisions: tuple[str, ...]
    snapshot_required: bool
    rehearsal_required: bool
    detail: str


@dataclass(frozen=True, slots=True)
class UpgradeResult:
    plan: UpgradePlan
    applied: bool
    snapshot_path: Path | None = None
    snapshot_sha256: str | None = None
    journal_path: Path | None = None
    rehearsal_schema_sha256: str | None = None
    live_schema_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    recovered: bool
    database_path: Path
    restored_revision: str | None = None
    snapshot_path: Path | None = None
    snapshot_sha256: str | None = None
    rescue_snapshot_path: Path | None = None
    rescue_snapshot_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _DatabaseVerification:
    revision: str
    schema_sha256: str


def upgrade_status(database_path: Path | str) -> UpgradeStatus:
    """Inspect upgrade state without creating or modifying the SQLite file."""

    path = Path(database_path).expanduser().absolute()
    target, scripts = _migration_graph()
    journal_phase: str | None = None
    journal_path = _journal_path(path)
    if journal_path.exists() or journal_path.is_symlink():
        try:
            journal = _read_journal(journal_path, expected_database=path)
        except UpgradeError as exc:
            return UpgradeStatus(
                database_path=path,
                state="invalid",
                current_revision=None,
                target_revision=target,
                detail=str(exc),
            )
        journal_phase = str(journal["phase"])
        if journal_phase not in TERMINAL_JOURNAL_PHASES:
            # Do not open SQLite while recovery is mandatory. A process may
            # have stopped between replacing the main file and removing stale
            # WAL/SHM sidecars, so even a read-only connection is unsafe here.
            known_revision = _known_journal_revision(journal)
            if known_revision is None:
                public_state: UpgradeState = "invalid"
                pending: tuple[str, ...] = ()
            else:
                public_state, pending, _detail = _classify_revision(
                    target, scripts, known_revision
                )
            return UpgradeStatus(
                database_path=path,
                state=public_state,
                current_revision=known_revision,
                target_revision=target,
                pending_revisions=pending,
                journal_phase=journal_phase,
                detail=(
                    f"interrupted upgrade phase {journal_phase!r} requires "
                    "verified recovery before database access"
                ),
            )

    state, current, detail = _read_database_state(path)
    pending: tuple[str, ...] = ()
    if state == "versioned":
        assert current is not None
        public_state, pending, detail = _classify_revision(target, scripts, current)
    elif state == "fresh":
        public_state = "fresh"
    else:
        public_state = "invalid"

    return UpgradeStatus(
        database_path=path,
        state=public_state,
        current_revision=current,
        target_revision=target,
        pending_revisions=pending,
        journal_phase=journal_phase,
        detail=detail,
    )


def _classify_revision(
    target: str, scripts: ScriptDirectory, current: str
) -> tuple[UpgradeState, tuple[str, ...], str]:
    if current == target:
        return "current", (), "database is at the current migration head"
    try:
        scripts.get_revision(current)
        descending = tuple(
            revision.revision for revision in scripts.iterate_revisions(target, current)
        )
    except (CommandError, RevisionError):
        return (
            "future",
            (),
            f"database revision {current!r} is not supported by this release",
        )
    pending = tuple(reversed(descending))
    if not pending:
        return (
            "future",
            (),
            f"database revision {current!r} is not an ancestor of migration head "
            f"{target!r}",
        )
    return (
        "upgrade_required",
        pending,
        f"database requires {len(pending)} migration(s) to reach {target}",
    )


def _known_journal_revision(journal: Mapping[str, Any]) -> str | None:
    phase = str(journal["phase"])
    if phase in {"snapshot_verified", "rehearsal_verified"}:
        return str(journal["source_revision"])
    if phase in {"live_migrated", "verified"}:
        return str(journal["target_revision"])
    return None


def plan_upgrade(database_path: Path | str) -> UpgradePlan:
    """Return a read-only, deterministic plan for the requested SQLite path."""

    status = upgrade_status(database_path)
    if status.recovery_required:
        action: UpgradeAction = "recover"
        detail = (
            f"interrupted upgrade phase {status.journal_phase!r} must be recovered "
            "before database access"
        )
    elif status.state == "fresh":
        action = "bootstrap"
        detail = "fresh databases are bootstrapped by normal Jobby initialization"
    elif status.state == "current":
        action = "none"
        detail = "database is already current"
    elif status.state == "upgrade_required":
        action = "upgrade"
        detail = (
            "apply requires an exclusive lock, verified snapshot, isolated "
            "rehearsal, journaled live migration, and post-apply verification"
        )
    else:
        action = "blocked"
        detail = status.detail
    return UpgradePlan(
        status=status,
        action=action,
        source_revision=status.current_revision,
        target_revision=status.target_revision,
        revisions=status.pending_revisions,
        snapshot_required=action == "upgrade",
        rehearsal_required=action == "upgrade",
        detail=detail,
    )


def apply_upgrade(
    database_path: Path | str,
    *,
    backup_dir: Path | str | None = None,
    lock_timeout: float = 0.0,
    phase_hook: PhaseHook | None = None,
) -> UpgradeResult:
    """Apply a pending upgrade only after backup and isolated rehearsal.

    A successful apply intentionally retains both the verified snapshot and a
    journal in the ``complete`` phase. This gives ``recover_upgrade`` a durable
    rollback point even after post-apply verification succeeded.
    """

    path = _validated_existing_database(database_path)
    if lock_timeout < 0:
        raise ValueError("upgrade lock timeout must not be negative")
    lock = StorageLock(
        path.parent / ".jobby.lock", exclusive=True, timeout=lock_timeout
    )
    with lock:
        plan = plan_upgrade(path)
        if plan.action == "recover":
            raise UpgradeError(
                "an interrupted upgrade must be recovered before another apply"
            )
        if plan.action == "none":
            return UpgradeResult(plan=plan, applied=False)
        if plan.action != "upgrade":
            raise UpgradeError(plan.detail)
        assert plan.source_revision is not None

        backups = (
            Path(backup_dir).expanduser().absolute()
            if backup_dir is not None
            else path.parent / "backups"
        )
        _ensure_private_directory(backups)
        transaction_id = uuid.uuid4().hex
        stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        snapshot_path = backups / (f"pre-upgrade-{stamp}-{transaction_id[:12]}.sqlite3")
        snapshot_sha256 = _create_verified_snapshot(
            path,
            snapshot_path,
            expected_revision=plan.source_revision,
        )

        journal_path = _journal_path(path)
        journal: dict[str, Any] = {
            "schema": UPGRADE_JOURNAL_SCHEMA,
            "transaction_id": transaction_id,
            "database_path": str(path),
            "snapshot_path": str(snapshot_path),
            "snapshot_sha256": snapshot_sha256,
            "source_revision": plan.source_revision,
            "target_revision": plan.target_revision,
            "revisions": list(plan.revisions),
            "phase": "snapshot_verified",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "rehearsal_schema_sha256": None,
            "live_schema_sha256": None,
            "rescue_snapshot_path": None,
            "rescue_snapshot_sha256": None,
        }
        _write_journal(journal_path, journal)
        _call_phase_hook(phase_hook, journal)

        with tempfile.TemporaryDirectory(
            prefix=".jobby-upgrade-rehearsal-", dir=path.parent
        ) as temporary_name:
            rehearsal_path = Path(temporary_name) / "rehearsal.sqlite3"
            _copy_exact(snapshot_path, rehearsal_path)
            if _sha256_file(rehearsal_path) != snapshot_sha256:
                raise UpgradeError(
                    "rehearsal copy does not match the verified snapshot"
                )
            _run_alembic_upgrade(rehearsal_path, plan.target_revision)
            rehearsal = _verify_database(
                rehearsal_path, expected_revision=plan.target_revision
            )
            journal["rehearsal_schema_sha256"] = rehearsal.schema_sha256
            _advance_phase(
                journal_path, journal, "rehearsal_verified", phase_hook=phase_hook
            )

            # The exclusive lock prevents another supported Jobby process from
            # changing the database between snapshot, rehearsal, and apply.
            live_before = _verify_database(path, expected_revision=plan.source_revision)
            if _sha256_file(snapshot_path) != snapshot_sha256:
                raise UpgradeError("verified upgrade snapshot changed before apply")
            if not live_before.schema_sha256:
                raise UpgradeError("live database schema could not be verified")

            _advance_phase(journal_path, journal, "applying", phase_hook=phase_hook)
            _run_alembic_upgrade(path, plan.target_revision)
            _advance_phase(
                journal_path, journal, "live_migrated", phase_hook=phase_hook
            )
            live = _verify_database(path, expected_revision=plan.target_revision)
            journal["live_schema_sha256"] = live.schema_sha256
            if live.schema_sha256 != rehearsal.schema_sha256:
                raise UpgradeError(
                    "live migrated schema does not match the verified rehearsal"
                )
            if _sha256_file(snapshot_path) != snapshot_sha256:
                raise UpgradeError("verified upgrade snapshot changed during apply")
            _advance_phase(journal_path, journal, "verified", phase_hook=phase_hook)
            _advance_phase(journal_path, journal, "complete", phase_hook=phase_hook)

        completed_status = upgrade_status(path)
        completed_plan = replace(plan, status=completed_status)
        return UpgradeResult(
            plan=completed_plan,
            applied=True,
            snapshot_path=snapshot_path,
            snapshot_sha256=snapshot_sha256,
            journal_path=journal_path,
            rehearsal_schema_sha256=rehearsal.schema_sha256,
            live_schema_sha256=live.schema_sha256,
        )


def recover_upgrade(
    database_path: Path | str,
    *,
    lock_timeout: float = 0.0,
    phase_hook: PhaseHook | None = None,
) -> RecoveryResult:
    """Restore the verified pre-upgrade snapshot for any journaled apply."""

    path = Path(database_path).expanduser().absolute()
    if lock_timeout < 0:
        raise ValueError("upgrade lock timeout must not be negative")
    journal_path = _journal_path(path)
    if not journal_path.exists() and not journal_path.is_symlink():
        return RecoveryResult(recovered=False, database_path=path)

    with StorageLock(path.parent / ".jobby.lock", exclusive=True, timeout=lock_timeout):
        journal = _read_journal(journal_path, expected_database=path)
        snapshot_path = Path(str(journal["snapshot_path"]))
        snapshot_sha256 = str(journal["snapshot_sha256"])
        source_revision = str(journal["source_revision"])
        _validate_snapshot(
            snapshot_path,
            expected_sha256=snapshot_sha256,
            expected_revision=source_revision,
        )
        rescue_path, rescue_sha256 = _prepare_recovery_snapshot(
            path,
            journal_path=journal_path,
            journal=journal,
            phase_hook=phase_hook,
        )
        _advance_phase(journal_path, journal, "recovering", phase_hook=phase_hook)

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.recover-", suffix=".tmp", dir=path.parent
        )
        os.close(descriptor)
        staged = Path(temporary_name)
        try:
            _copy_exact(snapshot_path, staged)
            _validate_snapshot(
                staged,
                expected_sha256=snapshot_sha256,
                expected_revision=source_revision,
            )
            os.replace(staged, path)
            _advance_phase(
                journal_path,
                journal,
                "database_replaced",
                phase_hook=phase_hook,
            )
            path.chmod(0o600)
            _remove_sqlite_sidecars(path)
            _fsync_directory(path.parent)
        finally:
            staged.unlink(missing_ok=True)

        _validate_snapshot(
            path,
            expected_sha256=snapshot_sha256,
            expected_revision=source_revision,
        )
        _advance_phase(journal_path, journal, "recovered", phase_hook=phase_hook)
        journal_path.unlink(missing_ok=True)
        _fsync_directory(journal_path.parent)
        return RecoveryResult(
            recovered=True,
            database_path=path,
            restored_revision=source_revision,
            snapshot_path=snapshot_path,
            snapshot_sha256=snapshot_sha256,
            rescue_snapshot_path=rescue_path,
            rescue_snapshot_sha256=rescue_sha256,
        )


def _prepare_recovery_snapshot(
    database_path: Path,
    *,
    journal_path: Path,
    journal: dict[str, Any],
    phase_hook: PhaseHook | None,
) -> tuple[Path | None, str | None]:
    """Retain verified live state before rollback can remove newer records."""

    rescue_value = journal.get("rescue_snapshot_path")
    checksum_value = journal.get("rescue_snapshot_sha256")
    if isinstance(rescue_value, str) and isinstance(checksum_value, str):
        rescue_path = Path(rescue_value)
        _validate_integrity_snapshot(rescue_path, expected_sha256=checksum_value)
        return rescue_path, checksum_value
    if rescue_value is not None or checksum_value is not None:
        raise UpgradeError("upgrade journal recovery snapshot fields are invalid")
    if not database_path.exists():
        return None, None
    if database_path.is_symlink() or not database_path.is_file():
        raise UpgradeError(
            "live database must be a regular non-symbolic file before recovery"
        )

    original_snapshot = Path(str(journal["snapshot_path"]))
    _ensure_private_directory(original_snapshot.parent)
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    transaction_id = str(journal["transaction_id"])
    rescue_path = original_snapshot.parent / (
        f"pre-recovery-{stamp}-{transaction_id[:12]}.sqlite3"
    )
    rescue_sha256 = _create_verified_snapshot(
        database_path,
        rescue_path,
        expected_revision=None,
    )
    journal["rescue_snapshot_path"] = str(rescue_path)
    journal["rescue_snapshot_sha256"] = rescue_sha256
    _advance_phase(
        journal_path,
        journal,
        "recovery_backup_verified",
        phase_hook=phase_hook,
    )
    return rescue_path, rescue_sha256


def _migration_graph() -> tuple[str, ScriptDirectory]:
    config = Config()
    config.set_main_option(
        "script_location", str(Path(__file__).with_name("migrations"))
    )
    scripts = ScriptDirectory.from_config(config)
    heads = scripts.get_heads()
    if len(heads) != 1:
        raise UpgradeError("Jobby upgrades require exactly one migration head")
    return heads[0], scripts


def _read_database_state(
    path: Path,
) -> tuple[Literal["fresh", "versioned", "invalid"], str | None, str]:
    if not path.exists():
        return "fresh", None, "database does not exist"
    if path.is_symlink() or not path.is_file():
        return "invalid", None, "database path must be a regular non-symbolic file"
    if path.stat().st_size == 0:
        return "fresh", None, "database file is empty"
    try:
        with _readonly_connection(path) as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
                if isinstance(row[0], str) and not str(row[0]).startswith("sqlite_")
            }
            if not tables:
                return "fresh", None, "database contains no application tables"
            if "alembic_version" not in tables:
                return (
                    "invalid",
                    None,
                    "existing database has application tables but no Alembic revision",
                )
            rows = connection.execute(
                "SELECT version_num FROM alembic_version"
            ).fetchall()
    except sqlite3.DatabaseError as exc:
        return "invalid", None, f"database cannot be inspected: {exc}"
    if len(rows) != 1 or not isinstance(rows[0][0], str) or not rows[0][0]:
        return "invalid", None, "database must contain exactly one migration revision"
    return "versioned", str(rows[0][0]), ""


def _run_alembic_upgrade(path: Path, target_revision: str) -> None:
    config = Config()
    config.set_main_option(
        "script_location", str(Path(__file__).with_name("migrations"))
    )
    engine = create_engine(
        URL.create("sqlite+pysqlite", database=str(path)),
        future=True,
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @event.listens_for(engine, "connect")
    def configure_sqlite(
        dbapi_connection: sqlite3.Connection, _connection_record: object
    ) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, target_revision)
    finally:
        engine.dispose()


def _create_verified_snapshot(
    source_path: Path,
    destination: Path,
    *,
    expected_revision: str | None,
) -> str:
    if destination.exists() or destination.is_symlink():
        raise UpgradeError(f"upgrade snapshot already exists: {destination}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    source: sqlite3.Connection | None = None
    target: sqlite3.Connection | None = None
    published = False
    try:
        source = _open_readonly(source_path)
        target = sqlite3.connect(temporary)
        source.backup(target)
        target.commit()
        target.close()
        target = None
        source.close()
        source = None
        _verify_snapshot_contents(temporary, expected_revision=expected_revision)
        temporary.chmod(0o600)
        _fsync_file(temporary)
        digest = _sha256_file(temporary)
        os.replace(temporary, destination)
        published = True
        _fsync_directory(destination.parent)
        _validate_snapshot_contents(
            destination, expected_sha256=digest, expected_revision=expected_revision
        )
        return digest
    except Exception:
        temporary.unlink(missing_ok=True)
        if published:
            with suppress(OSError):
                destination.unlink(missing_ok=True)
                _fsync_directory(destination.parent)
        raise
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()


def _validate_snapshot(
    path: Path, *, expected_sha256: str, expected_revision: str
) -> _DatabaseVerification:
    _validate_snapshot_file(path, expected_sha256=expected_sha256)
    return _verify_database(path, expected_revision=expected_revision)


def _validate_integrity_snapshot(path: Path, *, expected_sha256: str) -> str:
    _validate_snapshot_file(path, expected_sha256=expected_sha256)
    return _verify_database_integrity(path)


def _validate_snapshot_contents(
    path: Path, *, expected_sha256: str, expected_revision: str | None
) -> None:
    if expected_revision is None:
        _validate_integrity_snapshot(path, expected_sha256=expected_sha256)
    else:
        _validate_snapshot(
            path,
            expected_sha256=expected_sha256,
            expected_revision=expected_revision,
        )


def _verify_snapshot_contents(path: Path, *, expected_revision: str | None) -> None:
    if expected_revision is None:
        _verify_database_integrity(path)
    else:
        _verify_database(path, expected_revision=expected_revision)


def _validate_snapshot_file(path: Path, *, expected_sha256: str) -> None:
    if not HASH_PATTERN.fullmatch(expected_sha256):
        raise UpgradeError("upgrade snapshot checksum is invalid")
    if path.is_symlink() or not path.is_file():
        raise UpgradeError("verified upgrade snapshot is missing or unsafe")
    if _sha256_file(path) != expected_sha256:
        raise UpgradeError("verified upgrade snapshot checksum mismatch")


def _verify_database_integrity(path: Path) -> str:
    try:
        with _readonly_connection(path) as connection:
            integrity_rows = [
                str(row[0])
                for row in connection.execute("PRAGMA integrity_check").fetchall()
            ]
            foreign_key_error = connection.execute(
                "PRAGMA foreign_key_check"
            ).fetchone()
            schema_sha256 = _schema_sha256(connection)
    except sqlite3.DatabaseError as exc:
        raise UpgradeError(f"database verification failed: {exc}") from exc
    if integrity_rows != ["ok"]:
        detail = "; ".join(integrity_rows) or "no integrity result"
        raise UpgradeError(f"database integrity check failed: {detail}")
    if foreign_key_error is not None:
        raise UpgradeError(
            f"database foreign-key check failed: {tuple(foreign_key_error)}"
        )
    return schema_sha256


def _verify_database(path: Path, *, expected_revision: str) -> _DatabaseVerification:
    try:
        with _readonly_connection(path) as connection:
            integrity_rows = [
                str(row[0])
                for row in connection.execute("PRAGMA integrity_check").fetchall()
            ]
            foreign_key_error = connection.execute(
                "PRAGMA foreign_key_check"
            ).fetchone()
            revisions = connection.execute(
                "SELECT version_num FROM alembic_version"
            ).fetchall()
            schema_sha256 = _schema_sha256(connection)
    except sqlite3.DatabaseError as exc:
        raise UpgradeError(f"database verification failed: {exc}") from exc
    if integrity_rows != ["ok"]:
        detail = "; ".join(integrity_rows) or "no integrity result"
        raise UpgradeError(f"database integrity check failed: {detail}")
    if foreign_key_error is not None:
        raise UpgradeError(
            f"database foreign-key check failed: {tuple(foreign_key_error)}"
        )
    if revisions != [(expected_revision,)]:
        raise UpgradeError(
            f"database revision is {revisions!r}; expected {expected_revision!r}"
        )
    return _DatabaseVerification(
        revision=expected_revision, schema_sha256=schema_sha256
    )


def _schema_sha256(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        """
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name, tbl_name
        """
    ).fetchall()
    exact_schema = [
        [str(kind), str(name), str(table), str(sql)] for kind, name, table, sql in rows
    ]
    payload = json.dumps(
        exact_schema, ensure_ascii=False, sort_keys=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _journal_path(database_path: Path) -> Path:
    return database_path.parent / UPGRADE_JOURNAL_FILENAME


def _read_journal(path: Path, *, expected_database: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise UpgradeError("upgrade journal must be a regular non-symbolic file")
    if path.stat().st_size > MAX_JOURNAL_BYTES:
        raise UpgradeError("upgrade journal exceeds its safety limit")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UpgradeError("upgrade journal is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema") != UPGRADE_JOURNAL_SCHEMA:
        raise UpgradeError("upgrade journal has an unsupported schema")
    required_strings = (
        "transaction_id",
        "database_path",
        "snapshot_path",
        "snapshot_sha256",
        "source_revision",
        "target_revision",
        "phase",
    )
    if any(
        not isinstance(value.get(key), str) or not value[key]
        for key in required_strings
    ):
        raise UpgradeError("upgrade journal is missing required fields")
    if Path(str(value["database_path"])).absolute() != expected_database:
        raise UpgradeError("upgrade journal targets a different database")
    if not HASH_PATTERN.fullmatch(str(value["snapshot_sha256"])):
        raise UpgradeError("upgrade journal snapshot checksum is invalid")
    snapshot_path = Path(str(value["snapshot_path"]))
    if not snapshot_path.is_absolute() or not snapshot_path.name.startswith(
        "pre-upgrade-"
    ):
        raise UpgradeError("upgrade journal snapshot path is invalid")
    phase = str(value["phase"])
    allowed_phases = {
        "snapshot_verified",
        "rehearsal_verified",
        "applying",
        "live_migrated",
        "verified",
        "complete",
        "recovery_backup_verified",
        "recovering",
        "database_replaced",
        "recovered",
    }
    if phase not in allowed_phases:
        raise UpgradeError("upgrade journal phase is invalid")
    rescue_path = value.get("rescue_snapshot_path")
    rescue_checksum = value.get("rescue_snapshot_sha256")
    if rescue_path is None and rescue_checksum is None:
        return value
    if (
        not isinstance(rescue_path, str)
        or not rescue_path
        or not isinstance(rescue_checksum, str)
        or not HASH_PATTERN.fullmatch(rescue_checksum)
    ):
        raise UpgradeError("upgrade journal recovery snapshot fields are invalid")
    rescue = Path(rescue_path)
    if not rescue.is_absolute() or not rescue.name.startswith("pre-recovery-"):
        raise UpgradeError("upgrade journal recovery snapshot path is invalid")
    return value


def _write_journal(path: Path, journal: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise UpgradeError("upgrade journal must not be a symbolic link")
    payload = json.dumps(
        dict(journal), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if len(payload) > MAX_JOURNAL_BYTES:
        raise UpgradeError("upgrade journal exceeds its safety limit")
    descriptor: int | None
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        handle = os.fdopen(descriptor, "wb")
        descriptor = None
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _advance_phase(
    journal_path: Path,
    journal: dict[str, Any],
    phase: str,
    *,
    phase_hook: PhaseHook | None,
) -> None:
    journal["phase"] = phase
    journal["updated_at"] = datetime.now(timezone.utc).isoformat()
    _write_journal(journal_path, journal)
    _call_phase_hook(phase_hook, journal)


def _call_phase_hook(hook: PhaseHook | None, journal: Mapping[str, Any]) -> None:
    if hook is not None:
        hook(str(journal["phase"]), asdict(_journal_view(journal)))


@dataclass(frozen=True, slots=True)
class _JournalView:
    phase: str
    database_path: str
    snapshot_path: str
    source_revision: str
    target_revision: str


def _journal_view(journal: Mapping[str, Any]) -> _JournalView:
    return _JournalView(
        phase=str(journal["phase"]),
        database_path=str(journal["database_path"]),
        snapshot_path=str(journal["snapshot_path"]),
        source_revision=str(journal["source_revision"]),
        target_revision=str(journal["target_revision"]),
    )


def _validated_existing_database(path_value: Path | str) -> Path:
    path = Path(path_value).expanduser().absolute()
    if path.is_symlink() or not path.is_file():
        raise UpgradeError(
            "database path must be an existing regular non-symbolic file"
        )
    return path


def _ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise UpgradeError("upgrade backup directory must not be a symbolic link")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.is_dir():
        raise UpgradeError("upgrade backup destination must be a directory")
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _open_readonly(path: Path) -> sqlite3.Connection:
    # ``mode=ro`` keeps status, planning, and verification from creating a file.
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


@contextmanager
def _readonly_connection(path: Path) -> Iterator[sqlite3.Connection]:
    connection = _open_readonly(path)
    try:
        yield connection
    finally:
        connection.close()


def _copy_exact(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise UpgradeError("upgrade copy source must be a regular file")
    if destination.is_symlink():
        raise UpgradeError("upgrade copy destination must not be a symbolic link")
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
    _fsync_file(destination)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise UpgradeError("upgrade checksum source must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _remove_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        candidate = Path(f"{path}{suffix}")
        if candidate.is_symlink():
            raise UpgradeError("SQLite sidecar must not be a symbolic link")
        candidate.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


# Concise aliases make CLI integration read naturally while preserving explicit
# function names for embedders and tests.
status = upgrade_status
plan = plan_upgrade
apply = apply_upgrade
recover = recover_upgrade


__all__ = [
    "RecoveryResult",
    "UPGRADE_JOURNAL_FILENAME",
    "UpgradeError",
    "UpgradePlan",
    "UpgradeResult",
    "UpgradeStatus",
    "apply",
    "apply_upgrade",
    "plan",
    "plan_upgrade",
    "recover",
    "recover_upgrade",
    "status",
    "upgrade_status",
]
