"""SQLite engine lifecycle, transactional sessions, health checks, and backups."""

from __future__ import annotations

import math
from numbers import Real
import os
import sqlite3
import stat
import tempfile
import time
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Iterator

from sqlalchemy import Engine, URL, create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from .config import JobbyPaths, resolve_paths


IMMUTABLE_EVENT_TABLES = (
    "stage_events",
    "source_observations",
    "citations",
    "audit_events",
    "application_materials",
    "evaluation_compaction_ledger",
)
IMMUTABLE_ARTIFACT_COLUMNS = (
    "kind",
    "workspace_root",
    "source_path",
    "stored_path",
    "content_hash",
    "size_bytes",
    "mime_type",
    "source_mtime_ns",
    "source_immutable",
    "metadata_json",
)
RESTORE_JOURNAL_FILENAME = ".jobby-restore-journal.json"


class StorageBusyError(RuntimeError):
    """Raised when another Jobby process owns an incompatible storage lock."""


class DatabaseUpgradeRequiredError(RuntimeError):
    """Raised when an existing database needs the explicit upgrade workflow."""


class StorageLock:
    """Advisory process lock shared by every supported Jobby database client.

    Normal database lifetimes hold a shared lock. Destructive maintenance such
    as restore takes the exclusive form, so it can only activate data after all
    scanner and agent processes have closed their database handles.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        exclusive: bool,
        timeout: float = 0.0,
    ) -> None:
        self._descriptor: int | None = None
        if isinstance(timeout, bool) or not isinstance(timeout, Real):
            raise TypeError("storage-lock timeout must be a finite number")
        if not math.isfinite(float(timeout)) or timeout < 0:
            raise ValueError(
                "storage-lock timeout must be a finite non-negative number"
            )
        self.path = Path(path).expanduser().absolute()
        self.exclusive = exclusive
        self.timeout = float(timeout)

    def acquire(self) -> "StorageLock":
        if self._descriptor is not None:
            return self
        if self.path.is_symlink():
            raise ValueError("storage lock must not be a symbolic link")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(self.path, flags, 0o600)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("storage lock must be a regular file")
            if metadata.st_nlink != 1:
                raise ValueError("storage lock must not have multiple hard links")
            os.fchmod(descriptor, 0o600)
            try:
                import fcntl
            except ImportError as exc:  # pragma: no cover - V1 is POSIX-only
                raise RuntimeError("storage locking requires macOS or Linux") from exc
            operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        mode = "exclusive maintenance" if self.exclusive else "database"
                        raise StorageBusyError(
                            f"Jobby storage is busy; close other Jobby processes before {mode} access"
                        ) from exc
                    time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        except Exception:
            os.close(descriptor)
            raise
        self._descriptor = descriptor
        return self

    def release(self) -> None:
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is None:
            return
        try:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def __enter__(self) -> "StorageLock":
        return self.acquire()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()

    def __del__(self) -> None:
        # Process shutdown and abandoned short-lived CLI objects must not leave
        # advisory descriptors open. Explicit ``release``/``dispose`` remains
        # the deterministic path.
        with suppress(OSError, ImportError):
            self.release()


class Database:
    def __init__(
        self,
        path: Path | str | None = None,
        *,
        paths: JobbyPaths | None = None,
        acquire_lock: bool = True,
        lock_timeout: float = 0.0,
    ):
        self.paths = paths or resolve_paths()
        requested_path = Path(path or self.paths.database).expanduser()
        if requested_path.is_symlink():
            raise ValueError("database path must not be a symbolic link")
        self.path = requested_path.absolute()
        cache_dir = (
            self.paths.cache_dir
            if paths is not None or path is None
            else self.path.parent / "cache"
        )
        restore_data_dir = (
            self.paths.data_dir
            if paths is not None or path is None
            else self.path.parent
        )
        self.search_index_path = cache_dir / "search-index.sqlite3"
        self.restore_journal_path = restore_data_dir / RESTORE_JOURNAL_FILENAME
        if self.path.exists() and not self.path.is_file():
            raise ValueError("database path must be a regular file")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            URL.create("sqlite+pysqlite", database=str(self.path)),
            future=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        event.listen(self.engine, "connect", self._configure_sqlite)
        self.Session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=Session,
            info={"jobby_database": self},
        )
        self._initialized = False
        self._storage_lock = (
            StorageLock(
                self.path.parent / ".jobby.lock",
                exclusive=False,
                timeout=lock_timeout,
            )
            if acquire_lock
            else None
        )

    @staticmethod
    def _configure_sqlite(
        dbapi_connection: sqlite3.Connection, connection_record: object
    ) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()

    def initialize(self) -> None:
        if self._initialized:
            return
        restore_journal = self.restore_journal_path
        if self._storage_lock is not None and (
            restore_journal.exists() or restore_journal.is_symlink()
        ):
            raise StorageBusyError(
                "an interrupted restore requires recovery before database access"
            )
        if self._storage_lock is not None:
            self._storage_lock.acquire()
        try:
            if self.path.is_symlink():
                raise ValueError("database path became a symbolic link")
            from .upgrade import upgrade_status

            status = upgrade_status(self.path)
            if status.recovery_required:
                raise DatabaseUpgradeRequiredError(
                    "an interrupted database upgrade requires `jobby upgrade recover` "
                    "before Jobby can open this database"
                )
            if status.state == "upgrade_required":
                pending = ", ".join(status.pending_revisions)
                raise DatabaseUpgradeRequiredError(
                    "database upgrade required; inspect it with `jobby upgrade status` "
                    f"and apply it explicitly with `jobby upgrade apply` (pending: {pending})"
                )
            if status.state in {"future", "invalid"}:
                raise DatabaseUpgradeRequiredError(status.detail)

            from alembic import command
            from alembic.config import Config

            if status.state == "fresh":
                migration_dir = Path(__file__).with_name("migrations")
                config = Config()
                config.set_main_option("script_location", str(migration_dir))
                # Hand Alembic the already constructed engine connection. Rendering a
                # filesystem path back into a URL is lossy for valid characters such as
                # ``?`` and ``#`` and can migrate the wrong SQLite file.
                with self.engine.connect() as connection:
                    config.attributes["connection"] = connection
                    command.upgrade(config, "head")
            self._install_immutability_triggers()
            self._secure_database_files()
            self._initialized = True
        except Exception:
            if self._storage_lock is not None:
                self._storage_lock.release()
            raise

    def _secure_database_files(self) -> None:
        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            flags = (
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                descriptor = os.open(candidate, flags)
            except FileNotFoundError:
                continue
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise ValueError(
                        "database files must be regular, non-symbolic files"
                    )
                os.fchmod(descriptor, 0o600)
            finally:
                os.close(descriptor)

    def _install_immutability_triggers(self) -> None:
        """Protect append-only history even from direct SQLite writes.

        ORM listeners provide an early, readable error for normal application
        code. These triggers are the final guard for maintenance scripts and
        direct SQL access to the operational source of truth.
        """
        statements: list[str] = []
        for table in IMMUTABLE_EVENT_TABLES:
            statements.extend(
                (
                    f"""
                    CREATE TRIGGER IF NOT EXISTS {table}_immutable_update
                    BEFORE UPDATE ON {table}
                    BEGIN
                        SELECT RAISE(ABORT, '{table} records are immutable');
                    END
                    """,
                    f"""
                    CREATE TRIGGER IF NOT EXISTS {table}_immutable_delete
                    BEFORE DELETE ON {table}
                    BEGIN
                        SELECT RAISE(ABORT, '{table} records are immutable');
                    END
                    """,
                )
            )
        columns = ", ".join(IMMUTABLE_ARTIFACT_COLUMNS)
        statements.extend(
            (
                f"""
                CREATE TRIGGER IF NOT EXISTS artifacts_source_immutable_update
                BEFORE UPDATE OF {columns} ON artifacts
                WHEN OLD.source_immutable = 1
                BEGIN
                    SELECT RAISE(ABORT, 'artifact source fields are immutable');
                END
                """,
                """
                CREATE TRIGGER IF NOT EXISTS artifacts_source_immutable_delete
                BEFORE DELETE ON artifacts
                WHEN OLD.source_immutable = 1
                BEGIN
                    SELECT RAISE(ABORT, 'artifact source records are immutable');
                END
                """,
            )
        )
        with self.engine.begin() as connection:
            for statement in statements:
                connection.exec_driver_sql(statement)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.Session()
        session.info["jobby_database"] = self
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def integrity_check(self) -> tuple[bool, str]:
        with self.engine.connect() as connection:
            results = list(connection.execute(text("PRAGMA integrity_check")).scalars())
            foreign_key_error = connection.execute(
                text("PRAGMA foreign_key_check")
            ).first()
        if results != ["ok"]:
            return False, "; ".join(str(item) for item in results) or "no result"
        if foreign_key_error is not None:
            return False, f"foreign key violation: {tuple(foreign_key_error)}"
        return True, "ok"

    def backup_to(self, destination: Path | str) -> Path:
        if not self._initialized:
            self.initialize()
        requested = Path(destination).expanduser()
        if requested.is_symlink():
            raise ValueError("backup destination must not be a symbolic link")
        destination = requested.absolute()
        if destination == self.path:
            raise ValueError("backup destination must differ from the live database")
        if destination.exists() and not destination.is_file():
            raise ValueError("backup destination must be a regular file")
        destination.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
        )
        os.close(file_descriptor)
        temporary = Path(temporary_name)
        source: sqlite3.Connection | None = None
        target: sqlite3.Connection | None = None
        try:
            try:
                source = sqlite3.connect(self.path)
                target = sqlite3.connect(temporary)
                source.backup(target)
                target.commit()
                integrity = target.execute("PRAGMA integrity_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    detail = integrity[0] if integrity else "no result"
                    raise sqlite3.DatabaseError(
                        f"backup integrity check failed: {detail}"
                    )
                foreign_key_error = target.execute(
                    "PRAGMA foreign_key_check"
                ).fetchone()
                if foreign_key_error is not None:
                    raise sqlite3.IntegrityError(
                        f"backup foreign-key check failed: {foreign_key_error}"
                    )
            finally:
                if target is not None:
                    target.close()
                if source is not None:
                    source.close()
            temporary.chmod(0o600)
            sync_descriptor = os.open(temporary, os.O_RDONLY)
            try:
                os.fsync(sync_descriptor)
            finally:
                os.close(sync_descriptor)
            os.replace(temporary, destination)
            _sync_directory(destination.parent)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return destination

    def dispose(self) -> None:
        self.engine.dispose()
        if self._storage_lock is not None:
            self._storage_lock.release()
        self._initialized = False


def create_database(
    path: Path | str | None = None, *, initialize: bool = True
) -> Database:
    database = Database(path)
    if initialize:
        database.initialize()
    return database


def table_names(engine: Engine) -> set[str]:
    from sqlalchemy import inspect

    return set(inspect(engine).get_table_names())


def _sync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
