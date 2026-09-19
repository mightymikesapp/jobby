"""Disposable FTS5 trigram index synchronized from the operational database.

The SQLite cache is deliberately not part of backups.  Operational triggers
record generation-numbered job changes; applying cache writes before deleting
those exact queue generations makes synchronization safe to repeat after an
interruption.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence

from sqlalchemy import delete, func, select, text
from sqlalchemy.orm import Session

from .db import StorageLock
from .models import Company, Job, SearchIndexChange, SearchIndexState

if TYPE_CHECKING:
    from .db import Database
    from .config import JobbyPaths


SEARCH_INDEX_FILENAME = "search-index.sqlite3"
SEARCH_INDEX_SCHEMA_VERSION = 1
DEFAULT_EXCESSIVE_MATCH_LIMIT = 900
MAX_INCREMENTAL_CHANGES = 50_000
SEARCH_INDEX_LOCK_TIMEOUT = 10.0


@dataclass(frozen=True, slots=True)
class SearchIndexStatus:
    path: Path
    fts_available: bool
    fts_detail: str
    exists: bool
    integrity_ok: bool
    integrity_detail: str
    indexed_jobs: int
    operational_jobs: int
    pending_changes: int
    applied_generation: int
    current_generation: int

    @property
    def synchronized(self) -> bool:
        return (
            self.exists
            and self.integrity_ok
            and self.indexed_jobs == self.operational_jobs
            and self.pending_changes == 0
            and self.applied_generation == self.current_generation
        )


@dataclass(frozen=True, slots=True)
class SearchCandidateSnapshot:
    """Candidate IDs tied to the cache generation that produced them."""

    job_ids: tuple[str, ...]
    generation: int


class SearchIndex:
    """Manage one rebuildable cache belonging to a :class:`Database`."""

    def __init__(
        self,
        database: Database,
        *,
        path: Path | str | None = None,
        excessive_match_limit: int = DEFAULT_EXCESSIVE_MATCH_LIMIT,
    ) -> None:
        if excessive_match_limit < 1:
            raise ValueError("search-index match limit must be positive")
        self.database = database
        self.path = Path(path or database.search_index_path).expanduser().absolute()
        self.excessive_match_limit = excessive_match_limit

    @staticmethod
    def fts5_available() -> tuple[bool, str]:
        """Probe the exact FTS5 tokenizer required by this index."""

        connection = sqlite3.connect(":memory:")
        try:
            connection.execute(
                "CREATE VIRTUAL TABLE probe USING fts5(value, tokenize='trigram')"
            )
            connection.execute("INSERT INTO probe(value) VALUES ('availability')")
            found = connection.execute(
                "SELECT count(*) FROM probe WHERE probe MATCH ?", ('"ail"',)
            ).fetchone()
            if not found or found[0] != 1:
                return False, "FTS5 trigram probe returned an unexpected result"
            return True, "FTS5 trigram tokenizer is available"
        except sqlite3.Error as exc:
            return False, str(exc) or exc.__class__.__name__
        finally:
            connection.close()

    def status(self, session: Session | None = None) -> SearchIndexStatus:
        """Inspect cache and queue state without creating or repairing the cache."""

        owns_session = session is None
        session = session or self.database.Session()
        try:
            operational_jobs = int(
                session.scalar(select(func.count()).select_from(Job)) or 0
            )
            pending = int(
                session.scalar(select(func.count()).select_from(SearchIndexChange)) or 0
            )
            generation = int(
                session.scalar(
                    select(SearchIndexState.generation).where(SearchIndexState.id == 1)
                )
                or 0
            )
            available, available_detail = self.fts5_available()
            exists = (
                self.path.exists()
                and self.path.is_file()
                and not self.path.is_symlink()
            )
            integrity_ok = False
            integrity_detail = "cache has not been built"
            indexed_jobs = 0
            applied_generation = 0
            if exists:
                try:
                    connection = self._connect(create_parent=False)
                    try:
                        integrity_rows = [
                            str(row[0])
                            for row in connection.execute("PRAGMA integrity_check")
                        ]
                        integrity_ok = integrity_rows == ["ok"]
                        integrity_detail = (
                            "ok" if integrity_ok else "; ".join(integrity_rows)
                        )
                        if integrity_ok:
                            schema_version = self._metadata_value(
                                connection, "schema_version"
                            )
                            if schema_version != str(SEARCH_INDEX_SCHEMA_VERSION):
                                integrity_ok = False
                                integrity_detail = "cache schema version is unsupported"
                            else:
                                indexed_jobs = int(
                                    connection.execute(
                                        "SELECT count(*) FROM job_search"
                                    ).fetchone()[0]
                                )
                                applied_generation = int(
                                    self._metadata_value(
                                        connection, "applied_generation"
                                    )
                                    or 0
                                )
                    finally:
                        connection.close()
                except (OSError, sqlite3.Error, ValueError) as exc:
                    integrity_ok = False
                    integrity_detail = str(exc) or exc.__class__.__name__
            return SearchIndexStatus(
                path=self.path,
                fts_available=available,
                fts_detail=available_detail,
                exists=exists,
                integrity_ok=integrity_ok,
                integrity_detail=integrity_detail,
                indexed_jobs=indexed_jobs,
                operational_jobs=operational_jobs,
                pending_changes=pending,
                applied_generation=applied_generation,
                current_generation=generation,
            )
        finally:
            if owns_session:
                session.close()

    def rebuild(self, session: Session | None = None) -> SearchIndexStatus:
        """Atomically replace and durably acknowledge one operational snapshot.

        A supplied session is committed before the cross-process index lock is
        released. Callers should therefore pass only a dedicated maintenance
        session, never a transaction containing unrelated user changes.
        """

        owns_session = session is None
        session = session or self.database.Session()
        try:
            with self._lock():
                status = self._rebuild_locked(session)
                session.commit()
                return status
        except Exception:
            session.rollback()
            raise
        finally:
            if owns_session:
                session.close()

    def _rebuild_locked(self, session: Session) -> SearchIndexStatus:
        """Rebuild while the caller holds the search-index process lock."""

        available, detail = self.fts5_available()
        if not available:
            raise RuntimeError(f"FTS5 trigram search is unavailable: {detail}")
        temporary: Path | None = None
        try:
            generation = int(
                session.scalar(
                    select(SearchIndexState.generation).where(SearchIndexState.id == 1)
                )
                or 0
            )
            rows = session.execute(
                select(
                    Job.id,
                    Job.title,
                    Job.description,
                    Job.category,
                    Company.name,
                )
                .join(Company, Company.id == Job.company_id)
                .order_by(Job.id)
                .execution_options(yield_per=500)
            )
            self._validate_target()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.path.parent.chmod(0o700)
            except OSError:
                pass
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
            )
            os.close(descriptor)
            temporary = Path(temporary_name)
            connection = sqlite3.connect(temporary)
            try:
                self._configure_connection(connection)
                self._create_schema(connection)
                connection.executemany(
                    "INSERT INTO job_search(job_id, title, description, category, company) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        (
                            row.id,
                            row.title or "",
                            row.description or "",
                            row.category or "",
                            row.name or "",
                        )
                        for row in rows
                    ),
                )
                self._set_metadata(connection, "applied_generation", str(generation))
                connection.commit()
                integrity = connection.execute("PRAGMA integrity_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    raise sqlite3.DatabaseError(
                        f"rebuilt search index failed integrity check: {integrity}"
                    )
            finally:
                connection.close()
            temporary.chmod(0o600)
            _fsync_file(temporary)
            self._remove_sidecars()
            os.replace(temporary, self.path)
            temporary = None
            _fsync_file(self.path)
            _fsync_directory(self.path.parent)
            # The cache commit/replace happens first.  If this operational
            # deletion rolls back, replaying the retained generations is safe.
            session.execute(
                delete(SearchIndexChange).where(
                    SearchIndexChange.generation <= generation
                )
            )
            return self.status(session)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def synchronize(self, session: Session | None = None) -> None:
        """Durably apply queued changes and then acknowledge exact generations.

        As with :meth:`rebuild`, a supplied session is a dedicated maintenance
        session and is committed before the index lock is released.
        """

        owns_session = session is None
        session = session or self.database.Session()
        try:
            with self._lock():
                self._synchronize_locked(session)
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            if owns_session:
                session.close()

    def _synchronize_locked(self, session: Session) -> None:
        """Synchronize while the caller holds the search-index process lock."""

        if not self._healthy_cache():
            self._rebuild_locked(session)
            return
        current_generation = int(
            session.scalar(
                select(SearchIndexState.generation).where(SearchIndexState.id == 1)
            )
            or 0
        )
        changes = list(
            session.execute(
                select(SearchIndexChange.job_id, SearchIndexChange.generation)
                .order_by(SearchIndexChange.generation, SearchIndexChange.job_id)
                .limit(MAX_INCREMENTAL_CHANGES + 1)
            )
        )
        if not changes:
            if self._applied_generation() != current_generation:
                # A structural check cannot prove content corresponds to the
                # operational generation when no replayable queue remains.
                self._rebuild_locked(session)
            return
        if len(changes) > MAX_INCREMENTAL_CHANGES:
            # Returning candidates from a partly synchronized cache would
            # change search semantics. A large backlog is cheaper and safer to
            # collapse into one snapshot rebuild.
            self._rebuild_locked(session)
            return
        job_ids = sorted({str(row.job_id) for row in changes})
        current: dict[str, tuple[str, str, str, str, str]] = {}
        for chunk in _chunks(job_ids, 500):
            for row in session.execute(
                select(
                    Job.id,
                    Job.title,
                    Job.description,
                    Job.category,
                    Company.name,
                )
                .join(Company, Company.id == Job.company_id)
                .where(Job.id.in_(chunk))
            ):
                current[row.id] = (
                    row.id,
                    row.title or "",
                    row.description or "",
                    row.category or "",
                    row.name or "",
                )
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany(
                    "DELETE FROM job_search WHERE job_id = ?",
                    ((job_id,) for job_id in job_ids),
                )
                present = [current[job_id] for job_id in job_ids if job_id in current]
                if present:
                    connection.executemany(
                        "INSERT INTO job_search(job_id, title, description, category, company) "
                        "VALUES (?, ?, ?, ?, ?)",
                        present,
                    )
                self._set_metadata(
                    connection,
                    "applied_generation",
                    str(current_generation),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        except (OSError, sqlite3.Error, ValueError):
            # A corrupt cache is derived data.  Rebuild it from operational
            # truth rather than surfacing a permanent search failure.
            self._invalidate_locked()
            self._rebuild_locked(session)
            return
        _fsync_file(self.path)
        _fsync_directory(self.path.parent)
        session.execute(
            text(
                "DELETE FROM search_index_changes "
                "WHERE job_id = :job_id AND generation = :generation"
            ),
            [{"job_id": row.job_id, "generation": row.generation} for row in changes],
        )

    def candidate_job_ids(
        self,
        session: Session,
        *,
        query_terms: Sequence[str] = (),
        industry_terms: Sequence[str] = (),
    ) -> tuple[str, ...] | None:
        """Return candidate IDs, or ``None`` when callers should use LIKE.

        Terms below three characters cannot be represented faithfully by the
        trigram tokenizer.  An excessive candidate set similarly falls back so
        the operational query remains bounded and semantically authoritative.
        """

        snapshot = self.candidate_snapshot(
            session,
            query_terms=query_terms,
            industry_terms=industry_terms,
        )
        return snapshot.job_ids if snapshot is not None else None

    def candidate_snapshot(
        self,
        session: Session,
        *,
        query_terms: Sequence[str] = (),
        industry_terms: Sequence[str] = (),
    ) -> SearchCandidateSnapshot | None:
        """Return candidates plus the cache generation used to produce them."""

        del session  # synchronization owns its operational maintenance session
        query_terms = tuple(term for term in query_terms if term)
        industry_terms = tuple(term for term in industry_terms if term)
        all_terms = (*query_terms, *industry_terms)
        if not all_terms or any(len(term) < 3 for term in all_terms):
            return None
        available, _detail = self.fts5_available()
        if not available:
            return None
        try:
            with self._lock():
                maintenance = self.database.Session()
                try:
                    self._synchronize_locked(maintenance)
                    maintenance.commit()
                except Exception:
                    maintenance.rollback()
                    raise
                finally:
                    maintenance.close()
                expression_parts = [_quote_match(term) for term in query_terms]
                if industry_terms:
                    expression_parts.append(
                        "("
                        + " OR ".join(_quote_match(term) for term in industry_terms)
                        + ")"
                    )
                expression = " AND ".join(expression_parts)
                connection = self._connect(create_parent=False)
                try:
                    rows = connection.execute(
                        "SELECT job_id FROM job_search "
                        "WHERE job_search MATCH ? LIMIT ?",
                        (expression, self.excessive_match_limit + 1),
                    ).fetchall()
                    generation = int(
                        self._metadata_value(connection, "applied_generation") or 0
                    )
                finally:
                    connection.close()
            if len(rows) > self.excessive_match_limit:
                return None
            return SearchCandidateSnapshot(
                job_ids=tuple(str(row[0]) for row in rows),
                generation=generation,
            )
        except (OSError, sqlite3.Error, ValueError, RuntimeError):
            # Search must remain available through literal LIKE if the optional
            # derived cache is unavailable for any reason.
            return None

    def invalidate(self) -> None:
        """Remove all derived cache files; operational state is untouched."""

        with self._lock():
            self._invalidate_locked()

    def _invalidate_locked(self) -> None:
        """Invalidate while the caller holds the search-index process lock."""

        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
            Path(f"{self.path}-journal"),
        ):
            if candidate.is_symlink():
                candidate.unlink(missing_ok=True)
            elif candidate.exists() and candidate.is_file():
                candidate.unlink(missing_ok=True)

    def _lock(self) -> StorageLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.path.parent.chmod(0o700)
        except OSError:
            pass
        return StorageLock(
            self.path.parent / ".jobby-search-index.lock",
            exclusive=True,
            timeout=SEARCH_INDEX_LOCK_TIMEOUT,
        )

    def _applied_generation(self) -> int:
        connection = self._connect(create_parent=False)
        try:
            return int(self._metadata_value(connection, "applied_generation") or 0)
        finally:
            connection.close()

    def _remove_sidecars(self) -> None:
        for candidate in (
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
            Path(f"{self.path}-journal"),
        ):
            if candidate.is_symlink() or (candidate.exists() and candidate.is_file()):
                candidate.unlink(missing_ok=True)

    def _healthy_cache(self) -> bool:
        if not self.path.exists() or self.path.is_symlink() or not self.path.is_file():
            return False
        try:
            connection = self._connect(create_parent=False)
            try:
                integrity = connection.execute("PRAGMA quick_check").fetchone()
                return bool(
                    integrity
                    and integrity[0] == "ok"
                    and self._metadata_value(connection, "schema_version")
                    == str(SEARCH_INDEX_SCHEMA_VERSION)
                )
            finally:
                connection.close()
        except (OSError, sqlite3.Error, ValueError):
            return False

    def _connect(self, *, create_parent: bool = True) -> sqlite3.Connection:
        self._validate_target()
        if create_parent:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        self._configure_connection(connection)
        return connection

    def _validate_target(self) -> None:
        if self.path.is_symlink():
            raise ValueError("search-index cache must not be a symbolic link")
        if self.path.exists() and not self.path.is_file():
            raise ValueError("search-index cache must be a regular file")

    @staticmethod
    def _configure_connection(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE search_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE VIRTUAL TABLE job_search USING fts5("
            "job_id UNINDEXED, title, description, category, company, "
            "tokenize='trigram')"
        )
        SearchIndex._set_metadata(
            connection, "schema_version", str(SEARCH_INDEX_SCHEMA_VERSION)
        )
        SearchIndex._set_metadata(connection, "applied_generation", "0")

    @staticmethod
    def _metadata_value(connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute(
            "SELECT value FROM search_metadata WHERE key = ?", (key,)
        ).fetchone()
        return str(row[0]) if row else None

    @staticmethod
    def _set_metadata(connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO search_metadata(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def invalidate_search_index(paths: JobbyPaths) -> None:
    """Invalidate a restored home's derived cache without opening its database."""

    path = paths.cache_dir / SEARCH_INDEX_FILENAME
    for candidate in (
        path,
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
        Path(f"{path}-journal"),
    ):
        if candidate.is_symlink() or (candidate.exists() and candidate.is_file()):
            candidate.unlink(missing_ok=True)


def _quote_match(value: str) -> str:
    """Represent one literal substring safely in an FTS5 MATCH expression."""

    return '"' + value.replace('"', '""') + '"'


def _chunks(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "SEARCH_INDEX_FILENAME",
    "SearchCandidateSnapshot",
    "SearchIndex",
    "SearchIndexStatus",
    "invalidate_search_index",
]
