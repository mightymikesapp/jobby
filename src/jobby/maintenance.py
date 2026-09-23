"""Explicit, provenance-preserving operational maintenance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy import func, or_, select

from .audit import record_audit
from .config import JobbyPaths
from .db import Database, StorageLock
from .enums import AgentRunStatus
from .models import (
    AICacheEntry,
    AgentRun,
    Base,
    MaintenanceRun,
    ScanRun,
)


@dataclass(frozen=True, slots=True)
class TableStorage:
    table: str
    rows: int
    bytes: int | None


@dataclass(frozen=True, slots=True)
class MaintenanceStatus:
    database_path: Path
    database_bytes: int
    wal_bytes: int
    page_count: int
    page_size: int
    freelist_pages: int
    tables: tuple[TableStorage, ...]


@dataclass(frozen=True, slots=True)
class MaintenanceResult:
    kind: str
    changed: int
    detail: str
    run_id: str | None = None


def _run_exclusive_maintenance(
    database_path: Path | str,
    *,
    paths: JobbyPaths,
    lock_timeout: float,
    operation: Callable[[Database], MaintenanceResult],
) -> MaintenanceResult:
    """Open a dedicated database handle while holding the storage lock."""

    if lock_timeout < 0:
        raise ValueError("maintenance lock timeout must not be negative")
    path = Path(database_path).expanduser().absolute()
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError("maintenance database must be a regular file")
    with StorageLock(path.parent / ".jobby.lock", exclusive=True, timeout=lock_timeout):
        database = Database(path, paths=paths, acquire_lock=False)
        try:
            database.initialize()
            return operation(database)
        finally:
            database.dispose()


def maintenance_status(database: Database) -> MaintenanceStatus:
    """Report storage without modifying the operational database."""

    database.initialize()
    with database.session() as session:
        connection = session.connection()
        page_count = int(connection.exec_driver_sql("PRAGMA page_count").scalar() or 0)
        page_size = int(connection.exec_driver_sql("PRAGMA page_size").scalar() or 0)
        freelist = int(
            connection.exec_driver_sql("PRAGMA freelist_count").scalar() or 0
        )
        sizes: dict[str, int] = {}
        try:
            sizes = {
                str(name): int(size or 0)
                for name, size in connection.exec_driver_sql(
                    "SELECT name, sum(pgsize) FROM dbstat GROUP BY name"
                )
            }
        except Exception:
            # ``dbstat`` is optional in SQLite builds. Row counts remain useful.
            sizes = {}
        tables: list[TableStorage] = []
        for table_name in sorted(Base.metadata.tables):
            table = Base.metadata.tables[table_name]
            rows = int(session.scalar(select(func.count()).select_from(table)) or 0)
            tables.append(TableStorage(table_name, rows, sizes.get(table_name)))
    wal = Path(f"{database.path}-wal")
    return MaintenanceStatus(
        database_path=database.path,
        database_bytes=database.path.stat().st_size if database.path.exists() else 0,
        wal_bytes=wal.stat().st_size if wal.exists() and not wal.is_symlink() else 0,
        page_count=page_count,
        page_size=page_size,
        freelist_pages=freelist,
        tables=tuple(tables),
    )


def optimize_database(
    database_path: Path | str,
    *,
    paths: JobbyPaths,
    lock_timeout: float = 0.0,
) -> MaintenanceResult:
    """Checkpoint WAL safely and run SQLite's non-destructive optimizer."""

    path = Path(database_path).expanduser().absolute()
    if path.is_symlink() or not path.is_file():
        raise ValueError("maintenance database must be an existing regular file")
    with StorageLock(path.parent / ".jobby.lock", exclusive=True, timeout=lock_timeout):
        database = Database(path, paths=paths, acquire_lock=False)
        try:
            database.initialize()
            with database.engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                checkpoint = tuple(
                    connection.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)").one()
                )
                if (
                    len(checkpoint) != 3
                    or isinstance(checkpoint[0], bool)
                    or not isinstance(checkpoint[0], int)
                    or checkpoint[0] != 0
                ):
                    raise RuntimeError(
                        "WAL checkpoint did not complete; another SQLite client may be active"
                    )
                connection.exec_driver_sql("PRAGMA optimize")
            with database.session() as session:
                run = MaintenanceRun(
                    kind="optimize",
                    status="succeeded",
                    started_at=datetime.now(timezone.utc),
                    finished_at=datetime.now(timezone.utc),
                    result_json={"wal_checkpoint": list(checkpoint)},
                )
                session.add(run)
                session.flush()
                record_audit(
                    session,
                    action="maintenance.optimized",
                    entity_type="maintenance_run",
                    entity_id=run.id,
                    actor="user",
                    after=run.result_json,
                )
                return MaintenanceResult(
                    kind="optimize",
                    changed=0,
                    detail=f"WAL checkpoint result {checkpoint}; PRAGMA optimize completed",
                    run_id=run.id,
                )
        finally:
            database.dispose()


def rescore_evaluations(
    database: Database, config: object, *, batch_size: int = 500
) -> MaintenanceResult:
    """Re-run the deterministic ranker for every job with the current profile.

    Each job keeps its evaluation history: ``persist_evaluation`` reuses an
    identical automatic result, writes a new current row when the ranker or
    profile changed, and never replaces a locked manual override. Statuses
    follow the scan rule for automatically skipped roles.
    """

    from .models import Evaluation, Job
    from .ranking import (
        RANKER_VERSION,
        apply_automatic_status,
        persist_evaluation,
        ranking_profile_from_database,
    )

    if not 1 <= batch_size <= 5_000:
        raise ValueError("rescore batch size must be between 1 and 5,000")
    started = datetime.now(timezone.utc)
    profile = ranking_profile_from_database(database, config)
    with database.session() as session:
        job_ids = list(session.scalars(select(Job.id).order_by(Job.id)))
    changed = 0
    for offset in range(0, len(job_ids), batch_size):
        with database.session() as session:
            for job_id in job_ids[offset : offset + batch_size]:
                before = session.scalar(
                    select(Evaluation.id).where(
                        Evaluation.job_id == job_id, Evaluation.is_current.is_(True)
                    )
                )
                row = persist_evaluation(session, job_id, profile=profile)
                if row.id != before:
                    changed += 1
                # Same status rule as a scan: skipped roles are ignored and
                # reopen once the skip lifts; user-locked statuses are kept.
                apply_automatic_status(session.get(Job, job_id), row)
    with database.session() as session:
        run = MaintenanceRun(
            kind="rescore",
            status="succeeded",
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            result_json={
                "jobs": len(job_ids),
                "rescored": changed,
                "ranker_version": RANKER_VERSION,
            },
        )
        session.add(run)
        session.flush()
        record_audit(
            session,
            action="maintenance.rescored",
            entity_type="maintenance_run",
            entity_id=run.id,
            actor="user",
            after=run.result_json,
        )
        return MaintenanceResult(
            kind="rescore",
            changed=changed,
            detail=(
                f"{changed:,} of {len(job_ids):,} jobs have a new current evaluation "
                f"from {RANKER_VERSION}"
            ),
            run_id=run.id,
        )


def rescore_evaluations_exclusive(
    database_path: Path | str,
    *,
    paths: JobbyPaths,
    config: object,
    lock_timeout: float = 0.0,
) -> MaintenanceResult:
    return _run_exclusive_maintenance(
        database_path,
        paths=paths,
        lock_timeout=lock_timeout,
        operation=lambda database: rescore_evaluations(database, config),
    )


def recover_stale_runs(
    database: Database,
    *,
    stale_after: timedelta,
    now: datetime | None = None,
) -> MaintenanceResult:
    """Mark interrupted scan and agent records failed without deleting history."""

    if stale_after <= timedelta(0):
        raise ValueError("stale run threshold must be positive")
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - stale_after
    changed = 0
    with database.session() as session:
        scan_runs = list(
            session.scalars(
                select(ScanRun).where(
                    ScanRun.status == AgentRunStatus.RUNNING,
                    or_(ScanRun.started_at.is_(None), ScanRun.started_at < cutoff),
                )
            )
        )
        agent_runs = list(
            session.scalars(
                select(AgentRun).where(
                    AgentRun.status == AgentRunStatus.RUNNING,
                    or_(AgentRun.started_at.is_(None), AgentRun.started_at < cutoff),
                )
            )
        )
        for run in scan_runs:
            run.status = AgentRunStatus.FAILED
            run.finished_at = now
            run.error_summary = "Recovered stale RUNNING scan after interruption."
            changed += 1
        for run in agent_runs:
            run.status = AgentRunStatus.FAILED
            run.finished_at = now
            run.error = "Recovered stale RUNNING agent after interruption."
            run.summary = {**dict(run.summary or {}), "stale_recovered": True}
            changed += 1
        maintenance = MaintenanceRun(
            kind="recover_stale_runs",
            status="succeeded",
            started_at=now,
            finished_at=now,
            result_json={
                "scan_runs": len(scan_runs),
                "agent_runs": len(agent_runs),
            },
        )
        session.add(maintenance)
        session.flush()
        record_audit(
            session,
            action="maintenance.stale_runs_recovered",
            entity_type="maintenance_run",
            entity_id=maintenance.id,
            actor="user",
            after=maintenance.result_json,
        )
        return MaintenanceResult(
            kind="recover_stale_runs",
            changed=changed,
            detail=(
                f"recovered {len(scan_runs)} scan run(s) and "
                f"{len(agent_runs)} agent run(s)"
            ),
            run_id=maintenance.id,
        )


def recover_stale_runs_exclusive(
    database_path: Path | str,
    *,
    paths: JobbyPaths,
    stale_after: timedelta,
    now: datetime | None = None,
    lock_timeout: float = 0.0,
) -> MaintenanceResult:
    """Recover stale rows under the exclusive process-wide storage lock."""

    return _run_exclusive_maintenance(
        database_path,
        paths=paths,
        lock_timeout=lock_timeout,
        operation=lambda database: recover_stale_runs(
            database,
            stale_after=stale_after,
            now=now,
        ),
    )


def clean_expired_cache(
    database: Database,
    *,
    now: datetime | None = None,
) -> MaintenanceResult:
    """Deactivate expired AI cache rows while retaining their provenance."""

    now = _aware(now or datetime.now(timezone.utc))
    with database.session() as session:
        entries = list(
            session.scalars(
                select(AICacheEntry).where(
                    AICacheEntry.active.is_(True),
                    AICacheEntry.expires_at <= now,
                )
            )
        )
        for entry in entries:
            entry.active = False
            entry.invalidated_at = now
        maintenance = MaintenanceRun(
            kind="clean_expired_cache",
            status="succeeded",
            started_at=now,
            finished_at=now,
            result_json={"deactivated": len(entries), "deleted": 0},
        )
        session.add(maintenance)
        session.flush()
        record_audit(
            session,
            action="maintenance.expired_cache_deactivated",
            entity_type="maintenance_run",
            entity_id=maintenance.id,
            actor="user",
            after=maintenance.result_json,
        )
        return MaintenanceResult(
            kind="clean_expired_cache",
            changed=len(entries),
            detail=f"deactivated {len(entries)} expired cache row(s); deleted none",
            run_id=maintenance.id,
        )


def clean_expired_cache_exclusive(
    database_path: Path | str,
    *,
    paths: JobbyPaths,
    now: datetime | None = None,
    lock_timeout: float = 0.0,
) -> MaintenanceResult:
    """Deactivate expired cache rows under the exclusive storage lock."""

    return _run_exclusive_maintenance(
        database_path,
        paths=paths,
        lock_timeout=lock_timeout,
        operation=lambda database: clean_expired_cache(database, now=now),
    )


def stale_run_count(database: Database, *, cutoff: datetime) -> int:
    """Small read-only helper used by operational diagnostics."""

    cutoff = _aware(cutoff)
    with database.session() as session:
        scans = int(
            session.scalar(
                select(func.count(ScanRun.id)).where(
                    ScanRun.status == AgentRunStatus.RUNNING,
                    or_(ScanRun.started_at.is_(None), ScanRun.started_at < cutoff),
                )
            )
            or 0
        )
        agents = int(
            session.scalar(
                select(func.count(AgentRun.id)).where(
                    AgentRun.status == AgentRunStatus.RUNNING,
                    or_(AgentRun.started_at.is_(None), AgentRun.started_at < cutoff),
                )
            )
            or 0
        )
    return scans + agents


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


__all__ = [
    "MaintenanceResult",
    "MaintenanceStatus",
    "TableStorage",
    "clean_expired_cache",
    "clean_expired_cache_exclusive",
    "maintenance_status",
    "optimize_database",
    "recover_stale_runs",
    "recover_stale_runs_exclusive",
    "stale_run_count",
]
