"""Offline scale fixtures and repeatable release-performance gates.

The exact gate is intentionally opt-in.  It creates a temporary operational
database through the normal migration path, bulk-loads deterministic local
fixtures, exercises SQL-backed pages and the streaming CSV exporter, and
returns a machine-readable report.  No network, scheduler, integration, or
production-home access is involved.
"""

from __future__ import annotations

import gc
import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from sqlalchemy import Table, func, insert, select

from .db import Database
from .enums import ApplicationStage, JobStatus
from .exporter import export_data
from .job_queries import (
    JobListFilters,
    JobSort,
    RankedView,
    query_jobs_page,
    query_ranked_page,
)
from .models import Application, Company, Job, JobSourceState


REPORT_SCHEMA = "jobby-scale-gate-v1"
FIXTURE_SOURCE = "scale-fixture"
FIXTURE_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
INSERT_BATCH_SIZE = 2_000
UPDATE_BATCH_SIZE = 5_000
COMPANY_TABLE = cast(Table, Company.__table__)
JOB_TABLE = cast(Table, Job.__table__)
APPLICATION_TABLE = cast(Table, Application.__table__)
JOB_SOURCE_STATE_TABLE = cast(Table, JobSourceState.__table__)


@dataclass(frozen=True, slots=True)
class ScaleProfile:
    """Cardinalities for one deterministic scale run."""

    name: str
    jobs: int
    applications: int
    source_state_updates: int
    page_size: int = 100

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("scale profile name must not be blank")
        if self.jobs < 1:
            raise ValueError("scale profile must contain at least one job")
        if not 0 <= self.applications <= self.jobs:
            raise ValueError("applications must be between zero and the job count")
        if self.source_state_updates < 0:
            raise ValueError("source-state updates must not be negative")
        if not 1 <= self.page_size <= 1_000:
            raise ValueError("page size must be between 1 and 1000")

    @property
    def source_state_rows(self) -> int:
        """Use one current-state row per job touched by an update."""

        return min(self.jobs, self.source_state_updates)


SMOKE_PROFILE = ScaleProfile(
    name="smoke",
    jobs=250,
    applications=50,
    source_state_updates=2_000,
    page_size=50,
)
RELEASE_PROFILE = ScaleProfile(
    name="release-0.6",
    jobs=100_000,
    applications=10_000,
    source_state_updates=1_000_000,
    page_size=100,
)


@dataclass(frozen=True, slots=True)
class ScaleBudgets:
    """Portable ceilings; recorded-run comparison catches smaller regressions."""

    max_page_latency_ms: float = 5_000.0
    max_export_seconds: float = 180.0
    max_state_update_seconds: float = 300.0
    max_operation_peak_python_bytes: int = 256 * 1024 * 1024
    max_export_peak_python_bytes: int = 128 * 1024 * 1024
    max_storage_bytes_per_record: int = 32 * 1024
    max_update_growth_bytes_per_operation: float = 64.0


DEFAULT_BUDGETS = ScaleBudgets()


@dataclass(frozen=True, slots=True)
class OperationMeasurement:
    elapsed_seconds: float
    peak_python_bytes: int

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed_seconds * 1_000.0


@dataclass(frozen=True, slots=True)
class StorageSnapshot:
    database_bytes: int
    wal_bytes: int
    shm_bytes: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class ScaleGateReport:
    generated_at: str
    profile: ScaleProfile
    budgets: ScaleBudgets
    runtime: Mapping[str, Any]
    counts: Mapping[str, int]
    operations: Mapping[str, OperationMeasurement]
    storage: Mapping[str, StorageSnapshot]
    derived: Mapping[str, float | int]
    artifacts: Mapping[str, str | int]
    integrity_ok: bool
    integrity_detail: str
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.integrity_ok and not self.violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": REPORT_SCHEMA,
            "generated_at": self.generated_at,
            "profile": asdict(self.profile),
            "budgets": asdict(self.budgets),
            "runtime": dict(self.runtime),
            "counts": dict(self.counts),
            "operations": {
                name: {
                    **asdict(measurement),
                    "elapsed_ms": measurement.elapsed_ms,
                }
                for name, measurement in self.operations.items()
            },
            "storage": {
                name: asdict(snapshot) for name, snapshot in self.storage.items()
            },
            "derived": dict(self.derived),
            "artifacts": dict(self.artifacts),
            "integrity": {
                "ok": self.integrity_ok,
                "detail": self.integrity_detail,
            },
            "violations": list(self.violations),
            "passed": self.passed,
        }

    def with_violations(self, violations: tuple[str, ...]) -> ScaleGateReport:
        return replace(self, violations=self.violations + violations)

    def write_json(self, destination: Path | str) -> Path:
        path = Path(destination).expanduser().absolute()
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(self.to_dict(), handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            temporary.chmod(0o600)
            temporary.replace(path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return path


T = TypeVar("T")


def _measure(operation: Callable[[], T]) -> tuple[T, OperationMeasurement]:
    """Measure wall time and Python allocation peak for one bounded operation."""

    gc.collect()
    tracemalloc.start()
    started = time.perf_counter()
    try:
        result = operation()
        elapsed = time.perf_counter() - started
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, OperationMeasurement(elapsed, peak)


def _chunks(total: int, batch_size: int):
    for start in range(0, total, batch_size):
        yield range(start, min(start + batch_size, total))


def _insert_companies(database: Database, count: int) -> None:
    company_count = min(1_000, max(1, (count + 99) // 100))
    with database.session() as session:
        for batch in _chunks(company_count, INSERT_BATCH_SIZE):
            session.execute(
                insert(COMPANY_TABLE),
                [
                    {
                        "id": f"company-{index:06d}",
                        "name": f"Scale Company {index:06d}",
                        "normalized_name": f"scale company {index:06d}",
                        "created_at": FIXTURE_TIME,
                        "updated_at": FIXTURE_TIME,
                    }
                    for index in batch
                ],
            )


def _insert_jobs(database: Database, count: int) -> None:
    company_count = min(1_000, max(1, (count + 99) // 100))
    with database.session() as session:
        for batch in _chunks(count, INSERT_BATCH_SIZE):
            session.execute(
                insert(JOB_TABLE),
                [
                    {
                        "id": f"job-{index:09d}",
                        "company_id": f"company-{index % company_count:06d}",
                        "title": f"Policy and Legal Technology Role {index:09d}",
                        "normalized_title": (
                            f"policy and legal technology role {index:09d}"
                        ),
                        "description": (
                            "Deterministic offline scale fixture for legal, AI, "
                            f"policy, and intellectual property work shard {index % 97}."
                        ),
                        "status": JobStatus.SAVED
                        if index % 11 == 0
                        else JobStatus.DISCOVERED,
                        "latest_score": (index % 51) / 10.0,
                        "salary_min": 80_000 + (index % 40) * 1_000,
                        "salary_max": 120_000 + (index % 80) * 1_000,
                        "source_primary": FIXTURE_SOURCE,
                        "source_id": f"source-job-{index:09d}",
                        "launch_url": f"https://scale.invalid/jobs/{index:09d}",
                        "comparison_url": f"https://scale.invalid/jobs/{index:09d}",
                        "discovered_at": FIXTURE_TIME - timedelta(seconds=index),
                        "created_at": FIXTURE_TIME,
                        "updated_at": FIXTURE_TIME,
                    }
                    for index in batch
                ],
            )


def _insert_applications(database: Database, count: int) -> None:
    if count == 0:
        return
    with database.session() as session:
        for batch in _chunks(count, INSERT_BATCH_SIZE):
            session.execute(
                insert(APPLICATION_TABLE),
                [
                    {
                        "id": f"application-{index:09d}",
                        "job_id": f"job-{index:09d}",
                        "current_stage": ApplicationStage.APPLIED,
                        "submitted_at": FIXTURE_TIME + timedelta(seconds=index),
                        "applied_score": (index % 51) / 10.0,
                        "applied_ranker_version": "scale-fixture-v1",
                        "applied_sources": [
                            {
                                "source": FIXTURE_SOURCE,
                                "source_job_id": f"source-job-{index:09d}",
                            }
                        ],
                        "created_at": FIXTURE_TIME,
                        "updated_at": FIXTURE_TIME,
                    }
                    for index in batch
                ],
            )


def _insert_source_states(database: Database, count: int) -> None:
    if count == 0:
        return
    with database.session() as session:
        for batch in _chunks(count, INSERT_BATCH_SIZE):
            session.execute(
                insert(JOB_SOURCE_STATE_TABLE),
                [
                    {
                        "id": f"source-state-{index:09d}",
                        "job_id": f"job-{index:09d}",
                        "source": FIXTURE_SOURCE,
                        "source_job_id": f"source-job-{index:09d}",
                        "first_seen_at": FIXTURE_TIME,
                        "last_seen_at": FIXTURE_TIME,
                        "seen_count": 1,
                        "last_content_hash": f"{index:064x}",
                        "last_snapshot_hash": f"{index:064x}",
                        "is_live": True,
                        "created_at": FIXTURE_TIME,
                        "updated_at": FIXTURE_TIME,
                    }
                    for index in batch
                ],
            )


def _update_source_states(database: Database, *, row_count: int, updates: int) -> None:
    """Apply exactly ``updates`` current-state writes without history growth."""

    if updates == 0:
        return
    if row_count == 0:
        raise ValueError("source-state updates require at least one state row")
    completed = 0
    round_number = 0
    statement = """
        UPDATE job_source_states
        SET seen_count = seen_count + 1,
            last_seen_at = ?,
            last_content_hash = ?,
            updated_at = ?
        WHERE id = ?
    """
    while completed < updates:
        this_round = min(row_count, updates - completed)
        observed_at = FIXTURE_TIME + timedelta(minutes=round_number + 1)
        timestamp = observed_at.isoformat(sep=" ")
        content_hash = f"{round_number + 1:064x}"
        with database.engine.begin() as connection:
            for batch in _chunks(this_round, UPDATE_BATCH_SIZE):
                connection.exec_driver_sql(
                    statement,
                    [
                        (
                            timestamp,
                            content_hash,
                            timestamp,
                            f"source-state-{index:09d}",
                        )
                        for index in batch
                    ],
                )
        completed += this_round
        round_number += 1


def _storage_snapshot(database: Database) -> StorageSnapshot:
    """Checkpoint the WAL so stage-to-stage growth is comparable."""

    connection = sqlite3.connect(database.path, timeout=30)
    try:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    finally:
        connection.close()
    wal = Path(f"{database.path}-wal")
    shm = Path(f"{database.path}-shm")
    database_bytes = database.path.stat().st_size if database.path.exists() else 0
    wal_bytes = wal.stat().st_size if wal.exists() else 0
    shm_bytes = shm.stat().st_size if shm.exists() else 0
    return StorageSnapshot(
        database_bytes=database_bytes,
        wal_bytes=wal_bytes,
        shm_bytes=shm_bytes,
        total_bytes=database_bytes + wal_bytes + shm_bytes,
    )


def _process_peak_rss_bytes() -> int | None:
    try:
        import resource
    except ImportError:  # pragma: no cover - supported targets are POSIX
        return None
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # macOS reports bytes; Linux and the BSDs exposed by CI report KiB.
    return peak if sys.platform == "darwin" else peak * 1024


def _count_export_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(0, sum(1 for _line in handle) - 1)


def _budget_violations(
    *,
    profile: ScaleProfile,
    budgets: ScaleBudgets,
    operations: Mapping[str, OperationMeasurement],
    storage: Mapping[str, StorageSnapshot],
    derived: Mapping[str, float | int],
) -> tuple[str, ...]:
    violations: list[str] = []
    for name in (
        "job_page_first",
        "job_page_tail",
        "ranked_page_first",
        "ranked_page_tail",
    ):
        elapsed_ms = operations[name].elapsed_ms
        if elapsed_ms > budgets.max_page_latency_ms:
            violations.append(
                f"{name} took {elapsed_ms:.1f} ms "
                f"(budget {budgets.max_page_latency_ms:.1f} ms)"
            )
    export = operations["streaming_csv_export"]
    if export.elapsed_seconds > budgets.max_export_seconds:
        violations.append(
            f"streaming export took {export.elapsed_seconds:.2f} s "
            f"(budget {budgets.max_export_seconds:.2f} s)"
        )
    if export.peak_python_bytes > budgets.max_export_peak_python_bytes:
        violations.append(
            "streaming export peak Python allocation was "
            f"{export.peak_python_bytes} bytes "
            f"(budget {budgets.max_export_peak_python_bytes})"
        )
    updates = operations["source_state_updates"]
    if updates.elapsed_seconds > budgets.max_state_update_seconds:
        violations.append(
            f"source-state updates took {updates.elapsed_seconds:.2f} s "
            f"(budget {budgets.max_state_update_seconds:.2f} s)"
        )
    for name, measurement in operations.items():
        if name == "streaming_csv_export":
            continue
        if measurement.peak_python_bytes > budgets.max_operation_peak_python_bytes:
            violations.append(
                f"{name} peak Python allocation was {measurement.peak_python_bytes} "
                f"bytes (budget {budgets.max_operation_peak_python_bytes})"
            )
    bytes_per_record = float(derived["storage_bytes_per_persisted_record"])
    if bytes_per_record > budgets.max_storage_bytes_per_record:
        violations.append(
            f"storage used {bytes_per_record:.1f} bytes per persisted fixture row "
            f"(budget {budgets.max_storage_bytes_per_record})"
        )
    update_growth = float(derived["source_update_growth_bytes_per_operation"])
    if update_growth > budgets.max_update_growth_bytes_per_operation:
        violations.append(
            f"source-state writes grew storage by {update_growth:.2f} bytes/update "
            f"(budget {budgets.max_update_growth_bytes_per_operation:.2f})"
        )
    if (
        storage["after_updates"].total_bytes
        < storage["after_source_states"].total_bytes
    ):
        violations.append(
            "storage accounting moved backwards after source-state writes"
        )
    if profile.source_state_updates and int(derived["source_updates_applied"]) != (
        profile.source_state_updates
    ):
        violations.append("source-state update accounting did not match the profile")
    return tuple(violations)


def run_scale_gate(
    work_dir: Path | str,
    *,
    profile: ScaleProfile = SMOKE_PROFILE,
    budgets: ScaleBudgets = DEFAULT_BUDGETS,
) -> ScaleGateReport:
    """Create and measure one isolated offline fixture database.

    ``work_dir`` must not already contain the fixture database or export.  The
    caller owns cleanup, which keeps pytest temporary directories and explicit
    release-artifact directories equally straightforward.
    """

    root = Path(work_dir).expanduser().absolute()
    root.mkdir(parents=True, exist_ok=True)
    database_path = root / "scale-gate.sqlite3"
    export_path = root / "scale-gate.csv"
    if database_path.exists() or export_path.exists():
        raise FileExistsError("scale gate requires an empty work directory")

    operations: dict[str, OperationMeasurement] = {}
    storage: dict[str, StorageSnapshot] = {}
    database = Database(database_path)
    process_peak_before = _process_peak_rss_bytes()
    try:
        _ignored, operations["initialize_schema"] = _measure(database.initialize)
        storage["after_schema"] = _storage_snapshot(database)

        def insert_jobs() -> None:
            _insert_companies(database, profile.jobs)
            _insert_jobs(database, profile.jobs)

        _ignored, operations["insert_jobs"] = _measure(insert_jobs)
        storage["after_jobs"] = _storage_snapshot(database)

        _ignored, operations["insert_applications"] = _measure(
            lambda: _insert_applications(database, profile.applications)
        )
        storage["after_applications"] = _storage_snapshot(database)

        _ignored, operations["insert_source_states"] = _measure(
            lambda: _insert_source_states(database, profile.source_state_rows)
        )
        storage["after_source_states"] = _storage_snapshot(database)

        _ignored, operations["source_state_updates"] = _measure(
            lambda: _update_source_states(
                database,
                row_count=profile.source_state_rows,
                updates=profile.source_state_updates,
            )
        )
        storage["after_updates"] = _storage_snapshot(database)

        with database.session() as session:
            counts = {
                "jobs": int(session.scalar(select(func.count()).select_from(Job)) or 0),
                "applications": int(
                    session.scalar(select(func.count()).select_from(Application)) or 0
                ),
                "source_state_rows": int(
                    session.scalar(select(func.count()).select_from(JobSourceState))
                    or 0
                ),
                "source_state_seen_total": int(
                    session.scalar(select(func.sum(JobSourceState.seen_count))) or 0
                ),
            }

        page_size = min(profile.page_size, profile.jobs)
        tail_offset = max(0, profile.jobs - page_size)

        def job_page(offset: int):
            with database.session() as session:
                return query_jobs_page(
                    session,
                    JobListFilters(
                        sort=JobSort.SCORE_HIGH,
                        limit=page_size,
                        offset=offset,
                    ),
                )

        first_page, operations["job_page_first"] = _measure(lambda: job_page(0))
        tail_page, operations["job_page_tail"] = _measure(lambda: job_page(tail_offset))

        def ranked_page(offset: int):
            with database.session() as session:
                return query_ranked_page(
                    session,
                    view=RankedView.COMPENSATION,
                    limit=page_size,
                    offset=offset,
                )

        first_ranked, operations["ranked_page_first"] = _measure(lambda: ranked_page(0))
        tail_ranked, operations["ranked_page_tail"] = _measure(
            lambda: ranked_page(tail_offset)
        )

        exported, operations["streaming_csv_export"] = _measure(
            lambda: export_data(database, "csv", export_path)
        )
        export_rows = _count_export_rows(exported)
        storage["after_export"] = _storage_snapshot(database)

        integrity_ok, integrity_detail = database.integrity_check()
        counts = {
            **counts,
            "export_rows": export_rows,
            "job_page_first_rows": len(first_page.items),
            "job_page_tail_rows": len(tail_page.items),
            "ranked_page_first_rows": len(first_ranked.items),
            "ranked_page_tail_rows": len(tail_ranked.items),
        }
        expected_seen = profile.source_state_rows + profile.source_state_updates
        count_violations: list[str] = []
        expected_counts = {
            "jobs": profile.jobs,
            "applications": profile.applications,
            "source_state_rows": profile.source_state_rows,
            "source_state_seen_total": expected_seen,
            "export_rows": profile.jobs,
        }
        for name, expected in expected_counts.items():
            if counts[name] != expected:
                count_violations.append(
                    f"{name} was {counts[name]}, expected {expected}"
                )

        persisted_records = (
            profile.jobs + profile.applications + profile.source_state_rows
        )
        source_growth = max(
            0,
            storage["after_updates"].total_bytes
            - storage["after_source_states"].total_bytes,
        )
        derived: dict[str, float | int] = {
            "persisted_fixture_records": persisted_records,
            "storage_bytes_per_persisted_record": (
                storage["after_export"].total_bytes / persisted_records
            ),
            "source_update_growth_bytes": source_growth,
            "source_update_growth_bytes_per_operation": (
                source_growth / max(1, profile.source_state_updates)
            ),
            "source_updates_applied": (
                counts["source_state_seen_total"] - profile.source_state_rows
            ),
            "process_peak_rss_before_bytes": process_peak_before or 0,
            "process_peak_rss_after_bytes": _process_peak_rss_bytes() or 0,
        }
        violations = tuple(count_violations) + _budget_violations(
            profile=profile,
            budgets=budgets,
            operations=operations,
            storage=storage,
            derived=derived,
        )
        if not integrity_ok:
            violations += (f"database integrity check failed: {integrity_detail}",)
        return ScaleGateReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            profile=profile,
            budgets=budgets,
            runtime={
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "platform": platform.platform(),
                "sqlite": sqlite3.sqlite_version,
            },
            counts=counts,
            operations=operations,
            storage=storage,
            derived=derived,
            artifacts={
                "database": str(database_path),
                "export": str(export_path),
                "export_bytes": export_path.stat().st_size,
            },
            integrity_ok=integrity_ok,
            integrity_detail=integrity_detail,
            violations=violations,
        )
    finally:
        database.dispose()


BASELINE_METRICS = (
    "operations.insert_jobs.elapsed_seconds",
    "operations.insert_jobs.peak_python_bytes",
    "operations.insert_applications.elapsed_seconds",
    "operations.insert_source_states.elapsed_seconds",
    "operations.source_state_updates.elapsed_seconds",
    "operations.job_page_tail.elapsed_ms",
    "operations.ranked_page_tail.elapsed_ms",
    "operations.streaming_csv_export.elapsed_seconds",
    "operations.streaming_csv_export.peak_python_bytes",
    "storage.after_export.total_bytes",
    "derived.source_update_growth_bytes",
)


def _nested_number(payload: Mapping[str, Any], path: str) -> float:
    value: Any = payload
    for component in path.split("."):
        if not isinstance(value, Mapping) or component not in value:
            raise ValueError(f"baseline report is missing numeric metric {path!r}")
        value = value[component]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"baseline metric {path!r} must be numeric")
    return float(value)


def compare_to_recorded_baseline(
    report: ScaleGateReport,
    baseline: Mapping[str, Any],
    *,
    max_regression_fraction: float = 0.25,
) -> tuple[str, ...]:
    """Return regressions against a previously recorded same-profile report."""

    if not 0 <= max_regression_fraction <= 10:
        raise ValueError("maximum regression fraction must be between 0 and 10")
    if baseline.get("schema") != REPORT_SCHEMA:
        raise ValueError(f"baseline schema must be {REPORT_SCHEMA!r}")
    baseline_profile = baseline.get("profile")
    if baseline_profile != asdict(report.profile):
        raise ValueError("baseline and current report profiles must match exactly")

    current = report.to_dict()
    violations: list[str] = []
    for path in BASELINE_METRICS:
        previous = _nested_number(baseline, path)
        measured = _nested_number(current, path)
        if previous <= 0:
            continue
        ceiling = previous * (1.0 + max_regression_fraction)
        if measured > ceiling:
            violations.append(
                f"{path} regressed from {previous:.3f} to {measured:.3f} "
                f"(allowed {max_regression_fraction:.1%})"
            )
    return tuple(violations)


def load_report(path: Path | str) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("scale report must be a JSON object")
    return payload


__all__ = [
    "BASELINE_METRICS",
    "DEFAULT_BUDGETS",
    "RELEASE_PROFILE",
    "REPORT_SCHEMA",
    "SMOKE_PROFILE",
    "OperationMeasurement",
    "ScaleBudgets",
    "ScaleGateReport",
    "ScaleProfile",
    "StorageSnapshot",
    "compare_to_recorded_baseline",
    "load_report",
    "run_scale_gate",
]
