from __future__ import annotations

from collections import deque

import pytest
from sqlalchemy import func, select

from jobby.db import Database
from jobby.enums import JobStatus
from jobby.models import (
    AuditEvent,
    Evaluation,
    Job,
    JobSourceState,
    ScanRun,
    SourceHealth,
    SourceObservation,
    SourceRun,
)
from jobby.scanner import Scanner
from jobby.search_index import SearchIndex
from jobby.sources.base import (
    JobSource,
    ScanItem,
    ScanStatus,
    SourceError,
    SourceResult,
)


class SequenceSource(JobSource):
    name = "test"

    def __init__(self, source_key: str, results: list[SourceResult]) -> None:
        super().__init__(client=None, source_key=source_key)  # type: ignore[arg-type]
        self.results = deque(results)
        self.calls = 0

    def scan(self, query: str | None = None) -> SourceResult:
        self.calls += 1
        return self.results.popleft()


class RecordingEvent:
    def __init__(self, *, cancel_on_wait: bool = False) -> None:
        self.cancelled = False
        self.cancel_on_wait = cancel_on_wait
        self.waits: list[float] = []

    def clear(self) -> None:
        self.cancelled = False

    def set(self) -> None:
        self.cancelled = True

    def is_set(self) -> bool:
        return self.cancelled

    def wait(self, delay: float) -> bool:
        self.waits.append(delay)
        if self.cancel_on_wait:
            self.cancelled = True
        return self.cancelled


@pytest.fixture
def database(tmp_path) -> Database:
    value = Database(tmp_path / "jobby.sqlite3")
    value.initialize()
    yield value
    value.dispose()


def _item(
    source: str, source_id: str, *, description: str = "Legal AI role"
) -> ScanItem:
    return ScanItem(
        source=source,
        source_id=source_id,
        company="Example",
        title="Legal AI Counsel",
        url=f"https://jobs.example.test/{source_id}",
        description=description,
    )


def _success(source: str, *items: ScanItem) -> SourceResult:
    return SourceResult(source=source, status=ScanStatus.SUCCEEDED, items=items)


def test_unchanged_rescan_updates_state_without_duplicate_snapshots_or_evaluations(
    database,
) -> None:
    source_key = "greenhouse:example"
    posting = _item(source_key, "one")
    source = SequenceSource(
        source_key,
        [_success(source_key, posting), _success(source_key, posting)],
    )
    scanner = Scanner(database, retry_after_max_seconds=0)

    scanner.scan([source])
    scanner.scan([source])

    with database.session() as session:
        state = session.scalar(select(JobSourceState))
        assert state is not None and state.seen_count == 2
        assert session.scalar(select(func.count(SourceObservation.id))) == 1
        assert session.scalar(select(func.count(Evaluation.id))) == 1
        assert (
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "job.evaluated"
                )
            )
            == 1
        )


def test_changed_content_adds_one_snapshot_and_one_semantic_evaluation(
    database,
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            _success(source_key, _item(source_key, "one", description="Version one")),
            _success(source_key, _item(source_key, "one", description="Version two")),
        ],
    )
    scanner = Scanner(database, retry_after_max_seconds=0)

    scanner.scan([source])
    scanner.scan([source])

    with database.session() as session:
        assert session.scalar(select(func.count(SourceObservation.id))) == 2
        assert session.scalar(select(func.count(Evaluation.id))) == 2
        assert session.scalar(select(func.count(JobSourceState.id))) == 1


def test_retryable_failure_retries_once_and_records_normalized_telemetry(
    database,
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            SourceResult(
                source=source_key,
                status=ScanStatus.FAILED,
                errors=(
                    SourceError(
                        code="http_error",
                        message="temporary",
                        retryable=True,
                        http_status=429,
                        retry_after_seconds=0,
                    ),
                ),
            ),
            _success(source_key, _item(source_key, "one")),
        ],
    )

    Scanner(
        database,
        retry_attempts=2,
        retry_after_max_seconds=0,
    ).scan([source])

    with database.session() as session:
        run = session.scalar(select(SourceRun))
        health = session.scalar(select(SourceHealth))
        assert source.calls == 2
        assert run is not None and run.retries == 1 and run.complete is True
        assert health is not None and health.failure_streak == 0
        assert health.anomaly_state == "healthy"


def test_retryable_partial_page_result_is_retried_without_losing_rows(database) -> None:
    source_key = "workday:example:jobs"
    partial = SourceResult(
        source=source_key,
        status=ScanStatus.PARTIAL,
        items=(_item(source_key, "one"),),
        errors=(
            SourceError(
                code="page_read_error",
                message="temporary later-page failure",
                retryable=True,
                http_status=503,
            ),
        ),
    )
    source = SequenceSource(
        source_key,
        [
            partial,
            _success(source_key, _item(source_key, "one"), _item(source_key, "two")),
        ],
    )

    run = Scanner(database, retry_after_max_seconds=0).scan(
        [source], scan_kind="inventory"
    )

    assert run.status.value == "succeeded"
    assert source.calls == 2
    with database.session() as session:
        source_run = session.scalar(select(SourceRun))
        assert source_run is not None
        assert source_run.retries == 1
        assert source_run.result_count == 2
        assert source_run.duration_seconds >= 0


def test_nontransient_http_failure_is_not_retried(database) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            SourceResult(
                source=source_key,
                status=ScanStatus.FAILED,
                errors=(
                    SourceError(
                        code="http_error",
                        message="not implemented",
                        retryable=False,
                        http_status=501,
                    ),
                ),
            )
        ],
    )

    Scanner(database, retry_attempts=2, retry_after_max_seconds=0).scan([source])

    assert source.calls == 1


def test_retry_after_is_honored_with_bounded_jitter_and_two_retry_cap(
    database, monkeypatch
) -> None:
    source_key = "greenhouse:example"
    temporary = SourceResult(
        source=source_key,
        status=ScanStatus.FAILED,
        errors=(
            SourceError(
                code="http_error",
                message="temporary",
                retryable=True,
                http_status=503,
                retry_after_seconds=1.5,
            ),
        ),
    )
    source = SequenceSource(source_key, [temporary, temporary, temporary])
    event = RecordingEvent()
    scanner = Scanner(database, retry_attempts=2, retry_after_max_seconds=2)
    scanner._cancel_event = event
    monkeypatch.setattr("jobby.scanner.random.uniform", lambda *_args: 0.25)

    run = scanner.scan([source])

    assert run.status.value == "failed"
    assert source.calls == 3
    assert event.waits == [1.75, 1.75]
    with database.session() as session:
        source_run = session.scalar(select(SourceRun))
        assert source_run is not None and source_run.retries == 2


def test_cancellation_during_retry_aborts_and_journals_the_scan(database) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            SourceResult(
                source=source_key,
                status=ScanStatus.FAILED,
                errors=(
                    SourceError(
                        code="network_error",
                        message="timeout",
                        retryable=True,
                    ),
                ),
            )
        ],
    )
    event = RecordingEvent(cancel_on_wait=True)
    scanner = Scanner(database, retry_attempts=2)
    scanner._cancel_event = event

    with pytest.raises(InterruptedError, match="cancelled"):
        scanner.scan([source])

    assert source.calls == 1
    with database.session() as session:
        run = session.scalar(select(ScanRun))
        assert run is not None and run.status.value == "failed"
        assert "cancelled" in (run.error_summary or "")


def test_source_deadline_preserves_partial_rows_and_marks_incomplete(
    database, monkeypatch
) -> None:
    source_key = "workday:example:jobs"
    clock = {"now": 100.0}
    monkeypatch.setattr("jobby.scanner.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("jobby.sources.base.time.monotonic", lambda: clock["now"])

    class DeadlineAfterResult(SequenceSource):
        def scan(self, query: str | None = None) -> SourceResult:
            result = super().scan(query)
            clock["now"] = 131.0
            return result

    source = DeadlineAfterResult(
        source_key,
        [_success(source_key, _item(source_key, "one"))],
    )
    run = Scanner(database, source_deadline_seconds=30).scan(
        [source], scan_kind="inventory"
    )

    assert run.status.value == "partial"
    with database.session() as session:
        source_run = session.scalar(select(SourceRun))
        assert source_run is not None
        assert source_run.complete is False
        assert source_run.result_count == 1
        assert source_run.failure_class == "source_deadline"
        assert source_run.anomaly_state == "partial"
        assert session.scalar(select(func.count(SourceObservation.id))) == 1


def test_zero_inventory_is_anomalous_and_next_healthy_inventory_can_age_job(
    database,
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            _success(source_key, _item(source_key, "one")),
            _success(source_key),
            _success(source_key, _item(source_key, "two")),
        ],
    )
    scanner = Scanner(database, retry_after_max_seconds=0)

    scanner.scan([source], scan_kind="inventory")
    scanner.scan([source], scan_kind="inventory")
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "one"))
        runs = list(session.scalars(select(SourceRun).order_by(SourceRun.attempted_at)))
        assert job is not None and job.consecutive_misses == 0
        assert runs[-1].anomaly_state == "low_result"

    scanner.scan([source], scan_kind="inventory")
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "one"))
        assert job is not None and job.consecutive_misses == 1
        assert job.status is JobStatus.STALE


def test_explicit_zero_retry_after_is_honored_without_backoff(
    database, monkeypatch
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            SourceResult(
                source=source_key,
                status=ScanStatus.FAILED,
                errors=(
                    SourceError(
                        code="http_error",
                        message="retry now",
                        retryable=True,
                        http_status=429,
                        retry_after_seconds=0,
                    ),
                ),
            ),
            _success(source_key, _item(source_key, "one")),
        ],
    )
    event = RecordingEvent()
    scanner = Scanner(database)
    scanner._cancel_event = event
    monkeypatch.setattr(
        "jobby.scanner.random.uniform", lambda _minimum, maximum: maximum
    )

    scanner.scan([source])

    assert event.waits == [0]


def test_partial_runs_are_not_successes_and_advance_health_failure_streak(
    database,
) -> None:
    source_key = "greenhouse:example"
    partial = SourceResult(
        source=source_key,
        status=ScanStatus.PARTIAL,
        items=(_item(source_key, "one"),),
        errors=(
            SourceError(
                code="malformed_item",
                message="one listing was malformed",
            ),
        ),
    )
    scanner = Scanner(database, retry_attempts=0)

    scanner.scan([SequenceSource(source_key, [partial])])

    with database.session() as session:
        source_run = session.scalar(select(SourceRun))
        health = session.scalar(select(SourceHealth))
        assert source_run is not None and source_run.succeeded_at is None
        assert source_run.failure_streak == 1
        assert health is not None and health.last_success_at is None
        assert health.failure_streak == 1
    assert scanner._active_source_run_ids == {}


def test_fts_status_recording_failure_remains_nonfatal(database, monkeypatch) -> None:
    scanner = Scanner(database)
    monkeypatch.setattr(SearchIndex, "synchronize", lambda _self: None)
    monkeypatch.setattr(
        database,
        "session",
        lambda: (_ for _ in ()).throw(RuntimeError("database temporarily busy")),
    )

    status, error = scanner._synchronize_search_index("missing-run")

    assert status == "failed_nonfatal"
    assert error is not None and "status recording failed" in error
