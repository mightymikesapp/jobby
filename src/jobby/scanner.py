"""Discovery orchestration, persistence, deduplication, and liveness."""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import threading
from typing import Any, Sequence

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .audit import json_safe, record_audit
from .config import AppConfig, SecretStore
from .db import Database, StorageLock
from .dedup import deduplicate
from .enums import AgentRunStatus, JobStatus
from .liveness import LivenessSnapshot, LivenessState, record_miss
from .models import (
    Company,
    Job,
    JobSourceState,
    Location,
    ScanRun,
    SourceHealth,
    SourceObservation,
    SourceRun,
)
from .normalization import (
    NormalizedSalary,
    extract_compensation,
    is_public_http_url,
    normalize_company,
    normalize_location,
    normalize_url,
)
from .openai_provider import OpenAIProvider
from .ranking import RankingProfile, persist_evaluation
from .review_queues import suggest_duplicate
from .sources.ats import (
    AshbySource,
    GreenhouseSource,
    ICIMSSource,
    LeverSource,
    SmartRecruitersSource,
    TaleoSource,
    USAJobsSource,
    WorkableSource,
    WorkdaySource,
)
from .sources.catalog import (
    EightfoldSource,
    FreehireSource,
    OracleHCMSource,
    PaylocitySource,
    RipplingSource,
)
from .sources.base import (
    JobSource,
    ScanItem,
    ScanStatus,
    SourceError,
    SourceDeadlineExceeded,
    SourceResult,
    sanitize_error_message,
    source_error_from_exception,
)


class WebDiscoveredJob(BaseModel):
    company: str
    title: str
    url: str
    location: str = ""
    description: str = ""
    salary_text: str = ""


class WebDiscoveredJobs(BaseModel):
    jobs: list[WebDiscoveredJob] = Field(default_factory=list)


class Scanner:
    def __init__(
        self,
        database: Database,
        *,
        ranking_profile: RankingProfile | None = None,
        max_workers: int = 4,
        max_workers_per_source: int = 2,
        retry_attempts: int = 2,
        retry_after_max_seconds: float = 30.0,
        record_cap: int = 5_000,
        source_deadline_seconds: float = 600.0,
        max_response_bytes: int = 25 * 1024 * 1024,
        anomaly_ratio: float = 0.5,
        anomaly_window: int = 5,
    ):
        if (
            not isinstance(max_workers, int)
            or isinstance(max_workers, bool)
            or not 1 <= max_workers <= 8
        ):
            raise ValueError("discovery max workers must be between 1 and 8")
        if (
            not isinstance(max_workers_per_source, int)
            or isinstance(max_workers_per_source, bool)
            or not 1 <= max_workers_per_source <= 2
        ):
            raise ValueError("discovery max workers per source must be between 1 and 2")
        self.database = database
        self.ranking_profile = ranking_profile or RankingProfile()
        self.max_workers = max_workers
        self.max_workers_per_source = max_workers_per_source
        if not 0 <= retry_attempts <= 2:
            raise ValueError("source retry attempts must be between zero and two")
        if not 0 <= retry_after_max_seconds <= 120:
            raise ValueError("Retry-After limit must be between zero and 120 seconds")
        if not 100 <= record_cap <= 100_000:
            raise ValueError("source record cap must be between 100 and 100,000")
        if not 1 <= source_deadline_seconds <= 3_600:
            raise ValueError("source deadline must be between one second and one hour")
        if not 1_000_000 <= max_response_bytes <= 100 * 1024 * 1024:
            raise ValueError("source response limit must be between 1 MB and 100 MB")
        if not 0 < anomaly_ratio <= 1:
            raise ValueError("source anomaly ratio must be between zero and one")
        if not 3 <= anomaly_window <= 20:
            raise ValueError("source anomaly window must be between three and 20")
        self.retry_attempts = retry_attempts
        self.retry_after_max_seconds = retry_after_max_seconds
        self.record_cap = record_cap
        self.source_deadline_seconds = float(source_deadline_seconds)
        self.max_response_bytes = max_response_bytes
        self.anomaly_ratio = anomaly_ratio
        self.anomaly_window = anomaly_window
        self._cancel_event = threading.Event()
        self._active_source_run_ids: dict[tuple[str, str], str] = {}
        self.database.initialize()

    def scan(
        self,
        sources: Sequence[JobSource],
        *,
        query: str | None = None,
        requested_sources: Sequence[str] | None = None,
        scan_kind: str = "manual",
        profile_id: str | None = None,
    ) -> ScanRun:
        requested = list(requested_sources or [source.source_key for source in sources])
        self._cancel_event.clear()
        with self._scan_lease():
            run_id = self._start_scan_run(
                query=query,
                requested_sources=requested,
                scan_kind=scan_kind,
                profile_id=profile_id,
            )
            try:
                fetched_results = self._fetch_source_results(
                    sources,
                    query,
                    scan_kind=scan_kind,
                )
                with self.database.session() as session:
                    run = session.get(ScanRun, run_id)
                    if run is None:  # pragma: no cover - database corruption guard
                        raise LookupError(f"scan run {run_id} disappeared")
                    completed_run = self._persist_scan_results(
                        session,
                        run,
                        fetched_results,
                        query=query,
                    )
                fts_status, fts_error = self._synchronize_search_index(run_id)
                completed_run.fts_sync_status = fts_status
                completed_run.fts_sync_error = fts_error
                return completed_run
            except BaseException as exc:
                self._mark_scan_interrupted(run_id, exc)
                raise
            finally:
                self._clear_source_run_ids(run_id)

    def cancel(self) -> None:
        """Request cooperative cancellation between source calls and retries."""

        self._cancel_event.set()

    def _clear_source_run_ids(self, run_id: str) -> None:
        """Release per-run lookup state after its persistence transaction ends."""

        for key in tuple(self._active_source_run_ids):
            if key[0] == run_id:
                self._active_source_run_ids.pop(key, None)

    def _synchronize_search_index(self, run_id: str) -> tuple[str, str | None]:
        """Apply the disposable FTS queue after the scan transaction commits."""

        try:
            from .search_index import SearchIndex

            SearchIndex(self.database).synchronize()
            status, error = "succeeded", None
        except Exception as exc:
            status, error = "failed_nonfatal", _bounded_message(exc, limit=2_000)
        try:
            with self.database.session() as session:
                run = session.get(ScanRun, run_id)
                if run is not None:
                    run.fts_sync_status = status
                    run.fts_sync_error = error
        except Exception as exc:
            telemetry_error = (
                "search-index status recording failed: "
                f"{_bounded_message(exc, limit=1_000)}"
            )
            status = "failed_nonfatal"
            error = "; ".join(value for value in (error, telemetry_error) if value)[
                :2_000
            ]
        return status, error

    def _persist_scan_results(
        self,
        session: Session,
        run: ScanRun,
        fetched_results: Sequence[SourceResult],
        *,
        query: str | None,
    ) -> ScanRun:
        results: list[SourceResult] = []
        item_jobs: list[tuple[ScanItem, Job]] = []
        complete_snapshots: list[tuple[str, set[str], str]] = []
        observed_job_ids: set[str] = set()
        seen_observations: set[tuple[str, str]] = set()
        processing_errors: list[SourceError] = []
        for result in fetched_results:
            source_run = self._record_source_run(session, run, result)
            self._active_source_run_ids[(run.id, result.source)] = source_run.id
            observed_ids: set[str] = set()
            accepted_items: list[ScanItem] = []
            item_errors: list[SourceError] = []
            for index, item in enumerate(result.items):
                if item.source != result.source:
                    item_errors.append(
                        SourceError(
                            code="source_mismatch",
                            message=f"item {index} source did not match its adapter result",
                            item_index=index,
                        )
                    )
                    continue
                observed_ids.add(item.source_id)
                observation_key = (item.source, item.source_id)
                if observation_key in seen_observations:
                    continue
                try:
                    with session.begin_nested():
                        job = self._persist_item(session, run, item)
                        session.flush()
                except Exception as exc:
                    item_errors.append(
                        SourceError(
                            code="persistence_error",
                            message=f"item {index} could not be persisted: {_bounded_message(exc)}",
                            item_index=index,
                        )
                    )
                    continue
                seen_observations.add(observation_key)
                accepted_items.append(item)
                observed_job_ids.add(job.id)
                item_jobs.append((item, job))
            if item_errors:
                combined_errors = tuple(result.errors) + tuple(item_errors)
                result = SourceResult(
                    source=result.source,
                    status=ScanStatus.PARTIAL if accepted_items else ScanStatus.FAILED,
                    items=tuple(accepted_items),
                    errors=combined_errors,
                    started_at=result.started_at,
                    finished_at=result.finished_at,
                    metadata=result.metadata,
                )
            results.append(result)
            if (
                result.status == ScanStatus.SUCCEEDED
                and not (query or "").strip()
                and is_complete_snapshot(result)
                and source_run.anomaly_state == "healthy"
                and run.scan_kind != "focused"
            ):
                complete_snapshots.append((result.source, observed_ids, source_run.id))

        # Apply absence evidence only after every source has had a chance
        # to observe the canonical job. This makes liveness independent of
        # source ordering when URLs overlap.
        observed_at = datetime.now(timezone.utc)
        liveness_advanced_job_ids: set[str] = set()
        for source, observed_ids, source_run_id in complete_snapshots:
            self._record_source_misses(
                session,
                source,
                observed_ids,
                observed_job_ids,
                observed_at,
                advanced_job_ids=liveness_advanced_job_ids,
                source_run_id=source_run_id,
            )
        for job_id in sorted({job.id for _, job in item_jobs}):
            try:
                with session.begin_nested():
                    evaluation = persist_evaluation(
                        session, job_id, profile=self.ranking_profile
                    )
                    session.flush()
            except Exception as exc:
                processing_errors.append(
                    SourceError(
                        code="evaluation_error",
                        message=f"job {job_id} could not be evaluated: {_bounded_message(exc)}",
                    )
                )
                continue
            job = session.get(Job, job_id)
            if job and evaluation.automatic_skip and not job.manual_status_locked:
                job.status = JobStatus.IGNORED
            elif (
                job and job.status == JobStatus.IGNORED and not job.manual_status_locked
            ):
                # Automatically ignored unpaid roles may reopen if a later
                # source description removes that gate. User ignores lock.
                job.status = JobStatus.DISCOVERED
        try:
            with session.begin_nested():
                self._persist_duplicates(session, item_jobs)
                session.flush()
        except Exception as exc:
            processing_errors.append(
                SourceError(
                    code="deduplication_error",
                    message=f"duplicate analysis failed: {_bounded_message(exc)}",
                )
            )
        if processing_errors:
            results.append(
                SourceResult(
                    source="scanner_processing",
                    status=ScanStatus.FAILED,
                    errors=tuple(processing_errors),
                )
            )
        run.source_results = {
            result.source: result_to_json(result) for result in results
        }
        run.discovered_count = len(item_jobs)
        run.finished_at = datetime.now(timezone.utc)
        run.status = aggregate_status(results)
        errors = [
            _bounded_message(error.message, limit=500)
            for result in results
            for error in result.errors
        ]
        run.error_summary = "; ".join(errors[:10])[:4_000] or None
        record_audit(
            session,
            action="scan.completed",
            entity_type="scan_run",
            entity_id=run.id,
            actor="scanner",
            after={
                "status": run.status,
                "observations": len(item_jobs),
                "sources": len(results),
            },
        )
        session.flush()
        return run

    def _record_source_run(
        self,
        session: Session,
        scan_run: ScanRun,
        result: SourceResult,
    ) -> SourceRun:
        """Persist normalized source telemetry and conservative anomaly state."""

        complete = (
            scan_run.scan_kind != "focused"
            and result.status == ScanStatus.SUCCEEDED
            and is_complete_snapshot(result)
        )
        recent_counts = list(
            session.scalars(
                select(SourceRun.result_count)
                .where(
                    SourceRun.source == result.source,
                    SourceRun.complete.is_(True),
                    SourceRun.anomaly_state == "healthy",
                )
                .order_by(SourceRun.attempted_at.desc())
                .limit(self.anomaly_window)
            )
        )
        median_count = statistics.median(recent_counts) if recent_counts else None
        anomaly = "healthy"
        if complete and (
            len(result.items) == 0
            or (
                median_count is not None
                and median_count > 0
                and len(result.items) < median_count * self.anomaly_ratio
            )
        ):
            anomaly = "low_result"
        elif result.status == ScanStatus.FAILED:
            anomaly = "failed"
        elif result.status == ScanStatus.PARTIAL:
            anomaly = "partial"

        health = session.scalar(
            select(SourceHealth).where(SourceHealth.source == result.source)
        )
        if health is None:
            health = SourceHealth(source=result.source)
            session.add(health)
            session.flush()
        failure_class = result.errors[0].code if result.errors else None
        failure_streak = (
            health.failure_streak + 1
            if result.status is not ScanStatus.SUCCEEDED
            else 0
        )
        reported_total: int | None
        try:
            raw_total = result.metadata.get("total")
            reported_total = int(raw_total) if raw_total is not None else None
        except (TypeError, ValueError, OverflowError):
            reported_total = None
        try:
            retries = max(0, min(2, int(result.metadata.get("retries", 0))))
        except (TypeError, ValueError, OverflowError):
            retries = 0
        row = SourceRun(
            scan_run_id=scan_run.id,
            source=result.source,
            attempted_at=result.started_at,
            succeeded_at=(
                result.finished_at if result.status is ScanStatus.SUCCEEDED else None
            ),
            completed_at=result.finished_at if complete else None,
            duration_seconds=max(0.0, result.duration_seconds),
            result_count=len(result.items),
            reported_total=reported_total,
            retries=retries,
            complete=complete,
            failure_class=failure_class,
            anomaly_state=anomaly,
            failure_streak=failure_streak,
            detail=(
                "; ".join(error.message for error in result.errors)[:4_000] or None
            ),
        )
        session.add(row)
        session.flush()
        health.last_attempt_at = result.started_at
        if result.status is ScanStatus.SUCCEEDED:
            health.last_success_at = result.finished_at
        if complete:
            health.last_complete_at = result.finished_at
        health.last_result_count = len(result.items)
        health.last_reported_total = reported_total
        health.failure_streak = failure_streak
        health.last_failure_class = failure_class
        health.anomaly_state = anomaly
        return row

    def _start_scan_run(
        self,
        *,
        query: str | None,
        requested_sources: Sequence[str],
        scan_kind: str = "manual",
        profile_id: str | None = None,
    ) -> str:
        """Commit run identity before any network work starts."""

        with self.database.session() as session:
            run = ScanRun(
                status=AgentRunStatus.RUNNING,
                query=query,
                requested_sources=list(requested_sources),
                started_at=datetime.now(timezone.utc),
                scan_kind=scan_kind,
                profile_id=profile_id,
            )
            session.add(run)
            session.flush()
            return run.id

    def _fetch_source_results(
        self,
        sources: Sequence[JobSource],
        query: str | None,
        *,
        scan_kind: str = "manual",
    ) -> list[SourceResult]:
        """Perform untrusted/network adapter work without an open DB transaction."""

        if not sources:
            return [
                SourceResult(
                    source="scanner",
                    status=ScanStatus.FAILED,
                    errors=(
                        SourceError(
                            code="no_sources",
                            message="No discovery sources were configured for this scan.",
                        ),
                    ),
                )
            ]
        semaphores: dict[str, threading.BoundedSemaphore] = {}
        semaphore_lock = threading.Lock()

        def fetch(source: JobSource) -> SourceResult:
            concurrency_key = str(
                getattr(source, "concurrency_key", None) or source.source_key
            ).casefold()
            with semaphore_lock:
                semaphore = semaphores.setdefault(
                    concurrency_key,
                    threading.BoundedSemaphore(self.max_workers_per_source),
                )
            with semaphore:
                if self._cancel_event.is_set():
                    raise InterruptedError("discovery scan cancelled")
                deadline_at = time.monotonic() + self.source_deadline_seconds
                configure_runtime = getattr(source, "configure_runtime", None)
                if callable(configure_runtime):
                    configure_runtime(
                        deadline_at=deadline_at,
                        cancelled=self._cancel_event.is_set,
                        max_response_bytes=self.max_response_bytes,
                        hydration_workers=self.max_workers_per_source,
                        inventory_metadata_only=scan_kind == "inventory",
                    )

                def checkpoint() -> None:
                    if self._cancel_event.is_set():
                        raise InterruptedError("discovery scan cancelled")
                    if time.monotonic() >= deadline_at:
                        raise SourceDeadlineExceeded("source deadline exceeded")

                checkpoint()
                retries = 0
                latest = source.scan(query)
                result = latest
                first_started_at = latest.started_at
                try:
                    checkpoint()
                except SourceDeadlineExceeded:
                    return _source_deadline_result(result, retries=retries)
                while (
                    retries < self.retry_attempts
                    and latest.status in {ScanStatus.FAILED, ScanStatus.PARTIAL}
                    and any(error.retryable for error in latest.errors)
                ):
                    retry_after_values = [
                        error.retry_after_seconds
                        for error in latest.errors
                        if error.retryable and error.retry_after_seconds is not None
                    ]
                    base_delay = (
                        max(retry_after_values)
                        if retry_after_values
                        else min(2.0, 0.25 * (2**retries))
                    )
                    jitter = random.uniform(0.0, min(0.25, base_delay * 0.2))
                    delay = min(
                        self.retry_after_max_seconds,
                        base_delay + jitter,
                    )
                    remaining = max(0.0, deadline_at - time.monotonic())
                    if remaining <= 0:
                        return _source_deadline_result(result, retries=retries)
                    delay = min(delay, remaining)
                    if self._cancel_event.wait(delay):
                        raise InterruptedError("discovery scan cancelled")
                    checkpoint()
                    retries += 1
                    latest = source.scan(query)
                    result = _prefer_source_result(result, latest)
                    try:
                        checkpoint()
                    except SourceDeadlineExceeded:
                        return _source_deadline_result(result, retries=retries)
                metadata = {
                    **dict(result.metadata),
                    "retries": retries,
                    "attempts": retries + 1,
                }
                if len(result.items) > self.record_cap:
                    items = result.items[: self.record_cap]
                    errors = (
                        *result.errors,
                        SourceError(
                            code="record_cap",
                            message=(
                                f"Source exceeded the {self.record_cap:,}-record "
                                "inventory cap; the run is partial."
                            ),
                        ),
                    )
                    return SourceResult(
                        source=result.source,
                        status=ScanStatus.PARTIAL,
                        items=items,
                        errors=errors,
                        started_at=first_started_at,
                        finished_at=datetime.now(timezone.utc),
                        metadata={**metadata, "truncated": True},
                    )
                if (
                    metadata != dict(result.metadata)
                    or result.started_at != first_started_at
                    or retries
                ):
                    result = SourceResult(
                        source=result.source,
                        status=result.status,
                        items=result.items,
                        errors=result.errors,
                        started_at=first_started_at,
                        finished_at=datetime.now(timezone.utc),
                        metadata=metadata,
                    )
                return result

        executor = ThreadPoolExecutor(
            max_workers=min(self.max_workers, len(sources)),
            thread_name_prefix="jobby-discovery",
        )
        futures: list[Future[SourceResult]] = [
            executor.submit(fetch, source) for source in sources
        ]
        results: list[SourceResult] = []
        try:
            # Resolve in configured order rather than completion order so scan
            # history and downstream deterministic persistence do not vary with
            # network timing.
            for source, future in zip(sources, futures, strict=True):
                try:
                    result = future.result()
                    if not isinstance(result, SourceResult):
                        raise TypeError("source did not return a SourceResult")
                    if result.source != source.source_key:
                        raise ValueError(
                            f"source returned key {result.source!r}; expected {source.source_key!r}"
                        )
                except InterruptedError:
                    raise
                except Exception as exc:
                    # An independent adapter failure is persisted as data and must
                    # not discard useful results from the other sources.
                    result = _source_failure(source.source_key, exc)
                results.append(result)
        except BaseException:
            for future in futures:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        return results

    def _mark_scan_interrupted(self, run_id: str, exc: BaseException) -> None:
        """Persist terminal history when a fetch or persistence cycle is aborted."""

        try:
            with self.database.session() as session:
                run = session.get(ScanRun, run_id)
                if run is None or run.status is not AgentRunStatus.RUNNING:
                    return
                message = _bounded_message(exc, limit=4_000)
                run.status = AgentRunStatus.FAILED
                run.finished_at = datetime.now(timezone.utc)
                run.error_summary = message
                record_audit(
                    session,
                    action="scan.interrupted",
                    entity_type="scan_run",
                    entity_id=run.id,
                    actor="scanner",
                    after={"status": run.status, "error": message},
                )
        except Exception:
            # Preserve the original interruption; failure recording is best effort.
            return

    def _scan_lease(self) -> StorageLock:
        """Serialize discovery cycles so one absence window is counted once."""

        return StorageLock(
            self.database.path.parent / ".jobby-scan.lock",
            exclusive=True,
            timeout=0.0,
        )

    def scan_web(
        self,
        provider: OpenAIProvider,
        query: str,
        *,
        max_total_tokens: int | None = None,
    ) -> ScanRun:
        """Run billable web discovery without holding a SQLite writer lock."""

        provider_config = getattr(provider, "config", None)
        if provider_config is not None and not bool(
            getattr(provider_config, "openai_enabled", False)
        ):
            raise PermissionError("OpenAI web discovery is disabled in configuration")

        self._active_web_scan_run_id: str | None = None
        with self._scan_lease():
            try:
                run = self._scan_web_unlocked(
                    provider,
                    query,
                    max_total_tokens=max_total_tokens,
                )
                fts_status, fts_error = self._synchronize_search_index(run.id)
                run.fts_sync_status = fts_status
                run.fts_sync_error = fts_error
                return run
            except BaseException as exc:
                if self._active_web_scan_run_id is not None:
                    self._mark_scan_interrupted(self._active_web_scan_run_id, exc)
                raise
            finally:
                if self._active_web_scan_run_id is not None:
                    self._clear_source_run_ids(self._active_web_scan_run_id)
                self._active_web_scan_run_id = None

    def _scan_web_unlocked(
        self,
        provider: OpenAIProvider,
        query: str,
        *,
        max_total_tokens: int | None = None,
    ) -> ScanRun:
        query = query.strip()
        if not query:
            raise ValueError("web discovery requires a query")
        if len(query) > 2_000:
            raise ValueError("web discovery query must be 2,000 characters or fewer")
        output_limit: int | None = None
        if max_total_tokens is not None:
            if isinstance(max_total_tokens, bool) or not isinstance(
                max_total_tokens, int
            ):
                raise ValueError("web discovery token budget must be an integer")
            if not 1_000 <= max_total_tokens <= 2_000_000:
                raise ValueError(
                    "web discovery token budget must be between 1,000 and 2,000,000"
                )
            # Two provider calls are made. Bound each output to a conservative
            # share while leaving room for both calls' inputs.
            output_limit = max(256, min(8_000, max_total_tokens // 4))
        with self.database.session() as session:
            run = ScanRun(
                status=AgentRunStatus.RUNNING,
                query=query,
                requested_sources=["openai_web"],
                started_at=datetime.now(timezone.utc),
            )
            session.add(run)
            session.flush()
            self._active_web_scan_run_id = run.id
            # Make the durable run visible, then release SQLite's writer lock
            # before either potentially slow provider request.
            session.commit()
            try:
                search_kwargs: dict[str, Any] = {"session": session}
                if output_limit is not None:
                    search_kwargs["max_output_tokens"] = output_limit
                web = provider.search(
                    f"Find currently open public job postings matching: {query}. Return direct employer or government posting URLs and cite sources.",
                    **search_kwargs,
                )
                session.commit()
                structured_kwargs: dict[str, Any] = {"session": session}
                if output_limit is not None:
                    structured_kwargs["max_output_tokens"] = output_limit
                parsed = provider.structured(
                    purpose="web_job_extraction",
                    text=(
                        f"{web.text}\n\nCITED PUBLIC URLS (use these exact posting URLs only):\n"
                        + "\n".join(citation.url for citation in web.citations)
                    ),
                    output_type=WebDiscoveredJobs,
                    prompt_version="web-job-extraction-v1",
                    system="Extract only actual job postings explicitly present in the cited search result. Every URL must exactly match one supplied cited public URL; omit uncertain entries.",
                    tier="fast",
                    **structured_kwargs,
                ).value
                session.commit()
                cited_urls: dict[str, str] = {}
                for citation in web.citations:
                    launch_url = str(citation.url or "").strip()
                    if not is_public_http_url(launch_url):
                        continue
                    canonical = normalize_url(launch_url)
                    if canonical:
                        cited_urls.setdefault(canonical, launch_url)
                items_by_url: dict[str, ScanItem] = {}
                item_errors: list[SourceError] = []
                for index, job in enumerate(parsed.jobs):
                    if not is_public_http_url(job.url):
                        item_errors.append(
                            SourceError(
                                code="invalid_public_url",
                                message=f"web result item {index} did not contain a public HTTP(S) URL",
                                item_index=index,
                            )
                        )
                        continue
                    canonical_url = normalize_url(job.url)
                    if not canonical_url:
                        item_errors.append(
                            SourceError(
                                code="invalid_public_url",
                                message=f"web result item {index} did not contain a public HTTP(S) URL",
                                item_index=index,
                            )
                        )
                        continue
                    if canonical_url not in cited_urls:
                        item_errors.append(
                            SourceError(
                                code="uncited_url",
                                message=f"web result item {index} URL was not supported by a search citation",
                                item_index=index,
                            )
                        )
                        continue
                    try:
                        items_by_url.setdefault(
                            canonical_url,
                            ScanItem(
                                source="openai_web",
                                source_id=hashlib.sha256(
                                    canonical_url.encode()
                                ).hexdigest()[:32],
                                company=job.company,
                                title=job.title,
                                url=cited_urls[canonical_url],
                                location=job.location,
                                description=job.description,
                                salary_text=job.salary_text,
                                metadata={
                                    "citation_count": len(web.citations),
                                    "extracted_url": job.url,
                                },
                            ),
                        )
                    except (TypeError, ValueError) as exc:
                        item_errors.append(
                            SourceError(
                                code="malformed_web_item",
                                message=f"web result item {index}: {exc}",
                                item_index=index,
                            )
                        )
                items = tuple(items_by_url.values())
                status = (
                    ScanStatus.PARTIAL
                    if items and item_errors
                    else ScanStatus.FAILED
                    if item_errors
                    else ScanStatus.SUCCEEDED
                )
                result = SourceResult(
                    source="openai_web",
                    status=status,
                    items=items,
                    errors=tuple(item_errors),
                )
            except Exception as exc:
                result = SourceResult(
                    source="openai_web",
                    status=ScanStatus.FAILED,
                    errors=(
                        SourceError(
                            code="openai_error",
                            message=str(exc) or exc.__class__.__name__,
                        ),
                    ),
                )
            source_run = self._record_source_run(session, run, result)
            self._active_source_run_ids[(run.id, result.source)] = source_run.id
            pairs: list[tuple[ScanItem, Job]] = []
            persisted_items: list[ScanItem] = []
            persistence_errors: list[SourceError] = []
            for index, item in enumerate(result.items):
                try:
                    with session.begin_nested():
                        job = self._persist_item(session, run, item)
                        session.flush()
                except Exception as exc:
                    persistence_errors.append(
                        SourceError(
                            code="persistence_error",
                            message=f"web item {index} could not be persisted: {_bounded_message(exc)}",
                            item_index=index,
                        )
                    )
                    continue
                pairs.append((item, job))
                persisted_items.append(item)
            if persistence_errors:
                combined_errors = tuple(result.errors) + tuple(persistence_errors)
                result = SourceResult(
                    source=result.source,
                    status=ScanStatus.PARTIAL if persisted_items else ScanStatus.FAILED,
                    items=tuple(persisted_items),
                    errors=combined_errors,
                    started_at=result.started_at,
                    finished_at=result.finished_at,
                    metadata=result.metadata,
                )
            processing_errors: list[SourceError] = []
            for job_id in {job.id for _, job in pairs}:
                try:
                    with session.begin_nested():
                        evaluation = persist_evaluation(
                            session, job_id, profile=self.ranking_profile
                        )
                        session.flush()
                except Exception as exc:
                    processing_errors.append(
                        SourceError(
                            code="evaluation_error",
                            message=f"job {job_id} could not be evaluated: {_bounded_message(exc)}",
                        )
                    )
                    continue
                job = session.get(Job, job_id)
                if job and evaluation.automatic_skip and not job.manual_status_locked:
                    job.status = JobStatus.IGNORED
                elif (
                    job
                    and job.status == JobStatus.IGNORED
                    and not job.manual_status_locked
                ):
                    job.status = JobStatus.DISCOVERED
            try:
                with session.begin_nested():
                    self._persist_duplicates(session, pairs)
                    session.flush()
            except Exception as exc:
                processing_errors.append(
                    SourceError(
                        code="deduplication_error",
                        message=f"duplicate analysis failed: {_bounded_message(exc)}",
                    )
                )
            results = [result]
            if processing_errors:
                results.append(
                    SourceResult(
                        source="scanner_processing",
                        status=ScanStatus.FAILED,
                        errors=tuple(processing_errors),
                    )
                )
            run.source_results = {
                scan_result.source: result_to_json(scan_result)
                for scan_result in results
            }
            run.discovered_count = len(persisted_items)
            run.finished_at = datetime.now(timezone.utc)
            run.status = aggregate_status(results)
            run.error_summary = (
                "; ".join(
                    _bounded_message(error.message, limit=500)
                    for scan_result in results
                    for error in scan_result.errors
                )[:4_000]
                or None
            )
            record_audit(
                session,
                action="scan.web_completed",
                entity_type="scan_run",
                entity_id=run.id,
                actor="scanner",
                after={"status": run.status, "observations": len(result.items)},
            )
            return run

    def _persist_item(
        self,
        session: Session,
        run: ScanRun,
        item: ScanItem,
        *,
        source_run_id: str | None = None,
    ) -> Job:
        source_run_id = source_run_id or self._active_source_run_ids.get(
            (run.id, item.source)
        )
        company = _get_company(session, item.company)
        location = _get_location(session, item.location, item.remote.value)
        job = session.scalar(
            select(Job).where(
                Job.source_primary == item.source, Job.source_id == item.source_id
            )
        )
        if job is None:
            job = session.scalar(
                select(Job).where(
                    (Job.comparison_url == item.comparison_url)
                    | (Job.canonical_url == item.comparison_url)
                )
            )
        now = datetime.now(timezone.utc)
        extraction = (
            extract_compensation(item.description) if item.salary is None else None
        )
        salary = item.salary or (extraction.salary if extraction else None)
        compensation_text = item.salary_text or (
            extraction.evidence if extraction is not None else ""
        )
        if job is None:
            salary_min, salary_max, currency = _salary_values(salary)
            job = Job(
                company_id=company.id,
                location_id=location.id if location else None,
                title=item.title,
                normalized_title=item.normalized_title,
                canonical_url=item.comparison_url,
                launch_url=item.launch_url,
                comparison_url=item.comparison_url,
                source_primary=item.source,
                source_id=item.source_id,
                description=item.description or None,
                description_hash=item.description_hash or None,
                remote_status=item.remote.value,
                compensation_text=compensation_text or None,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_currency=currency,
                compensation_period=salary.period.value if salary else "unknown",
                compensation_confidence=salary.confidence if salary else 0.0,
                compensation_evidence=(
                    salary.evidence if salary and salary.evidence else compensation_text
                )
                or None,
                posted_at=item.posted_at,
                deadline=item.deadline.date() if item.deadline else None,
                discovered_at=now,
                last_seen_at=now,
                liveness_known=True,
            )
            session.add(job)
            session.flush()
        else:
            authoritative = set(job.authoritative_fields or [])
            job.last_seen_at = now
            job.consecutive_misses = 0
            job.closed_at = None
            job.explicit_closure = False
            job.liveness_known = True
            if "company" not in authoritative:
                job.company_id = company.id
            if "title" not in authoritative:
                job.title = item.title
                job.normalized_title = item.normalized_title
            if "url" not in authoritative:
                job.launch_url = item.launch_url
                job.comparison_url = item.comparison_url
                job.canonical_url = item.comparison_url
            if not job.manual_status_locked and job.status in {
                JobStatus.STALE,
                JobStatus.CLOSED,
            }:
                job.status = JobStatus.DISCOVERED
            if (
                "description" not in authoritative
                and item.description
                and item.description_hash != job.description_hash
            ):
                job.description = item.description
                job.description_hash = item.description_hash
            if location and "location" not in authoritative:
                job.location_id = location.id
            if item.remote.value != "unknown":
                job.remote_status = item.remote.value
            if compensation_text and "compensation" not in authoritative:
                job.compensation_text = compensation_text
            if salary is not None and "compensation" not in authoritative:
                salary_min, salary_max, currency = _salary_values(salary)
                job.salary_min = salary_min
                job.salary_max = salary_max
                job.salary_currency = currency
                job.compensation_period = salary.period.value
                job.compensation_confidence = salary.confidence
                job.compensation_evidence = salary.evidence or compensation_text or None
            if item.posted_at is not None:
                job.posted_at = item.posted_at
            if item.deadline is not None:
                job.deadline = item.deadline.date()
        effective_description_hash = item.description_hash or job.description_hash or ""
        source_metadata = {
            key: value
            for key, value in dict(item.metadata).items()
            if key not in {"hydrated"}
        }
        raw_payload = {
            "salary_text": item.salary_text,
            "remote": item.remote.value,
            "posted_at": item.posted_at.isoformat() if item.posted_at else None,
            "deadline": item.deadline.isoformat() if item.deadline else None,
            "metadata": json_safe(source_metadata),
        }
        snapshot_payload = {
            "source_url": item.launch_url,
            "title": item.title,
            "company": item.company,
            "location": item.location,
            "is_live": True,
            "content_hash": effective_description_hash or None,
            "raw_payload": raw_payload,
        }
        snapshot_hash = hashlib.sha256(
            json.dumps(
                snapshot_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        state = session.scalar(
            select(JobSourceState).where(
                JobSourceState.source == item.source,
                JobSourceState.source_job_id == item.source_id,
            )
        )
        previous_live = state.is_live if state is not None else None
        previous_snapshot = state.last_snapshot_hash if state is not None else None
        if state is None:
            state = JobSourceState(
                job_id=job.id,
                source=item.source,
                source_job_id=item.source_id,
                first_seen_at=now,
                last_seen_at=now,
                seen_count=1,
                last_content_hash=effective_description_hash or None,
                last_snapshot_hash=snapshot_hash,
                last_source_run_id=source_run_id,
                is_live=True,
            )
            session.add(state)
            observation_kind = "first_seen"
        else:
            state.job_id = job.id
            state.last_seen_at = now
            state.seen_count += 1
            state.last_content_hash = effective_description_hash or None
            state.last_snapshot_hash = snapshot_hash
            state.last_source_run_id = source_run_id
            state.is_live = True
            observation_kind = "liveness" if previous_live is not True else "content"

        if previous_snapshot != snapshot_hash or previous_live is not True:
            session.add(
                SourceObservation(
                    job_id=job.id,
                    scan_run_id=run.id,
                    source=item.source,
                    source_account=_source_account(item.source),
                    source_job_id=item.source_id,
                    source_url=item.url,
                    title_snapshot=item.title,
                    company_snapshot=item.company,
                    location_snapshot=item.location,
                    observed_at=now,
                    is_live=True,
                    content_hash=effective_description_hash or None,
                    raw_payload=raw_payload,
                    snapshot_hash=snapshot_hash,
                    observation_kind=observation_kind,
                )
            )
        return job

    def _record_source_misses(
        self,
        session: Session,
        source: str,
        observed_ids: set[str],
        observed_job_ids: set[str],
        at: datetime,
        *,
        advanced_job_ids: set[str],
        source_run_id: str | None = None,
    ) -> None:
        rows = session.execute(
            select(JobSourceState, Job)
            .join(Job, Job.id == JobSourceState.job_id)
            .where(
                JobSourceState.source == source,
                Job.liveness_known.is_(True),
            )
            .order_by(JobSourceState.job_id, JobSourceState.source_job_id)
        )
        source_run = (
            session.get(SourceRun, source_run_id) if source_run_id is not None else None
        )
        for state, job in rows:
            if state.source_job_id in observed_ids:
                continue
            prior_state = (
                LivenessState.CLOSED
                if job.explicit_closure or job.consecutive_misses >= 2
                else LivenessState.STALE
                if job.consecutive_misses == 1
                else LivenessState.ACTIVE
            )
            snapshot = LivenessSnapshot(
                state=prior_state,
                consecutive_misses=job.consecutive_misses,
                last_observed_at=job.last_seen_at,
                closed_at=job.closed_at,
                explicit_closure=job.explicit_closure,
            )
            other_live = bool(
                session.scalar(
                    select(JobSourceState.id).where(
                        JobSourceState.job_id == job.id,
                        JobSourceState.id != state.id,
                        JobSourceState.is_live.is_(True),
                    )
                )
            )
            advance_job = (
                job.id not in observed_job_ids
                and job.id not in advanced_job_ids
                and not other_live
            )
            updated = record_miss(snapshot, at=at) if advance_job else snapshot
            if advance_job:
                advanced_job_ids.add(job.id)
                job.consecutive_misses = updated.consecutive_misses
                job.closed_at = updated.closed_at
                if not job.manual_status_locked:
                    job.status = (
                        JobStatus.STALE
                        if updated.state == LivenessState.STALE
                        else JobStatus.CLOSED
                    )
            meaningful_change = state.is_live is not False or advance_job
            state.is_live = False
            state.last_source_run_id = source_run_id
            if not meaningful_change:
                continue
            liveness_hash = hashlib.sha256(
                json.dumps(
                    {
                        "previous_snapshot_hash": state.last_snapshot_hash,
                        "is_live": False,
                        "consecutive_misses": updated.consecutive_misses,
                        "state": updated.state.value,
                        "job_advance_suppressed": not advance_job,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            state.last_snapshot_hash = liveness_hash
            session.add(
                SourceObservation(
                    job_id=job.id,
                    scan_run_id=(source_run.scan_run_id if source_run else None),
                    source=source,
                    source_account=_source_account(source),
                    source_job_id=state.source_job_id,
                    source_url=job.launch_url or job.canonical_url,
                    title_snapshot=job.title,
                    observed_at=at,
                    is_live=False,
                    closure_evidence=(
                        "Absent from a healthy complete inventory; "
                        f"consecutive miss {updated.consecutive_misses}."
                        if advance_job
                        else "Absent from this source; job closure was suppressed "
                        "because another source remains live or observed."
                    ),
                    content_hash=job.description_hash,
                    raw_payload={
                        "liveness_state": updated.state.value,
                        "consecutive_misses": updated.consecutive_misses,
                        "job_advance_suppressed": not advance_job,
                    },
                    snapshot_hash=liveness_hash,
                    observation_kind="liveness",
                )
            )

    def _persist_duplicates(
        self, session: Session, pairs: list[tuple[ScanItem, Job]]
    ) -> None:
        if not pairs:
            return

        current_job_ids = {job.id for _, job in pairs}
        company_ids = {job.company_id for _, job in pairs}
        historical_pairs: list[tuple[ScanItem, Job]] = []
        if company_ids:
            current_items = [item for item, _job in pairs]
            candidate_urls = {item.canonical_url for item in current_items}
            candidate_source_ids = {item.source_id for item in current_items}
            candidate_titles = {item.normalized_title for item in current_items}
            candidate_blocks = [Job.normalized_title.in_(candidate_titles)]
            if candidate_urls:
                candidate_blocks.append(Job.canonical_url.in_(candidate_urls))
            if candidate_source_ids:
                candidate_blocks.append(Job.source_id.in_(candidate_source_ids))
            rows = session.execute(
                select(Job, Company.name, Location.display_name)
                .join(Company, Company.id == Job.company_id)
                .outerjoin(Location, Location.id == Job.location_id)
                .where(
                    Job.company_id.in_(company_ids),
                    Job.id.not_in(current_job_ids),
                    Job.canonical_url.is_not(None),
                    or_(*candidate_blocks),
                )
                .order_by(Job.discovered_at.desc(), Job.id)
            )
            for historical_job, company_name, location_name in rows:
                try:
                    historical_item = ScanItem(
                        source=historical_job.source_primary or "historical",
                        source_id=historical_job.source_id or historical_job.id,
                        company=company_name,
                        title=historical_job.title,
                        url=historical_job.canonical_url or "",
                        location=location_name or "",
                        description=historical_job.description or "",
                        salary_text=historical_job.compensation_text or "",
                    )
                except (TypeError, ValueError):
                    # Imported rows can predate today's stricter URL contract.
                    continue
                historical_pairs.append((historical_item, historical_job))

        all_pairs = [*historical_pairs, *pairs]
        matches = deduplicate(item for item, _job in all_pairs).duplicates
        pending_pairs: set[tuple[str, str]] = set()
        for match in matches:
            left_job = all_pairs[match.canonical_index][1]
            right_job = all_pairs[match.duplicate_index][1]
            if left_job.id == right_job.id:
                continue
            if (
                left_job.id not in current_job_ids
                and right_job.id not in current_job_ids
            ):
                continue
            first, second = sorted((left_job.id, right_job.id))
            pair = (first, second)
            if pair in pending_pairs:
                continue
            pending_pairs.add(pair)
            suggest_duplicate(
                session,
                left_job,
                right_job,
                rule=match.reason.value,
                similarity=match.similarity,
            )


def build_configured_sources(
    config: AppConfig,
    *,
    client: httpx.Client,
    secret_store: SecretStore | None = None,
    selector: str | None = None,
    inventory: bool = False,
) -> list[JobSource]:
    selected = (selector or "all").casefold()
    sources: list[JobSource] = []
    secrets = secret_store or SecretStore()

    def enabled(provider: str, key: str | None = None) -> bool:
        return (
            selected in {"all", provider, f"{provider}:all"}
            or selected == f"{provider}:{(key or '').casefold()}"
        )

    for token, company in config.sources.greenhouse.items():
        if enabled("greenhouse", token):
            sources.append(GreenhouseSource(client, board_token=token, company=company))
    for site, company in config.sources.lever.items():
        if enabled("lever", site):
            sources.append(LeverSource(client, site=site, company=company))
    for board, company in config.sources.ashby.items():
        if enabled("ashby", board):
            sources.append(AshbySource(client, board=board, company=company))
    for account, company in config.sources.workable.items():
        if enabled("workable", account):
            sources.append(WorkableSource(client, account=account, company=company))
    for key, item in config.sources.smartrecruiters.items():
        if enabled("smartrecruiters", key):
            sources.append(
                SmartRecruitersSource(
                    client,
                    company_slug=item.company_slug,
                    company=item.name,
                    max_pages=(
                        min(1_000, math.ceil(config.source_record_cap / 100))
                        if inventory
                        else 50
                    ),
                )
            )
    for key, item in config.sources.icims.items():
        if enabled("icims", key):
            sources.append(
                ICIMSSource(
                    client,
                    base_url=item.base_url,
                    company=item.name,
                    max_pages=(
                        min(1_000, config.source_record_cap) if inventory else 100
                    ),
                )
            )
    for key, item in config.sources.taleo.items():
        if enabled("taleo", key):
            sources.append(
                TaleoSource(
                    client,
                    search_url=item.search_url,
                    company=item.name,
                    max_pages=(
                        min(1_000, config.source_record_cap) if inventory else 100
                    ),
                )
            )
    for key, item in config.sources.workday.items():
        if enabled("workday", key):
            # ``WorkdayBoard`` is typed in current configuration, while this
            # tolerant access also accepts legacy mapping-shaped values loaded
            # from older callers during the compatibility window.
            def board_value(name: str, default: str = "") -> str:
                if isinstance(item, dict):
                    return str(item.get(name, default))
                return str(getattr(item, name, default))

            sources.append(
                WorkdaySource(
                    client,
                    tenant=board_value("tenant"),
                    wd=board_value("wd", "wd1"),
                    site=board_value("site"),
                    company=board_value("name") or key,
                    max_pages=min(
                        250 if inventory else config.workday_scan_max_pages,
                        math.ceil(config.source_record_cap / 20),
                    ),
                )
            )
    catalog_specs = (
        ("eightfold", config.sources.eightfold, EightfoldSource),
        ("oracle_hcm", config.sources.oracle_hcm, OracleHCMSource),
        ("rippling", config.sources.rippling, RipplingSource),
        ("paylocity", config.sources.paylocity, PaylocitySource),
    )
    for provider, boards, adapter in catalog_specs:
        for key, item in boards.items():
            if not enabled(provider, key):
                continue
            secret_name = item.credential_name
            credential = secrets.get(secret_name) if secret_name else None
            sources.append(
                adapter(
                    client,
                    endpoint=item.endpoint,
                    company=item.name,
                    credential=credential,
                    source_key=f"{provider}:{key}",
                    page_size=item.page_size,
                    contract_version=item.contract_version,
                    max_pages=(
                        min(
                            item.max_pages,
                            math.ceil(config.source_record_cap / item.page_size),
                        )
                        if inventory
                        else item.max_pages
                    ),
                )
            )
    # External catalogs are opt-in twice: a configured endpoint and a keyring
    # credential are both required.  Missing credentials simply disable the
    # source and never appear in persisted configuration or scan payloads.
    for key, item in config.sources.freehire.items():
        if not enabled("freehire", key):
            continue
        credential = secrets.get(item.credential_name) if item.credential_name else None
        if not credential:
            continue
        sources.append(
            FreehireSource(
                client,
                endpoint=item.endpoint,
                company=item.name,
                credential=credential,
                source_key=f"freehire:{key}",
                page_size=item.page_size,
                contract_version=item.contract_version,
                max_pages=(
                    min(
                        item.max_pages,
                        math.ceil(config.source_record_cap / item.page_size),
                    )
                    if inventory
                    else item.max_pages
                ),
            )
        )
    if selected in {"usajobs", "all"}:
        key = secrets.get("usajobs_api_key")
        email = secrets.get("usajobs_email")
        if key and email:
            for location in config.sources.usajobs_locations:
                sources.append(
                    USAJobsSource(
                        client,
                        api_key=key,
                        email=email,
                        location=None if location == "*" else location,
                        results_per_page=500 if inventory else 100,
                        max_pages=10 if inventory else 5,
                    )
                )
    return sources


def aggregate_status(results: Sequence[SourceResult]) -> AgentRunStatus:
    if not results or all(result.status == ScanStatus.FAILED for result in results):
        return AgentRunStatus.FAILED
    if all(result.status == ScanStatus.SUCCEEDED for result in results):
        return AgentRunStatus.SUCCEEDED
    return AgentRunStatus.PARTIAL


def _source_failure(source: str, exc: Exception) -> SourceResult:
    """Convert an unexpected adapter exception into a persisted source result."""

    classified = source_error_from_exception(exc)
    return SourceResult(
        source=source,
        status=ScanStatus.FAILED,
        errors=(
            SourceError(
                code="adapter_exception",
                message=_bounded_message(exc),
                retryable=classified.retryable,
                http_status=classified.http_status,
                retry_after_seconds=classified.retry_after_seconds,
            ),
        ),
    )


def _prefer_source_result(
    previous: SourceResult, candidate: SourceResult
) -> SourceResult:
    """Keep useful partial rows if a later whole-source retry regresses."""

    if candidate.status is ScanStatus.SUCCEEDED:
        return candidate
    if previous.status is ScanStatus.SUCCEEDED:
        return previous
    if candidate.status is ScanStatus.PARTIAL:
        if previous.status is not ScanStatus.PARTIAL:
            return candidate
        return candidate if len(candidate.items) >= len(previous.items) else previous
    return previous if previous.status is ScanStatus.PARTIAL else candidate


def _source_deadline_result(result: SourceResult, *, retries: int) -> SourceResult:
    errors = result.errors
    if not any(error.code == "source_deadline" for error in errors):
        errors = (
            *errors,
            SourceError(
                code="source_deadline",
                message="Source deadline expired; the inventory is incomplete.",
            ),
        )
    return SourceResult(
        source=result.source,
        status=ScanStatus.PARTIAL if result.items else ScanStatus.FAILED,
        items=result.items,
        errors=errors,
        started_at=result.started_at,
        finished_at=datetime.now(timezone.utc),
        metadata={
            **dict(result.metadata),
            "deadline_exceeded": True,
            "retries": retries,
        },
    )


def _bounded_message(value: object, *, limit: int = 1_000) -> str:
    return sanitize_error_message(value, limit=limit)


def is_complete_snapshot(result: SourceResult) -> bool:
    provider = result.source.split(":", 1)[0]
    if provider in {
        "greenhouse",
        "lever",
        "ashby",
        "workable",
    }:
        return True
    if provider == "workday":
        total = result.metadata.get("total")
        if total is None:
            return False
        try:
            expected = int(total)
            return expected >= 0 and expected <= len(result.items)
        except (TypeError, ValueError):
            return False
    if provider == "usajobs":
        total = result.metadata.get("total")
        if total is None or result.metadata.get("truncated"):
            return False
        try:
            expected = int(total)
            return expected >= 0 and expected <= len(result.items)
        except (TypeError, ValueError):
            return False
    if provider in {"smartrecruiters", "icims", "taleo"}:
        total = result.metadata.get("total")
        if total is None or result.metadata.get("truncated"):
            return False
        try:
            expected = int(total)
            return expected >= 0 and expected <= len(result.items)
        except (TypeError, ValueError):
            return False
    return False


def result_to_json(result: SourceResult) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "items": len(result.items),
        "errors": [asdict(error) for error in result.errors],
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "metadata": json_safe(dict(result.metadata)),
    }


def _get_company(session: Session, name: str) -> Company:
    normalized = normalize_company(name)
    company = session.scalar(
        select(Company).where(Company.normalized_name == normalized)
    )
    if company is None:
        company = Company(name=name.strip(), normalized_name=normalized)
        session.add(company)
        session.flush()
    return company


def _get_location(session: Session, display: str, remote: str) -> Location | None:
    if not display.strip() and remote == "unknown":
        return None
    normalized = normalize_location(display) or remote
    location = session.scalar(
        select(Location).where(Location.normalized_key == normalized)
    )
    if location is None:
        location = Location(
            display_name=display.strip() or remote.title(),
            normalized_key=normalized,
            remote=remote == "remote",
        )
        session.add(location)
        session.flush()
    return location


def _salary_values(
    salary: NormalizedSalary | None,
) -> tuple[int | None, int | None, str]:
    if salary is None:
        return None, None, "USD"
    low = salary.annual_minimum if salary.annualization_confident else salary.minimum
    high = salary.annual_maximum if salary.annualization_confident else salary.maximum
    return _decimal_int(low), _decimal_int(high), salary.currency


def _decimal_int(value: Decimal | None) -> int | None:
    return int(value) if value is not None else None


def _source_account(source: str) -> str | None:
    parts = source.split(":", 1)
    return parts[1] if len(parts) == 2 else None


__all__ = [
    "Scanner",
    "WebDiscoveredJob",
    "WebDiscoveredJobs",
    "aggregate_status",
    "build_configured_sources",
    "is_complete_snapshot",
    "result_to_json",
]
