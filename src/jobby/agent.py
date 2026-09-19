"""One scheduled local discovery/deadline cycle."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Iterator, Sequence
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import distinct, func, or_, select

from .audit import json_safe, record_audit
from .config import AppConfig, SecretStore
from .db import Database
from .enums import AgentRunStatus, AlertSeverity, JobStatus, TaskStatus
from .models import (
    AIRun,
    AgentRun,
    Alert,
    Interview,
    Job,
    JobSourceState,
    ScanRun,
    SourceObservation,
    Task,
)
from .notifications import NativeNotificationProvider
from .normalization import comparison_tokens, normalize_location
from .openai_provider import OpenAIProvider
from .planner import build_daily_plan
from .profiles import ScanProfileRecord, list_profiles
from .ranking import ranking_profile_from_database
from .review_queues import alert_fingerprint, create_or_recur_alert, resolve_alert
from .scanner import Scanner, build_configured_sources
from .scheduler import rotate_log_files
from .sources.base import (
    JobSource,
    ScanItem,
    ScanStatus,
    SourceError,
    SourceResult,
    sanitize_error_message,
    source_error_from_exception,
)


class AgentAlreadyRunning(RuntimeError):
    """A second local agent cycle was prevented from overlapping."""


class AgentRunMode(StrEnum):
    """Discovery work selected by an explicit scheduler definition."""

    LEGACY = "legacy"
    FOCUSED = "focused"
    INVENTORY = "inventory"


class DailyAgent:
    def __init__(
        self,
        database: Database,
        config: AppConfig,
        *,
        secrets: SecretStore | None = None,
        notifier: NativeNotificationProvider | None = None,
    ):
        self.database = database
        self.config = config
        self.secrets = secrets or SecretStore()
        self.notifier = notifier or NativeNotificationProvider()

    def run(
        self,
        *,
        include_web: bool | None = None,
        mode: AgentRunMode | str | None = None,
    ) -> AgentRun:
        self.database.initialize()
        environment_mode = os.environ.get("JOBBY_AGENT_MODE", "").strip()
        explicitly_selected = mode is not None or bool(environment_mode)
        run_mode = _agent_run_mode(mode or environment_mode or AgentRunMode.LEGACY)
        rotate_log_files(
            self.database.paths,
            max_bytes=self.config.scheduler_log_max_bytes,
            backup_count=self.config.scheduler_log_backup_count,
        )
        stale_after = timedelta(minutes=self.config.agent_stale_after_minutes)
        lock = _AgentFileLock(self.database.path.parent / ".jobby-agent.lock")
        with lock.acquire():
            self._recover_stale_runs(stale_after)
            with self.database.session() as session:
                active = session.scalar(
                    select(AgentRun.id).where(AgentRun.status == AgentRunStatus.RUNNING)
                )
                if active is not None:
                    raise AgentAlreadyRunning(
                        f"agent run {active} is already marked as running"
                    )
                agent_run = AgentRun(
                    status=AgentRunStatus.RUNNING,
                    started_at=datetime.now(timezone.utc),
                )
                session.add(agent_run)
                session.flush()
                agent_run_id = agent_run.id
            scheduled_web = include_web is None
            effective_web = (
                self.config.scheduled_web_enabled
                if scheduled_web and run_mode is not AgentRunMode.INVENTORY
                else bool(include_web)
            )
            try:
                # Unqualified callers retain the historical method shape for
                # subclass/monkeypatch compatibility. Installed schedules set
                # JOBBY_AGENT_MODE, so their calls are always explicit.
                if explicitly_selected:
                    completed = self._run_cycle(
                        agent_run_id,
                        include_web=effective_web,
                        scheduled_web=scheduled_web,
                        mode=run_mode,
                    )
                else:
                    completed = self._run_cycle(
                        agent_run_id,
                        include_web=effective_web,
                        scheduled_web=scheduled_web,
                    )
                if environment_mode:
                    self._post_cycle_recovery(completed, mode=run_mode)
                return completed
            except KeyboardInterrupt:
                try:
                    self._record_failure(
                        agent_run_id,
                        "Agent run was cancelled by the user.",
                        cancelled=True,
                    )
                finally:
                    raise
            except Exception as exc:
                message = sanitize_error_message(exc)
                try:
                    return self._record_failure(agent_run_id, message)
                except Exception:
                    raise exc

    def _post_cycle_recovery(
        self,
        agent_run: AgentRun,
        *,
        mode: AgentRunMode,
    ) -> None:
        """Attach nonfatal recovery results to a committed scheduled run."""

        try:
            from .operational_recovery import run_scheduled_recovery

            recovery = run_scheduled_recovery(
                self.database,
                self.config,
                secrets=self.secrets,
                mode=mode.value,
                succeeded=agent_run.status is AgentRunStatus.SUCCEEDED,
            )
            payload = recovery.as_summary()
        except Exception as exc:
            # Discovery has already committed. An operational recovery failure
            # must remain visible without rewriting that truthful scan result.
            payload = {
                "attempted": True,
                "warnings": [
                    f"post-cycle recovery failed: {sanitize_error_message(exc)}"
                ],
            }
        merged = {**dict(agent_run.summary or {}), "recovery": payload}
        with self.database.session() as session:
            persisted = session.get(AgentRun, agent_run.id)
            if persisted is not None:
                persisted.summary = merged
        agent_run.summary = merged

    def _run_cycle(
        self,
        agent_run_id: str,
        *,
        include_web: bool,
        scheduled_web: bool = False,
        mode: AgentRunMode = AgentRunMode.LEGACY,
    ) -> AgentRun:
        scanner = Scanner(
            self.database,
            ranking_profile=ranking_profile_from_database(self.database, self.config),
            max_workers=self.config.discovery_max_workers,
            max_workers_per_source=self.config.discovery_max_workers_per_source,
            retry_attempts=self.config.source_retry_attempts,
            retry_after_max_seconds=self.config.source_retry_after_max_seconds,
            record_cap=self.config.source_record_cap,
            source_deadline_seconds=self.config.source_deadline_seconds,
            max_response_bytes=self.config.source_max_response_bytes,
            anomaly_ratio=self.config.source_anomaly_ratio,
            anomaly_window=self.config.source_anomaly_window,
        )
        enabled_profiles = (
            list_profiles(self.database, enabled=True)
            if mode is AgentRunMode.FOCUSED
            else ()
        )
        if mode is AgentRunMode.FOCUSED and not enabled_profiles:
            scan_run = self._empty_focused_scan()
        else:
            with httpx.Client(
                timeout=httpx.Timeout(20, connect=10),
                follow_redirects=False,
                headers={"User-Agent": "Jobby/0.1 local personal job discovery"},
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                trust_env=False,
            ) as client:
                inventory = mode is AgentRunMode.INVENTORY
                sources = build_configured_sources(
                    self.config,
                    client=client,
                    secret_store=self.secrets,
                    inventory=inventory,
                )
                if mode is AgentRunMode.FOCUSED:
                    sources = _focused_sources(sources, enabled_profiles)
                    profile_id = (
                        enabled_profiles[0].id if len(enabled_profiles) == 1 else None
                    )
                    scan_run = scanner.scan(
                        sources,
                        query=_focused_query_label(enabled_profiles),
                        requested_sources=[source.source_key for source in sources],
                        scan_kind="focused",
                        profile_id=profile_id,
                    )
                elif inventory:
                    sources = [
                        _InventorySource(source, self.database) for source in sources
                    ]
                    scan_run = scanner.scan(
                        sources,
                        requested_sources=[source.source_key for source in sources],
                        scan_kind="inventory",
                    )
                else:
                    scan_run = scanner.scan(
                        sources,
                        requested_sources=[source.source_key for source in sources],
                    )
        web_run = None
        web_error: str | None = None
        web_skip_reason: str | None = None
        if include_web and not self.config.openai_enabled:
            web_skip_reason = "OpenAI is disabled in configuration"
        elif include_web and scheduled_web:
            web_skip_reason = self._scheduled_web_budget_reason(agent_run_id)
        if include_web and not web_skip_reason:
            credential = _credential_status(self.secrets, "openai_api_key")
            if credential != "configured":
                web_error = f"OpenAI credential is {credential}"
        if include_web and not web_skip_reason and not web_error:
            try:
                provider = OpenAIProvider(self.config, secret_store=self.secrets)
                web_run = scanner.scan_web(
                    provider,
                    self.config.scheduled_web_query,
                    max_total_tokens=(
                        self.config.scheduled_ai_max_total_tokens_per_run
                        if scheduled_web
                        else None
                    ),
                )
            except Exception as exc:
                web_error = sanitize_error_message(exc)

        now = datetime.now(timezone.utc)
        pending_notifications: list[tuple[str, str, bool]] = []
        with self.database.session() as session:
            agent_run = session.get(AgentRun, agent_run_id)
            if agent_run is None:
                raise LookupError("agent run disappeared before completion")
            agent_run.scan_run_id = scan_run.id
            statuses = [scan_run.status] + (
                [web_run.status]
                if web_run
                else [AgentRunStatus.FAILED]
                if web_error
                else []
            )
            agent_run.status = (
                AgentRunStatus.FAILED
                if all(status == AgentRunStatus.FAILED for status in statuses)
                else AgentRunStatus.SUCCEEDED
                if all(status == AgentRunStatus.SUCCEEDED for status in statuses)
                else AgentRunStatus.PARTIAL
            )
            run_errors = [
                scan_run.error_summary,
                web_run.error_summary if web_run else None,
                web_error,
            ]
            agent_run.error = "; ".join(error for error in run_errors if error) or None
            deadline_alerts = self._deadline_tasks(session, now)
            interview_count = len(
                list(
                    session.scalars(
                        select(Interview).where(
                            Interview.starts_at >= now,
                            Interview.starts_at <= now + timedelta(hours=24),
                        )
                    )
                )
            )
            high_roles = self._new_high_role_count(session, scan_run)
            web_failed = bool(
                web_error
                or (
                    web_run
                    and web_run.status
                    in {AgentRunStatus.PARTIAL, AgentRunStatus.FAILED}
                )
            )
            if (
                scan_run.status in {AgentRunStatus.PARTIAL, AgentRunStatus.FAILED}
                or web_failed
            ):
                alert = create_or_recur_alert(
                    session,
                    severity=AlertSeverity.WARNING,
                    title="Jobby scan needs attention",
                    message=agent_run.error or "One or more sources failed.",
                    deduplication_key="agent-scan-needs-attention",
                    entity_type="scan_run",
                    entity_id=scan_run.id,
                    now=now,
                )
                if self.config.notifications_enabled:
                    pending_notifications.append((alert.title, alert.message, False))
            else:
                self._resolve_recurring_alert(
                    session,
                    title="Jobby scan needs attention",
                    deduplication_key="agent-scan-needs-attention",
                    reason="A subsequent scan completed successfully.",
                    now=now,
                )
            if agent_run.status is AgentRunStatus.SUCCEEDED:
                self._resolve_recurring_alert(
                    session,
                    title="Jobby agent run failed",
                    deduplication_key="agent-run-failed",
                    reason="A subsequent agent run completed successfully.",
                    now=now,
                )
            if deadline_alerts and self.config.notifications_enabled:
                pending_notifications.append(
                    (
                        "Jobby deadlines",
                        f"{deadline_alerts} deadline(s) are due within 48 hours.",
                        True,
                    )
                )
            if interview_count and self.config.notifications_enabled:
                pending_notifications.append(
                    (
                        "Upcoming interview",
                        f"{interview_count} interview(s) are scheduled in the next 24 hours.",
                        True,
                    )
                )
            if high_roles and self.config.notifications_enabled:
                pending_notifications.append(
                    (
                        "High-ranking roles",
                        f"{high_roles} newly observed role(s) score 4.0 or higher.",
                        False,
                    )
                )
            plan = build_daily_plan(session, timezone_name=self.config.timezone)
            agent_run.summary = {
                "run_mode": mode.value,
                "enabled_profile_ids": [profile.id for profile in enabled_profiles],
                "ats_scan_run_id": scan_run.id,
                "web_scan_run_id": web_run.id if web_run else None,
                "web_error": web_error,
                "web_requested": include_web,
                "web_scheduled": scheduled_web,
                "web_skip_reason": web_skip_reason,
                "deadline_alerts": deadline_alerts,
                "interviews_next_24h": interview_count,
                "new_high_roles": high_roles,
                "daily_plan_items": len(plan.items),
                **self._ai_usage_summary(session, agent_run.started_at),
            }
            agent_run.finished_at = datetime.now(timezone.utc)
            record_audit(
                session,
                action="agent.completed",
                entity_type="agent_run",
                entity_id=agent_run.id,
                actor="agent",
                after={"status": agent_run.status, **agent_run.summary},
            )
            completed_run = agent_run
        # Native notifications are external side effects. Emit them only after
        # the local transaction and audit history have committed successfully.
        for title, message, urgent in pending_notifications:
            self._notify(title, message, urgent=urgent)
        return completed_run

    def _empty_focused_scan(self) -> ScanRun:
        """Persist a successful no-op when every profile remains disabled."""

        now = datetime.now(timezone.utc)
        with self.database.session() as session:
            run = ScanRun(
                status=AgentRunStatus.SUCCEEDED,
                query="focused profiles: none enabled",
                requested_sources=[],
                source_results={"profiles": {"enabled": 0, "skipped": True}},
                started_at=now,
                finished_at=now,
                discovered_count=0,
                scan_kind="focused",
                fts_sync_status="not_needed",
            )
            session.add(run)
            session.flush()
            record_audit(
                session,
                action="scan.focused_skipped",
                entity_type="scan_run",
                entity_id=run.id,
                actor="agent",
                after={"reason": "no enabled scan profiles"},
            )
            return run

    def _recover_stale_runs(self, stale_after: timedelta) -> int:
        now = datetime.now(timezone.utc)
        cutoff = now - stale_after
        recovered = 0
        with self.database.session() as session:
            stale = list(
                session.scalars(
                    select(AgentRun).where(
                        AgentRun.status == AgentRunStatus.RUNNING,
                        or_(
                            AgentRun.started_at.is_(None),
                            AgentRun.started_at < cutoff,
                        ),
                    )
                )
            )
            for run in stale:
                run.status = AgentRunStatus.FAILED
                run.finished_at = now
                run.error = (
                    "Recovered stale RUNNING agent record after an interrupted cycle."
                )
                run.summary = {
                    **dict(run.summary or {}),
                    "stale_recovered": True,
                }
                record_audit(
                    session,
                    action="agent.stale_recovered",
                    entity_type="agent_run",
                    entity_id=run.id,
                    actor="agent",
                    after={"status": run.status, **run.summary},
                    detail=run.error,
                )
                recovered += 1
            if recovered:
                create_or_recur_alert(
                    session,
                    severity=AlertSeverity.WARNING,
                    title="Interrupted agent run recovered",
                    message=(
                        f"Recovered {recovered} stale scheduled run record(s) "
                        "without deleting their history."
                    ),
                    deduplication_key="agent-stale-run-recovered",
                    now=now,
                )
        return recovered

    def _scheduled_web_budget_reason(self, _agent_run_id: str) -> str | None:
        maximum_tokens = self.config.scheduled_ai_max_total_tokens_per_run
        worst_case_cost = (
            maximum_tokens
            * self.config.scheduled_ai_cost_estimate_usd_per_million_tokens
            / 1_000_000
        )
        if worst_case_cost > self.config.scheduled_ai_max_cost_usd_per_run:
            return (
                "configured token ceiling exceeds the scheduled dollar ceiling "
                f"(${worst_case_cost:.2f} > "
                f"${self.config.scheduled_ai_max_cost_usd_per_run:.2f})"
            )
        local_zone = ZoneInfo(self.config.timezone)
        local_now = datetime.now(timezone.utc).astimezone(local_zone)
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start = local_start.astimezone(timezone.utc)
        with self.database.session() as session:
            count = session.scalar(
                select(func.count())
                .select_from(AIRun)
                .where(
                    AIRun.created_at >= day_start,
                    AIRun.purpose == "job_web_search",
                )
            )
        if int(count or 0) >= self.config.scheduled_ai_max_runs_per_day:
            return "scheduled AI daily run limit has already been reached"
        return None

    def _ai_usage_summary(
        self, session, started_at: datetime | None
    ) -> dict[str, object]:
        if started_at is None:
            return {}
        input_tokens, output_tokens = session.execute(
            select(
                func.coalesce(func.sum(AIRun.input_tokens), 0),
                func.coalesce(func.sum(AIRun.output_tokens), 0),
            ).where(AIRun.created_at >= started_at)
        ).one()
        total = int(input_tokens or 0) + int(output_tokens or 0)
        estimated_cost = (
            total
            * self.config.scheduled_ai_cost_estimate_usd_per_million_tokens
            / 1_000_000
        )
        return {
            "ai_input_tokens": int(input_tokens or 0),
            "ai_output_tokens": int(output_tokens or 0),
            "ai_estimated_cost_usd": round(estimated_cost, 6),
        }

    def _record_failure(
        self,
        agent_run_id: str,
        message: str,
        *,
        cancelled: bool = False,
    ) -> AgentRun:
        pending_notification: tuple[str, str, bool] | None = None
        with self.database.session() as session:
            agent_run = session.get(AgentRun, agent_run_id)
            if agent_run is None:
                raise LookupError("agent run disappeared before failure finalization")
            agent_run.status = AgentRunStatus.FAILED
            agent_run.finished_at = datetime.now(timezone.utc)
            agent_run.error = message
            agent_run.summary = {
                **dict(agent_run.summary or {}),
                "cancelled": cancelled,
                "failure": message,
            }
            alert = create_or_recur_alert(
                session,
                severity=AlertSeverity.WARNING,
                title="Jobby agent run failed",
                message=message,
                deduplication_key="agent-run-failed",
                entity_type="agent_run",
                entity_id=agent_run.id,
                now=agent_run.finished_at,
            )
            record_audit(
                session,
                action="agent.cancelled" if cancelled else "agent.failed",
                entity_type="agent_run",
                entity_id=agent_run.id,
                actor="agent",
                after={"status": agent_run.status, **agent_run.summary},
                detail=message,
            )
            if self.config.notifications_enabled and not cancelled:
                pending_notification = (alert.title, alert.message, True)
            failed_run = agent_run
        if pending_notification:
            self._notify(
                pending_notification[0],
                pending_notification[1],
                urgent=pending_notification[2],
            )
        return failed_run

    @staticmethod
    def _resolve_recurring_alert(
        session,
        *,
        title: str,
        deduplication_key: str,
        reason: str,
        now: datetime,
    ) -> Alert | None:
        fingerprint = alert_fingerprint(
            title=title,
            deduplication_key=deduplication_key,
        )
        alert = session.scalar(
            select(Alert).where(
                Alert.fingerprint == fingerprint,
                Alert.resolved_at.is_(None),
            )
        )
        if alert is not None:
            resolve_alert(session, alert, reason=reason, now=now)
        return alert

    def _notify(self, title: str, message: str, *, urgent: bool = False) -> bool:
        try:
            return self.notifier.notify(title, message, urgent=urgent)
        except Exception:
            # Notifications are best-effort and must never roll back the local
            # agent history, tasks, or alerts they are reporting.
            return False

    def _deadline_tasks(self, session, now: datetime) -> int:
        local_zone = ZoneInfo(self.config.timezone)
        local_now = now.astimezone(local_zone)
        inactive_statuses = {JobStatus.IGNORED, JobStatus.STALE, JobStatus.CLOSED}
        deadline_tasks = list(
            session.scalars(
                select(Task).where(
                    Task.job_id.is_not(None),
                    Task.title.like("Application deadline:%"),
                )
            )
        )
        tasks_by_job: dict[str, list[Task]] = {}
        for task in deadline_tasks:
            if task.job_id is not None:
                tasks_by_job.setdefault(task.job_id, []).append(task)

        jobs = list(session.scalars(select(Job)))
        for job in jobs:
            tasks = tasks_by_job.get(job.id, [])
            is_actionable = (
                job.status not in inactive_statuses
                and job.deadline is not None
                and job.deadline >= local_now.date()
            )
            if is_actionable:
                for index, task in enumerate(
                    task for task in tasks if task.status == TaskStatus.PENDING
                ):
                    if index:
                        task.status = TaskStatus.DISMISSED
                        task.completed_at = now
                        continue
                    task.title = f"Application deadline: {job.title}"
                    task.due_at = datetime.combine(
                        job.deadline,
                        time.max,
                        tzinfo=local_zone,
                    ).astimezone(timezone.utc)
            else:
                for task in tasks:
                    if task.status == TaskStatus.PENDING:
                        task.status = TaskStatus.DISMISSED
                        task.completed_at = now

        count = 0
        for job in session.scalars(
            select(Job).where(
                Job.status.not_in(inactive_statuses),
                Job.deadline >= local_now.date(),
                Job.deadline <= (local_now + timedelta(days=2)).date(),
            )
        ):
            title = f"Application deadline: {job.title}"
            tasks = tasks_by_job.get(job.id, [])
            pending = next(
                (task for task in tasks if task.status == TaskStatus.PENDING),
                None,
            )
            if pending is None and not tasks:
                pending = Task(
                    title=title,
                    description="Review the role and decide whether to apply. Jobby will not submit it automatically.",
                    status=TaskStatus.PENDING,
                    due_at=datetime.combine(
                        job.deadline,
                        time.max,
                        tzinfo=local_zone,
                    ).astimezone(timezone.utc),
                    job_id=job.id,
                )
                session.add(pending)
                tasks_by_job[job.id] = [pending]
            if pending is not None:
                pending.title = title
                pending.due_at = datetime.combine(
                    job.deadline,
                    time.max,
                    tzinfo=local_zone,
                ).astimezone(timezone.utc)
                count += 1
        return count

    @staticmethod
    def _new_high_role_count(session, scan_run) -> int:
        """Count high roles created and observed by this scan, not rediscoveries."""

        if scan_run.started_at is None:
            return 0
        count = session.scalar(
            select(func.count(distinct(Job.id)))
            .select_from(Job)
            .join(SourceObservation, SourceObservation.job_id == Job.id)
            .where(
                SourceObservation.scan_run_id == scan_run.id,
                Job.discovered_at >= scan_run.started_at,
                Job.latest_score >= 4.0,
                Job.status.not_in(
                    [JobStatus.IGNORED, JobStatus.STALE, JobStatus.CLOSED]
                ),
            )
        )
        return int(count or 0)


@dataclass(frozen=True, slots=True)
class _ProfileRule:
    profile: ScanProfileRecord

    @property
    def queries(self) -> tuple[str, ...]:
        return self.profile.query_pack or self.profile.role_filters

    def accepts(self, item: ScanItem, *, require_query: bool) -> bool:
        if require_query and self.profile.query_pack:
            searchable = comparison_tokens(f"{item.title} {item.description}")
            if not any(
                comparison_tokens(query) <= searchable
                for query in self.profile.query_pack
            ):
                return False
        if self.profile.role_filters:
            role_text = comparison_tokens(f"{item.title} {item.description}")
            if not any(
                comparison_tokens(role) <= role_text
                for role in self.profile.role_filters
            ):
                return False
        if self.profile.location_filters:
            item_location = comparison_tokens(normalize_location(item.location))
            remote = str(getattr(item.remote, "value", item.remote)).casefold()

            def location_matches(value: str) -> bool:
                normalized = normalize_location(value)
                if normalized == "remote":
                    return remote == "remote" or "remote" in item_location
                tokens = comparison_tokens(normalized)
                return bool(tokens) and tokens <= item_location

            if not any(
                location_matches(value) for value in self.profile.location_filters
            ):
                return False
        return True


class _FocusedSource(JobSource):
    """Union a profile query pack while retaining one source result identity."""

    name = "focused"
    _server_filtered = frozenset({"workday", "usajobs"})

    def __init__(self, source: JobSource, profiles: Sequence[ScanProfileRecord]):
        self.source = source
        self.source_key = source.source_key
        self.concurrency_key = getattr(source, "concurrency_key", source.source_key)
        self.rules = tuple(_ProfileRule(profile) for profile in profiles)
        self.hydration_workers = 1

    def configure_runtime(self, **controls) -> None:
        self.hydration_workers = int(controls.get("hydration_workers", 1))
        configure = getattr(self.source, "configure_runtime", None)
        if callable(configure):
            configure(**controls)

    def _checkpoint(self) -> None:
        checkpoint = getattr(self.source, "checkpoint", None)
        if callable(checkpoint):
            checkpoint()

    def scan(self, query: str | None = None) -> SourceResult:
        del query  # The validated profile pack is the only focused query source.
        provider = self.source_key.split(":", 1)[0].casefold()
        server_filtered = provider in self._server_filtered
        results: list[SourceResult] = []
        if server_filtered:
            queries = tuple(
                dict.fromkeys(
                    query
                    for rule in self.rules
                    for query in rule.queries
                    if query.strip()
                )
            )
            for focused_query in queries:
                self._checkpoint()
                results.append(self._scan_one(focused_query))
        else:
            self._checkpoint()
            results.append(self._scan_one(None))

        items: list[ScanItem] = []
        seen: set[tuple[str, str]] = set()
        errors: list[SourceError] = []
        for result in results:
            errors.extend(result.errors)
            for item in result.items:
                rules = (
                    tuple(
                        rule
                        for rule in self.rules
                        if not server_filtered
                        or rule.accepts(item, require_query=False)
                    )
                    if server_filtered
                    else self.rules
                )
                if not any(
                    rule.accepts(item, require_query=not server_filtered)
                    for rule in rules
                ):
                    continue
                identity = (item.source.casefold(), item.source_id.casefold())
                if identity in seen:
                    continue
                seen.add(identity)
                items.append(item)

        hydrate = getattr(self.source, "hydrate", None)
        hydrate_indices: set[int] = set()
        for index, item in enumerate(items):
            applicable = tuple(
                rule
                for rule in self.rules
                if rule.accepts(item, require_query=not server_filtered)
            )
            should_hydrate = any(
                rule.profile.hydration_policy != "metadata" for rule in applicable
            )
            if should_hydrate and callable(hydrate):
                hydrate_indices.add(index)
        hydrated, hydration_count, hydration_errors = _hydrate_items(
            self.source,
            output_items=items,
            hydrate_inputs=items,
            hydrate_indices=hydrate_indices,
            workers=self.hydration_workers,
            checkpoint=self._checkpoint,
        )
        errors.extend(hydration_errors)

        status = (
            ScanStatus.PARTIAL
            if hydrated and errors
            else ScanStatus.FAILED
            if errors
            else ScanStatus.SUCCEEDED
        )
        now = datetime.now(timezone.utc)
        started = min((result.started_at for result in results), default=now)
        return SourceResult(
            source=self.source_key,
            status=status,
            items=tuple(hydrated),
            errors=tuple(errors),
            started_at=started,
            finished_at=now,
            metadata={
                "focused": True,
                "server_filtered": server_filtered,
                "queries": [query for rule in self.rules for query in rule.queries],
                "profile_ids": [rule.profile.id for rule in self.rules],
                "upstream_calls": len(results),
                "upstream_items": sum(len(result.items) for result in results),
                "hydrated_items": hydration_count,
                "hydration_policies": sorted(
                    {rule.profile.hydration_policy for rule in self.rules}
                ),
            },
        )

    def _scan_one(self, query: str | None) -> SourceResult:
        """Keep successful query-pack rows when a sibling query adapter crashes."""

        try:
            result = self.source.scan(query)
            if not isinstance(result, SourceResult):
                raise TypeError("source did not return a SourceResult")
            if result.source != self.source_key:
                raise ValueError("source result identity changed during focused scan")
            return result
        except InterruptedError:
            raise
        except Exception as exc:
            return SourceResult(
                source=self.source_key,
                status=ScanStatus.FAILED,
                errors=(source_error_from_exception(exc),),
            )


class _InventorySource(JobSource):
    """Keep weekly inventory metadata-first and hydrate only meaningful rows."""

    name = "inventory"

    def __init__(self, source: JobSource, database: Database):
        self.source = source
        self.database = database
        self.source_key = source.source_key
        self.concurrency_key = getattr(source, "concurrency_key", source.source_key)
        self.hydration_workers = 1

    def configure_runtime(self, **controls) -> None:
        self.hydration_workers = int(controls.get("hydration_workers", 1))
        configure = getattr(self.source, "configure_runtime", None)
        if callable(configure):
            configure(**controls)

    def _checkpoint(self) -> None:
        checkpoint = getattr(self.source, "checkpoint", None)
        if callable(checkpoint):
            checkpoint()

    def scan(self, query: str | None = None) -> SourceResult:
        result = self.source.scan(query)
        if not result.items or result.status is ScanStatus.FAILED:
            return result
        existing: dict[str, tuple[str | None, str, JobStatus, str | None]] = {}
        source_ids = tuple(dict.fromkeys(item.source_id for item in result.items))
        with self.database.session() as session:
            for start in range(0, len(source_ids), 500):
                rows = session.execute(
                    select(
                        JobSourceState.source_job_id,
                        JobSourceState.last_content_hash,
                        Job.normalized_title,
                        Job.status,
                        JobSourceState.last_snapshot_hash,
                    )
                    .join(Job, Job.id == JobSourceState.job_id)
                    .where(
                        JobSourceState.source == self.source_key,
                        JobSourceState.source_job_id.in_(
                            source_ids[start : start + 500]
                        ),
                    )
                )
                existing.update(
                    {
                        source_id: (content_hash, title, status, snapshot_hash)
                        for source_id, content_hash, title, status, snapshot_hash in rows
                    }
                )
        hydrate = getattr(self.source, "hydrate", None)
        if not callable(hydrate):
            return result
        errors = list(result.errors)
        metadata_items = [replace(item, description="") for item in result.items]
        hydrate_indices: set[int] = set()
        for index, item in enumerate(result.items):
            prior = existing.get(item.source_id)
            changed = bool(
                prior
                and (
                    _inventory_snapshot_hash(
                        item,
                        effective_description_hash=(
                            item.description_hash or prior[0] or ""
                        ),
                    )
                    != prior[3]
                    or item.normalized_title != prior[1]
                )
            )
            shortlisted = bool(
                prior
                and prior[2] in {JobStatus.SAVED, JobStatus.EVALUATING, JobStatus.READY}
            )
            if not (changed or shortlisted):
                continue
            hydrate_indices.add(index)
        items, hydrated_count, hydration_errors = _hydrate_items(
            self.source,
            output_items=metadata_items,
            hydrate_inputs=list(result.items),
            hydrate_indices=hydrate_indices,
            workers=self.hydration_workers,
            checkpoint=self._checkpoint,
        )
        errors.extend(hydration_errors)
        status = (
            ScanStatus.PARTIAL
            if items and errors
            else ScanStatus.FAILED
            if errors
            else ScanStatus.SUCCEEDED
        )
        return SourceResult(
            source=result.source,
            status=status,
            items=tuple(items),
            errors=tuple(errors),
            started_at=result.started_at,
            finished_at=datetime.now(timezone.utc),
            metadata={
                **dict(result.metadata),
                "inventory_metadata_only": True,
                "hydrated_items": hydrated_count,
            },
        )


def _hydrate_items(
    source: JobSource,
    *,
    output_items: Sequence[ScanItem],
    hydrate_inputs: Sequence[ScanItem],
    hydrate_indices: set[int],
    workers: int,
    checkpoint: Callable[[], None],
) -> tuple[list[ScanItem], int, list[SourceError]]:
    """Hydrate selected rows concurrently while retaining deterministic order."""

    if len(output_items) != len(hydrate_inputs):
        raise ValueError("hydration input and output collections must align")
    hydrated = list(output_items)
    if not hydrate_indices:
        return hydrated, 0, []
    hydrate = getattr(source, "hydrate", None)
    if not callable(hydrate):
        return hydrated, 0, []

    def fetch(index: int) -> ScanItem:
        checkpoint()
        detail = hydrate(hydrate_inputs[index])
        checkpoint()
        return _merge_hydrated_item(hydrate_inputs[index], detail)

    errors: list[SourceError] = []
    completed = 0
    executor = ThreadPoolExecutor(
        max_workers=min(max(1, workers), len(hydrate_indices)),
        thread_name_prefix="jobby-hydration",
    )
    futures: dict[int, Future[ScanItem]] = {
        index: executor.submit(fetch, index) for index in sorted(hydrate_indices)
    }
    try:
        for index in sorted(futures):
            try:
                hydrated[index] = futures[index].result()
                completed += 1
            except InterruptedError:
                for future in futures.values():
                    future.cancel()
                raise
            except Exception as exc:
                errors.append(
                    SourceError(
                        code="hydration_error",
                        message=f"item {index}: {sanitize_error_message(exc)}",
                        item_index=index,
                    )
                )
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return hydrated, completed, errors


def _merge_hydrated_item(original: ScanItem, detail: ScanItem) -> ScanItem:
    if not isinstance(detail, ScanItem):
        raise TypeError("source hydration must return a ScanItem")
    if (detail.source, detail.source_id) != (original.source, original.source_id):
        raise ValueError("source hydration changed the listing identity")
    return replace(
        detail,
        company=detail.company or original.company,
        title=detail.title or original.title,
        url=detail.url or original.url,
        location=detail.location or original.location,
        description=detail.description or original.description,
        salary_text=detail.salary_text or original.salary_text,
        salary=detail.salary or original.salary,
        remote=(
            detail.remote
            if str(getattr(detail.remote, "value", detail.remote)) != "unknown"
            else original.remote
        ),
        posted_at=detail.posted_at or original.posted_at,
        deadline=detail.deadline or original.deadline,
        metadata={**dict(original.metadata), **dict(detail.metadata)},
    )


def _inventory_snapshot_hash(item: ScanItem, *, effective_description_hash: str) -> str:
    """Mirror the scanner snapshot identity before deciding to hydrate details."""

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
    payload = {
        "source_url": item.launch_url,
        "title": item.title,
        "company": item.company,
        "location": item.location,
        "is_live": True,
        "content_hash": effective_description_hash or None,
        "raw_payload": raw_payload,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _focused_sources(
    sources: Sequence[JobSource], profiles: Sequence[ScanProfileRecord]
) -> list[JobSource]:
    focused: list[JobSource] = []
    for source in sources:
        selected = tuple(
            profile
            for profile in profiles
            if any(
                _selector_matches(source.source_key, selector)
                for selector in profile.source_selectors
            )
        )
        if selected:
            focused.append(_FocusedSource(source, selected))
    return focused


def _selector_matches(source_key: str, selector: str) -> bool:
    source_key = source_key.casefold()
    selector = selector.casefold()
    provider = source_key.split(":", 1)[0]
    return selector in {"all", provider, f"{provider}:all", source_key}


def _focused_query_label(profiles: Sequence[ScanProfileRecord]) -> str:
    queries = tuple(
        dict.fromkeys(query for profile in profiles for query in profile.query_pack)
    )
    value = "focused: " + " | ".join(queries)
    return value[:2_000]


def _agent_run_mode(value: AgentRunMode | str) -> AgentRunMode:
    try:
        return value if isinstance(value, AgentRunMode) else AgentRunMode(str(value))
    except ValueError as exc:
        allowed = ", ".join(
            mode.value for mode in AgentRunMode if mode is not AgentRunMode.LEGACY
        )
        raise ValueError(f"agent run mode must be one of: {allowed}") from exc


class _AgentFileLock:
    def __init__(self, path: Path):
        self.path = path
        self._descriptor: int | None = None

    @contextmanager
    def acquire(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire()
        try:
            yield
        finally:
            self._release()

    def _acquire(self) -> None:
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise AgentAlreadyRunning(
                "agent lock path is unsafe or unavailable"
            ) from exc
        try:
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                raise AgentAlreadyRunning("agent lock path is not a regular file")
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise AgentAlreadyRunning(
                    "another Jobby agent cycle is running"
                ) from exc
            payload = json.dumps(
                {
                    "pid": os.getpid(),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                },
                sort_keys=True,
            ).encode()
            os.ftruncate(descriptor, 0)
            os.lseek(descriptor, 0, os.SEEK_SET)
            os.write(descriptor, payload)
            os.fsync(descriptor)
        except Exception:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def _release(self) -> None:
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is None:
            return
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _credential_status(secrets: object, name: str) -> str:
    status_method = getattr(secrets, "status", None)
    if callable(status_method):
        try:
            status = status_method(name)
            state = getattr(status, "state", None)
            if state in {"configured", "missing", "unavailable", "error"}:
                return str(state)
        except Exception:
            return "error"
    try:
        return "configured" if getattr(secrets, "get")(name) else "missing"
    except Exception:
        return "error"


__all__ = ["AgentAlreadyRunning", "AgentRunMode", "DailyAgent"]
