"""Operational diagnostics with optional integrations reported distinctly."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
import threading
import zipfile
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from io import BytesIO
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import aliased

from .config import AppConfig, JobbyPaths, SecretStore
from .db import Database
from .openai_provider import OpenAIProvider
from .notifications import NativeNotificationProvider
from .scheduler import Scheduler
from .sources.base import sanitize_error_message


KEYRING_STATUS_TIMEOUT_SECONDS = 2.0
_SECRET_CALL_TIMED_OUT = object()


@dataclass(slots=True)
class DoctorCheck:
    name: str
    status: Literal["pass", "warn", "fail"]
    message: str


def run_doctor(
    database: Database,
    config: AppConfig,
    paths: JobbyPaths,
    *,
    secrets: SecretStore | None = None,
    check_network: bool = True,
) -> list[DoctorCheck]:
    secrets = secrets or SecretStore()
    checks: list[DoctorCheck] = []
    try:
        database.initialize()
        ok, detail = database.integrity_check()
        checks.append(DoctorCheck("database", "pass" if ok else "fail", detail))
    except Exception as exc:
        checks.append(DoctorCheck("database", "fail", sanitize_error_message(exc)))
    try:
        from .search_index import SearchIndex

        search = SearchIndex(database).status()
        checks.append(
            DoctorCheck(
                "search_index_fts",
                "pass" if search.fts_available else "fail",
                search.fts_detail,
            )
        )
        checks.append(
            DoctorCheck(
                "search_index_integrity",
                "pass"
                if search.integrity_ok
                else "warn"
                if not search.exists
                else "fail",
                search.integrity_detail,
            )
        )
        count_matches = search.indexed_jobs == search.operational_jobs
        checks.append(
            DoctorCheck(
                "search_index_count",
                "pass" if search.integrity_ok and count_matches else "warn",
                f"{search.indexed_jobs} indexed / {search.operational_jobs} operational jobs",
            )
        )
        checks.append(
            DoctorCheck(
                "search_index_pending",
                "pass"
                if search.pending_changes == 0
                and search.applied_generation == search.current_generation
                else "warn",
                f"{search.pending_changes} pending change(s); generation "
                f"{search.applied_generation}/{search.current_generation}",
            )
        )
    except Exception as exc:
        checks.append(
            DoctorCheck("search_index_fts", "fail", sanitize_error_message(exc))
        )
    checks.append(_paths_check(paths))
    checks.append(_document_runtime_check())
    if not config.openai_enabled:
        openai_status = _secret_status(secrets, "openai_api_key")
        checks.append(
            DoctorCheck(
                "openai_credentials",
                "fail"
                if config.scheduled_web_enabled and openai_status != "configured"
                else "pass"
                if openai_status in {"configured", "missing"}
                else "warn",
                (
                    "credential is present but disabled by configuration"
                    if openai_status == "configured"
                    else "not configured; OpenAI remains disabled"
                    if openai_status == "missing"
                    else f"OS keyring is {openai_status}; OpenAI remains disabled"
                ),
            )
        )
        checks.append(
            DoctorCheck(
                "openai",
                "fail" if config.scheduled_web_enabled else "pass",
                (
                    "scheduled web discovery is enabled but OpenAI is disabled; "
                    "credentials and models were not probed"
                    if config.scheduled_web_enabled
                    else "disabled by configuration; credentials and models were not probed"
                ),
            )
        )
    else:
        openai_status = _secret_status(secrets, "openai_api_key")
        if openai_status != "configured":
            checks.append(
                DoctorCheck(
                    "openai_credentials",
                    "fail" if config.scheduled_web_enabled else "warn",
                    (
                        f"OS keyring is {openai_status}; deterministic features remain available"
                        if openai_status != "missing"
                        else "not configured; deterministic features remain available"
                    ),
                )
            )
        elif check_network:
            try:
                provider = OpenAIProvider(config, secret_store=secrets)
                failures = {
                    tier: error
                    for tier, error in provider.validate_models().items()
                    if error
                }
                if failures:
                    checks.append(
                        DoctorCheck(
                            "openai_models",
                            "fail",
                            "; ".join(
                                f"{tier}: {sanitize_error_message(error)}"
                                for tier, error in failures.items()
                            ),
                        )
                    )
                else:
                    checks.append(
                        DoctorCheck(
                            "openai_models",
                            "pass",
                            "all configured model IDs are available",
                        )
                    )
            except Exception as exc:
                checks.append(
                    DoctorCheck(
                        "openai_models",
                        "fail",
                        sanitize_error_message(exc),
                    )
                )
        else:
            checks.append(
                DoctorCheck("openai_models", "warn", "network validation skipped")
            )
    usajobs_key_status = _secret_status(secrets, "usajobs_api_key")
    usajobs_email_status = _secret_status(secrets, "usajobs_email")
    if {usajobs_key_status, usajobs_email_status} == {"configured"}:
        if check_network:
            checks.append(_usajobs_probe(config, secrets))
        else:
            checks.append(
                DoctorCheck(
                    "usajobs_credentials",
                    "pass",
                    "API key and account email are configured in the OS keyring; network probe skipped",
                )
            )
    elif "unavailable" in {usajobs_key_status, usajobs_email_status} or "error" in {
        usajobs_key_status,
        usajobs_email_status,
    }:
        checks.append(
            DoctorCheck(
                "usajobs_credentials",
                "warn",
                "OS keyring could not read optional USAJobs credentials; USAJobs scans are disabled",
            )
        )
    else:
        missing = []
        if usajobs_key_status != "configured":
            missing.append("API key")
        if usajobs_email_status != "configured":
            missing.append("account email")
        checks.append(
            DoctorCheck(
                "usajobs_credentials",
                "warn",
                f"missing {' and '.join(missing)}; USAJobs scans are disabled",
            )
        )
    if config.google_enabled:
        checks.append(_google_check(secrets, check_network=check_network))
    else:
        checks.append(DoctorCheck("google", "warn", "disabled"))
    notification = NativeNotificationProvider().availability()
    checks.append(
        DoctorCheck(
            "notifications",
            "pass"
            if notification.available and config.notifications_enabled
            else "warn",
            notification.detail
            if config.notifications_enabled
            else "disabled in configuration",
        )
    )
    scheduler_enabled: bool | None = None
    try:
        schedule = Scheduler(config, paths=paths).status()
        scheduler_enabled = bool(
            schedule.installed and schedule.enabled and schedule.matches_config is True
        )
        checks.append(
            DoctorCheck(
                "scheduler",
                "pass"
                if schedule.installed
                and schedule.enabled
                and schedule.matches_config is True
                else "warn",
                "installed and enabled"
                if schedule.installed
                and schedule.enabled
                and schedule.matches_config is True
                else schedule.detail
                or "not installed, not enabled, or configuration drift detected",
            )
        )
    except Exception as exc:
        checks.append(
            DoctorCheck(
                "scheduler",
                "warn",
                f"status unavailable: {sanitize_error_message(exc)}",
            )
        )
    checks.extend(_database_invariant_checks(database))
    checks.extend(
        _operational_checks(
            database,
            config,
            paths,
            secrets=secrets,
            scheduler_enabled=scheduler_enabled,
        )
    )
    return checks


def _database_invariant_checks(database: Database) -> list[DoctorCheck]:
    from .enums import ApprovalState
    from .models import (
        AICacheEntry,
        AIRun,
        Application,
        Artifact,
        DocumentVersion,
        Evaluation,
        Job,
        StageEvent,
    )

    checks: list[DoctorCheck] = []
    try:
        with database.session() as session:
            current = aliased(Evaluation)
            score_mismatches = int(
                session.scalar(
                    select(func.count(Job.id))
                    .outerjoin(
                        current,
                        and_(
                            current.job_id == Job.id,
                            current.is_current.is_(True),
                        ),
                    )
                    .where(
                        or_(
                            and_(Job.latest_score.is_(None), current.id.is_not(None)),
                            and_(Job.latest_score.is_not(None), current.id.is_(None)),
                            and_(
                                Job.latest_score.is_not(None),
                                current.id.is_not(None),
                                Job.latest_score != current.score,
                            ),
                        )
                    )
                )
                or 0
            )
            checks.append(
                DoctorCheck(
                    "evaluation_current_score",
                    "pass" if score_mismatches == 0 else "fail",
                    f"{score_mismatches} job(s) have inconsistent current evaluation/latest score",
                )
            )

            latest_stage: dict[str, object] = {}
            for application_id, to_stage in session.execute(
                select(StageEvent.application_id, StageEvent.to_stage).order_by(
                    StageEvent.application_id,
                    StageEvent.occurred_at,
                    StageEvent.id,
                )
            ):
                latest_stage[str(application_id)] = to_stage
            stage_mismatches = sum(
                1
                for application in session.scalars(select(Application))
                if latest_stage.get(application.id) != application.current_stage
            )
            checks.append(
                DoctorCheck(
                    "application_stage_history",
                    "pass" if stage_mismatches == 0 else "fail",
                    f"{stage_mismatches} application(s) disagree with immutable stage history",
                )
            )

            approved_documents = list(
                session.scalars(
                    select(DocumentVersion).where(
                        DocumentVersion.approval_state == ApprovalState.APPROVED
                    )
                )
            )
            document_mismatches = sum(
                hashlib.sha256(item.content_markdown.encode("utf-8")).hexdigest()
                != item.content_hash
                for item in approved_documents
            )
            checks.append(
                DoctorCheck(
                    "approved_document_hashes",
                    "pass" if document_mismatches == 0 else "fail",
                    f"{document_mismatches} approved document hash mismatch(es)",
                )
            )

            artifact_missing = 0
            artifact_mismatches = 0
            for artifact in session.scalars(select(Artifact)):
                raw_path = artifact.stored_path or artifact.source_path
                if not raw_path:
                    continue
                path = Path(raw_path).expanduser()
                if path.is_symlink() or not path.is_file():
                    artifact_missing += 1
                    continue
                try:
                    digest = _hash_file(path)
                except (OSError, RuntimeError, ValueError):
                    artifact_missing += 1
                    continue
                artifact_mismatches += digest != artifact.content_hash
            artifact_status = (
                "fail"
                if artifact_mismatches
                else "warn"
                if artifact_missing
                else "pass"
            )
            checks.append(
                DoctorCheck(
                    "artifact_hashes",
                    artifact_status,
                    f"{artifact_mismatches} hash mismatch(es); {artifact_missing} missing artifact file(s)",
                )
            )

            invalid_cache_entries = int(
                session.scalar(
                    select(func.count(AICacheEntry.id)).where(
                        or_(
                            AICacheEntry.source_ai_run_id.is_(None),
                            AICacheEntry.cache_key.is_(None),
                        )
                    )
                )
                or 0
            )
            invalid_cache_runs = int(
                session.scalar(
                    select(func.count(AIRun.id)).where(
                        AIRun.cache_hit.is_(True),
                        or_(
                            AIRun.cache_entry_id.is_(None),
                            AIRun.source_ai_run_id.is_(None),
                        ),
                    )
                )
                or 0
            )
            invalid_cache = invalid_cache_entries + invalid_cache_runs
            checks.append(
                DoctorCheck(
                    "ai_cache_provenance",
                    "pass" if invalid_cache == 0 else "fail",
                    f"{invalid_cache} AI cache provenance violation(s)",
                )
            )
    except Exception as exc:
        checks.append(
            DoctorCheck(
                "database_invariants",
                "fail",
                sanitize_error_message(exc),
            )
        )
    return checks


def _operational_checks(
    database: Database,
    config: AppConfig,
    paths: JobbyPaths,
    *,
    secrets: SecretStore | None = None,
    scheduler_enabled: bool | None = None,
    now: datetime | None = None,
) -> list[DoctorCheck]:
    from .enums import AgentRunStatus
    from .models import (
        AgentRun,
        Alert,
        MaintenanceRun,
        ScanRun,
        SourceConfig,
        SourceHealth,
    )

    now = now or datetime.now(timezone.utc)
    checks: list[DoctorCheck] = []
    try:
        with database.session() as session:
            pending_alerts = int(
                session.scalar(
                    select(func.count(Alert.id)).where(
                        Alert.acknowledged_at.is_(None),
                        Alert.resolved_at.is_(None),
                        or_(Alert.snoozed_until.is_(None), Alert.snoozed_until <= now),
                    )
                )
                or 0
            )
            checks.append(
                DoctorCheck(
                    "pending_alerts",
                    "warn" if pending_alerts else "pass",
                    f"{pending_alerts} unread unsnoozed alert(s)",
                )
            )
            unhealthy_sources = int(
                session.scalar(
                    select(func.count(SourceHealth.id)).where(
                        or_(
                            SourceHealth.failure_streak > 0,
                            SourceHealth.anomaly_state.not_in(("healthy", "unknown")),
                            and_(
                                SourceHealth.last_attempt_at.is_not(None),
                                SourceHealth.last_attempt_at < now - timedelta(days=8),
                            ),
                        )
                    )
                )
                or 0
            )
            checks.append(
                DoctorCheck(
                    "source_health",
                    "warn" if unhealthy_sources else "pass",
                    f"{unhealthy_sources} stale, anomalous, or failing source(s)",
                )
            )
            configured_sources = _configured_source_keys(
                config,
                tuple(
                    session.scalars(
                        select(SourceConfig).where(
                            SourceConfig.enabled.is_(True),
                            SourceConfig.provider == "static_http",
                        )
                    )
                ),
                secrets=secrets,
            )
            attempted_sources = {
                str(source).casefold()
                for source in session.scalars(
                    select(SourceHealth.source).where(
                        SourceHealth.last_attempt_at.is_not(None)
                    )
                )
            }
            never_attempted = tuple(
                source
                for source in configured_sources
                if source.casefold() not in attempted_sources
            )
            preview = ", ".join(never_attempted[:5])
            checks.append(
                DoctorCheck(
                    "source_attempts",
                    "warn" if never_attempted else "pass",
                    (
                        f"{len(never_attempted)} configured source(s) never attempted"
                        + (f": {preview}" if preview else "")
                        + (
                            f"; {len(never_attempted) - 5} more"
                            if len(never_attempted) > 5
                            else ""
                        )
                    ),
                )
            )
            stale_runs = int(
                session.scalar(
                    select(func.count(AgentRun.id)).where(
                        AgentRun.status == AgentRunStatus.RUNNING,
                        or_(
                            AgentRun.started_at.is_(None),
                            AgentRun.started_at
                            < now - timedelta(minutes=config.agent_stale_after_minutes),
                        ),
                    )
                )
                or 0
            )
            latest_run = session.scalar(
                select(AgentRun)
                .order_by(
                    func.coalesce(AgentRun.started_at, AgentRun.created_at).desc(),
                    AgentRun.created_at.desc(),
                )
                .limit(1)
            )
            if stale_runs:
                agent_status: Literal["pass", "warn", "fail"] = "fail"
                agent_detail = f"{stale_runs} stale running agent record(s)"
            elif latest_run is None:
                agent_status = "warn"
                agent_detail = "no scheduled run has completed yet"
            elif latest_run.status == AgentRunStatus.SUCCEEDED:
                agent_status = "pass"
                agent_detail = "last result: succeeded"
            elif latest_run.status == AgentRunStatus.FAILED:
                agent_status = "fail"
                agent_detail = "last result: failed"
            else:
                agent_status = "warn"
                agent_detail = f"last result: {latest_run.status.value}"
            checks.append(
                DoctorCheck(
                    "agent_runs",
                    agent_status,
                    agent_detail,
                )
            )
            checks.append(
                _scheduler_run_check(
                    tuple(
                        session.scalars(
                            select(AgentRun).order_by(
                                func.coalesce(
                                    AgentRun.finished_at,
                                    AgentRun.started_at,
                                    AgentRun.created_at,
                                ).desc()
                            )
                        )
                    ),
                    enabled=scheduler_enabled,
                    now=now,
                )
            )
            stale_scan_runs = int(
                session.scalar(
                    select(func.count(ScanRun.id)).where(
                        ScanRun.status == AgentRunStatus.RUNNING,
                        or_(
                            ScanRun.started_at.is_(None),
                            ScanRun.started_at
                            < now - timedelta(minutes=config.agent_stale_after_minutes),
                        ),
                    )
                )
                or 0
            )
            checks.append(
                DoctorCheck(
                    "scan_runs",
                    "fail" if stale_scan_runs else "pass",
                    f"{stale_scan_runs} stale running scan record(s)",
                )
            )
            rehearsal = session.scalar(
                select(MaintenanceRun)
                .where(MaintenanceRun.kind == "monthly_restore_rehearsal")
                .order_by(MaintenanceRun.started_at.desc())
                .limit(1)
            )
            if rehearsal is None:
                rehearsal_status = "warn"
                rehearsal_detail = "no monthly restore rehearsal has completed"
            else:
                rehearsal_age = now - rehearsal.started_at
                rehearsal_status = (
                    "pass"
                    if rehearsal.status == "succeeded"
                    and rehearsal_age <= timedelta(days=45)
                    else "warn"
                )
                rehearsal_detail = (
                    f"last result: {rehearsal.status}; "
                    f"{max(0, rehearsal_age.days)} day(s) ago"
                )
            checks.append(
                DoctorCheck(
                    "restore_rehearsal",
                    rehearsal_status,
                    rehearsal_detail,
                )
            )
    except Exception as exc:
        checks.append(DoctorCheck("operations", "warn", sanitize_error_message(exc)))

    size = database.path.stat().st_size if database.path.exists() else 0
    checks.append(
        DoctorCheck(
            "database_size",
            "warn" if size > 1_000_000_000 else "pass",
            f"{size:,} bytes",
        )
    )
    checks.append(_database_growth_check(database))
    checks.append(_verified_local_backup_check(paths, now=now))
    if config.external_backup_destination is not None:
        checks.append(
            _verified_external_backup_check(
                database,
                config,
                paths,
                secrets=secrets,
                now=now,
            )
        )
    return checks


def _configured_source_keys(
    config: AppConfig,
    source_configs: tuple[object, ...],
    *,
    secrets: SecretStore | None,
) -> tuple[str, ...]:
    """Return persisted source identities that are currently usable/enabled."""

    keys: set[str] = set()

    def add(value: str) -> None:
        value = value.strip().casefold()[:80]
        if value:
            keys.add(value)

    for provider, accounts in (
        ("greenhouse", config.sources.greenhouse),
        ("lever", config.sources.lever),
        ("ashby", config.sources.ashby),
        ("workable", config.sources.workable),
    ):
        for account in accounts:
            add(f"{provider}:{account}")
    for fallback, item in config.sources.workday.items():
        if isinstance(item, dict):
            tenant = str(item.get("tenant") or fallback)
            site = str(item.get("site") or fallback)
        else:
            tenant = str(getattr(item, "tenant", fallback))
            site = str(getattr(item, "site", fallback))
        add(f"workday:{tenant}:{site}")

    if secrets is not None and {
        _secret_status(secrets, "usajobs_api_key"),
        _secret_status(secrets, "usajobs_email"),
    } == {"configured"}:
        for location in config.sources.usajobs_locations:
            if location == "*":
                add("usajobs")
                continue
            normalized = re.sub(r"[^a-z0-9]+", "-", location.casefold()).strip("-")
            suffix = hashlib.sha256(location.casefold().encode()).hexdigest()[:8]
            add(f"usajobs:{normalized[:48]}-{suffix}")

    for source_config in source_configs:
        name = str(getattr(source_config, "name", "")).strip()
        config_json = getattr(source_config, "config_json", {})
        url = (
            str(config_json.get("careers_url") or "").strip()
            if isinstance(config_json, dict)
            else ""
        )
        if not name or not url:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", name.casefold()).strip("-")
        if not slug:
            slug = hashlib.sha256(name.encode()).hexdigest()[:12]
        add(f"portal:{slug}")
    return tuple(sorted(keys))


def _scheduler_run_check(
    runs: tuple[object, ...],
    *,
    enabled: bool | None,
    now: datetime,
) -> DoctorCheck:
    """Report durable focused/inventory results when both timers are active."""

    if enabled is False:
        return DoctorCheck(
            "scheduler_runs",
            "pass",
            "scheduler is not enabled; no scheduled cadence is expected",
        )

    latest_by_mode: dict[str, object] = {}
    for run in runs:
        summary = getattr(run, "summary", None)
        if not isinstance(summary, dict) or not isinstance(
            summary.get("recovery"), dict
        ):
            continue
        mode = str(summary.get("run_mode") or "").casefold()
        if mode in {"focused", "inventory"} and mode not in latest_by_mode:
            latest_by_mode[mode] = run

    if enabled is None and not latest_by_mode:
        return DoctorCheck(
            "scheduler_runs",
            "warn",
            "scheduler state is unavailable and no scheduled result is recorded",
        )

    details: list[str] = []
    overall: Literal["pass", "warn", "fail"] = "pass"
    for mode, maximum_age in (
        ("focused", timedelta(days=2)),
        ("inventory", timedelta(days=8)),
    ):
        run = latest_by_mode.get(mode)
        if run is None:
            if enabled:
                overall = "warn" if overall == "pass" else overall
                details.append(f"{mode}: no recorded result")
            continue
        status = getattr(getattr(run, "status", None), "value", "unknown")
        completed_at = (
            getattr(run, "finished_at", None)
            or getattr(run, "started_at", None)
            or getattr(run, "created_at", None)
        )
        age = (
            max(timedelta(0), _as_utc(now) - _as_utc(completed_at))
            if completed_at is not None
            else None
        )
        stale = age is None or age > maximum_age
        if status == "failed":
            overall = "fail"
        elif status != "succeeded" or stale:
            if overall != "fail":
                overall = "warn"
        age_detail = "unknown age" if age is None else f"{age.days} day(s) ago"
        details.append(
            f"{mode}: {status}, {age_detail}" + (" (stale)" if stale else "")
        )
    if not details:
        details.append("no scheduled focused or inventory result is recorded")
    return DoctorCheck("scheduler_runs", overall, "; ".join(details))


def _database_growth_check(database: Database) -> DoctorCheck:
    """Compare unchanged, creation-verified database snapshots in local backups."""

    from .models import BackupRecord

    try:
        with database.session() as session:
            records = tuple(
                session.scalars(
                    select(BackupRecord)
                    .where(BackupRecord.external.is_(False))
                    .order_by(BackupRecord.verified_at.desc())
                    .limit(50)
                )
            )
        samples: list[tuple[datetime, int]] = []
        for record in records:
            size = _recorded_backup_database_size(record)
            if size is not None:
                samples.append((_as_utc(record.verified_at), size))
        if len(samples) < 2:
            return DoctorCheck(
                "database_growth",
                "warn",
                "fewer than two unchanged verified local backups are available for a growth trend",
            )
        newest = samples[0]
        oldest = next(
            (
                sample
                for sample in reversed(samples[1:])
                if newest[0] - sample[0] >= timedelta(days=1)
            ),
            None,
        )
        if oldest is None:
            return DoctorCheck(
                "database_growth",
                "warn",
                "verified local backup history spans less than one day",
            )
        elapsed = max(timedelta(days=1), newest[0] - oldest[0])
        change = newest[1] - oldest[1]
        rapid = change > max(256 * 1024 * 1024, oldest[1] // 2)
        direction = "grew" if change >= 0 else "shrunk"
        return DoctorCheck(
            "database_growth",
            "warn" if rapid else "pass",
            f"database snapshot {direction} by {abs(change):,} bytes over "
            f"{elapsed.days} day(s) ({oldest[1]:,} to {newest[1]:,} bytes)",
        )
    except Exception as exc:
        return DoctorCheck(
            "database_growth",
            "warn",
            f"growth history unavailable: {sanitize_error_message(exc)}",
        )


def _recorded_backup_database_size(record: object) -> int | None:
    path = Path(str(getattr(record, "path", ""))).expanduser().absolute()
    try:
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode):
            return None
        if before.st_size != int(getattr(record, "size_bytes", -1)):
            return None
        if _hash_file(path) != str(getattr(record, "plaintext_sha256", "")):
            return None
        after = path.lstat()
        if _file_identity(before) != _file_identity(after):
            return None
        with zipfile.ZipFile(path) as archive:
            info = archive.getinfo("database/jobby.sqlite3")
            if info.file_size < 0 or info.file_size > 8 * 1024 * 1024 * 1024:
                return None
            return info.file_size
    except (KeyError, OSError, TypeError, ValueError, zipfile.BadZipFile):
        return None


def _verified_external_backup_check(
    database: Database,
    config: AppConfig,
    paths: JobbyPaths,
    *,
    secrets: SecretStore | None,
    now: datetime,
) -> DoctorCheck:
    """Authenticate and recovery-test the newest configured external file."""

    from .backup import verify_backup
    from .encrypted_backup import decrypt_backup
    from .models import BackupRecord

    destination = Path(config.external_backup_destination or "").expanduser().absolute()
    try:
        destination_identity = destination.lstat()
    except OSError:
        return DoctorCheck(
            "external_backup",
            "warn",
            "configured external backup destination is unavailable",
        )
    if destination.is_symlink() or not stat.S_ISDIR(destination_identity.st_mode):
        return DoctorCheck(
            "external_backup",
            "fail",
            "configured external backup destination is not a safe regular directory",
        )

    try:
        with database.session() as session:
            records = tuple(
                session.scalars(
                    select(BackupRecord)
                    .where(
                        BackupRecord.external.is_(True),
                        BackupRecord.recovery_tested_at.is_not(None),
                    )
                    .order_by(BackupRecord.verified_at.desc())
                    .limit(50)
                )
            )
    except Exception as exc:
        return DoctorCheck(
            "external_backup",
            "warn",
            f"external backup ledger is unavailable: {sanitize_error_message(exc)}",
        )
    record = next(
        (
            item
            for item in records
            if Path(item.path).expanduser().absolute().parent == destination
        ),
        None,
    )
    if record is None:
        return DoctorCheck(
            "external_backup",
            "warn",
            "no tested encrypted backup is recorded for the configured destination",
        )

    encrypted = Path(record.path).expanduser().absolute()
    try:
        before = encrypted.lstat()
    except OSError:
        return DoctorCheck(
            "external_backup",
            "warn",
            "newest recorded external backup file is unavailable",
        )
    if encrypted.is_symlink() or not stat.S_ISREG(before.st_mode):
        return DoctorCheck(
            "external_backup",
            "fail",
            "newest recorded external backup is not a safe regular file",
        )
    if before.st_size != record.size_bytes:
        return DoctorCheck(
            "external_backup",
            "fail",
            "newest recorded external backup size differs from its verified ledger",
        )

    try:
        passphrase = (
            _bounded_secret_call(
                lambda: getattr(secrets, "get")("external_backup_passphrase")
            )
            if secrets is not None
            else None
        )
    except Exception as exc:
        return DoctorCheck(
            "external_backup",
            "warn",
            f"external backup passphrase is unavailable: {sanitize_error_message(exc)}",
        )
    if passphrase is _SECRET_CALL_TIMED_OUT:
        return DoctorCheck(
            "external_backup",
            "warn",
            "external backup keyring lookup timed out; the file was not authenticated",
        )
    if not isinstance(passphrase, str) or not passphrase:
        return DoctorCheck(
            "external_backup",
            "warn",
            "external backup file is present but cannot be authenticated without the stored passphrase",
        )

    try:
        with tempfile.TemporaryDirectory(
            prefix="jobby-doctor-external-", dir=paths.cache_dir
        ) as temporary_name:
            recovered = Path(temporary_name) / "recovered.zip"
            decrypt_backup(encrypted, recovered, passphrase)
            verified, detail = verify_backup(recovered)
            if not verified:
                raise RuntimeError(f"decrypted backup verification failed: {detail}")
            if _hash_file(recovered) != record.plaintext_sha256:
                raise RuntimeError("decrypted backup checksum differs from its ledger")
        after = encrypted.lstat()
        if _file_identity(before) != _file_identity(after):
            raise RuntimeError("encrypted backup changed during verification")
    except Exception as exc:
        return DoctorCheck(
            "external_backup",
            "fail",
            f"encrypted backup recovery verification failed: {sanitize_error_message(exc)}",
        )

    age = max(timedelta(0), now - _as_utc(record.verified_at))
    return DoctorCheck(
        "external_backup",
        "pass" if age <= timedelta(days=8) else "warn",
        f"newest encrypted backup authenticated and recovery-verified; "
        f"{age.days} day(s) old",
    )


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("hash source must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        after = os.fstat(descriptor)
        if _file_identity(before) != _file_identity(after):
            raise RuntimeError("file changed while it was being hashed")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _verified_local_backup_check(
    paths: JobbyPaths,
    *,
    now: datetime,
) -> DoctorCheck:
    """Report age only for a regular, unchanged backup that passes verification."""

    from .backup import verify_backup

    try:
        entries = tuple(paths.backups_dir.iterdir())
    except OSError as exc:
        return DoctorCheck(
            "backup_age",
            "warn",
            f"local backup directory is unreadable: {sanitize_error_message(exc)}",
        )

    candidates: list[tuple[Path, os.stat_result]] = []
    ignored = 0
    unsafe = 0
    for item in entries:
        if item.suffix.casefold() != ".zip":
            ignored += 1
            continue
        try:
            identity = item.lstat()
        except OSError:
            unsafe += 1
            continue
        if item.is_symlink() or not stat.S_ISREG(identity.st_mode):
            unsafe += 1
            continue
        candidates.append((item, identity))
    candidates.sort(key=lambda item: item[1].st_mtime_ns, reverse=True)

    rejected: list[str] = []
    for candidate, identity in candidates:
        verified, detail = verify_backup(candidate)
        try:
            current_identity = candidate.lstat()
        except OSError:
            verified = False
            detail = "candidate disappeared during verification"
        else:
            before = (
                identity.st_dev,
                identity.st_ino,
                identity.st_mode,
                identity.st_size,
                identity.st_mtime_ns,
                identity.st_ctime_ns,
            )
            after = (
                current_identity.st_dev,
                current_identity.st_ino,
                current_identity.st_mode,
                current_identity.st_size,
                current_identity.st_mtime_ns,
                current_identity.st_ctime_ns,
            )
            if before != after:
                verified = False
                detail = "candidate changed during verification"
        if not verified:
            rejected.append(f"{candidate.name}: {detail}")
            continue

        modified_at = datetime.fromtimestamp(identity.st_mtime, timezone.utc)
        age = max(timedelta(0), now - modified_at)
        notes: list[str] = []
        if rejected:
            notes.append(f"{len(rejected)} newer invalid candidate(s) ignored")
        if unsafe:
            notes.append(f"{unsafe} unsafe ZIP candidate(s) ignored")
        if ignored:
            notes.append(f"{ignored} non-backup item(s) ignored")
        suffix = f"; {'; '.join(notes)}" if notes else ""
        return DoctorCheck(
            "backup_age",
            "warn" if age > timedelta(days=2) else "pass",
            f"newest verified local backup is {age.days} day(s) old{suffix}",
        )

    notes = []
    if rejected:
        notes.append(f"{len(rejected)} invalid ZIP candidate(s)")
    if unsafe:
        notes.append(f"{unsafe} unsafe ZIP candidate(s)")
    if ignored:
        notes.append(f"{ignored} non-backup item(s)")
    detail = "; ".join(notes)
    return DoctorCheck(
        "backup_age",
        "warn",
        "no verified local backup found" + (f"; ignored {detail}" if detail else ""),
    )


def _google_check(secrets: SecretStore, *, check_network: bool) -> DoctorCheck:
    """Validate Google without reading messages/events or mutating anything."""

    token_status = _secret_status(secrets, "google_oauth_token")
    if token_status != "configured":
        return DoctorCheck(
            "google",
            "fail",
            f"enabled but authorization token is {token_status}",
        )
    if not check_network:
        return DoctorCheck(
            "google",
            "warn",
            "authorization token is present; scope and connectivity validation skipped",
        )
    try:
        from .google_integration import (
            CALENDAR_EVENTS_READONLY_SCOPE,
            GMAIL_READONLY_SCOPE,
            CalendarProvider,
            GmailProvider,
            GoogleOAuthManager,
        )

        oauth = GoogleOAuthManager(secrets)
        oauth.credentials(
            [GMAIL_READONLY_SCOPE, CALENDAR_EVENTS_READONLY_SCOPE],
            interactive=False,
        )
        gmail = GmailProvider(oauth)
        calendar = CalendarProvider(oauth, allow_write=False)
        # These are the smallest useful read-only probes. Never retrieve a
        # message/event body or call an insert/update/delete endpoint here.
        gmail.service.users().getProfile(userId="me").execute()
        now = datetime.now(timezone.utc)
        calendar.service.events().list(
            calendarId="primary",
            timeMin=now.isoformat().replace("+00:00", "Z"),
            timeMax=(now + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
            maxResults=1,
            singleEvents=True,
        ).execute()
        return DoctorCheck(
            "google",
            "pass",
            "Gmail profile and Calendar connectivity verified read-only",
        )
    except Exception as exc:
        return DoctorCheck("google", "fail", sanitize_error_message(exc))


def _paths_check(paths: JobbyPaths) -> DoctorCheck:
    label = "paths"
    try:
        paths.ensure()
        directories = {
            "data": paths.data_dir,
            "config": paths.config_dir,
            "cache": paths.cache_dir,
            "artifacts": paths.artifacts_dir,
            "backups": paths.backups_dir,
            "logs": paths.logs_dir,
            "database parent": paths.database.parent,
        }
        checked: set[object] = set()
        for label, directory in directories.items():
            resolved = directory.resolve()
            if resolved in checked:
                continue
            checked.add(resolved)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix=".jobby-doctor-",
                dir=directory,
            ) as probe:
                probe.write("ok")
                probe.flush()
        return DoctorCheck(
            "paths",
            "pass",
            "data, config, cache, artifact, backup, log, and database directories are writable",
        )
    except Exception as exc:
        return DoctorCheck(
            "paths",
            "fail",
            f"{label} directory is not writable: {sanitize_error_message(exc)}",
        )


def _document_runtime_check() -> DoctorCheck:
    try:
        import httpx
        from pypdf import PdfReader
        from reportlab.pdfgen import canvas

        output = BytesIO()
        document = canvas.Canvas(output)
        document.drawString(72, 720, "Jobby diagnostic")
        document.save()
        output.seek(0)
        if len(PdfReader(output).pages) != 1:
            raise RuntimeError("generated PDF did not contain exactly one page")
        if not getattr(httpx, "__version__", None):
            raise RuntimeError("HTTP client version is unavailable")
        return DoctorCheck(
            "document_runtime",
            "pass",
            "browser-free PDF generation and static HTTP extraction runtime are available",
        )
    except Exception as exc:
        return DoctorCheck(
            "document_runtime",
            "fail",
            f"document/static HTTP runtime unavailable: {sanitize_error_message(exc)}",
        )


# Compatibility hook for older callers/tests. This no longer launches a browser.
def _browser_check() -> DoctorCheck:
    return _document_runtime_check()


def _secret_status(secrets: object, name: str) -> str:
    status_method = getattr(secrets, "status", None)
    if callable(status_method):
        try:
            status = _bounded_secret_call(lambda: status_method(name))
            if status is _SECRET_CALL_TIMED_OUT:
                return "unavailable"
            state = getattr(status, "state", None)
            if state in {"configured", "missing", "unavailable", "error"}:
                return str(state)
        except Exception:
            return "error"
    try:
        value = _bounded_secret_call(lambda: getattr(secrets, "get")(name))
        if value is _SECRET_CALL_TIMED_OUT:
            return "unavailable"
        return "configured" if value else "missing"
    except Exception:
        return "error"


def _bounded_secret_call(call: Callable[[], object]) -> object:
    """Keep diagnostics responsive if an OS keyring blocks awaiting UI."""

    values: list[object] = []
    errors: list[Exception] = []

    def invoke() -> None:
        try:
            values.append(call())
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(
        target=invoke,
        name="jobby-doctor-keyring",
        daemon=True,
    )
    worker.start()
    worker.join(KEYRING_STATUS_TIMEOUT_SECONDS)
    if worker.is_alive():
        return _SECRET_CALL_TIMED_OUT
    if errors:
        raise errors[0]
    return values[0] if values else _SECRET_CALL_TIMED_OUT


def _usajobs_probe(config: AppConfig, secrets: SecretStore) -> DoctorCheck:
    """Perform a one-item authenticated public search without writing data."""

    try:
        import httpx

        location = config.sources.usajobs_locations[0]
        params: dict[str, str | int] = {
            "Keyword": "policy",
            "ResultsPerPage": 1,
            "Page": 1,
        }
        if location != "*":
            params["LocationName"] = location
        response = httpx.get(
            "https://data.usajobs.gov/api/search",
            headers={
                "Accept": "application/json",
                "Authorization-Key": secrets.get("usajobs_api_key") or "",
                "User-Agent": secrets.get("usajobs_email") or "",
            },
            params=params,
            timeout=10,
            follow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(
            payload.get("SearchResult"), dict
        ):
            raise ValueError("authenticated probe returned an unexpected response")
        return DoctorCheck(
            "usajobs_credentials",
            "pass",
            "authenticated read-only probe succeeded for "
            + ("national search" if location == "*" else location),
        )
    except Exception as exc:
        return DoctorCheck("usajobs_credentials", "fail", sanitize_error_message(exc))


def doctor_exit_code(checks: list[DoctorCheck]) -> int:
    return 1 if any(check.status == "fail" for check in checks) else 0


__all__ = ["DoctorCheck", "doctor_exit_code", "run_doctor"]
