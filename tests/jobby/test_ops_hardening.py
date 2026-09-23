from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from jobby.agent import AgentAlreadyRunning, DailyAgent, _AgentFileLock
from jobby.config import AppConfig, JobbyPaths, SecretStore
from jobby.db import Database
from jobby.enums import AgentRunStatus
from jobby.google_integration import GoogleOAuthManager
from jobby.models import AgentRun, Alert
from jobby.notifications import NativeNotificationProvider
from jobby.openai_provider import OpenAIProvider
from jobby.scanner import build_configured_sources
from jobby.scheduler import Scheduler, rotate_log_files
from jobby.sources.ats import USAJobsSource
from jobby.sources.base import ScanStatus


def make_paths(tmp_path: Path) -> JobbyPaths:
    root = tmp_path / "jobby"
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()


class MappingSecrets:
    def __init__(self, values: dict[str, str] | None = None):
        self.values = values or {}

    def get(self, name: str) -> str | None:
        return self.values.get(name)


def test_scheduled_web_is_opt_in_and_budgets_are_strictly_bounded() -> None:
    config = AppConfig()
    assert config.scheduled_web_enabled is False
    assert config.scheduled_web_query
    assert config.scheduled_ai_max_runs_per_day == 1
    assert config.scheduled_ai_max_total_tokens_per_run == 50_000
    assert config.scheduled_ai_max_cost_usd_per_run == 1.0

    with pytest.raises(ValidationError):
        AppConfig(scheduled_ai_max_runs_per_day=True)
    with pytest.raises(ValidationError):
        AppConfig(scheduled_ai_max_total_tokens_per_run=999)
    with pytest.raises(ValidationError):
        AppConfig(scheduled_ai_max_cost_usd_per_run=float("inf"))


def test_secret_status_distinguishes_missing_and_keyring_failure(monkeypatch) -> None:
    keyring = MagicMock()
    monkeypatch.setitem(sys.modules, "keyring", keyring)
    store = SecretStore(service="test")

    keyring.get_password.return_value = None
    assert store.status("openai_api_key").state == "missing"

    class NoKeyringError(RuntimeError):
        pass

    keyring.get_password.side_effect = NoKeyringError()
    status = store.status("openai_api_key")
    assert status.state == "unavailable"
    assert "NoKeyringError" in status.message


def test_usajobs_uses_bounded_pagination_and_unique_location_sources() -> None:
    pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["Page"])
        pages.append(page)
        start = (page - 1) * 2
        items = [
            {
                "MatchedObjectDescriptor": {
                    "PositionID": f"job-{index}",
                    "PositionTitle": "Policy Analyst",
                    "PositionURI": f"https://www.usajobs.gov/job/{index}",
                    "OrganizationName": "Agency",
                }
            }
            for index in range(start, min(start + 2, 5))
        ]
        return httpx.Response(
            200,
            request=request,
            json={
                "SearchResult": {
                    "SearchResultCountAll": 5,
                    "SearchResultItems": items,
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = USAJobsSource(
            client,
            api_key="key",
            email="person@example.test",
            location="San Diego, California",
            results_per_page=2,
            max_pages=3,
        ).scan("policy")
    assert pages == [1, 2, 3]
    assert len(result.items) == 5
    assert result.metadata["pages_fetched"] == 3
    assert result.metadata["truncated"] is False

    config = AppConfig()
    config.sources.usajobs_locations = ["San Diego, California", "Remote"]
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        sources = build_configured_sources(
            config,
            client=client,
            secret_store=MappingSecrets(
                {
                    "usajobs_api_key": "key",
                    "usajobs_email": "person@example.test",
                }
            ),
            selector="usajobs",
        )
    assert len(sources) == 2
    assert len({source.source_key for source in sources}) == 2
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        inventory_sources = build_configured_sources(
            config,
            client=client,
            secret_store=MappingSecrets(
                {
                    "usajobs_api_key": "key",
                    "usajobs_email": "person@example.test",
                }
            ),
            selector="usajobs",
            inventory=True,
        )
    assert all(source.results_per_page == 500 for source in inventory_sources)
    assert all(source.max_pages == 10 for source in inventory_sources)


def test_usajobs_supports_explicit_national_mode_and_marks_caps_partial() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "LocationName" not in request.url.params
        return httpx.Response(
            200,
            request=request,
            json={
                "SearchResult": {
                    "SearchResultCountAll": 5,
                    "SearchResultItems": [
                        {
                            "MatchedObjectDescriptor": {
                                "PositionID": "USA-1",
                                "PositionTitle": "Policy Analyst",
                                "PositionURI": "https://www.usajobs.gov/job/1",
                                "OrganizationName": "Agency",
                            }
                        }
                    ],
                }
            },
        )

    config = AppConfig()
    config.sources.usajobs_locations = ["*"]
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        [source] = build_configured_sources(
            config,
            client=client,
            secret_store=MappingSecrets(
                {
                    "usajobs_api_key": "key",
                    "usajobs_email": "person@example.test",
                }
            ),
            selector="usajobs",
        )
        source.results_per_page = 1
        source.max_pages = 1
        result = source.scan("policy")

    assert result.status is ScanStatus.PARTIAL
    assert result.metadata["truncated"] is True
    assert result.errors[0].code == "result_truncated"


def test_agent_lock_prevents_overlap_and_recovers_stale_database_rows(
    monkeypatch, tmp_path: Path
) -> None:
    paths = make_paths(tmp_path)
    database = Database(paths=paths)
    database.initialize()
    lock = paths.data_dir / ".jobby-agent.lock"
    agent = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    )
    holder = _AgentFileLock(lock)
    with holder.acquire():
        old = (datetime.now(timezone.utc) - timedelta(days=7)).timestamp()
        os.utime(lock, (old, old))
        with pytest.raises(AgentAlreadyRunning):
            agent.run(include_web=False)

    # An unlocked file with stale metadata is harmless: flock state, not mtime,
    # is authoritative and a crashed process releases its lock in the kernel.
    with database.session() as session:
        stale = AgentRun(
            status=AgentRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        session.add(stale)
        session.flush()
        stale_id = stale.id

    seen: dict[str, bool] = {}

    def finish_cycle(
        self: DailyAgent,
        run_id: str,
        *,
        include_web: bool,
        scheduled_web: bool,
    ) -> AgentRun:
        seen.update(include_web=include_web, scheduled_web=scheduled_web)
        with self.database.session() as session:
            run = session.get(AgentRun, run_id)
            assert run is not None
            run.status = AgentRunStatus.SUCCEEDED
            run.finished_at = datetime.now(timezone.utc)
            return run

    monkeypatch.setattr(DailyAgent, "_run_cycle", finish_cycle)
    run = DailyAgent(
        database,
        AppConfig(
            notifications_enabled=False,
            agent_stale_after_minutes=15,
        ),
        secrets=MappingSecrets(),
    ).run()
    assert run.status is AgentRunStatus.SUCCEEDED
    assert seen == {"include_web": False, "scheduled_web": True}
    assert lock.exists()
    with database.session() as session:
        recovered = session.get(AgentRun, stale_id)
        assert recovered is not None
        assert recovered.status is AgentRunStatus.FAILED
        assert recovered.summary["stale_recovered"] is True
    database.dispose()


def test_recurring_agent_failures_and_stale_recoveries_reuse_alerts(
    tmp_path: Path,
) -> None:
    database = Database(paths=make_paths(tmp_path))
    database.initialize()
    agent = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    )

    for message in ("first source timeout", "second source timeout"):
        with database.session() as session:
            run = AgentRun(
                status=AgentRunStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
            session.add(run)
            session.flush()
            run_id = run.id
        agent._record_failure(run_id, message)

    for _ in range(2):
        with database.session() as session:
            session.add(
                AgentRun(
                    status=AgentRunStatus.RUNNING,
                    started_at=datetime.now(timezone.utc) - timedelta(hours=4),
                )
            )
        assert agent._recover_stale_runs(timedelta(hours=3)) == 1

    with database.session() as session:
        alerts = {alert.title: alert for alert in session.scalars(select(Alert))}
        assert alerts["Jobby agent run failed"].recurrence_count == 2
        assert alerts["Jobby agent run failed"].message == "second source timeout"
        assert alerts["Interrupted agent run recovered"].recurrence_count == 2
        assert all(alert.resolved_at is None for alert in alerts.values())
    database.dispose()


def test_scheduler_detects_drift_and_rotates_bounded_logs(
    monkeypatch, tmp_path: Path
) -> None:
    paths = make_paths(tmp_path)
    commands: list[list[str]] = []

    def runner(command, **_kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, "enabled", "")

    monkeypatch.setattr(
        "jobby.scheduler._agent_command",
        lambda: [sys.executable, "agent", "run"],
    )
    home = tmp_path / "home"
    scheduler = Scheduler(
        AppConfig(schedule_hour=7),
        paths=paths,
        home=home,
        runner=runner,
        platform="darwin",
    )
    assert scheduler.install().enabled is True
    assert scheduler.status().matches_config is True
    drifted = Scheduler(
        AppConfig(schedule_hour=8),
        paths=paths,
        home=home,
        runner=runner,
        platform="darwin",
    ).status()
    assert drifted.matches_config is False
    assert "drifted" in drifted.detail

    log = paths.logs_dir / "agent.stdout.log"
    log.write_bytes(b"x" * 100_001)
    rotated = rotate_log_files(paths, max_bytes=100_000, backup_count=2)
    assert rotated == [paths.logs_dir / "agent.stdout.log.1"]
    assert rotated[0].stat().st_size == 100_001
    assert not log.exists()


def test_openai_retries_only_safe_read_probe_and_caps_billable_output() -> None:
    class APITimeoutError(RuntimeError):
        pass

    client = MagicMock()
    client.models.retrieve.side_effect = [
        APITimeoutError("timeout"),
        SimpleNamespace(id="gpt-5.6-luna"),
        SimpleNamespace(id="gpt-5.6-terra"),
        SimpleNamespace(id="gpt-5.6-sol"),
    ]
    provider = OpenAIProvider(AppConfig(openai_enabled=True), client=client)
    assert provider.validate_models() == {
        "fast": None,
        "quality": None,
        "premium": None,
    }
    assert client.models.retrieve.call_count == 4

    client.responses.create.return_value = SimpleNamespace(
        output_text="result",
        output=[],
        usage=None,
    )
    provider.search("roles", max_output_tokens=512)
    assert client.responses.create.call_args.kwargs["max_output_tokens"] == 512


def test_notification_availability_and_google_disconnect_are_explicit() -> None:
    unavailable = NativeNotificationProvider(
        platform="linux", which=lambda _name: None
    ).availability()
    assert unavailable.available is False
    assert "libnotify" in unavailable.detail

    secrets = MagicMock()
    secrets.get.return_value = json.dumps({"token": "oauth-token"})
    secrets.delete.return_value = True
    response = MagicMock()
    client = MagicMock()
    client.post.return_value = response
    result = GoogleOAuthManager(secrets).disconnect(client=client)
    assert result.local_token_removed is True
    assert result.remote_token_revoked is True
    client.post.assert_called_once()
    secrets.delete.assert_called_once_with("google_oauth_token")


def test_google_oauth_rejects_web_client_configuration_before_authorizing() -> None:
    secrets = MagicMock()
    secrets.get.side_effect = lambda name: (
        None
        if name == "google_oauth_token"
        else json.dumps({"web": {"client_id": "web-client"}})
    )
    with pytest.raises(RuntimeError, match="Desktop app client JSON"):
        GoogleOAuthManager(secrets).credentials(
            ["https://www.googleapis.com/auth/gmail.readonly"],
            interactive=True,
        )


def test_workday_page_budget_is_configurable_and_bounded_by_record_cap() -> None:
    from jobby.scanner import build_configured_sources

    config = AppConfig(workday_scan_max_pages=40, source_record_cap=600)
    with httpx.Client() as client:
        daily = build_configured_sources(config, client=client, selector="workday")
        inventory = build_configured_sources(
            config, client=client, selector="workday", inventory=True
        )
    assert daily and all(source.max_pages == 30 for source in daily)
    assert inventory and all(source.max_pages == 30 for source in inventory)

    config = AppConfig(workday_scan_max_pages=40)
    with httpx.Client() as client:
        daily = build_configured_sources(config, client=client, selector="workday")
        inventory = build_configured_sources(
            config, client=client, selector="workday", inventory=True
        )
    assert all(source.max_pages == 40 for source in daily)
    assert all(source.max_pages == 250 for source in inventory)
    with pytest.raises(ValidationError):
        AppConfig(workday_scan_max_pages=0)
