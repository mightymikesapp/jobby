from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
import plistlib
from pathlib import Path
import subprocess
import threading
import time
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from jobby.agent import AgentRunMode, DailyAgent, _FocusedSource, _InventorySource
from jobby.config import AppConfig, JobbyPaths, SecretStore
from jobby.db import Database
from jobby.enums import AgentRunStatus, JobStatus
from jobby.models import AuditEvent, Job, ScanRun, SourceObservation
from jobby.profiles import (
    ScanProfileDraft,
    create_profile,
    delete_profile,
    get_profile,
    list_profiles,
    update_profile,
)
from jobby.scheduler import INVENTORY_LABEL, LABEL, Scheduler
from jobby.scanner import Scanner
from jobby.sources.base import ScanItem, ScanStatus, SourceResult


def _paths(root: Path) -> JobbyPaths:
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "backups",
        logs_dir=root / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database(paths=_paths(tmp_path / "jobby"), acquire_lock=False)
    value.initialize()
    yield value
    value.dispose()


class RecordingRunner:
    def __init__(self, returncodes: list[int] | None = None):
        self.commands: list[list[str]] = []
        self.returncodes = list(returncodes or [])

    def __call__(self, command, **_kwargs):
        self.commands.append(list(command))
        code = self.returncodes.pop(0) if self.returncodes else 0
        return subprocess.CompletedProcess(command, code, "enabled\n", "")


class NoSecrets(SecretStore):
    def __init__(self) -> None:
        pass

    def get(self, name: str) -> None:
        del name
        return None


def test_profile_crud_keeps_new_profiles_disabled_until_explicit_enable(
    database: Database,
) -> None:
    seeded = list_profiles(database)
    assert len(seeded) == 1
    assert seeded[0].name == "Focused legal AI / IP / policy"
    assert seeded[0].enabled is False

    created = create_profile(
        database,
        {
            "name": "Privacy and AI counsel",
            "description": "Editable focused scan",
            "sources": ["workday", "greenhouse:example"],
            "queries": ["privacy counsel", "legal AI", "privacy counsel"],
            "location_filters": ["Remote"],
            "role_filters": ["counsel"],
        },
    )
    assert created.enabled is False
    assert created.source_selectors == ("workday", "greenhouse:example")
    assert created.query_pack == ("privacy counsel", "legal AI")

    enabled = update_profile(
        database,
        created.id,
        enabled=True,
        hydration_policy="focused",
    )
    assert enabled.enabled is True
    assert get_profile(database, "PRIVACY AND AI COUNSEL").id == created.id
    assert [profile.id for profile in list_profiles(database, enabled=True)] == [
        created.id
    ]

    cleared = update_profile(database, created.id, {"description": None})
    assert cleared.description is None
    removed = delete_profile(database, created.id)
    assert removed.id == created.id
    with pytest.raises(LookupError):
        get_profile(database, created.id)
    with database.session() as session:
        actions = set(
            session.scalars(
                select(AuditEvent.action).where(AuditEvent.entity_id == created.id)
            )
        )
    assert actions == {
        "scan_profile.created",
        "scan_profile.updated",
        "scan_profile.deleted",
    }


def test_profile_validation_blocks_implicit_or_unusable_enable(
    database: Database,
) -> None:
    with pytest.raises(ValidationError, match="needs a query or role filter"):
        ScanProfileDraft(name="Empty enabled", enabled=True)
    with pytest.raises(ValidationError, match="source selector"):
        ScanProfileDraft(
            name="No source",
            enabled=True,
            source_selectors=(),
            query_pack=("legal",),
        )

    create_profile(database, {"name": "Case Name", "queries": ["legal"]})
    with pytest.raises(ValueError, match="already exists"):
        create_profile(database, {"name": "case name", "queries": ["policy"]})


def test_launchd_install_status_and_remove_cover_both_cadences(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    command = ["/usr/bin/true", "agent", "run"]
    monkeypatch.setattr("jobby.scheduler._agent_command", lambda: command)
    runner = RecordingRunner()
    scheduler = Scheduler(
        AppConfig(
            schedule_hour=7,
            inventory_weekday=6,
            inventory_hour=6,
            timezone="America/Los_Angeles",
        ),
        paths=_paths(tmp_path / "paths"),
        home=tmp_path / "home",
        runner=runner,
        platform="darwin",
    )

    installed = scheduler.install()

    assert installed.installed and installed.enabled
    assert {item.mode for item in installed.definitions} == {
        "focused",
        "inventory",
    }
    payloads = {
        item.mode: plistlib.loads(Path(item.path).read_bytes())
        for item in installed.definitions
    }
    assert payloads["focused"]["Label"] == LABEL
    assert payloads["focused"]["StartCalendarInterval"] == {
        "Hour": 7,
        "Minute": 0,
    }
    assert payloads["focused"]["EnvironmentVariables"]["JOBBY_AGENT_MODE"] == (
        "focused"
    )
    assert payloads["inventory"]["Label"] == INVENTORY_LABEL
    assert payloads["inventory"]["StartCalendarInterval"] == {
        "Weekday": 1,
        "Hour": 6,
        "Minute": 0,
    }
    assert payloads["inventory"]["EnvironmentVariables"]["JOBBY_AGENT_MODE"] == (
        "inventory"
    )
    assert [command[:2] for command in runner.commands] == [
        ["launchctl", "bootstrap"],
        ["launchctl", "bootstrap"],
    ]
    status = scheduler.status()
    assert status.installed and status.enabled and status.matches_config

    removed = scheduler.remove()
    assert removed.installed is False
    assert removed.enabled is False
    assert all(not Path(item.path).exists() for item in removed.definitions)


def test_systemd_install_writes_distinct_modes_and_weekly_sunday_timer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "jobby.scheduler._agent_command",
        lambda: ["/opt/Jobby App/jobby", "agent", "run"],
    )
    runner = RecordingRunner()
    scheduler = Scheduler(
        AppConfig(
            schedule_hour=7,
            inventory_weekday=6,
            inventory_hour=6,
            timezone="America/Los_Angeles",
        ),
        paths=_paths(tmp_path / "paths"),
        home=tmp_path / "home",
        runner=runner,
        platform="linux",
    )

    status = scheduler.install()
    directory = Path(status.path).parent
    focused_service = (directory / "jobby-agent.service").read_text()
    inventory_service = (directory / "jobby-agent-inventory.service").read_text()
    focused_timer = (directory / "jobby-agent.timer").read_text()
    inventory_timer = (directory / "jobby-agent-inventory.timer").read_text()

    assert 'Environment="JOBBY_AGENT_MODE=focused"' in focused_service
    assert 'Environment="JOBBY_AGENT_MODE=inventory"' in inventory_service
    assert "OnCalendar=*-*-* 07:00:00 America/Los_Angeles" in focused_timer
    assert "OnCalendar=Sun *-*-* 06:00:00 America/Los_Angeles" in inventory_timer
    assert runner.commands == [
        ["systemctl", "--user", "daemon-reload"],
        [
            "systemctl",
            "--user",
            "enable",
            "--now",
            "jobby-agent.timer",
            "jobby-agent-inventory.timer",
        ],
    ]


def test_scheduler_construction_and_empty_status_never_install_or_enable(
    tmp_path: Path,
) -> None:
    runner = RecordingRunner()
    home = tmp_path / "home"
    status = Scheduler(
        AppConfig(),
        paths=_paths(tmp_path / "paths"),
        home=home,
        runner=runner,
        platform="linux",
    ).status()

    assert not status.installed
    assert status.enabled is False
    assert runner.commands == []
    assert not (home / ".config" / "systemd" / "user").exists()


def test_scheduler_rebind_preserves_absent_opt_in_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "jobby.scheduler._agent_command",
        lambda: ["/usr/bin/true", "agent", "run"],
    )
    runner = RecordingRunner()
    scheduler = Scheduler(
        AppConfig(),
        paths=_paths(tmp_path / "paths"),
        home=tmp_path / "home",
        runner=runner,
        platform="darwin",
    )

    status = scheduler.rebind()

    assert status.installed is False
    assert status.enabled is False
    assert runner.commands == []


class FakeSource:
    def __init__(self, source_key: str, by_query: dict[object, tuple[ScanItem, ...]]):
        self.source_key = source_key
        self.concurrency_key = source_key
        self.by_query = by_query
        self.calls: list[str | None] = []
        self.hydration_calls: list[str] = []

    def scan(self, query: str | None = None) -> SourceResult:
        self.calls.append(query)
        return SourceResult(
            source=self.source_key,
            status=ScanStatus.SUCCEEDED,
            items=self.by_query.get(query, ()),
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        self.hydration_calls.append(item.source_id)
        return item


class OrchestrationScanner:
    calls: list[dict[str, Any]] = []

    def __init__(self, database: Database, **kwargs):
        self.database = database
        self.kwargs = kwargs

    def scan(self, sources, **kwargs):
        results = [source.scan(kwargs.get("query")) for source in sources]
        self.calls.append(
            {
                "kwargs": kwargs,
                "results": results,
                "constructor": self.kwargs,
            }
        )
        now = datetime.now(timezone.utc)
        with self.database.session() as session:
            run = ScanRun(
                status=AgentRunStatus.SUCCEEDED,
                query=kwargs.get("query"),
                requested_sources=list(kwargs.get("requested_sources", [])),
                source_results={},
                started_at=now,
                finished_at=now,
                discovered_count=sum(len(result.items) for result in results),
                scan_kind=kwargs.get("scan_kind", "manual"),
                profile_id=kwargs.get("profile_id"),
            )
            session.add(run)
            session.flush()
            return run


def _item(source: str, source_id: str, title: str) -> ScanItem:
    return ScanItem(
        source=source,
        source_id=source_id,
        company="Example",
        title=title,
        url=f"https://example.com/{source_id}",
        description=f"{title} advises product teams.",
    )


def test_focused_agent_unions_workday_queries_and_locally_filters_other_sources(
    monkeypatch: pytest.MonkeyPatch, database: Database
) -> None:
    profile = create_profile(
        database,
        {
            "name": "Enabled legal focus",
            "sources": ["all"],
            "queries": ["legal AI", "privacy"],
            "role_filters": ["counsel"],
            "enabled": True,
        },
    )
    workday = FakeSource(
        "workday:example:jobs",
        {
            "legal AI": (_item("workday:example:jobs", "w1", "Legal AI Counsel"),),
            "privacy": (
                _item("workday:example:jobs", "w1", "Legal AI Counsel"),
                _item("workday:example:jobs", "w2", "Privacy Counsel"),
            ),
        },
    )
    greenhouse = FakeSource(
        "greenhouse:example",
        {
            None: (
                _item("greenhouse:example", "g1", "Legal AI Counsel"),
                _item("greenhouse:example", "g2", "Software Engineer"),
            )
        },
    )
    build_calls: list[bool] = []

    def build_sources(*_args, inventory=False, **_kwargs):
        build_calls.append(inventory)
        return [workday, greenhouse]

    OrchestrationScanner.calls = []
    monkeypatch.setattr("jobby.agent.Scanner", OrchestrationScanner)
    monkeypatch.setattr("jobby.agent.build_configured_sources", build_sources)
    monkeypatch.setattr(
        "jobby.agent.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail("focused deterministic scan used OpenAI"),
    )

    run = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=NoSecrets(),
    ).run(mode=AgentRunMode.FOCUSED, include_web=False)

    assert run.status is AgentRunStatus.SUCCEEDED
    assert run.summary["run_mode"] == "focused"
    assert run.summary["enabled_profile_ids"] == [profile.id]
    assert build_calls == [False]
    assert workday.calls == ["legal AI", "privacy"]
    assert greenhouse.calls == [None]
    assert workday.hydration_calls == ["w1", "w2"]
    assert greenhouse.hydration_calls == ["g1"]
    call = OrchestrationScanner.calls[0]
    assert call["kwargs"]["scan_kind"] == "focused"
    assert call["kwargs"]["profile_id"] == profile.id
    results = call["results"]
    assert [item.source_id for item in results[0].items] == ["w1", "w2"]
    assert [item.source_id for item in results[1].items] == ["g1"]
    constructor = call["constructor"]
    assert constructor["record_cap"] == AppConfig().source_record_cap
    assert constructor["retry_attempts"] == AppConfig().source_retry_attempts
    assert constructor["source_deadline_seconds"] == AppConfig().source_deadline_seconds
    assert constructor["max_response_bytes"] == AppConfig().source_max_response_bytes


def test_focused_hydration_uses_bounded_concurrency(database: Database) -> None:
    profile = create_profile(
        database,
        {
            "name": "Concurrent hydration",
            "sources": ["greenhouse:example"],
            "queries": ["legal"],
            "enabled": True,
        },
    )

    class ConcurrentSource(FakeSource):
        def __init__(self) -> None:
            super().__init__(
                "greenhouse:example",
                {
                    None: (
                        _item("greenhouse:example", "one", "Legal Counsel"),
                        _item("greenhouse:example", "two", "Legal Policy Analyst"),
                    )
                },
            )
            self.active = 0
            self.max_active = 0
            self.lock = threading.Lock()

        def hydrate(self, item: ScanItem) -> ScanItem:
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            time.sleep(0.05)
            with self.lock:
                self.active -= 1
            return super().hydrate(item)

    source = ConcurrentSource()
    wrapped = _FocusedSource(source, [profile])

    run = Scanner(database, max_workers_per_source=2).scan(
        [wrapped], scan_kind="focused"
    )

    assert run.status is AgentRunStatus.SUCCEEDED
    assert source.max_active == 2
    assert sorted(source.hydration_calls) == ["one", "two"]


def test_focused_query_pack_preserves_sibling_results_when_one_query_crashes(
    database: Database,
) -> None:
    profile = create_profile(
        database,
        {
            "name": "Partial focused query pack",
            "sources": ["workday:example:jobs"],
            "queries": ["legal", "privacy"],
            "role_filters": ["counsel"],
            "enabled": True,
        },
    )

    class OneQueryFails(FakeSource):
        def scan(self, query: str | None = None) -> SourceResult:
            if query == "privacy":
                raise RuntimeError("one upstream query failed")
            return super().scan(query)

    source = OneQueryFails(
        "workday:example:jobs",
        {"legal": (_item("workday:example:jobs", "one", "Legal Counsel"),)},
    )

    result = _FocusedSource(source, [profile]).scan()

    assert result.status is ScanStatus.PARTIAL
    assert [item.source_id for item in result.items] == ["one"]
    assert result.metadata["upstream_calls"] == 2
    assert len(result.errors) == 1


def test_inventory_hydrates_when_source_metadata_snapshot_changes(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    initial = _item(source_key, "one", "Legal AI Counsel")
    initial = ScanItem(
        source=initial.source,
        source_id=initial.source_id,
        company=initial.company,
        title=initial.title,
        url=initial.url,
        description=initial.description,
        metadata={"source_updated_at": "2026-07-13T10:00:00Z"},
    )
    source = FakeSource(source_key, {None: (initial,)})
    Scanner(database).scan([source])
    source.hydration_calls.clear()
    changed_metadata = ScanItem(
        source=initial.source,
        source_id=initial.source_id,
        company=initial.company,
        title=initial.title,
        url=initial.url,
        description=initial.description,
        metadata={"source_updated_at": "2026-07-14T10:00:00Z"},
    )
    source.by_query[None] = (changed_metadata,)

    Scanner(database).scan([_InventorySource(source, database)], scan_kind="inventory")

    assert source.hydration_calls == ["one"]
    with database.session() as session:
        assert session.scalar(select(func.count(SourceObservation.id))) == 2


def test_inventory_agent_uses_inventory_source_limits_and_never_schedules_web(
    monkeypatch: pytest.MonkeyPatch, database: Database
) -> None:
    source = FakeSource("greenhouse:example", {None: ()})
    build_calls: list[bool] = []

    def build_sources(*_args, inventory=False, **_kwargs):
        build_calls.append(inventory)
        return [source]

    OrchestrationScanner.calls = []
    monkeypatch.setattr("jobby.agent.Scanner", OrchestrationScanner)
    monkeypatch.setattr("jobby.agent.build_configured_sources", build_sources)
    monkeypatch.setattr(
        "jobby.agent.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail("inventory must not construct OpenAI"),
    )

    run = DailyAgent(
        database,
        AppConfig(scheduled_web_enabled=True, notifications_enabled=False),
        secrets=NoSecrets(),
    ).run(mode="inventory")

    assert run.status is AgentRunStatus.SUCCEEDED
    assert run.summary["run_mode"] == "inventory"
    assert run.summary["web_requested"] is False
    assert build_calls == [True]
    assert source.calls == [None]
    assert OrchestrationScanner.calls[0]["kwargs"]["scan_kind"] == "inventory"


def test_inventory_is_metadata_only_until_change_or_shortlist(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    source = FakeSource(
        source_key,
        {None: (_item(source_key, "one", "Legal AI Counsel"),)},
    )
    Scanner(database).scan([source])
    source.hydration_calls.clear()

    Scanner(database).scan([_InventorySource(source, database)], scan_kind="inventory")
    assert source.hydration_calls == []
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "one"))
        assert job is not None
        original_description = job.description
        assert session.scalar(select(func.count(SourceObservation.id))) == 1
        job.status = JobStatus.SAVED

    Scanner(database).scan([_InventorySource(source, database)], scan_kind="inventory")
    assert source.hydration_calls == ["one"]
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "one"))
        assert job is not None and job.description == original_description
        assert session.scalar(select(func.count(SourceObservation.id))) == 1
        job.status = JobStatus.DISCOVERED

    source.hydration_calls.clear()
    changed = _item(source_key, "one", "Legal AI Counsel")
    changed = ScanItem(
        source=changed.source,
        source_id=changed.source_id,
        company=changed.company,
        title=changed.title,
        url=changed.url,
        location=changed.location,
        description="Materially changed legal AI and patent policy duties.",
    )
    source.by_query[None] = (changed,)
    Scanner(database).scan([_InventorySource(source, database)], scan_kind="inventory")
    assert source.hydration_calls == ["one"]
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "one"))
        assert job is not None and job.description == changed.description
        assert session.scalar(select(func.count(SourceObservation.id))) == 2


def test_focused_schedule_with_no_enabled_profiles_is_safe_noop(
    monkeypatch: pytest.MonkeyPatch, database: Database
) -> None:
    monkeypatch.setenv("JOBBY_AGENT_MODE", "focused")
    monkeypatch.setattr(
        "jobby.agent.build_configured_sources",
        lambda *_args, **_kwargs: pytest.fail("disabled profiles must not scan"),
    )

    run = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=NoSecrets(),
    ).run(include_web=False)

    assert run.status is AgentRunStatus.SUCCEEDED
    assert run.summary["run_mode"] == "focused"
    assert run.summary["enabled_profile_ids"] == []
    with database.session() as session:
        scan = session.get(ScanRun, run.scan_run_id)
        assert scan is not None
        assert scan.scan_kind == "focused"
        assert scan.discovered_count == 0


def test_invalid_scheduled_mode_fails_before_agent_history_write(
    monkeypatch: pytest.MonkeyPatch, database: Database
) -> None:
    monkeypatch.setenv("JOBBY_AGENT_MODE", "surprise")
    with pytest.raises(ValueError, match="focused, inventory"):
        DailyAgent(database, AppConfig(), secrets=NoSecrets()).run()
    with database.session() as session:
        assert session.scalar(select(ScanRun.id)) is None
