from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from jobby.config import AppConfig
from jobby.db import Database
from jobby.discovery_service import manual_source_options, run_discovery_scan
from jobby.enums import AgentRunStatus
from jobby.models import SourceConfig


class MappingSecrets:
    def __init__(self, values: dict[str, str] | None = None):
        self.values = values or {}

    def get(self, name: str) -> str | None:
        return self.values.get(name)


@dataclass
class FakeSource:
    source_key: str


class RecordingScanner:
    calls: list[dict[str, object]] = []

    def __init__(self, database, *, ranking_profile):
        self.database = database
        self.ranking_profile = ranking_profile

    def scan(self, sources, *, query=None, requested_sources=None):
        self.calls.append(
            {
                "sources": list(sources),
                "query": query,
                "requested_sources": list(requested_sources or []),
            }
        )
        return SimpleNamespace(
            id="scan",
            status=AgentRunStatus.SUCCEEDED,
            discovered_count=0,
            source_results={},
            error_summary=None,
        )


@pytest.fixture
def database(tmp_path) -> Database:
    value = Database(tmp_path / "jobby.sqlite3")
    value.initialize()
    yield value
    value.dispose()


def test_deterministic_all_scan_never_constructs_openai(
    database: Database, monkeypatch
) -> None:
    RecordingScanner.calls = []
    monkeypatch.setattr("jobby.discovery_service.Scanner", RecordingScanner)
    monkeypatch.setattr(
        "jobby.discovery_service.ranking_profile_from_database",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "jobby.discovery_service.build_configured_sources",
        lambda *_args, **_kwargs: [FakeSource("greenhouse:alpha")],
    )
    monkeypatch.setattr(
        "jobby.discovery_service.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail("OpenAI must not be constructed"),
    )

    run = run_discovery_scan(
        database,
        AppConfig(),
        secrets=MappingSecrets(),
        source_selector="all",
        query="legal technology",
    )

    assert run.status is AgentRunStatus.SUCCEEDED
    assert RecordingScanner.calls == [
        {
            "sources": [FakeSource("greenhouse:alpha")],
            "query": "legal technology",
            "requested_sources": ["greenhouse:alpha"],
        }
    ]


def test_manual_ats_client_ignores_proxy_environment_and_rejects_redirects(
    database: Database, monkeypatch
) -> None:
    captured: dict[str, object] = {}

    class NoNetworkClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    RecordingScanner.calls = []
    monkeypatch.setattr("jobby.discovery_service.httpx.Client", NoNetworkClient)
    monkeypatch.setattr("jobby.discovery_service.Scanner", RecordingScanner)
    monkeypatch.setattr(
        "jobby.discovery_service.ranking_profile_from_database",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "jobby.discovery_service.build_configured_sources",
        lambda *_args, **_kwargs: [FakeSource("greenhouse:alpha")],
    )

    run_discovery_scan(database, AppConfig(), source_selector="all")

    assert captured["trust_env"] is False
    assert captured["follow_redirects"] is False


def test_paid_web_requires_service_permission_and_nonblank_query(
    database: Database, monkeypatch
) -> None:
    monkeypatch.setattr(
        "jobby.discovery_service.Scanner",
        lambda *_args, **_kwargs: pytest.fail("scanner must not be constructed"),
    )
    with pytest.raises(PermissionError, match="paid OpenAI"):
        run_discovery_scan(
            database,
            AppConfig(),
            secrets=MappingSecrets(),
            source_selector="web",
            query="roles",
            allow_paid_web=False,
        )

    # Scanner construction occurs after local validation for ordinary requests,
    # but a blank paid query remains rejected without a provider request.
    monkeypatch.setattr("jobby.discovery_service.Scanner", RecordingScanner)
    monkeypatch.setattr(
        "jobby.discovery_service.ranking_profile_from_database",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "jobby.discovery_service.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail("provider must not be constructed"),
    )
    with pytest.raises(ValueError, match="query is required"):
        run_discovery_scan(
            database,
            AppConfig(),
            secrets=MappingSecrets(),
            source_selector="web",
            query="  ",
            allow_paid_web=True,
        )


def test_usajobs_missing_email_fails_before_network(database: Database) -> None:
    with pytest.raises(ValueError, match="account email is missing"):
        run_discovery_scan(
            database,
            AppConfig(),
            secrets=MappingSecrets({"usajobs_api_key": "configured"}),
            source_selector="usajobs",
        )


def test_individual_enabled_portals_are_selectable(
    database: Database, monkeypatch
) -> None:
    with database.session() as session:
        enabled = SourceConfig(
            provider="static_http",
            name="Acme Careers",
            enabled=True,
            config_json={"careers_url": "https://careers.example.test/jobs"},
        )
        disabled = SourceConfig(
            provider="static_http",
            name="Disabled Careers",
            enabled=False,
            config_json={"careers_url": "https://disabled.example.test/jobs"},
        )
        session.add_all([enabled, disabled])
        session.flush()
        enabled_id = enabled.id

    options = manual_source_options(database, AppConfig())
    assert ("Greenhouse — Anthropic", "greenhouse:anthropic") in options
    assert ("Portal — Acme Careers", f"portal-id:{enabled_id}") in options
    assert all("Disabled" not in label for label, _value in options)

    RecordingScanner.calls = []
    monkeypatch.setattr("jobby.discovery_service.Scanner", RecordingScanner)
    monkeypatch.setattr(
        "jobby.discovery_service.ranking_profile_from_database",
        lambda *_args: object(),
    )
    run_discovery_scan(
        database,
        AppConfig(),
        secrets=MappingSecrets(),
        source_selector=f"portal-id:{enabled_id}",
        query="counsel",
    )
    assert len(RecordingScanner.calls) == 1
    assert RecordingScanner.calls[0]["query"] == "counsel"
    assert RecordingScanner.calls[0]["requested_sources"] == ["portal:acme-careers"]
