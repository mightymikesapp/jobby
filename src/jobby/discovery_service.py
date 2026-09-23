"""Shared manual discovery orchestration for the CLI and MCP facade."""

from __future__ import annotations

from collections.abc import Sequence
import re
import socket

import httpx
from sqlalchemy import select

from . import __version__
from .config import AppConfig, SecretStore
from .db import Database
from .models import ScanRun, SourceConfig
from .openai_provider import OpenAIProvider
from .ranking import ranking_profile_from_database
from .scanner import Scanner, build_configured_sources
from .sources.base import JobSource
from .sources.browser import (
    PinnedPublicHTTPTransport,
    PortalConfig,
    PublicPortalSource,
)


MAX_MANUAL_QUERY_CHARS = 2_000
SOURCE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("All configured ATS sources", "all"),
    ("Greenhouse", "greenhouse"),
    ("Lever", "lever"),
    ("Ashby", "ashby"),
    ("Workable", "workable"),
    ("Workday", "workday"),
    ("SmartRecruiters", "smartrecruiters"),
    ("iCIMS", "icims"),
    ("Taleo", "taleo"),
    ("USAJobs", "usajobs"),
    ("Eightfold", "eightfold"),
    ("Oracle HCM", "oracle_hcm"),
    ("Rippling", "rippling"),
    ("Paylocity", "paylocity"),
    ("freehire catalog", "freehire"),
)


def manual_source_options(
    database: Database, config: AppConfig | None = None
) -> list[tuple[str, str]]:
    """Return deterministic providers plus individually selectable public portals."""

    options = [("All configured ATS sources", "all")]
    if config is None:
        options.extend(SOURCE_OPTIONS[1:])
    else:
        configured = (
            ("Greenhouse", "greenhouse", config.sources.greenhouse),
            ("Lever", "lever", config.sources.lever),
            ("Ashby", "ashby", config.sources.ashby),
            ("Workable", "workable", config.sources.workable),
            ("Workday", "workday", config.sources.workday),
            (
                "SmartRecruiters",
                "smartrecruiters",
                config.sources.smartrecruiters,
            ),
            ("iCIMS", "icims", config.sources.icims),
            ("Taleo", "taleo", config.sources.taleo),
            ("Eightfold", "eightfold", config.sources.eightfold),
            ("Oracle HCM", "oracle_hcm", config.sources.oracle_hcm),
            ("Rippling", "rippling", config.sources.rippling),
            ("Paylocity", "paylocity", config.sources.paylocity),
            ("freehire catalog", "freehire", config.sources.freehire),
        )
        for provider_label, provider, accounts in configured:
            if not accounts:
                continue
            options.append((provider_label, provider))
            for key, value in sorted(accounts.items()):
                company = (
                    value.get("name", key)
                    if isinstance(value, dict)
                    else getattr(value, "name", value)
                )
                company = company or key
                options.append(
                    (f"{provider_label} — {company}", f"{provider}:{key.casefold()}")
                )
        if config.sources.usajobs_locations:
            options.append(("USAJobs — configured locations", "usajobs"))
    with database.session() as session:
        portals = list(
            session.scalars(
                select(SourceConfig)
                .where(
                    SourceConfig.enabled.is_(True),
                    SourceConfig.provider == "static_http",
                )
                .order_by(SourceConfig.name, SourceConfig.id)
                .limit(500)
            )
        )
    options.extend(
        (f"Portal — {portal.name}", f"portal-id:{portal.id}")
        for portal in portals
        if str(portal.config_json.get("careers_url") or "").strip()
    )
    return options


def run_discovery_scan(
    database: Database,
    config: AppConfig,
    *,
    secrets: SecretStore | None = None,
    source_selector: str = "all",
    query: str | None = None,
    allow_paid_web: bool = False,
) -> ScanRun:
    """Run one explicitly requested scan and return its persisted outcome.

    The default path contains only deterministic ATS/public-portal reads. OpenAI
    web search requires both the explicit ``web`` selector and
    ``allow_paid_web=True`` so a UI button can never incur cost accidentally.
    """

    selector = str(source_selector or "all").strip().casefold()
    if not selector or len(selector) > 500:
        raise ValueError("manual source selector is invalid")
    normalized_query = str(query or "").strip() or None
    if normalized_query and len(normalized_query) > MAX_MANUAL_QUERY_CHARS:
        raise ValueError(
            f"manual search query must be {MAX_MANUAL_QUERY_CHARS:,} characters or fewer"
        )
    secrets = secrets or SecretStore()
    if selector == "web":
        if not allow_paid_web:
            raise PermissionError(
                "paid OpenAI web search is not available from this manual scan action"
            )
        if not normalized_query:
            raise ValueError("--query is required for web discovery")
        if not config.openai_enabled:
            raise PermissionError("OpenAI web discovery is disabled in configuration")
        scanner = _scanner(database, config)
        return scanner.scan_web(
            OpenAIProvider(config, secret_store=secrets), normalized_query
        )

    if selector == "usajobs" and not all(
        (secrets.get("usajobs_api_key"), secrets.get("usajobs_email"))
    ):
        raise ValueError(
            "USAJobs API key or account email is missing; configure both credentials first"
        )
    scanner = _scanner(database, config)
    if selector == "portals" or selector.startswith(("portal:", "portal-id:")):
        sources = _portal_sources(database, selector)
        if not sources:
            raise ValueError("no matching imported portal configuration")
        return scanner.scan(
            sources,
            query=normalized_query,
            requested_sources=[source.source_key for source in sources],
        )
    with httpx.Client(
        transport=PinnedPublicHTTPTransport(
            socket.getaddrinfo,
            max_connections=10,
            max_keepalive_connections=5,
        ),
        timeout=httpx.Timeout(20, connect=10),
        follow_redirects=False,
        headers={"User-Agent": f"Jobby/{__version__} local personal job discovery"},
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        trust_env=False,
    ) as client:
        sources = build_configured_sources(
            config,
            client=client,
            secret_store=secrets,
            selector=selector,
        )
        if not sources:
            raise ValueError(f"no configured source matches {selector!r}")
        return scanner.scan(
            sources,
            query=normalized_query,
            requested_sources=[source.source_key for source in sources],
        )


def _scanner(database: Database, config: AppConfig) -> Scanner:
    scanner = Scanner(
        database,
        ranking_profile=ranking_profile_from_database(database, config),
    )
    # Assign after construction to retain compatibility with local test/custom
    # Scanner doubles that implement the original constructor. AppConfig has
    # already validated both bounds.
    scanner.max_workers = config.discovery_max_workers
    scanner.max_workers_per_source = config.discovery_max_workers_per_source
    scanner.retry_attempts = config.source_retry_attempts
    scanner.retry_after_max_seconds = config.source_retry_after_max_seconds
    scanner.record_cap = config.source_record_cap
    scanner.source_deadline_seconds = float(config.source_deadline_seconds)
    scanner.max_response_bytes = config.source_max_response_bytes
    scanner.anomaly_ratio = config.source_anomaly_ratio
    scanner.anomaly_window = config.source_anomaly_window
    return scanner


def _portal_sources(database: Database, selector: str) -> Sequence[JobSource]:
    portal_id = selector.split(":", 1)[1] if selector.startswith("portal-id:") else None
    target = (
        selector.split(":", 1)[1].casefold() if selector.startswith("portal:") else None
    )
    with database.session() as session:
        statement = select(SourceConfig).where(
            SourceConfig.enabled.is_(True),
            SourceConfig.provider == "static_http",
        )
        if portal_id:
            statement = statement.where(SourceConfig.id == portal_id)
        configs = list(session.scalars(statement.order_by(SourceConfig.name)))
    result: list[JobSource] = []
    for source_config in configs:
        if target and target not in {
            source_config.name.casefold(),
            _slug(source_config.name),
        }:
            continue
        url = str(source_config.config_json.get("careers_url") or "").strip()
        if url:
            result.append(
                PublicPortalSource(PortalConfig(name=source_config.name, url=url))
            )
    return result


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


__all__ = [
    "MAX_MANUAL_QUERY_CHARS",
    "SOURCE_OPTIONS",
    "manual_source_options",
    "run_discovery_scan",
]
