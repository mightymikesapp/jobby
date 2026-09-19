"""Strict URL-to-source resolution for supported public ATS boards.

Resolution is deliberately allowlist-based. It never follows redirects,
accepts credentials, downgrades HTTPS, or turns an arbitrary URL into a fetch.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import socket
from typing import Any, Callable, Literal
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

import httpx

from jobby.config import (
    AppConfig,
    ICIMSBoard,
    JobbyPaths,
    SmartRecruitersBoard,
    TaleoBoard,
    save_config,
)
from jobby.normalization import is_public_hostname
from jobby.sources.ats import ICIMSSource, SmartRecruitersSource, TaleoSource
from jobby.sources.base import JobSource, ScanStatus
from jobby.sources.browser import PinnedPublicHTTPTransport


ProviderName = Literal["smartrecruiters", "icims", "taleo"]
_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")


@dataclass(frozen=True, slots=True)
class ResolvedSource:
    provider: ProviderName
    key: str
    configuration: SmartRecruitersBoard | ICIMSBoard | TaleoBoard

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "key": self.key,
            "configuration": self.configuration.model_dump(mode="json"),
        }

    def source(self, client: httpx.Client) -> JobSource:
        if self.provider == "smartrecruiters":
            board = self.configuration
            assert isinstance(board, SmartRecruitersBoard)
            return SmartRecruitersSource(
                client,
                company_slug=board.company_slug,
                company=board.name,
                max_pages=1,
            )
        if self.provider == "icims":
            board = self.configuration
            assert isinstance(board, ICIMSBoard)
            return ICIMSSource(
                client, base_url=board.base_url, company=board.name, max_pages=1
            )
        board = self.configuration
        assert isinstance(board, TaleoBoard)
        return TaleoSource(
            client, search_url=board.search_url, company=board.name, max_pages=1
        )


def resolve_source_url(url: str) -> ResolvedSource:
    """Detect a supported provider and return normalized typed configuration."""

    raw = str(url or "").strip()
    if not raw or len(raw) > 8_000:
        raise ValueError("source URL is blank or too long")
    if any(ord(character) < 32 or ord(character) == 127 for character in raw):
        raise ValueError("source URL contains control characters")
    try:
        parsed = urlsplit(raw)
        explicit_port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ValueError("source URL is malformed") from exc
    hostname = (parsed.hostname or "").rstrip(".").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or not hostname
        or parsed.username
        or parsed.password
        or explicit_port is not None
        or parsed.fragment
        or not is_public_hostname(hostname)
    ):
        raise ValueError(
            "source URL must be credential-free public HTTPS without a port or fragment"
        )

    if hostname in {
        "jobs.smartrecruiters.com",
        "careers.smartrecruiters.com",
        "api.smartrecruiters.com",
    }:
        return _resolve_smartrecruiters(parsed)
    if hostname.endswith(".icims.com") and hostname != "icims.com":
        return _resolve_icims(parsed)
    if hostname.endswith(".tbe.taleo.net") and hostname != "tbe.taleo.net":
        return _resolve_taleo(parsed)
    raise ValueError(
        "source URL is not a supported SmartRecruiters, iCIMS, or Taleo board"
    )


def _resolve_smartrecruiters(parsed: Any) -> ResolvedSource:
    segments = [unquote(item) for item in parsed.path.split("/") if item]
    if parsed.query:
        raise ValueError("SmartRecruiters board URL must not contain query parameters")
    if parsed.hostname.casefold() == "api.smartrecruiters.com":
        if (
            len(segments) < 4
            or [item.casefold() for item in segments[:2]] != ["v1", "companies"]
            or segments[3].casefold() != "postings"
            or len(segments) != 4
        ):
            raise ValueError("SmartRecruiters API URL is not a company postings board")
        slug = segments[2]
    else:
        if len(segments) != 1:
            raise ValueError(
                "SmartRecruiters URL must identify exactly one company board"
            )
        slug = segments[0]
    if not _SLUG.fullmatch(slug):
        raise ValueError("SmartRecruiters company slug is invalid")
    name = re.sub(r"[-_]+", " ", slug).strip().title()
    key = slug.casefold()
    return ResolvedSource(
        "smartrecruiters",
        key,
        SmartRecruitersBoard(company_slug=slug, name=name),
    )


def _resolve_icims(parsed: Any) -> ResolvedSource:
    path = unquote(parsed.path).rstrip("/")
    if "%" in path or path not in {"", "/jobs", "/jobs/search"}:
        raise ValueError("iCIMS URL must be the tenant root or jobs search page")
    if parsed.query:
        pairs = parse_qsl(parsed.query, keep_blank_values=True)
        allowed = {"ss", "searchrelation", "searchkeyword"}
        if any(key.casefold() not in allowed for key, _value in pairs):
            raise ValueError("iCIMS URL contains unsupported query parameters")
    hostname = parsed.hostname.rstrip(".").casefold()
    label = hostname[: -len(".icims.com")]
    key = re.sub(r"[^a-z0-9._-]+", "-", label).strip("-._")
    if not _SLUG.fullmatch(key):
        raise ValueError("iCIMS tenant name is invalid")
    name = re.sub(r"^(?:careers?|jobs?)-", "", key, flags=re.I)
    name = re.sub(r"[-_]+", " ", name).strip().title() or key
    return ResolvedSource(
        "icims",
        key,
        ICIMSBoard(base_url=f"https://{hostname}", name=name),
    )


def _resolve_taleo(parsed: Any) -> ResolvedSource:
    path = unquote(parsed.path)
    if "%" in path or not path.casefold().rstrip("/").endswith(
        "/ats/careers/v2/searchresults"
    ):
        raise ValueError("Taleo URL is not a Business Edition v2 search page")
    try:
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise ValueError("Taleo search query is malformed") from exc
    query: dict[str, str] = {}
    for raw_key, raw_value in pairs:
        key = raw_key.casefold()
        if key in query:
            raise ValueError("Taleo URL contains duplicate query parameters")
        query[key] = raw_value.strip()
    if not query.get("org") or not query.get("cws"):
        raise ValueError("Taleo URL must contain nonblank org and cws parameters")
    org = query["org"]
    cws = query["cws"]
    if not _SLUG.fullmatch(org) or not _SLUG.fullmatch(cws):
        raise ValueError("Taleo org or cws parameter is invalid")
    hostname = parsed.hostname.rstrip(".").casefold()
    normalized = urlunsplit(
        (
            "https",
            hostname,
            "/" + parsed.path.lstrip("/"),
            urlencode((("org", org), ("cws", cws))),
            "",
        )
    )
    key_base = re.sub(r"[^a-z0-9._-]+", "-", org.casefold()).strip("-._")
    key = f"{key_base}-{cws.casefold()}"
    if len(key) > 200:
        raise ValueError("Taleo configuration key is too long")
    name = re.sub(r"[-_]+", " ", org).strip().title()
    return ResolvedSource("taleo", key, TaleoBoard(search_url=normalized, name=name))


def verify_public_dns(
    resolved: ResolvedSource,
    resolver: Callable[..., list[Any]] = socket.getaddrinfo,
) -> tuple[str, ...]:
    """Reject DNS failures and any private/non-global answer before a fetch."""

    configuration = resolved.configuration
    url = (
        f"https://api.smartrecruiters.com/v1/companies/{configuration.company_slug}/postings"
        if isinstance(configuration, SmartRecruitersBoard)
        else configuration.base_url
        if isinstance(configuration, ICIMSBoard)
        else configuration.search_url
    )
    hostname = urlsplit(url).hostname
    assert hostname is not None
    try:
        entries = resolver(hostname, 443, type=socket.SOCK_STREAM)
    except (OSError, ValueError) as exc:
        raise ValueError(f"source hostname did not resolve: {exc}") from exc
    addresses: list[str] = []
    for entry in entries:
        try:
            address = str(entry[4][0]).split("%", 1)[0]
        except (IndexError, TypeError) as exc:
            raise ValueError("DNS resolver returned a malformed address") from exc
        if not is_public_hostname(address):
            raise ValueError(
                "source hostname resolved to a private or unsafe destination"
            )
        if address not in addresses:
            addresses.append(address)
    if not addresses:
        raise ValueError("source hostname did not resolve")
    return tuple(addresses)


def structural_test_and_add(
    resolved: ResolvedSource,
    config: AppConfig,
    *,
    paths: JobbyPaths,
    resolver: Callable[..., list[Any]] = socket.getaddrinfo,
    client: httpx.Client | None = None,
) -> AppConfig:
    """Run one bounded board read, then atomically persist a new typed source."""

    sources = config.sources
    target = getattr(sources, resolved.provider)
    existing_keys = {str(key).casefold() for key in target}
    if resolved.key.casefold() in existing_keys:
        raise ValueError(
            f"source key already exists: {resolved.provider}:{resolved.key}"
        )
    verify_public_dns(resolved, resolver)

    owns_client = client is None
    if client is None:
        client = httpx.Client(
            transport=PinnedPublicHTTPTransport(resolver),
            timeout=httpx.Timeout(20, connect=10),
            follow_redirects=False,
            trust_env=False,
            headers={"User-Agent": "Jobby source resolver"},
        )
    try:
        source = resolved.source(client)
        source.configure_runtime(
            deadline_at=None,
            cancelled=lambda: False,
            max_response_bytes=2_000_000,
            hydration_workers=1,
            inventory_metadata_only=True,
        )
        result = source.scan()
    finally:
        if owns_client:
            client.close()
    if result.status is ScanStatus.FAILED:
        reason = result.errors[0].message if result.errors else "unknown failure"
        raise ValueError(f"source structural test failed: {reason}")
    # A one-page probe can be partial solely because more pages exist. Item
    # parse failures, malformed payloads, redirects, and hostile responses fail.
    hard_errors = [
        error
        for error in result.errors
        if error.code not in {"result_truncated", "page_read_error"}
    ]
    if hard_errors:
        raise ValueError(f"source structural test failed: {hard_errors[0].message}")

    updated_sources = sources.model_copy(deep=True)
    getattr(updated_sources, resolved.provider)[resolved.key] = resolved.configuration
    updated = config.model_copy(update={"sources": updated_sources})
    save_config(updated, paths)
    return updated


__all__ = [
    "ResolvedSource",
    "resolve_source_url",
    "structural_test_and_add",
    "verify_public_dns",
]
