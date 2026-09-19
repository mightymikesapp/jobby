"""Static, SSRF-safe extraction for configured public career pages."""

from __future__ import annotations

import hashlib
import re
import socket
import ssl
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import urljoin, urlsplit

import httpcore
import httpx

from ..normalization import is_public_hostname, is_public_http_url, normalize_url
from .base import (
    TRANSIENT_HTTP_STATUSES,
    JobSource,
    ScanItem,
    ScanStatus,
    SourceDeadlineExceeded,
    SourceError,
    SourceResult,
    bounded_source_key,
)


DEFAULT_TITLE_TERMS = (
    "legal",
    "counsel",
    "patent",
    "policy",
    "copyright",
    "licensing",
    "governance",
    "compliance",
    "affairs",
    "clerk",
    "fellow",
    "analyst",
    "coordinator",
    "trademark",
    "regulatory",
    "trust",
    "safety",
)

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECTS = 5
_MAX_RESPONSE_BYTES = 2_000_000
_MAX_ANCHORS = 20_000
_USER_AGENT = "Jobby/0.1 (+local personal job discovery)"
SocketOption = (
    tuple[int, int, int]
    | tuple[int, int, bytes | bytearray]
    | tuple[int, int, None, int]
)


@dataclass(frozen=True, slots=True)
class PortalConfig:
    name: str
    url: str
    title_terms: tuple[str, ...] = DEFAULT_TITLE_TERMS
    timeout_ms: int = 20_000

    def __post_init__(self) -> None:
        name = re.sub(r"\s+", " ", str(self.name or "")).strip()
        url = str(self.url or "").strip()
        terms = tuple(
            term
            for raw in self.title_terms[:100]
            if (term := re.sub(r"\s+", " ", str(raw or "")).strip()[:100])
        )
        if not name:
            raise ValueError("portal name is required")
        if len(name) > 200:
            raise ValueError("portal name must be at most 200 characters")
        if len(url) > 8_000:
            raise ValueError("portal URL must be at most 8,000 characters")
        if isinstance(self.timeout_ms, bool) or not isinstance(self.timeout_ms, int):
            raise ValueError("portal timeout must be an integer number of milliseconds")
        if not 1_000 <= self.timeout_ms <= 120_000:
            raise ValueError("portal timeout must be between 1,000 and 120,000 ms")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "title_terms", terms or DEFAULT_TITLE_TERMS)


class _PinnedNetworkBackend(httpcore.SyncBackend):
    """Resolve and connect in one operation so DNS cannot rebind the target."""

    def __init__(self, resolver: Callable[..., list[Any]]) -> None:
        self._resolver = resolver

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.NetworkStream:
        addresses, error = _resolve_public_addresses(host, port, self._resolver)
        if error or not addresses:
            raise OSError(error or "public host did not resolve")
        last_error: Exception | None = None
        for address in addresses:
            try:
                return super().connect_tcp(
                    address,
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (
                Exception
            ) as exc:  # pragma: no cover - address fallback is OS-specific
                last_error = exc
        if last_error is not None:
            raise last_error
        raise OSError("public host did not resolve")  # pragma: no cover


class _PinnedHTTPTransport(httpx.HTTPTransport):
    def __init__(self, resolver: Callable[..., list[Any]]) -> None:
        super().__init__(trust_env=False, retries=0)
        cast(httpcore.ConnectionPool, self._pool).close()
        self._pool = httpcore.ConnectionPool(
            ssl_context=ssl.create_default_context(),
            max_connections=4,
            max_keepalive_connections=2,
            keepalive_expiry=5.0,
            http1=True,
            http2=False,
            retries=0,
            network_backend=_PinnedNetworkBackend(resolver),
        )


# Shared by fixed-host source resolvers so structural tests retain the same
# DNS-rebinding protection as imported static portals. This is intentionally a
# transport, not a generic fetch layer: each adapter still controls its exact
# legal endpoints, redirect policy, byte limit, and response validation.
PinnedPublicHTTPTransport = _PinnedHTTPTransport


class _PublicAnchorParser(HTMLParser):
    ignored_tags = frozenset({"script", "style", "svg", "template", "noscript"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[dict[str, str]] = []
        self.text_parts: list[str] = []
        self._href: str | None = None
        self._anchor_text: list[str] = []
        self._ignored_depth = 0
        self._text_length = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        if tag in self.ignored_tags:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if tag == "a" and len(self.anchors) < _MAX_ANCHORS:
            self._finish_anchor()
            attributes = dict(attrs)
            href = str(attributes.get("href") or "").strip()
            self._href = href[:8_000] if href else None
            self._anchor_text = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag in self.ignored_tags and self._ignored_depth:
            self._ignored_depth -= 1
        elif tag == "a" and not self._ignored_depth:
            self._finish_anchor()

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        if self._text_length < _MAX_RESPONSE_BYTES:
            part = data[: min(10_000, _MAX_RESPONSE_BYTES - self._text_length)]
            self.text_parts.append(part)
            self._text_length += len(part)
        if self._href is not None:
            self._anchor_text.append(data[:2_000])

    def close(self) -> None:
        super().close()
        self._finish_anchor()

    def _finish_anchor(self) -> None:
        if self._href and len(self.anchors) < _MAX_ANCHORS:
            self.anchors.append(
                {"href": self._href, "text": " ".join(self._anchor_text)}
            )
        self._href = None
        self._anchor_text = []


class PublicPortalSource(JobSource):
    """Read static links from public HTML without login or browser automation."""

    name = "portal"

    def __init__(
        self,
        config: PortalConfig,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        host_resolver: Callable[..., list[Any]] | None = None,
    ):
        self.config = config
        source_slug = (
            _slug(config.name)
            or hashlib.sha256(config.name.encode("utf-8")).hexdigest()[:12]
        )
        self.source_key = bounded_source_key("portal", source_slug)
        parsed = urlsplit(config.url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("portal URL must be public HTTP(S)")
        if parsed.username or parsed.password:
            raise ValueError("portal URL must not contain credentials")
        if not is_public_http_url(config.url):
            raise ValueError("portal URL must target a public HTTP(S) host")
        super().__init__(
            cast(httpx.Client, None),
            source_key=self.source_key,
            concurrency_key=parsed.hostname.casefold(),
        )
        self._host_resolver = host_resolver or socket.getaddrinfo
        self._client_factory = client_factory or self._default_client

    def _default_client(self) -> httpx.Client:
        seconds = self.config.timeout_ms / 1_000
        return httpx.Client(
            transport=_PinnedHTTPTransport(self._host_resolver),
            timeout=httpx.Timeout(seconds, connect=min(seconds, 10.0)),
            headers={
                "Accept": "text/html,application/xhtml+xml;q=0.9",
                "User-Agent": _USER_AGENT,
            },
            follow_redirects=False,
            trust_env=False,
        )

    def scan(self, query: str | None = None) -> SourceResult:
        started = datetime.now(timezone.utc)
        try:
            html_text, final_url, http_status = self._fetch_html()
        except InterruptedError:
            raise
        except _PortalFailure as exc:
            return self._failed(
                started,
                exc.code,
                exc.message,
                exc.http_status,
                retryable=exc.retryable,
                retry_after_seconds=exc.retry_after_seconds,
            )
        except httpx.TimeoutException:
            return self._failed(
                started,
                "timeout",
                "The public portal request timed out.",
                retryable=True,
            )
        except SourceDeadlineExceeded:
            return self._failed(
                started,
                "source_deadline",
                "The public portal source deadline expired.",
            )
        except httpx.HTTPError as exc:
            message = str(exc) or exc.__class__.__name__
            code = (
                "unsafe_target"
                if "unsafe resolved address" in message
                else "network_error"
            )
            return self._failed(started, code, message)
        except OSError as exc:
            message = str(exc) or exc.__class__.__name__
            code = (
                "unsafe_target"
                if "unsafe resolved address" in message
                else "network_error"
            )
            return self._failed(started, code, message)
        except Exception as exc:
            return self._failed(
                started,
                "parse_error",
                str(exc) or exc.__class__.__name__,
            )

        parser = _PublicAnchorParser()
        try:
            parser.feed(html_text)
            parser.close()
        except Exception as exc:
            return self._failed(started, "malformed_html", str(exc))
        body = re.sub(r"\s+", " ", " ".join(parser.text_parts))
        if re.search(
            r"\b(captcha|verify you are human|access denied|sign in to continue)\b",
            body,
            re.IGNORECASE,
        ):
            return self._failed(
                started,
                "restricted_page",
                "The public page requires authentication or a challenge; Jobby will not bypass it.",
            )

        terms = tuple(term.casefold() for term in self.config.title_terms)
        query_terms = tuple(
            re.findall(r"[\w+#.-]+", str(query or "")[:2_000].casefold())[:100]
        )
        seen: set[str] = set()
        items: list[ScanItem] = []
        for anchor in parser.anchors:
            title = re.sub(r"\s+", " ", anchor["text"]).strip()
            url = anchor["href"].strip()
            if len(title) < 3 or len(title) > 240 or not url:
                continue
            folded = title.casefold()
            if query_terms:
                if not all(term in folded for term in query_terms):
                    continue
            elif not any(term in folded for term in terms):
                continue
            launch_url = urljoin(final_url, url)
            canonical = normalize_url(launch_url)
            if not canonical or not is_public_http_url(canonical):
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            items.append(
                ScanItem(
                    source=self.source_key,
                    source_id=hashlib.sha256(canonical.encode()).hexdigest()[:24],
                    company=self.config.name,
                    title=title,
                    url=launch_url,
                    metadata={
                        "extraction": "static_html_anchor",
                        "portal_url": self.config.url,
                        "final_url": final_url,
                    },
                )
            )
        return SourceResult(
            source=self.source_key,
            status=ScanStatus.SUCCEEDED,
            items=tuple(items),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            metadata={
                "portal_url": self.config.url,
                "final_url": final_url,
                "http_status": http_status,
                "anchors_considered": len(parser.anchors),
                "extraction": "static_html",
            },
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        """Fetch one static public detail page without JavaScript or auth."""

        self.checkpoint()
        html_text, final_url, http_status = self._fetch_html(item.url)
        parser = _PublicAnchorParser()
        parser.feed(html_text)
        parser.close()
        body = re.sub(r"\s+", " ", " ".join(parser.text_parts)).strip()
        if re.search(
            r"\b(captcha|verify you are human|access denied|sign in to continue)\b",
            body,
            re.IGNORECASE,
        ):
            raise ValueError(
                "The detail page requires authentication or a challenge; Jobby will not bypass it."
            )
        self.checkpoint()
        return replace(
            item,
            description=body[:500_000] or item.description,
            metadata={
                **dict(item.metadata),
                "hydrated": True,
                "detail_final_url": final_url,
                "detail_http_status": http_status,
                "detail_extraction": "static_html_text",
            },
        )

    def _fetch_html(self, start_url: str | None = None) -> tuple[str, str, int]:
        current_url = start_url or self.config.url
        visited: set[str] = set()
        self.checkpoint()
        initial_addresses, initial_error = _resolve_public_target(
            current_url, self._host_resolver
        )
        if initial_error or not initial_addresses:
            code = (
                "unsafe_target"
                if initial_error and initial_error.startswith("unsafe")
                else "dns_error"
            )
            raise _PortalFailure(
                code,
                initial_error or "portal host did not resolve to a public address",
            )
        self.checkpoint()
        with self._client_factory() as client:
            for redirect_index in range(_MAX_REDIRECTS + 1):
                self.checkpoint()
                canonical = normalize_url(current_url)
                if not canonical or canonical in visited:
                    raise _PortalFailure(
                        "redirect_loop", "Portal redirect loop blocked."
                    )
                visited.add(canonical)
                addresses, error = _resolve_public_target(
                    current_url, self._host_resolver
                )
                if error or not addresses:
                    code = (
                        "unsafe_target"
                        if error and error.startswith("unsafe")
                        else "dns_error"
                    )
                    raise _PortalFailure(
                        code,
                        error or "portal host did not resolve to a public address",
                    )
                with client.stream(
                    "GET",
                    current_url,
                    timeout=self.request_timeout(self.config.timeout_ms / 1_000),
                ) as response:
                    if response.status_code in _REDIRECT_STATUSES:
                        location = response.headers.get("location", "").strip()
                        if not location:
                            raise _PortalFailure(
                                "invalid_redirect",
                                "Portal returned a redirect without a location.",
                                response.status_code,
                            )
                        if redirect_index >= _MAX_REDIRECTS:
                            raise _PortalFailure(
                                "redirect_limit",
                                "Portal exceeded the redirect limit.",
                                response.status_code,
                            )
                        destination = urljoin(current_url, location)
                        if not is_public_http_url(destination):
                            raise _PortalFailure(
                                "unsafe_redirect",
                                "The portal redirected to a non-public target; Jobby blocked it.",
                                response.status_code,
                            )
                        current_url = destination
                        continue
                    if response.status_code >= 400:
                        raise _PortalFailure(
                            "http_error",
                            f"HTTP {response.status_code}",
                            response.status_code,
                            retryable=response.status_code in TRANSIENT_HTTP_STATUSES,
                            retry_after_seconds=_retry_after_seconds(
                                response.headers.get("retry-after")
                            ),
                        )
                    content_type = response.headers.get("content-type", "").casefold()
                    if content_type and not (
                        "text/html" in content_type
                        or "application/xhtml+xml" in content_type
                    ):
                        raise _PortalFailure(
                            "unsupported_content",
                            "Portal response is not HTML.",
                            response.status_code,
                        )
                    content_length = response.headers.get("content-length")
                    if content_length:
                        try:
                            too_large = int(content_length) > min(
                                _MAX_RESPONSE_BYTES, self.max_response_bytes
                            )
                        except ValueError:
                            too_large = False
                        if too_large:
                            raise _PortalFailure(
                                "response_too_large",
                                "Portal HTML exceeds the 2 MB safety limit.",
                                response.status_code,
                            )
                    chunks: list[bytes] = []
                    size = 0
                    for chunk in response.iter_bytes():
                        self.checkpoint()
                        size += len(chunk)
                        if size > min(_MAX_RESPONSE_BYTES, self.max_response_bytes):
                            raise _PortalFailure(
                                "response_too_large",
                                "Portal HTML exceeds the 2 MB safety limit.",
                                response.status_code,
                            )
                        chunks.append(chunk)
                    encoding = response.encoding or "utf-8"
                    try:
                        body = b"".join(chunks).decode(encoding, errors="replace")
                    except LookupError:
                        body = b"".join(chunks).decode("utf-8", errors="replace")
                    return body, current_url, response.status_code
        raise _PortalFailure("network_error", "Portal request did not complete.")

    def _failed(
        self,
        started: datetime,
        code: str,
        message: str,
        http_status: int | None = None,
        *,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> SourceResult:
        return SourceResult(
            source=self.source_key,
            status=ScanStatus.FAILED,
            errors=(
                SourceError(
                    code=code,
                    message=message,
                    http_status=http_status,
                    retryable=retryable,
                    retry_after_seconds=retry_after_seconds,
                ),
            ),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
        )


class _PortalFailure(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        http_status: int | None = None,
        *,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            seconds = (target - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return min(120.0, max(0.0, seconds))


def _resolve_public_target(
    url: str, resolver: Callable[..., list[Any]]
) -> tuple[list[str], str | None]:
    try:
        parsed = urlsplit(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        return [], f"unsafe portal URL: {exc}"
    if not parsed.hostname or not is_public_http_url(url):
        return [], "unsafe portal URL blocked"
    return _resolve_public_addresses(parsed.hostname, port, resolver)


def _resolve_public_addresses(
    hostname: str,
    port: int,
    resolver: Callable[..., list[Any]],
) -> tuple[list[str], str | None]:
    if not is_public_hostname(hostname):
        return [], f"unsafe portal host blocked: {hostname}"
    try:
        entries = resolver(hostname, port, type=socket.SOCK_STREAM)
    except (OSError, ValueError) as exc:
        return [], f"DNS resolution failed for portal host: {exc}"
    if not entries:
        return [], "DNS resolution returned no addresses for portal host"
    addresses: list[str] = []
    for entry in entries:
        try:
            address = str(entry[4][0]).split("%", 1)[0]
        except (IndexError, TypeError):
            return [], "DNS resolver returned a malformed address"
        if not is_public_hostname(address):
            return [], f"unsafe resolved address blocked for portal host: {address}"
        if address not in addresses:
            addresses.append(address)
    return addresses, None


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


__all__ = ["PinnedPublicHTTPTransport", "PortalConfig", "PublicPortalSource"]
