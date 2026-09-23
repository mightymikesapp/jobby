"""Bounded same-site crawler for configured public career portals.

The crawler extends the single-page portal reader with four read-only discovery
paths, all through the same DNS-pinned, redirect-revalidated fetch:

* schema.org ``JobPosting`` JSON-LD that employers embed for search engines;
* pagination and listing links on the portal's own host;
* job-shaped URLs from the site's sitemaps;
* hand-off to a structured ATS adapter when a page embeds a known board.

The configured page is fetched as before because the user chose it. Every page
the crawler finds on its own must pass robots.txt, and requests are spaced by a
politeness delay. It never runs JavaScript, signs in, or bypasses a challenge.
"""

from __future__ import annotations

import hashlib
import heapq
import html
import json
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from datetime import time as dt_time
from itertools import islice
from typing import Any, cast
from urllib.parse import parse_qsl, unquote, urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from ..config import ICIMSBoard, SmartRecruitersBoard, TaleoBoard
from ..normalization import (
    NormalizedSalary,
    RemoteStatus,
    is_public_http_url,
    normalize_remote,
    normalize_salary,
    normalize_url,
)
from .ats import (
    AshbySource,
    GreenhouseSource,
    ICIMSSource,
    JibeSource,
    LeverSource,
    SmartRecruitersSource,
    TaleoSource,
    WorkableSource,
    WorkdaySource,
)
from .base import (
    JobSource,
    ScanItem,
    ScanStatus,
    SourceDeadlineExceeded,
    SourceError,
    SourceResult,
)
from .browser import (
    _USER_AGENT,
    JOB_PATH,
    LISTING_TITLE,
    PinnedPublicHTTPTransport,
    PortalConfig,
    PublicPortalSource,
    _is_restricted,
    _PortalFailure,
    _PublicAnchorParser,
)
from .resolver import resolve_source_url


_ROBOTS_MAX_BYTES = 500_000
_SITEMAP_MAX_BYTES = 2_000_000
_MAX_SITEMAPS = 4
_MAX_SITEMAP_LOCS = 50_000
_MAX_LD_BLOCKS = 50
_MAX_LD_BYTES = 1_000_000
_MAX_LD_NODES = 20_000
_MAX_CRAWL_DELAY = 10.0
_MAX_QUEUED = 5_000
_MAX_REPORTED = 20
_DENY_ALL = ("User-agent: *", "Disallow: /")
_DOCUMENT_ACCEPT = "text/plain, application/xml, text/xml;q=0.9, */*;q=0.8"

# Merge precedence when several paths observe the same canonical URL.
_RANK_ANCHOR = 0
_RANK_HEADING = 1
_RANK_JSON_LD = 2
_RANK_DELEGATE = 3

_PRIORITY_PAGINATION = 0
_PRIORITY_LISTING = 1
_PRIORITY_SITEMAP = 2

_SKIP_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".css",
    ".js", ".json", ".xml", ".zip", ".gz", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".mp4", ".mp3", ".mov", ".woff", ".woff2", ".ttf", ".rss",
    ".atom", ".ics",
)  # fmt: skip
_PAGINATION_KEYS = frozenset(
    {"page", "p", "pg", "paged", "pagenum", "page_num", "offset", "start", "from", "startrow"}
)  # fmt: skip
_PAGINATION_TEXT = re.compile(r"\d{1,3}|[›»>]+|next(?: page)?(?: ?[›»>]+)?", re.I)
_LISTING_SEGMENT = re.compile(
    r"(?:all-?)?(?:jobs?|careers?|openings?|positions?|opportunities|vacancies|"
    r"search(?:-results|-jobs)?|job-?search|search-?jobs|departments?|teams?|"
    r"locations?|join-?us|work-?with-?us|open-?roles|roles|current-?openings)",
    re.I,
)
_LISTING_TEXT = re.compile(
    r"(?:more|more jobs|load more|show more|view all(?: jobs| openings| positions| roles)?|"
    r"see all(?: jobs| openings| positions| roles)?|all jobs|search jobs|browse jobs|"
    r"open positions|open roles|current openings|job openings|careers?|jobs)",
    re.I,
)
_DETAIL_PATH = JOB_PATH
_JOBISH = re.compile(r"job|career|position|opening|vacanc|posting", re.I)
_SITEMAP_LOC = re.compile(
    r"<loc>\s*(?:<!\[CDATA\[)?\s*([^<\]]{1,8000}?)\s*(?:\]\]>)?\s*</loc>", re.I
)
_JIBE_MARKER = re.compile(r"JIBE_[A-Z]|jibecdn|/jibe/")
_SITEMAP_INDEX = re.compile(r"<sitemapindex\b", re.I)
_BOARD_SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")

_BOARD_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "greenhouse",
        re.compile(
            r"(?<![\w.-])(?:boards|job-boards)\.greenhouse\.io/embed/job_board(?:/js)?"
            r"\?(?:[^\"'<>\s]{0,500}?[&;])?for=([A-Za-z0-9][A-Za-z0-9_-]{0,199})",
            re.I,
        ),
    ),
    (
        "greenhouse",
        re.compile(
            r"(?<![\w.-])(?:boards|job-boards)\.greenhouse\.io/(?!embed\b)"
            r"([A-Za-z0-9][A-Za-z0-9_-]{0,199})",
            re.I,
        ),
    ),
    (
        "greenhouse",
        re.compile(
            r"(?<![\w.-])boards-api\.greenhouse\.io/v1/boards/([A-Za-z0-9][A-Za-z0-9_-]{0,199})",
            re.I,
        ),
    ),
    (
        "lever",
        re.compile(
            r"(?<![\w.-])(?:jobs\.lever\.co|api\.lever\.co/v0/postings)/"
            r"([A-Za-z0-9][A-Za-z0-9._-]{0,199})",
            re.I,
        ),
    ),
    (
        "ashby",
        re.compile(
            r"(?<![\w.-])(?:jobs\.ashbyhq\.com|api\.ashbyhq\.com/posting-api/job-board)/"
            r"([A-Za-z0-9][A-Za-z0-9._%-]{0,199})",
            re.I,
        ),
    ),
    (
        "workable",
        re.compile(
            r"(?<![\w.-])apply\.workable\.com/(?:api/v1/widget/accounts/)?"
            r"(?!api\b|j\b)([A-Za-z0-9][A-Za-z0-9_-]{0,199})",
            re.I,
        ),
    ),
)
_WORKDAY_PATTERN = re.compile(
    r"(?<![\w.-])([A-Za-z0-9][A-Za-z0-9-]{0,62})\.(wd\d{1,3})\.myworkdayjobs\.com/"
    r"(?:wday/cxs/[A-Za-z0-9_-]{1,200}/|[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9][A-Za-z0-9_-]{0,199})"
)
_SMARTRECRUITERS_APP_PATHS = frozenset(
    {
        "my-applications",
        "oneclick-ui",
        "app",
        "sr-jobs",
        "job",
        "jobs",
        "search",
        "embed",
    }
)
_RESERVED_SLUGS = frozenset(
    {"api", "embed", "v0", "v1", "static", "assets", "js", "css", "images",
     "favicon.ico", "robots.txt", "sitemap.xml", "wday", "j", "jobs"}
)  # fmt: skip


@dataclass(frozen=True, slots=True)
class CrawlLimits:
    """Per-portal crawl budget. ``max_pages=1`` reads only the configured page."""

    max_pages: int = 20
    max_depth: int = 2
    request_delay_seconds: float = 1.0
    use_sitemaps: bool = True
    max_delegates: int = 3

    def __post_init__(self) -> None:
        for name, low, high in (
            ("max_pages", 1, 200),
            ("max_depth", 0, 5),
            ("max_delegates", 0, 10),
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"crawl {name} must be an integer")
            if not low <= value <= high:
                raise ValueError(f"crawl {name} must be between {low} and {high}")
        delay = self.request_delay_seconds
        if isinstance(delay, bool) or not isinstance(delay, (int, float)):
            raise ValueError("crawl delay must be a number of seconds")
        if not 0 <= delay <= 30:
            raise ValueError("crawl delay must be between zero and 30 seconds")


@dataclass(frozen=True, slots=True)
class _BoardCandidate:
    label: str
    build: Callable[[httpx.Client], JobSource]


@dataclass(slots=True)
class _CrawlState:
    query: str | None
    scope_host: str = ""
    items: dict[str, tuple[int, ScanItem]] = field(default_factory=dict)
    frontier: list[tuple[int, int, int, str, str]] = field(default_factory=list)
    queued: set[str] = field(default_factory=set)
    fetched: set[str] = field(default_factory=set)
    boards: dict[str, _BoardCandidate] = field(default_factory=dict)
    robots: RobotFileParser | None = None
    robots_status: str = "not_checked"
    crawl_delay: float = 0.0
    pages_fetched: int = 1
    robots_blocked: int = 0
    restricted_pages: int = 0
    anchors_considered: int = 0
    structured_postings: int = 0
    ld_json_errors: int = 0
    sitemaps_read: int = 0
    sitemap_candidates: int = 0
    page_errors: list[dict[str, str]] = field(default_factory=list)
    delegated: list[str] = field(default_factory=list)
    delegates_skipped: list[str] = field(default_factory=list)
    delegate_errors: list[dict[str, str]] = field(default_factory=list)
    stop_reason: str = "complete"
    crawl_fault: str = ""
    counter: int = 0

    def add(self, item: ScanItem, rank: int) -> None:
        canonical = normalize_url(item.url)
        if not canonical:
            return
        existing = self.items.get(canonical)
        if existing is None or rank > existing[0]:
            self.items[canonical] = (rank, item)

    def page_error(self, url: str, exc: Exception) -> None:
        if len(self.page_errors) >= _MAX_REPORTED:
            return
        if isinstance(exc, _PortalFailure):
            code = exc.code
        elif isinstance(exc, httpx.TimeoutException):
            code = "timeout"
        elif isinstance(exc, (httpx.HTTPError, OSError)):
            code = "network_error"
        else:
            code = "parse_error"
        self.page_errors.append({"url": url[:300], "code": code})


class _CrawlPageParser(_PublicAnchorParser):
    """Anchor parser that also keeps JSON-LD, rel=next, embeds, and the heading."""

    def __init__(self) -> None:
        super().__init__()
        self.ld_json: list[str] = []
        self.next_links: list[str] = []
        self.resource_urls: list[str] = []
        self.heading = ""
        self._ld_parts: list[str] | None = None
        self._ld_size = 0
        self._heading_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.casefold()
        if not self._ignored_depth:
            attributes = {key.casefold(): value or "" for key, value in attrs}
            if name == "script":
                kind = attributes.get("type", "").split(";", 1)[0].strip().casefold()
                if kind == "application/ld+json" and len(self.ld_json) < _MAX_LD_BLOCKS:
                    self._ld_parts = []
                    self._ld_size = 0
            if len(self.resource_urls) < 500:
                if name in {"script", "iframe"} and attributes.get("src"):
                    self.resource_urls.append(attributes["src"][:8_000])
                elif name == "form" and attributes.get("action"):
                    self.resource_urls.append(attributes["action"][:8_000])
            if (
                name in {"a", "link"}
                and "next" in attributes.get("rel", "").casefold().split()
                and attributes.get("href")
                and len(self.next_links) < 20
            ):
                self.next_links.append(attributes["href"][:8_000])
            if name == "h1" and not self.heading and self._heading_parts is None:
                self._heading_parts = []
        super().handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name == "script" and self._ld_parts is not None:
            self.ld_json.append("".join(self._ld_parts))
            self._ld_parts = None
        if name == "h1" and self._heading_parts is not None:
            self.heading = re.sub(r"\s+", " ", " ".join(self._heading_parts)).strip()
            self._heading_parts = None
        super().handle_endtag(tag)

    def handle_data(self, data: str) -> None:
        if self._ld_parts is not None and self._ld_size < _MAX_LD_BYTES + 1:
            self._ld_parts.append(data)
            self._ld_size += len(data)
        if self._heading_parts is not None and not self._ignored_depth:
            self._heading_parts.append(data[:1_000])
        super().handle_data(data)


class CareerCrawlerSource(PublicPortalSource):
    """Crawl one configured career site within a small, polite page budget."""

    name = "portal"

    def __init__(
        self,
        config: PortalConfig,
        *,
        limits: CrawlLimits | None = None,
        client_factory: Callable[[], httpx.Client] | None = None,
        host_resolver: Callable[..., list[Any]] | None = None,
        delegate_client_factory: Callable[[], httpx.Client] | None = None,
        skip_delegate_keys: Iterable[str] = (),
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            config, client_factory=client_factory, host_resolver=host_resolver
        )
        self.limits = limits or CrawlLimits()
        self._delegate_client_factory = (
            delegate_client_factory or self._default_delegate_client
        )
        self._skip_delegate_keys = frozenset(
            str(key).strip().casefold() for key in skip_delegate_keys
        )
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None

    def _default_delegate_client(self) -> httpx.Client:
        seconds = self.config.timeout_ms / 1_000
        return httpx.Client(
            transport=PinnedPublicHTTPTransport(self._host_resolver),
            timeout=httpx.Timeout(seconds, connect=min(seconds, 10.0)),
            headers={"User-Agent": _USER_AGENT},
            follow_redirects=False,
            trust_env=False,
        )

    def scan(self, query: str | None = None) -> SourceResult:
        started = datetime.now(timezone.utc)
        state = _CrawlState(query=query)
        self._last_request_at = None
        try:
            # The seed opens its own client only after DNS validation passes,
            # exactly like the single-page reader.
            html_text, final_url, http_status = self._fetch_document(self.config.url)
        except InterruptedError:
            raise
        except Exception as exc:
            return self._fetch_failure(started, exc)
        self._last_request_at = self._clock()
        try:
            page = _parse_page(html_text)
        except Exception as exc:
            return self._failed(started, "malformed_html", str(exc))
        if _is_restricted(page.text_parts):
            return self._failed(
                started,
                "restricted_page",
                "The public page requires authentication or a challenge; Jobby will not bypass it.",
            )
        state.scope_host = _host_key(urlsplit(final_url).hostname or "")
        state.queued.update(
            filter(None, (normalize_url(self.config.url), normalize_url(final_url)))
        )
        state.fetched.add(normalize_url(final_url))
        try:
            self._harvest(state, page, html_text, final_url, depth=0, kind="listing")
            if self.limits.max_pages > 1:
                with self._client_factory() as client:
                    self._load_robots(state, client, final_url)
                    if self.limits.use_sitemaps:
                        self._read_sitemaps(state, client, final_url)
                    self._crawl_frontier(state, client)
            self._run_delegates(state)
        except SourceDeadlineExceeded:
            state.stop_reason = "source_deadline"
        except InterruptedError:
            raise
        except Exception as exc:
            # Keep what the seed and earlier pages produced; report the fault.
            state.stop_reason = "crawl_error"
            state.crawl_fault = exc.__class__.__name__
            state.page_error(self.config.url, exc)
        return self._crawl_result(started, state, final_url, http_status)

    def hydrate(self, item: ScanItem) -> ScanItem:
        """Keep structured descriptions; fetch the page only for bare anchors."""

        if item.description and item.metadata.get("extraction") in {
            "json_ld_job_posting",
            "ats_delegate",
            "detail_page_heading",
        }:
            self.checkpoint()
            return replace(item, metadata={**dict(item.metadata), "hydrated": True})
        return super().hydrate(item)

    # -- page handling -----------------------------------------------------

    def _harvest(
        self,
        state: _CrawlState,
        page: _CrawlPageParser,
        html_text: str,
        page_url: str,
        *,
        depth: int,
        kind: str,
        follow: bool = True,
    ) -> None:
        postings, errors = _job_postings(page.ld_json)
        state.ld_json_errors += errors
        for posting in postings:
            item = self._posting_item(
                posting, page_url, state.query, allow_page_url=len(postings) == 1
            )
            if item is not None:
                state.structured_postings += 1
                state.add(item, _RANK_JSON_LD)

        anchor_items = self._anchor_items(page.anchors, page_url, state.query)
        for item in anchor_items:
            state.add(item, _RANK_ANCHOR)
        state.anchors_considered += len(page.anchors)

        if (
            kind == "detail"
            and not postings
            and 3 <= len(page.heading) <= 300
            and self._title_matches(page.heading, state.query)
            and not LISTING_TITLE.search(page.heading)
            and is_public_http_url(page_url)
        ):
            body = re.sub(r"\s+", " ", " ".join(page.text_parts)).strip()
            state.add(
                ScanItem(
                    source=self.source_key,
                    source_id=_url_id(page_url),
                    company=self.config.name,
                    title=page.heading,
                    url=page_url,
                    description=body[:500_000],
                    metadata={
                        "extraction": "detail_page_heading",
                        "portal_url": self.config.url,
                        "final_url": page_url,
                    },
                ),
                _RANK_HEADING,
            )

        for candidate in _detect_boards(
            html_text, page_url, page.anchors, page.resource_urls, self.config.name
        ):
            state.boards.setdefault(candidate.label.casefold(), candidate)

        if not follow or kind == "detail" or depth >= self.limits.max_depth:
            return
        if self.limits.max_pages <= 1:
            return
        matched = {normalize_url(item.url) for item in anchor_items}
        for href in page.next_links:
            self._enqueue(
                state, urljoin(page_url, href), depth + 1, _PRIORITY_PAGINATION
            )
        for anchor in page.anchors:
            url = urljoin(page_url, anchor["href"].strip())
            if normalize_url(url) in matched:
                continue
            priority = _link_priority(url, anchor["text"])
            if priority is not None:
                self._enqueue(state, url, depth + 1, priority)

    def _posting_item(
        self,
        posting: Mapping[str, Any],
        page_url: str,
        query: str | None,
        *,
        allow_page_url: bool,
    ) -> ScanItem | None:
        title = _html_to_text(
            _ld_text(posting.get("title")) or _ld_text(posting.get("name"))
        )
        if not 3 <= len(title) <= 300 or not self._title_matches(title, query):
            return None
        raw_url = _ld_text(posting.get("url"))
        if raw_url:
            url = urljoin(page_url, raw_url)
        elif allow_page_url:
            url = page_url
        else:
            return None
        if not normalize_url(url) or not is_public_http_url(url):
            return None
        location_type = _ld_text(posting.get("jobLocationType"))
        location = _ld_location(posting.get("jobLocation"))
        if not location and "telecommute" in location_type.casefold():
            requirement = _ld_location(posting.get("applicantLocationRequirements"))
            location = f"Remote ({requirement})" if requirement else "Remote"
        salary_text, salary = _ld_salary(posting.get("baseSalary"))
        organization = posting.get("hiringOrganization")
        employment = posting.get("employmentType")
        return ScanItem(
            source=self.source_key,
            source_id=_url_id(url),
            company=self.config.name,
            title=title,
            url=url,
            location=location,
            description=_html_to_text(_ld_text(posting.get("description")))[:500_000],
            salary_text=salary_text,
            salary=salary,
            remote=_ld_remote(location_type, location),
            posted_at=_ld_datetime(posting.get("datePosted")),
            deadline=_ld_datetime(posting.get("validThrough")),
            metadata={
                "extraction": "json_ld_job_posting",
                "portal_url": self.config.url,
                "final_url": page_url,
                "hiring_organization": _ld_text(organization)[:300],
                "employment_type": (
                    [_ld_text(value) for value in employment[:10]]
                    if isinstance(employment, list)
                    else _ld_text(employment)
                ),
            },
        )

    # -- crawl mechanics ---------------------------------------------------

    def _enqueue(
        self,
        state: _CrawlState,
        url: str,
        depth: int,
        priority: int,
        kind: str = "listing",
    ) -> None:
        canonical = normalize_url(url)
        if (
            not canonical
            or canonical in state.queued
            or len(state.queued) >= _MAX_QUEUED
            or not is_public_http_url(url)
            or not self._in_scope(state, url)
            or urlsplit(url).path.casefold().endswith(_SKIP_EXTENSIONS)
        ):
            return
        state.queued.add(canonical)
        state.counter += 1
        heapq.heappush(
            state.frontier, (priority, depth, state.counter, url.split("#", 1)[0], kind)
        )

    def _crawl_frontier(self, state: _CrawlState, client: httpx.Client) -> None:
        while state.frontier and state.pages_fetched < self.limits.max_pages:
            _priority, depth, _order, url, kind = heapq.heappop(state.frontier)
            self.checkpoint()
            if not self._robots_allow(state, url):
                state.robots_blocked += 1
                continue
            self._polite_wait(state)
            state.pages_fetched += 1
            try:
                html_text, final_url, _status = self._fetch_document(url, client=client)
            except (InterruptedError, SourceDeadlineExceeded):
                raise
            except Exception as exc:
                state.page_error(url, exc)
                continue
            canonical = normalize_url(final_url)
            if canonical in state.fetched:
                continue
            state.fetched.add(canonical)
            try:
                page = _parse_page(html_text)
            except Exception as exc:
                state.page_error(url, exc)
                continue
            if _is_restricted(page.text_parts):
                state.restricted_pages += 1
                continue
            try:
                self._harvest(
                    state,
                    page,
                    html_text,
                    final_url,
                    depth=depth,
                    kind=kind,
                    follow=self._in_scope(state, final_url),
                )
            except (InterruptedError, SourceDeadlineExceeded):
                raise
            except Exception as exc:
                state.page_error(final_url, exc)
        if state.frontier:
            state.stop_reason = "page_budget"

    def _load_robots(
        self, state: _CrawlState, client: httpx.Client, page_url: str
    ) -> None:
        parts = urlsplit(page_url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        parser = RobotFileParser()
        self._polite_wait(state)
        try:
            text, _final, _status = self._fetch_document(
                robots_url,
                content_types=None,
                max_bytes=_ROBOTS_MAX_BYTES,
                truncate=True,
                client=client,
                accept=_DOCUMENT_ACCEPT,
            )
        except (InterruptedError, SourceDeadlineExceeded):
            raise
        except _PortalFailure as exc:
            status = exc.http_status
            if status in {401, 403}:
                parser.parse(_DENY_ALL)
                state.robots_status = "forbidden"
            elif status is not None and 400 <= status < 500 and status != 429:
                parser.parse([])
                state.robots_status = "missing"
            else:
                parser.parse(_DENY_ALL)
                state.robots_status = "unavailable"
            state.robots = parser
            return
        except Exception:
            parser.parse(_DENY_ALL)
            state.robots = parser
            state.robots_status = "unavailable"
            return
        parser.parse(text.splitlines())
        state.robots = parser
        state.robots_status = "loaded"
        delay = parser.crawl_delay(_USER_AGENT)
        if delay is not None:
            try:
                state.crawl_delay = min(_MAX_CRAWL_DELAY, max(0.0, float(delay)))
            except (TypeError, ValueError):
                pass

    def _read_sitemaps(
        self, state: _CrawlState, client: httpx.Client, page_url: str
    ) -> None:
        parts = urlsplit(page_url)
        declared = [
            url
            for url in ((state.robots.site_maps() if state.robots else None) or [])
            if self._in_scope(state, url)
        ]
        explicit = bool(declared)
        pending = declared[:_MAX_SITEMAPS] or [
            f"{parts.scheme}://{parts.netloc}/sitemap.xml"
        ]
        seen: set[str] = set()
        candidates: list[str] = []
        while pending and state.sitemaps_read < _MAX_SITEMAPS:
            url = pending.pop(0)
            canonical = normalize_url(url)
            if (
                not canonical
                or canonical in seen
                or url.casefold().endswith(".gz")
                or not is_public_http_url(url)
                or not self._robots_allow(state, url)
            ):
                continue
            seen.add(canonical)
            self._polite_wait(state)
            state.sitemaps_read += 1
            try:
                text, _final, _status = self._fetch_document(
                    url,
                    content_types=None,
                    max_bytes=_SITEMAP_MAX_BYTES,
                    truncate=True,
                    client=client,
                    accept=_DOCUMENT_ACCEPT,
                )
            except (InterruptedError, SourceDeadlineExceeded):
                raise
            except Exception as exc:
                # A missing conventional sitemap is normal, not an error.
                if explicit or not (
                    isinstance(exc, _PortalFailure) and exc.http_status == 404
                ):
                    state.page_error(url, exc)
                continue
            locs = [
                html.unescape(match.group(1)).strip()
                for match in islice(_SITEMAP_LOC.finditer(text), _MAX_SITEMAP_LOCS)
            ]
            if _SITEMAP_INDEX.search(text[:5_000]):
                children = [loc for loc in locs if self._in_scope(state, loc)]
                children.sort(
                    key=lambda loc: 0 if _JOBISH.search(urlsplit(loc).path) else 1
                )
                pending.extend(children[:_MAX_SITEMAPS])
                continue
            for loc in locs:
                if len(candidates) >= self.limits.max_pages:
                    break
                path = urlsplit(loc).path
                if (
                    not self._in_scope(state, loc)
                    or not _DETAIL_PATH.search(path)
                    or path.casefold().endswith(_SKIP_EXTENSIONS)
                ):
                    continue
                words = re.sub(r"[^a-z0-9+#]+", " ", unquote(path).casefold())
                if self._title_matches(words, state.query):
                    candidates.append(loc)
        state.sitemap_candidates = len(candidates)
        for loc in candidates:
            self._enqueue(
                state, loc, self.limits.max_depth, _PRIORITY_SITEMAP, kind="detail"
            )

    def _run_delegates(self, state: _CrawlState) -> None:
        if not state.boards or self.limits.max_delegates == 0:
            return
        with self._delegate_client_factory() as client:
            for candidate in state.boards.values():
                if len(state.delegated) + len(state.delegate_errors) >= (
                    self.limits.max_delegates
                ):
                    break
                self.checkpoint()
                try:
                    source = candidate.build(client)
                except (TypeError, ValueError):
                    state.delegate_errors.append(
                        {"board": candidate.label, "code": "invalid_board"}
                    )
                    continue
                if source.source_key.casefold() in self._skip_delegate_keys:
                    state.delegates_skipped.append(candidate.label)
                    continue
                source.configure_runtime(
                    deadline_at=self._deadline_at,
                    cancelled=self._cancelled,
                    max_response_bytes=self.max_response_bytes,
                    hydration_workers=1,
                    inventory_metadata_only=self.inventory_metadata_only,
                )
                result = source.scan(state.query)
                if result.status is ScanStatus.FAILED:
                    state.delegate_errors.append(
                        {"board": candidate.label, "code": result.errors[0].code}
                    )
                    continue
                state.delegated.append(candidate.label)
                for item in result.items:
                    if not self._title_matches(item.title, state.query):
                        continue
                    state.add(
                        replace(
                            item,
                            source=self.source_key,
                            source_id=_url_id(item.url),
                            company=self.config.name,
                            metadata={
                                **dict(item.metadata),
                                "extraction": "ats_delegate",
                                "portal_url": self.config.url,
                                "delegate_source": source.source_key,
                                "delegate_source_id": item.source_id,
                            },
                        ),
                        _RANK_DELEGATE,
                    )

    def _crawl_result(
        self,
        started: datetime,
        state: _CrawlState,
        final_url: str,
        http_status: int,
    ) -> SourceResult:
        items = tuple(item for _rank, item in state.items.values())
        errors: tuple[SourceError, ...] = ()
        status = ScanStatus.SUCCEEDED
        if state.stop_reason in {"source_deadline", "crawl_error"}:
            errors = (
                SourceError(
                    code=state.stop_reason,
                    message=(
                        "The portal crawl reached its source deadline; results are incomplete."
                        if state.stop_reason == "source_deadline"
                        else "The portal crawl stopped on an unexpected "
                        f"{state.crawl_fault or 'error'}; results are incomplete."
                    ),
                ),
            )
            status = ScanStatus.PARTIAL if items else ScanStatus.FAILED
        return SourceResult(
            source=self.source_key,
            status=status,
            items=items,
            errors=errors,
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            metadata={
                "portal_url": self.config.url,
                "final_url": final_url,
                "http_status": http_status,
                "extraction": "crawl",
                "stop_reason": state.stop_reason,
                "pages_fetched": state.pages_fetched,
                "pages_queued": len(state.frontier),
                "robots": state.robots_status,
                "robots_blocked": state.robots_blocked,
                "restricted_pages": state.restricted_pages,
                "anchors_considered": state.anchors_considered,
                "structured_postings": state.structured_postings,
                "ld_json_errors": state.ld_json_errors,
                "sitemaps_read": state.sitemaps_read,
                "sitemap_candidates": state.sitemap_candidates,
                "page_errors": state.page_errors,
                "detected_boards": list(state.boards)[:_MAX_REPORTED],
                "delegated_boards": state.delegated,
                "skipped_boards": state.delegates_skipped[:_MAX_REPORTED],
                "delegate_errors": state.delegate_errors,
            },
        )

    def _in_scope(self, state: _CrawlState, url: str) -> bool:
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        return (
            parts.scheme.casefold() in {"http", "https"}
            and bool(state.scope_host)
            and _host_key(parts.hostname or "") == state.scope_host
        )

    def _robots_allow(self, state: _CrawlState, url: str) -> bool:
        return state.robots is None or state.robots.can_fetch(_USER_AGENT, url)

    def _polite_wait(self, state: _CrawlState) -> None:
        delay = max(float(self.limits.request_delay_seconds), state.crawl_delay)
        if self._last_request_at is not None and delay > 0:
            wait_until = self._last_request_at + delay
            while (remaining := wait_until - self._clock()) > 0:
                self.checkpoint()
                self._sleep(min(remaining, 0.25))
        self.checkpoint()
        self._last_request_at = self._clock()


# -- parsing helpers -------------------------------------------------------


def _parse_page(html_text: str) -> _CrawlPageParser:
    parser = _CrawlPageParser()
    parser.feed(html_text)
    parser.close()
    return parser


def _host_key(hostname: str) -> str:
    host = hostname.rstrip(".").casefold()
    return host[4:] if host.startswith("www.") else host


def _url_id(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode()).hexdigest()[:24]


def _link_priority(url: str, text: str) -> int | None:
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    path = parts.path.casefold()
    if path.endswith(_SKIP_EXTENSIONS):
        return None
    label = re.sub(r"\s+", " ", text).strip()
    keys = {key.casefold() for key, _value in parse_qsl(parts.query)}
    if keys & _PAGINATION_KEYS or _PAGINATION_TEXT.fullmatch(label):
        return _PRIORITY_PAGINATION
    segments = [segment for segment in path.split("/") if segment]
    last = re.sub(r"\.(?:html?|aspx?|php|jsp)$", "", segments[-1]) if segments else ""
    if (
        (last and _LISTING_SEGMENT.fullmatch(last))
        or _LISTING_TEXT.fullmatch(label)
        or LISTING_TITLE.search(label)
    ):
        return _PRIORITY_LISTING
    return None


def _job_postings(blocks: Iterable[str]) -> tuple[list[Mapping[str, Any]], int]:
    postings: list[Mapping[str, Any]] = []
    errors = 0
    for raw in islice(blocks, _MAX_LD_BLOCKS):
        text = raw.strip()
        for prefix, suffix in (("<!--", "-->"), ("//<![CDATA[", "//]]>")):
            if text.startswith(prefix) and text.endswith(suffix):
                text = text[len(prefix) : -len(suffix)].strip()
        if not text or len(text) > _MAX_LD_BYTES:
            continue
        try:
            data = json.loads(text)
        except (ValueError, RecursionError):
            errors += 1
            continue
        stack: list[tuple[object, int]] = [(data, 0)]
        visited = 0
        while stack and visited < _MAX_LD_NODES and len(postings) < 500:
            node, depth = stack.pop()
            visited += 1
            if isinstance(node, dict):
                mapping = cast(dict[str, Any], node)
                if _is_job_posting(mapping):
                    postings.append(mapping)
                    continue
                children: Iterable[object] = list(mapping.values())[:200]
            elif isinstance(node, list):
                children = node[:1_000]
            else:
                continue
            if depth < 12:
                stack.extend(
                    (child, depth + 1)
                    for child in reversed(list(children))
                    if isinstance(child, (dict, list))
                )
    return postings, errors


def _is_job_posting(node: Mapping[str, Any]) -> bool:
    kinds = node.get("@type")
    values = kinds if isinstance(kinds, list) else [kinds]
    return any(
        isinstance(value, str)
        and re.split(r"[/:#]", value)[-1].casefold() == "jobposting"
        for value in values[:10]
    )


def _ld_text(value: object, depth: int = 0) -> str:
    if depth > 5 or value is None or isinstance(value, bool):
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Mapping):
        return _ld_text(value.get("name") or value.get("@value"), depth + 1)
    if isinstance(value, list):
        for entry in value[:20]:
            if text := _ld_text(entry, depth + 1):
                return text
    return ""


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    if "&lt;" in value or "&gt;" in value:
        value = html.unescape(value)
    if "<" not in value:
        return re.sub(r"\s+", " ", html.unescape(value)).strip()
    parser = _PublicAnchorParser()
    parser.feed(value)
    parser.close()
    return re.sub(r"\s+", " ", " ".join(parser.text_parts)).strip()


def _ld_location(value: object) -> str:
    places = value if isinstance(value, list) else [value]
    found: list[str] = []
    for place in places[:20]:
        text = ""
        if isinstance(place, str):
            text = place
        elif isinstance(place, Mapping):
            address = place.get("address", place)
            if isinstance(address, str):
                text = address
            elif isinstance(address, Mapping):
                parts = (
                    _ld_text(address.get(key))
                    for key in ("addressLocality", "addressRegion", "addressCountry")
                )
                text = ", ".join(part for part in parts if part)
            if not text:
                text = _ld_text(place.get("name"))
        text = re.sub(r"\s+", " ", text).strip()
        if text and text not in found:
            found.append(text)
    return "; ".join(found)[:500]


def _ld_remote(location_type: str, location: str) -> RemoteStatus:
    if "telecommute" in location_type.casefold():
        return RemoteStatus.REMOTE
    return normalize_remote(location_type or None, location=location)


def _ld_datetime(value: object) -> datetime | None:
    text = _ld_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.combine(date.fromisoformat(text[:10]), dt_time.min)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ld_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.replace(",", "").strip())
        except ValueError:
            return None
    else:
        return None
    return number if number == number and 0 < number < 1e12 else None


def _ld_salary(value: object) -> tuple[str, NormalizedSalary | None]:
    if isinstance(value, list):
        value = next((entry for entry in value if isinstance(entry, Mapping)), None)
    if not isinstance(value, Mapping):
        return "", None
    currency = _ld_text(value.get("currency"))
    amount = value.get("value")
    if isinstance(amount, Mapping):
        unit = _ld_text(amount.get("unitText"))
        low = _ld_number(amount.get("minValue", amount.get("value")))
        high = _ld_number(amount.get("maxValue"))
    else:
        unit = _ld_text(value.get("unitText"))
        low, high = _ld_number(amount), None
    if low is None and high is None:
        return "", None
    bounds = "-".join(f"{bound:,.0f}" for bound in (low, high) if bound is not None)
    text = " ".join(
        part
        for part in (currency, bounds, f"per {unit.casefold()}" if unit else "")
        if part
    )
    try:
        salary = normalize_salary(
            {"min": low, "max": high, "currency": currency, "interval": unit}
        )
    except (TypeError, ValueError, ArithmeticError):
        salary = None
    return text, salary


# -- ATS detection ---------------------------------------------------------


def _detect_boards(
    html_text: str,
    page_url: str,
    anchors: Iterable[Mapping[str, str]],
    resource_urls: Iterable[str],
    company: str,
) -> list[_BoardCandidate]:
    found: dict[str, _BoardCandidate] = {}
    haystack = f"{page_url}\n{html_text[:2_000_000]}"

    def add(label: str, build: Callable[[httpx.Client], JobSource]) -> None:
        if label.casefold() not in found and len(found) < _MAX_REPORTED:
            found[label.casefold()] = _BoardCandidate(label, build)

    for provider, pattern in _BOARD_PATTERNS:
        for match in islice(pattern.finditer(haystack), 50):
            slug = unquote(match.group(1)).rstrip(".")
            if slug.casefold() in _RESERVED_SLUGS or not _BOARD_SLUG.fullmatch(slug):
                continue
            add(f"{provider}:{slug}", _simple_builder(provider, slug, company))
    for match in islice(_WORKDAY_PATTERN.finditer(haystack), 50):
        tenant, wd, site = match.group(1), match.group(2), match.group(3)
        if site.casefold() in _RESERVED_SLUGS:
            continue
        add(
            f"workday:{tenant}:{site}",
            lambda client, tenant=tenant, wd=wd, site=site: WorkdaySource(
                client, tenant=tenant, site=site, wd=wd, company=company
            ),
        )

    # iCIMS career sites built on Jibe render client-side but expose /api/jobs.
    page = urlsplit(page_url)
    page_host = (page.hostname or "").casefold()
    is_jibe = False
    if (
        page.scheme.casefold() == "https"
        and page_host
        and not page_host.endswith(".icims.com")
        and _JIBE_MARKER.search(html_text[:2_000_000])
    ):
        is_jibe = True
        path = page.path
        jobs_path = path[: path.index("/jobs")] if "/jobs" in path else ""
        if not re.fullmatch(r"(?:/[A-Za-z0-9._-]{1,64}){0,4}", jobs_path):
            jobs_path = ""
        add(
            f"jibe:{page_host}",
            lambda client, host=page_host, jobs_path=jobs_path: JibeSource(
                client, base_url=f"https://{host}", jobs_path=jobs_path, company=company
            ),
        )

    urls = [anchor["href"] for anchor in anchors][:5_000] + list(resource_urls)
    for raw in urls:
        url = urljoin(page_url, raw.strip())
        board_url = _resolvable_board_url(url)
        if not board_url or (is_jibe and ".icims.com/" in board_url):
            # A Jibe site's iCIMS links only lead back to the same jobs.
            continue
        try:
            resolved = resolve_source_url(board_url)
        except ValueError:
            continue
        configuration = resolved.configuration
        if isinstance(configuration, SmartRecruitersBoard):
            slug = configuration.company_slug
            add(
                f"smartrecruiters:{resolved.key}",
                lambda client, slug=slug: SmartRecruitersSource(
                    client, company_slug=slug, company=company
                ),
            )
        elif isinstance(configuration, ICIMSBoard):
            base_url = configuration.base_url
            add(
                f"icims:{resolved.key}",
                lambda client, base_url=base_url: ICIMSSource(
                    client, base_url=base_url, company=company
                ),
            )
        elif isinstance(configuration, TaleoBoard):
            search_url = configuration.search_url
            add(
                f"taleo:{resolved.key}",
                lambda client, search_url=search_url: TaleoSource(
                    client, search_url=search_url, company=company
                ),
            )
    return list(found.values())


def _simple_builder(
    provider: str, slug: str, company: str
) -> Callable[[httpx.Client], JobSource]:
    if provider == "greenhouse":
        return lambda client: GreenhouseSource(
            client, board_token=slug, company=company
        )
    if provider == "lever":
        return lambda client: LeverSource(client, site=slug, company=company)
    if provider == "ashby":
        return lambda client: AshbySource(client, board=slug, company=company)
    return lambda client: WorkableSource(client, account=slug, company=company)


def _resolvable_board_url(url: str) -> str:
    """Reduce a SmartRecruiters/iCIMS/Taleo link to its board URL, if any."""

    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    host = (parts.hostname or "").rstrip(".").casefold()
    if parts.scheme.casefold() != "https" or not host:
        return ""
    if host in {"jobs.smartrecruiters.com", "careers.smartrecruiters.com"}:
        segments = [segment for segment in parts.path.split("/") if segment]
        if not segments or segments[0].casefold() in _SMARTRECRUITERS_APP_PATHS:
            return ""
        return f"https://{host}/{segments[0]}"
    if host.endswith(".icims.com") and host != "icims.com":
        # Asset CDNs and employee-only portals share the tenant host pattern.
        label = host[: -len(".icims.com")]
        if "." in label or re.match(r"(?:cdn\d*|internal|employees?)(?:[-.]|$)", label):
            return ""
        return f"https://{host}/jobs/search"
    if host.endswith(".tbe.taleo.net") and host != "tbe.taleo.net":
        return url.split("#", 1)[0]
    return ""


__all__ = ["CareerCrawlerSource", "CrawlLimits"]
