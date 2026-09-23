"""Public ATS and government job source adapters.

All adapters use a caller-supplied :class:`httpx.Client`.  They only call
documented or publicly used listing endpoints; they do not automate login,
solve CAPTCHAs, or attempt to evade access controls.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
from html import unescape as html_unescape
from html.parser import HTMLParser
import json
import re
from typing import Any
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)

import httpx

from jobby.normalization import is_public_http_url, normalize_remote, normalize_salary
from jobby.sources.base import (
    ScanItem,
    SourceError,
    StructuredJobSource,
    bounded_source_key,
    source_error_from_exception,
)


_MAX_RESPONSE_BYTES = 25 * 1024 * 1024


def _required(record: Mapping[str, Any], key: str) -> Any:
    value = record.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"missing {key}")
    return value


def _display_name(slug: str) -> str:
    return re.sub(r"[-_]+", " ", slug).strip().title()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _location(value: object) -> str:
    if isinstance(value, Mapping):
        direct = value.get("name") or value.get("location") or value.get("LocationName")
        if direct:
            return str(direct).strip()
        parts = [
            value.get("city") or value.get("CityName"),
            value.get("region") or value.get("CountrySubDivisionCode"),
            value.get("country") or value.get("CountryCode"),
        ]
        return ", ".join(str(part).strip() for part in parts if part)
    if isinstance(value, list):
        names = [_location(item) for item in value]
        return "; ".join(name for name in names if name)
    return str(value or "").strip()


def _datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _salary_text(value: object) -> str:
    if value in (None, ""):
        return ""
    if not isinstance(value, Mapping):
        return str(value).strip()
    folded = {str(key).casefold(): item for key, item in value.items()}
    low = folded.get("min", folded.get("minimumrange", folded.get("minimum")))
    high = folded.get("max", folded.get("maximumrange", folded.get("maximum")))
    currency = folded.get("currency", folded.get("currencycode", ""))
    interval = folded.get("interval", folded.get("rate", folded.get("description", "")))
    bounds = " - ".join(str(item) for item in (low, high) if item not in (None, ""))
    return " ".join(
        str(item) for item in (currency, bounds, interval) if item not in (None, "")
    ).strip()


def _json(
    response: httpx.Response,
    *,
    max_bytes: int = _MAX_RESPONSE_BYTES,
    checkpoint: Callable[[], None] | None = None,
) -> Any:
    response.raise_for_status()
    content_length = response.headers.get("Content-Length")
    try:
        declared_size = int(content_length) if content_length is not None else None
    except (TypeError, ValueError, OverflowError):
        declared_size = None
    if declared_size is not None and declared_size > max_bytes:
        raise ValueError("source response exceeds the configured safety limit")
    payload = bytearray()
    for chunk in response.iter_bytes():
        if checkpoint is not None:
            checkpoint()
        if len(payload) + len(chunk) > max_bytes:
            raise ValueError("source response exceeds the configured safety limit")
        payload.extend(chunk)
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("source response is not valid JSON") from exc


def _request_json(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    max_bytes: int,
    checkpoint: Callable[[], None] | None = None,
    reject_redirect: bool = False,
    **kwargs: Any,
) -> Any:
    """Stream one JSON response so the byte cap applies before buffering."""

    # These adapters target fixed API origins. Never inherit an ambient/shared
    # client's redirect policy: an unvalidated redirect could turn a public
    # listing read into SSRF, and USAJobs requests also carry credentials.
    kwargs.setdefault("follow_redirects", False)
    with client.stream(method, url, **kwargs) as response:
        if response.is_redirect:
            message = (
                "USAJobs API returned a redirect; refusing to forward credentials"
                if reject_redirect
                else "source API returned a redirect; refusing an unvalidated target"
            )
            raise ValueError(message)
        return _json(response, max_bytes=max_bytes, checkpoint=checkpoint)


def _request_text(
    client: httpx.Client,
    url: str,
    *,
    max_bytes: int,
    checkpoint: Callable[[], None] | None = None,
    **kwargs: Any,
) -> str:
    """Read one bounded public HTML response without following redirects."""

    kwargs.setdefault("follow_redirects", False)
    with client.stream("GET", url, **kwargs) as response:
        if response.is_redirect:
            raise ValueError("source page returned a redirect; refusing target")
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").casefold()
        if content_type and not any(
            item in content_type
            for item in ("text/html", "application/xhtml+xml", "text/plain")
        ):
            raise ValueError("source response is not HTML")
        declared = response.headers.get("content-length")
        try:
            if declared is not None and int(declared) > max_bytes:
                raise ValueError("source response exceeds the configured safety limit")
        except (TypeError, ValueError, OverflowError) as exc:
            if isinstance(exc, ValueError) and str(exc).startswith("source response"):
                raise
        payload = bytearray()
        for chunk in response.iter_bytes():
            if checkpoint is not None:
                checkpoint()
            if len(payload) + len(chunk) > max_bytes:
                raise ValueError("source response exceeds the configured safety limit")
            payload.extend(chunk)
        encoding = response.encoding or "utf-8"
        try:
            return payload.decode(encoding, errors="replace")
        except LookupError:
            return payload.decode("utf-8", errors="replace")


class _ATSHTMLParser(HTMLParser):
    """Small non-browser parser for public iCIMS and Taleo listing pages."""

    ignored = frozenset({"style", "svg", "template", "noscript"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[tuple[str, str]] = []
        self.page_text: list[str] = []
        self.scripts: list[str] = []
        self._href: str | None = None
        self._anchor_parts: list[str] = []
        self._script_parts: list[str] | None = None
        self._ignored_depth = 0
        self._text_length = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        folded = tag.casefold()
        if folded in self.ignored:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if folded == "a" and len(self.anchors) < 50_000:
            self._finish_anchor()
            self._href = str(dict(attrs).get("href") or "").strip()[:8_000]
            self._anchor_parts = []
        elif folded == "script":
            self._script_parts = []

    def handle_endtag(self, tag: str) -> None:
        folded = tag.casefold()
        if folded in self.ignored and self._ignored_depth:
            self._ignored_depth -= 1
        elif not self._ignored_depth and folded == "a":
            self._finish_anchor()
        elif not self._ignored_depth and folded == "script":
            if self._script_parts is not None:
                self.scripts.append("".join(self._script_parts)[:2_000_000])
            self._script_parts = None

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        if self._script_parts is not None:
            self._script_parts.append(data)
            return
        if self._text_length < 2_000_000:
            part = data[: min(20_000, 2_000_000 - self._text_length)]
            self.page_text.append(part)
            self._text_length += len(part)
        if self._href is not None:
            self._anchor_parts.append(data[:4_000])

    def close(self) -> None:
        super().close()
        self._finish_anchor()

    def _finish_anchor(self) -> None:
        if self._href:
            text = re.sub(r"\s+", " ", " ".join(self._anchor_parts)).strip()
            self.anchors.append((self._href, text))
        self._href = None
        self._anchor_parts = []


def _html_parser(value: str) -> _ATSHTMLParser:
    parser = _ATSHTMLParser()
    try:
        parser.feed(value)
        parser.close()
    except Exception as exc:
        raise ValueError("source response is malformed HTML") from exc
    return parser


def _html_text(value: str) -> str:
    parser = _html_parser(value)
    return re.sub(r"\s+", " ", " ".join(parser.page_text)).strip()


def _listing_anchors(
    html_text: str,
    *,
    base_url: str,
    provider: str,
) -> list[dict[str, Any]]:
    parser = _html_parser(html_text)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    expected_host = (urlsplit(base_url).hostname or "").casefold()
    for href, title in parser.anchors:
        absolute = urljoin(base_url, href)
        parsed = urlsplit(absolute)
        if (
            parsed.scheme.casefold() != "https"
            or (parsed.hostname or "").casefold() != expected_host
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            continue
        if provider == "icims":
            match = re.search(
                r"/jobs/(?P<id>\d+)(?:/[^/?#]+)?(?:/job)?/?$", parsed.path, re.I
            )
            if not match:
                continue
            source_id = match.group("id")
        else:
            query = parse_qs(parsed.query)
            source_id = str(
                next(
                    (
                        values[0]
                        for key in ("job", "rid", "requisitionid", "reqid")
                        if (values := query.get(key))
                    ),
                    "",
                )
            ).strip()
            if not source_id and not re.search(
                r"/(?:jobdetail|viewrequisition|requisition)/", parsed.path, re.I
            ):
                continue
            if not source_id:
                source_id = hashlib.sha256(absolute.encode()).hexdigest()[:24]
        if source_id in seen or not title:
            continue
        seen.add(source_id)
        records.append(
            {"id": source_id, "title": title, "url": absolute, "location": ""}
        )
    return records


_ICIMS_TOP_REDIRECT = re.compile(
    r"window\.top\.location\.href\s*=\s*['\"]([^'\"]{1,2000})['\"]"
)
_ICIMS_CARD = re.compile(r"<li[^>]*\biCIMS_JobCardItem\b[^>]*>", re.I)
_ICIMS_ANCHOR = re.compile(r"<a\b[^>]*\biCIMS_Anchor\b[^>]*>", re.I)
_ICIMS_FIELD = re.compile(
    r"<dt[^>]*>(?P<label>.*?)</dt>\s*<dd[^>]*>(?P<value>.*?)</dd>", re.I | re.S
)
_ICIMS_H3 = re.compile(r"<h3[^>]*>(?P<title>.*?)</h3>", re.I | re.S)
_ICIMS_DESCRIPTION = re.compile(
    r"<div[^>]*\bdescription\b[^>]*>(?P<text>.*?)</div>", re.I | re.S
)


def _attribute(tag: str, name: str) -> str:
    match = re.search(rf"\b{name}\s*=\s*(\"[^\"]*\"|'[^']*')", tag, re.I)
    return html_unescape(match.group(1)[1:-1]) if match else ""


def _without_iframe_flag(url: str) -> str:
    parts = urlsplit(url)
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() != "in_iframe"
    ]
    return urlunsplit(parts._replace(query=urlencode(query)))


def _icims_job_cards(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    """Read the job cards of an iCIMS iframe listing page."""

    expected_host = (urlsplit(base_url).hostname or "").casefold()
    starts = [match.start() for match in _ICIMS_CARD.finditer(html_text)]
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, start in enumerate(starts[:1_000]):
        end = starts[index + 1] if index + 1 < len(starts) else len(html_text)
        card = html_text[start : min(end, start + 50_000)]
        anchor = _ICIMS_ANCHOR.search(card)
        if not anchor:
            continue
        absolute = _without_iframe_flag(
            urljoin(base_url, _attribute(anchor.group(0), "href"))
        )
        parsed = urlsplit(absolute)
        match = re.search(r"/jobs/(?P<id>\d+)(?:/|$)", parsed.path)
        if (
            not match
            or parsed.scheme.casefold() != "https"
            or (parsed.hostname or "").casefold() != expected_host
        ):
            continue
        source_id = match.group("id")
        heading = _ICIMS_H3.search(card)
        title = _html_text(heading.group("title")) if heading else ""
        if not title:
            title = re.sub(r"^\s*\d+\s*-\s*", "", _attribute(anchor.group(0), "title"))
        if not title or source_id in seen:
            continue
        seen.add(source_id)
        fields: dict[str, str] = {}
        for field in _ICIMS_FIELD.finditer(card):
            label = _html_text(field.group("label")).split(":")[0].strip().casefold()
            value = _html_text(field.group("value"))
            if label and value and label not in fields:
                fields[label] = value
        locations = [
            fields.get("location", ""),
            fields.get("additional location(s)", ""),
        ]
        low = fields.get("posted min pay rate", "")
        high = fields.get("posted max pay rate", "")
        description = _ICIMS_DESCRIPTION.search(card)
        records.append(
            {
                "id": source_id,
                "title": title,
                "url": absolute,
                "location": " | ".join(item for item in locations if item),
                "description": _html_text(description.group("text"))
                if description
                else "",
                "salary_text": " - ".join(item for item in (low, high) if item),
                "work_arrangement": fields.get("work arrangement", ""),
                "department": fields.get("department", ""),
                "position_type": fields.get("position type", ""),
                "requisition": fields.get("job id", ""),
            }
        )
    return records


class GreenhouseSource(StructuredJobSource):
    name = "greenhouse"

    def __init__(
        self,
        client: httpx.Client,
        *,
        board_token: str,
        company: str | None = None,
    ) -> None:
        if not board_token.strip():
            raise ValueError("board_token is required")
        self.board_token = board_token.strip()
        self.company = (company or _display_name(self.board_token)).strip()
        super().__init__(
            client,
            source_key=bounded_source_key("greenhouse", self.board_token),
            concurrency_key="boards-api.greenhouse.io",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = (
            "https://boards-api.greenhouse.io/v1/boards/"
            f"{quote(self.board_token, safe='')}/jobs"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            params={"content": "false" if self.inventory_metadata_only else "true"},
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, Mapping) or not isinstance(data.get("jobs"), list):
            raise ValueError("Greenhouse response is missing jobs")
        return data["jobs"], {"board_token": self.board_token, "query": query or ""}

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        location = _location(record.get("location"))
        salary_value = record.get("pay_input_ranges") or record.get("salary_range")
        salary_text = _salary_text(salary_value)
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(record, "id")),
            company=self.company,
            title=str(_required(record, "title")),
            url=str(_required(record, "absolute_url")),
            location=location,
            description=str(record.get("content") or ""),
            salary_text=salary_text,
            salary=normalize_salary(salary_value) if salary_value else None,
            remote=normalize_remote(record.get("workplace_type"), location=location),
            posted_at=_datetime(
                record.get("first_published") or record.get("updated_at")
            ),
            metadata={
                "departments": record.get("departments", []),
                "offices": record.get("offices", []),
                "source_updated_at": record.get("updated_at"),
            },
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        """Fetch full Greenhouse content after metadata-only inventory discovery."""

        self.checkpoint()
        endpoint = (
            "https://boards-api.greenhouse.io/v1/boards/"
            f"{quote(self.board_token, safe='')}/jobs/{quote(item.source_id, safe='')}"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, Mapping):
            raise ValueError("Greenhouse detail response is not an object")
        detail = self._parse_record(data, 0)
        if detail.source_id != item.source_id:
            raise ValueError("Greenhouse detail response changed the listing identity")
        return replace(
            detail,
            metadata={**dict(item.metadata), **dict(detail.metadata), "hydrated": True},
        )


class LeverSource(StructuredJobSource):
    name = "lever"

    def __init__(
        self, client: httpx.Client, *, site: str, company: str | None = None
    ) -> None:
        if not site.strip():
            raise ValueError("site is required")
        self.site = site.strip()
        self.company = (company or _display_name(self.site)).strip()
        super().__init__(
            client,
            source_key=bounded_source_key("lever", self.site),
            concurrency_key="api.lever.co",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = f"https://api.lever.co/v0/postings/{quote(self.site, safe='')}"
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            params={"mode": "json"},
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, list):
            raise ValueError("Lever response is not a job list")
        return data, {"site": self.site, "query": query or ""}

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        categories = _mapping(record.get("categories"))
        location = _location(categories.get("location") or record.get("location"))
        salary_value = record.get("salaryRange") or record.get("salary_range")
        salary_text = _salary_text(salary_value)
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(record, "id")),
            company=self.company,
            title=str(_required(record, "text")),
            url=str(_required(record, "hostedUrl")),
            location=location,
            description=str(
                record.get("descriptionPlain") or record.get("description") or ""
            ),
            salary_text=salary_text,
            salary=normalize_salary(salary_value) if salary_value else None,
            remote=normalize_remote(record.get("workplaceType"), location=location),
            posted_at=_datetime(record.get("createdAt")),
            metadata={
                "apply_url": record.get("applyUrl", ""),
                "commitment": categories.get("commitment", ""),
                "department": categories.get("department", ""),
                "team": categories.get("team", ""),
            },
        )


class AshbySource(StructuredJobSource):
    name = "ashby"

    def __init__(
        self, client: httpx.Client, *, board: str, company: str | None = None
    ) -> None:
        if not board.strip():
            raise ValueError("board is required")
        self.board = board.strip()
        self.company = (company or _display_name(self.board)).strip()
        super().__init__(
            client,
            source_key=bounded_source_key("ashby", self.board),
            concurrency_key="api.ashbyhq.com",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        # The public posting API is the board path itself; ``/jobs`` is 401.
        endpoint = (
            "https://api.ashbyhq.com/posting-api/job-board/"
            f"{quote(self.board, safe='')}"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            params={"includeCompensation": "true"},
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, Mapping) or not isinstance(data.get("jobs"), list):
            raise ValueError("Ashby response is missing jobs")
        return data["jobs"], {"board": self.board, "query": query or ""}

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        location = _location(record.get("location"))
        compensation = _mapping(record.get("compensation"))
        salary_value = (
            compensation.get("scrapeableCompensationSalarySummary")
            or compensation.get("compensationTierSummary")
            or record.get("compensationTierSummary")
            or record.get("salary")
        )
        salary_text = _salary_text(salary_value)
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(record, "id")),
            company=self.company,
            title=str(_required(record, "title")),
            url=str(_required(record, "jobUrl")),
            location=location,
            description=str(
                record.get("descriptionPlain")
                or record.get("descriptionHtml")
                or record.get("description")
                or ""
            ),
            salary_text=salary_text,
            salary=normalize_salary(salary_value) if salary_value else None,
            remote=normalize_remote(record.get("isRemote"), location=location),
            posted_at=_datetime(
                record.get("publishedAt") or record.get("published_at")
            ),
            metadata={
                "department": record.get("department", ""),
                "team": record.get("team", ""),
                "employment_type": record.get("employmentType", ""),
            },
        )


class WorkableSource(StructuredJobSource):
    name = "workable"

    def __init__(
        self, client: httpx.Client, *, account: str, company: str | None = None
    ) -> None:
        if not account.strip():
            raise ValueError("account is required")
        self.account = account.strip()
        self.company = (company or _display_name(self.account)).strip()
        super().__init__(
            client,
            source_key=bounded_source_key("workable", self.account),
            concurrency_key="apply.workable.com",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = (
            "https://apply.workable.com/api/v1/widget/accounts/"
            f"{quote(self.account, safe='')}/vacancies"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, Mapping) or not isinstance(data.get("results"), list):
            raise ValueError("Workable response is missing results")
        return data["results"], {"account": self.account, "query": query or ""}

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        source_id = str(record.get("shortcode") or record.get("id") or "").strip()
        if not source_id:
            raise ValueError("missing shortcode")
        location = _location(record.get("location") or record.get("locations"))
        salary_value = record.get("salary") or record.get("salary_range")
        salary_text = _salary_text(salary_value)
        url = record.get("url") or (
            f"https://apply.workable.com/{quote(self.account, safe='')}/j/"
            f"{quote(source_id, safe='')}/"
        )
        return ScanItem(
            source=self.source_key,
            source_id=source_id,
            company=self.company,
            title=str(_required(record, "title")),
            url=str(url),
            location=location,
            description=str(
                record.get("description") or record.get("descriptionPlain") or ""
            ),
            salary_text=salary_text,
            salary=normalize_salary(salary_value) if salary_value else None,
            remote=normalize_remote(
                record.get("workplace_type") or record.get("remote"), location=location
            ),
            posted_at=_datetime(record.get("published") or record.get("created_at")),
            deadline=_datetime(
                record.get("application_deadline") or record.get("deadline")
            ),
            metadata={
                "department": record.get("department", ""),
                "employment_type": record.get("employment_type", ""),
            },
        )


class SmartRecruitersSource(StructuredJobSource):
    """Public SmartRecruiters company postings API."""

    name = "smartrecruiters"

    def __init__(
        self,
        client: httpx.Client,
        *,
        company_slug: str,
        company: str | None = None,
        limit: int = 100,
        max_pages: int = 50,
    ) -> None:
        if not company_slug.strip():
            raise ValueError("company_slug is required")
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if not 1 <= max_pages <= 1_000:
            raise ValueError("max_pages must be between 1 and 1,000")
        self.company_slug = company_slug.strip()
        self.company = (company or _display_name(self.company_slug)).strip()
        self.limit = limit
        self.max_pages = max_pages
        super().__init__(
            client,
            source_key=bounded_source_key("smartrecruiters", self.company_slug),
            concurrency_key="api.smartrecruiters.com",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = (
            "https://api.smartrecruiters.com/v1/companies/"
            f"{quote(self.company_slug, safe='')}/postings"
        )
        records: list[object] = []
        seen: set[str] = set()
        total: int | None = None
        pages_fetched = 0
        page_error: SourceError | None = None
        completed = False
        for page in range(self.max_pages):
            try:
                self.checkpoint()
                data = _request_json(
                    self.client,
                    "GET",
                    endpoint,
                    params={"limit": self.limit, "offset": page * self.limit},
                    max_bytes=self.max_response_bytes,
                    checkpoint=self.checkpoint,
                    timeout=self.request_timeout(),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if not records:
                    raise
                page_error = source_error_from_exception(exc)
                break
            if not isinstance(data, Mapping) or not isinstance(
                data.get("content"), list
            ):
                raise ValueError("SmartRecruiters response is missing content")
            page_records = data["content"]
            added = 0
            for record in page_records:
                if not isinstance(record, Mapping):
                    records.append(record)
                    added += 1
                    continue
                identity = str(record.get("id") or record.get("uuid") or "").strip()
                if identity and identity in seen:
                    continue
                if identity:
                    seen.add(identity)
                records.append(record)
                added += 1
            pages_fetched += 1
            raw_total = data.get("totalFound", data.get("total"))
            try:
                reported = int(raw_total)
            except (TypeError, ValueError, OverflowError):
                reported = None
            if reported is not None and reported >= len(records):
                total = max(total or 0, reported)
            if not page_records or len(page_records) < self.limit:
                completed = True
                total = len(records) if total is None else max(total, len(records))
                break
            if total is not None and len(records) >= total:
                completed = True
                break
            if added == 0:
                raise ValueError("SmartRecruiters pagination repeated a page")
        return records, {
            "company_slug": self.company_slug,
            "query": query or "",
            "total": total,
            "pages_fetched": pages_fetched,
            "truncated": page_error is not None or not completed,
            "truncation_code": "page_read_error" if page_error else None,
            "truncation_reason": page_error.message if page_error else None,
            "truncation_retryable": page_error.retryable if page_error else False,
            "truncation_http_status": page_error.http_status if page_error else None,
            "truncation_retry_after_seconds": (
                page_error.retry_after_seconds if page_error else None
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        source_id = str(record.get("id") or record.get("uuid") or "").strip()
        if not source_id:
            raise ValueError("missing id")
        location = _location(record.get("location"))
        department = _mapping(record.get("department"))
        function = _mapping(record.get("function"))
        employment = _mapping(record.get("typeOfEmployment"))
        url = str(record.get("ref") or "").strip() or (
            f"https://jobs.smartrecruiters.com/{quote(self.company_slug, safe='')}/"
            f"{quote(source_id, safe='')}"
        )
        return ScanItem(
            source=self.source_key,
            source_id=source_id,
            company=self.company,
            title=str(record.get("name") or record.get("title") or "").strip(),
            url=url,
            location=location,
            description=str(record.get("jobAd") or record.get("description") or ""),
            remote=normalize_remote(
                record.get("remote") or record.get("workplaceType"), location=location
            ),
            posted_at=_datetime(
                record.get("releasedDate")
                or record.get("createdOn")
                or record.get("updatedOn")
            ),
            metadata={
                "department": department.get("label", department.get("name", "")),
                "function": function.get("label", function.get("name", "")),
                "employment_type": employment.get("label", employment.get("name", "")),
            },
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        self.checkpoint()
        endpoint = (
            "https://api.smartrecruiters.com/v1/companies/"
            f"{quote(self.company_slug, safe='')}/postings/"
            f"{quote(item.source_id, safe='')}"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        if not isinstance(data, Mapping):
            raise ValueError("SmartRecruiters detail response is not an object")
        detail_id = str(data.get("id") or data.get("uuid") or "").strip()
        if detail_id and detail_id != item.source_id:
            raise ValueError("SmartRecruiters detail changed the listing identity")
        job_ad = _mapping(data.get("jobAd"))
        sections = _mapping(job_ad.get("sections"))
        description_parts: list[str] = []
        for value in sections.values():
            section = _mapping(value)
            text = section.get("text") or section.get("description") or value
            if text:
                description_parts.append(str(text))
        description = "\n\n".join(description_parts).strip()
        if not description:
            description = str(data.get("description") or item.description or "").strip()
        return replace(
            item,
            description=description,
            metadata={**dict(item.metadata), "hydrated": True},
        )


class ICIMSSource(StructuredJobSource):
    """Static public iCIMS listings without JavaScript or authentication."""

    name = "icims"

    def __init__(
        self,
        client: httpx.Client,
        *,
        base_url: str,
        company: str,
        max_pages: int = 100,
    ) -> None:
        parsed = urlsplit(base_url.strip())
        hostname = (parsed.hostname or "").rstrip(".").casefold()
        if (
            parsed.scheme.casefold() != "https"
            or not hostname.endswith(".icims.com")
            or hostname == "icims.com"
            or parsed.username
            or parsed.password
            or parsed.port is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "base_url must be a credential-free *.icims.com HTTPS origin"
            )
        if not company.strip():
            raise ValueError("company is required")
        if not 1 <= max_pages <= 1_000:
            raise ValueError("max_pages must be between 1 and 1,000")
        self.base_url = f"https://{hostname}"
        self.company = company.strip()
        self.max_pages = max_pages
        super().__init__(
            client,
            source_key=bounded_source_key("icims", hostname),
            concurrency_key=hostname,
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = f"{self.base_url}/jobs/search"
        records: list[object] = []
        seen: set[str] = set()
        pages_fetched = 0
        page_error: SourceError | None = None
        completed = False
        for page in range(self.max_pages):
            # The standard portal page only frames the listing; ``in_iframe=1``
            # returns the job cards themselves.
            params: dict[str, Any] = {
                "ss": 1,
                "searchRelation": "keyword_all",
                "pr": page,
                "in_iframe": 1,
            }
            if query:
                params["searchKeyword"] = query
            page_url = f"{endpoint}?{urlencode(params)}"
            try:
                self.checkpoint()
                html_text = _request_text(
                    self.client,
                    page_url,
                    max_bytes=self.max_response_bytes,
                    checkpoint=self.checkpoint,
                    timeout=self.request_timeout(),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if not records:
                    raise
                page_error = source_error_from_exception(exc)
                break
            page_records = _icims_job_cards(html_text, base_url=page_url)
            if not page_records:
                page_records = _listing_anchors(
                    html_text, base_url=page_url, provider="icims"
                )
                for record in page_records:
                    record["url"] = _without_iframe_flag(record["url"])
            handoff = _ICIMS_TOP_REDIRECT.search(html_text)
            if not page_records and handoff:
                target = handoff.group(1).replace("\\/", "/")[:300]
                raise ValueError(
                    "iCIMS portal hands off to the employer's own career site "
                    f"({target}); scan that site instead"
                )
            body = _html_text(html_text).casefold()
            structurally_valid = (
                "icims" in html_text.casefold()
                or "/jobs/" in html_text.casefold()
                or re.search(r"\b(?:0|no) (?:open )?(?:jobs|positions)\b", body)
            )
            if not structurally_valid:
                raise ValueError("iCIMS response does not contain a job board")
            added = 0
            for record in page_records:
                identity = str(record["id"])
                if identity in seen:
                    continue
                seen.add(identity)
                records.append(record)
                added += 1
            pages_fetched += 1
            if not page_records or added == 0:
                completed = True
                break
            parser = _html_parser(html_text)
            has_next = any(
                re.search(r"(?:[?&]pr=|/page/)(?:%d)\b" % (page + 1), href)
                or title.casefold() in {"next", "next page", "›", ">"}
                for href, title in parser.anchors
            )
            if not has_next:
                completed = True
                break
        return records, {
            "base_url": self.base_url,
            "query": query or "",
            "pages_fetched": pages_fetched,
            "total": len(records) if completed else None,
            "truncated": page_error is not None or not completed,
            "truncation_code": "page_read_error" if page_error else None,
            "truncation_reason": page_error.message if page_error else None,
            "truncation_retryable": page_error.retryable if page_error else False,
            "truncation_http_status": page_error.http_status if page_error else None,
            "truncation_retry_after_seconds": (
                page_error.retry_after_seconds if page_error else None
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        location = _location(record.get("location"))
        salary_text = str(record.get("salary_text") or "")
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(record, "id")),
            company=self.company,
            title=str(_required(record, "title")),
            url=str(_required(record, "url")),
            location=location,
            description=str(record.get("description") or ""),
            salary_text=salary_text,
            salary=normalize_salary(salary_text) if salary_text else None,
            remote=normalize_remote(record.get("work_arrangement"), location=location),
            metadata={
                "extraction": "static_html",
                "department": record.get("department", ""),
                "position_type": record.get("position_type", ""),
                "requisition": record.get("requisition", ""),
            },
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        self.checkpoint()
        html_text = _request_text(
            self.client,
            item.url,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        description = _html_text(html_text)
        if not description:
            raise ValueError("iCIMS detail response has no visible content")
        return replace(
            item,
            description=description,
            metadata={**dict(item.metadata), "hydrated": True},
        )


class JibeSource(StructuredJobSource):
    """iCIMS career sites built on Jibe, read through their public ``/api/jobs`` feed."""

    name = "jibe"

    def __init__(
        self,
        client: httpx.Client,
        *,
        base_url: str,
        company: str,
        jobs_path: str = "",
        page_size: int = 100,
        max_pages: int = 50,
    ) -> None:
        parsed = urlsplit(base_url.strip())
        hostname = (parsed.hostname or "").rstrip(".").casefold()
        if (
            parsed.scheme.casefold() != "https"
            or not hostname
            or parsed.username
            or parsed.password
            or parsed.port is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or not is_public_http_url(f"https://{hostname}")
        ):
            raise ValueError("base_url must be a credential-free public HTTPS origin")
        prefix = "/" + jobs_path.strip().strip("/") if jobs_path.strip("/ ") else ""
        if not re.fullmatch(r"(?:/[A-Za-z0-9._-]{1,64}){0,4}", prefix):
            raise ValueError("jobs_path must be a short path prefix such as /careers")
        if not company.strip():
            raise ValueError("company is required")
        if not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")
        if not 1 <= max_pages <= 200:
            raise ValueError("max_pages must be between 1 and 200")
        self.base_url = f"https://{hostname}"
        self.jobs_path = prefix
        self.company = company.strip()
        self.page_size = page_size
        self.max_pages = max_pages
        super().__init__(
            client,
            source_key=bounded_source_key("jibe", hostname),
            concurrency_key=hostname,
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = f"{self.base_url}/api/jobs"
        records: list[object] = []
        total: int | None = None
        for page in range(1, self.max_pages + 1):
            self.checkpoint()
            params: dict[str, Any] = {"page": page, "limit": self.page_size}
            if query:
                params["keywords"] = query
            data = _request_json(
                self.client,
                "GET",
                endpoint,
                params=params,
                max_bytes=self.max_response_bytes,
                checkpoint=self.checkpoint,
                timeout=self.request_timeout(),
            )
            if not isinstance(data, Mapping) or not isinstance(data.get("jobs"), list):
                raise ValueError("Jibe response is missing jobs")
            try:
                total = int(data.get("totalCount"))
            except (TypeError, ValueError):
                total = None
            page_jobs = [
                entry.get("data") if isinstance(entry, Mapping) else None
                for entry in data["jobs"]
            ]
            records.extend(page_jobs)
            if not page_jobs or (total is not None and len(records) >= total):
                break
        truncated = total is None or len(records) < total
        return records, {
            "base_url": self.base_url,
            "query": query or "",
            "total": total,
            "truncated": truncated,
            "truncation_reason": (
                "The Jibe feed reported more jobs than the page budget read."
                if total is not None
                else "The Jibe feed did not report a total job count."
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        slug = str(record.get("slug") or _required(record, "req_id")).strip()
        location = str(
            record.get("full_location")
            or record.get("location_name")
            or record.get("short_location")
            or ""
        )
        low = record.get("salary_min_value")
        high = record.get("salary_max_value")
        has_low = (
            isinstance(low, (int, float)) and not isinstance(low, bool) and low > 0
        )
        has_high = (
            isinstance(high, (int, float)) and not isinstance(high, bool) and high > 0
        )
        salary_text = ""
        salary = None
        if has_low:
            salary_text = f"{low:,.0f}" + (f" - {high:,.0f}" if has_high else "")
            salary = normalize_salary(low, high if has_high else None)
        categories = record.get("categories")
        return ScanItem(
            source=self.source_key,
            source_id=str(record.get("req_id") or slug),
            company=self.company,
            title=str(_required(record, "title")),
            url=f"{self.base_url}{self.jobs_path}/jobs/{quote(slug, safe='')}",
            location=location,
            description=_html_text(str(record.get("description") or "")),
            salary_text=salary_text,
            salary=salary,
            remote=normalize_remote(record.get("location_type"), location=location),
            posted_at=_datetime(record.get("posted_date") or record.get("create_date")),
            metadata={
                "department": record.get("department") or "",
                "employment_type": record.get("employment_type") or "",
                "categories": [
                    item.get("name")
                    for item in categories[:10]
                    if isinstance(item, Mapping) and item.get("name")
                ]
                if isinstance(categories, list)
                else [],
                "apply_url": record.get("apply_url") or "",
            },
        )


class TaleoSource(StructuredJobSource):
    """Static public Taleo Business Edition v2 listings."""

    name = "taleo"

    def __init__(
        self,
        client: httpx.Client,
        *,
        search_url: str,
        company: str,
        max_pages: int = 100,
    ) -> None:
        parsed = urlsplit(search_url.strip())
        hostname = (parsed.hostname or "").rstrip(".").casefold()
        query = parse_qs(parsed.query, keep_blank_values=True)
        if (
            parsed.scheme.casefold() != "https"
            or not hostname.endswith(".tbe.taleo.net")
            or hostname == "tbe.taleo.net"
            or parsed.username
            or parsed.password
            or parsed.port is not None
            or parsed.fragment
            or not parsed.path.casefold()
            .rstrip("/")
            .endswith("/ats/careers/v2/searchresults")
            or len(query.get("org", [])) != 1
            or len(query.get("cws", [])) != 1
            or not query["org"][0].strip()
            or not query["cws"][0].strip()
        ):
            raise ValueError(
                "search_url must be a credential-free *.tbe.taleo.net v2 search URL"
            )
        if not company.strip():
            raise ValueError("company is required")
        if not 1 <= max_pages <= 1_000:
            raise ValueError("max_pages must be between 1 and 1,000")
        self.search_url = urlunsplit(("https", hostname, parsed.path, parsed.query, ""))
        self.company = company.strip()
        self.max_pages = max_pages
        self._org = query["org"][0]
        self._cws = query["cws"][0]
        super().__init__(
            client,
            source_key=bounded_source_key(
                "taleo", f"{hostname}:{self._org}:{self._cws}"
            ),
            concurrency_key=hostname,
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        records: list[object] = []
        seen: set[str] = set()
        pages_fetched = 0
        page_error: SourceError | None = None
        completed = False
        for page in range(self.max_pages):
            parsed = urlsplit(self.search_url)
            params = {"org": self._org, "cws": self._cws}
            if page:
                params["page"] = str(page + 1)
            if query:
                params["keyword"] = query
            page_url = urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, urlencode(params), "")
            )
            try:
                self.checkpoint()
                html_text = _request_text(
                    self.client,
                    page_url,
                    max_bytes=self.max_response_bytes,
                    checkpoint=self.checkpoint,
                    timeout=self.request_timeout(),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if not records:
                    raise
                page_error = source_error_from_exception(exc)
                break
            page_records = _listing_anchors(
                html_text, base_url=page_url, provider="taleo"
            )
            body = _html_text(html_text).casefold()
            structurally_valid = (
                "taleo" in html_text.casefold()
                or "searchresults" in html_text.casefold()
                or "jobdetail" in html_text.casefold()
                or re.search(r"\b(?:0|no) (?:open )?(?:jobs|positions)\b", body)
            )
            if not structurally_valid:
                raise ValueError("Taleo response does not contain a job board")
            added = 0
            for record in page_records:
                identity = str(record["id"])
                if identity in seen:
                    continue
                seen.add(identity)
                records.append(record)
                added += 1
            pages_fetched += 1
            parser = _html_parser(html_text)
            has_next = any(
                title.casefold() in {"next", "next page", "›", ">"}
                or re.search(r"[?&]page=%d\b" % (page + 2), href)
                for href, title in parser.anchors
            )
            if not page_records or added == 0 or not has_next:
                completed = True
                break
        return records, {
            "search_url": self.search_url,
            "org": self._org,
            "cws": self._cws,
            "query": query or "",
            "pages_fetched": pages_fetched,
            "total": len(records) if completed else None,
            "truncated": page_error is not None or not completed,
            "truncation_code": "page_read_error" if page_error else None,
            "truncation_reason": page_error.message if page_error else None,
            "truncation_retryable": page_error.retryable if page_error else False,
            "truncation_http_status": page_error.http_status if page_error else None,
            "truncation_retry_after_seconds": (
                page_error.retry_after_seconds if page_error else None
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(record, "id")),
            company=self.company,
            title=str(_required(record, "title")),
            url=str(_required(record, "url")),
            location=_location(record.get("location")),
            description=str(record.get("description") or ""),
            metadata={"extraction": "static_html"},
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        self.checkpoint()
        html_text = _request_text(
            self.client,
            item.url,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        description = _html_text(html_text)
        if not description:
            raise ValueError("Taleo detail response has no visible content")
        return replace(
            item,
            description=description,
            metadata={**dict(item.metadata), "hydrated": True},
        )


class WorkdaySource(StructuredJobSource):
    name = "workday"

    def __init__(
        self,
        client: httpx.Client,
        *,
        tenant: str,
        site: str,
        company: str | None = None,
        wd: str = "wd1",
        host: str | None = None,
        locale: str = "en-US",
        limit: int = 20,
        max_pages: int = 20,
    ) -> None:
        if not tenant.strip() or not site.strip():
            raise ValueError("tenant and site are required")
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        if not 1 <= max_pages <= 250:
            raise ValueError("max_pages must be between 1 and 250")
        self.tenant = tenant.strip()
        self.site = site.strip()
        self.company = (company or _display_name(self.tenant)).strip()
        self.locale = locale.strip() or "en-US"
        self.limit = limit
        self.max_pages = max_pages
        default_host = f"https://{self.tenant}.{wd.strip()}.myworkdayjobs.com"
        self.host = (host or default_host).strip().rstrip("/")
        if "://" not in self.host:
            self.host = f"https://{self.host}"
        parsed_host = urlsplit(self.host)
        if (
            not is_public_http_url(self.host)
            or parsed_host.path not in {"", "/"}
            or parsed_host.query
            or parsed_host.fragment
        ):
            raise ValueError(
                "Workday host must be a credential-free public HTTP(S) origin"
            )
        # Canonicalize only the origin spelling; listing launch URLs remain exact.
        self.host = f"{parsed_host.scheme.casefold()}://{parsed_host.netloc}"
        super().__init__(
            client,
            source_key=bounded_source_key("workday", f"{self.tenant}:{self.site}"),
            concurrency_key=urlsplit(self.host).hostname or self.tenant.casefold(),
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        endpoint = (
            f"{self.host}/wday/cxs/{quote(self.tenant, safe='')}/"
            f"{quote(self.site, safe='')}/jobs"
        )
        records: list[object] = []
        total: int | None = None
        pages_fetched = 0
        page_error: SourceError | None = None
        completed = False
        for page in range(self.max_pages):
            payload = {
                "appliedFacets": {},
                "limit": self.limit,
                "offset": page * self.limit,
                "searchText": query or "",
            }
            try:
                self.checkpoint()
                data = _request_json(
                    self.client,
                    "POST",
                    endpoint,
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    max_bytes=self.max_response_bytes,
                    checkpoint=self.checkpoint,
                    timeout=self.request_timeout(),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if not records:
                    raise
                page_error = source_error_from_exception(exc)
                break
            if not isinstance(data, Mapping) or not isinstance(
                data.get("jobPostings"), list
            ):
                raise ValueError("Workday response is missing jobPostings")
            page_records = data["jobPostings"]
            records.extend(page_records)
            pages_fetched += 1
            raw_total = data.get("total")
            try:
                reported_total = int(raw_total) if raw_total is not None else None
            except (TypeError, ValueError, OverflowError):
                reported_total = None
            if reported_total is not None and reported_total >= len(records):
                # Some tenants report zero on the terminal empty page. Never
                # let page-local metadata contradict rows already observed.
                total = max(total or 0, reported_total)
            if not page_records or len(page_records) < self.limit:
                total = len(records) if total is None else max(total, len(records))
                completed = True
                break
            if total is not None and len(records) >= total:
                completed = True
                break
        return records, {
            "tenant": self.tenant,
            "site": self.site,
            "query": query or "",
            "total": total,
            "pages_fetched": pages_fetched,
            "truncated": page_error is not None or not completed,
            "truncation_code": "page_read_error" if page_error else None,
            "truncation_reason": page_error.message if page_error else None,
            "truncation_retryable": page_error.retryable if page_error else False,
            "truncation_http_status": page_error.http_status if page_error else None,
            "truncation_retry_after_seconds": (
                page_error.retry_after_seconds if page_error else None
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        path = str(_required(record, "externalPath"))
        source_id = str(record.get("jobReqId") or "").strip()
        if not source_id:
            tail = path.rstrip("/").rsplit("/", 1)[-1]
            source_id = tail.rsplit("_", 1)[-1] if "_" in tail else tail
        if not source_id:
            raise ValueError("cannot derive source id from externalPath")
        location = _location(record.get("locationsText") or record.get("location"))
        bullet_fields = record.get("bulletFields")
        if not isinstance(bullet_fields, list):
            bullet_fields = []
        salary_text = next(
            (
                str(item)
                for item in bullet_fields
                if re.search(
                    r"(?:[$£€]|\b(?:salary|pay|hourly|annual)\b)",
                    str(item),
                    re.IGNORECASE,
                )
            ),
            "",
        )
        url = (
            path
            if path.startswith(("http://", "https://"))
            else urljoin(f"{self.host}/", path.lstrip("/"))
        )
        return ScanItem(
            source=self.source_key,
            source_id=source_id,
            company=self.company,
            title=str(_required(record, "title")),
            url=url,
            location=location,
            description=str(
                record.get("jobDescription") or record.get("description") or ""
            ),
            salary_text=salary_text,
            salary=normalize_salary(salary_text) if salary_text else None,
            remote=normalize_remote(record.get("remoteType"), location=location),
            posted_at=_datetime(record.get("postedOn") or record.get("startDate")),
            metadata={
                "bullet_fields": bullet_fields,
                "external_path": path,
                "source_updated_at": record.get("updatedAt") or record.get("postedOn"),
            },
        )

    def hydrate(self, item: ScanItem) -> ScanItem:
        """Fetch Workday's static detail JSON for a focused or selected listing."""

        path = str(item.metadata.get("external_path") or "").strip()
        if not path:
            return super().hydrate(item)
        self.checkpoint()
        endpoint = (
            f"{self.host}/wday/cxs/{quote(self.tenant, safe='')}/"
            f"{quote(self.site, safe='')}/{path.lstrip('/')}"
        )
        data = _request_json(
            self.client,
            "GET",
            endpoint,
            max_bytes=self.max_response_bytes,
            checkpoint=self.checkpoint,
            timeout=self.request_timeout(),
        )
        self.checkpoint()
        if not isinstance(data, Mapping):
            raise ValueError("Workday detail response is not an object")
        detail = data.get("jobPostingInfo")
        if not isinstance(detail, Mapping):
            detail = data
        description = str(
            detail.get("jobDescription")
            or detail.get("description")
            or item.description
            or ""
        ).strip()
        location = (
            _location(
                detail.get("location")
                or detail.get("primaryLocation")
                or detail.get("additionalLocations")
            )
            or item.location
        )
        salary_value = detail.get("salary") or detail.get("compensation")
        salary_text = _salary_text(salary_value) if salary_value else item.salary_text
        return replace(
            item,
            title=str(detail.get("title") or item.title).strip(),
            location=location,
            description=description,
            salary_text=salary_text,
            salary=(normalize_salary(salary_value) if salary_value else item.salary),
            remote=normalize_remote(
                detail.get("remoteType"),
                location=location,
            )
            if detail.get("remoteType") is not None
            else item.remote,
            metadata={
                **dict(item.metadata),
                "hydrated": True,
            },
        )


class USAJobsSource(StructuredJobSource):
    name = "usajobs"

    def __init__(
        self,
        client: httpx.Client,
        *,
        api_key: str | None = None,
        email: str | None = None,
        keyword: str | None = None,
        location: str | None = None,
        days: int = 14,
        results_per_page: int = 100,
        max_pages: int = 5,
    ) -> None:
        if days < 0:
            raise ValueError("days cannot be negative")
        if not 1 <= results_per_page <= 500:
            raise ValueError("results_per_page must be between 1 and 500")
        if not str(api_key or "").strip():
            raise ValueError("USAJobs API key is required")
        if not str(email or "").strip() or "@" not in str(email):
            raise ValueError("USAJobs account email is required")
        if not 1 <= max_pages <= 10:
            raise ValueError("max_pages must be between 1 and 10")
        self.api_key = str(api_key).strip()
        self.email = str(email).strip()
        self.keyword = keyword or ""
        self.location = str(location or "").strip()
        self.days = days
        self.results_per_page = results_per_page
        self.max_pages = max_pages
        location_key = re.sub(r"[^a-z0-9]+", "-", self.location.casefold()).strip("-")
        if self.location:
            suffix = hashlib.sha256(self.location.casefold().encode()).hexdigest()[:8]
            source_key = f"usajobs:{location_key[:48]}-{suffix}"
        else:
            source_key = "usajobs"
        super().__init__(
            client,
            source_key=source_key,
            concurrency_key="data.usajobs.gov",
        )

    def _fetch_records(
        self, query: str | None
    ) -> tuple[list[object], Mapping[str, Any]]:
        headers = {
            "Accept": "application/json",
            "Authorization-Key": self.api_key,
            "User-Agent": self.email,
        }
        params: dict[str, Any] = {
            "Keyword": query or self.keyword,
            "DatePosted": self.days,
            "ResultsPerPage": self.results_per_page,
        }
        if self.location:
            params["LocationName"] = self.location
        records: list[object] = []
        total: int | None = None
        pages_fetched = 0
        page_error: SourceError | None = None
        completed = False
        for page in range(1, self.max_pages + 1):
            params["Page"] = page
            # This request carries both an API key and the account email.  Never
            # inherit the redirect policy of a caller-supplied/shared client:
            # custom headers such as Authorization-Key and User-Agent are not
            # guaranteed to be stripped when a redirect crosses origins.
            try:
                self.checkpoint()
                data = _request_json(
                    self.client,
                    "GET",
                    "https://data.usajobs.gov/api/search",
                    headers=headers,
                    params=params,
                    follow_redirects=False,
                    reject_redirect=True,
                    max_bytes=self.max_response_bytes,
                    checkpoint=self.checkpoint,
                    timeout=self.request_timeout(),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if not records:
                    raise
                page_error = source_error_from_exception(exc)
                break
            if not isinstance(data, Mapping):
                raise ValueError("USAJobs response is not an object")
            search_result = data.get("SearchResult")
            if not isinstance(search_result, Mapping) or not isinstance(
                search_result.get("SearchResultItems"), list
            ):
                raise ValueError("USAJobs response is missing SearchResultItems")
            page_items = search_result["SearchResultItems"]
            records.extend(page_items)
            pages_fetched += 1
            raw_total = search_result.get("SearchResultCountAll")
            try:
                reported_total = int(raw_total) if raw_total is not None else None
            except (TypeError, ValueError, OverflowError):
                reported_total = None
            if reported_total is not None and reported_total >= len(records):
                total = max(total or 0, reported_total)
            if not page_items or len(page_items) < self.results_per_page:
                total = len(records) if total is None else max(total, len(records))
                completed = True
                break
            if total is not None and len(records) >= total:
                completed = True
                break
        return records, {
            "query": params["Keyword"],
            "location": self.location,
            "total": total,
            "pages_fetched": pages_fetched,
            "truncated": page_error is not None or not completed,
            "truncation_code": "page_read_error" if page_error else None,
            "truncation_reason": page_error.message if page_error else None,
            "truncation_retryable": page_error.retryable if page_error else False,
            "truncation_http_status": page_error.http_status if page_error else None,
            "truncation_retry_after_seconds": (
                page_error.retry_after_seconds if page_error else None
            ),
        }

    def _parse_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        descriptor = record.get("MatchedObjectDescriptor")
        if not isinstance(descriptor, Mapping):
            raise ValueError("missing MatchedObjectDescriptor")
        locations = descriptor.get("PositionLocation")
        location = _location(locations)
        remuneration = descriptor.get("PositionRemuneration")
        first_salary = (
            remuneration[0] if isinstance(remuneration, list) and remuneration else None
        )
        salary_text = _salary_text(first_salary)
        details = _mapping(_mapping(descriptor.get("UserArea")).get("Details"))
        description_parts = [
            str(details.get("JobSummary") or ""),
            str(descriptor.get("QualificationSummary") or ""),
        ]
        duties = details.get("MajorDuties")
        if isinstance(duties, list):
            description_parts.extend(str(duty) for duty in duties)
        elif duties:
            description_parts.append(str(duties))
        return ScanItem(
            source=self.source_key,
            source_id=str(_required(descriptor, "PositionID")),
            company=str(_required(descriptor, "OrganizationName")),
            title=str(_required(descriptor, "PositionTitle")),
            url=str(_required(descriptor, "PositionURI")),
            location=location,
            description="\n\n".join(part for part in description_parts if part),
            salary_text=salary_text,
            salary=normalize_salary(first_salary) if first_salary else None,
            remote=normalize_remote(details.get("TeleworkEligible"), location=location),
            posted_at=_datetime(descriptor.get("PublicationStartDate")),
            deadline=_datetime(descriptor.get("ApplicationCloseDate")),
            metadata={
                "department": descriptor.get("DepartmentName", ""),
                "grade": descriptor.get("JobGrade", []),
                "schedule": descriptor.get("PositionSchedule", []),
                "qualification_summary": descriptor.get("QualificationSummary", ""),
            },
        )


__all__ = [
    "AshbySource",
    "GreenhouseSource",
    "ICIMSSource",
    "JibeSource",
    "LeverSource",
    "SmartRecruitersSource",
    "TaleoSource",
    "USAJobsSource",
    "WorkableSource",
    "WorkdaySource",
]
