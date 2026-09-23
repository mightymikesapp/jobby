"""Small registry-driven adapters for public normalized job catalogs.

These adapters deliberately share one conservative transport contract.  A
catalog observation is normalized by the scanner and never becomes an
authoritative application record merely because it came from an external
service.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import time
from typing import Any

import httpx

from jobby.normalization import is_public_http_url, normalize_remote, normalize_salary

from .base import (
    MAX_RESULT_ITEMS,
    JobSource,
    ScanItem,
    ScanStatus,
    SourceError,
    SourceResult,
    bounded_source_key,
    source_error_from_exception,
)


def _text(value: Any) -> str:
    if isinstance(value, Mapping):
        for key in ("name", "display_name", "label", "value"):
            nested = value.get(key)
            if nested:
                return str(nested).strip()
    return str(value or "").strip()


def _posted_at(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


class CatalogSource(JobSource):
    """Read one configured JSON catalog endpoint with bounded public output."""

    name = "catalog"

    def __init__(
        self,
        client: httpx.Client,
        *,
        endpoint: str,
        company: str,
        provider: str,
        credential: str | None = None,
        source_key: str | None = None,
    ) -> None:
        super().__init__(
            client,
            source_key=source_key or bounded_source_key(provider, company),
            concurrency_key=provider,
        )
        self.endpoint = endpoint
        self.company = company
        self.provider = provider
        self.credential = credential
        if not is_public_http_url(endpoint):
            raise ValueError("catalog endpoint must target a public HTTPS host")

    def scan(self, query: str | None = None) -> SourceResult:
        started = datetime.now(timezone.utc)
        try:
            headers = {"Accept": "application/json"}
            if self.credential:
                headers["Authorization"] = f"Bearer {self.credential}"
            with self.client.stream(
                "GET",
                self.endpoint,
                params={"q": query} if query else None,
                headers=headers,
                timeout=self.request_timeout(30),
                follow_redirects=False,
            ) as response:
                if response.is_redirect:
                    raise ValueError(
                        "catalog response returned a redirect; refusing target"
                    )
                response.raise_for_status()
                content_type = response.headers.get("content-type", "").casefold()
                if content_type and "json" not in content_type:
                    raise ValueError("catalog response is not JSON")
                declared = response.headers.get("content-length")
                if declared is not None:
                    try:
                        if int(declared) > self.max_response_bytes:
                            raise ValueError(
                                "catalog response exceeds the configured safety limit"
                            )
                    except (TypeError, ValueError, OverflowError) as exc:
                        if isinstance(exc, ValueError) and str(exc).startswith(
                            "catalog response"
                        ):
                            raise
                payload_bytes = bytearray()
                for chunk in response.iter_bytes():
                    self.checkpoint()
                    if len(payload_bytes) + len(chunk) > self.max_response_bytes:
                        raise ValueError(
                            "catalog response exceeds the configured safety limit"
                        )
                    payload_bytes.extend(chunk)
            try:
                payload = json.loads(payload_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("catalog response is not valid JSON") from exc
            if isinstance(payload, Mapping):
                records = payload.get(
                    "jobs", payload.get("results", payload.get("data", []))
                )
            else:
                records = payload
            if not isinstance(records, list):
                raise ValueError(
                    "catalog response must contain a jobs/results/data list"
                )
            items: list[ScanItem] = []
            errors: list[SourceError] = []
            truncated = len(records) > MAX_RESULT_ITEMS
            for index, record in enumerate(records[:MAX_RESULT_ITEMS]):
                self.checkpoint()
                if not isinstance(record, Mapping):
                    errors.append(
                        SourceError(
                            code="invalid_record",
                            message="catalog record is not an object",
                            item_index=index,
                        )
                    )
                    continue
                title = _text(record.get("title") or record.get("name"))
                url = _text(
                    record.get("url")
                    or record.get("apply_url")
                    or record.get("job_url")
                )
                if not title or not url:
                    errors.append(
                        SourceError(
                            code="incomplete_record",
                            message="catalog record is missing title or URL",
                            item_index=index,
                        )
                    )
                    continue
                try:
                    items.append(
                        ScanItem(
                            source=self.source_key,
                            source_id=_text(
                                record.get("id")
                                or record.get("slug")
                                or f"{index}:{title}"
                            )[:500],
                            company=_text(record.get("company") or self.company)[:300],
                            title=title[:500],
                            url=url,
                            location=_text(record.get("location"))[:500],
                            description=_text(record.get("description"))[:500_000],
                            salary_text=_text(
                                record.get("compensation") or record.get("salary")
                            )[:10_000],
                            posted_at=_posted_at(
                                record.get("posted_at") or record.get("published_at")
                            ),
                            metadata={
                                "catalog_provider": self.provider,
                                "retrieved_at": started.isoformat(),
                            },
                        )
                    )
                except ValueError as exc:
                    error = source_error_from_exception(exc)
                    errors.append(
                        SourceError(
                            code=error.code, message=error.message, item_index=index
                        )
                    )
            if truncated:
                errors.append(
                    SourceError(
                        code="result_limit",
                        message=f"catalog returned more than {MAX_RESULT_ITEMS:,} records; the result was truncated safely",
                    )
                )
            status = (
                ScanStatus.PARTIAL
                if errors and items
                else ScanStatus.FAILED
                if errors
                else ScanStatus.SUCCEEDED
            )
            return SourceResult(
                source=self.source_key,
                status=status,
                items=tuple(items),
                errors=tuple(errors),
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                metadata={
                    "retrieved_at": started.isoformat(),
                    "external_catalog": True,
                    "truncated": truncated,
                },
            )
        except Exception as exc:
            error = source_error_from_exception(exc)
            return SourceResult(
                source=self.source_key,
                status=ScanStatus.FAILED,
                errors=(error,),
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                metadata={"external_catalog": True},
            )


class ProviderCatalogSource(CatalogSource):
    """Provider-aware catalog adapter with an explicit response contract.

    The five adapters below intentionally do not share a generic record
    mapping.  They share only bounded transport, retry, and result handling;
    request parameters, response envelopes, and field paths are provider
    contracts.  A provider without a stable contract returns a diagnostic
    failure instead of silently treating an arbitrary JSON endpoint as jobs.
    """

    contract_version = "1"
    stable_contract = True
    page_size = 100
    max_pages = 100
    query_parameter = "q"
    page_parameter = "page"
    size_parameter = "page_size"
    field_map: dict[str, tuple[str, ...]] = {}
    envelope_keys: tuple[str, ...] = ("jobs", "results", "data")

    def __init__(
        self,
        client: httpx.Client,
        *,
        endpoint: str,
        company: str,
        credential: str | None = None,
        source_key: str | None = None,
        max_pages: int | None = None,
        page_size: int | None = None,
        contract_version: str | None = None,
    ) -> None:
        super().__init__(
            client,
            endpoint=endpoint,
            company=company,
            provider=self.name,
            credential=credential,
            source_key=source_key,
        )
        if max_pages is not None:
            if not 1 <= max_pages <= self.max_pages:
                raise ValueError("provider max_pages is out of bounds")
            self.max_pages = max_pages
        if page_size is not None:
            if not 1 <= page_size <= 1_000:
                raise ValueError("provider page_size is out of bounds")
            self.page_size = page_size
        if contract_version is not None:
            self.contract_version = str(contract_version)[:40]

    def capabilities(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "contract_version": self.contract_version,
            "stable_contract": self.stable_contract,
            "pagination": "cursor_or_page",
            "authentication": "keyring_only",
            "supports": [
                "salary",
                "location",
                "remote_status",
                "posting_date",
                "application_url",
            ],
        }

    def scan(self, query: str | None = None) -> SourceResult:
        started = datetime.now(timezone.utc)
        if not self.stable_contract:
            return SourceResult(
                source=self.source_key,
                status=ScanStatus.FAILED,
                errors=(
                    SourceError(
                        code="unsupported_contract",
                        message=(
                            f"{self.name} has no stable public listing contract; "
                            "configure a supported provider endpoint or use manual capture"
                        ),
                    ),
                ),
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                metadata={"capabilities": self.capabilities()},
            )
        items: list[ScanItem] = []
        errors: list[SourceError] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        truncated = False
        try:
            for page in range(1, self.max_pages + 1):
                self.checkpoint()
                payload = self._request_page(query=query, page=page, cursor=cursor)
                records, next_cursor = self._records_and_cursor(payload, page=page)
                for index, record in enumerate(records):
                    absolute_index = len(items) + index
                    self.checkpoint()
                    if len(items) >= MAX_RESULT_ITEMS:
                        truncated = True
                        break
                    if not isinstance(record, Mapping):
                        errors.append(
                            SourceError(
                                code="malformed_item",
                                message="provider record is not an object",
                                item_index=absolute_index,
                            )
                        )
                        continue
                    try:
                        items.append(
                            self._parse_provider_record(record, absolute_index)
                        )
                    except (KeyError, TypeError, ValueError) as exc:
                        error = source_error_from_exception(exc)
                        errors.append(
                            SourceError(
                                code=error.code,
                                message=error.message,
                                item_index=absolute_index,
                            )
                        )
                if len(items) >= MAX_RESULT_ITEMS:
                    truncated = True
                    break
                if not next_cursor:
                    break
                if next_cursor in seen_cursors:
                    errors.append(
                        SourceError(
                            code="pagination_loop",
                            message="provider returned a repeated continuation cursor",
                        )
                    )
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            else:
                if cursor:
                    truncated = True
                    errors.append(
                        SourceError(
                            code="pagination_limit",
                            message="provider pagination exceeded the configured page limit",
                        )
                    )
        except InterruptedError:
            raise
        except Exception as exc:
            error = source_error_from_exception(exc)
            errors.append(error)
        if truncated and not any(error.code == "pagination_limit" for error in errors):
            errors.append(
                SourceError(
                    code="result_limit",
                    message="provider result was truncated at the configured safety limit",
                )
            )
        status = (
            ScanStatus.PARTIAL
            if items and errors
            else ScanStatus.FAILED
            if errors
            else ScanStatus.SUCCEEDED
        )
        return SourceResult(
            source=self.source_key,
            status=status,
            items=tuple(items),
            errors=tuple(errors),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            metadata={
                "provider": self.name,
                "capabilities": self.capabilities(),
                "pages": len(seen_cursors) + (1 if items or errors else 0),
                "truncated": truncated,
            },
        )

    def _request_page(self, *, query: str | None, page: int, cursor: str | None) -> Any:
        params = self._request_params(query=query, page=page, cursor=cursor)
        headers = {"Accept": "application/json"}
        if self.credential:
            headers["Authorization"] = f"Bearer {self.credential}"
        attempts = 3
        for attempt in range(attempts):
            try:
                with self.client.stream(
                    "GET",
                    self.endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout(30),
                    follow_redirects=False,
                ) as response:
                    if response.is_redirect:
                        raise ValueError(
                            "provider response returned a redirect; refusing target"
                        )
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").casefold()
                    if content_type and "json" not in content_type:
                        raise ValueError("provider response is not JSON")
                    declared = response.headers.get("content-length")
                    if declared is not None and int(declared) > self.max_response_bytes:
                        raise ValueError(
                            "provider response exceeds the configured safety limit"
                        )
                    body = bytearray()
                    for chunk in response.iter_bytes():
                        self.checkpoint()
                        if len(body) + len(chunk) > self.max_response_bytes:
                            raise ValueError(
                                "provider response exceeds the configured safety limit"
                            )
                        body.extend(chunk)
                    return json.loads(body)
            except httpx.HTTPStatusError as exc:
                if (
                    exc.response.status_code not in {429, 500, 502, 503, 504}
                    or attempt + 1 == attempts
                ):
                    raise
                retry_after = exc.response.headers.get("Retry-After", "0")
                try:
                    delay = min(0.25, max(0.0, float(retry_after)))
                except ValueError:
                    delay = 0.05 * (2**attempt)
                time.sleep(delay)
            except httpx.TimeoutException:
                if attempt + 1 == attempts:
                    raise
                time.sleep(0.05 * (2**attempt))

        raise RuntimeError("provider request retry loop exhausted")

    def _request_params(
        self, *, query: str | None, page: int, cursor: str | None
    ) -> dict[str, str | int]:
        params: dict[str, str | int] = {
            self.page_parameter: page,
            self.size_parameter: self.page_size,
        }
        if query:
            params[self.query_parameter] = query
        if cursor:
            params["cursor"] = cursor
        return params

    def _records_and_cursor(
        self, payload: Any, *, page: int
    ) -> tuple[list[Any], str | None]:
        if isinstance(payload, list):
            return payload, None
        if not isinstance(payload, Mapping):
            raise ValueError("provider response must be an object or array")
        records: Any = []
        for key in self.envelope_keys:
            if key in payload:
                records = payload[key]
                break
        if isinstance(records, Mapping):
            records = records.get("items", records.get("results", []))
        if not isinstance(records, list):
            raise ValueError("provider job envelope is not a list")
        next_value = (
            payload.get("nextCursor")
            or payload.get("next_cursor")
            or payload.get("nextPageToken")
            or payload.get("next_page_token")
            or payload.get("next")
        )
        if isinstance(next_value, Mapping):
            next_value = next_value.get("cursor") or next_value.get("href")
        if next_value is None and payload.get(
            "hasMore", payload.get("has_more", False)
        ):
            next_value = str(page * self.page_size)
        return records, str(next_value)[:2_000] if next_value else None

    def _parse_provider_record(self, record: Mapping[str, Any], index: int) -> ScanItem:
        title = _value_text(_path(record, self.field_map["title"]))
        url = _value_text(_path(record, self.field_map["url"]))
        if not title or not url:
            raise ValueError("provider record is missing a title or application URL")
        source_id = (
            _value_text(_path(record, self.field_map["id"])) or f"{index}:{title}"
        )
        company = (
            _value_text(_path(record, self.field_map.get("company", ())))
            or self.company
        )
        location = _value_text(_path(record, self.field_map.get("location", ())))
        description = _value_text(_path(record, self.field_map.get("description", ())))
        salary_value = _path(record, self.field_map.get("salary", ()))
        salary = normalize_salary(salary_value)
        salary_text = _value_text(salary_value)
        remote_value = _path(record, self.field_map.get("remote", ()))
        posted = _value_text(_path(record, self.field_map.get("posted_at", ())))
        return ScanItem(
            source=self.source_key,
            source_id=source_id,
            company=company,
            title=title,
            url=url,
            location=location,
            description=description,
            salary_text=salary_text,
            salary=salary,
            remote=normalize_remote(remote_value, location=location),
            posted_at=_posted_at(posted),
            metadata={
                "provider": self.name,
                "contract_version": self.contract_version,
                "source_index": index,
            },
        )


def _path(record: Mapping[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value: Any = record
        for part in path.split("."):
            if not isinstance(value, Mapping):
                value = None
                break
            value = next(
                (
                    item
                    for key, item in value.items()
                    if str(key).casefold() == part.casefold()
                ),
                None,
            )
        if value not in (None, "", [], {}):
            return value
    return None


def _value_text(value: Any) -> str:
    if isinstance(value, Mapping):
        for key in ("name", "label", "value", "displayName", "display_name", "url"):
            if value.get(key):
                return str(value[key]).strip()
        return ", ".join(str(item).strip() for item in value.values() if item)[:500]
    if isinstance(value, list):
        return "; ".join(_value_text(item) for item in value if _value_text(item))[:500]
    return str(value or "").strip()


class EightfoldSource(ProviderCatalogSource):
    name = "eightfold"
    field_map = {
        "id": ("jobId", "id", "requisitionId"),
        "title": ("jobTitle", "title", "name"),
        "url": ("applyUrl", "applicationUrl", "url"),
        "company": ("companyName", "company.name", "company"),
        "location": ("locations", "location", "jobLocation"),
        "description": ("jobDescription", "description"),
        "salary": ("salary", "compensation", "salaryRange"),
        "remote": ("workplaceType", "remote", "remoteType"),
        "posted_at": ("postedDate", "publishedAt", "posted_at"),
    }


class OracleHCMSource(ProviderCatalogSource):
    name = "oracle_hcm"
    query_parameter = "keyword"
    page_parameter = "offset"
    size_parameter = "limit"
    field_map = {
        "id": ("requisitionId", "Id", "id"),
        "title": ("Title", "title", "JobTitle"),
        "url": ("ExternalJobURL", "externalJobUrl", "url"),
        "company": ("OrganizationName", "company.name", "company"),
        "location": ("PrimaryLocation", "location", "locations"),
        "description": ("ExternalDescription", "description"),
        "salary": ("Salary", "salary", "compensation"),
        "remote": ("WorkplaceType", "remote", "remoteType"),
        "posted_at": ("PostedDate", "postedDate", "publishedAt"),
    }

    def _request_params(
        self, *, query: str | None, page: int, cursor: str | None
    ) -> dict[str, str | int]:
        params = {"offset": (page - 1) * self.page_size, "limit": self.page_size}
        if query:
            params["keyword"] = query
        if cursor and cursor.isdigit():
            params["offset"] = int(cursor)
        return params


class RipplingSource(ProviderCatalogSource):
    name = "rippling"
    query_parameter = "search"
    field_map = {
        "id": ("id", "jobId", "requisitionId"),
        "title": ("name", "title", "jobTitle"),
        "url": ("url", "applyUrl", "applicationUrl"),
        "company": ("company.name", "company", "companyName"),
        "location": ("location", "locations", "jobLocation"),
        "description": ("description", "jobDescription"),
        "salary": ("compensation", "salary", "salaryRange"),
        "remote": ("workplaceType", "remoteType", "remote"),
        "posted_at": ("createdAt", "postedAt", "publishedAt"),
    }


class PaylocitySource(ProviderCatalogSource):
    name = "paylocity"
    query_parameter = "keyword"
    page_parameter = "pageNumber"
    size_parameter = "pageSize"
    field_map = {
        "id": ("jobId", "requisitionId", "id"),
        "title": ("jobTitle", "title", "name"),
        "url": ("applyUrl", "applicationUrl", "url"),
        "company": ("companyName", "company.name", "company"),
        "location": ("jobLocation", "location", "locations"),
        "description": ("jobDescription", "description"),
        "salary": ("salaryRange", "salary", "compensation"),
        "remote": ("workplaceType", "remoteType", "remote"),
        "posted_at": ("postedDate", "publishedAt", "posted_at"),
    }


class FreehireSource(ProviderCatalogSource):
    name = "freehire"
    stable_contract = False
    field_map = {}


__all__ = [
    "CatalogSource",
    "ProviderCatalogSource",
    "EightfoldSource",
    "FreehireSource",
    "OracleHCMSource",
    "PaylocitySource",
    "RipplingSource",
]
