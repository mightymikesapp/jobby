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
from typing import Any

import httpx

from jobby.normalization import is_public_http_url

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


class EightfoldSource(CatalogSource):
    name = "eightfold"

    def __init__(self, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(client, provider="eightfold", **kwargs)


class OracleHCMSource(CatalogSource):
    name = "oracle_hcm"

    def __init__(self, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(client, provider="oracle_hcm", **kwargs)


class RipplingSource(CatalogSource):
    name = "rippling"

    def __init__(self, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(client, provider="rippling", **kwargs)


class PaylocitySource(CatalogSource):
    name = "paylocity"

    def __init__(self, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(client, provider="paylocity", **kwargs)


class FreehireSource(CatalogSource):
    name = "freehire"

    def __init__(self, client: httpx.Client, **kwargs: Any) -> None:
        super().__init__(client, provider="freehire", **kwargs)


__all__ = [
    "CatalogSource",
    "EightfoldSource",
    "FreehireSource",
    "OracleHCMSource",
    "PaylocitySource",
    "RipplingSource",
]
