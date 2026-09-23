"""Explicit, bounded serializers for the CLI/MCP application boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from enum import Enum
import json
from typing import Any

MAX_RESPONSE_BYTES = 1_048_576
MAX_RESPONSE_DEPTH = 8
MAX_RESPONSE_ITEMS = 200
MAX_RESPONSE_STRING = 100_000


def bounded_text(value: object, limit: int) -> tuple[str | None, int]:
    if value is None:
        return None, 0
    text = str(value).replace("\x00", "�")
    if len(text) <= limit:
        return text, 0
    return text[:limit], len(text) - limit


def _convert(value: Any, *, depth: int = 0) -> Any:
    if depth >= MAX_RESPONSE_DEPTH:
        return "[maximum depth exceeded]"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _convert(value.value, depth=depth + 1)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:MAX_RESPONSE_STRING]
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_RESPONSE_ITEMS:
                result["_omitted_items"] = len(value) - MAX_RESPONSE_ITEMS
                break
            result[str(key)[:300]] = _convert(item, depth=depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = list(value)
        result = [
            _convert(item, depth=depth + 1) for item in values[:MAX_RESPONSE_ITEMS]
        ]
        if len(values) > MAX_RESPONSE_ITEMS:
            result.append({"_omitted_items": len(values) - MAX_RESPONSE_ITEMS})
        return result
    if isinstance(value, (set, frozenset)):
        values = list(value)
        result = [
            _convert(item, depth=depth + 1) for item in values[:MAX_RESPONSE_ITEMS]
        ]
        if len(values) > MAX_RESPONSE_ITEMS:
            result.append({"_omitted_items": len(values) - MAX_RESPONSE_ITEMS})
        return result
    return str(value)[:MAX_RESPONSE_STRING]


def json_value(value: Any) -> Any:
    return _convert(value)


def _encoded(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), default=str
    ).encode("utf-8")


def _fit(value: Any, budget: int) -> tuple[Any, int]:
    if budget <= 64:
        return "[truncated]", 1
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        if len(encoded) <= budget:
            return value, 0
        keep = max(0, budget - 80)
        text = encoded[:keep].decode("utf-8", errors="ignore")
        return f"{text}…[truncated]", len(value) - len(text)
    if isinstance(value, list):
        fitted: list[Any] = []
        omitted = 0
        for item in value:
            candidate, item_omitted = _fit(
                item, max(128, budget - len(_encoded(fitted)))
            )
            if len(_encoded(fitted + [candidate])) > budget:
                omitted += 1 + item_omitted
                break
            fitted.append(candidate)
            omitted += item_omitted
        omitted += max(0, len(value) - len(fitted))
        return fitted, omitted
    if isinstance(value, dict):
        fitted: dict[str, Any] = {}
        omitted = 0
        for key, item in value.items():
            if key.startswith("_"):
                continue
            candidate, item_omitted = _fit(
                item, max(128, budget - len(_encoded(fitted)))
            )
            if len(_encoded({**fitted, key: candidate})) > budget:
                omitted += 1 + item_omitted
                continue
            fitted[key] = candidate
            omitted += item_omitted
        return fitted, omitted
    return value, 0


def enforce_response_budget(value: Any, *, budget: int = MAX_RESPONSE_BYTES) -> Any:
    if not 16_384 <= budget <= 100 * 1024 * 1024:
        raise ValueError("response budget must be between 16 KiB and 100 MiB")
    converted = json_value(value)
    if len(_encoded(converted)) <= budget:
        return converted
    fitted, omitted = _fit(converted, budget - 128)
    metadata = {"truncated": True, "omitted_items": omitted, "max_bytes": budget}
    if isinstance(fitted, dict):
        fitted["_response"] = metadata
        if len(_encoded(fitted)) <= budget:
            return fitted
    return {"value": "[response truncated]", "_response": metadata}


def model_dto(
    row: object, fields: Sequence[str], *, limits: Mapping[str, int] | None = None
) -> dict[str, Any]:
    limits = limits or {}
    result: dict[str, Any] = {}
    omitted: dict[str, int] = {}
    for field in fields:
        value = getattr(row, field, None)
        if field in limits and value is not None:
            value, count = bounded_text(value, limits[field])
            if count:
                omitted[field] = count
        result[field] = value
    if omitted:
        result["_truncated_fields"] = omitted
    return result


__all__ = [
    "MAX_RESPONSE_BYTES",
    "MAX_RESPONSE_DEPTH",
    "MAX_RESPONSE_ITEMS",
    "bounded_text",
    "enforce_response_budget",
    "json_value",
    "model_dto",
]
