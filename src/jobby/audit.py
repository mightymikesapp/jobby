"""Append-only activity history helpers."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
import math
from pathlib import Path
import re
from itertools import islice
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import AuditEvent


REDACTED = "[REDACTED]"
MAX_AUDIT_DEPTH = 12
MAX_AUDIT_ITEMS = 1_000
MAX_AUDIT_STRING = 20_000
SENSITIVE_KEY_RE = re.compile(
    r"(?:^token$|^secret$|^authorization$|^cookie$|"
    r"(?:^|[_-])(?:api[_-]?key|private[_-]?key|secret[_-]?key|"
    r"access[_-]?token|refresh[_-]?token|oauth[_-]?token|id[_-]?token|auth[_-]?token|"
    r"password|passwd|client[_-]?secret|credentials?)(?:$|[_-])|"
    r"(?:^|[_-])(?:authorization|cookie)[_-](?:header|value)$)",
    re.IGNORECASE,
)
SENSITIVE_TEXT_PATTERNS = (
    re.compile(
        r"\b(?:Authorization|Proxy-Authorization|Cookie|Set-Cookie)\s*:\s*[^\r\n]+",
        re.IGNORECASE,
    ),
    re.compile(r"\bBearer\s+[^\s,;]+", re.IGNORECASE),
    re.compile(r"\bBasic\s+[A-Za-z0-9+/=]+", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}"),
    re.compile(r"\bya29\.[A-Za-z0-9._-]+"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{20,}"),
    re.compile(r"\bGOCSPX-[0-9A-Za-z_-]{10,}"),
    re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----.*",
        re.DOTALL,
    ),
)
KEY_VALUE_SECRET_RE = re.compile(
    r"(?P<key>api[_-]?key|private[_-]?key|secret[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|oauth[_-]?token|id[_-]?token|auth[_-]?token|"
    r"password|passwd|client[_-]?secret|authorization|proxy[_-]?authorization|"
    r"cookie|set[_-]?cookie)"
    r"(?P<separator>\s*[:=]\s*)(?P<value>[^\s,;]+)",
    re.IGNORECASE,
)


def redact_text(value: str) -> str:
    truncated = len(value) > MAX_AUDIT_STRING
    if truncated:
        value = value[:MAX_AUDIT_STRING]
    for pattern in SENSITIVE_TEXT_PATTERNS:
        value = pattern.sub(REDACTED, value)
    value = KEY_VALUE_SECRET_RE.sub(
        lambda match: f"{match.group('key')}{match.group('separator')}{REDACTED}",
        value,
    )
    if truncated:
        value = f"{value}…[truncated]"
    return value


def json_safe(value: Any, *, _depth: int = 0) -> Any:
    if _depth > MAX_AUDIT_DEPTH:
        return "[maximum depth exceeded]"
    if isinstance(value, float):
        return value if math.isfinite(value) else "[non-finite number]"
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return redact_text(value) if isinstance(value, str) else value
    if isinstance(value, int):
        return (
            value if value.bit_length() <= 4_096 else "[integer exceeds supported size]"
        )
    if isinstance(value, Enum):
        return json_safe(value.value, _depth=_depth + 1)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return redact_text(str(value))
    if is_dataclass(value) and not isinstance(value, type):
        result: dict[str, Any] = {}
        value_fields = fields(value)
        for item in value_fields[:MAX_AUDIT_ITEMS]:
            try:
                nested = getattr(value, item.name)
            except Exception:
                nested = "[field could not be read]"
            result[item.name] = json_safe(nested, _depth=_depth + 1)
        if len(value_fields) > MAX_AUDIT_ITEMS:
            result["__truncated__"] = (
                f"{len(value_fields) - MAX_AUDIT_ITEMS} fields omitted"
            )
        return result
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_AUDIT_ITEMS:
                result["__truncated__"] = (
                    f"{len(value) - MAX_AUDIT_ITEMS} items omitted"
                )
                break
            key_text = str(key)
            safe_key = redact_text(key_text)
            if safe_key in result and safe_key != key_text:
                safe_key = f"{safe_key}#{index}"
            result[safe_key] = (
                REDACTED
                if SENSITIVE_KEY_RE.search(key_text)
                else json_safe(item, _depth=_depth + 1)
            )
        return result
    if isinstance(value, (list, tuple)):
        items = value[:MAX_AUDIT_ITEMS]
        result = [json_safe(item, _depth=_depth + 1) for item in items]
        if len(value) > MAX_AUDIT_ITEMS:
            result.append(f"[{len(value) - MAX_AUDIT_ITEMS} items omitted]")
        return result
    if isinstance(value, set):
        items = list(islice(value, MAX_AUDIT_ITEMS))
        result = [json_safe(item, _depth=_depth + 1) for item in items]
        if len(value) > MAX_AUDIT_ITEMS:
            result.append(f"[{len(value) - MAX_AUDIT_ITEMS} items omitted]")
        return result
    try:
        rendered = str(value)
    except Exception:
        rendered = f"[unserializable {type(value).__name__}]"
    return redact_text(rendered)


def record_audit(
    session: Session,
    *,
    action: str,
    entity_type: str,
    entity_id: str | None = None,
    actor: str = "user",
    before: Any = None,
    after: Any = None,
    detail: str | None = None,
    correlation_id: str | None = None,
) -> AuditEvent:
    event = AuditEvent(
        action=redact_text(action)[:200],
        entity_type=redact_text(entity_type)[:100],
        entity_id=redact_text(entity_id)[:100] if entity_id is not None else None,
        actor=redact_text(actor)[:100],
        before_json=json_safe(before) if before is not None else None,
        after_json=json_safe(after) if after is not None else None,
        detail=redact_text(detail) if detail is not None else None,
        correlation_id=(
            redact_text(correlation_id)[:100] if correlation_id is not None else None
        ),
    )
    session.add(event)
    return event


def recent_activity(session: Session, limit: int = 100) -> list[AuditEvent]:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("activity limit must be an integer")
    if not 1 <= limit <= 1_000:
        raise ValueError("activity limit must be between 1 and 1000")
    return list(
        session.scalars(
            select(AuditEvent).order_by(AuditEvent.occurred_at.desc()).limit(limit)
        )
    )
