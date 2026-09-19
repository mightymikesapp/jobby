"""Pure state helpers for conservative job-posting liveness transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum


class LivenessState(StrEnum):
    ACTIVE = "active"
    STALE = "stale"
    CLOSED = "closed"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime:
    value = value or _now()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class LivenessSnapshot:
    state: LivenessState = LivenessState.ACTIVE
    consecutive_misses: int = 0
    last_checked_at: datetime | None = None
    last_observed_at: datetime | None = None
    closed_at: datetime | None = None
    explicit_closure: bool = False
    reason: str = ""

    def __post_init__(self) -> None:
        if self.consecutive_misses < 0:
            raise ValueError("consecutive_misses cannot be negative")
        if self.state is LivenessState.ACTIVE and self.consecutive_misses:
            raise ValueError("active postings cannot have consecutive misses")
        if self.state is LivenessState.STALE and self.consecutive_misses != 1:
            raise ValueError("stale postings require exactly one consecutive miss")
        if (
            self.state is LivenessState.CLOSED
            and not self.explicit_closure
            and self.consecutive_misses < 2
        ):
            raise ValueError("closed postings require two misses or explicit closure")
        if self.explicit_closure and self.state is not LivenessState.CLOSED:
            raise ValueError("explicit closure must be closed")


def record_observation(
    previous: LivenessSnapshot | None = None,
    *,
    at: datetime | None = None,
) -> LivenessSnapshot:
    """Record current source evidence and reset any prior misses.

    A later positive observation can reopen a previously closed posting; the
    audit layer can preserve the prior closure event separately.
    """

    observed_at = _aware(at)
    return LivenessSnapshot(
        state=LivenessState.ACTIVE,
        consecutive_misses=0,
        last_checked_at=observed_at,
        last_observed_at=observed_at,
    )


def record_miss(
    previous: LivenessSnapshot | None = None,
    *,
    at: datetime | None = None,
) -> LivenessSnapshot:
    """Mark one miss as stale and two consecutive misses as closed."""

    checked_at = _aware(at)
    previous = previous or LivenessSnapshot()
    if previous.state is LivenessState.CLOSED:
        return LivenessSnapshot(
            state=LivenessState.CLOSED,
            consecutive_misses=max(2, previous.consecutive_misses + 1),
            last_checked_at=checked_at,
            last_observed_at=previous.last_observed_at,
            closed_at=previous.closed_at or checked_at,
            explicit_closure=previous.explicit_closure,
            reason=previous.reason,
        )
    misses = previous.consecutive_misses + 1
    if misses == 1:
        return LivenessSnapshot(
            state=LivenessState.STALE,
            consecutive_misses=1,
            last_checked_at=checked_at,
            last_observed_at=previous.last_observed_at,
            reason="missing from one successful source scan",
        )
    return LivenessSnapshot(
        state=LivenessState.CLOSED,
        consecutive_misses=misses,
        last_checked_at=checked_at,
        last_observed_at=previous.last_observed_at,
        closed_at=checked_at,
        reason="missing from two consecutive successful source scans",
    )


def record_explicit_closure(
    previous: LivenessSnapshot | None = None,
    *,
    reason: str = "source explicitly reports the posting closed",
    at: datetime | None = None,
) -> LivenessSnapshot:
    """Close immediately when the source supplies explicit closure evidence."""

    closed_at = _aware(at)
    previous = previous or LivenessSnapshot()
    return LivenessSnapshot(
        state=LivenessState.CLOSED,
        consecutive_misses=previous.consecutive_misses,
        last_checked_at=closed_at,
        last_observed_at=previous.last_observed_at,
        closed_at=closed_at,
        explicit_closure=True,
        reason=reason.strip() or "source explicitly reports the posting closed",
    )


def apply_liveness_evidence(
    previous: LivenessSnapshot | None,
    *,
    observed: bool,
    explicit_closed: bool = False,
    closure_reason: str = "",
    at: datetime | None = None,
) -> LivenessSnapshot:
    """Apply one successful source check to a liveness snapshot."""

    if explicit_closed:
        return record_explicit_closure(previous, reason=closure_reason, at=at)
    if observed:
        return record_observation(previous, at=at)
    return record_miss(previous, at=at)


__all__ = [
    "LivenessSnapshot",
    "LivenessState",
    "apply_liveness_evidence",
    "record_explicit_closure",
    "record_miss",
    "record_observation",
]
