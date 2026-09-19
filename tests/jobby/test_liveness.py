from datetime import datetime, timezone

from jobby.liveness import (
    LivenessState,
    apply_liveness_evidence,
    record_explicit_closure,
    record_miss,
    record_observation,
)


T0 = datetime(2026, 7, 10, 7, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 7, 11, 7, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 7, 12, 7, 0, tzinfo=timezone.utc)


def test_one_miss_is_stale_and_two_consecutive_misses_are_closed() -> None:
    active = record_observation(at=T0)
    stale = record_miss(active, at=T1)
    closed = record_miss(stale, at=T2)

    assert active.state is LivenessState.ACTIVE
    assert stale.state is LivenessState.STALE
    assert stale.consecutive_misses == 1
    assert closed.state is LivenessState.CLOSED
    assert closed.consecutive_misses == 2
    assert closed.closed_at == T2
    assert closed.explicit_closure is False


def test_observation_resets_a_miss_and_can_reopen() -> None:
    stale = record_miss(record_observation(at=T0), at=T1)
    active = record_observation(stale, at=T2)

    assert active.state is LivenessState.ACTIVE
    assert active.consecutive_misses == 0
    assert active.last_observed_at == T2
    assert active.closed_at is None


def test_explicit_closure_closes_immediately_with_evidence() -> None:
    active = record_observation(at=T0)
    closed = record_explicit_closure(active, reason="API returned status=closed", at=T1)

    assert closed.state is LivenessState.CLOSED
    assert closed.consecutive_misses == 0
    assert closed.explicit_closure is True
    assert closed.reason == "API returned status=closed"
    assert closed.closed_at == T1


def test_apply_liveness_evidence_prefers_explicit_closure() -> None:
    active = record_observation(at=T0)
    closed = apply_liveness_evidence(
        active,
        observed=True,
        explicit_closed=True,
        closure_reason="explicit source flag",
        at=T1,
    )
    assert closed.state is LivenessState.CLOSED
    assert closed.reason == "explicit source flag"


def test_additional_misses_keep_a_closed_posting_closed() -> None:
    closed = record_miss(record_miss(record_observation(at=T0), at=T1), at=T2)
    later = record_miss(closed, at=datetime(2026, 7, 13, 7, 0, tzinfo=timezone.utc))
    assert later.state is LivenessState.CLOSED
    assert later.consecutive_misses == 3
    assert later.closed_at == T2
