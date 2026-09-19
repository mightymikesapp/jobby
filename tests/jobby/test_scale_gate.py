"""Always-small and explicitly opt-in exact scale-gate coverage."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path

import pytest

from jobby.scale_gate import (
    RELEASE_PROFILE,
    REPORT_SCHEMA,
    SMOKE_PROFILE,
    compare_to_recorded_baseline,
    run_scale_gate,
)


def test_scale_gate_smoke_records_cardinality_latency_memory_and_storage(
    tmp_path: Path,
) -> None:
    report = run_scale_gate(tmp_path / "smoke", profile=SMOKE_PROFILE)

    assert report.passed, report.violations
    assert report.counts["jobs"] == 250
    assert report.counts["applications"] == 50
    assert report.counts["source_state_rows"] == 250
    assert report.counts["source_state_seen_total"] == 2_250
    assert report.counts["export_rows"] == 250
    assert report.derived["source_updates_applied"] == 2_000
    assert report.derived["source_update_growth_bytes_per_operation"] <= 64
    assert report.operations["job_page_first"].peak_python_bytes > 0
    assert report.operations["streaming_csv_export"].peak_python_bytes > 0
    assert report.storage["after_export"].total_bytes > 0
    assert report.integrity_ok is True

    destination = report.write_json(tmp_path / "report.json")
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["schema"] == REPORT_SCHEMA
    assert payload["passed"] is True
    assert payload["profile"] == {
        "applications": 50,
        "jobs": 250,
        "name": "smoke",
        "page_size": 50,
        "source_state_updates": 2_000,
    }
    assert payload["artifacts"]["export_bytes"] > 0


def test_recorded_baseline_comparison_rejects_regressions(tmp_path: Path) -> None:
    report = run_scale_gate(tmp_path / "baseline", profile=SMOKE_PROFILE)
    baseline = report.to_dict()

    assert compare_to_recorded_baseline(report, baseline) == ()

    faster_baseline = deepcopy(baseline)
    measured = baseline["operations"]["job_page_tail"]["elapsed_ms"]
    faster_baseline["operations"]["job_page_tail"]["elapsed_ms"] = measured / 2
    violations = compare_to_recorded_baseline(
        report,
        faster_baseline,
        max_regression_fraction=0.25,
    )
    assert len(violations) == 1
    assert "operations.job_page_tail.elapsed_ms regressed" in violations[0]


@pytest.mark.scale
@pytest.mark.skipif(
    os.environ.get("JOBBY_RUN_EXACT_SCALE") != "1",
    reason="set JOBBY_RUN_EXACT_SCALE=1 to run the 100k/10k/1m release gate",
)
def test_exact_release_scale_gate(tmp_path: Path) -> None:
    report = run_scale_gate(tmp_path / "release", profile=RELEASE_PROFILE)

    assert report.passed, report.violations
    assert report.counts["jobs"] == 100_000
    assert report.counts["applications"] == 10_000
    assert report.counts["source_state_rows"] == 100_000
    assert report.derived["source_updates_applied"] == 1_000_000
    assert report.counts["export_rows"] == 100_000
    report.write_json(tmp_path / "release-scale-report.json")
