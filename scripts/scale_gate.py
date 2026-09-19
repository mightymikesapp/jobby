#!/usr/bin/env python3
"""Run Jobby's isolated smoke or exact release-scale performance gate."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from jobby.scale_gate import (
    RELEASE_PROFILE,
    SMOKE_PROFILE,
    compare_to_recorded_baseline,
    load_report,
    run_scale_gate,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline temporary Jobby database and record fixture, SQL page, "
            "storage-growth, memory, and streaming-export measurements."
        )
    )
    parser.add_argument(
        "--profile",
        choices=("smoke", "release"),
        default="smoke",
        help="release is the exact 100k/10k/1m gate and requires confirmation",
    )
    parser.add_argument(
        "--confirm-exact-scale",
        action="store_true",
        help="required with --profile release to prevent an accidental long run",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="empty directory in which to retain the fixture database and CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("scale-gate-report.json"),
        help="machine-readable report path (default: scale-gate-report.json)",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="previous same-profile report to enforce as a recorded baseline",
    )
    parser.add_argument(
        "--max-regression-percent",
        type=float,
        default=25.0,
        help="maximum increase over baseline metrics (default: 25)",
    )
    return parser


def _run(args: argparse.Namespace, work_dir: Path) -> int:
    profile = RELEASE_PROFILE if args.profile == "release" else SMOKE_PROFILE
    report = run_scale_gate(work_dir, profile=profile)
    if args.baseline is not None:
        regressions = compare_to_recorded_baseline(
            report,
            load_report(args.baseline),
            max_regression_fraction=args.max_regression_percent / 100.0,
        )
        report = report.with_violations(regressions)
    destination = report.write_json(args.output)
    print(f"scale gate: {'PASS' if report.passed else 'FAIL'}")
    print(f"profile: {profile.name}")
    print(f"report: {destination}")
    print(f"work directory: {work_dir}")
    for violation in report.violations:
        print(f"violation: {violation}")
    return 0 if report.passed else 1


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    if args.profile == "release" and not args.confirm_exact_scale:
        parser.error("--profile release requires --confirm-exact-scale")
    if not 0 <= args.max_regression_percent <= 1_000:
        parser.error("--max-regression-percent must be between 0 and 1000")
    if args.work_dir is not None:
        return _run(args, args.work_dir.expanduser().absolute())
    with tempfile.TemporaryDirectory(prefix="jobby-scale-gate-") as temporary:
        return _run(args, Path(temporary))


if __name__ == "__main__":
    raise SystemExit(main())
