from __future__ import annotations

import hashlib
import json
import sqlite3
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest

import jobby.backup_rotation as rotation
from jobby.backup import BACKUP_SCHEMA, verify_backup
from jobby.backup_rotation import (
    ReplacementBackupError,
    StaleRotationPlanError,
    UnsafeBackupError,
    apply_backup_rotation,
    plan_backup_rotation,
    rotate_backups,
)


def _write_backup(path: Path, created_at: str) -> Path:
    database = path.with_name(f".{path.stem}.sqlite3")
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY)")
    database_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    manifest = {
        "schema": BACKUP_SCHEMA,
        "created_at": created_at,
        "database_sha256": database_hash,
        "artifacts": [],
        "skipped_artifacts": [],
        "excluded_artifacts": [],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(database, "database/jobby.sqlite3")
        archive.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
    database.unlink()
    assert verify_backup(path) == (True, "ok")
    return path


def _seed_retention_history(root: Path) -> dict[str, Path]:
    return {
        "jan31": _write_backup(root / "a-jan31.zip", "2026-01-31T08:00:00+00:00"),
        "feb01": _write_backup(root / "b-feb01.zip", "2026-02-01T08:00:00+00:00"),
        "feb28": _write_backup(root / "c-feb28.zip", "2026-02-28T08:00:00+00:00"),
        "mar01": _write_backup(root / "d-mar01.zip", "2026-03-01T08:00:00+00:00"),
        "mar08": _write_backup(root / "e-mar08.zip", "2026-03-08T08:00:00+00:00"),
        "mar15": _write_backup(root / "f-mar15.zip", "2026-03-15T08:00:00+00:00"),
    }


def test_plan_classifies_periods_and_keeps_union_deterministically(
    tmp_path: Path,
) -> None:
    paths = _seed_retention_history(tmp_path)

    first = plan_backup_rotation(
        tmp_path,
        paths["mar15"],
        daily_retention=1,
        weekly_retention=2,
        monthly_retention=3,
    )
    second = plan_backup_rotation(
        tmp_path,
        paths["mar15"],
        daily_retention=1,
        weekly_retention=2,
        monthly_retention=3,
    )

    assert first == second
    assert set(first.keep_paths) == {
        paths["jan31"],
        paths["feb28"],
        paths["mar08"],
        paths["mar15"],
    }
    assert first.prune_paths == (paths["feb01"], paths["mar01"])
    replacement = next(
        item for item in first.kept if item.backup.path == paths["mar15"]
    )
    assert replacement.retained_for == ("daily", "weekly", "monthly")
    assert replacement.backup.daily_bucket == "2026-03-15"
    assert replacement.backup.weekly_bucket == "2026-W11"
    assert replacement.backup.monthly_bucket == "2026-03"


def test_dry_run_is_non_mutating_and_apply_prunes_only_the_plan(tmp_path: Path) -> None:
    paths = _seed_retention_history(tmp_path)
    result = rotate_backups(
        tmp_path,
        paths["mar15"],
        apply=False,
        daily_retention=1,
        weekly_retention=2,
        monthly_retention=3,
    )

    assert result.applied is False
    assert result.pruned == ()
    assert all(path.exists() for path in paths.values())

    applied = apply_backup_rotation(result.plan)

    assert applied.applied is True
    assert applied.pruned == (paths["feb01"], paths["mar01"])
    assert not paths["feb01"].exists()
    assert not paths["mar01"].exists()
    assert all(
        path.exists() for name, path in paths.items() if name not in {"feb01", "mar01"}
    )


def test_apply_verifies_every_planned_backup_before_first_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _seed_retention_history(tmp_path)
    plan = plan_backup_rotation(
        tmp_path,
        paths["mar15"],
        daily_retention=1,
        weekly_retention=2,
        monthly_retention=3,
    )
    events: list[str] = []
    real_verify = rotation.verify_backup
    real_unlink = rotation._unlink_backup

    def traced_verify(path: Path) -> tuple[bool, str]:
        events.append(f"verify:{Path(path).name}")
        return real_verify(path)

    def traced_unlink(item) -> None:
        events.append(f"unlink:{item.path.name}")
        real_unlink(item)

    monkeypatch.setattr(rotation, "verify_backup", traced_verify)
    monkeypatch.setattr(rotation, "_unlink_backup", traced_unlink)

    apply_backup_rotation(plan)

    first_unlink = next(
        index for index, item in enumerate(events) if item.startswith("unlink:")
    )
    assert events[0] == f"verify:{paths['mar15'].name}"
    assert first_unlink == len(plan.verified)
    assert all(item.startswith("verify:") for item in events[:first_unlink])


def test_apply_aborts_without_pruning_when_replacement_reverification_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _seed_retention_history(tmp_path)
    plan = plan_backup_rotation(
        tmp_path,
        paths["mar15"],
        daily_retention=1,
        weekly_retention=2,
        monthly_retention=3,
    )
    real_verify = rotation.verify_backup
    unlink_calls: list[Path] = []

    def fail_replacement(path: Path) -> tuple[bool, str]:
        if Path(path) == paths["mar15"]:
            return False, "injected verification failure"
        return real_verify(path)

    monkeypatch.setattr(rotation, "verify_backup", fail_replacement)
    monkeypatch.setattr(
        rotation, "_unlink_backup", lambda item: unlink_calls.append(item.path)
    )

    with pytest.raises(ReplacementBackupError, match="apply-time verification"):
        apply_backup_rotation(plan)

    assert unlink_calls == []
    assert all(path.exists() for path in paths.values())


def test_unverified_zip_is_reported_and_never_pruned(tmp_path: Path) -> None:
    replacement = _write_backup(
        tmp_path / "replacement.zip", "2026-03-15T08:00:00+00:00"
    )
    broken = tmp_path / "broken.zip"
    broken.write_bytes(b"not a zip")

    plan = plan_backup_rotation(
        tmp_path,
        replacement,
        daily_retention=0,
        weekly_retention=0,
        monthly_retention=0,
    )

    assert plan.keep_paths == (replacement,)
    assert plan.prune == ()
    assert tuple(item.path for item in plan.unverified) == (broken,)
    assert "zip" in plan.unverified[0].reason.casefold()

    result = apply_backup_rotation(plan)
    assert result.pruned == ()
    assert replacement.exists()
    assert broken.exists()


def test_zero_retention_still_keeps_replacement_and_never_prunes_last_copy(
    tmp_path: Path,
) -> None:
    replacement = _write_backup(
        tmp_path / "replacement.zip", "2026-03-15T08:00:00+00:00"
    )

    result = rotate_backups(
        tmp_path,
        replacement,
        apply=True,
        daily_retention=0,
        weekly_retention=0,
        monthly_retention=0,
    )

    assert result.pruned == ()
    assert result.plan.keep_paths == (replacement,)
    assert replacement.exists()


def test_apply_rejects_a_forged_plan_that_would_prune_replacement(
    tmp_path: Path,
) -> None:
    older = _write_backup(tmp_path / "older.zip", "2026-03-14T08:00:00+00:00")
    replacement_path = _write_backup(
        tmp_path / "replacement.zip", "2026-03-15T08:00:00+00:00"
    )
    plan = plan_backup_rotation(
        tmp_path,
        replacement_path,
        daily_retention=2,
        weekly_retention=0,
        monthly_retention=0,
    )
    replacement_record = next(
        item for item in plan.verified if item.path == replacement_path
    )
    forged = replace(plan, prune=(replacement_record,))

    with pytest.raises(ReplacementBackupError, match="may not prune"):
        apply_backup_rotation(forged)

    assert older.exists()
    assert replacement_path.exists()


def test_replacement_must_be_existing_verified_direct_child(tmp_path: Path) -> None:
    valid = _write_backup(tmp_path / "valid.zip", "2026-03-15T08:00:00+00:00")
    broken = tmp_path / "broken.zip"
    broken.write_bytes(b"invalid")

    with pytest.raises(ReplacementBackupError, match="already exist"):
        plan_backup_rotation(tmp_path, tmp_path / "missing.zip")
    with pytest.raises(ReplacementBackupError, match="verify successfully"):
        plan_backup_rotation(tmp_path, broken)
    with pytest.raises(ReplacementBackupError, match="direct .zip child"):
        plan_backup_rotation(tmp_path, tmp_path.parent / valid.name)


def test_symlinked_directory_and_zip_candidates_fail_closed(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    replacement = _write_backup(real / "replacement.zip", "2026-03-15T08:00:00+00:00")
    directory_link = tmp_path / "linked"
    directory_link.symlink_to(real, target_is_directory=True)

    with pytest.raises(UnsafeBackupError, match="real directory"):
        plan_backup_rotation(directory_link, directory_link / replacement.name)

    outside = _write_backup(tmp_path / "outside.zip", "2026-03-14T08:00:00+00:00")
    (real / "unsafe.zip").symlink_to(outside)
    with pytest.raises(UnsafeBackupError, match="regular file"):
        plan_backup_rotation(real, replacement)


def test_apply_rejects_directory_changes_after_dry_run(tmp_path: Path) -> None:
    replacement = _write_backup(
        tmp_path / "replacement.zip", "2026-03-15T08:00:00+00:00"
    )
    plan = plan_backup_rotation(tmp_path, replacement)
    _write_backup(tmp_path / "new.zip", "2026-03-16T08:00:00+00:00")

    with pytest.raises(StaleRotationPlanError, match="directory changed"):
        apply_backup_rotation(plan)

    assert replacement.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("daily_retention", -1),
        ("weekly_retention", True),
        ("monthly_retention", 1.5),
    ],
)
def test_retention_counts_must_be_nonnegative_integers(
    tmp_path: Path, field: str, value: object
) -> None:
    replacement = _write_backup(
        tmp_path / "replacement.zip", "2026-03-15T08:00:00+00:00"
    )
    values = {
        "daily_retention": 7,
        "weekly_retention": 4,
        "monthly_retention": 6,
        field: value,
    }

    with pytest.raises(ValueError, match=field):
        plan_backup_rotation(tmp_path, replacement, **values)


def test_manifest_created_at_requires_timezone(tmp_path: Path) -> None:
    replacement = _write_backup(tmp_path / "replacement.zip", "2026-03-15T08:00:00")

    with pytest.raises(ReplacementBackupError, match="UTC offset"):
        plan_backup_rotation(tmp_path, replacement)


def test_bucket_selection_uses_manifest_offset_calendar_date(tmp_path: Path) -> None:
    older = _write_backup(tmp_path / "older.zip", "2026-03-01T23:30:00-08:00")
    replacement = _write_backup(
        tmp_path / "replacement.zip", "2026-03-02T00:15:00-08:00"
    )

    plan = plan_backup_rotation(
        tmp_path,
        replacement,
        daily_retention=2,
        weekly_retention=0,
        monthly_retention=0,
    )

    assert set(plan.keep_paths) == {older, replacement}
    buckets = {item.path: item.daily_bucket for item in plan.verified}
    assert buckets == {older: "2026-03-01", replacement: "2026-03-02"}
