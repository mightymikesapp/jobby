"""Deterministic, verification-gated rotation for local Jobby backups.

Rotation is deliberately separate from backup creation.  A caller first creates
and verifies a replacement backup, then passes that existing file here.  Planning
is read-only; applying a plan re-verifies every backup that informed the plan
before the first unlink operation.
"""

from __future__ import annotations

import json
import os
import stat
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from .backup import MAX_MANIFEST_BYTES, verify_backup


DEFAULT_DAILY_RETENTION = 7
DEFAULT_WEEKLY_RETENTION = 4
DEFAULT_MONTHLY_RETENTION = 6


class BackupRotationError(RuntimeError):
    """Base error for a rotation that could not be safely planned or applied."""


class UnsafeBackupError(BackupRotationError):
    """A rotation directory or ZIP candidate is not a safe regular file."""


class ReplacementBackupError(BackupRotationError):
    """The required replacement backup is absent, invalid, or unverified."""


class StaleRotationPlanError(BackupRotationError):
    """Files changed after planning, so applying the old plan is unsafe."""


@dataclass(frozen=True, slots=True)
class FileIdentity:
    """Filesystem identity used to reject replacement between plan and apply."""

    device: int
    inode: int
    size_bytes: int
    modified_ns: int


@dataclass(frozen=True, slots=True)
class VerifiedBackup:
    """A verified local backup and its three retention period identities."""

    path: Path
    created_at: datetime
    daily_bucket: str
    weekly_bucket: str
    monthly_bucket: str
    identity: FileIdentity


@dataclass(frozen=True, slots=True)
class UnverifiedBackup:
    """A regular ZIP that rotation will report and preserve, never prune."""

    path: Path
    reason: str
    identity: FileIdentity


@dataclass(frozen=True, slots=True)
class RetentionDecision:
    backup: VerifiedBackup
    retained_for: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RotationPlan:
    directory: Path
    replacement: Path
    daily_retention: int
    weekly_retention: int
    monthly_retention: int
    verified: tuple[VerifiedBackup, ...]
    kept: tuple[RetentionDecision, ...]
    prune: tuple[VerifiedBackup, ...]
    unverified: tuple[UnverifiedBackup, ...]

    @property
    def prune_paths(self) -> tuple[Path, ...]:
        return tuple(item.path for item in self.prune)

    @property
    def keep_paths(self) -> tuple[Path, ...]:
        return tuple(item.backup.path for item in self.kept)


@dataclass(frozen=True, slots=True)
class RotationResult:
    plan: RotationPlan
    applied: bool
    pruned: tuple[Path, ...] = ()


def plan_backup_rotation(
    directory: Path | str,
    replacement: Path | str,
    *,
    daily_retention: int = DEFAULT_DAILY_RETENTION,
    weekly_retention: int = DEFAULT_WEEKLY_RETENTION,
    monthly_retention: int = DEFAULT_MONTHLY_RETENTION,
) -> RotationPlan:
    """Build a deterministic, non-mutating rotation plan.

    One newest backup is selected per retained local calendar day, ISO week, and
    calendar month.  A backup may satisfy multiple tiers.  Unverified regular
    ZIPs are reported but never deleted.  Symlinks and non-regular ZIP candidates
    fail closed instead of being ignored.
    """

    counts = (
        _retention_count("daily", daily_retention),
        _retention_count("weekly", weekly_retention),
        _retention_count("monthly", monthly_retention),
    )
    root = _safe_directory(directory)
    replacement_path = _direct_backup_path(root, replacement)

    verified: list[VerifiedBackup] = []
    unverified: list[UnverifiedBackup] = []
    for path in _zip_candidates(root):
        identity = _regular_file_identity(path)
        ok, detail = verify_backup(path)
        if not ok:
            unverified.append(
                UnverifiedBackup(
                    path=path, reason=_bounded_reason(detail), identity=identity
                )
            )
            continue
        try:
            created_at = _manifest_created_at(path)
        except (OSError, KeyError, TypeError, ValueError, zipfile.BadZipFile) as exc:
            unverified.append(
                UnverifiedBackup(
                    path=path,
                    reason=_bounded_reason(f"manifest created_at is invalid: {exc}"),
                    identity=identity,
                )
            )
            continue
        if _regular_file_identity(path) != identity:
            raise StaleRotationPlanError(
                f"backup changed while it was being inspected: {path.name}"
            )
        verified.append(_verified_record(path, created_at, identity))

    replacement_record = next(
        (item for item in verified if item.path == replacement_path), None
    )
    if replacement_record is None:
        invalid = next(
            (item for item in unverified if item.path == replacement_path), None
        )
        detail = f": {invalid.reason}" if invalid is not None else ""
        raise ReplacementBackupError(
            f"replacement backup must already exist and verify successfully{detail}"
        )

    ordered = tuple(sorted(verified, key=_newest_sort_key))
    reasons: dict[Path, list[str]] = {item.path: [] for item in ordered}
    for reason, key, count in (
        ("daily", lambda item: item.daily_bucket, counts[0]),
        ("weekly", lambda item: item.weekly_bucket, counts[1]),
        ("monthly", lambda item: item.monthly_bucket, counts[2]),
    ):
        for item in _newest_per_bucket(ordered, key=key, count=count):
            reasons[item.path].append(reason)

    if not reasons[replacement_path]:
        reasons[replacement_path].append("replacement")
    kept = tuple(
        RetentionDecision(item, tuple(reasons[item.path]))
        for item in ordered
        if reasons[item.path]
    )
    kept_paths = {item.backup.path for item in kept}
    prune = tuple(
        sorted(
            (item for item in ordered if item.path not in kept_paths),
            key=_oldest_sort_key,
        )
    )
    if replacement_path in {item.path for item in prune} or not kept:
        raise BackupRotationError("rotation plan would remove its verified replacement")

    return RotationPlan(
        directory=root,
        replacement=replacement_path,
        daily_retention=counts[0],
        weekly_retention=counts[1],
        monthly_retention=counts[2],
        verified=ordered,
        kept=kept,
        prune=prune,
        unverified=tuple(sorted(unverified, key=lambda item: item.path.name)),
    )


def apply_backup_rotation(plan: RotationPlan) -> RotationResult:
    """Apply a previously built plan after a complete second verification pass."""

    root = _safe_directory(plan.directory)
    if root != plan.directory:
        raise StaleRotationPlanError("rotation directory changed after planning")
    verified_paths = tuple(item.path for item in plan.verified)
    keep_paths = plan.keep_paths
    prune_paths = plan.prune_paths
    if (
        len(set(verified_paths)) != len(verified_paths)
        or len(set(keep_paths)) != len(keep_paths)
        or len(set(prune_paths)) != len(prune_paths)
        or not set(keep_paths).issubset(verified_paths)
        or not set(prune_paths).issubset(verified_paths)
    ):
        raise BackupRotationError("rotation plan has inconsistent backup membership")
    if plan.replacement not in keep_paths or plan.replacement in prune_paths:
        raise ReplacementBackupError(
            "rotation plan must retain and may not prune its replacement backup"
        )
    if set(keep_paths) & set(prune_paths):
        raise BackupRotationError("rotation plan has inconsistent backup membership")
    if not set(verified_paths) - set(prune_paths):
        raise BackupRotationError("rotation may not prune the last verified backup")
    current_paths = tuple(_zip_candidates(root))
    planned_paths = tuple(
        sorted(
            (
                *(item.path for item in plan.verified),
                *(item.path for item in plan.unverified),
            ),
            key=lambda path: path.name,
        )
    )
    if current_paths != planned_paths:
        raise StaleRotationPlanError(
            "backup directory changed after planning; build a new rotation plan"
        )

    for item in (*plan.verified, *plan.unverified):
        if _regular_file_identity(item.path) != item.identity:
            raise StaleRotationPlanError(
                f"backup changed after planning: {item.path.name}"
            )

    replacement = next(
        (item for item in plan.verified if item.path == plan.replacement), None
    )
    if replacement is None:
        raise ReplacementBackupError(
            "rotation plan does not retain a verified replacement backup"
        )

    # The replacement is always first. Every other verified input is checked
    # before any call to unlink, so verification failure cannot produce a partial
    # prune. Re-reading created_at also prevents a valid ZIP from changing its
    # retention identity between dry-run and apply.
    verification_order = (replacement,) + tuple(
        item for item in plan.verified if item.path != replacement.path
    )
    for item in verification_order:
        ok, detail = verify_backup(item.path)
        if not ok:
            error = (
                ReplacementBackupError
                if item.path == replacement.path
                else StaleRotationPlanError
            )
            raise error(
                f"backup failed apply-time verification: {item.path.name}: "
                f"{_bounded_reason(detail)}"
            )
        if _manifest_created_at(item.path) != item.created_at:
            raise StaleRotationPlanError(
                f"backup manifest changed after planning: {item.path.name}"
            )
        if _regular_file_identity(item.path) != item.identity:
            raise StaleRotationPlanError(
                f"backup changed during apply verification: {item.path.name}"
            )

    if len(plan.keep_paths) < 1 or len(plan.prune) >= len(plan.verified):
        raise BackupRotationError("rotation may not prune the last verified backup")

    # Complete a final identity pass before the first irreversible operation.
    for item in plan.prune:
        if _regular_file_identity(item.path) != item.identity:
            raise StaleRotationPlanError(
                f"backup changed before pruning: {item.path.name}"
            )

    pruned: list[Path] = []
    for item in plan.prune:
        _unlink_backup(item)
        pruned.append(item.path)
    if pruned:
        _sync_directory(root)
    return RotationResult(plan=plan, applied=True, pruned=tuple(pruned))


def rotate_backups(
    directory: Path | str,
    replacement: Path | str,
    *,
    apply: bool = False,
    daily_retention: int = DEFAULT_DAILY_RETENTION,
    weekly_retention: int = DEFAULT_WEEKLY_RETENTION,
    monthly_retention: int = DEFAULT_MONTHLY_RETENTION,
) -> RotationResult:
    """Plan rotation by default, or explicitly apply the verified plan."""

    plan = plan_backup_rotation(
        directory,
        replacement,
        daily_retention=daily_retention,
        weekly_retention=weekly_retention,
        monthly_retention=monthly_retention,
    )
    return apply_backup_rotation(plan) if apply else RotationResult(plan, False)


def _retention_count(label: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label}_retention must be a non-negative integer")
    return value


def _safe_directory(value: Path | str) -> Path:
    path = Path(os.path.abspath(os.fspath(Path(value).expanduser())))
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise UnsafeBackupError(f"backup directory cannot be inspected: {exc}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise UnsafeBackupError(
            "backup directory must be a real directory, not a symlink"
        )
    return path


def _direct_backup_path(directory: Path, value: Path | str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = directory / path
    path = Path(os.path.abspath(os.fspath(path)))
    if path.parent != directory or path.suffix.casefold() != ".zip":
        raise ReplacementBackupError(
            "replacement must be a direct .zip child of the rotation directory"
        )
    return path


def _zip_candidates(directory: Path) -> tuple[Path, ...]:
    try:
        entries = tuple(directory.iterdir())
    except OSError as exc:
        raise UnsafeBackupError(f"backup directory cannot be read: {exc}") from exc
    candidates = sorted(
        (item for item in entries if item.suffix.casefold() == ".zip"),
        key=lambda item: item.name,
    )
    for path in candidates:
        _regular_file_identity(path)
    return tuple(candidates)


def _regular_file_identity(path: Path) -> FileIdentity:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise UnsafeBackupError(
            f"backup candidate cannot be inspected: {path.name}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise UnsafeBackupError(
            f"backup ZIP candidate must be a real regular file: {path.name}"
        )
    return FileIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        size_bytes=metadata.st_size,
        modified_ns=metadata.st_mtime_ns,
    )


def _manifest_created_at(path: Path) -> datetime:
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo("manifest.json")
        if info.file_size > MAX_MANIFEST_BYTES:
            raise ValueError("manifest exceeds the safe size limit")
        payload = json.loads(archive.read(info))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be an object")
    raw = payload.get("created_at")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("manifest has no created_at value")
    normalized = raw.strip()
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    created_at = datetime.fromisoformat(normalized)
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("manifest created_at must include a UTC offset")
    return created_at


def _verified_record(
    path: Path, created_at: datetime, identity: FileIdentity
) -> VerifiedBackup:
    calendar = created_at.date()
    iso_year, iso_week, _ = calendar.isocalendar()
    return VerifiedBackup(
        path=path,
        created_at=created_at,
        daily_bucket=calendar.isoformat(),
        weekly_bucket=f"{iso_year:04d}-W{iso_week:02d}",
        monthly_bucket=f"{calendar.year:04d}-{calendar.month:02d}",
        identity=identity,
    )


def _newest_per_bucket(
    records: tuple[VerifiedBackup, ...],
    *,
    key: Callable[[VerifiedBackup], str],
    count: int,
) -> tuple[VerifiedBackup, ...]:
    if count == 0:
        return ()
    selected: list[VerifiedBackup] = []
    seen: set[str] = set()
    for item in records:
        bucket = key(item)
        if bucket in seen:
            continue
        seen.add(bucket)
        selected.append(item)
        if len(selected) == count:
            break
    return tuple(selected)


def _newest_sort_key(item: VerifiedBackup) -> tuple[float, str]:
    return (-item.created_at.timestamp(), item.path.name)


def _oldest_sort_key(item: VerifiedBackup) -> tuple[float, str]:
    return (item.created_at.timestamp(), item.path.name)


def _bounded_reason(value: object) -> str:
    return " ".join(str(value).split())[:500] or "verification failed"


def _unlink_backup(item: VerifiedBackup) -> None:
    if _regular_file_identity(item.path) != item.identity:
        raise StaleRotationPlanError(f"backup changed before unlink: {item.path.name}")
    item.path.unlink()


def _sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "BackupRotationError",
    "DEFAULT_DAILY_RETENTION",
    "DEFAULT_MONTHLY_RETENTION",
    "DEFAULT_WEEKLY_RETENTION",
    "FileIdentity",
    "ReplacementBackupError",
    "RetentionDecision",
    "RotationPlan",
    "RotationResult",
    "StaleRotationPlanError",
    "UnsafeBackupError",
    "UnverifiedBackup",
    "VerifiedBackup",
    "apply_backup_rotation",
    "plan_backup_rotation",
    "rotate_backups",
]
