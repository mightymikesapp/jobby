"""Explicit, backup-first compaction of exact duplicate evaluations.

Planning is read-only and deterministic.  Applying a plan requires Jobby's
exclusive storage lock and a newly created backup that passes full backup
verification before the first evaluation row is deleted.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import stat
from typing import Any

from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from .backup import create_backup, verify_backup
from .config import JobbyPaths
from .db import Database, StorageLock
from .models import (
    Application,
    Evaluation,
    EvaluationCompactionBatch,
    EvaluationCompactionLedger,
    new_id,
    utc_now,
)


COMPACTION_MANIFEST_SCHEMA = "jobby-evaluation-compaction-v1"
_AUTOMATIC_KIND = "automatic"
_SQLITE_ID_CHUNK = 400


class EvaluationCompactionError(RuntimeError):
    """Base error for a compaction that was safely refused or rolled back."""


class BackupVerificationError(EvaluationCompactionError):
    """Raised when the required pre-compaction backup cannot be trusted."""


class StaleCompactionPlanError(EvaluationCompactionError):
    """Raised when evaluation state changed after a dry-run plan was produced."""


@dataclass(frozen=True)
class EvaluationCompactionCandidate:
    """One exact duplicate and the durable evaluation that replaces it."""

    removed_evaluation_id: str
    retained_evaluation_id: str
    job_id: str
    payload_hash: str
    identity_basis: str
    fingerprint: str | None
    ranker_version: str
    evaluation_kind: str


@dataclass(frozen=True)
class EvaluationCompactionPlan:
    """Deterministic dry-run output; safe to display or pass back to apply."""

    manifest_hash: str
    manifest_json: str
    total_count: int
    candidate_count: int
    retained_count: int
    candidates: tuple[EvaluationCompactionCandidate, ...]

    @property
    def removable_evaluation_ids(self) -> tuple[str, ...]:
        return tuple(candidate.removed_evaluation_id for candidate in self.candidates)


@dataclass(frozen=True)
class EvaluationCompactionResult:
    """Outcome of a dry run or an explicitly applied compaction."""

    plan: EvaluationCompactionPlan
    applied: bool
    backup_path: Path | None = None
    batch_id: str | None = None
    removed_evaluation_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class _EvaluationSnapshot:
    id: str
    job_id: str
    created_at: datetime
    is_current: bool
    manual_override: bool
    locked: bool
    ai_run_id: str | None
    import_key: str | None
    fingerprint: str | None
    payload_hash: str | None
    ranker_version: str
    evaluation_kind: str
    reference_key: str | None
    semantic_payload_hash: str | None


@dataclass(frozen=True)
class _BackupIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int
    content_hash: str


def plan_evaluation_compaction(session: Session) -> EvaluationCompactionPlan:
    """Find removable exact duplicates without mutating the session or database."""

    groups: dict[tuple[str, ...], list[_EvaluationSnapshot]] = {}
    total_count = 0
    with session.no_autoflush:
        application_references = {
            evaluation_id
            for evaluation_id in session.scalars(
                select(Application.applied_evaluation_id).where(
                    Application.applied_evaluation_id.is_not(None)
                )
            )
            if evaluation_id is not None
        }
        evaluations = session.scalars(
            select(Evaluation)
            .order_by(Evaluation.job_id, Evaluation.created_at, Evaluation.id)
            .execution_options(yield_per=1_000)
        )
        for evaluation in evaluations:
            total_count += 1
            snapshot = _snapshot(evaluation)
            identity = _duplicate_identity(snapshot)
            if identity is not None:
                groups.setdefault(identity, []).append(snapshot)

    candidates: list[EvaluationCompactionCandidate] = []
    for identity in sorted(groups):
        rows = groups[identity]
        if len(rows) < 2:
            continue
        protected = [
            row for row in rows if _retention_reasons(row, application_references)
        ]
        retained = min(protected or rows, key=_snapshot_order)
        for row in sorted(rows, key=_snapshot_order):
            if row.id == retained.id or row in protected:
                continue
            identity_basis, identity_hash = _ledger_identity(row)
            candidates.append(
                EvaluationCompactionCandidate(
                    removed_evaluation_id=row.id,
                    retained_evaluation_id=retained.id,
                    job_id=row.job_id,
                    payload_hash=identity_hash,
                    identity_basis=identity_basis,
                    fingerprint=_valid_hash(row.fingerprint),
                    ranker_version=row.ranker_version,
                    evaluation_kind=row.evaluation_kind,
                )
            )

    candidates.sort(
        key=lambda candidate: (
            candidate.job_id,
            candidate.removed_evaluation_id,
            candidate.retained_evaluation_id,
        )
    )
    manifest_json = _manifest_json(total_count, candidates)
    manifest_hash = sha256(manifest_json.encode("utf-8")).hexdigest()
    return EvaluationCompactionPlan(
        manifest_hash=manifest_hash,
        manifest_json=manifest_json,
        total_count=total_count,
        candidate_count=len(candidates),
        retained_count=total_count - len(candidates),
        candidates=tuple(candidates),
    )


def compact_evaluations(
    target: Database | Path | str,
    *,
    apply: bool = False,
    expected_plan: EvaluationCompactionPlan | None = None,
    backup_output: Path | str | None = None,
    paths: JobbyPaths | None = None,
    lock_timeout: float = 0.0,
) -> EvaluationCompactionResult:
    """Dry-run by default, or explicitly apply after locking and backup.

    An already-open normal :class:`Database` owns a shared storage lock, so an
    apply attempt will correctly fail until that client is closed.  Maintenance
    callers may instead pass the database path after closing normal clients.
    """

    if apply:
        return apply_evaluation_compaction(
            target,
            expected_plan=expected_plan,
            backup_output=backup_output,
            paths=paths,
            lock_timeout=lock_timeout,
        )
    plan = _plan_target(target, paths=paths)
    return EvaluationCompactionResult(plan=plan, applied=False)


def apply_evaluation_compaction(
    target: Database | Path | str,
    *,
    expected_plan: EvaluationCompactionPlan | None = None,
    backup_output: Path | str | None = None,
    paths: JobbyPaths | None = None,
    lock_timeout: float = 0.0,
) -> EvaluationCompactionResult:
    """Apply an exact-duplicate plan under an exclusive, backup-first workflow."""

    if lock_timeout < 0:
        raise ValueError("compaction lock timeout must not be negative")
    database_path, effective_paths = _target_details(target, explicit_paths=paths)
    _validate_existing_database(database_path)
    lock = StorageLock(
        database_path.parent / ".jobby.lock",
        exclusive=True,
        timeout=lock_timeout,
    )
    with lock:
        maintenance = Database(
            database_path,
            paths=effective_paths,
            acquire_lock=False,
        )
        try:
            maintenance.initialize()
            with maintenance.session() as session:
                locked_plan = plan_evaluation_compaction(session)
            _assert_expected_plan(expected_plan, locked_plan)
            if not locked_plan.candidates:
                return EvaluationCompactionResult(plan=locked_plan, applied=False)

            backup_path = create_backup(
                maintenance,
                output=backup_output,
                paths=effective_paths,
            ).absolute()
            backup_identity = _verify_published_backup(backup_path)

            with maintenance.session() as session:
                final_plan = plan_evaluation_compaction(session)
                if final_plan.manifest_hash != locked_plan.manifest_hash:
                    raise StaleCompactionPlanError(
                        "evaluation state changed after backup; nothing was deleted"
                    )
                _assert_backup_unchanged(backup_path, backup_identity)
                batch_id = _apply_plan(session, final_plan, backup_path)
            return EvaluationCompactionResult(
                plan=final_plan,
                applied=True,
                backup_path=backup_path,
                batch_id=batch_id,
                removed_evaluation_ids=final_plan.removable_evaluation_ids,
            )
        finally:
            maintenance.dispose()


def _plan_target(
    target: Database | Path | str,
    *,
    paths: JobbyPaths | None,
) -> EvaluationCompactionPlan:
    database_path, effective_paths = _target_details(target, explicit_paths=paths)
    _validate_existing_database(database_path)
    if isinstance(target, Database):
        target.initialize()
        with target.session() as session:
            return plan_evaluation_compaction(session)

    database = Database(database_path, paths=effective_paths)
    try:
        database.initialize()
        with database.session() as session:
            return plan_evaluation_compaction(session)
    finally:
        database.dispose()


def _snapshot(evaluation: Evaluation) -> _EvaluationSnapshot:
    return _EvaluationSnapshot(
        id=evaluation.id,
        job_id=evaluation.job_id,
        created_at=evaluation.created_at,
        is_current=evaluation.is_current,
        manual_override=evaluation.manual_override,
        locked=evaluation.locked,
        ai_run_id=evaluation.ai_run_id,
        import_key=evaluation.import_key,
        fingerprint=evaluation.fingerprint,
        payload_hash=evaluation.payload_hash,
        ranker_version=evaluation.ranker_version,
        evaluation_kind=evaluation.evaluation_kind,
        reference_key=evaluation.reference_key,
        semantic_payload_hash=_semantic_payload_hash(evaluation),
    )


def _duplicate_identity(snapshot: _EvaluationSnapshot) -> tuple[str, ...] | None:
    if snapshot.evaluation_kind != _AUTOMATIC_KIND:
        return None
    payload_hash = _valid_hash(snapshot.payload_hash)
    fingerprint = _valid_hash(snapshot.fingerprint)
    if payload_hash is not None and snapshot.semantic_payload_hash is not None:
        return (
            snapshot.job_id,
            snapshot.evaluation_kind,
            snapshot.ranker_version,
            "payload",
            payload_hash,
            fingerprint or "no-fingerprint",
            snapshot.semantic_payload_hash,
        )
    if fingerprint is not None and snapshot.semantic_payload_hash is not None:
        return (
            snapshot.job_id,
            snapshot.evaluation_kind,
            snapshot.ranker_version,
            "fingerprint",
            fingerprint,
            snapshot.semantic_payload_hash,
        )
    return None


def _retention_reasons(
    snapshot: _EvaluationSnapshot,
    application_references: set[str],
) -> tuple[str, ...]:
    reasons: list[str] = []
    if snapshot.is_current:
        reasons.append("current")
    if snapshot.evaluation_kind != _AUTOMATIC_KIND:
        reasons.append("non-automatic")
    if snapshot.manual_override:
        reasons.append("manual")
    if snapshot.locked:
        reasons.append("locked")
    if snapshot.ai_run_id is not None:
        reasons.append("ai-linked")
    if snapshot.reference_key is not None:
        reasons.append("referenced")
    if snapshot.import_key is not None:
        reasons.append("import-referenced")
    if snapshot.id in application_references:
        reasons.append("application-referenced")
    return tuple(reasons)


def _ledger_identity(snapshot: _EvaluationSnapshot) -> tuple[str, str]:
    payload_hash = _valid_hash(snapshot.payload_hash)
    if payload_hash is not None:
        return "payload_hash", payload_hash
    if snapshot.semantic_payload_hash is None:
        raise EvaluationCompactionError(
            "candidate lost its exact semantic payload identity"
        )
    return "fingerprint_and_payload", snapshot.semantic_payload_hash


def _semantic_payload_hash(evaluation: Evaluation) -> str | None:
    payload: dict[str, Any] = {
        "score": evaluation.score,
        "components": evaluation.components,
        "gates": evaluation.gates,
        "evidence": evaluation.evidence,
        "warnings": evaluation.warnings,
        "explanation": evaluation.explanation,
        "confidence": evaluation.confidence,
        "automatic_skip": evaluation.automatic_skip,
        "manual_override": evaluation.manual_override,
        "locked": evaluation.locked,
    }
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return sha256(encoded).hexdigest()


def _valid_hash(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.casefold()
    if len(normalized) != 64:
        return None
    if any(character not in "0123456789abcdef" for character in normalized):
        return None
    return normalized


def _snapshot_order(snapshot: _EvaluationSnapshot) -> tuple[datetime, str]:
    return snapshot.created_at, snapshot.id


def _manifest_json(
    total_count: int,
    candidates: list[EvaluationCompactionCandidate],
) -> str:
    manifest = {
        "schema": COMPACTION_MANIFEST_SCHEMA,
        "total_count": total_count,
        "candidate_count": len(candidates),
        "retained_count": total_count - len(candidates),
        "candidates": [
            {
                "removed_evaluation_id": candidate.removed_evaluation_id,
                "retained_evaluation_id": candidate.retained_evaluation_id,
                "job_id": candidate.job_id,
                "payload_hash": candidate.payload_hash,
                "identity_basis": candidate.identity_basis,
                "fingerprint": candidate.fingerprint,
                "ranker_version": candidate.ranker_version,
                "evaluation_kind": candidate.evaluation_kind,
            }
            for candidate in candidates
        ],
    }
    return json.dumps(
        manifest,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _assert_expected_plan(
    expected: EvaluationCompactionPlan | None,
    actual: EvaluationCompactionPlan,
) -> None:
    if expected is not None and expected.manifest_hash != actual.manifest_hash:
        raise StaleCompactionPlanError(
            "evaluation state changed since dry run; make a new plan before apply"
        )


def _apply_plan(
    session: Session,
    plan: EvaluationCompactionPlan,
    backup_path: Path,
) -> str:
    removed_at = utc_now().astimezone(timezone.utc)
    batch = EvaluationCompactionBatch(
        manifest_hash=plan.manifest_hash,
        backup_path=str(backup_path),
        candidate_count=plan.candidate_count,
        removed_count=plan.candidate_count,
        retained_count=plan.retained_count,
        created_at=removed_at,
        updated_at=removed_at,
    )
    session.add(batch)
    session.flush()

    for chunk in _chunks(plan.candidates):
        session.execute(
            insert(EvaluationCompactionLedger),
            [
                {
                    "id": new_id(),
                    "batch_id": batch.id,
                    "removed_evaluation_id": candidate.removed_evaluation_id,
                    "retained_evaluation_id": candidate.retained_evaluation_id,
                    "job_id": candidate.job_id,
                    "payload_hash": candidate.payload_hash,
                    "manifest_hash": plan.manifest_hash,
                    "removed_at": removed_at,
                }
                for candidate in chunk
            ],
        )
    _delete_candidates(session, plan.removable_evaluation_ids)
    return batch.id


def _delete_candidates(session: Session, evaluation_ids: tuple[str, ...]) -> None:
    removed = 0
    for chunk in _id_chunks(evaluation_ids):
        result = session.execute(delete(Evaluation).where(Evaluation.id.in_(chunk)))
        rowcount = getattr(result, "rowcount", None)
        if rowcount is None or rowcount < 0:
            remaining = session.scalar(
                select(Evaluation.id).where(Evaluation.id.in_(chunk)).limit(1)
            )
            if remaining is not None:
                raise StaleCompactionPlanError(
                    "a planned evaluation could not be deleted; transaction rolled back"
                )
            removed += len(chunk)
        else:
            removed += int(rowcount)
    if removed != len(evaluation_ids):
        raise StaleCompactionPlanError(
            "evaluation rows changed during compaction; transaction rolled back"
        )


def _chunks(
    values: tuple[EvaluationCompactionCandidate, ...],
) -> tuple[tuple[EvaluationCompactionCandidate, ...], ...]:
    return tuple(
        values[index : index + _SQLITE_ID_CHUNK]
        for index in range(0, len(values), _SQLITE_ID_CHUNK)
    )


def _id_chunks(values: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    return tuple(
        values[index : index + _SQLITE_ID_CHUNK]
        for index in range(0, len(values), _SQLITE_ID_CHUNK)
    )


def _verify_published_backup(path: Path) -> _BackupIdentity:
    before = _backup_identity(path)
    verified, detail = verify_backup(path)
    if not verified:
        raise BackupVerificationError(
            f"pre-compaction backup verification failed: {detail}"
        )
    after = _backup_identity(path)
    if after != before:
        raise BackupVerificationError(
            "pre-compaction backup changed during verification"
        )
    return after


def _assert_backup_unchanged(path: Path, expected: _BackupIdentity) -> None:
    if _backup_identity(path) != expected:
        raise BackupVerificationError(
            "verified pre-compaction backup changed before deletion"
        )


def _backup_identity(path: Path) -> _BackupIdentity:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise BackupVerificationError(
            f"pre-compaction backup is unavailable: {exc}"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise BackupVerificationError(
                "pre-compaction backup must be a regular, non-symlink file"
            )
        digest = sha256()
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        after = os.fstat(descriptor)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if after_identity != before_identity:
            raise BackupVerificationError(
                "pre-compaction backup changed while it was being inspected"
            )
        return _BackupIdentity(
            device=after.st_dev,
            inode=after.st_ino,
            size=after.st_size,
            modified_ns=after.st_mtime_ns,
            changed_ns=after.st_ctime_ns,
            content_hash=digest.hexdigest(),
        )
    finally:
        os.close(descriptor)


def _target_details(
    target: Database | Path | str,
    *,
    explicit_paths: JobbyPaths | None,
) -> tuple[Path, JobbyPaths]:
    if isinstance(target, Database):
        path = target.path
        candidate_paths = explicit_paths or target.paths
    else:
        path = Path(target).expanduser().absolute()
        candidate_paths = explicit_paths
    if (
        candidate_paths is not None
        and candidate_paths.database.expanduser().absolute() == path
    ):
        return path, candidate_paths
    root = path.parent
    local_paths = JobbyPaths(
        data_dir=root,
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=path,
        artifacts_dir=root / "artifacts",
        backups_dir=root / "backups",
        logs_dir=root / "logs",
        config_file=root / "config" / "config.toml",
    )
    return path, local_paths


def _validate_existing_database(path: Path) -> None:
    if path.is_symlink():
        raise ValueError("compaction database must not be a symbolic link")
    if not path.exists():
        raise FileNotFoundError(f"compaction database does not exist: {path}")
    if not path.is_file():
        raise ValueError("compaction database must be a regular file")
