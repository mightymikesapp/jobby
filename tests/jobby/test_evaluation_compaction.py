from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import os
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

import jobby.evaluation_compaction as compaction
from jobby.backup import verify_backup
from jobby.config import JobbyPaths
from jobby.db import Database, StorageBusyError
from jobby.evaluation_compaction import (
    BackupVerificationError,
    StaleCompactionPlanError,
    apply_evaluation_compaction,
    compact_evaluations,
    plan_evaluation_compaction,
)
from jobby.models import (
    AIRun,
    Application,
    Company,
    Evaluation,
    EvaluationCompactionBatch,
    EvaluationCompactionLedger,
    Job,
)


NOW = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)


def _paths(root: Path) -> JobbyPaths:
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    )


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    paths = _paths(tmp_path / "home").ensure()
    db = Database(paths=paths, acquire_lock=False)
    db.initialize()
    yield db
    db.dispose()


def _hash(label: str) -> str:
    return sha256(label.encode("utf-8")).hexdigest()


def _job(database: Database, *, source_id: str = "job-1") -> str:
    with database.session() as session:
        company = Company(name=f"Acme {source_id}", normalized_name=f"acme-{source_id}")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Policy Counsel",
            normalized_title="policy counsel",
            source_primary="manual",
            source_id=source_id,
        )
        session.add(job)
        session.flush()
        return job.id


def _evaluation(
    *,
    job_id: str,
    index: int,
    payload_hash: str | None,
    fingerprint: str | None = None,
    score: float = 7.5,
    is_current: bool = False,
    evaluation_kind: str = "automatic",
    **values: object,
) -> Evaluation:
    return Evaluation(
        job_id=job_id,
        score=score,
        components={"mission": 2.0},
        gates=[],
        evidence=[{"source": "description", "text": "policy"}],
        warnings=[],
        confidence=0.9,
        is_current=is_current,
        payload_hash=payload_hash,
        fingerprint=fingerprint,
        ranker_version="deterministic-test-v1",
        evaluation_kind=evaluation_kind,
        created_at=NOW + timedelta(minutes=index),
        updated_at=NOW + timedelta(minutes=index),
        **values,
    )


def _seed_simple_duplicates(database: Database, *, count: int = 3) -> list[str]:
    job_id = _job(database)
    payload_hash = _hash("same-payload")
    with database.session() as session:
        rows = [
            _evaluation(
                job_id=job_id,
                index=index,
                payload_hash=payload_hash,
                is_current=index == count - 1,
            )
            for index in range(count)
        ]
        session.add_all(rows)
        session.flush()
        return [row.id for row in rows]


def test_dry_run_is_deterministic_and_non_mutating(database: Database) -> None:
    evaluation_ids = _seed_simple_duplicates(database)

    first = compact_evaluations(database, apply=False)
    second = compact_evaluations(database, apply=False)

    assert first.applied is False
    assert first.plan == second.plan
    assert first.plan.total_count == 3
    assert first.plan.candidate_count == 2
    assert first.plan.retained_count == 1
    assert (
        first.plan.manifest_hash
        == sha256(first.plan.manifest_json.encode("utf-8")).hexdigest()
    )
    assert set(first.plan.removable_evaluation_ids) == set(evaluation_ids[:2])
    assert all(
        candidate.retained_evaluation_id == evaluation_ids[2]
        for candidate in first.plan.candidates
    )
    with database.session() as session:
        assert session.scalar(select(func.count(Evaluation.id))) == 3
        assert session.scalar(select(func.count(EvaluationCompactionBatch.id))) == 0
        assert session.scalar(select(func.count(EvaluationCompactionLedger.id))) == 0
    assert not list(database.paths.backups_dir.glob("*.zip"))


def test_planning_does_not_autoflush_unrelated_pending_changes(
    database: Database,
) -> None:
    _seed_simple_duplicates(database)
    job_id = _job(database, source_id="pending-job")
    session = database.Session()
    try:
        pending = _evaluation(
            job_id=job_id,
            index=30,
            payload_hash=_hash("pending"),
            is_current=True,
        )
        session.add(pending)

        plan = plan_evaluation_compaction(session)

        assert pending in session.new
        assert pending.id is None
        assert plan.total_count == 3
    finally:
        session.rollback()
        session.close()


def test_every_meaningful_evaluation_is_retained(database: Database) -> None:
    job_id = _job(database)
    payload_hash = _hash("protected-payload")
    with database.session() as session:
        ai_run = AIRun(
            purpose="evaluation",
            model="offline-test",
            prompt_version="v1",
            input_hash=_hash("ai-input"),
        )
        session.add(ai_run)
        session.flush()
        rows = {
            "current": _evaluation(
                job_id=job_id,
                index=0,
                payload_hash=payload_hash,
                is_current=True,
            ),
            "locked": _evaluation(
                job_id=job_id,
                index=1,
                payload_hash=payload_hash,
                locked=True,
            ),
            "manual_flag": _evaluation(
                job_id=job_id,
                index=2,
                payload_hash=payload_hash,
                manual_override=True,
            ),
            "ai": _evaluation(
                job_id=job_id,
                index=3,
                payload_hash=payload_hash,
                ai_run_id=ai_run.id,
            ),
            "reference": _evaluation(
                job_id=job_id,
                index=4,
                payload_hash=payload_hash,
                reference_key="explicit-review",
            ),
            "import": _evaluation(
                job_id=job_id,
                index=5,
                payload_hash=payload_hash,
                import_key=_hash("import-key"),
            ),
            "application": _evaluation(
                job_id=job_id,
                index=6,
                payload_hash=payload_hash,
            ),
            "removable": _evaluation(
                job_id=job_id,
                index=7,
                payload_hash=payload_hash,
            ),
            "manual_kind": _evaluation(
                job_id=job_id,
                index=8,
                payload_hash=payload_hash,
                evaluation_kind="manual",
            ),
            "forced_kind": _evaluation(
                job_id=job_id,
                index=9,
                payload_hash=payload_hash,
                evaluation_kind="forced",
            ),
        }
        session.add_all(rows.values())
        session.flush()
        application = Application(
            job_id=job_id,
            applied_evaluation_id=rows["application"].id,
            applied_score=rows["application"].score,
            applied_ranker_version=rows["application"].ranker_version,
        )
        session.add(application)
        session.flush()
        protected_ids = {row.id for name, row in rows.items() if name != "removable"}
        removable_id = rows["removable"].id

    with database.session() as session:
        plan = plan_evaluation_compaction(session)

    assert plan.removable_evaluation_ids == (removable_id,)
    assert protected_ids.isdisjoint(plan.removable_evaluation_ids)


def test_fingerprint_fallback_requires_identical_persisted_payload(
    database: Database,
) -> None:
    job_id = _job(database)
    fingerprint = _hash("legacy-fingerprint")
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP INDEX ix_evaluation_job_fingerprint")
    with database.session() as session:
        exact_old = _evaluation(
            job_id=job_id,
            index=0,
            payload_hash=None,
            fingerprint=fingerprint,
        )
        exact_current = _evaluation(
            job_id=job_id,
            index=1,
            payload_hash=None,
            fingerprint=fingerprint,
            is_current=True,
        )
        divergent = _evaluation(
            job_id=job_id,
            index=2,
            payload_hash=None,
            fingerprint=fingerprint,
            score=4.0,
        )
        session.add_all([exact_old, exact_current, divergent])
        session.flush()
        old_id = exact_old.id
        current_id = exact_current.id
        divergent_id = divergent.id

    with database.session() as session:
        plan = plan_evaluation_compaction(session)

    assert plan.removable_evaluation_ids == (old_id,)
    assert plan.candidates[0].retained_evaluation_id == current_id
    assert plan.candidates[0].identity_basis == "fingerprint_and_payload"
    assert divergent_id not in plan.removable_evaluation_ids


def test_invalid_or_conflicting_hash_identity_is_never_compacted(
    database: Database,
) -> None:
    job_id = _job(database)
    with database.session() as session:
        session.add_all(
            [
                _evaluation(
                    job_id=job_id,
                    index=0,
                    payload_hash="not-a-sha256",
                ),
                _evaluation(
                    job_id=job_id,
                    index=1,
                    payload_hash="not-a-sha256",
                ),
                _evaluation(
                    job_id=job_id,
                    index=2,
                    payload_hash=_hash("same-output"),
                    fingerprint=_hash("input-a"),
                ),
                _evaluation(
                    job_id=job_id,
                    index=3,
                    payload_hash=_hash("same-output"),
                    fingerprint=_hash("input-b"),
                    is_current=True,
                ),
                _evaluation(
                    job_id=job_id,
                    index=4,
                    payload_hash=_hash("stale-payload-hash"),
                    score=1.0,
                ),
                _evaluation(
                    job_id=job_id,
                    index=5,
                    payload_hash=_hash("stale-payload-hash"),
                    score=2.0,
                ),
            ]
        )

    with database.session() as session:
        plan = plan_evaluation_compaction(session)

    assert plan.candidate_count == 0
    assert plan.retained_count == 6


def test_apply_creates_verified_backup_batch_and_one_ledger_per_removal(
    database: Database,
    tmp_path: Path,
) -> None:
    evaluation_ids = _seed_simple_duplicates(database)
    expected = compact_evaluations(database, apply=False).plan
    backup_path = tmp_path / "pre-compaction.zip"

    result = apply_evaluation_compaction(
        database.path,
        expected_plan=expected,
        backup_output=backup_path,
        paths=database.paths,
    )

    assert result.applied is True
    assert result.backup_path == backup_path.absolute()
    assert verify_backup(backup_path) == (True, "ok")
    assert set(result.removed_evaluation_ids) == set(evaluation_ids[:2])
    with database.session() as session:
        remaining = set(session.scalars(select(Evaluation.id)))
        batch = session.scalar(select(EvaluationCompactionBatch))
        ledgers = list(
            session.scalars(
                select(EvaluationCompactionLedger).order_by(
                    EvaluationCompactionLedger.removed_evaluation_id
                )
            )
        )
        assert remaining == {evaluation_ids[2]}
        assert batch is not None
        assert batch.id == result.batch_id
        assert batch.manifest_hash == expected.manifest_hash
        assert batch.backup_path == str(backup_path.absolute())
        assert batch.candidate_count == 2
        assert batch.removed_count == 2
        assert batch.retained_count == 1
        assert len(ledgers) == 2
        assert {ledger.removed_evaluation_id for ledger in ledgers} == set(
            evaluation_ids[:2]
        )
        assert all(
            ledger.retained_evaluation_id == evaluation_ids[2] for ledger in ledgers
        )
        assert all(ledger.manifest_hash == expected.manifest_hash for ledger in ledgers)

    ledger_id = ledgers[0].id
    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE evaluation_compaction_ledger "
                    "SET retained_evaluation_id = 'rewritten' WHERE id = :id"
                ),
                {"id": ledger_id},
            )
    with pytest.raises(IntegrityError, match="immutable"):
        with database.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM evaluation_compaction_ledger WHERE id = :id"),
                {"id": ledger_id},
            )


def test_failed_backup_verification_prevents_every_deletion(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluation_ids = _seed_simple_duplicates(database)
    backup_path = tmp_path / "untrusted.zip"
    monkeypatch.setattr(
        compaction,
        "verify_backup",
        lambda _path: (False, "injected verification failure"),
    )

    with pytest.raises(BackupVerificationError, match="injected verification failure"):
        apply_evaluation_compaction(
            database.path,
            backup_output=backup_path,
            paths=database.paths,
        )

    assert backup_path.exists()
    with database.session() as session:
        assert set(session.scalars(select(Evaluation.id))) == set(evaluation_ids)
        assert session.scalar(select(func.count(EvaluationCompactionBatch.id))) == 0
        assert session.scalar(select(func.count(EvaluationCompactionLedger.id))) == 0


def test_verified_backup_identity_includes_content_not_only_file_metadata(
    tmp_path: Path,
) -> None:
    backup = tmp_path / "backup.zip"
    backup.write_bytes(b"original")
    original = compaction._backup_identity(backup)

    backup.write_bytes(b"modified")
    os.utime(backup, ns=(backup.stat().st_atime_ns, original.modified_ns))
    replaced_identity = compaction._backup_identity(backup)
    expected = replace(replaced_identity, content_hash=original.content_hash)

    with pytest.raises(BackupVerificationError, match="changed before deletion"):
        compaction._assert_backup_unchanged(backup, expected)


def test_apply_rolls_back_ledger_and_batch_if_deletion_fails(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluation_ids = _seed_simple_duplicates(database)

    def fail_delete(_session: object, _evaluation_ids: object) -> None:
        raise RuntimeError("injected deletion failure")

    monkeypatch.setattr(compaction, "_delete_candidates", fail_delete)
    with pytest.raises(RuntimeError, match="injected deletion failure"):
        apply_evaluation_compaction(
            database.path,
            backup_output=tmp_path / "rollback-backup.zip",
            paths=database.paths,
        )

    with database.session() as session:
        assert set(session.scalars(select(Evaluation.id))) == set(evaluation_ids)
        assert session.scalar(select(func.count(EvaluationCompactionBatch.id))) == 0
        assert session.scalar(select(func.count(EvaluationCompactionLedger.id))) == 0


def test_stale_expected_plan_is_rejected_before_backup(
    database: Database,
    tmp_path: Path,
) -> None:
    _seed_simple_duplicates(database)
    expected = compact_evaluations(database, apply=False).plan
    second_job = _job(database, source_id="job-2")
    with database.session() as session:
        session.add(
            _evaluation(
                job_id=second_job,
                index=20,
                payload_hash=_hash("unique"),
                is_current=True,
            )
        )
    backup_path = tmp_path / "stale-plan.zip"

    with pytest.raises(StaleCompactionPlanError, match="changed since dry run"):
        apply_evaluation_compaction(
            database.path,
            expected_plan=expected,
            backup_output=backup_path,
            paths=database.paths,
        )

    assert not backup_path.exists()
    with database.session() as session:
        assert session.scalar(select(func.count(Evaluation.id))) == 4


def test_apply_requires_exclusive_storage_lock(tmp_path: Path) -> None:
    paths = _paths(tmp_path / "locked-home").ensure()
    normal_client = Database(paths=paths)
    normal_client.initialize()
    _seed_simple_duplicates(normal_client)
    backup_path = tmp_path / "must-not-exist.zip"
    try:
        with pytest.raises(StorageBusyError, match="close other Jobby processes"):
            apply_evaluation_compaction(
                paths.database,
                backup_output=backup_path,
                paths=paths,
            )
    finally:
        normal_client.dispose()

    assert not backup_path.exists()


def test_apply_with_no_candidates_is_a_non_mutating_noop(
    database: Database,
    tmp_path: Path,
) -> None:
    job_id = _job(database)
    with database.session() as session:
        session.add(
            _evaluation(
                job_id=job_id,
                index=0,
                payload_hash=_hash("unique"),
                is_current=True,
            )
        )
    backup_path = tmp_path / "unnecessary.zip"

    result = apply_evaluation_compaction(
        database.path,
        backup_output=backup_path,
        paths=database.paths,
    )

    assert result.applied is False
    assert result.plan.candidate_count == 0
    assert result.backup_path is None
    assert not backup_path.exists()
