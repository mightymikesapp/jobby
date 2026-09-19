from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading

import pytest
from sqlalchemy import func, select

from jobby.db import Database
from jobby.models import AuditEvent, Company, Evaluation, Job
from jobby.ranking import RANKER_VERSION, RankingProfile, persist_evaluation


@pytest.fixture
def seeded_database(tmp_path) -> tuple[Database, str]:
    database = Database(tmp_path / "jobby.sqlite3")
    database.initialize()
    with database.session() as session:
        company = Company(name="Example", normalized_name="example")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Legal AI Counsel",
            normalized_title="legal ai counsel",
            description="Legal technology, intellectual property, and policy work.",
            source_primary="manual",
            source_id="one",
            launch_url="https://jobs.example.test/one?ref=exact",
            comparison_url="https://jobs.example.test/one",
            canonical_url="https://jobs.example.test/one",
        )
        session.add(job)
        session.flush()
        job_id = job.id
    yield database, job_id
    database.dispose()


def test_automatic_rescan_reuses_payload_without_audit_growth(seeded_database) -> None:
    database, job_id = seeded_database
    profile = RankingProfile(salary_floor=100_000)
    with database.session() as session:
        first = persist_evaluation(session, job_id, profile=profile)
        first_id = first.id
    with database.session() as session:
        second = persist_evaluation(session, job_id, profile=profile)
        assert second.id == first_id

    with database.session() as session:
        assert session.scalar(select(func.count(Evaluation.id))) == 1
        assert (
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "job.evaluated"
                )
            )
            == 1
        )
        row = session.get(Evaluation, first_id)
        assert row is not None
        assert row.ranker_version == RANKER_VERSION
        assert row.fingerprint is not None and row.payload_hash is not None


def test_policy_change_and_explicit_reruns_remain_distinct(seeded_database) -> None:
    database, job_id = seeded_database
    with database.session() as session:
        first = persist_evaluation(session, job_id, profile=RankingProfile())
        second = persist_evaluation(
            session,
            job_id,
            profile=RankingProfile(strict_location=True),
        )
        forced = persist_evaluation(session, job_id, force_rerun=True)
        referenced = persist_evaluation(
            session,
            job_id,
            reference_key="application-review-1",
        )

        assert len({first.id, second.id, forced.id, referenced.id}) == 4
        assert forced.evaluation_kind == "forced" and forced.fingerprint is None
        assert referenced.evaluation_kind == "referenced"
        assert referenced.reference_key == "application-review-1"


def test_concurrent_automatic_attempts_converge_on_one_row(seeded_database) -> None:
    database, job_id = seeded_database
    barrier = threading.Barrier(2)

    def evaluate() -> str:
        with database.session() as session:
            barrier.wait(timeout=5)
            return persist_evaluation(session, job_id).id

    with ThreadPoolExecutor(max_workers=2) as executor:
        identifiers = list(executor.map(lambda _index: evaluate(), range(2)))

    assert len(set(identifiers)) == 1
    with database.session() as session:
        assert session.scalar(select(func.count(Evaluation.id))) == 1
        assert (
            session.scalar(
                select(func.count(Evaluation.id)).where(Evaluation.is_current.is_(True))
            )
            == 1
        )
