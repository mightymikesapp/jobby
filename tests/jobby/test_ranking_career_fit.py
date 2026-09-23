from __future__ import annotations

import pytest
from sqlalchemy import select

from jobby.config import AppConfig
from jobby.db import create_database
from jobby.maintenance import rescore_evaluations
from jobby.models import Company, Evaluation, Job, Location
from jobby.ranking import (
    RANKER_VERSION,
    GateName,
    GateStatus,
    JobEvaluationResult,
    RankingProfile,
    ScoreDimension,
    evaluate_job,
    persist_evaluation,
    ranking_profile_from_config,
)

EXCLUDED = ["software engineer", "engineer", "recruiting", "sales", "paralegal"]


def gate(result: JobEvaluationResult, name: GateName):
    return next(item for item in result.gates if item.name == name)


def early(**updates) -> RankingProfile:
    return RankingProfile(
        target_seniority="early", excluded_title_terms=EXCLUDED, **updates
    )


def fit(result: JobEvaluationResult) -> float:
    return result.components[ScoreDimension.FIT].score


def test_a_legal_title_outranks_a_well_paid_excluded_role() -> None:
    """The regression that motivated v3: pay and perks must not beat fit."""

    fellow = evaluate_job(
        {
            "title": "Legal Fellow (Spring 2027)",
            "description": (
                "Recent law school graduates research complex copyright questions, "
                "draft licensing agreements, and develop internal policies."
            ),
            "salary_min": 76_960,
            "salary_max": 94_848,
            "location": "Remote",
        },
        early(salary_floor=100_000),
    )
    engineer = evaluate_job(
        {
            "title": "Senior Software Engineer, Content Platform",
            "description": (
                "5+ years of related professional experience in software engineering. "
                "Protect intellectual property rights. Work-life balance and equity."
            ),
            "salary_min": 116_633,
            "salary_max": 181_243,
            "location": "Remote",
        },
        early(salary_floor=100_000),
    )

    assert fellow.score > engineer.score
    assert fit(engineer) == 1.0
    assert gate(engineer, GateName.ROLE_FAMILY).status is GateStatus.FAIL
    assert gate(engineer, GateName.SENIORITY).status is GateStatus.WARNING
    assert gate(engineer, GateName.EXPERIENCE_YEARS).status is GateStatus.FAIL


def test_title_role_families_outweigh_description_boilerplate() -> None:
    profile = early()
    titled = evaluate_job(
        {"title": "Copyright Counsel", "description": "Join our team."}, profile
    )
    boilerplate = evaluate_job(
        {
            "title": "Operations Coordinator",
            "description": "Respect our intellectual property and legal research policies.",
        },
        profile,
    )

    assert fit(titled) > fit(boilerplate)
    assert "Title role families: ip_patent, jd_legal" in (
        titled.components[ScoreDimension.FIT].rationale
    )


@pytest.mark.parametrize(
    ("title", "excluded"),
    [
        ("Legal Engineer", False),  # a target family overrides "engineer"
        ("Trademark Paralegal", False),  # specialized, names a target family
        ("Litigation Paralegal", True),  # generic paralegal
        ("Recruiting Coordinator - Contract", True),  # "Contract" = employment type
        ("Contract Manager", False),
        ("Account Sales Lead", True),
    ],
)
def test_excluded_role_families_yield_to_target_families(
    title: str, excluded: bool
) -> None:
    result = evaluate_job({"title": title, "description": ""}, early())

    role_family = [item for item in result.gates if item.name == GateName.ROLE_FAMILY]
    assert (fit(result) == 1.0) is excluded
    assert bool(role_family) is excluded


@pytest.mark.parametrize(
    ("title", "stage", "expected"),
    [
        ("Director, Legal Affairs", "early", GateStatus.FAIL),
        ("Associate General Counsel", "early", GateStatus.FAIL),
        ("Senior Counsel", "early", GateStatus.WARNING),
        ("Sr. Policy Analyst", "early", GateStatus.WARNING),
        ("Staff Attorney", "early", GateStatus.PASS),
        ("Business Partner, Compliance", "early", GateStatus.PASS),
        ("Counsel", "early", GateStatus.PASS),
        ("Director, Legal Affairs", "mid", GateStatus.WARNING),
        ("Senior Counsel", "mid", GateStatus.PASS),
        ("Director, Legal Affairs", "any", GateStatus.NOT_APPLICABLE),
        ("Director, Legal Affairs", "senior", GateStatus.NOT_APPLICABLE),
    ],
)
def test_seniority_gate_follows_the_configured_career_stage(
    title: str, stage: str, expected: GateStatus
) -> None:
    result = evaluate_job(
        {"title": title, "description": ""}, RankingProfile(target_seniority=stage)
    )

    assert gate(result, GateName.SENIORITY).status is expected


@pytest.mark.parametrize(
    ("requirement", "expected"),
    [
        ("at least 2 years of practicing law", GateStatus.PASS),
        ("4-6 years of experience in compliance", GateStatus.WARNING),
        ("5+ years of related professional experience", GateStatus.FAIL),
        ("10 years’ experience", GateStatus.FAIL),
    ],
)
def test_unstated_candidate_years_use_the_career_stage(
    requirement: str, expected: GateStatus
) -> None:
    result = evaluate_job(
        {"title": "Counsel", "description": f"Requires {requirement}."},
        RankingProfile(target_seniority="early"),
    )

    assert gate(result, GateName.EXPERIENCE_YEARS).status is expected


def test_an_approved_years_fact_still_overrides_the_career_stage() -> None:
    result = evaluate_job(
        {"title": "Counsel", "description": "Minimum 5 years experience."},
        RankingProfile(target_seniority="early", years_experience=6),
    )

    assert gate(result, GateName.EXPERIENCE_YEARS).status is GateStatus.PASS


def test_a_stated_but_unconfirmed_requirement_costs_more_than_silence() -> None:
    profile = RankingProfile(bar_admissions_known=False)
    stated = evaluate_job(
        {
            "title": "Counsel",
            "description": "Active California Bar membership is required.",
        },
        profile,
    )
    silent = evaluate_job({"title": "Counsel", "description": "Join us."}, profile)

    assert gate(stated, GateName.BAR_ADMISSION).status is GateStatus.UNKNOWN
    assert (
        stated.components[ScoreDimension.GATE_PASSABILITY].score
        < silent.components[ScoreDimension.GATE_PASSABILITY].score
    )


@pytest.mark.parametrize(
    ("salary_max", "expected"),
    [(95_000, 2.5), (60_000, 1.75), (40_000, 1.25)],
)
def test_pay_under_the_floor_is_graded_and_never_ties_with_unpaid(
    salary_max: int, expected: float
) -> None:
    result = evaluate_job(
        {
            "title": "Counsel",
            "description": "",
            "salary_min": salary_max - 5_000,
            "salary_max": salary_max,
        },
        RankingProfile(salary_floor=100_000),
    )
    unpaid = evaluate_job(
        {"title": "Counsel", "description": "This is an unpaid internship position."}
    )

    assert result.components[ScoreDimension.COMPENSATION].score == expected
    assert unpaid.components[ScoreDimension.COMPENSATION].score == 1.0


def test_config_supplies_career_stage_and_exclusions_with_safe_defaults() -> None:
    default = ranking_profile_from_config(AppConfig())
    configured = ranking_profile_from_config(
        AppConfig(
            ranking_target_seniority="early",
            ranking_excluded_title_terms=["software engineer", " sales "],
        )
    )

    assert default.target_seniority == "any"
    assert default.excluded_title_terms == []
    assert configured.target_seniority == "early"
    assert configured.excluded_title_terms == ["software engineer", "sales"]
    with pytest.raises(ValueError):
        RankingProfile(target_seniority="intern")


def test_rescore_moves_every_job_to_the_current_ranker_and_keeps_history(
    tmp_path,
) -> None:
    database = create_database(tmp_path / "jobby.sqlite3")
    try:
        with database.session() as session:
            company = Company(name="Example", normalized_name="example")
            location = Location(
                display_name="Remote", normalized_key="remote", remote=True
            )
            session.add_all([company, location])
            session.flush()
            jobs = [
                Job(
                    company_id=company.id,
                    location_id=location.id,
                    title=title,
                    normalized_title=title.casefold(),
                    description="5+ years of related professional experience.",
                    remote_status="remote",
                )
                for title in ("Senior Software Engineer", "Copyright Counsel", "Pinned")
            ]
            session.add_all(jobs)
            session.flush()
            ids = [job.id for job in jobs]
            for job in jobs[:2]:
                persist_evaluation(session, job, profile=RankingProfile())
            pinned = evaluate_job(
                jobs[2], manual_override={"score": 4.9, "locked": True}
            )
            persist_evaluation(session, jobs[2], pinned)

        config = AppConfig(
            ranking_target_seniority="early",
            ranking_excluded_title_terms=["software engineer"],
        )
        first = rescore_evaluations(database, config)
        second = rescore_evaluations(database, config)

        assert first.kind == "rescore"
        assert first.changed == 2  # the locked manual score is untouched
        assert second.changed == 0  # identical results are reused, not duplicated
        with database.session() as session:
            current = {
                row.job_id: row
                for row in session.scalars(
                    select(Evaluation).where(Evaluation.is_current.is_(True))
                )
            }
            history = session.scalars(select(Evaluation)).all()
        assert current[ids[0]].score < current[ids[1]].score
        assert current[ids[0]].ranker_version == RANKER_VERSION
        assert current[ids[2]].score == 4.9 and current[ids[2]].locked
        assert len(history) == 5  # two superseded rows remain as history
    finally:
        database.dispose()
