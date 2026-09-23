from __future__ import annotations

import pytest
from sqlalchemy import select

from jobby.config import AppConfig
from jobby.db import create_database
from jobby.enums import JobStatus
from jobby.maintenance import rescore_evaluations
from jobby.models import Company, Job, Location, ProfileFact
from jobby.ranking import (
    GateName,
    GateStatus,
    JobEvaluationResult,
    RankingProfile,
    ScoreDimension,
    evaluate_job,
    ranking_profile_from_config,
    ranking_profile_from_database,
)

NOT_ADMITTED = AppConfig(
    ranking_bar_status="not_admitted", ranking_skip_credential_gaps=True
)


def gate(result: JobEvaluationResult, name: GateName):
    return next(item for item in result.gates if item.name == name)


def not_admitted() -> RankingProfile:
    return ranking_profile_from_config(NOT_ADMITTED)


def test_bar_status_setting_maps_to_the_ranking_profile() -> None:
    unknown = ranking_profile_from_config(AppConfig())
    missing = not_admitted()
    admitted = ranking_profile_from_config(
        AppConfig(
            ranking_bar_status="admitted", ranking_bar_jurisdictions=["California"]
        )
    )
    anywhere = ranking_profile_from_config(AppConfig(ranking_bar_status="admitted"))

    # The default adds no bar policy: no legal-years cap and no skipping.
    assert unknown.max_legal_years is None and unknown.skip_credential_gaps is False
    assert missing.bar_admissions_known is True and missing.bar_admissions == []
    assert missing.max_legal_years == 2 and missing.skip_credential_gaps is True
    assert (
        admitted.bar_admissions == ["California"] and admitted.max_legal_years is None
    )
    assert anywhere.bar_admissions_known is True and anywhere.bar_admissions


@pytest.mark.parametrize(
    "requirement",
    [
        "Membership in the Washington State Bar Association.",
        "Have a JD and a license or qualification to practice in California.",
        "JD from an accredited law school and active membership in at least one U.S. state bar.",
        "Hold a JD and are an active member of at least one U.S. state bar.",
        "JD degree and admission to practice law in a U.S. jurisdiction.",
    ],
)
def test_required_bar_phrasings_skip_a_candidate_not_yet_admitted(
    requirement: str,
) -> None:
    result = evaluate_job(
        {
            "title": "Product Counsel",
            "description": f"Minimum qualifications: {requirement}",
        },
        not_admitted(),
    )

    assert gate(result, GateName.BAR_ADMISSION).status is GateStatus.FAIL
    assert result.automatic_skip
    assert result.skip_reason == "Required bar admission is not held"
    # Compensation keeps its own meaning; only unpaid work scores 1.0.
    assert result.components[ScoreDimension.COMPENSATION].score == 3.0


@pytest.mark.parametrize(
    "description",
    [
        "Education: JD. Bar admission in any jurisdiction strongly preferred.",
        "Desirable Skills, Knowledge, and Experience: Been admitted to practice law in the US.",
    ],
)
def test_preferred_bar_admission_warns_but_does_not_skip(description: str) -> None:
    result = evaluate_job(
        {"title": "Legal Fellow", "description": description}, not_admitted()
    )

    assert gate(result, GateName.BAR_ADMISSION).status is GateStatus.WARNING
    assert not result.automatic_skip


@pytest.mark.parametrize(
    ("title", "description", "skipped"),
    [
        (
            "Commercial Counsel",
            "7+ years of relevant post-qualification legal experience.",
            True,
        ),
        (
            "Analyst, Content Clearance",
            "3 to 5 years of legal clearance experience.",
            True,
        ),
        # On a Counsel title, the experience asked for is legal practice.
        (
            "Privacy Counsel",
            "4+ years of data security, data privacy, data protection and governance experience.",
            True,
        ),
        ("Legal Assistant", "2 years of legal experience.", False),
        # "legal" after a comma belongs to another clause.
        (
            "Compliance Manager",
            "8-10 years of experience in healthcare compliance, and are familiar with legal frameworks.",
            False,
        ),
    ],
)
def test_legal_experience_beyond_two_years_is_unmet_before_admission(
    title: str, description: str, skipped: bool
) -> None:
    result = evaluate_job({"title": title, "description": description}, not_admitted())

    assert result.automatic_skip is skipped
    if skipped:
        assert gate(result, GateName.EXPERIENCE_YEARS).blocking


def test_legal_experience_under_preferred_qualifications_only_warns() -> None:
    result = evaluate_job(
        {
            "title": "Privacy Counsel",
            "description": (
                "Minimum qualifications: excellent legal writing. Preferred qualifications: "
                "At least 8 years of privacy and data protection legal experience."
            ),
        },
        not_admitted(),
    )

    assert gate(result, GateName.EXPERIENCE_YEARS).status is GateStatus.WARNING
    assert not result.automatic_skip


def test_skipping_is_opt_in_and_admission_lifts_the_gate() -> None:
    posting = {
        "title": "Counsel",
        "description": "Must be admitted to the California bar.",
    }
    without_skip = evaluate_job(
        posting,
        ranking_profile_from_config(AppConfig(ranking_bar_status="not_admitted")),
    )
    admitted = evaluate_job(
        posting,
        ranking_profile_from_config(
            AppConfig(
                ranking_bar_status="admitted",
                ranking_bar_jurisdictions=["California"],
                ranking_skip_credential_gaps=True,
            )
        ),
    )

    assert gate(without_skip, GateName.BAR_ADMISSION).status is GateStatus.FAIL
    assert not without_skip.automatic_skip
    assert gate(admitted, GateName.BAR_ADMISSION).status is GateStatus.PASS
    assert not admitted.automatic_skip


def test_an_approved_admission_fact_outranks_the_configured_status(tmp_path) -> None:
    database = create_database(tmp_path / "facts.sqlite3")
    try:
        with database.session() as session:
            session.add(
                ProfileFact(
                    fact_key="credentials.bar_admissions",
                    value_json=["California"],
                    approved=True,
                    content_hash="a" * 64,
                )
            )
        profile = ranking_profile_from_database(database, NOT_ADMITTED)

        assert profile.bar_admissions == ["California"]
        assert profile.max_legal_years is None
        result = evaluate_job(
            {
                "title": "Counsel",
                "description": "Must be admitted to the California bar.",
            },
            profile,
        )
        assert gate(result, GateName.BAR_ADMISSION).status is GateStatus.PASS
    finally:
        database.dispose()


def test_rescore_ignores_skipped_roles_and_reopens_them_after_admission(
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
            barred, locked = (
                Job(
                    company_id=company.id,
                    location_id=location.id,
                    title="Counsel",
                    normalized_title="counsel",
                    description="Must be admitted to the California bar.",
                    manual_status_locked=lock,
                )
                for lock in (False, True)
            )
            session.add_all([barred, locked])
            session.flush()
            barred_id, locked_id = barred.id, locked.id

        rescore_evaluations(database, NOT_ADMITTED)
        with database.session() as session:
            assert session.get(Job, barred_id).status == JobStatus.IGNORED
            assert session.get(Job, locked_id).status == JobStatus.DISCOVERED

        rescore_evaluations(
            database,
            AppConfig(
                ranking_bar_status="admitted",
                ranking_bar_jurisdictions=["California"],
                ranking_skip_credential_gaps=True,
            ),
        )
        with database.session() as session:
            assert session.get(Job, barred_id).status == JobStatus.DISCOVERED
            assert len(session.scalars(select(Job)).all()) == 2
    finally:
        database.dispose()
