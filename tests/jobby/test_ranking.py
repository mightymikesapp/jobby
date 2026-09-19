from __future__ import annotations

from types import SimpleNamespace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlalchemy import select

from jobby.config import AppConfig
from jobby.db import create_database
from jobby.models import Company, Evaluation, Job, Location, ProfileFact
from jobby.ranking import (
    GateName,
    GateStatus,
    JobEvaluationResult,
    RankingProfile,
    SCORE_WEIGHTS,
    ScoreDimension,
    evaluate_job,
    persist_evaluation,
    ranking_profile_from_config,
    ranking_profile_from_database,
)


def gate(result: JobEvaluationResult, name: GateName):
    return next(item for item in result.gates if item.name == name)


def test_unbounded_numeric_salary_input_degrades_to_unknown() -> None:
    result = evaluate_job(
        {
            "title": "Counsel",
            "description": "General legal work.",
            "salary_min": "1e309",
            "salary_max": "-1e309",
            "salary_currency": "USD",
            "compensation_period": "year",
            "compensation_confidence": 1,
        }
    )

    assert result.facts is not None
    assert result.facts.salary_min is None
    assert result.facts.salary_max is None


@pytest.mark.parametrize("salary", [-1, "-0.5", "1e20"])
def test_invalid_persisted_salary_cannot_trigger_an_automatic_floor_rejection(
    salary,
) -> None:
    result = evaluate_job(
        {
            "title": "Counsel",
            "description": "General legal work.",
            "salary_min": salary,
            "salary_max": salary,
            "salary_currency": "USD",
            "compensation_period": "year",
            "compensation_confidence": 1,
        },
        RankingProfile(salary_floor=100_000),
    )

    assert result.facts is not None
    assert result.facts.salary_min is None
    assert result.facts.salary_max is None
    assert gate(result, GateName.SALARY_FLOOR).status is GateStatus.UNKNOWN


def test_weights_and_final_score_are_exactly_the_stress_adjusted_scorecard():
    assert SCORE_WEIGHTS == {
        ScoreDimension.COMPENSATION: 0.25,
        ScoreDimension.WORKLOAD_STRESS: 0.25,
        ScoreDimension.FIT: 0.20,
        ScoreDimension.GATE_PASSABILITY: 0.15,
        ScoreDimension.STRATEGIC_OPTIONALITY: 0.10,
        ScoreDimension.LOCATION_COL: 0.05,
    }
    assert sum(SCORE_WEIGHTS.values()) == pytest.approx(1.0)

    result = evaluate_job(
        {
            "title": "Legal AI Automation Specialist",
            "description": (
                "Build legal technology and workflow automation for intellectual property teams. "
                "Use legal research and AI governance expertise across product strategy. "
                "Predictable hours, work-life balance, equity, and cross-functional ownership. "
                "Candidates must be authorized to work in the United States without sponsorship."
            ),
            "compensation_text": "$160,000 - $200,000 per year",
            "salary_min": 160_000,
            "salary_max": 200_000,
            "location": "Remote, United States",
            "remote_status": "remote",
        },
        RankingProfile(salary_floor=120_000, years_experience=3),
    )

    assert set(result.components) == set(ScoreDimension)
    assert result.score == pytest.approx(
        round(sum(item.score * item.weight for item in result.components.values()), 2)
    )
    assert 1 <= result.score <= 5
    assert result.final_score == result.weighted_score == result.score
    for dimension, component in result.components.items():
        assert component.weight == SCORE_WEIGHTS[dimension]
        assert component.rationale
        assert 0 <= component.confidence <= 1
    assert result.components[ScoreDimension.FIT].evidence
    assert result.components[ScoreDimension.COMPENSATION].evidence


@pytest.mark.parametrize(
    ("job", "expected", "context"),
    [
        (
            {
                "company": "Federal Trade Commission",
                "title": "Policy Analyst",
                "description": "Federal GS-11 position.",
                "salary_min": 80_000,
                "salary_max": 95_000,
                "location": "Washington, DC",
            },
            GateStatus.PASS,
            "federal",
        ),
        (
            {
                "company": "Private Co",
                "title": "Legal Operations Analyst",
                "salary_min": 120_000,
                "salary_max": 150_000,
                "location": "San Francisco, CA",
            },
            GateStatus.FAIL,
            "Bay Area",
        ),
        (
            {
                "company": "Legal AI Co",
                "title": "Legal AI Specialist",
                "salary_min": 130_000,
                "salary_max": 150_000,
                "location": "Remote",
            },
            GateStatus.WARNING,
            "legal AI",
        ),
    ],
)
def test_contextual_salary_floors_follow_personal_location_and_role_policy(
    job, expected, context
):
    profile = RankingProfile(contextual_salary_floors_enabled=True)

    result = evaluate_job(job, profile)

    salary_gate = gate(result, GateName.SALARY_FLOOR)
    assert salary_gate.status == expected
    assert context in salary_gate.rationale


@pytest.mark.parametrize(
    ("job", "expected_phrase"),
    [
        (
            {
                "company": "Federal Trade Commission",
                "title": "Legal AI Counsel",
                "description": "Federal GS-13 position working on responsible AI.",
                "location": "San Francisco or New York, NY",
            },
            "$71,000 federal salary floor",
        ),
        (
            {
                "company": "Private Co",
                "title": "Legal AI Counsel",
                "description": "Build legal automation.",
                "location": "San Francisco or New York, NY",
            },
            "$181,000 Bay Area salary floor",
        ),
        (
            {
                "company": "Private Co",
                "title": "Legal AI Counsel",
                "description": "Build legal automation.",
                "location": "Manhattan, New York",
            },
            "$161,000 New York City salary floor",
        ),
        (
            {
                "company": "Private Co",
                "title": "Legal AI Counsel",
                "description": "Build legal automation.",
                "location": "Remote",
            },
            "$141,000 legal AI salary floor",
        ),
        (
            {
                "company": "Private Co",
                "title": "Operations Analyst",
                "description": "Manage internal programs.",
                "location": "Remote",
            },
            "$101,000 private-sector salary floor",
        ),
    ],
)
def test_contextual_salary_floor_precedence_is_explicit_and_stable(
    job, expected_phrase
):
    profile = RankingProfile(
        contextual_salary_floors_enabled=True,
        federal_salary_floor=71_000,
        private_salary_floor=101_000,
        legal_ai_salary_floor=141_000,
        nyc_salary_floor=161_000,
        bay_area_salary_floor=181_000,
    )

    first = evaluate_job(job, profile)
    second = evaluate_job(job, profile)

    salary_gate = gate(first, GateName.SALARY_FLOOR)
    assert expected_phrase in salary_gate.rationale
    assert first == second
    assert profile.salary_floor == 0
    assert profile.salary_floor_context is None


def test_explicit_global_salary_floor_overrides_every_contextual_floor():
    result = evaluate_job(
        {
            "company": "Federal Trade Commission",
            "title": "Legal AI Counsel",
            "description": "Federal GS-13 responsible AI position.",
            "salary_min": 125_000,
            "salary_max": 150_000,
            "location": "San Francisco or New York, NY",
        },
        RankingProfile(
            salary_floor=123_000,
            contextual_salary_floors_enabled=True,
            federal_salary_floor=71_000,
            private_salary_floor=101_000,
            legal_ai_salary_floor=141_000,
            nyc_salary_floor=161_000,
            bay_area_salary_floor=181_000,
        ),
    )

    salary_gate = gate(result, GateName.SALARY_FLOOR)
    assert salary_gate.status == GateStatus.PASS
    assert "$123,000 salary floor" in salary_gate.rationale
    assert "federal salary floor" not in salary_gate.rationale
    assert (
        "$123,000 salary floor"
        in result.components[ScoreDimension.COMPENSATION].rationale
    )


def test_federal_clients_do_not_turn_a_private_job_into_a_federal_role():
    result = evaluate_job(
        {
            "company": "Federal Compliance Partners",
            "title": "Government Contracts Analyst",
            "description": (
                "Advise federal agencies, commissions, and government departments "
                "as private-sector clients."
            ),
            "location": "Remote",
        },
        RankingProfile(
            contextual_salary_floors_enabled=True,
            federal_salary_floor=74_000,
            private_salary_floor=100_000,
        ),
    )

    assert (
        "$100,000 private-sector salary floor"
        in gate(result, GateName.SALARY_FLOOR).rationale
    )


def test_contextual_policy_is_opt_in_for_direct_ranking_calls():
    job = {
        "company": "Private Co",
        "title": "Operations Analyst",
        "description": "Manage internal programs.",
        "salary_min": 90_000,
        "salary_max": 110_000,
        "location": "Remote",
    }

    original_default = evaluate_job(job)
    explicitly_disabled = evaluate_job(
        job, RankingProfile(contextual_salary_floors_enabled=False)
    )

    assert original_default == explicitly_disabled
    assert gate(original_default, GateName.SALARY_FLOOR).rationale == (
        "Compensation evidence is available and no salary floor is configured."
    )


def test_config_adapter_preserves_old_config_compatibility_and_personal_policy():
    old_config_profile = ranking_profile_from_config(
        SimpleNamespace(salary_floor=95_000)
    )
    personal_profile = ranking_profile_from_config(
        SimpleNamespace(
            salary_floor=0,
            contextual_salary_floors_enabled=True,
            federal_salary_floor=74_000,
            private_salary_floor=100_000,
            legal_ai_salary_floor=140_000,
            nyc_salary_floor=160_000,
            bay_area_salary_floor=180_000,
            unrelated_setting="ignored",
        )
    )

    assert old_config_profile.salary_floor == 95_000
    assert old_config_profile.contextual_salary_floors_enabled is False
    assert personal_profile.model_dump(
        include={
            "salary_floor",
            "contextual_salary_floors_enabled",
            "federal_salary_floor",
            "private_salary_floor",
            "legal_ai_salary_floor",
            "nyc_salary_floor",
            "bay_area_salary_floor",
        }
    ) == {
        "salary_floor": 0,
        "contextual_salary_floors_enabled": True,
        "federal_salary_floor": 74_000,
        "private_salary_floor": 100_000,
        "legal_ai_salary_floor": 140_000,
        "nyc_salary_floor": 160_000,
        "bay_area_salary_floor": 180_000,
    }


def test_application_defaults_enable_the_personal_contextual_policy():
    profile = ranking_profile_from_config(AppConfig())

    assert profile.contextual_salary_floors_enabled is True
    assert profile.salary_floor == 0
    assert profile.federal_salary_floor == 74_000
    assert profile.private_salary_floor == 100_000
    assert profile.legal_ai_salary_floor == 140_000
    assert profile.nyc_salary_floor == 160_000
    assert profile.bay_area_salary_floor == 180_000


def test_candidate_authorization_defaults_are_unknown_not_optimistic():
    profile = RankingProfile()

    assert profile.work_authorized is None
    assert profile.us_citizen is None


def test_database_profile_uses_only_explicit_approved_facts_with_provenance(
    tmp_path,
):
    database = create_database(tmp_path / "profile.sqlite3")
    try:
        with database.session() as session:
            session.add_all(
                [
                    ProfileFact(
                        fact_key="candidate.citizenship",
                        value_json="U.S. Citizen",
                        approved=True,
                        content_hash="a" * 64,
                    ),
                    ProfileFact(
                        fact_key="location.visa_status",
                        value_json="U.S. Citizen — no sponsorship needed",
                        approved=True,
                        content_hash="b" * 64,
                    ),
                    ProfileFact(
                        fact_key="experience.years_experience",
                        value_json=4,
                        approved=True,
                        content_hash="c" * 64,
                    ),
                    ProfileFact(
                        fact_key="credentials.bar_admissions",
                        value_json=["California"],
                        approved=True,
                        content_hash="d" * 64,
                    ),
                    ProfileFact(
                        fact_key="location.flexibility",
                        value_json=["San Diego, Los Angeles, Remote"],
                        approved=True,
                        content_hash="e" * 64,
                    ),
                    ProfileFact(
                        fact_key="narrative.headline",
                        value_json="Legal AI platform founder",
                        approved=True,
                        content_hash="f" * 64,
                    ),
                    ProfileFact(
                        fact_key="unreviewed.work_authorized",
                        value_json=False,
                        approved=False,
                        content_hash="0" * 64,
                    ),
                ]
            )

        profile = ranking_profile_from_database(database, AppConfig())

        assert profile.us_citizen is True
        assert profile.work_authorized is True
        assert profile.years_experience == 4
        assert profile.bar_admissions_known is True
        assert profile.bar_admissions == ["California"]
        assert {"San Diego", "Los Angeles", "Remote"}.issubset(
            profile.preferred_locations
        )
        assert profile.edge_assets["legal_ai_build"] == ["legal ai"]
        assert "unreviewed.work_authorized" not in {
            item.fact_key for item in profile.profile_evidence
        }
        assert {item.field for item in profile.profile_evidence}.issuperset(
            {
                "us_citizen",
                "work_authorized",
                "years_experience",
                "bar_admissions",
                "preferred_locations",
                "edge_assets.legal_ai_build",
            }
        )

        result = evaluate_job(
            {
                "title": "Legal AI Counsel",
                "description": (
                    "Active California Bar membership is required. Minimum 3 years "
                    "experience. Must be a U.S. citizen."
                ),
                "location": "Remote",
            },
            profile,
        )
        assert gate(result, GateName.BAR_ADMISSION).status == GateStatus.PASS
        assert gate(result, GateName.EXPERIENCE_YEARS).status == GateStatus.PASS
        assert gate(result, GateName.WORK_AUTHORIZATION).status == GateStatus.PASS
        assert result.profile_evidence
        assert "approved profile fact(s)" in result.explanation
    finally:
        database.dispose()


def test_unapproved_or_changed_fact_cannot_keep_an_automatic_gate_pass(tmp_path):
    database = create_database(tmp_path / "changed-profile.sqlite3")
    try:
        with database.session() as session:
            fact = ProfileFact(
                fact_key="candidate.work_authorization",
                value_json="Authorized to work in the United States",
                approved=True,
                content_hash="a" * 64,
            )
            session.add(fact)
            session.flush()
            fact_id = fact.id

        assert (
            ranking_profile_from_database(database, AppConfig()).work_authorized is True
        )

        with database.session() as session:
            fact = session.get(ProfileFact, fact_id)
            assert fact is not None
            fact.value_json = "Changed source value requiring review"
            fact.content_hash = "b" * 64
            fact.approved = False
            fact.approved_at = None

        changed = ranking_profile_from_database(database, AppConfig())
        assert changed.work_authorized is None
        assert all(item.profile_fact_id != fact_id for item in changed.profile_evidence)
        assert any(
            "work authorization is unknown" in item for item in changed.profile_warnings
        )
    finally:
        database.dispose()


def test_anticipated_bar_admission_is_never_treated_as_current(tmp_path):
    database = create_database(tmp_path / "future-bar.sqlite3")
    try:
        with database.session() as session:
            session.add(
                ProfileFact(
                    fact_key="education.bar_admission_anticipated",
                    value_json="California, October 2026",
                    approved=True,
                    content_hash="a" * 64,
                )
            )

        profile = ranking_profile_from_database(database, AppConfig())
        result = evaluate_job(
            {
                "title": "Counsel",
                "description": "Active California Bar membership is required.",
            },
            profile,
        )

        assert profile.bar_admissions == []
        assert profile.bar_admissions_known is False
        assert gate(result, GateName.BAR_ADMISSION).status == GateStatus.UNKNOWN
        assert gate(result, GateName.BAR_ADMISSION).blocking is False
        assert any("anticipated" in warning for warning in result.warnings)
    finally:
        database.dispose()


def test_unpaid_work_is_an_automatic_skip_with_verbatim_evidence():
    result = evaluate_job(
        {
            "title": "AI Policy Fellow",
            "description": "This is an unpaid fellowship. Fellows conduct AI governance research remotely.",
            "location": "Remote",
        }
    )

    unpaid = gate(result, GateName.UNPAID_WORK)
    assert result.automatic_skip is True
    assert result.ranking_eligible is False
    assert result.skip_reason
    assert unpaid.status == GateStatus.FAIL
    assert unpaid.blocking is True
    assert "unpaid fellowship" in unpaid.evidence[0].passage.lower()
    assert result.components[ScoreDimension.COMPENSATION].score == 1


def test_unpaid_leave_is_not_unpaid_work_and_negated_stress_traps_are_positive_evidence():
    result = evaluate_job(
        {
            "title": "Legal Operations Analyst",
            "description": (
                "Benefits include paid and unpaid leave. This role has no billable hours, "
                "no sales quota, and no on-call work."
            ),
            "salary_min": 100_000,
            "salary_max": 120_000,
            "remote": True,
        }
    )

    assert result.automatic_skip is False
    assert gate(result, GateName.UNPAID_WORK).status == GateStatus.PASS
    assert gate(result, GateName.BILLABLES).status == GateStatus.PASS
    assert gate(result, GateName.QUOTA).status == GateStatus.PASS
    assert gate(result, GateName.ON_CALL).status == GateStatus.PASS
    assert result.components[ScoreDimension.WORKLOAD_STRESS].score == 5
    assert result.components[ScoreDimension.LOCATION_COL].score == 5


def test_explicit_gates_and_stress_warnings_are_evidence_backed():
    result = evaluate_job(
        {
            "title": "Product Counsel",
            "description": (
                "Active California Bar membership is required. Minimum 5 years experience. "
                "Must be a U.S. citizen. Annual billable target is 1,900 hours. "
                "This quota-carrying role requires up to 50% travel and an on-call weekend rotation."
            ),
            "compensation_text": "$90,000-$105,000",
            "salary_currency": "USD",
            "compensation_period": "year",
            "compensation_confidence": 1.0,
            "location": "On-site, San Francisco, CA",
        },
        RankingProfile(
            salary_floor=120_000,
            years_experience=2,
            bar_admissions=[],
            us_citizen=True,
            strict_location=True,
            max_travel_percent=20,
        ),
    )

    expected_failures = {
        GateName.BAR_ADMISSION,
        GateName.EXPERIENCE_YEARS,
        GateName.SALARY_FLOOR,
        GateName.LOCATION,
        GateName.BILLABLES,
        GateName.TRAVEL,
        GateName.ON_CALL,
    }
    for name in expected_failures:
        item = gate(result, name)
        assert item.status == GateStatus.FAIL
        assert item.evidence, name
        assert item.evidence[0].passage
    assert gate(result, GateName.WORK_AUTHORIZATION).status == GateStatus.PASS
    assert gate(result, GateName.QUOTA).status == GateStatus.WARNING
    assert result.components[ScoreDimension.WORKLOAD_STRESS].score == 1
    assert result.components[ScoreDimension.GATE_PASSABILITY].score == 1


def test_missing_data_is_neutral_low_confidence_and_visible_not_an_assumed_pass():
    result = evaluate_job({"title": "Analyst", "description": "Join our team."})

    compensation = result.components[ScoreDimension.COMPENSATION]
    workload = result.components[ScoreDimension.WORKLOAD_STRESS]
    location = result.components[ScoreDimension.LOCATION_COL]
    assert compensation.score == workload.score == location.score == 3
    assert compensation.missing_data_warning in result.warnings
    assert workload.missing_data_warning in result.warnings
    assert location.missing_data_warning in result.warnings
    assert gate(result, GateName.BAR_ADMISSION).status == GateStatus.UNKNOWN
    assert gate(result, GateName.EXPERIENCE_YEARS).status == GateStatus.UNKNOWN
    assert gate(result, GateName.WORK_AUTHORIZATION).status == GateStatus.UNKNOWN


@settings(max_examples=100, deadline=None)
@given(
    title=st.text(max_size=200),
    description=st.text(max_size=2_000),
    compensation=st.text(max_size=200),
    location=st.text(max_size=200),
)
def test_deterministic_ranking_is_total_and_bounded_for_arbitrary_posting_text(
    title: str,
    description: str,
    compensation: str,
    location: str,
) -> None:
    result = evaluate_job(
        {
            "title": title,
            "description": description,
            "compensation_text": compensation,
            "location": location,
        }
    )

    assert 1.0 <= result.score <= 5.0
    assert 0.0 <= result.confidence <= 1.0
    assert set(result.components) == set(ScoreDimension)
    assert all(
        len(passage.passage) <= 320
        for component in result.components.values()
        for passage in component.evidence
    )
    assert all(
        len(passage.passage) <= 320
        for extracted_gate in result.gates
        for passage in extracted_gate.evidence
    )


def test_locked_manual_result_is_preserved_until_explicit_unlock():
    original = evaluate_job(
        {"title": "Manual role", "description": "Legal AI role", "location": "Remote"},
        manual_override={
            "score": 4.8,
            "explanation": "Reviewed by Mike",
            "locked": True,
        },
    )
    changed_job = {
        "title": "Changed role",
        "description": "Unpaid role with 2,400 billable hours and on-call weekends.",
        "location": "San Francisco",
    }

    preserved = evaluate_job(changed_job, existing_evaluation=original)
    assert preserved.score == 4.8
    assert preserved.rescored is False
    assert preserved.preserved_manual_override is True
    assert preserved.explanation == "Reviewed by Mike"

    unlocked = evaluate_job(
        changed_job,
        existing_evaluation=original,
        unlock_manual_override=True,
    )
    assert unlocked.rescored is True
    assert unlocked.manual_override is False
    assert unlocked.automatic_skip is True
    assert unlocked.score != original.score


def test_persistence_replaces_current_auto_score_but_protects_locked_manual_override(
    tmp_path,
):
    database = create_database(tmp_path / "jobby.sqlite3")
    try:
        with database.session() as session:
            company = Company(name="Example", normalized_name="example")
            location = Location(
                display_name="Remote", normalized_key="remote", remote=True
            )
            session.add_all([company, location])
            session.flush()
            job = Job(
                company_id=company.id,
                location_id=location.id,
                title="Legal AI Specialist",
                normalized_title="legal ai specialist",
                description="Legal technology and patent workflow automation with predictable hours.",
                compensation_text="$140,000-$170,000",
                salary_min=140_000,
                salary_max=170_000,
                remote_status="remote",
            )
            session.add(job)
            session.flush()
            job_id = job.id

        with database.session() as session:
            job = session.get(Job, job_id)
            first = evaluate_job(job, RankingProfile(salary_floor=100_000))
            first_row = persist_evaluation(session, job, first)
            assert first_row.is_current is True
            assert job.latest_score == first.score

        with database.session() as session:
            job = session.get(Job, job_id)
            manual = evaluate_job(job, manual_override={"score": 4.9, "locked": True})
            manual_row = persist_evaluation(session, job, manual)
            manual_id = manual_row.id

        with database.session() as session:
            job = session.get(Job, job_id)
            offered_auto = evaluate_job(
                {"title": job.title, "description": "Unpaid role."}
            )
            preserved_row = persist_evaluation(session, job, offered_auto)
            assert preserved_row.id == manual_id
            assert preserved_row.score == 4.9
            assert job.latest_score == 4.9

        with database.session() as session:
            job = session.get(Job, job_id)
            replacement = evaluate_job(
                {"title": job.title, "description": "Unpaid role."}
            )
            replacement_row = persist_evaluation(
                session,
                job,
                replacement,
                unlock_manual_override=True,
            )
            assert replacement_row.id != manual_id
            assert replacement_row.automatic_skip is True
            rows = session.scalars(
                select(Evaluation)
                .where(Evaluation.job_id == job_id)
                .order_by(Evaluation.created_at)
            ).all()
            assert len(rows) == 3
            assert sum(row.is_current for row in rows) == 1
            assert rows[-1].is_current is True
    finally:
        database.dispose()
