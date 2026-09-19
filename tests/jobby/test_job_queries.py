"""Focused tests for deterministic job listing filters and ordering."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import event, select

from jobby.db import Database
from jobby.enums import JobStatus
from jobby.job_queries import (
    JobListFilters,
    JobSort,
    RankedView,
    query_jobs,
    query_jobs_page,
    query_ranked_page,
)
from jobby.models import (
    CanonicalJobGroup,
    CanonicalJobMember,
    Company,
    Evaluation,
    Job,
    Location,
    SearchIndexState,
)


def _database(tmp_path) -> Database:
    database = Database(tmp_path / "job-queries.sqlite3")
    database.initialize()
    return database


def _seed(database: Database) -> None:
    with database.session() as session:
        alpha = Company(
            id="company-alpha",
            name="Alpha Legal AI",
            normalized_name="alpha legal ai",
        )
        beta = Company(
            id="company-beta",
            name="Beta Energy",
            normalized_name="beta energy",
        )
        percent = Company(
            id="company-percent",
            name="Percent%Works",
            normalized_name="percent works",
        )
        remote = Location(
            id="location-remote",
            display_name="Remote — California",
            normalized_key="remote california",
            remote=True,
        )
        san_diego = Location(
            id="location-sd",
            display_name="San_Diego, CA",
            normalized_key="san diego ca",
        )
        session.add_all([alpha, beta, percent, remote, san_diego])
        session.flush()
        session.add_all(
            [
                Job(
                    id="job-alpha",
                    company_id=alpha.id,
                    location_id=remote.id,
                    title="AI Product Counsel",
                    normalized_title="ai product counsel",
                    description="Build healthcare contracts and legal tooling.",
                    category="Legal Technology",
                    deadline=date(2026, 7, 31),
                    latest_score=4.5,
                    status=JobStatus.SAVED,
                    discovered_at=datetime(2026, 7, 5, tzinfo=timezone.utc),
                ),
                Job(
                    id="job-beta",
                    company_id=beta.id,
                    title="Senior Policy Analyst",
                    normalized_title="senior policy analyst",
                    description="Research renewable energy markets.",
                    category="Public Policy",
                    deadline=None,
                    latest_score=3.0,
                    status=JobStatus.DISCOVERED,
                    discovered_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
                ),
                Job(
                    id="job-percent",
                    company_id=percent.id,
                    location_id=san_diego.id,
                    title="Compliance Counsel",
                    normalized_title="compliance counsel",
                    description="Fintech regulatory compliance.",
                    category="Financial Services",
                    deadline=date(2026, 7, 20),
                    latest_score=4.5,
                    status=JobStatus.STALE,
                    discovered_at=datetime(2026, 7, 5, tzinfo=timezone.utc),
                ),
                Job(
                    id="job-unscored",
                    company_id=beta.id,
                    location_id=san_diego.id,
                    title="Associate Researcher",
                    normalized_title="associate researcher",
                    description=None,
                    category=None,
                    deadline=date(2026, 8, 15),
                    latest_score=None,
                    status=JobStatus.READY,
                    discovered_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
                ),
            ]
        )


def _ids(database: Database, filters: JobListFilters) -> list[str]:
    with database.session() as session:
        return [row.job.id for row in query_jobs(session, filters)]


def test_filters_are_combinable_and_join_display_values(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    filters = JobListFilters(
        query="healthcare AI",
        industry_terms=("technology", "government"),
        company="legal",
        location="california",
        closing_from=date(2026, 7, 31),
        closing_to=date(2026, 7, 31),
        min_score=4.5,
        max_score=4.5,
        statuses=frozenset({JobStatus.SAVED}),
    )
    with database.session() as session:
        rows = query_jobs(session, filters)

    assert len(rows) == 1
    assert rows[0].job.id == "job-alpha"
    assert rows[0].company_name == "Alpha Legal AI"
    assert rows[0].location_name == "Remote — California"
    database.dispose()


def test_text_terms_can_match_different_supported_fields(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    # "AI" matches the title while "healthcare" matches the description.
    assert _ids(database, JobListFilters(query="AI healthcare")) == ["job-alpha"]
    # Industry alternatives search title/company/description/category as well.
    assert set(
        _ids(
            database,
            JobListFilters(industry_terms=("renewable", "financial services")),
        )
    ) == {"job-beta", "job-percent"}
    database.dispose()


def test_company_and_location_filters_treat_like_characters_literally(
    tmp_path,
) -> None:
    database = _database(tmp_path)
    _seed(database)

    assert _ids(database, JobListFilters(company="%")) == ["job-percent"]
    assert set(_ids(database, JobListFilters(location="_"))) == {
        "job-percent",
        "job-unscored",
    }
    assert _ids(database, JobListFilters(company="%' OR 1=1 --")) == []
    database.dispose()


def test_closing_bounds_are_inclusive_and_exclude_missing_deadlines(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    assert _ids(
        database,
        JobListFilters(
            closing_from=date(2026, 7, 20),
            closing_to=date(2026, 7, 31),
            sort=JobSort.CLOSING_DATE,
        ),
    ) == ["job-percent", "job-alpha"]
    assert "job-beta" in _ids(database, JobListFilters())
    database.dispose()


def test_score_bounds_are_inclusive_and_exclude_unscored_jobs(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    assert set(_ids(database, JobListFilters(min_score=4.5, max_score=4.5))) == {
        "job-alpha",
        "job-percent",
    }
    assert _ids(database, JobListFilters(max_score=3.0)) == ["job-beta"]
    database.dispose()


@pytest.mark.parametrize(
    ("sort", "expected"),
    [
        (
            JobSort.SCORE_HIGH,
            ["job-alpha", "job-percent", "job-beta", "job-unscored"],
        ),
        (
            JobSort.SCORE_LOW,
            ["job-beta", "job-alpha", "job-percent", "job-unscored"],
        ),
        (
            JobSort.NEWEST,
            ["job-unscored", "job-beta", "job-alpha", "job-percent"],
        ),
        (
            JobSort.CLOSING_DATE,
            ["job-percent", "job-alpha", "job-unscored", "job-beta"],
        ),
        (
            JobSort.COMPANY,
            ["job-alpha", "job-unscored", "job-beta", "job-percent"],
        ),
        (
            JobSort.TITLE,
            ["job-alpha", "job-unscored", "job-percent", "job-beta"],
        ),
    ],
)
def test_all_sort_modes_are_stable_and_keep_missing_values_last(
    tmp_path, sort: JobSort, expected: list[str]
) -> None:
    database = _database(tmp_path)
    _seed(database)

    assert _ids(database, JobListFilters(sort=sort)) == expected
    database.dispose()


def test_limit_offset_and_status_filter_are_applied_after_stable_sort(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    assert _ids(
        database,
        JobListFilters(
            statuses=frozenset({JobStatus.SAVED, JobStatus.STALE}),
            sort=JobSort.SCORE_HIGH,
            offset=1,
            limit=1,
        ),
    ) == ["job-percent"]
    database.dispose()


def test_page_has_total_and_contains_only_detached_immutable_values(tmp_path) -> None:
    database = _database(tmp_path)
    _seed(database)

    with database.session() as session:
        page = query_jobs_page(
            session,
            JobListFilters(sort=JobSort.TITLE, limit=2, offset=2),
        )
    database.dispose()

    assert page.total_count == 4
    assert page.offset == 2
    assert page.limit == 2
    assert page.has_previous is True
    assert page.has_next is False
    assert [item.id for item in page.items] == ["job-percent", "job-beta"]
    assert page.items[0].job is page.items[0]
    with pytest.raises(AttributeError):
        page.items[0].title = "mutated"  # type: ignore[misc]


def test_generation_guard_bypasses_candidates_after_concurrent_update(
    tmp_path, monkeypatch
) -> None:
    database = _database(tmp_path)
    _seed(database)

    def raced_candidates(session, **_kwargs):
        owning_database = session.info["jobby_database"]
        with owning_database.session() as writer:
            generation = int(
                writer.scalar(
                    select(SearchIndexState.generation).where(SearchIndexState.id == 1)
                )
                or 0
            )
            writer.get(Job, "job-beta").description = "newlymatching policy work"
        # This empty candidate set was valid immediately before the writer
        # committed. The SQL generation guard must now select via literal LIKE.
        return (), generation

    monkeypatch.setattr("jobby.job_queries._fts_candidates", raced_candidates)

    assert _ids(database, JobListFilters(query="newlymatching")) == ["job-beta"]
    database.dispose()


def test_ranked_component_and_compensation_views_sort_and_page_in_sql(
    tmp_path,
) -> None:
    database = _database(tmp_path)
    _seed(database)
    with database.session() as session:
        jobs = {job.id: job for job in session.scalars(select(Job))}
        jobs["job-alpha"].salary_max = 210_000
        jobs["job-beta"].salary_max = 180_000
        jobs["job-percent"].salary_max = None
        session.add_all(
            [
                Evaluation(
                    job_id="job-alpha",
                    score=4.5,
                    components={
                        "workload_stress": {"score": 2.0},
                        "strategic_optionality": {"score": 5.0},
                    },
                    is_current=True,
                ),
                Evaluation(
                    job_id="job-beta",
                    score=3.0,
                    components={
                        "workload_stress": {"score": 5.0},
                        "strategic_optionality": {"score": 1.0},
                    },
                    is_current=True,
                ),
            ]
        )

    statements: list[str] = []

    def capture(_conn, _cursor, statement, _params, _context, _many) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(database.engine, "before_cursor_execute", capture)
    try:
        with database.session() as session:
            compensation = query_ranked_page(
                session, view=RankedView.COMPENSATION, limit=1
            )
            stress = query_ranked_page(session, view=RankedView.STRESS, limit=2)
            strategic = query_ranked_page(session, view=RankedView.STRATEGIC, limit=2)
    finally:
        event.remove(database.engine, "before_cursor_execute", capture)

    assert [row.id for row in compensation.items] == ["job-alpha"]
    assert compensation.total_count == 4
    assert compensation.has_next is True
    assert [row.id for row in stress.items] == ["job-beta", "job-alpha"]
    assert [row.id for row in strategic.items] == ["job-alpha", "job-beta"]
    ranked_selects = [
        " ".join(sql.casefold().split())
        for sql in statements
        if "from jobs" in sql.casefold() and "companies" in sql.casefold()
    ]
    assert ranked_selects
    assert all(" limit ? offset ?" in sql for sql in ranked_selects)
    assert any("json_extract" in sql for sql in ranked_selects)
    database.dispose()


def test_canonical_groups_hide_members_and_search_the_group_as_one_result(
    tmp_path,
) -> None:
    database = _database(tmp_path)
    _seed(database)
    with database.session() as session:
        canonical = session.get(Job, "job-alpha")
        hidden = session.get(Job, "job-beta")
        assert canonical is not None and hidden is not None
        canonical.source_primary = "greenhouse:alpha"
        hidden.source_primary = "lever:beta"
        group = CanonicalJobGroup(
            id="group-alpha-beta",
            canonical_job_id=canonical.id,
        )
        session.add(group)
        session.flush()
        session.add_all(
            [
                CanonicalJobMember(
                    id="member-alpha",
                    group_id=group.id,
                    job_id=canonical.id,
                    is_canonical=True,
                    hidden_by_default=False,
                ),
                CanonicalJobMember(
                    id="member-beta",
                    group_id=group.id,
                    job_id=hidden.id,
                    is_canonical=False,
                    hidden_by_default=True,
                ),
            ]
        )

    with database.session() as session:
        all_jobs = query_jobs_page(session, JobListFilters(limit=10))
        grouped_search = query_jobs_page(
            session,
            JobListFilters(query="renewable", limit=10),
        )
        ranked = query_ranked_page(session, limit=10)

    assert all_jobs.total_count == 3
    assert "job-beta" not in {item.id for item in all_jobs.items}
    assert grouped_search.total_count == 1
    assert [item.id for item in grouped_search.items] == ["job-alpha"]
    assert set((grouped_search.items[0].source_primary or "").split(",")) == {
        "greenhouse:alpha",
        "lever:beta",
    }
    assert ranked.total_count == 3
    assert "job-beta" not in {item.id for item in ranked.items}
    database.dispose()


@pytest.mark.parametrize(
    "values",
    [
        {"query": "   "},
        {"company": "x" * 201},
        {"industry_terms": ("x" * 101,)},
        {"industry_terms": tuple(str(value) for value in range(21))},
        {"closing_from": date(2026, 8, 1), "closing_to": date(2026, 7, 1)},
        {"min_score": -0.1},
        {"max_score": 5.1},
        {"min_score": 4.0, "max_score": 3.9},
        {"statuses": frozenset()},
        {"limit": 0},
        {"limit": 1001},
    ],
)
def test_invalid_filters_are_rejected(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        JobListFilters.model_validate(values)
