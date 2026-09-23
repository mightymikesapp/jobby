"""Validated, deterministic queries for job-listing views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import ColumnElement, Float, case, cast, func, literal, or_, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute

from .enums import JobStatus
from .models import (
    CanonicalJobGroup,
    CanonicalJobMember,
    Company,
    Evaluation,
    Job,
    Location,
    SearchIndexState,
)


MAX_QUERY_LENGTH = 300
MAX_SUBSTRING_LENGTH = 200
MAX_INDUSTRY_TERMS = 20

IndustryTerm = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]


class JobSort(StrEnum):
    """Supported stable ordering modes for a job listing."""

    SCORE_HIGH = "score_high"
    SCORE_LOW = "score_low"
    NEWEST = "newest"
    CLOSING_DATE = "closing_date"
    COMPANY = "company"
    TITLE = "title"


class RankedView(StrEnum):
    """SQL-backed orderings offered by the Ranked screen."""

    SCORE = "score"
    NEWEST = "newest"
    DEADLINE = "deadline"
    COMPENSATION = "comp"
    STRESS = "stress"
    STRATEGIC = "strategic"


class JobListFilters(BaseModel):
    """Validated filters for :func:`query_jobs`.

    ``query`` is split on whitespace. Every resulting term must occur in at
    least one of title, company, description, or category. Multiple
    ``industry_terms`` are alternatives and search the same four fields. All
    other populated fields are combined with those predicates using AND.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    query: str | None = Field(default=None, max_length=MAX_QUERY_LENGTH)
    industry_terms: tuple[IndustryTerm, ...] = Field(
        default_factory=tuple,
        max_length=MAX_INDUSTRY_TERMS,
    )
    company: str | None = Field(default=None, max_length=MAX_SUBSTRING_LENGTH)
    location: str | None = Field(default=None, max_length=MAX_SUBSTRING_LENGTH)
    category: str | None = Field(default=None, max_length=MAX_SUBSTRING_LENGTH)
    source: str | None = Field(default=None, max_length=MAX_SUBSTRING_LENGTH)
    closing_from: date | None = None
    closing_to: date | None = None
    discovered_after: datetime | None = None
    discovered_after_job_id: str | None = Field(default=None, max_length=36)
    min_score: float | None = Field(default=None, ge=0, le=5, allow_inf_nan=False)
    max_score: float | None = Field(default=None, ge=0, le=5, allow_inf_nan=False)
    statuses: frozenset[JobStatus] | None = None
    sort: JobSort = JobSort.SCORE_HIGH
    limit: int = Field(default=500, ge=1, le=1000)
    offset: int = Field(default=0, ge=0, le=1_000_000)
    # Internal keyset position. The public facade carries this in a signed-
    # by-hash opaque cursor and never exposes this mapping as an input field.
    after: dict[str, Any] | None = None

    @field_validator("query", "company", "location")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("text filters must not be blank")
        return normalized

    @field_validator("statuses")
    @classmethod
    def reject_empty_status_set(
        cls, value: frozenset[JobStatus] | None
    ) -> frozenset[JobStatus] | None:
        if value is not None and not value:
            raise ValueError("statuses must contain at least one status")
        return value

    @model_validator(mode="after")
    def validate_ranges(self) -> JobListFilters:
        if (
            self.closing_from is not None
            and self.closing_to is not None
            and self.closing_from > self.closing_to
        ):
            raise ValueError("closing_from must be on or before closing_to")
        if (
            self.min_score is not None
            and self.max_score is not None
            and self.min_score > self.max_score
        ):
            raise ValueError("min_score must be less than or equal to max_score")
        if self.discovered_after_job_id is not None and self.discovered_after is None:
            raise ValueError(
                "discovered_after_job_id requires a discovered_after timestamp"
            )
        return self


@dataclass(frozen=True, slots=True)
class JobListItem:
    """Immutable detached values safe for CLI, MCP, and service callers.

    The ``job`` property is a compatibility bridge for callers of the original
    ``JobListRow`` API; it returns this detached DTO, never an ORM instance.
    """

    id: str
    title: str
    company_name: str
    location_name: str | None
    description: str | None
    category: str | None
    canonical_url: str | None
    source_primary: str | None
    status: JobStatus
    latest_score: float | None
    deadline: date | None
    discovered_at: datetime
    last_seen_at: datetime | None
    salary_min: int | None
    salary_max: int | None

    @property
    def job(self) -> JobListItem:
        """Return the detached display object for legacy ``row.job`` users."""

        return self


# Compatibility for external callers that imported the earlier name.
JobListRow = JobListItem


@dataclass(frozen=True, slots=True)
class JobListPage:
    """One stable page of detached display rows plus the full match count."""

    items: tuple[JobListItem, ...]
    total_count: int
    offset: int
    limit: int

    @property
    def has_previous(self) -> bool:
        return self.offset > 0

    @property
    def has_next(self) -> bool:
        return self.offset + len(self.items) < self.total_count


@dataclass(frozen=True, slots=True)
class RankedListItem:
    """Detached values for one ranked SQL result."""

    id: str
    score: float | None
    view_score: float | None
    company: str
    title: str
    deadline: date | None
    salary_min: int | None
    salary_max: int | None
    status: JobStatus
    discovered_at: datetime


@dataclass(frozen=True, slots=True)
class RankedPage:
    items: tuple[RankedListItem, ...]
    total_count: int
    offset: int
    limit: int
    view: RankedView

    @property
    def has_previous(self) -> bool:
        return self.offset > 0

    @property
    def has_next(self) -> bool:
        return self.offset + len(self.items) < self.total_count


def _escape_like(value: str) -> str:
    """Escape SQL LIKE metacharacters so filters always mean substrings."""

    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _substring(
    column: InstrumentedAttribute[str]
    | InstrumentedAttribute[str | None]
    | ColumnElement[str]
    | ColumnElement[str | None],
    value: str,
) -> ColumnElement[bool]:
    pattern = f"%{_escape_like(value.lower())}%"
    return func.lower(func.coalesce(column, "")).like(pattern, escape="\\")


def _search_term_for(job, company, term: str) -> ColumnElement[bool]:
    return or_(
        _substring(job.title, term),
        _substring(company.name, term),
        _substring(job.description, term),
        _substring(job.category, term),
    )


def _search_term(term: str) -> ColumnElement[bool]:
    """Match the visible job or any member of its canonical group."""

    canonical_membership = aliased(CanonicalJobMember)
    group_membership = aliased(CanonicalJobMember)
    member_job = aliased(Job)
    member_company = aliased(Company)
    grouped_match = (
        select(group_membership.id)
        .select_from(canonical_membership)
        .join(
            group_membership,
            group_membership.group_id == canonical_membership.group_id,
        )
        .join(member_job, member_job.id == group_membership.job_id)
        .join(member_company, member_company.id == member_job.company_id)
        .where(
            canonical_membership.job_id == Job.id,
            _search_term_for(member_job, member_company, term),
        )
        .correlate(Job)
        .exists()
    )
    return or_(_search_term_for(Job, Company, term), grouped_match)


def _visible_job() -> ColumnElement[bool]:
    hidden_membership = (
        select(CanonicalJobMember.id)
        .where(
            CanonicalJobMember.job_id == Job.id,
            CanonicalJobMember.hidden_by_default.is_(True),
        )
        .correlate(Job)
        .exists()
    )
    return ~hidden_membership


def _group_sources():
    canonical_membership = aliased(CanonicalJobMember)
    group_membership = aliased(CanonicalJobMember)
    member_job = aliased(Job)
    return (
        select(func.group_concat(func.distinct(member_job.source_primary)))
        .select_from(canonical_membership)
        .join(
            group_membership,
            group_membership.group_id == canonical_membership.group_id,
        )
        .join(member_job, member_job.id == group_membership.job_id)
        .where(canonical_membership.job_id == Job.id)
        .correlate(Job)
        .scalar_subquery()
    )


def _ordering(sort: JobSort) -> tuple[ColumnElement[Any], ...]:
    score_missing = Job.latest_score.is_(None).asc()
    deadline_missing = Job.deadline.is_(None).asc()
    if sort is JobSort.SCORE_HIGH:
        return (
            score_missing,
            Job.latest_score.desc(),
            Job.discovered_at.desc(),
            Job.id.asc(),
        )
    if sort is JobSort.SCORE_LOW:
        return (
            score_missing,
            Job.latest_score.asc(),
            Job.discovered_at.desc(),
            Job.id.asc(),
        )
    if sort is JobSort.NEWEST:
        return (
            Job.discovered_at.desc(),
            score_missing,
            Job.latest_score.desc(),
            Job.id.asc(),
        )
    if sort is JobSort.CLOSING_DATE:
        return (
            deadline_missing,
            Job.deadline.asc(),
            score_missing,
            Job.latest_score.desc(),
            Job.id.asc(),
        )
    if sort is JobSort.COMPANY:
        return (
            func.lower(Company.name).asc(),
            func.lower(Job.title).asc(),
            Job.id.asc(),
        )
    return (
        func.lower(Job.title).asc(),
        func.lower(Company.name).asc(),
        Job.id.asc(),
    )


def query_jobs(
    session: Session,
    filters: JobListFilters | None = None,
) -> list[JobListItem]:
    """Return jobs matching every supplied filter in a stable order.

    Company is required by the schema; location is outer-joined so jobs with
    no location remain visible unless a location filter is supplied. Scores
    and closing dates are inclusive, and their missing values are excluded
    only when the corresponding bounds are active.
    """

    filters = filters or JobListFilters()
    statement = _filtered_statement(filters, session=session).add_columns(
        Job.id,
        Job.title,
        Company.name,
        Location.display_name,
        Job.description,
        Job.category,
        # The DTO retains its legacy field name, but launch actions must use
        # the exact validated URL rather than the lossy comparison identity.
        func.coalesce(Job.launch_url, Job.canonical_url),
        func.coalesce(_group_sources(), Job.source_primary),
        Job.status,
        Job.latest_score,
        Job.deadline,
        Job.discovered_at,
        Job.last_seen_at,
        Job.salary_min,
        Job.salary_max,
    )

    records = session.execute(
        statement.order_by(*_ordering(filters.sort))
        .offset(filters.offset)
        .limit(filters.limit)
    ).all()
    return [JobListItem(*record) for record in records]


def query_jobs_page(
    session: Session,
    filters: JobListFilters | None = None,
) -> JobListPage:
    """Return a detached page and an uncapped count using the same filters."""

    filters = filters or JobListFilters()
    count_filters = filters.model_copy(update={"after": None, "offset": 0})
    count_statement = _filtered_statement(count_filters, session=session).add_columns(
        func.count(Job.id)
    )
    total_count = int(session.scalar(count_statement) or 0)
    items = tuple(query_jobs(session, filters))
    return JobListPage(
        items=items,
        total_count=total_count,
        offset=filters.offset,
        limit=filters.limit,
    )


def query_ranked_page(
    session: Session,
    *,
    view: RankedView | str = RankedView.SCORE,
    statuses: frozenset[JobStatus] | None = None,
    limit: int = 500,
    offset: int = 0,
) -> RankedPage:
    """Return one stable, SQL-sorted Ranked page and its uncapped count.

    Compensation and evaluation-component views deliberately calculate their
    sort values inside SQLite.  This prevents headless clients from loading the full
    jobs table merely to sort and then discard every row after the first page.
    """

    view = RankedView(view)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
        raise ValueError("ranked page limit must be between 1 and 1000")
    if (
        isinstance(offset, bool)
        or not isinstance(offset, int)
        or not 0 <= offset <= 1_000_000
    ):
        raise ValueError("ranked page offset must be between 0 and 1000000")
    if statuses is not None and not statuses:
        raise ValueError("ranked statuses must not be empty")

    component_name = {
        RankedView.STRESS: "workload_stress",
        RankedView.STRATEGIC: "strategic_optionality",
    }.get(view)
    if component_name is None:
        component_score = cast(literal(None), Float)
    else:
        path = f'$."{component_name}".score'
        component_type = func.json_type(Evaluation.components, path)
        component_score = case(
            (
                component_type.in_(("integer", "real")),
                cast(func.json_extract(Evaluation.components, path), Float),
            ),
            else_=None,
        )

    base = (
        select()
        .select_from(Job)
        .join(Company, Company.id == Job.company_id)
        .outerjoin(
            Evaluation,
            (Evaluation.job_id == Job.id) & Evaluation.is_current.is_(True),
        )
        .where(_visible_job())
    )
    count_statement = select(func.count(Job.id)).select_from(Job).where(_visible_job())
    if statuses is not None:
        base = base.where(Job.status.in_(statuses))
        count_statement = count_statement.where(Job.status.in_(statuses))

    score_missing = Job.latest_score.is_(None).asc()
    deadline_missing = Job.deadline.is_(None).asc()
    compensation_missing = Job.salary_max.is_(None).asc()
    component_missing = component_score.is_(None).asc()
    ordering: tuple[ColumnElement[Any], ...]
    if view is RankedView.NEWEST:
        ordering = (
            Job.discovered_at.desc(),
            score_missing,
            Job.latest_score.desc(),
            Job.id.asc(),
        )
    elif view is RankedView.DEADLINE:
        ordering = (
            deadline_missing,
            Job.deadline.asc(),
            score_missing,
            Job.latest_score.desc(),
            Job.id.asc(),
        )
    elif view is RankedView.COMPENSATION:
        ordering = (compensation_missing, Job.salary_max.desc(), Job.id.desc())
    elif view in {RankedView.STRESS, RankedView.STRATEGIC}:
        ordering = (
            component_missing,
            component_score.desc(),
            score_missing,
            Job.latest_score.desc(),
            Job.id.desc(),
        )
    else:
        ordering = (
            score_missing,
            Job.latest_score.desc(),
            Job.discovered_at.desc(),
            Job.id.asc(),
        )

    statement = base.add_columns(
        Job.id,
        Job.latest_score,
        component_score.label("view_score"),
        Company.name,
        Job.title,
        Job.deadline,
        Job.salary_min,
        Job.salary_max,
        Job.status,
        Job.discovered_at,
    )
    records = session.execute(statement.order_by(*ordering).offset(offset).limit(limit))
    items = tuple(RankedListItem(*record) for record in records)
    return RankedPage(
        items=items,
        total_count=int(session.scalar(count_statement) or 0),
        offset=offset,
        limit=limit,
        view=view,
    )


def _filtered_statement(filters: JobListFilters, *, session: Session | None = None):
    """Build joined filtering state shared by the count and item queries."""

    statement = (
        select()
        .select_from(Job)
        .join(Company, Company.id == Job.company_id)
        .outerjoin(Location, Location.id == Job.location_id)
        .where(_visible_job())
    )

    query_terms = tuple(filters.query.split()) if filters.query else ()
    candidates = _fts_candidates(
        session,
        query_terms=query_terms,
        industry_terms=filters.industry_terms,
    )
    if candidates is not None:
        job_ids, candidate_generation = candidates
        live_generation = (
            select(SearchIndexState.generation)
            .where(SearchIndexState.id == 1)
            .scalar_subquery()
        )
        statement = statement.where(
            or_(
                Job.id.in_(job_ids),
                func.coalesce(live_generation, -1) != candidate_generation,
            )
        )
    # FTS is only a prefilter. Literal predicates remain authoritative so an
    # optional cache cannot change Unicode/case behavior or return stale rows.
    # The generation guard above bypasses a stale candidate set when a writer
    # commits between candidate preparation and statement execution.
    for term in query_terms:
        statement = statement.where(_search_term(term))
    if filters.industry_terms:
        statement = statement.where(
            or_(*(_search_term(term) for term in filters.industry_terms))
        )
    if filters.company is not None:
        statement = statement.where(_substring(Company.name, filters.company))
    if filters.location is not None:
        statement = statement.where(_substring(Location.display_name, filters.location))
    if filters.category is not None:
        statement = statement.where(_substring(Job.category, filters.category))
    if filters.source is not None:
        statement = statement.where(_substring(Job.source_primary, filters.source))
    if filters.closing_from is not None:
        statement = statement.where(Job.deadline >= filters.closing_from)
    if filters.closing_to is not None:
        statement = statement.where(Job.deadline <= filters.closing_to)
    if filters.discovered_after is not None:
        if filters.discovered_after_job_id is None:
            statement = statement.where(Job.discovered_at > filters.discovered_after)
        else:
            statement = statement.where(
                or_(
                    Job.discovered_at > filters.discovered_after,
                    (
                        (Job.discovered_at == filters.discovered_after)
                        & (Job.id > filters.discovered_after_job_id)
                    ),
                )
            )
    if filters.min_score is not None:
        statement = statement.where(Job.latest_score >= filters.min_score)
    if filters.max_score is not None:
        statement = statement.where(Job.latest_score <= filters.max_score)
    if filters.statuses is not None:
        statement = statement.where(Job.status.in_(filters.statuses))

    if filters.after is not None:
        statement = statement.where(_after_predicate(filters))

    return statement


def _after_predicate(filters: JobListFilters) -> ColumnElement[bool]:
    """Return the strict keyset successor for the selected stable ordering."""

    after = filters.after or {}
    job_id = str(after.get("id") or "")
    if not job_id:
        raise ValueError("job cursor is missing its stable id")
    discovered = after.get("discovered_at")
    if discovered is not None:
        discovered = datetime.fromisoformat(str(discovered))
    deadline = after.get("deadline")
    if deadline is not None:
        deadline = date.fromisoformat(str(deadline))
    score = after.get("score")
    title = str(after.get("title") or "").casefold()
    company = str(after.get("company") or "").casefold()

    if filters.sort in {JobSort.SCORE_HIGH, JobSort.SCORE_LOW}:
        missing = score is None
        score_order = (
            Job.latest_score < score
            if filters.sort is JobSort.SCORE_HIGH
            else Job.latest_score > score
        )
        same_score = (
            Job.latest_score.is_(None) if missing else Job.latest_score == score
        )
        same_position = (Job.discovered_at < discovered) | (
            (Job.discovered_at == discovered) & (Job.id > job_id)
        )
        if missing:
            return Job.latest_score.is_(None) & same_position
        return or_(
            Job.latest_score.is_(None),
            score_order,
            (Job.latest_score == score) & same_position,
        )

    if filters.sort is JobSort.NEWEST:
        score_missing = Job.latest_score.is_(None)
        if score is None:
            same_secondary = score_missing & (Job.id > job_id)
        else:
            same_secondary = or_(
                (Job.latest_score < score),
                (Job.latest_score == score) & (Job.id > job_id),
                score_missing,
            )
        return or_(
            Job.discovered_at < discovered,
            (Job.discovered_at == discovered) & same_secondary,
        )

    if filters.sort is JobSort.CLOSING_DATE:
        if deadline is None:
            if score is None:
                same_score = Job.latest_score.is_(None) & (Job.id > job_id)
            else:
                same_score = or_(
                    Job.latest_score.is_(None),
                    (Job.latest_score < score),
                    (Job.latest_score == score) & (Job.id > job_id),
                )
            return Job.deadline.is_(None) & same_score
        if score is None:
            same_score = Job.latest_score.is_(None) & (Job.id > job_id)
        else:
            same_score = or_(
                Job.latest_score.is_(None),
                (Job.latest_score < score),
                (Job.latest_score == score) & (Job.id > job_id),
            )
        same_deadline = or_(
            Job.deadline.is_(None), (Job.deadline == deadline) & same_score
        )
        return or_(Job.deadline > deadline, same_deadline)

    if filters.sort is JobSort.COMPANY:
        return or_(
            func.lower(Company.name) > company,
            (func.lower(Company.name) == company)
            & or_(
                func.lower(Job.title) > title,
                (func.lower(Job.title) == title) & (Job.id > job_id),
            ),
        )
    return or_(
        func.lower(Job.title) > title,
        (func.lower(Job.title) == title)
        & or_(
            func.lower(Company.name) > company,
            (func.lower(Company.name) == company) & (Job.id > job_id),
        ),
    )


def _fts_candidates(
    session: Session | None,
    *,
    query_terms: tuple[str, ...],
    industry_terms: tuple[str, ...],
) -> tuple[tuple[str, ...], int] | None:
    """Use the owning Database's derived cache when one is available."""

    if session is None or not (query_terms or industry_terms):
        return None
    database = session.info.get("jobby_database")
    if database is None:
        return None
    try:
        from .search_index import SearchIndex

        snapshot = SearchIndex(database).candidate_snapshot(
            session,
            query_terms=query_terms,
            industry_terms=industry_terms,
        )
        if snapshot is None:
            return None
        candidate_ids = list(snapshot.job_ids)
        if candidate_ids:
            canonical_ids = session.scalars(
                select(CanonicalJobGroup.canonical_job_id)
                .join(
                    CanonicalJobMember,
                    CanonicalJobMember.group_id == CanonicalJobGroup.id,
                )
                .where(CanonicalJobMember.job_id.in_(candidate_ids))
            )
            candidate_ids.extend(canonical_ids)
        return tuple(dict.fromkeys(candidate_ids)), snapshot.generation
    except Exception:
        # A derived cache must never make the operational listing unavailable.
        return None


__all__ = [
    "JobListFilters",
    "JobListItem",
    "JobListPage",
    "JobListRow",
    "JobSort",
    "RankedListItem",
    "RankedPage",
    "RankedView",
    "query_jobs",
    "query_jobs_page",
    "query_ranked_page",
]
