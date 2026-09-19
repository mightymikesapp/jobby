"""Trustworthy, snapshot-based application analytics.

The small legacy helpers remain available for integrations that consume their
original shapes.  :func:`analytics_report` is the release 0.5 contract used by
the headless interfaces: it declares sample sufficiency, uses application-time provenance, and
keeps right-censored stage intervals separate from completed durations.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from statistics import mean, median
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .enums import ApprovalState, ApplicationStage, JobStatus
from .models import (
    Application,
    CanonicalJobGroup,
    CanonicalJobMember,
    DocumentVersion,
    Job,
    ProfileFact,
    StageEvent,
)


MIN_ANALYTICS_SAMPLE = 10
MIN_REQUIREMENT_SAMPLE = 5
REQUIREMENT_SCORE_THRESHOLD = 4.0


@dataclass(frozen=True, slots=True)
class RequirementTerm:
    canonical: str
    aliases: tuple[str, ...]


REQUIREMENT_TERM_LEXICON: tuple[RequirementTerm, ...] = (
    RequirementTerm(
        "AI governance",
        ("AI governance", "artificial intelligence governance", "model governance"),
    ),
    RequirementTerm("AI policy", ("AI policy", "artificial intelligence policy")),
    RequirementTerm("AI safety", ("AI safety", "artificial intelligence safety")),
    RequirementTerm("algorithmic accountability", ("algorithmic accountability",)),
    RequirementTerm(
        "responsible AI", ("responsible AI", "responsible artificial intelligence")
    ),
    RequirementTerm("generative AI", ("generative AI", "gen AI", "GenAI")),
    RequirementTerm(
        "large language models",
        ("large language model", "large language models", "LLM", "LLMs"),
    ),
    RequirementTerm("machine learning", ("machine learning", "ML")),
    RequirementTerm(
        "intellectual property", ("intellectual property", "IP law", "IP rights")
    ),
    RequirementTerm("patent prosecution", ("patent prosecution",)),
    RequirementTerm("prior art", ("prior art",)),
    RequirementTerm("patents", ("patent", "patents")),
    RequirementTerm("copyright", ("copyright", "copyrights")),
    RequirementTerm("trademark", ("trademark", "trademarks")),
    RequirementTerm(
        "licensing", ("license agreement", "license agreements", "licensing")
    ),
    RequirementTerm("DMCA", ("DMCA", "Digital Millennium Copyright Act")),
    RequirementTerm(
        "technology transactions",
        ("technology transaction", "technology transactions", "tech transactions"),
    ),
    RequirementTerm("open source", ("open source", "open-source")),
    RequirementTerm("data privacy", ("data privacy", "privacy law", "privacy laws")),
    RequirementTerm("GDPR", ("GDPR", "General Data Protection Regulation")),
    RequirementTerm(
        "CCPA/CPRA",
        (
            "CCPA",
            "CPRA",
            "California Consumer Privacy Act",
            "California Privacy Rights Act",
        ),
    ),
    RequirementTerm(
        "regulatory compliance", ("regulatory compliance", "legal compliance")
    ),
    RequirementTerm("FTC", ("FTC", "Federal Trade Commission")),
    RequirementTerm("Section 230", ("Section 230", "47 U.S.C. 230", "47 USC 230")),
    RequirementTerm("legal operations", ("legal operations", "legal ops")),
    RequirementTerm(
        "contract management",
        ("contract management", "contract lifecycle management", "CLM"),
    ),
    RequirementTerm("product counsel", ("product counsel", "product counseling")),
    RequirementTerm("litigation", ("litigation", "litigate")),
    RequirementTerm(
        "export controls", ("export control", "export controls", "ITAR", "EAR")
    ),
    RequirementTerm(
        "content moderation",
        ("content moderation", "trust and safety", "trust & safety"),
    ),
)
_RESPONSE_STAGES = frozenset(
    {
        ApplicationStage.SCREENING,
        ApplicationStage.INTERVIEW,
        ApplicationStage.ASSESSMENT,
        ApplicationStage.OFFER,
    }
)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _progression_history(session: Session) -> tuple[set[str], set[str]]:
    """Return applications that ever reached a response or offer stage."""

    responded = set(
        session.scalars(
            select(StageEvent.application_id)
            .where(StageEvent.to_stage.in_(_RESPONSE_STAGES))
            .distinct()
        )
    )
    offered = set(
        session.scalars(
            select(StageEvent.application_id)
            .where(StageEvent.to_stage == ApplicationStage.OFFER)
            .distinct()
        )
    )
    return responded, offered


def funnel_summary(session: Session) -> dict[str, Any]:
    counts = Counter()
    for stage in ApplicationStage:
        counts[stage.value] = int(
            session.scalar(
                select(func.count())
                .select_from(Application)
                .where(Application.current_stage == stage)
            )
            or 0
        )
    total = int(session.scalar(select(func.count()).select_from(Application)) or 0)
    historical_responses, historical_offers = _progression_history(session)
    current_responses = set(
        session.scalars(
            select(Application.id).where(
                Application.current_stage.in_(_RESPONSE_STAGES)
            )
        )
    )
    current_offers = set(
        session.scalars(
            select(Application.id).where(
                Application.current_stage == ApplicationStage.OFFER
            )
        )
    )
    advanced = len(historical_responses | current_responses)
    offers = len(historical_offers | current_offers)
    return {
        "total": total,
        "by_stage": {stage.value: counts[stage.value] for stage in ApplicationStage},
        "response_rate": round(advanced / total, 4) if total else 0.0,
        "offer_rate": round(offers / total, 4) if total else 0.0,
    }


def _application_sources(applied_sources: object) -> tuple[str, ...]:
    """Return distinct source names from the frozen application snapshot.

    An empty tuple deliberately means that no application-time source
    provenance exists.  Analytics callers render that gap as ``unknown``;
    they must never substitute the job's mutable current source.
    """

    names: dict[str, str] = {}
    if isinstance(applied_sources, list):
        for entry in applied_sources:
            if not isinstance(entry, dict):
                continue
            value = entry.get("source")
            if isinstance(value, str) and value.strip():
                cleaned = value.strip()
                names.setdefault(cleaned.casefold(), cleaned)
    return tuple(names[key] for key in sorted(names))


def source_yield(session: Session) -> list[dict[str, Any]]:
    """Attribute each application to every source frozen when it was applied.

    A canonical group can therefore contribute one application to several
    sources without moving or deleting any of its member jobs.  Sources are
    de-duplicated within an application snapshot so repeated observations do
    not inflate yield.
    """

    counts: dict[str, Counter[str]] = defaultdict(Counter)
    labels: dict[str, str] = {}
    historical_responses, historical_offers = _progression_history(session)
    rows = session.execute(
        select(
            Application.id,
            Application.current_stage,
            Application.applied_sources,
        ).order_by(Application.id)
    )
    for application_id, stage, applied_sources in rows:
        sources = _application_sources(applied_sources) or ("unknown",)
        for source in sources:
            source_key = source.casefold()
            labels.setdefault(source_key, source)
            counts[source_key]["applications"] += 1
            if application_id in historical_responses or stage in _RESPONSE_STAGES:
                counts[source_key]["responses"] += 1
            if application_id in historical_offers or stage == ApplicationStage.OFFER:
                counts[source_key]["offers"] += 1
    return [
        {
            "source": labels[source_key],
            **values,
            "response_rate": round(values["responses"] / values["applications"], 4),
        }
        for source_key, values in sorted(counts.items())
    ]


def time_in_stage(session: Session) -> dict[str, float]:
    """Return the legacy mean elapsed duration, including open intervals.

    New presentation code uses :func:`stage_duration_summary`, which labels
    open intervals as censored and never treats them as completed observations.
    This helper retains its v0.1 shape for scripts and exports.
    """

    durations: dict[str, list[float]] = defaultdict(list)
    now = datetime.now(timezone.utc)
    grouped: dict[str, list[StageEvent]] = defaultdict(list)
    for event in session.scalars(
        select(StageEvent).order_by(
            StageEvent.application_id, StageEvent.occurred_at, StageEvent.id
        )
    ):
        grouped[event.application_id].append(event)
    for events in grouped.values():
        for index, event in enumerate(events):
            end = events[index + 1].occurred_at if index + 1 < len(events) else now
            durations[event.to_stage.value].append(
                max((_as_utc(end) - _as_utc(event.occurred_at)).total_seconds(), 0)
                / 86400
            )
    return {
        stage: round(mean(values), 2) for stage, values in durations.items() if values
    }


def _duration_days(start: datetime, end: datetime) -> float:
    return max((_as_utc(end) - _as_utc(start)).total_seconds(), 0.0) / 86400


def _percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return round(ordered[0], 2)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 2)


def stage_duration_summary(
    session: Session, *, now: datetime | None = None
) -> list[dict[str, Any]]:
    """Summarize completed stage intervals and label open ones as censored.

    Medians and percentiles are calculated only from completed intervals.
    ``longest_open_days`` describes the right-censored observations without
    pretending that their eventual duration is already known.
    """

    observed_at = _as_utc(now or datetime.now(timezone.utc))
    completed: dict[ApplicationStage, list[float]] = defaultdict(list)
    censored: dict[ApplicationStage, list[float]] = defaultdict(list)
    rows = session.execute(
        select(
            Application.id,
            Application.current_stage,
            Application.created_at,
            StageEvent.id,
            StageEvent.from_stage,
            StageEvent.to_stage,
            StageEvent.occurred_at,
        )
        .outerjoin(StageEvent, StageEvent.application_id == Application.id)
        .order_by(Application.id, StageEvent.occurred_at, StageEvent.id)
    )

    current_application: str | None = None
    current_stage: ApplicationStage | None = None
    created_at: datetime | None = None
    events: list[tuple[ApplicationStage | None, ApplicationStage, datetime]] = []

    def finish_application() -> None:
        if current_application is None or current_stage is None or created_at is None:
            return
        if not events:
            censored[current_stage].append(_duration_days(created_at, observed_at))
            return
        first_from, _first_to, first_at = events[0]
        if first_from is not None:
            completed[first_from].append(_duration_days(created_at, first_at))
        for index, (_from_stage, to_stage, started_at) in enumerate(events):
            if index + 1 < len(events):
                completed[to_stage].append(
                    _duration_days(started_at, events[index + 1][2])
                )
            else:
                # The final stage has no known exit time.  Treat it as a
                # right-censored duration even for terminal workflow stages.
                censored[to_stage].append(_duration_days(started_at, observed_at))

    for row in rows:
        application_id = row[0]
        if application_id != current_application:
            finish_application()
            current_application = application_id
            current_stage = row[1]
            created_at = row[2]
            events = []
        if row[3] is not None:
            events.append((row[4], row[5], row[6]))
    finish_application()

    results: list[dict[str, Any]] = []
    for stage in ApplicationStage:
        closed_values = completed.get(stage, [])
        open_values = censored.get(stage, [])
        if not closed_values and not open_values:
            continue
        results.append(
            {
                "stage": stage.value,
                "observations": len(closed_values) + len(open_values),
                "completed": len(closed_values),
                "censored": len(open_values),
                "median_days": (
                    round(median(closed_values), 2) if closed_values else None
                ),
                "p25_days": _percentile(closed_values, 0.25),
                "p75_days": _percentile(closed_values, 0.75),
                "p90_days": _percentile(closed_values, 0.90),
                "longest_open_days": (
                    round(max(open_values), 2) if open_values else None
                ),
            }
        )
    return results


def _score_rows(
    session: Session,
) -> list[tuple[float, str, ApplicationStage]]:
    """Return only application-time score snapshots and their outcomes."""

    rows = session.execute(
        select(
            Application.applied_score,
            Application.id,
            Application.current_stage,
        )
        .where(Application.applied_score.is_not(None))
        .order_by(Application.id)
    )
    return [
        (float(applied_score), application_id, stage)
        for applied_score, application_id, stage in rows
    ]


def score_calibration(session: Session) -> list[dict[str, Any]]:
    historical_responses, _ = _progression_history(session)
    buckets: dict[int, list[int]] = defaultdict(list)
    for score, application_id, stage in _score_rows(session):
        bucket = min(max(int(score), 1), 5)
        progressed = int(
            application_id in historical_responses or stage in _RESPONSE_STAGES
        )
        buckets[bucket].append(progressed)
    return [
        {
            "score_bucket": bucket,
            "applications": len(values),
            "progression_rate": round(mean(values), 4),
        }
        for bucket, values in sorted(buckets.items())
    ]


def rejection_patterns(session: Session) -> list[dict[str, Any]]:
    reasons = Counter(
        reason.strip().lower()
        for reason in session.scalars(
            select(Application.rejection_reason).where(
                Application.current_stage == ApplicationStage.REJECTED,
                Application.rejection_reason.is_not(None),
            )
        )
        if reason and reason.strip()
    )
    return [
        {"reason": reason, "count": count} for reason, count in reasons.most_common()
    ]


def _term_pattern(term: RequirementTerm) -> re.Pattern[str]:
    alternatives = []
    for alias in sorted(term.aliases, key=lambda item: (-len(item), item.casefold())):
        escaped = re.escape(alias).replace(r"\ ", r"[\s\-]+")
        alternatives.append(escaped)
    return re.compile(rf"(?<![\w])(?:{'|'.join(alternatives)})(?![\w])", re.IGNORECASE)


_REQUIREMENT_PATTERNS = tuple(
    (term, _term_pattern(term)) for term in REQUIREMENT_TERM_LEXICON
)


def _canonical_requirement_jobs(session: Session) -> list[Job]:
    """Return one qualifying canonical record per deduplicated job group."""

    eligible = list(
        session.scalars(
            select(Job)
            .where(
                Job.status != JobStatus.CLOSED,
                Job.latest_score >= REQUIREMENT_SCORE_THRESHOLD,
            )
            .order_by(Job.id)
        )
    )
    if not eligible:
        return []
    eligible_by_id = {job.id: job for job in eligible}
    memberships = list(
        session.execute(
            select(
                CanonicalJobMember.job_id,
                CanonicalJobMember.group_id,
                CanonicalJobGroup.canonical_job_id,
            ).join(
                CanonicalJobGroup,
                CanonicalJobGroup.id == CanonicalJobMember.group_id,
            )
        )
    )
    member_ids = {job_id for job_id, _group_id, _canonical_id in memberships}
    canonical_ids = {
        canonical_id
        for _job_id, _group_id, canonical_id in memberships
        if canonical_id in eligible_by_id
    }
    standalone_ids = set(eligible_by_id) - member_ids
    selected_ids = standalone_ids | canonical_ids
    return [eligible_by_id[job_id] for job_id in sorted(selected_ids)]


def requirement_analytics(session: Session) -> dict[str, Any]:
    """Analyze boundary-safe requirements across active shortlisted jobs."""

    jobs = _canonical_requirement_jobs(session)
    qualifying_jobs = len(jobs)
    evidence_parts: list[str] = []
    for key, value in session.execute(
        select(ProfileFact.fact_key, ProfileFact.value_json)
        .where(ProfileFact.approved.is_(True))
        .order_by(ProfileFact.fact_key)
    ):
        evidence_parts.append(f"{key} {value}")
    evidence_parts.extend(
        content
        for content in session.scalars(
            select(DocumentVersion.content_markdown)
            .where(
                DocumentVersion.approval_state == ApprovalState.APPROVED,
                DocumentVersion.is_canonical.is_(True),
            )
            .order_by(DocumentVersion.id)
        )
        if content
    )
    evidence_text = "\n".join(evidence_parts)
    rows: list[dict[str, Any]] = []
    for term, pattern in _REQUIREMENT_PATTERNS:
        job_count = 0
        for job in jobs:
            text = f"{job.title}\n{job.description or ''}"
            if pattern.search(text):
                job_count += 1
        if not job_count:
            continue
        evidenced = bool(pattern.search(evidence_text))
        rows.append(
            {
                "term": term.canonical,
                "aliases": list(term.aliases),
                "job_count": job_count,
                # One occurrence means one canonical job group containing the
                # term, regardless of repeated mentions or duplicate aliases.
                "occurrence_count": job_count,
                "prevalence": round(job_count / qualifying_jobs, 4),
                "evidence": "evidenced" if evidenced else "unverified",
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["job_count"]),
            -int(row["occurrence_count"]),
            str(row["term"]).casefold(),
        )
    )
    sufficient = qualifying_jobs >= MIN_REQUIREMENT_SAMPLE
    top_terms = rows[:20] if sufficient else []
    return {
        "sample": {
            "jobs": qualifying_jobs,
            "minimum": MIN_REQUIREMENT_SAMPLE,
            "score_threshold": REQUIREMENT_SCORE_THRESHOLD,
            "status": "ready" if sufficient else "insufficient_sample",
        },
        "job_count": qualifying_jobs,
        "top_terms": top_terms,
        "evidenced": [row for row in top_terms if row["evidence"] == "evidenced"],
        "unverified": [row for row in top_terms if row["evidence"] == "unverified"],
    }


def analytics_report(
    session: Session, *, now: datetime | None = None
) -> dict[str, Any]:
    """Build the release 0.5 analytics data contract."""

    funnel = funnel_summary(session)
    score_rows = _score_rows(session)
    score_snapshot_count = len(score_rows)
    source_snapshot_count = sum(
        bool(_application_sources(applied_sources))
        for applied_sources in session.scalars(select(Application.applied_sources))
    )
    total = funnel["total"]
    sufficient = funnel["total"] >= MIN_ANALYTICS_SAMPLE
    return {
        "sample": {
            "applications": total,
            "minimum": MIN_ANALYTICS_SAMPLE,
            "status": "ready" if sufficient else "insufficient_sample",
            "snapshot_scores": score_snapshot_count,
            "missing_score_snapshots": total - score_snapshot_count,
            "score_snapshot_coverage": (
                round(score_snapshot_count / total, 4) if total else 0.0
            ),
            "snapshot_sources": source_snapshot_count,
            "missing_source_snapshots": total - source_snapshot_count,
            "source_snapshot_coverage": (
                round(source_snapshot_count / total, 4) if total else 0.0
            ),
        },
        "funnel": funnel,
        "source_yield": source_yield(session),
        "stage_durations": stage_duration_summary(session, now=now),
        "score_calibration": score_calibration(session),
        "rejection_patterns": rejection_patterns(session),
        "requirements": requirement_analytics(session),
    }


def _rate(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return f"{float(value) * 100:.1f}%"


def _number(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _bar(value: int, maximum: int, width: int = 12) -> str:
    if value <= 0 or maximum <= 0:
        return ""
    return "█" * max(1, round(width * value / maximum))


def _table_cell(value: object) -> str:
    """Keep local/user-provided labels inside one Markdown table cell."""

    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _render_requirement_markdown(report: dict[str, Any]) -> str:
    requirements = report["requirements"]
    sample = requirements["sample"]
    lines = ["# Shortlist requirement signals", ""]
    if sample["status"] != "ready":
        lines.append(
            f"Insufficient shortlist sample: {sample['jobs']} of "
            f"{sample['minimum']} qualifying jobs at score "
            f"{sample['score_threshold']:.1f} or higher."
        )
        return "\n".join(lines)
    lines.extend(
        [
            f"Based on {sample['jobs']} active canonical shortlisted jobs. "
            "Unverified means no supporting term was found in approved profile "
            "facts or an approved canonical document.",
            "",
            "| Requirement | Evidence | Jobs | Occurrences | Prevalence |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in requirements["top_terms"]:
        lines.append(
            f"| {_table_cell(row['term'])} | {row['evidence']} | "
            f"{row['job_count']} | {row['occurrence_count']} | "
            f"{_rate(row['prevalence'])} |"
        )
    return "\n".join(lines)


def render_analytics_markdown(report: dict[str, Any]) -> str:
    """Render compact tables/charts for headless views without raw JSON."""

    sample = report["sample"]
    if sample["status"] != "ready":
        application_section = (
            "# Analytics\n\n"
            f"Insufficient sample: {sample['applications']} of "
            f"{sample['minimum']} applications. Analytics appear after the "
            "minimum is reached.\n\n"
            f"Application-time snapshot coverage: scores "
            f"{sample['snapshot_scores']}/{sample['applications']}; sources "
            f"{sample['snapshot_sources']}/{sample['applications']}."
        )
        return application_section + "\n\n" + _render_requirement_markdown(report)

    funnel = report["funnel"]
    stage_counts = funnel["by_stage"]
    maximum_stage = max(stage_counts.values(), default=0)
    lines = [
        "# Application funnel",
        "",
        f"Response rate: **{_rate(funnel['response_rate'])}** · "
        f"Offer rate: **{_rate(funnel['offer_rate'])}**",
        "",
        f"Application-time snapshot coverage: scores "
        f"{sample['snapshot_scores']}/{sample['applications']} "
        f"({_rate(sample['score_snapshot_coverage'])}); sources "
        f"{sample['snapshot_sources']}/{sample['applications']} "
        f"({_rate(sample['source_snapshot_coverage'])}).",
        "",
        "| Stage | Applications | Distribution |",
        "| --- | ---: | --- |",
    ]
    for stage in ApplicationStage:
        count = stage_counts[stage.value]
        lines.append(f"| {stage.value} | {count} | {_bar(count, maximum_stage)} |")

    lines.extend(
        [
            "",
            "# Source yield",
            "",
            "| Source | Applications | Responses | Offers | Response rate |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in report["source_yield"]:
        lines.append(
            f"| {_table_cell(row['source'])} | {row['applications']} | "
            f"{row.get('responses', 0)} | {row.get('offers', 0)} | "
            f"{_rate(row['response_rate'])} |"
        )

    lines.extend(
        [
            "",
            "# Time in stage (days)",
            "",
            "Open intervals are right-censored and excluded from percentiles.",
            "",
            "| Stage | Complete | Censored | Median | P25 | P75 | P90 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in report["stage_durations"]:
        lines.append(
            f"| {row['stage']} | {row['completed']} | {row['censored']} | "
            f"{_number(row['median_days'])} | {_number(row['p25_days'])} | "
            f"{_number(row['p75_days'])} | {_number(row['p90_days'])} |"
        )

    lines.extend(
        [
            "",
            "# Application-time score calibration",
            "",
            "| Score bucket | Applications | Progression rate |",
            "| ---: | ---: | ---: |",
        ]
    )
    for row in report["score_calibration"]:
        lines.append(
            f"| {row['score_bucket']} | {row['applications']} | "
            f"{_rate(row['progression_rate'])} |"
        )

    lines.extend(
        [
            "",
            "# Rejection patterns",
            "",
            "| Reason | Count |",
            "| --- | ---: |",
        ]
    )
    for row in report["rejection_patterns"]:
        lines.append(f"| {_table_cell(row['reason'])} | {row['count']} |")
    return "\n".join(lines) + "\n\n" + _render_requirement_markdown(report)


__all__ = [
    "MIN_ANALYTICS_SAMPLE",
    "MIN_REQUIREMENT_SAMPLE",
    "REQUIREMENT_SCORE_THRESHOLD",
    "REQUIREMENT_TERM_LEXICON",
    "RequirementTerm",
    "analytics_report",
    "funnel_summary",
    "rejection_patterns",
    "requirement_analytics",
    "render_analytics_markdown",
    "score_calibration",
    "source_yield",
    "stage_duration_summary",
    "time_in_stage",
]
