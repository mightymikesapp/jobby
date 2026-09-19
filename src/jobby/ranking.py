"""Deterministic, explainable stress-adjusted job ranking.

The evaluator in this module is deliberately pure: it accepts a mapping, a
Pydantic input, or a ``Job``-like object and returns a Pydantic result without
performing I/O.  ``persist_evaluation`` is the small persistence boundary that
translates that result to Jobby's SQLAlchemy models.

Automatic ranking is evidence-led.  A missing fact is never silently treated
as a positive fact; it lowers confidence and is surfaced as a warning instead.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from enum import StrEnum
from functools import lru_cache
import hashlib
import html
import json
import math
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator


class ScoreDimension(StrEnum):
    COMPENSATION = "compensation"
    WORKLOAD_STRESS = "workload_stress"
    WORKLOAD = "workload_stress"
    FIT = "fit"
    GATE_PASSABILITY = "gate_passability"
    GATES = "gate_passability"
    STRATEGIC_OPTIONALITY = "strategic_optionality"
    STRATEGIC = "strategic_optionality"
    LOCATION_COL = "location_col"
    LOCATION = "location_col"


SCORE_WEIGHTS: dict[ScoreDimension, float] = {
    ScoreDimension.COMPENSATION: 0.25,
    ScoreDimension.WORKLOAD_STRESS: 0.25,
    ScoreDimension.FIT: 0.20,
    ScoreDimension.GATE_PASSABILITY: 0.15,
    ScoreDimension.STRATEGIC_OPTIONALITY: 0.10,
    ScoreDimension.LOCATION_COL: 0.05,
}
# A concise alias is useful to renderers and callers that already use the term.
WEIGHTS = SCORE_WEIGHTS
RANKER_VERSION = "deterministic-v2.0.0"


class GateName(StrEnum):
    UNPAID_WORK = "unpaid_work"
    BAR_ADMISSION = "bar_admission"
    EXPERIENCE_YEARS = "experience_years"
    WORK_AUTHORIZATION = "work_authorization"
    SALARY_FLOOR = "salary_floor"
    LOCATION = "location"
    BILLABLES = "billables"
    QUOTA = "quota"
    TRAVEL = "travel"
    ON_CALL = "on_call"


class GateStatus(StrEnum):
    PASS = "pass"
    PASSED = "pass"
    FAIL = "fail"
    FAILED = "fail"
    WARNING = "warning"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class EvidencePassage(BaseModel):
    """A verbatim, bounded passage and the input field it came from."""

    model_config = ConfigDict(frozen=True)

    passage: str
    source: str = "description"
    label: str | None = None
    start: int | None = None
    end: int | None = None

    @computed_field
    @property
    def text(self) -> str:
        """Compatibility/readability alias for UI clients."""

        return self.passage


class ScoreComponent(BaseModel):
    """One explainable 1--5 scorecard component."""

    model_config = ConfigDict(frozen=True)

    dimension: ScoreDimension
    score: float = Field(ge=1.0, le=5.0)
    weight: float = Field(gt=0.0, le=1.0)
    evidence: list[EvidencePassage] = Field(default_factory=list)
    missing_data_warning: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str

    @computed_field
    @property
    def weighted_score(self) -> float:
        return round(self.score * self.weight, 4)


class GateResult(BaseModel):
    """A requirement or stress trap found by deterministic extraction."""

    model_config = ConfigDict(frozen=True)

    name: GateName
    status: GateStatus
    evidence: list[EvidencePassage] = Field(default_factory=list)
    rationale: str
    warning: str | None = None
    blocking: bool = False
    confidence: float = Field(ge=0.0, le=1.0)


class ProfileFactEvidence(BaseModel):
    """Immutable provenance for one candidate input used by the ranker."""

    model_config = ConfigDict(frozen=True)

    field: str
    fact_key: str
    profile_fact_id: str
    content_hash: str


DEFAULT_EDGE_ASSETS: dict[str, list[str]] = {
    "legal_ai_build": [
        "legal ai",
        "legal technology",
        "legal tech",
        "artificial intelligence",
        "machine learning",
        "workflow automation",
        "ai automation",
        "knowledge engineer",
    ],
    "ip_patent": [
        "intellectual property",
        "patent",
        "copyright",
        "trademark",
        "licensing",
        "dmca",
    ],
    "jd_legal": [
        "juris doctor",
        "j.d.",
        " jd ",
        "legal research",
        "legal analyst",
        "attorney",
        "counsel",
        "paralegal",
    ],
    "ai_governance_policy": [
        "ai governance",
        "technology policy",
        "public policy",
        "policy analyst",
        "responsible ai",
        "trust and safety",
        "regulatory",
    ],
    "operations_execution": [
        "legal operations",
        "program management",
        "project management",
        "operations",
        "implementation",
        "cross-functional",
    ],
}


class RankingProfile(BaseModel):
    """Candidate facts and hard preferences used by deterministic ranking."""

    model_config = ConfigDict(extra="ignore")

    salary_floor: int = Field(default=0, ge=0)
    salary_floor_context: str | None = None
    contextual_salary_floors_enabled: bool = False
    federal_salary_floor: int = Field(default=74_000, ge=0)
    private_salary_floor: int = Field(default=100_000, ge=0)
    legal_ai_salary_floor: int = Field(default=140_000, ge=0)
    nyc_salary_floor: int = Field(default=160_000, ge=0)
    bay_area_salary_floor: int = Field(default=180_000, ge=0)
    years_experience: float | None = Field(default=None, ge=0)
    bar_admissions: list[str] = Field(default_factory=list)
    # Direct ranking calls historically treat an explicit empty list as "no
    # admission". Automatic app paths override this flag when no approved fact
    # exists, so unknown is not silently converted into a failed credential.
    bar_admissions_known: bool = True
    work_authorized: bool | None = None
    us_citizen: bool | None = None
    preferred_locations: list[str] = Field(
        default_factory=lambda: [
            "remote",
            "san diego",
            "los angeles",
            "washington dc",
            "washington d.c.",
            "honolulu",
            "nashville",
            "indianapolis",
            "salt lake city",
            "cincinnati",
            "fort worth",
        ]
    )
    strict_location: bool = False
    reject_billables: bool = True
    reject_quota: bool = False
    reject_on_call: bool = True
    max_travel_percent: int | None = Field(default=25, ge=0, le=100)
    edge_assets: dict[str, list[str]] = Field(
        default_factory=lambda: {
            key: list(values) for key, values in DEFAULT_EDGE_ASSETS.items()
        }
    )
    profile_evidence: list[ProfileFactEvidence] = Field(default_factory=list)
    profile_warnings: list[str] = Field(default_factory=list)

    @field_validator("bar_admissions", "preferred_locations")
    @classmethod
    def discard_blank_list_values(cls, value: list[str]) -> list[str]:
        return [item.strip() for item in value if item and item.strip()]


class JobFacts(BaseModel):
    """Small, persistence-independent view of a job used by the scorecard."""

    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    title: str = ""
    company: str | None = None
    description: str = ""
    compensation_text: str | None = None
    salary_min: int | None = Field(default=None, ge=0)
    salary_max: int | None = Field(default=None, ge=0)
    salary_currency: str = "UNK"
    compensation_period: str = "unknown"
    compensation_confidence: float = Field(default=0.0, ge=0, le=1)
    location: str | None = None
    remote_status: str | None = None

    @field_validator("title", "description", mode="before")
    @classmethod
    def text_not_none(cls, value: Any) -> str:
        return str(value or "")


def ranking_profile_from_config(config: Any) -> RankingProfile:
    """Build the personal ranking profile without coupling ranking to config I/O."""

    names = (
        "salary_floor",
        "contextual_salary_floors_enabled",
        "federal_salary_floor",
        "private_salary_floor",
        "legal_ai_salary_floor",
        "nyc_salary_floor",
        "bay_area_salary_floor",
    )
    return RankingProfile(
        **{name: getattr(config, name) for name in names if hasattr(config, name)}
    )


_CURRENT_BAR_KEYS = {
    "active_bar_admissions",
    "bar_admission",
    "bar_admissions",
    "bar_membership",
    "bar_memberships",
    "bar_status",
    "current_bar_admission",
    "current_bar_admissions",
    "licensed_jurisdictions",
}
_FUTURE_BAR_KEY_PARTS = (
    "anticipated",
    "exam",
    "expected",
    "future",
    "pending",
    "planned",
)
_FUTURE_BAR_VALUE_RE = re.compile(
    r"\b(?:anticipat(?:e|ed)|expect(?:ed)?|future|pending|plan(?:ned)?|"
    r"will (?:be|sit|take)|exam|candidate)\b",
    re.IGNORECASE,
)
_NO_CURRENT_BAR_RE = re.compile(
    r"\b(?:none|not (?:admitted|licensed)|no (?:active )?bar|unlicensed)\b",
    re.IGNORECASE,
)
_YEARS_EXPERIENCE_KEYS = {
    "candidate_years_experience",
    "professional_years_experience",
    "relevant_years_experience",
    "years_experience",
}
_WORK_AUTHORIZATION_KEYS = {
    "employment_authorization",
    "visa_status",
    "work_authorization",
    "work_authorized",
    "work_authorized_us",
}
_CITIZENSHIP_KEYS = {"citizen", "citizenship", "us_citizen"}
_LOCATION_KEYS = {
    "acceptable_locations",
    "city",
    "flexibility",
    "preferred_locations",
}


def _fact_normalized_key(fact_key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", fact_key.casefold()).strip("_")


def _key_is(normalized_key: str, candidates: set[str]) -> bool:
    return normalized_key in candidates or any(
        normalized_key.endswith(f"_{candidate}") for candidate in candidates
    )


def _fact_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, bool):
        return ["true" if value else "false"]
    if isinstance(value, (str, int, float)):
        text = str(value).strip()
        return [text] if text else []
    if isinstance(value, Mapping):
        result: list[str] = []
        for nested in value.values():
            result.extend(_fact_strings(nested))
        return result
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        result = []
        for nested in value:
            result.extend(_fact_strings(nested))
        return result
    return []


def _explicit_citizenship(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    folded = " ".join(_fact_strings(value)).casefold()
    if re.search(r"\b(?:not|non)[ -]?(?:a )?u\.?s\.? citizen\b", folded):
        return False
    if re.search(r"\bu\.?s\.? citizen(?:ship)?\b", folded):
        return True
    return None


def _explicit_work_authorization(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    folded = " ".join(_fact_strings(value)).casefold()
    if re.search(
        r"\b(?:not authorized|requires? (?:visa )?sponsorship|"
        r"needs? (?:visa )?sponsorship)\b",
        folded,
    ):
        return False
    if re.search(
        r"\b(?:no (?:visa )?sponsorship (?:is )?(?:needed|required)|"
        r"authorized to work (?:in )?(?:the )?(?:u\.?s\.?|united states)|"
        r"work authori[sz](?:ed|ation))\b",
        folded,
    ):
        return True
    return None


def _explicit_years(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if float(value) >= 0 else None
    strings = _fact_strings(value)
    if len(strings) != 1:
        return None
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(?:years?|yrs?)?\s*", strings[0], re.I)
    return float(match.group(1)) if match else None


def _current_bar_admissions(fact_key: str, value: Any) -> tuple[bool, list[str]]:
    normalized_key = _fact_normalized_key(fact_key)
    if any(part in normalized_key for part in _FUTURE_BAR_KEY_PARTS):
        return False, []
    if not _key_is(normalized_key, _CURRENT_BAR_KEYS):
        return False, []
    strings = _fact_strings(value)
    if not strings or all(_NO_CURRENT_BAR_RE.search(item) for item in strings):
        return True, []
    admissions = [
        item.strip()
        for item in strings
        if item.strip()
        and not _NO_CURRENT_BAR_RE.search(item)
        and not _FUTURE_BAR_VALUE_RE.search(item)
    ]
    # A future-only value is context, not evidence of current admission.
    if strings and not admissions:
        return False, []
    return True, admissions


def _preferred_location_values(value: Any) -> list[str]:
    result: list[str] = []
    for raw in _fact_strings(value):
        for item in re.split(r"[,;]", raw):
            item = item.strip()
            if ":" in item:
                item = item.rsplit(":", 1)[-1].strip()
            item = re.sub(r"\s*\([^)]*\)\s*", " ", item).strip()
            if item and item.casefold() not in {"ca", "usa", "us", "united states"}:
                result.append(item)
    return list(dict.fromkeys(result))


def ranking_profile_from_database(database: Any, config: Any) -> RankingProfile:
    """Build an automatic profile solely from approved candidate facts.

    Salary policy comes from ``AppConfig``. Candidate credentials and
    preferences never come from optimistic defaults, unapproved facts, or
    future-state facts such as an anticipated bar admission.
    """

    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from .models import ProfileFact

    def load(session: Session) -> list[tuple[str, str, Any, str]]:
        return [
            (fact.id, fact.fact_key, fact.value_json, fact.content_hash)
            for fact in session.scalars(
                select(ProfileFact)
                .where(ProfileFact.approved.is_(True))
                .order_by(ProfileFact.fact_key, ProfileFact.id)
            )
        ]

    if isinstance(database, Session):
        facts = load(database)
    else:
        database.initialize()
        with database.session() as session:
            facts = load(session)

    evidence: list[ProfileFactEvidence] = []

    def source(field: str, fact: tuple[str, str, Any, str]) -> None:
        fact_id, fact_key, _, content_hash = fact
        item = ProfileFactEvidence(
            field=field,
            fact_key=fact_key,
            profile_fact_id=fact_id,
            content_hash=content_hash,
        )
        if item not in evidence:
            evidence.append(item)

    citizenship_values: list[bool] = []
    work_authorization_values: list[bool] = []
    years_values: list[float] = []
    bar_admissions: list[str] = []
    bar_known = False
    future_bar_seen = False
    preferred_locations: list[str] = []
    all_approved_text: list[tuple[tuple[str, str, Any, str], str]] = []

    for fact in facts:
        _, fact_key, value, _ = fact
        normalized_key = _fact_normalized_key(fact_key)
        strings = _fact_strings(value)
        all_approved_text.append((fact, " ".join(strings)))

        if _key_is(normalized_key, _CITIZENSHIP_KEYS):
            if (citizenship := _explicit_citizenship(value)) is not None:
                citizenship_values.append(citizenship)
                source("us_citizen", fact)
                if citizenship:
                    work_authorization_values.append(True)
                    source("work_authorized", fact)

        if _key_is(normalized_key, _WORK_AUTHORIZATION_KEYS):
            if (authorized := _explicit_work_authorization(value)) is not None:
                work_authorization_values.append(authorized)
                source("work_authorized", fact)
            if (citizenship := _explicit_citizenship(value)) is not None:
                citizenship_values.append(citizenship)
                source("us_citizen", fact)

        if _key_is(normalized_key, _YEARS_EXPERIENCE_KEYS):
            if (years := _explicit_years(value)) is not None:
                years_values.append(years)
                source("years_experience", fact)

        recognized_bar, admissions = _current_bar_admissions(fact_key, value)
        if recognized_bar:
            bar_known = True
            bar_admissions.extend(admissions)
            source("bar_admissions", fact)
        elif "bar" in normalized_key and (
            any(part in normalized_key for part in _FUTURE_BAR_KEY_PARTS)
            or _FUTURE_BAR_VALUE_RE.search(" ".join(strings))
        ):
            future_bar_seen = True

        location_key = _key_is(normalized_key, _LOCATION_KEYS)
        # ``candidate.location`` is an explicit present location; country and
        # timezone fields are deliberately not interpreted as preferences.
        if location_key or normalized_key == "candidate_location":
            values = _preferred_location_values(value)
            if values:
                preferred_locations.extend(values)
                source("preferred_locations", fact)

    warnings: list[str] = []

    def resolved_bool(values: list[bool], label: str) -> bool | None:
        unique = set(values)
        if len(unique) == 1:
            return unique.pop()
        if len(unique) > 1:
            warnings.append(
                f"Candidate {label} is unknown because approved profile facts conflict."
            )
        else:
            warnings.append(
                f"Candidate {label} is unknown because no approved profile fact explicitly establishes it."
            )
        return None

    us_citizen = resolved_bool(citizenship_values, "U.S. citizenship")
    work_authorized = resolved_bool(
        work_authorization_values, "U.S. work authorization"
    )

    unique_years = set(years_values)
    if len(unique_years) == 1:
        years_experience = next(iter(unique_years))
    else:
        years_experience = None
        warnings.append(
            "Candidate years of experience are unknown because approved profile facts conflict."
            if unique_years
            else "Candidate years of experience are unknown because no approved numeric profile fact establishes them."
        )

    if not bar_known:
        warnings.append(
            "Candidate current bar admission is unknown; approved anticipated, pending, or exam facts are not treated as active admission."
            if future_bar_seen
            else "Candidate current bar admission is unknown because no approved profile fact establishes active admission."
        )

    edge_assets: dict[str, list[str]] = {}
    for asset, terms in DEFAULT_EDGE_ASSETS.items():
        matched: list[str] = []
        for fact, text in all_approved_text:
            folded = _normalized(text)
            matching_terms = [term for term in terms if _normalized(term) in folded]
            if matching_terms:
                matched.extend(matching_terms)
                source(f"edge_assets.{asset}", fact)
        if matched:
            edge_assets[asset] = list(dict.fromkeys(matched))
    if not edge_assets:
        warnings.append(
            "Candidate fit assets are unavailable because no approved profile fact supports a configured edge."
        )

    if not preferred_locations:
        warnings.append(
            "Candidate location preferences are unavailable because no approved profile fact establishes them."
        )

    base = ranking_profile_from_config(config)
    return base.model_copy(
        update={
            "years_experience": years_experience,
            "bar_admissions": list(dict.fromkeys(bar_admissions)),
            "bar_admissions_known": bar_known,
            "work_authorized": work_authorized,
            "us_citizen": us_citizen,
            "preferred_locations": list(dict.fromkeys(preferred_locations)),
            "edge_assets": edge_assets,
            "profile_evidence": evidence,
            "profile_warnings": list(dict.fromkeys(warnings)),
        }
    )


class ManualOverride(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=1.0, le=5.0)
    explanation: str | None = None
    locked: bool = True


class JobEvaluationResult(BaseModel):
    """Complete, serializable explanation of a ranking decision."""

    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=1.0, le=5.0)
    components: dict[ScoreDimension, ScoreComponent] = Field(default_factory=dict)
    gates: list[GateResult] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    explanation: str
    confidence: float = Field(ge=0.0, le=1.0)
    automatic_skip: bool = False
    skip_reason: str | None = None
    ranking_eligible: bool = True
    manual_override: bool = False
    locked: bool = False
    rescored: bool = True
    preserved_manual_override: bool = False
    method: str = RANKER_VERSION
    facts: JobFacts | None = None
    profile_evidence: list[ProfileFactEvidence] = Field(default_factory=list)

    @computed_field
    @property
    def final_score(self) -> float:
        return self.score

    @computed_field
    @property
    def weighted_score(self) -> float:
        return self.score


# Shorter public alias used in a few call sites and convenient in type hints.
JobEvaluation = JobEvaluationResult
EvaluationResult = JobEvaluationResult
CandidateProfile = RankingProfile
Subscore = ScoreComponent


_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_UNPAID_RE = re.compile(
    r"\b(?:(?:unpaid|uncompensated)(?:\s+[a-z][\w-]*){0,4}\s+(?:position|role|job|internship|fellowship|appointment|opportunity|work)|(?:position|role|job|internship|fellowship|appointment|opportunity|work)\s+(?:is|will be)\s+unpaid|academic credit only|no (?:salary|pay|compensation)|without compensation|volunteer(?:\s+[a-z][\w-]*){0,3}\s+(?:position|role|internship))\b",
    re.IGNORECASE,
)
_PAID_RE = re.compile(
    r"\b(?:paid (?:position|role|internship)|salary|base pay|compensation range)\b",
    re.IGNORECASE,
)
_BAR_REQUIRED_RE = re.compile(
    r"\b(?:bar (?:admission|membership) (?:is )?required|required to (?:be )?(?:admitted|licensed)|must (?:be )?(?:admitted|licensed)|active (?:member|membership)[^.]{0,80}\bbar\b|active [^.]{0,80}\bbar (?:member|membership)\b|admitted to (?:the )?[^.]{0,40}\bbar\b|member(?:ship)? in good standing[^.]{0,60}\bbar\b|licensed (?:attorney|to practice law))",
    re.IGNORECASE,
)
_BAR_PREFERRED_RE = re.compile(
    r"\b(?:(?:bar admission|licensed attorney)[^.]{0,50}(?:preferred|a plus)|prefer(?:red)?[^.]{0,50}(?:bar admission|licensed attorney))\b",
    re.IGNORECASE,
)
_EXPERIENCE_RE = re.compile(
    r"\b(?:minimum (?:of )?|at least )?(?P<minimum>\d{1,2})(?:\s*[-\u2013\u2014]\s*(?P<maximum>\d{1,2}))?\+?\s+years?(?:\s+of)?(?:\s+(?:relevant|related|professional|legal|post[- ]qualification))?\s+experience\b",
    re.IGNORECASE,
)
_AUTH_RE = re.compile(
    r"\b(?:authorized to work|authorization to work|work authorization|eligible to work|no (?:visa )?sponsorship|without (?:visa )?sponsorship|sponsorship (?:is )?not available)\b",
    re.IGNORECASE,
)
_CITIZEN_RE = re.compile(
    r"\b(?:u\.?s\.? citizenship (?:is )?required|must be (?:a )?u\.?s\.? citizen|united states citizenship (?:is )?required)\b",
    re.IGNORECASE,
)
_BILLABLE_RE = re.compile(
    r"\b(?:billable(?:[- ]hours?)?|annual billable|billables)\b", re.IGNORECASE
)
_NO_BILLABLE_RE = re.compile(
    r"\b(?:no|without)\s+(?:annual\s+)?billable(?:[- ]hours?)?\b", re.IGNORECASE
)
_QUOTA_RE = re.compile(
    r"\b(?:sales quota|quota[- ]carrying|quota attainment|revenue target|book of business|commission[- ]based|on[- ]target earnings|\bOTE\b)\b",
    re.IGNORECASE,
)
_NO_QUOTA_RE = re.compile(r"\b(?:no|without)\s+(?:sales\s+)?quota\b", re.IGNORECASE)
_TRAVEL_RE = re.compile(
    r"\b(?:(?:up to |approximately |about )?(?P<percent>\d{1,3})\s*%\s*travel|(?P<level>frequent|extensive|regular|significant) travel|travel (?:is )?required)\b",
    re.IGNORECASE,
)
_ON_CALL_RE = re.compile(
    r"\b(?:on[- ]call|after[- ]hours support|nights and weekends|weekend rotation|incident response rotation|24\s*/\s*7)\b",
    re.IGNORECASE,
)
_NO_ON_CALL_RE = re.compile(r"\b(?:no|without)\s+on[- ]call\b", re.IGNORECASE)
_HIGH_STRESS_RE = re.compile(
    r"\b(?:fast[- ]paced|high[- ]pressure|high velocity|tight deadlines|crisis response|wear many hats|first legal hire|always[- ]on)\b",
    re.IGNORECASE,
)
_LOW_STRESS_RE = re.compile(
    r"\b(?:predictable hours|work[- ]life balance|flexible schedule|flexible hours|no billable|standard business hours)\b",
    re.IGNORECASE,
)
_REMOTE_RE = re.compile(
    r"\b(?:remote|work from home|telework|telecommut(?:e|ing)|distributed)\b",
    re.IGNORECASE,
)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
_ONSITE_RE = re.compile(
    r"\b(?:on[- ]site|onsite|in[- ]office|office[- ]based)\b", re.IGNORECASE
)
_FEDERAL_ROLE_RE = re.compile(
    r"\b(?:federal (?:civil service|government) (?:position|role|job|employment)|"
    r"(?:federal )?gs \d{1,2}|general schedule|usajobs)\b"
)
_FEDERAL_COMPANY_RE = re.compile(
    r"\b(?:"
    r"(?:united states|u s) (?:government|department|agency|commission|administration|"
    r"bureau|service|office|court)|"
    r"department of (?:agriculture|commerce|defense|education|energy|health and human "
    r"services|homeland security|housing and urban development|justice|labor|state|"
    r"the interior|the treasury|transportation|veterans affairs)|"
    r"federal (?:communications|election|energy regulatory|maritime|trade) commission|"
    r"consumer financial protection bureau|environmental protection agency|"
    r"general services administration|government accountability office|"
    r"internal revenue service|national aeronautics and space administration|"
    r"national labor relations board|office of personnel management|"
    r"securities and exchange commission|small business administration|"
    r"social security administration"
    r")\b"
)
_BAY_AREA_LOCATION_RE = re.compile(
    r"\b(?:bay area|berkeley|cupertino|menlo park|mountain view|oakland|palo alto|"
    r"redwood city|san francisco|san jose|san mateo|santa clara|silicon valley|"
    r"sunnyvale|walnut creek)\b"
)
_NYC_LOCATION_RE = re.compile(
    r"\b(?:brooklyn|manhattan|new york city|new york ny|nyc|queens)\b"
)
_LEGAL_AI_ROLE_RE = re.compile(
    r"\b(?:ai governance|legal ai|legal automation|legal engineer|legal tech|"
    r"legal technology|responsible ai)\b"
)
_STRATEGIC_TERMS: dict[str, list[str]] = {
    "ownership": [
        "build from scratch",
        "founding",
        "ownership",
        "lead the",
        "shape the",
    ],
    "product": [
        "product strategy",
        "product development",
        "workflow automation",
        "implementation",
    ],
    "upside": ["equity", "stock options", "profit sharing"],
    "research": ["research", "thought leadership", "publication", "policy development"],
    "ecosystem": ["partnerships", "cross-functional", "industry", "stakeholders"],
}
_HIGH_COL_TERMS = (
    "san francisco",
    "bay area",
    "new york",
    "manhattan",
    "silicon valley",
)
_SALARY_RANGE_RE = re.compile(
    r"(?P<currency>\$|USD\s*)?(?P<minimum>\d{1,6}(?:,\d{3})*|\d{1,6}(?:\.\d+)?)\s*(?P<mink>[kK])?\s*(?:-|\u2013|\u2014|to)\s*(?:\$|USD\s*)?(?P<maximum>\d{1,6}(?:,\d{3})*|\d{1,6}(?:\.\d+)?)\s*(?P<maxk>[kK])?\s*(?:(?:per\s+|/\s*|an?\s+)?(?P<range_period>year|yr|annual(?:ly)?|hour|hr))?",
    re.IGNORECASE,
)
_SALARY_SINGLE_RE = re.compile(
    r"(?:\$|USD\s*)(?P<amount>\d{1,6}(?:,\d{3})*|\d{1,6}(?:\.\d+)?)\s*(?P<k>[kK])?\s*(?:(?:per\s+|/\s*|an?\s+)?(?P<period>year|yr|annual(?:ly)?|hour|hr))?",
    re.IGNORECASE,
)


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = _TAG_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _object_value(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _coerce_job(job: JobFacts | Mapping[str, Any] | Any) -> JobFacts:
    if isinstance(job, JobFacts):
        return job

    location = _object_value(job, "location", "location_name", "location_display")
    if location is not None and not isinstance(location, str):
        location = _object_value(location, "display_name", "name")
    company = _object_value(job, "company", "company_name")
    if company is not None and not isinstance(company, str):
        company = _object_value(company, "name")
    remote_status = _object_value(job, "remote_status", "remote")
    if isinstance(remote_status, bool):
        remote_status = "remote" if remote_status else "onsite"
    # Before compensation provenance fields existed, callers supplied numeric
    # ``salary_min``/``salary_max`` as already-annualized USD values. Preserve
    # that API contract only when the new metadata keys are wholly absent.
    # Persisted rows always expose those keys, including explicit ``unknown``.
    legacy_annual_usd = (
        isinstance(job, Mapping)
        and any(key in job for key in ("salary_min", "salary_max"))
        and not any(
            key in job
            for key in (
                "salary_currency",
                "currency",
                "compensation_period",
                "salary_period",
                "compensation_confidence",
                "salary_confidence",
            )
        )
    )
    return JobFacts(
        id=_object_value(job, "id"),
        title=_object_value(job, "title", default=""),
        company=company,
        description=_object_value(job, "description", "job_description", default=""),
        compensation_text=_object_value(
            job, "compensation_text", "compensation", "salary_text"
        ),
        salary_min=_coerce_int(_object_value(job, "salary_min", "minimum_salary")),
        salary_max=_coerce_int(_object_value(job, "salary_max", "maximum_salary")),
        salary_currency=str(
            _object_value(
                job,
                "salary_currency",
                "currency",
                default="USD" if legacy_annual_usd else "UNK",
            )
            or ("USD" if legacy_annual_usd else "UNK")
        ).upper(),
        compensation_period=str(
            _object_value(
                job,
                "compensation_period",
                "salary_period",
                default="year" if legacy_annual_usd else "unknown",
            )
            or ("year" if legacy_annual_usd else "unknown")
        ).casefold(),
        compensation_confidence=float(
            _object_value(
                job,
                "compensation_confidence",
                "salary_confidence",
                default=1.0 if legacy_annual_usd else 0.0,
            )
            or (1.0 if legacy_annual_usd else 0.0)
        ),
        location=location,
        remote_status=remote_status,
    )


def _coerce_profile(
    profile: RankingProfile | Mapping[str, Any] | Any | None,
) -> RankingProfile:
    if profile is None:
        return RankingProfile()
    if isinstance(profile, RankingProfile):
        return profile
    if isinstance(profile, Mapping):
        payload = dict(profile)
    elif hasattr(profile, "model_dump"):
        payload = profile.model_dump()
    else:
        payload = {
            field: getattr(profile, field)
            for field in RankingProfile.model_fields
            if hasattr(profile, field)
        }

    # Accept a few natural names used by config/profile readers without making
    # the scorecard depend on any particular config parser.
    aliases = {
        "candidate_years_experience": "years_experience",
        "work_authorized_us": "work_authorized",
        "citizen": "us_citizen",
        "acceptable_locations": "preferred_locations",
    }
    for source, target in aliases.items():
        if source in payload and target not in payload:
            payload[target] = payload[source]
    return RankingProfile.model_validate(payload)


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        numeric = float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(numeric) or numeric < 0:
        return None
    parsed = int(numeric)
    return parsed if parsed <= 2**63 - 1 else None


def _corpus(facts: JobFacts) -> dict[str, str]:
    return {
        "title": _clean_text(facts.title),
        "description": _clean_text(facts.description),
        "compensation": _clean_text(facts.compensation_text),
        "location": _clean_text(facts.location),
        "remote_status": _clean_text(facts.remote_status),
    }


def _sentence_passage(
    source: str, text: str, match: re.Match[str], label: str
) -> EvidencePassage:
    # Prefer a sentence-like boundary, while bounding pathological ATS blobs.
    start = max(
        text.rfind(".", 0, match.start()),
        text.rfind(";", 0, match.start()),
        text.rfind("\n", 0, match.start()),
    )
    start = 0 if start < 0 else start + 1
    ends = [
        position
        for token in (".", ";", "\n")
        if (position := text.find(token, match.end())) >= 0
    ]
    end = min(ends) + 1 if ends else len(text)
    if end - start > 320:
        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 180)
    return EvidencePassage(
        passage=text[start:end].strip(),
        source=source,
        label=label,
        start=match.start(),
        end=match.end(),
    )


def _find(
    corpus: Mapping[str, str],
    pattern: re.Pattern[str],
    label: str,
    *,
    sources: Sequence[str] | None = None,
) -> tuple[EvidencePassage | None, re.Match[str] | None]:
    for source in sources or tuple(corpus):
        text = corpus.get(source, "")
        if text and (match := pattern.search(text)):
            return _sentence_passage(source, text, match, label), match
    return None, None


def _find_literal(
    corpus: Mapping[str, str], term: str, label: str
) -> EvidencePassage | None:
    pattern = _literal_pattern(term)
    passage, _ = _find(corpus, pattern, label)
    return passage


@lru_cache(maxsize=512)
def _literal_pattern(term: str) -> re.Pattern[str]:
    return re.compile(rf"(?<!\w){re.escape(term)}(?!\w)", re.IGNORECASE)


def _salary_from_text(
    text: str,
) -> tuple[int | None, int | None, EvidencePassage | None]:
    if not text:
        return None, None, None
    if match := _SALARY_RANGE_RE.search(text):
        minimum = float(match.group("minimum").replace(",", ""))
        maximum = float(match.group("maximum").replace(",", ""))
        period = (match.group("range_period") or "").lower()
        if period in {"hour", "hr"}:
            minimum *= 2080
            maximum *= 2080
        else:
            if match.group("mink") or minimum < 1000:
                minimum *= 1000
            if match.group("maxk") or maximum < 1000:
                maximum *= 1000
        passage = _sentence_passage("compensation", text, match, "salary")
        return int(minimum), int(maximum), passage
    if match := _SALARY_SINGLE_RE.search(text):
        amount = float(match.group("amount").replace(",", ""))
        if match.group("k"):
            amount *= 1000
        period = (match.group("period") or "").lower()
        if period in {"hour", "hr"}:
            amount *= 2080
        elif amount < 1000:
            # A dollar-prefixed two/three-digit number without a period is most
            # commonly hourly in job postings; keep the inference explicit.
            amount *= 2080
        passage = _sentence_passage("compensation", text, match, "salary")
        return int(amount), int(amount), passage
    return None, None, None


def _salary_facts(
    facts: JobFacts, corpus: Mapping[str, str]
) -> tuple[int | None, int | None, list[EvidencePassage]]:
    minimum, maximum = facts.salary_min, facts.salary_max
    evidence: list[EvidencePassage] = []
    if minimum is not None or maximum is not None:
        raw = corpus.get("compensation", "")
        if raw:
            evidence.append(
                EvidencePassage(
                    passage=raw[:320], source="compensation", label="salary"
                )
            )
        else:
            lower = minimum if minimum is not None else maximum
            upper = maximum if maximum is not None else minimum
            evidence.append(
                EvidencePassage(
                    passage=f"Structured salary range: ${lower:,}--${upper:,}",
                    source="salary_fields",
                    label="salary",
                )
            )
    parsed_min, parsed_max, parsed_evidence = _salary_from_text(
        corpus.get("compensation", "")
    )
    minimum = minimum if minimum is not None else parsed_min
    maximum = maximum if maximum is not None else parsed_max
    if parsed_evidence and not evidence:
        evidence.append(parsed_evidence)
    if minimum is None:
        minimum = maximum
    if maximum is None:
        maximum = minimum
    if minimum is not None and maximum is not None and minimum > maximum:
        minimum, maximum = maximum, minimum
    return minimum, maximum, evidence


def _normalized(value: str) -> str:
    return _SPACE_RE.sub(" ", re.sub(r"[^a-z0-9]+", " ", value.casefold())).strip()


_BAR_JURISDICTIONS = {
    "california": ("california", "ca bar"),
    "new york": ("new york", "ny bar"),
    "district of columbia": (
        "district of columbia",
        "washington dc",
        "dc bar",
        "d c bar",
    ),
    "texas": ("texas", "tx bar"),
    "florida": ("florida", "fl bar"),
    "illinois": ("illinois", "il bar"),
    "massachusetts": ("massachusetts", "ma bar"),
    "virginia": ("virginia", "va bar"),
    "washington": ("washington state", "wa bar"),
    "hawaii": ("hawaii", "hi bar"),
}
_BAR_ABBREVIATIONS = {
    "ca": "california",
    "ny": "new york",
    "dc": "district of columbia",
    "d c": "district of columbia",
    "tx": "texas",
    "fl": "florida",
    "il": "illinois",
    "ma": "massachusetts",
    "va": "virginia",
    "wa": "washington",
    "hi": "hawaii",
}


def _bar_matches(posting_text: str, admissions: Sequence[str]) -> bool:
    admitted = {
        _BAR_ABBREVIATIONS.get(_normalized(item), _normalized(item))
        for item in admissions
    }
    if not admitted:
        return False
    folded = _normalized(posting_text)
    if re.search(r"\bany (?:us |u s )?(?:state |jurisdiction )?bar\b", folded):
        return True
    required: set[str] = set()
    for canonical, aliases in _BAR_JURISDICTIONS.items():
        if any(_normalized(alias) in folded for alias in aliases):
            required.add(canonical)
    if not required:
        return True
    for item in admitted:
        for canonical, aliases in _BAR_JURISDICTIONS.items():
            if (
                canonical in item
                or item == canonical
                or any(_normalized(alias) == item for alias in aliases)
            ):
                if canonical in required:
                    return True
    return False


def _salary_floor_phrase(profile: RankingProfile) -> str:
    context = f"{profile.salary_floor_context} " if profile.salary_floor_context else ""
    return f"${profile.salary_floor:,} {context}salary floor"


def _salary_policy_reliability(
    facts: JobFacts, corpus: Mapping[str, str]
) -> tuple[bool, str | None]:
    """Return whether amounts may safely participate in USD annual policy."""

    from .normalization import SalaryPeriod, normalize_salary

    has_persisted_provenance = (
        facts.salary_currency.upper() != "UNK"
        or facts.compensation_period.casefold() != SalaryPeriod.UNKNOWN.value
        or facts.compensation_confidence > 0
    )
    parsed = normalize_salary(corpus.get("compensation", ""))
    if has_persisted_provenance:
        currency = facts.salary_currency.upper()
        period = facts.compensation_period.casefold()
        confidence = facts.compensation_confidence
    elif parsed is not None:
        currency = parsed.currency
        period = parsed.period.value
        confidence = parsed.confidence
    else:
        currency = facts.salary_currency.upper()
        period = facts.compensation_period.casefold()
        confidence = facts.compensation_confidence
    if currency != "USD":
        return (
            False,
            "Compensation is non-USD or its currency is unknown; it remains visible but cannot reject this job.",
        )
    if period == SalaryPeriod.UNKNOWN.value:
        return (
            False,
            "Compensation period is unknown or not confidently annualized; it remains visible but cannot reject this job.",
        )
    if confidence < 0.8:
        return (
            False,
            "Compensation normalization confidence is too low for automatic salary-floor gating.",
        )
    return True, None


def _extract_gates(
    facts: JobFacts,
    profile: RankingProfile,
    corpus: Mapping[str, str],
    salary_min: int | None,
    salary_max: int | None,
    salary_evidence: list[EvidencePassage],
    salary_policy_reliable: bool,
    salary_policy_warning: str | None,
) -> list[GateResult]:
    gates: list[GateResult] = []
    floor_phrase = _salary_floor_phrase(profile)

    evidence, _ = _find(corpus, _UNPAID_RE, "unpaid work")
    if evidence:
        gates.append(
            GateResult(
                name=GateName.UNPAID_WORK,
                status=GateStatus.FAIL,
                evidence=[evidence],
                rationale="The posting explicitly describes uncompensated work.",
                warning="Unpaid work is an automatic skip.",
                blocking=True,
                confidence=0.99,
            )
        )
    else:
        paid_evidence, _ = _find(corpus, _PAID_RE, "paid work")
        if not paid_evidence and salary_evidence:
            paid_evidence = salary_evidence[0]
        gates.append(
            GateResult(
                name=GateName.UNPAID_WORK,
                status=GateStatus.PASS if paid_evidence else GateStatus.UNKNOWN,
                evidence=[paid_evidence] if paid_evidence else [],
                rationale="Paid-work language was found."
                if paid_evidence
                else "No explicit paid or unpaid language was found.",
                warning=None
                if paid_evidence
                else "Compensation status is not explicit; confirm that the role is paid.",
                confidence=0.9 if paid_evidence else 0.45,
            )
        )

    bar_evidence, _ = _find(corpus, _BAR_REQUIRED_RE, "bar admission")
    preferred_bar_evidence, _ = _find(
        corpus, _BAR_PREFERRED_RE, "bar admission preference"
    )
    if bar_evidence:
        passes = _bar_matches(bar_evidence.passage, profile.bar_admissions)
        known = profile.bar_admissions_known or bool(profile.bar_admissions)
        gates.append(
            GateResult(
                name=GateName.BAR_ADMISSION,
                status=(
                    GateStatus.PASS
                    if passes
                    else GateStatus.FAIL
                    if known
                    else GateStatus.UNKNOWN
                ),
                evidence=[bar_evidence],
                rationale="Candidate bar admission matches the requirement."
                if passes
                else (
                    "A required bar admission could not be matched to the approved candidate profile."
                    if known
                    else "Current candidate bar admission is unknown because no approved current-state fact establishes it."
                ),
                warning=(
                    None
                    if passes
                    else "Required bar admission appears unmet."
                    if known
                    else "Required bar admission cannot be resolved from approved candidate facts."
                ),
                blocking=known and not passes,
                confidence=0.93 if profile.bar_admissions else 0.88 if known else 0.5,
            )
        )
    elif preferred_bar_evidence:
        passes = bool(profile.bar_admissions)
        known = profile.bar_admissions_known or passes
        gates.append(
            GateResult(
                name=GateName.BAR_ADMISSION,
                status=(
                    GateStatus.PASS
                    if passes
                    else GateStatus.WARNING
                    if known
                    else GateStatus.UNKNOWN
                ),
                evidence=[preferred_bar_evidence],
                rationale=(
                    "Bar admission is described as a preference, not a requirement."
                    if known
                    else "Preferred bar admission is stated, but current candidate admission is unknown."
                ),
                warning=(
                    None
                    if passes
                    else "Preferred bar admission is not present in the approved candidate profile."
                    if known
                    else "Preferred bar admission cannot be resolved from approved candidate facts."
                ),
                confidence=0.88 if known else 0.5,
            )
        )
    else:
        gates.append(
            GateResult(
                name=GateName.BAR_ADMISSION,
                status=GateStatus.UNKNOWN,
                rationale="No explicit bar-admission language was found.",
                warning="Bar-admission requirements may be unstated; verify before applying.",
                confidence=0.55,
            )
        )

    experience_evidence, experience_match = _find(
        corpus, _EXPERIENCE_RE, "experience requirement"
    )
    if experience_evidence and experience_match:
        required_years = float(experience_match.group("minimum"))
        if profile.years_experience is None:
            gates.append(
                GateResult(
                    name=GateName.EXPERIENCE_YEARS,
                    status=GateStatus.UNKNOWN,
                    evidence=[experience_evidence],
                    rationale=f"The posting asks for at least {required_years:g} years; candidate years are not configured.",
                    warning="Candidate experience years are missing, so this gate cannot be resolved.",
                    confidence=0.72,
                )
            )
        else:
            passes = profile.years_experience >= required_years
            gates.append(
                GateResult(
                    name=GateName.EXPERIENCE_YEARS,
                    status=GateStatus.PASS if passes else GateStatus.FAIL,
                    evidence=[experience_evidence],
                    rationale=(
                        f"Candidate experience ({profile.years_experience:g} years) meets the {required_years:g}-year requirement."
                        if passes
                        else f"Candidate experience ({profile.years_experience:g} years) is below the {required_years:g}-year requirement."
                    ),
                    warning=None
                    if passes
                    else "The explicit years-of-experience gate appears unmet.",
                    blocking=not passes,
                    confidence=0.94,
                )
            )
    else:
        gates.append(
            GateResult(
                name=GateName.EXPERIENCE_YEARS,
                status=GateStatus.UNKNOWN,
                rationale="No explicit years-of-experience requirement was found.",
                warning="Years-of-experience requirements may be unstated.",
                confidence=0.55,
            )
        )

    citizen_evidence, _ = _find(corpus, _CITIZEN_RE, "citizenship requirement")
    auth_evidence, _ = _find(corpus, _AUTH_RE, "work authorization")
    if citizen_evidence:
        if profile.us_citizen is None:
            status, warning, blocking = (
                GateStatus.UNKNOWN,
                "Candidate citizenship is not configured.",
                False,
            )
        elif profile.us_citizen:
            status, warning, blocking = GateStatus.PASS, None, False
        else:
            status, warning, blocking = (
                GateStatus.FAIL,
                "U.S. citizenship is explicitly required.",
                True,
            )
        gates.append(
            GateResult(
                name=GateName.WORK_AUTHORIZATION,
                status=status,
                evidence=[citizen_evidence],
                rationale="The posting contains a U.S.-citizenship requirement.",
                warning=warning,
                blocking=blocking,
                confidence=0.96,
            )
        )
    elif auth_evidence:
        if profile.work_authorized is None:
            status, warning, blocking = (
                GateStatus.UNKNOWN,
                "Candidate work authorization is not configured.",
                False,
            )
        elif profile.work_authorized:
            status, warning, blocking = GateStatus.PASS, None, False
        else:
            status, warning, blocking = (
                GateStatus.FAIL,
                "The posting's work-authorization condition appears unmet.",
                True,
            )
        gates.append(
            GateResult(
                name=GateName.WORK_AUTHORIZATION,
                status=status,
                evidence=[auth_evidence],
                rationale="The posting contains an explicit work-authorization or sponsorship condition.",
                warning=warning,
                blocking=blocking,
                confidence=0.93,
            )
        )
    else:
        gates.append(
            GateResult(
                name=GateName.WORK_AUTHORIZATION,
                status=GateStatus.UNKNOWN,
                rationale="No explicit work-authorization language was found.",
                warning="Work-authorization or sponsorship policy is not stated.",
                confidence=0.5,
            )
        )

    if not salary_policy_reliable and salary_evidence:
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=GateStatus.UNKNOWN,
                evidence=salary_evidence,
                rationale="The salary range is shown but is not eligible for automatic USD annual policy.",
                warning=salary_policy_warning,
                blocking=False,
                confidence=0.45,
            )
        )
    elif profile.salary_floor <= 0:
        status = (
            GateStatus.PASS if salary_min is not None else GateStatus.NOT_APPLICABLE
        )
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=status,
                evidence=salary_evidence,
                rationale="No salary floor is configured."
                if not salary_evidence
                else "Compensation evidence is available and no salary floor is configured.",
                confidence=0.95 if salary_evidence else 0.8,
            )
        )
    elif salary_min is None or salary_max is None:
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=GateStatus.UNKNOWN,
                rationale=f"The {floor_phrase} cannot be tested because compensation is missing.",
                warning="Compensation is missing; verify the salary floor before investing application time.",
                confidence=0.35,
            )
        )
    elif salary_max < profile.salary_floor:
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=GateStatus.FAIL,
                evidence=salary_evidence,
                rationale=f"The top of the range (${salary_max:,}) is below the {floor_phrase}.",
                warning="The stated compensation cannot meet the configured floor.",
                blocking=True,
                confidence=0.98,
            )
        )
    elif salary_min < profile.salary_floor:
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=GateStatus.WARNING,
                evidence=salary_evidence,
                rationale=f"The range overlaps the {floor_phrase}.",
                warning="Only part of the stated compensation range meets the configured floor.",
                confidence=0.95,
            )
        )
    else:
        gates.append(
            GateResult(
                name=GateName.SALARY_FLOOR,
                status=GateStatus.PASS,
                evidence=salary_evidence,
                rationale=f"The stated minimum (${salary_min:,}) meets the {floor_phrase}.",
                confidence=0.98,
            )
        )

    location_text = " ".join(
        filter(None, (corpus.get("location"), corpus.get("remote_status")))
    )
    remote_evidence, _ = _find(
        corpus,
        _REMOTE_RE,
        "remote location",
        sources=("location", "remote_status", "description"),
    )
    location_evidence = remote_evidence
    if not location_evidence and corpus.get("location"):
        location_evidence = EvidencePassage(
            passage=corpus["location"][:320], source="location", label="location"
        )
    normalized_location = _normalized(location_text)
    preferred = bool(normalized_location) and any(
        _normalized(item) in normalized_location
        or normalized_location in _normalized(item)
        for item in profile.preferred_locations
        if _normalized(item) != "remote"
    )
    if remote_evidence or preferred:
        gates.append(
            GateResult(
                name=GateName.LOCATION,
                status=GateStatus.PASS,
                evidence=[location_evidence] if location_evidence else [],
                rationale="The role is remote or in a preferred location.",
                confidence=0.9,
            )
        )
    elif normalized_location:
        gates.append(
            GateResult(
                name=GateName.LOCATION,
                status=GateStatus.FAIL
                if profile.strict_location
                else GateStatus.WARNING,
                evidence=[location_evidence] if location_evidence else [],
                rationale="The listed location is outside the configured preferred locations.",
                warning="Confirm relocation, commute, and cost-of-living implications.",
                blocking=profile.strict_location,
                confidence=0.85,
            )
        )
    else:
        gates.append(
            GateResult(
                name=GateName.LOCATION,
                status=GateStatus.UNKNOWN,
                rationale="No usable location evidence was found.",
                warning="Location and remote policy are missing.",
                confidence=0.3,
            )
        )

    no_billable_evidence, _ = _find(corpus, _NO_BILLABLE_RE, "no billable hours")
    if no_billable_evidence:
        gates.append(
            GateResult(
                name=GateName.BILLABLES,
                status=GateStatus.PASS,
                evidence=[no_billable_evidence],
                rationale="The posting explicitly says there is no billable-hours requirement.",
                confidence=0.94,
            )
        )
    else:
        billable_evidence, _ = _find(corpus, _BILLABLE_RE, "billable hours")
        gates.append(
            _stress_gate(
                GateName.BILLABLES,
                billable_evidence,
                reject=profile.reject_billables,
                found_rationale="The posting contains billable-hours language.",
                warning="Billable-hour work is a recurring workload/stress risk.",
            )
        )

    no_quota_evidence, _ = _find(corpus, _NO_QUOTA_RE, "no quota")
    if no_quota_evidence:
        gates.append(
            GateResult(
                name=GateName.QUOTA,
                status=GateStatus.PASS,
                evidence=[no_quota_evidence],
                rationale="The posting explicitly says the role has no quota.",
                confidence=0.94,
            )
        )
    else:
        quota_evidence, _ = _find(corpus, _QUOTA_RE, "quota or OTE")
        gates.append(
            _stress_gate(
                GateName.QUOTA,
                quota_evidence,
                reject=profile.reject_quota,
                found_rationale="The posting contains quota, revenue-target, or OTE language.",
                warning="Clarify base pay, quota ownership, and attainment expectations.",
            )
        )

    travel_evidence, travel_match = _find(corpus, _TRAVEL_RE, "travel requirement")
    if travel_evidence and travel_match:
        percent = (
            int(travel_match.group("percent"))
            if travel_match.group("percent")
            else None
        )
        excessive = profile.max_travel_percent is not None and (
            (percent is not None and percent > profile.max_travel_percent)
            or (percent is None and bool(travel_match.group("level")))
        )
        gates.append(
            GateResult(
                name=GateName.TRAVEL,
                status=GateStatus.FAIL if excessive else GateStatus.WARNING,
                evidence=[travel_evidence],
                rationale="The travel requirement exceeds the configured tolerance."
                if excessive
                else "The posting includes travel that should be clarified.",
                warning="Travel may increase workload volatility and reduce stress-adjusted compensation.",
                blocking=excessive,
                confidence=0.92 if percent is not None else 0.8,
            )
        )
    else:
        gates.append(
            GateResult(
                name=GateName.TRAVEL,
                status=GateStatus.NOT_APPLICABLE,
                rationale="No explicit travel requirement was found.",
                confidence=0.65,
            )
        )

    no_on_call_evidence, _ = _find(corpus, _NO_ON_CALL_RE, "no on-call work")
    if no_on_call_evidence:
        gates.append(
            GateResult(
                name=GateName.ON_CALL,
                status=GateStatus.PASS,
                evidence=[no_on_call_evidence],
                rationale="The posting explicitly says the role has no on-call work.",
                confidence=0.94,
            )
        )
    else:
        on_call_evidence, _ = _find(corpus, _ON_CALL_RE, "on-call work")
        gates.append(
            _stress_gate(
                GateName.ON_CALL,
                on_call_evidence,
                reject=profile.reject_on_call,
                found_rationale="The posting contains on-call or after-hours language.",
                warning="On-call or after-hours work is a workload-volatility risk.",
            )
        )
    return gates


def _stress_gate(
    name: GateName,
    evidence: EvidencePassage | None,
    *,
    reject: bool,
    found_rationale: str,
    warning: str,
) -> GateResult:
    if evidence:
        return GateResult(
            name=name,
            status=GateStatus.FAIL if reject else GateStatus.WARNING,
            evidence=[evidence],
            rationale=found_rationale,
            warning=warning,
            blocking=reject,
            confidence=0.92,
        )
    return GateResult(
        name=name,
        status=GateStatus.NOT_APPLICABLE,
        rationale=f"No explicit {name.value.replace('_', ' ')} language was found.",
        confidence=0.65,
    )


def _clip_score(value: float) -> float:
    return round(max(1.0, min(5.0, value)), 2)


def _compensation_component(
    profile: RankingProfile,
    salary_min: int | None,
    salary_max: int | None,
    evidence: list[EvidencePassage],
    automatic_skip: bool,
    salary_policy_reliable: bool,
    salary_policy_warning: str | None,
) -> ScoreComponent:
    missing = None
    if automatic_skip:
        score = 1.0
        rationale = (
            "Explicit unpaid-work language sets compensation to the minimum score."
        )
        confidence = 0.99
    elif salary_min is None or salary_max is None or not salary_policy_reliable:
        score = 3.0
        rationale = (
            salary_policy_warning
            or "Compensation is unknown, so the neutral midpoint is used rather than assuming favorable pay."
        )
        confidence = 0.3
        missing = salary_policy_warning or "No reliable salary range was found."
    else:
        midpoint = (salary_min + salary_max) / 2
        if profile.salary_floor:
            if salary_max < profile.salary_floor:
                score = 1.0
            elif salary_min >= profile.salary_floor * 1.75:
                score = 5.0
            elif midpoint >= profile.salary_floor * 1.4:
                score = 4.75
            elif salary_min >= profile.salary_floor:
                score = 4.25
            elif midpoint >= profile.salary_floor:
                score = 3.5
            else:
                score = 2.0
            rationale = (
                f"The ${salary_min:,}--${salary_max:,} range is scored against "
                f"the {_salary_floor_phrase(profile)}."
            )
        else:
            score = (
                5.0
                if midpoint >= 200_000
                else 4.5
                if midpoint >= 140_000
                else 4.0
                if midpoint >= 100_000
                else 3.25
                if midpoint >= 75_000
                else 2.5
                if midpoint >= 50_000
                else 1.5
            )
            rationale = (
                f"The stated annualized range is ${salary_min:,}--${salary_max:,}."
            )
        confidence = 0.96
    return ScoreComponent(
        dimension=ScoreDimension.COMPENSATION,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.COMPENSATION],
        evidence=evidence,
        missing_data_warning=missing,
        confidence=confidence,
        rationale=rationale,
    )


def _workload_component(
    corpus: Mapping[str, str], gates: Sequence[GateResult]
) -> ScoreComponent:
    evidence: list[EvidencePassage] = []
    score = 4.0
    signals = 0
    penalties = {
        GateName.BILLABLES: 2.0,
        GateName.QUOTA: 1.2,
        GateName.TRAVEL: 0.8,
        GateName.ON_CALL: 1.5,
    }
    for gate in gates:
        if (
            gate.name in penalties
            and gate.evidence
            and gate.status in {GateStatus.FAIL, GateStatus.WARNING}
        ):
            score -= penalties[gate.name]
            evidence.extend(gate.evidence)
            signals += 1
        elif (
            gate.name in penalties and gate.evidence and gate.status == GateStatus.PASS
        ):
            score += 0.15
            evidence.extend(gate.evidence)
            signals += 1
    stress_evidence, _ = _find(corpus, _HIGH_STRESS_RE, "workload stress")
    if stress_evidence:
        score -= 0.7
        evidence.append(stress_evidence)
        signals += 1
    balance_evidence, _ = _find(corpus, _LOW_STRESS_RE, "workload stability")
    if balance_evidence:
        score += 0.6
        evidence.append(balance_evidence)
        signals += 1
    missing = None
    if not signals:
        score = 3.0
        missing = "No reliable workload, hours, billables, quota, travel, or on-call evidence was found."
        rationale = "Workload is unknown, so the neutral midpoint is used."
        confidence = 0.38
    else:
        rationale = "Explicit workload benefits and stress traps were applied as deterministic adjustments."
        confidence = min(0.95, 0.65 + 0.08 * signals)
    return ScoreComponent(
        dimension=ScoreDimension.WORKLOAD_STRESS,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.WORKLOAD_STRESS],
        evidence=_unique_evidence(evidence),
        missing_data_warning=missing,
        confidence=confidence,
        rationale=rationale,
    )


def _fit_component(
    corpus: Mapping[str, str], profile: RankingProfile
) -> tuple[ScoreComponent, set[str]]:
    evidence: list[EvidencePassage] = []
    matched_assets: set[str] = set()
    for asset, terms in profile.edge_assets.items():
        for term in terms:
            if passage := _find_literal(corpus, term, f"fit: {asset}"):
                matched_assets.add(asset)
                evidence.append(passage)
                break
    count = len(matched_assets)
    score = {0: 1.5, 1: 2.5, 2: 3.5, 3: 4.25}.get(count, 5.0)
    missing = (
        None
        if count
        else "No configured candidate-edge asset was found in the posting."
    )
    rationale = (
        f"The posting uses {count} candidate-edge asset categor{'y' if count == 1 else 'ies'}: "
        + (", ".join(sorted(matched_assets)) if matched_assets else "none")
        + "."
    )
    return ScoreComponent(
        dimension=ScoreDimension.FIT,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.FIT],
        evidence=_unique_evidence(evidence),
        missing_data_warning=missing,
        confidence=0.85 if count >= 2 else 0.7 if count == 1 else 0.35,
        rationale=rationale,
    ), matched_assets


def _gate_component(gates: Sequence[GateResult]) -> ScoreComponent:
    failures = [gate for gate in gates if gate.status == GateStatus.FAIL]
    warnings = [gate for gate in gates if gate.status == GateStatus.WARNING]
    unknown_core = [
        gate
        for gate in gates
        if gate.status == GateStatus.UNKNOWN
        and gate.name
        in {
            GateName.UNPAID_WORK,
            GateName.BAR_ADMISSION,
            GateName.EXPERIENCE_YEARS,
            GateName.WORK_AUTHORIZATION,
            GateName.SALARY_FLOOR,
            GateName.LOCATION,
        }
    ]
    score = 5.0 - 1.25 * len(failures) - 0.5 * len(warnings) - 0.15 * len(unknown_core)
    evidence = _unique_evidence([item for gate in gates for item in gate.evidence])
    missing = None
    if unknown_core:
        missing = (
            "Unresolved gates: "
            + ", ".join(gate.name.value for gate in unknown_core)
            + "."
        )
    rationale = f"{len(failures)} failed, {len(warnings)} warning, and {len(unknown_core)} unresolved core gates."
    known_confidences = [
        gate.confidence for gate in gates if gate.status != GateStatus.NOT_APPLICABLE
    ]
    confidence = (
        sum(known_confidences) / len(known_confidences) if known_confidences else 0.5
    )
    return ScoreComponent(
        dimension=ScoreDimension.GATE_PASSABILITY,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.GATE_PASSABILITY],
        evidence=evidence,
        missing_data_warning=missing,
        confidence=round(confidence, 3),
        rationale=rationale,
    )


def _strategic_component(
    corpus: Mapping[str, str], matched_assets: set[str]
) -> ScoreComponent:
    evidence: list[EvidencePassage] = []
    matched: set[str] = set()
    for category, terms in _STRATEGIC_TERMS.items():
        for term in terms:
            if passage := _find_literal(
                corpus, term, f"strategic optionality: {category}"
            ):
                matched.add(category)
                evidence.append(passage)
                break
    score = 2.0 + 0.55 * len(matched) + 0.2 * min(3, len(matched_assets))
    missing = (
        None
        if matched
        else "No explicit ownership, upside, research, product, or ecosystem signal was found."
    )
    rationale = (
        f"Strategic signals found: {', '.join(sorted(matched)) if matched else 'none'}."
    )
    return ScoreComponent(
        dimension=ScoreDimension.STRATEGIC_OPTIONALITY,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.STRATEGIC_OPTIONALITY],
        evidence=_unique_evidence(evidence),
        missing_data_warning=missing,
        confidence=0.78 if matched else 0.35,
        rationale=rationale,
    )


def _location_component(
    corpus: Mapping[str, str], profile: RankingProfile
) -> ScoreComponent:
    evidence: list[EvidencePassage] = []
    remote, _ = _find(
        corpus,
        _REMOTE_RE,
        "remote",
        sources=("location", "remote_status", "description"),
    )
    hybrid, _ = _find(
        corpus,
        _HYBRID_RE,
        "hybrid",
        sources=("location", "remote_status", "description"),
    )
    onsite, _ = _find(
        corpus,
        _ONSITE_RE,
        "on-site",
        sources=("location", "remote_status", "description"),
    )
    for item in (remote, hybrid, onsite):
        if item:
            evidence.append(item)
    location = corpus.get("location", "")
    if location:
        evidence.append(
            EvidencePassage(passage=location[:320], source="location", label="location")
        )
    folded = _normalized(location)
    preferred = bool(folded) and any(
        _normalized(item) in folded or folded in _normalized(item)
        for item in profile.preferred_locations
        if _normalized(item) != "remote"
    )
    high_col = any(term in location.casefold() for term in _HIGH_COL_TERMS)
    if remote:
        score, rationale, confidence, missing = (
            5.0,
            "The role has explicit remote evidence.",
            0.92,
            None,
        )
    elif hybrid and preferred:
        score, rationale, confidence, missing = (
            4.5,
            "The role is hybrid in a preferred location.",
            0.9,
            None,
        )
    elif preferred:
        score, rationale, confidence, missing = (
            4.0,
            "The role is in a configured preferred location.",
            0.88,
            None,
        )
    elif location:
        score = 2.0 if high_col and (onsite or not hybrid) else 2.75
        rationale = "The location is outside the preferred list" + (
            " and carries a high-COL penalty." if high_col else "."
        )
        confidence, missing = 0.82, None
    else:
        score, rationale, confidence = (
            3.0,
            "Location/COL is unknown, so the neutral midpoint is used.",
            0.3,
        )
        missing = "No reliable location or remote-policy data was found."
    return ScoreComponent(
        dimension=ScoreDimension.LOCATION_COL,
        score=_clip_score(score),
        weight=SCORE_WEIGHTS[ScoreDimension.LOCATION_COL],
        evidence=_unique_evidence(evidence),
        missing_data_warning=missing,
        confidence=confidence,
        rationale=rationale,
    )


def _unique_evidence(evidence: Sequence[EvidencePassage]) -> list[EvidencePassage]:
    seen: set[tuple[str, str, str | None]] = set()
    result: list[EvidencePassage] = []
    for item in evidence:
        key = (item.source, item.passage, item.label)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def _warning_list(
    components: Mapping[ScoreDimension, ScoreComponent], gates: Sequence[GateResult]
) -> list[str]:
    warnings: list[str] = []
    for component in components.values():
        if component.missing_data_warning:
            warnings.append(component.missing_data_warning)
    for gate in gates:
        if gate.warning:
            warnings.append(gate.warning)
    return list(dict.fromkeys(warnings))


def _coerce_manual_override(
    value: ManualOverride | Mapping[str, Any] | float | int | None,
) -> ManualOverride | None:
    if value is None:
        return None
    if isinstance(value, ManualOverride):
        return value
    if isinstance(value, (int, float)):
        return ManualOverride(score=float(value))
    return ManualOverride.model_validate(value)


def _preserved_result(existing: Any) -> JobEvaluationResult:
    if isinstance(existing, JobEvaluationResult):
        return existing.model_copy(
            update={
                "rescored": False,
                "preserved_manual_override": True,
                "manual_override": True,
                "locked": True,
                "method": "manual-preserved",
            }
        )

    components: dict[ScoreDimension, ScoreComponent] = {}
    raw_components = _object_value(existing, "components", default={}) or {}
    if isinstance(raw_components, Mapping):
        for raw_name, payload in raw_components.items():
            try:
                component = ScoreComponent.model_validate(payload)
                components[ScoreDimension(str(raw_name))] = component
            except (TypeError, ValueError):
                continue
    gates: list[GateResult] = []
    for payload in _object_value(existing, "gates", default=[]) or []:
        try:
            gates.append(GateResult.model_validate(payload))
        except (TypeError, ValueError):
            continue
    score = float(_object_value(existing, "score", "final_score", default=3.0))
    automatic_skip = bool(_object_value(existing, "automatic_skip", default=False))
    return JobEvaluationResult(
        score=score,
        components=components,
        gates=gates,
        warnings=list(_object_value(existing, "warnings", default=[]) or []),
        explanation=_object_value(existing, "explanation", default=None)
        or "Locked manual evaluation preserved without automatic rescoring.",
        confidence=float(_object_value(existing, "confidence", default=1.0)),
        automatic_skip=automatic_skip,
        skip_reason="Unpaid work" if automatic_skip else None,
        ranking_eligible=not automatic_skip,
        manual_override=True,
        locked=True,
        rescored=False,
        preserved_manual_override=True,
        method="manual-preserved",
    )


def _contextualized_profile(
    profile: RankingProfile,
    facts: JobFacts,
    corpus: Mapping[str, str],
) -> RankingProfile:
    """Select one personal salary policy without mutating the caller's profile.

    A non-zero legacy/global floor is always authoritative.  Contextual policy
    then uses the deliberately ordered precedence federal, Bay Area, New York
    City, legal AI, and finally private sector.  Federal classification relies
    on an explicit civil-service signal or a recognized agency name; merely
    serving federal clients must not turn a private role into a federal one.
    """

    if profile.salary_floor > 0 or not profile.contextual_salary_floors_enabled:
        return profile

    company = _normalized(facts.company or "")
    role_text = _normalized(" ".join((facts.title, facts.description)))
    posting = _normalized(" ".join([facts.company or "", *corpus.values()]))
    location = _normalized(
        " ".join(filter(None, (facts.location, facts.remote_status)))
    )
    if _FEDERAL_ROLE_RE.search(role_text) or _FEDERAL_COMPANY_RE.search(company):
        floor, context = profile.federal_salary_floor, "federal"
    elif _BAY_AREA_LOCATION_RE.search(location):
        floor, context = profile.bay_area_salary_floor, "Bay Area"
    elif _NYC_LOCATION_RE.search(location):
        floor, context = profile.nyc_salary_floor, "New York City"
    elif _LEGAL_AI_ROLE_RE.search(posting):
        floor, context = profile.legal_ai_salary_floor, "legal AI"
    else:
        floor, context = profile.private_salary_floor, "private-sector"
    return profile.model_copy(
        update={"salary_floor": floor, "salary_floor_context": context}
    )


def evaluate_job(
    job: JobFacts | Mapping[str, Any] | Any,
    profile: RankingProfile | Mapping[str, Any] | Any | None = None,
    *,
    existing_evaluation: JobEvaluationResult | Mapping[str, Any] | Any | None = None,
    unlock_manual_override: bool = False,
    manual_override: ManualOverride | Mapping[str, Any] | float | int | None = None,
) -> JobEvaluationResult:
    """Return a deterministic, explainable 1--5 evaluation.

    Supplying a locked manual ``existing_evaluation`` prevents automatic
    rescoring.  The caller must set ``unlock_manual_override=True`` to replace
    it.  A new explicit ``manual_override`` retains the deterministic evidence
    and components while replacing the aggregate score.
    """

    if (
        existing_evaluation is not None
        and bool(_object_value(existing_evaluation, "manual_override", default=False))
        and bool(_object_value(existing_evaluation, "locked", default=False))
        and not unlock_manual_override
        and manual_override is None
    ):
        return _preserved_result(existing_evaluation)

    facts = _coerce_job(job)
    ranking_profile = _coerce_profile(profile)
    corpus = _corpus(facts)
    ranking_profile = _contextualized_profile(ranking_profile, facts, corpus)
    salary_min, salary_max, salary_evidence = _salary_facts(facts, corpus)
    salary_policy_reliable, salary_policy_warning = _salary_policy_reliability(
        facts, corpus
    )
    gates = _extract_gates(
        facts,
        ranking_profile,
        corpus,
        salary_min,
        salary_max,
        salary_evidence,
        salary_policy_reliable,
        salary_policy_warning,
    )
    automatic_skip = any(
        gate.name == GateName.UNPAID_WORK and gate.status == GateStatus.FAIL
        for gate in gates
    )

    compensation = _compensation_component(
        ranking_profile,
        salary_min,
        salary_max,
        salary_evidence,
        automatic_skip,
        salary_policy_reliable,
        salary_policy_warning,
    )
    workload = _workload_component(corpus, gates)
    fit, matched_assets = _fit_component(corpus, ranking_profile)
    gate_passability = _gate_component(gates)
    strategic = _strategic_component(corpus, matched_assets)
    location = _location_component(corpus, ranking_profile)
    components = {
        component.dimension: component
        for component in (
            compensation,
            workload,
            fit,
            gate_passability,
            strategic,
            location,
        )
    }
    score = round(
        sum(component.score * component.weight for component in components.values()), 2
    )
    confidence = round(
        sum(
            component.confidence * component.weight for component in components.values()
        ),
        3,
    )
    warnings = list(
        dict.fromkeys(
            [*_warning_list(components, gates), *ranking_profile.profile_warnings]
        )
    )
    blocking = [gate.name.value for gate in gates if gate.blocking]
    explanation = (
        f"Stress-adjusted score {score:.2f}/5.00 from deterministic evidence. "
        + (
            f"Blocking gates: {', '.join(blocking)}. "
            if blocking
            else "No blocking gate was found. "
        )
        + (
            "The role is automatically skipped because it is unpaid."
            if automatic_skip
            else "Missing facts remain warnings rather than assumed passes."
        )
    )
    if ranking_profile.profile_evidence:
        approved_fact_count = len(
            {item.profile_fact_id for item in ranking_profile.profile_evidence}
        )
        explanation += (
            f" Candidate inputs use {approved_fact_count} approved profile fact(s); "
            "their immutable IDs and content hashes are attached as provenance."
        )
    elif ranking_profile.profile_warnings:
        explanation += " No approved candidate fact was used to assume a gate pass."

    override = _coerce_manual_override(manual_override)
    if override:
        score = override.score
        explanation = (
            override.explanation
            or f"Manual score {score:.2f}/5.00; deterministic components retained for provenance."
        )

    return JobEvaluationResult(
        score=score,
        components=components,
        gates=gates,
        warnings=warnings,
        explanation=explanation,
        confidence=confidence,
        automatic_skip=automatic_skip,
        skip_reason="Explicit unpaid-work language" if automatic_skip else None,
        ranking_eligible=not automatic_skip,
        manual_override=override is not None,
        locked=override.locked if override else False,
        rescored=True,
        preserved_manual_override=False,
        method="manual-with-deterministic-evidence" if override else RANKER_VERSION,
        facts=facts,
        profile_evidence=ranking_profile.profile_evidence,
    )


def _serialized_components(result: JobEvaluationResult) -> dict[str, Any]:
    return {
        dimension.value: component.model_dump(mode="json")
        for dimension, component in result.components.items()
    }


def _serialized_evidence(result: JobEvaluationResult) -> list[dict[str, Any]]:
    evidence = _unique_evidence(
        [
            passage
            for component in result.components.values()
            for passage in component.evidence
        ]
    )
    serialized = [item.model_dump(mode="json") for item in evidence]
    serialized.extend(
        {
            "kind": "approved_profile_fact",
            **item.model_dump(mode="json"),
        }
        for item in result.profile_evidence
    )
    return serialized


def _canonical_hash(payload: Any) -> str:
    """Hash a JSON contract without depending on mapping insertion order."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def evaluation_fingerprint(
    job: JobFacts | Mapping[str, Any] | Any,
    profile: RankingProfile | Mapping[str, Any] | Any | None = None,
) -> str:
    """Return the semantic identity of an automatic ranking decision.

    The contract intentionally includes both ranker inputs and acquisition
    identity.  A posting can therefore be rescanned indefinitely without
    producing redundant evaluations, while a meaningful posting, policy, or
    semantic-ranker change creates a new decision record.
    """

    ranking_profile = _coerce_profile(profile)
    job_payload = {
        "id": _object_value(job, "id"),
        "company_id": _object_value(job, "company_id"),
        "location_id": _object_value(job, "location_id"),
        "title": _object_value(job, "title", default=""),
        "normalized_title": _object_value(job, "normalized_title"),
        "description": _object_value(job, "description", default=""),
        "description_hash": _object_value(job, "description_hash"),
        "launch_url": _object_value(job, "launch_url", "canonical_url"),
        "comparison_url": _object_value(job, "comparison_url", "canonical_url"),
        "compensation_text": _object_value(job, "compensation_text"),
        "salary_min": _object_value(job, "salary_min"),
        "salary_max": _object_value(job, "salary_max"),
        "salary_currency": _object_value(job, "salary_currency", default="UNK"),
        "compensation_period": _object_value(
            job, "compensation_period", default="unknown"
        ),
        "compensation_confidence": _object_value(
            job, "compensation_confidence", default=0.0
        ),
        "remote_status": _object_value(job, "remote_status"),
        "deadline": _object_value(job, "deadline"),
        "category": _object_value(job, "category"),
    }
    return _canonical_hash(
        {
            "contract": "jobby-evaluation-fingerprint-v1",
            "ranker_version": RANKER_VERSION,
            "job": job_payload,
            "profile": ranking_profile.model_dump(mode="json"),
        }
    )


def _evaluation_payload_hash(result: JobEvaluationResult) -> str:
    return _canonical_hash(
        {
            "contract": "jobby-evaluation-payload-v1",
            "ranker_version": RANKER_VERSION,
            "score": result.score,
            "components": _serialized_components(result),
            "gates": [gate.model_dump(mode="json") for gate in result.gates],
            "evidence": _serialized_evidence(result),
            "warnings": list(result.warnings),
            "explanation": result.explanation,
            "confidence": result.confidence,
            "automatic_skip": result.automatic_skip,
            "manual_override": result.manual_override,
            "locked": result.locked,
            "method": result.method,
        }
    )


def _persistence_fingerprint(
    persisted_job: Any,
    result: JobEvaluationResult,
    profile: RankingProfile | Mapping[str, Any] | Any | None,
) -> str:
    """Bind evaluated facts to stable acquisition identity for storage reuse."""

    return _canonical_hash(
        {
            "contract": "jobby-persisted-evaluation-fingerprint-v1",
            "decision_inputs": evaluation_fingerprint(
                result.facts or persisted_job, profile
            ),
            "job_id": _object_value(persisted_job, "id"),
            "company_id": _object_value(persisted_job, "company_id"),
            "location_id": _object_value(persisted_job, "location_id"),
            "description_hash": _object_value(persisted_job, "description_hash"),
            "launch_url": _object_value(persisted_job, "launch_url", "canonical_url"),
            "comparison_url": _object_value(
                persisted_job, "comparison_url", "canonical_url"
            ),
        }
    )


def persist_evaluation(
    database: Any,
    job: Any,
    evaluation: JobEvaluationResult | Mapping[str, Any] | None = None,
    *,
    profile: RankingProfile | Mapping[str, Any] | Any | None = None,
    unlock_manual_override: bool = False,
    force_rerun: bool = False,
    ai_run_id: str | None = None,
    reference_key: str | None = None,
) -> Any:
    """Persist a result and make it the current evaluation for ``job``.

    ``database`` may be :class:`jobby.db.Database` or an existing SQLAlchemy
    ``Session``.  A current locked manual evaluation is returned untouched when
    an automatic result is offered, unless ``unlock_manual_override`` is true.
    The function returns the ORM :class:`jobby.models.Evaluation` row.
    """

    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session

    from .models import Evaluation, Job

    job_id = _object_value(job, "id") if not isinstance(job, str) else job
    if not job_id:
        raise ValueError("job must be a persisted Job or job ID")

    if isinstance(database, Session):
        session_context = nullcontext(database)
    elif hasattr(database, "session"):
        session_context = database.session()
    else:
        raise TypeError("database must be a jobby.db.Database or SQLAlchemy Session")

    with session_context as session:
        persisted_job = session.get(Job, str(job_id))
        if persisted_job is None:
            raise ValueError(f"job {job_id!r} is not present in the database")
        current = session.scalars(
            select(Evaluation)
            .where(Evaluation.job_id == str(job_id), Evaluation.is_current.is_(True))
            .order_by(Evaluation.created_at.desc())
        ).first()

        explicit_manual = (
            bool(_object_value(evaluation, "manual_override", default=False))
            if evaluation is not None
            else False
        )
        if (
            current
            and current.manual_override
            and current.locked
            and not unlock_manual_override
            and not explicit_manual
        ):
            return current

        if evaluation is None:
            result = evaluate_job(
                persisted_job,
                profile,
                existing_evaluation=current,
                unlock_manual_override=unlock_manual_override,
            )
            if result.preserved_manual_override and current:
                return current
        elif isinstance(evaluation, JobEvaluationResult):
            result = evaluation
        else:
            result = JobEvaluationResult.model_validate(evaluation)

        automatic_reusable = (
            not result.manual_override
            and not force_rerun
            and ai_run_id is None
            and reference_key is None
        )
        fingerprint = (
            _persistence_fingerprint(persisted_job, result, profile)
            if automatic_reusable
            else None
        )
        if fingerprint is not None:
            matching = session.scalars(
                select(Evaluation)
                .where(
                    Evaluation.job_id == str(job_id),
                    Evaluation.fingerprint == fingerprint,
                    Evaluation.evaluation_kind == "automatic",
                )
                .order_by(Evaluation.created_at.desc())
            ).first()
            if matching is not None:
                if not matching.is_current:
                    for old_evaluation in session.scalars(
                        select(Evaluation).where(
                            Evaluation.job_id == str(job_id),
                            Evaluation.is_current.is_(True),
                        )
                    ):
                        old_evaluation.is_current = False
                    session.flush()
                    matching.is_current = True
                persisted_job.latest_score = matching.score
                return matching

        for old_evaluation in session.scalars(
            select(Evaluation).where(
                Evaluation.job_id == str(job_id), Evaluation.is_current.is_(True)
            )
        ):
            old_evaluation.is_current = False
        # Flush retirements before inserting the replacement so the partial
        # unique index remains valid under SQLite's statement-level checks.
        session.flush()

        if result.manual_override:
            evaluation_kind = "manual"
            fingerprint = None
        elif ai_run_id is not None:
            evaluation_kind = "ai_linked"
            fingerprint = None
        elif reference_key is not None:
            evaluation_kind = "referenced"
            fingerprint = None
        elif force_rerun:
            evaluation_kind = "forced"
            fingerprint = None
        else:
            evaluation_kind = "automatic"

        row = Evaluation(
            job_id=str(job_id),
            score=result.score,
            components=_serialized_components(result),
            gates=[gate.model_dump(mode="json") for gate in result.gates],
            evidence=_serialized_evidence(result),
            warnings=list(result.warnings),
            explanation=result.explanation,
            confidence=result.confidence,
            automatic_skip=result.automatic_skip,
            manual_override=result.manual_override,
            locked=result.locked,
            is_current=True,
            ai_run_id=ai_run_id,
            fingerprint=fingerprint,
            payload_hash=_evaluation_payload_hash(result),
            ranker_version=RANKER_VERSION,
            evaluation_kind=evaluation_kind,
            reference_key=reference_key,
        )
        try:
            # A savepoint lets concurrent automatic evaluations converge on
            # the unique (job, fingerprint) row without rolling back unrelated
            # work in a caller-owned session.
            with session.begin_nested():
                session.add(row)
                session.flush()
        except IntegrityError:
            if fingerprint is None:
                raise
            matching = session.scalars(
                select(Evaluation).where(
                    Evaluation.job_id == str(job_id),
                    Evaluation.fingerprint == fingerprint,
                    Evaluation.evaluation_kind == "automatic",
                )
            ).first()
            if matching is None:
                raise
            matching.is_current = True
            persisted_job.latest_score = matching.score
            return matching
        persisted_job.latest_score = result.score
        from .audit import record_audit

        record_audit(
            session,
            action="job.evaluated",
            entity_type="evaluation",
            entity_id=row.id,
            actor="user" if result.manual_override else "deterministic_ranker",
            after={
                "job_id": str(job_id),
                "score": result.score,
                "method": result.method,
                "ranker_version": RANKER_VERSION,
                "fingerprint": fingerprint,
                "automatic_skip": result.automatic_skip,
                "manual_override": result.manual_override,
            },
        )
        return row


def evaluate_and_persist(
    database: Any,
    job: Any,
    profile: RankingProfile | Mapping[str, Any] | Any | None = None,
    *,
    unlock_manual_override: bool = False,
) -> Any:
    """Convenience wrapper for callers that want evaluation plus persistence."""

    return persist_evaluation(
        database,
        job,
        profile=profile,
        unlock_manual_override=unlock_manual_override,
    )


# Descriptive aliases keep the persistence boundary discoverable without
# requiring callers to know the exact verb chosen by this module.
save_evaluation = persist_evaluation
persist_job_evaluation = persist_evaluation


__all__ = [
    "CandidateProfile",
    "EvidencePassage",
    "EvaluationResult",
    "GateName",
    "GateResult",
    "GateStatus",
    "JobEvaluation",
    "JobEvaluationResult",
    "JobFacts",
    "ManualOverride",
    "ProfileFactEvidence",
    "RankingProfile",
    "RANKER_VERSION",
    "SCORE_WEIGHTS",
    "ScoreComponent",
    "ScoreDimension",
    "Subscore",
    "WEIGHTS",
    "evaluate_and_persist",
    "evaluation_fingerprint",
    "evaluate_job",
    "persist_evaluation",
    "persist_job_evaluation",
    "ranking_profile_from_config",
    "ranking_profile_from_database",
    "save_evaluation",
]
