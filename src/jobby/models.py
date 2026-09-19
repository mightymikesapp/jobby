"""SQLAlchemy persistence model for Jobby's local operational database."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    DDL,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    event,
    text,
    update,
)
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .enums import (
    AgentRunStatus,
    AlertSeverity,
    ApplicationStage,
    ApprovalState,
    ArtifactKind,
    DocumentStatus,
    ImportReviewStatus,
    JobStatus,
    SuggestionKind,
    TaskStatus,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class UTCDateTime(TypeDecorator[datetime]):
    """Persist UTC and always return timezone-aware values, including on SQLite."""

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def process_result_value(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def enum_type(enum_cls: type[Enum], name: str) -> SAEnum:
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=False,
        values_callable=lambda members: [item.value for item in members],
        validate_strings=True,
    )


class Base(DeclarativeBase):
    pass


class IdentityMixin:
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )


class Company(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "companies"

    name: Mapped[str] = mapped_column(String(300), nullable=False)
    normalized_name: Mapped[str] = mapped_column(
        String(300), nullable=False, unique=True, index=True
    )
    website: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)


class Location(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "locations"

    display_name: Mapped[str] = mapped_column(String(500), nullable=False)
    normalized_key: Mapped[str] = mapped_column(
        String(500), nullable=False, unique=True, index=True
    )
    city: Mapped[str | None] = mapped_column(String(200))
    region: Mapped[str | None] = mapped_column(String(200))
    country: Mapped[str | None] = mapped_column(String(100))
    remote: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class Job(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("source_primary", "source_id", name="uq_job_source_id"),
        Index("ix_jobs_status_score", "status", "latest_score"),
    )

    company_id: Mapped[str] = mapped_column(
        ForeignKey("companies.id", ondelete="RESTRICT"), index=True
    )
    location_id: Mapped[str | None] = mapped_column(
        ForeignKey("locations.id", ondelete="SET NULL"), index=True
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    normalized_title: Mapped[str] = mapped_column(
        String(500), nullable=False, index=True
    )
    canonical_url: Mapped[str | None] = mapped_column(Text)
    source_primary: Mapped[str | None] = mapped_column(String(80), index=True)
    source_id: Mapped[str | None] = mapped_column(String(500), index=True)
    description: Mapped[str | None] = mapped_column(Text)
    description_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    status: Mapped[JobStatus] = mapped_column(
        enum_type(JobStatus, "job_status"),
        default=JobStatus.DISCOVERED,
        nullable=False,
        index=True,
    )
    remote_status: Mapped[str | None] = mapped_column(String(40))
    compensation_text: Mapped[str | None] = mapped_column(Text)
    salary_min: Mapped[int | None] = mapped_column(Integer)
    salary_max: Mapped[int | None] = mapped_column(Integer)
    salary_currency: Mapped[str] = mapped_column(
        String(3), default="USD", nullable=False
    )
    posted_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    deadline: Mapped[date | None] = mapped_column(Date, index=True)
    discovered_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    last_seen_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    closed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    consecutive_misses: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    explicit_closure: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    liveness_known: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    latest_score: Mapped[float | None] = mapped_column(Float, index=True)
    category: Mapped[str | None] = mapped_column(String(100), index=True)
    notes: Mapped[str | None] = mapped_column(Text)
    manual_status_locked: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    # Explicit mixin overrides pin legacy column order ahead of release-added
    # columns so fresh metadata matches SQLite ALTER-based upgrades.
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    # Release 0.2 migration-added fields remain last to preserve byte-for-byte
    # SQLite schema equivalence between fresh and sequential databases.
    # ``launch_url`` is exact; ``comparison_url`` is lossy and match-only.
    launch_url: Mapped[str | None] = mapped_column(Text)
    comparison_url: Mapped[str | None] = mapped_column(Text, index=True)
    compensation_period: Mapped[str] = mapped_column(
        String(20), default="unknown", server_default=text("'unknown'"), nullable=False
    )
    compensation_confidence: Mapped[float] = mapped_column(
        Float, default=0.0, server_default=text("0"), nullable=False
    )
    compensation_evidence: Mapped[str | None] = mapped_column(Text)
    # Release 0.4 migration-added field follows all 0.2 additions.
    authoritative_fields: Mapped[list[str]] = mapped_column(
        JSON, default=list, server_default=text("'[]'"), nullable=False
    )


class SearchIndexState(Base):
    """Singleton generation counter for the disposable search cache."""

    __tablename__ = "search_index_state"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_search_index_state_singleton"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)


class SearchIndexChange(Base):
    """Idempotent generation-numbered work queued by SQLite triggers."""

    __tablename__ = "search_index_changes"
    __table_args__ = (Index("ix_search_index_changes_generation", "generation"),)

    job_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, primary_key=True)


class ScanRun(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "scan_runs"

    status: Mapped[AgentRunStatus] = mapped_column(
        enum_type(AgentRunStatus, "scan_run_status"),
        default=AgentRunStatus.QUEUED,
        nullable=False,
    )
    query: Mapped[str | None] = mapped_column(Text)
    requested_sources: Mapped[list[str]] = mapped_column(
        JSON, default=list, nullable=False
    )
    source_results: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    discovered_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(Text)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    # Release 0.3 migration-added fields remain last for schema equivalence.
    scan_kind: Mapped[str] = mapped_column(
        String(40),
        default="manual",
        server_default=text("'manual'"),
        nullable=False,
        index=True,
    )
    profile_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "scan_profiles.id",
            name="fk_scan_runs_profile_id_scan_profiles",
            ondelete="SET NULL",
        ),
        index=True,
    )
    fts_sync_status: Mapped[str | None] = mapped_column(String(40))
    fts_sync_error: Mapped[str | None] = mapped_column(Text)


class SourceObservation(IdentityMixin, Base):
    __tablename__ = "source_observations"
    __table_args__ = (
        UniqueConstraint(
            "scan_run_id",
            "source",
            "source_job_id",
            name="uq_observation_run_source_job",
        ),
        Index("ix_observation_job_observed", "job_id", "observed_at"),
    )

    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    import_key: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    scan_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("scan_runs.id", ondelete="SET NULL"), index=True
    )
    source: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    source_account: Mapped[str | None] = mapped_column(String(200), index=True)
    source_job_id: Mapped[str | None] = mapped_column(String(500))
    source_url: Mapped[str | None] = mapped_column(Text)
    title_snapshot: Mapped[str | None] = mapped_column(String(500))
    company_snapshot: Mapped[str | None] = mapped_column(String(500))
    location_snapshot: Mapped[str | None] = mapped_column(String(500))
    observed_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    is_live: Mapped[bool | None] = mapped_column(Boolean)
    closure_evidence: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    # Release 0.3 migration-added fields.
    snapshot_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    observation_kind: Mapped[str] = mapped_column(
        String(40), default="content", server_default=text("'content'"), nullable=False
    )


class DuplicateRelationship(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "duplicate_relationships"
    __table_args__ = (
        UniqueConstraint("job_id", "duplicate_job_id", name="uq_duplicate_pair"),
    )

    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    duplicate_job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    rule: Mapped[str] = mapped_column(String(80), nullable=False)
    similarity: Mapped[float | None] = mapped_column(Float)
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    resolution: Mapped[str] = mapped_column(
        String(20),
        default="pending",
        server_default=text("'pending'"),
        nullable=False,
        index=True,
    )
    comparison_identity: Mapped[str | None] = mapped_column(String(64), index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    canonical_group_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "canonical_job_groups.id",
            name="fk_duplicate_relationships_group",
            ondelete="SET NULL",
        ),
        index=True,
    )


class Evaluation(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "evaluations"
    __table_args__ = (
        Index(
            "ix_evaluation_job_current",
            "job_id",
            unique=True,
            sqlite_where=text("is_current = 1"),
        ),
        Index(
            "ix_evaluation_job_fingerprint",
            "job_id",
            "fingerprint",
            unique=True,
            sqlite_where=text(
                "fingerprint IS NOT NULL AND evaluation_kind = 'automatic'"
            ),
        ),
    )

    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    score: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    components: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    gates: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, nullable=False
    )
    evidence: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, nullable=False
    )
    warnings: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    explanation: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    automatic_skip: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    manual_override: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    locked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    ai_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_runs.id", ondelete="SET NULL")
    )
    import_key: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    fingerprint: Mapped[str | None] = mapped_column(String(64), index=True)
    payload_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    ranker_version: Mapped[str] = mapped_column(
        String(100),
        default="deterministic-v1",
        server_default=text("'deterministic-v1'"),
        nullable=False,
    )
    evaluation_kind: Mapped[str] = mapped_column(
        String(40),
        default="automatic",
        server_default=text("'automatic'"),
        nullable=False,
    )
    reference_key: Mapped[str | None] = mapped_column(String(200), index=True)


class Application(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "applications"

    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"), index=True
    )
    current_stage: Mapped[ApplicationStage] = mapped_column(
        enum_type(ApplicationStage, "application_stage"),
        default=ApplicationStage.PLANNED,
        nullable=False,
        index=True,
    )
    submission_channel: Mapped[str | None] = mapped_column(String(200))
    submitted_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    follow_up_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    import_key: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    applied_evaluation_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "evaluations.id",
            name="fk_applications_applied_evaluation",
            ondelete="SET NULL",
        ),
        index=True,
    )
    applied_score: Mapped[float | None] = mapped_column(Float)
    applied_ranker_version: Mapped[str | None] = mapped_column(String(100))
    applied_sources: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, server_default=text("'[]'"), nullable=False
    )


class StageEvent(IdentityMixin, Base):
    __tablename__ = "stage_events"
    __table_args__ = (
        Index("ix_stage_events_application_time", "application_id", "occurred_at"),
    )

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    from_stage: Mapped[ApplicationStage | None] = mapped_column(
        enum_type(ApplicationStage, "stage_event_from")
    )
    to_stage: Mapped[ApplicationStage] = mapped_column(
        enum_type(ApplicationStage, "stage_event_to"), nullable=False
    )
    occurred_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    reason: Mapped[str | None] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(String(100), default="user", nullable=False)
    source: Mapped[str] = mapped_column(String(100), default="manual", nullable=False)


class Task(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "tasks"
    __table_args__ = (Index("ix_tasks_status_due", "status", "due_at"),)

    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[TaskStatus] = mapped_column(
        enum_type(TaskStatus, "task_status"), default=TaskStatus.PENDING, nullable=False
    )
    due_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    job_id: Mapped[str | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    application_id: Mapped[str | None] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    automation_key: Mapped[str | None] = mapped_column(
        String(200), unique=True, index=True
    )


class Contact(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "contacts"

    company_id: Mapped[str | None] = mapped_column(
        ForeignKey("companies.id", ondelete="SET NULL"), index=True
    )
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    email: Mapped[str | None] = mapped_column(String(500), index=True)
    title: Mapped[str | None] = mapped_column(String(300))
    linkedin_url: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)


class Interview(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "interviews"

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    starts_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, index=True
    )
    ends_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    interview_type: Mapped[str | None] = mapped_column(String(100))
    location_or_link: Mapped[str | None] = mapped_column(Text)
    contact_id: Mapped[str | None] = mapped_column(
        ForeignKey("contacts.id", ondelete="SET NULL")
    )
    notes: Mapped[str | None] = mapped_column(Text)
    calendar_event_id: Mapped[str | None] = mapped_column(String(500), unique=True)


class DocumentVersion(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "document_versions"
    __table_args__ = (Index("ix_documents_kind_canonical", "kind", "is_canonical"),)

    kind: Mapped[ArtifactKind] = mapped_column(
        enum_type(ArtifactKind, "document_kind"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    parent_id: Mapped[str | None] = mapped_column(
        ForeignKey("document_versions.id", ondelete="SET NULL")
    )
    job_id: Mapped[str | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL"), index=True
    )
    content_markdown: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[DocumentStatus] = mapped_column(
        enum_type(DocumentStatus, "document_status"),
        default=DocumentStatus.SOURCE,
        nullable=False,
    )
    approval_state: Mapped[ApprovalState] = mapped_column(
        enum_type(ApprovalState, "document_approval"),
        default=ApprovalState.PENDING,
        nullable=False,
    )
    is_canonical: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, nullable=False
    )
    diff_data: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, nullable=False
    )
    validation: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    ai_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_runs.id", ondelete="SET NULL")
    )


class ApplicationMaterial(IdentityMixin, Base):
    """Immutable record of a specific document version used for an application."""

    __tablename__ = "application_materials"
    __table_args__ = (
        UniqueConstraint(
            "application_id",
            "document_version_id",
            "purpose",
            name="uq_application_material_purpose",
        ),
    )

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    document_version_id: Mapped[str] = mapped_column(
        ForeignKey("document_versions.id", ondelete="RESTRICT"), index=True
    )
    purpose: Mapped[str] = mapped_column(String(100), nullable=False)
    used_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class Offer(IdentityMixin, TimestampMixin, Base):
    """A durable offer snapshot linked to the application that produced it."""

    __tablename__ = "offers"
    __table_args__ = (
        Index("ix_offers_application_decision", "application_id", "decision"),
    )

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    base_salary: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    annual_bonus: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    annualized_equity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="USD", nullable=False)
    cost_of_living_index: Mapped[float] = mapped_column(
        Float, default=100.0, nullable=False
    )
    stress_score: Mapped[float] = mapped_column(Float, default=3.0, nullable=False)
    terms: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    decision: Mapped[str | None] = mapped_column(String(100), index=True)
    offered_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class Artifact(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        UniqueConstraint(
            "workspace_root",
            "source_path",
            "content_hash",
            name="uq_artifact_source_version",
        ),
    )

    kind: Mapped[ArtifactKind] = mapped_column(
        enum_type(ArtifactKind, "artifact_kind"), nullable=False, index=True
    )
    workspace_root: Mapped[str | None] = mapped_column(Text)
    source_path: Mapped[str | None] = mapped_column(Text)
    stored_path: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    size_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(200))
    source_mtime_ns: Mapped[int | None] = mapped_column(Integer)
    source_immutable: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False
    )
    document_version_id: Mapped[str | None] = mapped_column(
        ForeignKey("document_versions.id", ondelete="SET NULL"), index=True
    )
    job_id: Mapped[str | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL"), index=True
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )


class AIRun(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "ai_runs"

    purpose: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String(80), default="openai", nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    cached_tokens: Mapped[int | None] = mapped_column(Integer)
    output_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    approval_state: Mapped[ApprovalState] = mapped_column(
        enum_type(ApprovalState, "ai_approval"),
        default=ApprovalState.PENDING,
        nullable=False,
    )
    approved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    error: Mapped[str | None] = mapped_column(Text)
    # ``cached_tokens`` above is provider-reported prompt caching.  A local
    # guarded extraction-cache hit is tracked separately and never presented as
    # billable usage.  Keep these migration-added fields last for exact SQLite
    # schema equivalence with upgraded databases.
    cache_hit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    cache_entry_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_cache_entries.id", ondelete="SET NULL"), index=True
    )
    source_ai_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_runs.id", ondelete="SET NULL"), index=True
    )


class AICacheEntry(IdentityMixin, TimestampMixin, Base):
    """Validated, expiring output for an explicitly cacheable AI purpose."""

    __tablename__ = "ai_cache_entries"
    __table_args__ = (
        UniqueConstraint("cache_key", name="uq_ai_cache_key"),
        UniqueConstraint(
            "provider",
            "model",
            "purpose",
            "prompt_version",
            "output_schema_hash",
            "request_hash",
            "max_output_tokens",
            name="uq_ai_cache_identity",
        ),
        Index("ix_ai_cache_active_expiry", "active", "expires_at"),
    )

    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    purpose: Mapped[str] = mapped_column(String(100), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    output_schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # SQLite treats NULL values as distinct in UNIQUE constraints.  Zero is the
    # canonical representation of an unspecified output limit.
    max_output_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False)
    output_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    source_ai_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_runs.id", ondelete="SET NULL"), index=True
    )
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    invalidated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class Citation(IdentityMixin, Base):
    __tablename__ = "citations"

    ai_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_runs.id", ondelete="CASCADE"), index=True
    )
    scan_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("scan_runs.id", ondelete="CASCADE"), index=True
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    quoted_text: Mapped[str | None] = mapped_column(Text)
    start_index: Mapped[int | None] = mapped_column(Integer)
    end_index: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class Alert(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "alerts"

    severity: Mapped[AlertSeverity] = mapped_column(
        enum_type(AlertSeverity, "alert_severity"),
        default=AlertSeverity.INFO,
        nullable=False,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[str | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    application_id: Mapped[str | None] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE")
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )
    fingerprint: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    recurrence_count: Mapped[int] = mapped_column(
        Integer, default=1, server_default=text("1"), nullable=False
    )
    last_recurred_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    snoozed_until: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    resolution_reason: Mapped[str | None] = mapped_column(Text)
    entity_type: Mapped[str | None] = mapped_column(String(100), index=True)
    entity_id: Mapped[str | None] = mapped_column(String(100), index=True)


class AgentRun(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "agent_runs"

    status: Mapped[AgentRunStatus] = mapped_column(
        enum_type(AgentRunStatus, "agent_run_status"),
        default=AgentRunStatus.QUEUED,
        nullable=False,
        index=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    scan_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("scan_runs.id", ondelete="SET NULL")
    )
    summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)


class AuditEvent(IdentityMixin, Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_entity_time", "entity_type", "entity_id", "occurred_at"),
    )

    occurred_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    actor: Mapped[str] = mapped_column(String(100), default="user", nullable=False)
    action: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(100))
    before_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    after_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    detail: Mapped[str | None] = mapped_column(Text)
    correlation_id: Mapped[str | None] = mapped_column(String(100), index=True)


class MutationApproval(IdentityMixin, TimestampMixin, Base):
    """One-time approval bound to an exact non-human mutation payload."""

    __tablename__ = "mutation_approvals"
    __table_args__ = (
        Index("ix_mutation_approvals_actor_status", "actor", "status"),
        Index("ix_mutation_approvals_expires_at", "expires_at"),
    )

    action: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    actor: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    approved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    consumed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class OperationRun(IdentityMixin, TimestampMixin, Base):
    """Durable status/result envelope for background facade operations."""

    __tablename__ = "operation_runs"
    __table_args__ = (
        Index("ix_operation_runs_status_created", "status", "created_at"),
    )

    kind: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    request_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    result_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class ImportReview(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "import_reviews"
    __table_args__ = (
        UniqueConstraint(
            "workspace_root",
            "source_path",
            "record_key",
            name="uq_import_review_record",
        ),
    )

    workspace_root: Mapped[str] = mapped_column(Text, nullable=False)
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    record_key: Mapped[str] = mapped_column(String(500), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    raw_excerpt: Mapped[str | None] = mapped_column(Text)
    proposed_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    status: Mapped[ImportReviewStatus] = mapped_column(
        enum_type(ImportReviewStatus, "import_review_status"),
        default=ImportReviewStatus.PENDING,
        nullable=False,
    )


class EmailMessage(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "email_messages"

    provider_message_id: Mapped[str] = mapped_column(
        String(500), unique=True, nullable=False
    )
    sender: Mapped[str | None] = mapped_column(Text)
    subject: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    snippet: Mapped[str | None] = mapped_column(Text)
    application_id: Mapped[str | None] = mapped_column(
        ForeignKey("applications.id", ondelete="SET NULL"), index=True
    )
    full_body_retrieved: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )


class ExternalSuggestion(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "external_suggestions"

    kind: Mapped[SuggestionKind] = mapped_column(
        enum_type(SuggestionKind, "suggestion_kind"), nullable=False
    )
    application_id: Mapped[str | None] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    email_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("email_messages.id", ondelete="CASCADE")
    )
    external_event_id: Mapped[str | None] = mapped_column(String(500))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    approval_state: Mapped[ApprovalState] = mapped_column(
        enum_type(ApprovalState, "suggestion_approval"),
        default=ApprovalState.PENDING,
        nullable=False,
    )
    applied_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class IntegrationState(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "integration_states"

    provider: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    account_hint: Mapped[str | None] = mapped_column(String(300))
    scopes: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    last_sync_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    health: Mapped[str | None] = mapped_column(String(100))


class LegacySeenIdentifier(IdentityMixin, Base):
    __tablename__ = "legacy_seen_identifiers"
    __table_args__ = (
        UniqueConstraint(
            "workspace_root", "source_uid", name="uq_legacy_seen_workspace_uid"
        ),
    )

    workspace_root: Mapped[str] = mapped_column(Text, nullable=False)
    source_uid: Mapped[str] = mapped_column(String(700), nullable=False, index=True)
    imported_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    legacy_last_run: Mapped[str | None] = mapped_column(String(100))


class LegacyMetric(IdentityMixin, Base):
    __tablename__ = "legacy_metrics"

    import_key: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    rubric: Mapped[str] = mapped_column(String(100), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(200), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    scale_min: Mapped[float] = mapped_column(Float, nullable=False)
    scale_max: Mapped[float] = mapped_column(Float, nullable=False)
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    raw_text: Mapped[str | None] = mapped_column(Text)
    imported_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class SourceConfig(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "source_configs"

    import_key: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    provider: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )


class ProfileFact(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "profile_facts"

    fact_key: Mapped[str] = mapped_column(
        String(500), unique=True, nullable=False, index=True
    )
    value_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    approved: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, index=True
    )
    approved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    source_artifact_id: Mapped[str | None] = mapped_column(
        ForeignKey("artifacts.id", ondelete="SET NULL")
    )
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class EvaluationCompactionBatch(IdentityMixin, TimestampMixin, Base):
    """Auditable manifest for one explicit duplicate-evaluation compaction."""

    __tablename__ = "evaluation_compaction_batches"

    manifest_hash: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    backup_path: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False)
    removed_count: Mapped[int] = mapped_column(Integer, nullable=False)
    retained_count: Mapped[int] = mapped_column(Integer, nullable=False)


class EvaluationCompactionLedger(IdentityMixin, Base):
    """Append-only tombstone retaining identity for one removed exact duplicate."""

    __tablename__ = "evaluation_compaction_ledger"
    __table_args__ = (
        UniqueConstraint(
            "batch_id", "removed_evaluation_id", name="uq_compaction_removed_eval"
        ),
    )

    batch_id: Mapped[str] = mapped_column(
        ForeignKey("evaluation_compaction_batches.id", ondelete="RESTRICT"), index=True
    )
    removed_evaluation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    retained_evaluation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    removed_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class ScanProfile(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "scan_profiles"

    name: Mapped[str] = mapped_column(String(300), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    source_selectors: Mapped[list[str]] = mapped_column(
        JSON, default=list, nullable=False
    )
    query_pack: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    location_filters: Mapped[list[str]] = mapped_column(
        JSON, default=list, nullable=False
    )
    role_filters: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    hydration_policy: Mapped[str] = mapped_column(
        String(40), default="focused", nullable=False
    )
    enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, index=True
    )


class SavedDiscoveryView(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "saved_discovery_views"

    name: Mapped[str] = mapped_column(String(300), unique=True, nullable=False)
    filters_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    sort_key: Mapped[str] = mapped_column(
        String(80), default="score_high", nullable=False
    )


class DiscoveryReviewCursor(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "discovery_review_cursors"
    __table_args__ = (
        UniqueConstraint("view_id", name="uq_discovery_review_cursor_view"),
    )

    view_id: Mapped[str] = mapped_column(
        ForeignKey("saved_discovery_views.id", ondelete="CASCADE"), index=True
    )
    reviewed_through: Mapped[datetime | None] = mapped_column(UTCDateTime())
    reviewed_job_id: Mapped[str | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL")
    )


class SourceRun(IdentityMixin, Base):
    __tablename__ = "source_runs"
    __table_args__ = (Index("ix_source_runs_source_attempt", "source", "attempted_at"),)

    scan_run_id: Mapped[str] = mapped_column(
        ForeignKey("scan_runs.id", ondelete="CASCADE"), index=True
    )
    source: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    attempted_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    succeeded_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    duration_seconds: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    result_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    reported_total: Mapped[int | None] = mapped_column(Integer)
    retries: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    complete: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    failure_class: Mapped[str | None] = mapped_column(String(100), index=True)
    anomaly_state: Mapped[str] = mapped_column(
        String(40), default="healthy", nullable=False, index=True
    )
    failure_streak: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    detail: Mapped[str | None] = mapped_column(Text)


class SourceHealth(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "source_health"

    source: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    last_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_success_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_complete_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_result_count: Mapped[int | None] = mapped_column(Integer)
    last_reported_total: Mapped[int | None] = mapped_column(Integer)
    failure_streak: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_failure_class: Mapped[str | None] = mapped_column(String(100))
    anomaly_state: Mapped[str] = mapped_column(
        String(40), default="unknown", nullable=False, index=True
    )


class JobSourceState(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "job_source_states"
    __table_args__ = (
        UniqueConstraint(
            "source", "source_job_id", name="uq_job_source_state_identity"
        ),
        Index("ix_job_source_state_job_seen", "job_id", "last_seen_at"),
    )

    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    source: Mapped[str] = mapped_column(String(200), nullable=False)
    source_job_id: Mapped[str] = mapped_column(String(500), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    seen_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    last_content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    last_snapshot_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    last_source_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_runs.id", ondelete="SET NULL"), index=True
    )
    is_live: Mapped[bool | None] = mapped_column(Boolean)


class ApplicationContact(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "application_contacts"
    __table_args__ = (
        UniqueConstraint("application_id", "contact_id", name="uq_application_contact"),
    )

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), index=True
    )
    contact_id: Mapped[str] = mapped_column(
        ForeignKey("contacts.id", ondelete="CASCADE"), index=True
    )
    role: Mapped[str | None] = mapped_column(String(200))


class CanonicalJobGroup(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "canonical_job_groups"

    canonical_job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"), unique=True, nullable=False
    )
    notes: Mapped[str | None] = mapped_column(Text)


class CanonicalJobMember(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "canonical_job_members"
    __table_args__ = (
        UniqueConstraint("job_id", name="uq_canonical_job_member_job"),
        Index("ix_canonical_job_member_group", "group_id", "is_canonical"),
    )

    group_id: Mapped[str] = mapped_column(
        ForeignKey("canonical_job_groups.id", ondelete="CASCADE"), index=True
    )
    job_id: Mapped[str] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"), index=True
    )
    is_canonical: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    hidden_by_default: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False
    )


class BackupRecord(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "backup_records"

    backup_kind: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    plaintext_sha256: Mapped[str] = mapped_column(
        String(64), nullable=False, index=True
    )
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    external: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    recovery_tested_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class MaintenanceRun(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "maintenance_runs"
    __table_args__ = (Index("ix_maintenance_kind_started", "kind", "started_at"),)

    kind: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    result_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, nullable=False
    )
    error: Mapped[str | None] = mapped_column(Text)


class InterviewQuestion(IdentityMixin, TimestampMixin, Base):
    """Reusable, structured interview question and evidence prompt."""

    __tablename__ = "interview_questions"
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    role_focus: Mapped[str | None] = mapped_column(String(300), index=True)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    skills: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    evidence_keys: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class InterviewSession(IdentityMixin, TimestampMixin, Base):
    """One preparation or retrospective session for an application."""

    __tablename__ = "interview_sessions"
    __table_args__ = (
        Index("ix_interview_sessions_application", "application_id", "started_at"),
    )

    application_id: Mapped[str] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE")
    )
    interview_id: Mapped[str | None] = mapped_column(
        ForeignKey("interviews.id", ondelete="SET NULL"), index=True
    )
    session_type: Mapped[str] = mapped_column(String(80), nullable=False)
    role_focus: Mapped[str | None] = mapped_column(String(300))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)
    retrospective: Mapped[str | None] = mapped_column(Text)
    outcome: Mapped[str | None] = mapped_column(String(100))
    follow_up_task_id: Mapped[str | None] = mapped_column(
        ForeignKey("tasks.id", ondelete="SET NULL"), index=True
    )


class InterviewAnswer(IdentityMixin, TimestampMixin, Base):
    """An answer/evidence record captured during an interview session."""

    __tablename__ = "interview_answers"
    __table_args__ = (
        UniqueConstraint(
            "session_id", "question_id", name="uq_interview_answer_question"
        ),
    )

    session_id: Mapped[str] = mapped_column(
        ForeignKey("interview_sessions.id", ondelete="CASCADE"), index=True
    )
    question_id: Mapped[str | None] = mapped_column(
        ForeignKey("interview_questions.id", ondelete="SET NULL"), index=True
    )
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_refs: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    rating: Mapped[int | None] = mapped_column(Integer)


class CompanyCandidate(IdentityMixin, TimestampMixin, Base):
    """Evidence-bound company discovery candidate awaiting a decision."""

    __tablename__ = "company_candidates"
    __table_args__ = (
        Index("ix_company_candidates_decision", "decision", "discovered_at"),
    )

    company_id: Mapped[str | None] = mapped_column(
        ForeignKey("companies.id", ondelete="SET NULL"), index=True
    )
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    website: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    role_filter: Mapped[str | None] = mapped_column(String(300))
    location_filter: Mapped[str | None] = mapped_column(String(300))
    industry_filter: Mapped[str | None] = mapped_column(String(300))
    evidence: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, default=list, nullable=False
    )
    score: Mapped[float | None] = mapped_column(Float)
    decision: Mapped[str] = mapped_column(
        String(40), default="pending", server_default=text("'pending'"), nullable=False
    )
    discovered_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )


class CompanyWatchlistEntry(IdentityMixin, TimestampMixin, Base):
    """Approved recurring company watchlist decision."""

    __tablename__ = "company_watchlist"
    __table_args__ = (
        UniqueConstraint("company_id", name="uq_company_watchlist_company"),
        Index("ix_company_watchlist_enabled", "enabled"),
    )

    company_id: Mapped[str] = mapped_column(
        ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    criteria: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    cadence_days: Mapped[int] = mapped_column(Integer, default=7, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_scanned_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


IMMUTABLE_MODELS = (
    StageEvent,
    SourceObservation,
    Citation,
    AuditEvent,
    ApplicationMaterial,
    EvaluationCompactionLedger,
)

IMMUTABLE_APPROVED_DOCUMENT_FIELDS = (
    "kind",
    "name",
    "version",
    "parent_id",
    "job_id",
    "content_markdown",
    "content_hash",
    "provenance",
    "diff_data",
    "validation",
    "ai_run_id",
)

# Imported source artifacts may be linked to jobs and document versions after
# registration, but their provenance identity must never be rewritten in place.
# A changed legacy file is represented by another Artifact row instead.
IMMUTABLE_ARTIFACT_FIELDS = (
    "kind",
    "workspace_root",
    "source_path",
    "stored_path",
    "content_hash",
    "size_bytes",
    "mime_type",
    "source_mtime_ns",
    "source_immutable",
    "metadata_json",
)


def _reject_immutable_change(mapper: Any, connection: Any, target: Any) -> None:
    raise ValueError(f"{type(target).__name__} records are immutable")


def _artifact_was_immutable(target: Artifact) -> bool:
    history = sa_inspect(target).attrs.source_immutable.history
    return bool(target.source_immutable or True in history.deleted)


def _reject_immutable_artifact_update(
    mapper: Any, connection: Any, target: Artifact
) -> None:
    if not _artifact_was_immutable(target):
        return
    state = sa_inspect(target)
    changed = [
        name
        for name in IMMUTABLE_ARTIFACT_FIELDS
        if state.attrs[name].history.has_changes()
    ]
    if changed:
        raise ValueError(f"Artifact source fields are immutable: {', '.join(changed)}")


def _reject_immutable_artifact_delete(
    mapper: Any, connection: Any, target: Artifact
) -> None:
    if _artifact_was_immutable(target):
        raise ValueError("Artifact source records are immutable")


def _document_was_approved(target: DocumentVersion) -> bool:
    state = sa_inspect(target)
    approval_history = state.attrs.approval_state.history
    status_history = state.attrs.status.history
    approval_was_frozen = (
        ApprovalState.APPROVED in approval_history.deleted
        if approval_history.has_changes()
        else target.approval_state == ApprovalState.APPROVED
    )
    status_was_frozen = (
        any(
            value in {DocumentStatus.APPROVED, DocumentStatus.READY}
            for value in status_history.deleted
        )
        if status_history.has_changes()
        else target.status in {DocumentStatus.APPROVED, DocumentStatus.READY}
    )
    return bool(approval_was_frozen or status_was_frozen)


def _reject_approved_document_update(
    mapper: Any, connection: Any, target: DocumentVersion
) -> None:
    if not _document_was_approved(target):
        return
    state = sa_inspect(target)
    changed = [
        name
        for name in IMMUTABLE_APPROVED_DOCUMENT_FIELDS
        if state.attrs[name].history.has_changes()
    ]
    if (
        state.attrs.approval_state.history.has_changes()
        and target.approval_state != ApprovalState.APPROVED
    ):
        changed.append("approval_state")
    if state.attrs.status.history.has_changes() and target.status not in {
        DocumentStatus.APPROVED,
        DocumentStatus.READY,
    }:
        changed.append("status")
    if changed:
        raise ValueError(
            "Approved document content and provenance are immutable: "
            + ", ".join(changed)
        )


def _reject_approved_document_delete(
    mapper: Any, connection: Any, target: DocumentVersion
) -> None:
    if _document_was_approved(target):
        raise ValueError("Approved document versions are immutable")


def _invalidate_ai_cache_after_rejection(
    mapper: Any, connection: Any, target: AIRun
) -> None:
    """A rejected source or reused result may never remain reusable."""

    if target.approval_state != ApprovalState.REJECTED:
        return
    history = sa_inspect(target).attrs.approval_state.history
    if not history.has_changes():
        return
    condition = AICacheEntry.source_ai_run_id == target.id
    if target.cache_entry_id is not None:
        condition = condition | (AICacheEntry.id == target.cache_entry_id)
    connection.execute(
        update(AICacheEntry)
        .where(condition, AICacheEntry.active.is_(True))
        .values(active=False, invalidated_at=utc_now(), updated_at=utc_now())
    )


for _model in IMMUTABLE_MODELS:
    event.listen(_model, "before_update", _reject_immutable_change)
    event.listen(_model, "before_delete", _reject_immutable_change)

event.listen(Artifact, "before_update", _reject_immutable_artifact_update)
event.listen(Artifact, "before_delete", _reject_immutable_artifact_delete)
event.listen(DocumentVersion, "before_update", _reject_approved_document_update)
event.listen(DocumentVersion, "before_delete", _reject_approved_document_delete)


# These triggers are part of the declared operational schema as well as the
# migration.  Registering the identical DDL on metadata keeps fresh
# ``Base.metadata.create_all`` databases equivalent to Alembic-upgraded ones.
SEARCH_INDEX_TRIGGER_SQL = (
    """
    CREATE TRIGGER search_jobs_insert
    AFTER INSERT ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT NEW.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_jobs_update
    AFTER UPDATE OF title, description, category, company_id ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT NEW.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_jobs_delete
    AFTER DELETE ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT OLD.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_companies_rename
    AFTER UPDATE OF name ON companies
    WHEN OLD.name IS NOT NEW.name
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT id, (SELECT generation FROM search_index_state WHERE id = 1)
        FROM jobs WHERE company_id = NEW.id;
    END
    """,
)

event.listen(
    Base.metadata,
    "after_create",
    DDL(
        "INSERT OR IGNORE INTO search_index_state(id, generation) VALUES (1, 0)"
    ).execute_if(dialect="sqlite"),
)
for _statement in SEARCH_INDEX_TRIGGER_SQL:
    event.listen(
        Base.metadata,
        "after_create",
        DDL(_statement).execute_if(dialect="sqlite"),
    )
event.listen(AIRun, "after_update", _invalidate_ai_cache_after_rejection)
