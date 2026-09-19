"""Stable domain enumerations stored as lowercase strings in SQLite."""

from enum import StrEnum


class JobStatus(StrEnum):
    DISCOVERED = "discovered"
    SAVED = "saved"
    EVALUATING = "evaluating"
    READY = "ready"
    IGNORED = "ignored"
    STALE = "stale"
    CLOSED = "closed"


class ApplicationStage(StrEnum):
    PLANNED = "planned"
    APPLIED = "applied"
    SCREENING = "screening"
    INTERVIEW = "interview"
    ASSESSMENT = "assessment"
    OFFER = "offer"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"
    ARCHIVED = "archived"


class ArtifactKind(StrEnum):
    JOB_DESCRIPTION = "job_description"
    SOURCE_REPORT = "source_report"
    RESUME = "resume"
    COVER_LETTER = "cover_letter"
    EMAIL = "email"
    INTERVIEW_NOTE = "interview_note"
    GENERATED_EXPORT = "generated_export"
    SOURCE_STATE = "source_state"
    PROFILE = "profile"
    CONFIGURATION = "configuration"
    TRACKER = "tracker"
    TRANSCRIPT = "transcript"
    FORM = "form"
    WRITING_SAMPLE = "writing_sample"
    FOLLOW_UP = "follow_up"
    APPLICATION_PREP = "application_prep"
    OTHER = "other"


class TaskStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    DISMISSED = "dismissed"


class AgentRunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class ApprovalState(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    LEGACY_UNKNOWN = "legacy_unknown"


class DocumentStatus(StrEnum):
    SOURCE = "source"
    PROPOSED = "proposed"
    APPROVED = "approved"
    READY = "ready"
    REJECTED = "rejected"


class ImportReviewStatus(StrEnum):
    PENDING = "pending"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"


class SuggestionKind(StrEnum):
    APPLIED = "applied"
    INTERVIEW_REQUESTED = "interview_requested"
    REJECTED = "rejected"
    POSITION_CLOSED = "position_closed"
    FOLLOW_UP_NEEDED = "follow_up_needed"
    CALENDAR_INTERVIEW = "calendar_interview"


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    URGENT = "urgent"
