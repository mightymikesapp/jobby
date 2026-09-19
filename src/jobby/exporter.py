"""Portable Markdown, JSON, and CSV exports with no credentials."""

from __future__ import annotations

import csv
from contextlib import contextmanager
import json
import os
import tempfile
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import Connection, func, select
from sqlalchemy.engine import RowMapping

from .audit import json_safe, record_audit, redact_text
from .db import Database
from .enums import ArtifactKind
from .models import (
    AICacheEntry,
    AIRun,
    AgentRun,
    Alert,
    Application,
    ApplicationContact,
    ApplicationMaterial,
    Artifact,
    AuditEvent,
    BackupRecord,
    CanonicalJobGroup,
    CanonicalJobMember,
    Citation,
    Company,
    Contact,
    CompanyCandidate,
    CompanyWatchlistEntry,
    DiscoveryReviewCursor,
    DocumentVersion,
    DuplicateRelationship,
    EmailMessage,
    Evaluation,
    EvaluationCompactionBatch,
    EvaluationCompactionLedger,
    ExternalSuggestion,
    ImportReview,
    IntegrationState,
    Interview,
    InterviewAnswer,
    InterviewQuestion,
    InterviewSession,
    Job,
    JobSourceState,
    LegacyMetric,
    LegacySeenIdentifier,
    Location,
    MaintenanceRun,
    MutationApproval,
    Offer,
    OperationRun,
    ProfileFact,
    SavedDiscoveryView,
    ScanProfile,
    ScanRun,
    SearchIndexChange,
    SearchIndexState,
    SourceConfig,
    SourceHealth,
    SourceObservation,
    SourceRun,
    StageEvent,
    Task,
)


EXPORT_MODELS = (
    Company,
    Location,
    Job,
    ScanRun,
    SourceObservation,
    DuplicateRelationship,
    Evaluation,
    Application,
    StageEvent,
    Task,
    Interview,
    InterviewAnswer,
    InterviewQuestion,
    InterviewSession,
    Contact,
    CompanyCandidate,
    CompanyWatchlistEntry,
    DocumentVersion,
    ApplicationMaterial,
    Offer,
    Artifact,
    AIRun,
    AICacheEntry,
    Citation,
    Alert,
    AgentRun,
    AuditEvent,
    ImportReview,
    EmailMessage,
    ExternalSuggestion,
    IntegrationState,
    LegacySeenIdentifier,
    LegacyMetric,
    SourceConfig,
    ProfileFact,
    SearchIndexState,
    SearchIndexChange,
    EvaluationCompactionBatch,
    EvaluationCompactionLedger,
    ScanProfile,
    SavedDiscoveryView,
    DiscoveryReviewCursor,
    SourceRun,
    SourceHealth,
    JobSourceState,
    ApplicationContact,
    CanonicalJobGroup,
    CanonicalJobMember,
    BackupRecord,
    MaintenanceRun,
    MutationApproval,
    OperationRun,
)
EXPORT_BATCH_SIZE = 1_000
CSV_FIELDS = (
    "job_id",
    "company",
    "title",
    "status",
    "application_stage",
    "score",
    "salary_min",
    "salary_max",
    "location_id",
    "remote_status",
    "deadline",
    "source",
    "url",
)


def export_data(database: Database, format_name: str, output: Path | str) -> Path:
    if not isinstance(format_name, str):
        raise ValueError("format must be markdown, json, or csv")
    normalized = format_name.casefold()
    if normalized not in {"json", "csv", "markdown", "md"}:
        raise ValueError("format must be markdown, json, or csv")
    requested_output = Path(output).expanduser().absolute()
    requested_output.parent.mkdir(parents=True, exist_ok=True)
    # Resolve the containing directory, but deliberately not the final path:
    # replacing a final-component symlink is safe, while an unresolved parent
    # symlink could disguise the live database or another managed artifact.
    output = requested_output.parent.resolve(strict=True) / requested_output.name
    protected_database_paths = {
        _path_identity(database.path),
        _path_identity(Path(f"{database.path}-wal")),
        _path_identity(Path(f"{database.path}-shm")),
        _path_identity(database.paths.config_file),
    }
    if _path_identity(output) in protected_database_paths:
        raise ValueError("export output must not overwrite the operational database")
    if output.exists() and output.is_dir():
        raise ValueError("export output must be a file")
    with database.session() as session:
        registered_artifacts = _registered_artifacts_at(
            session,
            output,
            aliases=(requested_output,),
        )
    if any(item.source_immutable for item in registered_artifacts):
        raise ValueError(
            "export output must not overwrite an immutable source artifact"
        )
    if any(
        item.kind != ArtifactKind.GENERATED_EXPORT
        or item.source_immutable
        or item.document_version_id is not None
        or not item.metadata_json.get("portable")
        for item in registered_artifacts
    ):
        raise ValueError("export output must not overwrite another managed artifact")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        if normalized == "json":
            _export_json(database, temporary)
        elif normalized == "csv":
            _export_csv(database, temporary)
        else:
            _export_markdown(database, temporary)
        temporary.chmod(0o600)
        sync_descriptor = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(sync_descriptor)
        finally:
            os.close(sync_descriptor)
        os.replace(temporary, output)
        _fsync_directory(output.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    digest = _hash_file(output)
    with database.session() as session:
        existing_artifacts = list(
            session.scalars(
                select(Artifact)
                .where(
                    Artifact.stored_path == str(output),
                    Artifact.kind == ArtifactKind.GENERATED_EXPORT,
                    Artifact.source_immutable.is_(False),
                )
                .order_by(Artifact.created_at, Artifact.id)
            )
        )
        if existing_artifacts:
            existing = existing_artifacts[0]
            for artifact in existing_artifacts:
                artifact.content_hash = digest
                artifact.size_bytes = output.stat().st_size
                artifact.mime_type = _export_mime_type(normalized)
                artifact.metadata_json = {"format": normalized, "portable": True}
        else:
            existing = Artifact(
                kind=ArtifactKind.GENERATED_EXPORT,
                stored_path=str(output),
                content_hash=digest,
                size_bytes=output.stat().st_size,
                mime_type=_export_mime_type(normalized),
                source_immutable=False,
                metadata_json={"format": normalized, "portable": True},
            )
            session.add(existing)
            session.flush()
        record_audit(
            session,
            action="export.created",
            entity_type="artifact",
            entity_id=existing.id,
            actor="user",
            after={"format": normalized, "path": str(output), "content_hash": digest},
        )
    return output


def _path_identity(path: Path | str) -> Path:
    """Normalize a path's parent without following its final component."""

    candidate = Path(path).expanduser().absolute()
    return candidate.parent.resolve(strict=False) / candidate.name


def _registered_artifacts_at(
    session: Any,
    output: Path,
    *,
    aliases: tuple[Path, ...] = (),
) -> list[Artifact]:
    """Find managed paths even when a stored parent uses a symlink alias."""

    identities = {_path_identity(output), *(_path_identity(path) for path in aliases)}
    matching_ids: list[str] = []
    rows = session.execute(
        select(Artifact.id, Artifact.stored_path).execution_options(yield_per=500)
    )
    for artifact_id, stored_path in rows:
        try:
            matches = _path_identity(stored_path) in identities
        except (OSError, RuntimeError, ValueError):
            matches = False
        if matches:
            matching_ids.append(str(artifact_id))
    if not matching_ids:
        return []
    return list(session.scalars(select(Artifact).where(Artifact.id.in_(matching_ids))))


def _export_mime_type(format_name: str) -> str:
    return {
        "json": "application/json",
        "csv": "text/csv",
        "markdown": "text/markdown",
        "md": "text/markdown",
    }[format_name]


def _export_json(database: Database, output: Path) -> None:
    exported_at = datetime.now().astimezone().isoformat()
    with (
        output.open("w", encoding="utf-8") as handle,
        _consistent_read(database) as connection,
    ):
        handle.write("{\n")
        handle.write('  "schema": "jobby-export-v1",\n')
        handle.write(f'  "exported_at": {json.dumps(exported_at)}')
        for model in EXPORT_MODELS:
            primary_key = list(model.__table__.primary_key)
            statement = (
                select(model.__table__)
                .order_by(*primary_key)
                .execution_options(yield_per=EXPORT_BATCH_SIZE)
            )
            handle.write(f",\n  {json.dumps(model.__tablename__)}: [")
            first = True
            for row in connection.execute(statement).mappings():
                handle.write("\n    " if first else ",\n    ")
                handle.write(
                    json.dumps(
                        _mapping_row(row),
                        ensure_ascii=False,
                        separators=(",", ": "),
                    )
                )
                first = False
            if not first:
                handle.write("\n  ")
            handle.write("]")
        handle.write("\n}\n")


def _export_csv(database: Database, output: Path) -> None:
    latest_applications = _latest_applications()
    statement = (
        select(
            Job.id.label("job_id"),
            Company.name.label("company"),
            Job.title.label("title"),
            Job.status.label("status"),
            latest_applications.c.current_stage.label("application_stage"),
            Evaluation.score.label("score"),
            Job.salary_min.label("salary_min"),
            Job.salary_max.label("salary_max"),
            Job.location_id.label("location_id"),
            Job.remote_status.label("remote_status"),
            Job.deadline.label("deadline"),
            Job.source_primary.label("source"),
            func.coalesce(Job.launch_url, Job.canonical_url).label("url"),
        )
        .join(Company, Company.id == Job.company_id)
        .outerjoin(
            Evaluation,
            (Evaluation.job_id == Job.id) & Evaluation.is_current.is_(True),
        )
        .outerjoin(
            latest_applications,
            (latest_applications.c.job_id == Job.id)
            & (latest_applications.c.row_number == 1),
        )
        .order_by(Job.latest_score.desc(), Job.discovered_at.desc(), Job.id)
        .execution_options(yield_per=EXPORT_BATCH_SIZE)
    )
    with (
        output.open("w", encoding="utf-8", newline="") as handle,
        _consistent_read(database) as connection,
    ):
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in connection.execute(statement).mappings():
            writer.writerow(
                {
                    key: _csv_safe(_safe(row[key]) if row[key] is not None else "")
                    for key in CSV_FIELDS
                }
            )


def _export_markdown(database: Database, output: Path) -> None:
    latest_applications = _latest_applications()
    statement = (
        select(
            Job.title,
            Job.status,
            Job.latest_score,
            Job.deadline,
            Job.source_primary,
            Job.launch_url,
            Job.canonical_url,
            Company.name.label("company_name"),
            latest_applications.c.current_stage,
        )
        .join(Company, Company.id == Job.company_id)
        .outerjoin(
            latest_applications,
            (latest_applications.c.job_id == Job.id)
            & (latest_applications.c.row_number == 1),
        )
        .order_by(Job.latest_score.desc(), Job.discovered_at.desc(), Job.id)
        .execution_options(yield_per=EXPORT_BATCH_SIZE)
    )
    with (
        output.open("w", encoding="utf-8") as handle,
        _consistent_read(database) as connection,
    ):
        handle.write("# Jobby Export\n\n")
        handle.write(f"Generated {datetime.now().astimezone().isoformat()}\n\n")
        handle.write("## Jobs\n\n")
        for row in connection.execute(statement).mappings():
            company = _markdown_safe(row["company_name"] or "Unknown")
            score = (
                f"{row['latest_score']:.2f}" if row["latest_score"] is not None else "—"
            )
            stage = row["current_stage"]
            stage_text = _safe(stage) if stage is not None else "not started"
            deadline = row["deadline"]
            source = row["source_primary"] or "legacy/manual"
            url = row["launch_url"] or row["canonical_url"] or "not recorded"
            handle.write(
                f"### {company} — {_markdown_safe(row['title'])}\n\n"
                f"- Score: {score}\n"
                f"- Job status: {_safe(row['status'])}\n"
                f"- Application: {stage_text}\n"
                f"- Deadline: {deadline.isoformat() if deadline else 'unknown'}\n"
                f"- Source: {_markdown_safe(source)}\n"
                f"- URL: {_markdown_safe(url)}\n\n"
            )


def _latest_applications():
    return select(
        Application.job_id,
        Application.current_stage,
        func.row_number()
        .over(
            partition_by=Application.job_id,
            order_by=(Application.updated_at.desc(), Application.id.desc()),
        )
        .label("row_number"),
    ).subquery("latest_applications")


@contextmanager
def _consistent_read(database: Database) -> Iterator[Connection]:
    """Hold one explicit SQLite read transaction for an entire export."""

    with database.engine.connect() as connection:
        connection.exec_driver_sql("BEGIN")
        try:
            # Pin the WAL snapshot before any table cursor is opened.  Without
            # an explicit BEGIN, sqlite3 can end its implicit read transaction
            # when each cursor is exhausted and later tables could see writes
            # from a different point in time.
            connection.exec_driver_sql(
                "SELECT name FROM sqlite_schema ORDER BY name LIMIT 1"
            ).first()
            yield connection
        finally:
            connection.rollback()


def _row(instance: Any) -> dict[str, Any]:
    return {
        column.name: _safe(getattr(instance, column.name))
        for column in instance.__table__.columns
    }


def _mapping_row(row: RowMapping) -> dict[str, Any]:
    return {str(key): _safe(value) for key, value in row.items()}


def _safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return json_safe(value)


def _csv_safe(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = redact_text(value)
    if value.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")):
        return f"'{value}"
    return value


def _markdown_safe(value: str) -> str:
    return redact_text(value).replace("\r", " ").replace("\n", " ")


def _hash_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["EXPORT_BATCH_SIZE", "EXPORT_MODELS", "export_data"]
