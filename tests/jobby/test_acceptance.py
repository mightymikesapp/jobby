"""Offline end-to-end acceptance and secret-isolation coverage."""

from __future__ import annotations

import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import keyring
import pytest
from pypdf import PdfWriter
from sqlalchemy import select

from jobby.analytics import (
    funnel_summary,
    score_calibration,
    source_yield,
    time_in_stage,
)
from jobby.backup import create_backup, verify_backup
from jobby.config import (
    AppConfig,
    JobbyPaths,
    SecretStore,
    redacted_config,
    save_config,
)
from jobby.db import Database
from jobby.documents import DocumentService
from jobby.enums import ApplicationStage, ApprovalState, ArtifactKind, DocumentStatus
from jobby.exporter import export_data
from jobby.google_integration import CalendarProvider, ingest_gmail_metadata
from jobby.importer import LegacyImporter, sha256_file
from jobby.models import (
    Application,
    AuditEvent,
    DocumentVersion,
    EmailMessage,
    Evaluation,
    ExternalSuggestion,
    Interview,
    Job,
)
from jobby.pipeline import (
    apply_external_suggestion,
    attach_application_material,
    create_application,
    create_interview,
    transition_application,
)
from jobby.ranking import RankingProfile, evaluate_and_persist


SECRET_CANARY = "fixture-acceptance-secret-never-persist"


def _paths(tmp_path: Path) -> JobbyPaths:
    root = tmp_path / "jobby-home"
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()


def _write_workspace(workspace: Path) -> dict[str, tuple[str, int]]:
    files = {
        "new_jobs_2026-07-10.md": (
            "## AI Policy Counsel\n"
            "**Example Co** | GH\n\n"
            "Keywords matched: policy, governance\n\n"
            "https://boards.greenhouse.io/example/jobs/123?gh_jid=123\n\n"
            "---\n"
        ),
        "Example Co Job Description.md": (
            "# AI Policy Counsel\n\n"
            "**Company:** Example Co\n"
            "**URL:** https://boards.greenhouse.io/example/jobs/123?gh_jid=123\n\n"
            "Lead responsible AI governance and cross-functional regulatory projects. "
            "This is a full-time remote role paying $150,000 to $180,000. "
            "No billable-hour requirement, quota, travel, or on-call rotation.\n"
        ),
        "Fixture Candidate Resume 2026.md": (
            "# Fixture Candidate\n\n"
            "## Experience\n\n"
            "- Drafted responsible AI policy guidance.\n"
        ),
        "Cover Letter - Example Co.md": (
            "# Fixture Candidate\n\nI am interested in responsible technology policy work.\n"
        ),
    }
    for relative, content in files.items():
        path = workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return {
        relative: (
            sha256_file(workspace / relative),
            (workspace / relative).stat().st_mtime_ns,
        )
        for relative in files
    }


class FakePDFRenderer:
    def __init__(self) -> None:
        self.calls: list[tuple[Path, str]] = []

    def render(self, content: str, output: Path, *, title: str = "") -> Path:
        assert content.lstrip().startswith("<!doctype html>")
        self.calls.append((output, title))
        output.parent.mkdir(parents=True, exist_ok=True)
        writer = PdfWriter()
        writer.add_blank_page(width=612, height=792)
        with output.open("wb") as handle:
            writer.write(handle)
        return output


class MetadataOnlyMailProvider:
    def __init__(self) -> None:
        self.body_calls = 0
        self.send_calls = 0

    def list_metadata(self, *, query: str, limit: int) -> list[dict[str, str]]:
        assert "interview" in query
        assert limit == 100
        return [
            {
                "message_id": "message-1",
                "sender": "recruiter@example.test",
                "subject": "Interview invitation",
                "date": "Sat, 11 Jul 2026 09:00:00 -0700",
                "snippet": "Please schedule your interview with our hiring manager.",
            }
        ]

    def get_body(self, message_id: str) -> str:  # pragma: no cover - must remain unused
        self.body_calls += 1
        raise AssertionError(f"full body unexpectedly fetched: {message_id}")

    def send_message(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
        self.send_calls += 1
        raise AssertionError("Jobby must not send recruiting email")


def _provenance(base: DocumentVersion) -> list[dict[str, str]]:
    return [
        {
            "relationship": "approved_source_document",
            "document_version_id": base.id,
            "content_hash": base.content_hash,
        }
    ]


def test_offline_end_to_end_flow_preserves_approval_boundaries_and_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    database = Database(paths=paths)
    database.initialize()
    workspace = tmp_path / "legacy-workspace"
    source_state = _write_workspace(workspace)

    keyring_write = MagicMock()
    monkeypatch.setattr(keyring, "set_password", keyring_write)
    SecretStore(service="jobby-acceptance-test").set("openai_api_key", SECRET_CANARY)
    keyring_write.assert_called_once_with(
        "jobby-acceptance-test", "openai_api_key", SECRET_CANARY
    )
    config = AppConfig(openai_enabled=False, google_enabled=False)
    save_config(config, paths)
    assert SECRET_CANARY not in json.dumps(redacted_config(config))

    report = LegacyImporter(database, paths=paths).run(workspace)
    assert report.imported_jobs == 1
    assert report.imported_documents == 2
    assert report.errors == []
    assert {
        relative: (
            sha256_file(workspace / relative),
            (workspace / relative).stat().st_mtime_ns,
        )
        for relative in source_state
    } == source_state

    with database.session() as session:
        job = session.scalar(select(Job).where(Job.title == "AI Policy Counsel"))
        assert job is not None
        assert job.description and "responsible AI governance" in job.description
        job_id = job.id
        source_documents = list(
            session.scalars(
                select(DocumentVersion).where(
                    DocumentVersion.kind.in_(
                        [ArtifactKind.RESUME, ArtifactKind.COVER_LETTER]
                    )
                )
            )
        )
        assert {document.kind for document in source_documents} == {
            ArtifactKind.RESUME,
            ArtifactKind.COVER_LETTER,
        }
        source_by_kind = {document.kind: document for document in source_documents}

    evaluation = evaluate_and_persist(
        database,
        job_id,
        RankingProfile(
            salary_floor=120_000,
            years_experience=4,
            work_authorized=True,
            preferred_locations=["remote"],
        ),
    )
    assert 1 <= evaluation.score <= 5
    assert evaluation.components
    assert evaluation.evidence
    assert evaluation.automatic_skip is False

    documents = DocumentService(database, paths=paths)
    renderer = FakePDFRenderer()
    approved_document_ids: list[str] = []
    tailored_content = {
        ArtifactKind.RESUME: (
            "# Fixture Candidate\n\n## Experience\n\n"
            "- Drafted responsible AI policy guidance for cross-functional governance projects.\n"
        ),
        ArtifactKind.COVER_LETTER: (
            "# Fixture Candidate\n\n"
            "I am interested in the AI Policy Counsel role and its responsible AI governance work.\n"
        ),
    }
    for kind in (ArtifactKind.RESUME, ArtifactKind.COVER_LETTER):
        base = source_by_kind[kind]
        proposal = documents.propose(
            base_version_id=base.id,
            content_markdown=tailored_content[kind],
            job_id=job_id,
            provenance=_provenance(base),
        )
        assert proposal.approval_state == ApprovalState.PENDING
        assert proposal.status == DocumentStatus.PROPOSED
        if kind == ArtifactKind.RESUME:
            with pytest.raises(PermissionError, match="explicitly approved"):
                documents.render_all(
                    proposal.id,
                    output_dir=paths.artifacts_dir / "blocked",
                    formats=("markdown",),
                )
        approved = documents.approve(proposal.id)
        outputs = documents.render_all(
            approved.id,
            output_dir=paths.artifacts_dir / "acceptance" / kind.value,
            pdf_renderer=renderer,
        )
        assert set(outputs) == {"markdown", "html", "docx", "pdf"}
        assert all(path.is_file() for path in outputs.values())
        approved_document_ids.append(approved.id)
    assert len(renderer.calls) == 2
    assert not (paths.artifacts_dir / "blocked").exists()

    with database.session() as session:
        application = create_application(
            session,
            job_id,
            submission_channel="company career site (recorded manually)",
        )
        session.flush()
        applied_at = datetime.now(timezone.utc)
        transition_application(
            session,
            application,
            ApplicationStage.APPLIED,
            reason="User confirmed submission",
            occurred_at=applied_at,
        )
        for purpose, document_id in zip(
            ("resume", "cover_letter"), approved_document_ids, strict=True
        ):
            attach_application_material(
                session,
                application,
                document_id,
                purpose=purpose,
            )
        application_id = application.id

    mail = MetadataOnlyMailProvider()
    with patch.object(
        CalendarProvider,
        "list_events",
        autospec=True,
    ) as calendar_read:
        with database.session() as session:
            assert ingest_gmail_metadata(session, mail) == (1, 1)
        with database.session() as session:
            message = session.scalar(
                select(EmailMessage).where(
                    EmailMessage.provider_message_id == "message-1"
                )
            )
            suggestion = session.scalar(select(ExternalSuggestion))
            assert message is not None and suggestion is not None
            message.application_id = application_id
            suggestion.application_id = application_id
            application = session.get(Application, application_id)
            assert application is not None
            assert application.current_stage == ApplicationStage.APPLIED
            assert suggestion.approval_state == ApprovalState.PENDING

        with database.session() as session:
            suggestion = session.scalar(select(ExternalSuggestion))
            event = apply_external_suggestion(session, suggestion, approved=True)
            assert event is not None
            assert event.to_stage == ApplicationStage.INTERVIEW
            interview = create_interview(
                session,
                application_id,
                applied_at + timedelta(days=3),
                ends_at=applied_at + timedelta(days=3, minutes=45),
                interview_type="recruiter screen",
                location_or_link="https://meet.example.test/interview",
            )
            interview_id = interview.id
        calendar_read.assert_not_called()

    assert not hasattr(CalendarProvider, "create_or_update_event")

    assert mail.body_calls == 0
    assert mail.send_calls == 0

    with database.session() as session:
        application = session.get(Application, application_id)
        interview = session.get(Interview, interview_id)
        suggestion = session.scalar(select(ExternalSuggestion))
        assert application is not None
        assert application.current_stage == ApplicationStage.INTERVIEW
        assert interview is not None and interview.calendar_event_id is None
        assert suggestion is not None
        assert suggestion.approval_state == ApprovalState.APPROVED
        assert suggestion.applied_at is not None
        funnel = funnel_summary(session)
        yields = source_yield(session)
        calibration = score_calibration(session)
        durations = time_in_stage(session)
        actions = set(session.scalars(select(AuditEvent.action)))
        assert funnel["total"] == 1
        assert funnel["by_stage"][ApplicationStage.INTERVIEW.value] == 1
        assert funnel["response_rate"] == 1.0
        assert yields == [
            {
                "source": "greenhouse",
                "applications": 1,
                "responses": 1,
                "response_rate": 1.0,
            }
        ]
        assert calibration and calibration[0]["applications"] == 1
        assert ApplicationStage.PLANNED.value in durations
        assert {
            "workspace.imported",
            "document.proposed",
            "document.approved",
            "document.rendered",
            "application.created",
            "application.stage_changed",
            "application.material_attached",
            "gmail.metadata_ingested",
            "suggestion.approved",
            "interview.created",
        } <= actions
        assert (
            session.scalar(select(Evaluation).where(Evaluation.job_id == job_id))
            is not None
        )

    export_path = export_data(database, "json", tmp_path / "jobby-export.json")
    exported = json.loads(export_path.read_text(encoding="utf-8"))
    assert exported["schema"] == "jobby-export-v1"
    assert any(row["id"] == job_id for row in exported["jobs"])
    assert any(
        row["id"] == application_id
        and row["current_stage"] == ApplicationStage.INTERVIEW.value
        for row in exported["applications"]
    )
    assert any(row["action"] == "interview.created" for row in exported["audit_events"])

    backup_path = create_backup(
        database,
        output=tmp_path / "jobby-backup.zip",
        paths=paths,
    )
    assert verify_backup(backup_path) == (True, "ok")

    with database.engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
    secret_bytes = SECRET_CANARY.encode()
    assert secret_bytes not in paths.database.read_bytes()
    assert secret_bytes not in export_path.read_bytes()
    assert secret_bytes not in paths.config_file.read_bytes()
    with zipfile.ZipFile(backup_path) as archive:
        assert "database/jobby.sqlite3" in archive.namelist()
        assert "config/config.toml" in archive.namelist()
        assert all(
            secret_bytes not in archive.read(name) for name in archive.namelist()
        )

    with database.session() as session:
        assert (
            session.scalar(
                select(AuditEvent).where(AuditEvent.action == "export.created")
            )
            is not None
        )
    assert database.integrity_check() == (True, "ok")
    database.dispose()
