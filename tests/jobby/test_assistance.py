"""Offline security checks for approval-gated application assistance."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from jobby.assistance import ApplicationAssistant, DraftKind, DraftResponse
from jobby.config import AppConfig, JobbyPaths, ModelSettings
from jobby.db import Database
from jobby.documents import DocumentService
from jobby.enums import ApplicationStage, ApprovalState, ArtifactKind, DocumentStatus
from jobby.models import (
    AIRun,
    Application,
    AuditEvent,
    Company,
    DocumentVersion,
    Job,
    ProfileFact,
)
from jobby.openai_provider import OpenAIProvider
from jobby.pipeline import create_application


def _database(tmp_path: Path) -> Database:
    paths = JobbyPaths(
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
        cache_dir=tmp_path / "cache",
        database=tmp_path / "data" / "jobby.sqlite3",
        artifacts_dir=tmp_path / "data" / "artifacts",
        backups_dir=tmp_path / "data" / "backups",
        logs_dir=tmp_path / "data" / "logs",
        config_file=tmp_path / "config" / "config.toml",
    )
    database = Database(paths=paths)
    database.initialize()
    return database


def _config() -> AppConfig:
    return AppConfig(
        openai_enabled=True,
        models=ModelSettings(
            fast="fast-model",
            quality="quality-model",
            premium="premium-model",
        ),
    )


def _application_with_facts(database: Database) -> tuple[str, str]:
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="AI Policy Counsel",
            normalized_title="ai policy counsel",
            description="Advise on responsible AI governance.",
        )
        session.add(job)
        session.flush()
        application = create_application(session, job.id)
        approved = ProfileFact(
            fact_key="experience.ai_policy",
            value_json={"statement": "Drafted responsible AI policy guidance"},
            approved=True,
            content_hash=hashlib.sha256(b"approved-policy-fact").hexdigest(),
        )
        unapproved = ProfileFact(
            fact_key="credentials.unreviewed",
            value_json="UNAPPROVED_CREDENTIAL_CANARY",
            approved=False,
            content_hash=hashlib.sha256(b"unapproved-credential").hexdigest(),
        )
        session.add_all([approved, unapproved])
        session.flush()
        return application.id, approved.id


def _provider(response: DraftResponse) -> tuple[OpenAIProvider, MagicMock]:
    client = MagicMock()
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=response,
        usage=SimpleNamespace(input_tokens=41, output_tokens=23),
    )
    return OpenAIProvider(_config(), client=client), client


def test_ai_assistance_uses_only_approved_facts_and_stays_pending(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    application_id, approved_fact_id = _application_with_facts(database)
    provider, client = _provider(
        DraftResponse(
            subject="Following up on the AI Policy Counsel role",
            body_markdown="I have drafted responsible AI policy guidance and remain interested in the role.",
            facts_used=["experience.ai_policy", "experience.ai_policy"],
            review_warnings=["Confirm the preferred greeting."],
        )
    )

    draft = ApplicationAssistant(database).draft(
        provider,
        application_id=application_id,
        kind=DraftKind.FOLLOW_UP,
        instructions="Keep it concise.",
    )

    call = client.responses.parse.call_args
    assert call.kwargs["model"] == "quality-model"
    assert call.kwargs["text_format"] is DraftResponse
    prompt = call.kwargs["input"][1]["content"]
    system = call.kwargs["input"][0]["content"]
    assert "Drafted responsible AI policy guidance" in prompt
    assert "UNAPPROVED_CREDENTIAL_CANARY" not in prompt
    assert "will not be sent automatically" in system
    assert draft.kind == ArtifactKind.EMAIL
    assert draft.status == DocumentStatus.PROPOSED
    assert draft.approval_state == ApprovalState.PENDING
    assert draft.is_canonical is False
    assert draft.provenance == [
        {
            "relationship": "application_context",
            "application_id": application_id,
            "job_id": draft.job_id,
            "job_description_hash": hashlib.sha256(
                b"Advise on responsible AI governance."
            ).hexdigest(),
        },
        {
            "fact_key": "experience.ai_policy",
            "profile_fact_id": approved_fact_id,
            "content_hash": hashlib.sha256(b"approved-policy-fact").hexdigest(),
        },
    ]
    assert draft.validation == {
        "review_warnings": ["Confirm the preferred greeting."],
        "external_action_performed": False,
    }
    with pytest.raises(PermissionError, match="explicitly approved"):
        DocumentService(database).render_all(
            draft.id,
            output_dir=tmp_path / "blocked",
            formats=("markdown",),
        )

    with database.session() as session:
        application = session.get(Application, application_id)
        ai_run = session.get(AIRun, draft.ai_run_id)
        audit = session.scalar(
            select(AuditEvent).where(
                AuditEvent.action == "application_assistance.drafted"
            )
        )
        assert application is not None
        assert application.current_stage == ApplicationStage.PLANNED
        assert application.submitted_at is None
        assert ai_run is not None and ai_run.approval_state == ApprovalState.PENDING
        assert ai_run.model == "quality-model"
        assert audit is not None
        assert audit.after_json["external_action_performed"] is False
    client.responses.create.assert_not_called()
    assert not (tmp_path / "blocked").exists()
    database.dispose()


def test_unapproved_fact_reference_rejects_draft_but_keeps_ai_run_auditable(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    application_id, _ = _application_with_facts(database)
    provider, client = _provider(
        DraftResponse(
            body_markdown="I hold an invented credential.",
            facts_used=["credentials.unreviewed"],
        )
    )

    with pytest.raises(ValueError, match="unapproved facts: credentials.unreviewed"):
        ApplicationAssistant(database).draft(
            provider,
            application_id=application_id,
            kind=DraftKind.FORM_ANSWERS,
        )

    client.responses.parse.assert_called_once()
    with database.session() as session:
        runs = list(session.scalars(select(AIRun)))
        drafts = list(
            session.scalars(
                select(DocumentVersion).where(
                    DocumentVersion.kind.in_(
                        [ArtifactKind.EMAIL, ArtifactKind.APPLICATION_PREP]
                    )
                )
            )
        )
        assert len(runs) == 1
        assert runs[0].approval_state == ApprovalState.REJECTED
        assert "credentials.unreviewed" in (runs[0].error or "")
        assert drafts == []
        assert (
            session.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "application_assistance.drafted"
                )
            )
            is None
        )
    database.dispose()
