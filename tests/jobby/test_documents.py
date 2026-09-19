from __future__ import annotations

from contextlib import contextmanager
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from docx import Document as WordDocument
from pypdf import PdfReader, PdfWriter
from sqlalchemy import select

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.documents import (
    DocumentService,
    DraftDocumentResponse,
    PlaywrightPDFRenderer,
    ReportLabPDFRenderer,
    document_html,
    section_diffs,
    split_sections,
    validate_document,
)
from jobby.enums import ApprovalState, ArtifactKind, DocumentStatus
from jobby.models import AIRun, Artifact, Company, DocumentVersion, Job, ProfileFact


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_database(tmp_path: Path) -> Database:
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


def add_source_document(
    database: Database,
    *,
    content: str = "# Mike Sapp\n\n## Experience\n\n- Drafted policy guidance",
    kind: ArtifactKind = ArtifactKind.RESUME,
) -> str:
    with database.session() as session:
        version = DocumentVersion(
            kind=kind,
            name="Master Resume"
            if kind == ArtifactKind.RESUME
            else "Master Cover Letter",
            version=1,
            content_markdown=content,
            content_hash=digest(content),
            status=DocumentStatus.SOURCE,
            approval_state=ApprovalState.APPROVED,
            is_canonical=True,
            provenance=[
                {"artifact_id": "source-artifact", "content_hash": digest(content)}
            ],
        )
        session.add(version)
        session.flush()
        return version.id


def test_approved_document_cannot_be_unapproved_before_editing(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    version_id = add_source_document(database)

    with pytest.raises(ValueError, match="immutable"):
        with database.session() as session:
            version = session.get(DocumentVersion, version_id)
            assert version is not None
            version.approval_state = ApprovalState.REJECTED

    with pytest.raises(ValueError, match="immutable"):
        with database.session() as session:
            version = session.get(DocumentVersion, version_id)
            assert version is not None
            version.status = DocumentStatus.REJECTED

    with database.session() as session:
        version = session.get(DocumentVersion, version_id)
        assert version is not None
        assert version.approval_state == ApprovalState.APPROVED
        assert version.status == DocumentStatus.SOURCE
    database.dispose()


def test_section_diffs_preserve_heading_order_and_added_sections() -> None:
    before = "Preamble\n\n## Experience\nOld\n\n## Skills\nPython"
    after = "Preamble\n\n## Experience\nNew\n\n## Projects\nJobby"

    assert list(split_sections(before)) == ["Document", "Experience", "Skills"]
    diffs = section_diffs(before, after)

    assert [item.section for item in diffs] == [
        "Document",
        "Experience",
        "Skills",
        "Projects",
    ]
    assert diffs[0].changed is False
    assert "-Old" in diffs[1].unified_diff
    assert "+New" in diffs[1].unified_diff
    assert diffs[2].after == ""
    assert diffs[3].before == ""


def test_proposal_requires_review_and_approval_updates_canonical_and_diff(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposed_content = "# Mike Sapp\n\n## Experience\n\n- Drafted AI policy guidance"

    proposal = service.propose(
        base_version_id=source_id,
        content_markdown=proposed_content,
        job_id=None,
        provenance=[{"profile_fact_id": "fact-1", "content_hash": "abc"}],
    )

    assert proposal.status == DocumentStatus.PROPOSED
    assert proposal.approval_state == ApprovalState.PENDING
    assert proposal.is_canonical is False
    assert any(
        item["section"] == "Experience" and item["changed"]
        for item in proposal.diff_data
    )
    with pytest.raises(PermissionError, match="explicitly approved"):
        service.render_all(
            proposal.id, output_dir=tmp_path / "blocked", formats=("markdown",)
        )

    edited = proposed_content.replace("AI policy", "responsible AI policy")
    approved = service.approve(proposal.id, edited_content=edited)
    assert approved.status == DocumentStatus.APPROVED
    assert approved.approval_state == ApprovalState.APPROVED
    assert approved.is_canonical is True
    assert approved.content_hash == digest(edited)

    with database.session() as session:
        source = session.get(DocumentVersion, source_id)
        stored = session.get(DocumentVersion, proposal.id)
        assert source is not None and source.is_canonical is False
        assert stored is not None and stored.provenance == [
            {"profile_fact_id": "fact-1", "content_hash": "abc"}
        ]
        assert any("responsible AI" in item["after"] for item in stored.diff_data)

    with pytest.raises(ValueError, match="only pending proposals"):
        service.approve(proposal.id)
    database.dispose()


def test_reject_only_accepts_a_pending_proposal(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Revised",
        job_id=None,
        provenance=[{"source": "manual"}],
    )

    rejected = service.reject(proposal.id)
    assert rejected.status == DocumentStatus.REJECTED
    assert rejected.approval_state == ApprovalState.REJECTED
    with pytest.raises(ValueError, match="already been reviewed"):
        service.reject(proposal.id)

    with database.session() as session:
        pending_source = DocumentVersion(
            kind=ArtifactKind.RESUME,
            name="Unreviewed Source",
            version=1,
            content_markdown="# Source",
            content_hash=digest("# Source"),
            status=DocumentStatus.SOURCE,
            approval_state=ApprovalState.PENDING,
            provenance=[{"source": "import"}],
        )
        session.add(pending_source)
        session.flush()
        pending_source_id = pending_source.id
    with pytest.raises(ValueError, match="only pending proposals"):
        service.reject(pending_source_id)
    database.dispose()


def test_approval_blocks_unresolved_fields_until_user_edits_them(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Letter\n\nDear {{HIRING_MANAGER}},",
        job_id=None,
        provenance=[{"source": "manual"}],
    )

    with pytest.raises(ValueError, match="missing_fields"):
        service.approve(proposal.id)

    with database.session() as session:
        pending = session.get(DocumentVersion, proposal.id)
        assert pending is not None
        assert pending.status == DocumentStatus.PROPOSED
        assert pending.approval_state == ApprovalState.PENDING

    approved = service.approve(
        proposal.id, edited_content="# Letter\n\nDear Hiring Manager,"
    )
    assert approved.status == DocumentStatus.APPROVED
    assert approved.validation["valid"] is True
    database.dispose()


class StubAIProvider:
    def __init__(self, response: DraftDocumentResponse):
        self.response = response
        self.kwargs: dict[str, object] | None = None

    def structured(self, **kwargs: object) -> SimpleNamespace:
        self.kwargs = kwargs
        session = kwargs["session"]
        run = AIRun(
            purpose="document_draft",
            provider="openai",
            model="quality-model",
            prompt_version="document-draft-v1",
            input_hash="a" * 64,
            output_json=self.response.model_dump(mode="json"),
            approval_state=ApprovalState.PENDING,
        )
        session.add(run)
        session.flush()
        return SimpleNamespace(value=self.response, ai_run=run)


def test_ai_proposal_uses_only_approved_facts_and_records_base_provenance(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="AI Policy Counsel",
            normalized_title="ai policy counsel",
            description="Advise on AI governance and policy.",
        )
        approved_fact = ProfileFact(
            fact_key="experience.policy",
            value_json="Drafted policy guidance",
            approved=True,
            content_hash="b" * 64,
        )
        unapproved_fact = ProfileFact(
            fact_key="secret.unapproved",
            value_json="DO NOT SEND",
            approved=False,
            content_hash="c" * 64,
        )
        session.add_all([job, approved_fact, unapproved_fact])
        session.flush()
        job_id = job.id
        approved_fact_id = approved_fact.id

    provider = StubAIProvider(
        DraftDocumentResponse(
            content_markdown="# Mike Sapp\n\n## Experience\n\n- Drafted policy guidance",
            facts_used=["experience.policy", "experience.policy"],
            keywords_addressed=["governance"],
        )
    )
    proposal = DocumentService(database).propose_with_ai(
        provider, base_version_id=source_id, job_id=job_id
    )

    assert provider.kwargs is not None
    assert provider.kwargs["tier"] == "quality"
    prompt = str(provider.kwargs["text"])
    assert "Drafted policy guidance" in prompt
    assert "DO NOT SEND" not in prompt
    assert proposal.ai_run_id is not None
    assert proposal.validation == {"keywords_addressed": ["governance"]}
    assert proposal.provenance[0] == {
        "relationship": "base_document",
        "document_version_id": source_id,
        "content_hash": digest(
            "# Mike Sapp\n\n## Experience\n\n- Drafted policy guidance"
        ),
    }
    assert proposal.provenance[1] == {
        "fact_key": "experience.policy",
        "profile_fact_id": approved_fact_id,
        "content_hash": "b" * 64,
    }
    assert len(proposal.provenance) == 2
    DocumentService(database).reject(proposal.id)
    with database.session() as session:
        ai_run = session.get(AIRun, proposal.ai_run_id)
        assert ai_run is not None and ai_run.approval_state == ApprovalState.REJECTED
    database.dispose()


def test_invalid_ai_draft_is_rejected_but_its_run_remains_auditable(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        fact = ProfileFact(
            fact_key="experience.policy",
            value_json="Drafted policy guidance",
            approved=True,
            content_hash="b" * 64,
        )
        session.add_all([company, fact])
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            description="Policy role",
        )
        session.add(job)
        session.flush()
        job_id = job.id
    provider = StubAIProvider(
        DraftDocumentResponse(
            content_markdown="# Invented",
            facts_used=["unapproved.invention"],
        )
    )

    with pytest.raises(ValueError, match="unapproved fact keys"):
        DocumentService(database).propose_with_ai(
            provider, base_version_id=source_id, job_id=job_id
        )

    with database.session() as session:
        runs = list(session.scalars(select(AIRun)))
        documents = list(
            session.scalars(
                select(DocumentVersion).where(
                    DocumentVersion.status == DocumentStatus.PROPOSED
                )
            )
        )
        assert len(runs) == 1
        assert runs[0].approval_state == ApprovalState.REJECTED
        assert "unapproved.invention" in (runs[0].error or "")
        assert documents == []
    database.dispose()


class FakePDFRenderer:
    def __init__(self):
        self.calls: list[tuple[str, Path, str]] = []

    def render(self, content: str, output: Path, *, title: str = "") -> Path:
        self.calls.append((content, output, title))
        output.parent.mkdir(parents=True, exist_ok=True)
        writer = PdfWriter()
        writer.add_blank_page(width=612, height=792)
        with output.open("wb") as handle:
            writer.write(handle)
        return output


class FailingPDFRenderer:
    def render(self, _content: str, output: Path, *, title: str = "") -> Path:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"partial")
        raise RuntimeError(f"render failed for {title}")


def test_approved_document_renders_md_html_docx_and_mocked_pdf(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    content = "# Mike & Sapp\n\n## Experience\n\n- **AI policy** & governance"
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown=content,
        job_id=None,
        provenance=[{"profile_fact_id": "fact-1", "content_hash": "abc"}],
    )
    service.approve(proposal.id)
    renderer = FakePDFRenderer()

    outputs = service.render_all(
        proposal.id,
        output_dir=tmp_path / "exports",
        pdf_renderer=renderer,
    )

    assert set(outputs) == {"markdown", "html", "docx", "pdf"}
    assert outputs["markdown"].read_text(encoding="utf-8") == content
    rendered_html = outputs["html"].read_text(encoding="utf-8")
    assert "<h1>Mike &amp; Sapp</h1>" in rendered_html
    assert "<strong>AI policy</strong> &amp; governance" in rendered_html
    assert "<script" not in document_html("<script>alert(1)</script>")
    word_text = "\n".join(
        paragraph.text for paragraph in WordDocument(outputs["docx"]).paragraphs
    )
    assert "AI policy & governance" in word_text
    assert len(PdfReader(outputs["pdf"]).pages) == 1
    assert len(renderer.calls) == 1
    assert renderer.calls[0][2] == "Master Resume"

    with database.session() as session:
        stored = session.get(DocumentVersion, proposal.id)
        artifacts = list(
            session.scalars(
                select(Artifact).where(Artifact.document_version_id == proposal.id)
            )
        )
        assert stored is not None and stored.status == DocumentStatus.READY
        assert stored.validation["valid"] is True
        assert stored.validation["page_count"] is None
        rendered = list(
            session.scalars(
                select(Artifact).where(Artifact.document_version_id == stored.id)
            )
        )
        assert rendered
        assert all(
            item.metadata_json["validation"]["page_count"] == 1 for item in rendered
        )
        assert {item.metadata_json["format"] for item in artifacts} == set(outputs)
    database.dispose()


def test_render_all_reports_progress_after_each_staged_format(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Approved\n\nLocal export",
        job_id=None,
        provenance=[{"source": "manual"}],
    )
    service.approve(proposal.id)
    progress: list[tuple[str, int, int]] = []

    outputs = service.render_all(
        proposal.id,
        output_dir=tmp_path / "exports",
        formats=("markdown", "html", "markdown"),
        progress_callback=lambda name, completed, total: progress.append(
            (name, completed, total)
        ),
    )

    assert set(outputs) == {"markdown", "html"}
    assert progress == [("markdown", 1, 2), ("html", 2, 2)]
    database.dispose()


def test_export_formats_are_validated_before_any_output_is_written(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Approved",
        job_id=None,
        provenance=[{"source": "manual"}],
    )
    service.approve(proposal.id)
    output_dir = tmp_path / "exports"

    with pytest.raises(ValueError, match="unsupported document format: latex"):
        service.render_all(
            proposal.id, output_dir=output_dir, formats=("markdown", "latex")
        )

    assert not output_dir.exists()
    database.dispose()


def test_render_failure_preserves_existing_outputs_and_rolls_back_artifacts(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Approved",
        job_id=None,
        provenance=[{"source": "manual"}],
    )
    service.approve(proposal.id)
    output_dir = tmp_path / "exports"
    output_dir.mkdir()
    existing = output_dir / "master-resume-v2.md"
    existing.write_text("previous good output", encoding="utf-8")

    with pytest.raises(RuntimeError, match="render failed"):
        service.render_all(
            proposal.id,
            output_dir=output_dir,
            formats=("markdown", "pdf"),
            pdf_renderer=FailingPDFRenderer(),
        )

    assert existing.read_text(encoding="utf-8") == "previous good output"
    assert list(output_dir.glob("*.pdf")) == []
    assert list(output_dir.glob(".jobby-render-*")) == []
    with database.session() as session:
        stored = session.get(DocumentVersion, proposal.id)
        artifacts = list(
            session.scalars(
                select(Artifact).where(Artifact.document_version_id == proposal.id)
            )
        )
        assert stored is not None and stored.status == DocumentStatus.APPROVED
        assert artifacts == []
    database.dispose()


def test_render_commit_failure_removes_newly_activated_content_addressed_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = make_database(tmp_path)
    source_id = add_source_document(database)
    service = DocumentService(database)
    proposal = service.propose(
        base_version_id=source_id,
        content_markdown="# Approved",
        job_id=None,
        provenance=[{"source": "manual"}],
    )
    service.approve(proposal.id)
    real_session = database.session

    @contextmanager
    def failing_session():
        with real_session() as session:
            yield session
            raise RuntimeError("simulated metadata commit failure")

    monkeypatch.setattr(database, "session", failing_session)
    output_dir = tmp_path / "exports"

    with pytest.raises(RuntimeError, match="metadata commit failure"):
        service.render_all(
            proposal.id,
            output_dir=output_dir,
            formats=("markdown",),
        )

    assert list(output_dir.glob("*")) == []
    with real_session() as session:
        assert (
            list(
                session.scalars(
                    select(Artifact).where(Artifact.document_version_id == proposal.id)
                )
            )
            == []
        )
    database.dispose()


def test_reportlab_renderer_is_deterministic_browser_free_and_handles_unicode(
    tmp_path: Path,
) -> None:
    content = """# Safe résumé

## Experience

- **Legal policy** and *governance*
- [Portfolio](https://example.test/profile)
- Emoji fallback: 🙂

<script src="https://tracker.example.test/pixel.js">ignored as text</script>
"""
    first = tmp_path / "first.pdf"
    second = tmp_path / "second.pdf"

    ReportLabPDFRenderer().render(content, first, title="Résumé")
    # The old public name remains a browser-free compatibility alias.
    PlaywrightPDFRenderer().render(content, second, title="Résumé")

    assert first.read_bytes() == second.read_bytes()
    reader = PdfReader(first)
    assert len(reader.pages) == 1
    extracted = "\n".join(page.extract_text() or "" for page in reader.pages)
    assert "Safe résumé" in extracted
    assert "Legal policy" in extracted
    assert "[U+01F642]" in extracted
    assert "Page 1" in extracted


def test_reportlab_renderer_rejects_oversized_input_without_replacing_output(
    tmp_path: Path,
) -> None:
    output = tmp_path / "existing.pdf"
    output.write_bytes(b"previous")
    renderer = ReportLabPDFRenderer()

    with pytest.raises(ValueError, match="character limit"):
        renderer.render("x" * (renderer.max_input_chars + 1), output)

    assert output.read_bytes() == b"previous"


def test_validation_reports_fields_unicode_provenance_page_count_and_ats(
    tmp_path: Path,
) -> None:
    pdf_path = tmp_path / "two-pages.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    writer.add_blank_page(width=612, height=792)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    version = DocumentVersion(
        kind=ArtifactKind.COVER_LETTER,
        name="Draft",
        version=1,
        content_markdown="# Letter\n\n{{HIRING_MANAGER}}\x00",
        content_hash="d" * 64,
        status=DocumentStatus.PROPOSED,
        approval_state=ApprovalState.PENDING,
        provenance=[],
    )
    job = Job(
        company_id="company",
        title="Cybersecurity Privacy Governance Counsel",
        normalized_title="cybersecurity privacy governance counsel",
        description="Lead compliance investigations and regulatory strategy.",
    )

    validation = validate_document(version, job=job, pdf_path=pdf_path)
    codes = {item.code for item in validation.issues}

    assert validation.valid is False
    assert {"missing_fields", "unsupported_unicode", "missing_provenance"} <= codes
    assert {"page_count", "ats_coverage"} <= codes
    assert validation.page_count == 2
    assert validation.ats_keyword_coverage == 0.0
    assert "cybersecurity" in validation.missing_keywords


def test_validation_turns_an_unreadable_pdf_into_an_issue(tmp_path: Path) -> None:
    pdf_path = tmp_path / "broken.pdf"
    pdf_path.write_bytes(b"not a pdf")
    version = DocumentVersion(
        kind=ArtifactKind.RESUME,
        name="Resume",
        version=2,
        content_markdown="# Resume",
        content_hash="e" * 64,
        status=DocumentStatus.APPROVED,
        approval_state=ApprovalState.APPROVED,
        provenance=[{"source": "manual"}],
    )

    validation = validate_document(version, pdf_path=pdf_path)

    assert validation.valid is False
    assert any(issue.code == "pdf_unreadable" for issue in validation.issues)
    assert validation.page_count is None


def test_ats_keyword_coverage_prioritizes_title_terms_and_removes_stopwords() -> None:
    filler = " ".join(f"alpha{index:03d}" for index in range(150))
    version = DocumentVersion(
        kind=ArtifactKind.RESUME,
        name="Resume",
        version=2,
        content_markdown="# Resume\n\nUnrelated content",
        content_hash="f" * 64,
        status=DocumentStatus.APPROVED,
        approval_state=ApprovalState.APPROVED,
        provenance=[{"source": "manual"}],
    )
    job = Job(
        company_id="company",
        title="Zebracounsel Privacy",
        normalized_title="zebracounsel privacy",
        description=f"This role will work with your company. {filler}",
    )

    validation = validate_document(version, job=job)

    assert "zebracounsel" in validation.missing_keywords
    assert "privacy" in validation.missing_keywords
    assert "this" not in validation.missing_keywords
    assert "with" not in validation.missing_keywords
    assert "your" not in validation.missing_keywords
