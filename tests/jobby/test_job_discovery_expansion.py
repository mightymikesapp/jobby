from __future__ import annotations

import hashlib
import socket
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from jobby.analytics import requirement_analytics
from jobby.config import AppConfig, JobbyPaths, load_config
from jobby.db import Database
from jobby.documents import (
    DocumentService,
    DocumentSectionEdit,
    StructuredDraftDocumentResponse,
    apply_structured_document_edits,
)
from jobby.enums import (
    ApprovalState,
    ArtifactKind,
    DocumentStatus,
    JobStatus,
    SuggestionKind,
)
from jobby.google_integration import classify_gmail_metadata
from jobby.models import (
    AIRun,
    CanonicalJobGroup,
    CanonicalJobMember,
    Company,
    DocumentVersion,
    Job,
    ProfileFact,
)
from jobby.sources.ats import ICIMSSource, SmartRecruitersSource, TaleoSource
from jobby.sources.base import ScanStatus
from jobby.sources.resolver import (
    resolve_source_url,
    structural_test_and_add,
    verify_public_dns,
)


def _paths(tmp_path: Path) -> JobbyPaths:
    root = tmp_path / "jobby"
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    )


def _public_resolver(host: str, port: int, **_kwargs: object) -> list[object]:
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def test_resolver_normalizes_supported_variants_and_rejects_hostile_urls() -> None:
    first = resolve_source_url("https://jobs.smartrecruiters.com/Acme-Co")
    second = resolve_source_url(
        "https://api.smartrecruiters.com/v1/companies/Acme-Co/postings"
    )
    assert first == second
    assert first.as_dict()["configuration"]["company_slug"] == "Acme-Co"

    icims = resolve_source_url(
        "https://careers-amd.icims.com/jobs/search?ss=1&searchKeyword=legal"
    )
    assert icims.configuration.base_url == "https://careers-amd.icims.com"

    taleo = resolve_source_url(
        "https://acme.tbe.taleo.net/acme01/ats/careers/v2/searchResults?cws=40&org=ACME"
    )
    assert taleo.configuration.search_url.endswith("?org=ACME&cws=40")

    hostile = (
        "http://jobs.smartrecruiters.com/Acme",
        "https://user:pass@jobs.smartrecruiters.com/Acme",
        "https://jobs.smartrecruiters.com:443/Acme",
        "https://jobs.smartrecruiters.com/Acme#fragment",
        "https://jobs.smartrecruiters.com.evil.example/Acme",
        "https://jobs.smartrecruiters.com/Acme%252Fadmin",
        "https://acme.icims.com.evil.example/jobs/search",
        "https://acme.tbe.taleo.net/x/ats/careers/v2/searchResults?org=A",
        "https://acme.tbe.taleo.net/x/ats/careers/v2/searchResults?org=A&org=B&cws=1",
    )
    for url in hostile:
        with pytest.raises(ValueError):
            resolve_source_url(url)


def test_resolver_rejects_private_dns_and_adds_only_after_structural_test(
    tmp_path: Path,
) -> None:
    resolved = resolve_source_url("https://jobs.smartrecruiters.com/Acme")

    def private(_host: str, port: int, **_kwargs: object) -> list[object]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

    with pytest.raises(ValueError, match="private or unsafe"):
        verify_public_dns(resolved, private)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.smartrecruiters.com"
        return httpx.Response(
            200,
            request=request,
            json={"content": [], "totalFound": 0},
        )

    paths = _paths(tmp_path)
    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        updated = structural_test_and_add(
            resolved,
            AppConfig(),
            paths=paths,
            resolver=_public_resolver,
            client=client,
        )
    assert "acme" in updated.sources.smartrecruiters
    assert load_config(paths) == updated
    with pytest.raises(ValueError, match="already exists"):
        structural_test_and_add(
            resolved,
            updated,
            paths=paths,
            resolver=lambda *_args, **_kwargs: pytest.fail(
                "duplicates must fail before DNS"
            ),
        )


def test_smartrecruiters_paginates_deduplicates_and_hydrates() -> None:
    requests: list[tuple[str, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/postings/job-2"):
            return httpx.Response(
                200,
                request=request,
                json={
                    "id": "job-2",
                    "jobAd": {"sections": {"jobDescription": {"text": "Policy work"}}},
                },
            )
        offset = int(request.url.params["offset"])
        requests.append((request.method, offset))
        rows = (
            [
                {
                    "id": "job-1",
                    "name": "Counsel",
                    "ref": "https://jobs.smartrecruiters.com/Acme/job-1",
                },
                {
                    "id": "job-2",
                    "name": "Policy Counsel",
                    "ref": "https://jobs.smartrecruiters.com/Acme/job-2",
                },
            ]
            if offset == 0
            else [
                {
                    "id": "job-2",
                    "name": "Policy Counsel",
                    "ref": "https://jobs.smartrecruiters.com/Acme/job-2",
                }
            ]
        )
        return httpx.Response(
            200, request=request, json={"content": rows, "totalFound": 3}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        source = SmartRecruitersSource(
            client, company_slug="Acme", company="Acme", limit=2
        )
        result = source.scan()
        hydrated = source.hydrate(result.items[1])

    assert requests == [("GET", 0), ("GET", 2)]
    assert result.status is ScanStatus.SUCCEEDED
    assert [item.source_id for item in result.items] == ["job-1", "job-2"]
    assert hydrated.description == "Policy work"


def test_icims_and_taleo_parse_static_html_and_accept_empty_boards() -> None:
    icims_html = """
    <html><head><title>iCIMS Careers</title></head><body>
      <a href="/jobs/123/policy-counsel/job">Policy Counsel</a>
      <a href="/jobs/123/policy-counsel/job">Policy Counsel duplicate</a>
    </body></html>
    """
    taleo_html = """
    <html><head><title>Taleo SearchResults</title></head><body>
      <a href="/acme01/ats/careers/v2/viewRequisition?org=ACME&amp;cws=1&amp;rid=42">IP Counsel</a>
    </body></html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = icims_html if request.url.host.endswith("icims.com") else taleo_html
        return httpx.Response(
            200, request=request, text=body, headers={"content-type": "text/html"}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        icims = ICIMSSource(
            client, base_url="https://careers-acme.icims.com", company="Acme"
        ).scan()
        taleo = TaleoSource(
            client,
            search_url=(
                "https://acme.tbe.taleo.net/acme01/ats/careers/v2/"
                "searchResults?org=ACME&cws=1"
            ),
            company="Acme",
        ).scan()

    assert icims.status is ScanStatus.SUCCEEDED
    assert [(item.source_id, item.title) for item in icims.items] == [
        ("123", "Policy Counsel")
    ]
    assert taleo.status is ScanStatus.SUCCEEDED
    assert [(item.source_id, item.title) for item in taleo.items] == [
        ("42", "IP Counsel")
    ]


def _base_resume() -> tuple[DocumentVersion, str]:
    content = (
        "# Fixture Candidate\n\n## Summary\nPolicy professional.\n\n"
        "## Experience\n\n### Acme — Counsel | 2024–2026\n"
        "- Drafted policy guidance.\n\n## Education\nJD, 2026"
    )
    summary = "## Summary\nPolicy professional.\n\n"
    version = DocumentVersion(
        kind=ArtifactKind.RESUME,
        name="Resume",
        version=1,
        content_markdown=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
        status=DocumentStatus.SOURCE,
        approval_state=ApprovalState.APPROVED,
    )
    return version, summary


def test_structured_document_edits_accept_supported_claims_and_reject_guards() -> None:
    base, summary = _base_resume()
    edit = DocumentSectionEdit(
        section_name="Summary",
        preimage_sha256=hashlib.sha256(summary.encode()).hexdigest(),
        replacement_markdown=(
            "## Summary\nAI governance professional with 15 patent applications."
        ),
        cited_fact_keys=["patents"],
        addressed_keywords=["AI governance"],
    )
    applied = apply_structured_document_edits(
        base,
        StructuredDraftDocumentResponse(edits=[edit]),
        approved_facts={"patents": "Filed 15 USPTO patent applications"},
    )
    assert applied.valid
    assert applied.cited_fact_keys == ["patents"]
    assert "15 patent applications" in applied.content_markdown

    stale = edit.model_copy(update={"preimage_sha256": "0" * 64})
    stale_result = apply_structured_document_edits(
        base,
        StructuredDraftDocumentResponse(edits=[stale]),
        approved_facts={"patents": "15"},
    )
    assert {issue.code for issue in stale_result.issues} == {"edit_stale_preimage"}

    invented = edit.model_copy(
        update={
            "replacement_markdown": "## Summary\nImproved outcomes by 99%.",
            "cited_fact_keys": [],
        }
    )
    invented_result = apply_structured_document_edits(
        base,
        StructuredDraftDocumentResponse(edits=[invented]),
        approved_facts={},
    )
    assert "invented_numeric_claim" in {issue.code for issue in invented_result.issues}

    protected = edit.model_copy(update={"section_name": "Education"})
    education_raw = "## Education\nJD, 2026"
    protected = protected.model_copy(
        update={
            "preimage_sha256": hashlib.sha256(education_raw.encode()).hexdigest(),
            "replacement_markdown": "## Education\nPhD, 2026",
        }
    )
    protected_result = apply_structured_document_edits(
        base,
        StructuredDraftDocumentResponse(edits=[protected]),
        approved_facts={"patents": "15"},
    )
    assert "edit_section_not_mutable" in {
        issue.code for issue in protected_result.issues
    }


class _V2Provider:
    def __init__(self, response: StructuredDraftDocumentResponse) -> None:
        self.value = response
        self.prompt_version: str | None = None

    def structured(self, **kwargs: object) -> SimpleNamespace:
        self.prompt_version = str(kwargs["prompt_version"])
        assert kwargs["output_type"] is StructuredDraftDocumentResponse
        session = kwargs["session"]
        run = AIRun(
            purpose="document_draft",
            provider="openai",
            model="quality",
            prompt_version=self.prompt_version,
            input_hash="a" * 64,
            output_json=self.value.model_dump(mode="json"),
        )
        session.add(run)
        session.flush()
        return SimpleNamespace(value=self.value, ai_run=run)


def test_v2_ai_violation_rejects_run_without_saving_proposal(tmp_path: Path) -> None:
    database = Database(paths=_paths(tmp_path))
    database.initialize()
    base, summary = _base_resume()
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Counsel",
            normalized_title="counsel",
            description="AI governance",
        )
        fact = ProfileFact(
            fact_key="experience.ai",
            value_json="AI governance",
            approved=True,
            content_hash="b" * 64,
        )
        session.add_all([base, job, fact])
        session.flush()
        base_id, job_id = base.id, job.id

    provider = _V2Provider(
        StructuredDraftDocumentResponse(
            edits=[
                DocumentSectionEdit(
                    section_name="Summary",
                    preimage_sha256=hashlib.sha256(summary.encode()).hexdigest(),
                    replacement_markdown="## Summary\nWon 99% of all matters.",
                    cited_fact_keys=["experience.ai"],
                )
            ]
        )
    )
    with pytest.raises(ValueError, match="invented_numeric_claim"):
        DocumentService(database).propose_with_ai(
            provider, base_version_id=base_id, job_id=job_id
        )

    with database.session() as session:
        run = session.scalar(select(AIRun))
        proposals = list(
            session.scalars(
                select(DocumentVersion).where(
                    DocumentVersion.status == DocumentStatus.PROPOSED
                )
            )
        )
    assert provider.prompt_version == "document-draft-v2"
    assert run is not None and run.approval_state == ApprovalState.REJECTED
    assert proposals == []
    database.dispose()


def test_requirement_analytics_deduplicates_groups_and_classifies_evidence(
    tmp_path: Path,
) -> None:
    database = Database(paths=_paths(tmp_path))
    database.initialize()
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        jobs: list[Job] = []
        for index in range(6):
            job = Job(
                company_id=company.id,
                title=f"AI Policy Counsel {index}",
                normalized_title=f"ai policy counsel {index}",
                description="AI governance and GDPR; AI governance.",
                latest_score=4.0 if index < 5 else 3.9,
                status=JobStatus.DISCOVERED,
            )
            session.add(job)
            jobs.append(job)
        session.flush()
        group = CanonicalJobGroup(canonical_job_id=jobs[0].id)
        session.add(group)
        session.flush()
        session.add_all(
            [
                CanonicalJobMember(
                    group_id=group.id, job_id=jobs[0].id, is_canonical=True
                ),
                CanonicalJobMember(group_id=group.id, job_id=jobs[1].id),
            ]
        )
        session.add(
            ProfileFact(
                fact_key="experience.ai_governance",
                value_json="AI governance",
                approved=True,
                content_hash="a" * 64,
            )
        )

    with database.session() as session:
        report = requirement_analytics(session)
    # Five score-qualified records become four canonical/standalone groups.
    assert report["job_count"] == 4
    assert report["sample"]["status"] == "insufficient_sample"

    with database.session() as session:
        company_id = session.scalar(select(Company.id))
        session.add(
            Job(
                company_id=company_id,
                title="Copyright Counsel",
                normalized_title="copyright counsel",
                description="AI governance and copyright",
                latest_score=4.5,
            )
        )
    with database.session() as session:
        report = requirement_analytics(session)
    assert report["sample"]["status"] == "ready"
    ai_row = next(row for row in report["top_terms"] if row["term"] == "AI governance")
    assert ai_row["occurrence_count"] == 5
    assert ai_row["evidence"] == "evidenced"
    assert len(report["top_terms"]) <= 20
    database.dispose()


@pytest.mark.parametrize(
    ("sender", "subject", "snippet", "expected"),
    [
        (
            "Greenhouse <no-reply@greenhouse.io>",
            "Application received",
            "Thank you",
            SuggestionKind.APPLIED,
        ),
        (
            "Recruiting <coordinator@jobs.lever.co>",
            "Interview invitation",
            "Choose a time",
            SuggestionKind.INTERVIEW_REQUESTED,
        ),
        (
            "updates@smartrecruiters.com",
            "We are not moving forward",
            "Application update",
            SuggestionKind.REJECTED,
        ),
        (
            "alerts@example.com",
            "Your daily job alert",
            "Interview roles near you",
            None,
        ),
        (
            "coach@example.com",
            "Mock interview invitation",
            "Practice now",
            None,
        ),
        (
            "no-reply@greenhouse.io.evil.example",
            "Application received",
            "Thank you",
            None,
        ),
    ],
)
def test_metadata_gmail_classifier_exclusions_and_exact_domains(
    sender: str,
    subject: str,
    snippet: str,
    expected: SuggestionKind | None,
) -> None:
    result = classify_gmail_metadata(sender=sender, subject=subject, snippet=snippet)
    assert (result[0] if result else None) == expected
