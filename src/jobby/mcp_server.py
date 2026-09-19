"""Vendor-neutral local MCP surface for Jobby.

The SDK is optional. Importing :mod:`jobby` or using ordinary CLI commands
does not require MCP to be installed.
"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from .capture import CapturePreview
from .facade import MAX_PAGE, ApplicationFacade, CaptureInput, SearchInput


class MCPRecord(BaseModel):
    """Bounded, documented extension record for evidence-bearing inputs."""

    model_config = ConfigDict(extra="allow")

    source: str | None = Field(default=None, max_length=200)
    source_id: str | None = Field(default=None, max_length=500)
    url: str | None = Field(default=None, max_length=8_000)
    fact_key: str | None = Field(default=None, max_length=300)
    evidence: str | None = Field(default=None, max_length=10_000)


class MCPInterviewAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer: str = Field(min_length=1, max_length=500_000)
    question_id: str | None = None
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    tags: list[str] = Field(default_factory=list, max_length=50)
    rating: int | None = Field(default=None, ge=1, le=5)


class MCPFollowUpTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(default="Interview follow-up", min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=500_000)
    due_at: datetime | None = None


class MCPCompanyCandidate(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str = Field(min_length=1, max_length=300)
    website: str | None = Field(default=None, max_length=2_000)
    source: str = Field(default="configured_ats", max_length=100)
    role: str | None = Field(default=None, max_length=300)
    location: str | None = Field(default=None, max_length=300)
    industry: str | None = Field(default=None, max_length=300)
    evidence: list[dict[str, Any]] = Field(default_factory=list, max_length=50)
    score: float | None = Field(default=None, ge=0, le=1)


class MCPWatchCriteria(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str | None = Field(default=None, max_length=300)
    location: str | None = Field(default=None, max_length=300)
    industry: str | None = Field(default=None, max_length=300)


class MCPOfferInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_salary: float = Field(default=0, ge=0)
    annual_bonus: float = Field(default=0, ge=0)
    annualized_equity: float = Field(default=0, ge=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    cost_of_living_index: float = Field(default=100, ge=0)
    stress_score: float = Field(default=3, ge=0, le=5)
    terms: dict[str, Any] = Field(default_factory=dict)


class MCPMutationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal[
        "application.create",
        "application.transition",
        "task.create",
        "contact.create",
        "interview.create",
        "interview_question.create",
        "interview_session.create",
        "offer.create",
        "offer.decision",
        "company.watch",
        "company.unwatch",
    ]
    payload: dict[str, Any]
    ttl_seconds: int = Field(default=600, ge=30, le=3_600)


def create_server(facade: ApplicationFacade | None = None):
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise RuntimeError(
            "MCP support is optional; install Jobby with the 'mcp' extra"
        ) from exc

    facade = facade or ApplicationFacade(actor="mcp_client")
    server = FastMCP("jobby")

    # Read-only tools
    @server.tool()
    def get_search_facets() -> dict[str, Any]:
        return facade.get_search_facets()

    @server.tool()
    def search_jobs(request: SearchInput | None = None) -> dict[str, Any]:
        return facade.search_jobs(SearchInput.model_validate(request or {}))

    @server.tool()
    def get_job(job_id: str, full_content: bool = False) -> dict[str, Any]:
        return facade.get_job(job_id, full_content=full_content)

    @server.tool()
    def get_company(company_id: str) -> dict[str, Any]:
        return facade.get_company(company_id)

    @server.tool()
    def get_latest_scan() -> dict[str, Any] | None:
        return facade.get_latest_scan()

    @server.tool()
    def get_operation_status(operation_id: str) -> dict[str, Any]:
        return facade.get_operation_status(operation_id)

    @server.tool()
    def get_source_health() -> list[dict[str, Any]]:
        return facade.get_source_health()

    @server.tool()
    def get_market_fit(job_id: str) -> dict[str, Any]:
        return facade.get_market_fit(job_id)

    @server.tool()
    def list_pipeline(
        stage: str | None = None, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return facade.list_pipeline(stage=stage, limit=limit, offset=offset)

    @server.tool()
    def get_application(application_id: str) -> dict[str, Any]:
        return facade.get_application(application_id)

    @server.tool()
    def list_contacts(
        company_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        return facade.list_contacts(company_id=company_id, limit=limit)

    @server.tool()
    def list_tasks(
        status: str | None = None, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return facade.list_tasks(status=status, limit=limit, offset=offset)

    @server.tool()
    def list_alerts(limit: int = 50, unread_only: bool = True) -> list[dict[str, Any]]:
        return facade.list_alerts(limit=limit, unread_only=unread_only)

    @server.tool()
    def list_pending_reviews(limit: int = 50) -> list[dict[str, Any]]:
        return facade.list_pending_reviews(limit=limit)

    @server.tool()
    def list_documents(
        status: str | None = None, limit: int = 50, full_content: bool = False
    ) -> list[dict[str, Any]]:
        return facade.list_documents(
            status=status, limit=limit, full_content=full_content
        )

    @server.tool()
    def get_analytics() -> dict[str, Any]:
        return facade.get_analytics()

    @server.tool()
    def list_questions(
        role_focus: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        return facade.list_questions(role_focus=role_focus, limit=limit)

    @server.tool()
    def list_interviews(
        application_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        return facade.list_interviews(application_id=application_id, limit=limit)

    @server.tool()
    def create_contact(
        name: str,
        company_id: str | None = None,
        email: str | None = None,
        title: str | None = None,
        linkedin_url: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.create_contact(
            name=name,
            company_id=company_id,
            email=email,
            title=title,
            linkedin_url=linkedin_url,
            notes=notes,
            approval_id=approval_id,
        )

    @server.tool()
    def create_interview(
        application_id: str,
        starts_at: datetime,
        ends_at: datetime | None = None,
        interview_type: str | None = None,
        location_or_link: str | None = None,
        contact_id: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.create_interview(
            application_id=application_id,
            starts_at=starts_at,
            ends_at=ends_at,
            interview_type=interview_type,
            location_or_link=location_or_link,
            contact_id=contact_id,
            notes=notes,
            approval_id=approval_id,
        )

    @server.tool()
    def list_offers(
        application_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        return facade.list_offers(application_id=application_id, limit=limit)

    @server.tool()
    def create_offer(
        application_id: str, offer: MCPOfferInput, approval_id: str | None = None
    ) -> dict[str, Any]:
        return facade.create_offer(
            application_id, approval_id=approval_id, **offer.model_dump()
        )

    # Controlled actions. Capture and document generation intentionally have
    # separate preview/proposal and approval calls.
    @server.tool()
    def prepare_mutation(request: MCPMutationInput) -> dict[str, Any]:
        return facade.prepare_mutation(
            request.action, request.payload, ttl_seconds=request.ttl_seconds
        )

    @server.tool()
    def approve_mutation(approval_id: str, expected_hash: str) -> dict[str, Any]:
        return facade.approve_mutation(approval_id, expected_hash=expected_hash)

    @server.tool()
    def run_scan(
        source: str = "all", query: str | None = None, background: bool = False
    ) -> dict[str, Any]:
        return facade.run_scan(source=source, query=query, background=background)

    @server.tool()
    def run_agent(
        include_web: bool | None = None,
        mode: str | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        return facade.run_agent(
            include_web=include_web, mode=mode, background=background
        )

    @server.tool()
    def preview_capture(request: CaptureInput) -> dict[str, Any]:
        return facade.preview_capture(request)

    @server.tool()
    def save_captured_job(preview: CapturePreview) -> dict[str, Any]:
        return facade.save_captured_job(preview)

    @server.tool()
    def create_application(
        job_id: str,
        submission_channel: str | None = None,
        notes: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.create_application(
            job_id,
            submission_channel=submission_channel,
            notes=notes,
            approval_id=approval_id,
        )

    @server.tool()
    def transition_application(
        application_id: str,
        to_stage: str,
        reason: str | None = None,
        expected_stage: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.transition_application(
            application_id,
            to_stage,
            reason=reason,
            expected_stage=expected_stage,
            approval_id=approval_id,
        )

    @server.tool()
    def create_task(
        title: str,
        description: str | None = None,
        due_at: datetime | None = None,
        job_id: str | None = None,
        application_id: str | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.create_task(
            title=title,
            description=description,
            due_at=due_at,
            job_id=job_id,
            application_id=application_id,
            approval_id=approval_id,
        )

    @server.tool()
    def log_activity(
        action: str,
        entity_type: str,
        entity_id: str | None = None,
        detail: str | None = None,
    ) -> dict[str, Any]:
        return facade.log_activity(
            action=action, entity_type=entity_type, entity_id=entity_id, detail=detail
        )

    @server.tool()
    def create_document_draft(
        base_version_id: str,
        content_markdown: str,
        job_id: str | None = None,
        provenance: list[MCPRecord] | None = None,
    ) -> dict[str, Any]:
        records = [item.model_dump(exclude_none=True) for item in provenance or []]
        return facade.create_document_draft(
            base_version_id=base_version_id,
            content_markdown=content_markdown,
            job_id=job_id,
            provenance=records,
        )

    @server.tool()
    def approve_document(
        document_id: str, expected_hash: str, edited_content: str | None = None
    ) -> dict[str, Any]:
        return facade.approve_document(
            document_id, expected_hash=expected_hash, edited_content=edited_content
        )

    @server.tool()
    def reject_document(document_id: str, expected_hash: str) -> dict[str, Any]:
        return facade.reject_document(document_id, expected_hash=expected_hash)

    @server.tool()
    def approve_review(
        review_id: str,
        review_type: str = "duplicate",
        canonical_job_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.approve_review(
            review_id, review_type=review_type, canonical_job_id=canonical_job_id
        )

    @server.tool()
    def dismiss_review(
        review_id: str, review_type: str = "duplicate", reason: str | None = None
    ) -> dict[str, Any]:
        return facade.dismiss_review(review_id, review_type=review_type, reason=reason)

    @server.tool()
    def create_interview_question(
        prompt: str,
        role_focus: str | None = None,
        tags: list[str] | None = None,
        skills: list[str] | None = None,
        evidence_keys: list[str] | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        return facade.create_interview_question(
            prompt=prompt,
            role_focus=role_focus,
            tags=tags or [],
            skills=skills or [],
            evidence_keys=evidence_keys or [],
            approval_id=approval_id,
        )

    @server.tool()
    def create_interview_session(
        application_id: str,
        session_type: str = "preparation",
        interview_id: str | None = None,
        role_focus: str | None = None,
        notes: str | None = None,
        answers: list[MCPInterviewAnswer] | None = None,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        records = [item.model_dump(exclude_none=True) for item in answers or []]
        return facade.create_interview_session(
            application_id=application_id,
            session_type=session_type,
            interview_id=interview_id,
            role_focus=role_focus,
            notes=notes,
            answers=records,
            approval_id=approval_id,
        )

    @server.tool()
    def record_interview_review(
        session_id: str,
        retrospective: str,
        outcome: str | None = None,
        follow_up_task: MCPFollowUpTask | None = None,
    ) -> dict[str, Any]:
        task = follow_up_task.model_dump(exclude_none=True) if follow_up_task else None
        return facade.record_interview_review(
            session_id,
            retrospective=retrospective,
            outcome=outcome,
            follow_up_task=task,
        )

    @server.tool()
    def compare_offers(application_id: str | None = None) -> list[dict[str, Any]]:
        return facade.compare_offers(application_id=application_id)

    @server.tool()
    def set_offer_decision(
        offer_id: str, decision: str | None, approval_id: str | None = None
    ) -> dict[str, Any]:
        return facade.set_offer_decision(offer_id, decision, approval_id=approval_id)

    @server.tool()
    def discover_companies(
        candidates: list[MCPCompanyCandidate] | None = None,
        role: str | None = None,
        location: str | None = None,
        industry: str | None = None,
    ) -> dict[str, Any]:
        records = [item.model_dump(exclude_none=True) for item in candidates or []]
        return facade.discover_companies(
            records, role=role, location=location, industry=industry
        )

    @server.tool()
    def watch_company(
        company_id: str,
        criteria: MCPWatchCriteria | None = None,
        cadence_days: int = 7,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        values = criteria.model_dump(exclude_none=True) if criteria else None
        return facade.watch_company(
            company_id,
            criteria=values,
            cadence_days=cadence_days,
            approval_id=approval_id,
        )

    @server.tool()
    def unwatch_company(
        company_id: str, approval_id: str | None = None
    ) -> dict[str, Any]:
        return facade.unwatch_company(company_id, approval_id=approval_id)

    # Stable read-only context resources. They are intentionally compact and
    # never expose keyring values or arbitrary filesystem contents.
    @server.resource("jobby://profile")
    def profile_resource() -> str:
        with facade.database.session() as session:
            from .models import ProfileFact

            facts = session.scalars(
                select(ProfileFact)
                .where(ProfileFact.approved.is_(True))
                .order_by(ProfileFact.fact_key)
                .limit(200)
            )
            return json.dumps(
                {
                    "approved_facts": [
                        {
                            "fact_key": row.fact_key,
                            "value": row.value_json,
                            "content_hash": row.content_hash,
                        }
                        for row in facts
                    ]
                },
                default=str,
            )

    @server.resource("jobby://sources")
    def sources_resource() -> str:
        configured: dict[str, int] = {}
        for provider, boards in facade.config.sources.model_dump().items():
            if isinstance(boards, (dict, list)) and boards:
                configured[provider] = len(boards)
        return json.dumps(
            {"configured_providers": configured, "credentials": "keyring-only"}
        )

    @server.resource("jobby://jobs/{job_id}")
    def job_resource(job_id: str) -> str:
        return json.dumps(facade.get_job(job_id), default=str)

    @server.resource("jobby://applications/{application_id}")
    def application_resource(application_id: str) -> str:
        return json.dumps(facade.get_application(application_id), default=str)

    @server.resource("jobby://documents/{document_id}")
    def document_resource(document_id: str) -> str:
        docs = [
            item
            for item in facade.list_documents(limit=MAX_PAGE)
            if item.get("id") == document_id
        ]
        if not docs:
            raise ValueError("document not found")
        return json.dumps(docs[0], default=str)

    @server.resource("jobby://pending-reviews")
    def pending_reviews_resource() -> str:
        return json.dumps(facade.list_pending_reviews(), default=str)

    return server


def run_stdio(facade: ApplicationFacade | None = None) -> int:
    """Run the first transport over local stdio and close the facade."""

    owned_facade = facade or ApplicationFacade(actor="mcp_client")
    try:
        server = create_server(owned_facade)
        server.run(transport="stdio")
        return 0
    finally:
        owned_facade.close()


__all__ = ["create_server", "run_stdio"]
