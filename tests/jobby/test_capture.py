from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from jobby.capture import (
    CaptureCancelledError,
    CaptureConflictError,
    CaptureDraft,
    CaptureFetchError,
    CaptureNotReadyError,
    StaticPage,
    fetch_static_page,
    preview_capture,
    save_capture,
)
from jobby.db import Database
from jobby.models import (
    AuditEvent,
    Base,
    Company,
    Job,
    JobSourceState,
    Location,
    SourceObservation,
)
from jobby.normalization import MAX_PERSISTED_SALARY


@pytest.fixture
def database(tmp_path) -> Iterator[Database]:
    database = Database(tmp_path / "capture.sqlite3", acquire_lock=False)
    Base.metadata.create_all(database.engine)
    yield database
    database.dispose()


def _counts(database: Database) -> tuple[int, int, int, int, int]:
    with database.session() as session:
        values = tuple(
            int(session.scalar(select(func.count()).select_from(model)) or 0)
            for model in (Company, Location, Job, SourceObservation, JobSourceState)
        )
    return values[0], values[1], values[2], values[3], values[4]


def test_preview_preserves_pasted_authority_and_fills_blanks_without_writes(
    database: Database,
) -> None:
    page = StaticPage(
        html="""
        <html><head>
          <title>Static title that should lose to JSON-LD | Fetched Corp</title>
          <meta property="og:site_name" content="Fetched Corp">
          <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "title": "Senior AI Counsel",
            "hiringOrganization": {"@type": "Organization", "name": "Fetched Corp"},
            "jobLocation": {"address": {
              "addressLocality": "Seattle", "addressRegion": "WA",
              "addressCountry": "US"
            }},
            "description": "Fetched description with useful static details.",
            "baseSalary": {
              "currency": "USD",
              "value": {"minValue": 55, "maxValue": 70, "unitText": "HOUR"}
            }
          }
          </script>
        </head><body>Public static job content.</body></html>
        """,
        final_url="https://careers.example.com/jobs/7",
        http_status=200,
    )
    before = _counts(database)

    preview = preview_capture(
        database,
        CaptureDraft(
            url="http://careers.example.com/jobs/7?utm_source=mail#apply",
            company="Pasted Company, LLC",
        ),
        fetcher=lambda _url: page,
    )

    assert preview.ready_to_save
    assert preview.company == "Pasted Company, LLC"
    assert preview.title == "Senior AI Counsel"
    assert preview.location == "Seattle, WA, US"
    assert preview.description == "Fetched description with useful static details."
    assert preview.compensation == "USD 55 - 70 per hour"
    assert preview.launch_url == (
        "http://careers.example.com/jobs/7?utm_source=mail#apply"
    )
    assert preview.comparison_url == "https://careers.example.com/jobs/7"
    assert preview.fetched is not None
    assert preview.fetched.final_url == "https://careers.example.com/jobs/7"
    assert preview.authoritative_fields == ("url", "company")
    assert preview.salary_min == 114_400
    assert preview.salary_max == 145_600
    assert preview.salary_currency == "USD"
    assert preview.compensation_period.value == "hour"
    assert preview.compensation_confidence >= 0.8
    assert _counts(database) == before


def test_description_compensation_evidence_keeps_unknown_non_usd_visible(
    database: Database,
) -> None:
    preview = preview_capture(
        database,
        company="Example",
        title="Counsel",
        description=(
            "The role supports product teams. Compensation: EUR 70,000. "
            "Benefits are offered separately."
        ),
    )

    assert preview.compensation == "Compensation: EUR 70,000."
    assert preview.compensation_evidence == "Compensation: EUR 70,000."
    assert preview.salary_min == 70_000
    assert preview.salary_currency == "EUR"
    assert preview.compensation_period.value == "unknown"
    assert preview.compensation_confidence < 0.8
    assert any("non-USD" in warning for warning in preview.warnings)
    assert any("period is unknown" in warning for warning in preview.warnings)


def test_non_usd_hourly_capture_remains_raw_and_cannot_be_annualized_for_gating(
    database: Database,
) -> None:
    preview = preview_capture(
        database,
        company="Example",
        title="Counsel",
        compensation="CAD $50-$60 per hour",
    )

    assert preview.salary_currency == "CAD"
    assert preview.salary_min == 50
    assert preview.salary_max == 60
    assert preview.compensation_period.value == "hour"
    assert any("non-USD" in warning for warning in preview.warnings)


def test_oversized_compensation_stays_visible_without_numeric_persistence(
    database: Database,
) -> None:
    compensation = f"USD {MAX_PERSISTED_SALARY + 1} annually"

    preview = preview_capture(
        database,
        company="Example",
        title="Counsel",
        compensation=compensation,
    )

    assert preview.compensation == compensation
    assert preview.salary_min is None
    assert preview.salary_max is None
    assert any("verify compensation manually" in item for item in preview.warnings)

    saved = save_capture(database, preview)
    with database.session() as session:
        job = session.get(Job, saved.id)
        assert job is not None
        assert job.compensation_text == compensation
        assert job.salary_min is None
        assert job.salary_max is None


@pytest.mark.parametrize(
    "url",
    (
        "http://127.0.0.1/jobs/1",
        "http://localhost/jobs/1",
        "https://user:password@example.com/jobs/1",
        "file:///tmp/job.html",
    ),
)
def test_unsafe_urls_are_rejected_before_fetch(database: Database, url: str) -> None:
    called = False

    def fetcher(_url: str) -> StaticPage:
        nonlocal called
        called = True
        raise AssertionError("unsafe URL must never reach the fetcher")

    with pytest.raises(ValidationError, match="public HTTP"):
        preview_capture(database, url=url, fetcher=fetcher)
    assert not called


def test_oversized_pasted_input_is_rejected_instead_of_truncated(
    database: Database,
) -> None:
    with pytest.raises(ValidationError, match="at most 300 characters"):
        preview_capture(database, company="x" * 301, title="Counsel")
    with pytest.raises(ValidationError, match="at most 500000 characters"):
        CaptureDraft(description="x" * 500_001)


def test_fetch_failure_is_bounded_and_pasted_fields_remain_saveable(
    database: Database,
) -> None:
    def blocked(_url: str) -> StaticPage:
        raise CaptureFetchError(
            "restricted_page",
            "page contains a CAPTCHA and must not be bypassed",
        )

    before = _counts(database)
    preview = preview_capture(
        database,
        url="https://example.com/jobs/blocked",
        company="Pasted Co",
        title="Pasted Counsel",
        description="Pasted job description",
        fetcher=blocked,
    )

    assert preview.ready_to_save
    assert preview.fetch_error == "restricted_page"
    assert preview.fetched is None
    assert preview.company == "Pasted Co"
    assert preview.title == "Pasted Counsel"
    assert any("did not bypass" in warning for warning in preview.warnings)
    assert _counts(database) == before


def test_static_restricted_page_is_not_used_for_enrichment(
    database: Database,
) -> None:
    page = StaticPage(
        html="""
        <html><head><title>Fake job</title></head>
        <body>Access denied. Verify you are human to continue.</body></html>
        """,
        final_url="https://example.com/jobs/1",
        http_status=200,
    )

    preview = preview_capture(
        database,
        url="https://example.com/jobs/1",
        fetcher=lambda _url: page,
    )

    assert not preview.ready_to_save
    assert preview.fetch_error == "restricted_page"
    assert preview.company is None
    assert preview.title is None
    with pytest.raises(CaptureNotReadyError):
        save_capture(database, preview)


def test_javascript_only_page_reports_unsupported_without_browser_fallback(
    database: Database,
) -> None:
    page = StaticPage(
        html="<html><body>Please enable JavaScript to view this job.</body></html>",
        final_url="https://example.com/jobs/js-only",
        http_status=200,
    )

    preview = preview_capture(
        database,
        url="https://example.com/jobs/js-only",
        fetcher=lambda _url: page,
    )

    assert preview.fetch_error == "static_content_unavailable"
    assert preview.fetched is None
    assert not preview.ready_to_save
    assert any("JavaScript" in warning for warning in preview.warnings)


def test_duplicate_preview_is_read_only_and_explains_exact_url(
    database: Database,
) -> None:
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        session.add(
            Job(
                company_id=company.id,
                title="Product Counsel",
                normalized_title="product counsel",
                canonical_url="https://example.com/jobs/42",
                launch_url="https://example.com/jobs/42?ref=original",
                comparison_url="https://example.com/jobs/42",
                source_primary="greenhouse",
                source_id="42",
                description="Existing richer description",
                description_hash="a" * 64,
            )
        )
    before = _counts(database)

    preview = preview_capture(
        database,
        url="http://www.example.com/jobs/42?utm_campaign=test#apply",
        company="Different pasted company",
        title="Different pasted title",
        fetcher=lambda _url: (_minimal_html(), "https://example.com/jobs/42", 200),
    )

    assert len(preview.duplicate_candidates) == 1
    candidate = preview.duplicate_candidates[0]
    assert candidate.reason == "comparison_url"
    assert candidate.similarity == 1
    assert candidate.exact
    assert _counts(database) == before
    with pytest.raises(CaptureConflictError, match="no records were overwritten"):
        save_capture(database, preview)
    assert _counts(database) == before


def test_exact_url_duplicate_is_prioritized_ahead_of_candidate_query_limit(
    database: Database, monkeypatch
) -> None:
    now = datetime.now(timezone.utc)
    with database.session() as session:
        company = Company(name="Acme", normalized_name="acme")
        session.add(company)
        session.flush()
        session.add(
            Job(
                company_id=company.id,
                title="Original Counsel",
                normalized_title="original counsel",
                canonical_url="https://example.com/jobs/exact",
                launch_url="https://example.com/jobs/exact?ref=old",
                comparison_url="https://example.com/jobs/exact",
                source_primary="greenhouse",
                source_id="exact",
                discovered_at=now - timedelta(days=10),
            )
        )
        for index in range(3):
            session.add(
                Job(
                    company_id=company.id,
                    title=f"New Counsel {index}",
                    normalized_title=f"new counsel {index}",
                    canonical_url=f"https://example.com/jobs/new-{index}",
                    launch_url=f"https://example.com/jobs/new-{index}",
                    comparison_url=f"https://example.com/jobs/new-{index}",
                    source_primary="greenhouse",
                    source_id=f"new-{index}",
                    discovered_at=now + timedelta(minutes=index),
                )
            )
    monkeypatch.setattr("jobby.capture.MAX_DUPLICATE_ROWS", 2)

    preview = preview_capture(
        database,
        url="https://example.com/jobs/exact?utm_source=friend",
        company="Acme",
        title="Different title",
        fetcher=lambda _url: (_minimal_html(), "https://example.com/jobs/exact", 200),
    )

    assert preview.duplicate_candidates
    assert preview.duplicate_candidates[0].reason == "comparison_url"
    assert preview.duplicate_candidates[0].launch_url == (
        "https://example.com/jobs/exact?ref=old"
    )


def test_explicit_save_atomically_persists_manual_provenance(
    database: Database,
) -> None:
    preview = preview_capture(
        database,
        url="https://example.com/jobs/99?utm_source=friend#apply",
        company="Acme Legal, LLC",
        title="AI Product Counsel",
        location="Remote",
        description="Own legal strategy for an AI product.",
        compensation="$50 per hour",
        fetcher=lambda _url: (_minimal_html(), "https://example.com/jobs/99", 200),
    )

    saved = save_capture(database, preview)

    with database.session() as session:
        job = session.get(Job, saved.id)
        assert job is not None
        company = session.get(Company, job.company_id)
        location = session.get(Location, job.location_id)
        observation = session.scalar(
            select(SourceObservation).where(SourceObservation.job_id == job.id)
        )
        source_state = session.scalar(
            select(JobSourceState).where(JobSourceState.job_id == job.id)
        )
        audit = session.scalar(
            select(AuditEvent).where(
                AuditEvent.entity_id == job.id,
                AuditEvent.action == "job.capture_saved",
            )
        )

        assert company is not None and company.name == "Acme Legal, LLC"
        assert location is not None and location.display_name == "Remote"
        assert location.remote
        assert job.source_primary == "manual"
        assert job.launch_url == "https://example.com/jobs/99?utm_source=friend#apply"
        assert job.comparison_url == "https://example.com/jobs/99"
        assert job.canonical_url == job.comparison_url
        assert job.description == "Own legal strategy for an AI product."
        assert job.compensation_text == "$50 per hour"
        assert job.salary_min == 104_000
        assert job.salary_currency == "USD"
        assert job.compensation_period == "hour"
        assert job.compensation_confidence >= 0.8
        assert job.authoritative_fields == [
            "url",
            "company",
            "title",
            "location",
            "description",
            "compensation",
        ]
        assert not job.liveness_known

        assert observation is not None
        assert observation.source == "manual"
        assert observation.source_job_id == job.source_id
        assert observation.source_url == job.launch_url
        assert observation.observation_kind == "manual_capture"
        assert observation.raw_payload["description"] == job.description
        assert observation.raw_payload["preview_fingerprint"] == preview.fingerprint
        assert observation.snapshot_hash

        assert source_state is not None
        assert source_state.source == "manual"
        assert source_state.seen_count == 1
        assert source_state.last_snapshot_hash == observation.snapshot_hash
        assert source_state.is_live is None
        assert audit is not None


def test_save_reuses_company_without_overwriting_existing_display_data(
    database: Database,
) -> None:
    with database.session() as session:
        company = Company(
            name="ACME LEGAL (user spelling)",
            normalized_name="acme legal user spelling",
            notes="user-owned notes",
        )
        session.add(company)
        session.flush()
        company_id = company.id

    preview = preview_capture(
        database,
        company="Acme Legal User Spelling, LLC",
        title="Counsel",
        description="Manual description",
    )
    saved = save_capture(database, preview)

    with database.session() as session:
        company = session.get(Company, company_id)
        job = session.get(Job, saved.id)
        assert company is not None
        assert company.name == "ACME LEGAL (user spelling)"
        assert company.notes == "user-owned notes"
        assert job is not None and job.company_id == company_id
        assert session.scalar(select(func.count()).select_from(Company)) == 1


def test_cancellation_during_save_rolls_back_every_new_row(
    database: Database,
) -> None:
    preview = preview_capture(
        database,
        company="Rollback Company",
        title="Rollback Counsel",
        location="Portland, OR",
        description="Should not persist",
    )
    calls = 0

    def cancel_after_lookups() -> bool:
        nonlocal calls
        calls += 1
        return calls == 2

    with pytest.raises(CaptureCancelledError):
        save_capture(database, preview, cancel=cancel_after_lookups)

    assert _counts(database) == (0, 0, 0, 0, 0)


def test_cancelled_preview_never_fetches_or_queries(database: Database) -> None:
    called = False

    def fetcher(_url: str) -> StaticPage:
        nonlocal called
        called = True
        raise AssertionError

    with pytest.raises(CaptureCancelledError):
        preview_capture(
            database,
            url="https://example.com/jobs/1",
            fetcher=fetcher,
            cancel=lambda: True,
        )
    assert not called
    assert _counts(database) == (0, 0, 0, 0, 0)


def test_static_fetch_honors_cancellation_before_opening_a_client() -> None:
    with pytest.raises(CaptureCancelledError):
        fetch_static_page(
            "https://example.com/jobs/1",
            cancel=lambda: True,
        )


def test_tampered_preview_is_rejected_without_writes(database: Database) -> None:
    preview = preview_capture(database, company="Acme", title="Counsel")
    tampered = preview.model_copy(update={"title": "Different title"})

    with pytest.raises(ValueError, match="fingerprint"):
        save_capture(database, tampered)
    assert _counts(database) == (0, 0, 0, 0, 0)


def _minimal_html() -> str:
    return """
    <html><head><meta name="description" content="Static fallback"></head>
    <body>Static fallback</body></html>
    """
