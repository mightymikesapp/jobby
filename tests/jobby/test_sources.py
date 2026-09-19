from __future__ import annotations

from datetime import timezone
from decimal import Decimal
import json
import time

import httpx
import pytest

from jobby.normalization import RemoteStatus, SalaryPeriod
from jobby.sources import (
    AshbySource,
    GreenhouseSource,
    LeverSource,
    ScanStatus,
    USAJobsSource,
    WorkableSource,
    WorkdaySource,
)
from jobby.sources.base import ScanItem, SourceError


def mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_source_contract_bounds_payloads_and_redacts_diagnostics() -> None:
    error = SourceError(
        code="HTTP ERROR!",
        message=(
            "Authorization: Bearer super-secret-token password=hunter2 "
            "Authorization-Key: usa-secret "
        )
        + "x" * 5_000,
    )
    assert error.code == "http_error_"
    assert "super-secret-token" not in error.message
    assert "hunter2" not in error.message
    assert "usa-secret" not in error.message
    assert len(error.message) <= 2_000

    item = ScanItem(
        source="s" * 200,
        source_id="i" * 1_000,
        company="c" * 500,
        title="t" * 1_000,
        url="https://jobs.example.test/role",
        description="d" * 600_000,
        metadata={"huge": "m" * 100_000, "unsafe": object()},
    )
    assert len(item.source) == 80
    assert len(item.source_id) == 500
    assert len(item.company) == 300
    assert len(item.title) == 500
    assert len(item.description) == 500_000
    assert len(item.metadata["huge"]) <= 4_000
    assert isinstance(item.metadata["unsafe"], str)


def test_long_configured_board_identity_gets_a_stable_bounded_source_key() -> None:
    board = "a" * 200
    with mock_client(lambda request: httpx.Response(500, request=request)) as client:
        first = GreenhouseSource(client, board_token=board)
        second = GreenhouseSource(client, board_token=board)

    assert first.source_key == second.source_key
    assert first.source_key.startswith("greenhouse:")
    assert len(first.source_key) == 80


def test_greenhouse_maps_public_board_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/boards/example/jobs"
        assert request.url.params["content"] == "true"
        return httpx.Response(
            200,
            request=request,
            json={
                "jobs": [
                    {
                        "id": 42,
                        "title": "Product Counsel",
                        "absolute_url": "https://boards.example.test/jobs/42",
                        "location": {"name": "Remote - US"},
                        "content": "<p>Advise the product team.</p>",
                        "first_published": "2026-07-10T09:30:00Z",
                        "workplace_type": "remote",
                    }
                ]
            },
        )

    with mock_client(handler) as client:
        result = GreenhouseSource(
            client, board_token="example", company="Example, Inc."
        ).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert result.source == "greenhouse:example"
    assert result.completed_at >= result.started_at
    assert len(result.items) == 1
    item = result.items[0]
    assert item.source_id == "42"
    assert item.company == "Example, Inc."
    assert item.location == "Remote - US"
    assert item.remote is RemoteStatus.REMOTE
    assert item.posted_at is not None and item.posted_at.tzinfo is timezone.utc


def test_lever_maps_salary_and_categories() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v0/postings/example"
        assert request.url.params["mode"] == "json"
        return httpx.Response(
            200,
            request=request,
            json=[
                {
                    "id": "lev-1",
                    "text": "Privacy Counsel",
                    "hostedUrl": "https://jobs.lever.co/example/lev-1",
                    "descriptionPlain": "Privacy role",
                    "categories": {"location": "New York, NY", "team": "Legal"},
                    "workplaceType": "hybrid",
                    "createdAt": 1_752_134_400_000,
                    "salaryRange": {
                        "min": 120000,
                        "max": 160000,
                        "currency": "USD",
                        "interval": "per-year-salary",
                    },
                }
            ],
        )

    with mock_client(handler) as client:
        result = LeverSource(client, site="example", company="Example").scan()

    item = result.items[0]
    assert result.status is ScanStatus.SUCCEEDED
    assert item.remote is RemoteStatus.HYBRID
    assert item.salary_text == "USD 120000 - 160000 per-year-salary"
    assert item.salary is not None
    assert item.salary.minimum == Decimal("120000")
    assert item.salary.period is SalaryPeriod.YEAR


def test_ashby_maps_remote_posting() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/posting-api/job-board/example/jobs"
        return httpx.Response(
            200,
            request=request,
            json={
                "jobs": [
                    {
                        "id": "ash-1",
                        "title": "Legal Operations Analyst",
                        "jobUrl": "https://jobs.ashbyhq.com/example/ash-1",
                        "location": "United States",
                        "descriptionPlain": "Build legal workflows.",
                        "isRemote": True,
                        "publishedAt": "2026-07-09",
                        "compensationTierSummary": "$90K - $110K annually",
                    }
                ]
            },
        )

    with mock_client(handler) as client:
        result = AshbySource(client, board="example").scan()

    assert result.status is ScanStatus.SUCCEEDED
    item = result.items[0]
    assert item.remote is RemoteStatus.REMOTE
    assert item.salary is not None
    assert item.salary.maximum == Decimal("110000")


def test_workable_builds_public_job_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/widget/accounts/example/vacancies"
        return httpx.Response(
            200,
            request=request,
            json={
                "results": [
                    {
                        "shortcode": "ABC123",
                        "title": "Compliance Manager",
                        "location": {"city": "Austin", "region": "TX", "country": "US"},
                        "description": "Own compliance operations.",
                        "workplace_type": "on-site",
                        "application_deadline": "2026-07-31T23:59:00Z",
                    }
                ]
            },
        )

    with mock_client(handler) as client:
        result = WorkableSource(client, account="example", company="Example").scan()

    item = result.items[0]
    assert result.status is ScanStatus.SUCCEEDED
    assert item.url == "https://apply.workable.com/example/j/ABC123/"
    assert item.location == "Austin, TX, US"
    assert item.deadline is not None


def test_workday_posts_query_to_public_search_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/wday/cxs/acme/External/jobs"
        payload = json.loads(request.content)
        assert payload["searchText"] == "counsel"
        assert payload["limit"] == 20
        return httpx.Response(
            200,
            request=request,
            json={
                "total": 1,
                "jobPostings": [
                    {
                        "title": "Commercial Counsel",
                        "externalPath": "/job/California/Commercial-Counsel_R-100",
                        "locationsText": "San Francisco, CA",
                        "postedOn": "2026-07-01",
                        "bulletFields": ["Full time", "$150,000 - $190,000 annually"],
                    }
                ],
            },
        )

    with mock_client(handler) as client:
        result = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            company="Acme",
            host="jobs.acme.test",
        ).scan("counsel")

    assert result.status is ScanStatus.SUCCEEDED
    assert result.metadata["total"] == 1
    item = result.items[0]
    assert item.source_id == "R-100"
    assert item.url == "https://jobs.acme.test/job/California/Commercial-Counsel_R-100"
    assert item.salary is not None and item.salary.minimum == Decimal("150000")


def test_workday_focused_hydration_fetches_static_detail_and_preserves_identity() -> (
    None
):
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method == "POST":
            return httpx.Response(
                200,
                request=request,
                json={
                    "total": 1,
                    "jobPostings": [
                        {
                            "title": "AI Policy Counsel",
                            "externalPath": "/job/AI-Policy-Counsel_R-200",
                        }
                    ],
                },
            )
        return httpx.Response(
            200,
            request=request,
            json={
                "jobPostingInfo": {
                    "title": "AI Policy Counsel",
                    "jobDescription": "Advise on AI governance and patent policy.",
                    "location": "Remote - US",
                    "remoteType": "remote",
                }
            },
        )

    with mock_client(handler) as client:
        source = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            company="Acme",
            host="jobs.acme.test",
        )
        listing = source.scan("policy").items[0]
        hydrated = source.hydrate(listing)

    assert requests == [
        ("POST", "/wday/cxs/acme/External/jobs"),
        ("GET", "/wday/cxs/acme/External/job/AI-Policy-Counsel_R-200"),
    ]
    assert (hydrated.source, hydrated.source_id, hydrated.url) == (
        listing.source,
        listing.source_id,
        listing.url,
    )
    assert hydrated.description == "Advise on AI governance and patent policy."
    assert hydrated.location == "Remote - US"
    assert hydrated.metadata["hydrated"] is True


def test_workday_paginates_and_reports_a_capped_result_as_partial() -> None:
    offsets: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        offsets.append(payload["offset"])
        offset = payload["offset"]
        return httpx.Response(
            200,
            request=request,
            json={
                "total": 3,
                "jobPostings": [
                    {
                        "title": f"Counsel {offset}",
                        "externalPath": f"/job/Counsel_{offset}",
                    }
                ],
            },
        )

    with mock_client(handler) as client:
        result = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            company="Acme",
            host="jobs.acme.test",
            limit=1,
            max_pages=2,
        ).scan()

    assert offsets == [0, 1]
    assert len(result.items) == 2
    assert result.status is ScanStatus.PARTIAL
    assert result.metadata["truncated"] is True
    assert result.errors[0].code == "result_truncated"


def test_workday_missing_total_cannot_present_an_exhausted_page_cap_as_complete() -> (
    None
):
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        return httpx.Response(
            200,
            request=request,
            json={
                "jobPostings": [
                    {
                        "title": f"Counsel {payload['offset']}",
                        "externalPath": f"/job/Counsel_{payload['offset']}",
                    }
                ]
            },
        )

    with mock_client(handler) as client:
        result = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            host="jobs.acme.test",
            limit=1,
            max_pages=2,
        ).scan()

    assert len(result.items) == 2
    assert result.status is ScanStatus.PARTIAL
    assert result.metadata["truncated"] is True


def test_usajobs_missing_total_cannot_present_an_exhausted_page_cap_as_complete() -> (
    None
):
    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params["Page"]
        return httpx.Response(
            200,
            request=request,
            json={
                "SearchResult": {
                    "SearchResultItems": [
                        {
                            "MatchedObjectDescriptor": {
                                "PositionID": f"USA-{page}",
                                "PositionTitle": "Policy Analyst",
                                "PositionURI": f"https://www.usajobs.gov/job/{page}",
                                "OrganizationName": "Agency",
                            }
                        }
                    ]
                }
            },
        )

    with mock_client(handler) as client:
        result = USAJobsSource(
            client,
            api_key="key",
            email="person@example.test",
            results_per_page=1,
            max_pages=2,
        ).scan()

    assert len(result.items) == 2
    assert result.status is ScanStatus.PARTIAL
    assert result.metadata["truncated"] is True


def test_workday_preserves_earlier_pages_when_a_later_page_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["offset"]:
            return httpx.Response(503, request=request, text="temporary outage")
        return httpx.Response(
            200,
            request=request,
            json={
                "total": 3,
                "jobPostings": [
                    {
                        "title": "Policy Counsel",
                        "externalPath": "/job/Policy-Counsel_R-1",
                    }
                ],
            },
        )

    with mock_client(handler) as client:
        result = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            company="Acme",
            host="jobs.acme.test",
            limit=1,
            max_pages=3,
        ).scan()

    assert result.status is ScanStatus.PARTIAL
    assert [item.source_id for item in result.items] == ["R-1"]
    assert result.metadata["pages_fetched"] == 1
    assert result.metadata["truncated"] is True
    assert "503" in result.metadata["truncation_reason"]
    assert result.errors[0].code == "page_read_error"
    assert result.errors[0].retryable is True
    assert result.errors[0].http_status == 503


def test_workday_terminal_zero_total_never_contradicts_observed_rows() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["offset"]:
            return httpx.Response(
                200, request=request, json={"total": 0, "jobPostings": []}
            )
        return httpx.Response(
            200,
            request=request,
            json={
                "total": 1,
                "jobPostings": [
                    {
                        "title": "Counsel",
                        "externalPath": "/job/Counsel_R-1",
                    }
                ],
            },
        )

    with mock_client(handler) as client:
        result = WorkdaySource(
            client,
            tenant="acme",
            site="External",
            host="jobs.acme.test",
            limit=1,
        ).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert len(result.items) == 1
    assert result.metadata["total"] == 1


def test_usajobs_maps_deadline_agency_and_standard_credentials() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.headers["authorization-key"] == "test-key"
        assert request.headers["user-agent"] == "person@example.test"
        assert request.url.params["Keyword"] == "policy"
        return httpx.Response(
            200,
            request=request,
            json={
                "SearchResult": {
                    "SearchResultCountAll": 1,
                    "SearchResultItems": [
                        {
                            "MatchedObjectDescriptor": {
                                "PositionID": "USA-1",
                                "PositionTitle": "Policy Analyst",
                                "PositionURI": "https://www.usajobs.gov/job/123",
                                "OrganizationName": "Federal Energy Regulatory Commission",
                                "PositionLocation": [
                                    {"LocationName": "Washington, District of Columbia"}
                                ],
                                "PublicationStartDate": "2026-07-01T00:00:00Z",
                                "ApplicationCloseDate": "2026-07-20T23:59:59Z",
                                "PositionRemuneration": [
                                    {
                                        "MinimumRange": "117962",
                                        "MaximumRange": "153354",
                                        "RateIntervalCode": "Per Year",
                                        "Description": "USD",
                                    }
                                ],
                                "QualificationSummary": "One year of specialized experience.",
                                "UserArea": {
                                    "Details": {"JobSummary": "Analyze energy policy."}
                                },
                            }
                        }
                    ],
                }
            },
        )

    with mock_client(handler) as client:
        result = USAJobsSource(
            client,
            api_key="test-key",
            email="person@example.test",
            keyword="fallback",
        ).scan("policy")

    assert result.status is ScanStatus.SUCCEEDED
    item = result.items[0]
    assert item.company == "Federal Energy Regulatory Commission"
    assert item.deadline is not None
    assert item.salary is not None and item.salary.period is SalaryPeriod.YEAR
    assert "Analyze energy policy" in item.description


def test_usajobs_never_forwards_credentials_to_redirect_target() -> None:
    redirect_target_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "data.usajobs.gov":
            assert request.headers["authorization-key"] == "super-secret-key"
            assert request.headers["user-agent"] == "person@example.test"
            return httpx.Response(
                307,
                request=request,
                headers={"location": "https://collector.example.test/credentials"},
            )
        redirect_target_requests.append(request)
        return httpx.Response(200, request=request, json={})

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        result = USAJobsSource(
            client,
            api_key="super-secret-key",
            email="person@example.test",
        ).scan("policy")

    assert result.status is ScanStatus.FAILED
    assert result.errors[0].code == "malformed_response"
    assert "refusing to forward credentials" in result.errors[0].message
    assert "super-secret-key" not in result.errors[0].message
    assert "person@example.test" not in result.errors[0].message
    assert redirect_target_requests == []


def test_malformed_item_yields_explicit_partial_result() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            json={
                "jobs": [
                    {
                        "id": "good",
                        "title": "Counsel",
                        "absolute_url": "https://example.test/good",
                    },
                    {"id": "bad", "title": "Missing URL"},
                ]
            },
        )

    with mock_client(handler) as client:
        result = GreenhouseSource(client, board_token="example").scan()

    assert result.status is ScanStatus.PARTIAL
    assert len(result.items) == 1
    assert len(result.errors) == 1
    assert result.errors[0].code == "malformed_item"
    assert result.errors[0].item_index == 1


def test_source_failure_is_not_reported_as_an_empty_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request, json={"error": "unavailable"})

    with mock_client(handler) as client:
        result = LeverSource(client, site="example").scan()

    assert result.status is ScanStatus.FAILED
    assert result.items == ()
    assert result.errors[0].code == "http_error"
    assert result.errors[0].http_status == 503
    assert result.errors[0].retryable is True


def test_configured_response_byte_limit_is_enforced_before_json_parsing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, content=b"x" * 1_000_001)

    with mock_client(handler) as client:
        source = GreenhouseSource(client, board_token="example")
        source.configure_runtime(
            deadline_at=None,
            cancelled=lambda: False,
            max_response_bytes=1_000_000,
        )
        result = source.scan()

    assert result.status is ScanStatus.FAILED
    assert result.errors[0].code == "malformed_response"
    assert "configured safety limit" in result.errors[0].message


def test_streamed_response_limit_stops_before_the_remaining_body_is_buffered() -> None:
    yielded: list[int] = []

    class OversizedStream(httpx.SyncByteStream):
        def __iter__(self):
            for index in range(4):
                yielded.append(index)
                yield b"x" * 600_000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, stream=OversizedStream())

    with mock_client(handler) as client:
        source = GreenhouseSource(client, board_token="example")
        source.configure_runtime(
            deadline_at=None,
            cancelled=lambda: False,
            max_response_bytes=1_000_000,
        )
        result = source.scan()

    assert result.status is ScanStatus.FAILED
    assert "configured safety limit" in result.errors[0].message
    assert yielded == [0, 1]


def test_greenhouse_inventory_requests_metadata_without_description_content() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["content"] == "false"
        return httpx.Response(200, request=request, json={"jobs": []})

    with mock_client(handler) as client:
        source = GreenhouseSource(client, board_token="example")
        source.configure_runtime(
            deadline_at=None,
            cancelled=lambda: False,
            inventory_metadata_only=True,
        )
        result = source.scan()

    assert result.status is ScanStatus.SUCCEEDED


def test_valid_empty_source_is_a_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json={"jobs": []})

    with mock_client(handler) as client:
        result = GreenhouseSource(client, board_token="example").scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert result.items == ()
    assert result.errors == ()


@pytest.mark.parametrize(
    "host",
    (
        "http://127.0.0.1",
        "https://user:secret@jobs.example.test",
        "https://jobs.example.test/path",
        "https://jobs.example.test?redirect=elsewhere",
    ),
)
def test_workday_rejects_nonpublic_or_nonorigin_custom_hosts(host: str) -> None:
    with mock_client(lambda request: httpx.Response(500, request=request)) as client:
        with pytest.raises(ValueError, match="public HTTP.*origin"):
            WorkdaySource(client, tenant="acme", site="External", host=host)


def test_ats_requests_reject_redirects_even_when_shared_client_follows_them() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(
            302,
            request=request,
            headers={"location": "http://127.0.0.1/private"},
        )

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        result = GreenhouseSource(client, board_token="example").scan()

    assert result.status is ScanStatus.FAILED
    assert result.errors[0].code == "malformed_response"
    assert "unvalidated target" in result.errors[0].message
    assert len(requested) == 1


def test_request_timeout_is_capped_by_remaining_source_deadline() -> None:
    timeout_values: dict[str, float] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        timeout_values.update(request.extensions["timeout"])
        return httpx.Response(200, request=request, json={"jobs": []})

    with mock_client(handler) as client:
        source = GreenhouseSource(client, board_token="example")
        source.configure_runtime(
            deadline_at=time.monotonic() + 0.25,
            cancelled=lambda: False,
        )
        result = source.scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert timeout_values
    assert max(timeout_values.values()) <= 0.25


def test_greenhouse_metadata_listing_can_hydrate_full_static_detail() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/boards/example/jobs/42"
        return httpx.Response(
            200,
            request=request,
            json={
                "id": 42,
                "title": "Product Counsel",
                "absolute_url": "https://boards.example.test/jobs/42",
                "content": "Full legal product description.",
                "updated_at": "2026-07-14T10:00:00Z",
            },
        )

    listing = ScanItem(
        source="greenhouse:example",
        source_id="42",
        company="Example",
        title="Product Counsel",
        url="https://boards.example.test/jobs/42",
        metadata={"source_updated_at": "2026-07-14T09:00:00Z"},
    )
    with mock_client(handler) as client:
        hydrated = GreenhouseSource(
            client, board_token="example", company="Example"
        ).hydrate(listing)

    assert hydrated.description == "Full legal product description."
    assert hydrated.metadata["hydrated"] is True
    assert hydrated.metadata["source_updated_at"] == "2026-07-14T10:00:00Z"


def test_scan_item_rejects_control_characters_in_exact_launch_url() -> None:
    with pytest.raises(ValueError, match="control characters"):
        ScanItem(
            source="greenhouse:example",
            source_id="42",
            company="Example",
            title="Counsel",
            url="https://jobs.example.test/42\nX-Injected: true",
        )
