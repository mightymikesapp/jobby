"""
Contract tests for all HTTP fetchers, parametrized over FETCHER_REGISTRY.
Dedicated test classes for the three non-standard fetchers:
  - Workday (POST, multi-term, dedup)
  - Jobvite (dual response format, URL-extracted fallback IDs)
  - USAJobs (env-dependent, returns raw API items without normalization)
"""

import pytest
import requests
from unittest.mock import MagicMock, patch

import job_monitor
from tests.conftest import FETCHER_REGISTRY


# ── CONTRACT TESTS (parametrized over all registry entries) ───────────────────


@pytest.mark.parametrize(
    "func_name,args,mock_path,mock_response,expected", FETCHER_REGISTRY
)
def test_fetcher_contract_happy_path(
    func_name, args, mock_path, mock_response, expected
):
    """Every fetcher must return a list of dicts with string title/url/id on 200."""
    func = getattr(job_monitor, func_name)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = mock_response
    mock_resp.raise_for_status.return_value = None

    with patch(mock_path, return_value=mock_resp):
        result = func(*args)

    assert isinstance(result, list)
    assert len(result) == len(expected)
    for item, exp in zip(result, expected):
        assert isinstance(item["title"], str)
        assert isinstance(item["url"], str)
        assert isinstance(item["id"], str)
        assert item["title"] == exp["title"]
        assert item["url"] == exp["url"]
        assert item["id"] == exp["id"]


@pytest.mark.parametrize(
    "func_name,args,mock_path,mock_response,expected", FETCHER_REGISTRY
)
@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 422, 500, 503])
def test_fetcher_contract_http_errors_return_empty(
    func_name, args, mock_path, mock_response, expected, status_code
):
    """Every fetcher must return [] and never raise on any HTTP error status."""
    func = getattr(job_monitor, func_name)
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

    with patch(mock_path, return_value=mock_resp):
        result = func(*args)

    assert result == []


@pytest.mark.parametrize(
    "func_name,args,mock_path,mock_response,expected", FETCHER_REGISTRY
)
def test_fetcher_contract_network_error_returns_empty(
    func_name, args, mock_path, mock_response, expected
):
    """Every fetcher must return [] and never raise on a network-level error."""
    func = getattr(job_monitor, func_name)

    with patch(mock_path, side_effect=requests.ConnectionError("network error")):
        result = func(*args)

    assert result == []


# ── WORKDAY (POST + multi-term + dedup) ───────────────────────────────────────


class TestFetchWorkday:
    """fetch_workday uses POST, iterates over WORKDAY_SEARCH_TERMS, and deduplicates by job ID."""

    def _ok_response(self, jobs_data):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"jobPostings": jobs_data}
        mock_resp.raise_for_status.return_value = None
        return mock_resp

    def _error_response(self, status_code=404):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        return mock_resp

    def _empty_response(self):
        return self._ok_response([])

    def test_deduplication_across_search_terms(self):
        """Same job returned by multiple search terms should appear exactly once."""
        job = {"title": "Legal Counsel", "externalPath": "/job/Legal-Counsel_JR1234"}
        with patch("job_monitor.requests.post", return_value=self._ok_response([job])):
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert len(result) == 1
        assert result[0]["title"] == "Legal Counsel"

    def test_id_extraction_standard_path(self):
        """Job ID is the segment after the last underscore in the external path."""
        job = {"title": "Legal Counsel", "externalPath": "/job/Legal-Counsel_JR9999"}
        n = len(job_monitor.WORKDAY_SEARCH_TERMS)
        responses = [self._ok_response([job])] + [self._empty_response()] * (n - 1)
        with patch("job_monitor.requests.post", side_effect=responses):
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert result[0]["id"] == "JR9999"

    def test_id_extraction_path_without_underscore(self):
        """When no underscore in path, job_id falls back to the full path string."""
        job = {"title": "Legal Counsel", "externalPath": "/jobs/nounderscore"}
        n = len(job_monitor.WORKDAY_SEARCH_TERMS)
        responses = [self._ok_response([job])] + [self._empty_response()] * (n - 1)
        with patch("job_monitor.requests.post", side_effect=responses):
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert result[0]["id"] == "/jobs/nounderscore"

    def test_404_breaks_all_terms(self):
        """404 aborts all remaining search terms — board does not exist."""
        with patch(
            "job_monitor.requests.post", return_value=self._error_response(404)
        ) as mock_post:
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert mock_post.call_count == 1
        assert result == []

    def test_non_404_http_error_continues_to_next_term(self):
        """Non-404 errors (500, 503) are transient — remaining terms are still tried."""
        n = len(job_monitor.WORKDAY_SEARCH_TERMS)
        with patch(
            "job_monitor.requests.post", return_value=self._error_response(500)
        ) as mock_post:
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert mock_post.call_count == n
        assert result == []

    def test_connection_error_continues_to_next_term(self):
        """A connection error on one term does not abort the remaining terms."""
        n = len(job_monitor.WORKDAY_SEARCH_TERMS)
        with patch(
            "job_monitor.requests.post", side_effect=requests.ConnectionError("timeout")
        ) as mock_post:
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert mock_post.call_count == n
        assert result == []

    def test_url_constructed_from_base_and_path(self):
        """URL in output should be base_url + externalPath."""
        path = "/job/Legal-Counsel_JR1234"
        job = {"title": "Legal Counsel", "externalPath": path}
        n = len(job_monitor.WORKDAY_SEARCH_TERMS)
        responses = [self._ok_response([job])] + [self._empty_response()] * (n - 1)
        with patch("job_monitor.requests.post", side_effect=responses):
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        expected_url = f"https://nvidia.wd5.myworkdayjobs.com{path}"
        assert result[0]["url"] == expected_url

    def test_empty_path_skipped(self):
        """Jobs with externalPath='' produce an empty job_id and are filtered out."""
        job = {"title": "Legal Counsel", "externalPath": ""}
        with patch("job_monitor.requests.post", return_value=self._ok_response([job])):
            result = job_monitor.fetch_workday(
                "nvidia", "wd5", "NVIDIAExternalCareerSite"
            )
        assert result == []


# ── JOBVITE (dual response format + URL-extracted fallback IDs) ───────────────


class TestFetchJobvite:
    """fetch_jobvite handles both dict and top-level list API responses."""

    def _mock_get(self, payload):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        mock_resp.raise_for_status.return_value = None
        return mock_resp

    def test_dict_response_format(self):
        """Standard format: {"jobs": [...]} with jobId and jobUrl fields."""
        data = {
            "jobs": [
                {
                    "title": "IP Manager",
                    "jobUrl": "https://example.com/1",
                    "jobId": "jv-001",
                }
            ]
        }
        with patch("job_monitor.requests.get", return_value=self._mock_get(data)):
            result = job_monitor.fetch_jobvite("capcomusa")
        assert len(result) == 1
        assert result[0]["title"] == "IP Manager"
        assert result[0]["url"] == "https://example.com/1"
        assert result[0]["id"] == "jv-001"

    def test_list_response_format(self):
        """Alternate format: top-level list (some Jobvite boards respond this way)."""
        data = [
            {
                "title": "IP Manager",
                "jobUrl": "https://example.com/1",
                "jobId": "jv-001",
            }
        ]
        with patch("job_monitor.requests.get", return_value=self._mock_get(data)):
            result = job_monitor.fetch_jobvite("capcomusa")
        assert len(result) == 1
        assert result[0]["id"] == "jv-001"

    def test_fallback_id_extracted_from_url(self):
        """When jobId is absent, ID is extracted from the last URL path segment."""
        data = {
            "jobs": [
                {
                    "title": "Patent Analyst",
                    "jobUrl": "https://jobs.jobvite.com/capcomusa/job/abc123",
                }
            ]
        }
        with patch("job_monitor.requests.get", return_value=self._mock_get(data)):
            result = job_monitor.fetch_jobvite("capcomusa")
        assert result[0]["id"] == "abc123"

    def test_fallback_id_when_no_url(self):
        """When both jobId and URL are absent, fallback is idx_{i}."""
        data = {"jobs": [{"title": "Patent Analyst"}]}
        with patch("job_monitor.requests.get", return_value=self._mock_get(data)):
            result = job_monitor.fetch_jobvite("capcomusa")
        assert result[0]["id"] == "idx_0"

    def test_unexpected_format_returns_empty(self):
        """Non-list, non-dict jobs payload (e.g. nested dict) returns []."""
        data = {"jobs": {"not": "a list"}}
        with patch("job_monitor.requests.get", return_value=self._mock_get(data)):
            result = job_monitor.fetch_jobvite("capcomusa")
        assert result == []


# ── USAJOBS (env-dependent, raw output) ───────────────────────────────────────


class TestFetchUsajobs:
    """fetch_usajobs is gated on USAJOBS_API_KEY and returns raw API items without normalization."""

    def test_returns_empty_when_no_api_key(self, monkeypatch):
        """When USAJOBS_API_KEY is empty, returns [] without making any network call."""
        monkeypatch.setattr(job_monitor, "USAJOBS_API_KEY", "")
        with patch("job_monitor.requests.get") as mock_get:
            result = job_monitor.fetch_usajobs("patent attorney")
        mock_get.assert_not_called()
        assert result == []

    def test_returns_raw_search_result_items(self, monkeypatch):
        """Output is raw MatchedObjectDescriptor dicts — NOT normalized to title/url/id."""
        monkeypatch.setattr(job_monitor, "USAJOBS_API_KEY", "test-key-123")
        raw_items = [
            {
                "MatchedObjectDescriptor": {
                    "PositionTitle": "Patent Attorney",
                    "PositionID": "PA-001",
                }
            },
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"SearchResult": {"SearchResultItems": raw_items}}
        mock_resp.raise_for_status.return_value = None
        with patch("job_monitor.requests.get", return_value=mock_resp):
            result = job_monitor.fetch_usajobs("patent attorney")
        assert result == raw_items
        assert "MatchedObjectDescriptor" in result[0]

    def test_location_param_is_optional(self, monkeypatch):
        """LocationName param is present when location is given, absent when omitted."""
        monkeypatch.setattr(job_monitor, "USAJOBS_API_KEY", "test-key-123")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"SearchResult": {"SearchResultItems": []}}
        mock_resp.raise_for_status.return_value = None

        with patch("job_monitor.requests.get", return_value=mock_resp) as mock_get:
            job_monitor.fetch_usajobs("patent", location="San Diego, California")
            params_with = mock_get.call_args.kwargs["params"]
            assert "LocationName" in params_with
            assert params_with["LocationName"] == "San Diego, California"

            job_monitor.fetch_usajobs("patent")
            params_without = mock_get.call_args.kwargs["params"]
            assert "LocationName" not in params_without
