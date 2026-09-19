"""
Tests for the process_jobs() pipeline.

process_jobs mutates two external data structures (seen set, new_jobs list)
and returns only the jobs found for the current company call.
"""

from datetime import date

import job_monitor


def make_raw(title="Patent Counsel", url="https://example.com/job/1", id="001"):
    return {"title": title, "url": url, "id": id}


def test_uid_format():
    """UID is source_prefix + '_' + job id."""
    seen, new_jobs = set(), []
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert result[0]["uid"] == "gh_anthropic_001"


def test_source_extracted_from_prefix():
    """Source is the first segment of source_prefix (before the first underscore)."""
    seen, new_jobs = set(), []
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert result[0]["source"] == "gh"


def test_excluded_jobs_not_added():
    """Jobs whose titles match EXCLUDE_PATTERNS are silently dropped."""
    seen, new_jobs = set(), []
    result = job_monitor.process_jobs(
        [make_raw(title="Senior Director of Legal Affairs")],
        "gh_anthropic",
        "Anthropic",
        seen,
        new_jobs,
        0,
    )
    assert result == []
    assert new_jobs == []


def test_mutates_seen_set():
    """The seen set is updated with the new job's UID."""
    seen, new_jobs = set(), []
    job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert "gh_anthropic_001" in seen


def test_mutates_new_jobs_list():
    """The global new_jobs list grows by one for each accepted job."""
    seen, new_jobs = set(), []
    job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert len(new_jobs) == 1


def test_already_seen_jobs_not_readded():
    """Jobs whose UID is already in the seen set are skipped entirely."""
    seen = {"gh_anthropic_001"}
    new_jobs = []
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert result == []
    assert new_jobs == []


def test_min_score_filter():
    """Jobs scoring below min_score are not added even if not excluded."""
    seen, new_jobs = set(), []
    # "Administrative Assistant" scores 0
    result = job_monitor.process_jobs(
        [make_raw(title="Administrative Assistant")],
        "gh_anthropic",
        "Anthropic",
        seen,
        new_jobs,
        min_score=1,
    )
    assert result == []


def test_found_date_is_today():
    """The 'found' field is today's date as an ISO string."""
    seen, new_jobs = set(), []
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert result[0]["found"] == str(date.today())


def test_returns_company_hits_not_global_new_jobs():
    """
    Return value is scoped to this company call only.
    The global new_jobs list may contain prior entries — they are NOT in the return value.
    """
    pre_existing = {
        "uid": "lever_spotify_999",
        "company": "Spotify",
        "title": "Other Job",
        "url": "",
        "score": 0,
        "matched_keywords": [],
        "source": "lever",
        "found": "2026-01-01",
    }
    seen, new_jobs = set(), [pre_existing]
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )

    assert len(result) == 1
    assert result[0]["company"] == "Anthropic"
    assert len(new_jobs) == 2  # global list includes pre-existing + new


def test_close_date_passed_through():
    """close_date from the raw job dict is included in the output entry."""
    seen, new_jobs = set(), []
    raw = [
        {
            "title": "Patent Counsel",
            "url": "https://example.com/1",
            "id": "001",
            "close_date": "2026-05-01",
        }
    ]
    result = job_monitor.process_jobs(raw, "usa", "USPTO", seen, new_jobs, 0)
    assert result[0].get("close_date") == "2026-05-01"


def test_no_close_date_field_when_absent():
    """When close_date is not in the raw job, the key is absent from the output entry."""
    seen, new_jobs = set(), []
    result = job_monitor.process_jobs(
        [make_raw()], "gh_anthropic", "Anthropic", seen, new_jobs, 0
    )
    assert "close_date" not in result[0]
