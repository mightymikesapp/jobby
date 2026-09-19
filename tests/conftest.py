"""
Shared fixtures and the FETCHER_REGISTRY that drives all contract tests.

To add a new ATS fetcher to the contract test suite:
  1. Write fetch_newats(slug) in job_monitor.py
  2. Add one pytest.param(...) entry to FETCHER_REGISTRY below
  3. All three contract tests in test_fetchers.py run automatically — no new test functions needed
"""

from datetime import date
import os
from pathlib import Path
import sys

import pytest


# Every test process is offline by construction. HTTPX MockTransport and
# requests mocks never reach this layer; Unix sockets and IP loopback remain
# available for isolated local services. Child Python processes inherit a
# sitecustomize hook through PYTHONPATH so a subprocess cannot bypass the gate.
OFFLINE_SUPPORT = Path(__file__).with_name("offline_support").absolute()
if str(OFFLINE_SUPPORT) not in sys.path:
    sys.path.insert(0, str(OFFLINE_SUPPORT))
from offline_guard import install_offline_network_guard  # noqa: E402

install_offline_network_guard()
os.environ["JOBBY_TEST_OFFLINE"] = "1"
python_path = os.environ.get("PYTHONPATH", "")
python_entries = [entry for entry in python_path.split(os.pathsep) if entry]
if str(OFFLINE_SUPPORT) not in python_entries:
    os.environ["PYTHONPATH"] = os.pathsep.join([str(OFFLINE_SUPPORT), *python_entries])


# ── FETCHER REGISTRY ──────────────────────────────────────────────────────────
# Each entry: (func_name, args, mock_path, mock_response_json, expected_output)

FETCHER_REGISTRY = [
    pytest.param(
        "fetch_greenhouse",
        ("anthropic",),
        "job_monitor.requests.get",
        {
            "jobs": [
                {
                    "title": "IP Counsel",
                    "absolute_url": "https://example.com/job/1",
                    "id": 99,
                }
            ]
        },
        [{"title": "IP Counsel", "url": "https://example.com/job/1", "id": "99"}],
        id="greenhouse",
    ),
    pytest.param(
        "fetch_lever",
        ("spotify",),
        "job_monitor.requests.get",
        [
            {
                "text": "Policy Analyst",
                "hostedUrl": "https://example.com/job/2",
                "id": "lev-001",
            }
        ],
        [
            {
                "title": "Policy Analyst",
                "url": "https://example.com/job/2",
                "id": "lev-001",
            }
        ],
        id="lever",
    ),
    pytest.param(
        "fetch_ashby",
        ("somecompany",),
        "job_monitor.requests.get",
        {
            "jobs": [
                {
                    "title": "Patent Specialist",
                    "jobUrl": "https://example.com/job/3",
                    "id": "ash-001",
                }
            ]
        },
        [
            {
                "title": "Patent Specialist",
                "url": "https://example.com/job/3",
                "id": "ash-001",
            }
        ],
        id="ashby",
    ),
    pytest.param(
        "fetch_workable",
        ("huggingface",),
        "job_monitor.requests.get",
        {"results": [{"title": "Legal Ops", "shortcode": "ABC123"}]},
        [
            {
                "title": "Legal Ops",
                "url": "https://apply.workable.com/huggingface/j/ABC123/",
                "id": "ABC123",
            }
        ],
        id="workable",
    ),
    pytest.param(
        "fetch_jobvite",
        ("capcomusa",),
        "job_monitor.requests.get",
        {
            "jobs": [
                {
                    "title": "IP Manager",
                    "jobUrl": "https://example.com/job/5",
                    "jobId": "jv-001",
                }
            ]
        },
        [{"title": "IP Manager", "url": "https://example.com/job/5", "id": "jv-001"}],
        id="jobvite",
    ),
]


# ── SHARED FIXTURES ───────────────────────────────────────────────────────────


@pytest.fixture
def raw_job():
    """Minimal normalized job dict as returned by any fetcher."""
    return {"title": "Patent Counsel", "url": "https://example.com/job/1", "id": "001"}


@pytest.fixture
def processed_job():
    """Full post-process_jobs entry with all expected fields."""
    return {
        "uid": "gh_anthropic_001",
        "company": "Anthropic",
        "title": "Patent Counsel",
        "url": "https://example.com/job/1",
        "score": 2,
        "matched_keywords": ["patent", "counsel"],
        "source": "gh",
        "found": str(date.today()),
    }


@pytest.fixture
def sample_job():
    """Typical high-scoring job for output tests."""
    return {
        "uid": "gh_anthropic_001",
        "company": "Anthropic",
        "title": "IP Counsel",
        "url": "https://boards.greenhouse.io/anthropic/jobs/001",
        "score": 2,
        "matched_keywords": ["IP", "counsel"],
        "source": "gh",
        "found": str(date.today()),
    }
