from __future__ import annotations

import httpx

from jobby.normalization import RemoteStatus
from jobby.sources.ats import ICIMSSource, JibeSource
from jobby.sources.base import ScanStatus


def _card(job_id: int, title: str, location: str, extra: str = "") -> str:
    return f"""
    <li class="iCIMS_JobCardItem"><div class="row">
      <div class="col-xs-12 title">
        <a href="https://careers-acme.icims.com/jobs/{job_id}/slug/job?in_iframe=1"
           class="iCIMS_Anchor" title="{job_id} - {title}">
          <span class="sr-only field-label">Title</span><h3>{title}</h3></a>
      </div>
      <div class="col-xs-12 description">Summary for {title}&hellip;</div>
      <div class="col-xs-12 additionalFields"><dl class="iCIMS_JobHeaderGroup">
        <div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">
          <span class="sr-only field-label">Location : Location</span></dt>
          <dd class="iCIMS_JobHeaderData"><span>{location}</span></dd></div>
        {extra}
      </dl></div>
    </div></li>"""


PAY = """
<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Work Arrangement</dt>
  <dd class="iCIMS_JobHeaderData"><span>Hybrid</span></dd></div>
<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Posted Min Pay Rate</dt>
  <dd class="iCIMS_JobHeaderData"><span>USD $75,000.00/Yr.</span></dd></div>
<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Posted Max Pay Rate</dt>
  <dd class="iCIMS_JobHeaderData"><span>USD $95,000.00/Yr.</span></dd></div>
<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Department</dt>
  <dd class="iCIMS_JobHeaderData"><span>Office of the General Counsel</span></dd></div>
"""


def test_icims_reads_iframe_job_cards_across_pages() -> None:
    pages = {
        "0": f"""<html><body><ul>{_card(11076, "OGC Analyst &ndash; Conflicts", "US-CA-San Diego", PAY)}
             {_card(11075, "Paralegal", "US-NY-New York")}</ul>
             <a href="https://careers-acme.icims.com/jobs/search?pr=1&amp;in_iframe=1">Next</a>
             </body></html>""",
        "1": f"<html><body><ul>{_card(11070, 'Knowledge Manager', 'US-DC-Washington')}</ul></body></html>",
    }
    requested: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url)
        assert request.url.params["in_iframe"] == "1"
        return httpx.Response(
            200,
            text=pages[request.url.params["pr"]],
            headers={"content-type": "text/html"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = ICIMSSource(
            client, base_url="https://careers-acme.icims.com", company="Acme"
        ).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.source_id for item in result.items] == ["11076", "11075", "11070"]
    first = result.items[0]
    assert first.title == "OGC Analyst – Conflicts"
    assert first.url == "https://careers-acme.icims.com/jobs/11076/slug/job"
    assert first.location == "US-CA-San Diego"
    assert first.salary_text == "USD $75,000.00/Yr. - USD $95,000.00/Yr."
    assert first.salary is not None
    assert first.remote is RemoteStatus.HYBRID
    assert first.metadata["department"] == "Office of the General Counsel"
    assert result.metadata["total"] == 3
    assert len(requested) == 2


def test_icims_reports_handoff_to_employer_career_site() -> None:
    body = "<script>window.top.location.href = 'https:\\/\\/careers.acme.com\\/jobs';</script>"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=body, headers={"content-type": "text/html"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = ICIMSSource(
            client, base_url="https://careers-acme.icims.com", company="Acme"
        ).scan()

    assert result.status is ScanStatus.FAILED
    assert "https://careers.acme.com/jobs" in result.errors[0].message


def _jibe_job(slug: str, title: str, **extra: object) -> dict[str, object]:
    return {
        "data": {
            "slug": slug,
            "req_id": slug,
            "title": title,
            "description": "<p>Draft <b>licenses</b>.</p>",
            "full_location": "Remote (US), United States",
            "location_type": "LAT_LNG",
            "posted_date": "2026-09-23T14:50:00+0000",
            "employment_type": "FULL_TIME",
            "categories": [{"name": "Legal"}],
            "apply_url": f"https://careers-acme.icims.com/jobs/{slug}/login",
            "salary_min_value": 0,
            "salary_max_value": 0,
            **extra,
        }
    }


def test_jibe_pages_through_the_public_feed() -> None:
    pages = {
        "1": {
            "jobs": [
                _jibe_job(
                    "7",
                    "Licensing Specialist",
                    salary_min_value=90000,
                    salary_max_value=120000,
                )
            ],
            "totalCount": 2,
        },
        "2": {"jobs": [_jibe_job("8", "Policy Counsel")], "totalCount": 2},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/jobs"
        return httpx.Response(200, json=pages[request.url.params["page"]])

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = JibeSource(
            client,
            base_url="https://careers.acme.com",
            jobs_path="/careers",
            company="Acme",
        ).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.title for item in result.items] == [
        "Licensing Specialist",
        "Policy Counsel",
    ]
    first, second = result.items
    assert first.url == "https://careers.acme.com/careers/jobs/7"
    assert first.description == "Draft licenses ."
    assert first.remote is RemoteStatus.REMOTE
    assert first.salary_text == "90,000 - 120,000" and first.salary is not None
    assert second.salary_text == "" and second.salary is None
    assert first.posted_at is not None and first.posted_at.day == 23
    assert first.metadata["categories"] == ["Legal"]
    assert result.metadata["total"] == 2


def test_jibe_marks_truncation_when_page_budget_runs_out() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params["page"]
        return httpx.Response(
            200, json={"jobs": [_jibe_job(page, f"Role {page}")], "totalCount": 5}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = JibeSource(
            client, base_url="https://careers.acme.com", company="Acme", max_pages=2
        ).scan()

    assert result.status is ScanStatus.PARTIAL
    assert len(result.items) == 2


def test_jibe_rejects_unsafe_origins() -> None:
    import pytest

    for base in (
        "http://careers.acme.com",
        "https://careers.acme.com/jobs",
        "https://127.0.0.1",
    ):
        with pytest.raises(ValueError):
            JibeSource(httpx.Client(), base_url=base, company="Acme")
