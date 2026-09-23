from __future__ import annotations

import json
from collections.abc import Callable, Mapping

import httpx
import pytest

from jobby.config import AppConfig
from jobby.discovery_service import _configured_board_keys
from jobby.normalization import RemoteStatus
from jobby.sources.base import ScanStatus
from jobby.sources.browser import PortalConfig
from jobby.sources.crawler import CareerCrawlerSource, CrawlLimits


SEED = "https://careers.example.test/jobs"
PUBLIC = [(2, 1, 6, "", ("93.184.216.34", 443))]


def html_response(body: str, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status, headers={"content-type": "text/html; charset=utf-8"}, text=body
    )


def text_response(body: str, content_type: str = "text/plain") -> httpx.Response:
    return httpx.Response(200, headers={"content-type": content_type}, text=body)


class Site:
    """Route mock requests by URL and record the order they were made in."""

    def __init__(
        self, routes: Mapping[str, httpx.Response | Callable[[], httpx.Response]]
    ):
        self.routes = dict(routes)
        self.requested: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.requested.append(url)
        route = self.routes.get(url)
        if route is None:
            return httpx.Response(404, headers={"content-type": "text/html"})
        return route() if callable(route) else route

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def crawler(
    site: Site,
    *,
    limits: CrawlLimits | None = None,
    delegate_site: Site | None = None,
    **kwargs,
) -> CareerCrawlerSource:
    return CareerCrawlerSource(
        PortalConfig(name="Example", url=SEED),
        limits=limits or CrawlLimits(request_delay_seconds=0),
        client_factory=site.client,
        host_resolver=lambda *_args, **_kwargs: PUBLIC,
        delegate_client_factory=(delegate_site or Site({})).client,
        **kwargs,
    )


def ld_json(payload: object) -> str:
    return f'<script type="application/ld+json">{json.dumps(payload)}</script>'


def test_json_ld_postings_are_mapped_and_outrank_matching_anchors() -> None:
    postings = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "itemListElement": [
            {
                "@type": "ListItem",
                "item": {
                    "@type": "JobPosting",
                    "title": "Legal Counsel, AI Policy",
                    "url": "/jobs/101",
                    "description": "&lt;p&gt;Advise on &lt;b&gt;AI governance&lt;/b&gt;.&lt;/p&gt;",
                    "datePosted": "2026-09-01",
                    "validThrough": "2026-10-15T23:59:00Z",
                    "employmentType": ["FULL_TIME"],
                    "hiringOrganization": {
                        "@type": "Organization",
                        "name": "Example Inc",
                    },
                    "jobLocation": {
                        "@type": "Place",
                        "address": {
                            "@type": "PostalAddress",
                            "addressLocality": "San Diego",
                            "addressRegion": "CA",
                            "addressCountry": {"@type": "Country", "name": "US"},
                        },
                    },
                    "baseSalary": {
                        "@type": "MonetaryAmount",
                        "currency": "USD",
                        "value": {
                            "@type": "QuantitativeValue",
                            "minValue": 158000,
                            "maxValue": 237000,
                            "unitText": "YEAR",
                        },
                    },
                },
            },
            {
                "@type": "ListItem",
                "item": {
                    "@type": "JobPosting",
                    "title": "Staff Software Engineer",
                    "url": "/jobs/102",
                },
            },
            {
                "@type": "ListItem",
                "item": {
                    "@type": "JobPosting",
                    "title": "Privacy Counsel",
                    "url": "/jobs/103",
                    "jobLocationType": "TELECOMMUTE",
                    "applicantLocationRequirements": {
                        "@type": "Country",
                        "name": "USA",
                    },
                },
            },
        ],
    }
    seed = f"""<html><head>{ld_json(postings)}</head><body>
      <a href="/jobs/101?utm_source=list">Legal Counsel (AI Policy)</a>
      <a href="/jobs/104">Compliance Analyst</a>
    </body></html>"""
    site = Site({SEED: html_response(seed)})

    result = crawler(site, limits=CrawlLimits(max_pages=1)).scan()

    assert result.status is ScanStatus.SUCCEEDED
    by_url = {item.url.rsplit("/", 1)[-1]: item for item in result.items}
    assert set(by_url) == {"101", "103", "104"}
    counsel = by_url["101"]
    assert counsel.title == "Legal Counsel, AI Policy"
    assert counsel.metadata["extraction"] == "json_ld_job_posting"
    assert counsel.description == "Advise on AI governance ."
    assert counsel.location == "San Diego, CA, US"
    assert counsel.salary_text == "USD 158,000-237,000 per year"
    assert counsel.salary is not None
    assert counsel.posted_at is not None and counsel.posted_at.isoformat().startswith(
        "2026-09-01"
    )
    assert counsel.deadline is not None and counsel.deadline.day == 15
    assert counsel.metadata["hiring_organization"] == "Example Inc"
    assert counsel.company == "Example"
    remote = by_url["103"]
    assert remote.remote is RemoteStatus.REMOTE
    assert remote.location == "Remote (USA)"
    assert by_url["104"].metadata["extraction"] == "static_html_anchor"
    assert result.metadata["structured_postings"] == 2
    # max_pages=1 is the legacy single-page read: no robots, no sitemap.
    assert site.requested == [SEED]


def test_crawl_follows_pagination_and_listing_links_within_robots_and_scope() -> None:
    seed = """<html><head><link rel="next" href="/jobs?page=2"></head><body>
      <a href="/jobs/1">Patent Agent</a>
      <a href="/jobs/2">Software Engineer</a>
      <a href="/departments">Departments</a>
      <a href="/private/jobs">View all jobs</a>
      <a href="https://elsewhere.example.org/jobs">All jobs</a>
      <a href="/about-us">About us</a>
      <a href="/brochure.pdf">Careers</a>
    </body></html>"""
    page_two = """<html><body>
      <a href="/jobs/3">Trademark Paralegal</a>
      <a href="/jobs?page=3">3</a>
    </body></html>"""
    page_three = '<html><body><a href="/jobs/4">Policy Analyst</a></body></html>'
    departments = '<html><body><a href="/jobs/5">Regulatory Counsel</a></body></html>'
    robots = "User-agent: *\nDisallow: /private\n"
    site = Site(
        {
            SEED: html_response(seed),
            "https://careers.example.test/robots.txt": text_response(robots),
            "https://careers.example.test/jobs?page=2": html_response(page_two),
            "https://careers.example.test/jobs?page=3": html_response(page_three),
            "https://careers.example.test/departments": html_response(departments),
        }
    )

    result = crawler(site).scan()

    assert result.status is ScanStatus.SUCCEEDED
    titles = sorted(item.title for item in result.items)
    assert titles == [
        "Patent Agent",
        "Policy Analyst",
        "Regulatory Counsel",
        "Trademark Paralegal",
    ]
    assert "https://careers.example.test/private/jobs" not in site.requested
    assert not any("elsewhere" in url or "about-us" in url for url in site.requested)
    assert not any(url.endswith(".pdf") for url in site.requested)
    # Pagination is read before other listing links.
    assert site.requested.index(
        "https://careers.example.test/jobs?page=2"
    ) < site.requested.index("https://careers.example.test/departments")
    assert result.metadata["robots"] == "loaded"
    assert result.metadata["robots_blocked"] == 1
    assert result.metadata["stop_reason"] == "complete"
    assert all(item.source == "portal:example" for item in result.items)


def test_depth_and_page_budget_bound_the_crawl() -> None:
    def listing(n: int) -> httpx.Response:
        return html_response(
            f'<html><body><a href="/jobs?page={n + 1}">Next</a></body></html>'
        )

    routes: dict[str, httpx.Response] = {SEED: listing(1)}
    for n in range(2, 10):
        routes[f"https://careers.example.test/jobs?page={n}"] = listing(n)
    routes["https://careers.example.test/robots.txt"] = text_response("")
    site = Site(routes)

    result = crawler(
        site, limits=CrawlLimits(max_pages=3, max_depth=5, request_delay_seconds=0)
    ).scan()

    pages = [
        url for url in site.requested if "robots" not in url and "sitemap" not in url
    ]
    assert len(pages) == 3
    assert result.metadata["stop_reason"] == "page_budget"

    shallow = Site(routes)
    crawler(shallow, limits=CrawlLimits(max_depth=1, request_delay_seconds=0)).scan()
    pages = [
        url for url in shallow.requested if "robots" not in url and "sitemap" not in url
    ]
    assert pages == [SEED, "https://careers.example.test/jobs?page=2"]


def test_unavailable_robots_limits_the_crawl_to_the_configured_page() -> None:
    seed = """<html><body>
      <a href="/jobs/1">Copyright Counsel</a>
      <a href="/jobs?page=2">Next</a>
    </body></html>"""
    site = Site(
        {
            SEED: html_response(seed),
            "https://careers.example.test/robots.txt": httpx.Response(503),
        }
    )

    result = crawler(site).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.title for item in result.items] == ["Copyright Counsel"]
    assert site.requested == [SEED, "https://careers.example.test/robots.txt"]
    assert result.metadata["robots"] == "unavailable"
    assert result.metadata["robots_blocked"] == 1


def test_sitemaps_supply_job_detail_pages_matched_by_url_slug() -> None:
    robots = (
        "User-agent: *\nAllow: /\n"
        "Sitemap: https://careers.example.test/sitemap_index.xml\n"
    )
    index = """<?xml version="1.0"?><sitemapindex>
      <sitemap><loc>https://careers.example.test/sitemaps/pages.xml</loc></sitemap>
      <sitemap><loc>https://careers.example.test/sitemaps/jobs.xml</loc></sitemap>
      <sitemap><loc>https://cdn.example.org/sitemaps/other.xml</loc></sitemap>
    </sitemapindex>"""
    jobs = """<?xml version="1.0"?><urlset>
      <url><loc>https://careers.example.test/jobs/123-senior-patent-counsel</loc></url>
      <url><loc>https://careers.example.test/jobs/456-software-engineer</loc></url>
      <url><loc><![CDATA[https://careers.example.test/jobs/789-ai-policy-fellow?a=1&amp;b=2]]></loc></url>
    </urlset>"""
    detail_heading = """<html><body><h1>Senior Patent <em>Counsel</em></h1>
      <p>Prosecute patents.</p></body></html>"""
    detail_ld = (
        "<html><head>"
        + ld_json(
            {
                "@type": "JobPosting",
                "title": "AI Policy Fellow",
                "description": "Research.",
            }
        )
        + "</head><body><h1>Ignored heading</h1></body></html>"
    )
    site = Site(
        {
            SEED: html_response("<html><body><div id='app'></div></body></html>"),
            "https://careers.example.test/robots.txt": text_response(robots),
            "https://careers.example.test/sitemap_index.xml": text_response(
                index, "application/xml"
            ),
            "https://careers.example.test/sitemaps/pages.xml": text_response(
                "<urlset></urlset>", "application/xml"
            ),
            "https://careers.example.test/sitemaps/jobs.xml": text_response(
                jobs, "application/xml"
            ),
            "https://careers.example.test/jobs/123-senior-patent-counsel": html_response(
                detail_heading
            ),
            "https://careers.example.test/jobs/789-ai-policy-fellow?a=1&b=2": html_response(
                detail_ld
            ),
        }
    )

    result = crawler(site).scan()

    by_title = {item.title: item for item in result.items}
    assert set(by_title) == {"Senior Patent Counsel", "AI Policy Fellow"}
    assert (
        by_title["Senior Patent Counsel"].metadata["extraction"]
        == "detail_page_heading"
    )
    assert "Prosecute patents." in by_title["Senior Patent Counsel"].description
    assert by_title["AI Policy Fellow"].metadata["extraction"] == "json_ld_job_posting"
    assert by_title["AI Policy Fellow"].url.endswith("789-ai-policy-fellow?a=1&b=2")
    assert not any(
        "software-engineer" in url or "cdn.example.org" in url for url in site.requested
    )
    # The job-shaped child sitemap is read before the generic one.
    assert site.requested.index(
        "https://careers.example.test/sitemaps/jobs.xml"
    ) < site.requested.index("https://careers.example.test/sitemaps/pages.xml")
    assert result.metadata["sitemap_candidates"] == 2


def test_embedded_greenhouse_board_is_read_through_its_api() -> None:
    seed = """<html><body>
      <div id="grnhse_app"></div>
      <script src="https://boards.greenhouse.io/embed/job_board/js?for=acme"></script>
      <a href="https://boards.greenhouse.io/acme/jobs/11">Litigation Counsel</a>
    </body></html>"""
    board = {
        "jobs": [
            {
                "id": 11,
                "title": "Litigation Counsel",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/11",
                "location": {"name": "Remote - US"},
                "content": "<p>Handle disputes.</p>",
                "updated_at": "2026-09-10T00:00:00Z",
            },
            {
                "id": 12,
                "title": "Backend Engineer",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/12",
                "location": {"name": "Remote"},
            },
        ]
    }
    site = Site({SEED: html_response(seed)})
    api = Site(
        {
            "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true": httpx.Response(
                200, json=board
            )
        }
    )

    result = crawler(site, limits=CrawlLimits(max_pages=1), delegate_site=api).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert len(result.items) == 1
    item = result.items[0]
    assert item.source == "portal:example"
    assert item.metadata["extraction"] == "ats_delegate"
    assert item.metadata["delegate_source"] == "greenhouse:acme"
    assert item.metadata["delegate_source_id"] == "11"
    assert result.metadata["detected_boards"] == ["greenhouse:acme"]
    assert result.metadata["delegated_boards"] == ["greenhouse:acme"]


def test_already_configured_boards_are_not_read_twice() -> None:
    seed = '<html><body><a href="https://jobs.lever.co/spotify/abc">Music Licensing Counsel</a></body></html>'
    site = Site({SEED: html_response(seed)})
    api = Site({})

    result = crawler(
        site,
        limits=CrawlLimits(max_pages=1),
        delegate_site=api,
        skip_delegate_keys=_configured_board_keys(AppConfig()),
    ).scan()

    assert api.requested == []
    assert result.metadata["skipped_boards"] == ["lever:spotify"]
    assert [item.title for item in result.items] == ["Music Licensing Counsel"]


@pytest.mark.parametrize(
    ("snippet", "label"),
    [
        (
            '<a href="https://job-boards.greenhouse.io/acme/jobs/1">x</a>',
            "greenhouse:acme",
        ),
        (
            '<iframe src="https://jobs.ashbyhq.com/Acme-Co?embed=js"></iframe>',
            "ashby:Acme-Co",
        ),
        ('<a href="https://apply.workable.com/acme/j/ABC123/">x</a>', "workable:acme"),
        (
            '<a href="https://acme.wd5.myworkdayjobs.com/en-US/External/job/X_1">x</a>',
            "workday:acme:External",
        ),
        (
            '<a href="https://jobs.smartrecruiters.com/AcmeCo/74400001-counsel">x</a>',
            "smartrecruiters:acmeco",
        ),
        (
            '<a href="https://careers-acme.icims.com/jobs/5021/counsel/job">x</a>',
            "icims:careers-acme",
        ),
    ],
)
def test_known_boards_are_detected(snippet: str, label: str) -> None:
    site = Site({SEED: html_response(f"<html><body>{snippet}</body></html>")})

    result = crawler(site, limits=CrawlLimits(max_pages=1, max_delegates=0)).scan()

    assert result.metadata["detected_boards"] == [label.casefold()]


def test_unsafe_redirect_on_a_crawled_page_is_recorded_not_fatal() -> None:
    seed = """<html><body>
      <a href="/jobs/1">Compliance Counsel</a>
      <a href="/jobs?page=2">Next</a>
    </body></html>"""
    site = Site(
        {
            SEED: html_response(seed),
            "https://careers.example.test/robots.txt": text_response(""),
            "https://careers.example.test/jobs?page=2": httpx.Response(
                302, headers={"location": "http://169.254.169.254/latest"}
            ),
        }
    )

    result = crawler(site).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.title for item in result.items] == ["Compliance Counsel"]
    assert {
        "url": "https://careers.example.test/jobs?page=2",
        "code": "unsafe_redirect",
    } in (result.metadata["page_errors"])
    assert not any("169.254" in url for url in site.requested)


def test_requests_are_spaced_by_the_larger_of_delay_and_crawl_delay() -> None:
    now = [100.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    seed = '<html><body><a href="/jobs?page=2">Next</a></body></html>'
    site = Site(
        {
            SEED: html_response(seed),
            "https://careers.example.test/robots.txt": text_response(
                "User-agent: *\nCrawl-delay: 3\n"
            ),
            "https://careers.example.test/jobs?page=2": html_response("<html></html>"),
        }
    )

    crawler(
        site,
        limits=CrawlLimits(request_delay_seconds=1, use_sitemaps=False),
        sleep=sleep,
        clock=lambda: now[0],
    ).scan()

    # One second before robots.txt, then three (Crawl-delay) before page two.
    assert sum(sleeps) == pytest.approx(4.0)


def test_seed_failures_keep_the_single_page_error_contract() -> None:
    site = Site({SEED: html_response("<p>Please verify you are human</p>")})

    result = crawler(site).scan()

    assert result.status is ScanStatus.FAILED
    assert result.errors[0].code == "restricted_page"
    assert site.requested == [SEED]


def test_crawl_limits_are_validated() -> None:
    with pytest.raises(ValueError, match="max_pages"):
        CrawlLimits(max_pages=0)
    with pytest.raises(ValueError, match="delay"):
        CrawlLimits(request_delay_seconds=-1)


def test_trailing_slash_and_www_redirects_are_not_loops() -> None:
    site = Site(
        {
            SEED: httpx.Response(301, headers={"location": SEED + "/"}),
            SEED + "/": httpx.Response(
                308, headers={"location": "https://www.careers.example.test/jobs"}
            ),
            "https://www.careers.example.test/jobs": html_response(
                '<a href="/jobs/1">Patent Counsel</a>'
            ),
        }
    )

    result = crawler(site, limits=CrawlLimits(max_pages=1)).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.title for item in result.items] == ["Patent Counsel"]

    looping = Site({SEED: httpx.Response(302, headers={"location": SEED})})
    result = crawler(looping, limits=CrawlLimits(max_pages=1)).scan()
    assert result.errors[0].code == "redirect_loop"


def test_site_chrome_links_are_not_postings_and_role_collections_are_followed() -> None:
    seed = """<html><body>
      <a href="/legal/">Legal</a>
      <a href="/privacy-notice-cookie-policy">Website Privacy Notice and Cookie Policy</a>
      <a href="/resources/copyright-notices/">Copyright Infringement Notices</a>
      <a href="/jobs/77">Privacy Policy Analyst</a>
      <a href="https://privacypolicy.example.org/">Privacy Policy</a>
      <a href="/docs/eeo.pdf#x">Equal Employment Opportunity Policy</a>
      <a href="/company/#org">© Copyright 2026 Example. All rights reserved</a>
      <a href="/careers/paralegals-staff">Paralegal &amp; Staff Openings</a>
    </body></html>"""
    staff = '<html><body><a href="/jobs/78">Litigation Paralegal</a></body></html>'
    site = Site(
        {
            SEED: html_response(seed),
            "https://careers.example.test/robots.txt": text_response(""),
            "https://careers.example.test/careers/paralegals-staff": html_response(
                staff
            ),
        }
    )

    result = crawler(
        site, limits=CrawlLimits(request_delay_seconds=0, use_sitemaps=False)
    ).scan()

    assert sorted(item.title for item in result.items) == [
        "Litigation Paralegal",
        "Privacy Policy Analyst",
    ]


def test_mailto_links_are_not_postings() -> None:
    seed = """<html><body>
      <a href="mailto:legaltalent@example.test">legaltalent@example.test</a>
      <a href="/jobs/9">Legal Assistant</a>
    </body></html>"""
    site = Site({SEED: html_response(seed)})

    result = crawler(site, limits=CrawlLimits(max_pages=1)).scan()

    assert result.status is ScanStatus.SUCCEEDED
    assert [item.title for item in result.items] == ["Legal Assistant"]


def test_application_paths_and_asset_hosts_are_not_boards() -> None:
    snippets = (
        '<a href="https://jobs.smartrecruiters.com/my-applications">My applications</a>'
        '<a href="https://jobs.smartrecruiters.com/oneclick-ui/company/X/publication/1">x</a>'
        '<script src="https://cdn02.icims.com/js/portal.js"></script>'
        '<a href="https://internal-acme.icims.com/jobs/1/x/job">Internal</a>'
        '<a href="https://employees-acme.icims.com/jobs">Employees</a>'
        '<img src="https://c-1-2026-www-acme-com.i.icims.com/x.png">'
        '<a href="https://c-1-2026-www-acme-com.i.icims.com/jobs">x</a>'
    )
    site = Site({SEED: html_response(f"<html><body>{snippets}</body></html>")})

    result = crawler(site, limits=CrawlLimits(max_pages=1, max_delegates=0)).scan()

    assert result.metadata["detected_boards"] == []


def test_jibe_career_sites_are_read_through_their_feed() -> None:
    seed_url = "https://careers.example.test/careers/jobs"
    seed = """<html><head><script src="/jibe/app.js"></script></head><body>
      <span data-i18n="JIBE_INPUT-ADDRESS"></span>
      <a href="https://careers-example.icims.com/jobs/search">Old portal</a>
    </body></html>"""
    site = Site({seed_url: html_response(seed)})
    feed = {
        "jobs": [
            {"data": {"slug": "17", "req_id": "17", "title": "Licensing Counsel"}}
        ],
        "totalCount": 1,
    }
    api = Site(
        {
            "https://careers.example.test/api/jobs?page=1&limit=100": httpx.Response(
                200, json=feed
            )
        }
    )

    result = CareerCrawlerSource(
        PortalConfig(name="Example", url=seed_url),
        limits=CrawlLimits(max_pages=1),
        client_factory=site.client,
        host_resolver=lambda *_args, **_kwargs: PUBLIC,
        delegate_client_factory=api.client,
    ).scan()

    assert result.metadata["detected_boards"] == ["jibe:careers.example.test"]
    assert result.metadata["delegated_boards"] == ["jibe:careers.example.test"]
    assert [item.url for item in result.items] == [
        "https://careers.example.test/careers/jobs/17"
    ]
