from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
import threading
import time
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import func, select

from jobby.db import Database, StorageBusyError
from jobby.enums import AgentRunStatus, JobStatus
from jobby.models import (
    DuplicateRelationship,
    Evaluation,
    Job,
    JobSourceState,
    ScanRun,
    SourceObservation,
)
from jobby.ranking import RankingProfile
from jobby.review_queues import dismiss_duplicate
from jobby.scanner import (
    Scanner,
    WebDiscoveredJob,
    WebDiscoveredJobs,
    is_complete_snapshot,
)
from jobby.openai_provider import WebCitation
from jobby.sources.base import ScanItem, ScanStatus, SourceError, SourceResult
from jobby.sources.browser import PortalConfig, PublicPortalSource


@dataclass
class SequenceSource:
    source_key: str
    results: list[SourceResult | Exception]

    def scan(self, query: str | None = None) -> SourceResult:
        value = self.results.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def item(
    source: str,
    source_id: str,
    *,
    url: str | None = None,
    title: str = "Legal Operations Analyst",
    description: str = "Paid full-time legal operations role with flexible scheduling.",
) -> ScanItem:
    return ScanItem(
        source=source,
        source_id=source_id,
        company="Example",
        title=title,
        url=url or f"https://jobs.example.test/{source_id}",
        description=description,
    )


def result(
    source: str,
    items: Iterable[ScanItem] = (),
    *,
    status: ScanStatus = ScanStatus.SUCCEEDED,
    errors: tuple[SourceError, ...] = (),
    metadata: dict[str, object] | None = None,
) -> SourceResult:
    return SourceResult(
        source=source,
        status=status,
        items=tuple(items),
        errors=errors,
        metadata=metadata or {},
    )


@pytest.fixture
def database(tmp_path) -> Database:
    db = Database(tmp_path / "jobby.sqlite3")
    db.initialize()
    yield db
    db.dispose()


def test_source_exception_is_a_partial_scan_and_preserves_other_results(
    database: Database,
) -> None:
    good = SequenceSource(
        "greenhouse:good",
        [result("greenhouse:good", [item("greenhouse:good", "1")])],
    )
    broken = SequenceSource("lever:broken", [RuntimeError("temporary adapter failure")])

    run = Scanner(database).scan([good, broken])

    assert run.status is AgentRunStatus.PARTIAL
    assert run.discovered_count == 1
    assert run.source_results["lever:broken"]["status"] == "failed"
    assert "temporary adapter failure" in (run.error_summary or "")
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def test_fetch_concurrency_is_globally_bounded_and_order_is_deterministic(
    database: Database,
) -> None:
    lock = threading.Lock()
    active = 0
    maximum = 0

    class TimedSource:
        def __init__(self, index: int) -> None:
            self.source_key = f"custom:{index}"
            self.concurrency_key = self.source_key
            self.index = index

        def scan(self, query: str | None = None) -> SourceResult:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            # Reverse completion order to prove persisted result order is based
            # on configuration rather than timing.
            time.sleep((6 - self.index) * 0.005)
            with lock:
                active -= 1
            return result(self.source_key)

    sources = [TimedSource(index) for index in range(6)]
    fetched = Scanner(database, max_workers=3)._fetch_source_results(sources, None)

    assert maximum == 3
    assert [item.source for item in fetched] == [
        source.source_key for source in sources
    ]


def test_fetch_concurrency_is_bounded_per_provider_key(database: Database) -> None:
    lock = threading.Lock()
    active_by_key: dict[str, int] = {}
    maximum_by_key: dict[str, int] = {}

    class TimedSource:
        def __init__(self, index: int, concurrency_key: str) -> None:
            self.source_key = f"custom:{index}"
            self.concurrency_key = concurrency_key

        def scan(self, query: str | None = None) -> SourceResult:
            with lock:
                active_by_key[self.concurrency_key] = (
                    active_by_key.get(self.concurrency_key, 0) + 1
                )
                maximum_by_key[self.concurrency_key] = max(
                    maximum_by_key.get(self.concurrency_key, 0),
                    active_by_key[self.concurrency_key],
                )
            time.sleep(0.02)
            with lock:
                active_by_key[self.concurrency_key] -= 1
            return result(self.source_key)

    sources = [
        *(TimedSource(index, "shared.example") for index in range(5)),
        TimedSource(5, "other.example"),
    ]
    Scanner(database, max_workers=6, max_workers_per_source=2)._fetch_source_results(
        sources, None
    )

    assert maximum_by_key["shared.example"] == 2
    assert maximum_by_key["other.example"] == 1


def test_item_persistence_failure_is_partial_and_keeps_other_items(
    database: Database,
    monkeypatch,
) -> None:
    source_key = "greenhouse:example"
    scanner = Scanner(database)
    original = scanner._persist_item

    def flaky_persist(session, run, scan_item):
        if scan_item.source_id == "bad":
            raise RuntimeError("simulated row failure")
        return original(session, run, scan_item)

    monkeypatch.setattr(scanner, "_persist_item", flaky_persist)
    run = scanner.scan(
        [
            SequenceSource(
                source_key,
                [
                    result(
                        source_key, [item(source_key, "bad"), item(source_key, "good")]
                    )
                ],
            )
        ]
    )

    assert run.status is AgentRunStatus.PARTIAL
    assert run.discovered_count == 1
    assert run.source_results[source_key]["errors"][0]["code"] == "persistence_error"
    with database.session() as session:
        assert session.scalar(select(Job.source_id)) == "good"


def test_result_and_item_source_keys_must_match_adapter(database: Database) -> None:
    mismatched_result = SequenceSource(
        "greenhouse:expected",
        [result("lever:spoofed", [item("lever:spoofed", "1")])],
    )
    mismatched_item = SequenceSource(
        "greenhouse:item-check",
        [result("greenhouse:item-check", [item("lever:wrong", "2")])],
    )

    run = Scanner(database).scan([mismatched_result, mismatched_item])

    assert run.status is AgentRunStatus.FAILED
    assert (
        run.source_results["greenhouse:expected"]["errors"][0]["code"]
        == "adapter_exception"
    )
    assert (
        run.source_results["greenhouse:item-check"]["errors"][0]["code"]
        == "source_mismatch"
    )
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_empty_scan_has_explicit_failure_reason(database: Database) -> None:
    run = Scanner(database).scan([])
    assert run.status is AgentRunStatus.FAILED
    assert run.source_results["scanner"]["errors"][0]["code"] == "no_sources"
    assert "No discovery sources" in (run.error_summary or "")


def test_non_json_source_metadata_is_safely_serialized(database: Database) -> None:
    source_key = "greenhouse:metadata"
    observed_at = datetime(2026, 7, 12, tzinfo=timezone.utc)
    run = Scanner(database).scan(
        [
            SequenceSource(
                source_key,
                [
                    result(
                        source_key,
                        [
                            ScanItem(
                                source=source_key,
                                source_id="1",
                                company="Example",
                                title="Legal Analyst",
                                url="https://jobs.example.test/1",
                                metadata={"observed": observed_at},
                            )
                        ],
                        metadata={"observed": observed_at},
                    )
                ],
            )
        ]
    )
    assert run.status is AgentRunStatus.SUCCEEDED
    assert (
        run.source_results[source_key]["metadata"]["observed"]
        == observed_at.isoformat()
    )


def test_complete_snapshot_marks_one_miss_stale_and_two_closed(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            result(source_key, [item(source_key, "1"), item(source_key, "2")]),
            result(source_key, [item(source_key, "1")]),
            result(source_key, [item(source_key, "1")]),
        ],
    )
    scanner = Scanner(database)

    scanner.scan([source])
    scanner.scan([source])
    with database.session() as session:
        missing = session.scalar(select(Job).where(Job.source_id == "2"))
        assert missing is not None
        assert missing.status is JobStatus.STALE
        assert missing.consecutive_misses == 1

    scanner.scan([source])
    with database.session() as session:
        missing = session.scalar(select(Job).where(Job.source_id == "2"))
        assert missing is not None
        assert missing.status is JobStatus.CLOSED
        assert missing.consecutive_misses == 2
        assert missing.closed_at is not None


def test_query_filtered_success_never_counts_absence_as_a_miss(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    source = SequenceSource(
        source_key,
        [
            result(source_key, [item(source_key, "1"), item(source_key, "2")]),
            result(source_key, [item(source_key, "1")]),
        ],
    )
    scanner = Scanner(database)
    scanner.scan([source])
    scanner.scan([source], query="legal")

    with database.session() as session:
        missing = session.scalar(select(Job).where(Job.source_id == "2"))
        assert missing is not None
        assert missing.status is JobStatus.DISCOVERED
        assert missing.consecutive_misses == 0


@pytest.mark.parametrize(
    "source_key", ["usajobs", "portal:example", "custom:example", "openai_web"]
)
def test_non_snapshot_sources_never_age_jobs(
    database: Database, source_key: str
) -> None:
    source = SequenceSource(
        source_key,
        [result(source_key, [item(source_key, "1")]), result(source_key)],
    )
    scanner = Scanner(database)
    scanner.scan([source])
    scanner.scan([source])

    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "1"))
        assert job is not None
        assert job.status is JobStatus.DISCOVERED
        assert job.consecutive_misses == 0


def test_workday_only_ages_jobs_when_all_reported_rows_were_returned(
    database: Database,
) -> None:
    source_key = "workday:example:external"
    source = SequenceSource(
        source_key,
        [
            result(source_key, [item(source_key, "1")], metadata={"total": 1}),
            result(source_key, metadata={"total": 2}),
            result(source_key, metadata={"total": 0}),
        ],
    )
    scanner = Scanner(database)
    scanner.scan([source])
    scanner.scan([source])
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "1"))
        assert job is not None and job.consecutive_misses == 0

    scanner.scan([source])
    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "1"))
        assert job is not None
        # Zero-result inventories are anomalous and cannot supply absence
        # evidence until a later healthy complete inventory succeeds.
        assert job.status is JobStatus.DISCOVERED
        assert job.consecutive_misses == 0


def test_partial_snapshot_does_not_age_unreturned_jobs(database: Database) -> None:
    source_key = "lever:example"
    source = SequenceSource(
        source_key,
        [
            result(source_key, [item(source_key, "1"), item(source_key, "2")]),
            result(
                source_key,
                [item(source_key, "1")],
                status=ScanStatus.PARTIAL,
                errors=(
                    SourceError(code="malformed_item", message="one row was malformed"),
                ),
            ),
        ],
    )
    scanner = Scanner(database)
    scanner.scan([source])
    run = scanner.scan([source])

    assert run.status is AgentRunStatus.PARTIAL
    with database.session() as session:
        missing = session.scalar(select(Job).where(Job.source_id == "2"))
        assert missing is not None and missing.consecutive_misses == 0


@pytest.mark.parametrize("reverse", [False, True])
def test_observation_from_another_source_prevents_order_dependent_miss(
    database: Database,
    reverse: bool,
) -> None:
    canonical = "https://jobs.example.test/shared"
    scanner = Scanner(database)
    scanner.scan(
        [
            SequenceSource(
                "greenhouse:example",
                [
                    result(
                        "greenhouse:example",
                        [item("greenhouse:example", "gh-1", url=canonical)],
                    )
                ],
            )
        ]
    )
    greenhouse = SequenceSource(
        "greenhouse:example",
        [
            result(
                "greenhouse:example",
                [item("greenhouse:example", "gh-other")],
            )
        ],
    )
    lever = SequenceSource(
        "lever:example",
        [result("lever:example", [item("lever:example", "lev-1", url=canonical)])],
    )
    scanner.scan([lever, greenhouse] if reverse else [greenhouse, lever])

    with database.session() as session:
        job = session.scalar(select(Job).where(Job.canonical_url == canonical))
        assert job is not None
        assert job.status is JobStatus.DISCOVERED
        assert job.consecutive_misses == 0
        states = {
            state.source: state
            for state in session.scalars(
                select(JobSourceState).where(JobSourceState.job_id == job.id)
            )
        }
        assert states["greenhouse:example"].is_live is False
        assert states["lever:example"].is_live is True
        secondary_miss = session.scalar(
            select(SourceObservation).where(
                SourceObservation.job_id == job.id,
                SourceObservation.source == "greenhouse:example",
                SourceObservation.is_live.is_(False),
            )
        )
        assert secondary_miss is not None
        assert secondary_miss.source_job_id == "gh-1"


def test_duplicate_source_rows_create_one_observation_and_one_evaluation(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    duplicate = item(source_key, "1")
    run = Scanner(database).scan(
        [SequenceSource(source_key, [result(source_key, [duplicate, duplicate])])]
    )

    assert run.discovered_count == 1
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SourceObservation)) == 1
        assert session.scalar(select(func.count()).select_from(Evaluation)) == 1


def test_scan_fetch_does_not_hold_a_sqlite_writer_and_scan_lease_prevents_overlap(
    database: Database,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    class BlockingSource:
        source_key = "custom:blocking"

        def scan(self, query: str | None = None) -> SourceResult:
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("test source was not released")
            return result(self.source_key)

    def run_scan() -> None:
        try:
            Scanner(database).scan([BlockingSource()])
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    thread = threading.Thread(target=run_scan)
    thread.start()
    assert entered.wait(timeout=5)
    try:
        with sqlite3.connect(database.path, timeout=0.1) as connection:
            connection.execute("PRAGMA busy_timeout=100")
            now = datetime.now(timezone.utc).isoformat()
            connection.execute(
                "INSERT INTO companies (id, name, normalized_name, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "writer",
                    "Writer",
                    "writer",
                    now,
                    now,
                ),
            )

        with pytest.raises(StorageBusyError, match="storage is busy"):
            Scanner(database).scan([])
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert failures == []


def test_interrupted_fetch_leaves_a_terminal_scan_run(database: Database) -> None:
    class CancelledSource:
        source_key = "custom:cancelled"

        def scan(self, query: str | None = None) -> SourceResult:
            raise KeyboardInterrupt("cancelled by user")

    with pytest.raises(KeyboardInterrupt, match="cancelled by user"):
        Scanner(database).scan([CancelledSource()])

    with database.session() as session:
        run = session.scalar(select(ScanRun).order_by(ScanRun.started_at.desc()))
        assert run is not None
        assert run.status is AgentRunStatus.FAILED
        assert run.finished_at is not None
        assert "cancelled by user" in (run.error_summary or "")


def test_duplicate_analysis_includes_historical_same_company_roles(
    database: Database,
) -> None:
    scanner = Scanner(database)
    scanner.scan(
        [
            SequenceSource(
                "custom:first",
                [
                    result(
                        "custom:first",
                        [
                            item(
                                "custom:first",
                                "one",
                                url="https://jobs.example.test/one",
                                description="First source description.",
                            )
                        ],
                    )
                ],
            )
        ]
    )
    scanner.scan(
        [
            SequenceSource(
                "custom:second",
                [
                    result(
                        "custom:second",
                        [
                            item(
                                "custom:second",
                                "two",
                                url="https://jobs.example.test/two",
                                description="Different source description.",
                            )
                        ],
                    )
                ],
            )
        ]
    )

    with database.session() as session:
        jobs = list(session.scalars(select(Job).order_by(Job.source_id)))
        relationship = session.scalar(select(DuplicateRelationship))
        assert len(jobs) == 2
        assert relationship is not None
        assert {relationship.job_id, relationship.duplicate_job_id} == {
            jobs[0].id,
            jobs[1].id,
        }
        assert relationship.rule == "normalized_fields"


def test_scanner_respects_dismissal_until_material_comparison_changes(
    database: Database,
) -> None:
    scanner = Scanner(database)
    first = "greenhouse:first"
    second = "lever:second"
    scanner.scan(
        [
            SequenceSource(first, [result(first, [item(first, "one")])]),
            SequenceSource(second, [result(second, [item(second, "two")])]),
        ]
    )
    with database.session() as session:
        relationship = session.scalar(select(DuplicateRelationship))
        assert relationship is not None
        dismiss_duplicate(session, relationship)
        relationship_id = relationship.id

    # An unchanged comparison remains dismissed.
    scanner.scan([SequenceSource(second, [result(second, [item(second, "two")])])])
    with database.session() as session:
        relationship = session.get(DuplicateRelationship, relationship_id)
        assert relationship is not None and relationship.resolution == "dismissed"

    # Updating one material field lets the same row return to pending review.
    scanner.scan(
        [
            SequenceSource(
                second,
                [
                    result(
                        second,
                        [
                            item(
                                second,
                                "two",
                                description=(
                                    "Materially expanded paid legal operations role "
                                    "with AI policy responsibilities."
                                ),
                            )
                        ],
                    )
                ],
            )
        ]
    )
    with database.session() as session:
        relationship = session.get(DuplicateRelationship, relationship_id)
        assert relationship is not None
        assert relationship.resolution == "pending"
        assert session.scalar(select(func.count(DuplicateRelationship.id))) == 1


def test_scanner_persists_deterministic_score_and_ignores_unpaid_work(
    database: Database,
) -> None:
    source_key = "greenhouse:example"
    scanner = Scanner(database, ranking_profile=RankingProfile(salary_floor=100_000))
    scanner.scan(
        [
            SequenceSource(
                source_key,
                [
                    result(
                        source_key,
                        [
                            item(
                                source_key,
                                "unpaid",
                                description="This is an unpaid full-time legal fellowship.",
                            )
                        ],
                    )
                ],
            )
        ]
    )

    with database.session() as session:
        job = session.scalar(select(Job).where(Job.source_id == "unpaid"))
        evaluation = session.scalar(
            select(Evaluation).where(Evaluation.job_id == job.id)
        )
        assert job is not None and evaluation is not None
        assert evaluation.automatic_skip is True
        assert job.latest_score == evaluation.score
        assert job.status is JobStatus.IGNORED
        assert job.manual_status_locked is False


def test_complete_snapshot_contract_is_conservative() -> None:
    assert is_complete_snapshot(result("greenhouse:x")) is True
    assert is_complete_snapshot(result("lever:x")) is True
    assert is_complete_snapshot(result("ashby:x")) is True
    assert is_complete_snapshot(result("workable:x")) is True
    assert is_complete_snapshot(result("workday:x:y", metadata={"total": 1})) is False
    assert (
        is_complete_snapshot(
            result("workday:x:y", [item("workday:x:y", "1")], metadata={"total": "1"})
        )
        is True
    )
    assert is_complete_snapshot(result("usajobs")) is False
    assert (
        is_complete_snapshot(
            result("usajobs", [item("usajobs", "1")], metadata={"total": "1"})
        )
        is True
    )
    assert (
        is_complete_snapshot(
            result(
                "usajobs",
                [item("usajobs", "1")],
                metadata={"total": 2, "truncated": True},
            )
        )
        is False
    )
    assert is_complete_snapshot(result("portal:x")) is False


class FakeWebProvider:
    def __init__(self, jobs: list[WebDiscoveredJob], citations: list[WebCitation]):
        self.jobs = jobs
        self.citations = citations
        self.structured_text = ""

    def search(self, _query: str, *, session):
        return SimpleNamespace(
            text="Search results",
            citations=self.citations,
        )

    def structured(self, *, text: str, **_kwargs):
        self.structured_text = text
        return SimpleNamespace(value=WebDiscoveredJobs(jobs=self.jobs))


def test_web_scan_persists_only_citation_supported_public_urls(
    database: Database,
) -> None:
    cited = "https://jobs.example.test/role/1?utm_source=search"
    provider = FakeWebProvider(
        jobs=[
            WebDiscoveredJob(
                company="Example",
                title="Legal Analyst",
                url="https://jobs.example.test/role/1",
            ),
            WebDiscoveredJob(
                company="Invented",
                title="Policy Counsel",
                url="https://uncited.example.test/role/2",
            ),
        ],
        citations=[WebCitation(url=cited, title="Legal Analyst")],
    )

    run = Scanner(database).scan_web(provider, "legal analyst")

    assert run.status is AgentRunStatus.PARTIAL
    assert run.discovered_count == 1
    assert run.source_results["openai_web"]["errors"][0]["code"] == "uncited_url"
    assert cited in provider.structured_text
    with database.session() as session:
        jobs = list(session.scalars(select(Job)))
        assert [job.title for job in jobs] == ["Legal Analyst"]
        assert jobs[0].launch_url == cited
        assert jobs[0].comparison_url == "https://jobs.example.test/role/1"


def test_web_scan_rejects_private_and_uncited_results_without_false_success(
    database: Database,
) -> None:
    provider = FakeWebProvider(
        jobs=[
            WebDiscoveredJob(
                company="Local",
                title="Local Admin",
                url="http://127.0.0.1/admin",
            ),
            WebDiscoveredJob(
                company="Invented",
                title="Uncited Counsel",
                url="https://uncited.example.test/job",
            ),
        ],
        citations=[WebCitation(url="https://jobs.example.test/real")],
    )

    run = Scanner(database).scan_web(provider, "policy")

    assert run.status is AgentRunStatus.FAILED
    assert run.discovered_count == 0
    codes = {item["code"] for item in run.source_results["openai_web"]["errors"]}
    assert codes == {"invalid_public_url", "uncited_url"}


def test_web_scan_query_length_is_bounded_before_provider_call(
    database: Database,
) -> None:
    provider = FakeWebProvider([], [])
    with pytest.raises(ValueError, match="2,000"):
        Scanner(database).scan_web(provider, "x" * 2_001)


def test_public_portal_uses_static_html_and_canonicalizes_links() -> None:
    html = """
    <html><body><main>
      <a href="/jobs/1?utm_source=test"><strong>Legal Policy</strong> Analyst</a>
      <a href="https://careers.example.test/jobs/1">Legal Policy Analyst</a>
      <a href="/jobs/2">Software Engineer</a>
      <script><a href="/secret">Legal Secret</a></script>
    </main></body></html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://careers.example.test/jobs"
        return httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            text=html,
        )

    source = PublicPortalSource(
        PortalConfig(name="Example", url="https://careers.example.test/jobs"),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        host_resolver=lambda *_args, **_kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )

    scan = source.scan("legal analyst")

    assert scan.status is ScanStatus.SUCCEEDED
    assert len(scan.items) == 1
    assert scan.items[0].url == "https://careers.example.test/jobs/1?utm_source=test"
    assert scan.items[0].comparison_url == "https://careers.example.test/jobs/1"
    assert scan.items[0].metadata["extraction"] == "static_html_anchor"
    assert scan.metadata["extraction"] == "static_html"


def test_public_portal_rejects_credentials_in_url() -> None:
    with pytest.raises(ValueError, match="credentials"):
        PublicPortalSource(
            PortalConfig(name="Unsafe", url="https://user:pass@example.test/jobs")
        )


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/jobs",
        "http://127.0.0.1/jobs",
        "http://10.0.0.2/jobs",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/jobs",
        "https://careers.internal/jobs",
    ],
)
def test_public_portal_rejects_private_targets_before_http_request(url: str) -> None:
    with pytest.raises(ValueError, match="public HTTP"):
        PublicPortalSource(PortalConfig(name="Unsafe", url=url))


def test_public_portal_blocks_dns_resolution_to_private_address() -> None:
    requested = False

    def client_factory():
        nonlocal requested
        requested = True
        raise AssertionError("HTTP client must not open")

    source = PublicPortalSource(
        PortalConfig(name="Rebound", url="https://careers.example.test/jobs"),
        client_factory=client_factory,
        host_resolver=lambda *_args, **_kwargs: [(2, 1, 6, "", ("127.0.0.1", 443))],
    )
    scan = source.scan()
    assert scan.status is ScanStatus.FAILED
    assert scan.errors[0].code == "unsafe_target"
    assert requested is False


def test_public_portal_deadline_expires_before_dns_or_http_is_opened() -> None:
    opened = False

    def client_factory():
        nonlocal opened
        opened = True
        raise AssertionError("expired source must not open an HTTP client")

    source = PublicPortalSource(
        PortalConfig(name="Expired", url="https://careers.example.test/jobs"),
        client_factory=client_factory,
        host_resolver=lambda *_args, **_kwargs: pytest.fail(
            "expired source must not resolve DNS"
        ),
    )
    source.configure_runtime(
        deadline_at=time.monotonic() - 1,
        cancelled=lambda: False,
    )

    scan = source.scan()

    assert scan.status is ScanStatus.FAILED
    assert scan.errors[0].code == "source_deadline"
    assert opened is False


def test_public_portal_pins_dns_at_connection_time_to_block_rebinding() -> None:
    calls = 0

    def rebinding_resolver(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        address = "93.184.216.34" if calls < 3 else "127.0.0.1"
        return [(2, 1, 6, "", (address, 443))]

    source = PublicPortalSource(
        PortalConfig(name="Rebound", url="https://careers.example.test/jobs"),
        host_resolver=rebinding_resolver,
    )

    scan = source.scan()

    assert scan.status is ScanStatus.FAILED
    assert scan.errors[0].code == "unsafe_target"
    assert calls == 3


def test_public_portal_blocks_redirect_to_private_target() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://127.0.0.1/admin"})

    source = PublicPortalSource(
        PortalConfig(name="Redirect", url="https://careers.example.test/jobs"),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        host_resolver=lambda *_args, **_kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )

    scan = source.scan()

    assert scan.status is ScanStatus.FAILED
    assert scan.errors[0].code == "unsafe_redirect"


def test_public_portal_bounds_decompressed_response_size() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"x" * 2_000_001,
        )

    source = PublicPortalSource(
        PortalConfig(name="Large", url="https://careers.example.test/jobs"),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        host_resolver=lambda *_args, **_kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )

    scan = source.scan()

    assert scan.status is ScanStatus.FAILED
    assert scan.errors[0].code == "response_too_large"
