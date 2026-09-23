from __future__ import annotations

from hashlib import sha256
import httpx
import pytest

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.enums import ArtifactKind, DocumentStatus, JobStatus
from jobby.facade import ApplicationFacade, SearchInput
from jobby.mcp_server import MCPMutationInput
from jobby.models import Company, DocumentVersion, ImportReview, Job
from jobby.sources.catalog import (
    EightfoldSource,
    FreehireSource,
    OracleHCMSource,
    PaylocitySource,
    RipplingSource,
)


@pytest.fixture
def database(tmp_path):
    root = tmp_path / "jobby-home"
    paths = JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    ).ensure()
    instance = Database(paths=paths)
    instance.initialize()
    yield instance
    instance.dispose()


def _job(database, *, title: str = "Policy Counsel") -> str:
    with database.session() as session:
        company = (
            session.query(Company)
            .filter_by(normalized_name="fixture company")
            .one_or_none()
        )
        if company is None:
            company = Company(name="Fixture Company", normalized_name="fixture company")
            session.add(company)
            session.flush()
        row = Job(
            company_id=company.id,
            title=title,
            normalized_title=title.casefold(),
            canonical_url=f"https://jobs.example.test/{title.casefold().replace(' ', '-')}",
            source_primary="fixture",
            source_id=title.casefold(),
            status=JobStatus.DISCOVERED,
        )
        session.add(row)
        session.flush()
        return row.id


def test_document_lookup_does_not_depend_on_the_first_page(database) -> None:
    with database.session() as session:
        rows = []
        for index in range(205):
            content = f"# Document {index}"
            rows.append(
                DocumentVersion(
                    kind=ArtifactKind.OTHER,
                    name=f"Fixture {index:03d}",
                    content_markdown=content,
                    content_hash=sha256(content.encode()).hexdigest(),
                    status=DocumentStatus.SOURCE,
                )
            )
        session.add_all(rows)
        session.flush()
        old_id = rows[0].id

    with ApplicationFacade(database) as facade:
        document = facade.get_document(old_id)
        assert document["id"] == old_id
        assert document["content_markdown"] == "# Document 0"


def test_cursor_binds_to_filter_identity(database) -> None:
    for index in range(3):
        _job(database, title=f"Policy Counsel {index}")
    with ApplicationFacade(database) as facade:
        first = facade.search_jobs(SearchInput(limit=1, sort="title"))
        assert first["has_more"]
        second = facade.search_jobs(
            SearchInput(limit=1, sort="title", cursor=first["next_cursor"])
        )
        assert second["items"][0]["id"] != first["items"][0]["id"]
        with pytest.raises(ValueError, match="does not match"):
            facade.search_jobs(
                SearchInput(limit=1, sort="company", cursor=first["next_cursor"])
            )


def test_pending_review_hash_rejects_stale_state(database) -> None:
    with database.session() as session:
        row = ImportReview(
            workspace_root="/fixture",
            source_path="records.json",
            record_key="one",
            reason="ambiguous",
            proposed_json={"title": "Fixture"},
        )
        session.add(row)
        session.flush()
        review_id = row.id
    with ApplicationFacade(database) as facade:
        page = facade.list_pending_reviews_page(limit=10)
        review = next(item for item in page["items"] if item["id"] == review_id)
        with database.session() as session:
            session.get(ImportReview, review_id).reason = "changed"
        with pytest.raises(ValueError, match="stale"):
            facade.approve_review(
                review_id, review_type="import", expected_hash=review["review_hash"]
            )


def test_merged_review_keyset_cursor_reaches_rows_beyond_the_first_fetch(
    database,
) -> None:
    with database.session() as session:
        session.add_all(
            [
                ImportReview(
                    workspace_root="/fixture",
                    source_path=f"records-{index:03d}.json",
                    record_key=str(index),
                    reason="ambiguous",
                    proposed_json={"title": "Fixture"},
                )
                for index in range(205)
            ]
        )

    with ApplicationFacade(database) as facade:
        first = facade.list_pending_reviews_page(limit=200)
        assert len(first["items"]) == 200
        assert first["has_more"]
        second = facade.list_pending_reviews_page(
            limit=200, cursor=first["next_cursor"]
        )
        assert len(second["items"]) == 5
        assert not second["has_more"]
        assert not {item["id"] for item in first["items"]} & {
            item["id"] for item in second["items"]
        }


def test_strict_action_payloads_reject_unknown_fields() -> None:
    with pytest.raises(ValueError):
        MCPMutationInput.model_validate(
            {
                "action": "task.create",
                "payload": {"title": "Fixture", "unexpected": True},
            }
        )


@pytest.mark.parametrize(
    ("source_type", "record"),
    [
        (
            EightfoldSource,
            {
                "jobId": "e-1",
                "jobTitle": "Policy Counsel",
                "applyUrl": "https://jobs.example.test/e-1",
                "locations": "Remote",
                "postedDate": "2026-09-18T12:00:00Z",
                "salary": {
                    "min": 100000,
                    "max": 120000,
                    "currency": "USD",
                    "interval": "year",
                },
            },
        ),
        (
            OracleHCMSource,
            {
                "requisitionId": "o-1",
                "Title": "Policy Counsel",
                "ExternalJobURL": "https://jobs.example.test/o-1",
                "PrimaryLocation": "Remote",
                "PostedDate": "2026-09-18T12:00:00Z",
            },
        ),
        (
            RipplingSource,
            {
                "id": "r-1",
                "name": "Policy Counsel",
                "url": "https://jobs.example.test/r-1",
            },
        ),
        (
            PaylocitySource,
            {
                "jobId": "p-1",
                "jobTitle": "Policy Counsel",
                "applyUrl": "https://jobs.example.test/p-1",
            },
        ),
    ],
)
def test_provider_contract_maps_fixture_and_handles_continuation(
    source_type, record
) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                200, json={"jobs": [record], "hasMore": True}, request=request
            )
        return httpx.Response(200, json={"jobs": [], "hasMore": False}, request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        source = source_type(
            client,
            endpoint="https://provider.example.test/jobs",
            company="Fixture Company",
            credential=None,
        )
        source.configure_runtime(deadline_at=None, cancelled=lambda: False)
        result = source.scan(query="policy")
    finally:
        client.close()
    assert result.status.value == "succeeded"
    assert result.items[0].title == "Policy Counsel"
    assert len(calls) == 2
    assert result.metadata["capabilities"]["authentication"] == "keyring_only"


def test_freehire_without_stable_contract_is_explicitly_unsupported() -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    )
    try:
        source = FreehireSource(
            client,
            endpoint="https://provider.example.test/jobs",
            company="Fixture Company",
        )
        result = source.scan()
    finally:
        client.close()
    assert result.status.value == "failed"
    assert result.errors[0].code == "unsupported_contract"
