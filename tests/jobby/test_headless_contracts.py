from __future__ import annotations

from pathlib import Path
import time

import httpx
import pytest

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.enums import JobStatus
from jobby.facade import ApplicationFacade
from jobby.mcp_server import create_server
from jobby.models import Company, Job
from jobby.sources.catalog import CatalogSource


@pytest.fixture
def database(tmp_path: Path):
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


def _job(database: Database) -> str:
    with database.session() as session:
        company = Company(name="Example", normalized_name="example")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title="Policy Counsel",
            normalized_title="policy counsel",
            canonical_url="https://jobs.example.test/policy-counsel",
            source_primary="manual",
            source_id="policy-1",
            status=JobStatus.DISCOVERED,
        )
        session.add(job)
        session.flush()
        return job.id


def test_non_human_mutations_require_hash_bound_single_use_approval(database: Database):
    job_id = _job(database)
    with ApplicationFacade(database, actor="mcp_client") as facade:
        payload = {"job_id": job_id, "submission_channel": None, "notes": None}
        prepared = facade.prepare_mutation("application.create", payload)

        with pytest.raises(PermissionError, match="requires an approved approval_id"):
            facade.create_application(job_id)
        with pytest.raises(ValueError, match="hash does not match"):
            facade.approve_mutation(prepared["approval_id"], expected_hash="0" * 64)

        facade.approve_mutation(
            prepared["approval_id"], expected_hash=prepared["payload_hash"]
        )
        result = facade.create_application(job_id, approval_id=prepared["approval_id"])
        assert result["audit_id"]
        with pytest.raises(ValueError, match="not approved"):
            facade.create_application(job_id, approval_id=prepared["approval_id"])


def test_mcp_schemas_expose_typed_nested_inputs(database: Database):
    server = create_server(ApplicationFacade(database, actor="mcp_client"))
    search_schema = server._tool_manager._tools["search_jobs"].parameters
    capture_schema = server._tool_manager._tools["preview_capture"].parameters
    approval_schema = server._tool_manager._tools["prepare_mutation"].parameters

    assert "SearchInput" in search_schema["$defs"]
    assert "limit" in search_schema["$defs"]["SearchInput"]["properties"]
    assert capture_schema["$defs"]["CaptureInput"]["additionalProperties"] is False
    assert (
        "application.create"
        in approval_schema["$defs"]["MCPMutationInput"]["properties"]["action"]["enum"]
    )


def test_catalog_source_streams_and_classifies_failures():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/unauthorized"):
            return httpx.Response(401, json={"error": "no"}, request=request)
        return httpx.Response(
            200,
            json={
                "jobs": [
                    {
                        "id": "1",
                        "title": "Policy Counsel",
                        "url": "https://jobs.example.test/1",
                        "posted_at": "2026-09-18T12:00:00Z",
                    },
                    {"id": "2", "title": "Missing URL"},
                ]
            },
            headers={"content-type": "application/json"},
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    source = CatalogSource(
        client,
        endpoint="https://catalog.example.test/jobs",
        company="Example",
        provider="freehire",
    )
    source.configure_runtime(deadline_at=None, cancelled=lambda: False)
    result = source.scan()
    assert result.status.value == "partial"
    assert len(result.items) == 1
    assert result.errors[0].retryable is False
    assert result.items[0].posted_at is not None

    unauthorized = CatalogSource(
        client,
        endpoint="https://catalog.example.test/unauthorized",
        company="Example",
        provider="freehire",
    )
    unauthorized.configure_runtime(deadline_at=None, cancelled=lambda: False)
    failed = unauthorized.scan()
    assert failed.status.value == "failed"
    assert failed.errors[0].http_status == 401
    assert failed.errors[0].retryable is False


def test_background_operation_returns_durable_status(database: Database):
    with ApplicationFacade(database, actor="mcp_client") as facade:
        queued = facade.run_scan(source="does-not-exist", background=True)
        assert queued["operation_id"]
        for _ in range(50):
            status = facade.get_operation_status(queued["operation_id"])
            if status["status"] == "failed":
                break
            time.sleep(0.01)
        assert status["status"] == "failed"
        assert status["error"]
