"""Offline regression tests for storage, import, backup, and export trust boundaries."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import json
import os
import sqlite3
import stat
import zipfile
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from jobby.audit import REDACTED, json_safe, recent_activity, record_audit, redact_text
from jobby.backup import create_backup, verify_backup
from jobby.config import JobbyPaths
from jobby.db import (
    Database,
    RESTORE_JOURNAL_FILENAME,
    StorageBusyError,
    StorageLock,
)
from jobby.enums import ArtifactKind
from jobby.exporter import export_data
import jobby.exporter as exporter_module
from jobby.importer import (
    LegacyImporter,
    discover_legacy_artifacts,
    extract_document_text,
)
from jobby.models import (
    Artifact,
    AuditEvent,
    Company,
    Evaluation,
    ImportReview,
    Job,
    ProfileFact,
    SourceConfig,
)


def _paths(root: Path) -> JobbyPaths:
    data = root / "data"
    config = root / "config"
    cache = root / "cache"
    return JobbyPaths(
        data_dir=data,
        config_dir=config,
        cache_dir=cache,
        database=data / "jobby.sqlite3",
        artifacts_dir=data / "artifacts",
        backups_dir=data / "backups",
        logs_dir=data / "logs",
        config_file=config / "config.toml",
    ).ensure()


def _database(tmp_path: Path) -> tuple[Database, JobbyPaths]:
    paths = _paths(tmp_path / "home")
    database = Database(paths=paths)
    database.initialize()
    return database, paths


def _job(session, *, company_name: str = "Security Co", title: str = "Analyst") -> Job:
    company = Company(
        name=company_name,
        normalized_name=" ".join(company_name.casefold().split()),
    )
    session.add(company)
    session.flush()
    job = Job(
        company_id=company.id,
        title=title,
        normalized_title=" ".join(title.casefold().split()),
    )
    session.add(job)
    session.flush()
    return job


def test_storage_lock_rejects_hard_links_before_changing_permissions(
    tmp_path: Path,
) -> None:
    victim = tmp_path / "shared-file"
    victim.write_bytes(b"must remain untouched")
    victim.chmod(0o644)
    lock_path = tmp_path / ".jobby.lock"
    os.link(victim, lock_path)

    with pytest.raises(ValueError, match="multiple hard links"):
        StorageLock(lock_path, exclusive=True).acquire()

    assert stat.S_IMODE(victim.stat().st_mode) == 0o644
    assert victim.read_bytes() == b"must remain untouched"


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -1.0])
def test_storage_lock_rejects_non_finite_or_negative_timeouts(
    tmp_path: Path, timeout: float
) -> None:
    with pytest.raises(ValueError, match="finite non-negative"):
        StorageLock(tmp_path / ".jobby.lock", exclusive=True, timeout=timeout)


def test_database_paths_are_url_safe_private_and_never_back_up_over_the_live_db(
    tmp_path,
):
    path = tmp_path / "db # percent % and query ?.sqlite3"
    database = Database(path)
    database.initialize()

    assert database.path.exists()
    assert stat.S_IMODE(database.path.stat().st_mode) == 0o600
    with pytest.raises(ValueError, match="live database"):
        database.backup_to(database.path)
    assert database.integrity_check() == (True, "ok")
    database.dispose()

    target = tmp_path / "target.sqlite3"
    target.write_bytes(b"not a database")
    link = tmp_path / "database-link.sqlite3"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        Database(link)
    assert target.read_bytes() == b"not a database"


def test_database_permission_hardening_never_follows_sidecar_symlinks(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobby.sqlite3"
    path.write_bytes(b"database placeholder")
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside")
    outside.chmod(0o644)
    Path(f"{path}-wal").symlink_to(outside)
    database = Database(path, acquire_lock=False)

    with pytest.raises(OSError):
        database._secure_database_files()

    assert stat.S_IMODE(outside.stat().st_mode) == 0o644
    assert outside.read_bytes() == b"outside"
    database.dispose()


def test_database_backup_fsyncs_destination_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database, _ = _database(tmp_path)
    original_fsync = os.fsync
    synced_modes: list[int] = []

    def recording_fsync(descriptor: int) -> None:
        synced_modes.append(os.fstat(descriptor).st_mode)
        original_fsync(descriptor)

    monkeypatch.setattr("jobby.db.os.fsync", recording_fsync)
    database.backup_to(tmp_path / "backup.sqlite3")

    assert any(stat.S_ISDIR(mode) for mode in synced_modes)
    database.dispose()


def test_import_report_publication_replaces_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    victim = tmp_path / "outside-report.md"
    victim.write_text("must remain unchanged", encoding="utf-8")
    report_link = paths.artifacts_dir / "import-reconciliation.md"
    report_link.symlink_to(victim)

    report = LegacyImporter(database, paths=paths).run(workspace)

    assert victim.read_text(encoding="utf-8") == "must remain unchanged"
    assert not report_link.is_symlink()
    assert report_link.read_text(encoding="utf-8").startswith(
        "# Jobby Import Reconciliation"
    )
    assert report.report_json is not None
    assert stat.S_IMODE(Path(report.report_json).stat().st_mode) == 0o600
    database.dispose()


def test_docx_parser_rejects_excessive_expanded_archives_before_xml_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    document = tmp_path / "compressed-bomb.docx"
    with zipfile.ZipFile(document, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", b"x" * 100)
    monkeypatch.setattr("jobby.importer.MAX_DOCX_EXPANDED_BYTES", 50)

    assert extract_document_text(document) == ""


def test_explicit_database_uses_its_own_restore_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = _paths(tmp_path / "configured-home")
    configured_journal = configured.data_dir / RESTORE_JOURNAL_FILENAME
    configured_journal.write_text("unrelated interrupted restore", encoding="utf-8")
    monkeypatch.setattr("jobby.db.resolve_paths", lambda: configured)

    explicit_path = tmp_path / "isolated" / "custom.sqlite3"
    database = Database(explicit_path)
    assert (
        database.restore_journal_path == explicit_path.parent / RESTORE_JOURNAL_FILENAME
    )
    database.initialize()
    database.dispose()

    database.restore_journal_path.write_text(
        "local interrupted restore", encoding="utf-8"
    )
    blocked = Database(explicit_path)
    with pytest.raises(StorageBusyError, match="interrupted restore"):
        blocked.initialize()
    blocked.dispose()


def test_audit_redaction_covers_auth_headers_paths_and_non_finite_json() -> None:
    basic = "dXNlcjpzdXBlci1zZWNyZXQ="
    cookie = "session=super-secret-cookie"
    value = (
        f"request failed\nAuthorization: Basic {basic}\nCookie: {cookie}\nstatus=401"
    )

    redacted = redact_text(value)
    assert basic not in redacted
    assert cookie not in redacted
    assert REDACTED in redacted
    assert basic not in json_safe(Path(f"/tmp/Authorization: Basic {basic}"))

    payload = {
        "nan": json_safe(float("nan")),
        "positive_infinity": json_safe(float("inf")),
        "negative_infinity": json_safe(float("-inf")),
    }
    assert set(payload.values()) == {"[non-finite number]"}
    json.dumps(payload, allow_nan=False)
    secret_key = json_safe({f"Bearer {basic}": "value"})
    assert basic not in json.dumps(secret_key)
    assert json_safe(1 << 20_000) == "[integer exceeds supported size]"

    @dataclass
    class CyclicValue:
        child: object | None = None

    cyclic = CyclicValue()
    cyclic.child = cyclic
    assert "maximum depth exceeded" in json.dumps(json_safe(cyclic))

    class BrokenString:
        def __str__(self) -> str:
            raise RuntimeError("cannot render")

    assert json_safe(BrokenString()) == "[unserializable BrokenString]"


def test_recent_activity_rejects_boolean_and_non_integer_limits(tmp_path: Path) -> None:
    database, _ = _database(tmp_path)
    with database.session() as session:
        with pytest.raises(ValueError, match="must be an integer"):
            recent_activity(session, True)
        with pytest.raises(ValueError, match="must be an integer"):
            recent_activity(session, 1.5)
    database.dispose()


def test_database_backup_failure_is_atomic_and_preserves_existing_destination(tmp_path):
    database, _ = _database(tmp_path)
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            "CREATE TABLE deliberately_broken (company_id TEXT REFERENCES companies(id))"
        )
        connection.execute("INSERT INTO deliberately_broken VALUES ('missing')")

    destination = tmp_path / "existing.sqlite3"
    destination.write_bytes(b"keep me")
    with pytest.raises(sqlite3.IntegrityError, match="foreign-key"):
        database.backup_to(destination)
    assert destination.read_bytes() == b"keep me"
    assert not list(tmp_path.glob(".existing.sqlite3.*.tmp"))
    database.dispose()


def test_import_ignores_symlinks_and_refuses_traversal_style_report_links(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    (workspace / "data").mkdir(parents=True)
    (workspace / "reports").mkdir()
    outside = tmp_path / "outside-secret.md"
    outside.write_text("SECRET_CANARY_FROM_OUTSIDE", encoding="utf-8")
    (workspace / "reports" / "linked.md").symlink_to(outside)
    (workspace / "data" / "pipeline.md").write_text(
        "# Pipeline\n\n"
        "- [4.0/5] Safe Co — Analyst | PINNED | "
        "[Report](reports/../../outside-secret.md)\n",
        encoding="utf-8",
    )

    discovered = {
        item.relative_to(workspace).as_posix()
        for item in discover_legacy_artifacts(workspace)
    }
    assert discovered == {"data/pipeline.md"}
    report = LegacyImporter(database, paths=paths).run(workspace)

    assert any(item.category == "unsafe_path" for item in report.errors)
    with database.session() as session:
        artifacts = list(session.scalars(select(Artifact)))
        assert all(item.source_path != "reports/linked.md" for item in artifacts)
        assert "SECRET_CANARY_FROM_OUTSIDE" not in json.dumps(
            [item.metadata_json for item in artifacts]
        )
    assert outside.read_text(encoding="utf-8") == "SECRET_CANARY_FROM_OUTSIDE"
    database.dispose()


def test_import_parses_the_verified_snapshot_even_if_live_source_changes(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    portal = workspace / "portals.yml"
    portal.write_text(
        "tracked_companies:\n"
        "  - name: Original Co\n"
        "    careers_url: https://original.example/jobs\n",
        encoding="utf-8",
    )

    class MutatingImporter(LegacyImporter):
        def _reconcile_state_files(self, session):
            portal.write_text(
                "tracked_companies:\n"
                "  - name: Replaced Co\n"
                "    careers_url: https://replaced.example/jobs\n",
                encoding="utf-8",
            )
            return super()._reconcile_state_files(session)

    MutatingImporter(database, paths=paths).run(workspace)

    with database.session() as session:
        configs = list(session.scalars(select(SourceConfig)))
        artifact = session.scalar(
            select(Artifact).where(Artifact.source_path == "portals.yml")
        )
    assert [item.name for item in configs] == ["Original Co"]
    assert (
        Path(artifact.stored_path).read_text(encoding="utf-8").find("Original Co") > 0
    )
    assert "Replaced Co" in portal.read_text(encoding="utf-8")
    database.dispose()


def test_import_accepts_hash_verified_legacy_snapshot_filename(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "job_monitor_state.json.bak-2026-06-03"
    source.write_text('{"seen_jobs": []}\n', encoding="utf-8")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    legacy_snapshot = (
        paths.artifacts_dir / "imported" / digest[:2] / f"{digest}.bak-2026-06-03"
    )
    legacy_snapshot.parent.mkdir(parents=True)
    legacy_snapshot.write_bytes(source.read_bytes())
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.SOURCE_STATE,
                workspace_root=str(workspace.resolve()),
                source_path=source.name,
                stored_path=str(legacy_snapshot),
                content_hash=digest,
                size_bytes=source.stat().st_size,
                source_mtime_ns=source.stat().st_mtime_ns,
                source_immutable=True,
            )
        )

    report = LegacyImporter(database, paths=paths).run(workspace)

    assert report.imported_artifacts == 0
    assert report.skipped_artifacts == 1
    assert legacy_snapshot.read_bytes() == source.read_bytes()
    assert not (legacy_snapshot.parent / f"{digest}.bin").exists()
    with database.session() as session:
        artifacts = list(
            session.scalars(select(Artifact).where(Artifact.source_path == source.name))
        )
    assert len(artifacts) == 1
    assert artifacts[0].stored_path == str(legacy_snapshot)
    database.dispose()


def test_normal_import_repairs_hash_verified_pathless_legacy_registration(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "job_monitor_state.json"
    source.write_text('{"seen_jobs": []}\n', encoding="utf-8")
    original_bytes = source.read_bytes()
    original_stat = source.stat()
    digest = hashlib.sha256(original_bytes).hexdigest()
    with database.session() as session:
        artifact = Artifact(
            kind=ArtifactKind.SOURCE_STATE,
            workspace_root=str(workspace.resolve()),
            source_path=source.name,
            stored_path=None,
            content_hash=digest,
            size_bytes=source.stat().st_size,
            source_mtime_ns=source.stat().st_mtime_ns,
            source_immutable=True,
        )
        session.add(artifact)
        session.flush()
        artifact_id = artifact.id

    report = LegacyImporter(database, paths=paths).run(workspace)

    assert report.imported_artifacts == 0
    assert report.skipped_artifacts == 1
    assert report.copied_artifacts == 1
    assert source.read_bytes() == original_bytes
    assert source.stat().st_mtime_ns == original_stat.st_mtime_ns
    with database.session() as session:
        artifact = session.get(Artifact, artifact_id)
        assert artifact is not None and artifact.stored_path is not None
        snapshot = Path(artifact.stored_path)
        assert snapshot.is_relative_to(paths.artifacts_dir / "imported")
        assert snapshot.read_bytes() == original_bytes
        assert artifact.content_hash == digest
        assert artifact.source_immutable is True
        assert session.scalar(
            select(AuditEvent).where(
                AuditEvent.action == "artifact.snapshot_backfilled",
                AuditEvent.entity_id == artifact_id,
            )
        )

    backup = create_backup(database, output=tmp_path / "repaired.zip", paths=paths)
    assert verify_backup(backup) == (True, "ok")
    database.dispose()


def test_import_rejects_unverified_legacy_snapshot_filename(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "job_monitor_state.json.bak-2026-06-03"
    source.write_text('{"seen_jobs": []}\n', encoding="utf-8")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    corrupted_snapshot = (
        paths.artifacts_dir / "imported" / digest[:2] / f"{digest}.bak-2026-06-03"
    )
    corrupted_snapshot.parent.mkdir(parents=True)
    corrupted_snapshot.write_text("corrupted", encoding="utf-8")
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.SOURCE_STATE,
                workspace_root=str(workspace.resolve()),
                source_path=source.name,
                stored_path=str(corrupted_snapshot),
                content_hash=digest,
                size_bytes=source.stat().st_size,
                source_mtime_ns=source.stat().st_mtime_ns,
                source_immutable=True,
            )
        )

    with pytest.raises(RuntimeError, match="immutable artifact storage mismatch"):
        LegacyImporter(database, paths=paths).run(workspace)
    database.dispose()


def test_import_rejects_matching_legacy_snapshot_outside_managed_root(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "job_monitor_state.json.bak-2026-06-03"
    source.write_text('{"seen_jobs": []}\n', encoding="utf-8")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    outside_snapshot = tmp_path / f"{digest}.bak-2026-06-03"
    outside_snapshot.write_bytes(source.read_bytes())
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.SOURCE_STATE,
                workspace_root=str(workspace.resolve()),
                source_path=source.name,
                stored_path=str(outside_snapshot),
                content_hash=digest,
                size_bytes=source.stat().st_size,
                source_mtime_ns=source.stat().st_mtime_ns,
                source_immutable=True,
            )
        )

    with pytest.raises(RuntimeError, match="immutable artifact storage mismatch"):
        LegacyImporter(database, paths=paths).run(workspace)
    assert outside_snapshot.read_bytes() == source.read_bytes()
    database.dispose()


def test_recursive_yaml_is_queued_for_review_instead_of_crashing_import(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    profile = workspace / "config" / "profile.yml"
    profile.parent.mkdir(parents=True)
    profile.write_text("profile: &profile\n  recursive: *profile\n", encoding="utf-8")

    report = LegacyImporter(database, paths=paths).run(workspace)

    assert any("recursive alias" in item.message for item in report.unparsed)
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(ImportReview)) == 1
    database.dispose()


def test_import_omits_credential_shaped_profile_facts_and_private_portal_urls(tmp_path):
    database, paths = _database(tmp_path)
    workspace = tmp_path / "workspace"
    profile = workspace / "config" / "profile.yml"
    profile.parent.mkdir(parents=True)
    profile.write_text(
        "identity:\n"
        "  name: Mike\n"
        "  work_authorization: true\n"
        "  openai_api_key: sk-profile-secret-123456789\n",
        encoding="utf-8",
    )
    (workspace / "portals.yml").write_text(
        "tracked_companies:\n"
        "  - name: Unsafe Portal\n"
        "    careers_url: https://user:password@example.test/jobs\n",
        encoding="utf-8",
    )

    report = LegacyImporter(database, paths=paths).run(workspace)

    with database.session() as session:
        facts = list(session.scalars(select(ProfileFact)))
        configs = list(session.scalars(select(SourceConfig)))
        reviews = list(session.scalars(select(ImportReview)))
    assert {(fact.fact_key, fact.value_json) for fact in facts} == {
        ("identity.name", "Mike"),
        ("identity.work_authorization", True),
    }
    assert configs == []
    assert len(reviews) == 2
    assert "sk-profile-secret" not in json.dumps(report.to_dict())
    assert all(
        "sk-profile-secret" not in (review.raw_excerpt or "") for review in reviews
    )
    database.dispose()


def test_audit_and_json_export_redact_nested_credentials_but_keep_usage_metrics(
    tmp_path,
):
    database, _ = _database(tmp_path)
    canary = "sk-this-is-a-secret-canary-123456"
    cyclic: dict[str, object] = {"label": "cycle"}
    cyclic["self"] = cyclic
    with database.session() as session:
        event = record_audit(
            session,
            action="security.test",
            entity_type="test",
            after={
                "openai_api_key": canary,
                "nested": {"google_oauth_token": "ya29.secret-token"},
                "input_tokens": 123,
                "note": f"Authorization: Bearer {canary}",
                "provider_error": "Google rejected AIza1234567890abcdefghijklmnop",
                "oauth_error": "client GOCSPX-abcdefghijklmnop was rejected",
                "cyclic": cyclic,
            },
            detail=f"password={canary}",
        )
        session.add(
            SourceConfig(
                provider="test",
                name="unsafe-config",
                config_json={
                    "client_secret": canary,
                    "endpoint": "https://example.test",
                },
            )
        )
        session.flush()
        event_id = event.id

    output = export_data(database, "json", tmp_path / "export.json")
    serialized = output.read_text(encoding="utf-8")
    payload = json.loads(serialized)

    assert canary not in serialized
    assert "ya29.secret-token" not in serialized
    assert "AIza1234567890abcdefghijklmnop" not in serialized
    assert "GOCSPX-abcdefghijklmnop" not in serialized
    with database.session() as session:
        event = session.get(AuditEvent, event_id)
    assert event.after_json["openai_api_key"] == REDACTED
    assert event.after_json["nested"]["google_oauth_token"] == REDACTED
    assert event.after_json["input_tokens"] == 123
    assert "maximum depth exceeded" in str(event.after_json["cyclic"])
    assert REDACTED in event.detail
    exported_config = next(
        row for row in payload["source_configs"] if row["name"] == "unsafe-config"
    )
    assert exported_config["config_json"]["client_secret"] == REDACTED
    database.dispose()


def test_csv_export_neutralizes_formula_cells_and_atomic_export_replaces_symlink(
    tmp_path,
):
    database, _ = _database(tmp_path)
    with database.session() as session:
        _job(
            session,
            company_name='=HYPERLINK("https://evil.test")',
            title="@SUM(1+1)",
        ).canonical_url = "-cmd|' /C calc'!A0"

    csv_path = export_data(database, "csv", tmp_path / "jobs.csv")
    with csv_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["company"].startswith("'=")
    assert row["title"].startswith("'@")
    assert row["url"].startswith("'-")
    assert stat.S_IMODE(csv_path.stat().st_mode) == 0o600

    victim = tmp_path / "victim.txt"
    victim.write_text("do not overwrite", encoding="utf-8")
    output_link = tmp_path / "portable.json"
    output_link.symlink_to(victim)
    export_data(database, "json", output_link)
    assert not output_link.is_symlink()
    assert victim.read_text(encoding="utf-8") == "do not overwrite"
    assert (
        json.loads(output_link.read_text(encoding="utf-8"))["schema"]
        == "jobby-export-v1"
    )

    with pytest.raises(ValueError, match="operational database"):
        export_data(database, "json", database.path)
    assert database.integrity_check() == (True, "ok")
    database.dispose()


def test_export_rejects_operational_paths_hidden_behind_parent_symlinks(
    tmp_path: Path,
) -> None:
    database, paths = _database(tmp_path)
    data_alias = tmp_path / "data-alias"
    data_alias.symlink_to(paths.data_dir, target_is_directory=True)
    config_alias = tmp_path / "config-alias"
    config_alias.symlink_to(paths.config_dir, target_is_directory=True)

    with pytest.raises(ValueError, match="operational database"):
        export_data(database, "json", data_alias / database.path.name)
    with pytest.raises(ValueError, match="operational database"):
        export_data(database, "json", config_alias / paths.config_file.name)

    assert database.integrity_check() == (True, "ok")
    assert paths.config_file.exists() is False
    database.dispose()


def test_export_rejects_immutable_artifact_hidden_behind_parent_symlink(
    tmp_path: Path,
) -> None:
    database, paths = _database(tmp_path)
    immutable = paths.artifacts_dir / "source.md"
    immutable.write_text("source provenance", encoding="utf-8")
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.RESUME,
                stored_path=str(immutable),
                content_hash=hashlib.sha256(immutable.read_bytes()).hexdigest(),
                size_bytes=immutable.stat().st_size,
                source_immutable=True,
            )
        )
    artifact_alias = tmp_path / "artifact-alias"
    artifact_alias.symlink_to(paths.artifacts_dir, target_is_directory=True)

    with pytest.raises(ValueError, match="immutable source artifact"):
        export_data(database, "json", artifact_alias / immutable.name)

    assert immutable.read_text(encoding="utf-8") == "source provenance"
    database.dispose()


def test_failed_export_preserves_previous_file_and_removes_staging_file(
    tmp_path, monkeypatch
):
    database, _ = _database(tmp_path)
    output = tmp_path / "existing.json"
    output.write_text("keep previous export", encoding="utf-8")

    def fail_after_partial_write(_database, temporary):
        temporary.write_text("partial", encoding="utf-8")
        raise RuntimeError("simulated export interruption")

    monkeypatch.setattr(exporter_module, "_export_json", fail_after_partial_write)
    with pytest.raises(RuntimeError, match="interruption"):
        export_data(database, "json", output)
    assert output.read_text(encoding="utf-8") == "keep previous export"
    assert not list(tmp_path.glob(".existing.json.*.tmp"))
    database.dispose()


def test_reexport_to_managed_path_refreshes_one_artifact_and_remains_backup_safe(
    tmp_path,
):
    database, paths = _database(tmp_path)
    output = paths.artifacts_dir / "exports" / "portable.json"

    export_data(database, "json", output)
    with database.session() as session:
        first = list(
            session.scalars(select(Artifact).where(Artifact.stored_path == str(output)))
        )
        assert len(first) == 1
        artifact_id = first[0].id
        first_hash = first[0].content_hash

    export_data(database, "json", output)
    actual_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    with database.session() as session:
        artifacts = list(
            session.scalars(select(Artifact).where(Artifact.stored_path == str(output)))
        )
        assert len(artifacts) == 1
        assert artifacts[0].id == artifact_id
        assert artifacts[0].content_hash == actual_hash
        assert artifacts[0].content_hash != first_hash

    backup = create_backup(database, output=tmp_path / "export-safe.zip", paths=paths)
    assert verify_backup(backup) == (True, "ok")
    database.dispose()


def test_backup_only_reads_hash_matching_managed_artifacts_and_is_private(tmp_path):
    database, paths = _database(tmp_path)
    managed = paths.artifacts_dir / "resume.md"
    managed.write_text("managed resume", encoding="utf-8")
    outside = tmp_path / "outside-secret.txt"
    outside.write_text("BACKUP_SECRET_CANARY", encoding="utf-8")
    linked = paths.artifacts_dir / "linked-secret.txt"
    linked.symlink_to(outside)
    with database.session() as session:
        for source, name in (
            (managed, "managed"),
            (outside, "outside"),
            (linked, "linked"),
        ):
            session.add(
                Artifact(
                    kind=ArtifactKind.OTHER,
                    stored_path=str(source),
                    content_hash=hashlib.sha256(source.read_bytes()).hexdigest(),
                    size_bytes=source.stat().st_size,
                    source_immutable=False,
                    metadata_json={"name": name},
                )
            )

    destination = tmp_path / "backup.zip"
    with pytest.raises(
        RuntimeError,
        match=r"backup verification failed: backup is incomplete: 2 managed artifact",
    ):
        create_backup(database, output=destination, paths=paths)
    assert not destination.exists()
    assert not list(tmp_path.glob(".backup.zip.*.tmp"))

    managed.write_text("tampered after registration", encoding="utf-8")
    existing = tmp_path / "existing.zip"
    existing.write_bytes(b"keep existing backup")
    with pytest.raises(FileExistsError, match="already exists"):
        create_backup(database, output=existing, paths=paths)
    assert existing.read_bytes() == b"keep existing backup"
    with pytest.raises(ValueError, match="hash mismatch"):
        create_backup(database, output=tmp_path / "new.zip", paths=paths)
    assert not (tmp_path / "new.zip").exists()
    database.dispose()


def test_backup_verifier_rejects_missing_outside_and_pathless_artifacts(tmp_path):
    database, paths = _database(tmp_path)
    missing = paths.artifacts_dir / "missing.md"
    outside = tmp_path / "outside.md"
    outside.write_text("outside", encoding="utf-8")
    with database.session() as session:
        session.add_all(
            [
                Artifact(
                    kind=ArtifactKind.OTHER,
                    stored_path=str(missing),
                    content_hash=hashlib.sha256(b"missing").hexdigest(),
                    size_bytes=7,
                ),
                Artifact(
                    kind=ArtifactKind.OTHER,
                    stored_path=str(outside),
                    content_hash=hashlib.sha256(outside.read_bytes()).hexdigest(),
                    size_bytes=outside.stat().st_size,
                ),
                Artifact(
                    kind=ArtifactKind.OTHER,
                    stored_path=None,
                    content_hash=hashlib.sha256(b"").hexdigest(),
                    size_bytes=0,
                ),
            ]
        )

    destination = tmp_path / "incomplete.zip"
    with pytest.raises(
        RuntimeError,
        match=(
            r"backup verification failed: backup is incomplete: 3 managed artifact\(s\) "
            r"were skipped \(1 no stored path, 1 stored file is missing, "
            r"1 stored file is outside managed storage\)"
        ),
    ):
        create_backup(database, output=destination, paths=paths)
    assert not destination.exists()
    database.dispose()


def test_external_generated_export_is_explicitly_optional_in_backup(tmp_path):
    database, paths = _database(tmp_path)
    external_export = tmp_path / "portable-export.json"
    export_data(database, "json", external_export)

    backup = create_backup(database, output=tmp_path / "complete.zip", paths=paths)

    assert verify_backup(backup) == (True, "ok")
    with zipfile.ZipFile(backup) as archive:
        manifest = json.loads(archive.read("manifest.json"))
    assert manifest["artifacts"] == []
    assert manifest["skipped_artifacts"] == []
    assert len(manifest["excluded_artifacts"]) == 1
    assert manifest["excluded_artifacts"][0]["kind"] == "generated_export"
    assert manifest["excluded_artifacts"][0]["reason"] == "external generated export"
    database.dispose()


def test_default_backup_names_do_not_collide_within_one_second(tmp_path):
    database, paths = _database(tmp_path)

    first = create_backup(database, paths=paths)
    second = create_backup(database, paths=paths)

    assert first != second
    assert verify_backup(first) == (True, "ok")
    assert verify_backup(second) == (True, "ok")
    database.dispose()


def test_missing_managed_generated_export_remains_a_hard_backup_failure(tmp_path):
    database, paths = _database(tmp_path)
    missing = paths.artifacts_dir / "missing-export.json"
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.GENERATED_EXPORT,
                stored_path=str(missing),
                content_hash=hashlib.sha256(b"missing export").hexdigest(),
                size_bytes=14,
                source_immutable=False,
            )
        )

    destination = tmp_path / "incomplete.zip"
    with pytest.raises(
        RuntimeError,
        match=r"backup verification failed: backup is incomplete: 1 managed artifact",
    ):
        create_backup(database, output=destination, paths=paths)
    assert not destination.exists()
    database.dispose()


def test_backup_verifier_authenticates_config_and_rejects_unmanifested_members(
    tmp_path,
):
    database, paths = _database(tmp_path)
    paths.config_file.write_text('timezone = "America/Los_Angeles"\n', encoding="utf-8")
    original = create_backup(database, output=tmp_path / "original.zip", paths=paths)
    tampered = tmp_path / "tampered.zip"
    with zipfile.ZipFile(original) as source, zipfile.ZipFile(tampered, "w") as target:
        for info in source.infolist():
            content = source.read(info)
            if info.filename == "config/config.toml":
                content = b'openai_api_key = "stolen"\n'
            target.writestr(info, content)
    assert verify_backup(tampered)[0] is False

    extra = tmp_path / "extra.zip"
    with zipfile.ZipFile(original) as source, zipfile.ZipFile(extra, "w") as target:
        for info in source.infolist():
            target.writestr(info, source.read(info))
        target.writestr("../unexpected", b"payload")
    assert verify_backup(extra) == (False, "backup contains unmanifested members")
    database.dispose()


def test_backup_verifier_never_follows_archive_symlinks(tmp_path: Path) -> None:
    database, paths = _database(tmp_path)
    original = create_backup(database, output=tmp_path / "original.zip", paths=paths)
    alias = tmp_path / "alias.zip"
    alias.symlink_to(original)

    verified, detail = verify_backup(alias)

    assert verified is False
    assert "symbolic link" in detail
    database.dispose()


def test_partial_unique_index_allows_only_one_current_evaluation(tmp_path):
    database, _ = _database(tmp_path)
    with pytest.raises(IntegrityError):
        with database.session() as session:
            job = _job(session)
            session.add_all(
                [
                    Evaluation(job_id=job.id, score=3.0, is_current=True),
                    Evaluation(job_id=job.id, score=4.0, is_current=True),
                ]
            )
    database.dispose()
