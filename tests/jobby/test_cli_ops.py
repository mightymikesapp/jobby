from __future__ import annotations

import hashlib
import json
import os
import plistlib
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select

from jobby import cli
from jobby.agent import DailyAgent
from jobby.backup import create_backup, verify_backup
from jobby.config import AppConfig, JobbyPaths
from jobby.db import Database
from jobby.doctor import DoctorCheck, doctor_exit_code, run_doctor
from jobby.enums import (
    AgentRunStatus,
    ApplicationStage,
    ArtifactKind,
    JobStatus,
    TaskStatus,
)
from jobby.exporter import export_data
from jobby.models import (
    Application,
    AgentRun,
    Alert,
    Artifact,
    AuditEvent,
    BackupRecord,
    Base,
    Company,
    Interview,
    IntegrationState,
    Job,
    ScanRun,
    SourceObservation,
    Task,
)
from jobby.notifications import NativeNotificationProvider
from jobby.planner import ALLOCATION, allocation_targets, build_daily_plan, classify_job
from jobby.scheduler import LABEL, ScheduleStatus, Scheduler, _agent_command


def test_cli_import_defers_subcommand_dependency_graph() -> None:
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, sys; import jobby.cli; "
                "names=('jobby.agent','jobby.importer','jobby.scheduler',"
                "'jobby.doctor','jobby.exporter','jobby.sources.base','sqlalchemy'); "
                "print(json.dumps([name for name in names if name in sys.modules]))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(probe.stdout) == []


def make_paths(root: Path) -> JobbyPaths:
    data = root / "data files"
    config = root / "config files"
    cache = root / "cache files"
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


@pytest.fixture
def paths(tmp_path) -> JobbyPaths:
    return make_paths(tmp_path / "Jobby Home")


@pytest.fixture
def database(paths: JobbyPaths) -> Database:
    db = Database(paths=paths)
    db.initialize()
    yield db
    db.dispose()


def seed_job(database: Database, *, title: str = "Legal Operations Analyst") -> str:
    with database.session() as session:
        company = Company(name="Example", normalized_name="example")
        session.add(company)
        session.flush()
        job = Job(
            company_id=company.id,
            title=title,
            normalized_title=title.casefold(),
            canonical_url="https://jobs.example.test/1",
            source_primary="manual",
            source_id="1",
            status=JobStatus.DISCOVERED,
            latest_score=4.2,
        )
        session.add(job)
        session.flush()
        return job.id


def test_cli_help_lists_the_supported_offline_operations(capsys) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["--help"])
    assert raised.value.code == 0
    help_text = capsys.readouterr().out
    for command in (
        "import",
        "scan",
        "agent",
        "schedule",
        "profiles",
        "source",
        "export",
        "backup",
        "capture",
        "maintenance",
        "restore",
        "upgrade",
        "config",
        "doctor",
        "credentials",
        "google",
    ):
        assert command in help_text


def test_cli_profile_management_and_empty_source_health(
    monkeypatch, paths: JobbyPaths, capsys
) -> None:
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)

    assert (
        cli.main(
            [
                "profiles",
                "create",
                "--name",
                "Privacy counsel",
                "--source",
                "workday",
                "--query",
                "privacy counsel",
                "--enable",
            ]
        )
        == 0
    )
    created = json.loads(capsys.readouterr().out)
    assert created["enabled"] is True
    assert created["query_pack"] == ["privacy counsel"]

    assert cli.main(["profiles", "show", "PRIVACY COUNSEL"]) == 0
    assert json.loads(capsys.readouterr().out)["id"] == created["id"]
    assert (
        cli.main(
            [
                "profiles",
                "update",
                created["id"],
                "--disable",
                "--clear-queries",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["enabled"] is False

    assert cli.main(["source", "health"]) == 0
    assert json.loads(capsys.readouterr().out) == []
    assert cli.main(["profiles", "delete", created["id"], "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["deleted"]["id"] == created["id"]


def test_cli_config_can_initialize_update_validate_and_repair_malformed_toml(
    monkeypatch, paths: JobbyPaths, capsys
) -> None:
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)

    assert cli.main(["config", "init"]) == 0
    assert paths.config_file.exists()
    assert cli.main(["config", "set", "scheduled_web_enabled", "true"]) == 0
    assert cli.main(["config", "show"]) == 0
    shown = capsys.readouterr().out
    assert '"scheduled_web_enabled": true' in shown
    assert cli.main(["config", "validate"]) == 0

    paths.config_file.write_text("not = [valid\n", encoding="utf-8")
    assert cli.main(["config", "init", "--force"]) == 0
    assert cli.load_config(paths) == AppConfig()


@pytest.mark.parametrize("value", ["0", "366", "not-a-number"])
def test_cli_rejects_invalid_calendar_sync_ranges(value: str) -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["google", "calendar-sync", "--days", value])


def test_cli_disallows_bypassing_google_revocation_via_generic_credential_delete() -> (
    None
):
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["credentials", "delete", "google-token"])


def test_google_disconnect_failure_keeps_integration_enabled_for_retry(
    monkeypatch, database: Database, paths: JobbyPaths
) -> None:
    from jobby.google_integration import GoogleDisconnectResult, GoogleOAuthManager

    with database.session() as session:
        session.add(
            IntegrationState(
                provider="gmail", enabled=True, health="ok", scopes=["read-only"]
            )
        )
    monkeypatch.setattr(
        GoogleOAuthManager,
        "disconnect",
        lambda *_args, **_kwargs: GoogleDisconnectResult(
            False,
            False,
            "remote revocation failed; local token retained",
            authorization_retained=True,
        ),
    )
    config = AppConfig(google_enabled=True)
    result = cli._google(
        SimpleNamespace(google_command="disconnect", local_only=False),
        database,
        config,
        paths,
        MappingSecrets(),
    )
    assert result == 1
    assert config.google_enabled is True
    with database.session() as session:
        state = session.scalar(
            select(IntegrationState).where(IntegrationState.provider == "gmail")
        )
        assert state is not None and state.enabled is True and state.health == "ok"


def test_cli_import_empty_workspace_and_repeated_import_are_successful(
    monkeypatch,
    paths: JobbyPaths,
    tmp_path,
    capsys,
) -> None:
    workspace = tmp_path / "legacy-workspace"
    workspace.mkdir()
    (workspace / "notes.md").write_text("# Job search notes\n", encoding="utf-8")
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)

    first = cli.main(["import", str(workspace)])
    second = cli.main(["import", str(workspace)])

    output = capsys.readouterr().out
    assert first == second == 0
    assert "Imported" in output
    assert "Reconciliation:" in output
    assert "skipped" in output
    assert (paths.artifacts_dir / "import-reconciliation.md").exists()
    with Database(paths=paths).session() as session:
        artifact = session.scalar(
            select(Artifact).where(Artifact.source_path == "notes.md")
        )
        assert artifact is not None
        assert artifact.stored_path is not None
        assert Path(artifact.stored_path).is_relative_to(paths.artifacts_dir)


def test_cli_import_no_longer_accepts_pathless_no_copy_mode() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["import", "/workspace", "--no-copy"])


def test_cli_export_backup_and_doctor_paths(
    monkeypatch,
    paths: JobbyPaths,
    database: Database,
    tmp_path,
    capsys,
) -> None:
    seed_job(database)
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    exported = tmp_path / "portable.json"
    backup = tmp_path / "snapshot.zip"

    assert cli.main(["export", "--format", "json", "--output", str(exported)]) == 0
    assert cli.main(["backup", "--output", str(backup)]) == 0

    seen: dict[str, object] = {}

    def fake_doctor(db, config, received_paths, *, secrets, check_network):
        seen.update(paths=received_paths, check_network=check_network)
        return [
            DoctorCheck("database", "pass", "ok"),
            DoctorCheck("google", "warn", "disabled"),
        ]

    monkeypatch.setattr(cli, "run_doctor", fake_doctor)
    assert cli.main(["doctor", "--no-network"]) == 0
    assert seen == {"paths": paths, "check_network": False}
    assert exported.exists()
    assert backup.exists()
    assert verify_backup(backup) == (True, "ok")
    output = capsys.readouterr().out
    assert str(exported) in output
    assert "✓ database: ok" in output
    assert "! google: disabled" in output


def test_encrypted_backup_is_recorded_and_reported_when_keyring_storage_fails(
    monkeypatch: pytest.MonkeyPatch,
    paths: JobbyPaths,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from jobby import encrypted_backup as encrypted

    passphrase = "retained-elsewhere-test-passphrase"

    class FailingKeyringSecrets:
        def get(self, name: str) -> str:
            assert name == "external_backup_passphrase"
            return passphrase

        def set(self, name: str, value: str) -> None:
            assert name == "external_backup_passphrase"
            assert value == passphrase
            raise RuntimeError(f"injected keyring failure for {passphrase}")

    config = AppConfig(external_backup_scrypt_n=encrypted.MIN_SCRYPT_N)
    local = tmp_path / "manual-backup.zip"
    external = tmp_path / "manual-backup.jobbyenc"
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    monkeypatch.setattr(cli, "load_config", lambda _paths: config)
    monkeypatch.setattr("jobby.config.SecretStore", lambda: FailingKeyringSecrets())

    exit_code = cli.main(
        [
            "backup",
            "--output",
            str(local),
            "--encrypt-to",
            str(external),
            "--store-passphrase",
            "--confirm-passphrase-retained",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 1
    assert payload["status"] == "partial"
    assert payload["local_backup"] == str(local.absolute())
    assert payload["encrypted_backup"] == str(external.absolute())
    assert payload["recovery_tested"] is True
    assert payload["scrypt_n"] == encrypted.MIN_SCRYPT_N
    assert payload["passphrase_storage"] == {
        "requested": True,
        "stored": False,
        "error": "injected keyring failure for [redacted]",
    }
    assert "encrypted backup succeeded and was recorded" in captured.err
    assert passphrase not in captured.out
    assert passphrase not in captured.err
    assert verify_backup(local) == (True, "ok")
    assert external.exists()
    with external.open("rb") as handle:
        fields = encrypted._HEADER.unpack(handle.read(encrypted._HEADER.size))
    assert fields[3] == encrypted.MIN_SCRYPT_LOG_N

    inspection = Database(paths=paths)
    inspection.initialize()
    try:
        with inspection.session() as session:
            records = list(
                session.scalars(select(BackupRecord).order_by(BackupRecord.external))
            )
            assert len(records) == 2
            external_record = next(record for record in records if record.external)
            assert external_record.path == str(external.absolute())
            assert external_record.recovery_tested_at is not None
            failed_audit = session.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "backup.passphrase_store_failed"
                )
            )
            assert failed_audit is not None
            assert failed_audit.entity_id == external_record.id
            assert failed_audit.after_json == {
                "stored": False,
                "error": "injected keyring failure for [redacted]",
            }
            assert passphrase not in json.dumps(failed_audit.after_json)
    finally:
        inspection.dispose()


def test_encrypted_backup_prompt_mismatch_retains_and_records_local_backup(
    monkeypatch: pytest.MonkeyPatch,
    paths: JobbyPaths,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from jobby.encrypted_backup import MIN_SCRYPT_N

    class EmptySecrets:
        def get(self, _name: str) -> None:
            return None

    prompted = iter(("secret-one", "secret-two"))
    local = tmp_path / "retained-local.zip"
    external = tmp_path / "not-published.jobbyenc"
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda _paths: AppConfig(external_backup_scrypt_n=MIN_SCRYPT_N),
    )
    monkeypatch.setattr("jobby.config.SecretStore", EmptySecrets)
    monkeypatch.setattr(cli.getpass, "getpass", lambda _prompt: next(prompted))

    exit_code = cli.main(
        [
            "backup",
            "--output",
            str(local),
            "--encrypt-to",
            str(external),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["status"] == "partial"
    assert payload["local_backup"] == str(local.absolute())
    assert payload["encrypted_backup"] is None
    assert "passphrases did not match" in payload["error"]
    assert verify_backup(local) == (True, "ok")
    assert not external.exists()
    inspection = Database(paths=paths)
    inspection.initialize()
    try:
        with inspection.session() as session:
            records = tuple(session.scalars(select(BackupRecord)))
            assert len(records) == 1 and records[0].external is False
            failure = session.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "backup.encryption_failed"
                )
            )
            assert failure is not None
            assert failure.entity_id == records[0].id
    finally:
        inspection.dispose()


def test_cli_web_scan_requires_query_without_network(
    monkeypatch, paths: JobbyPaths, capsys
) -> None:
    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    assert cli.main(["scan", "--source", "web"]) == 1
    assert "--query is required" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("action", "status", "expected"),
    [
        ("install", ScheduleStatus("test", True, False, "/tmp/unit"), 1),
        ("status", ScheduleStatus("test", True, False, "/tmp/unit"), 1),
        ("status", ScheduleStatus("test", True, True, "/tmp/unit"), 1),
        (
            "status",
            ScheduleStatus("test", True, True, "/tmp/unit", matches_config=True),
            0,
        ),
        (
            "rebind",
            ScheduleStatus("test", True, True, "/tmp/unit", matches_config=True),
            0,
        ),
        ("rebind", ScheduleStatus("test", False, False, "/tmp/unit"), 1),
        ("remove", ScheduleStatus("test", True, True, "/tmp/unit"), 1),
        ("remove", ScheduleStatus("test", False, False, "/tmp/unit"), 0),
    ],
)
def test_cli_scheduler_exit_code_reflects_enabled_or_removed_state(
    monkeypatch,
    paths: JobbyPaths,
    action: str,
    status: ScheduleStatus,
    expected: int,
) -> None:
    class FakeScheduler:
        def __init__(self, *_args, **_kwargs):
            pass

        def install(self):
            return status

        def status(self):
            return status

        def remove(self):
            return status

        def rebind(self):
            return status

    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    monkeypatch.setattr(cli, "Scheduler", FakeScheduler)
    assert cli.main(["schedule", action]) == expected


def test_schedule_status_does_not_open_or_bootstrap_sqlite(
    monkeypatch: pytest.MonkeyPatch, paths: JobbyPaths
) -> None:
    class FakeScheduler:
        def __init__(self, *_args, **_kwargs):
            pass

        def status(self) -> ScheduleStatus:
            return ScheduleStatus("test", True, True, "/tmp/unit", matches_config=True)

    monkeypatch.setattr(cli, "resolve_paths", lambda: paths)
    monkeypatch.setattr(cli, "Scheduler", FakeScheduler)
    monkeypatch.setattr(
        "jobby.db.Database",
        lambda *_args, **_kwargs: pytest.fail("schedule must not construct Database"),
    )

    assert cli.main(["schedule", "status"]) == 0
    assert not paths.database.exists()


def test_cli_validates_credential_files_and_prompted_google_json(
    monkeypatch,
    tmp_path,
) -> None:
    stored: dict[str, str] = {}
    secrets = SimpleNamespace(
        set=lambda key, value: stored.__setitem__(key, value),
        delete=lambda _key: None,
    )
    invalid = SimpleNamespace(
        credentials_command="set", name="google-client", file=None
    )
    monkeypatch.setattr(cli.getpass, "getpass", lambda _prompt: "not-json")
    with pytest.raises(ValueError, match="valid JSON"):
        cli._credentials(invalid, secrets)

    client_file = tmp_path / "client.json"
    client_file.write_text(
        json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8"
    )
    valid = SimpleNamespace(
        credentials_command="set", name="google-client", file=client_file
    )
    assert cli._credentials(valid, secrets) == 0
    assert (
        json.loads(stored["google_oauth_client"])["installed"]["client_id"]
        == "client-id"
    )

    unsupported_file = SimpleNamespace(
        credentials_command="set", name="openai", file=client_file
    )
    with pytest.raises(ValueError, match="only for google-client"):
        cli._credentials(unsupported_file, secrets)

    linked = tmp_path / "linked-client.json"
    linked.symlink_to(client_file)
    linked_args = SimpleNamespace(
        credentials_command="set", name="google-client", file=linked
    )
    with pytest.raises(ValueError, match="must not be a symbolic link"):
        cli._credentials(linked_args, secrets)


@pytest.mark.parametrize("value", ["0", "501", "not-a-number"])
def test_cli_rejects_invalid_gmail_sync_limits(value: str) -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["google", "gmail-sync", "--limit", value])


def test_blank_gmail_query_is_rejected_before_provider_or_network(
    monkeypatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    monkeypatch.setattr(
        "jobby.google_integration.GmailProvider",
        lambda *_args, **_kwargs: pytest.fail("provider must not be constructed"),
    )
    args = SimpleNamespace(google_command="gmail-sync", query="   ", limit=10)
    with pytest.raises(ValueError, match="must not be blank"):
        cli._google(args, database, AppConfig(), paths, MappingSecrets())


def test_export_json_contains_every_nonsecret_domain_table_and_registers_artifact(
    database: Database,
    tmp_path,
) -> None:
    seed_job(database)
    destination = tmp_path / "export.json"

    export_data(database, "json", destination)
    payload = json.loads(destination.read_text(encoding="utf-8"))

    assert set(Base.metadata.tables).issubset(payload)
    serialized = destination.read_text(encoding="utf-8")
    assert "openai_api_key" not in serialized
    assert "google_oauth_token" not in serialized
    with database.session() as session:
        artifact = session.scalar(
            select(Artifact).where(
                Artifact.kind == ArtifactKind.GENERATED_EXPORT,
                Artifact.stored_path == str(destination),
            )
        )
        assert artifact is not None
        event = session.scalar(
            select(AuditEvent).where(
                AuditEvent.action == "export.created",
                AuditEvent.entity_id == artifact.id,
            )
        )
        assert event is not None


@pytest.mark.parametrize("format_name,suffix", [("markdown", "md"), ("csv", "csv")])
def test_job_centric_exports_are_created(
    database: Database, tmp_path, format_name: str, suffix: str
) -> None:
    seed_job(database)
    output = tmp_path / f"jobs.{suffix}"
    assert export_data(database, format_name, output) == output
    text = output.read_text(encoding="utf-8")
    assert "Legal Operations Analyst" in text
    assert "Example" in text


def test_backup_contains_verified_database_artifacts_and_nonsecret_config(
    database: Database,
    paths: JobbyPaths,
    tmp_path,
) -> None:
    stored = paths.artifacts_dir / "resume.md"
    stored.write_text("# Resume\n", encoding="utf-8")
    digest = hashlib.sha256(stored.read_bytes()).hexdigest()
    with database.session() as session:
        session.add(
            Artifact(
                kind=ArtifactKind.RESUME,
                stored_path=str(stored),
                content_hash=digest,
                size_bytes=stored.stat().st_size,
            )
        )
    paths.config_file.write_text('timezone = "America/Los_Angeles"\n', encoding="utf-8")

    backup = create_backup(database, output=tmp_path / "backup", paths=paths)

    assert backup.suffix == ".zip"
    assert verify_backup(backup) == (True, "ok")
    with zipfile.ZipFile(backup) as archive:
        names = archive.namelist()
        assert "database/jobby.sqlite3" in names
        assert "config/config.toml" in names
        assert any(name.endswith("/resume.md") for name in names)


def test_verify_backup_rejects_unknown_schema(tmp_path) -> None:
    path = tmp_path / "invalid.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("manifest.json", json.dumps({"schema": "unknown"}))
        archive.writestr("database/jobby.sqlite3", b"not sqlite")
    assert verify_backup(path) == (False, "unsupported backup schema")


class RecordingRunner:
    def __init__(self, returncodes: list[int] | None = None):
        self.commands: list[list[str]] = []
        self.returncodes = list(returncodes or [])

    def __call__(self, command, **_kwargs):
        self.commands.append(list(command))
        code = self.returncodes.pop(0) if self.returncodes else 0
        return subprocess.CompletedProcess(
            command, code, "enabled\n" if code == 0 else "", ""
        )


def test_scheduler_uses_frozen_executable_without_python_module_flags(
    monkeypatch,
) -> None:
    monkeypatch.setattr("jobby.scheduler.sys.frozen", True, raising=False)
    monkeypatch.setattr("jobby.scheduler.sys.executable", "/Applications/Jobby/jobby")
    monkeypatch.setattr("jobby.scheduler.shutil.which", lambda _name: "/usr/bin/jobby")

    assert _agent_command() == ["/Applications/Jobby/jobby", "agent", "run"]


def test_scheduler_writes_safe_launchd_file_without_running_real_commands(
    monkeypatch,
    paths: JobbyPaths,
    tmp_path,
) -> None:
    runner = RecordingRunner()
    executable = "/Applications/Jobby App/jobby"
    monkeypatch.setattr(
        "jobby.scheduler._agent_command", lambda: [executable, "agent", "run"]
    )
    scheduler = Scheduler(
        AppConfig(schedule_hour=7, timezone="America/Los_Angeles"),
        paths=paths,
        home=tmp_path / "home",
        runner=runner,
        platform="darwin",
    )

    installed = scheduler.install()
    payload = plistlib.loads(Path(installed.path).read_bytes())

    assert installed.installed and installed.enabled
    assert payload["Label"] == LABEL
    assert payload["ProgramArguments"] == [executable, "agent", "run"]
    assert payload["StartCalendarInterval"] == {"Hour": 7, "Minute": 0}
    assert payload["EnvironmentVariables"]["TZ"] == "America/Los_Angeles"
    assert payload["EnvironmentVariables"]["JOBBY_DATABASE"] == str(paths.database)
    # A fresh install does not issue pointless bootouts for services that are
    # not loaded; it atomically writes and bootstraps both definitions.
    assert runner.commands == [
        ["launchctl", "bootstrap", f"gui/{os.getuid()}", item.path]
        for item in installed.definitions
    ]
    assert scheduler.status().installed is True
    payload["KeepAlive"] = True
    Path(installed.path).write_bytes(plistlib.dumps(payload, sort_keys=True))
    assert scheduler.status().matches_config is False
    del payload["KeepAlive"]
    payload["EnvironmentVariables"]["JOBBY_DATABASE"] = "/wrong/database.sqlite3"
    Path(installed.path).write_bytes(plistlib.dumps(payload, sort_keys=True))
    assert scheduler.status().matches_config is False
    removed = scheduler.remove()
    assert removed.installed is False
    assert all(not Path(item.path).exists() for item in removed.definitions)


def test_scheduler_writes_quoted_systemd_units_without_real_commands(
    monkeypatch,
    paths: JobbyPaths,
    tmp_path,
) -> None:
    runner = RecordingRunner()
    monkeypatch.setattr(
        "jobby.scheduler._agent_command",
        lambda: ["/opt/Jobby 100%/jobby", "agent", "run"],
    )
    scheduler = Scheduler(
        AppConfig(schedule_hour=6, timezone="America/Los_Angeles"),
        paths=paths,
        home=tmp_path / "home with spaces",
        runner=runner,
        platform="linux",
    )

    installed = scheduler.install()
    service = (Path(installed.path).parent / "jobby-agent.service").read_text(
        encoding="utf-8"
    )
    timer = Path(installed.path).read_text(encoding="utf-8")

    assert installed.installed and installed.enabled
    assert 'ExecStart="/opt/Jobby 100%%/jobby" "agent" "run"' in service
    assert f'Environment="JOBBY_DATA_DIR={paths.data_dir}"' in service
    assert f'Environment="JOBBY_CONFIG_DIR={paths.config_dir}"' in service
    assert f'Environment="JOBBY_DATABASE={paths.database}"' in service
    assert 'Environment="TZ=America/Los_Angeles"' in service
    assert "OnCalendar=*-*-* 06:00:00" in timer
    assert "local 06:00" in timer
    assert runner.commands == [
        ["systemctl", "--user", "daemon-reload"],
        [
            "systemctl",
            "--user",
            "enable",
            "--now",
            "jobby-agent.timer",
            "jobby-agent-inventory.timer",
        ],
    ]
    service_path = Path(installed.path).parent / "jobby-agent.service"
    service_path.write_text(service + "NoNewPrivileges=true\n", encoding="utf-8")
    assert scheduler.status().matches_config is False
    service_path.write_text(
        service.replace(str(paths.database), "/wrong/database.sqlite3"),
        encoding="utf-8",
    )
    assert scheduler.status().matches_config is False


def test_scheduler_refuses_symlinked_definition_directory_components(
    monkeypatch: pytest.MonkeyPatch,
    paths: JobbyPaths,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (home / "Library").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(
        "jobby.scheduler._agent_command", lambda: ["/usr/bin/jobby", "agent", "run"]
    )
    scheduler = Scheduler(
        AppConfig(),
        paths=paths,
        home=home,
        runner=RecordingRunner(),
        platform="darwin",
    )

    with pytest.raises(ValueError, match="only real directories"):
        scheduler.install()

    assert tuple(outside.iterdir()) == ()


def test_scheduler_does_not_enable_systemd_timer_after_reload_failure(
    monkeypatch,
    paths: JobbyPaths,
    tmp_path,
) -> None:
    runner = RecordingRunner([1])
    monkeypatch.setattr(
        "jobby.scheduler._agent_command", lambda: ["/usr/bin/jobby", "agent", "run"]
    )
    scheduler = Scheduler(
        AppConfig(),
        paths=paths,
        home=tmp_path / "home",
        runner=runner,
        platform="linux",
    )
    status = scheduler.install()
    assert status.installed is True
    assert status.enabled is False
    assert runner.commands == [["systemctl", "--user", "daemon-reload"]]


def test_scheduler_retains_units_when_disabling_active_timer_fails(
    paths: JobbyPaths,
    tmp_path,
) -> None:
    directory = tmp_path / "home" / ".config" / "systemd" / "user"
    directory.mkdir(parents=True)
    timer = directory / "jobby-agent.timer"
    service = directory / "jobby-agent.service"
    timer.write_text("timer", encoding="utf-8")
    service.write_text("service", encoding="utf-8")
    runner = RecordingRunner([0, 1])
    scheduler = Scheduler(
        AppConfig(),
        paths=paths,
        home=tmp_path / "home",
        runner=runner,
        platform="linux",
    )
    status = scheduler.remove()
    assert status.installed is True
    assert status.enabled is True
    assert timer.exists() and service.exists()
    assert runner.commands[-1] == [
        "systemctl",
        "--user",
        "disable",
        "--now",
        "jobby-agent.timer",
    ]


def test_scheduler_remove_is_idempotent_without_subprocesses(
    paths: JobbyPaths,
    tmp_path,
) -> None:
    runner = RecordingRunner()
    scheduler = Scheduler(
        AppConfig(),
        paths=paths,
        home=tmp_path / "empty-home",
        runner=runner,
        platform="linux",
    )
    first = scheduler.remove()
    second = scheduler.remove()
    assert first.installed is second.installed is False
    assert runner.commands == []


def test_native_notifications_use_argument_arrays_and_are_best_effort() -> None:
    runner = RecordingRunner()
    provider = NativeNotificationProvider(
        runner,
        platform="darwin",
        which=lambda name: f"/usr/bin/{name}",
    )
    message = 'Interview; do shell "things"'
    assert provider.notify("Jobby", message) is True
    command = runner.commands[0]
    # Invoke the exact executable returned by which; do not re-resolve PATH at
    # subprocess time.
    assert command[0] == "/usr/bin/osascript"
    assert command[-2:] == [message, "Jobby"]
    assert message not in command[2]
    assert (
        NativeNotificationProvider(platform="plan9", which=lambda _name: None).notify(
            "x", "y"
        )
        is False
    )


class MappingSecrets:
    def __init__(self, values: dict[str, str] | None = None):
        self.values = values or {}

    def get(self, name: str):
        return self.values.get(name)


class UnavailableSecrets:
    def status(self, name: str):
        return SimpleNamespace(name=name, state="unavailable")

    def get(self, _name: str):
        raise RuntimeError("no keyring backend")


class HealthyScheduler:
    def __init__(self, *_args, **_kwargs):
        pass

    def status(self):
        return ScheduleStatus("test", True, True, "/tmp/test")


def test_doctor_never_constructs_openai_when_disabled_even_with_key(
    monkeypatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)
    monkeypatch.setattr(
        "jobby.doctor.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail(
            "disabled Doctor must not construct an OpenAI client"
        ),
    )

    checks = run_doctor(
        database,
        AppConfig(openai_enabled=False),
        paths,
        secrets=MappingSecrets({"openai_api_key": "sk-present-but-disabled"}),
        check_network=True,
    )

    openai = next(check for check in checks if check.name == "openai")
    assert openai.status == "pass"
    assert "not probed" in openai.message


def test_doctor_fails_inconsistent_scheduled_web_without_probing_openai(
    monkeypatch: pytest.MonkeyPatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)
    monkeypatch.setattr(
        "jobby.doctor.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail(
            "disabled Doctor must not construct an OpenAI client"
        ),
    )

    checks = run_doctor(
        database,
        AppConfig(openai_enabled=False, scheduled_web_enabled=True),
        paths,
        secrets=MappingSecrets({"openai_api_key": "sk-present-but-disabled"}),
        check_network=True,
    )

    openai = next(check for check in checks if check.name == "openai")
    assert openai.status == "fail"
    assert "OpenAI is disabled" in openai.message


def test_doctor_no_network_checks_token_presence_without_google_api_calls(
    monkeypatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    monkeypatch.setattr(
        "jobby.doctor._browser_check", lambda: DoctorCheck("browser", "warn", "mocked")
    )
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)
    monkeypatch.setattr(
        "jobby.google_integration.GoogleOAuthManager.credentials",
        lambda *_args, **_kwargs: pytest.fail("OAuth validation must be skipped"),
    )

    checks = run_doctor(
        database,
        AppConfig(google_enabled=True),
        paths,
        secrets=MappingSecrets({"google_oauth_token": "{}"}),
        check_network=False,
    )

    google = next(check for check in checks if check.name == "google")
    assert google.status == "warn"
    assert "validation skipped" in google.message
    assert doctor_exit_code(checks) == 0


def test_doctor_keeps_unconfigured_features_optional_without_keyring_backend(
    monkeypatch, database: Database, paths: JobbyPaths
) -> None:
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)

    optional = run_doctor(
        database,
        AppConfig(scheduled_web_enabled=False, google_enabled=False),
        paths,
        secrets=UnavailableSecrets(),
        check_network=False,
    )
    assert (
        next(check for check in optional if check.name == "openai_credentials").status
        == "warn"
    )
    assert (
        next(check for check in optional if check.name == "usajobs_credentials").status
        == "warn"
    )
    assert doctor_exit_code(optional) == 0

    required = run_doctor(
        database,
        AppConfig(scheduled_web_enabled=True, google_enabled=False),
        paths,
        secrets=UnavailableSecrets(),
        check_network=False,
    )
    assert (
        next(check for check in required if check.name == "openai_credentials").status
        == "fail"
    )
    assert doctor_exit_code(required) == 1


def test_doctor_google_network_probe_is_minimal_and_read_only(
    monkeypatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class Execute:
        def __init__(self, name: str, kwargs: dict[str, object]):
            self.name = name
            self.kwargs = kwargs

        def execute(self):
            calls.append((self.name, self.kwargs))
            return {}

    class Users:
        def getProfile(self, **kwargs):
            return Execute("gmail.getProfile", kwargs)

    class GmailService:
        def users(self):
            return Users()

    class Events:
        def list(self, **kwargs):
            return Execute("calendar.events.list", kwargs)

    class CalendarService:
        def events(self):
            return Events()

    class OAuth:
        def __init__(self, _secrets):
            pass

        def credentials(self, scopes, *, interactive):
            calls.append(
                (
                    "oauth.credentials",
                    {"scopes": tuple(scopes), "interactive": interactive},
                )
            )
            return object()

    class Gmail:
        def __init__(self, _oauth):
            self.service = GmailService()

    class Calendar:
        def __init__(self, _oauth, *, allow_write):
            assert allow_write is False
            self.service = CalendarService()

    monkeypatch.setattr(
        "jobby.doctor._browser_check", lambda: DoctorCheck("browser", "warn", "mocked")
    )
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)
    monkeypatch.setattr("jobby.google_integration.GoogleOAuthManager", OAuth)
    monkeypatch.setattr("jobby.google_integration.GmailProvider", Gmail)
    monkeypatch.setattr("jobby.google_integration.CalendarProvider", Calendar)

    checks = run_doctor(
        database,
        AppConfig(google_enabled=True),
        paths,
        secrets=MappingSecrets({"google_oauth_token": "token"}),
        check_network=True,
    )

    google = next(check for check in checks if check.name == "google")
    assert google.status == "pass"
    assert [name for name, _ in calls] == [
        "oauth.credentials",
        "gmail.getProfile",
        "calendar.events.list",
    ]
    assert calls[0][1]["interactive"] is False
    assert calls[1][1] == {"userId": "me"}
    assert calls[2][1]["calendarId"] == "primary"
    assert calls[2][1]["maxResults"] == 1
    assert calls[2][1]["singleEvents"] is True


def test_doctor_enabled_google_without_token_is_a_failure(
    monkeypatch,
    database: Database,
    paths: JobbyPaths,
) -> None:
    monkeypatch.setattr(
        "jobby.doctor._browser_check", lambda: DoctorCheck("browser", "warn", "mocked")
    )
    monkeypatch.setattr("jobby.doctor.Scheduler", HealthyScheduler)
    checks = run_doctor(
        database,
        AppConfig(google_enabled=True),
        paths,
        secrets=MappingSecrets(),
        check_network=False,
    )
    assert next(check for check in checks if check.name == "google").status == "fail"
    assert doctor_exit_code(checks) == 1


class SuccessfulAgentScanner:
    def __init__(self, database: Database, **_kwargs):
        self.database = database

    def scan(self, _sources, **_kwargs):
        now = datetime.now(timezone.utc)
        with self.database.session() as session:
            run = ScanRun(
                status=AgentRunStatus.SUCCEEDED,
                requested_sources=[],
                source_results={},
                started_at=now,
                finished_at=now,
            )
            session.add(run)
            session.flush()
            return run


def test_agent_explicit_web_request_still_makes_zero_openai_calls_when_disabled(
    monkeypatch,
    database: Database,
) -> None:
    monkeypatch.setattr("jobby.agent.Scanner", SuccessfulAgentScanner)
    monkeypatch.setattr(
        "jobby.agent.build_configured_sources", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(
        "jobby.agent.OpenAIProvider",
        lambda *_args, **_kwargs: pytest.fail(
            "disabled agent must not construct an OpenAI client"
        ),
    )

    run = DailyAgent(
        database,
        AppConfig(openai_enabled=False, notifications_enabled=False),
        secrets=MappingSecrets({"openai_api_key": "sk-present-but-disabled"}),
    ).run(include_web=True)

    assert run.status is AgentRunStatus.SUCCEEDED
    assert run.summary["web_skip_reason"] == "OpenAI is disabled in configuration"
    assert run.summary["web_scan_run_id"] is None


def test_agent_records_unexpected_failure_instead_of_leaving_running_row(
    monkeypatch,
    database: Database,
) -> None:
    class FailingScanner:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("scanner initialization failed")

    monkeypatch.setattr("jobby.agent.Scanner", FailingScanner)
    run = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    ).run(include_web=False)

    assert run.status is AgentRunStatus.FAILED
    assert run.finished_at is not None
    assert "scanner initialization failed" in (run.error or "")
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Alert)) == 1
        persisted = session.get(AgentRun, run.id)
        assert persisted is not None and persisted.status is AgentRunStatus.FAILED


def test_healthy_agent_run_resolves_recurring_failure_alerts_with_entity_links(
    monkeypatch,
    database: Database,
) -> None:
    class FailingScanner:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("scanner initialization failed")

    class PartialAgentScanner:
        def __init__(self, database: Database, **_kwargs):
            self.database = database

        def scan(self, *_args, **_kwargs):
            now = datetime.now(timezone.utc)
            with self.database.session() as session:
                run = ScanRun(
                    status=AgentRunStatus.PARTIAL,
                    requested_sources=[],
                    source_results={},
                    started_at=now,
                    finished_at=now,
                    error_summary="one source timed out",
                )
                session.add(run)
                session.flush()
                return run

    monkeypatch.setattr(
        "jobby.agent.build_configured_sources", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr("jobby.agent.Scanner", FailingScanner)
    failed = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    ).run(include_web=False)

    monkeypatch.setattr("jobby.agent.Scanner", PartialAgentScanner)
    partial = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    ).run(include_web=False)

    monkeypatch.setattr("jobby.agent.Scanner", SuccessfulAgentScanner)
    healthy = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    ).run(include_web=False)

    assert failed.status is AgentRunStatus.FAILED
    assert partial.status is AgentRunStatus.PARTIAL
    assert healthy.status is AgentRunStatus.SUCCEEDED
    with database.session() as session:
        alerts = {alert.title: alert for alert in session.scalars(select(Alert))}
        assert set(alerts) == {
            "Jobby agent run failed",
            "Jobby scan needs attention",
        }
        assert all(alert.resolved_at is not None for alert in alerts.values())
        assert alerts["Jobby agent run failed"].entity_type == "agent_run"
        assert alerts["Jobby agent run failed"].entity_id == failed.id
        assert alerts["Jobby scan needs attention"].entity_type == "scan_run"
        assert alerts["Jobby scan needs attention"].entity_id == partial.scan_run_id


def test_agent_cancellation_is_audited_and_re_raised(
    monkeypatch,
    database: Database,
) -> None:
    class CancelledScanner:
        def __init__(self, *_args, **_kwargs):
            pass

        def scan(self, *_args, **_kwargs):
            raise KeyboardInterrupt

    monkeypatch.setattr("jobby.agent.Scanner", CancelledScanner)
    monkeypatch.setattr(
        "jobby.agent.build_configured_sources", lambda *_args, **_kwargs: []
    )
    with pytest.raises(KeyboardInterrupt):
        DailyAgent(
            database,
            AppConfig(notifications_enabled=False),
            secrets=MappingSecrets(),
        ).run(include_web=False)

    with database.session() as session:
        run = session.scalar(select(AgentRun))
        assert run is not None
        assert run.status is AgentRunStatus.FAILED
        assert run.summary["cancelled"] is True
        assert run.finished_at is not None


def test_agent_commits_local_state_before_native_notification(
    monkeypatch,
    database: Database,
) -> None:
    job_id = seed_job(database, title="Deadline Role")
    with database.session() as session:
        job = session.get(Job, job_id)
        assert job is not None
        job.deadline = (datetime.now(timezone.utc) + timedelta(days=1)).date()

    notifications: list[tuple[str, bool]] = []

    class CommitCheckingNotifier:
        def notify(self, title, _message, *, urgent=False):
            with database.session() as session:
                run = session.scalar(
                    select(AgentRun).order_by(AgentRun.created_at.desc())
                )
                assert run is not None and run.status is AgentRunStatus.SUCCEEDED
                assert run.finished_at is not None
                assert session.scalar(select(func.count()).select_from(Task)) == 1
            notifications.append((title, urgent))
            return True

    monkeypatch.setattr("jobby.agent.Scanner", SuccessfulAgentScanner)
    monkeypatch.setattr(
        "jobby.agent.build_configured_sources", lambda *_args, **_kwargs: []
    )
    run = DailyAgent(
        database,
        AppConfig(notifications_enabled=True),
        secrets=MappingSecrets(),
        notifier=CommitCheckingNotifier(),
    ).run(include_web=False)
    assert run.status is AgentRunStatus.SUCCEEDED
    assert ("Jobby deadlines", True) in notifications


def test_dismissed_deadline_task_stays_terminal_and_stops_alerting(
    database: Database,
) -> None:
    job_id = seed_job(database, title="One Deadline")
    now = datetime.now(timezone.utc)
    with database.session() as session:
        job = session.get(Job, job_id)
        assert job is not None
        job.deadline = now.astimezone(ZoneInfo("America/Los_Angeles")).date()
    agent = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    )
    with database.session() as session:
        assert agent._deadline_tasks(session, now) == 1
    with database.session() as session:
        task = session.scalar(select(Task))
        assert task is not None
        assert task.due_at is not None
        task.status = TaskStatus.DISMISSED
    with database.session() as session:
        assert agent._deadline_tasks(session, now) == 0
    with database.session() as session:
        task = session.scalar(select(Task))
        assert task is not None
        assert task.due_at.astimezone(ZoneInfo("America/Los_Angeles")).time().hour == 23
        assert (
            task.due_at.astimezone(ZoneInfo("America/Los_Angeles")).date()
            == job.deadline
        )


@pytest.mark.parametrize(
    "status", [JobStatus.IGNORED, JobStatus.STALE, JobStatus.CLOSED]
)
def test_inactive_job_dismisses_pending_deadline_task(
    database: Database, status: JobStatus
) -> None:
    job_id = seed_job(database, title=f"{status.value} Deadline")
    now = datetime.now(timezone.utc)
    agent = DailyAgent(
        database,
        AppConfig(notifications_enabled=False),
        secrets=MappingSecrets(),
    )
    with database.session() as session:
        job = session.get(Job, job_id)
        assert job is not None
        job.deadline = now.astimezone(ZoneInfo("America/Los_Angeles")).date()
        assert agent._deadline_tasks(session, now) == 1
    with database.session() as session:
        job = session.get(Job, job_id)
        assert job is not None
        job.status = status
        assert agent._deadline_tasks(session, now) == 0
    with database.session() as session:
        task = session.scalar(select(Task).where(Task.job_id == job_id))
        assert task is not None
        assert task.status is TaskStatus.DISMISSED
        assert task.completed_at == now


def test_high_role_count_uses_creation_and_current_scan_observation(
    database: Database,
) -> None:
    now = datetime.now(timezone.utc)
    with database.session() as session:
        company = Company(name="Fresh Co", normalized_name="fresh co")
        first_run = ScanRun(
            status=AgentRunStatus.SUCCEEDED,
            requested_sources=["test"],
            source_results={},
            started_at=now,
            finished_at=now + timedelta(seconds=2),
        )
        session.add_all([company, first_run])
        session.flush()
        existing = Job(
            company_id=company.id,
            title="Existing High Role",
            normalized_title="existing high role",
            source_primary="test",
            source_id="existing",
            latest_score=4.8,
            discovered_at=now - timedelta(days=1),
        )
        fresh = Job(
            company_id=company.id,
            title="Fresh High Role",
            normalized_title="fresh high role",
            source_primary="test",
            source_id="fresh",
            latest_score=4.2,
            discovered_at=now + timedelta(seconds=1),
        )
        session.add_all([existing, fresh])
        session.flush()
        session.add_all(
            [
                SourceObservation(
                    job_id=job.id,
                    scan_run_id=first_run.id,
                    source="test",
                    source_job_id=job.source_id,
                    observed_at=now + timedelta(seconds=1),
                )
                for job in (existing, fresh)
            ]
        )
        session.flush()
        assert DailyAgent._new_high_role_count(session, first_run) == 1

        second_run = ScanRun(
            status=AgentRunStatus.SUCCEEDED,
            requested_sources=["test"],
            source_results={},
            started_at=now + timedelta(minutes=1),
            finished_at=now + timedelta(minutes=1, seconds=2),
        )
        session.add(second_run)
        session.flush()
        session.add(
            SourceObservation(
                job_id=fresh.id,
                scan_run_id=second_run.id,
                source="test",
                source_job_id=fresh.source_id,
                observed_at=now + timedelta(minutes=1),
            )
        )
        session.flush()
        assert DailyAgent._new_high_role_count(session, second_run) == 0


@pytest.mark.parametrize("total", range(0, 41))
def test_allocation_targets_are_integral_and_sum_to_weekly_target(total: int) -> None:
    targets = allocation_targets(total)
    assert set(targets) == set(ALLOCATION)
    assert all(isinstance(value, int) and value >= 0 for value in targets.values())
    assert sum(targets.values()) == total


def test_default_allocation_keeps_the_five_percent_strategic_bucket() -> None:
    assert allocation_targets(10) == {
        "legal_ai": 6,
        "ai_ip_policy": 2,
        "federal": 1,
        "lottery_ticket": 1,
    }


def test_daily_plan_handles_sqlite_datetimes_and_explains_allocation(
    database: Database,
) -> None:
    now = datetime(2026, 7, 8, 16, 0, tzinfo=timezone.utc)
    with database.session() as session:
        company = Company(name="Planner Co", normalized_name="planner co")
        session.add(company)
        session.flush()
        legal = Job(
            company_id=company.id,
            title="Legal AI Analyst",
            normalized_title="legal ai analyst",
            description="Legal technology workflows",
            latest_score=4.5,
            deadline=(now + timedelta(days=3)).date(),
        )
        federal = Job(
            company_id=company.id,
            title="Federal Policy Analyst",
            normalized_title="federal policy analyst",
            description="FERC federal government policy",
            latest_score=4.0,
        )
        session.add_all([legal, federal])
        session.flush()
        application = Application(
            job_id=federal.id,
            current_stage=ApplicationStage.APPLIED,
            submitted_at=now - timedelta(days=1),
        )
        session.add(application)
        session.flush()
        session.add(
            Task(
                title="Follow up",
                status=TaskStatus.PENDING,
                due_at=(now - timedelta(hours=1)).replace(tzinfo=None),
                job_id=legal.id,
            )
        )
        session.add(
            Interview(
                application_id=application.id,
                starts_at=(now + timedelta(days=1)).replace(tzinfo=None),
                interview_type="screen",
            )
        )

    with database.session() as session:
        plan = build_daily_plan(session, weekly_target=10, now=now.replace(tzinfo=None))

    assert plan.allocation_targets == allocation_targets(10)
    assert plan.allocation_current["federal"] == 1
    assert plan.items[0].kind == "task"
    assert plan.items[0].reason == "Overdue"
    assert all(
        item.due_at is None or item.due_at.tzinfo is not None for item in plan.items
    )
    application_item = next(item for item in plan.items if item.kind == "application")
    assert "allocation deficit" in application_item.reason
    assert classify_job(legal) == "legal_ai"
