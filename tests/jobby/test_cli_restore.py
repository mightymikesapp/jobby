"""End-user restore command safety and recovery tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select

from jobby import cli
from jobby.backup import create_backup, verify_backup
from jobby.config import AppConfig, JobbyPaths, load_config, save_config
from jobby.db import Database
from jobby.models import Company, Job
from jobby.restore import restore_backup
import jobby.restore as restore_module


def _paths(root: Path, *, ensure: bool = True) -> JobbyPaths:
    data = root / "data"
    config = root / "config"
    paths = JobbyPaths(
        data_dir=data,
        config_dir=config,
        cache_dir=root / "cache",
        database=data / "jobby.sqlite3",
        artifacts_dir=data / "artifacts",
        backups_dir=data / "backups",
        logs_dir=data / "logs",
        config_file=config / "config.toml",
    )
    return paths.ensure() if ensure else paths


def _database(paths: JobbyPaths, company_name: str) -> Database:
    database = Database(paths=paths)
    database.initialize()
    with database.session() as session:
        company = Company(
            name=company_name,
            normalized_name=company_name.casefold(),
        )
        session.add(company)
        session.flush()
        session.add(
            Job(
                company_id=company.id,
                title="Counsel",
                normalized_title="counsel",
            )
        )
    return database


def _archive(tmp_path: Path, *, company_name: str = "Source Co") -> Path:
    paths = _paths(tmp_path / "source")
    database = _database(paths, company_name)
    save_config(AppConfig(salary_floor=123_000), paths)
    archive = tmp_path / "source.zip"
    try:
        create_backup(database, output=archive, paths=paths)
    finally:
        database.dispose()
    assert verify_backup(archive) == (True, "ok")
    return archive


def _company_names(paths: JobbyPaths) -> list[str]:
    database = Database(paths=paths)
    database.initialize()
    try:
        with database.session() as session:
            return list(session.scalars(select(Company.name).order_by(Company.name)))
    finally:
        database.dispose()


def test_cli_restore_defaults_to_non_mutating_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", str(archive)]) == 0

    output = capsys.readouterr().out
    payload = json.loads(output.split("\nDry-run only.", 1)[0])
    assert payload["applied"] is False
    assert payload["plan"]["config_action"] == "preserve"
    assert not target.data_dir.exists()
    assert not target.config_dir.exists()


def test_cli_restore_applies_new_home_and_config_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive = _archive(tmp_path)
    target = _paths(tmp_path / "target", ensure=False)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert (
        cli.main(
            [
                "restore",
                str(archive),
                "--apply",
                "--config",
                "restore-if-missing",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True
    assert payload["audit_recorded"] is True
    assert payload["plan"]["config_action"] == "restore"
    assert _company_names(target) == ["Source Co"]
    assert load_config(target).salary_floor == 123_000


def test_cli_restore_requires_replace_and_honors_config_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, "Original Co")
    current.dispose()
    save_config(AppConfig(salary_floor=77_000), target)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", str(archive), "--apply"]) == 1
    assert "--replace" in capsys.readouterr().err
    assert _company_names(target) == ["Original Co"]
    assert load_config(target).salary_floor == 77_000

    assert (
        cli.main(
            [
                "restore",
                str(archive),
                "--apply",
                "--replace",
                "--config",
                "replace",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert _company_names(target) == ["Replacement Co"]
    assert load_config(target).salary_floor == 123_000
    pre_restore = Path(payload["pre_restore_backup"])
    assert verify_backup(pre_restore) == (True, "ok")


def test_cli_restore_reports_storage_lock_without_changing_current_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive = _archive(tmp_path)
    target = _paths(tmp_path / "target")
    current = _database(target, "Busy Co")
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)
    try:
        assert cli.main(["restore", str(archive), "--apply", "--replace"]) == 1
        assert "close other Jobby processes" in capsys.readouterr().err
    finally:
        current.dispose()
    assert _company_names(target) == ["Busy Co"]


def test_cli_restore_recover_needs_no_archive_or_existing_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _paths(tmp_path / "target", ensure=False)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", "--recover"]) == 0

    output = capsys.readouterr().out
    assert json.loads(output.split("\nNo interrupted", 1)[0]) == {"recovered": False}
    assert not target.data_dir.exists()


def test_cli_restore_recovers_committed_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive = _archive(tmp_path, company_name="Replacement Co")
    target = _paths(tmp_path / "target")
    current = _database(target, "Original Co")
    current.dispose()
    real_remove = restore_module._remove_path

    def crash_after_commit(path: Path | None) -> None:
        if path is not None and ".rollback-" in path.name:
            raise KeyboardInterrupt("simulated process loss after commit")
        real_remove(path)

    monkeypatch.setattr(restore_module, "_remove_path", crash_after_commit)
    with pytest.raises(KeyboardInterrupt, match="simulated process loss"):
        restore_backup(
            archive,
            paths=target,
            apply=True,
            replace_current=True,
        )
    monkeypatch.setattr(restore_module, "_remove_path", real_remove)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", "--recover"]) == 0

    assert json.loads(capsys.readouterr().out) == {"recovered": True}
    assert _company_names(target) == ["Replacement Co"]
    assert not (target.data_dir / ".jobby-restore-journal.json").exists()


@pytest.mark.parametrize("value", ["-1", "nan", "inf"])
def test_cli_restore_rejects_unsafe_lock_timeout(value: str) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["restore", "backup.zip", "--lock-timeout", value])
    assert raised.value.code == 2


def test_cli_restore_recover_rejects_apply_options_without_creating_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _paths(tmp_path / "target", ensure=False)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", "--recover", "--apply"]) == 1

    assert "cannot be combined" in capsys.readouterr().err
    assert not target.data_dir.exists()


def test_cli_restore_recover_rejects_even_explicit_preserve_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _paths(tmp_path / "target", ensure=False)
    monkeypatch.setattr(cli, "resolve_paths", lambda: target)

    assert cli.main(["restore", "--recover", "--config", "preserve"]) == 1

    assert "cannot be combined" in capsys.readouterr().err
    assert not target.data_dir.exists()
