from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from jobby.config import (
    MAX_CONFIG_BYTES,
    AppConfig,
    JobbyPaths,
    SecretStore,
    WorkdayBoard,
    load_config,
    resolve_paths,
    save_config,
)


def make_paths(tmp_path: Path) -> JobbyPaths:
    root = tmp_path / "jobby"
    return JobbyPaths(
        data_dir=root / "data",
        config_dir=root / "config",
        cache_dir=root / "cache",
        database=root / "data" / "jobby.sqlite3",
        artifacts_dir=root / "data" / "artifacts",
        backups_dir=root / "data" / "backups",
        logs_dir=root / "data" / "logs",
        config_file=root / "config" / "config.toml",
    )


def test_config_round_trip_is_atomic_private_and_rejects_unknown_keys(
    tmp_path: Path,
) -> None:
    paths = make_paths(tmp_path)
    config = AppConfig(schedule_hour=6, notifications_enabled=False)

    saved = save_config(config, paths)

    assert load_config(paths) == config
    assert list(paths.config_dir.glob(".config-*.tmp")) == []
    if os.name == "posix":
        assert saved.stat().st_mode & 0o777 == 0o600
        for directory in (
            paths.data_dir,
            paths.config_dir,
            paths.cache_dir,
            paths.artifacts_dir,
            paths.backups_dir,
            paths.logs_dir,
        ):
            assert directory.stat().st_mode & 0o777 == 0o700

    saved.write_text("unknown_setting = true\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="unknown_setting"):
        load_config(paths)


def test_explicit_empty_environment_does_not_fall_back_to_process_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    process_home = tmp_path / "must-not-be-used"
    monkeypatch.setenv("JOBBY_HOME", str(process_home))

    paths = resolve_paths({})

    assert paths.data_dir != process_home / "data"


def test_config_loader_rejects_symlink_without_reading_target(tmp_path: Path) -> None:
    paths = make_paths(tmp_path).ensure()
    target = tmp_path / "outside-config.toml"
    target.write_text('timezone = "UTC"\n', encoding="utf-8")
    paths.config_file.symlink_to(target)

    with pytest.raises(ValueError, match="must not be a symbolic link"):
        load_config(paths)

    assert target.read_text(encoding="utf-8") == 'timezone = "UTC"\n'


@pytest.mark.parametrize("symlink_at_root", [False, True])
def test_path_setup_rejects_symlink_components_without_chmodding_target(
    tmp_path: Path, symlink_at_root: bool
) -> None:
    paths = make_paths(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    external.chmod(0o755)
    root = tmp_path / "jobby"
    if symlink_at_root:
        root.symlink_to(external, target_is_directory=True)
    else:
        root.mkdir()
        paths.data_dir.symlink_to(external, target_is_directory=True)

    with pytest.raises(ValueError, match="only real directories"):
        paths.ensure()

    assert external.stat().st_mode & 0o777 == 0o755


def test_config_omits_optional_none_values_and_round_trips_paths(
    tmp_path: Path,
) -> None:
    paths = make_paths(tmp_path)
    config = AppConfig()
    config.sources.workday["unnamed"] = WorkdayBoard(
        tenant="example", site="External", name=None
    )

    saved = save_config(config, paths)
    text = saved.read_text(encoding="utf-8")

    assert "external_backup_destination" not in text
    unnamed_table = text.split("[sources.workday.unnamed]", maxsplit=1)[1]
    assert "name =" not in unnamed_table.split("[", maxsplit=1)[0]
    assert load_config(paths) == config

    destination = tmp_path / "external backups"
    configured = config.model_copy(update={"external_backup_destination": destination})
    save_config(configured, paths)

    assert load_config(paths).external_backup_destination == destination


def test_config_and_secret_size_limits_fail_closed(monkeypatch, tmp_path: Path) -> None:
    paths = make_paths(tmp_path).ensure()
    paths.config_file.write_bytes(b"x" * (MAX_CONFIG_BYTES + 1))
    with pytest.raises(ValueError, match="safety limit"):
        load_config(paths)

    keyring = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "keyring", keyring)
    store = SecretStore(service="test")
    with pytest.raises(ValueError, match="must not be blank"):
        store.set("openai_api_key", "   ")
    with pytest.raises(ValueError, match="safety limit"):
        store.set("openai_api_key", "x" * 1_000_001)
    keyring.set_password.assert_not_called()


def test_schedule_hour_and_salary_floor_reject_boolean_coercion() -> None:
    with pytest.raises(ValidationError):
        AppConfig(schedule_hour=True)
    with pytest.raises(ValidationError):
        AppConfig(salary_floor=False)


def test_external_backup_scrypt_cost_is_strict_power_of_two_and_bounded() -> None:
    assert AppConfig(external_backup_scrypt_n=16_384).external_backup_scrypt_n == 16_384
    assert AppConfig(external_backup_scrypt_n=65_536).external_backup_scrypt_n == 65_536
    for invalid in (True, 16_385, 131_072):
        with pytest.raises(ValidationError):
            AppConfig(external_backup_scrypt_n=invalid)


def test_old_workday_toml_shape_loads_into_typed_board(tmp_path: Path) -> None:
    paths = make_paths(tmp_path).ensure()
    paths.config_file.write_text(
        """
[sources.workday.legacy_board]
tenant = "legacy"
site = "External_Careers"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(paths)

    board = config.sources.workday["legacy_board"]
    assert isinstance(board, WorkdayBoard)
    assert board.tenant == "legacy"
    assert board.site == "External_Careers"
    assert board.wd == "wd1"
    assert board.name is None


def test_source_types_and_assignment_validation_fail_closed() -> None:
    config = AppConfig()

    with pytest.raises(ValidationError):
        config.ai_cache_ttl_days = 0
    with pytest.raises(ValidationError):
        config.discovery_max_workers = 9
    with pytest.raises(ValidationError):
        config.models.fast = "  "
    with pytest.raises(ValidationError):
        config.sources.workday["nvidia"].site = "bad/site"
    with pytest.raises(ValidationError):
        config.sources.workday = {
            "bad key": {
                "tenant": "valid",
                "site": "External",
            }
        }
    with pytest.raises(ValidationError):
        config.sources.workday = {
            "valid": {
                "tenant": "valid",
                "site": "External",
                "unknown": "rejected",
            }
        }
