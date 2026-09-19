"""Explicitly install and manage focused-daily and inventory-weekly schedules."""

from __future__ import annotations

import os
import plistlib
import shutil
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import AppConfig, JobbyPaths, _ensure_private_directory, resolve_paths


LABEL = "com.jobby.agent"
INVENTORY_LABEL = f"{LABEL}.inventory"
SCHEDULE_MODES = ("focused", "inventory")
LOG_NAMES = ("agent.stdout.log", "agent.stderr.log")
MAX_SCHEDULE_FILE_BYTES = 1_000_000
_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


@dataclass(slots=True)
class ScheduleDefinitionStatus:
    mode: str
    installed: bool
    enabled: bool | None
    path: str
    detail: str = ""
    matches_config: bool | None = None


@dataclass(slots=True)
class ScheduleStatus:
    platform: str
    installed: bool
    enabled: bool | None
    path: str
    detail: str = ""
    matches_config: bool | None = None
    definitions: tuple[ScheduleDefinitionStatus, ...] = ()


class Scheduler:
    def __init__(
        self,
        config: AppConfig,
        *,
        paths: JobbyPaths | None = None,
        home: Path | None = None,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        platform: str | None = None,
    ):
        self.config = config
        self.paths = (paths or resolve_paths()).ensure()
        self.home = (home or Path.home()).expanduser()
        self.runner = runner or subprocess.run
        self.platform = platform or sys.platform

    def install(self) -> ScheduleStatus:
        """Explicitly install and enable both schedule definitions."""

        _validate_agent_command(_agent_command())
        rotate_log_files(
            self.paths,
            max_bytes=self.config.scheduler_log_max_bytes,
            backup_count=self.config.scheduler_log_backup_count,
        )
        if self.platform == "darwin":
            return self._install_launchd()
        if self.platform.startswith("linux"):
            return self._install_systemd()
        raise RuntimeError("scheduled runs are supported on macOS and Linux only")

    def status(self) -> ScheduleStatus:
        if self.platform == "darwin":
            return self._aggregate_status(
                "launchd",
                tuple(self._launchd_definition_status(mode) for mode in SCHEDULE_MODES),
            )
        if self.platform.startswith("linux"):
            return self._aggregate_status(
                "systemd-user",
                tuple(self._systemd_definition_status(mode) for mode in SCHEDULE_MODES),
            )
        return ScheduleStatus(self.platform, False, None, "", "unsupported platform")

    def remove(self) -> ScheduleStatus:
        if self.platform == "darwin":
            return self._remove_launchd()
        if self.platform.startswith("linux"):
            return self._remove_systemd()
        raise RuntimeError("scheduled runs are supported on macOS and Linux only")

    def rebind(self) -> ScheduleStatus:
        """Explicitly revalidate installed definitions after a release switch.

        An absent scheduler stays absent, preserving the opt-in boundary.  If
        definitions were previously installed, the ordinary install path
        atomically rewrites them with the current stable executable and cadence.
        """

        current = self.status()
        if not current.installed:
            return current
        return self.install()

    def _install_launchd(self) -> ScheduleStatus:
        current = self.status()
        current_by_mode = {item.mode: item for item in current.definitions}
        command = _agent_command()
        definitions: list[ScheduleDefinitionStatus] = []
        for mode in SCHEDULE_MODES:
            path = self._launchd_path(mode)
            _ensure_private_directory(path.parent)
            _atomic_write(
                path,
                plistlib.dumps(self._launchd_payload(mode, command), sort_keys=True),
            )
            previous = current_by_mode.get(mode)
            if previous and previous.enabled:
                unload = self._run(
                    ["launchctl", "bootout", f"gui/{os.getuid()}", str(path)]
                )
                if unload.returncode != 0:
                    definitions.append(
                        ScheduleDefinitionStatus(
                            mode,
                            True,
                            True,
                            str(path),
                            _join_detail(
                                _command_detail(unload),
                                "existing launch agent could not be unloaded; new definition was not activated",
                            ),
                            False,
                        )
                    )
                    continue
            result = self._run(
                ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(path)]
            )
            definitions.append(
                ScheduleDefinitionStatus(
                    mode,
                    True,
                    result.returncode == 0,
                    str(path),
                    _command_detail(result),
                    True,
                )
            )
        return self._aggregate_status("launchd", tuple(definitions))

    def _install_systemd(self) -> ScheduleStatus:
        directory = self._systemd_dir()
        _ensure_private_directory(directory)
        command = " ".join(_systemd_quote(part) for part in _agent_command())
        definitions: list[ScheduleDefinitionStatus] = []
        for mode in SCHEDULE_MODES:
            service = directory / self._systemd_service_name(mode)
            timer = directory / self._systemd_timer_name(mode)
            _atomic_write(
                service,
                self._systemd_service_content(mode, command).encode("utf-8"),
            )
            _atomic_write(
                timer,
                self._systemd_timer_content(mode).encode("utf-8"),
            )
            definitions.append(
                ScheduleDefinitionStatus(
                    mode, True, False, str(timer), matches_config=True
                )
            )
        reload_result = self._run(["systemctl", "--user", "daemon-reload"])
        if reload_result.returncode != 0:
            definitions = [
                ScheduleDefinitionStatus(
                    item.mode,
                    True,
                    False,
                    item.path,
                    _command_detail(reload_result),
                    True,
                )
                for item in definitions
            ]
            return self._aggregate_status("systemd-user", tuple(definitions))
        result = self._run(
            [
                "systemctl",
                "--user",
                "enable",
                "--now",
                *(self._systemd_timer_name(mode) for mode in SCHEDULE_MODES),
            ]
        )
        definitions = [
            ScheduleDefinitionStatus(
                item.mode,
                True,
                result.returncode == 0,
                item.path,
                _command_detail(result),
                True,
            )
            for item in definitions
        ]
        return self._aggregate_status("systemd-user", tuple(definitions))

    def _remove_launchd(self) -> ScheduleStatus:
        current = self.status()
        definitions: list[ScheduleDefinitionStatus] = []
        for item in current.definitions:
            path = Path(item.path)
            if item.enabled:
                result = self._run(
                    ["launchctl", "bootout", f"gui/{os.getuid()}", str(path)]
                )
                if result.returncode != 0:
                    definitions.append(
                        ScheduleDefinitionStatus(
                            item.mode,
                            path.exists(),
                            True,
                            str(path),
                            _command_detail(result),
                            item.matches_config,
                        )
                    )
                    continue
            path.unlink(missing_ok=True)
            definitions.append(
                ScheduleDefinitionStatus(item.mode, False, False, str(path))
            )
        return self._aggregate_status("launchd", tuple(definitions))

    def _remove_systemd(self) -> ScheduleStatus:
        directory = self._systemd_dir()
        current = self.status()
        enabled_timers = [
            self._systemd_timer_name(item.mode)
            for item in current.definitions
            if item.enabled
        ]
        if enabled_timers:
            result = self._run(
                ["systemctl", "--user", "disable", "--now", *enabled_timers]
            )
            if result.returncode != 0:
                failed = tuple(
                    ScheduleDefinitionStatus(
                        item.mode,
                        item.installed,
                        item.enabled,
                        item.path,
                        _join_detail(item.detail, _command_detail(result)),
                        item.matches_config,
                    )
                    for item in current.definitions
                )
                return self._aggregate_status("systemd-user", failed)
        names = tuple(
            name
            for mode in SCHEDULE_MODES
            for name in (
                self._systemd_timer_name(mode),
                self._systemd_service_name(mode),
            )
        )
        had_files = any((directory / name).exists() for name in names)
        for name in names:
            (directory / name).unlink(missing_ok=True)
        detail = ""
        if had_files:
            result = self._run(["systemctl", "--user", "daemon-reload"])
            detail = _command_detail(result) if result.returncode else ""
        definitions = tuple(
            ScheduleDefinitionStatus(
                mode,
                False,
                False,
                str(directory / self._systemd_timer_name(mode)),
                detail,
            )
            for mode in SCHEDULE_MODES
        )
        return self._aggregate_status("systemd-user", definitions)

    def _run(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return self.runner(
                command, check=False, capture_output=True, text=True, timeout=20
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return subprocess.CompletedProcess(command, 1, "", str(exc))

    def _launchd_path(self, mode: str = "focused") -> Path:
        return (
            self.home
            / "Library"
            / "LaunchAgents"
            / f"{self._launchd_label(mode)}.plist"
        )

    def _systemd_dir(self) -> Path:
        return self.home / ".config" / "systemd" / "user"

    def _launchd_definition_status(self, mode: str) -> ScheduleDefinitionStatus:
        path = self._launchd_path(mode)
        present = path.exists() or path.is_symlink()
        enabled: bool | None = None
        detail = ""
        if present:
            result = self._run(
                [
                    "launchctl",
                    "print",
                    f"gui/{os.getuid()}/{self._launchd_label(mode)}",
                ]
            )
            enabled = result.returncode == 0
            detail = (result.stderr or result.stdout).strip()[:500]
        matches, drift = self._launchd_matches(path, mode)
        return ScheduleDefinitionStatus(
            mode,
            present,
            enabled,
            str(path),
            _join_detail(detail, drift),
            matches,
        )

    def _systemd_definition_status(self, mode: str) -> ScheduleDefinitionStatus:
        timer = self._systemd_dir() / self._systemd_timer_name(mode)
        present = timer.exists() or timer.is_symlink()
        enabled: bool | None = None
        detail = ""
        if present:
            result = self._run(
                ["systemctl", "--user", "is-enabled", self._systemd_timer_name(mode)]
            )
            enabled = result.returncode == 0
            detail = (result.stdout or result.stderr).strip()[:500]
        matches, drift = self._systemd_matches(timer, mode)
        return ScheduleDefinitionStatus(
            mode,
            present,
            enabled,
            str(timer),
            _join_detail(detail, drift),
            matches,
        )

    def _launchd_matches(
        self, path: Path, mode: str = "focused"
    ) -> tuple[bool | None, str]:
        if not path.exists() and not path.is_symlink():
            return None, ""
        try:
            payload = plistlib.loads(_read_schedule_file(path))
            expected_command = _agent_command()
            matches = payload == self._launchd_payload(mode, expected_command)
            command_detail = _agent_command_detail(expected_command)
            if command_detail:
                matches = False
            return matches, (
                _join_detail(
                    "schedule definition matches current configuration"
                    if matches
                    else "schedule definition has drifted from current configuration",
                    command_detail,
                )
            )
        except Exception as exc:
            return False, f"schedule definition could not be validated: {exc}"[:500]

    def _systemd_matches(
        self, timer: Path, mode: str = "focused"
    ) -> tuple[bool | None, str]:
        if not timer.exists() and not timer.is_symlink():
            return None, ""
        service = timer.with_name(self._systemd_service_name(mode))
        try:
            if not service.exists() and not service.is_symlink():
                raise ValueError("systemd unit files are missing, unsafe, or too large")
            timer_text = _read_schedule_file(timer).decode("utf-8")
            service_text = _read_schedule_file(service).decode("utf-8")
            expected_command = " ".join(
                _systemd_quote(part) for part in _agent_command()
            )
            matches = service_text == self._systemd_service_content(
                mode, expected_command
            ) and timer_text == self._systemd_timer_content(mode)
            command_detail = _agent_command_detail(_agent_command())
            if command_detail:
                matches = False
            return matches, _join_detail(
                "schedule definition matches current configuration"
                if matches
                else "schedule definition has drifted from current configuration",
                command_detail,
            )
        except Exception as exc:
            return False, f"schedule definition could not be validated: {exc}"[:500]

    def _launchd_payload(self, mode: str, command: list[str]) -> dict[str, object]:
        return {
            "Label": self._launchd_label(mode),
            "ProgramArguments": command,
            "StartCalendarInterval": self._launchd_calendar(mode),
            "RunAtLoad": False,
            "StandardOutPath": str(self.paths.logs_dir / LOG_NAMES[0]),
            "StandardErrorPath": str(self.paths.logs_dir / LOG_NAMES[1]),
            "EnvironmentVariables": {
                "JOBBY_DATA_DIR": str(self.paths.data_dir),
                "JOBBY_CONFIG_DIR": str(self.paths.config_dir),
                "JOBBY_DATABASE": str(self.paths.database),
                "JOBBY_AGENT_MODE": mode,
                "TZ": self.config.timezone,
            },
            "ProcessType": "Background",
            "ThrottleInterval": 60,
            "Umask": 0o077,
        }

    def _systemd_service_content(self, mode: str, command: str) -> str:
        label = "daily focused discovery" if mode == "focused" else "weekly inventory"
        return "\n".join(
            [
                "[Unit]",
                f"Description=Jobby {label} agent",
                "",
                "[Service]",
                "Type=oneshot",
                f"ExecStart={command}",
                f"Environment={_systemd_quote(f'JOBBY_DATA_DIR={self.paths.data_dir}')}",
                f"Environment={_systemd_quote(f'JOBBY_CONFIG_DIR={self.paths.config_dir}')}",
                f"Environment={_systemd_quote(f'JOBBY_DATABASE={self.paths.database}')}",
                f"Environment={_systemd_quote(f'JOBBY_AGENT_MODE={mode}')}",
                f"Environment={_systemd_quote(f'TZ={self.config.timezone}')}",
                "UMask=0077",
                "TimeoutStartSec=30min",
                f"StandardOutput={_systemd_quote(f'append:{self.paths.logs_dir / LOG_NAMES[0]}')}",
                f"StandardError={_systemd_quote(f'append:{self.paths.logs_dir / LOG_NAMES[1]}')}",
                "",
            ]
        )

    def _systemd_timer_content(self, mode: str) -> str:
        label = "daily focused" if mode == "focused" else "weekly inventory"
        return "\n".join(
            [
                "[Unit]",
                f"Description=Run Jobby {label} at {self._display_schedule(mode)}",
                "",
                "[Timer]",
                f"OnCalendar={self._systemd_calendar(mode)}",
                "Persistent=true",
                f"Unit={self._systemd_service_name(mode)}",
                "",
                "[Install]",
                "WantedBy=timers.target",
                "",
            ]
        )

    def _launchd_calendar(self, mode: str) -> dict[str, int]:
        if mode == "focused":
            return {"Hour": self.config.schedule_hour, "Minute": 0}
        # launchd uses Sunday=1, Monday=2, ..., Saturday=7.
        launchd_weekday = ((self.config.inventory_weekday + 1) % 7) + 1
        return {
            "Weekday": launchd_weekday,
            "Hour": self.config.inventory_hour,
            "Minute": 0,
        }

    def _systemd_calendar(self, mode: str) -> str:
        if mode == "focused":
            return f"*-*-* {self.config.schedule_hour:02d}:00:00 {self.config.timezone}"
        return (
            f"{_WEEKDAYS[self.config.inventory_weekday]} *-*-* "
            f"{self.config.inventory_hour:02d}:00:00 {self.config.timezone}"
        )

    def _display_schedule(self, mode: str) -> str:
        if mode == "focused":
            return f"local {self.config.schedule_hour:02d}:00 daily"
        weekday = _WEEKDAYS[self.config.inventory_weekday]
        return f"local {weekday} {self.config.inventory_hour:02d}:00"

    @staticmethod
    def _launchd_label(mode: str) -> str:
        return LABEL if mode == "focused" else INVENTORY_LABEL

    @staticmethod
    def _systemd_service_name(mode: str) -> str:
        return (
            "jobby-agent.service"
            if mode == "focused"
            else "jobby-agent-inventory.service"
        )

    @staticmethod
    def _systemd_timer_name(mode: str) -> str:
        return (
            "jobby-agent.timer" if mode == "focused" else "jobby-agent-inventory.timer"
        )

    @staticmethod
    def _aggregate_status(
        platform: str, definitions: tuple[ScheduleDefinitionStatus, ...]
    ) -> ScheduleStatus:
        installed = any(item.installed for item in definitions)
        complete = len(definitions) == len(SCHEDULE_MODES) and all(
            item.installed for item in definitions
        )
        enabled: bool | None
        if not definitions:
            enabled = None
        elif not installed:
            enabled = False
        elif not complete:
            # Preserve visibility of a still-loaded partial installation so
            # remove cannot report success while one definition remains live.
            enabled = any(item.enabled is True for item in definitions)
        else:
            enabled = all(item.enabled is True for item in definitions)
        matches: bool | None
        if not installed:
            matches = None
        else:
            matches = complete and all(
                item.matches_config is True for item in definitions
            )
        detail = _join_detail(
            *(f"{item.mode}: {item.detail}" for item in definitions if item.detail)
        )
        path = definitions[0].path if definitions else ""
        return ScheduleStatus(
            platform,
            installed,
            enabled,
            path,
            detail,
            matches,
            definitions,
        )


def _agent_command() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "agent", "run"]
    executable = shutil.which("jobby")
    return (
        [executable, "agent", "run"]
        if executable
        else [sys.executable, "-m", "jobby", "agent", "run"]
    )


def _validate_agent_command(command: list[str]) -> None:
    if len(command) < 3 or command[-2:] != ["agent", "run"]:
        raise RuntimeError("scheduler command must invoke 'jobby agent run'")
    executable = Path(command[0]).expanduser()
    if not executable.is_absolute():
        raise RuntimeError("scheduler executable must use a stable absolute path")
    folded = {part.casefold() for part in executable.parts}
    if "tmp" in folded or "cache" in folded or "caches" in folded:
        raise RuntimeError(
            "scheduler executable must not live in a temporary/cache path"
        )
    if executable.exists() and (
        not executable.is_file() or not os.access(executable, os.X_OK)
    ):
        raise RuntimeError("scheduler executable exists but is not executable")


def _agent_command_detail(command: list[str]) -> str:
    try:
        _validate_agent_command(command)
    except RuntimeError as exc:
        return str(exc)
    executable = Path(command[0]).expanduser()
    if not executable.exists():
        return f"scheduled executable is currently missing: {executable}"[:500]
    return ""


def rotate_log_files(
    paths: JobbyPaths,
    *,
    max_bytes: int,
    backup_count: int,
) -> list[Path]:
    """Rotate only Jobby's fixed local logs, never caller-supplied paths."""

    if isinstance(max_bytes, bool) or not 100_000 <= max_bytes <= 100_000_000:
        raise ValueError("log max_bytes must be between 100,000 and 100,000,000")
    if isinstance(backup_count, bool) or not 1 <= backup_count <= 20:
        raise ValueError("log backup_count must be between 1 and 20")
    paths.ensure()
    rotated: list[Path] = []
    for name in LOG_NAMES:
        path = paths.logs_dir / name
        if path.is_symlink():
            raise ValueError(f"refusing to rotate symbolic-link log: {path}")
        if not path.exists() or path.stat().st_size <= max_bytes:
            continue
        oldest = path.with_name(f"{path.name}.{backup_count}")
        if oldest.is_symlink():
            raise ValueError(f"refusing to replace symbolic-link log: {oldest}")
        oldest.unlink(missing_ok=True)
        for index in range(backup_count - 1, 0, -1):
            source = path.with_name(f"{path.name}.{index}")
            destination = path.with_name(f"{path.name}.{index + 1}")
            if source.is_symlink() or destination.is_symlink():
                raise ValueError("refusing to rotate symbolic-link log backup")
            if source.exists():
                source.replace(destination)
        destination = path.with_name(f"{path.name}.1")
        path.replace(destination)
        try:
            destination.chmod(0o600)
        except OSError:
            pass
        rotated.append(destination)
    return rotated


def _systemd_quote(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("%", "%%")
    )
    return f'"{escaped}"'


def _atomic_write(path: Path, content: bytes) -> None:
    _ensure_private_directory(path.parent)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).chmod(0o600)
        Path(temporary_name).replace(path)
        _fsync_directory(path.parent)
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def _read_schedule_file(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if path.is_symlink():
            raise ValueError(
                "scheduler definition must not be a symbolic link"
            ) from None
        raise
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("scheduler definition must be a regular file")
        if before.st_size > MAX_SCHEDULE_FILE_BYTES:
            raise ValueError("scheduler definition is unexpectedly large")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read(MAX_SCHEDULE_FILE_BYTES + 1)
        after = os.fstat(descriptor)
        if _schedule_stat_identity(before) != _schedule_stat_identity(after):
            raise ValueError("scheduler definition changed while it was read")
        if len(payload) > MAX_SCHEDULE_FILE_BYTES:
            raise ValueError("scheduler definition is unexpectedly large")
        return payload
    finally:
        os.close(descriptor)


def _schedule_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _command_detail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr or result.stdout or "").strip()[:500]


def _join_detail(*parts: str) -> str:
    return "; ".join(part.strip() for part in parts if part and part.strip())[:500]


__all__ = [
    "INVENTORY_LABEL",
    "LABEL",
    "LOG_NAMES",
    "SCHEDULE_MODES",
    "ScheduleDefinitionStatus",
    "ScheduleStatus",
    "Scheduler",
    "rotate_log_files",
]
