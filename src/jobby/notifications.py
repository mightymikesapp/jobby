"""Native local notifications; no remote messaging is performed."""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NotificationAvailability:
    available: bool
    platform: str
    executable: str | None = None
    detail: str = ""


class NativeNotificationProvider:
    def __init__(
        self,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        *,
        platform: str | None = None,
        which: Callable[[str], str | None] | None = None,
    ):
        self.runner = runner or subprocess.run
        self.platform = platform or sys.platform
        self.which = which or shutil.which

    def availability(self) -> NotificationAvailability:
        if self.platform == "darwin":
            executable = self.which("osascript")
            return NotificationAvailability(
                available=bool(executable),
                platform="macOS",
                executable=executable,
                detail=(
                    "osascript is available"
                    if executable
                    else "osascript was not found; native alerts are unavailable"
                ),
            )
        if self.platform.startswith("linux"):
            executable = self.which("notify-send")
            return NotificationAvailability(
                available=bool(executable),
                platform="Linux",
                executable=executable,
                detail=(
                    "notify-send is available"
                    if executable
                    else "notify-send was not found; install libnotify for native alerts"
                ),
            )
        return NotificationAvailability(
            available=False,
            platform=self.platform,
            detail="native notifications are supported on macOS and Linux only",
        )

    def notify(self, title: str, message: str, *, urgent: bool = False) -> bool:
        title = title.strip()[:200]
        message = message.strip()[:1000]
        if not title or not message:
            return False
        command: list[str] | None = None
        if self.platform == "darwin" and (executable := self.which("osascript")):
            script = "display notification (item 1 of argv) with title (item 2 of argv)"
            command = [
                executable,
                "-e",
                f"on run argv\n{script}\nend run",
                message,
                title,
            ]
        elif self.platform.startswith("linux") and (
            executable := self.which("notify-send")
        ):
            command = [
                executable,
                "--urgency",
                "critical" if urgent else "normal",
                title,
                message,
            ]
        if command is None:
            return False
        try:
            completed = self.runner(
                command, check=False, capture_output=True, text=True, timeout=10
            )
            return completed.returncode == 0
        except Exception:
            return False


__all__ = [
    "NativeNotificationProvider",
    "NotificationAvailability",
]
