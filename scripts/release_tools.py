#!/usr/bin/env python3
"""Cross-platform metadata, checksum, and frozen-release verification helpers."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
import posixpath
import re
import shutil
import sqlite3
import statistics
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterator


ROOT = Path(__file__).resolve().parents[1]
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[a-zA-Z0-9.+-]*)?\Z")
MAX_CHECKSUM_FILE_BYTES = 1_000_000
MAX_RELEASE_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAX_RELEASE_ARCHIVE_MEMBERS = 100_000


def project_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        version = str(tomllib.load(handle)["project"]["version"])
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError(f"unsupported project version: {version!r}")
    return version


def expected_migration_revision() -> str:
    migration_dir = ROOT / "src" / "jobby" / "migrations" / "versions"
    revisions: set[str] = set()
    predecessors: set[str] = set()
    for path in sorted(migration_dir.glob("*.py")):
        assignments: dict[str, object] = {}
        for node in ast.parse(
            path.read_text(encoding="utf-8"), filename=str(path)
        ).body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name) and target.id in {
                    "revision",
                    "down_revision",
                }:
                    assignments[target.id] = ast.literal_eval(node.value)
        revision = assignments.get("revision")
        down_revision = assignments.get("down_revision")
        if not isinstance(revision, str) or not revision:
            raise ValueError(f"migration has no revision: {path}")
        revisions.add(revision)
        if isinstance(down_revision, str):
            predecessors.add(down_revision)
        elif isinstance(down_revision, (list, tuple)):
            predecessors.update(str(item) for item in down_revision)
        elif down_revision is not None:
            raise ValueError(f"unsupported down_revision in {path}")
    heads = revisions - predecessors
    if len(heads) != 1:
        raise ValueError(f"expected one migration head, found {sorted(heads)}")
    return heads.pop()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with _regular_file_handle(path, label="checksum source") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_checksums(output: Path, paths: list[Path]) -> None:
    resolved = [path.expanduser().absolute() for path in paths]
    for path in resolved:
        if path.is_symlink() or not path.is_file():
            raise ValueError(
                f"checksum input must be a regular non-symbolic file: {path}"
            )
    names = [path.name for path in resolved]
    if len(names) != len(set(names)):
        raise ValueError("checksum inputs must have unique basenames")
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{sha256(path)}  {path.name}" for path in sorted(resolved)]
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f".{output.name}.",
            suffix=".tmp",
            dir=output.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write("\n".join(lines) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, output)
        _fsync_directory(output.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def verify_checksums(checksum_file: Path) -> list[str]:
    checksum_file = checksum_file.expanduser().absolute()
    with _regular_file_handle(
        checksum_file,
        maximum_size=MAX_CHECKSUM_FILE_BYTES,
        label="checksum file",
    ) as handle:
        payload = handle.read(MAX_CHECKSUM_FILE_BYTES + 1)
    if len(payload) > MAX_CHECKSUM_FILE_BYTES:
        raise ValueError("checksum file exceeds its safety limit")
    checksum_text = payload.decode("utf-8")
    verified: list[str] = []
    for line_number, raw_line in enumerate(checksum_text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            expected, filename = raw_line.split("  ", 1)
        except ValueError as exc:
            raise ValueError(
                f"invalid checksum line {line_number}: {raw_line!r}"
            ) from exc
        if not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError(f"invalid SHA-256 on line {line_number}")
        if Path(filename).name != filename:
            raise ValueError(f"checksum filename must be a basename: {filename!r}")
        if not filename or filename in verified:
            raise ValueError(f"duplicate or blank checksum filename: {filename!r}")
        candidate = checksum_file.parent / filename
        actual = sha256(candidate)
        if actual != expected:
            raise ValueError(f"checksum mismatch for {filename}")
        verified.append(filename)
    if not verified:
        raise ValueError("checksum file is empty")
    return verified


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


@contextmanager
def _regular_file_handle(
    path: Path,
    *,
    label: str,
    maximum_size: int | None = None,
) -> Iterator[BinaryIO]:
    requested = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError:
        if requested.is_symlink():
            raise ValueError(f"{label} must not be a symbolic link: {path}") from None
        raise
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"{label} must be a regular file: {path}")
        if maximum_size is not None and before.st_size > maximum_size:
            raise ValueError(f"{label} exceeds its safety limit")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            yield handle
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            raise ValueError(f"{label} changed while it was read: {path}")
    finally:
        os.close(descriptor)


def _safe_member_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        bool(name)
        and name not in {".", "./"}
        and "\\" not in name
        and not path.is_absolute()
        and ".." not in path.parts
    )


def verify_archive(archive: Path) -> dict[str, int]:
    archive = archive.expanduser().absolute()
    executable_names = {"launcher": "jobby/jobby"}
    forbidden_payloads = (
        "/_pytest/",
        "/playwright/",
        "/.local-browsers/",
        "/chrome-headless-shell",
        "/chromium-",
    )
    found: dict[str, int] = {}
    names: set[str] = set()
    link_targets: list[tuple[str, str]] = []
    member_count = 0
    with _regular_file_handle(
        archive,
        maximum_size=MAX_RELEASE_ARCHIVE_BYTES,
        label="release archive",
    ) as source:
        with tarfile.open(fileobj=source, mode="r:gz") as handle:
            for member in handle:
                member_count += 1
                if member_count > MAX_RELEASE_ARCHIVE_MEMBERS:
                    raise ValueError("release archive contains too many members")
                if not _safe_member_name(member.name):
                    raise ValueError(f"unsafe archive member: {member.name!r}")
                if member.name in names:
                    raise ValueError(f"duplicate archive member: {member.name!r}")
                names.add(member.name)
                if not (
                    member.isfile()
                    or member.isdir()
                    or member.issym()
                    or member.islnk()
                ):
                    raise ValueError(
                        f"unsupported archive member type: {member.name!r}"
                    )
                if (member.issym() or member.islnk()) and not _safe_member_name(
                    member.linkname
                ):
                    raise ValueError(
                        f"unsafe archive link target: {member.name!r} -> "
                        f"{member.linkname!r}"
                    )
                if member.issym():
                    target = posixpath.normpath(
                        posixpath.join(posixpath.dirname(member.name), member.linkname)
                    )
                    if not _safe_member_name(target):
                        raise ValueError(
                            f"unsafe archive link target: {member.name!r} -> "
                            f"{member.linkname!r}"
                        )
                    link_targets.append((member.name, target))
                elif member.islnk():
                    target = posixpath.normpath(member.linkname)
                    if not _safe_member_name(target):
                        raise ValueError(
                            f"unsafe archive link target: {member.name!r} -> "
                            f"{member.linkname!r}"
                        )
                    link_targets.append((member.name, target))
                lowered = f"/{member.name.casefold()}"
                if any(token in lowered for token in forbidden_payloads):
                    raise ValueError(
                        f"archive contains a forbidden runtime payload: {member.name!r}"
                    )
                for key, suffix in executable_names.items():
                    if member.name == suffix:
                        if not member.isfile() or member.mode & 0o111 == 0:
                            raise ValueError(
                                "archive executable mode was not preserved: "
                                f"{member.name}"
                            )
                        found[key] = member.mode
    missing_targets = [
        f"{name} -> {target}" for name, target in link_targets if target not in names
    ]
    if missing_targets:
        raise ValueError(
            f"archive contains dangling link targets: {missing_targets[:5]}"
        )
    missing = sorted(set(executable_names) - set(found))
    if missing:
        raise ValueError(
            f"archive is missing executable payloads: {', '.join(missing)}"
        )
    return {"members": member_count, "executables": len(found)}


def install_versioned_bundle(
    bundle: Path,
    install_root: Path,
    *,
    version: str,
    scheduler_rebinder=None,
) -> dict[str, str | bool]:
    """Install an onedir bundle and atomically switch its stable launcher.

    The caller has already verified the frozen bundle.  Installation never
    edits an existing version directory; it publishes a complete staged copy
    first, then replaces only the ``jobby`` symlink in one atomic operation.
    An explicit upgrade workflow may pass ``scheduler_rebinder`` to revalidate
    definitions after the launcher switch.  Merely installing a release never
    enables scheduling.
    """

    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError(f"unsupported project version: {version!r}")
    source = bundle.expanduser().absolute()
    if source.is_symlink() or not source.is_dir():
        raise ValueError("bundle must be a real onedir directory")
    source_launcher = source / "jobby"
    if (
        source_launcher.is_symlink()
        or not source_launcher.is_file()
        or not os.access(source_launcher, os.X_OK)
    ):
        raise ValueError("bundle must contain an executable jobby launcher")
    _validate_installed_tree(source)

    root = install_root.expanduser().absolute()
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise ValueError("install root must be a real directory")
    root.mkdir(parents=True, exist_ok=True)
    versions = root / "versions"
    if versions.exists() and (versions.is_symlink() or not versions.is_dir()):
        raise ValueError("version directory must be a real directory")
    versions.mkdir(mode=0o755, exist_ok=True)
    target = versions / version
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"version is already installed: {version}")
    stable = root / "jobby"
    if stable.exists() and not stable.is_symlink():
        raise ValueError("stable launcher exists but is not a symbolic link")

    staged_parent = Path(tempfile.mkdtemp(prefix=f".{version}-install-", dir=versions))
    staged = staged_parent / "bundle"
    try:
        shutil.copytree(source, staged, symlinks=True)
        _validate_installed_tree(staged)
        os.replace(staged, target)
        _fsync_directory(versions)
    finally:
        shutil.rmtree(staged_parent, ignore_errors=True)

    temporary_link = root / f".jobby-switch-{os.getpid()}-{time.time_ns()}"
    try:
        temporary_link.symlink_to(Path("versions") / version / "jobby")
        os.replace(temporary_link, stable)
        _fsync_directory(root)
    finally:
        temporary_link.unlink(missing_ok=True)

    rebound = False
    if scheduler_rebinder is not None:
        try:
            scheduler_rebinder(stable)
        except Exception as exc:
            raise RuntimeError(
                f"release {version} is installed and the stable launcher now points "
                f"to {stable}, but scheduler rebind failed: {exc}"
            ) from exc
        rebound = True
    return {
        "version": version,
        "version_directory": str(target),
        "stable_launcher": str(stable),
        "scheduler_revalidated": rebound,
    }


def _validate_installed_tree(root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    launcher = root / "jobby"
    if (
        launcher.is_symlink()
        or not launcher.is_file()
        or not os.access(launcher, os.X_OK)
    ):
        raise ValueError("staged release has no executable launcher")
    for path in root.rglob("*"):
        if not path.is_symlink():
            if not path.is_file() and not path.is_dir():
                raise ValueError(f"bundle contains a non-file payload: {path}")
            continue
        target = Path(os.readlink(path))
        if target.is_absolute() or ".." in target.parts:
            raise ValueError(f"bundle contains an unsafe symbolic link: {path}")
        try:
            resolved_target = path.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ValueError(
                f"bundle contains a dangling symbolic link: {path}"
            ) from exc
        if not resolved_target.is_relative_to(resolved_root):
            raise ValueError(f"bundle symbolic link escapes its root: {path}")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def rebind_scheduler_definitions(
    stable_launcher: Path,
    *,
    runner=subprocess.run,
) -> dict[str, object]:
    """Rebind an existing opt-in scheduler through the stable launcher.

    ``jobby schedule rebind`` intentionally returns one when no scheduler is
    installed.  For a release install that is a successful no-op, not a reason
    to enable scheduling. Installed definitions must return a fully healthy,
    configuration-matching status.
    """

    stable = stable_launcher.expanduser().absolute()
    if not stable.is_file() or not os.access(stable, os.X_OK):
        raise ValueError("stable launcher must be an executable file")
    result = runner(
        [str(stable), "schedule", "rebind"],
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
        env=os.environ.copy(),
    )
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError):
        payload = None
    if not isinstance(payload, dict) or type(payload.get("installed")) is not bool:
        detail = (result.stderr or result.stdout or "no status output").strip()[:1_000]
        raise RuntimeError(f"scheduler rebind returned invalid status: {detail}")

    installed = payload["installed"]
    if installed is False:
        if result.returncode not in {0, 1}:
            detail = result.stderr or payload.get("detail") or "unknown error"
            raise RuntimeError(f"scheduler absence check failed: {str(detail)[:1_000]}")
        if payload.get("enabled") is True or payload.get("matches_config") is True:
            raise RuntimeError(
                "scheduler absence check returned an inconsistent enabled state"
            )
        return {
            "status": "absent",
            "installed": False,
            "returncode": result.returncode,
        }

    if (
        result.returncode != 0
        or payload.get("enabled") is not True
        or payload.get("matches_config") is not True
    ):
        detail = result.stderr or payload.get("detail") or "definitions are unhealthy"
        raise RuntimeError(f"scheduler rebind did not validate: {str(detail)[:1_000]}")
    return {
        "status": "rebound",
        "installed": True,
        "returncode": result.returncode,
    }


def macos_minimum_versions(bundle: Path) -> list[str]:
    """Return deployment targets for Mach-O files in a macOS bundle."""

    if sys.platform != "darwin":
        return []
    candidates = [bundle / "jobby"]
    candidates.extend(bundle.rglob("*.so"))
    candidates.extend(bundle.rglob("*.dylib"))
    versions: set[str] = set()
    for candidate in candidates:
        result = subprocess.run(
            ["otool", "-l", str(candidate)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            continue
        versions.update(_extract_macos_minimum_versions(result.stdout))
    return sorted(versions, key=_version_tuple)


def _extract_macos_minimum_versions(output: str) -> set[str]:
    versions: set[str] = set()
    command = ""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Load command "):
            command = ""
        elif stripped.startswith("cmd "):
            command = stripped.removeprefix("cmd ")
        elif command == "LC_BUILD_VERSION" and stripped.startswith("minos "):
            versions.add(stripped.removeprefix("minos ").strip())
        elif command == "LC_VERSION_MIN_MACOSX" and stripped.startswith("version "):
            versions.add(stripped.removeprefix("version ").strip())
    return versions


def _version_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def directory_size(path: Path) -> int:
    return sum(
        item.stat(follow_symlinks=False).st_size
        for item in path.rglob("*")
        if not item.is_symlink() and item.is_file()
    )


def _run(
    command: list[str], *, env: dict[str, str], timeout: float = 90
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            f"{' '.join(command)} failed ({result.returncode}): {detail}"
        )
    return result


def verify_bundle(
    bundle: Path,
    *,
    expected_version: str,
    expected_migration: str,
    max_median_startup: float,
    max_bundle_mib: float,
    max_macos_min_version: str = "13.0",
    max_cold_dashboard_ready: float = 5.0,
    max_warm_dashboard_ready: float = 2.5,
) -> dict[str, object]:
    if not VERSION_PATTERN.fullmatch(expected_version):
        raise ValueError(f"unsupported expected version: {expected_version!r}")
    for label, value in (
        ("max_median_startup", max_median_startup),
        ("max_bundle_mib", max_bundle_mib),
        ("max_cold_dashboard_ready", max_cold_dashboard_ready),
        ("max_warm_dashboard_ready", max_warm_dashboard_ready),
    ):
        _positive_finite(label, value)
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", max_macos_min_version):
        raise ValueError("max_macos_min_version must be a dotted numeric version")
    bundle = bundle.expanduser().absolute()
    if bundle.is_symlink() or not bundle.is_dir():
        raise ValueError("bundle must be a real onedir directory")
    _validate_installed_tree(bundle)
    executable = (bundle / "jobby").resolve(strict=True)
    if not os.access(executable, os.X_OK):
        raise ValueError(f"bundle launcher is not executable: {executable}")
    bundle_bytes = directory_size(bundle)
    if bundle_bytes > max_bundle_mib * 1024 * 1024:
        raise ValueError(
            f"bundle size {bundle_bytes / 1024 / 1024:.1f} MiB exceeds "
            f"{max_bundle_mib:.1f} MiB"
        )
    forbidden = [
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if any(
            token in path.relative_to(bundle).as_posix().casefold()
            for token in (
                "_pytest",
                "playwright",
                ".local-browsers",
                "chrome-headless-shell",
            )
        )
    ]
    if forbidden:
        raise ValueError(f"bundle contains forbidden runtime payloads: {forbidden[:5]}")
    deployment_targets = macos_minimum_versions(bundle)
    if sys.platform == "darwin" and not deployment_targets:
        raise ValueError("macOS bundle has no inspectable deployment targets")
    too_new = [
        target
        for target in deployment_targets
        if _version_tuple(target) > _version_tuple(max_macos_min_version)
    ]
    if too_new:
        raise ValueError(
            "bundle contains binaries requiring newer macOS than "
            f"{max_macos_min_version}: {too_new}"
        )
    discovery_dir = (
        bundle / "_internal" / "googleapiclient" / "discovery_cache" / "documents"
    )
    discovery_documents = sorted(path.name for path in discovery_dir.glob("*.json"))
    if discovery_documents != ["calendar.v3.json", "gmail.v1.json"]:
        raise ValueError(
            f"unexpected bundled Google discovery documents: {discovery_documents}"
        )

    base_env = os.environ.copy()
    with tempfile.TemporaryDirectory(prefix="jobby-release-home-") as home_name:
        home = Path(home_name)
        env = base_env | {
            "JOBBY_HOME": str(home),
            # Frozen verification must never inspect or prompt for credentials
            # from the developer's machine-wide OS keyring.
            "PYTHON_KEYRING_BACKEND": "keyring.backends.null.Keyring",
        }
        # This must be the first invocation of the extracted executable. In
        # addition to a fresh home, that keeps executable/import pages cold.
        started = time.perf_counter()
        dashboard = json.loads(
            _run(
                [str(executable), "--dashboard-ready-self-test"], env=env
            ).stdout.strip()
        )
        cold_dashboard_ready = time.perf_counter() - started
        expected_dashboard = {
            "version": expected_version,
            "dashboard_ready": True,
            "database_revision": expected_migration,
        }
        if dashboard != expected_dashboard:
            raise ValueError(f"unexpected dashboard-ready result: {dashboard!r}")
        if cold_dashboard_ready > max_cold_dashboard_ready:
            raise ValueError(
                f"cold dashboard ready {cold_dashboard_ready:.3f}s exceeds "
                f"{max_cold_dashboard_ready:.3f}s"
            )

        timings: list[float] = []
        for _ in range(5):
            started = time.perf_counter()
            version_result = _run(
                [str(executable), "--version"], env=base_env, timeout=30
            )
            timings.append(time.perf_counter() - started)
            if version_result.stdout.strip() != f"Jobby {expected_version}":
                raise ValueError(
                    f"binary version mismatch: {version_result.stdout.strip()!r}"
                )
        median_startup = statistics.median(timings)
        if median_startup > max_median_startup:
            raise ValueError(
                f"median --version startup {median_startup:.3f}s exceeds "
                f"{max_median_startup:.3f}s"
            )

        self_test = json.loads(
            _run([str(executable), "--release-self-test"], env=env).stdout.strip()
        )
        if self_test != {
            "version": expected_version,
            "google_discovery": ["gmail:v1", "calendar:v3"],
            "cryptography": "aes-256-gcm+scrypt-round-trip",
        }:
            raise ValueError(f"unexpected frozen self-test result: {self_test!r}")

        warm_dashboard_timings: list[float] = []
        for _ in range(5):
            started = time.perf_counter()
            warm_dashboard = json.loads(
                _run(
                    [str(executable), "--dashboard-ready-self-test"], env=env
                ).stdout.strip()
            )
            warm_dashboard_timings.append(time.perf_counter() - started)
            if warm_dashboard != expected_dashboard:
                raise ValueError(
                    f"unexpected warm dashboard-ready result: {warm_dashboard!r}"
                )
        warm_dashboard_median = statistics.median(warm_dashboard_timings)
        if warm_dashboard_median > max_warm_dashboard_ready:
            raise ValueError(
                f"warm dashboard-ready median {warm_dashboard_median:.3f}s exceeds "
                f"{max_warm_dashboard_ready:.3f}s"
            )

        _run([str(executable), "doctor", "--no-network"], env=env)

        export_path = home / "release-smoke.json"
        _run(
            [
                str(executable),
                "export",
                "--format",
                "json",
                "--output",
                str(export_path),
            ],
            env=env,
        )
        exported = json.loads(export_path.read_text(encoding="utf-8"))
        database = home / "data" / "jobby.sqlite3"
        with sqlite3.connect(database) as connection:
            migration = connection.execute(
                "SELECT version_num FROM alembic_version"
            ).fetchone()
        if not migration:
            raise ValueError("fresh frozen database has no Alembic version")
        if migration[0] != expected_migration:
            raise ValueError(
                f"frozen migration head {migration[0]!r} does not match "
                f"source head {expected_migration!r}"
            )

    return {
        "bundle_bytes": bundle_bytes,
        "cold_start_first_seconds": round(timings[0], 4),
        "cold_start_median_seconds": round(median_startup, 4),
        "cold_dashboard_ready_seconds": round(cold_dashboard_ready, 4),
        "warm_dashboard_ready_median_seconds": round(warm_dashboard_median, 4),
        "database_revision": migration[0],
        "exported_tables": len(exported),
        "google_discovery_documents": discovery_documents,
        "macos_deployment_targets": deployment_targets,
    }


def _positive_finite(label: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a finite positive number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{label} must be a finite positive number")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("version", help="Print the pyproject version")

    tag = subparsers.add_parser("check-tag", help="Require tag v<project version>")
    tag.add_argument("tag")

    checksums = subparsers.add_parser("checksums", help="Write SHA256SUMS")
    checksums.add_argument("--output", required=True, type=Path)
    checksums.add_argument("files", nargs="+", type=Path)

    install = subparsers.add_parser(
        "install", help="Install a verified onedir release under a versioned root"
    )
    install.add_argument("--bundle", required=True, type=Path)
    install.add_argument("--install-root", required=True, type=Path)
    install.add_argument("--version", dest="install_version", type=str)
    install.add_argument(
        "--rebind-scheduler",
        action="store_true",
        help=(
            "after switching the stable launcher, revalidate and rewrite only "
            "scheduler definitions that were already installed"
        ),
    )

    verify = subparsers.add_parser("verify", help="Verify a frozen release")
    verify.add_argument("--bundle", required=True, type=Path)
    verify.add_argument("--archive", required=True, type=Path)
    verify.add_argument("--checksums", required=True, type=Path)
    verify.add_argument("--expected-version", type=str)
    verify.add_argument("--max-median-startup", default=5.0, type=float)
    verify.add_argument("--max-cold-dashboard-ready", default=5.0, type=float)
    verify.add_argument("--max-warm-dashboard-ready", default=2.5, type=float)
    verify.add_argument("--max-bundle-mib", default=120.0, type=float)
    verify.add_argument("--max-macos-min-version", default="13.0", type=str)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    version = project_version()
    if args.command == "version":
        print(version)
        return 0
    if args.command == "check-tag":
        expected = f"v{version}"
        if args.tag != expected:
            raise SystemExit(
                f"release tag {args.tag!r} does not match project version {expected!r}"
            )
        print(expected)
        return 0
    if args.command == "checksums":
        write_checksums(args.output, args.files)
        print(args.output)
        return 0
    if args.command == "install":
        result = install_versioned_bundle(
            args.bundle,
            args.install_root,
            version=args.install_version or version,
            scheduler_rebinder=(
                rebind_scheduler_definitions if args.rebind_scheduler else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "verify":
        expected_version = args.expected_version or version
        checksums = verify_checksums(args.checksums)
        if args.archive.name not in checksums:
            raise ValueError("release archive is not listed in the checksum manifest")
        checksummed_archive = (
            args.checksums.expanduser().absolute().parent / args.archive.name
        )
        if sha256(args.archive) != sha256(checksummed_archive):
            raise ValueError("verified archive does not match the checksummed artifact")
        expected_wheel_prefix = f"jobby-{expected_version}-"
        if not any(
            name.startswith(expected_wheel_prefix) and name.endswith(".whl")
            for name in checksums
        ):
            raise ValueError("checksum manifest has no wheel for the expected version")
        archive = verify_archive(args.archive)
        bundle = verify_bundle(
            args.bundle,
            expected_version=expected_version,
            expected_migration=expected_migration_revision(),
            max_median_startup=args.max_median_startup,
            max_bundle_mib=args.max_bundle_mib,
            max_macos_min_version=args.max_macos_min_version,
            max_cold_dashboard_ready=args.max_cold_dashboard_ready,
            max_warm_dashboard_ready=args.max_warm_dashboard_ready,
        )
        print(
            json.dumps(
                {
                    "version": expected_version,
                    "checksums": checksums,
                    "archive": archive,
                    "bundle": bundle,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
