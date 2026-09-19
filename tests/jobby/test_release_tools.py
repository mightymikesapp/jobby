from __future__ import annotations

import importlib.util
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "release_tools.py"
SPEC = importlib.util.spec_from_file_location("jobby_release_tools", SCRIPT)
assert SPEC and SPEC.loader
release_tools = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_tools)


def test_project_version_and_tag_must_match(capsys: pytest.CaptureFixture[str]) -> None:
    version = release_tools.project_version()
    assert version == "0.6.0"
    assert release_tools.expected_migration_revision() == "0013_operation_runs"
    assert release_tools.main(["check-tag", f"v{version}"]) == 0
    assert capsys.readouterr().out.strip() == f"v{version}"
    with pytest.raises(SystemExit, match="does not match"):
        release_tools.main(["check-tag", "v99.0.0"])


def test_checksum_round_trip_and_tamper_detection(tmp_path: Path) -> None:
    archive = tmp_path / "jobby.tar.gz"
    wheel = tmp_path / "jobby.whl"
    archive.write_bytes(b"archive")
    wheel.write_bytes(b"wheel")
    checksums = tmp_path / "SHA256SUMS"

    release_tools.write_checksums(checksums, [wheel, archive])
    assert release_tools.verify_checksums(checksums) == [
        "jobby.tar.gz",
        "jobby.whl",
    ]

    wheel.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum mismatch"):
        release_tools.verify_checksums(checksums)


def test_checksum_publication_and_verification_reject_link_and_duplicate_edges(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "jobby.tar.gz"
    archive.write_bytes(b"archive")
    checksums = tmp_path / "SHA256SUMS"
    victim = tmp_path / "victim"
    victim.write_text("untouched", encoding="utf-8")
    (tmp_path / ".SHA256SUMS.tmp").symlink_to(victim)

    release_tools.write_checksums(checksums, [archive])

    assert victim.read_text(encoding="utf-8") == "untouched"
    checksums.write_text(
        checksums.read_text(encoding="utf-8") * 2,
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate"):
        release_tools.verify_checksums(checksums)

    alias = tmp_path / "checksums-link"
    alias.symlink_to(checksums)
    with pytest.raises(ValueError, match="symbolic link"):
        release_tools.verify_checksums(alias)


def _write_archive(
    path: Path,
    *,
    launcher_mode: int = 0o755,
    browser_payload: bool = False,
    pytest_payload: bool = False,
) -> None:
    members = {"jobby/jobby": launcher_mode}
    if browser_payload:
        members["jobby/_internal/playwright/driver/node"] = 0o755
    if pytest_payload:
        members["jobby/_internal/_pytest/__init__.py"] = 0o644
    with tarfile.open(path, "w:gz") as handle:
        for name, mode in members.items():
            payload = b"x"
            member = tarfile.TarInfo(name)
            member.mode = mode
            member.size = len(payload)
            handle.addfile(member, io.BytesIO(payload))


def test_archive_verification_requires_all_executable_modes(tmp_path: Path) -> None:
    archive = tmp_path / "jobby.tar.gz"
    _write_archive(archive)
    assert release_tools.verify_archive(archive) == {
        "members": 1,
        "executables": 1,
    }

    _write_archive(archive, launcher_mode=0o644)
    with pytest.raises(ValueError, match="mode was not preserved"):
        release_tools.verify_archive(archive)

    _write_archive(archive, browser_payload=True)
    with pytest.raises(ValueError, match="forbidden runtime payload"):
        release_tools.verify_archive(archive)

    _write_archive(archive, pytest_payload=True)
    with pytest.raises(ValueError, match="forbidden runtime payload"):
        release_tools.verify_archive(archive)


def test_archive_verification_rejects_duplicates_dangling_links_and_symlinks(
    tmp_path: Path,
) -> None:
    duplicate = tmp_path / "duplicate.tar.gz"
    with tarfile.open(duplicate, "w:gz") as handle:
        for _ in range(2):
            member = tarfile.TarInfo("jobby/jobby")
            member.mode = 0o755
            member.size = 1
            handle.addfile(member, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="duplicate archive member"):
        release_tools.verify_archive(duplicate)

    dangling = tmp_path / "dangling.tar.gz"
    with tarfile.open(dangling, "w:gz") as handle:
        launcher = tarfile.TarInfo("jobby/jobby")
        launcher.mode = 0o755
        launcher.size = 1
        handle.addfile(launcher, io.BytesIO(b"x"))
        link = tarfile.TarInfo("jobby/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "missing"
        handle.addfile(link)
    with pytest.raises(ValueError, match="dangling link targets"):
        release_tools.verify_archive(dangling)

    alias = tmp_path / "archive-link.tar.gz"
    alias.symlink_to(dangling)
    with pytest.raises(ValueError, match="symbolic link"):
        release_tools.verify_archive(alias)


def test_bundle_thresholds_reject_nan_before_running_bundle(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="finite positive"):
        release_tools.verify_bundle(
            tmp_path / "missing-bundle",
            expected_version="0.6.0",
            expected_migration="0009_release_0_6",
            max_median_startup=float("nan"),
            max_bundle_mib=120.0,
        )


def test_verify_cli_requires_archive_and_versioned_wheel_in_manifest(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "jobby-0.6.0.tar.gz"
    archive.write_bytes(b"archive")
    wheel = tmp_path / "jobby-0.6.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    checksums = tmp_path / "SHA256SUMS"
    release_tools.write_checksums(checksums, [wheel])

    with pytest.raises(ValueError, match="archive is not listed"):
        release_tools.main(
            [
                "verify",
                "--bundle",
                str(tmp_path / "bundle"),
                "--archive",
                str(archive),
                "--checksums",
                str(checksums),
            ]
        )


def test_release_configuration_excludes_pytest_and_gates_dashboard_ready() -> None:
    root = SCRIPT.parents[1]
    spec = (root / "jobby.spec").read_text(encoding="utf-8")
    entrypoint = (root / "scripts" / "jobby_entrypoint.py").read_text(encoding="utf-8")
    release_source = (root / "scripts" / "release_tools.py").read_text(encoding="utf-8")

    assert '"_pytest"' in spec
    assert '"pytest"' in spec
    assert '"unittest"' not in spec
    assert '"pydoc_data"' not in spec
    assert "--dashboard-ready-self-test" in entrypoint
    assert '"PYTHON_KEYRING_BACKEND"' in release_source
    assert '"keyring.backends.null.Keyring"' in release_source
    assert release_source.index('"--dashboard-ready-self-test"') < release_source.index(
        '"--version"'
    )
    args = release_tools.build_parser().parse_args(
        [
            "verify",
            "--bundle",
            "bundle",
            "--archive",
            "release.tgz",
            "--checksums",
            "SHA256SUMS",
        ]
    )
    assert args.max_cold_dashboard_ready == 5.0
    assert args.max_warm_dashboard_ready == 2.5


def test_macos_deployment_parser_ignores_dylib_current_versions() -> None:
    output = """
Load command 8
          cmd LC_BUILD_VERSION
      cmdsize 32
     platform 1
        minos 11.0
          sdk 15.5
Load command 9
          cmd LC_ID_DYLIB
      cmdsize 56
         name libexample.dylib
current version 1267.0.0
compatibility version 1.0.0
Load command 10
          cmd LC_VERSION_MIN_MACOSX
      cmdsize 16
      version 10.13
          sdk 12.0
"""

    assert release_tools._extract_macos_minimum_versions(output) == {
        "10.13",
        "11.0",
    }


def _write_bundle(path: Path, payload: bytes) -> Path:
    path.mkdir()
    launcher = path / "jobby"
    launcher.write_bytes(payload)
    launcher.chmod(0o755)
    (path / "_internal").mkdir()
    (path / "_internal" / "runtime.dat").write_bytes(payload)
    return path


def test_versioned_install_atomically_switches_stable_launcher_and_keeps_old(
    tmp_path: Path,
) -> None:
    first = _write_bundle(tmp_path / "bundle-1", b"first")
    second = _write_bundle(tmp_path / "bundle-2", b"second")
    install_root = tmp_path / "installed"
    rebound: list[Path] = []

    initial = release_tools.install_versioned_bundle(
        first,
        install_root,
        version="0.6.0",
        scheduler_rebinder=rebound.append,
    )

    stable = install_root / "jobby"
    assert stable.is_symlink()
    assert stable.read_bytes() == b"first"
    assert rebound == [stable]
    assert initial["scheduler_revalidated"] is True

    release_tools.install_versioned_bundle(
        second,
        install_root,
        version="0.6.1",
    )

    assert stable.is_symlink()
    assert stable.read_bytes() == b"second"
    assert (install_root / "versions" / "0.6.0" / "jobby").read_bytes() == b"first"
    assert (install_root / "versions" / "0.6.1" / "jobby").read_bytes() == b"second"
    assert os.readlink(stable) == "versions/0.6.1/jobby"


def test_versioned_install_rejects_existing_version_and_unsafe_bundle_link(
    tmp_path: Path,
) -> None:
    bundle = _write_bundle(tmp_path / "bundle", b"release")
    install_root = tmp_path / "installed"
    release_tools.install_versioned_bundle(
        bundle,
        install_root,
        version="0.6.0",
    )
    with pytest.raises(FileExistsError, match="already installed"):
        release_tools.install_versioned_bundle(
            bundle,
            install_root,
            version="0.6.0",
        )

    unsafe = _write_bundle(tmp_path / "unsafe", b"unsafe")
    (unsafe / "escape").symlink_to("../outside")
    with pytest.raises(ValueError, match="unsafe symbolic link"):
        release_tools.install_versioned_bundle(
            unsafe,
            install_root,
            version="0.6.2",
        )
    assert not (install_root / "versions" / "0.6.2").exists()


def test_scheduler_rebind_helper_accepts_healthy_or_absent_opt_in_state(
    tmp_path: Path,
) -> None:
    bundle = _write_bundle(tmp_path / "bundle", b"release")
    launcher = bundle / "jobby"
    commands: list[list[str]] = []

    def healthy_runner(command, **_kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "installed": True,
                    "enabled": True,
                    "matches_config": True,
                }
            ),
            "",
        )

    assert release_tools.rebind_scheduler_definitions(
        launcher, runner=healthy_runner
    ) == {
        "status": "rebound",
        "installed": True,
        "returncode": 0,
    }
    assert commands == [[str(launcher.absolute()), "schedule", "rebind"]]

    def absent_runner(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            1,
            json.dumps(
                {
                    "installed": False,
                    "enabled": False,
                    "matches_config": None,
                }
            ),
            "",
        )

    assert release_tools.rebind_scheduler_definitions(
        launcher, runner=absent_runner
    ) == {
        "status": "absent",
        "installed": False,
        "returncode": 1,
    }


def test_scheduler_rebind_failure_is_an_explicit_post_install_partial_failure(
    tmp_path: Path,
) -> None:
    bundle = _write_bundle(tmp_path / "bundle", b"release")
    install_root = tmp_path / "installed"

    def fail_rebind(_stable: Path) -> None:
        raise RuntimeError("injected scheduler failure")

    with pytest.raises(
        RuntimeError,
        match=(
            "release 0.6.0 is installed and the stable launcher now points.*"
            "scheduler rebind failed: injected scheduler failure"
        ),
    ):
        release_tools.install_versioned_bundle(
            bundle,
            install_root,
            version="0.6.0",
            scheduler_rebinder=fail_rebind,
        )

    stable = install_root / "jobby"
    assert stable.is_symlink()
    assert stable.read_bytes() == b"release"
    assert (install_root / "versions" / "0.6.0").is_dir()


def test_install_cli_explicitly_wires_scheduler_rebind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path / "bundle", b"release")
    install_root = tmp_path / "installed"
    rebound: list[Path] = []
    monkeypatch.setattr(
        release_tools,
        "rebind_scheduler_definitions",
        rebound.append,
    )

    assert (
        release_tools.main(
            [
                "install",
                "--bundle",
                str(bundle),
                "--install-root",
                str(install_root),
                "--version",
                "0.6.0",
                "--rebind-scheduler",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["scheduler_revalidated"] is True
    assert rebound == [install_root.absolute() / "jobby"]
