"""Absolute-import entry point used by the standalone release build."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path


def _release_self_test() -> int:
    """Exercise selectively bundled, optional runtime resources offline."""
    from google.auth.credentials import AnonymousCredentials
    from googleapiclient.discovery import build

    from jobby import __version__
    from jobby.encrypted_backup import decrypt_backup, encrypt_backup

    discovered: list[str] = []
    for service_name, service_version in (("gmail", "v1"), ("calendar", "v3")):
        service = build(
            service_name,
            service_version,
            credentials=AnonymousCredentials(),
            cache_discovery=False,
            static_discovery=True,
        )
        root = service._rootDesc
        if root.get("name") != service_name or root.get("version") != service_version:
            raise RuntimeError(f"invalid {service_name} discovery document")
        discovered.append(f"{service_name}:{service_version}")
    cryptography_check = b"jobby frozen cryptography offline self-test\n"
    with tempfile.TemporaryDirectory(prefix="jobby-release-crypto-") as temp_name:
        temporary = Path(temp_name)
        plaintext = temporary / "plaintext.zip"
        container = temporary / "backup.jobbyenc"
        recovered = temporary / "recovered.zip"
        plaintext.write_bytes(cryptography_check)
        encrypt_backup(plaintext, container, "offline-release-self-test")
        decrypt_backup(container, recovered, "offline-release-self-test")
        if recovered.read_bytes() != cryptography_check:
            raise RuntimeError("cryptography encrypted-backup round trip failed")
    print(
        json.dumps(
            {
                "version": __version__,
                "google_discovery": discovered,
                "cryptography": "aes-256-gcm+scrypt-round-trip",
            }
        )
    )
    return 0


def _dashboard_ready_self_test() -> int:
    """Start the real dashboard headlessly and report only after it is usable."""

    from sqlalchemy import text
    from textual.widgets import Static

    from jobby import __version__
    from jobby.config import load_config, resolve_paths
    from jobby.db import Database
    from jobby.tui import DashboardScreen, JobbyApp

    paths = resolve_paths().ensure()
    database = Database(paths=paths)
    database.initialize()

    async def ready() -> None:
        app = JobbyApp(database, load_config(paths), paths)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()
            screen = app.screen
            if not isinstance(screen, DashboardScreen):
                raise RuntimeError("headless launch did not reach the dashboard")
            for selector in (
                "#dashboard-summary",
                "#dashboard-allocation",
                "#dashboard-agent",
            ):
                rendered = str(screen.query_one(selector, Static).render())
                if "Loading" in rendered:
                    raise RuntimeError(f"dashboard widget remained pending: {selector}")

    try:
        asyncio.run(ready())
        with database.engine.connect() as connection:
            revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
    finally:
        database.dispose()
    print(
        json.dumps(
            {
                "version": __version__,
                "dashboard_ready": True,
                "database_revision": revision,
            }
        )
    )
    return 0


def _main() -> int:
    # Avoid importing the full application graph for the most common release
    # metadata probe. The regular console script continues to use jobby.cli.
    if sys.argv[1:] == ["--version"]:
        from jobby import __version__

        print(f"Jobby {__version__}")
        return 0
    if sys.argv[1:] == ["--release-self-test"]:
        return _release_self_test()
    if sys.argv[1:] == ["--dashboard-ready-self-test"]:
        return _dashboard_ready_self_test()

    from jobby.cli import main

    return main()


if __name__ == "__main__":
    raise SystemExit(_main())
