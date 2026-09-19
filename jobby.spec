# PyInstaller specification for local one-folder macOS/Linux release builds.
# ruff: noqa: F821  # Analysis/PYZ/EXE/COLLECT are injected by PyInstaller.
from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_data_files

datas, binaries, hiddenimports = [], [], ["logging.config"]
excluded_prefixes = (
    "_pytest",
    "alembic.testing",
    "googleapiclient.sample_tools",
    "keyring.testing",
    "openai.helpers",
    "pydantic.mypy",
    "pydantic.v1",
    "sqlalchemy.testing",
)


def keep_runtime_submodule(name):
    return not any(
        name == prefix or name.startswith(f"{prefix}.") for prefix in excluded_prefixes
    )


for package in (
    "textual",
    "sqlalchemy",
    "alembic",
    "cryptography",
    "pydantic",
    "openai",
    "keyring",
    "googleapiclient",
    "google_auth_oauthlib",
    "google.auth",
):
    exclude_datas = (
        ["discovery_cache/documents/*"] if package == "googleapiclient" else None
    )
    package_datas, package_binaries, package_hidden = collect_all(
        package,
        include_py_files=False,
        filter_submodules=keep_runtime_submodule,
        exclude_datas=exclude_datas,
    )
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hidden

# google-api-python-client ships roughly 100 MB of static discovery documents.
# Jobby only uses Gmail v1 and Calendar v3; the frozen entry-point self-test
# verifies that both selected documents can be loaded without network access.
datas += collect_data_files(
    "googleapiclient",
    includes=[
        "discovery_cache/documents/calendar.v3.json",
        "discovery_cache/documents/gmail.v1.json",
    ],
)

# The browser-free PDF renderer embeds these small, redistributable fonts so
# output is identical on macOS and Linux and never depends on system fonts.
datas += collect_data_files(
    "reportlab",
    includes=[
        "fonts/Vera.ttf",
        "fonts/VeraBd.ttf",
        "fonts/VeraBI.ttf",
        "fonts/VeraIt.ttf",
    ],
)

migration_root = Path("src/jobby/migrations")
migration_datas = [
    (str(migration_root / "README"), "jobby/migrations"),
    (str(migration_root / "baseline_v0001.json"), "jobby/migrations"),
    (str(migration_root / "env.py"), "jobby/migrations"),
    (str(migration_root / "script.py.mako"), "jobby/migrations"),
]
migration_datas += [
    (str(path), "jobby/migrations/versions")
    for path in sorted((migration_root / "versions").glob("*.py"))
]
if len(migration_datas) == 4:
    raise RuntimeError("no Alembic migration revisions were found")
migration_datas.append(("THIRD_PARTY_NOTICES.md", "."))

analysis = Analysis(
    ["scripts/jobby_entrypoint.py"],
    pathex=["src"],
    binaries=binaries,
    datas=datas + migration_datas,
    hiddenimports=hiddenimports,
    hookspath=["scripts/pyinstaller_hooks"],
    excludes=[
        "_pytest",
        "alembic.testing",
        "hypothesis",
        "keyring.testing",
        "openai.helpers",
        "playwright",
        "pydantic.mypy",
        "pydantic.v1",
        "pytest",
        "sqlalchemy.testing",
    ],
    noarchive=False,
)
pyz = PYZ(analysis.pure)
exe = EXE(pyz, analysis.scripts, [], exclude_binaries=True, name="jobby", console=True)
coll = COLLECT(exe, analysis.binaries, analysis.datas, name="jobby")
