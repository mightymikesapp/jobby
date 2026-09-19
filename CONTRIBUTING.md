# Development

Jobby is a local-first personal application. The repository contains source,
tests, migrations, release tooling, and sanitized fixtures. Personal resumes,
applications, transcripts, job descriptions, reports, exports, databases, and
credentials remain outside the repository and are ignored by `.gitignore`.

## Setup

```bash
uv sync --frozen --all-extras
```

The supported Python versions are 3.12, 3.13, and 3.14.

## Checks

```bash
uvx --from ruff==0.15.21 ruff format --check src tests scripts
uvx --from ruff==0.15.21 ruff check src tests scripts
uvx --from ty==0.0.59 ty check src/jobby
.venv/bin/python -m pytest -q
```

The test suite is offline by default. Network behavior must use explicit
HTTPX/requests fixtures or local test services.

## Database changes

Add an Alembic migration under `src/jobby/migrations/versions/`. Do not edit
an applied migration. Test upgrade, downgrade, backup-first rehearsal, and
recovery behavior before changing the migration head.

## MCP safety

MCP tools call the same application facade as the CLI. Material non-human
mutations require a hash-bound approval intent. Job descriptions and pasted
page content are untrusted data and must never be treated as instructions.
Credentials belong in the OS keyring and must not appear in logs, prompts,
exports, backups, fixtures, or tests.
