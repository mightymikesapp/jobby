# Data Contract

Defines which files are user data (never auto-updated) vs system data (safe to update with new versions).

## User Layer (PROTECTED — never auto-modify)

These files contain personal data, customizations, and work product. No automated process may read, modify, or delete them without explicit user approval.

| File | Purpose |
|------|---------|
| `Mike Sapp Resume 2026.md` | Master resume — source of truth |
| `Mike_Sapp_AI_Policy_Portfolio.md` | Policy positioning document |
| `Patent Portfolio Licensing Model — mikecheck.md` | Patent strategy |
| `Cover Letter - *.md` | Tailored cover letters (27+) |
| `Job Search Tracker - Mike Sapp 2026.md` | Master tracker |
| `modes/_profile.md` | Candidate narrative, constraints, skip list |
| `config/profile.yml` | Identity, contact, targets |
| `reports/*` | Evaluation reports |
| `output/*` | Generated PDFs |
| `jds/*` | Saved job descriptions |
| `interview-prep/story-bank.md` | STAR+R story bank |
| `data/pipeline.md` | Evaluated roles pipeline |
| `data/scan-history.tsv` | Scan dedup history |
| `job_monitor_state.json` | Scanner state (2,596+ seen jobs) |
| `portals.yml` | Portal configuration |

## System Layer (safe to update)

These files contain system logic, templates, and scripts. They can be updated to improve the system without affecting personal data.

| File | Purpose |
|------|---------|
| `modes/evaluate.md` | Evaluation mode instructions |
| `modes/_shared.md` | System context, scoring, archetypes |
| `modes/generate.md` | Generation mode instructions |
| `modes/apply.md` | Application mode instructions |
| `modes/prep.md` | Interview prep mode instructions |
| `templates/*.html` | Resume and cover letter HTML templates |
| `generate-pdf.mjs` | Legacy Playwright PDF script (not used or packaged by Jobby) |
| `check-liveness.mjs` | Job posting liveness verification |
| `merge-tracker.mjs` | Tracker merge utility |
| `dedup-tracker.mjs` | Tracker dedup utility |
| `CLAUDE.md` | Claude Code guidance (mode routing, ATS strategy) |
| `DATA_CONTRACT.md` | This file |
| `src/jobby/**` | Standalone Jobby package logic and migrations |
| `tests/**` | Offline automated tests and fixtures |
| `pyproject.toml`, `uv.lock`, `alembic.ini` | Python package, dependency, and migration metadata |
| `jobby.spec`, `scripts/build_release.sh` | Standalone release packaging |

## Jobby Import Boundary

`jobby import <workspace>` is the explicit approval boundary for reading and
registering protected source files. The importer records path, SHA-256, size,
mtime, and an immutable content-addressed copy in Jobby's platform data
directory. It never writes back to a source path. Ambiguous records go to an
import-review queue and reconciliation report rather than being guessed.

## Operational Database Boundary

The SQLite database under Jobby's platform data directory is the operational
source of truth after import. Normal, explicit user actions may add or update
workflow records, but installing a newer package never mutates an existing
database automatically. Schema changes require `jobby upgrade apply`, which
takes an exclusive lock, creates and verifies a pre-upgrade snapshot, rehearses
the migration on that snapshot, journals the live apply, and verifies integrity
and schema equivalence. `jobby upgrade recover` restores the verified snapshot
after an interrupted or failed apply.

Provenance is append-only by default. Exact redundant evaluations are removed
only by the explicit `jobby maintenance compact-evaluations --apply` workflow,
after an exclusive lock and verified backup, with a per-record ledger and hashed
batch manifest. Canonical duplicate groups hide noncanonical jobs by default but
never delete jobs, source observations, applications, tasks, or imported source
artifacts. Approved documents and their recorded artifact hashes are immutable.
