# Jobby

Jobby is a local-first, single-user, headless job-search engine for macOS and Linux. SQLite
is its operational source of truth; legacy trackers, reports, resumes, cover
letters, saved job descriptions, and scanner state are imported as immutable,
content-addressed source artifacts.

It works offline for capture, importing, deterministic ranking, pipeline
tracking, document review, analytics, export, and backup. OpenAI-powered web
discovery and document assistance, Gmail, and Google Calendar are optional and
disabled on a fresh install. Jobby never
submits an application, sends email, contacts a recruiter, or changes an
external calendar. Review approvals affect Jobby's local records only.

## Install

Python 3.12 through 3.14 is supported.

For a reproducible checkout using the exact dependency graph in `uv.lock`:

```bash
uv sync --frozen
uv run jobby
```

An ordinary editable install is also supported; it uses the compatible major
version bounds declared in `pyproject.toml`:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/jobby
```

For optional Gmail and Google Calendar review, install the Google clients and
authorize the least-privilege read-only scopes:

```bash
uv sync --frozen --extra google
uv run jobby credentials set google-client --file /path/to/client-secret.json
uv run jobby google connect
```

For the ordinary pip setup, install `-e '.[google]'` instead of `-e .`.

V1 Google integration is read-only: it can suggest application changes and
Calendar interview additions, reschedules, or cancellations for review, but it
does not send mail or create, update, or delete Calendar events. Undated email
language such as “we will follow up” is retained as message metadata rather than
turned into an unusable deadline suggestion.

To produce a personal, architecture-specific one-folder build with an embedded
Python runtime, install `uv` and run:

```bash
./scripts/build_release.sh
dist/jobby/jobby
```

The release script refuses a stale lockfile, uses pinned uv 0.11.28 to provision
a pinned uv-managed Python 3.13.14 interpreter, installs the exact locked
dependency set, runs all tests, builds a wheel, and verifies the extracted frozen
bundle. It also rejects embedded browser and pytest payloads, runs a real
headless dashboard-ready check (5-second cold and 2.5-second warm-median gates),
and, on macOS, rejects binaries
requiring newer than the configured deployment target (13.0 by default). Its
versioned `.tar.gz` preserves Unix executable modes; `dist/SHA256SUMS` covers
both the archive and wheel. For example, the personal macOS ARM64 build produces:

```text
dist/jobby-0.6.0-Darwin-arm64.tar.gz
dist/jobby-0.6.0-py3-none-any.whl
dist/SHA256SUMS
```

Verify downloaded artifacts from that directory with `shasum -a 256 -c SHA256SUMS`
on macOS or `sha256sum -c SHA256SUMS` on Linux before extracting.

Git tags used by the release workflow must exactly match `v<project-version>`.
Frozen releases are platform and architecture specific. The macOS archive is a
personal Apple Silicon build: it is neither Developer ID signed nor notarized
and is not intended for public distribution. The wheel remains the supported
portable installation artifact for Python 3.12 through 3.14 on macOS and Linux.

Verified onedir bundles can be installed without overwriting the previous
release. The helper publishes `versions/<version>` first and then atomically
switches the stable `jobby` launcher:

```bash
.venv/bin/python scripts/release_tools.py install \
  --bundle dist/jobby --install-root "$HOME/.local/lib/jobby" \
  --rebind-scheduler
```

The explicit `--rebind-scheduler` option runs only after the stable launcher is
switched, revalidates and rewrites existing scheduler definitions against that
launcher, and treats an absent scheduler as a successful no-op. It never opts
in to scheduling. If rebind fails, the helper reports that the new version and
launcher switch remain installed so recovery can use `jobby schedule rebind`
directly.

PDFs are rendered locally with ReportLab and bundled fonts. Configured public
career portals are fetched as bounded static HTML over HTTPX with redirect,
public-address, and DNS-rebinding protections. Pages that require JavaScript,
authentication, or a CAPTCHA are intentionally left for manual review; Jobby no
longer installs or embeds Chromium.

Each portal scan is a small same-site crawl (`jobby scan --source portals`). It
reads schema.org `JobPosting` data, follows pagination and listing links, checks
job-shaped sitemap URLs, and hands embedded Greenhouse, Lever, Ashby, Workable,
Workday, SmartRecruiters, iCIMS, or Taleo boards to their API adapters. It obeys
robots.txt for every page it finds on its own, waits between requests, and stays
within `portal_crawl_max_pages` (default 20; `1` reads only the configured page)
and `portal_crawl_delay_seconds` (default 1.0; a larger robots `Crawl-delay`
wins, up to 10 seconds).

## Commands

```bash
jobby                                      # CLI help
jobby import /path/to/legacy/workspace     # repeatable, immutable import
jobby capture --url https://example.test/job/123      # preview only
jobby capture --company Example --title Counsel --save
jobby jobs search --query "legal AI" --limit 50
jobby applications list
jobby documents list
jobby reviews list
jobby tasks list
jobby interviews list
jobby offers compare
jobby questions list
jobby companies discover
jobby analytics report
jobby mcp serve                           # optional local stdio MCP server
jobby scan --source greenhouse
jobby scan --source web --query "legal AI roles"
jobby agent run                              # scheduled-safe defaults
jobby agent run --web                        # also requires OpenAI to be enabled
jobby schedule install|status|remove|rebind
jobby profiles list|show|create|update|delete
jobby source health|test
jobby source resolve 'https://jobs.smartrecruiters.com/Example'
jobby source resolve 'https://careers-example.icims.com/jobs/search' --add
jobby upgrade status|plan|apply|recover
jobby maintenance status|optimize|recover-stale-runs|clean-expired-cache
jobby maintenance compact-evaluations --dry-run|--apply
jobby config init|show|validate
jobby config set openai_enabled true         # explicit AI opt-in
jobby config set scheduled_web_enabled true  # separate scheduled-web opt-in
jobby credentials status
jobby google connect|gmail-sync|calendar-sync|disconnect
jobby export --format json --output export.json
jobby backup --output jobby-backup.zip
jobby backup --encrypt-to /Volumes/Backup/jobby.jobbyenc
jobby search-index status                  # inspect disposable FTS cache health
jobby search-index rebuild                 # rebuild it from operational SQLite
jobby restore jobby-backup.zip               # verified dry-run only
jobby restore jobby.jobbyenc                  # prompts for its passphrase
jobby restore jobby-backup.zip --apply --replace
jobby restore --recover                      # finish/roll back an interrupted restore
jobby doctor | jobby doctor --json
```

Fresh databases bootstrap at the current schema. An existing database is never
silently migrated: `upgrade status` and `upgrade plan` are read-only, while
`upgrade apply` takes an exclusive lock, creates and verifies a snapshot,
rehearses the complete migration against that snapshot, journals the live
operation, and verifies integrity and schema equivalence. If activation is
interrupted, `upgrade recover` restores the verified pre-upgrade snapshot.

Evaluation compaction is similarly explicit. Its dry run reports exact
redundant fingerprints without changing the database. Apply takes an exclusive
lock and a verified backup, retains every meaningful or current evaluation, and
records both per-row ledger entries and a hashed batch manifest. Ordinary
unchanged rescans reuse deterministic evaluations and source state without
creating duplicate evaluation, audit, or observation rows.

Installed focused schedules create and rotate a verified local backup after a
successful run. The Sunday inventory creates an authenticated encrypted copy
only when the configured external destination is already mounted and its tested
passphrase is available from the OS keyring. Missing media is never created as
a local directory; it produces one recurring alert and leaves the newest
verified local backup in place. Once each month, a scheduled backup is restored
into a disposable isolated home as a recovery rehearsal, and `jobby doctor`
reports its result.

Jobs are searched through the stable CLI or the local MCP server. Search supports
score, newest, closing date, company, and title ordering, plus bounded role,
industry, company, location, source, status, and score filters. Results are
paginated at 200 rows and include stable IDs, provenance, timestamps, liveness,
and score explanations. `jobby scan` performs a read-only source scan; it never
invokes paid OpenAI web search unless the explicit web source is selected.
Configured sources are fetched with four workers by default, capped at two
concurrent requests for any one provider or host; `discovery_max_workers` (1–8)
and `discovery_max_workers_per_source` (1–2) are ordinary TOML settings.

Configured ATS adapters include Greenhouse, Lever, Ashby, Workable, Workday,
SmartRecruiters, iCIMS, Taleo Business Edition, USAJobs, Eightfold, Oracle HCM,
Rippling, and Paylocity. An optional credential-gated freehire catalog is
available as an observation source. `source resolve`
previews a normalized typed configuration without writing local state. `--add`
runs one DNS-pinned, no-redirect, 2 MB structural probe and saves only a valid
new board via atomic TOML replacement; an empty but recognizable board is valid.

Search terms of at least three characters use a disposable FTS5 trigram index in
the platform cache directory. SQLite triggers queue generation-numbered changes,
so updates, deletions, and company renames are synchronized idempotently. Short
terms or an unavailable, corrupt, or overly broad index automatically fall back
to literal SQL substring filters. The FTS database is derived data: backups omit
it and restore invalidates it.

Credentials are entered with `jobby credentials set openai`, `usajobs`, or
`google-client` and live in the OS keyring. They are never written to SQLite,
TOML, logs, exports, backups, or model prompts. Non-secret settings use the
platform config directory; SQLite and generated artifacts use the platform data
directory. Set `JOBBY_HOME` for an isolated/test installation, or
`JOBBY_DATABASE` for an explicit database path. Installed scheduler definitions
pin that exact path and drift checks reject mismatches.

OpenAI is disabled by default even when a credential exists. Provider, manual
web-discovery, agent, drafting, enrichment, and Doctor paths all enforce
`openai_enabled`; enabling scheduled paid discovery is a separate opt-in.
The model tiers are explicit and configurable:

- fast: `gpt-5.6-luna`
- quality: `gpt-5.6-terra`
- premium manual rerun: `gpt-5.6-sol`

When OpenAI is enabled, `jobby doctor` validates configured model IDs and never
silently changes a model or cost tier. When it is disabled, Doctor reports the
disabled state and skips provider/model network probes.

Stable job enrichment may reuse validated local AI output for 30 days by default
(`ai_cache_ttl_days`, 1–365). Cache identity includes the exact provider model,
purpose, prompt version, output schema, normalized request, and output-token
limit. Every local hit still creates an auditable zero-token AI run. Web search,
document and outreach drafting, premium reruns, failed/rejected output, and
explicit bypasses are never reused.

AI document tailoring uses the `document-draft-v2` section-edit schema. Each
edit names one mutable section, binds to its exact preimage hash, and cites
approved fact keys. Jobby applies and verifies those edits locally; protected
labels, unsupported numeric claims, dropped sections, changed headings, and
material expansion reject the AI run before any proposal is saved. Accepted
drafts remain pending until explicit review.

Analytics separately ranks boundary-safe legal, IP, and AI-policy requirement
signals once five active canonical jobs score at least 4.0. This shortlist view
continues to render when application-outcome analytics has too small a sample,
and marks terms as evidenced only from approved facts or approved canonical
documents. Gmail sync remains metadata-only: sender, subject, and snippet are
classified with alert/marketing/mock-interview exclusions before ATS-positive
rules, and every result is only a pending suggestion.

Scheduled OpenAI web discovery is disabled by default and bounded by daily-run,
token, and estimated-cost guards when enabled. ATS/API scans and local planning
still run without it. Ranking uses the personal contextual salary floors in the
configuration (federal, private, legal-AI, NYC, and Bay Area); a nonzero legacy
`salary_floor` remains an explicit global override. Salary floors apply only to
confidently annualized USD compensation. Unknown-period and non-USD figures stay
visible as warnings and never cause an automatic rejection.

The deterministic score weights fit and gate passability most (30% each), then
compensation (15%), strategic optionality and workload (10% each), and
location (5%). Fit counts role families named in the job title far above
description mentions. `ranking_excluded_title_terms` lists unwanted role
families (for example `"software engineer"`); a title that names one and no
target family gets the minimum fit and a `role_family` gate failure.
`ranking_target_seniority` (`any`, `early`, `mid`, or `senior`) adds a
seniority gate for title tiers and, when no approved years-of-experience fact
exists, grades stated experience requirements against that stage. Stated
requirements the profile cannot confirm, such as a required bar admission,
cost more than unmentioned ones. `ranking_bar_status` (`unknown`,
`not_admitted`, or `admitted` with optional `ranking_bar_jurisdictions`) sets
bar admission when no approved current-admission fact exists; `not_admitted`
fails required bar admission and more than two years of required legal
experience (any experience requirement on a Counsel or Attorney title counts).
Requirements listed under "Preferred" or "Nice to have" only warn. With
`ranking_skip_credential_gaps = true`, those roles are moved to ignored the way
unpaid roles are, and reopen on rescore after the status changes. After changing these settings or upgrading
the ranker, `jobby maintenance rescore` re-ranks every job while keeping
evaluation history and locked manual scores.

`schedule install` is the only operation that activates local scheduling. It
installs the enabled focused-profile cadence at 7:00 AM and a metadata inventory
at 6:00 AM Sunday by default. Upgrades and `schedule rebind` never install a
missing schedule. Focused runs union every query pack, hydrate matches, and do
not create liveness-absence evidence for sources that cannot filter server-side.
Weekly inventories paginate to bounded completion and report cap/deadline exits
as partial. Source health records make retries, anomalies, partial results, and
suppressed liveness changes inspectable through the CLI and MCP.

Restore is deliberately conservative: the default command only verifies and
rehearses a backup. Applying one requires an explicit flag, replacing an
existing database requires a second explicit flag, and Jobby takes and verifies
an automatic pre-restore backup before activation. Active scanner/agent
processes hold a shared storage lock so a restore cannot swap their database.
Backups are verified in staging and atomically published without overwriting an
existing destination. Optional portable containers are encrypted with
AES-256-GCM using a random salt and nonce and a Scrypt-derived key; authenticated
metadata and the plaintext checksum are verified after decryption. A passphrase
is stored in the OS keyring only after an encrypt/decrypt recovery test and the
user confirms that a separate copy is retained. Scrypt `N` is configurable only
as a power of two from 16,384 through 65,536, keeping untrusted-header work
bounded. If optional keyring storage fails, the verified local and encrypted
backups are still committed to the recovery ledger and the command reports a
`partial` result with a nonzero exit status. Imports always preserve a
managed content-addressed copy;
the unsafe path-only import mode has been removed.

## Deliberate V1 boundaries

- Public portal extraction reads bounded static HTML. JavaScript-only listings,
  login walls, bot challenges, and CAPTCHAs are reported for manual review. The
  portal crawler reaches such sites only through their sitemaps, embedded
  structured data, or a recognized ATS board.
- Workday is paginated and USAJobs supports an explicit `*` national location.
  If either provider reaches its configured safety cap, the scan is marked
  partial rather than presented as complete. USAJobs remains unavailable until
  its API key and account email are configured.
- Google is read-only. Gmail and Calendar metadata become pending local
  suggestions; only an explicit review can change Jobby's local pipeline, and
  Jobby has no mail-send or Calendar-write method.
- Approved documents can be exported locally to Markdown, HTML, DOCX, and PDF.
  Credential-dependent AI drafting is deliberately not exposed as an automatic
  submission action; ReportLab PDF output favors a small browser-free runtime over exact
  browser print fidelity.
- The frozen macOS build is personal, Apple Silicon, unsigned, and unnotarized.
  The Python wheel is the portable macOS/Linux artifact.
- OpenAI and Google are optional. Deterministic ranking, tracking, documents,
  import, export, backup, and restore remain usable without either credential.
- Scheduled paid web search is off by default, scheduler installation is an
  explicit user command, and native desktop notifications are best-effort;
  durable local alerts remain available when an OS notification fails.
- V1 is personal-only: one local user, no cloud sync, no accounts, no Windows,
  and no multi-user coordination. Jobby does not add database-level encryption;
  use FileVault on macOS or LUKS on Linux and protect exports/backups as sensitive
  personal data.

## Legacy workspace

This repository contains a local-first job-search workspace. It combines a
job-board scanner, role evaluation reports, application materials, and small
utilities for PDF generation, liveness checks, and pipeline maintenance. Local
source material and generated work product are intentionally excluded from the
public source tree.

## Main Files

- `job_monitor.py` preserves legacy helper functions for compatibility, but command execution delegates to SQLite-backed `jobby scan`.
- `portals.yml` is the structured source for manual portal checks printed by the monitor.
- `data/pipeline.md` tracks evaluated roles and application status.
- `reports/`, `jds/`, `output/`, cover letters, and resume files are user work product.
- `generate-pdf.mjs`, `check-liveness.mjs`, `merge-tracker.mjs`, and `dedup-tracker.mjs` are maintenance utilities.

## Legacy scanner setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-test.txt
npm install
```

`USAJOBS_API_KEY` is optional. Without it, USAJobs scanning is skipped.

## Legacy commands

```bash
.venv/bin/python job_monitor.py
.venv/bin/python -m pytest -q
node merge-tracker.mjs --dry-run
node dedup-tracker.mjs --dry-run
npm run liveness -- --file urls.txt
```

Historical monitor flags are accepted only to emit a migration warning; they no
longer write `job_monitor_state.json`, reports, JDs, or pipeline Markdown. This
removes the legacy/SQLite split-brain while keeping old imports and tests usable.

## Data Safety

See `DATA_CONTRACT.md` before editing user work-product files. System logic and templates are safe to update; resumes, cover letters, reports, pipeline data, saved JDs, and generated outputs are protected user data.

`python job_monitor.py` remains available as a deprecated, read-through compatibility command.
The Jobby importer never rewrites those protected files and emits a reconciliation
report for imported, skipped, conflicting, and unparsed records.
