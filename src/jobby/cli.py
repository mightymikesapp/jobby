"""The ``jobby`` console command."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import getpass
import json
import math
import os
import re
import stat
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import __version__

if TYPE_CHECKING:
    from .config import AppConfig, SecretStore
    from .db import Database

# Backward-compatible test/embedding seams.  ``None`` means resolve the heavy
# implementation only when its command is actually selected.
Scheduler: Any | None = None
run_doctor: Any | None = None


def resolve_paths():
    from .config import resolve_paths as implementation

    return implementation()


def load_config(paths):
    from .config import load_config as implementation

    return implementation(paths)


def _safe_error(error: object) -> str:
    # Error formatting belongs to the discovery contract, which imports HTTP
    # dependencies.  Most commands never need it on the successful path.
    from .sources.base import sanitize_error_message

    return sanitize_error_message(error)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="jobby",
        description="Local-first, headless personal job-search operating system",
    )
    parser.add_argument("--version", action="version", version=f"Jobby {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    import_parser = subparsers.add_parser(
        "import", help="Import an existing workspace without modifying it"
    )
    import_parser.add_argument("workspace", type=Path)

    scan_parser = subparsers.add_parser("scan", help="Run discovery manually")
    scan_parser.add_argument(
        "--source", help="all, provider, provider:board, web, portals, or portal:NAME"
    )
    scan_parser.add_argument("--query", help="Optional provider/search query")

    agent_parser = subparsers.add_parser("agent", help="Run the local scheduled agent")
    agent_sub = agent_parser.add_subparsers(dest="agent_command", required=True)
    agent_run = agent_sub.add_parser("run")
    web_mode = agent_run.add_mutually_exclusive_group()
    web_mode.add_argument(
        "--web",
        dest="include_web",
        action="store_const",
        const=True,
        help="Explicitly include manual OpenAI web discovery for this cycle",
    )
    web_mode.add_argument(
        "--no-web",
        dest="include_web",
        action="store_const",
        const=False,
        help="Skip OpenAI web discovery for this cycle",
    )
    agent_run.set_defaults(include_web=None)
    agent_run.add_argument(
        "--mode",
        choices=("legacy", "focused", "inventory"),
        default=None,
        help="Select an explicit discovery cadence for an interactive run",
    )

    schedule_parser = subparsers.add_parser(
        "schedule", help="Manage the opt-in daily and weekly local schedules"
    )
    schedule_parser.add_argument(
        "action", choices=("install", "status", "remove", "rebind")
    )

    profiles_parser = subparsers.add_parser(
        "profiles", help="Manage editable focused-discovery profiles"
    )
    profiles_sub = profiles_parser.add_subparsers(
        dest="profiles_command", required=True
    )
    profiles_list = profiles_sub.add_parser("list")
    profiles_list.add_argument("--enabled-only", action="store_true")
    profiles_show = profiles_sub.add_parser("show")
    profiles_show.add_argument("identity")
    profiles_create = profiles_sub.add_parser("create")
    _add_profile_fields(profiles_create, creating=True)
    profiles_update = profiles_sub.add_parser("update")
    profiles_update.add_argument("identity")
    _add_profile_fields(profiles_update, creating=False)
    profiles_delete = profiles_sub.add_parser("delete")
    profiles_delete.add_argument("identity")
    profiles_delete.add_argument(
        "--yes", action="store_true", help="Confirm the profile deletion"
    )

    source_parser = subparsers.add_parser(
        "source", help="Inspect source health or run an explicit source test"
    )
    source_sub = source_parser.add_subparsers(dest="source_command", required=True)
    source_health = source_sub.add_parser("health")
    source_health.add_argument("name", nargs="?")
    source_test = source_sub.add_parser("test")
    source_test.add_argument("selector")
    source_test.add_argument("--query")
    source_resolve = source_sub.add_parser(
        "resolve", help="Detect a supported ATS board URL"
    )
    source_resolve.add_argument("url")
    source_resolve.add_argument(
        "--add",
        action="store_true",
        help="Run one bounded structural test and atomically save the source",
    )

    export_parser = subparsers.add_parser("export", help="Create a portable export")
    export_parser.add_argument(
        "--format", required=True, choices=("markdown", "json", "csv")
    )
    export_parser.add_argument("--output", required=True, type=Path)

    backup_parser = subparsers.add_parser(
        "backup", help="Back up the database and managed artifacts"
    )
    backup_parser.add_argument("--output", type=Path)
    backup_parser.add_argument(
        "--encrypt-to",
        type=Path,
        help="Also publish an authenticated encrypted backup container",
    )
    backup_parser.add_argument(
        "--store-passphrase",
        action="store_true",
        help="Store the tested external-backup passphrase in the OS keyring",
    )
    backup_parser.add_argument(
        "--confirm-passphrase-retained",
        action="store_true",
        help="Confirm you retain the external-backup passphrase separately",
    )

    capture_parser = subparsers.add_parser(
        "capture", help="Preview and explicitly save a manually supplied job"
    )
    capture_parser.add_argument("--url")
    capture_parser.add_argument("--company")
    capture_parser.add_argument("--title")
    capture_parser.add_argument("--location")
    capture_parser.add_argument("--description")
    capture_parser.add_argument("--description-file", type=Path)
    capture_parser.add_argument("--compensation")
    capture_parser.add_argument(
        "--save",
        action="store_true",
        help="Persist the exact preview; without this flag capture is read-only",
    )

    jobs_parser = subparsers.add_parser("jobs", help="Search and inspect jobs")
    jobs_sub = jobs_parser.add_subparsers(dest="jobs_command", required=True)
    jobs_search = jobs_sub.add_parser("search")
    jobs_search.add_argument("--query")
    jobs_search.add_argument("--company")
    jobs_search.add_argument("--location")
    jobs_search.add_argument("--category")
    jobs_search.add_argument("--source")
    jobs_search.add_argument("--status", action="append", dest="statuses")
    jobs_search.add_argument(
        "--sort",
        default="score_high",
        choices=[
            item.value
            for item in __import__("jobby.job_queries", fromlist=["JobSort"]).JobSort
        ],
    )
    jobs_search.add_argument("--limit", type=int, default=50)
    jobs_search.add_argument("--offset", type=int, default=0)
    jobs_search.add_argument("--full-content", action="store_true")
    jobs_get = jobs_sub.add_parser("get")
    jobs_get.add_argument("job_id")
    jobs_get.add_argument("--full-content", action="store_true")
    jobs_sub.add_parser("facets")

    applications_parser = subparsers.add_parser(
        "applications", help="Track applications"
    )
    applications_sub = applications_parser.add_subparsers(
        dest="applications_command", required=True
    )
    applications_list = applications_sub.add_parser("list")
    applications_list.add_argument(
        "--stage",
        choices=[
            item.value
            for item in __import__(
                "jobby.enums", fromlist=["ApplicationStage"]
            ).ApplicationStage
        ],
    )
    applications_list.add_argument("--limit", type=int, default=50)
    applications_list.add_argument("--offset", type=int, default=0)
    applications_get = applications_sub.add_parser("get")
    applications_get.add_argument("application_id")
    applications_create = applications_sub.add_parser("create")
    applications_create.add_argument("job_id")
    applications_create.add_argument("--submission-channel")
    applications_create.add_argument("--notes")
    applications_transition = applications_sub.add_parser("transition")
    applications_transition.add_argument("application_id")
    applications_transition.add_argument("stage")
    applications_transition.add_argument("--reason")
    applications_transition.add_argument("--expected-stage")

    documents_parser = subparsers.add_parser(
        "documents", help="Review and approve documents"
    )
    documents_sub = documents_parser.add_subparsers(
        dest="documents_command", required=True
    )
    documents_list = documents_sub.add_parser("list")
    documents_list.add_argument("--status")
    documents_list.add_argument("--limit", type=int, default=50)
    documents_draft = documents_sub.add_parser("draft")
    documents_draft.add_argument("base_version_id")
    documents_draft.add_argument("content_file", type=Path)
    documents_draft.add_argument("--job-id")
    documents_draft.add_argument("--provenance-json")
    documents_approve = documents_sub.add_parser("approve")
    documents_approve.add_argument("document_id")
    documents_approve.add_argument("--expected-hash", required=True)
    documents_approve.add_argument("--content-file", type=Path)
    documents_reject = documents_sub.add_parser("reject")
    documents_reject.add_argument("document_id")
    documents_reject.add_argument("--expected-hash", required=True)

    reviews_parser = subparsers.add_parser("reviews", help="Review queued changes")
    reviews_sub = reviews_parser.add_subparsers(dest="reviews_command", required=True)
    reviews_list = reviews_sub.add_parser("list")
    reviews_list.add_argument("--limit", type=int, default=50)
    reviews_approve = reviews_sub.add_parser("approve")
    reviews_approve.add_argument("review_id")
    reviews_approve.add_argument(
        "--type",
        default="duplicate",
        choices=("duplicate", "import", "company_candidate", "suggestion"),
    )
    reviews_approve.add_argument("--canonical-job-id")
    reviews_approve.add_argument("--expected-hash")
    reviews_dismiss = reviews_sub.add_parser("dismiss")
    reviews_dismiss.add_argument("review_id")
    reviews_dismiss.add_argument(
        "--type",
        default="duplicate",
        choices=("duplicate", "import", "company_candidate", "suggestion"),
    )
    reviews_dismiss.add_argument("--reason")
    reviews_dismiss.add_argument("--expected-hash")

    tasks_parser = subparsers.add_parser("tasks", help="Manage follow-up tasks")
    tasks_sub = tasks_parser.add_subparsers(dest="tasks_command", required=True)
    tasks_list = tasks_sub.add_parser("list")
    tasks_list.add_argument("--status")
    tasks_list.add_argument("--limit", type=int, default=50)
    tasks_list.add_argument("--offset", type=int, default=0)
    tasks_create = tasks_sub.add_parser("create")
    tasks_create.add_argument("title")
    tasks_create.add_argument("--description")
    tasks_create.add_argument("--due-at")
    tasks_create.add_argument("--job-id")
    tasks_create.add_argument("--application-id")

    contacts_parser = subparsers.add_parser(
        "contacts", help="Manage recruiting contacts"
    )
    contacts_sub = contacts_parser.add_subparsers(
        dest="contacts_command", required=True
    )
    contacts_sub.add_parser("list")
    contacts_create = contacts_sub.add_parser("create")
    contacts_create.add_argument("name")
    contacts_create.add_argument("--company-id")
    contacts_create.add_argument("--email")
    contacts_create.add_argument("--title")
    contacts_create.add_argument("--linkedin-url")
    contacts_create.add_argument("--notes")

    interviews_parser = subparsers.add_parser(
        "interviews", help="Prepare and review interviews"
    )
    interviews_sub = interviews_parser.add_subparsers(
        dest="interviews_command", required=True
    )
    interviews_list = interviews_sub.add_parser("list")
    interviews_list.add_argument("--application-id")
    interviews_list.add_argument("--limit", type=int, default=50)
    interviews_create = interviews_sub.add_parser("create")
    interviews_create.add_argument("application_id")
    interviews_create.add_argument("starts_at")
    interviews_create.add_argument("--ends-at")
    interviews_create.add_argument("--type", dest="interview_type")
    interviews_create.add_argument("--location")
    interviews_create.add_argument("--contact-id")
    interviews_create.add_argument("--notes")
    interviews_session = interviews_sub.add_parser("session")
    interviews_session.add_argument("application_id")
    interviews_session.add_argument(
        "--type", default="preparation", dest="session_type"
    )
    interviews_session.add_argument("--role-focus")
    interviews_session.add_argument("--notes")
    interviews_review = interviews_sub.add_parser("review")
    interviews_review.add_argument("session_id")
    interviews_review.add_argument("retrospective")
    interviews_review.add_argument("--outcome")

    offers_parser = subparsers.add_parser("offers", help="Compare and decide offers")
    offers_sub = offers_parser.add_subparsers(dest="offers_command", required=True)
    offers_list = offers_sub.add_parser("list")
    offers_list.add_argument("--application-id")
    offers_compare = offers_sub.add_parser("compare")
    offers_compare.add_argument("--application-id")
    offers_create = offers_sub.add_parser("create")
    offers_create.add_argument("application_id")
    offers_create.add_argument("--base-salary", type=float, default=0)
    offers_create.add_argument("--annual-bonus", type=float, default=0)
    offers_create.add_argument("--annualized-equity", type=float, default=0)
    offers_create.add_argument("--currency", default="USD")
    offers_create.add_argument("--cost-of-living-index", type=float, default=100)
    offers_create.add_argument("--stress-score", type=float, default=3)
    offers_create.add_argument("--terms-json")
    offers_decision = offers_sub.add_parser("decision")
    offers_decision.add_argument("offer_id")
    offers_decision.add_argument("decision")

    questions_parser = subparsers.add_parser(
        "questions", help="Manage interview questions"
    )
    questions_sub = questions_parser.add_subparsers(
        dest="questions_command", required=True
    )
    questions_list = questions_sub.add_parser("list")
    questions_list.add_argument("--role-focus")
    questions_create = questions_sub.add_parser("create")
    questions_create.add_argument("prompt")
    questions_create.add_argument("--role-focus")
    questions_create.add_argument("--tag", action="append", default=[])
    questions_create.add_argument("--skill", action="append", default=[])
    questions_create.add_argument("--evidence-key", action="append", default=[])

    companies_parser = subparsers.add_parser(
        "companies", help="Discover and watch companies"
    )
    companies_sub = companies_parser.add_subparsers(
        dest="companies_command", required=True
    )
    companies_get = companies_sub.add_parser("get")
    companies_get.add_argument("company_id")
    companies_discover = companies_sub.add_parser("discover")
    companies_discover.add_argument("--candidates-json")
    companies_discover.add_argument("--role")
    companies_discover.add_argument("--location")
    companies_discover.add_argument("--industry")
    companies_watch = companies_sub.add_parser("watch")
    companies_watch.add_argument("company_id")
    companies_watch.add_argument("--criteria-json")
    companies_watch.add_argument("--cadence-days", type=int, default=7)
    companies_unwatch = companies_sub.add_parser("unwatch")
    companies_unwatch.add_argument("company_id")

    analytics_parser = subparsers.add_parser(
        "analytics", help="Report job-search analytics"
    )
    analytics_sub = analytics_parser.add_subparsers(
        dest="analytics_command", required=True
    )
    analytics_sub.add_parser("report")

    mcp_parser = subparsers.add_parser(
        "mcp", help="Run the optional vendor-neutral MCP server"
    )
    mcp_sub = mcp_parser.add_subparsers(dest="mcp_command", required=True)
    mcp_sub.add_parser("serve", help="Serve MCP tools/resources over local stdio")

    maintenance_parser = subparsers.add_parser(
        "maintenance", help="Inspect or explicitly maintain local storage"
    )
    maintenance_sub = maintenance_parser.add_subparsers(
        dest="maintenance_command", required=True
    )
    maintenance_sub.add_parser("status")
    maintenance_optimize = maintenance_sub.add_parser("optimize")
    maintenance_optimize.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )
    maintenance_recover = maintenance_sub.add_parser("recover-stale-runs")
    maintenance_recover.add_argument("--older-than-minutes", type=int, default=180)
    maintenance_recover.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )
    maintenance_clean = maintenance_sub.add_parser("clean-expired-cache")
    maintenance_clean.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )
    maintenance_compact = maintenance_sub.add_parser("compact-evaluations")
    compact_mode = maintenance_compact.add_mutually_exclusive_group(required=True)
    compact_mode.add_argument("--dry-run", action="store_true")
    compact_mode.add_argument("--apply", action="store_true")
    maintenance_compact.add_argument("--backup-output", type=Path)
    maintenance_compact.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )

    search_index_parser = subparsers.add_parser(
        "search-index", help="Inspect or rebuild the disposable job search index"
    )
    search_index_sub = search_index_parser.add_subparsers(
        dest="search_index_command", required=True
    )
    search_index_sub.add_parser("status")
    search_index_sub.add_parser("rebuild")

    restore_parser = subparsers.add_parser(
        "restore", help="Verify or explicitly apply a Jobby backup"
    )
    restore_source = restore_parser.add_mutually_exclusive_group(required=True)
    restore_source.add_argument("archive", type=Path, nargs="?")
    restore_source.add_argument(
        "--recover",
        action="store_true",
        help="Recover or finalize an interrupted restore without an archive",
    )
    restore_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the restore; without this flag Jobby performs a dry-run",
    )
    restore_parser.add_argument(
        "--replace",
        action="store_true",
        help="Allow replacement of an existing operational database",
    )
    restore_parser.add_argument(
        "--config",
        choices=("preserve", "restore-if-missing", "replace"),
        default=None,
        help="Choose how non-secret TOML configuration is handled",
    )
    restore_parser.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )

    upgrade_parser = subparsers.add_parser(
        "upgrade", help="Inspect, rehearse, apply, or recover database upgrades"
    )
    upgrade_sub = upgrade_parser.add_subparsers(dest="upgrade_command", required=True)
    upgrade_sub.add_parser("status", help="Read the current and target revisions")
    upgrade_sub.add_parser("plan", help="Show the backup-first upgrade plan")
    upgrade_apply = upgrade_sub.add_parser(
        "apply", help="Back up, rehearse, apply, and verify a pending upgrade"
    )
    upgrade_apply.add_argument("--backup-dir", type=Path)
    upgrade_apply.add_argument("--lock-timeout", type=_nonnegative_timeout, default=0.0)
    upgrade_recover = upgrade_sub.add_parser(
        "recover", help="Restore the verified pre-upgrade snapshot"
    )
    upgrade_recover.add_argument(
        "--lock-timeout", type=_nonnegative_timeout, default=0.0
    )

    config_parser = subparsers.add_parser(
        "config", help="Initialize, inspect, update, or validate settings"
    )
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_init = config_sub.add_parser("init")
    config_init.add_argument("--force", action="store_true")
    config_sub.add_parser("show")
    config_set = config_sub.add_parser("set")
    config_set.add_argument("key")
    config_set.add_argument("value")
    config_sub.add_parser("validate")

    doctor_parser = subparsers.add_parser(
        "doctor", help="Validate Jobby's local and optional integrations"
    )
    doctor_parser.add_argument(
        "--no-network", action="store_true", help="Skip model availability checks"
    )
    doctor_parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable diagnostic results"
    )

    credentials = subparsers.add_parser(
        "credentials", help="Store credentials in the OS keyring"
    )
    cred_sub = credentials.add_subparsers(dest="credentials_command", required=True)
    cred_set = cred_sub.add_parser("set")
    cred_set.add_argument(
        "name",
        choices=("openai", "usajobs", "usajobs-email", "google-client", "freehire"),
    )
    cred_set.add_argument(
        "--file", type=Path, help="Read Google OAuth client JSON from a file"
    )
    cred_delete = cred_sub.add_parser("delete")
    cred_delete.add_argument(
        "name",
        choices=(
            "openai",
            "usajobs",
            "usajobs-email",
            "google-client",
            "freehire",
            "external-backup",
        ),
    )
    cred_status = cred_sub.add_parser(
        "status", help="Show credential presence and keyring health, never values"
    )
    cred_status.add_argument(
        "name",
        nargs="?",
        choices=(
            "openai",
            "usajobs",
            "usajobs-email",
            "google-client",
            "google-token",
            "freehire",
            "external-backup",
        ),
    )

    google = subparsers.add_parser("google", help="Manage optional Google integration")
    google_sub = google.add_subparsers(dest="google_command", required=True)
    google_sub.add_parser("connect", help="Run interactive OAuth authorization")
    gmail_sync = google_sub.add_parser(
        "gmail-sync", help="Ingest recruiting-message metadata and previews"
    )
    gmail_sync.add_argument(
        "--query",
        default="newer_than:90d (interview OR application OR recruiter OR hiring)",
    )
    gmail_sync.add_argument("--limit", type=_gmail_limit, default=100)
    calendar_sync = google_sub.add_parser(
        "calendar-sync", help="Import read-only interview-event previews"
    )
    calendar_sync.add_argument("--days", type=_calendar_days, default=30)
    google_disconnect = google_sub.add_parser(
        "disconnect", help="Revoke Google authorization and disable the integration"
    )
    google_disconnect.add_argument(
        "--local-only",
        action="store_true",
        help="Force-remove the local token even though the remote grant may remain active",
    )
    return parser


def _add_profile_fields(parser: argparse.ArgumentParser, *, creating: bool) -> None:
    parser.add_argument("--name", required=creating)
    parser.add_argument("--description")
    parser.add_argument("--source", dest="profile_sources", action="append")
    parser.add_argument("--query", dest="profile_queries", action="append")
    parser.add_argument("--location", dest="profile_locations", action="append")
    parser.add_argument("--role", dest="profile_roles", action="append")
    parser.add_argument("--hydration-policy")
    enabled = parser.add_mutually_exclusive_group()
    enabled.add_argument("--enable", dest="profile_enabled", action="store_true")
    enabled.add_argument("--disable", dest="profile_enabled", action="store_false")
    parser.set_defaults(profile_enabled=None)
    if not creating:
        parser.add_argument("--clear-description", action="store_true")
        parser.add_argument("--clear-sources", action="store_true")
        parser.add_argument("--clear-queries", action="store_true")
        parser.add_argument("--clear-locations", action="store_true")
        parser.add_argument("--clear-roles", action="store_true")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    from .config import AppConfig, SecretStore

    try:
        paths = resolve_paths()
        if args.command != "restore":
            paths = paths.ensure()
    except Exception as exc:
        print(
            f"jobby: could not initialize application paths: {_safe_error(exc)}",
            file=sys.stderr,
        )
        return 1
    if args.command == "restore":
        try:
            return _restore_command(args, paths)
        except KeyboardInterrupt:
            print("Cancelled.", file=sys.stderr)
            return 130
        except Exception as exc:
            print(f"jobby: {_safe_error(exc)}", file=sys.stderr)
            return 1
    if args.command == "upgrade":
        try:
            return _upgrade_command(args, paths)
        except KeyboardInterrupt:
            print("Cancelled.", file=sys.stderr)
            return 130
        except Exception as exc:
            print(f"jobby: {_safe_error(exc)}", file=sys.stderr)
            return 1
    # ``config init --force`` is the recovery path for malformed TOML, so it
    # must not try to parse the file it has been asked to replace.
    if args.command == "config" and args.config_command == "init":
        config = AppConfig()
    else:
        try:
            config = load_config(paths)
        except Exception as exc:
            parser.error(f"invalid configuration: {exc}")
    secrets = SecretStore()
    try:
        if args.command == "config":
            return _config_command(args, config, paths)
        if args.command == "credentials":
            return _credentials(args, secrets)
        if args.command == "maintenance":
            return _maintenance_command(args, paths, config)
        if args.command == "schedule":
            return _schedule_command(args, config, paths)
        if args.command == "source" and args.source_command == "resolve":
            return _source_resolve_command(args, config, paths)
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"jobby: {_safe_error(exc)}", file=sys.stderr)
        return 1
    from .db import Database

    try:
        database = Database(paths=paths)
        # Doctor owns its initialization boundary so it can report a broken or
        # upgrade-blocked database in the normal diagnostic result contract.
        if args.command != "doctor":
            database.initialize()
    except Exception as exc:
        print(
            f"jobby: could not initialize database: {_safe_error(exc)}",
            file=sys.stderr,
        )
        return 1
    try:
        if args.command in {
            "jobs",
            "applications",
            "documents",
            "reviews",
            "tasks",
            "contacts",
            "interviews",
            "offers",
            "questions",
            "companies",
            "analytics",
            "mcp",
        }:
            return _headless_command(args, database, config, paths, secrets)
        if args.command is None:
            parser.print_help()
            return 0
        if args.command == "import":
            from .importer import import_workspace

            report = import_workspace(
                args.workspace,
                database=database,
                paths=paths,
                copy_sources=True,
            )
            print(
                f"Imported {report.imported_artifacts} artifact(s), {report.imported_jobs} job(s), "
                f"{report.imported_applications} application(s); skipped {report.skipped_artifacts} unchanged artifact(s)."
            )
            print(
                f"Review queue: {len(report.unparsed)}; conflicts: {len(report.conflicts)}; errors: {len(report.errors)}"
            )
            print(f"Automatically resolved or dismissed: {len(report.resolved)}")
            print(f"Reconciliation: {report.report_markdown}")
            return 1 if report.errors else 0
        if args.command == "scan":
            return _scan(args, database, config, secrets)
        if args.command == "capture":
            return _capture_command(args, database)
        if args.command == "agent":
            from .facade import ApplicationFacade

            facade = ApplicationFacade(
                database, config=config, paths=paths, secrets=secrets
            )
            try:
                payload = facade.run_agent(include_web=args.include_web, mode=args.mode)
            finally:
                facade.close()
            print(json.dumps(payload, indent=2, default=str))
            return 0 if payload["status"] in {"succeeded", "partial"} else 1
        if args.command == "profiles":
            return _profiles_command(args, database)
        if args.command == "source":
            return _source_command(args, database, config, secrets)
        if args.command == "export":
            from .exporter import export_data

            output = export_data(database, args.format, args.output)
            print(output)
            return 0
        if args.command == "backup":
            return _backup_command(args, database, config, paths, secrets)
        if args.command == "search-index":
            from .search_index import SearchIndex

            index = SearchIndex(database)
            status = (
                index.rebuild()
                if args.search_index_command == "rebuild"
                else index.status()
            )
            print(json.dumps(asdict(status), indent=2, default=str))
            return (
                0
                if status.fts_available and (status.integrity_ok or not status.exists)
                else 1
            )
        if args.command == "doctor":
            from .doctor import doctor_exit_code

            doctor_runner = run_doctor
            if doctor_runner is None:
                from .doctor import run_doctor as doctor_runner

            checks = doctor_runner(
                database,
                config,
                paths,
                secrets=secrets,
                check_network=not args.no_network,
            )
            if args.json:
                print(json.dumps([asdict(check) for check in checks], indent=2))
            else:
                for check in checks:
                    marker = {"pass": "✓", "warn": "!", "fail": "✗"}[check.status]
                    print(f"{marker} {check.name}: {check.message}")
            return doctor_exit_code(checks)
        if args.command == "google":
            return _google(args, database, config, paths, secrets)
        parser.error("unknown command")
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"jobby: {_safe_error(exc)}", file=sys.stderr)
        return 1
    finally:
        database.dispose()
    return 0


def _schedule_command(args: argparse.Namespace, config: AppConfig, paths) -> int:
    """Manage scheduler definitions without opening or bootstrapping SQLite."""

    scheduler_type = Scheduler
    if scheduler_type is None:
        from .scheduler import Scheduler as scheduler_type

    scheduler = scheduler_type(config, paths=paths)
    status = getattr(scheduler, args.action)()
    print(json.dumps(asdict(status), indent=2))
    if args.action == "remove":
        return 0 if not status.installed and status.enabled is not True else 1
    return (
        0
        if status.installed and status.enabled is True and status.matches_config is True
        else 1
    )


def _scan(
    args: argparse.Namespace,
    database: Database,
    config: AppConfig,
    secrets: SecretStore,
) -> int:
    selector = (args.source or "all").strip()
    if selector.casefold() == "web" and not (args.query or "").strip():
        raise ValueError("--query is required for OpenAI web discovery")
    from .facade import ApplicationFacade

    facade = ApplicationFacade(database, config=config, secrets=secrets)
    try:
        payload = facade.run_scan(source=selector, query=args.query)
    finally:
        facade.close()
    print(json.dumps(payload, indent=2, default=str))
    return 0 if payload["status"] in {"succeeded", "partial"} else 1


def _capture_command(args: argparse.Namespace, database: Database) -> int:
    from .capture import preview_capture, save_capture

    if args.description and args.description_file:
        raise ValueError("use either --description or --description-file, not both")
    description = args.description
    if args.description_file:
        path = args.description_file.expanduser().absolute()
        if path.is_symlink() or not path.is_file():
            raise ValueError("capture description file must be a regular file")
        if path.stat().st_size > 500_000:
            raise ValueError("capture description file exceeds 500,000 bytes")
        description = path.read_text(encoding="utf-8")
    preview = preview_capture(
        database,
        url=args.url,
        company=args.company,
        title=args.title,
        location=args.location,
        description=description,
        compensation=args.compensation,
    )
    payload = preview.model_dump(mode="json")
    if args.save:
        job = save_capture(database, preview)
        payload["saved_job_id"] = job.id
    else:
        payload["saved_job_id"] = None
        payload["next_step"] = "Review this preview, then repeat with --save."
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def _read_json_argument(value: str | None, *, default: Any) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("JSON argument is malformed") from exc


def _read_json_object(
    value: str | None, *, default: Mapping[str, Any]
) -> dict[str, Any]:
    parsed = _read_json_argument(value, default=dict(default))
    if not isinstance(parsed, dict):
        raise ValueError("JSON argument must be an object")
    return parsed


def _read_json_records(
    value: str | None, *, default: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    parsed = _read_json_argument(value, default=list(default))
    if not isinstance(parsed, list) or any(
        not isinstance(item, dict) for item in parsed
    ):
        raise ValueError("JSON argument must be a list of objects")
    return parsed


def _read_regular_text(path: Path, *, limit: int = 500_000) -> str:
    path = path.expanduser().absolute()
    if path.is_symlink() or not path.is_file():
        raise ValueError("content file must be a regular file")
    if path.stat().st_size > limit:
        raise ValueError("content file exceeds the safety limit")
    return path.read_text(encoding="utf-8")


def _cli_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _required_cli_datetime(value: str | None, *, label: str) -> datetime:
    parsed = _cli_datetime(value)
    if parsed is None:
        raise ValueError(f"{label} is required")
    return parsed


def _headless_command(
    args: argparse.Namespace, database: Database, config, paths, secrets
) -> int:
    """Dispatch the scriptable command groups through the shared facade."""

    from .facade import ApplicationFacade, SearchInput
    from .enums import ApplicationStage, TaskStatus
    from .job_queries import JobSort

    if args.command == "mcp":
        if args.mcp_command != "serve":
            raise ValueError("unknown MCP command")
        from .mcp_server import run_stdio

        return run_stdio(
            ApplicationFacade(
                database,
                config=config,
                paths=paths,
                secrets=secrets,
                actor="mcp_client",
            )
        )

    facade = ApplicationFacade(database, config=config, paths=paths, secrets=secrets)
    try:
        command = args.command
        if command == "jobs":
            if args.jobs_command == "search":
                payload = facade.search_jobs(
                    SearchInput(
                        query=args.query,
                        company=args.company,
                        location=args.location,
                        category=args.category,
                        source=args.source,
                        statuses=tuple(args.statuses or ()),
                        sort=JobSort(args.sort),
                        limit=args.limit,
                        offset=args.offset,
                        full_content=args.full_content,
                    )
                )
            elif args.jobs_command == "get":
                payload = facade.get_job(args.job_id, full_content=args.full_content)
            elif args.jobs_command == "facets":
                payload = facade.get_search_facets()
            else:
                raise ValueError("unknown jobs command")
        elif command == "applications":
            if args.applications_command == "list":
                payload = facade.list_pipeline(
                    stage=ApplicationStage(args.stage) if args.stage else None,
                    limit=args.limit,
                    offset=args.offset,
                )
            elif args.applications_command == "get":
                payload = facade.get_application(args.application_id)
            elif args.applications_command == "create":
                payload = facade.create_application(
                    args.job_id,
                    submission_channel=args.submission_channel,
                    notes=args.notes,
                )
            else:
                payload = facade.transition_application(
                    args.application_id,
                    args.stage,
                    reason=args.reason,
                    expected_stage=args.expected_stage,
                )
        elif command == "documents":
            if args.documents_command == "list":
                payload = facade.list_documents(status=args.status, limit=args.limit)
            elif args.documents_command == "draft":
                payload = facade.create_document_draft(
                    base_version_id=args.base_version_id,
                    content_markdown=_read_regular_text(args.content_file),
                    job_id=args.job_id,
                    provenance=_read_json_records(args.provenance_json, default=[]),
                )
            elif args.documents_command == "approve":
                payload = facade.approve_document(
                    args.document_id,
                    expected_hash=args.expected_hash,
                    edited_content=_read_regular_text(args.content_file)
                    if args.content_file
                    else None,
                )
            else:
                payload = facade.reject_document(
                    args.document_id, expected_hash=args.expected_hash
                )
        elif command == "reviews":
            if args.reviews_command == "list":
                payload = facade.list_pending_reviews(limit=args.limit)
            elif args.reviews_command == "approve":
                payload = facade.approve_review(
                    args.review_id,
                    review_type=args.type,
                    canonical_job_id=args.canonical_job_id,
                    expected_hash=args.expected_hash,
                )
            else:
                payload = facade.dismiss_review(
                    args.review_id,
                    review_type=args.type,
                    reason=args.reason,
                    expected_hash=args.expected_hash,
                )
        elif command == "tasks":
            if args.tasks_command == "list":
                payload = facade.list_tasks(
                    status=TaskStatus(args.status) if args.status else None,
                    limit=args.limit,
                    offset=args.offset,
                )
            else:
                payload = facade.create_task(
                    title=args.title,
                    description=args.description,
                    due_at=_cli_datetime(args.due_at),
                    job_id=args.job_id,
                    application_id=args.application_id,
                )
        elif command == "contacts":
            if args.contacts_command == "list":
                payload = facade.list_contacts()
            else:
                payload = facade.create_contact(
                    name=args.name,
                    company_id=args.company_id,
                    email=args.email,
                    title=args.title,
                    linkedin_url=args.linkedin_url,
                    notes=args.notes,
                )
        elif command == "interviews":
            if args.interviews_command == "list":
                payload = facade.list_interviews(
                    application_id=args.application_id, limit=args.limit
                )
            elif args.interviews_command == "create":
                payload = facade.create_interview(
                    application_id=args.application_id,
                    starts_at=_required_cli_datetime(
                        args.starts_at, label="--starts-at"
                    ),
                    ends_at=_cli_datetime(args.ends_at),
                    interview_type=args.interview_type,
                    location_or_link=args.location,
                    contact_id=args.contact_id,
                    notes=args.notes,
                )
            elif args.interviews_command == "session":
                payload = facade.create_interview_session(
                    application_id=args.application_id,
                    session_type=args.session_type,
                    role_focus=args.role_focus,
                    notes=args.notes,
                )
            else:
                payload = facade.record_interview_review(
                    args.session_id,
                    retrospective=args.retrospective,
                    outcome=args.outcome,
                )
        elif command == "offers":
            if args.offers_command == "list":
                payload = facade.list_offers(application_id=args.application_id)
            elif args.offers_command == "compare":
                payload = facade.compare_offers(application_id=args.application_id)
            elif args.offers_command == "create":
                payload = facade.create_offer(
                    args.application_id,
                    base_salary=args.base_salary,
                    annual_bonus=args.annual_bonus,
                    annualized_equity=args.annualized_equity,
                    currency=args.currency,
                    cost_of_living_index=args.cost_of_living_index,
                    stress_score=args.stress_score,
                    terms=_read_json_object(args.terms_json, default={}),
                )
            else:
                payload = facade.set_offer_decision(args.offer_id, args.decision)
        elif command == "questions":
            if args.questions_command == "list":
                payload = facade.list_questions(role_focus=args.role_focus)
            else:
                payload = facade.create_interview_question(
                    prompt=args.prompt,
                    role_focus=args.role_focus,
                    tags=args.tag,
                    skills=args.skill,
                    evidence_keys=args.evidence_key,
                )
        elif command == "companies":
            if args.companies_command == "get":
                payload = facade.get_company(args.company_id)
            elif args.companies_command == "discover":
                payload = facade.discover_companies(
                    _read_json_records(args.candidates_json, default=[]),
                    role=args.role,
                    location=args.location,
                    industry=args.industry,
                )
            elif args.companies_command == "watch":
                payload = facade.watch_company(
                    args.company_id,
                    criteria=_read_json_object(args.criteria_json, default={}),
                    cadence_days=args.cadence_days,
                )
            else:
                payload = facade.unwatch_company(args.company_id)
        elif command == "analytics":
            payload = facade.get_analytics()
        else:  # pragma: no cover - parser constrains the command set
            raise ValueError("unknown headless command")
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        return 0
    finally:
        facade.close()


def _profiles_command(args: argparse.Namespace, database: Database) -> int:
    from .profiles import (
        create_profile,
        delete_profile,
        get_profile,
        list_profiles,
        update_profile,
    )

    command = args.profiles_command
    if command == "list":
        records = list_profiles(database, enabled=True if args.enabled_only else None)
        payload: object = [record.model_dump(mode="json") for record in records]
    elif command == "show":
        payload = get_profile(database, args.identity).model_dump(mode="json")
    elif command in {"create", "update"}:
        values: dict[str, object] = {}
        for argument, field in (
            ("name", "name"),
            ("description", "description"),
            ("profile_sources", "source_selectors"),
            ("profile_queries", "query_pack"),
            ("profile_locations", "location_filters"),
            ("profile_roles", "role_filters"),
            ("hydration_policy", "hydration_policy"),
            ("profile_enabled", "enabled"),
        ):
            value = getattr(args, argument, None)
            if value is not None:
                values[field] = value
        if command == "update":
            for flag, field in (
                ("clear_description", "description"),
                ("clear_sources", "source_selectors"),
                ("clear_queries", "query_pack"),
                ("clear_locations", "location_filters"),
                ("clear_roles", "role_filters"),
            ):
                if getattr(args, flag, False):
                    if field in values:
                        raise ValueError(
                            f"cannot combine --clear-{field.replace('_', '-')} "
                            f"with a replacement value"
                        )
                    values[field] = None if field == "description" else []
            record = update_profile(database, args.identity, values)
        else:
            record = create_profile(database, values)
        payload = record.model_dump(mode="json")
    elif command == "delete":
        if not args.yes:
            raise ValueError("profile deletion requires --yes")
        payload = {
            "deleted": delete_profile(database, args.identity).model_dump(mode="json")
        }
    else:  # pragma: no cover - argparse enforces commands
        raise ValueError("unknown profiles command")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def _source_command(
    args: argparse.Namespace,
    database: Database,
    config: AppConfig,
    secrets: SecretStore,
) -> int:
    if args.source_command == "resolve":
        paths = database.paths
        if paths is None:
            raise ValueError("source resolution requires configured application paths")
        return _source_resolve_command(args, config, paths)
    if args.source_command == "test":
        selector = args.selector.strip()
        if not selector:
            raise ValueError("source selector must not be blank")
        if selector.casefold() == "web" and not (args.query or "").strip():
            raise ValueError("--query is required for OpenAI web discovery")
        from .discovery_service import run_discovery_scan

        run = run_discovery_scan(
            database,
            config,
            secrets=secrets,
            source_selector=selector,
            query=args.query,
            allow_paid_web=selector.casefold() == "web",
        )
        payload = {
            "scan_run_id": run.id,
            "status": run.status.value,
            "observations": run.discovered_count,
            "sources": run.source_results,
            "error": run.error_summary,
        }
        print(json.dumps(payload, indent=2))
        return 0 if run.status.value in {"succeeded", "partial"} else 1
    if args.source_command != "health":  # pragma: no cover
        raise ValueError("unknown source command")

    from sqlalchemy import select

    from .models import SourceHealth, SourceRun

    with database.session() as session:
        statement = select(SourceHealth)
        if args.name:
            statement = statement.where(SourceHealth.source == args.name.strip())
        statement = statement.order_by(SourceHealth.source)
        health_rows = list(session.scalars(statement))
        payload = []
        for health in health_rows:
            last_run = session.scalar(
                select(SourceRun)
                .where(SourceRun.source == health.source)
                .order_by(SourceRun.attempted_at.desc())
                .limit(1)
            )
            payload.append(
                {
                    "source": health.source,
                    "last_attempt_at": health.last_attempt_at,
                    "last_success_at": health.last_success_at,
                    "last_complete_at": health.last_complete_at,
                    "last_result_count": health.last_result_count,
                    "last_reported_total": health.last_reported_total,
                    "failure_streak": health.failure_streak,
                    "last_failure_class": health.last_failure_class,
                    "anomaly_state": health.anomaly_state,
                    "last_run": (
                        {
                            "id": last_run.id,
                            "scan_run_id": last_run.scan_run_id,
                            "duration_seconds": last_run.duration_seconds,
                            "result_count": last_run.result_count,
                            "reported_total": last_run.reported_total,
                            "retries": last_run.retries,
                            "complete": last_run.complete,
                            "failure_class": last_run.failure_class,
                            "anomaly_state": last_run.anomaly_state,
                        }
                        if last_run is not None
                        else None
                    ),
                }
            )
    print(json.dumps(payload, indent=2, default=str))
    return 0


def _source_resolve_command(
    args: argparse.Namespace,
    config: AppConfig,
    paths: Any,
) -> int:
    from .sources.resolver import resolve_source_url, structural_test_and_add

    resolved = resolve_source_url(args.url)
    payload = resolved.as_dict()
    payload["added"] = False
    if args.add:
        structural_test_and_add(resolved, config, paths=paths)
        payload["added"] = True
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def _maintenance_command(
    args: argparse.Namespace,
    paths,
    config: AppConfig,
) -> int:
    from .db import Database
    from .maintenance import (
        clean_expired_cache_exclusive,
        maintenance_status,
        optimize_database,
        recover_stale_runs_exclusive,
    )

    command = args.maintenance_command
    if command == "optimize":
        result: object = optimize_database(
            paths.database,
            paths=paths,
            lock_timeout=args.lock_timeout,
        )
    elif command == "compact-evaluations":
        from .evaluation_compaction import compact_evaluations

        result = compact_evaluations(
            paths.database,
            apply=args.apply,
            backup_output=args.backup_output,
            paths=paths,
            lock_timeout=args.lock_timeout,
        )
    elif command == "recover-stale-runs":
        if not 1 <= args.older_than_minutes <= 10_080:
            raise ValueError("--older-than-minutes must be between 1 and 10,080")
        result = recover_stale_runs_exclusive(
            paths.database,
            paths=paths,
            stale_after=timedelta(minutes=args.older_than_minutes),
            lock_timeout=args.lock_timeout,
        )
    elif command == "clean-expired-cache":
        result = clean_expired_cache_exclusive(
            paths.database,
            paths=paths,
            lock_timeout=args.lock_timeout,
        )
    else:
        database = Database(paths=paths)
        try:
            database.initialize()
            if command == "status":
                result = maintenance_status(database)
            else:  # pragma: no cover - argparse enforces commands
                raise ValueError("unknown maintenance command")
        finally:
            database.dispose()
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


def _backup_command(
    args: argparse.Namespace,
    database: Database,
    config: AppConfig,
    paths,
    secrets: SecretStore,
) -> int:
    import hashlib
    import tempfile

    from .audit import record_audit
    from .backup import create_backup, verify_backup
    from .backup_rotation import rotate_backups
    from .models import BackupRecord

    if args.store_passphrase and not args.encrypt_to:
        raise ValueError("--store-passphrase requires --encrypt-to")
    if args.store_passphrase and not args.confirm_passphrase_retained:
        raise ValueError("--store-passphrase requires --confirm-passphrase-retained")
    if args.confirm_passphrase_retained and not args.store_passphrase:
        raise ValueError(
            "--confirm-passphrase-retained is meaningful only with --store-passphrase"
        )
    output = create_backup(database, output=args.output, paths=paths)
    ok, detail = verify_backup(output)
    if not ok:
        raise RuntimeError(f"backup verification failed: {detail}")

    digest = hashlib.sha256()
    with output.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    plaintext_sha256 = digest.hexdigest()
    with database.session() as session:
        now = datetime.now(timezone.utc)
        local_record = BackupRecord(
            backup_kind="local",
            path=str(output),
            plaintext_sha256=plaintext_sha256,
            size_bytes=output.stat().st_size,
            verified_at=now,
            external=False,
        )
        session.add(local_record)
        session.flush()
        record_audit(
            session,
            action="backup.created",
            entity_type="backup_record",
            entity_id=local_record.id,
            actor="user",
            after={
                "local": str(output),
                "plaintext_sha256": plaintext_sha256,
                "verified": True,
            },
        )

    rotation_payload: dict[str, object] | None = None
    try:
        if output.parent == paths.backups_dir.absolute():
            rotation = rotate_backups(
                output.parent,
                output,
                apply=True,
                daily_retention=config.backup_daily_retention,
                weekly_retention=config.backup_weekly_retention,
                monthly_retention=config.backup_monthly_retention,
            )
            rotation_payload = {
                "pruned": [str(path) for path in rotation.pruned],
                "kept": [str(path) for path in rotation.plan.keep_paths],
            }
    except Exception as exc:
        rotation_error = _safe_error(exc)
        with database.session() as session:
            record_audit(
                session,
                action="backup.rotation_failed",
                entity_type="backup_record",
                entity_id=local_record.id,
                actor="user",
                after={"error": rotation_error},
                detail="Verified local backup retained; rotation did not complete.",
            )
        _print_backup_result(
            status="partial",
            output=output,
            plaintext_sha256=plaintext_sha256,
            encrypted_output=None,
            recovery_tested=False,
            scrypt_n=None,
            passphrase_stored=None,
            storage_error=None,
            operation_error=f"backup rotation failed: {rotation_error}",
            rotation=None,
            store_requested=bool(args.store_passphrase),
        )
        return 1

    encrypted_output: Path | None = None
    recovery_tested_at: datetime | None = None
    passphrase: str | None = None
    if args.encrypt_to:
        try:
            from .encrypted_backup import decrypt_backup, encrypt_backup

            passphrase = secrets.get("external_backup_passphrase")
            if not passphrase:
                passphrase = getpass.getpass("External-backup passphrase: ")
                confirmation = getpass.getpass("Repeat passphrase: ")
                if passphrase != confirmation:
                    raise ValueError("external-backup passphrases did not match")
            encrypted_output = encrypt_backup(
                output,
                args.encrypt_to,
                passphrase,
                scrypt_n=config.external_backup_scrypt_n,
            )
            with tempfile.TemporaryDirectory(
                prefix=".jobby-encrypted-recovery-test-", dir=output.parent
            ) as temporary_name:
                recovered = Path(temporary_name) / "recovered.zip"
                decrypt_backup(encrypted_output, recovered, passphrase)
                recovered_ok, recovered_detail = verify_backup(recovered)
                if not recovered_ok:
                    raise RuntimeError(
                        f"encrypted backup recovery test failed: {recovered_detail}"
                    )
                recovered_hash = hashlib.sha256()
                with recovered.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        recovered_hash.update(chunk)
                recovered_digest = recovered_hash.hexdigest()
                if recovered_digest != plaintext_sha256:
                    raise RuntimeError(
                        "encrypted backup recovery test checksum mismatch"
                    )
            recovery_tested_at = datetime.now(timezone.utc)
        except Exception as exc:
            encryption_error = _safe_error(exc)
            if passphrase:
                encryption_error = encryption_error.replace(passphrase, "[redacted]")
            candidate = Path(args.encrypt_to).expanduser().absolute()
            published = (
                candidate
                if candidate.exists()
                and not candidate.is_symlink()
                and candidate.is_file()
                else None
            )
            with database.session() as session:
                record_audit(
                    session,
                    action="backup.encryption_failed",
                    entity_type="backup_record",
                    entity_id=local_record.id,
                    actor="user",
                    after={
                        "published_external": str(published) if published else None,
                        "error": encryption_error,
                    },
                    detail="Verified local backup retained; encrypted backup did not complete.",
                )
            _print_backup_result(
                status="partial",
                output=output,
                plaintext_sha256=plaintext_sha256,
                encrypted_output=published,
                recovery_tested=False,
                scrypt_n=config.external_backup_scrypt_n,
                passphrase_stored=None,
                storage_error=None,
                operation_error=f"encrypted backup failed: {encryption_error}",
                rotation=rotation_payload,
                store_requested=bool(args.store_passphrase),
            )
            return 1

    external_record_id: str | None = None
    if encrypted_output:
        try:
            with database.session() as session:
                external_record = BackupRecord(
                    backup_kind="encrypted_external",
                    path=str(encrypted_output),
                    plaintext_sha256=plaintext_sha256,
                    size_bytes=encrypted_output.stat().st_size,
                    verified_at=recovery_tested_at or datetime.now(timezone.utc),
                    external=True,
                    recovery_tested_at=recovery_tested_at,
                )
                session.add(external_record)
                session.flush()
                external_record_id = external_record.id
                record_audit(
                    session,
                    action="backup.encrypted_created",
                    entity_type="backup_record",
                    entity_id=external_record.id,
                    actor="user",
                    after={
                        "local_backup_record_id": local_record.id,
                        "encrypted_external": str(encrypted_output),
                        "plaintext_sha256": plaintext_sha256,
                        "recovery_tested": True,
                        "scrypt_n": config.external_backup_scrypt_n,
                        "passphrase_storage_requested": bool(args.store_passphrase),
                    },
                )
        except Exception as exc:
            record_error = _safe_error(exc)
            _print_backup_result(
                status="partial",
                output=output,
                plaintext_sha256=plaintext_sha256,
                encrypted_output=encrypted_output,
                recovery_tested=True,
                scrypt_n=config.external_backup_scrypt_n,
                passphrase_stored=None,
                storage_error=None,
                operation_error=(
                    "encrypted backup was verified but provenance recording failed: "
                    f"{record_error}"
                ),
                rotation=rotation_payload,
                store_requested=bool(args.store_passphrase),
            )
            return 1

    # The verified files and their provenance are committed before touching
    # the optional keyring.  A backend failure must never turn a recoverable
    # encrypted backup into an unreported orphan.
    passphrase_stored: bool | None = None
    passphrase_storage_error: str | None = None
    if args.store_passphrase:
        if passphrase is None or external_record_id is None:  # pragma: no cover
            raise RuntimeError("encrypted backup passphrase state is incomplete")
        try:
            secrets.set("external_backup_passphrase", passphrase)
            passphrase_stored = True
            passphrase_action = "backup.passphrase_stored"
        except Exception as exc:
            passphrase_stored = False
            passphrase_action = "backup.passphrase_store_failed"
            passphrase_storage_error = _safe_error(exc).replace(
                passphrase, "[redacted]"
            )
        with database.session() as session:
            record_audit(
                session,
                action=passphrase_action,
                entity_type="backup_record",
                entity_id=external_record_id,
                actor="user",
                after={
                    "stored": passphrase_stored,
                    "error": passphrase_storage_error,
                },
                detail=(
                    "Tested external-backup passphrase stored in the OS keyring."
                    if passphrase_stored
                    else "Encrypted backup remains verified; OS keyring storage failed."
                ),
            )
    _print_backup_result(
        status="partial" if passphrase_storage_error else "succeeded",
        output=output,
        plaintext_sha256=plaintext_sha256,
        encrypted_output=encrypted_output,
        recovery_tested=recovery_tested_at is not None,
        scrypt_n=config.external_backup_scrypt_n if encrypted_output else None,
        passphrase_stored=passphrase_stored,
        storage_error=passphrase_storage_error,
        operation_error=passphrase_storage_error,
        rotation=rotation_payload,
        store_requested=bool(args.store_passphrase),
    )
    if passphrase_storage_error:
        print(
            "jobby: encrypted backup succeeded and was recorded, but passphrase "
            f"storage failed: {passphrase_storage_error}",
            file=sys.stderr,
        )
        return 1
    return 0


def _print_backup_result(
    *,
    status: str,
    output: Path,
    plaintext_sha256: str,
    encrypted_output: Path | None,
    recovery_tested: bool,
    scrypt_n: int | None,
    passphrase_stored: bool | None,
    storage_error: str | None,
    operation_error: str | None,
    rotation: dict[str, object] | None,
    store_requested: bool,
) -> None:
    """Report every published backup, including durable partial-success cases."""

    print(
        json.dumps(
            {
                "status": status,
                "local_backup": str(output),
                "encrypted_backup": str(encrypted_output) if encrypted_output else None,
                "plaintext_sha256": plaintext_sha256,
                "recovery_tested": recovery_tested,
                "scrypt_n": scrypt_n,
                "error": operation_error,
                "passphrase_storage": {
                    "requested": store_requested,
                    "stored": passphrase_stored,
                    "error": storage_error,
                },
                "rotation": rotation,
            },
            indent=2,
        )
    )


def _restore_command(args: argparse.Namespace, paths) -> int:
    import tempfile

    from .restore import recover_interrupted_restore, restore_backup

    if args.recover:
        if args.apply or args.replace or args.config is not None:
            raise ValueError(
                "--recover cannot be combined with --apply, --replace, or --config"
            )
        recovered = recover_interrupted_restore(
            paths=paths, lock_timeout=args.lock_timeout
        )
        print(json.dumps({"recovered": recovered}, indent=2))
        if not recovered:
            print("No interrupted restore was found.")
        return 0

    archive = args.archive.expanduser().absolute()
    if archive.is_symlink() or not archive.is_file():
        raise ValueError("restore archive must be a regular non-symbolic file")
    with archive.open("rb") as handle:
        prefix = handle.read(8)
    if prefix == b"JOBBYENC":
        from .config import SecretStore
        from .encrypted_backup import decrypt_backup

        passphrase = SecretStore().get("external_backup_passphrase")
        if not passphrase:
            passphrase = getpass.getpass("External-backup passphrase: ")
        with tempfile.TemporaryDirectory(
            prefix=".jobby-encrypted-restore-"
        ) as temporary_name:
            decrypted = Path(temporary_name) / "backup.zip"
            decrypt_backup(archive, decrypted, passphrase)
            result = restore_backup(
                decrypted,
                paths=paths,
                apply=args.apply,
                replace_current=args.replace,
                config_policy=args.config or "preserve",
                lock_timeout=args.lock_timeout,
            )
    else:
        result = restore_backup(
            archive,
            paths=paths,
            apply=args.apply,
            replace_current=args.replace,
            config_policy=args.config or "preserve",
            lock_timeout=args.lock_timeout,
        )
    payload = {
        "applied": result.applied,
        "plan": asdict(result.plan),
        "restored_artifacts": result.restored_artifacts,
        "reused_artifacts": result.reused_artifacts,
        "pre_restore_backup": result.pre_restore_backup,
        "audit_recorded": result.audit_recorded,
    }
    print(json.dumps(payload, indent=2, default=str))
    if not result.applied:
        print("Dry-run only. Re-run with --apply and, if needed, --replace.")
    return 0


def _upgrade_command(args: argparse.Namespace, paths) -> int:
    from .upgrade import (
        apply_upgrade,
        plan_upgrade,
        recover_upgrade,
        upgrade_status,
    )

    if args.upgrade_command == "status":
        result: object = upgrade_status(paths.database)
    elif args.upgrade_command == "plan":
        result = plan_upgrade(paths.database)
    elif args.upgrade_command == "apply":
        result = apply_upgrade(
            paths.database,
            backup_dir=args.backup_dir or paths.backups_dir,
            lock_timeout=args.lock_timeout,
        )
    elif args.upgrade_command == "recover":
        result = recover_upgrade(
            paths.database,
            lock_timeout=args.lock_timeout,
        )
    else:  # pragma: no cover - argparse enforces the choices
        raise ValueError("unknown upgrade command")
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


def _nonnegative_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number") from exc
    if not math.isfinite(timeout) or timeout < 0:
        raise argparse.ArgumentTypeError("timeout must be a finite non-negative number")
    return timeout


def _config_command(args: argparse.Namespace, config: AppConfig, paths) -> int:
    from .config import AppConfig, load_config, redacted_config, save_config

    if args.config_command == "init":
        if paths.config_file.exists() and not args.force:
            raise FileExistsError(
                f"configuration already exists: {paths.config_file}; use --force to replace it"
            )
        output = save_config(AppConfig(), paths)
        print(output)
        return 0
    if args.config_command == "show":
        print(json.dumps(redacted_config(config), indent=2, sort_keys=True))
        return 0
    if args.config_command == "validate":
        validated = load_config(paths)
        print(
            f"Configuration is valid: {paths.config_file}"
            if paths.config_file.exists()
            else "Configuration defaults are valid; no config.toml exists yet."
        )
        # Force complete model serialization as an additional schema check.
        validated.model_dump(mode="json")
        return 0
    if args.config_command != "set":
        raise ValueError("unknown configuration command")

    data = config.model_dump(mode="json")
    parts = [item.strip() for item in args.key.split(".")]
    if not parts or any(not item for item in parts):
        raise ValueError(
            "configuration key must not be blank or contain empty path segments"
        )
    target: dict[str, Any] = data
    for part in parts[:-1]:
        child = target.get(part)
        if not isinstance(child, dict):
            raise ValueError(f"unknown or non-object configuration key: {args.key}")
        target = child
    leaf = parts[-1]
    if leaf not in target:
        raise ValueError(f"unknown configuration key: {args.key}")
    target[leaf] = _parse_config_value(args.value)
    updated = AppConfig.model_validate(data)
    output = save_config(updated, paths)
    print(f"Updated {args.key} in {output}")
    return 0


def _parse_config_value(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _credentials(args: argparse.Namespace, secrets: SecretStore) -> int:
    names = {
        "openai": "openai_api_key",
        "usajobs": "usajobs_api_key",
        "usajobs-email": "usajobs_email",
        "google-client": "google_oauth_client",
        "google-token": "google_oauth_token",
        "external-backup": "external_backup_passphrase",
        "freehire": "freehire_api_key",
    }
    if args.credentials_command == "status":
        selected = [args.name] if args.name else list(names)
        rows = []
        for display_name in selected:
            status = secrets.status(names[display_name])
            rows.append(
                {
                    "name": display_name,
                    "state": status.state,
                    "message": status.message,
                }
            )
        print(json.dumps(rows, indent=2))
        return 1 if any(row["state"] in {"unavailable", "error"} for row in rows) else 0

    key = names[args.name]
    if args.credentials_command == "delete":
        removed = secrets.delete(key)
        print(
            f"Deleted {args.name} from the OS keyring."
            if removed
            else f"{args.name} was not configured."
        )
        return 0
    if args.file and args.name != "google-client":
        raise ValueError("--file is supported only for google-client credentials")
    if args.name == "google-client" and args.file:
        value = _read_credential_file(args.file)
    elif args.name == "usajobs-email":
        value = input("USAJobs account email: ")
    else:
        value = getpass.getpass(f"{args.name} value: ")
    value = value.strip()
    if not value:
        raise ValueError("credential value must not be blank")
    if args.name == "google-client":
        try:
            client_config = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Google OAuth client configuration must be valid JSON"
            ) from exc
        if not isinstance(client_config, dict) or not (
            isinstance(client_config.get("installed"), dict)
            and client_config["installed"].get("client_id")
        ):
            raise ValueError(
                "Google OAuth client JSON must be a Desktop app client with installed.client_id"
            )
    if args.name == "usajobs-email" and not re.fullmatch(
        r"[^\s@]+@[^\s@]+\.[^\s@]+", value
    ):
        raise ValueError("USAJobs account email is invalid")
    if args.name == "usajobs":
        email = input("USAJobs account email: ").strip()
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
            raise ValueError("USAJobs account email is invalid; nothing was stored")
        secrets.set("usajobs_email", email)
    secrets.set(key, value)
    print(f"Stored {args.name} in the OS keyring.")
    return 0


def _read_credential_file(value: Path | str) -> str:
    """Read a bounded regular credential file without following a final symlink."""

    from .config import MAX_SECRET_CHARS

    path = Path(value).expanduser().absolute()
    if path.is_symlink():
        raise ValueError("credential file must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if path.is_symlink():
            raise ValueError("credential file must not be a symbolic link") from None
        raise ValueError(f"credential file cannot be read: {_safe_error(exc)}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("credential file must be a regular file")
        if metadata.st_size > MAX_SECRET_CHARS:
            raise ValueError("credential file exceeds the safety limit")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read(MAX_SECRET_CHARS + 1)
    finally:
        os.close(descriptor)
    if len(payload) > MAX_SECRET_CHARS:
        raise ValueError("credential file exceeds the safety limit")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("credential file must contain valid UTF-8") from exc


def _google(
    args: argparse.Namespace,
    database: Database,
    config: AppConfig,
    paths,
    secrets: SecretStore,
) -> int:
    from sqlalchemy import select

    from .config import save_config
    from .google_integration import (
        CALENDAR_EVENTS_READONLY_SCOPE,
        GMAIL_READONLY_SCOPE,
        CalendarProvider,
        GmailProvider,
        GoogleOAuthManager,
        ingest_calendar_metadata,
        ingest_gmail_metadata,
    )
    from .models import IntegrationState

    oauth = GoogleOAuthManager(secrets)
    if args.google_command == "connect":
        scopes = [GMAIL_READONLY_SCOPE, CALENDAR_EVENTS_READONLY_SCOPE]
        oauth.credentials(scopes, interactive=True)
        config.google_enabled = True
        save_config(config, paths)
        print(
            "Google read-only integration authorized; no mailbox or calendar data was changed."
        )
        return 0
    if args.google_command == "disconnect":
        result = oauth.disconnect(revoke=not args.local_only)
        if not result.authorization_retained:
            config.google_enabled = False
            save_config(config, paths)
            with database.session() as session:
                for state in session.scalars(select(IntegrationState)):
                    if state.provider in {"gmail", "calendar"}:
                        state.enabled = False
                        state.health = "disconnected"
        print(json.dumps(asdict(result), indent=2))
        return 1 if result.authorization_retained else 0
    if args.google_command == "calendar-sync":
        now = datetime.now(timezone.utc)
        provider = CalendarProvider(oauth)
        with database.session() as session:
            suggestions = ingest_calendar_metadata(
                session,
                provider,
                start=now,
                end=now + timedelta(days=args.days),
            )
        print(
            f"Imported {suggestions} read-only Calendar interview preview(s); no events were changed."
        )
        return 0
    if args.google_command == "gmail-sync":
        query = args.query.strip()
        if not query:
            raise ValueError("Gmail sync query must not be blank")
        provider = GmailProvider(oauth)
        with database.session() as session:
            imported, suggestions = ingest_gmail_metadata(
                session, provider, query=query, limit=args.limit
            )
        print(
            f"Imported {imported} message metadata record(s); created {suggestions} preview suggestion(s)."
        )
        return 0
    raise ValueError("unknown Google command")


def _gmail_limit(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc
    if not 1 <= parsed <= 500:
        raise argparse.ArgumentTypeError("limit must be between 1 and 500")
    return parsed


def _calendar_days(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("days must be an integer") from exc
    if not 1 <= parsed <= 365:
        raise argparse.ArgumentTypeError("days must be between 1 and 365")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
