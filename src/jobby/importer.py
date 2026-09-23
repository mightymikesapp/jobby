"""Idempotent, conservative migration of the legacy job-search workspace."""

from __future__ import annotations

import csv
import difflib
import hashlib
import html
import io
import json
import logging
import math
import mimetypes
import os
import re
import stat as stat_module
import tempfile
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from .audit import SENSITIVE_KEY_RE, record_audit, redact_text
from .config import JobbyPaths, resolve_paths
from .db import Database
from .enums import (
    ApplicationStage,
    ApprovalState,
    ArtifactKind,
    DocumentStatus,
    ImportReviewStatus,
    JobStatus,
)
from .models import (
    Application,
    Artifact,
    Company,
    DocumentVersion,
    Evaluation,
    ImportReview,
    Job,
    LegacyMetric,
    LegacySeenIdentifier,
    ProfileFact,
    SourceObservation,
    SourceConfig,
    StageEvent,
    utc_now,
)
from .normalization import is_public_http_url


SKIP_DIRS = {
    ".git",
    ".venv",
    ".pytest_cache",
    ".hypothesis",
    "__pycache__",
    "node_modules",
    "tmp",
}

TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {"fbclid", "gclid", "gh_src", "source", "ref", "referrer"}
MAX_STRUCTURED_TEXT_BYTES = 25 * 1024 * 1024
MAX_DOCUMENT_PARSE_BYTES = 100 * 1024 * 1024
MAX_DOCX_EXPANDED_BYTES = 100 * 1024 * 1024
MAX_DOCX_MEMBERS = 10_000


@dataclass(slots=True)
class ReconciliationIssue:
    category: str
    source_path: str
    record_key: str
    message: str


@dataclass(slots=True)
class ReconciliationReport:
    workspace: str
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    finished_at: str | None = None
    imported_artifacts: int = 0
    skipped_artifacts: int = 0
    copied_artifacts: int = 0
    imported_jobs: int = 0
    imported_evaluations: int = 0
    imported_applications: int = 0
    imported_documents: int = 0
    imported_seen_identifiers: int = 0
    imported_source_configs: int = 0
    imported_profile_facts: int = 0
    conflicts: list[ReconciliationIssue] = field(default_factory=list)
    resolved: list[ReconciliationIssue] = field(default_factory=list)
    unparsed: list[ReconciliationIssue] = field(default_factory=list)
    errors: list[ReconciliationIssue] = field(default_factory=list)
    report_json: str | None = None
    report_markdown: str | None = None

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _open_regular_file(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat_module.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"source is not a regular file: {path}")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _atomic_write_text(path: Path, content: str) -> None:
    """Publish a private text file without following a pre-existing symlink."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def _safe_relative_key(value: str) -> str | None:
    if not value or "\\" in value:
        return None
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        return None
    normalized = path.as_posix()
    return normalized if normalized not in {"", "."} else None


def _normalize_loaded_data(
    value: Any,
    *,
    _depth: int = 0,
    _seen: set[int] | None = None,
    _counter: list[int] | None = None,
) -> Any:
    """Constrain YAML/JSON to bounded, acyclic JSON-compatible data."""
    if _depth > 30:
        raise ValueError("structured data exceeds maximum nesting depth")
    _seen = _seen if _seen is not None else set()
    _counter = _counter if _counter is not None else [0]
    _counter[0] += 1
    if _counter[0] > 200_000:
        raise ValueError("structured data exceeds maximum item count")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("structured data contains a non-finite number")
        return value
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        if len(value) > MAX_STRUCTURED_TEXT_BYTES:
            raise ValueError("structured data contains an oversized string")
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        identity = id(value)
        if identity in _seen:
            raise ValueError("structured data contains a recursive alias")
        _seen.add(identity)
        try:
            if isinstance(value, dict):
                result: dict[str, Any] = {}
                for key, item in value.items():
                    key_text = str(key)
                    if key_text in result:
                        raise ValueError(
                            "structured data contains keys that collide after JSON normalization"
                        )
                    result[key_text] = _normalize_loaded_data(
                        item,
                        _depth=_depth + 1,
                        _seen=_seen,
                        _counter=_counter,
                    )
                return result
            return [
                _normalize_loaded_data(
                    item,
                    _depth=_depth + 1,
                    _seen=_seen,
                    _counter=_counter,
                )
                for item in value
            ]
        finally:
            _seen.remove(identity)
    raise ValueError(f"unsupported structured-data type: {type(value).__name__}")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise TypeError("hash chunk size must be an integer")
    if chunk_size <= 0:
        raise ValueError("hash chunk size must be positive")
    descriptor = _open_regular_file(path)
    try:
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(chunk_size), b""):
                digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _looks_like_multi_role_report(value: str) -> bool:
    urls = {
        canonicalize_url(match) for match in re.findall(r"https?://[^\s)>]+", value)
    }
    urls.discard(None)
    if len(urls) >= 2:
        return True
    numbered_sections = re.findall(r"^##\s+(?:#?\d+|\d+\.)\b", value, re.MULTILINE)
    ranked_rows = re.findall(r"^\|\s*\d+\s*\|", value, re.MULTILINE)
    return len(numbered_sections) >= 2 or len(ranked_rows) >= 2


def canonicalize_url(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip().strip("<>")
    if not is_public_http_url(value):
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or not parts.netloc:
        return value
    query = [
        (key, item)
        for key, item in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() not in TRACKING_QUERY_KEYS
        and not key.casefold().startswith(TRACKING_QUERY_PREFIXES)
    ]
    hostname = (parts.hostname or "").casefold()
    try:
        port = parts.port
    except ValueError:
        port = None
    if (
        port
        and not (parts.scheme.casefold() == "https" and port == 443)
        and not (parts.scheme.casefold() == "http" and port == 80)
    ):
        hostname = f"{hostname}:{port}"
    path = re.sub(r"/{2,}", "/", parts.path).rstrip("/") or "/"
    scheme = (
        "https"
        if parts.scheme.casefold() in {"http", "https"}
        else parts.scheme.casefold()
    )
    return urlunsplit((scheme, hostname, path, urlencode(query), ""))


def artifact_kind(path: Path) -> ArtifactKind:
    name = path.name.casefold()
    parts = {part.casefold() for part in path.parts}
    if "jds" in parts or "job description" in name:
        return ArtifactKind.JOB_DESCRIPTION
    if name.startswith("job_monitor_state") or name == "scan-history.tsv":
        return ArtifactKind.SOURCE_STATE
    if "tracker" in name or name == "pipeline.md":
        return ArtifactKind.TRACKER
    if "report" in parts or name.startswith("new_jobs_") or "report" in name:
        return ArtifactKind.SOURCE_REPORT
    if "resume" in name or name.endswith("cv.md") or name.endswith("cv.docx"):
        return ArtifactKind.RESUME
    if "cover letter" in name or "cover_letter" in name or "cover-letter" in name:
        return ArtifactKind.COVER_LETTER
    if "interview" in parts or "interview" in name:
        return ArtifactKind.INTERVIEW_NOTE
    if "follow-up" in name or "follow_up" in name:
        return ArtifactKind.FOLLOW_UP
    if "application prep" in name or "application-prep" in name:
        return ArtifactKind.APPLICATION_PREP
    if "transcript" in name:
        return ArtifactKind.TRANSCRIPT
    if "hawaii-ag-applications" in parts:
        return ArtifactKind.FORM
    if name in {"profile.yml", "_profile.md"}:
        return ArtifactKind.PROFILE
    if name == "portals.yml":
        return ArtifactKind.CONFIGURATION
    if "output" in parts:
        return ArtifactKind.GENERATED_EXPORT
    if path.suffix.casefold() in {".md", ".docx", ".pdf"}:
        return ArtifactKind.WRITING_SAMPLE
    return ArtifactKind.OTHER


def is_legacy_artifact(relative: Path) -> bool:
    if any(part in SKIP_DIRS for part in relative.parts):
        return False
    if not relative.parts:
        return False
    top = relative.parts[0].casefold()
    name = relative.name.casefold()
    if top in {"reports", "jds", "output", "interview-prep", "hawaii-ag-applications"}:
        # ``relative`` is deliberately detached from its workspace root. The
        # discovery caller already proved the absolute path is a file; checking
        # this relative path here would accidentally resolve against process CWD.
        return True
    if top == "data" and name in {"pipeline.md", "scan-history.tsv"}:
        return True
    if relative.as_posix().casefold() in {"config/profile.yml", "modes/_profile.md"}:
        return True
    if name in {"portals.yml", "job_monitor_state.json"} or name.startswith(
        "job_monitor_state.json.bak"
    ):
        return True
    if name.startswith("new_jobs_") and name.endswith(".md"):
        return True
    if "job search tracker" in name:
        return True
    if (
        "resume" in name
        or "cover letter" in name
        or "cover_letter" in name
        or "cover-letter" in name
    ):
        return relative.suffix.casefold() in {".md", ".docx", ".pdf", ".html"}
    if len(relative.parts) == 1 and relative.suffix.casefold() in {
        ".md",
        ".docx",
        ".pdf",
    }:
        system_files = {
            "readme.md",
            "claude.md",
            "data_contract.md",
        }
        return name not in system_files
    return False


def discover_legacy_artifacts(workspace: Path) -> list[Path]:
    workspace = workspace.resolve()
    artifacts: list[Path] = []
    for path in workspace.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(workspace)
        except (OSError, ValueError):
            continue
        relative = path.relative_to(workspace)
        if is_legacy_artifact(relative):
            artifacts.append(path)
    return sorted(
        artifacts, key=lambda item: item.relative_to(workspace).as_posix().casefold()
    )


class LegacyImporter:
    def __init__(
        self,
        database: Database,
        *,
        paths: JobbyPaths | None = None,
        copy_sources: bool = True,
    ):
        self.database = database
        self.paths = (paths or database.paths or resolve_paths()).ensure()
        self.copy_sources = copy_sources
        self.report: ReconciliationReport
        self.workspace: Path
        self._artifact_by_relative: dict[str, Artifact] = {}
        self._verified_read_paths: dict[str, Path] = {}

    def run(self, workspace: Path | str) -> ReconciliationReport:
        self.workspace = Path(workspace).expanduser().resolve()
        if not self.workspace.is_dir():
            raise FileNotFoundError(f"workspace does not exist: {self.workspace}")
        self.report = ReconciliationReport(workspace=str(self.workspace))
        self._artifact_by_relative.clear()
        self._verified_read_paths.clear()
        self.database.initialize()
        created_copies: list[Path] = []
        try:
            with self.database.session() as session:
                for source in discover_legacy_artifacts(self.workspace):
                    artifact, created, copied = self._register_artifact(session, source)
                    self._artifact_by_relative[
                        source.relative_to(self.workspace).as_posix()
                    ] = artifact
                    if created:
                        self.report.imported_artifacts += 1
                    else:
                        self.report.skipped_artifacts += 1
                    if copied:
                        self.report.copied_artifacts += 1
                        created_copies.append(Path(artifact.stored_path or ""))
                self._reconcile_state_files(session)
                self._parse_portals(session)
                self._parse_profile(session)
                self._parse_pipeline(session)
                self._parse_scan_reports(session)
                self._parse_master_trackers(session)
                self._parse_evaluation_reports(session)
                self._parse_scan_history(session)
                self._import_documents(session)
                self._link_job_descriptions(session)
                record_audit(
                    session,
                    action="workspace.imported",
                    entity_type="workspace",
                    entity_id=sha256_text(str(self.workspace))[:36],
                    actor="importer",
                    after={
                        "workspace": str(self.workspace),
                        "artifacts": self.report.imported_artifacts,
                        "jobs": self.report.imported_jobs,
                        "reviews": len(self.report.unparsed),
                    },
                )
        except Exception:
            for path in created_copies:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise
        self.report.finish()
        self._write_report()
        return self.report

    def _register_artifact(
        self, session: Session, source: Path
    ) -> tuple[Artifact, bool, bool]:
        relative = source.relative_to(self.workspace).as_posix()
        digest, source_stat, stored_path, copied = self._capture_source(source)
        existing = session.scalar(
            select(Artifact).where(
                Artifact.workspace_root == str(self.workspace),
                Artifact.source_path == relative,
                Artifact.content_hash == digest,
            )
        )
        if existing is not None:
            if stored_path and existing.stored_path != stored_path:
                if existing.stored_path:
                    if copied:
                        Path(stored_path).unlink(missing_ok=True)
                    if self._valid_existing_snapshot(existing, digest):
                        return existing, False, False
                    raise RuntimeError(
                        f"immutable artifact storage mismatch for {relative}"
                    )
                self._backfill_snapshot_path(
                    session,
                    existing,
                    stored_path=stored_path,
                    digest=digest,
                    relative=relative,
                )
                return existing, False, copied
            return existing, False, False
        artifact = Artifact(
            kind=artifact_kind(source.relative_to(self.workspace)),
            workspace_root=str(self.workspace),
            source_path=relative,
            stored_path=stored_path,
            content_hash=digest,
            size_bytes=source_stat.st_size,
            mime_type=mimetypes.guess_type(source.name)[0],
            source_mtime_ns=source_stat.st_mtime_ns,
            source_immutable=True,
            metadata_json={"original_absolute_path": str(source)},
        )
        session.add(artifact)
        session.flush()
        return artifact, True, copied

    def _backfill_snapshot_path(
        self,
        session: Session,
        artifact: Artifact,
        *,
        stored_path: str,
        digest: str,
        relative: str,
    ) -> None:
        """Repair a legacy pathless registration without weakening immutability.

        Early ``--no-copy`` imports recorded an immutable source identity but no
        managed snapshot. This narrowly backfills the previously-null locator
        after the content-addressed copy has been verified. SQLite DDL is
        transactional, so other connections never observe the protection trigger
        as absent and any failure restores it with the rest of the transaction.
        """

        if (
            not artifact.source_immutable
            or artifact.stored_path is not None
            or artifact.content_hash != digest
            or artifact.workspace_root != str(self.workspace)
            or artifact.source_path != relative
        ):
            raise RuntimeError(
                f"artifact is not eligible for snapshot repair: {relative}"
            )
        candidate = Path(stored_path)
        managed_root = (self.paths.artifacts_dir / "imported").resolve()
        if not candidate.is_absolute() or candidate.is_symlink():
            raise RuntimeError(f"unsafe snapshot repair path for {relative}")
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(managed_root)
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"unsafe snapshot repair path for {relative}") from exc
        if not resolved.is_file() or sha256_file(resolved) != digest:
            raise RuntimeError(f"snapshot repair hash mismatch for {relative}")

        connection = session.connection()
        trigger_row = connection.exec_driver_sql(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'trigger' AND name = 'artifacts_source_immutable_update'"
        ).one_or_none()
        if trigger_row is None or not isinstance(trigger_row[0], str):
            raise RuntimeError("artifact immutability trigger is unavailable")
        trigger_sql = trigger_row[0]
        connection.exec_driver_sql("DROP TRIGGER artifacts_source_immutable_update")
        try:
            result = connection.exec_driver_sql(
                "UPDATE artifacts SET stored_path = ? "
                "WHERE id = ? AND source_immutable = 1 AND stored_path IS NULL "
                "AND content_hash = ? AND workspace_root = ? AND source_path = ?",
                (
                    str(resolved),
                    artifact.id,
                    digest,
                    str(self.workspace),
                    relative,
                ),
            )
            if result.rowcount != 1:
                raise RuntimeError(f"snapshot repair raced for {relative}")
        finally:
            connection.exec_driver_sql(trigger_sql)
        session.expire(artifact)
        record_audit(
            session,
            action="artifact.snapshot_backfilled",
            entity_type="artifact",
            entity_id=artifact.id,
            actor="importer",
            after={"stored_path": str(resolved), "content_hash": digest},
        )

    def _valid_existing_snapshot(self, artifact: Artifact, digest: str) -> bool:
        """Accept legacy snapshot names only inside the managed content store."""
        if not artifact.stored_path or artifact.content_hash != digest:
            return False
        candidate = Path(artifact.stored_path)
        managed_root = (self.paths.artifacts_dir / "imported").resolve()
        if not candidate.is_absolute() or candidate.is_symlink():
            return False
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(managed_root)
            return sha256_file(resolved) == digest
        except (OSError, ValueError):
            return False

    def _capture_source(
        self, source: Path
    ) -> tuple[str, os.stat_result, str | None, bool]:
        descriptor = _open_regular_file(source)
        temporary: Path | None = None
        output_handle = None
        digest = hashlib.sha256()
        try:
            try:
                before = os.fstat(descriptor)
                if self.copy_sources:
                    staging = self.paths.artifacts_dir / "imported" / ".staging"
                    staging.mkdir(parents=True, exist_ok=True)
                    try:
                        staging.chmod(0o700)
                    except OSError:
                        pass
                    output_descriptor, temporary_name = tempfile.mkstemp(
                        prefix="source-", suffix=".tmp", dir=staging
                    )
                    temporary = Path(temporary_name)
                    output_handle = os.fdopen(output_descriptor, "wb")
                with os.fdopen(descriptor, "rb", closefd=False) as input_handle:
                    for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                        if output_handle is not None:
                            output_handle.write(chunk)
                after = os.fstat(descriptor)
                identity_before = (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                )
                identity_after = (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                )
                if identity_before != identity_after:
                    raise RuntimeError(f"source changed while being imported: {source}")
                if output_handle is not None:
                    output_handle.flush()
                    os.fsync(output_handle.fileno())
                    output_handle.close()
                    output_handle = None
            finally:
                if output_handle is not None:
                    output_handle.close()
                os.close(descriptor)
        except Exception:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise

        digest_text = digest.hexdigest()
        if not self.copy_sources:
            return digest_text, before, None, False
        if temporary is None:
            raise RuntimeError("source-copy staging file was not created")
        suffix = source.suffix.casefold()
        if not re.fullmatch(r"\.[a-z0-9]{1,10}", suffix):
            suffix = ".bin"
        target_dir = self.paths.artifacts_dir / "imported" / digest_text[:2]
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            target_dir.chmod(0o700)
        except OSError:
            pass
        target = target_dir / f"{digest_text}{suffix}"
        copied = False
        try:
            try:
                try:
                    os.link(temporary, target)
                    copied = True
                except FileExistsError:
                    if target.is_symlink() or sha256_file(target) != digest_text:
                        raise RuntimeError(
                            f"content-addressed artifact collision at {target}"
                        )
                target.chmod(0o400)
            except Exception:
                if copied:
                    target.unlink(missing_ok=True)
                raise
        finally:
            temporary.unlink(missing_ok=True)
        return digest_text, before, str(target), copied

    def _artifact_read_path(self, relative: str) -> Path | None:
        key = _safe_relative_key(relative)
        if key is None:
            return None
        artifact = self._artifact_by_relative.get(key)
        if artifact is None:
            return None
        cached = self._verified_read_paths.get(artifact.id)
        if cached is not None:
            return cached
        if artifact.stored_path:
            candidate = Path(artifact.stored_path)
            root = (self.paths.artifacts_dir / "imported").resolve()
        else:
            candidate = self.workspace / key
            root = self.workspace
        if candidate.is_symlink():
            return None
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, ValueError):
            return None
        try:
            if sha256_file(resolved) != artifact.content_hash:
                return None
        except OSError:
            return None
        if artifact.stored_path:
            self._verified_read_paths[artifact.id] = resolved
        return resolved

    def _read_text(self, relative: str) -> str | None:
        path = self._artifact_read_path(relative)
        if path is None:
            self.report.errors.append(
                ReconciliationIssue(
                    "unsafe_path",
                    relative,
                    relative,
                    "Source is unregistered, changed, or outside the imported workspace.",
                )
            )
            return None
        try:
            data = path.read_bytes()
            if len(data) > MAX_STRUCTURED_TEXT_BYTES:
                raise ValueError(
                    f"structured text exceeds {MAX_STRUCTURED_TEXT_BYTES} bytes"
                )
            return data.decode("utf-8")
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            self.report.errors.append(
                ReconciliationIssue("read_error", relative, relative, str(exc))
            )
            return None

    def _import_key(self, relative: str, record_key: str) -> str:
        return sha256_text(f"{self.workspace}\0{relative}\0{record_key}")

    def _company(self, session: Session, name: str) -> Company:
        name = re.sub(r"\s+", " ", name).strip(" -*_") or "Unknown company"
        normalized = normalize_text(name)
        company = session.scalar(
            select(Company).where(Company.normalized_name == normalized)
        )
        if company is None:
            company = Company(name=name, normalized_name=normalized)
            session.add(company)
            session.flush()
        return company

    def _job(
        self,
        session: Session,
        *,
        company_name: str,
        title: str,
        url: str | None = None,
        source: str | None = None,
        source_id: str | None = None,
        deadline: date | None = None,
        observed_at: datetime | None = None,
    ) -> tuple[Job, bool]:
        company = self._company(session, company_name)
        normalized_title = normalize_text(title)
        job = None
        strong_identity = bool(source and source_id)
        if strong_identity:
            job = session.scalar(
                select(Job).where(
                    Job.source_primary == source, Job.source_id == source_id
                )
            )
        launch_url = str(url).strip() if url and is_public_http_url(url) else None
        canonical_url = canonicalize_url(launch_url)
        if job is None and canonical_url:
            job = session.scalar(select(Job).where(Job.canonical_url == canonical_url))
        if job is None and not strong_identity:
            job = session.scalar(
                select(Job).where(
                    Job.company_id == company.id,
                    Job.normalized_title == normalized_title,
                )
            )
        created = job is None
        if job is None:
            job = Job(
                company_id=company.id,
                title=re.sub(r"\s+", " ", title).strip(),
                normalized_title=normalized_title,
                canonical_url=canonical_url,
                launch_url=launch_url,
                comparison_url=canonical_url,
                source_primary=source,
                source_id=source_id,
                status=JobStatus.DISCOVERED,
                deadline=deadline,
                discovered_at=observed_at or utc_now(),
                last_seen_at=observed_at,
            )
            session.add(job)
            session.flush()
            self.report.imported_jobs += 1
        else:
            if not job.canonical_url and launch_url:
                job.canonical_url = canonical_url
            if not job.comparison_url and launch_url:
                job.comparison_url = canonical_url
            if not job.launch_url and launch_url:
                job.launch_url = launch_url
            if not job.deadline and deadline:
                job.deadline = deadline
            if observed_at:
                if job.discovered_at is None or observed_at < _as_aware(
                    job.discovered_at
                ):
                    job.discovered_at = observed_at
                if job.last_seen_at is None or observed_at > _as_aware(
                    job.last_seen_at
                ):
                    job.last_seen_at = observed_at
        return job, created

    def _evaluation(
        self, session: Session, job: Job, score: float, relative: str, key: str
    ) -> None:
        score = min(max(float(score), 1.0), 5.0)
        import_key = self._import_key(relative, f"evaluation:{key}:{score:.8g}")
        existing = session.scalar(
            select(Evaluation).where(Evaluation.import_key == import_key)
        )
        if existing is not None:
            if not existing.is_current:
                for current in session.scalars(
                    select(Evaluation).where(
                        Evaluation.job_id == job.id,
                        Evaluation.is_current.is_(True),
                    )
                ):
                    current.is_current = False
                session.flush()
                existing.is_current = True
                job.latest_score = existing.score
            return
        for current in session.scalars(
            select(Evaluation).where(
                Evaluation.job_id == job.id, Evaluation.is_current.is_(True)
            )
        ):
            current.is_current = False
        session.flush()
        evaluation = Evaluation(
            job_id=job.id,
            score=score,
            components={"legacy_score": score},
            gates=[],
            evidence=[],
            warnings=[
                "Imported legacy score; deterministic evidence review recommended."
            ],
            confidence=0.5,
            explanation="Imported from an immutable legacy source artifact.",
            is_current=True,
            import_key=import_key,
        )
        session.add(evaluation)
        job.latest_score = evaluation.score
        self.report.imported_evaluations += 1

    def _legacy_metric(
        self,
        session: Session,
        job: Job,
        *,
        relative: str,
        record_key: str,
        metric_name: str,
        value: float,
        scale_max: float,
        raw_text: str,
    ) -> None:
        import_key = self._import_key(
            relative,
            f"legacy_metric:{record_key}:{metric_name}:{sha256_text(raw_text)}",
        )
        if session.scalar(
            select(LegacyMetric.id).where(LegacyMetric.import_key == import_key)
        ):
            return
        session.add(
            LegacyMetric(
                import_key=import_key,
                job_id=job.id,
                rubric="legacy_tracker",
                metric_name=metric_name,
                value=value,
                scale_min=1,
                scale_max=scale_max,
                source_path=relative,
                raw_text=raw_text,
            )
        )

    def _application(
        self,
        session: Session,
        job: Job,
        stage: ApplicationStage,
        relative: str,
        key: str,
        *,
        status_text: str,
    ) -> None:
        import_key = self._import_key(relative, f"application:{key}")
        application = session.scalar(
            select(Application).where(Application.import_key == import_key)
        )
        imported_at = utc_now()
        submitted_at = status_stage_datetime(status_text, ApplicationStage.APPLIED)
        target_at = status_stage_datetime(status_text, stage)
        if application is not None:
            current_stage = ApplicationStage(application.current_stage)
            application.notes = f"Imported status: {status_text}"
            if submitted_at and (
                application.submitted_at is None
                or _as_aware(submitted_at) < _as_aware(application.submitted_at)
            ):
                application.submitted_at = submitted_at
            if current_stage == stage:
                return

            from .pipeline import ALLOWED_TRANSITIONS

            route: list[ApplicationStage]
            if stage in ALLOWED_TRANSITIONS[current_stage]:
                route = [stage]
            elif (
                current_stage == ApplicationStage.PLANNED
                and ApplicationStage.APPLIED in _legacy_stage_path(stage, status_text)
                and stage in ALLOWED_TRANSITIONS[ApplicationStage.APPLIED]
            ):
                route = [ApplicationStage.APPLIED, stage]
            else:
                self._queue_review(
                    session,
                    relative,
                    f"application_transition:{key}:{stage.value}",
                    f"Imported application cannot move from {current_stage.value} to {stage.value} without review.",
                    status_text,
                    proposed={"application_id": application.id, "stage": stage.value},
                )
                return

            previous = current_stage
            latest_occurred_at = session.scalar(
                select(StageEvent.occurred_at)
                .where(StageEvent.application_id == application.id)
                .order_by(StageEvent.occurred_at.desc())
                .limit(1)
            )
            for item in route:
                occurred_at = _imported_stage_time(
                    item,
                    status_text=status_text,
                    submitted_at=submitted_at,
                    target_stage=stage,
                    target_at=target_at,
                    fallback=imported_at,
                )
                if latest_occurred_at is not None and occurred_at <= _as_aware(
                    latest_occurred_at
                ):
                    occurred_at = _as_aware(latest_occurred_at) + timedelta(
                        microseconds=1
                    )
                session.add(
                    StageEvent(
                        application_id=application.id,
                        from_stage=previous,
                        to_stage=item,
                        occurred_at=occurred_at,
                        source="legacy_import",
                        actor="importer",
                        reason=status_text,
                    )
                )
                if (
                    item == ApplicationStage.APPLIED
                    and application.submitted_at is None
                ):
                    application.submitted_at = occurred_at
                previous = item
                latest_occurred_at = occurred_at
            application.current_stage = stage
            return

        path = _legacy_stage_path(stage, status_text)
        application = Application(
            job_id=job.id,
            current_stage=stage,
            notes=f"Imported status: {status_text}",
            import_key=import_key,
        )
        session.add(application)
        session.flush()
        previous: ApplicationStage | None = None
        last_occurred_at: datetime | None = None
        for item in path:
            occurred_at = _imported_stage_time(
                item,
                status_text=status_text,
                submitted_at=submitted_at,
                target_stage=stage,
                target_at=target_at,
                fallback=imported_at,
            )
            if last_occurred_at is not None and occurred_at <= last_occurred_at:
                occurred_at = last_occurred_at + timedelta(microseconds=1)
            session.add(
                StageEvent(
                    application_id=application.id,
                    from_stage=previous,
                    to_stage=item,
                    occurred_at=occurred_at,
                    source="legacy_import",
                    actor="importer",
                    reason=status_text,
                )
            )
            previous = item
            last_occurred_at = occurred_at
            if item == ApplicationStage.APPLIED:
                application.submitted_at = submitted_at or occurred_at
        self.report.imported_applications += 1

    def _queue_review(
        self,
        session: Session,
        relative: str,
        record_key: str,
        reason: str,
        raw: str | None = None,
        proposed: dict[str, Any] | None = None,
    ) -> None:
        existing = session.scalar(
            select(ImportReview).where(
                ImportReview.workspace_root == str(self.workspace),
                ImportReview.source_path == relative,
                ImportReview.record_key == record_key,
            )
        )
        if existing is None:
            existing = ImportReview(
                workspace_root=str(self.workspace),
                source_path=relative,
                record_key=record_key,
                reason=reason,
                raw_excerpt=redact_text((raw or "")[:1000]) or None,
                proposed_json=proposed or {},
                status=ImportReviewStatus.PENDING,
            )
            session.add(existing)
        elif existing.reason != reason:
            existing.reason = reason
            existing.raw_excerpt = redact_text((raw or "")[:1000]) or None
            existing.proposed_json = proposed or {}
            existing.status = ImportReviewStatus.PENDING
        if existing.status == ImportReviewStatus.PENDING:
            self.report.unparsed.append(
                ReconciliationIssue("review", relative, record_key, reason)
            )

    def _resolve_review(
        self,
        session: Session,
        relative: str,
        record_key: str,
        reason: str,
        *,
        status: ImportReviewStatus = ImportReviewStatus.RESOLVED,
        proposed: dict[str, Any] | None = None,
    ) -> None:
        if status not in {
            ImportReviewStatus.RESOLVED,
            ImportReviewStatus.DISMISSED,
        }:
            raise ValueError("automatic import-review resolution must be final")
        review = session.scalar(
            select(ImportReview).where(
                ImportReview.workspace_root == str(self.workspace),
                ImportReview.source_path == relative,
                ImportReview.record_key == record_key,
            )
        )
        before = None if review is None else review.status
        if review is None:
            review = ImportReview(
                workspace_root=str(self.workspace),
                source_path=relative,
                record_key=record_key,
                reason=reason,
                proposed_json=proposed or {},
                status=status,
            )
            session.add(review)
            session.flush()
        else:
            review.reason = reason
            review.proposed_json = proposed or dict(review.proposed_json or {})
            review.status = status
        if before != status:
            record_audit(
                session,
                action="import_review.auto_resolved",
                entity_type="import_review",
                entity_id=review.id,
                actor="importer",
                before={"status": before} if before is not None else None,
                after={"status": status, "record_key": record_key},
                detail=reason,
            )
        self.report.resolved.append(
            ReconciliationIssue(status.value, relative, record_key, reason)
        )

    def _reconcile_state_files(self, session: Session) -> None:
        state_paths = sorted(
            relative
            for relative in self._artifact_by_relative
            if relative == "job_monitor_state.json"
            or relative.startswith("job_monitor_state.json.bak")
        )
        parsed_sets: dict[str, set[str]] = {}
        existing_uids = set(
            session.scalars(
                select(LegacySeenIdentifier.source_uid).where(
                    LegacySeenIdentifier.workspace_root == str(self.workspace)
                )
            )
        )
        for relative in state_paths:
            try:
                text = self._read_text(relative)
                if text is None:
                    continue
                data = _normalize_loaded_data(json.loads(text))
                if not isinstance(data, dict):
                    raise ValueError("scanner state must be an object")
                seen = data.get("seen_jobs", [])
                if not isinstance(seen, list):
                    raise ValueError("seen_jobs must be a list")
                if len(seen) > 1_000_000:
                    raise ValueError("seen_jobs exceeds the safe import limit")
                parsed_sets[relative] = {
                    str(item)
                    for item in seen
                    if isinstance(item, (str, int)) and len(str(item)) <= 700
                }
                for uid in parsed_sets[relative]:
                    if uid in existing_uids:
                        continue
                    session.add(
                        LegacySeenIdentifier(
                            workspace_root=str(self.workspace),
                            source_uid=uid,
                            legacy_last_run=str(data.get("last_run") or "") or None,
                        )
                    )
                    existing_uids.add(uid)
                    self.report.imported_seen_identifiers += 1
            except (OSError, RecursionError, ValueError, json.JSONDecodeError) as exc:
                self._queue_review(
                    session, relative, "state", f"Malformed scanner state: {exc}"
                )
        current_seen = parsed_sets.get("job_monitor_state.json", set())
        for relative, backup_seen in parsed_sets.items():
            if relative == "job_monitor_state.json" or backup_seen == current_seen:
                continue
            message = (
                f"State versions differ ({len(backup_seen)} vs {len(current_seen)} IDs); "
                "their identifiers were safely unioned without inventing job records."
            )
            self._resolve_review(
                session,
                relative,
                "seen_jobs_conflict",
                message,
                proposed={
                    "policy": "union_identifiers",
                    "backup_count": len(backup_seen),
                    "current_count": len(current_seen),
                    "union_count": len(backup_seen | current_seen),
                },
            )

    def _parse_portals(self, session: Session) -> None:
        relative = "portals.yml"
        if relative not in self._artifact_by_relative:
            return
        try:
            text = self._read_text(relative)
            if text is None:
                return
            data = _normalize_loaded_data(yaml.safe_load(text) or {})
            if not isinstance(data, dict):
                raise ValueError("portal configuration must be an object")
        except Exception as exc:
            self._queue_review(
                session,
                relative,
                "yaml",
                f"Could not parse portal configuration: {exc}",
            )
            return
        tracked_companies = data.get("tracked_companies", [])
        if not isinstance(tracked_companies, list):
            self._queue_review(
                session,
                relative,
                "tracked_companies",
                "tracked_companies must be a list",
            )
            tracked_companies = []
        for index, item in enumerate(tracked_companies):
            if (
                not isinstance(item, dict)
                or not item.get("name")
                or not item.get("careers_url")
            ):
                self._queue_review(
                    session,
                    relative,
                    f"portal:{index}",
                    "Portal is missing name or careers_url",
                )
                continue
            name = str(item["name"]).strip()
            careers_url = str(item["careers_url"]).strip()
            legacy_method = str(item.get("scan_method") or "static_http")
            provider = "static_http" if legacy_method == "playwright" else legacy_method
            url_parts = urlsplit(careers_url)
            if (
                url_parts.scheme.casefold() not in {"http", "https"}
                or not url_parts.hostname
                or url_parts.username is not None
                or url_parts.password is not None
            ):
                self._queue_review(
                    session,
                    relative,
                    f"portal:{index}",
                    "Portal careers_url must be a public HTTP(S) URL without credentials.",
                )
                continue
            import_key = self._import_key(relative, f"portal:{normalize_text(name)}")
            existing_config = session.scalar(
                select(SourceConfig).where(SourceConfig.import_key == import_key)
            )
            if existing_config is not None:
                if existing_config.provider == "playwright":
                    existing_config.provider = "static_http"
                continue
            session.add(
                SourceConfig(
                    import_key=import_key,
                    provider=provider,
                    name=name,
                    enabled=bool(item.get("enabled", True)),
                    config_json={
                        "careers_url": canonicalize_url(careers_url),
                        "notes": redact_text(str(item.get("notes", ""))),
                    },
                )
            )
            self.report.imported_source_configs += 1
        title_filter = data.get("title_filter", {})
        negative_values = (
            title_filter.get("negative", []) if isinstance(title_filter, dict) else []
        )
        negative = (
            {str(item).casefold() for item in negative_values}
            if isinstance(negative_values, list)
            else set()
        )
        if {"intern", "internship"} & negative:
            self._resolve_review(
                session,
                relative,
                "internship_filter_conflict",
                "Jobby retains paid legal internships and deterministically rejects explicitly unpaid work; the legacy negative filter remains preserved only as source provenance.",
                proposed={
                    "policy": "paid_internships_allowed",
                    "unpaid_work": "automatic_skip",
                    "source_file_modified": False,
                },
            )

    def _parse_profile(self, session: Session) -> None:
        relative = "config/profile.yml"
        artifact = self._artifact_by_relative.get(relative)
        if artifact is None:
            return
        try:
            text = self._read_text(relative)
            if text is None:
                return
            data = _normalize_loaded_data(yaml.safe_load(text) or {})
            if not isinstance(data, dict):
                raise ValueError("profile must be an object")
        except Exception as exc:
            self._queue_review(
                session, relative, "yaml", f"Could not parse profile: {exc}"
            )
            return
        for key, value in flatten_mapping(data):
            if SENSITIVE_KEY_RE.search(key):
                self._queue_review(
                    session,
                    relative,
                    f"profile_fact:{key}",
                    "Sensitive-looking profile field was omitted; store credentials in the OS keyring.",
                )
                continue
            digest = sha256_text(json.dumps(value, sort_keys=True, ensure_ascii=False))
            fact = session.scalar(
                select(ProfileFact).where(ProfileFact.fact_key == key)
            )
            if fact is None:
                fact = ProfileFact(
                    fact_key=key,
                    value_json=value,
                    approved=True,
                    approved_at=utc_now(),
                    source_artifact_id=artifact.id,
                    content_hash=digest,
                )
                session.add(fact)
                self.report.imported_profile_facts += 1
            elif fact.content_hash != digest:
                fact.value_json = value
                fact.content_hash = digest
                fact.source_artifact_id = artifact.id
                fact.approved = False
                fact.approved_at = None
                self._queue_review(
                    session,
                    relative,
                    f"profile_fact:{key}",
                    "An approved profile fact changed in the source and requires re-approval.",
                    proposed={"fact_key": key},
                )

    PIPELINE_RE = re.compile(
        r"^- \[(?P<score>\d+(?:\.\d+)?|—|-)/5\]\s+"
        r"(?P<company>.+?)\s+[—–]\s+(?P<title>.+?)\s+\|\s+"
        r"(?P<status>[^|\n]+?)(?:\s+\|\s+(?P<remainder>.*))?$",
        re.MULTILINE,
    )

    def _parse_pipeline(self, session: Session) -> None:
        relative = "data/pipeline.md"
        if relative not in self._artifact_by_relative:
            return
        text = self._read_text(relative)
        if text is None:
            return
        matched_line_numbers: set[int] = set()
        for index, match in enumerate(self.PIPELINE_RE.finditer(text)):
            matched_line_numbers.add(text.count("\n", 0, match.start()) + 1)
            company = match.group("company").strip(" *")
            title = match.group("title").strip(" *")
            status = match.group("status").strip()
            report_link = re.search(r"\((reports/[^)]+)\)", match.group(0))
            url = None
            if report_link:
                report_text = self._read_text(report_link.group(1))
                if report_text:
                    url_match = re.search(r"\*\*URL:\*\*\s+(\S+)", report_text)
                    url = url_match.group(1) if url_match else None
            job, _ = self._job(
                session,
                company_name=company,
                title=title,
                url=url,
                source="legacy_pipeline",
            )
            job.status = (
                JobStatus.SAVED
                if "pinned" in status.casefold()
                else JobStatus.EVALUATING
            )
            score_text = match.group("score")
            if score_text not in {"—", "-"}:
                self._evaluation(
                    session,
                    job,
                    float(score_text),
                    relative,
                    f"pipeline:{company}:{title}",
                )
        for line_number, line in enumerate(text.splitlines(), 1):
            if (
                line.startswith("- [")
                and "/5]" in line
                and line_number not in matched_line_numbers
            ):
                self._queue_review(
                    session,
                    relative,
                    f"line:{line_number}",
                    "Pipeline entry did not match the supported score/company/title/status structure.",
                    line,
                )

    def _parse_scan_reports(self, session: Session) -> None:
        report_paths = sorted(
            relative
            for relative in self._artifact_by_relative
            if "/" not in relative
            and Path(relative).name.startswith("new_jobs_")
            and Path(relative).suffix.casefold() == ".md"
        )
        for relative in report_paths:
            text = self._read_text(relative)
            if text is None:
                continue
            artifact = self._artifact_by_relative.get(relative)
            report_date = parse_report_date(Path(relative), text)
            observed_at = (
                datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc)
                if report_date
                else None
            )
            blocks = re.split(r"(?=^##\s+)", text, flags=re.MULTILINE)
            for index, block in enumerate(blocks):
                heading = re.match(r"^##\s+(.+?)(?:\s+⚠.*)?$", block, re.MULTILINE)
                meta = re.search(
                    r"\*\*(.+?)\*\*\s*\|\s*([A-Za-z0-9_-]+)(?:\s*\|\s*Closes:\s*([^\n]+))?",
                    block,
                )
                url = re.search(r"^https?://\S+", block, re.MULTILINE)
                if not heading:
                    continue
                if not meta or not url:
                    self._queue_review(
                        session,
                        relative,
                        f"block:{index}",
                        "Scan report entry is missing company/source/URL",
                        block,
                    )
                    continue
                deadline = parse_legacy_date(meta.group(3)) if meta.group(3) else None
                source, account, source_id = source_identity(
                    meta.group(2), url.group(0)
                )
                job, _ = self._job(
                    session,
                    company_name=meta.group(1),
                    title=heading.group(1).strip(),
                    url=url.group(0),
                    source=source,
                    source_id=f"{account}:{source_id}" if account else source_id,
                    deadline=deadline,
                    observed_at=observed_at,
                )
                observation_key = self._import_key(
                    relative,
                    f"observation:{artifact.content_hash if artifact else 'unregistered'}:{index}",
                )
                if not session.scalar(
                    select(SourceObservation.id).where(
                        SourceObservation.import_key == observation_key
                    )
                ):
                    keyword_match = re.search(r"Keywords matched:\s*(.+)", block)
                    session.add(
                        SourceObservation(
                            job_id=job.id,
                            import_key=observation_key,
                            source=source,
                            source_account=account,
                            source_job_id=source_id,
                            source_url=url.group(0),
                            title_snapshot=heading.group(1).strip(),
                            company_snapshot=meta.group(1).strip(),
                            observed_at=observed_at or utc_now(),
                            is_live=None,
                            content_hash=sha256_text(block),
                            raw_payload={
                                "artifact_id": artifact.id if artifact else None,
                                "block_ordinal": index,
                                "deadline": deadline.isoformat() if deadline else None,
                                "keywords": [
                                    item.strip()
                                    for item in keyword_match.group(1).split(",")
                                ]
                                if keyword_match
                                else [],
                            },
                        )
                    )

    TRACKER_HEADING_RE = re.compile(
        r"^###\s+[\w.]+\s+(.+?)\s+[—–]\s+(.+?)\s*$", re.MULTILINE
    )

    def _parse_master_trackers(self, session: Session) -> None:
        tracker_paths = sorted(
            relative
            for relative in self._artifact_by_relative
            if "/" not in relative
            and (
                "job search tracker" in Path(relative).name.casefold()
                or "application tracker" in Path(relative).name.casefold()
            )
            and Path(relative).suffix.casefold() == ".md"
        )
        for relative in tracker_paths:
            text = self._read_text(relative)
            if text is None:
                continue
            matches = list(self.TRACKER_HEADING_RE.finditer(text))
            for index, match in enumerate(matches):
                end = (
                    matches[index + 1].start()
                    if index + 1 < len(matches)
                    else len(text)
                )
                block = text[match.end() : end]
                fields = {
                    normalize_text(key): value.strip()
                    for key, value in re.findall(
                        r"^\|\s*\*\*(.+?)\*\*\s*\|\s*(.*?)\s*\|\s*$",
                        block,
                        re.MULTILINE,
                    )
                }
                if not fields:
                    continue
                company, title = match.group(1).strip(), match.group(2).strip()
                link_match = re.search(
                    r"\[[^]]+\]\((https?://[^)]+)\)", fields.get("link", "")
                )
                status = re.sub(r"[*✅🚨]", "", fields.get("status", "")).strip()
                job, _ = self._job(
                    session,
                    company_name=company,
                    title=title,
                    url=link_match.group(1) if link_match else None,
                    source="legacy_tracker",
                )
                for field_name, metric_name in (
                    ("skills match", "skills_match"),
                    ("application success", "application_success"),
                ):
                    raw_metric = fields.get(field_name, "")
                    score_match = re.search(r"(\d+(?:\.\d+)?)\s*/\s*(5|10)", raw_metric)
                    if score_match:
                        self._legacy_metric(
                            session,
                            job,
                            relative=relative,
                            record_key=f"tracker:{company}:{title}",
                            metric_name=metric_name,
                            value=float(score_match.group(1)),
                            scale_max=float(score_match.group(2)),
                            raw_text=raw_metric,
                        )
                stage = status_to_stage(status)
                if stage:
                    self._application(
                        session,
                        job,
                        stage,
                        relative,
                        f"tracker:{company}:{title}",
                        status_text=status,
                    )

    EVALUATION_RE = re.compile(
        r"^# Evaluation:\s*(.+?)\s+[—–]\s+(.+?)\s*$", re.MULTILINE
    )

    def _parse_evaluation_reports(self, session: Session) -> None:
        report_paths = sorted(
            relative
            for relative in self._artifact_by_relative
            if PurePosixPath(relative).parent == PurePosixPath("reports")
            and PurePosixPath(relative).suffix.casefold() == ".md"
        )
        for relative in report_paths:
            text = self._read_text(relative)
            if text is None:
                continue
            match = self.EVALUATION_RE.search(text)
            if not match:
                if _looks_like_multi_role_report(text):
                    self._resolve_review(
                        session,
                        relative,
                        "freeform_report",
                        "Multi-role analysis is intentionally preserved as a source report rather than misrepresented as one evaluation.",
                        status=ImportReviewStatus.DISMISSED,
                        proposed={
                            "policy": "preserve_source_report",
                            "single_evaluation_created": False,
                        },
                    )
                else:
                    self._queue_review(
                        session,
                        relative,
                        "freeform_report",
                        "Freeform report registered as an artifact but not parsed into a single evaluation.",
                    )
                continue
            score_match = re.search(
                r"^\*\*Score:\*\*\s*(\d+(?:\.\d+)?)\s*/\s*5", text, re.MULTILINE
            )
            url_match = re.search(r"^\*\*URL:\*\*\s*(\S+)", text, re.MULTILINE)
            job, _ = self._job(
                session,
                company_name=match.group(1),
                title=match.group(2),
                url=url_match.group(1) if url_match else None,
                source="legacy_report",
            )
            if score_match:
                self._evaluation(
                    session, job, float(score_match.group(1)), relative, "report"
                )
            artifact = self._artifact_by_relative.get(relative)
            if artifact:
                artifact.job_id = job.id

    def _parse_scan_history(self, session: Session) -> None:
        relative = "data/scan-history.tsv"
        if relative not in self._artifact_by_relative:
            return
        try:
            text = self._read_text(relative)
            if text is None:
                return
            with io.StringIO(text, newline="") as handle:
                for index, row in enumerate(csv.DictReader(handle, delimiter="\t"), 2):
                    if not any(row.values()):
                        continue
                    if not row.get("title") or not row.get("company"):
                        self._queue_review(
                            session,
                            relative,
                            f"row:{index}",
                            "Scan history row lacks title or company",
                            json.dumps(row),
                        )
                        continue
                    self._job(
                        session,
                        company_name=row["company"],
                        title=row["title"],
                        url=row.get("url"),
                        source=row.get("portal") or "legacy_scan_history",
                    )
        except (OSError, csv.Error) as exc:
            self._queue_review(
                session, relative, "tsv", f"Could not parse scan history: {exc}"
            )

    def _import_documents(self, session: Session) -> None:
        canonical_resumes = list(
            session.scalars(
                select(DocumentVersion)
                .where(
                    DocumentVersion.kind == ArtifactKind.RESUME,
                    DocumentVersion.is_canonical.is_(True),
                )
                .order_by(
                    DocumentVersion.version,
                    DocumentVersion.created_at,
                    DocumentVersion.id,
                )
            )
        )
        canonical_resume = canonical_resumes[0] if canonical_resumes else None
        for duplicate in canonical_resumes[1:]:
            duplicate.is_canonical = False
            if duplicate.approval_state == ApprovalState.APPROVED:
                duplicate.approval_state = ApprovalState.LEGACY_UNKNOWN

        document_artifacts = sorted(
            self._artifact_by_relative.items(),
            key=lambda item: document_source_sort_key(item[0]),
        )
        for relative, artifact in document_artifacts:
            if artifact.kind not in {ArtifactKind.RESUME, ArtifactKind.COVER_LETTER}:
                continue
            source = Path(relative)
            read_path = self._artifact_read_path(relative)
            if read_path is None:
                self._queue_review(
                    session,
                    relative,
                    "document_source",
                    "Document source changed or is outside managed artifact storage.",
                )
                continue
            if artifact.size_bytes > MAX_DOCUMENT_PARSE_BYTES:
                self._queue_review(
                    session,
                    relative,
                    "document_size",
                    f"Document exceeds the {MAX_DOCUMENT_PARSE_BYTES}-byte parsing limit.",
                )
                continue
            content = extract_document_text(read_path)
            if not content.strip():
                continue
            digest = sha256_text(content)
            logical_name = normalize_document_name(source.stem, artifact.kind)
            canonical_master = is_canonical_master_resume(source, artifact.kind)
            candidates = list(
                session.scalars(
                    select(DocumentVersion).where(DocumentVersion.kind == artifact.kind)
                )
            )
            canonical_has_same_source = bool(
                canonical_resume
                and any(
                    item.get("source_path") == relative
                    for item in (canonical_resume.provenance or [])
                )
            )
            existing = (
                canonical_resume
                if canonical_master
                and canonical_resume is not None
                and not canonical_has_same_source
                else None
            )
            normalized_content = normalized_document_text(content)
            for candidate in candidates if existing is None else ():
                if (
                    normalize_document_name(candidate.name, candidate.kind)
                    != logical_name
                ):
                    continue
                provenance = list(candidate.provenance or [])
                same_source_version = any(
                    item.get("source_path") == relative for item in provenance
                )
                # A changed file at the same path is a new source version even
                # if the edit is small enough to remain semantically similar.
                if same_source_version and candidate.content_hash != digest:
                    continue
                candidate_text = normalized_document_text(candidate.content_markdown)
                similarity = semantic_document_similarity(
                    normalized_content, candidate_text
                )
                threshold = 0.96 if artifact.kind == ArtifactKind.RESUME else 0.985
                if candidate.content_hash == digest or similarity >= threshold:
                    existing = candidate
                    break
            if existing is None:
                logical_versions = [
                    item
                    for item in candidates
                    if normalize_document_name(item.name, item.kind) == logical_name
                ]
                existing = DocumentVersion(
                    kind=artifact.kind,
                    name=source.stem,
                    version=len(logical_versions) + 1,
                    parent_id=logical_versions[-1].id if logical_versions else None,
                    content_markdown=content,
                    content_hash=digest,
                    status=DocumentStatus.SOURCE,
                    approval_state=(
                        ApprovalState.APPROVED
                        if canonical_master and canonical_resume is None
                        else ApprovalState.LEGACY_UNKNOWN
                    ),
                    is_canonical=canonical_master and canonical_resume is None,
                    provenance=[
                        {
                            "artifact_id": artifact.id,
                            "source_path": relative,
                            "content_hash": artifact.content_hash,
                            "format": source.suffix.casefold().lstrip("."),
                        }
                    ],
                    validation={"source_import": True},
                )
                session.add(existing)
                session.flush()
                if existing.is_canonical:
                    canonical_resume = existing
                self.report.imported_documents += 1
            else:
                provenance = list(existing.provenance or [])
                if existing.approval_state == ApprovalState.APPROVED:
                    # Approved document provenance is frozen. Additional
                    # equivalent source formats remain independently preserved
                    # by their Artifact rows and link to this version below.
                    pass
                elif not any(
                    item.get("artifact_id") == artifact.id for item in provenance
                ):
                    provenance.append(
                        {
                            "artifact_id": artifact.id,
                            "source_path": relative,
                            "content_hash": artifact.content_hash,
                            "format": source.suffix.casefold().lstrip("."),
                        }
                    )
                    existing.provenance = provenance
            artifact.document_version_id = existing.id

    def _existing_job_for_description(
        self, session: Session, content: str
    ) -> tuple[Job | None, str | None]:
        url_match = re.search(r"^\*\*URL:\*\*\s*(\S+)", content, re.MULTILINE)
        if url_match:
            url = canonicalize_url(url_match.group(1))
            if url:
                matches = list(
                    session.scalars(
                        select(Job).where(Job.canonical_url == url).limit(2)
                    )
                )
                if len(matches) == 1:
                    return matches[0], "canonical_url"

        heading_match = re.search(r"^#\s+(.+?)\s*$", content, re.MULTILINE)
        if not heading_match:
            return None, None
        heading = re.sub(r"^JD:\s*", "", heading_match.group(1), flags=re.I)
        parts = [item.strip() for item in re.split(r"\s+[—–]\s+", heading)]
        if len(parts) != 2:
            return None, None
        matches: dict[str, Job] = {}
        for company_name, title in (parts, tuple(reversed(parts))):
            company_key = normalize_text(company_name)
            title_key = normalize_text(title)
            for job in session.scalars(
                select(Job)
                .join(Company, Job.company_id == Company.id)
                .where(
                    Company.normalized_name == company_key,
                    Job.normalized_title == title_key,
                )
                .limit(2)
            ):
                matches[job.id] = job
        if len(matches) == 1:
            return next(iter(matches.values())), "normalized_company_title"
        return None, None

    def _link_description(
        self,
        session: Session,
        *,
        relative: str,
        artifact: Artifact,
        job: Job,
        content: str,
        method: str,
    ) -> None:
        content_hash = sha256_text(content)
        artifact.job_id = job.id
        if job.description and job.description_hash not in {None, content_hash}:
            self._queue_review(
                session,
                relative,
                "job_description_conflict",
                "The exact job already has a different description; the source artifact was linked without overwriting current text.",
                proposed={"job_id": job.id, "match_method": method},
            )
            return
        job.description = content
        job.description_hash = content_hash
        self._resolve_review(
            session,
            relative,
            "job_description_link",
            f"Saved job description linked deterministically by {method.replace('_', ' ')}.",
            proposed={"job_id": job.id, "match_method": method},
        )
        record_audit(
            session,
            action="import.job_description_linked",
            entity_type="artifact",
            entity_id=artifact.id,
            actor="importer",
            after={"job_id": job.id, "match_method": method},
        )

    def _link_job_descriptions(self, session: Session) -> None:
        for relative, artifact in self._artifact_by_relative.items():
            if artifact.kind != ArtifactKind.JOB_DESCRIPTION or artifact.job_id:
                continue
            read_path = self._artifact_read_path(relative)
            if read_path is None:
                self._queue_review(
                    session,
                    relative,
                    "job_description_source",
                    "Saved job description changed or is outside managed artifact storage.",
                )
                continue
            if artifact.size_bytes > MAX_DOCUMENT_PARSE_BYTES:
                self._queue_review(
                    session,
                    relative,
                    "job_description_size",
                    f"Saved job description exceeds the {MAX_DOCUMENT_PARSE_BYTES}-byte parsing limit.",
                )
                continue
            content = extract_document_text(read_path)
            existing_job, match_method = self._existing_job_for_description(
                session, content
            )
            if existing_job is not None and match_method is not None:
                self._link_description(
                    session,
                    relative=relative,
                    artifact=artifact,
                    job=existing_job,
                    content=content,
                    method=match_method,
                )
                continue
            title_match = re.search(r"^#\s+(.+?)\s*$", content, re.MULTILINE)
            company_match = re.search(
                r"^\*\*(?:Company|Organization):\*\*\s*(.+?)\s*$", content, re.MULTILINE
            )
            url_match = re.search(r"^\*\*URL:\*\*\s*(\S+)", content, re.MULTILINE)
            if title_match and company_match:
                job, _ = self._job(
                    session,
                    company_name=company_match.group(1),
                    title=title_match.group(1),
                    url=url_match.group(1) if url_match else None,
                    source="legacy_jd",
                )
                self._link_description(
                    session,
                    relative=relative,
                    artifact=artifact,
                    job=job,
                    content=content,
                    method="explicit_company_title",
                )
            else:
                self._queue_review(
                    session,
                    relative,
                    "job_description_link",
                    "Saved job description could not be linked confidently to a company and role.",
                )

    def _write_report(self) -> None:
        directory = self.paths.data_dir / "imports"
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        json_path = directory / f"reconciliation-{stamp}.json"
        markdown_path = directory / f"reconciliation-{stamp}.md"
        self.report.report_json = str(json_path)
        self.report.report_markdown = str(markdown_path)
        _atomic_write_text(
            json_path,
            json.dumps(self.report.to_dict(), indent=2, ensure_ascii=False),
        )
        lines = [
            "# Jobby Import Reconciliation",
            "",
            f"Workspace: `{self.workspace}`",
            "",
            f"- Imported artifacts: {self.report.imported_artifacts}",
            f"- Skipped unchanged artifacts: {self.report.skipped_artifacts}",
            f"- Imported jobs: {self.report.imported_jobs}",
            f"- Imported evaluations: {self.report.imported_evaluations}",
            f"- Imported applications: {self.report.imported_applications}",
            f"- Imported documents: {self.report.imported_documents}",
            f"- Imported legacy seen IDs: {self.report.imported_seen_identifiers}",
            f"- Imported approved profile facts: {self.report.imported_profile_facts}",
            f"- Conflicts: {len(self.report.conflicts)}",
            f"- Automatically resolved or dismissed: {len(self.report.resolved)}",
            f"- Review queue entries: {len(self.report.unparsed)}",
            f"- Errors: {len(self.report.errors)}",
            "",
        ]
        for heading, issues in (
            ("Conflicts", self.report.conflicts),
            ("Resolved during import", self.report.resolved),
            ("Needs review", self.report.unparsed),
            ("Errors", self.report.errors),
        ):
            if issues:
                lines.extend([f"## {heading}", ""])
                lines.extend(
                    f"- `{item.source_path}` / `{item.record_key}` — {item.message}"
                    for item in issues
                )
                lines.append("")
        markdown_content = "\n".join(lines)
        _atomic_write_text(markdown_path, markdown_content)
        # A stable, human-friendly pointer complements the immutable timestamped
        # exports used for provenance and CLI history.
        latest_markdown_path = self.paths.artifacts_dir / "import-reconciliation.md"
        _atomic_write_text(latest_markdown_path, markdown_content)
        with self.database.session() as session:
            generated: list[Artifact] = []
            for output_path, mime_type in (
                (json_path, "application/json"),
                (markdown_path, "text/markdown"),
            ):
                relative = output_path.relative_to(self.paths.data_dir).as_posix()
                artifact = Artifact(
                    kind=ArtifactKind.GENERATED_EXPORT,
                    workspace_root=str(self.paths.data_dir),
                    source_path=relative,
                    stored_path=str(output_path),
                    content_hash=sha256_file(output_path),
                    size_bytes=output_path.stat().st_size,
                    mime_type=mime_type,
                    source_mtime_ns=output_path.stat().st_mtime_ns,
                    source_immutable=False,
                    metadata_json={
                        "export_type": "import_reconciliation",
                        "source_workspace": str(self.workspace),
                    },
                )
                session.add(artifact)
                generated.append(artifact)
            session.flush()
            record_audit(
                session,
                action="import.reconciliation_exported",
                entity_type="workspace",
                entity_id=sha256_text(str(self.workspace))[:36],
                actor="importer",
                after={
                    "workspace": str(self.workspace),
                    "artifact_ids": [artifact.id for artifact in generated],
                    "formats": ["json", "markdown"],
                },
            )


def _as_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def parse_report_date(path: Path, text: str) -> date | None:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if not match:
        match = re.search(
            r"^# New Job Postings\s+[—–-]\s+(20\d{2}-\d{2}-\d{2})", text, re.MULTILINE
        )
    return parse_legacy_date(match.group(1)) if match else None


def source_identity(label: str, url: str) -> tuple[str, str | None, str]:
    """Derive provider/account/ID without discarding ATS identity-bearing queries."""
    aliases = {
        "gh": "greenhouse",
        "greenhouse": "greenhouse",
        "usa": "usajobs",
        "usajobs": "usajobs",
        "lever": "lever",
        "ashby": "ashby",
        "workable": "workable",
        "workday": "workday",
        "jobvite": "jobvite",
    }
    source = aliases.get(label.strip().casefold(), label.strip().casefold())
    parts = urlsplit(url)
    segments = [segment for segment in parts.path.split("/") if segment]
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    account: str | None = None
    source_id = ""
    host = (parts.hostname or "").casefold()
    if source == "greenhouse":
        account = segments[0] if segments else None
        if "jobs" in segments:
            index = segments.index("jobs")
            source_id = segments[index + 1] if index + 1 < len(segments) else ""
        source_id = query.get("gh_jid") or source_id
    elif source == "lever":
        account = segments[0] if segments else None
        source_id = segments[-1] if segments else ""
    elif source == "ashby":
        account = segments[0] if segments else None
        source_id = segments[-1] if segments else ""
    elif source == "workable":
        account = segments[0] if segments else None
        if "j" in segments:
            index = segments.index("j")
            source_id = segments[index + 1] if index + 1 < len(segments) else ""
    elif source == "workday":
        account = host.split(".")[0] if host else None
        last = segments[-1] if segments else ""
        source_id = last.rsplit("_", 1)[-1]
    elif source == "usajobs":
        account = "federal"
        source_id = segments[-1] if segments else query.get("JobID", "")
    else:
        account = host or None
        source_id = segments[-1] if segments else ""
    source_id = source_id.strip() or sha256_text(canonicalize_url(url) or url)[:24]
    return source, account, source_id


def parse_legacy_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = re.sub(r"[*🚨]", "", value).strip()
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%B %d", "%b %d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            year = parsed.year if "%Y" in fmt else datetime.now().year
            return parsed.replace(year=year).date()
        except ValueError:
            continue
    return None


STATUS_DATE_RE = re.compile(
    r"(?:20\d{2}-\d{2}-\d{2}|"
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
    r"Dec(?:ember)?)\s+\d{1,2}(?:,\s*20\d{2})?)",
    re.IGNORECASE,
)


def _status_plain_text(status: str) -> str:
    value = re.sub(r"[*_`]+", "", status)
    value = re.sub(r"[✅❌🕐🚨]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def status_to_stage(status: str) -> ApplicationStage | None:
    lower = _status_plain_text(status).casefold()
    if not lower:
        return None
    if re.search(r"\barchiv(?:e|ed)\b", lower):
        return ApplicationStage.ARCHIVED
    if re.search(r"\b(?:withdrawn|withdrew|withdraw application)\b", lower):
        return ApplicationStage.WITHDRAWN
    if re.search(r"\b(?:rejected|not selected|declined by employer)\b", lower):
        return ApplicationStage.REJECTED
    if not re.search(r"\bno\s+offer\b", lower) and re.search(
        r"(?:^|\b)(?:offer(?:ed)?|offer\s+(?:received|extended|accepted|declined))(?:\b|$)",
        lower,
    ):
        return ApplicationStage.OFFER
    if re.search(
        r"\bassessment(?:\s+(?:received|requested|scheduled|completed))?\b", lower
    ):
        return ApplicationStage.ASSESSMENT
    if re.search(r"\binterview(?:ed|ing)?\b", lower):
        return ApplicationStage.INTERVIEW
    if re.search(r"\b(?:phone\s+)?screen(?:ed|ing)?\b", lower):
        return ApplicationStage.SCREENING
    if _submission_is_explicit(lower):
        return ApplicationStage.APPLIED
    if re.search(r"\b(?:planned|planning\s+to\s+apply)\b", lower):
        return ApplicationStage.PLANNED
    return None


def status_stage_datetime(status: str, stage: ApplicationStage) -> datetime | None:
    """Extract a date only when it is attached to an explicit stage cue."""
    cues = {
        ApplicationStage.PLANNED: r"planned|planning\s+to\s+apply",
        ApplicationStage.APPLIED: r"submitted|applied|application\s+sent",
        ApplicationStage.SCREENING: r"(?:phone\s+)?screen(?:ed|ing)?",
        ApplicationStage.INTERVIEW: r"interview(?:ed|ing)?",
        ApplicationStage.ASSESSMENT: r"assessment",
        ApplicationStage.OFFER: r"offer(?:ed)?",
        ApplicationStage.REJECTED: r"rejected|not\s+selected|declined\s+by\s+employer",
        ApplicationStage.WITHDRAWN: r"withdrawn|withdrew|withdraw\s+application",
        ApplicationStage.ARCHIVED: r"archiv(?:e|ed)",
    }
    cleaned = _status_plain_text(status)
    match = re.search(
        rf"\b(?:{cues[stage]})\b(?:\s+on)?[\s—–:,(]{{0,24}}"
        rf"(?P<date>{STATUS_DATE_RE.pattern})",
        cleaned,
        re.IGNORECASE,
    )
    if match is None:
        return None
    parsed = parse_legacy_date(match.group("date"))
    return (
        datetime.combine(parsed, datetime.min.time(), tzinfo=timezone.utc)
        if parsed
        else None
    )


def _legacy_stage_path(
    stage: ApplicationStage, status_text: str
) -> list[ApplicationStage]:
    if stage == ApplicationStage.PLANNED:
        return [ApplicationStage.PLANNED]
    explicitly_applied = _submission_is_explicit(
        _status_plain_text(status_text).casefold()
    )
    necessarily_applied = stage in {
        ApplicationStage.SCREENING,
        ApplicationStage.INTERVIEW,
        ApplicationStage.ASSESSMENT,
        ApplicationStage.OFFER,
        ApplicationStage.REJECTED,
    }
    path = [ApplicationStage.PLANNED]
    if explicitly_applied or necessarily_applied:
        path.append(ApplicationStage.APPLIED)
    if stage != ApplicationStage.APPLIED:
        path.append(stage)
    return path


def _submission_is_explicit(status: str) -> bool:
    negated = re.search(
        r"\b(?:not|never)\s+(?:yet\s+)?(?:submitted|applied|sent)\b",
        status,
    ) or re.search(r"\bapplication\s+(?:not|never)\s+(?:submitted|sent)\b", status)
    return not negated and bool(
        re.search(r"\b(?:submitted|applied|application\s+sent)\b", status)
    )


def _imported_stage_time(
    stage: ApplicationStage,
    *,
    status_text: str,
    submitted_at: datetime | None,
    target_stage: ApplicationStage,
    target_at: datetime | None,
    fallback: datetime,
) -> datetime:
    explicit = status_stage_datetime(status_text, stage)
    if explicit:
        return explicit
    if stage == ApplicationStage.APPLIED and submitted_at:
        return submitted_at
    if stage == ApplicationStage.APPLIED and target_at:
        return target_at - timedelta(microseconds=1)
    if stage == ApplicationStage.PLANNED:
        basis = submitted_at or target_at or fallback
        offset = 2 if target_at and not submitted_at else 1
        return basis - timedelta(microseconds=offset)
    if stage == target_stage and target_at:
        return target_at
    return fallback


def extract_document_text(path: Path) -> str:
    suffix = path.suffix.casefold()
    if suffix in {".md", ".txt"}:
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return ""
    if suffix in {".html", ".htm"}:
        try:
            raw = path.read_text(encoding="utf-8")
            raw = re.sub(
                r"<(script|style)\b[^>]*>.*?</\1>",
                " ",
                raw,
                flags=re.IGNORECASE | re.DOTALL,
            )
            return re.sub(
                r"\n{3,}", "\n\n", html.unescape(re.sub(r"<[^>]+>", "\n", raw))
            ).strip()
        except (OSError, UnicodeDecodeError):
            return ""
    if suffix == ".docx":
        descriptor: int | None = None
        try:
            from docx import Document

            descriptor = _open_regular_file(path)
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = None
                with zipfile.ZipFile(handle) as archive:
                    members = archive.infolist()
                    if len(members) > MAX_DOCX_MEMBERS:
                        return ""
                    expanded_size = 0
                    for member in members:
                        if member.flag_bits & 0x1:
                            return ""
                        expanded_size += member.file_size
                        if expanded_size > MAX_DOCX_EXPANDED_BYTES:
                            return ""
                handle.seek(0)
                document = Document(handle)
                return "\n\n".join(
                    paragraph.text
                    for paragraph in document.paragraphs
                    if paragraph.text.strip()
                )
        except Exception:
            return ""
        finally:
            if descriptor is not None:
                os.close(descriptor)
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader

            # Some valid legacy PDFs contain repairable, wrong-pointing xref
            # objects. pypdf logs one warning per object; keep errors visible
            # while preventing thousands of non-actionable import messages.
            logger = logging.getLogger("pypdf")
            previous_level = logger.level
            logger.setLevel(max(previous_level, logging.ERROR))
            try:
                reader = PdfReader(path)
                return "\n\n".join(
                    (page.extract_text() or "").strip() for page in reader.pages
                ).strip()
            finally:
                logger.setLevel(previous_level)
        except Exception:
            return ""
    return ""


def normalized_document_text(content: str) -> str:
    content = html.unescape(content)
    content = re.sub(r"[#*_`>|\[\]()]", " ", content)
    return normalize_text(content)


def semantic_document_similarity(left: str, right: str) -> float:
    """Compare cross-format text without over-weighting line/page ordering."""
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0
    sequence = difflib.SequenceMatcher(None, left, right, autojunk=False).ratio()
    left_tokens = Counter(left.split())
    right_tokens = Counter(right.split())
    overlap = sum((left_tokens & right_tokens).values())
    token_dice = (2 * overlap) / (
        sum(left_tokens.values()) + sum(right_tokens.values())
    )
    return max(sequence, token_dice)


def normalize_document_name(name: str, kind: ArtifactKind) -> str:
    value = normalize_text(name)
    for phrase in (
        "cover letter",
        "coverletter",
        "resume",
        "curriculum vitae",
        "cv",
        "revised",
        "final",
    ):
        value = re.sub(rf"\b{re.escape(phrase)}\b", " ", value)
    return re.sub(r"\s+", " ", value).strip() or kind.value


def document_source_sort_key(relative: str) -> tuple[str, int, str]:
    """Prefer editable text as the representative for cross-format variants."""
    path = Path(relative)
    format_priority = {
        ".md": 0,
        ".txt": 1,
        ".docx": 2,
        ".html": 3,
        ".htm": 3,
        ".pdf": 4,
    }
    return (
        path.with_suffix("").as_posix().casefold(),
        format_priority.get(path.suffix.casefold(), 99),
        relative.casefold(),
    )


def is_canonical_master_resume(path: Path, kind: ArtifactKind) -> bool:
    return kind == ArtifactKind.RESUME and path.name.casefold() in {
        "fixture candidate resume 2026.md",
        "fixture candidate resume 2026.docx",
        "fixture candidate resume 2026.pdf",
        # Preserve recognition for legacy local workspaces without publishing
        # those names in public fixtures.
        "mike sapp resume 2026.md",
        "mike sapp resume 2026.docx",
        "mike sapp resume 2026.pdf",
    }


def flatten_mapping(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            yield from flatten_mapping(item, child)
        return
    yield prefix, value


def import_workspace(
    workspace: Path | str,
    *,
    database: Database | None = None,
    paths: JobbyPaths | None = None,
    copy_sources: bool = True,
) -> ReconciliationReport:
    paths = (paths or resolve_paths()).ensure()
    database = database or Database(paths=paths)
    return LegacyImporter(database, paths=paths, copy_sources=copy_sources).run(
        workspace
    )
