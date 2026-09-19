"""Consistent, integrity-checked SQLite and managed-artifact backups."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
import zipfile
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from .config import JobbyPaths, MAX_CONFIG_BYTES, resolve_paths
from .db import Database


BACKUP_SCHEMA = "jobby-backup-v1"
EXTERNAL_GENERATED_EXPORT_REASON = "external generated export"
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_DATABASE_BYTES = 8 * 1024 * 1024 * 1024
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED_BYTES = 16 * 1024 * 1024 * 1024
MAX_ARTIFACTS = 100_000
MAX_ARCHIVE_MEMBERS = MAX_ARTIFACTS + 3
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,100}$")


def create_backup(
    database: Database,
    *,
    output: Path | str | None = None,
    paths: JobbyPaths | None = None,
) -> Path:
    paths = (paths or database.paths or resolve_paths()).ensure()
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S-%f")
    destination = (
        Path(output).expanduser()
        if output
        else paths.backups_dir / f"jobby-backup-{stamp}.zip"
    )
    if destination.suffix.casefold() != ".zip":
        destination = destination.with_suffix(".zip")
    if destination.is_symlink():
        raise ValueError("backup destination must not be a symbolic link")
    destination = destination.absolute()
    if destination == database.path:
        raise ValueError("backup destination must not overwrite the live database")
    if destination.exists() and not destination.is_file():
        raise ValueError("backup destination must be a regular file")
    if destination.exists():
        raise FileExistsError(f"backup destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="jobby-backup-", dir=paths.cache_dir
    ) as temp_dir:
        snapshot = Path(temp_dir) / "jobby.sqlite3"
        database.backup_to(snapshot)
        artifact_rows = _snapshot_artifacts(snapshot)
        artifact_manifest: list[dict[str, object]] = []
        skipped_artifacts: list[dict[str, str]] = []
        excluded_artifacts: list[dict[str, str]] = []
        manifest: dict[str, object] = {
            "schema": BACKUP_SCHEMA,
            "created_at": datetime.now().astimezone().isoformat(),
            "database_sha256": _hash_file(snapshot),
            "artifacts": artifact_manifest,
            "skipped_artifacts": skipped_artifacts,
            "excluded_artifacts": excluded_artifacts,
        }
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
        )
        temporary_destination = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w+b") as output_handle:
                with zipfile.ZipFile(
                    output_handle,
                    "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=6,
                ) as archive:
                    database_hash, _ = _write_member(
                        archive,
                        snapshot,
                        "database/jobby.sqlite3",
                        expected_hash=str(manifest["database_sha256"]),
                        maximum_size=MAX_DATABASE_BYTES,
                    )
                    manifest["database_sha256"] = database_hash
                    managed_roots = (
                        paths.data_dir.resolve(),
                        paths.artifacts_dir.resolve(),
                    )
                    for artifact_id, kind, stored_path, expected_hash in artifact_rows:
                        if (
                            stored_path
                            and kind == "generated_export"
                            and _is_outside_managed_storage(stored_path, managed_roots)
                        ):
                            excluded_artifacts.append(
                                {
                                    "id": artifact_id,
                                    "kind": kind,
                                    "reason": EXTERNAL_GENERATED_EXPORT_REASON,
                                }
                            )
                            continue
                        if not stored_path:
                            skipped_artifacts.append(
                                {"id": artifact_id, "reason": "no stored path"}
                            )
                            continue
                        source, skip_reason = _managed_artifact_file(
                            stored_path, managed_roots
                        )
                        if source is None:
                            skipped_artifacts.append(
                                {
                                    "id": artifact_id,
                                    "reason": skip_reason,
                                }
                            )
                            continue
                        safe_id = (
                            artifact_id
                            if SAFE_ID_RE.fullmatch(artifact_id)
                            else hashlib.sha256(artifact_id.encode()).hexdigest()[:32]
                        )
                        safe_name = _safe_archive_filename(source.name)
                        arcname = f"artifacts/{safe_id}/{safe_name}"
                        digest, size = _write_member(
                            archive,
                            source,
                            arcname,
                            expected_hash=expected_hash,
                            maximum_size=MAX_ARTIFACT_BYTES,
                        )
                        artifact_manifest.append(
                            {
                                "id": artifact_id,
                                "kind": kind,
                                "path": arcname,
                                "sha256": digest,
                                "size_bytes": size,
                            }
                        )
                    config = _managed_regular_file(
                        str(paths.config_file), (paths.config_dir.resolve(),)
                    )
                    if config is not None:
                        digest, size = _write_member(
                            archive,
                            config,
                            "config/config.toml",
                            maximum_size=MAX_CONFIG_BYTES,
                        )
                        manifest["config"] = {
                            "path": "config/config.toml",
                            "sha256": digest,
                            "size_bytes": size,
                        }
                    manifest_bytes = json.dumps(
                        manifest, indent=2, ensure_ascii=False
                    ).encode("utf-8")
                    if len(manifest_bytes) > MAX_MANIFEST_BYTES:
                        raise ValueError("backup manifest exceeds the safe size limit")
                    archive.writestr("manifest.json", manifest_bytes)
                output_handle.flush()
                os.fsync(output_handle.fileno())
            temporary_destination.chmod(0o600)
            verified, detail = verify_backup(temporary_destination)
            if not verified:
                raise RuntimeError(f"backup verification failed: {detail}")
            # Linking is an atomic, no-clobber publish because the staging file is
            # created in the destination directory. Unlike os.replace(), it also
            # preserves a destination created by another process during backup.
            os.link(temporary_destination, destination)
            temporary_destination.unlink()
            _sync_directory(destination.parent)
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary_destination.unlink(missing_ok=True)
            raise
    return destination


def _sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def verify_backup(path: Path | str) -> tuple[bool, str]:
    path = Path(path)
    try:
        with _regular_backup_handle(path) as source, zipfile.ZipFile(source) as archive:
            infos = archive.infolist()
            if len(infos) > MAX_ARCHIVE_MEMBERS:
                return False, "backup contains too many archive members"
            names = [item.filename for item in infos]
            if len(names) != len(set(names)):
                return False, "backup contains duplicate member names"
            if "manifest.json" not in names:
                return False, "backup is missing manifest.json"
            manifest_info = archive.getinfo("manifest.json")
            if manifest_info.file_size > MAX_MANIFEST_BYTES:
                return False, "backup manifest is too large"
            manifest = json.loads(archive.read(manifest_info))
            if not isinstance(manifest, dict):
                return False, "backup manifest must be an object"
            if manifest.get("schema") != BACKUP_SCHEMA:
                return False, "unsupported backup schema"
            artifacts = manifest.get("artifacts", [])
            if not isinstance(artifacts, list) or len(artifacts) > MAX_ARTIFACTS:
                return False, "backup artifact manifest is invalid"
            skipped_artifacts = manifest.get("skipped_artifacts", [])
            skipped_detail = _skipped_artifact_detail(skipped_artifacts)
            skipped_ids = {str(item["id"]) for item in skipped_artifacts}
            excluded_artifacts = manifest.get("excluded_artifacts", [])
            excluded_ids = _excluded_artifact_ids(excluded_artifacts)
            if skipped_ids & excluded_ids:
                return False, "backup artifact IDs overlap manifest categories"
            if (
                len(artifacts) + len(skipped_artifacts) + len(excluded_artifacts)
                > MAX_ARTIFACTS
            ):
                return False, "backup artifact manifests exceed the safe limit"

            expected_members = {"manifest.json", "database/jobby.sqlite3"}
            database_info = _validated_member(
                archive, "database/jobby.sqlite3", MAX_DATABASE_BYTES
            )
            expected_database_hash = _manifest_hash(
                manifest.get("database_sha256"), "database"
            )
            with tempfile.TemporaryDirectory(prefix="jobby-verify-") as temp_dir:
                snapshot = Path(temp_dir) / "jobby.sqlite3"
                database_hash = _copy_and_hash_member(
                    archive,
                    database_info,
                    destination=snapshot,
                    maximum_size=MAX_DATABASE_BYTES,
                )
                if database_hash != expected_database_hash:
                    return False, "database hash mismatch"
                connection = sqlite3.connect(snapshot)
                try:
                    integrity = connection.execute("PRAGMA integrity_check").fetchone()
                    foreign_key_error = connection.execute(
                        "PRAGMA foreign_key_check"
                    ).fetchone()
                finally:
                    connection.close()
                if not integrity or integrity[0] != "ok":
                    return (
                        False,
                        f"database integrity check failed: {integrity[0] if integrity else 'no result'}",
                    )
                if foreign_key_error is not None:
                    return (
                        False,
                        f"database foreign-key check failed: {foreign_key_error}",
                    )

            total_size = database_info.file_size + manifest_info.file_size
            seen_artifact_paths: set[str] = set()
            seen_artifact_ids: set[str] = set()
            for item in artifacts:
                if not isinstance(item, dict):
                    return False, "backup artifact manifest entry is invalid"
                artifact_id = item.get("id")
                if (
                    not isinstance(artifact_id, str)
                    or not artifact_id
                    or artifact_id in seen_artifact_ids
                    or artifact_id in skipped_ids
                    or artifact_id in excluded_ids
                ):
                    return False, "backup artifact IDs are invalid or duplicated"
                seen_artifact_ids.add(artifact_id)
                member_path = item.get("path")
                if (
                    not isinstance(member_path, str)
                    or member_path in seen_artifact_paths
                ):
                    return False, "backup artifact path is invalid or duplicated"
                if not member_path.startswith("artifacts/"):
                    return False, f"unsafe artifact member path: {member_path}"
                seen_artifact_paths.add(member_path)
                info = _validated_member(archive, member_path, MAX_ARTIFACT_BYTES)
                declared_size = item.get("size_bytes")
                if declared_size is not None and declared_size != info.file_size:
                    return False, f"artifact size mismatch: {item.get('id', 'unknown')}"
                total_size += info.file_size
                if total_size > MAX_TOTAL_UNCOMPRESSED_BYTES:
                    return False, "backup expands beyond the verification limit"
                expected_hash = _manifest_hash(item.get("sha256"), "artifact")
                if (
                    _copy_and_hash_member(
                        archive, info, maximum_size=MAX_ARTIFACT_BYTES
                    )
                    != expected_hash
                ):
                    return False, f"artifact hash mismatch: {item.get('id', 'unknown')}"
                expected_members.add(member_path)

            config = manifest.get("config")
            if config is not None:
                if (
                    not isinstance(config, dict)
                    or config.get("path") != "config/config.toml"
                ):
                    return False, "backup config manifest is invalid"
                info = _validated_member(
                    archive, "config/config.toml", MAX_CONFIG_BYTES
                )
                if config.get("size_bytes") not in {None, info.file_size}:
                    return False, "config size mismatch"
                if _copy_and_hash_member(
                    archive, info, maximum_size=MAX_ARTIFACT_BYTES
                ) != _manifest_hash(config.get("sha256"), "config"):
                    return False, "config hash mismatch"
                expected_members.add("config/config.toml")
            elif "config/config.toml" in names:
                # Older v1 archives did not hash config. Keep them readable;
                # newly created archives always authenticate this member.
                _validated_member(archive, "config/config.toml", MAX_CONFIG_BYTES)
                expected_members.add("config/config.toml")

            if set(names) != expected_members:
                return False, "backup contains unmanifested members"
            if skipped_artifacts:
                return (
                    False,
                    "backup is incomplete: "
                    f"{len(skipped_artifacts)} managed artifact(s) were skipped"
                    f" ({skipped_detail})",
                )
        return True, "ok"
    except (
        AttributeError,
        OSError,
        KeyError,
        OverflowError,
        RuntimeError,
        TypeError,
        ValueError,
        sqlite3.DatabaseError,
        zipfile.BadZipFile,
        json.JSONDecodeError,
    ) as exc:
        return False, str(exc)


@contextmanager
def _regular_backup_handle(path: Path) -> Iterator[BinaryIO]:
    """Pin a regular backup inode so verification never follows a link race."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if path.is_symlink():
            raise ValueError("backup must not be a symbolic link") from None
        raise
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("backup must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            yield handle
    finally:
        os.close(descriptor)


def _snapshot_artifacts(snapshot: Path) -> list[tuple[str, str, str | None, str]]:
    connection = sqlite3.connect(snapshot)
    try:
        rows = connection.execute(
            "SELECT id, kind, stored_path, content_hash FROM artifacts ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    if len(rows) > MAX_ARTIFACTS:
        raise ValueError("database contains too many artifacts for one backup")
    return [
        (str(item[0]), str(item[1]), str(item[2]) if item[2] else None, str(item[3]))
        for item in rows
    ]


def _managed_regular_file(value: str, managed_roots: tuple[Path, ...]) -> Path | None:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute() or candidate.is_symlink():
        return None
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        return None
    if not resolved.is_file() or not any(
        _is_relative_to(resolved, root) for root in managed_roots
    ):
        return None
    return resolved


def _managed_artifact_file(
    value: str, managed_roots: tuple[Path, ...]
) -> tuple[Path | None, str]:
    """Return a safe artifact file or a stable, user-facing skip reason."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        return None, "stored path is not absolute"
    if candidate.is_symlink():
        return None, "stored path is a symbolic link"
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError:
        return None, "stored file is missing"
    except OSError:
        return None, "stored file is unavailable"
    if not resolved.is_file():
        return None, "stored path is not a regular file"
    if not any(_is_relative_to(resolved, root) for root in managed_roots):
        return None, "stored file is outside managed storage"
    return resolved, ""


def _is_outside_managed_storage(value: str, managed_roots: tuple[Path, ...]) -> bool:
    """Identify deliberately external paths without treating malformed paths as optional."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        return False
    try:
        resolved = candidate.resolve(strict=False)
    except OSError:
        return False
    return not any(_is_relative_to(resolved, root) for root in managed_roots)


def _skipped_artifact_detail(value: object) -> str:
    if not isinstance(value, list) or len(value) > MAX_ARTIFACTS:
        raise ValueError("backup skipped-artifact manifest is invalid")
    reasons: Counter[str] = Counter()
    seen_ids: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("backup skipped-artifact manifest entry is invalid")
        artifact_id = item.get("id")
        reason = item.get("reason")
        if (
            not isinstance(artifact_id, str)
            or not artifact_id
            or artifact_id in seen_ids
            or not isinstance(reason, str)
            or not reason.strip()
        ):
            raise ValueError("backup skipped-artifact manifest entry is invalid")
        seen_ids.add(artifact_id)
        reasons[reason.strip()] += 1
    return ", ".join(f"{count} {reason}" for reason, count in sorted(reasons.items()))


def _excluded_artifact_ids(value: object) -> set[str]:
    if not isinstance(value, list) or len(value) > MAX_ARTIFACTS:
        raise ValueError("backup excluded-artifact manifest is invalid")
    ids: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("backup excluded-artifact manifest entry is invalid")
        artifact_id = item.get("id")
        if (
            not isinstance(artifact_id, str)
            or not artifact_id
            or artifact_id in ids
            or item.get("kind") != "generated_export"
            or item.get("reason") != EXTERNAL_GENERATED_EXPORT_REASON
        ):
            raise ValueError("backup excluded-artifact manifest entry is invalid")
        ids.add(artifact_id)
    return ids


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_archive_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "_", value).strip(" .")
    return cleaned[:200] or "artifact"


def _write_member(
    archive: zipfile.ZipFile,
    source: Path,
    arcname: str,
    *,
    expected_hash: str | None = None,
    maximum_size: int,
) -> tuple[str, int]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    digest = hashlib.sha256()
    size = 0
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"backup member is not a regular file: {source}")
        if before.st_size > maximum_size:
            raise ValueError(f"backup member exceeds size limit: {source.name}")
        info = zipfile.ZipInfo(arcname)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (0o600 & 0xFFFF) << 16
        with os.fdopen(descriptor, "rb", closefd=False) as input_handle:
            with archive.open(info, "w") as output_handle:
                for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)
                    if size > maximum_size:
                        raise ValueError(
                            f"backup member exceeds size limit: {source.name}"
                        )
                    output_handle.write(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RuntimeError(f"backup member changed while being read: {source}")
    finally:
        os.close(descriptor)
    actual_hash = digest.hexdigest()
    if expected_hash is not None and actual_hash != expected_hash:
        raise ValueError(f"artifact content hash mismatch: {source.name}")
    return actual_hash, size


def _validated_member(
    archive: zipfile.ZipFile, name: str, maximum_size: int
) -> zipfile.ZipInfo:
    pure = PurePosixPath(name)
    if pure.is_absolute() or ".." in pure.parts or "\\" in name:
        raise ValueError(f"unsafe backup member path: {name}")
    info = archive.getinfo(name)
    member_mode = (info.external_attr >> 16) & 0xFFFF
    if info.is_dir() or stat.S_ISLNK(member_mode) or info.flag_bits & 0x1:
        raise ValueError(f"unsupported backup member: {name}")
    if info.file_size > maximum_size:
        raise ValueError(f"backup member exceeds size limit: {name}")
    if info.file_size > 10 * 1024 * 1024 and info.compress_size == 0:
        raise ValueError(f"invalid compressed size for member: {name}")
    if info.compress_size and info.file_size / info.compress_size > 10_000:
        raise ValueError(f"backup member compression ratio is unsafe: {name}")
    return info


def _copy_and_hash_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    *,
    destination: Path | None = None,
    maximum_size: int,
) -> str:
    digest = hashlib.sha256()
    size = 0
    output: BinaryIO | None = destination.open("wb") if destination else None
    try:
        with archive.open(info) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                if size > maximum_size:
                    raise ValueError(
                        f"backup member exceeds size limit: {info.filename}"
                    )
                digest.update(chunk)
                if output is not None:
                    output.write(chunk)
    finally:
        if output is not None:
            output.close()
    if size != info.file_size:
        raise ValueError(f"backup member size mismatch: {info.filename}")
    return digest.hexdigest()


def _manifest_hash(value: object, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{label} hash is invalid")
    return value


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["create_backup", "verify_backup"]
