"""Streaming authenticated container for portable encrypted Jobby backups.

The container deliberately has a small, fixed version-one surface.  Its JSON
metadata is visible but authenticated as AES-GCM additional data; backup bytes
are encrypted.  Publication is no-clobber and atomic within the destination
directory, so a failed operation never exposes a partial destination file.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import struct
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt


CONTAINER_SCHEMA = "jobby-encrypted-backup-v1"
CONTAINER_VERSION = 1
MAGIC = b"JOBBYENC"

# Version one fixes its algorithms and bounds its KDF work factor.  The header
# records log2(N), and the complete prefix is authenticated as AES-GCM
# additional data.  Keeping the accepted range deliberately narrow prevents an
# untrusted header from selecting an unbounded CPU or memory cost while still
# allowing the local configuration to choose 16, 32, or 64 MiB of Scrypt work
# memory (for r=8).
MIN_SCRYPT_LOG_N = 14
SCRYPT_LOG_N = 15
MAX_SCRYPT_LOG_N = 16
DEFAULT_SCRYPT_N = 1 << SCRYPT_LOG_N
MIN_SCRYPT_N = 1 << MIN_SCRYPT_LOG_N
MAX_SCRYPT_N = 1 << MAX_SCRYPT_LOG_N
SCRYPT_R = 8
SCRYPT_P = 1
KEY_BYTES = 32
SALT_BYTES = 16
NONCE_BYTES = 12
TAG_BYTES = 16

DEFAULT_CHUNK_BYTES = 1024 * 1024
MIN_CHUNK_BYTES = 4 * 1024
MAX_CHUNK_BYTES = 8 * 1024 * 1024
MAX_METADATA_BYTES = 64 * 1024
MAX_PASSPHRASE_BYTES = 4 * 1024
MAX_PLAINTEXT_BYTES = 16 * 1024 * 1024 * 1024

# magic, version, flags, log2(N), r, p, reserved, metadata length, plaintext size
_HEADER = struct.Struct(">8sBBBBHHIQ")
_FLAGS = 0
_RESERVED = 0
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EncryptedBackupError(Exception):
    """Base class for safe encrypted-container failures."""


class EncryptedBackupFormatError(EncryptedBackupError):
    """The input is not a supported, strictly formed encrypted container."""


class EncryptedBackupAuthenticationError(EncryptedBackupError):
    """Authentication failed without distinguishing a key error from tampering."""


class EncryptedBackupIntegrityError(EncryptedBackupError):
    """Authenticated plaintext did not match its authenticated checksum."""


@dataclass(frozen=True, slots=True)
class EncryptedBackupMetadata:
    """Authenticated version-one metadata."""

    schema: str
    version: int
    created_at: str
    plaintext_size: int
    plaintext_sha256: str


@dataclass(frozen=True, slots=True)
class _ContainerPrefix:
    raw: bytes
    salt: bytes
    nonce: bytes
    metadata: EncryptedBackupMetadata
    ciphertext_size: int
    scrypt_n: int


def encrypt_backup(
    source: Path | str,
    destination: Path | str,
    passphrase: str | bytes,
    *,
    chunk_size: int = DEFAULT_CHUNK_BYTES,
    scrypt_n: int = DEFAULT_SCRYPT_N,
) -> Path:
    """Encrypt one regular file into a versioned, authenticated container.

    The source is hashed in a bounded-memory first pass so its SHA-256 can be
    authenticated in the metadata.  A second-pass digest detects source changes
    while encryption is in progress.
    """

    chunk_size = _validated_chunk_size(chunk_size)
    scrypt_log_n = _validated_scrypt_n(scrypt_n)
    passphrase_bytes = _validated_passphrase(passphrase)
    _source_path, source_handle, source_size = _open_regular_source(
        source,
        maximum_size=MAX_PLAINTEXT_BYTES,
        description="plaintext backup source",
    )
    try:
        first_digest, first_size = _hash_stream(
            source_handle,
            maximum_size=MAX_PLAINTEXT_BYTES,
            chunk_size=chunk_size,
        )
        if first_size != source_size:
            raise EncryptedBackupIntegrityError(
                "plaintext backup source changed while it was being read"
            )
        source_handle.seek(0)

        metadata = EncryptedBackupMetadata(
            schema=CONTAINER_SCHEMA,
            version=CONTAINER_VERSION,
            created_at=datetime.now(timezone.utc).isoformat(),
            plaintext_size=first_size,
            plaintext_sha256=first_digest,
        )
        metadata_bytes = _encode_metadata(metadata)
        salt = os.urandom(SALT_BYTES)
        nonce = os.urandom(NONCE_BYTES)
        header = _pack_header(len(metadata_bytes), first_size, scrypt_log_n)
        authenticated_prefix = header + salt + nonce + metadata_bytes
        key = _derive_key(passphrase_bytes, salt, scrypt_n=1 << scrypt_log_n)

        destination_path = _prepare_destination(destination)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination_path.name}.encrypted-",
            suffix=".tmp",
            dir=destination_path.parent,
        )
        temporary = Path(temporary_name)
        descriptor_open = True
        try:
            with os.fdopen(descriptor, "wb") as output_handle:
                descriptor_open = False
                output_handle.write(authenticated_prefix)
                encryptor = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
                encryptor.authenticate_additional_data(authenticated_prefix)
                second_hash = hashlib.sha256()
                encrypted_size = 0
                while chunk := source_handle.read(chunk_size):
                    encrypted_size += len(chunk)
                    if encrypted_size > MAX_PLAINTEXT_BYTES:
                        raise EncryptedBackupIntegrityError(
                            "plaintext backup source exceeds the safe size limit"
                        )
                    second_hash.update(chunk)
                    output_handle.write(encryptor.update(chunk))
                output_handle.write(encryptor.finalize())
                output_handle.write(encryptor.tag)
                if (
                    encrypted_size != first_size
                    or second_hash.hexdigest() != first_digest
                ):
                    raise EncryptedBackupIntegrityError(
                        "plaintext backup source changed during encryption"
                    )
                output_handle.flush()
                os.fsync(output_handle.fileno())
            temporary.chmod(0o600)
            _publish_no_clobber(temporary, destination_path)
        finally:
            if descriptor_open:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
        return destination_path
    finally:
        source_handle.close()


def decrypt_backup(
    source: Path | str,
    destination: Path | str,
    passphrase: str | bytes,
    *,
    chunk_size: int = DEFAULT_CHUNK_BYTES,
) -> Path:
    """Authenticate and decrypt a container without publishing partial output."""

    chunk_size = _validated_chunk_size(chunk_size)
    passphrase_bytes = _validated_passphrase(passphrase)
    _source_path, source_handle, container_size = _open_regular_source(
        source,
        maximum_size=_maximum_container_size(),
        description="encrypted backup source",
    )
    try:
        prefix = _read_container_prefix(source_handle, container_size)
        ciphertext_offset = source_handle.tell()
        source_handle.seek(prefix.ciphertext_size, os.SEEK_CUR)
        tag = _read_exact(source_handle, TAG_BYTES, "authentication tag")
        if source_handle.read(1):
            raise EncryptedBackupFormatError(
                "encrypted backup contains unexpected trailing data"
            )
        source_handle.seek(ciphertext_offset)

        key = _derive_key(
            passphrase_bytes,
            prefix.salt,
            scrypt_n=prefix.scrypt_n,
        )
        decryptor = Cipher(
            algorithms.AES(key), modes.GCM(prefix.nonce, tag)
        ).decryptor()
        decryptor.authenticate_additional_data(prefix.raw)

        destination_path = _prepare_destination(destination)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination_path.name}.decrypted-",
            suffix=".tmp",
            dir=destination_path.parent,
        )
        temporary = Path(temporary_name)
        descriptor_open = True
        try:
            with os.fdopen(descriptor, "wb") as output_handle:
                descriptor_open = False
                plaintext_hash = hashlib.sha256()
                plaintext_size = 0
                remaining = prefix.ciphertext_size
                try:
                    while remaining:
                        chunk = _read_exact(
                            source_handle,
                            min(chunk_size, remaining),
                            "encrypted payload",
                        )
                        remaining -= len(chunk)
                        plaintext = decryptor.update(chunk)
                        plaintext_size += len(plaintext)
                        if plaintext_size > MAX_PLAINTEXT_BYTES:
                            raise EncryptedBackupFormatError(
                                "decrypted backup exceeds the safe size limit"
                            )
                        plaintext_hash.update(plaintext)
                        output_handle.write(plaintext)
                    final_plaintext = decryptor.finalize()
                except InvalidTag:
                    raise EncryptedBackupAuthenticationError(
                        "encrypted backup authentication failed"
                    ) from None
                plaintext_size += len(final_plaintext)
                if plaintext_size > MAX_PLAINTEXT_BYTES:
                    raise EncryptedBackupFormatError(
                        "decrypted backup exceeds the safe size limit"
                    )
                plaintext_hash.update(final_plaintext)
                output_handle.write(final_plaintext)
                if (
                    plaintext_size != prefix.metadata.plaintext_size
                    or plaintext_hash.hexdigest() != prefix.metadata.plaintext_sha256
                ):
                    raise EncryptedBackupIntegrityError(
                        "decrypted backup failed its authenticated checksum"
                    )
                output_handle.flush()
                os.fsync(output_handle.fileno())
            temporary.chmod(0o600)
            _publish_no_clobber(temporary, destination_path)
        finally:
            if descriptor_open:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
        return destination_path
    finally:
        source_handle.close()


def _pack_header(
    metadata_size: int,
    plaintext_size: int,
    scrypt_log_n: int = SCRYPT_LOG_N,
) -> bytes:
    return _HEADER.pack(
        MAGIC,
        CONTAINER_VERSION,
        _FLAGS,
        scrypt_log_n,
        SCRYPT_R,
        SCRYPT_P,
        _RESERVED,
        metadata_size,
        plaintext_size,
    )


def _encode_metadata(metadata: EncryptedBackupMetadata) -> bytes:
    payload = {
        "algorithms": {"cipher": "AES-256-GCM", "kdf": "scrypt"},
        "created_at": metadata.created_at,
        "plaintext": {
            "sha256": metadata.plaintext_sha256,
            "size_bytes": metadata.plaintext_size,
        },
        "schema": metadata.schema,
        "version": metadata.version,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if not 1 <= len(encoded) <= MAX_METADATA_BYTES:
        raise EncryptedBackupFormatError(
            "encrypted backup metadata exceeds the safe size limit"
        )
    return encoded


def _read_container_prefix(
    source_handle: BinaryIO, container_size: int
) -> _ContainerPrefix:
    if container_size < _minimum_container_size():
        raise EncryptedBackupFormatError("encrypted backup is truncated")
    raw_header = _read_exact(source_handle, _HEADER.size, "fixed header")
    try:
        (
            magic,
            version,
            flags,
            log_n,
            r_value,
            p_value,
            reserved,
            metadata_size,
            plaintext_size,
        ) = _HEADER.unpack(raw_header)
    except struct.error:
        raise EncryptedBackupFormatError("encrypted backup header is invalid") from None
    if magic != MAGIC:
        raise EncryptedBackupFormatError("encrypted backup header is invalid")
    if version != CONTAINER_VERSION:
        raise EncryptedBackupFormatError(
            "encrypted backup container version is unsupported"
        )
    if flags != _FLAGS or reserved != _RESERVED:
        raise EncryptedBackupFormatError("encrypted backup header flags are invalid")
    if (
        not MIN_SCRYPT_LOG_N <= log_n <= MAX_SCRYPT_LOG_N
        or r_value != SCRYPT_R
        or p_value != SCRYPT_P
    ):
        raise EncryptedBackupFormatError(
            "encrypted backup key-derivation parameters are unsupported"
        )
    if not 1 <= metadata_size <= MAX_METADATA_BYTES:
        raise EncryptedBackupFormatError("encrypted backup metadata length is invalid")
    if plaintext_size > MAX_PLAINTEXT_BYTES:
        raise EncryptedBackupFormatError(
            "encrypted backup payload exceeds the safe size limit"
        )
    expected_size = (
        _HEADER.size
        + SALT_BYTES
        + NONCE_BYTES
        + metadata_size
        + plaintext_size
        + TAG_BYTES
    )
    if expected_size != container_size:
        raise EncryptedBackupFormatError(
            "encrypted backup length does not match its header"
        )
    salt = _read_exact(source_handle, SALT_BYTES, "salt")
    nonce = _read_exact(source_handle, NONCE_BYTES, "nonce")
    metadata_bytes = _read_exact(source_handle, metadata_size, "metadata")
    metadata = _decode_metadata(metadata_bytes)
    if metadata.version != version or metadata.plaintext_size != plaintext_size:
        raise EncryptedBackupFormatError(
            "encrypted backup metadata does not match its header"
        )
    return _ContainerPrefix(
        raw=raw_header + salt + nonce + metadata_bytes,
        salt=salt,
        nonce=nonce,
        metadata=metadata,
        ciphertext_size=plaintext_size,
        scrypt_n=1 << log_n,
    )


def _decode_metadata(encoded: bytes) -> EncryptedBackupMetadata:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        payload = json.loads(encoded.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise EncryptedBackupFormatError(
            "encrypted backup metadata is invalid"
        ) from None
    if not isinstance(payload, dict) or set(payload) != {
        "algorithms",
        "created_at",
        "plaintext",
        "schema",
        "version",
    }:
        raise EncryptedBackupFormatError("encrypted backup metadata is invalid")
    algorithms_value = payload.get("algorithms")
    plaintext_value = payload.get("plaintext")
    if (
        not isinstance(algorithms_value, dict)
        or algorithms_value != {"cipher": "AES-256-GCM", "kdf": "scrypt"}
        or not isinstance(plaintext_value, dict)
        or set(plaintext_value) != {"sha256", "size_bytes"}
    ):
        raise EncryptedBackupFormatError("encrypted backup metadata is invalid")
    schema = payload.get("schema")
    version = payload.get("version")
    created_at = payload.get("created_at")
    size = plaintext_value.get("size_bytes")
    digest = plaintext_value.get("sha256")
    if (
        schema != CONTAINER_SCHEMA
        or type(version) is not int
        or version != CONTAINER_VERSION
        or not isinstance(created_at, str)
        or not 1 <= len(created_at) <= 64
        or type(size) is not int
        or not 0 <= size <= MAX_PLAINTEXT_BYTES
        or not isinstance(digest, str)
        or _SHA256_RE.fullmatch(digest) is None
    ):
        raise EncryptedBackupFormatError("encrypted backup metadata is invalid")
    try:
        parsed_time = datetime.fromisoformat(created_at)
    except ValueError:
        raise EncryptedBackupFormatError(
            "encrypted backup metadata is invalid"
        ) from None
    if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
        raise EncryptedBackupFormatError("encrypted backup metadata is invalid")
    return EncryptedBackupMetadata(
        schema=schema,
        version=version,
        created_at=created_at,
        plaintext_size=size,
        plaintext_sha256=digest,
    )


def _derive_key(
    passphrase: bytes,
    salt: bytes,
    *,
    scrypt_n: int = DEFAULT_SCRYPT_N,
) -> bytes:
    return Scrypt(
        salt=salt,
        length=KEY_BYTES,
        n=scrypt_n,
        r=SCRYPT_R,
        p=SCRYPT_P,
    ).derive(passphrase)


def _validated_passphrase(passphrase: str | bytes) -> bytes:
    if isinstance(passphrase, str):
        try:
            encoded = passphrase.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("backup passphrase is not valid UTF-8 text") from None
    elif isinstance(passphrase, bytes):
        encoded = passphrase
    else:
        raise TypeError("backup passphrase must be text or bytes")
    if not 1 <= len(encoded) <= MAX_PASSPHRASE_BYTES:
        raise ValueError("backup passphrase length is outside the safe bounds")
    return encoded


def _validated_chunk_size(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("encrypted-backup chunk size must be an integer")
    if not MIN_CHUNK_BYTES <= value <= MAX_CHUNK_BYTES:
        raise ValueError("encrypted-backup chunk size is outside the safe bounds")
    return value


def _validated_scrypt_n(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("encrypted-backup Scrypt N must be an integer")
    if not MIN_SCRYPT_N <= value <= MAX_SCRYPT_N or value & (value - 1):
        raise ValueError(
            "encrypted-backup Scrypt N must be a supported power of two "
            f"between {MIN_SCRYPT_N:,} and {MAX_SCRYPT_N:,}"
        )
    return value.bit_length() - 1


def _open_regular_source(
    path: Path | str,
    *,
    maximum_size: int,
    description: str,
) -> tuple[Path, BinaryIO, int]:
    requested = Path(path).expanduser().absolute()
    if requested.is_symlink():
        raise ValueError(f"{description} must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError as exc:
        if requested.is_symlink():
            raise ValueError(f"{description} must not be a symbolic link") from None
        raise exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{description} must be a regular file")
        if metadata.st_size > maximum_size:
            raise ValueError(f"{description} exceeds the safe size limit")
        return requested, os.fdopen(descriptor, "rb"), metadata.st_size
    except Exception:
        os.close(descriptor)
        raise


def _prepare_destination(path: Path | str) -> Path:
    destination = Path(path).expanduser().absolute()
    if destination.is_symlink():
        raise ValueError("encrypted-backup destination must not be a symbolic link")
    if destination.exists():
        raise FileExistsError(
            "encrypted-backup destination already exists; refusing to overwrite it"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError(
            "encrypted-backup destination parent must be a regular directory"
        )
    if destination.is_symlink():
        raise ValueError("encrypted-backup destination must not be a symbolic link")
    if destination.exists():
        raise FileExistsError(
            "encrypted-backup destination already exists; refusing to overwrite it"
        )
    return destination


def _hash_stream(
    handle: BinaryIO, *, maximum_size: int, chunk_size: int
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := handle.read(chunk_size):
        size += len(chunk)
        if size > maximum_size:
            raise ValueError("plaintext backup source exceeds the safe size limit")
        digest.update(chunk)
    return digest.hexdigest(), size


def _read_exact(handle: BinaryIO, size: int, description: str) -> bytes:
    if size < 0:
        raise EncryptedBackupFormatError("encrypted backup length is invalid")
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = handle.read(remaining)
        if not chunk:
            raise EncryptedBackupFormatError(
                f"encrypted backup is truncated in its {description}"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _publish_no_clobber(staged: Path, destination: Path) -> None:
    try:
        os.link(staged, destination, follow_symlinks=False)
    except FileExistsError:
        raise FileExistsError(
            "encrypted-backup destination appeared during publication; it was preserved"
        ) from None
    # ``staged`` was created mode 0600 and a hard link inherits that inode mode.
    # Avoid a path-based chmod after publication: another local process could
    # otherwise swap the destination name for a symlink between link and chmod.
    staged.unlink()
    _fsync_directory(destination.parent)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _minimum_container_size() -> int:
    return _HEADER.size + SALT_BYTES + NONCE_BYTES + 1 + TAG_BYTES


def _maximum_container_size() -> int:
    return (
        _HEADER.size
        + SALT_BYTES
        + NONCE_BYTES
        + MAX_METADATA_BYTES
        + MAX_PLAINTEXT_BYTES
        + TAG_BYTES
    )


__all__ = [
    "CONTAINER_SCHEMA",
    "CONTAINER_VERSION",
    "DEFAULT_SCRYPT_N",
    "EncryptedBackupAuthenticationError",
    "EncryptedBackupError",
    "EncryptedBackupFormatError",
    "EncryptedBackupIntegrityError",
    "EncryptedBackupMetadata",
    "MAX_SCRYPT_N",
    "MIN_SCRYPT_N",
    "decrypt_backup",
    "encrypt_backup",
]
