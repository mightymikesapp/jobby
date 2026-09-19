from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest


pytest.importorskip("cryptography")

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from jobby import encrypted_backup as encrypted


PASSPHRASE = "correct horse battery staple"


def _source(tmp_path: Path, *, size: int = 25_019) -> tuple[Path, bytes]:
    block = bytes(range(256))
    payload = (block * (size // len(block) + 1))[:size]
    source = tmp_path / "backup.zip"
    source.write_bytes(payload)
    return source, payload


def _container_parts(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    fields = encrypted._HEADER.unpack(raw[: encrypted._HEADER.size])
    metadata_size = fields[7]
    plaintext_size = fields[8]
    salt_start = encrypted._HEADER.size
    nonce_start = salt_start + encrypted.SALT_BYTES
    metadata_start = nonce_start + encrypted.NONCE_BYTES
    ciphertext_start = metadata_start + metadata_size
    tag_start = ciphertext_start + plaintext_size
    return {
        "raw": raw,
        "fields": fields,
        "salt_start": salt_start,
        "nonce_start": nonce_start,
        "metadata_start": metadata_start,
        "metadata_size": metadata_size,
        "ciphertext_start": ciphertext_start,
        "plaintext_size": plaintext_size,
        "tag_start": tag_start,
    }


def _copy_with_mutation(source: Path, destination: Path, offset: int) -> Path:
    payload = bytearray(source.read_bytes())
    payload[offset] ^= 0x01
    destination.write_bytes(payload)
    return destination


def test_streaming_round_trip_has_versioned_authenticated_metadata_and_randomness(
    tmp_path: Path,
) -> None:
    source, payload = _source(tmp_path, size=encrypted.MIN_CHUNK_BYTES * 3 + 137)
    first = tmp_path / "first.jobbyenc"
    second = tmp_path / "second.jobbyenc"

    assert (
        encrypted.encrypt_backup(
            source,
            first,
            PASSPHRASE,
            chunk_size=encrypted.MIN_CHUNK_BYTES,
        )
        == first
    )
    encrypted.encrypt_backup(
        source,
        second,
        PASSPHRASE,
        chunk_size=encrypted.MIN_CHUNK_BYTES,
    )

    parts = _container_parts(first)
    fields = parts["fields"]
    assert fields[0] == encrypted.MAGIC
    assert fields[1] == encrypted.CONTAINER_VERSION
    assert fields[2:7] == (
        0,
        encrypted.SCRYPT_LOG_N,
        encrypted.SCRYPT_R,
        encrypted.SCRYPT_P,
        0,
    )
    raw = parts["raw"]
    metadata_start = int(parts["metadata_start"])
    metadata_end = metadata_start + int(parts["metadata_size"])
    metadata = json.loads(raw[metadata_start:metadata_end])
    assert metadata["schema"] == encrypted.CONTAINER_SCHEMA
    assert metadata["version"] == encrypted.CONTAINER_VERSION
    assert metadata["algorithms"] == {
        "cipher": "AES-256-GCM",
        "kdf": "scrypt",
    }
    assert metadata["plaintext"] == {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }
    assert stat.S_IMODE(first.stat().st_mode) == 0o600
    assert first.read_bytes() != second.read_bytes()

    restored = tmp_path / "restored.zip"
    assert (
        encrypted.decrypt_backup(
            first,
            restored,
            PASSPHRASE,
            chunk_size=encrypted.MIN_CHUNK_BYTES,
        )
        == restored
    )
    assert restored.read_bytes() == payload
    assert stat.S_IMODE(restored.stat().st_mode) == 0o600


def test_empty_plaintext_round_trips(tmp_path: Path) -> None:
    source = tmp_path / "empty.zip"
    source.write_bytes(b"")
    container = encrypted.encrypt_backup(source, tmp_path / "empty.enc", PASSPHRASE)
    restored = encrypted.decrypt_backup(
        container, tmp_path / "empty-restored.zip", PASSPHRASE
    )
    assert restored.read_bytes() == b""


@pytest.mark.parametrize(
    ("scrypt_n", "expected_log_n"),
    [
        (encrypted.MIN_SCRYPT_N, encrypted.MIN_SCRYPT_LOG_N),
        (encrypted.MAX_SCRYPT_N, encrypted.MAX_SCRYPT_LOG_N),
    ],
)
def test_bounded_configurable_scrypt_cost_is_authenticated_and_round_trips(
    tmp_path: Path,
    scrypt_n: int,
    expected_log_n: int,
) -> None:
    source, payload = _source(tmp_path, size=101)
    container = encrypted.encrypt_backup(
        source,
        tmp_path / f"backup-{scrypt_n}.enc",
        PASSPHRASE,
        scrypt_n=scrypt_n,
    )

    fields = _container_parts(container)["fields"]
    assert fields[3] == expected_log_n
    restored = encrypted.decrypt_backup(
        container,
        tmp_path / f"restored-{scrypt_n}.zip",
        PASSPHRASE,
    )
    assert restored.read_bytes() == payload


def test_in_range_scrypt_header_tampering_fails_authentication(
    tmp_path: Path,
) -> None:
    source, _payload = _source(tmp_path, size=101)
    container = encrypted.encrypt_backup(
        source,
        tmp_path / "backup.enc",
        PASSPHRASE,
    )
    raw = bytearray(container.read_bytes())
    raw[len(encrypted.MAGIC) + 2] = encrypted.MIN_SCRYPT_LOG_N
    tampered = tmp_path / "tampered-scrypt.enc"
    tampered.write_bytes(raw)
    destination = tmp_path / "must-not-exist.zip"

    with pytest.raises(encrypted.EncryptedBackupAuthenticationError):
        encrypted.decrypt_backup(tampered, destination, PASSPHRASE)

    assert not destination.exists()


def test_wrong_passphrase_is_generic_and_never_publishes_plaintext(
    tmp_path: Path,
) -> None:
    source, _payload = _source(tmp_path)
    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    destination = tmp_path / "should-not-exist.zip"
    wrong_secret = "DO-NOT-LEAK-THIS-PASSPHRASE"

    with pytest.raises(encrypted.EncryptedBackupAuthenticationError) as caught:
        encrypted.decrypt_backup(container, destination, wrong_secret)

    assert wrong_secret not in str(caught.value)
    assert wrong_secret not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert not destination.exists()
    assert not list(tmp_path.glob(f".{destination.name}.decrypted-*.tmp"))


@pytest.mark.parametrize("area", ["salt", "nonce", "metadata", "ciphertext", "tag"])
def test_authenticated_container_rejects_tampering_without_output(
    tmp_path: Path, area: str
) -> None:
    source, _payload = _source(tmp_path)
    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    parts = _container_parts(container)
    offsets = {
        "salt": int(parts["salt_start"]),
        "nonce": int(parts["nonce_start"]),
        "metadata": int(parts["metadata_start"]) + 4,
        "ciphertext": int(parts["ciphertext_start"]) + 3,
        "tag": int(parts["tag_start"]) + 2,
    }
    tampered = _copy_with_mutation(
        container, tmp_path / f"tampered-{area}.enc", offsets[area]
    )
    output = tmp_path / f"tampered-{area}.zip"

    with pytest.raises(encrypted.EncryptedBackupError):
        encrypted.decrypt_backup(tampered, output, PASSPHRASE)

    assert not output.exists()


@pytest.mark.parametrize("removed", [1, encrypted.TAG_BYTES, 100, 1_000])
def test_truncated_containers_are_rejected_before_publication(
    tmp_path: Path, removed: int
) -> None:
    source, _payload = _source(tmp_path)
    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    raw = container.read_bytes()
    truncated = tmp_path / f"truncated-{removed}.enc"
    truncated.write_bytes(raw[:-removed])
    destination = tmp_path / f"truncated-{removed}.zip"

    with pytest.raises(encrypted.EncryptedBackupFormatError):
        encrypted.decrypt_backup(truncated, destination, PASSPHRASE)

    assert not destination.exists()


def test_corrupt_header_version_parameters_and_trailing_data_are_rejected(
    tmp_path: Path,
) -> None:
    source, _payload = _source(tmp_path)
    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    raw = container.read_bytes()

    corruptions: dict[str, bytes] = {}
    bad_magic = bytearray(raw)
    bad_magic[0] ^= 0x01
    corruptions["magic"] = bytes(bad_magic)
    bad_version = bytearray(raw)
    bad_version[len(encrypted.MAGIC)] = encrypted.CONTAINER_VERSION + 1
    corruptions["version"] = bytes(bad_version)
    bad_kdf = bytearray(raw)
    bad_kdf[len(encrypted.MAGIC) + 2] = encrypted.MAX_SCRYPT_LOG_N + 1
    corruptions["kdf"] = bytes(bad_kdf)
    corruptions["trailing"] = raw + b"unexpected"

    for name, corrupted in corruptions.items():
        candidate = tmp_path / f"bad-{name}.enc"
        candidate.write_bytes(corrupted)
        output = tmp_path / f"bad-{name}.zip"
        with pytest.raises(encrypted.EncryptedBackupFormatError):
            encrypted.decrypt_backup(candidate, output, PASSPHRASE)
        assert not output.exists()


def test_authenticated_but_false_plaintext_checksum_is_rejected(
    tmp_path: Path,
) -> None:
    """Exercise checksum verification independently of the GCM tag check."""

    source, payload = _source(tmp_path)
    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    parts = _container_parts(container)
    raw = parts["raw"]
    metadata_start = int(parts["metadata_start"])
    metadata_end = metadata_start + int(parts["metadata_size"])
    ciphertext_start = int(parts["ciphertext_start"])
    tag_start = int(parts["tag_start"])
    old_prefix = raw[:ciphertext_start]
    salt = raw[int(parts["salt_start"]) : int(parts["nonce_start"])]
    nonce = raw[int(parts["nonce_start"]) : metadata_start]
    ciphertext = raw[ciphertext_start:tag_start]
    tag = raw[tag_start:]
    key = encrypted._derive_key(PASSPHRASE.encode("utf-8"), salt)
    decryptor = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
    decryptor.authenticate_additional_data(old_prefix)
    assert decryptor.update(ciphertext) + decryptor.finalize() == payload

    metadata = json.loads(raw[metadata_start:metadata_end])
    false_digest = "0" * 64
    assert false_digest != hashlib.sha256(payload).hexdigest()
    metadata["plaintext"]["sha256"] = false_digest
    metadata_bytes = json.dumps(
        metadata,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert len(metadata_bytes) == int(parts["metadata_size"])
    new_nonce = os.urandom(encrypted.NONCE_BYTES)
    header = encrypted._pack_header(len(metadata_bytes), len(payload))
    new_prefix = header + salt + new_nonce + metadata_bytes
    encryptor = Cipher(algorithms.AES(key), modes.GCM(new_nonce)).encryptor()
    encryptor.authenticate_additional_data(new_prefix)
    forged_ciphertext = encryptor.update(payload) + encryptor.finalize()
    forged = tmp_path / "forged-checksum.enc"
    forged.write_bytes(new_prefix + forged_ciphertext + encryptor.tag)

    destination = tmp_path / "forged-checksum.zip"
    with pytest.raises(encrypted.EncryptedBackupIntegrityError):
        encrypted.decrypt_backup(forged, destination, PASSPHRASE)
    assert not destination.exists()


def test_sources_destinations_and_parent_directories_reject_symlinks(
    tmp_path: Path,
) -> None:
    source, _payload = _source(tmp_path)
    source_link = tmp_path / "source-link.zip"
    source_link.symlink_to(source)
    with pytest.raises(ValueError, match="symbolic link"):
        encrypted.encrypt_backup(
            source_link, tmp_path / "linked-source.enc", PASSPHRASE
        )

    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    container_link = tmp_path / "container-link.enc"
    container_link.symlink_to(container)
    with pytest.raises(ValueError, match="symbolic link"):
        encrypted.decrypt_backup(
            container_link, tmp_path / "linked-container.zip", PASSPHRASE
        )

    victim = tmp_path / "victim"
    victim.write_bytes(b"preserve me")
    destination_link = tmp_path / "destination-link.enc"
    destination_link.symlink_to(victim)
    with pytest.raises(ValueError, match="symbolic link"):
        encrypted.encrypt_backup(source, destination_link, PASSPHRASE)
    assert victim.read_bytes() == b"preserve me"

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(ValueError, match="parent"):
        encrypted.encrypt_backup(
            source, linked_parent / "through-parent.enc", PASSPHRASE
        )
    assert not (real_parent / "through-parent.enc").exists()


def test_existing_destinations_are_preserved_for_encrypt_and_decrypt(
    tmp_path: Path,
) -> None:
    source, _payload = _source(tmp_path)
    existing_container = tmp_path / "existing.enc"
    existing_container.write_bytes(b"preserve encrypted destination")
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        encrypted.encrypt_backup(source, existing_container, PASSPHRASE)
    assert existing_container.read_bytes() == b"preserve encrypted destination"

    container = encrypted.encrypt_backup(source, tmp_path / "backup.enc", PASSPHRASE)
    existing_plaintext = tmp_path / "existing.zip"
    existing_plaintext.write_bytes(b"preserve plaintext destination")
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        encrypted.decrypt_backup(container, existing_plaintext, PASSPHRASE)
    assert existing_plaintext.read_bytes() == b"preserve plaintext destination"


def test_publication_failure_cleans_staging_file_and_leaves_no_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _payload = _source(tmp_path)
    destination = tmp_path / "publication-failed.enc"

    def fail_publication(_staged: Path, _destination: Path) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(encrypted, "_publish_no_clobber", fail_publication)
    with pytest.raises(OSError, match="injected publication failure"):
        encrypted.encrypt_backup(source, destination, PASSPHRASE)

    assert not destination.exists()
    assert not list(tmp_path.glob(f".{destination.name}.encrypted-*.tmp"))


def test_strict_input_and_resource_bounds_are_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _payload = _source(tmp_path, size=32)

    for invalid in (
        True,
        0,
        encrypted.MIN_CHUNK_BYTES - 1,
        encrypted.MAX_CHUNK_BYTES + 1,
    ):
        with pytest.raises((TypeError, ValueError)):
            encrypted.encrypt_backup(
                source,
                tmp_path / f"chunk-{invalid}.enc",
                PASSPHRASE,
                chunk_size=invalid,
            )
    for invalid_passphrase in ("", b"", "x" * (encrypted.MAX_PASSPHRASE_BYTES + 1)):
        with pytest.raises(ValueError, match="passphrase"):
            encrypted.encrypt_backup(
                source,
                tmp_path / f"pass-{len(invalid_passphrase)}.enc",
                invalid_passphrase,
            )
    with pytest.raises(TypeError, match="passphrase"):
        encrypted.encrypt_backup(source, tmp_path / "bad-type.enc", object())

    for invalid_scrypt_n in (
        True,
        0,
        encrypted.MIN_SCRYPT_N + 1,
        encrypted.MAX_SCRYPT_N * 2,
    ):
        with pytest.raises((TypeError, ValueError), match="Scrypt N"):
            encrypted.encrypt_backup(
                source,
                tmp_path / f"scrypt-{invalid_scrypt_n}.enc",
                PASSPHRASE,
                scrypt_n=invalid_scrypt_n,
            )

    monkeypatch.setattr(encrypted, "MAX_PLAINTEXT_BYTES", 8)
    with pytest.raises(ValueError, match="size limit"):
        encrypted.encrypt_backup(source, tmp_path / "oversized.enc", PASSPHRASE)
    assert not (tmp_path / "oversized.enc").exists()


def test_non_regular_sources_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="regular file"):
        encrypted.encrypt_backup(tmp_path, tmp_path / "directory.enc", PASSPHRASE)
