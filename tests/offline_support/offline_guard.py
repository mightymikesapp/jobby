"""Process-wide test guard that permits only Unix and IP loopback sockets."""

from __future__ import annotations

import ipaddress
import socket
from typing import Any


class LiveNetworkBlockedError(OSError):
    """A test attempted DNS resolution or a non-loopback socket operation."""


_INSTALLED = False
_ORIGINAL_CONNECT = socket.socket.connect
_ORIGINAL_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_SENDTO = socket.socket.sendto
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_GETHOSTBYNAME = socket.gethostbyname
_ORIGINAL_GETHOSTBYNAME_EX = socket.gethostbyname_ex
_ORIGINAL_GETHOSTBYADDR = socket.gethostbyaddr


def _loopback_host(value: object) -> bool:
    if value is None or value == "":
        return True
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return False
    if not isinstance(value, str):
        return False
    normalized = value.strip().casefold().rstrip(".")
    if normalized == "localhost" or normalized.endswith(".localhost"):
        return True
    if normalized.startswith("[") and normalized.endswith("]"):
        normalized = normalized[1:-1]
    if "%" in normalized:
        normalized = normalized.split("%", maxsplit=1)[0]
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    if address.is_loopback:
        return True
    mapped = getattr(address, "ipv4_mapped", None)
    return bool(mapped is not None and mapped.is_loopback)


def _allow_socket_address(sock: socket.socket, address: object) -> bool:
    if sock.family == socket.AF_UNIX:
        return True
    return isinstance(address, tuple) and bool(address) and _loopback_host(address[0])


def _blocked(operation: str, target: object) -> LiveNetworkBlockedError:
    return LiveNetworkBlockedError(
        f"live network disabled during tests: {operation} {target!r}"
    )


def _guarded_connect(sock: socket.socket, address: Any) -> None:
    if not _allow_socket_address(sock, address):
        raise _blocked("connect", address)
    _ORIGINAL_CONNECT(sock, address)


def _guarded_connect_ex(sock: socket.socket, address: Any) -> int:
    if not _allow_socket_address(sock, address):
        raise _blocked("connect_ex", address)
    return _ORIGINAL_CONNECT_EX(sock, address)


def _guarded_sendto(sock: socket.socket, data: bytes, *args: Any) -> int:
    address = args[-1] if args else None
    if not _allow_socket_address(sock, address):
        raise _blocked("sendto", address)
    return _ORIGINAL_SENDTO(sock, data, *args)


def _guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
    if not _loopback_host(host):
        raise _blocked("getaddrinfo", host)
    return _ORIGINAL_GETADDRINFO(host, *args, **kwargs)


def _guarded_gethostbyname(host: Any) -> str:
    if not _loopback_host(host):
        raise _blocked("gethostbyname", host)
    return _ORIGINAL_GETHOSTBYNAME(host)


def _guarded_gethostbyname_ex(host: Any) -> Any:
    if not _loopback_host(host):
        raise _blocked("gethostbyname_ex", host)
    return _ORIGINAL_GETHOSTBYNAME_EX(host)


def _guarded_gethostbyaddr(host: Any) -> Any:
    if not _loopback_host(host):
        raise _blocked("gethostbyaddr", host)
    return _ORIGINAL_GETHOSTBYADDR(host)


def install_offline_network_guard() -> None:
    """Install the idempotent guard in the current Python process."""

    global _INSTALLED
    if _INSTALLED:
        return
    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
    socket.socket.sendto = _guarded_sendto
    socket.getaddrinfo = _guarded_getaddrinfo
    socket.gethostbyname = _guarded_gethostbyname
    socket.gethostbyname_ex = _guarded_gethostbyname_ex
    socket.gethostbyaddr = _guarded_gethostbyaddr
    _INSTALLED = True


__all__ = [
    "LiveNetworkBlockedError",
    "install_offline_network_guard",
]
