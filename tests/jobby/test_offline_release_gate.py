"""Mechanical checks for the release suite's no-live-network boundary."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys

import httpx
import pytest

from offline_guard import LiveNetworkBlockedError


def test_live_dns_and_ip_connections_are_blocked() -> None:
    assert os.environ["JOBBY_TEST_OFFLINE"] == "1"
    with pytest.raises(LiveNetworkBlockedError, match="getaddrinfo"):
        socket.getaddrinfo("example.com", 443)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        with pytest.raises(LiveNetworkBlockedError, match="connect"):
            client.connect(("93.184.216.34", 443))


def test_loopback_and_mock_transport_remain_available() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        server.settimeout(2)
        with socket.create_connection(server.getsockname(), timeout=2) as client:
            connection, _address = server.accept()
            with connection:
                client.sendall(b"offline")
                assert connection.recv(7) == b"offline"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json={"mocked": True})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert client.get("https://never-resolved.example.test").json() == {
            "mocked": True
        }


def test_child_python_process_inherits_the_offline_guard() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import socket; "
                "socket.create_connection(('93.184.216.34', 443), timeout=1)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode != 0
    assert "live network disabled during tests" in result.stderr


def test_release_workflow_enforces_offline_tests_and_exact_scale_baseline() -> None:
    root = Path(__file__).resolve().parents[2]
    workflow = (root / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert 'JOBBY_TEST_OFFLINE: "1"' in workflow
    assert "tests/offline_support" in workflow
    assert "--profile release" in workflow
    assert "--confirm-exact-scale" in workflow
    assert "release-0.6-macos-arm64-python313.json" in workflow
    assert "matrix.os == 'macos-15' && matrix.python == '3.13'" in workflow
