from __future__ import annotations

import job_monitor


def test_legacy_command_delegates_to_sqlite_without_mutating_legacy_state(
    monkeypatch, capsys
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr("jobby.cli.main", lambda argv: calls.append(list(argv)) or 0)
    monkeypatch.setattr(
        job_monitor,
        "run",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy mutable scanner must not run")
        ),
    )

    result = job_monitor.compatibility_main(
        ["--reset", "--score", "2", "--pipeline", "--fetch-jds"]
    )

    assert result == 0
    assert calls == [["scan"]]
    assert "no longer write JSON, reports, JDs, or pipeline Markdown" in (
        capsys.readouterr().err
    )
