"""
Tests for load_state() and save_state() file I/O.
All tests use tmp_path to avoid touching job_monitor_state.json on disk.
"""

import json

import job_monitor


def test_returns_default_when_no_file(tmp_path, monkeypatch):
    """Missing state file returns the default empty-state dict."""
    monkeypatch.setattr(job_monitor, "STATE_FILE", tmp_path / "nonexistent.json")
    result = job_monitor.load_state()
    assert result == {"seen_jobs": [], "last_run": None}


def test_reads_existing_file(tmp_path, monkeypatch):
    """Existing valid JSON file is loaded and returned as-is."""
    state_file = tmp_path / "state.json"
    data = {"seen_jobs": ["gh_anthropic_001"], "last_run": "2026-03-14T10:00:00"}
    state_file.write_text(json.dumps(data))
    monkeypatch.setattr(job_monitor, "STATE_FILE", state_file)

    result = job_monitor.load_state()
    assert result == data


def test_creates_file_with_json(tmp_path, monkeypatch):
    """save_state writes a valid JSON file that can be read back."""
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(job_monitor, "STATE_FILE", state_file)
    data = {"seen_jobs": ["gh_anthropic_001"], "last_run": "2026-03-14T10:00:00"}

    job_monitor.save_state(data)

    assert state_file.exists()
    saved = json.loads(state_file.read_text())
    assert saved == data


def test_roundtrip(tmp_path, monkeypatch):
    """save_state then load_state returns an identical dict."""
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(job_monitor, "STATE_FILE", state_file)
    data = {
        "seen_jobs": ["gh_anthropic_001", "lever_spotify_002"],
        "last_run": "2026-03-14",
    }

    job_monitor.save_state(data)
    loaded = job_monitor.load_state()
    assert loaded == data
