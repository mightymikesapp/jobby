import job_monitor


def test_load_manual_watchlist_reads_enabled_portals(tmp_path, monkeypatch):
    portals_file = tmp_path / "portals.yml"
    portals_file.write_text(
        """
tracked_companies:
  - name: Active Co
    careers_url: https://example.com/jobs
    notes: legal roles
    enabled: true
  - name: Disabled Co
    careers_url: https://example.com/disabled
    notes: skip me
    enabled: false
"""
    )
    monkeypatch.setattr(job_monitor, "PORTALS_FILE", portals_file)

    result = job_monitor.load_manual_watchlist()

    assert result == [("Active Co", "https://example.com/jobs", "legal roles")]


def test_load_manual_watchlist_falls_back_without_yaml(monkeypatch):
    fallback = [("Fallback Co", "https://example.com", "manual")]
    monkeypatch.setattr(job_monitor, "yaml", None)
    monkeypatch.setattr(job_monitor, "MANUAL_WATCHLIST_FALLBACK", fallback)

    result = job_monitor.load_manual_watchlist()

    assert result == fallback
