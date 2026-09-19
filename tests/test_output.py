"""
Tests for print_job() and save_markdown() output functions.
"""

import pytest
from datetime import date

import job_monitor


@pytest.fixture
def zero_score_job():
    return {
        "uid": "gh_anthropic_002",
        "company": "Anthropic",
        "title": "Administrative Coordinator",
        "url": "https://boards.greenhouse.io/anthropic/jobs/002",
        "score": 0,
        "matched_keywords": [],
        "source": "gh",
        "found": str(date.today()),
    }


class TestPrintJob:
    def test_high_score_shows_star(self, capsys, sample_job):
        job_monitor.print_job(sample_job)
        out = capsys.readouterr().out
        assert "★" in out

    def test_zero_score_shows_dot(self, capsys, zero_score_job):
        job_monitor.print_job(zero_score_job)
        out = capsys.readouterr().out
        assert "·" in out

    def test_url_on_second_line(self, capsys, sample_job):
        job_monitor.print_job(sample_job)
        lines = capsys.readouterr().out.splitlines()
        assert sample_job["url"] in lines[1]

    def test_keywords_in_brackets(self, capsys, sample_job):
        job_monitor.print_job(sample_job)
        out = capsys.readouterr().out
        assert "[IP, counsel]" in out

    def test_close_date_printed_when_present(self, capsys, sample_job):
        job = {**sample_job, "close_date": "2026-04-01"}
        job_monitor.print_job(job)
        out = capsys.readouterr().out
        assert "2026-04-01" in out

    def test_no_keywords_line_when_empty(self, capsys, zero_score_job):
        job_monitor.print_job(zero_score_job)
        out = capsys.readouterr().out
        assert "[" not in out


class TestSaveMarkdown:
    def test_empty_list_no_file_written(self, tmp_path, monkeypatch):
        monkeypatch.setattr(job_monitor, "OUTPUT_DIR", tmp_path)
        job_monitor.save_markdown([])
        assert list(tmp_path.glob("*.md")) == []

    def test_nonempty_creates_dated_file(self, tmp_path, monkeypatch, sample_job):
        monkeypatch.setattr(job_monitor, "OUTPUT_DIR", tmp_path)
        job_monitor.save_markdown([sample_job])
        expected = tmp_path / f"new_jobs_{date.today()}.md"
        assert expected.exists()

    def test_jobs_sorted_by_score_descending(
        self, tmp_path, monkeypatch, sample_job, zero_score_job
    ):
        monkeypatch.setattr(job_monitor, "OUTPUT_DIR", tmp_path)
        # Pass zero_score_job first; expect high-score job to appear first in file
        job_monitor.save_markdown([zero_score_job, sample_job])
        content = (tmp_path / f"new_jobs_{date.today()}.md").read_text()
        ip_pos = content.index("IP Counsel")
        admin_pos = content.index("Administrative Coordinator")
        assert ip_pos < admin_pos

    def test_file_contains_title_company_url(self, tmp_path, monkeypatch, sample_job):
        monkeypatch.setattr(job_monitor, "OUTPUT_DIR", tmp_path)
        job_monitor.save_markdown([sample_job])
        content = (tmp_path / f"new_jobs_{date.today()}.md").read_text()
        assert sample_job["title"] in content
        assert sample_job["company"] in content
        assert sample_job["url"] in content
