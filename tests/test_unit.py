"""
Pure function tests for score_title() and is_excluded().

Hypothesis property tests guard against regressions on arbitrary input.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import job_monitor


class TestScoreTitle:
    @pytest.mark.parametrize(
        "title,min_score,must_include",
        [
            ("IP Counsel", 2, ["IP", "counsel"]),
            ("Patent Attorney", 1, ["patent"]),
            ("Policy Analyst", 1, ["policy analyst"]),
            ("Research Fellow", 1, ["research fellow"]),
            ("Copyright Coordinator", 2, ["copyright", "coordinator"]),
            ("Legal Operations Manager", 1, ["legal operations"]),
            ("Content Policy Manager", 2, ["content policy", "policy manager"]),
            ("Business Affairs Coordinator", 2, ["business affairs", "coordinator"]),
        ],
    )
    def test_score_title_known_matches(self, title, min_score, must_include):
        score, matched = job_monitor.score_title(title)
        assert score >= min_score
        for kw in must_include:
            assert kw in matched

    @pytest.mark.parametrize(
        "title,expect_ip",
        [
            ("IP Counsel", True),  # standalone uppercase IP
            ("ip counsel", False),  # lowercase — IP_PATTERN is case-sensitive
            ("VIP Manager", False),  # no word boundary before "I" in "VIP"
            ("Principal Attorney", False),  # no uppercase "IP" substring
        ],
    )
    def test_ip_pattern_case_sensitivity(self, title, expect_ip):
        _, matched = job_monitor.score_title(title)
        assert ("IP" in matched) == expect_ip

    def test_score_title_no_overlap_inflation(self):
        """
        'Legal Counsel' now scores 1 — phrase match subsumes its constituent words.
        Words covered by a matched phrase are not double-counted.
        """
        score, matched = job_monitor.score_title("Legal Counsel")
        assert score == 1
        assert "legal counsel" in matched
        assert "legal" not in matched
        assert "counsel" not in matched

    def test_score_title_none_returns_empty(self):
        """score_title(None) returns (0, []) — None guard prevents the crash."""
        score, matched = job_monitor.score_title(None)
        assert score == 0
        assert matched == []

    def test_score_title_empty_string(self):
        score, matched = job_monitor.score_title("")
        assert score == 0
        assert matched == []

    @pytest.mark.parametrize(
        "title",
        [
            "Software Engineer",
            "Data Scientist",
            "Product Designer",
            "DevOps Engineer",
            "Marketing Manager",
        ],
    )
    def test_score_title_irrelevant_roles(self, title):
        score, _ = job_monitor.score_title(title)
        assert score == 0

    @given(st.text())
    @settings(max_examples=500)
    def test_score_title_never_crashes(self, title):
        """Hypothesis: score_title never crashes on any string input."""
        score, matched = job_monitor.score_title(title)
        assert score >= 0

    @given(st.text())
    @settings(max_examples=500)
    def test_score_equals_matched_length(self, title):
        """Invariant: the returned score always equals len(matched)."""
        score, matched = job_monitor.score_title(title)
        assert score == len(matched)


class TestIsExcluded:
    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Senior Counsel", True),
            ("Director of Legal Affairs", True),
            ("VP Legal", True),
            ("VP, Legal Affairs", True),  # comma after VP — word boundary fix
            ("Sr. Associate", True),
            ("Head of Policy", True),
            ("Principal Attorney", True),
            ("Chief Officer, Legal", True),
            ("Vice President, Legal", True),
            (
                "Advisor, Legal Affairs",
                True,
            ),  # title-start Advisor — now correctly excluded
            ("Attorney Advisor", False),  # federal GS-0905 entry point; keep visible
            ("Policy Advisor", False),  # policy-adjacent role; keep visible
            ("Policy Analyst", False),
            ("Research Fellow", False),
            ("Legal Counsel", False),
            ("Junior Analyst", False),
            ("Associate General Counsel", False),
        ],
    )
    def test_is_excluded_parametrized(self, title, expected):
        assert job_monitor.is_excluded(title) == expected

    def test_is_excluded_head_of_no_false_positive(self):
        """
        'Ahead of the Curve Analyst' no longer triggers is_excluded.
        Word-boundary matching on 'head of' prevents the substring false positive.
        """
        result = job_monitor.is_excluded("Ahead of the Curve Analyst")
        assert not result

    def test_is_excluded_advisor_at_title_start(self):
        """
        'Advisor, Legal Affairs' is excluded, but compound titles like
        'Attorney Advisor' remain visible.
        """
        result = job_monitor.is_excluded("Advisor, Legal Affairs")
        assert result

    def test_is_excluded_keeps_attorney_advisor(self):
        """Federal Attorney Advisor roles are a target category, not seniority noise."""
        result = job_monitor.is_excluded("Attorney Advisor")
        assert not result

    @pytest.mark.parametrize(
        "title",
        [
            "SENIOR Counsel",
            "senior counsel",
            "Senior Counsel",
        ],
    )
    def test_is_excluded_case_insensitive(self, title):
        assert job_monitor.is_excluded(title)

    @given(st.text())
    @settings(max_examples=500)
    def test_is_excluded_never_crashes(self, title):
        """Hypothesis: is_excluded always returns a bool and never raises."""
        result = job_monitor.is_excluded(title)
        assert isinstance(result, bool)
