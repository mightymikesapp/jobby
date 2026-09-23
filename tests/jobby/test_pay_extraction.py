from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from jobby.db import Database
from jobby.models import Job
from jobby.normalization import SalaryPeriod, extract_compensation
from jobby.scanner import Scanner
from jobby.sources.base import ScanItem, ScanStatus, SourceResult


@pytest.mark.parametrize(
    ("text", "low", "high", "period"),
    [
        # A period word elsewhere in the passage must not set the pay period.
        (
            "Generous sick days, vacation days, holidays, and Impact Day 401(k) "
            "company matching Compensation $50,000 - $55,000 EEO Statement.",
            50_000,
            55_000,
            SalaryPeriod.YEAR,
        ),
        (
            "The base salary range for this position is: $141,600 — $177,000 USD. "
            "Our policy requires a 90-day waiting period before reapplying.",
            141_600,
            177_000,
            SalaryPeriod.YEAR,
        ),
        (
            "The hourly rate for this position is $37.42 - $45.60.",
            37.42,
            45.60,
            SalaryPeriod.HOUR,
        ),
        ("Pay: $25 per hour.", 25, None, SalaryPeriod.HOUR),
        # The pay word sat on the previous line; an hourly rate is still pay.
        (
            "US based candidates: $50/hour - $100/hour depending on experience.",
            50,
            None,
            SalaryPeriod.HOUR,
        ),
        (
            "The annual base salary range is $120,000 - $150,000.",
            120_000,
            150_000,
            SalaryPeriod.YEAR,
        ),
        ("Salary: $140,000/yr to $165,000/yr", 140_000, None, SalaryPeriod.YEAR),
        ("Compensation: $120K - $150K", 120_000, 150_000, SalaryPeriod.YEAR),
    ],
)
def test_pay_period_comes_from_the_words_next_to_the_amount(
    text: str, low: float, high: float | None, period: SalaryPeriod
) -> None:
    salary = extract_compensation(text).salary

    assert salary is not None
    assert salary.minimum == Decimal(str(low))
    assert salary.maximum == (Decimal(str(high)) if high is not None else None)
    assert salary.period is period


@pytest.mark.parametrize(
    "text",
    [
        "Experience managing large-scale paid media budgets ($10M+ annually).",
        "Achieving revenue targets >$2M per year for more than 3 years.",
        "It has 32 staff members and an annual budget of $4 million.",
        "A workplace savings scheme and a £50 monthly allowance for wellness.",
        "Earn up to $500 referral bonus.",
    ],
)
def test_money_that_is_not_pay_is_not_a_salary(text: str) -> None:
    assert extract_compensation(text).salary is None


def test_foreign_currency_keeps_its_currency() -> None:
    salary = extract_compensation(
        "Estimated annual salary is between SEK 1,342,000 - SEK 1,846,000."
    ).salary

    assert salary is not None
    assert salary.currency == "SEK"


def test_a_rescan_clears_pay_an_earlier_extraction_misread(tmp_path) -> None:
    database = Database(tmp_path / "jobby.sqlite3")
    database.initialize()
    try:

        def item(description: str) -> ScanItem:
            return ScanItem(
                source="greenhouse:example",
                source_id="1",
                company="Example",
                title="Growth Marketing Manager",
                url="https://jobs.example.test/1",
                description=description,
            )

        class Source:
            source_key = "greenhouse:example"

            def __init__(self, description: str) -> None:
                self.description = description

            def scan(self, query=None) -> SourceResult:
                return SourceResult(
                    source=self.source_key,
                    status=ScanStatus.SUCCEEDED,
                    items=(item(self.description),),
                )

        with database.session() as session:
            # Simulate a value stored by the old extractor.
            Scanner(database).scan([Source("Salary $90,000 - $110,000 per year.")])
            job = session.scalar(select(Job))
            assert job is not None and job.salary_min == 90_000

        Scanner(database).scan(
            [Source("Experience managing paid media budgets ($10M+ annually).")]
        )
        with database.session() as session:
            job = session.scalar(select(Job))
            assert job is not None
            assert job.salary_min is None and job.salary_max is None
            assert job.compensation_period == "unknown"
    finally:
        database.dispose()


def test_pay_range_in_double_escaped_html_is_read_whole() -> None:
    # Greenhouse serves content as escaped HTML; the range spans several tags.
    content = (
        "&lt;div class=&quot;title&quot;&gt;Local Pay Range&lt;/div&gt;"
        "&lt;div class=&quot;pay-range&quot;&gt;&lt;span&gt;$166,000&lt;/span&gt;"
        "&lt;span class=&quot;divider&quot;&gt;&amp;mdash;&lt;/span&gt;"
        "&lt;span&gt;$225,000 USD&lt;/span&gt;&lt;/div&gt;"
    )
    salary = extract_compensation(content).salary
    assert salary is not None
    assert (salary.minimum, salary.maximum) == (Decimal("166000"), Decimal("225000"))
    assert salary.period is SalaryPeriod.YEAR
