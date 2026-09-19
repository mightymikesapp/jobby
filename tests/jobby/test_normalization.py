from decimal import Decimal

import pytest

from jobby.normalization import (
    MAX_PERSISTED_SALARY,
    NormalizedSalary,
    RemoteStatus,
    SalaryPeriod,
    content_hash,
    extract_compensation,
    is_public_hostname,
    is_public_http_url,
    normalize_company,
    normalize_location,
    normalize_remote,
    normalize_salary,
    normalize_title,
    normalize_url,
)


def test_url_normalization_removes_tracking_fragment_and_default_port() -> None:
    url = "HTTP://WWW.Example.com:80/jobs//42/?utm_source=mail&b=2&a=1#apply"
    assert normalize_url(url) == "https://example.com/jobs/42?a=1&b=2"


def test_url_normalization_rejects_non_http_and_credentials_are_not_preserved() -> None:
    assert normalize_url("javascript:alert(1)") == ""
    assert (
        normalize_url("https://user:secret@example.com/job")
        == "https://example.com/job"
    )


def test_public_target_validation_rejects_local_and_malformed_hosts() -> None:
    assert is_public_hostname("jobs.example.com") is True
    assert is_public_hostname("93.184.216.34") is True
    for host in ("localhost", "service.internal", "127.0.0.1", "10.0.0.1", "::1"):
        assert is_public_hostname(host) is False
    assert is_public_http_url("https://jobs.example.com/role") is True
    assert is_public_http_url("https://user:secret@jobs.example.com/role") is False
    assert is_public_http_url("http://169.254.169.254/latest/meta-data") is False
    assert is_public_http_url("https://jobs.example.com:99999/role") is False


def test_company_title_and_location_normalization() -> None:
    assert normalize_company("The Acmé, Inc.") == "acme"
    assert normalize_title("Sr. Product-Counsel") == "senior product counsel"
    assert normalize_location("San Francisco, CA") == normalize_location(
        "San Francisco, California"
    )
    assert normalize_location("Washington, DC") == "washington district of columbia"


def test_remote_normalization_uses_explicit_evidence() -> None:
    assert normalize_remote(True, location="Boston, MA") is RemoteStatus.REMOTE
    assert normalize_remote("flexible hybrid") is RemoteStatus.HYBRID
    assert (
        normalize_remote(None, location="Remote - United States") is RemoteStatus.REMOTE
    )
    assert normalize_remote("in-office") is RemoteStatus.ONSITE
    assert normalize_remote(None, location="Chicago, IL") is RemoteStatus.UNKNOWN


def test_salary_string_normalization_and_annualization() -> None:
    salary = normalize_salary("USD $120K - $150K annually")
    assert salary is not None
    assert salary.minimum == Decimal("120000")
    assert salary.maximum == Decimal("150000")
    assert salary.currency == "USD"
    assert salary.period is SalaryPeriod.YEAR
    assert salary.annual_maximum == Decimal("150000")

    hourly = normalize_salary("$45 per hour")
    assert hourly is not None
    assert hourly.minimum == Decimal("45")
    assert hourly.period is SalaryPeriod.HOUR
    assert hourly.annual_minimum == Decimal("93600")


def test_salary_mapping_and_upper_bound_only() -> None:
    mapped = normalize_salary(
        {
            "MinimumRange": "40",
            "MaximumRange": "55",
            "CurrencyCode": "usd",
            "RateIntervalCode": "hour",
        }
    )
    assert mapped is not None
    assert mapped.minimum == Decimal("40")
    assert mapped.maximum == Decimal("55")
    assert mapped.period is SalaryPeriod.HOUR

    upper = normalize_salary("up to £80k per year")
    assert upper is not None
    assert upper.minimum is None
    assert upper.maximum == Decimal("80000")
    assert upper.currency == "GBP"

    euro = normalize_salary("EUR 70,000 annually")
    assert euro is not None and euro.currency == "EUR"


def test_empty_or_non_numeric_salary_is_unknown() -> None:
    assert normalize_salary("") is None
    assert normalize_salary("competitive compensation") is None
    assert normalize_salary(float("nan")) is None
    assert normalize_salary(float("inf")) is None
    assert normalize_salary(-1) is None


def test_unpersistable_or_negative_salary_ranges_degrade_to_unknown() -> None:
    too_large = MAX_PERSISTED_SALARY + 1
    annualized_overflow = (MAX_PERSISTED_SALARY // 2_080) + 1

    assert normalize_salary(f"USD {too_large} annually") is None
    assert (
        normalize_salary(
            {
                "min": annualized_overflow,
                "currency": "USD",
                "period": "hour",
            }
        )
        is None
    )
    assert (
        normalize_salary({"min": -1, "max": 100, "currency": "USD", "period": "year"})
        is None
    )
    assert normalize_salary(Decimal("NaN")) is None
    with pytest.raises(ValueError, match="persistence range"):
        NormalizedSalary(
            Decimal(too_large),
            None,
            "USD",
            SalaryPeriod.YEAR,
            0.98,
        )


def test_compensation_currency_code_overrides_ambiguous_dollar_symbol() -> None:
    extracted = extract_compensation("CAD $120,000-$150,000 annually")
    canadian_symbol = extract_compensation("Salary: C$120,000-C$150,000 annually")
    australian_symbol = extract_compensation("Salary: A$120,000-A$150,000 annually")

    assert extracted.salary is not None
    assert extracted.salary.currency == "CAD"
    assert not extracted.salary.annualization_confident
    assert any("non-USD" in warning for warning in extracted.warnings)
    assert canadian_symbol.salary is not None
    assert canadian_symbol.salary.currency == "CAD"
    assert australian_symbol.salary is not None
    assert australian_symbol.salary.currency == "AUD"


def test_compensation_extraction_ignores_years_retirement_numbers_and_stipends() -> (
    None
):
    retirement = extract_compensation(
        "Compensation includes a 401(k), and the salary range is "
        "$120,000 to $150,000 per year."
    )
    dated = extract_compensation(
        "The 2026 base pay range is $130,000-$160,000 annually."
    )
    stipend = extract_compensation("Benefits include a $2,000 wellness stipend.")

    assert retirement.salary is not None
    assert retirement.salary.minimum == Decimal("120000")
    assert retirement.salary.maximum == Decimal("150000")
    assert dated.salary is not None
    assert dated.salary.minimum == Decimal("130000")
    assert dated.salary.maximum == Decimal("160000")
    assert stipend.salary is None


def test_content_hash_normalizes_html_case_and_whitespace() -> None:
    assert content_hash("<p>Hello&nbsp; World</p>") == content_hash(" hello world ")
    assert content_hash("") == ""
