from unittest.mock import patch

from jobby.dedup import DuplicateReason, compare_items, deduplicate, token_jaccard
from jobby.sources import ScanItem


def item(
    source_id: str,
    *,
    source: str = "greenhouse:acme",
    company: str = "Acme, Inc.",
    title: str = "Product Counsel",
    url: str | None = None,
    location: str = "San Francisco, CA",
    description: str = "",
) -> ScanItem:
    return ScanItem(
        source=source,
        source_id=source_id,
        company=company,
        title=title,
        url=url or f"https://example.test/jobs/{source_id}",
        location=location,
        description=description,
    )


def test_source_id_match_is_scoped_to_configured_source() -> None:
    left = item("42")
    same_source = item("42", url="https://other.test/new-url")
    other_source = item(
        "42",
        source="lever:other",
        company="Other Corp",
        title="Policy Analyst",
        url="https://other.test/lever-42",
    )

    assert compare_items(left, same_source) == (DuplicateReason.SOURCE_ID, 1.0)
    assert compare_items(left, other_source) is None


def test_canonical_url_match_ignores_tracking_and_scheme() -> None:
    left = item("a", url="http://www.example.test/jobs/a/?utm_source=email")
    right = item(
        "b",
        source="lever:acme",
        company="Different Company",
        url="https://example.test/jobs/a#apply",
    )
    assert compare_items(left, right) == (DuplicateReason.CANONICAL_URL, 1.0)


def test_content_hash_match_uses_normalized_description() -> None:
    left = item("a", description="<p>Advise the Product Team</p>")
    right = item(
        "b",
        source="lever:acme",
        company="Another Company",
        description=" advise the product team ",
    )
    assert compare_items(left, right) == (DuplicateReason.CONTENT_HASH, 1.0)


def test_normalized_company_title_and_location_match() -> None:
    left = item("a", title="Sr. Product Counsel", company="The Acme, Inc.")
    right = item(
        "b",
        source="lever:acme",
        title="Senior Product Counsel",
        company="Acme LLC",
        location="San Francisco, California",
    )
    assert compare_items(left, right) == (DuplicateReason.NORMALIZED_FIELDS, 1.0)


def test_fuzzy_token_jaccard_requires_same_company_and_threshold() -> None:
    left = item(
        "a",
        title="Privacy Product Counsel",
        location="Remote - United States",
    )
    reordered = item(
        "b",
        source="lever:acme",
        title="Product Counsel, Privacy",
        location="United States (Remote)",
    )
    other_company = item(
        "c",
        source="lever:other",
        company="Other Corp",
        title="Product Counsel, Privacy",
        location="United States (Remote)",
    )

    assert token_jaccard("privacy product counsel", "product counsel privacy") == 1.0
    assert compare_items(left, reordered) == (DuplicateReason.FUZZY, 1.0)
    assert compare_items(left, other_company) is None


def test_zero_fuzzy_threshold_considers_disjoint_same_company_titles() -> None:
    left = item("left", title="Counsel", location="New York, NY")
    right = item(
        "right",
        source="lever:acme",
        title="Engineer",
        location="Austin, TX",
    )

    assert compare_items(left, right, fuzzy_threshold=0) == (
        DuplicateReason.FUZZY,
        0.0,
    )
    result = deduplicate([left, right], fuzzy_threshold=0)
    assert result.unique == (left,)
    assert result.duplicates[0].duplicate == right


def test_fuzzy_match_accepts_exact_threshold_when_one_location_is_missing() -> None:
    left = item("a", title="Senior Privacy Product Legal Counsel", location="")
    right = item(
        "b",
        source="lever:acme",
        title="Senior Privacy Product Counsel",
        location="New York, NY",
    )
    reason, similarity = compare_items(left, right) or (None, 0.0)
    assert reason is DuplicateReason.FUZZY
    assert similarity == 0.8


def test_deduplicate_is_stable_and_reports_original_indexes() -> None:
    first = item("a")
    duplicate = item("a", url="https://other.test/a")
    distinct = item("c", company="Other", title="Policy Analyst")

    result = deduplicate([first, duplicate, distinct])

    assert result.unique == (first, distinct)
    assert len(result.duplicates) == 1
    match = result.duplicates[0]
    assert match.canonical_index == 0
    assert match.duplicate_index == 1
    assert match.reason is DuplicateReason.SOURCE_ID


def test_deduplicate_preserves_earliest_match_across_reason_precedence() -> None:
    fuzzy_first = item(
        "first",
        title="Privacy Product Counsel",
        url="https://example.test/jobs/first",
    )
    exact_url_later = item(
        "second",
        company="Other Corp",
        title="Unrelated Role",
        url="https://example.test/jobs/shared",
    )
    duplicate = item(
        "third",
        source="lever:acme",
        title="Product Counsel Privacy",
        url="https://example.test/jobs/shared",
    )

    result = deduplicate([fuzzy_first, exact_url_later, duplicate])

    assert result.duplicates[0].canonical_index == 0
    assert result.duplicates[0].reason is DuplicateReason.FUZZY


def test_deduplicate_avoids_cross_company_quadratic_comparisons() -> None:
    distinct = [
        item(
            str(index),
            company=f"Company {index}",
            title=f"Role {index}",
        )
        for index in range(500)
    ]
    duplicate = item(
        "0",
        url="https://other.example.test/jobs/duplicate",
    )

    with patch("jobby.dedup.compare_items", wraps=compare_items) as compared:
        result = deduplicate([*distinct, duplicate])

    assert len(result.unique) == 500
    assert len(result.duplicates) == 1
    assert compared.call_count == 1


def test_deduplicate_blocks_weak_same_company_candidates_before_deep_compare() -> None:
    suffixes = [
        f"{chr(97 + index // 26)}{chr(97 + index % 26)}" for index in range(500)
    ]
    distinct = [
        item(
            str(index),
            company="Large Employer",
            title=f"Counsel Specialty{suffixes[index]}",
            location=f"City {suffixes[index]}, CA",
            description=("Long posting text " * 200) + suffixes[index],
        )
        for index in range(500)
    ]
    duplicate = item(
        "duplicate",
        source="lever:large",
        company="Large Employer",
        title=f"Counsel Specialty{suffixes[0]}",
        location=f"City {suffixes[0]}, CA",
        url="https://other.example.test/jobs/duplicate",
        description="Different posting text",
    )

    with patch("jobby.dedup.compare_items", wraps=compare_items) as compared:
        result = deduplicate([*distinct, duplicate])

    assert len(result.unique) == 500
    assert len(result.duplicates) == 1
    assert compared.call_count <= 2


def test_invalid_fuzzy_threshold_is_rejected() -> None:
    left, right = item("a"), item("b")
    for invalid in (1.1, float("nan"), float("inf"), True, "0.8"):
        try:
            compare_items(left, right, fuzzy_threshold=invalid)
        except ValueError as exc:
            assert "between 0 and 1" in str(exc)
        else:  # pragma: no cover - documents the contract without pytest coupling
            raise AssertionError("expected ValueError")
        try:
            deduplicate([left, right], fuzzy_threshold=invalid)
        except ValueError as exc:
            assert "between 0 and 1" in str(exc)
        else:  # pragma: no cover - documents the contract without pytest coupling
            raise AssertionError("expected ValueError")
