"""Explainable, deterministic duplicate detection for source observations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math
from numbers import Real
from typing import Iterable

from jobby.normalization import comparison_tokens
from jobby.sources.base import ScanItem


class DuplicateReason(StrEnum):
    SOURCE_ID = "source_id"
    CANONICAL_URL = "canonical_url"
    CONTENT_HASH = "content_hash"
    NORMALIZED_FIELDS = "normalized_fields"
    FUZZY = "fuzzy"


@dataclass(frozen=True, slots=True)
class DuplicateMatch:
    canonical_index: int
    duplicate_index: int
    canonical: ScanItem
    duplicate: ScanItem
    reason: DuplicateReason
    similarity: float = 1.0


@dataclass(frozen=True, slots=True)
class DeduplicationResult:
    unique: tuple[ScanItem, ...]
    duplicates: tuple[DuplicateMatch, ...]


def token_jaccard(left: object, right: object) -> float:
    """Return Jaccard similarity over deterministic case-folded word tokens."""

    left_tokens = comparison_tokens(left)
    right_tokens = comparison_tokens(right)
    if not left_tokens and not right_tokens:
        return 1.0
    union = left_tokens | right_tokens
    return len(left_tokens & right_tokens) / len(union) if union else 0.0


def compare_items(
    left: ScanItem,
    right: ScanItem,
    *,
    fuzzy_threshold: float = 0.8,
) -> tuple[DuplicateReason, float] | None:
    """Compare observations in precedence order and explain any match."""

    fuzzy_threshold = _fuzzy_threshold(fuzzy_threshold)

    if (
        left.source.casefold() == right.source.casefold()
        and left.source_id.casefold() == right.source_id.casefold()
    ):
        return DuplicateReason.SOURCE_ID, 1.0

    left_url, right_url = left.canonical_url, right.canonical_url
    if left_url and left_url == right_url:
        return DuplicateReason.CANONICAL_URL, 1.0

    left_hash, right_hash = left.description_hash, right.description_hash
    if left_hash and left_hash == right_hash:
        return DuplicateReason.CONTENT_HASH, 1.0

    same_company = (
        bool(left.normalized_company)
        and left.normalized_company == right.normalized_company
    )
    if (
        same_company
        and left.normalized_title
        and left.normalized_title == right.normalized_title
        and left.normalized_location == right.normalized_location
    ):
        return DuplicateReason.NORMALIZED_FIELDS, 1.0

    if not same_company:
        return None
    if left.normalized_location and right.normalized_location:
        left_identity = f"{left.normalized_title} {left.normalized_location}".strip()
        right_identity = f"{right.normalized_title} {right.normalized_location}".strip()
    else:
        # Missing location data should not prevent a strong same-company title
        # match; when both locations exist they remain part of the identity.
        left_identity = left.normalized_title
        right_identity = right.normalized_title
    similarity = token_jaccard(left_identity, right_identity)
    if similarity >= fuzzy_threshold:
        return DuplicateReason.FUZZY, similarity
    return None


def deduplicate(
    items: Iterable[ScanItem],
    *,
    fuzzy_threshold: float = 0.8,
) -> DeduplicationResult:
    """Keep the first observation and report every later duplicate.

    Input order is stable, which lets callers put trusted or more complete
    sources first.  The result records original input indexes for auditability.
    """

    fuzzy_threshold = _fuzzy_threshold(fuzzy_threshold)
    unique: list[ScanItem] = []
    unique_indexes: list[int] = []
    duplicates: list[DuplicateMatch] = []
    source_positions: dict[tuple[str, str], int] = {}
    url_positions: dict[str, int] = {}
    hash_positions: dict[str, int] = {}
    field_positions: dict[tuple[str, str, str], int] = {}
    company_title_positions: dict[tuple[str, str], set[int]] = {}
    company_identity_positions: dict[tuple[str, str], set[int]] = {}
    company_positions: dict[str, set[int]] = {}
    unique_title_tokens: list[frozenset[str]] = []
    unique_identity_tokens: list[frozenset[str]] = []
    unique_has_location: list[bool] = []

    for duplicate_index, item in enumerate(items):
        match = None
        source_key = (item.source.casefold(), item.source_id.casefold())
        canonical_url = item.canonical_url
        description_hash = item.description_hash
        company = item.normalized_company
        title = item.normalized_title
        location = item.normalized_location
        field_key = (company, title, location)

        title_tokens = comparison_tokens(title)
        identity_tokens = comparison_tokens(
            f"{title} {location}" if location else title
        )
        exact_positions: set[int] = set()
        for position in (
            source_positions.get(source_key),
            url_positions.get(canonical_url),
            hash_positions.get(description_hash) if description_hash else None,
            field_positions.get(field_key) if company and title else None,
        ):
            if position is not None:
                exact_positions.add(position)

        # Fuzzy comparison is blocked through inverted token indexes. A cheap
        # precomputed set comparison rejects weak candidates before
        # ``compare_items`` normalizes long descriptions or other fields.
        fuzzy_positions: set[int] = set()
        if company:
            if fuzzy_threshold == 0:
                fuzzy_positions.update(company_positions.get(company, ()))
            for token in title_tokens:
                fuzzy_positions.update(
                    company_title_positions.get((company, token), ())
                )
            if location:
                for token in identity_tokens:
                    fuzzy_positions.update(
                        company_identity_positions.get((company, token), ())
                    )
        candidate_positions = set(exact_positions)
        for position in fuzzy_positions - exact_positions:
            candidate_tokens = (
                unique_identity_tokens[position]
                if location and unique_has_location[position]
                else unique_title_tokens[position]
            )
            current_tokens = (
                identity_tokens
                if location and unique_has_location[position]
                else title_tokens
            )
            union = candidate_tokens | current_tokens
            similarity = (
                len(candidate_tokens & current_tokens) / len(union) if union else 0.0
            )
            if similarity >= fuzzy_threshold:
                candidate_positions.add(position)

        for position in sorted(candidate_positions):
            canonical_index = unique_indexes[position]
            canonical = unique[position]
            comparison = compare_items(canonical, item, fuzzy_threshold=fuzzy_threshold)
            if comparison is not None:
                reason, similarity = comparison
                match = DuplicateMatch(
                    canonical_index=canonical_index,
                    duplicate_index=duplicate_index,
                    canonical=canonical,
                    duplicate=item,
                    reason=reason,
                    similarity=similarity,
                )
                break
        if match is None:
            position = len(unique)
            unique.append(item)
            unique_indexes.append(duplicate_index)
            unique_title_tokens.append(title_tokens)
            unique_identity_tokens.append(identity_tokens)
            unique_has_location.append(bool(location))
            source_positions[source_key] = position
            url_positions[canonical_url] = position
            if description_hash:
                hash_positions[description_hash] = position
            if company and title:
                field_positions[field_key] = position
            if company:
                company_positions.setdefault(company, set()).add(position)
                for token in title_tokens:
                    company_title_positions.setdefault((company, token), set()).add(
                        position
                    )
                if location:
                    for token in identity_tokens:
                        company_identity_positions.setdefault(
                            (company, token), set()
                        ).add(position)
        else:
            duplicates.append(match)
    return DeduplicationResult(tuple(unique), tuple(duplicates))


def _fuzzy_threshold(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError("fuzzy_threshold must be a finite number between 0 and 1")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0 <= normalized <= 1:
        raise ValueError("fuzzy_threshold must be a finite number between 0 and 1")
    return normalized


__all__ = [
    "DeduplicationResult",
    "DuplicateMatch",
    "DuplicateReason",
    "compare_items",
    "deduplicate",
    "token_jaccard",
]
