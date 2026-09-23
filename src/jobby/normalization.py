"""Deterministic normalization helpers used by discovery and deduplication.

The functions in this module deliberately do not make network calls or depend on
an AI model.  Raw source values remain available on :class:`ScanItem`; these
helpers provide stable comparison values without destroying provenance.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import hashlib
import html
import ipaddress
import re
import unicodedata
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit


_SPACE_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^\w]+", re.UNICODE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "referrer",
    "source",
}
_TRACKING_PREFIXES = ("utm_", "pk_")


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(
        character for character in text if not unicodedata.combining(character)
    )
    return _SPACE_RE.sub(" ", text.casefold()).strip()


def normalize_url(value: object) -> str:
    """Return a canonical public HTTP(S) URL suitable for exact comparison.

    Tracking parameters, credentials, fragments, default ports, duplicate path
    slashes, and a leading ``www.`` are removed.  HTTP and HTTPS observations of
    the same resource canonicalize to HTTPS.  Invalid or empty values return an
    empty string rather than raising during a scan.
    """

    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif "://" not in raw:
        raw = f"https://{raw}"

    try:
        parts = urlsplit(raw)
        if parts.scheme.casefold() not in {"http", "https"} or not parts.hostname:
            return ""
        hostname = parts.hostname.rstrip(".").casefold().encode("idna").decode("ascii")
        if hostname.startswith("www."):
            hostname = hostname[4:]
        port = parts.port
        if port and port not in {80, 443}:
            hostname = f"{hostname}:{port}"

        decoded_path = unquote(parts.path or "/")
        decoded_path = re.sub(r"/{2,}", "/", decoded_path)
        path = quote(decoded_path, safe="/%:@!$&'()*+,;=-._~")
        if path != "/":
            path = path.rstrip("/")

        query_items = []
        for key, item_value in parse_qsl(parts.query, keep_blank_values=False):
            folded_key = key.casefold()
            if folded_key in _TRACKING_PARAMETERS or folded_key.startswith(
                _TRACKING_PREFIXES
            ):
                continue
            query_items.append((key, item_value))
        query_items.sort(key=lambda item: (item[0].casefold(), item[1]))
        query = urlencode(query_items, doseq=True)
        return urlunsplit(("https", hostname, path, query, ""))
    except (UnicodeError, ValueError):
        return ""


canonicalize_url = normalize_url


_PRIVATE_HOST_SUFFIXES = (
    ".home",
    ".internal",
    ".lan",
    ".local",
    ".localdomain",
    ".localhost",
)


def is_public_hostname(value: object) -> bool:
    """Return whether a host is safe for public-page discovery.

    Literal IP addresses must be globally routable. Hostnames used by common
    local resolvers and single-label intranet names are rejected before any
    browser or HTTP client is opened. Callers that dereference DNS names should
    additionally verify every resolved address immediately before use.
    """

    hostname = str(value or "").strip().rstrip(".").casefold()
    if (
        not hostname
        or hostname == "localhost"
        or hostname.endswith(_PRIVATE_HOST_SUFFIXES)
    ):
        return False
    try:
        return ipaddress.ip_address(hostname).is_global
    except ValueError:
        pass
    try:
        ascii_host = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    if "." not in ascii_host or len(ascii_host) > 253:
        return False
    labels = ascii_host.split(".")
    return all(
        label
        and len(label) <= 63
        and not label.startswith("-")
        and not label.endswith("-")
        and re.fullmatch(r"[a-z0-9-]+", label) is not None
        for label in labels
    )


def is_public_http_url(value: object) -> bool:
    """Validate an explicit, credential-free public HTTP(S) target."""

    try:
        raw = str(value or "").strip()
        if any(ord(character) < 32 or ord(character) == 127 for character in raw):
            return False
        parts = urlsplit(raw)
        _ = parts.port  # Access validates malformed and out-of-range ports.
        return bool(
            parts.scheme.casefold() in {"http", "https"}
            and parts.hostname
            and not parts.username
            and not parts.password
            and is_public_hostname(parts.hostname)
        )
    except (UnicodeError, ValueError):
        return False


_COMPANY_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "llp",
    "limited",
    "ltd",
    "plc",
}


def normalize_company(value: object) -> str:
    """Normalize a company name while retaining its meaningful words."""

    text = _fold(value).replace("&", " and ")
    tokens = [token for token in _NON_WORD_RE.sub(" ", text).split() if token]
    while tokens and tokens[-1] in _COMPANY_SUFFIXES:
        tokens.pop()
    if tokens[:1] == ["the"]:
        tokens = tokens[1:]
    return " ".join(tokens)


_TITLE_TOKEN_ALIASES = {
    "sr": "senior",
    "snr": "senior",
    "jr": "junior",
    "mgr": "manager",
    "vp": "vice president",
}


def normalize_title(value: object) -> str:
    """Normalize punctuation, seniority abbreviations, and whitespace in a title."""

    tokens = _NON_WORD_RE.sub(" ", _fold(value)).split()
    return " ".join(_TITLE_TOKEN_ALIASES.get(token, token) for token in tokens)


_US_STATE_NAMES = {
    "ak": "alaska",
    "al": "alabama",
    "ar": "arkansas",
    "az": "arizona",
    "ca": "california",
    "co": "colorado",
    "ct": "connecticut",
    "dc": "district of columbia",
    "de": "delaware",
    "fl": "florida",
    "ga": "georgia",
    "hi": "hawaii",
    "ia": "iowa",
    "id": "idaho",
    "il": "illinois",
    "in": "indiana",
    "ks": "kansas",
    "ky": "kentucky",
    "la": "louisiana",
    "ma": "massachusetts",
    "md": "maryland",
    "me": "maine",
    "mi": "michigan",
    "mn": "minnesota",
    "mo": "missouri",
    "ms": "mississippi",
    "mt": "montana",
    "nc": "north carolina",
    "nd": "north dakota",
    "ne": "nebraska",
    "nh": "new hampshire",
    "nj": "new jersey",
    "nm": "new mexico",
    "nv": "nevada",
    "ny": "new york",
    "oh": "ohio",
    "ok": "oklahoma",
    "or": "oregon",
    "pa": "pennsylvania",
    "ri": "rhode island",
    "sc": "south carolina",
    "sd": "south dakota",
    "tn": "tennessee",
    "tx": "texas",
    "ut": "utah",
    "va": "virginia",
    "vt": "vermont",
    "wa": "washington",
    "wi": "wisconsin",
    "wv": "west virginia",
    "wy": "wyoming",
}


def normalize_location(value: object) -> str:
    """Return a stable, human-readable location comparison value."""

    text = _fold(value)
    text = re.sub(r"\bu\.?s\.?a?\b", "united states", text)
    text = text.replace("united states of america", "united states")
    tokens = _NON_WORD_RE.sub(" ", text).split()
    expanded = [_US_STATE_NAMES.get(token, token) for token in tokens]
    return " ".join(expanded)


class RemoteStatus(StrEnum):
    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"
    UNKNOWN = "unknown"


def normalize_remote(value: object = None, *, location: object = None) -> RemoteStatus:
    """Classify explicit remote, hybrid, or on-site evidence.

    A source boolean is considered explicit evidence.  Free-form values and the
    location are checked for common public-posting terminology.
    """

    if isinstance(value, bool):
        return RemoteStatus.REMOTE if value else RemoteStatus.ONSITE
    text = _fold(value)
    location_text = _fold(location)
    combined = f"{text} {location_text}".strip()
    if re.search(r"\bhybrid\b", combined):
        return RemoteStatus.HYBRID
    if re.search(
        r"\b(remote|telework|telecommut(?:e|ing)|work from home|distributed)\b",
        combined,
    ):
        return RemoteStatus.REMOTE
    if re.search(r"\b(on[ -]?site|in[ -]?office|office based)\b", combined):
        return RemoteStatus.ONSITE
    return RemoteStatus.UNKNOWN


class SalaryPeriod(StrEnum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    UNKNOWN = "unknown"


_ANNUAL_FACTORS = {
    SalaryPeriod.HOUR: Decimal("2080"),
    SalaryPeriod.DAY: Decimal("260"),
    SalaryPeriod.WEEK: Decimal("52"),
    SalaryPeriod.MONTH: Decimal("12"),
    SalaryPeriod.YEAR: Decimal("1"),
}

# Salary bounds are stored in SQLite INTEGER columns.  Keeping the limit next
# to normalization makes every ingestion path reject an unpersistable numeric
# interpretation before it can overflow a flush or influence salary gating.
MAX_PERSISTED_SALARY = (2**63) - 1
_MAX_PERSISTED_SALARY_DECIMAL = Decimal(MAX_PERSISTED_SALARY)


def _salary_bound_is_persistable(
    value: Decimal | None,
    *,
    currency: str,
    period: SalaryPeriod,
    confidence: float,
) -> bool:
    if value is None:
        return True
    if not value.is_finite() or value < 0 or value > _MAX_PERSISTED_SALARY_DECIMAL:
        return False
    if currency == "USD" and confidence >= 0.8:
        factor = _ANNUAL_FACTORS.get(period)
        if factor is not None and value * factor > _MAX_PERSISTED_SALARY_DECIMAL:
            return False
    return True


@dataclass(frozen=True, slots=True)
class NormalizedSalary:
    minimum: Decimal | None
    maximum: Decimal | None
    currency: str = "USD"
    period: SalaryPeriod = SalaryPeriod.UNKNOWN
    confidence: float = 0.0
    evidence: str | None = None

    def __post_init__(self) -> None:
        if self.minimum is None and self.maximum is None:
            raise ValueError("a salary range needs at least one bound")
        if self.minimum is not None and (
            not self.minimum.is_finite() or self.minimum < 0
        ):
            raise ValueError("salary minimum cannot be negative")
        if self.maximum is not None and (
            not self.maximum.is_finite() or self.maximum < 0
        ):
            raise ValueError("salary maximum cannot be negative")
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            low, high = self.maximum, self.minimum
            object.__setattr__(self, "minimum", low)
            object.__setattr__(self, "maximum", high)
        normalized_currency = str(self.currency or "").strip().upper()
        if re.fullmatch(r"[A-Z]{3}", normalized_currency) is None:
            normalized_currency = "UNK"
        object.__setattr__(self, "currency", normalized_currency)
        if not 0 <= self.confidence <= 1:
            raise ValueError("salary normalization confidence must be between 0 and 1")
        if not all(
            _salary_bound_is_persistable(
                bound,
                currency=normalized_currency,
                period=self.period,
                confidence=self.confidence,
            )
            for bound in (self.minimum, self.maximum)
        ):
            raise ValueError("salary bound exceeds the supported persistence range")
        if self.evidence is not None:
            object.__setattr__(self, "evidence", self.evidence.strip()[:2_000] or None)

    @property
    def annual_minimum(self) -> Decimal | None:
        factor = _ANNUAL_FACTORS.get(self.period)
        return self.minimum * factor if self.minimum is not None and factor else None

    @property
    def annual_maximum(self) -> Decimal | None:
        factor = _ANNUAL_FACTORS.get(self.period)
        return self.maximum * factor if self.maximum is not None and factor else None

    @property
    def annualization_confident(self) -> bool:
        return (
            self.currency == "USD"
            and self.period in _ANNUAL_FACTORS
            and self.confidence >= 0.8
        )


_NUMBER_RE = re.compile(
    r"(?<![\w])(-?[0-9][0-9,]*(?:\.[0-9]+)?)(?:\s?([km])(?:illion)?(?![a-z]))?",
    re.IGNORECASE,
)


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, Decimal):
            parsed = value
        else:
            cleaned = str(value).strip().replace(",", "").replace("$", "")
            if not cleaned:
                return None
            multiplier = Decimal("1")
            if cleaned[-1:].casefold() == "k":
                multiplier, cleaned = Decimal("1000"), cleaned[:-1]
            elif cleaned[-1:].casefold() == "m":
                multiplier, cleaned = Decimal("1000000"), cleaned[:-1]
            parsed = Decimal(cleaned) * multiplier
        if not parsed.is_finite() or parsed < 0:
            return None
        return parsed
    except (InvalidOperation, ValueError):
        return None


def _explicitly_invalid_salary_bound(value: object) -> bool:
    """Distinguish unsafe numeric input from an ordinary missing/unknown value."""

    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (int, float, Decimal)):
        try:
            parsed = Decimal(str(value))
        except InvalidOperation:
            return True
        return not parsed.is_finite() or parsed < 0
    text = str(value).strip().replace(",", "").replace("$", "")
    if not text:
        return False
    if text[-1:].casefold() in {"k", "m"}:
        text = text[:-1]
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return False
    return not parsed.is_finite() or parsed < 0


_CURRENCY_ALIASES = {
    "$": "USD",
    "US$": "USD",
    "USD$": "USD",
    "C$": "CAD",
    "CA$": "CAD",
    "CAD$": "CAD",
    "A$": "AUD",
    "AU$": "AUD",
    "AUD$": "AUD",
    "£": "GBP",
    "€": "EUR",
}
_CURRENCY_CODE_RE = re.compile(
    r"\b(USD|CAD|AUD|EUR|GBP|JPY|CHF|NZD|CNY|RMB|INR|KRW|SGD|HKD|"
    r"MXN|BRL|ZAR|SEK|NOK|DKK|PLN)\b",
    re.IGNORECASE,
)


def _normalize_currency(value: object, evidence: str) -> tuple[str, bool]:
    """Prefer an explicit ISO code over an ambiguous currency symbol."""

    explicit = str(value or "").strip().upper().replace(" ", "")
    if explicit:
        alias = _CURRENCY_ALIASES.get(explicit)
        if alias:
            return alias, True
        match = _CURRENCY_CODE_RE.search(explicit)
        if match:
            code = match.group(1).upper()
            return ("CNY" if code == "RMB" else code), True
        if re.fullmatch(r"[A-Z]{3}", explicit):
            return explicit, True
        return "UNK", False

    # A code such as ``CAD $`` is more specific than the dollar glyph.  Check
    # it first so non-USD compensation can never accidentally activate a USD
    # salary floor.
    match = _CURRENCY_CODE_RE.search(evidence)
    if match:
        code = match.group(1).upper()
        return ("CNY" if code == "RMB" else code), True
    if re.search(r"\b(?:C|CA)\$", evidence, re.IGNORECASE):
        return "CAD", True
    if re.search(r"\b(?:A|AU)\$", evidence, re.IGNORECASE):
        return "AUD", True
    if "£" in evidence:
        return "GBP", True
    if "€" in evidence:
        return "EUR", True
    if "$" in evidence:
        return "USD", True
    return "UNK", False


def _mapping_value(value: Mapping[str, object], *keys: str) -> object:
    folded = {str(key).casefold(): item for key, item in value.items()}
    for key in keys:
        if key.casefold() in folded:
            return folded[key.casefold()]
    return None


def _salary_period(value: object) -> SalaryPeriod:
    text = _fold(value)
    if re.search(r"\b(hour|hourly|hr)\b", text):
        return SalaryPeriod.HOUR
    if re.search(r"\b(day|daily)\b", text):
        return SalaryPeriod.DAY
    if re.search(r"\b(week|weekly)\b", text):
        return SalaryPeriod.WEEK
    if re.search(r"\b(month|monthly)\b", text):
        return SalaryPeriod.MONTH
    if re.search(r"\b(year|yearly|yr|annual|annually|annum)\b", text):
        return SalaryPeriod.YEAR
    return SalaryPeriod.UNKNOWN


def normalize_salary(
    value: object,
    maximum: object = None,
    *,
    currency: object = None,
    period: object = None,
) -> NormalizedSalary | None:
    """Normalize ATS salary mappings, ranges, or free-form salary strings.

    Supported examples include ``{"min": 40, "max": 50, "interval": "hour"}``,
    ``"USD 120K-150K annually"``, and numeric lower/upper bounds.
    Non-salary text returns ``None``.
    """

    raw_text = ""
    minimum: Decimal | None
    maximum_value: Decimal | None
    if isinstance(value, Mapping):
        salary_mapping = {str(key): item for key, item in value.items()}
        raw_minimum = _mapping_value(
            salary_mapping, "min", "minimum", "minimumrange", "from"
        )
        raw_maximum = _mapping_value(
            salary_mapping, "max", "maximum", "maximumrange", "to"
        )
        if any(
            _explicitly_invalid_salary_bound(item)
            for item in (raw_minimum, raw_maximum)
        ):
            return None
        minimum = _decimal(raw_minimum)
        maximum_value = _decimal(raw_maximum)
        currency = currency or _mapping_value(
            salary_mapping, "currency", "currencycode"
        )
        period = period or _mapping_value(
            salary_mapping,
            "interval",
            "intervalcode",
            "rate",
            "rateintervalcode",
            "period",
            "description",
        )
        raw_text = " ".join(str(item) for item in value.values() if item is not None)
    elif isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        if _explicitly_invalid_salary_bound(value) or (
            maximum is not None and _explicitly_invalid_salary_bound(maximum)
        ):
            return None
        minimum = _decimal(value)
        maximum_value = _decimal(maximum)
    else:
        raw_text = str(value or "").strip()
        matches = _NUMBER_RE.findall(raw_text)
        if any(number.startswith("-") for number, _suffix in matches[:2]):
            return None
        numbers = []
        for number, suffix in matches[:2]:
            parsed = _decimal(f"{number}{suffix}")
            if parsed is not None:
                numbers.append(parsed)
        if not numbers:
            return None
        minimum = numbers[0]
        maximum_value = numbers[1] if len(numbers) > 1 else None
        folded = _fold(raw_text)
        if len(numbers) == 1 and re.search(r"\b(up to|maximum|max)\b", folded):
            minimum, maximum_value = None, minimum
    if (
        maximum is not None
        and not isinstance(value, str)
        and _explicitly_invalid_salary_bound(maximum)
    ):
        return None
    if maximum is not None and not isinstance(value, str):
        maximum_value = _decimal(maximum)
    if minimum is None and maximum_value is None:
        return None

    evidence = raw_text
    currency_text, currency_explicit = _normalize_currency(currency, evidence)
    normalized_period = _salary_period(period or evidence)
    confidence = (
        0.98
        if currency_explicit and normalized_period != SalaryPeriod.UNKNOWN
        else 0.55
    )
    if normalized_period == SalaryPeriod.UNKNOWN:
        confidence = min(confidence, 0.45)
    if not currency_explicit:
        confidence = min(confidence, 0.5)
    if not all(
        _salary_bound_is_persistable(
            bound,
            currency=currency_text,
            period=normalized_period,
            confidence=confidence,
        )
        for bound in (minimum, maximum_value)
    ):
        return None
    return NormalizedSalary(
        minimum,
        maximum_value,
        currency_text,
        normalized_period,
        confidence,
        evidence[:2_000] or None,
    )


@dataclass(frozen=True, slots=True)
class CompensationExtraction:
    salary: NormalizedSalary | None
    evidence: str | None
    warnings: tuple[str, ...] = ()


_COMPENSATION_SIGNAL_RE = re.compile(
    r"(?:[$£€]\s*\d|\b(?:USD|CAD|AUD|EUR|GBP|JPY|CHF|NZD)\s+\d|"
    r"\b(?:salary|compensation|base pay|pay range|hourly rate)\b)",
    re.IGNORECASE,
)

_PAY_SIGNAL_RE = re.compile(
    r"\b(salary|base pay|pay range|hourly rate|compensation)\b", re.IGNORECASE
)
_BASE_PAY_SIGNAL_RE = re.compile(
    r"\b(salary|base pay|pay range|hourly rate)\b", re.IGNORECASE
)
_NON_BASE_COMPENSATION_RE = re.compile(
    r"\b(?:bonus|stipend|reimbursement|allowance|relocation|sign(?:ing|-on)|"
    r"commission|equity|stock grant|wellness|tuition)\b",
    re.IGNORECASE,
)
_MONEY_RANGE_RE = re.compile(
    r"(?P<currency>"
    r"(?:(?:USD|CAD|AUD|EUR|GBP|JPY|CHF|NZD|CNY|RMB|INR|KRW|SGD|HKD|"
    r"MXN|BRL|ZAR|SEK|NOK|DKK|PLN)\s*(?:[$£€])?|"
    r"(?:US|C|CA|A|AU)?[$£€])"
    r")\s*"
    r"(?P<low>[0-9][0-9,]*(?:\.[0-9]+)?(?:\s?[kKmM](?:illion)?(?![A-Za-z]))?)"
    r"(?:\s*(?:-|\u2013|\u2014|to)\s*"
    r"(?:(?:(?:USD|CAD|AUD|EUR|GBP|JPY|CHF|NZD|CNY|RMB|INR|KRW|SGD|HKD|"
    r"MXN|BRL|ZAR|SEK|NOK|DKK|PLN)\s*(?:[$£€])?|"
    r"(?:US|C|CA|A|AU)?[$£€])\s*)?"
    r"(?P<high>[0-9][0-9,]*(?:\.[0-9]+)?(?:\s?[kKmM](?:illion)?(?![A-Za-z]))?))?",
    re.IGNORECASE,
)


# The pay period must be stated next to the amount. A period word elsewhere in
# the passage ("sick days", "a 90-day waiting period") says nothing about pay.
_PERIOD_AFTER_RE = re.compile(
    r"^\s*(?:[A-Z]{3}\b\s*)?(?:(?:per|an?|/|each)\s*(?P<unit>hour|hr|day|week|month|"
    r"year|yr|annum)\b|(?P<adverb>hourly|daily|weekly|monthly|annually|yearly|annual))",
    re.IGNORECASE,
)
_PERIOD_BEFORE_RE = re.compile(
    r"\b(?P<adjective>hourly|daily|weekly|monthly|annual|annualized|yearly)\s+"
    r"(?:base\s+)?(?:salary|pay|rate|wage|compensation)\b[^$£€0-9]{0,60}$|"
    r"\b(?:salary|pay|rate|wage|compensation)\s+per\s+(?P<unit>hour|day|week|month|"
    r"year|annum)\b[^$£€0-9]{0,60}$",
    re.IGNORECASE,
)
# A lone amount needs a pay word right before it; budgets and revenue are money
# but not compensation.
_PAY_CONTEXT_RE = re.compile(
    r"\b(?:salary|base pay|pay|pay range|pay rate|rate of pay|hourly rate|wages?|"
    r"compensation|earn(?:ing)?s?)\b",
    re.IGNORECASE,
)
_NON_PAY_AMOUNT_RE = re.compile(
    r"\b(?:budgets?|revenue|sales|funding|raised|valuation|assets|spend|grants?|"
    r"members|donations?|deals?|transactions?|arr|gmv|targets?|quota)\b",
    re.IGNORECASE,
)
_MAJOR_CURRENCIES = frozenset({"USD", "CAD", "AUD", "EUR", "GBP", "CHF", "NZD", "SGD"})
_MAX_PLAUSIBLE_ANNUAL = Decimal("2000000")
_MIN_INFERRED_ANNUAL = Decimal("15000")


def _local_period(passage: str, start: int, end: int) -> SalaryPeriod:
    after = _PERIOD_AFTER_RE.search(passage[end : end + 40])
    if after:
        return _salary_period(after.group("unit") or after.group("adverb"))
    before = _PERIOD_BEFORE_RE.search(passage[max(0, start - 120) : start])
    if before:
        return _salary_period(before.group("adjective") or before.group("unit"))
    return SalaryPeriod.UNKNOWN


def _plausible(salary: NormalizedSalary) -> bool:
    """Reject annualized major-currency figures no salary reaches."""

    if salary.currency not in _MAJOR_CURRENCIES or not salary.annualization_confident:
        return True
    top = salary.annual_maximum or salary.annual_minimum
    return top is None or top <= _MAX_PLAUSIBLE_ANNUAL


def _with_salary_evidence(salary: NormalizedSalary, evidence: str) -> NormalizedSalary:
    return NormalizedSalary(
        minimum=salary.minimum,
        maximum=salary.maximum,
        currency=salary.currency,
        period=salary.period,
        confidence=salary.confidence,
        evidence=evidence,
    )


def _looks_like_plain_year(match: re.Match[str]) -> bool:
    number, suffix = match.groups()
    if suffix or "," in number or "." in number or number.startswith("-"):
        return False
    try:
        value = int(number)
    except ValueError:
        return False
    return 1900 <= value <= 2100


def _salary_after_pay_signal(
    passage: str, signal: re.Match[str]
) -> NormalizedSalary | None:
    """Parse pay text after a signal while ignoring an intervening calendar year."""

    suffix = passage[signal.start() : signal.start() + 1_000]
    matches = list(_NUMBER_RE.finditer(suffix))
    non_year = [match for match in matches if not _looks_like_plain_year(match)]
    if not non_year:
        return None
    # Remove only obvious standalone years before invoking the tolerant public
    # normalizer. This retains range syntax (including ``up to``) and suffixes.
    scrubbed = list(suffix)
    for match in matches:
        if _looks_like_plain_year(match):
            scrubbed[match.start() : match.end()] = " " * (match.end() - match.start())
    salary = normalize_salary(
        "".join(scrubbed),
        # Only the words right after the pay signal can state its period.
        period=_salary_period(suffix[:120]),
    )
    return _with_salary_evidence(salary, passage) if salary is not None else None


def _passage_salary_candidates(
    passage: str,
) -> list[tuple[float, NormalizedSalary]]:
    candidates: list[tuple[float, NormalizedSalary]] = []
    contains_non_base = _NON_BASE_COMPENSATION_RE.search(passage) is not None
    contains_base_pay = _BASE_PAY_SIGNAL_RE.search(passage) is not None

    for match in _MONEY_RANGE_RE.finditer(passage):
        has_range = match.group("high") is not None
        nearby = passage[max(0, match.start() - 100) : match.start()]
        nearby_base_pay = _BASE_PAY_SIGNAL_RE.search(nearby) is not None
        close_before = passage[max(0, match.start() - 60) : match.start()]
        pay_context = _PAY_CONTEXT_RE.search(close_before) is not None
        # A lone stipend/bonus/benefit amount is not a salary. A genuine range,
        # explicit base-pay phrase, or pay period remains useful evidence.
        if contains_non_base and not (nearby_base_pay or contains_base_pay):
            continue
        local_period = _local_period(passage, match.start(), match.end())
        # A lone amount must follow a pay word, unless it is an hourly rate
        # ("$50/hour"); "$10M+ annually" in a line about media budgets is
        # money, not compensation.
        hourly = local_period == SalaryPeriod.HOUR
        if not has_range and not pay_context and not hourly:
            continue
        surrounding = passage[max(0, match.start() - 60) : match.end() + 40]
        if _NON_PAY_AMOUNT_RE.search(surrounding) and not pay_context:
            continue
        salary = normalize_salary(match.group(0), period=local_period)
        if salary is None:
            continue
        if (
            local_period == SalaryPeriod.UNKNOWN
            and has_range
            and salary.currency in _MAJOR_CURRENCIES
            and salary.minimum is not None
            and salary.maximum is not None
            and _MIN_INFERRED_ANNUAL <= salary.minimum <= salary.maximum
            and salary.maximum <= _MAX_PLAUSIBLE_ANNUAL
            and (pay_context or nearby_base_pay or contains_base_pay)
        ):
            # A pay range with no stated period and salary-sized bounds is
            # annual; smaller or unlabeled figures stay unannualized.
            salary = normalize_salary(match.group(0), period="year") or salary
        if not _plausible(salary):
            continue
        salary = _with_salary_evidence(salary, passage)
        score = (
            salary.confidence * 10
            + (3 if has_range else 0)
            + (2 if nearby_base_pay else 0)
            + (1 if salary.annualization_confident else 0)
        )
        candidates.append((score, salary))

    # Currency-marked candidates above are more precise. Signal-relative
    # parsing is the conservative fallback for ranges such as
    # ``salary range 120,000 to 150,000 per year``.
    if not candidates:
        for signal in _PAY_SIGNAL_RE.finditer(passage):
            salary = _salary_after_pay_signal(passage, signal)
            if salary is None:
                continue
            is_base = signal.group(1).casefold() != "compensation"
            if contains_non_base and not (is_base or contains_base_pay):
                continue
            has_range = salary.minimum is not None and salary.maximum is not None
            score = (
                salary.confidence * 10 + (3 if has_range else 0) + (2 if is_base else 0)
            )
            candidates.append((score, salary))
    return candidates


def extract_compensation(value: object) -> CompensationExtraction:
    """Extract a deterministic range and exact quoted passage from a posting.

    This intentionally recognizes only explicit compensation-shaped passages;
    isolated years, headcounts, and experience requirements are ignored.
    """

    text = html.unescape(_HTML_TAG_RE.sub(" ", str(value or "")))
    text = text.replace("\x00", "�")[:500_000]
    candidates: list[tuple[float, int, NormalizedSalary, str]] = []
    for passage in re.split(r"(?<=[.!?])\s+|[\r\n]+", text):
        passage = _SPACE_RE.sub(" ", passage).strip()
        if (
            not passage
            or len(passage) > 2_000
            or not _COMPENSATION_SIGNAL_RE.search(passage)
        ):
            continue
        passage_candidates = _passage_salary_candidates(passage)
        if not passage_candidates:
            continue
        _, salary = max(passage_candidates, key=lambda item: item[0])
        candidates.append((salary.confidence, len(passage), salary, passage))
    if not candidates:
        return CompensationExtraction(None, None)
    _, _, salary, evidence = max(candidates, key=lambda item: (item[0], -item[1]))
    warnings: list[str] = []
    if salary.currency != "USD":
        warnings.append(
            "Compensation is non-USD or its currency is unknown; salary floors were not applied."
        )
    if salary.period == SalaryPeriod.UNKNOWN:
        warnings.append(
            "Compensation period is unknown; the amount was not annualized or used as a rejection gate."
        )
    elif salary.confidence < 0.8:
        warnings.append(
            "Compensation normalization confidence is low; verify the range manually."
        )
    return CompensationExtraction(salary, evidence, tuple(dict.fromkeys(warnings)))


def normalize_content(value: object) -> str:
    """Reduce HTML or plain text to stable comparison text."""

    text = html.unescape(_HTML_TAG_RE.sub(" ", str(value or "")))
    return _SPACE_RE.sub(" ", unicodedata.normalize("NFKC", text).casefold()).strip()


def content_hash(value: object) -> str:
    """Return a SHA-256 content hash, or an empty string for empty content."""

    normalized = normalize_content(value)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def comparison_tokens(value: object) -> frozenset[str]:
    """Tokenize normalized text for deterministic similarity checks."""

    return frozenset(_NON_WORD_RE.sub(" ", _fold(value)).split())


__all__ = [
    "CompensationExtraction",
    "MAX_PERSISTED_SALARY",
    "NormalizedSalary",
    "RemoteStatus",
    "SalaryPeriod",
    "canonicalize_url",
    "comparison_tokens",
    "content_hash",
    "extract_compensation",
    "is_public_hostname",
    "is_public_http_url",
    "normalize_company",
    "normalize_content",
    "normalize_location",
    "normalize_remote",
    "normalize_salary",
    "normalize_title",
    "normalize_url",
]
