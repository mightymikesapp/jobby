"""Platform paths, non-secret TOML configuration, and keyring-backed secrets."""

from __future__ import annotations

import os
import re
import stat
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from platformdirs import PlatformDirs
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)


APP_NAME = "Jobby"
APP_AUTHOR = "MikeSapp"
KEYRING_SERVICE = "jobby"
MAX_CONFIG_BYTES = 1_000_000
MAX_SECRET_CHARS = 1_000_000
MIN_EXTERNAL_BACKUP_SCRYPT_N = 16_384
MAX_EXTERNAL_BACKUP_SCRYPT_N = 65_536
DEFAULT_SCHEDULED_WEB_QUERY = (
    "paid legal AI, legal technology, AI policy, intellectual property, or "
    "federal technology policy roles in the United States"
)
_SECRET_NAME = re.compile(r"^[a-z][a-z0-9_]{0,99}$")

# Source maps remain ordinary TOML tables, but their keys and display values are
# validated consistently across every adapter.  The slug alphabet deliberately
# includes the punctuation used by existing ATS board identifiers.
BoardSlug = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]
BoardName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=300),
]


class JobbyPaths(BaseModel):
    model_config = ConfigDict(
        frozen=True, validate_assignment=True, validate_default=True
    )

    data_dir: Path
    config_dir: Path
    cache_dir: Path
    database: Path
    artifacts_dir: Path
    backups_dir: Path
    logs_dir: Path
    config_file: Path

    def ensure(self) -> "JobbyPaths":
        for path in (
            self.data_dir,
            self.config_dir,
            self.cache_dir,
            self.artifacts_dir,
            self.backups_dir,
            self.logs_dir,
        ):
            _ensure_private_directory(path)
        return self


def _ensure_private_directory(path: Path) -> None:
    """Create one application directory without traversing symbolic links."""

    absolute = path.expanduser().absolute()
    parts = absolute.parts
    if len(parts) <= 1:
        raise ValueError("application directory must not be the filesystem root")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(absolute.anchor, flags)
    try:
        for component in parts[1:]:
            try:
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                os.fsync(descriptor)
            except FileExistsError:
                pass
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as exc:
                raise ValueError(
                    "application directory path must contain only real directories: "
                    f"{absolute}"
                ) from exc
            os.close(descriptor)
            descriptor = child
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def resolve_paths(env: Mapping[str, str] | None = None) -> JobbyPaths:
    if env is None:
        env = os.environ
    if home := env.get("JOBBY_HOME"):
        root = Path(home).expanduser().resolve()
        data_dir = root / "data"
        config_dir = root / "config"
        cache_dir = root / "cache"
    else:
        dirs = PlatformDirs(APP_NAME, APP_AUTHOR, roaming=False, ensure_exists=False)
        data_dir = Path(env.get("JOBBY_DATA_DIR", dirs.user_data_dir)).expanduser()
        config_dir = Path(
            env.get("JOBBY_CONFIG_DIR", dirs.user_config_dir)
        ).expanduser()
        cache_dir = Path(env.get("JOBBY_CACHE_DIR", dirs.user_cache_dir)).expanduser()
    database = Path(env.get("JOBBY_DATABASE", data_dir / "jobby.sqlite3")).expanduser()
    return JobbyPaths(
        data_dir=data_dir,
        config_dir=config_dir,
        cache_dir=cache_dir,
        database=database,
        artifacts_dir=data_dir / "artifacts",
        backups_dir=data_dir / "backups",
        logs_dir=data_dir / "logs",
        config_file=config_dir / "config.toml",
    )


class ModelSettings(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    fast: str = Field(default="gpt-5.6-luna", max_length=200)
    quality: str = Field(default="gpt-5.6-terra", max_length=200)
    premium: str = Field(default="gpt-5.6-sol", max_length=200)

    @field_validator("fast", "quality", "premium")
    @classmethod
    def model_id_is_explicit(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("model IDs must not be blank")
        return value


class WorkdayBoard(BaseModel):
    """Typed form of the long-supported Workday nested source table."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    tenant: BoardSlug
    wd: BoardSlug = "wd1"
    site: BoardSlug
    # Older TOML source maps omitted the display name and used the table key.
    name: BoardName | None = None


class SmartRecruitersBoard(BaseModel):
    """One public SmartRecruiters company board."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    company_slug: BoardSlug
    name: BoardName


class ICIMSBoard(BaseModel):
    """One public iCIMS tenant, stored as a validated HTTPS origin."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    base_url: str = Field(min_length=1, max_length=2_000)
    name: BoardName

    @field_validator("base_url")
    @classmethod
    def valid_base_url(cls, value: str) -> str:
        from urllib.parse import urlsplit

        raw = value.strip()
        try:
            parsed = urlsplit(raw)
            port = parsed.port
        except ValueError as exc:
            raise ValueError("iCIMS base URL is malformed") from exc
        hostname = (parsed.hostname or "").rstrip(".").casefold()
        if (
            parsed.scheme.casefold() != "https"
            or not hostname.endswith(".icims.com")
            or hostname == "icims.com"
            or parsed.username
            or parsed.password
            or port is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "iCIMS base URL must be a credential-free *.icims.com HTTPS origin"
            )
        return f"https://{hostname}"


class TaleoBoard(BaseModel):
    """One Taleo Business Edition v2 search page."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    search_url: str = Field(min_length=1, max_length=4_000)
    name: BoardName

    @field_validator("search_url")
    @classmethod
    def valid_search_url(cls, value: str) -> str:
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

        raw = value.strip()
        try:
            parsed = urlsplit(raw)
            port = parsed.port
            pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        except ValueError as exc:
            raise ValueError("Taleo search URL is malformed") from exc
        hostname = (parsed.hostname or "").rstrip(".").casefold()
        query: dict[str, str] = {}
        for key, item in pairs:
            folded = key.casefold()
            if folded in query:
                raise ValueError("Taleo search URL contains duplicate parameters")
            query[folded] = item.strip()
        if (
            parsed.scheme.casefold() != "https"
            or not hostname.endswith(".tbe.taleo.net")
            or hostname == "tbe.taleo.net"
            or parsed.username
            or parsed.password
            or port is not None
            or parsed.fragment
            or not parsed.path.casefold()
            .rstrip("/")
            .endswith("/ats/careers/v2/searchresults")
            or not query.get("org")
            or not query.get("cws")
        ):
            raise ValueError(
                "Taleo URL must be a credential-free *.tbe.taleo.net HTTPS "
                "v2 search URL containing org and cws"
            )
        if set(query) - {"org", "cws"}:
            raise ValueError("Taleo search URL contains unsupported parameters")
        normalized_path = "/" + parsed.path.lstrip("/")
        return urlunsplit(
            (
                "https",
                hostname,
                normalized_path,
                urlencode((("org", query["org"]), ("cws", query["cws"]))),
                "",
            )
        )


class CatalogBoard(BaseModel):
    """Credential-free endpoint metadata for a registry-driven catalog."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    endpoint: str = Field(min_length=1, max_length=4_000)
    name: BoardName
    credential_name: str | None = Field(default=None, max_length=100)

    @field_validator("endpoint")
    @classmethod
    def valid_endpoint(cls, value: str) -> str:
        from urllib.parse import urlsplit, urlunsplit
        from .normalization import is_public_http_url

        raw = value.strip()
        parsed = urlsplit(raw)
        if (
            parsed.scheme.casefold() != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or not is_public_http_url(raw)
        ):
            raise ValueError("catalog endpoint must be a credential-free HTTPS URL")
        return urlunsplit(
            ("https", parsed.hostname.casefold(), parsed.path, parsed.query, "")
        )


class ProviderCatalogBoard(CatalogBoard):
    """Typed provider configuration for adapters with an explicit contract."""

    contract_version: str = Field(default="1", min_length=1, max_length=40)
    page_size: int = Field(default=100, ge=1, le=1_000)
    max_pages: int = Field(default=100, ge=1, le=1_000)


def _default_workday_boards() -> dict[BoardSlug, WorkdayBoard]:
    return {
        "nvidia": WorkdayBoard(
            tenant="nvidia",
            wd="wd5",
            site="NVIDIAExternalCareerSite",
            name="NVIDIA",
        ),
        "intel": WorkdayBoard(tenant="intel", wd="wd1", site="External", name="Intel"),
        "adobe": WorkdayBoard(
            tenant="adobe",
            wd="wd5",
            site="external_experienced",
            name="Adobe",
        ),
        "illumina": WorkdayBoard(
            tenant="illumina",
            wd="wd1",
            site="illumina-careers",
            name="Illumina",
        ),
        "disney": WorkdayBoard(
            tenant="disney",
            wd="wd5",
            site="disneycareer",
            name="Disney",
        ),
    }


class SourceSettings(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    greenhouse: dict[BoardSlug, BoardName] = Field(
        default_factory=lambda: {
            "anthropic": "Anthropic",
            "riotgames": "Riot Games",
            "neumora": "Neumora",
            "cloudflare": "Cloudflare",
            "figma": "Figma",
            "roblox": "Roblox",
            "discord": "Discord",
            "reddit": "Reddit",
            "pinterest": "Pinterest",
            "twitch": "Twitch",
            "databricks": "Databricks",
            "stabilityai": "Stability AI",
        }
    )
    lever: dict[BoardSlug, BoardName] = Field(
        default_factory=lambda: {
            "spotify": "Spotify",
            "wmg": "Warner Music Group",
            "palantir": "Palantir",
        }
    )
    ashby: dict[BoardSlug, BoardName] = Field(default_factory=dict)
    workable: dict[BoardSlug, BoardName] = Field(default_factory=dict)
    smartrecruiters: dict[BoardSlug, SmartRecruitersBoard] = Field(default_factory=dict)
    icims: dict[BoardSlug, ICIMSBoard] = Field(default_factory=dict)
    taleo: dict[BoardSlug, TaleoBoard] = Field(default_factory=dict)
    eightfold: dict[BoardSlug, ProviderCatalogBoard] = Field(default_factory=dict)
    oracle_hcm: dict[BoardSlug, ProviderCatalogBoard] = Field(default_factory=dict)
    rippling: dict[BoardSlug, ProviderCatalogBoard] = Field(default_factory=dict)
    paylocity: dict[BoardSlug, ProviderCatalogBoard] = Field(default_factory=dict)
    freehire: dict[BoardSlug, ProviderCatalogBoard] = Field(default_factory=dict)
    workday: dict[BoardSlug, WorkdayBoard] = Field(
        default_factory=_default_workday_boards
    )
    usajobs_locations: list[str] = Field(
        default_factory=lambda: ["San Diego, California"],
        max_length=20,
        description="USAJobs locations; use '*' for an explicit national search.",
    )

    @field_validator("usajobs_locations")
    @classmethod
    def valid_usajobs_locations(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in values:
            value = str(raw).strip()
            if not value:
                raise ValueError("USAJobs locations must not be blank")
            if len(value) > 200:
                raise ValueError("USAJobs locations must be 200 characters or fewer")
            key = value.casefold()
            if key not in seen:
                seen.add(key)
                normalized.append(value)
        return normalized


class AppConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, validate_default=True
    )

    timezone: str = "America/Los_Angeles"
    schedule_hour: int = Field(default=7, ge=0, le=23, strict=True)
    inventory_weekday: int = Field(
        default=6,
        ge=0,
        le=6,
        strict=True,
        description="Weekly inventory weekday using Monday=0 and Sunday=6.",
    )
    inventory_hour: int = Field(default=6, ge=0, le=23, strict=True)
    follow_up_days: int = Field(default=7, ge=0, le=90, strict=True)
    # A non-zero legacy salary_floor overrides these contextual floors.
    salary_floor: int = Field(default=0, ge=0, strict=True)
    contextual_salary_floors_enabled: bool = True
    federal_salary_floor: int = Field(default=74_000, ge=0, strict=True)
    private_salary_floor: int = Field(default=100_000, ge=0, strict=True)
    legal_ai_salary_floor: int = Field(default=140_000, ge=0, strict=True)
    nyc_salary_floor: int = Field(default=160_000, ge=0, strict=True)
    bay_area_salary_floor: int = Field(default=180_000, ge=0, strict=True)
    models: ModelSettings = Field(default_factory=ModelSettings)
    sources: SourceSettings = Field(default_factory=SourceSettings)
    notifications_enabled: bool = True
    # AI is an explicit opt-in. Merely storing a credential never enables a
    # provider or authorizes a paid request.
    openai_enabled: bool = False
    google_enabled: bool = False
    # Scheduled web discovery is a second opt-in because it can incur API
    # charges. A manual ``jobby scan --source web`` still requires
    # ``openai_enabled`` but does not require this scheduled-work flag.
    scheduled_web_enabled: bool = False
    scheduled_web_query: str = Field(
        default=DEFAULT_SCHEDULED_WEB_QUERY, min_length=1, max_length=2_000
    )
    scheduled_ai_max_runs_per_day: int = Field(default=1, ge=1, le=24, strict=True)
    scheduled_ai_max_total_tokens_per_run: int = Field(
        default=50_000, ge=1_000, le=2_000_000, strict=True
    )
    scheduled_ai_max_cost_usd_per_run: float = Field(
        default=1.0, gt=0, le=100, strict=True, allow_inf_nan=False
    )
    scheduled_ai_cost_estimate_usd_per_million_tokens: float = Field(
        default=20.0, gt=0, le=1_000, strict=True, allow_inf_nan=False
    )
    agent_stale_after_minutes: int = Field(default=180, ge=15, le=10_080, strict=True)
    scheduler_log_max_bytes: int = Field(
        default=5_000_000, ge=100_000, le=100_000_000, strict=True
    )
    scheduler_log_backup_count: int = Field(default=5, ge=1, le=20, strict=True)
    discovery_max_workers: int = Field(default=4, ge=1, le=8, strict=True)
    discovery_max_workers_per_source: int = Field(default=2, ge=1, le=2, strict=True)
    source_retry_attempts: int = Field(default=2, ge=0, le=2, strict=True)
    source_retry_after_max_seconds: float = Field(
        default=30.0, ge=0, le=120, allow_inf_nan=False
    )
    source_max_response_bytes: int = Field(
        default=25 * 1024 * 1024,
        ge=1_000_000,
        le=100 * 1024 * 1024,
        strict=True,
    )
    source_deadline_seconds: int = Field(default=600, ge=30, le=3_600, strict=True)
    source_record_cap: int = Field(default=5_000, ge=100, le=100_000, strict=True)
    source_anomaly_ratio: float = Field(default=0.5, gt=0, le=1, allow_inf_nan=False)
    source_anomaly_window: int = Field(default=5, ge=3, le=20, strict=True)
    # Configured career portals are crawled within this per-portal page budget;
    # 1 reads only the configured page, as Jobby did before crawling existed.
    portal_crawl_max_pages: int = Field(default=20, ge=1, le=200, strict=True)
    # Pages of 20 postings read per Workday board on daily and manual scans.
    # Weekly inventory scans read up to ``source_record_cap`` regardless.
    workday_scan_max_pages: int = Field(default=100, ge=1, le=250, strict=True)
    # Ranking career stage: "early" gates senior titles and long experience
    # requirements; "any" keeps evidence-only ranking with no seniority gate.
    ranking_target_seniority: Literal["any", "early", "mid", "senior"] = "any"
    # Title terms for unwanted role families (e.g. "software engineer"); they
    # cap fit unless the title also names a target role family.
    ranking_excluded_title_terms: list[
        Annotated[
            str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)
        ]
    ] = Field(default_factory=list, max_length=200)
    portal_crawl_delay_seconds: float = Field(
        default=1.0, ge=0, le=30, allow_inf_nan=False
    )
    ai_cache_ttl_days: int = Field(default=30, ge=1, le=365, strict=True)
    backup_daily_retention: int = Field(default=7, ge=1, le=365, strict=True)
    backup_weekly_retention: int = Field(default=4, ge=1, le=104, strict=True)
    backup_monthly_retention: int = Field(default=6, ge=1, le=120, strict=True)
    external_backup_destination: Path | None = None
    external_backup_scrypt_n: int = Field(
        default=32_768,
        ge=MIN_EXTERNAL_BACKUP_SCRYPT_N,
        le=MAX_EXTERNAL_BACKUP_SCRYPT_N,
        strict=True,
    )

    @field_validator("schedule_hour")
    @classmethod
    def valid_hour(cls, value: int) -> int:
        if not 0 <= value <= 23:
            raise ValueError("schedule_hour must be between 0 and 23")
        return value

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value: str) -> str:
        value = value.strip()
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(
                "timezone must be an IANA timezone such as America/Los_Angeles"
            ) from exc
        return value

    @field_validator("scheduled_web_query")
    @classmethod
    def valid_scheduled_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("scheduled_web_query must not be blank")
        return value

    @field_validator("external_backup_scrypt_n")
    @classmethod
    def valid_external_backup_scrypt_n(cls, value: int) -> int:
        if value & (value - 1):
            raise ValueError("external_backup_scrypt_n must be a power of two")
        return value


def load_config(paths: JobbyPaths | None = None) -> AppConfig:
    paths = paths or resolve_paths()
    config_file = paths.config_file
    if not config_file.exists() and not config_file.is_symlink():
        return AppConfig()
    if config_file.is_symlink():
        raise ValueError("configuration must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(config_file, flags)
    except OSError as exc:
        if config_file.is_symlink():
            raise ValueError("configuration must not be a symbolic link") from None
        raise ValueError(f"configuration cannot be read: {exc}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("configuration must be a regular file")
        if metadata.st_size > MAX_CONFIG_BYTES:
            raise ValueError(
                f"configuration exceeds the {MAX_CONFIG_BYTES:,}-byte safety limit"
            )
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = tomllib.load(handle)
    finally:
        os.close(descriptor)
    return AppConfig.model_validate(raw)


def save_config(config: AppConfig, paths: JobbyPaths | None = None) -> Path:
    """Write only validated, non-secret settings to TOML."""
    paths = (paths or resolve_paths()).ensure()
    try:
        import tomli_w
    except ImportError as exc:  # pragma: no cover - declared dependency
        raise RuntimeError("tomli-w is required to save configuration") from exc
    # TOML has no null value. Omit optional values recursively; Pydantic
    # restores their validated defaults when the file is read again.
    payload = config.model_dump(mode="json", exclude_none=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=".config-",
            suffix=".toml.tmp",
            dir=paths.config_dir,
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(tomli_w.dumps(payload))
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.chmod(0o600)
        temporary_path.replace(paths.config_file)
        _sync_directory(paths.config_dir)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return paths.config_file


def _sync_directory(path: Path) -> None:
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class SecretStoreError(RuntimeError):
    """A keyring operation failed without exposing credential material."""


@dataclass(frozen=True, slots=True)
class SecretLookup:
    name: str
    state: Literal["configured", "missing", "unavailable", "error"]
    value: str | None = field(default=None, repr=False)
    message: str = ""


@dataclass(frozen=True, slots=True)
class CredentialStatus:
    name: str
    state: Literal["configured", "missing", "unavailable", "error"]
    message: str = ""

    @property
    def configured(self) -> bool:
        return self.state == "configured"


class SecretStore:
    """Secrets live exclusively in the OS keyring."""

    def __init__(self, service: str = KEYRING_SERVICE):
        self.service = service

    @staticmethod
    def _validate_name(name: str) -> str:
        name = str(name or "").strip()
        if not _SECRET_NAME.fullmatch(name):
            raise ValueError(
                "secret name must use lowercase letters, digits, and underscores"
            )
        return name

    def lookup(self, name: str) -> SecretLookup:
        name = self._validate_name(name)
        try:
            import keyring

            value = keyring.get_password(self.service, name)
        except Exception as exc:
            kind = exc.__class__.__name__
            unavailable = kind in {
                "NoKeyringError",
                "InitError",
                "KeyringLocked",
                "DBusErrorResponse",
            }
            return SecretLookup(
                name=name,
                state="unavailable" if unavailable else "error",
                message=f"OS keyring {kind}"[:200],
            )
        if value is None:
            return SecretLookup(name=name, state="missing", message="not configured")
        if not isinstance(value, str) or not value.strip():
            return SecretLookup(
                name=name, state="error", message="stored value is blank or invalid"
            )
        if len(value) > MAX_SECRET_CHARS:
            return SecretLookup(
                name=name,
                state="error",
                message="stored value exceeds the safety limit",
            )
        return SecretLookup(name=name, state="configured", value=value)

    def get(self, name: str) -> str | None:
        return self.lookup(name).value

    def status(self, name: str) -> CredentialStatus:
        lookup = self.lookup(name)
        return CredentialStatus(lookup.name, lookup.state, lookup.message)

    def set(self, name: str, value: str) -> None:
        name = self._validate_name(name)
        if not value or not value.strip():
            raise ValueError("secret value must not be blank")
        if len(value) > MAX_SECRET_CHARS:
            raise ValueError(
                f"secret exceeds the {MAX_SECRET_CHARS:,}-character safety limit"
            )
        import keyring

        try:
            keyring.set_password(self.service, name, value)
        except Exception as exc:
            raise SecretStoreError(
                f"OS keyring could not store {name!r} ({exc.__class__.__name__})"
            ) from None

    def delete(self, name: str) -> bool:
        name = self._validate_name(name)
        lookup = self.lookup(name)
        if lookup.state == "missing":
            return False
        if lookup.state != "configured":
            raise SecretStoreError(
                f"OS keyring could not read {name!r} ({lookup.state})"
            )
        try:
            import keyring

            keyring.delete_password(self.service, name)
        except Exception as exc:
            raise SecretStoreError(
                f"OS keyring could not delete {name!r} ({exc.__class__.__name__})"
            ) from None
        return True


__all__ = [
    "APP_AUTHOR",
    "APP_NAME",
    "DEFAULT_SCHEDULED_WEB_QUERY",
    "AppConfig",
    "CredentialStatus",
    "JobbyPaths",
    "ModelSettings",
    "SecretLookup",
    "SecretStore",
    "SecretStoreError",
    "SmartRecruitersBoard",
    "SourceSettings",
    "ProviderCatalogBoard",
    "ICIMSBoard",
    "TaleoBoard",
    "WorkdayBoard",
    "load_config",
    "redacted_config",
    "resolve_paths",
    "save_config",
]


def redacted_config(config: AppConfig) -> dict[str, Any]:
    """A serialization helper that can never include credentials."""
    return config.model_dump(mode="json")
