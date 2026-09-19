"""Validated CRUD and detached views for editable discovery scan profiles."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import re
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from .audit import record_audit
from .db import Database
from .models import ScanProfile


MAX_PROFILE_ITEMS = 100
MAX_QUERY_LENGTH = 2_000
MAX_FILTER_LENGTH = 200
MAX_DESCRIPTION_LENGTH = 10_000
_HYDRATION_POLICY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,39}$")

ProfileSelector = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
]
ProfileQuery = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_QUERY_LENGTH),
]
ProfileFilter = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True, min_length=1, max_length=MAX_FILTER_LENGTH
    ),
]


class ScanProfileDraft(BaseModel):
    """Complete validated state for creating a scan profile."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    name: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    source_selectors: tuple[ProfileSelector, ...] = Field(
        default=("all",),
        max_length=MAX_PROFILE_ITEMS,
        validation_alias=AliasChoices("source_selectors", "sources"),
    )
    query_pack: tuple[ProfileQuery, ...] = Field(
        default_factory=tuple,
        max_length=MAX_PROFILE_ITEMS,
        validation_alias=AliasChoices("query_pack", "queries"),
    )
    location_filters: tuple[ProfileFilter, ...] = Field(
        default_factory=tuple, max_length=MAX_PROFILE_ITEMS
    )
    role_filters: tuple[ProfileFilter, ...] = Field(
        default_factory=tuple, max_length=MAX_PROFILE_ITEMS
    )
    hydration_policy: str = Field(default="focused", min_length=1, max_length=40)
    enabled: bool = False

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return _required_single_line(value, "profile name")

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.replace("\x00", "�").strip()
        return value or None

    @field_validator("source_selectors")
    @classmethod
    def normalize_selectors(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            dict.fromkeys(
                _required_single_line(value, "source selector").casefold()
                for value in values
            )
        )
        return normalized

    @field_validator("query_pack", "location_filters", "role_filters")
    @classmethod
    def normalize_lists(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                _required_single_line(value, "profile value") for value in values
            )
        )

    @field_validator("hydration_policy")
    @classmethod
    def normalize_hydration_policy(cls, value: str) -> str:
        value = value.strip().casefold()
        if not _HYDRATION_POLICY_RE.fullmatch(value):
            raise ValueError(
                "hydration policy must be a lowercase identifier of at most 40 characters"
            )
        return value

    @model_validator(mode="after")
    def validate_enabled_profile(self) -> ScanProfileDraft:
        if self.enabled and not self.source_selectors:
            raise ValueError("an enabled profile needs at least one source selector")
        if self.enabled and not (self.query_pack or self.role_filters):
            raise ValueError("an enabled profile needs a query or role filter")
        return self


class ScanProfilePatch(BaseModel):
    """Partial profile update; only explicitly supplied values are changed."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    name: str | None = Field(default=None, min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=MAX_DESCRIPTION_LENGTH)
    source_selectors: tuple[ProfileSelector, ...] | None = Field(
        default=None,
        max_length=MAX_PROFILE_ITEMS,
        validation_alias=AliasChoices("source_selectors", "sources"),
    )
    query_pack: tuple[ProfileQuery, ...] | None = Field(
        default=None,
        max_length=MAX_PROFILE_ITEMS,
        validation_alias=AliasChoices("query_pack", "queries"),
    )
    location_filters: tuple[ProfileFilter, ...] | None = Field(
        default=None, max_length=MAX_PROFILE_ITEMS
    )
    role_filters: tuple[ProfileFilter, ...] | None = Field(
        default=None, max_length=MAX_PROFILE_ITEMS
    )
    hydration_policy: str | None = Field(default=None, min_length=1, max_length=40)
    enabled: bool | None = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str | None) -> str | None:
        return (
            _required_single_line(value, "profile name") if value is not None else None
        )

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.replace("\x00", "�").strip()
        return value or None

    @field_validator("source_selectors")
    @classmethod
    def normalize_selectors(
        cls, values: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if values is None:
            return None
        return tuple(
            dict.fromkeys(
                _required_single_line(value, "source selector").casefold()
                for value in values
            )
        )

    @field_validator("query_pack", "location_filters", "role_filters")
    @classmethod
    def normalize_lists(cls, values: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if values is None:
            return None
        return tuple(
            dict.fromkeys(
                _required_single_line(value, "profile value") for value in values
            )
        )

    @field_validator("hydration_policy")
    @classmethod
    def normalize_hydration_policy(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().casefold()
        if not _HYDRATION_POLICY_RE.fullmatch(value):
            raise ValueError(
                "hydration policy must be a lowercase identifier of at most 40 characters"
            )
        return value


class ScanProfileRecord(BaseModel):
    """Immutable detached profile safe to use outside an ORM session."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    name: str
    description: str | None
    source_selectors: tuple[str, ...]
    query_pack: tuple[str, ...]
    location_filters: tuple[str, ...]
    role_filters: tuple[str, ...]
    hydration_policy: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


def list_profiles(
    database: Database, *, enabled: bool | None = None
) -> tuple[ScanProfileRecord, ...]:
    """List profiles deterministically without returning session-bound rows."""

    database.initialize()
    statement = select(ScanProfile)
    if enabled is not None:
        statement = statement.where(ScanProfile.enabled.is_(enabled))
    statement = statement.order_by(func.lower(ScanProfile.name), ScanProfile.id)
    with database.session() as session:
        return tuple(_record(profile) for profile in session.scalars(statement))


def get_profile(database: Database, identity: str) -> ScanProfileRecord:
    database.initialize()
    identity = _required_single_line(identity, "profile identity")
    with database.session() as session:
        profile = _resolve_profile(session, identity)
        return _record(profile)


def create_profile(
    database: Database,
    draft: ScanProfileDraft | Mapping[str, object],
) -> ScanProfileRecord:
    """Create exactly one profile; disabled remains the default."""

    database.initialize()
    value = (
        draft
        if isinstance(draft, ScanProfileDraft)
        else ScanProfileDraft.model_validate(draft)
    )
    try:
        with database.session() as session:
            duplicate = session.scalar(
                select(ScanProfile.id).where(
                    func.lower(ScanProfile.name) == value.name.casefold()
                )
            )
            if duplicate is not None:
                raise ValueError(f"scan profile already exists: {value.name}")
            profile = ScanProfile(
                name=value.name,
                description=value.description,
                source_selectors=list(value.source_selectors),
                query_pack=list(value.query_pack),
                location_filters=list(value.location_filters),
                role_filters=list(value.role_filters),
                hydration_policy=value.hydration_policy,
                enabled=value.enabled,
            )
            session.add(profile)
            session.flush()
            record_audit(
                session,
                action="scan_profile.created",
                entity_type="scan_profile",
                entity_id=profile.id,
                after=_audit_state(profile),
            )
            result = _record(profile)
        return result
    except IntegrityError as exc:
        raise ValueError(f"scan profile already exists: {value.name}") from exc


def update_profile(
    database: Database,
    identity: str,
    patch: ScanProfilePatch | Mapping[str, object] | None = None,
    **changes: object,
) -> ScanProfileRecord:
    """Apply an explicit partial update after validating the complete result."""

    database.initialize()
    if patch is not None and changes:
        raise ValueError("pass either a profile patch or keyword changes, not both")
    if patch is None:
        value = ScanProfilePatch.model_validate(changes)
    elif isinstance(patch, ScanProfilePatch):
        value = patch
    else:
        value = ScanProfilePatch.model_validate(patch)
    updates = value.model_dump(exclude_unset=True)
    if not updates:
        raise ValueError("profile update did not include any changes")

    try:
        with database.session() as session:
            profile = _resolve_profile(
                session, _required_single_line(identity, "profile identity")
            )
            before = _audit_state(profile)
            merged = {
                "name": profile.name,
                "description": profile.description,
                "source_selectors": tuple(profile.source_selectors),
                "query_pack": tuple(profile.query_pack),
                "location_filters": tuple(profile.location_filters),
                "role_filters": tuple(profile.role_filters),
                "hydration_policy": profile.hydration_policy,
                "enabled": profile.enabled,
                **updates,
            }
            validated = ScanProfileDraft.model_validate(merged)
            duplicate = session.scalar(
                select(ScanProfile.id).where(
                    func.lower(ScanProfile.name) == validated.name.casefold(),
                    ScanProfile.id != profile.id,
                )
            )
            if duplicate is not None:
                raise ValueError(f"scan profile already exists: {validated.name}")
            profile.name = validated.name
            profile.description = validated.description
            profile.source_selectors = list(validated.source_selectors)
            profile.query_pack = list(validated.query_pack)
            profile.location_filters = list(validated.location_filters)
            profile.role_filters = list(validated.role_filters)
            profile.hydration_policy = validated.hydration_policy
            profile.enabled = validated.enabled
            session.flush()
            record_audit(
                session,
                action="scan_profile.updated",
                entity_type="scan_profile",
                entity_id=profile.id,
                before=before,
                after=_audit_state(profile),
            )
            result = _record(profile)
        return result
    except IntegrityError as exc:
        raise ValueError(
            "scan profile name conflicts with an existing profile"
        ) from exc


def delete_profile(database: Database, identity: str) -> ScanProfileRecord:
    """Delete one explicitly selected profile and retain an audit snapshot."""

    database.initialize()
    with database.session() as session:
        profile = _resolve_profile(
            session, _required_single_line(identity, "profile identity")
        )
        result = _record(profile)
        record_audit(
            session,
            action="scan_profile.deleted",
            entity_type="scan_profile",
            entity_id=profile.id,
            before=_audit_state(profile),
        )
        session.delete(profile)
    return result


def _resolve_profile(session: Any, identity: str) -> ScanProfile:
    profile = session.get(ScanProfile, identity)
    if profile is None:
        profile = session.scalar(
            select(ScanProfile).where(
                func.lower(ScanProfile.name) == identity.casefold()
            )
        )
    if profile is None:
        raise LookupError(f"scan profile not found: {identity}")
    return profile


def _record(profile: ScanProfile) -> ScanProfileRecord:
    return ScanProfileRecord(
        id=profile.id,
        name=profile.name,
        description=profile.description,
        source_selectors=tuple(profile.source_selectors),
        query_pack=tuple(profile.query_pack),
        location_filters=tuple(profile.location_filters),
        role_filters=tuple(profile.role_filters),
        hydration_policy=profile.hydration_policy,
        enabled=profile.enabled,
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


def _audit_state(profile: ScanProfile) -> dict[str, object]:
    return {
        "name": profile.name,
        "description": profile.description,
        "source_selectors": list(profile.source_selectors),
        "query_pack": list(profile.query_pack),
        "location_filters": list(profile.location_filters),
        "role_filters": list(profile.role_filters),
        "hydration_policy": profile.hydration_policy,
        "enabled": profile.enabled,
    }


def _required_single_line(value: object, label: str) -> str:
    normalized = " ".join(str(value or "").replace("\x00", "�").split())
    if not normalized:
        raise ValueError(f"{label} must not be blank")
    return normalized


# Explicit scan-prefixed aliases read naturally from CLI/MCP service code.
create_scan_profile = create_profile
delete_scan_profile = delete_profile
get_scan_profile = get_profile
list_scan_profiles = list_profiles
update_scan_profile = update_profile


__all__ = [
    "ScanProfileDraft",
    "ScanProfilePatch",
    "ScanProfileRecord",
    "create_profile",
    "create_scan_profile",
    "delete_profile",
    "delete_scan_profile",
    "get_profile",
    "get_scan_profile",
    "list_profiles",
    "list_scan_profiles",
    "update_profile",
    "update_scan_profile",
]
