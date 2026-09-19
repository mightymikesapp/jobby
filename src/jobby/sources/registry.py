"""Registry for constructing independent discovery adapters from configuration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import httpx

from jobby.sources.ats import (
    AshbySource,
    GreenhouseSource,
    ICIMSSource,
    LeverSource,
    SmartRecruitersSource,
    TaleoSource,
    USAJobsSource,
    WorkableSource,
    WorkdaySource,
)
from jobby.sources.base import JobSource
from jobby.sources.catalog import (
    EightfoldSource,
    FreehireSource,
    OracleHCMSource,
    PaylocitySource,
    RipplingSource,
)


SourceFactory = Callable[..., JobSource]


BUILTIN_SOURCES: Mapping[str, SourceFactory] = {
    "greenhouse": GreenhouseSource,
    "lever": LeverSource,
    "smartrecruiters": SmartRecruitersSource,
    "icims": ICIMSSource,
    "taleo": TaleoSource,
    "ashby": AshbySource,
    "workable": WorkableSource,
    "workday": WorkdaySource,
    "usajobs": USAJobsSource,
    "eightfold": EightfoldSource,
    "oracle_hcm": OracleHCMSource,
    "rippling": RipplingSource,
    "paylocity": PaylocitySource,
    "freehire": FreehireSource,
}


class SourceRegistry:
    """A small explicit registry; importing an adapter has no scan side effects."""

    def __init__(self, factories: Mapping[str, SourceFactory] | None = None) -> None:
        self._factories: dict[str, SourceFactory] = {}
        for name, factory in (factories or {}).items():
            self.register(name, factory)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))

    def register(
        self, name: str, factory: SourceFactory, *, replace: bool = False
    ) -> None:
        normalized = name.strip().casefold()
        if not normalized:
            raise ValueError("source name is required")
        if normalized in self._factories and not replace:
            raise ValueError(f"source already registered: {normalized}")
        if not callable(factory):
            raise TypeError("source factory must be callable")
        self._factories[normalized] = factory

    def unregister(self, name: str) -> None:
        normalized = name.strip().casefold()
        try:
            del self._factories[normalized]
        except KeyError as exc:
            raise KeyError(f"unknown source: {normalized}") from exc

    def create(
        self,
        name: str,
        *,
        client: httpx.Client,
        config: Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> JobSource:
        normalized = name.strip().casefold()
        try:
            factory = self._factories[normalized]
        except KeyError as exc:
            available = ", ".join(self.names) or "none"
            raise KeyError(
                f"unknown source {normalized!r}; available: {available}"
            ) from exc
        options = dict(config or {})
        options.update(overrides)
        return factory(client=client, **options)


def build_default_registry() -> SourceRegistry:
    return SourceRegistry(BUILTIN_SOURCES)


DEFAULT_REGISTRY = build_default_registry()


__all__ = [
    "BUILTIN_SOURCES",
    "DEFAULT_REGISTRY",
    "SourceFactory",
    "SourceRegistry",
    "build_default_registry",
]
