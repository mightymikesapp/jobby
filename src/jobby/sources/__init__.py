"""Job discovery source contracts, ATS adapters, and registry."""

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
from jobby.sources.base import (
    JobSource,
    ScanItem,
    ScanStatus,
    SourceError,
    SourceResult,
)
from jobby.sources.catalog import (
    CatalogSource,
    EightfoldSource,
    FreehireSource,
    OracleHCMSource,
    PaylocitySource,
    RipplingSource,
)
from jobby.sources.registry import BUILTIN_SOURCES, DEFAULT_REGISTRY, SourceRegistry


__all__ = [
    "AshbySource",
    "CatalogSource",
    "BUILTIN_SOURCES",
    "DEFAULT_REGISTRY",
    "GreenhouseSource",
    "EightfoldSource",
    "FreehireSource",
    "ICIMSSource",
    "JobSource",
    "LeverSource",
    "OracleHCMSource",
    "PaylocitySource",
    "RipplingSource",
    "SmartRecruitersSource",
    "TaleoSource",
    "ScanItem",
    "ScanStatus",
    "SourceError",
    "SourceRegistry",
    "SourceResult",
    "USAJobsSource",
    "WorkableSource",
    "WorkdaySource",
]
