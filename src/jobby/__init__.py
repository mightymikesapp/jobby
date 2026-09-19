"""Jobby: a local-first personal job-hunting workbench."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("jobby")
except PackageNotFoundError:  # pragma: no cover - source checkout before install
    __version__ = "0.6.0"

__all__ = ["__version__"]
