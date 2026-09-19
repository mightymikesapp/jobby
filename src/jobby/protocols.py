"""Provider boundaries that keep optional external systems out of core workflows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from .sources.base import SourceResult


T = TypeVar("T", bound=BaseModel)


@runtime_checkable
class JobSource(Protocol):
    name: str
    source_key: str
    concurrency_key: str | None

    def scan(self, query: str | None = None) -> SourceResult: ...


@runtime_checkable
class WebSearchProvider(Protocol):
    def search(self, query: str) -> Any: ...


@runtime_checkable
class AIProvider(Protocol):
    def parse(
        self, *, purpose: str, text: str, output_type: type[T], prompt_version: str
    ) -> T: ...
    def validate_models(self) -> Mapping[str, str | None]: ...


@runtime_checkable
class DocumentRenderer(Protocol):
    def render(self, content: str, output: Path, *, title: str = "") -> Path: ...


@runtime_checkable
class MailProvider(Protocol):
    def list_metadata(
        self, *, query: str, limit: int = 100
    ) -> Iterable[Mapping[str, Any]]: ...
    def get_body(self, message_id: str) -> str: ...


@runtime_checkable
class CalendarProvider(Protocol):
    def list_events(
        self, *, start: datetime, end: datetime, limit: int = 250
    ) -> Iterable[Mapping[str, Any]]: ...


@runtime_checkable
class NotificationProvider(Protocol):
    def notify(self, title: str, message: str, *, urgent: bool = False) -> bool: ...
