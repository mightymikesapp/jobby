from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy import select

from jobby.config import JobbyPaths
from jobby.db import Database
from jobby.enums import ApprovalState, SuggestionKind
from jobby.google_integration import (
    CALENDAR_EVENTS_READONLY_SCOPE,
    CALENDAR_READONLY_SCOPE,
    GMAIL_READONLY_SCOPE,
    CalendarProvider,
    GmailProvider,
    GoogleIntegrationUnavailable,
    GoogleOAuthManager,
    calendar_interview_suggestions,
    ingest_calendar_metadata,
    ingest_gmail_metadata,
    retrieve_full_body,
    suggest_email_event,
)
from jobby.models import AuditEvent, EmailMessage, ExternalSuggestion, IntegrationState


def make_database(tmp_path: Path) -> Database:
    paths = JobbyPaths(
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
        cache_dir=tmp_path / "cache",
        database=tmp_path / "data" / "jobby.sqlite3",
        artifacts_dir=tmp_path / "data" / "artifacts",
        backups_dir=tmp_path / "data" / "backups",
        logs_dir=tmp_path / "data" / "logs",
        config_file=tmp_path / "config" / "config.toml",
    )
    database = Database(paths=paths)
    database.initialize()
    return database


class DictSecrets:
    def __init__(self, values: dict[str, str]):
        self.values = dict(values)

    def get(self, name: str) -> str | None:
        return self.values.get(name)

    def delete(self, name: str) -> bool:
        return self.values.pop(name, None) is not None


def test_google_disconnect_revokes_refresh_token_before_removing_local_copy() -> None:
    secrets = DictSecrets(
        {
            "google_oauth_token": json.dumps(
                {"token": "short-lived", "refresh_token": "durable-grant"}
            )
        }
    )
    client = MagicMock()
    client.post.return_value.raise_for_status.return_value = None

    result = GoogleOAuthManager(secrets).disconnect(client=client)

    assert result.remote_token_revoked is True
    assert result.local_token_removed is True
    assert result.authorization_retained is False
    assert "google_oauth_token" not in secrets.values
    assert client.post.call_args.kwargs["data"] == {"token": "durable-grant"}


def test_google_disconnect_retains_retry_token_when_remote_revocation_fails() -> None:
    serialized = json.dumps({"refresh_token": "retry-me"})
    secrets = DictSecrets({"google_oauth_token": serialized})
    client = MagicMock()
    client.post.side_effect = RuntimeError("offline")

    result = GoogleOAuthManager(secrets).disconnect(client=client)

    assert result.remote_token_revoked is False
    assert result.local_token_removed is False
    assert result.authorization_retained is True
    assert secrets.values["google_oauth_token"] == serialized
    assert "retained" in result.detail


def test_google_disconnect_requires_explicit_local_only_for_malformed_token() -> None:
    secrets = DictSecrets({"google_oauth_token": "not-json"})
    manager = GoogleOAuthManager(secrets)

    retained = manager.disconnect()
    assert retained.authorization_retained is True
    assert "google_oauth_token" in secrets.values

    removed = manager.disconnect(revoke=False)
    assert removed.local_token_removed is True
    assert removed.remote_token_revoked is False
    assert removed.authorization_retained is False
    assert "google_oauth_token" not in secrets.values


def test_gmail_lists_metadata_without_fetching_full_bodies() -> None:
    service = MagicMock()
    messages = service.users.return_value.messages.return_value
    messages.list.return_value.execute.return_value = {
        "messages": [{"id": "m-1"}, {"id": "m-2"}]
    }
    first = MagicMock()
    first.execute.return_value = {
        "id": "m-1",
        "threadId": "t-1",
        "snippet": "Thank you for applying",
        "payload": {
            "headers": [
                {"name": "From", "value": "recruiter@example.com"},
                {"name": "Subject", "value": "Application received"},
                {"name": "Date", "value": "Fri, 10 Jul 2026 09:00:00 -0700"},
            ]
        },
    }
    second = MagicMock()
    second.execute.return_value = {
        "id": "m-2",
        "threadId": "t-2",
        "snippet": "Please schedule an interview",
        "payload": {"headers": [{"name": "Subject", "value": "Interview"}]},
    }
    messages.get.side_effect = [first, second]
    provider = GmailProvider(service=service)

    result = provider.list_metadata(query="newer_than:30d", limit=999)

    messages.list.assert_called_once_with(
        userId="me", q="newer_than:30d", maxResults=500
    )
    assert [item["message_id"] for item in result] == ["m-1", "m-2"]
    assert result[0] == {
        "message_id": "m-1",
        "thread_id": "t-1",
        "sender": "recruiter@example.com",
        "subject": "Application received",
        "date": "Fri, 10 Jul 2026 09:00:00 -0700",
        "snippet": "Thank you for applying",
    }
    assert all(
        call.kwargs["format"] == "metadata" for call in messages.get.call_args_list
    )


def test_gmail_rejects_unbounded_queries_and_skips_malformed_stubs() -> None:
    service = MagicMock()
    messages = service.users.return_value.messages.return_value
    provider = GmailProvider(service=service)

    with pytest.raises(ValueError, match="must not be blank"):
        provider.list_metadata(query="  ")
    with pytest.raises(ValueError, match="1,000-character limit"):
        provider.list_metadata(query="x" * 1_001)
    with pytest.raises(ValueError, match="must be an integer"):
        provider.list_metadata(query="recruiter", limit=True)
    with pytest.raises(ValueError, match="must be an integer"):
        provider.list_metadata(query="recruiter", limit=1.5)
    messages.list.assert_not_called()

    messages.list.return_value.execute.return_value = {
        "messages": [None, {}, {"id": ""}, {"id": "valid"}]
    }
    messages.get.return_value.execute.return_value = {
        "id": "valid",
        "snippet": "Interview",
        "payload": {"headers": []},
    }

    assert provider.list_metadata(query="recruiter") == [
        {
            "message_id": "valid",
            "thread_id": None,
            "sender": None,
            "subject": None,
            "date": None,
            "snippet": "Interview",
        }
    ]
    messages.get.assert_called_once()


def test_gmail_rejects_malformed_remote_response_shapes() -> None:
    service = MagicMock()
    messages = service.users.return_value.messages.return_value
    provider = GmailProvider(service=service)

    messages.list.return_value.execute.return_value = {"messages": "not-a-list"}
    with pytest.raises(ValueError, match="messages must be a list"):
        provider.list_metadata(query="recruiter")

    messages.list.return_value.execute.return_value = {"messages": [{"id": "m-1"}]}
    messages.get.return_value.execute.return_value = {
        "id": "m-1",
        "payload": {"headers": ["not-an-object"]},
    }
    with pytest.raises(ValueError, match="header must be an object"):
        provider.list_metadata(query="recruiter")

    messages.get.return_value.execute.return_value = {
        "id": "m-1",
        "payload": "not-an-object",
    }
    with pytest.raises(ValueError, match="payload must be an object"):
        provider.get_body("m-1")


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        (
            "We regret to inform you that you were not selected.",
            SuggestionKind.REJECTED,
        ),
        (
            "Please schedule your interview this week.",
            SuggestionKind.INTERVIEW_REQUESTED,
        ),
        ("This role has been closed.", SuggestionKind.POSITION_CLOSED),
        (
            "Thank you for applying; your application was received.",
            SuggestionKind.APPLIED,
        ),
    ],
)
def test_email_suggestions_are_deterministic_previews(
    text: str, kind: SuggestionKind
) -> None:
    suggestion = suggest_email_event(text)

    assert suggestion is not None
    assert suggestion[0] == kind
    assert 0 < suggestion[1] <= 1
    assert suggestion[2]


def test_email_follow_up_without_a_date_is_not_an_unusable_suggestion() -> None:
    assert suggest_email_event("We will be in touch about next steps.") is None


def test_gmail_metadata_ingestion_is_idempotent_and_never_fetches_body(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    provider = MagicMock()
    provider.list_metadata.return_value = [
        {
            "message_id": "m-1",
            "sender": "recruiter@example.com",
            "subject": "Application received",
            "date": "Fri, 10 Jul 2026 09:00:00 -0700",
            "snippet": "Thank you for applying.",
        },
        {
            "message_id": "m-2",
            "sender": "coordinator@example.com",
            "subject": "Interview invitation",
            "date": "Sat, 11 Jul 2026 10:30:00 -0700",
            "snippet": "Please schedule your interview.",
        },
    ]

    with database.session() as session:
        assert ingest_gmail_metadata(session, provider) == (2, 2)
    with database.session() as session:
        assert ingest_gmail_metadata(session, provider) == (0, 0)

    provider.get_body.assert_not_called()
    with database.session() as session:
        messages = list(
            session.scalars(
                select(EmailMessage).order_by(EmailMessage.provider_message_id)
            )
        )
        suggestions = list(
            session.scalars(
                select(ExternalSuggestion).order_by(ExternalSuggestion.kind)
            )
        )
        state = session.scalar(
            select(IntegrationState).where(IntegrationState.provider == "gmail")
        )
        audits = list(
            session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gmail.metadata_ingested")
            )
        )
        assert len(messages) == 2
        assert messages[0].sender == "recruiter@example.com"
        assert messages[0].full_body_retrieved is False
        assert messages[0].received_at is not None
        assert {item.kind for item in suggestions} == {
            SuggestionKind.APPLIED,
            SuggestionKind.INTERVIEW_REQUESTED,
        }
        assert all(item.approval_state == ApprovalState.PENDING for item in suggestions)
        assert all(item.payload["preview_only"] is True for item in suggestions)
        assert state is not None
        assert state.scopes == [GMAIL_READONLY_SCOPE]
        assert state.health == "ok"
        assert state.last_sync_at is not None
        assert all(item.entity_id == state.id for item in audits)
    database.dispose()


def test_gmail_metadata_ingestion_rejects_invalid_injected_message_ids(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    provider = MagicMock()
    provider.list_metadata.return_value = [{"message_id": None}]

    with database.session() as session:
        with pytest.raises(ValueError, match="invalid message ID"):
            ingest_gmail_metadata(session, provider)

    with database.session() as session:
        assert session.scalar(select(EmailMessage)) is None
        assert session.scalar(select(IntegrationState)) is None
    database.dispose()


def test_full_body_is_retrieved_only_on_demand_and_marked(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    with database.session() as session:
        message = EmailMessage(
            provider_message_id="m-1",
            sender="recruiter@example.com",
            subject="Details",
            snippet="Preview",
            full_body_retrieved=False,
        )
        session.add(message)
    provider = MagicMock()
    provider.get_body.return_value = "Full recruiting message"

    with database.session() as session:
        body = retrieve_full_body(session, provider, "m-1")

    assert body == "Full recruiting message"
    provider.get_body.assert_called_once_with("m-1")
    with database.session() as session:
        message = session.scalar(
            select(EmailMessage).where(EmailMessage.provider_message_id == "m-1")
        )
        assert message is not None and message.full_body_retrieved is True
    database.dispose()


def test_gmail_body_decoder_prefers_plain_text() -> None:
    service = MagicMock()
    request = service.users.return_value.messages.return_value.get.return_value
    plain = base64.urlsafe_b64encode(b"Plain interview details").decode().rstrip("=")
    html = base64.urlsafe_b64encode(b"<p>HTML details</p>").decode().rstrip("=")
    request.execute.return_value = {
        "payload": {
            "mimeType": "multipart/alternative",
            "parts": [
                {"mimeType": "text/html", "body": {"data": html}},
                {"mimeType": "text/plain", "body": {"data": plain}},
            ],
        }
    }

    assert GmailProvider(service=service).get_body("m-1") == "Plain interview details"


def test_gmail_body_decoder_rejects_invalid_base64_and_excessive_nesting() -> None:
    service = MagicMock()
    request = service.users.return_value.messages.return_value.get.return_value
    request.execute.return_value = {
        "payload": {"mimeType": "text/plain", "body": {"data": "!!!!"}}
    }
    provider = GmailProvider(service=service)
    with pytest.raises(ValueError, match="invalid base64"):
        provider.get_body("m-1")

    nested: dict[str, object] = {"parts": []}
    root = nested
    for _ in range(22):
        child: dict[str, object] = {"parts": []}
        root["parts"] = [child]
        root = child
    request.execute.return_value = {"payload": nested}
    with pytest.raises(ValueError, match="nesting"):
        provider.get_body("m-2")

    request.execute.return_value = {"payload": {"parts": "not-a-list"}}
    with pytest.raises(ValueError, match="parts must be a list"):
        provider.get_body("m-3")


def test_gmail_body_decoder_enforces_one_total_output_budget(monkeypatch) -> None:
    monkeypatch.setattr("jobby.google_integration.MAX_EMAIL_BODY_CHARS", 10)
    service = MagicMock()
    request = service.users.return_value.messages.return_value.get.return_value
    first = base64.urlsafe_b64encode(b"12345678").decode().rstrip("=")
    second = base64.urlsafe_b64encode(b"ABCDEFGH").decode().rstrip("=")
    request.execute.return_value = {
        "payload": {
            "mimeType": "multipart/mixed",
            "parts": [
                {"mimeType": "text/plain", "body": {"data": first}},
                {"mimeType": "text/plain", "body": {"data": second}},
            ],
        }
    }

    assert GmailProvider(service=service).get_body("m-1") == "12345678\nA"


def test_calendar_interview_imports_are_pending_previews() -> None:
    events = [
        {
            "id": "event-1",
            "summary": "Recruiter screen — Acme",
            "description": "Meet the hiring manager",
            "start": {"dateTime": "2026-07-12T10:00:00-07:00"},
            "end": {"dateTime": "2026-07-12T10:30:00-07:00"},
            "hangoutLink": "https://meet.example/abc",
        },
        {"id": "event-2", "summary": "Dentist"},
    ]

    suggestions = calendar_interview_suggestions(events)

    assert suggestions == [
        {
            "kind": SuggestionKind.CALENDAR_INTERVIEW.value,
            "external_event_id": "event-1",
            "summary": "Recruiter screen — Acme",
            "start": {"dateTime": "2026-07-12T10:00:00-07:00"},
            "end": {"dateTime": "2026-07-12T10:30:00-07:00"},
            "location": "https://meet.example/abc",
            "approval_state": ApprovalState.PENDING.value,
        }
    ]


def test_calendar_is_read_only_even_when_a_caller_requests_write() -> None:
    service = MagicMock()
    readonly = CalendarProvider(service=service, allow_write=False)

    assert not hasattr(readonly, "create_or_update_event")
    with pytest.raises(PermissionError, match="read-only"):
        CalendarProvider(service=service, allow_write=True)
    service.events.assert_not_called()


def test_calendar_provider_rejects_malformed_and_bounds_remote_results() -> None:
    service = MagicMock()
    request = service.events.return_value.list.return_value
    provider = CalendarProvider(service=service)
    start = datetime(2026, 7, 12, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    request.execute.return_value = {"items": "not-a-list"}
    with pytest.raises(ValueError, match="items must be a list"):
        provider.list_events(start=start, end=end, limit=2)

    request.execute.return_value = {
        "items": [{"id": "one"}, None, {"id": "three"}, {"id": "four"}]
    }
    assert provider.list_events(start=start, end=end, limit=2) == [{"id": "one"}]


def test_calendar_metadata_ingestion_is_idempotent_and_read_only(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    provider = MagicMock(spec=CalendarProvider)
    provider.list_events.return_value = [
        {
            "id": "event-1",
            "summary": "Recruiter screen — Acme",
            "description": "Meet the hiring manager",
            "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
            "end": {"dateTime": "2026-07-14T10:30:00-07:00"},
            "hangoutLink": "https://meet.example/abc",
        },
        {
            "id": "event-2",
            "summary": "Dentist",
            "start": {"dateTime": "2026-07-15T11:00:00-07:00"},
            "end": {"dateTime": "2026-07-15T11:30:00-07:00"},
        },
        {
            "summary": "Interview with Example Corp",
            "start": {"dateTime": "2026-07-16T09:00:00-07:00"},
            "end": {"dateTime": "2026-07-16T09:30:00-07:00"},
        },
    ]
    start = datetime(2026, 7, 12, tzinfo=timezone.utc)
    end = datetime(2026, 8, 11, tzinfo=timezone.utc)

    # Simulate an old integration row so a read-only sync must narrow its
    # recorded capabilities instead of preserving stale write scopes.
    with database.session() as session:
        session.add(
            IntegrationState(
                provider="calendar",
                enabled=False,
                scopes=["https://www.googleapis.com/auth/calendar.events"],
                health="stale",
            )
        )
    with database.session() as session:
        assert (
            ingest_calendar_metadata(session, provider, start=start, end=end, limit=100)
            == 1
        )
    with database.session() as session:
        assert (
            ingest_calendar_metadata(session, provider, start=start, end=end, limit=100)
            == 0
        )

    assert provider.method_calls == [
        call.list_events(start=start, end=end, limit=100),
        call.list_events(start=start, end=end, limit=100),
    ]
    with database.session() as session:
        suggestions = list(session.scalars(select(ExternalSuggestion)))
        state = session.scalar(
            select(IntegrationState).where(IntegrationState.provider == "calendar")
        )
        audits = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "calendar.metadata_ingested"
                )
            )
        )

        assert len(suggestions) == 1
        suggestion = suggestions[0]
        assert suggestion.kind == SuggestionKind.CALENDAR_INTERVIEW
        assert suggestion.external_event_id == "event-1"
        assert suggestion.application_id is None
        assert suggestion.approval_state == ApprovalState.PENDING
        assert suggestion.applied_at is None
        assert suggestion.payload == {
            "summary": "Recruiter screen — Acme",
            "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
            "end": {"dateTime": "2026-07-14T10:30:00-07:00"},
            "location": "https://meet.example/abc",
            "preview_only": True,
        }
        assert state is not None
        assert state.enabled is True
        assert state.scopes == [CALENDAR_EVENTS_READONLY_SCOPE]
        assert state.health == "ok"
        assert state.last_sync_at is not None
        assert [item.after_json["suggestions"] for item in audits] == [1, 0]
        assert all(item.entity_id == state.id for item in audits)
    database.dispose()


def test_calendar_pending_preview_is_refreshed_when_the_event_reschedules(
    tmp_path: Path,
) -> None:
    database = make_database(tmp_path)
    provider = MagicMock(spec=CalendarProvider)
    original = {
        "id": "event-1",
        "summary": "Recruiter interview",
        "start": {"dateTime": "2026-07-14T10:00:00-07:00"},
        "end": {"dateTime": "2026-07-14T10:30:00-07:00"},
    }
    revised = {
        **original,
        "start": {"dateTime": "2026-07-15T11:00:00-07:00"},
        "end": {"dateTime": "2026-07-15T11:30:00-07:00"},
    }
    start = datetime(2026, 7, 12, tzinfo=timezone.utc)
    end = datetime(2026, 8, 11, tzinfo=timezone.utc)
    provider.list_events.side_effect = [[original], [revised]]

    with database.session() as session:
        assert ingest_calendar_metadata(session, provider, start=start, end=end) == 1
    with database.session() as session:
        assert ingest_calendar_metadata(session, provider, start=start, end=end) == 1

    with database.session() as session:
        suggestions = list(session.scalars(select(ExternalSuggestion)))
        refresh = session.scalar(
            select(AuditEvent).where(
                AuditEvent.action == "calendar.suggestion_refreshed"
            )
        )
        assert len(suggestions) == 1
        assert suggestions[0].payload["start"] == revised["start"]
        assert suggestions[0].approval_state == ApprovalState.PENDING
        assert refresh is not None
    database.dispose()


def test_google_oauth_rejects_scopes_outside_the_least_privilege_allowlist() -> None:
    manager = GoogleOAuthManager(secret_store=MagicMock())

    with pytest.raises(ValueError, match="unsupported Google OAuth scope"):
        manager.credentials(["https://www.googleapis.com/auth/gmail.modify"])
    with pytest.raises(ValueError, match="unsupported Google OAuth scope"):
        manager.credentials(["https://www.googleapis.com/auth/calendar.events"])


def test_google_oauth_rejects_a_stored_token_with_a_write_scope() -> None:
    secrets = MagicMock()
    secrets.get.return_value = json.dumps(
        {
            "token": "redacted",
            "scopes": [
                GMAIL_READONLY_SCOPE,
                "https://www.googleapis.com/auth/calendar.events",
            ],
        }
    )

    with pytest.raises(GoogleIntegrationUnavailable, match="non-read-only scope"):
        GoogleOAuthManager(secret_store=secrets).credentials([GMAIL_READONLY_SCOPE])


def test_calendar_read_uses_rfc3339_and_declared_scopes_are_least_privilege() -> None:
    service = MagicMock()
    service.events.return_value.list.return_value.execute.return_value = {
        "items": [{"id": "1"}]
    }
    provider = CalendarProvider(service=service)
    start = datetime(2026, 7, 11, 7, 0)
    end = datetime(2026, 7, 12, 7, 0, tzinfo=timezone.utc)

    assert provider.list_events(start=start, end=end) == [{"id": "1"}]
    service.events.return_value.list.assert_called_once_with(
        calendarId="primary",
        timeMin="2026-07-11T07:00:00Z",
        timeMax="2026-07-12T07:00:00Z",
        singleEvents=True,
        showDeleted=True,
        orderBy="startTime",
        maxResults=250,
    )
    assert GMAIL_READONLY_SCOPE.endswith("gmail.readonly")
    assert CALENDAR_READONLY_SCOPE.endswith("calendar.events.readonly")
    assert CALENDAR_EVENTS_READONLY_SCOPE.endswith("calendar.events.readonly")

    with pytest.raises(ValueError, match="must end after"):
        provider.list_events(start=end, end=end - timedelta(seconds=1))
