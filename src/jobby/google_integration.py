"""Optional least-privilege Gmail and Calendar integrations."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any, Iterable, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from .audit import record_audit
from .config import SecretStore
from .enums import ApprovalState, SuggestionKind
from .models import EmailMessage, ExternalSuggestion, IntegrationState, Interview


GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
CALENDAR_EVENTS_READONLY_SCOPE = (
    "https://www.googleapis.com/auth/calendar.events.readonly"
)
# Backward-compatible names now resolve to the narrower read-only scope. They
# must never be used to infer write capability.
CALENDAR_READONLY_SCOPE = CALENDAR_EVENTS_READONLY_SCOPE
ALLOWED_GOOGLE_SCOPES = frozenset(
    {GMAIL_READONLY_SCOPE, CALENDAR_EVENTS_READONLY_SCOPE}
)
MAX_GMAIL_QUERY_CHARS = 1_000
MAX_EMAIL_BODY_CHARS = 2_000_000
MAX_EMAIL_MIME_PARTS = 10_000
MAX_GOOGLE_MESSAGE_ID_CHARS = 500


class GoogleIntegrationUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GoogleDisconnectResult:
    local_token_removed: bool
    remote_token_revoked: bool
    detail: str = ""
    authorization_retained: bool = False


class GoogleOAuthManager:
    def __init__(self, secret_store: SecretStore | None = None):
        self.secrets = secret_store or SecretStore()

    def credentials(self, scopes: list[str], *, interactive: bool = False) -> Any:
        scopes = list(dict.fromkeys(scopes))
        if not scopes:
            raise ValueError("at least one Google OAuth scope is required")
        unsupported = sorted(set(scopes) - ALLOWED_GOOGLE_SCOPES)
        if unsupported:
            raise ValueError(
                "unsupported Google OAuth scope(s): " + ", ".join(unsupported)
            )
        token_json = self.secrets.get("google_oauth_token")
        token_info: dict[str, Any] | None = None
        if token_json:
            try:
                decoded = json.loads(token_json)
                if not isinstance(decoded, dict):
                    raise TypeError("authorization payload must be an object")
                token_info = decoded
            except (TypeError, json.JSONDecodeError) as exc:
                raise GoogleIntegrationUnavailable(
                    "stored Google authorization is malformed; disconnect and reconnect"
                ) from exc
            stored_scopes = _serialized_oauth_scopes(token_info)
            if stored_scopes - ALLOWED_GOOGLE_SCOPES:
                raise GoogleIntegrationUnavailable(
                    "stored Google authorization includes a non-read-only scope; "
                    "disconnect and reconnect"
                )
        client_config: dict[str, Any] | None = None
        if interactive and token_info is None:
            client_config = self._client_config()
        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError as exc:
            raise GoogleIntegrationUnavailable(
                "install Jobby with the 'google' extra"
            ) from exc
        credentials = None
        if token_info is not None:
            try:
                credentials = Credentials.from_authorized_user_info(
                    token_info, scopes=scopes
                )
            except Exception as exc:
                raise GoogleIntegrationUnavailable(
                    "stored Google authorization is malformed; disconnect and reconnect"
                ) from exc
        if credentials and credentials.expired and credentials.refresh_token:
            try:
                credentials.refresh(Request())
                self.secrets.set("google_oauth_token", credentials.to_json())
            except Exception as exc:
                raise GoogleIntegrationUnavailable(
                    "Google authorization refresh failed; reconnect the integration"
                ) from exc
        if credentials and credentials.valid:
            missing = set(scopes) - set(credentials.scopes or [])
            if not missing:
                return credentials
        if not interactive:
            raise GoogleIntegrationUnavailable(
                "Google authorization is missing, expired, or lacks required scopes"
            )
        client_config = client_config or self._client_config()
        flow = InstalledAppFlow.from_client_config(client_config, scopes=scopes)
        credentials = flow.run_local_server(
            port=0,
            open_browser=True,
            timeout_seconds=180,
        )
        self.secrets.set("google_oauth_token", credentials.to_json())
        return credentials

    def _client_config(self) -> dict[str, Any]:
        client_json = self.secrets.get("google_oauth_client")
        if not client_json:
            raise GoogleIntegrationUnavailable(
                "Google OAuth client configuration is not in the OS keyring"
            )
        try:
            client_config = json.loads(client_json)
        except json.JSONDecodeError as exc:
            raise GoogleIntegrationUnavailable(
                "Google OAuth client configuration is malformed"
            ) from exc
        installed = (
            client_config.get("installed") if isinstance(client_config, dict) else None
        )
        if (
            not isinstance(installed, dict)
            or not str(installed.get("client_id") or "").strip()
        ):
            raise GoogleIntegrationUnavailable(
                "Google OAuth requires Desktop app client JSON with installed.client_id"
            )
        return client_config

    def disconnect(
        self,
        *,
        revoke: bool = True,
        client: Any | None = None,
    ) -> GoogleDisconnectResult:
        """Revoke authorization, retaining the local token until revocation succeeds."""

        token_json = self.secrets.get("google_oauth_token")
        if not token_json:
            return GoogleDisconnectResult(False, False, "not connected")
        if not revoke:
            removed = bool(self.secrets.delete("google_oauth_token"))
            return GoogleDisconnectResult(
                removed,
                False,
                "local token removed without remote revocation",
            )

        token: str | None = None
        try:
            payload = json.loads(token_json)
            if isinstance(payload, dict):
                # Revoking the refresh token invalidates the durable grant;
                # revoking only an access token can leave refresh access live.
                token = str(payload.get("refresh_token") or payload.get("token") or "")
        except (TypeError, json.JSONDecodeError):
            token = None
        if not token:
            return GoogleDisconnectResult(
                False,
                False,
                "stored authorization has no usable revocation token; local token retained",
                authorization_retained=True,
            )
        try:
            import httpx

            requester = client or httpx
            response = requester.post(
                "https://oauth2.googleapis.com/revoke",
                data={"token": token},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:
            return GoogleDisconnectResult(
                False,
                False,
                f"remote revocation failed ({exc.__class__.__name__}); local token retained",
                authorization_retained=True,
            )
        removed = bool(self.secrets.delete("google_oauth_token"))
        return GoogleDisconnectResult(removed, True)


class GmailProvider:
    def __init__(
        self, oauth: GoogleOAuthManager | None = None, *, service: Any | None = None
    ):
        if service is not None:
            self.service = service
            return
        oauth = oauth or GoogleOAuthManager()
        credentials = oauth.credentials([GMAIL_READONLY_SCOPE])
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise GoogleIntegrationUnavailable(
                "install Jobby with the 'google' extra"
            ) from exc
        self.service = build(
            "gmail", "v1", credentials=credentials, cache_discovery=False
        )

    def list_metadata(self, *, query: str, limit: int = 100) -> list[dict[str, Any]]:
        query = str(query or "").strip()
        if not query:
            raise ValueError("Gmail query must not be blank")
        if len(query) > MAX_GMAIL_QUERY_CHARS:
            raise ValueError(
                f"Gmail query exceeds the {MAX_GMAIL_QUERY_CHARS:,}-character limit"
            )
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("Gmail result limit must be an integer")
        bounded_limit = min(max(limit, 1), 500)
        response = (
            self.service.users()
            .messages()
            .list(userId="me", q=query, maxResults=bounded_limit)
            .execute()
        )
        if not isinstance(response, Mapping):
            raise ValueError("Gmail metadata response must be an object")
        raw_messages = response.get("messages", [])
        if raw_messages is None:
            raw_messages = []
        if not isinstance(raw_messages, (list, tuple)):
            raise ValueError("Gmail metadata messages must be a list")
        result: list[dict[str, Any]] = []
        for stub in raw_messages[:bounded_limit]:
            if not isinstance(stub, Mapping):
                continue
            stub_id = _google_identifier(stub.get("id"))
            if stub_id is None:
                continue
            message = (
                self.service.users()
                .messages()
                .get(
                    userId="me",
                    id=stub_id,
                    format="metadata",
                    metadataHeaders=["From", "Subject", "Date"],
                )
                .execute()
            )
            if not isinstance(message, Mapping):
                raise ValueError("Gmail message response must be an object")
            message_id = _google_identifier(message.get("id"))
            if message_id is None:
                raise ValueError("Gmail message response has an invalid ID")
            headers = _headers(message)
            result.append(
                {
                    "message_id": message_id,
                    "thread_id": message.get("threadId"),
                    "sender": headers.get("from"),
                    "subject": headers.get("subject"),
                    "date": headers.get("date"),
                    "snippet": message.get("snippet", ""),
                }
            )
        return result

    def get_body(self, message_id: str) -> str:
        message_id = str(message_id or "").strip()
        if not message_id:
            raise ValueError("Gmail message ID must not be blank")
        if len(message_id) > MAX_GOOGLE_MESSAGE_ID_CHARS:
            raise ValueError("Gmail message ID is too long")
        message = (
            self.service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute()
        )
        if not isinstance(message, Mapping):
            raise ValueError("Gmail message response must be an object")
        payload = message.get("payload", {})
        if not isinstance(payload, Mapping):
            raise ValueError("Gmail message payload must be an object")
        return _message_body(payload)


class CalendarProvider:
    def __init__(
        self,
        oauth: GoogleOAuthManager | None = None,
        *,
        service: Any | None = None,
        allow_write: bool = False,
    ):
        if allow_write:
            raise PermissionError("Jobby V1 Google integration is read-only")
        if service is not None:
            self.service = service
            return
        oauth = oauth or GoogleOAuthManager()
        credentials = oauth.credentials([CALENDAR_EVENTS_READONLY_SCOPE])
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise GoogleIntegrationUnavailable(
                "install Jobby with the 'google' extra"
            ) from exc
        self.service = build(
            "calendar", "v3", credentials=credentials, cache_discovery=False
        )

    def list_events(
        self, *, start: datetime, end: datetime, limit: int = 250
    ) -> list[dict[str, Any]]:
        if _aware_utc(end) <= _aware_utc(start):
            raise ValueError("calendar event range must end after it starts")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("calendar event limit must be an integer")
        if not 1 <= limit <= 500:
            raise ValueError("calendar event limit must be between 1 and 500")
        result = (
            self.service.events()
            .list(
                calendarId="primary",
                timeMin=_rfc3339(start),
                timeMax=_rfc3339(end),
                singleEvents=True,
                showDeleted=True,
                orderBy="startTime",
                maxResults=limit,
            )
            .execute()
        )
        if not isinstance(result, Mapping):
            raise ValueError("Calendar response must be an object")
        raw_items = result.get("items", [])
        if raw_items is None:
            raw_items = []
        if not isinstance(raw_items, (list, tuple)):
            raise ValueError("Calendar response items must be a list")
        return [dict(item) for item in raw_items[:limit] if isinstance(item, Mapping)]


def ingest_gmail_metadata(
    session: Session,
    provider: GmailProvider,
    *,
    query: str = "newer_than:90d (interview OR application OR recruiter OR hiring)",
    limit: int = 100,
) -> tuple[int, int]:
    imported = suggestions = 0
    for item in provider.list_metadata(query=query, limit=limit):
        if not isinstance(item, Mapping):
            raise ValueError("Gmail metadata item must be an object")
        provider_id = _google_identifier(item.get("message_id"))
        if provider_id is None:
            raise ValueError("Gmail metadata item has an invalid message ID")
        message = session.scalar(
            select(EmailMessage).where(EmailMessage.provider_message_id == provider_id)
        )
        if message is None:
            message = EmailMessage(
                provider_message_id=provider_id,
                sender=item.get("sender"),
                subject=item.get("subject"),
                received_at=_parse_email_date(item.get("date")),
                snippet=item.get("snippet"),
                full_body_retrieved=False,
            )
            session.add(message)
            session.flush()
            imported += 1
        suggestion = classify_gmail_metadata(
            sender=message.sender,
            subject=message.subject,
            snippet=message.snippet,
        )
        if suggestion and not session.scalar(
            select(ExternalSuggestion.id).where(
                ExternalSuggestion.email_message_id == message.id,
                ExternalSuggestion.kind == suggestion[0],
            )
        ):
            kind, confidence, evidence = suggestion
            session.add(
                ExternalSuggestion(
                    kind=kind,
                    email_message_id=message.id,
                    payload={"evidence": evidence, "preview_only": True},
                    confidence=confidence,
                    approval_state=ApprovalState.PENDING,
                )
            )
            suggestions += 1
    state = session.scalar(
        select(IntegrationState).where(IntegrationState.provider == "gmail")
    )
    if state is None:
        state = IntegrationState(
            provider="gmail", enabled=True, scopes=[GMAIL_READONLY_SCOPE]
        )
        session.add(state)
    else:
        state.enabled = True
        state.scopes = [GMAIL_READONLY_SCOPE]
    state.last_sync_at = datetime.now(timezone.utc)
    state.health = "ok"
    session.flush()
    record_audit(
        session,
        action="gmail.metadata_ingested",
        entity_type="integration",
        entity_id=state.id,
        actor="user",
        after={"messages": imported, "suggestions": suggestions},
    )
    return imported, suggestions


def ingest_calendar_metadata(
    session: Session,
    provider: CalendarProvider,
    *,
    start: datetime,
    end: datetime,
    limit: int = 250,
) -> int:
    """Persist interview-like Calendar events as unapproved local previews."""

    imported = 0
    for item in calendar_interview_suggestions(
        provider.list_events(start=start, end=end, limit=limit)
    ):
        external_id = str(item.get("external_event_id") or "").strip()
        if not external_id:
            continue
        existing = list(
            session.scalars(
                select(ExternalSuggestion)
                .where(
                    ExternalSuggestion.external_event_id == external_id,
                    ExternalSuggestion.kind == SuggestionKind.CALENDAR_INTERVIEW,
                )
                .order_by(
                    ExternalSuggestion.created_at.desc(), ExternalSuggestion.id.desc()
                )
            )
        )
        payload = dict(item)
        payload.pop("kind", None)
        payload.pop("external_event_id", None)
        payload.pop("approval_state", None)
        new_payload = {**payload, "preview_only": True}
        latest = existing[0] if existing else None
        if latest is not None and _calendar_payload(
            latest.payload
        ) == _calendar_payload(new_payload):
            continue
        if latest is not None and latest.approval_state == ApprovalState.PENDING:
            before = latest.payload
            latest.payload = new_payload
            record_audit(
                session,
                action="calendar.suggestion_refreshed",
                entity_type="external_suggestion",
                entity_id=latest.id,
                actor="user",
                before={"payload": before},
                after={"payload": new_payload},
            )
            imported += 1
            continue
        if new_payload.get("cancelled") and latest is None:
            # A deleted non-recruiting calendar event is not actionable.
            continue
        application_id = latest.application_id if latest is not None else None
        if application_id is None:
            interview = session.scalar(
                select(Interview).where(Interview.calendar_event_id == external_id)
            )
            application_id = interview.application_id if interview is not None else None
        session.add(
            ExternalSuggestion(
                kind=SuggestionKind.CALENDAR_INTERVIEW,
                application_id=application_id,
                external_event_id=external_id,
                payload={
                    **new_payload,
                    **(
                        {"supersedes_suggestion_id": latest.id}
                        if latest is not None
                        else {}
                    ),
                },
                confidence=0.85,
                approval_state=ApprovalState.PENDING,
            )
        )
        imported += 1
    state = session.scalar(
        select(IntegrationState).where(IntegrationState.provider == "calendar")
    )
    if state is None:
        state = IntegrationState(
            provider="calendar",
            enabled=True,
            scopes=[CALENDAR_EVENTS_READONLY_SCOPE],
        )
        session.add(state)
    else:
        # Never retain a stale write-capable scope from an earlier Jobby
        # version in the local integration record.
        state.enabled = True
        state.scopes = [CALENDAR_EVENTS_READONLY_SCOPE]
    state.last_sync_at = datetime.now(timezone.utc)
    state.health = "ok"
    session.flush()
    record_audit(
        session,
        action="calendar.metadata_ingested",
        entity_type="integration",
        entity_id=state.id,
        actor="user",
        after={"suggestions": imported, "start": start, "end": end},
    )
    return imported


def retrieve_full_body(
    session: Session, provider: GmailProvider, message_id: str
) -> str:
    message = session.scalar(
        select(EmailMessage).where(EmailMessage.provider_message_id == message_id)
    )
    if message is None:
        raise LookupError("email metadata not found")
    body = provider.get_body(message_id)
    message.full_body_retrieved = True
    record_audit(
        session,
        action="gmail.body_retrieved",
        entity_type="email_message",
        entity_id=message.id,
        actor="user",
        after={"length": len(body)},
    )
    return body


_ATS_SENDER_DOMAINS = frozenset(
    {
        "ashbyhq.com",
        "greenhouse.io",
        "icims.com",
        "lever.co",
        "myworkday.com",
        "smartrecruiters.com",
        "taleo.net",
        "usajobs.gov",
        "workable.com",
        "workday.com",
    }
)
_GMAIL_EXCLUSIONS = (
    r"\b(?:newsletter|weekly digest|daily digest|unsubscribe)\b",
    r"\b(?:verify (?:your )?email|activate (?:your )?account|account (?:created|creation)|reset (?:your )?password)\b",
    r"\b(?:webinar|marketing update|product update|special offer|promotion(?:al)?)\b",
    r"\b(?:mock interview|interview prep(?:aration)?|practice interview)\b",
    r"\b(?:job alert|new jobs? (?:for|matching)|recommended jobs?|jobs? you may like)\b",
)


def _sender_domain(sender: str | None) -> str | None:
    _display, address = parseaddr(str(sender or ""))
    if not address or address.count("@") != 1:
        return None
    local, domain = address.rsplit("@", 1)
    domain = domain.strip().rstrip(".").casefold()
    if not local.strip() or not domain or ".." in domain:
        return None
    try:
        return domain.encode("idna").decode("ascii")
    except UnicodeError:
        return None


def _is_ats_domain(domain: str | None) -> bool:
    return bool(
        domain
        and any(
            domain == item or domain.endswith(f".{item}")
            for item in _ATS_SENDER_DOMAINS
        )
    )


def classify_gmail_metadata(
    *,
    sender: str | None,
    subject: str | None,
    snippet: str | None,
) -> tuple[SuggestionKind, float, str] | None:
    """Classify bounded Gmail metadata with exclusions taking precedence."""

    subject_text = re.sub(r"\s+", " ", str(subject or "").casefold()).strip()
    snippet_text = re.sub(r"\s+", " ", str(snippet or "").casefold()).strip()
    folded = f"{subject_text} {snippet_text}".strip()
    if any(re.search(pattern, folded) for pattern in _GMAIL_EXCLUSIONS):
        return None
    domain = _sender_domain(sender)
    ats_sender = _is_ats_domain(domain)
    if (
        domain
        and not ats_sender
        and any(
            token in domain
            for token in (
                "greenhouse",
                "icims",
                "lever",
                "smartrecruiters",
                "taleo",
                "workday",
                "workable",
                "ashby",
                "usajobs",
            )
        )
    ):
        return None

    subject_patterns = (
        (
            SuggestionKind.REJECTED,
            r"\b(?:not moving forward|other candidates|regret to inform|not selected|application update)\b",
            0.95,
        ),
        (
            SuggestionKind.INTERVIEW_REQUESTED,
            r"\b(?:interview (?:invitation|request|availability)|(?:schedule|invitation|invite).{0,30}\binterview)\b",
            0.9,
        ),
        (
            SuggestionKind.POSITION_CLOSED,
            r"\b(?:position|role|requisition).{0,30}\b(?:closed|cancelled|canceled|filled)\b",
            0.9,
        ),
        (
            SuggestionKind.APPLIED,
            r"\b(?:application (?:was |has been )?received|thank you for applying|application confirmation|we received your application)\b",
            0.85,
        ),
    )
    for kind, pattern, confidence in subject_patterns:
        match = re.search(pattern, subject_text)
        if match:
            adjusted = min(0.99, confidence + (0.03 if ats_sender else 0.0))
            return kind, adjusted, f"subject: {match.group(0)}"

    generic_patterns = (
        (
            SuggestionKind.REJECTED,
            r"\b(?:not moving forward|other candidates|regret to inform|not selected)\b",
            0.92,
        ),
        (
            SuggestionKind.INTERVIEW_REQUESTED,
            r"\b(?:schedule|invitation|invite).{0,30}\binterview\b|\binterview availability\b",
            0.87,
        ),
        (
            SuggestionKind.POSITION_CLOSED,
            r"\b(?:position|role).{0,20}\b(?:closed|cancelled|canceled|filled)\b",
            0.87,
        ),
        (
            SuggestionKind.APPLIED,
            r"\b(?:application (?:was )?received|thank you for applying|application confirmation)\b",
            0.82,
        ),
    )
    for kind, pattern, confidence in generic_patterns:
        match = re.search(pattern, folded)
        if match:
            adjusted = min(0.99, confidence + (0.03 if ats_sender else 0.0))
            return kind, adjusted, match.group(0)
    return None


def suggest_email_event(text: str) -> tuple[SuggestionKind, float, str] | None:
    """Compatibility wrapper for callers that only have combined text."""

    return classify_gmail_metadata(sender=None, subject=text, snippet=None)


def calendar_interview_suggestions(
    events: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for event in events:
        if str(event.get("status") or "").casefold() == "cancelled":
            suggestions.append(
                {
                    "kind": SuggestionKind.CALENDAR_INTERVIEW.value,
                    "external_event_id": event.get("id"),
                    "summary": str(event.get("summary") or "Cancelled interview"),
                    "start": event.get("start"),
                    "end": event.get("end"),
                    "location": event.get("location") or event.get("hangoutLink"),
                    "cancelled": True,
                    "approval_state": ApprovalState.PENDING.value,
                }
            )
            continue
        summary = str(event.get("summary") or "")
        description = str(event.get("description") or "")
        if not re.search(
            r"\b(interview|recruiter|hiring manager|screen)\b",
            f"{summary} {description}",
            re.I,
        ):
            continue
        suggestions.append(
            {
                "kind": SuggestionKind.CALENDAR_INTERVIEW.value,
                "external_event_id": event.get("id"),
                "summary": summary,
                "start": event.get("start"),
                "end": event.get("end"),
                "location": event.get("location") or event.get("hangoutLink"),
                "approval_state": ApprovalState.PENDING.value,
            }
        )
    return suggestions


def _calendar_payload(value: object) -> dict[str, Any]:
    """Compare event revisions while ignoring local bookkeeping fields."""

    if not isinstance(value, Mapping):
        return {}
    ignored = {"preview_only", "supersedes_suggestion_id"}
    return {str(key): nested for key, nested in value.items() if key not in ignored}


def _headers(message: Mapping[str, Any]) -> dict[str, str]:
    payload = message.get("payload", {})
    if not isinstance(payload, Mapping):
        raise ValueError("Gmail message payload must be an object")
    raw_headers = payload.get("headers", [])
    if raw_headers is None:
        raw_headers = []
    if not isinstance(raw_headers, (list, tuple)):
        raise ValueError("Gmail message headers must be a list")
    result: dict[str, str] = {}
    for item in raw_headers:
        if not isinstance(item, Mapping):
            raise ValueError("Gmail message header must be an object")
        name = item.get("name", "")
        value = item.get("value", "")
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("Gmail message header name and value must be text")
        result[name.casefold()] = value
    return result


def _google_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    identifier = value.strip()
    if not identifier or len(identifier) > MAX_GOOGLE_MESSAGE_ID_CHARS:
        return None
    return identifier


def _message_body(payload: Mapping[str, Any]) -> str:
    plain: list[str] = []
    html_parts: list[str] = []
    _collect_message_parts(
        payload,
        plain=plain,
        html_parts=html_parts,
        part_count=[0],
    )
    selected = plain or html_parts
    chunks: list[str] = []
    remaining = MAX_EMAIL_BODY_CHARS
    for encoded in selected:
        if remaining <= 0:
            break
        decoded = _decode(encoded)
        if not plain:
            decoded = re.sub(r"<[^>]+>", " ", decoded)
        if chunks:
            chunks.append("\n")
            remaining -= 1
        if remaining <= 0:
            break
        chunk = decoded[:remaining]
        chunks.append(chunk)
        remaining -= len(chunk)
    return "".join(chunks)


def _collect_message_parts(
    payload: Mapping[str, Any],
    *,
    plain: list[str],
    html_parts: list[str],
    part_count: list[int],
    _depth: int = 0,
) -> None:
    if _depth > 20:
        raise ValueError("Gmail MIME nesting exceeds the supported depth")
    if not isinstance(payload, Mapping):
        raise ValueError("Gmail MIME part must be an object")
    part_count[0] += 1
    if part_count[0] > MAX_EMAIL_MIME_PARTS:
        raise ValueError("Gmail message contains too many MIME parts")
    raw_body = payload.get("body", {})
    if raw_body is None:
        raw_body = {}
    if not isinstance(raw_body, Mapping):
        raise ValueError("Gmail MIME body must be an object")
    body = raw_body.get("data")
    mime = str(payload.get("mimeType") or "text/plain").casefold()
    if body is not None:
        if not isinstance(body, str):
            raise ValueError("Gmail MIME body data must be text")
        if body and mime == "text/plain":
            plain.append(body)
        elif body and mime == "text/html":
            html_parts.append(body)
    parts = payload.get("parts", [])
    if parts is None:
        parts = []
    if not isinstance(parts, (list, tuple)):
        raise ValueError("Gmail MIME parts must be a list")
    for part in parts:
        if not isinstance(part, Mapping):
            raise ValueError("Gmail MIME part must be an object")
        _collect_message_parts(
            part,
            plain=plain,
            html_parts=html_parts,
            part_count=part_count,
            _depth=_depth + 1,
        )


def _decode(value: str) -> str:
    if len(value) > MAX_EMAIL_BODY_CHARS * 2:
        raise ValueError("Gmail body exceeds the supported size")
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(
            value + padding, altchars=b"-_", validate=True
        ).decode("utf-8", errors="replace")
    except (ValueError, TypeError) as exc:
        raise ValueError("Gmail body contains invalid base64 data") from exc
    return decoded[:MAX_EMAIL_BODY_CHARS]


def _parse_email_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _rfc3339(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _serialized_oauth_scopes(payload: Mapping[str, Any]) -> set[str]:
    raw = payload.get("scopes", payload.get("scope"))
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {item for item in raw.split() if item}
    if isinstance(raw, (list, tuple, set)):
        return {str(item).strip() for item in raw if str(item).strip()}
    raise GoogleIntegrationUnavailable(
        "stored Google authorization has malformed scopes; disconnect and reconnect"
    )


__all__ = [
    "CALENDAR_EVENTS_READONLY_SCOPE",
    "CALENDAR_READONLY_SCOPE",
    "GMAIL_READONLY_SCOPE",
    "CalendarProvider",
    "GmailProvider",
    "GoogleIntegrationUnavailable",
    "GoogleOAuthManager",
    "GoogleDisconnectResult",
    "calendar_interview_suggestions",
    "classify_gmail_metadata",
    "ingest_calendar_metadata",
    "ingest_gmail_metadata",
    "retrieve_full_body",
    "suggest_email_event",
]
