from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from sqlalchemy import select

from jobby.config import AppConfig, JobbyPaths, ModelSettings
from jobby.db import Database
from jobby.enums import ApprovalState
from jobby.models import AICacheEntry, AIRun, Citation
from jobby.openai_provider import (
    MAX_AI_INPUT_CHARS,
    MAX_WEB_QUERY_CHARS,
    ModelUnavailable,
    OpenAIProvider,
    ProviderRequestError,
    ProviderUnavailable,
    _extract_citations,
)


class ParsedAnswer(BaseModel):
    answer: str


class AlternateAnswer(BaseModel):
    answer: str
    confidence: float = 1.0


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


def config() -> AppConfig:
    return AppConfig(
        openai_enabled=True,
        models=ModelSettings(
            fast="fast-model",
            quality="quality-model",
            premium="premium-model",
        ),
    )


def test_provider_without_client_requires_a_key() -> None:
    secrets = MagicMock()
    secrets.get.return_value = None

    with pytest.raises(ProviderUnavailable, match="OS keyring"):
        OpenAIProvider(config(), secret_store=secrets)


def test_disabled_provider_rejects_even_an_injected_client_without_calls() -> None:
    """An API key or injected test client must never bypass the explicit opt-in."""

    client = MagicMock()
    secrets = MagicMock()
    secrets.get.return_value = "fixture-key-present-but-disabled"

    with pytest.raises(ProviderUnavailable, match="OpenAI is disabled"):
        OpenAIProvider(AppConfig(), secret_store=secrets, client=client)

    secrets.get.assert_not_called()
    assert client.mock_calls == []


def test_structured_uses_exact_tier_without_fallback_and_persists_usage(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="supported"),
        usage={
            "input_tokens": 21,
            "output_tokens": 8,
            "input_tokens_details": {"cached_tokens": 5},
        },
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        result = provider.structured(
            purpose="classification",
            text="source text",
            output_type=ParsedAnswer,
            prompt_version="classification-v2",
            system="Use evidence only.",
            tier="quality",
            session=session,
        )
        run_id = result.ai_run.id

    assert result.value == ParsedAnswer(answer="supported")
    kwargs = client.responses.parse.call_args.kwargs
    assert kwargs["model"] == "quality-model"
    assert kwargs["text_format"] is ParsedAnswer
    assert kwargs["input"] == [
        {"role": "system", "content": "Use evidence only."},
        {"role": "user", "content": "source text"},
    ]
    with database.session() as session:
        run = session.get(AIRun, run_id)
        assert run is not None
        assert run.model == "quality-model"
        assert run.prompt_version == "classification-v2"
        assert len(run.input_hash) == 64
        assert run.output_json == {"answer": "supported"}
        assert (run.input_tokens, run.output_tokens, run.cached_tokens) == (21, 8, 5)

    with pytest.raises(ValueError, match="unknown model tier"):
        provider.structured(
            purpose="classification",
            text="source text",
            output_type=ParsedAnswer,
            prompt_version="classification-v2",
            system="Use evidence only.",
            tier="economy",
        )
    assert client.responses.parse.call_count == 1
    database.dispose()


def test_parse_failure_is_recorded_without_retrying_another_model(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.side_effect = RuntimeError("configured model unavailable")
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        with pytest.raises(RuntimeError, match="configured model unavailable"):
            provider.structured(
                purpose="extraction",
                text="source text",
                output_type=ParsedAnswer,
                prompt_version="extract-v1",
                system="Extract.",
                tier="premium",
                session=session,
            )

    client.responses.parse.assert_called_once()
    assert client.responses.parse.call_args.kwargs["model"] == "premium-model"
    with database.session() as session:
        runs = list(session.scalars(select(AIRun)))
        assert len(runs) == 1
        assert runs[0].model == "premium-model"
        assert runs[0].error == "configured model unavailable"
        assert runs[0].approval_state == ApprovalState.REJECTED
    database.dispose()


def test_model_validation_reports_each_configured_id_and_require_models_fails() -> None:
    client = MagicMock()

    def retrieve(model_id: str) -> SimpleNamespace:
        if model_id == "quality-model":
            raise RuntimeError("not entitled")
        return SimpleNamespace(id=model_id)

    client.models.retrieve.side_effect = retrieve
    provider = OpenAIProvider(config(), client=client)

    assert provider.validate_models() == {
        "fast": None,
        "quality": "not entitled",
        "premium": None,
    }
    with pytest.raises(ModelUnavailable, match="quality: not entitled"):
        provider.require_models()
    assert [call.args[0] for call in client.models.retrieve.call_args_list] == [
        "fast-model",
        "quality-model",
        "premium-model",
        "fast-model",
        "quality-model",
        "premium-model",
    ]


def test_web_search_persists_citations_and_usage(tmp_path: Path) -> None:
    client = MagicMock()
    client.responses.create.return_value = SimpleNamespace(
        output_text="Two current roles were found.",
        usage=SimpleNamespace(
            input_tokens=13,
            output_tokens=9,
            input_tokens_details=SimpleNamespace(cached_tokens=3),
        ),
        output=[
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": "Two current roles were found.",
                        "annotations": [
                            {
                                "type": "url_citation",
                                "url": "https://jobs.example/one",
                                "title": "Role one",
                                "start_index": 0,
                                "end_index": 10,
                            },
                            {
                                "type": "url_citation",
                                "url_citation": {
                                    "url": "https://jobs.example/two",
                                    "title": "Role two",
                                    "quoted_text": "Policy counsel",
                                    "start_index": 11,
                                    "end_index": 20,
                                },
                            },
                            {
                                "type": "url_citation",
                                "url": "https://jobs.example/one",
                                "title": "duplicate",
                                "start_index": 0,
                                "end_index": 10,
                            },
                        ],
                    }
                ],
            }
        ],
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        response = provider.search("current policy counsel roles", session=session)
        run_id = response.ai_run_id

    assert response.model == "fast-model"
    assert response.text == "Two current roles were found."
    assert [item.url for item in response.citations] == [
        "https://jobs.example/one",
        "https://jobs.example/two",
    ]
    assert response.citations[1].quoted_text == "Policy counsel"
    assert run_id is not None
    client.responses.create.assert_called_once_with(
        model="fast-model",
        tools=[{"type": "web_search"}],
        input="current policy counsel roles",
    )
    with database.session() as session:
        run = session.get(AIRun, run_id)
        citations = list(
            session.scalars(
                select(Citation)
                .where(Citation.ai_run_id == run_id)
                .order_by(Citation.url)
            )
        )
        assert run is not None
        assert (run.input_tokens, run.output_tokens, run.cached_tokens) == (13, 9, 3)
        assert run.output_json["text"] == response.text
        assert len(run.output_json["citations"]) == 2
        assert [item.url for item in citations] == [
            "https://jobs.example/one",
            "https://jobs.example/two",
        ]
    database.dispose()


def test_blank_search_never_calls_provider() -> None:
    client = MagicMock()
    provider = OpenAIProvider(config(), client=client)

    with pytest.raises(ValueError, match="must not be blank"):
        provider.search("  \n")

    client.responses.create.assert_not_called()


def test_empty_web_response_is_a_recorded_failure_not_an_empty_success(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.create.return_value = SimpleNamespace(
        output_text="  ",
        output=[],
        usage={"input_tokens": 4, "output_tokens": 0},
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        with pytest.raises(ProviderRequestError, match="returned no text"):
            provider.search("current roles", session=session)

    client.responses.create.assert_called_once()
    with database.session() as session:
        runs = list(session.scalars(select(AIRun)))
        assert len(runs) == 1
        assert runs[0].error == "OpenAI web search returned no text"
        assert runs[0].approval_state == ApprovalState.REJECTED
        assert (runs[0].input_tokens, runs[0].output_tokens) == (4, 0)
    database.dispose()


def test_local_input_limits_fail_before_any_billable_call() -> None:
    client = MagicMock()
    provider = OpenAIProvider(config(), client=client)

    with pytest.raises(ValueError, match="AI input must not be blank"):
        provider.structured(
            purpose="extraction",
            text="  ",
            output_type=ParsedAnswer,
            prompt_version="v1",
            system="Extract.",
        )
    with pytest.raises(ValueError, match="200,000-character limit"):
        provider.structured(
            purpose="extraction",
            text="x" * (MAX_AI_INPUT_CHARS + 1),
            output_type=ParsedAnswer,
            prompt_version="v1",
            system="Extract.",
        )
    with pytest.raises(ValueError, match="8,000-character limit"):
        provider.search("x" * (MAX_WEB_QUERY_CHARS + 1))

    client.responses.parse.assert_not_called()
    client.responses.create.assert_not_called()


def test_provider_errors_are_bounded_redacted_and_usage_is_sanitized(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.side_effect = RuntimeError(
        # Assembled at runtime so the source never contains a key-shaped literal.
        "Authorization: Bearer "
        + "sk-"
        + "secretvalue123"
        + " api_key="
        + "sk-"
        + "anothersecret456 "
        + "x" * 5_000
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        with pytest.raises(ProviderRequestError) as caught:
            provider.structured(
                purpose="extraction",
                text="source",
                output_type=ParsedAnswer,
                prompt_version="v1",
                system="Extract.",
                session=session,
            )
    assert "secretvalue" not in str(caught.value)
    assert "anothersecret" not in str(caught.value)

    with database.session() as session:
        run = session.scalar(select(AIRun))
        assert run is not None
        assert "secretvalue" not in (run.error or "")
        assert "anothersecret" not in (run.error or "")
        assert "[REDACTED" in (run.error or "")
        assert len(run.error or "") <= 4_000
    database.dispose()


def test_citation_extraction_rejects_unsafe_urls_and_handles_cycles() -> None:
    response: dict[str, object] = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "annotations": [
                            {
                                "type": "url_citation",
                                "url": "javascript:alert(1)",
                            },
                            {
                                "type": "url_citation",
                                "url": "https://www.example.test/job?utm_source=ai",
                                "quoted_text": "q" * 12_000,
                                "start_index": 0,
                                "end_index": 3,
                            },
                            {
                                "type": "url_citation",
                                "url": "https://example.test/bad-index",
                                "start_index": -1,
                            },
                        ]
                    }
                ],
            }
        ]
    }
    response["cycle"] = response

    citations = _extract_citations(response)

    assert len(citations) == 1
    assert citations[0].url == "https://example.test/job"
    assert len(citations[0].quoted_text or "") == 10_000


def test_guarded_structured_cache_records_a_zero_token_audit_hit(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="stable"),
        usage={"input_tokens": 20, "output_tokens": 4},
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    def cached_call():
        with database.session() as session:
            return provider.structured(
                purpose="classification",
                text="same source",
                output_type=ParsedAnswer,
                prompt_version="classify-v1",
                system="Classify.",
                tier="quality",
                session=session,
                use_cache=True,
            )

    first = cached_call()
    second = cached_call()

    client.responses.parse.assert_called_once()
    assert second.value == first.value
    with database.session() as session:
        entries = list(session.scalars(select(AICacheEntry)))
        runs = list(session.scalars(select(AIRun).order_by(AIRun.created_at)))
        assert len(entries) == 1
        assert len(runs) == 2
        hit = session.get(AIRun, second.ai_run.id)
        assert hit is not None and hit.cache_hit is True
        assert (hit.input_tokens, hit.output_tokens, hit.cached_tokens) == (0, 0, None)
        assert hit.cache_entry_id == entries[0].id
        assert hit.source_ai_run_id == first.ai_run.id
        assert hit.output_json == {"answer": "stable"}
    database.dispose()


def test_cache_provenance_mismatch_forces_one_refresh_and_repairs_entry(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="stable"),
        usage={"input_tokens": 20, "output_tokens": 4},
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    def cached_call():
        with database.session() as session:
            return provider.structured(
                purpose="classification",
                text="same source",
                output_type=ParsedAnswer,
                prompt_version="classify-v1",
                system="Classify.",
                tier="quality",
                session=session,
                use_cache=True,
            )

    cached_call()
    with database.session() as session:
        entry = session.scalar(select(AICacheEntry))
        assert entry is not None
        entry.model = "corrupted-model"

    refreshed = cached_call()
    cached = cached_call()

    assert refreshed.value.answer == "stable"
    assert cached.value.answer == "stable"
    assert client.responses.parse.call_count == 2
    with database.session() as session:
        entry = session.scalar(select(AICacheEntry))
        assert entry is not None
        assert entry.active is True
        assert entry.model == "quality-model"
        source = session.get(AIRun, entry.source_ai_run_id)
        assert source is not None
        assert source.model == entry.model
        assert source.output_json == entry.output_json
    database.dispose()


def test_cache_identity_ttl_bypass_and_rejection_are_guarded(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="stable"), usage=None
    )
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    def call(*, prompt="v1", limit=None, bypass=False, output=ParsedAnswer):
        with database.session() as session:
            return provider.structured(
                purpose="extraction",
                text="source",
                output_type=output,
                prompt_version=prompt,
                system="Extract.",
                tier="quality",
                session=session,
                max_output_tokens=limit,
                use_cache=True,
                bypass_cache=bypass,
            )

    first = call()
    assert call().ai_run.cache_hit is True
    assert call(bypass=True).ai_run.cache_hit is False
    call(prompt="v2")
    call(limit=300)
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=AlternateAnswer(answer="stable"), usage=None
    )
    call(output=AlternateAnswer)
    assert client.responses.parse.call_count == 5

    with database.session() as session:
        entry = session.scalar(
            select(AICacheEntry).where(AICacheEntry.source_ai_run_id == first.ai_run.id)
        )
        assert entry is not None
        entry_id = entry.id
        cache_key = entry.cache_key
        entry.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="refreshed"), usage=None
    )
    refreshed = call()
    assert refreshed.ai_run.cache_hit is False
    assert refreshed.value.answer == "refreshed"
    with database.session() as session:
        entry = session.scalar(
            select(AICacheEntry).where(AICacheEntry.cache_key == cache_key)
        )
        assert entry is not None and entry.active is True
        assert entry.id == entry_id
        assert entry.source_ai_run_id == refreshed.ai_run.id
        refreshed_run = session.get(AIRun, refreshed.ai_run.id)
        assert refreshed_run is not None
        refreshed_run.approval_state = ApprovalState.REJECTED
    with database.session() as session:
        invalidated = session.get(AICacheEntry, entry_id)
        assert invalidated is not None and invalidated.active is False
        assert invalidated.invalidated_at is not None
    call()
    assert client.responses.parse.call_count == 7
    database.dispose()


def test_failed_and_non_cacheable_purposes_never_create_entries(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.responses.parse.side_effect = RuntimeError("failed extraction")
    provider = OpenAIProvider(config(), client=client)
    database = make_database(tmp_path)

    with database.session() as session:
        with pytest.raises(ProviderRequestError):
            provider.structured(
                purpose="extraction",
                text="source",
                output_type=ParsedAnswer,
                prompt_version="v1",
                system="Extract.",
                session=session,
                use_cache=True,
            )
    client.responses.parse.side_effect = None
    client.responses.parse.return_value = SimpleNamespace(
        output_parsed=ParsedAnswer(answer="draft"), usage=None
    )
    for _ in range(2):
        with database.session() as session:
            result = provider.structured(
                purpose="document_draft",
                text="source",
                output_type=ParsedAnswer,
                prompt_version="v1",
                system="Draft.",
                session=session,
                use_cache=True,
            )
            assert result.ai_run.cache_hit is False

    assert client.responses.parse.call_count == 3
    with database.session() as session:
        assert session.scalar(select(AICacheEntry)) is None
        runs = list(session.scalars(select(AIRun)))
        assert len(runs) == 3
        assert sum(run.approval_state == ApprovalState.REJECTED for run in runs) == 1
    database.dispose()
