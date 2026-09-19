import httpx

from jobby.sources import GreenhouseSource
from jobby.sources.registry import SourceRegistry, build_default_registry


def test_default_registry_lists_all_required_adapters() -> None:
    registry = build_default_registry()
    assert registry.names == (
        "ashby",
        "eightfold",
        "freehire",
        "greenhouse",
        "icims",
        "lever",
        "oracle_hcm",
        "paylocity",
        "rippling",
        "smartrecruiters",
        "taleo",
        "usajobs",
        "workable",
        "workday",
    )


def test_registry_constructs_adapter_with_injected_client() -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    try:
        source = build_default_registry().create(
            "GREENHOUSE",
            client=client,
            config={"board_token": "example", "company": "Example"},
        )
    finally:
        client.close()

    assert isinstance(source, GreenhouseSource)
    assert source.client is client
    assert source.source_key == "greenhouse:example"


def test_registry_rejects_duplicate_and_unknown_names() -> None:
    registry = SourceRegistry()
    registry.register("test", GreenhouseSource)
    try:
        registry.register("TEST", GreenhouseSource)
    except ValueError as exc:
        assert "already registered" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")

    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    try:
        try:
            registry.create("missing", client=client)
        except KeyError as exc:
            assert "unknown source" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected KeyError")
    finally:
        client.close()
