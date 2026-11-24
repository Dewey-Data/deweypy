from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import re
from collections.abc import Iterator
from importlib.metadata import version

import httpx
import pytest
import respx

import deweypy
from deweypy import user_agent
from deweypy.context import main_context
from deweypy.download.asynchronous import async_api_request, make_async_client
from deweypy.download.synchronous import api_request, make_client

# The pattern older Dewey backends use: the whole `User-Agent` must be exactly
# `deweypy/<version>`, with this character set and length for the version.
OLD_DEWEY_SERVER_PATTERN = re.compile(r"^deweypy\/([0-9a-zA-Z\.\-\_]{1,15})$")

TEST_API_KEY = "test-api-key-0123456789abcdef"
PING_URL = "https://api.deweydata.io/api/v1/ping"


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(main_context, "_api_key", TEST_API_KEY)
    return TEST_API_KEY


@pytest.fixture
def patched_metadata(monkeypatch: pytest.MonkeyPatch) -> Iterator[pytest.MonkeyPatch]:
    """Patch freely, then undo the patches and reload `deweypy.user_agent`."""
    yield monkeypatch
    monkeypatch.undo()
    importlib.reload(user_agent)


def test_version_comes_from_package_metadata():
    assert user_agent.get_deweypy_version() == version("deweypy")
    assert deweypy.__version__ == version("deweypy")
    assert deweypy.__version__ != user_agent.UNKNOWN_VERSION


def test_user_agent_is_bare_deweypy_version():
    assert user_agent.USER_AGENT == f"deweypy/{deweypy.__version__}"
    match = OLD_DEWEY_SERVER_PATTERN.fullmatch(user_agent.USER_AGENT)
    assert match is not None
    assert match.group(1) == deweypy.__version__


@pytest.mark.parametrize(
    "error",
    [
        importlib.metadata.PackageNotFoundError("deweypy"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        OSError("unreadable METADATA"),
    ],
)
def test_version_falls_back_when_metadata_lookup_fails(
    monkeypatch: pytest.MonkeyPatch, error: Exception
):
    def raise_error(distribution_name: str) -> str:
        raise error

    monkeypatch.setattr(user_agent, "version", raise_error)

    assert user_agent.get_deweypy_version() == user_agent.UNKNOWN_VERSION


@pytest.mark.parametrize("bad_version", [None, ""])
def test_version_falls_back_when_metadata_has_no_version(
    monkeypatch: pytest.MonkeyPatch, bad_version: str | None
):
    # `importlib.metadata.version()` returns `None` when `METADATA` has no
    # `Version` field.
    monkeypatch.setattr(user_agent, "version", lambda distribution_name: bad_version)

    assert user_agent.get_deweypy_version() == user_agent.UNKNOWN_VERSION


def test_import_survives_unreadable_metadata(patched_metadata: pytest.MonkeyPatch):
    def raise_decode_error(distribution_name: str) -> str:
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    patched_metadata.setattr(importlib.metadata, "version", raise_decode_error)

    reloaded = importlib.reload(user_agent)

    assert reloaded.__version__ == user_agent.UNKNOWN_VERSION
    assert reloaded.USER_AGENT == "deweypy/unknown"


@pytest.mark.parametrize(
    ("deweypy_version", "expected_user_agent"),
    [
        ("0.8.0", "deweypy/0.8.0"),
        ("1.2.3rc1", "deweypy/1.2.3rc1"),
        ("0.9.0.dev3", "deweypy/0.9.0.dev3"),
        # The local version label is dropped.
        ("0.8.0+corp.1", "deweypy/0.8.0"),
        # Versions that can't fit the server's pattern are sent as `unknown`.
        ("1!0.8.0", "deweypy/unknown"),
        ("0.8.0.dev1234567890", "deweypy/unknown"),
        ("unknown", "deweypy/unknown"),
    ],
)
def test_build_user_agent_fits_the_server_pattern(
    deweypy_version: str, expected_user_agent: str
):
    built = user_agent.build_user_agent(deweypy_version)

    assert built == expected_user_agent
    assert OLD_DEWEY_SERVER_PATTERN.fullmatch(built)


@pytest.mark.usefixtures("api_key")
def test_make_client_sends_user_agent():
    with make_client() as client:
        assert client.headers["User-Agent"] == user_agent.USER_AGENT


@pytest.mark.usefixtures("api_key")
def test_make_async_client_sends_user_agent():
    async def check() -> None:
        async with make_async_client() as client:
            assert client.headers["User-Agent"] == user_agent.USER_AGENT

    asyncio.run(check())


@pytest.mark.usefixtures("api_key")
def test_api_request_sends_user_agent():
    with respx.mock(assert_all_called=True) as respx_mock:
        route = respx_mock.get(PING_URL).mock(
            return_value=httpx.Response(200, json={}),
        )
        api_request("GET", "/v1/ping")

    sent_request = route.calls.last.request
    assert sent_request.headers["User-Agent"] == user_agent.USER_AGENT
    assert sent_request.headers["X-API-Key"] == TEST_API_KEY


@pytest.mark.usefixtures("api_key")
def test_async_api_request_sends_user_agent():
    async def send() -> None:
        async with httpx.AsyncClient() as client:
            await async_api_request("GET", "/v1/ping", client=client)

    with respx.mock(assert_all_called=True) as respx_mock:
        route = respx_mock.get(PING_URL).mock(
            return_value=httpx.Response(200, json={}),
        )
        asyncio.run(send())

    sent_request = route.calls.last.request
    assert sent_request.headers["User-Agent"] == user_agent.USER_AGENT
    assert sent_request.headers["X-API-Key"] == TEST_API_KEY
