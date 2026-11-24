from __future__ import annotations

import re
import tomllib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import httpx
import pytest
import respx

import deweypy
from deweypy import user_agent
from deweypy.context import main_context
from deweypy.download.asynchronous import make_async_client
from deweypy.download.synchronous import api_request, make_client

# Mirrors the version pattern Dewey's backend and download proxy use to parse the
# `deweypy` client version out of the `User-Agent`.
DEWEY_SERVER_VERSION_PATTERN = re.compile(r"deweypy/([0-9a-zA-Z.\-_]{1,15})")

TEST_API_KEY = "test-api-key-0123456789abcdef"
PYPROJECT_PATH = Path(__file__).resolve().parents[1] / "pyproject.toml"


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(main_context, "_api_key", TEST_API_KEY)
    return TEST_API_KEY


def test_version_comes_from_package_metadata():
    assert user_agent.get_deweypy_version() == version("deweypy")
    assert deweypy.__version__ == version("deweypy")


def test_version_matches_pyproject():
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text())
    assert deweypy.__version__ == pyproject["project"]["version"]


def test_user_agent_is_bare_deweypy_version():
    assert f"deweypy/{deweypy.__version__}" == user_agent.USER_AGENT
    # Older Dewey backends only parse the version from this exact, anchored form.
    assert re.fullmatch(DEWEY_SERVER_VERSION_PATTERN, user_agent.USER_AGENT)


def test_version_falls_back_without_package_metadata(
    monkeypatch: pytest.MonkeyPatch,
):
    def raise_not_found(distribution_name: str) -> str:
        raise PackageNotFoundError(distribution_name)

    monkeypatch.setattr(user_agent, "version", raise_not_found)

    assert user_agent.get_deweypy_version() == user_agent.UNKNOWN_VERSION
    fallback_user_agent = user_agent.build_user_agent()
    assert fallback_user_agent == "deweypy/unknown"
    match = re.fullmatch(DEWEY_SERVER_VERSION_PATTERN, fallback_user_agent)
    assert match is not None
    assert match.group(1) == "unknown"


def test_build_user_agent_uses_the_given_version():
    assert user_agent.build_user_agent("1.2.3") == "deweypy/1.2.3"


@pytest.mark.usefixtures("api_key")
def test_make_client_sends_user_agent():
    with make_client() as client:
        assert client.headers["User-Agent"] == user_agent.USER_AGENT


@pytest.mark.usefixtures("api_key")
def test_make_async_client_sends_user_agent():
    client = make_async_client()
    assert client.headers["User-Agent"] == user_agent.USER_AGENT


@pytest.mark.usefixtures("api_key")
def test_api_request_sends_user_agent():
    with respx.mock(assert_all_called=True) as respx_mock:
        route = respx_mock.get("https://api.deweydata.io/api/v1/ping").mock(
            return_value=httpx.Response(200, json={}),
        )
        api_request("GET", "/v1/ping")

    sent_request = route.calls.last.request
    assert sent_request.headers["User-Agent"] == user_agent.USER_AGENT
    assert sent_request.headers["X-API-Key"] == TEST_API_KEY
