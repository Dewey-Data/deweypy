from __future__ import annotations

import os
from collections.abc import Callable
from typing import Literal

from deweypy.context import main_context


def set_api_key(
    api_key: str,
    *,
    api_key_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set"
    ] = "manually_set",
) -> None:
    if not api_key:
        raise ValueError("API key cannot be empty.")
    main_context.api_key = api_key.strip()
    main_context.api_key_source = api_key_source


def resolve_api_key(
    *,
    potentially_provided_value: str | None,
    callback_if_missing: Callable[[], str],
    invalid_exception_class: type[RuntimeError],
) -> tuple[str, Literal["provided", "environment", "callback"]]:
    # If `potentially_provided_value` is non-empty, then use it.
    if (cli_api_key := potentially_provided_value) is not None:
        sanity_check_api_key_value(
            cli_api_key,
            invalid_exception_class=invalid_exception_class,
            empty_message="The provided API Key cannot be empty.",
            too_short_message="The provided API Key does not appear to be valid (sanity check, too short).",
        )
        return (cli_api_key, "provided")

    # Otherwise, check the `DEWEY_API_KEY` environment variable and use that if
    # it's present.
    if env_apikey := os.environ.get("DEWEY_API_KEY"):
        sanity_check_api_key_value(
            env_apikey,
            invalid_exception_class=invalid_exception_class,
            empty_message=(
                "The provided API Key from the environment variable DEWEY_API_KEY "
                "cannot be empty."
            ),
            too_short_message=(
                "The provided API Key from the environment variable DEWEY_API_KEY does "
                "not appear to be valid (sanity check, too short)."
            ),
        )
        return (env_apikey, "environment")

    callback_api_key = callback_if_missing()
    sanity_check_api_key_value(
        callback_api_key, invalid_exception_class=invalid_exception_class
    )
    assert callback_api_key, "Post-condition"
    return (callback_api_key, "callback")


def sanity_check_api_key_value(
    api_key: str | None,
    *,
    invalid_exception_class: type[RuntimeError] | type[ValueError] = ValueError,
    empty_message: str = "The API Key cannot be empty.",
    too_short_message: str = "The API Key does not appear to be valid (sanity check, too short).",
) -> str:
    if api_key in ("", None):
        raise invalid_exception_class(empty_message)
    if len(api_key) <= 18:
        raise invalid_exception_class(too_short_message)
    return api_key
