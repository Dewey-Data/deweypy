from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal, NoReturn
from textwrap import dedent

from deweypy.display import ELLIPSIS_CHAR


@dataclass(kw_only=True)
class MainContext:
    entrypoint: Literal["cli", "other", "unknown"] = "unknown"
    _api_key: str = field(repr=False, default="")
    api_key_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set", "unknown"
    ] = "unknown"
    api_key_repr_preview: str = field(init=False, repr=True)

    def __post_init__(self):
        self._set_api_key_repr_preview()

    def __setattr__(self, name: str, value: Any):
        parent_return_value = super().__setattr__(name, value)
        if name == "_api_key":
            self._set_api_key_repr_preview()
        return parent_return_value

    @property
    def api_key(self) -> str:
        if self._api_key:
            return self._api_key
        auth_module = _get_auth_module()
        resolved_api_key = auth_module.resolve_api_key(
            potentially_provided_value=self._api_key,
            callback_if_missing=self._missing_api_key_callback,
            invalid_exception_class=RuntimeError,
        )
        self._api_key = resolved_api_key
        auth_module.sanity_check_api_key_value(self._api_key)
        return resolved_api_key

    @api_key.setter
    def api_key(self, value: str):
        self._api_key = value
        self._set_api_key_repr_preview()

    def _set_api_key_repr_preview(self):
        if self._api_key in ("", None):
            self.api_key_repr_preview = "not_set"
            return
        auth_module = _get_auth_module()
        auth_module.sanity_check_api_key_value(self._api_key)
        preview_value = self._api_key[:4] + ELLIPSIS_CHAR + self.api_key[-3:]
        self.api_key_repr_preview = preview_value

    @staticmethod
    def _missing_api_key_callback() -> NoReturn:
        raise RuntimeError(
            dedent(
                """
                The API Key is not set. You can set it via one of three ways:
                1. If using `dewepy` directly from the shell/command line, you can
                   provide the --api-key option to set it.
                2. Set the `DEWEY_API_KEY` environment variable.
                3. If using `dewepy` as a Python library/module, you can call
                   `deweypy.auth.set_api_key()` with the API key as the argument.
                   For example:
                   ```
                   import deweypy.auth
                   deweypy.auth.set_api_key("your_api_key_value...")
                   ```
                """
            ).strip()
        )


main_context = MainContext()


def set_entrypoint(entrypoint: Literal["cli", "other"]):
    main_context.entrypoint = entrypoint


@lru_cache(1)
def _get_auth_module():
    from deweypy import auth as auth_module

    return auth_module
