from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from textwrap import dedent
from typing import Any, Literal, NoReturn

from deweypy.display import ELLIPSIS_CHAR


@dataclass(kw_only=True)
class MainContext:
    entrypoint: Literal["cli", "other", "unknown"] = "unknown"

    _api_key: str = field(repr=False, default="")
    api_key_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set", "unknown"
    ] = "unknown"
    api_key_repr_preview: str = field(init=False, repr=True)

    _download_directory: Path | None = field(default=None)
    download_directory_repr_preview: str = field(init=False, repr=True)
    download_directory_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set", "unknown"
    ] = "unknown"

    def __post_init__(self):
        self._set_api_key_repr_preview()
        self._set_download_directory_repr_preview()

    def __setattr__(self, name: str, value: Any):
        parent_return_value = super().__setattr__(name, value)
        if name == "_api_key":
            self._set_api_key_repr_preview()
        elif name == "_download_directory":
            self._set_download_directory_repr_preview()
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
                1. If using `deweypy` directly from the shell/command line, you can
                   provide the --api-key option to set it.
                2. Set the `DEWEY_API_KEY` environment variable.
                3. If using `deweypy` as a Python library/module, you can call
                   `deweypy.auth.set_api_key()` with the API key as the argument.
                   For example:
                   ```
                   import deweypy.auth
                   deweypy.auth.set_api_key("your_api_key_value...")
                   ```
                """
            ).strip()
        )

    @property
    def download_directory(self) -> Path:
        if self._download_directory:
            return self._download_directory
        download_module = _get_download_module()
        resolved_download_directory = download_module.resolve_download_directory(
            potentially_provided_value=self._download_directory,
            callback_if_missing=self._missing_download_directory_callback,
            invalid_exception_class=RuntimeError,
        )
        self._download_directory = resolved_download_directory
        download_module.sanity_check_download_directory_value(self._download_directory)
        return resolved_download_directory

    @download_directory.setter
    def download_directory(self, value: Path):
        self._download_directory = value
        self._set_download_directory_repr_preview()

    def _set_download_directory_repr_preview(self):
        if self._download_directory in ("", None):
            self.download_directory_repr_preview = "not_set"
            return
        download_module = _get_download_module()
        download_module.sanity_check_download_directory_value(self._download_directory)
        assert self._download_directory is not None, "Post-condition"
        preview_value = self._download_directory.as_posix()
        self.download_directory_repr_preview = preview_value

    @staticmethod
    def _missing_download_directory_callback() -> NoReturn:
        raise RuntimeError(
            dedent(
                """
                The Download Directory is not set. You can set it via one of three ways:
                1. If using `deweypy` directly from the shell/command line, you
                   can provide the --download-directory option to set it.
                2. Set the `DEWEY_DOWNLOAD_DIRECTORY` environment variable.
                3. If using `deweypy` as a Python library/module, you can call
                   `deweypy.downloads.set_download_directory()` with the
                   download directory as the argument. For example:
                   ```
                   import deweypy.downloads
                   deweypy.downloads.set_download_directory("your_download_directory_value...")
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


@lru_cache(1)
def _get_download_module():
    from deweypy import download as download_module

    return download_module
