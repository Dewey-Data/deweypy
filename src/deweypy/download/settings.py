from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Literal,
)

from rich import print as rprint

from deweypy.context import main_context

if TYPE_CHECKING:
    pass  # pragma: no cover


def set_download_directory(
    download_directory: Path | None,
    *,
    download_directory_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set"
    ] = "manually_set",
    auto_create: bool = False,
) -> None:
    if not download_directory:
        raise ValueError("Download directory cannot be empty.")
    if not download_directory.exists():
        if auto_create:
            rprint(f"[dim]Creating download directory: {download_directory}[/dim]")
            download_directory.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError("Download directory does not exist.")
    if not download_directory.is_dir():
        raise ValueError("Download directory must be a directory.")
    main_context.download_directory = download_directory
    main_context.download_directory_source = download_directory_source


def resolve_download_directory(
    *,
    potentially_provided_value: Path | None,
    callback_if_missing: Callable[[], str],
    invalid_exception_class: type[RuntimeError],
    auto_create: bool = False,
) -> tuple[Path, Literal["provided", "environment", "callback"]]:
    # If `potentially_provided_value` is non-empty, then use it.
    if cli_download_directory := potentially_provided_value:
        sanity_check_download_directory_value(
            cli_download_directory,
            invalid_exception_class=invalid_exception_class,
            auto_create=auto_create,
        )
        return (cli_download_directory, "provided")

    # Otherwise, check the `DEWEY_DOWNLOAD_DIRECTORY` environment variable and
    # use that if it's present.
    if env_download_directory := os.environ.get("DEWEY_DOWNLOAD_DIRECTORY"):
        sanity_check_download_directory_value(
            Path(env_download_directory),
            invalid_exception_class=invalid_exception_class,
            empty_message=(
                "The provided Download Directory from the environment variable "
                "DEWEY_DOWNLOAD_DIRECTORY must be a valid folder or path."
            ),
            does_not_exist_message=(
                "The provided Download Directory from the environment variable "
                "DEWEY_DOWNLOAD_DIRECTORY must exist and be a valid folder."
            ),
            not_a_directory_message=(
                "The provided Download Directory from the environment variable "
                "DEWEY_DOWNLOAD_DIRECTORY must be a directory."
            ),
            auto_create=auto_create,
        )
        return (Path(env_download_directory), "environment")

    callback_download_directory: Path | str = callback_if_missing()
    if isinstance(callback_download_directory, str) and callback_download_directory:
        callback_download_directory = Path(callback_download_directory)
    sanity_check_download_directory_value(
        callback_download_directory,
        invalid_exception_class=invalid_exception_class,
        auto_create=auto_create,
    )
    assert callback_download_directory, "Post-condition"
    return (callback_download_directory, "callback")


def sanity_check_download_directory_value(
    download_directory: Path | None | Literal[""],
    *,
    invalid_exception_class: type[RuntimeError] | type[ValueError] = ValueError,
    empty_message: str = "The Download Directory must be provided.",
    does_not_exist_message: str = "The Download Directory must exist and be a valid folder.",
    not_a_directory_message: str = "The Download Directory must be a directory.",
    auto_create: bool = False,
) -> Path:
    if download_directory in ("", None):
        raise invalid_exception_class(empty_message)
    assert isinstance(download_directory, Path), "Pre-condition"
    if not download_directory.exists():
        if auto_create:
            rprint(f"[dim]Creating download directory: {download_directory}[/dim]")
            download_directory.mkdir(parents=True, exist_ok=True)
        else:
            raise invalid_exception_class(does_not_exist_message)
    if not download_directory.is_dir():
        raise invalid_exception_class(not_a_directory_message)
    return download_directory
