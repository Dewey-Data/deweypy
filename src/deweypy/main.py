from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import typer
from rich import print as rprint

from deweypy.app import app
from deweypy.auth import (
    resolve_api_key,
    set_api_key,
)
from deweypy.context import main_context, set_entrypoint
from deweypy.downloads import resolve_download_directory, set_download_directory

_shared_api_key_option = typer.Option(
    None, "--api-key", help="Your Dewey API Key.", show_default=False
)
_shared_download_directory_option = typer.Option(
    None,
    "--download-directory",
    help="Directory to download the data to.",
    show_default=False,
)
_shared_print_debug_info_option = typer.Option(
    False, "--print-debug-info", help="Print debug info?", show_default=False
)


def _handle_api_key_option(
    api_key: str | None = typer.Option(
        None,
        "--api-key",
        prompt="Paste your API key",
        hide_input=True,
        help="Your Dewey API Key.",
        show_default=False,
    ),
):
    def prompt_callback() -> str:
        return cast(
            str,
            typer.prompt(
                "Paste your API key (input hidden, won't trigger until you paste/type and then press Enter)",
                hide_input=True,
            ),
        )

    resolved_api_key, resolved_source = resolve_api_key(
        potentially_provided_value=api_key,
        callback_if_missing=prompt_callback,
        invalid_exception_class=RuntimeError,
    )
    assert resolved_source in ("provided", "environment", "callback"), "Post-condition"
    api_key_source: Literal["cli_args", "cli_fallback", "environment"]
    if resolved_source == "provided":
        api_key_source = "cli_args"
    elif resolved_source == "environment":
        api_key_source = "environment"
    else:
        assert resolved_source == "callback", "Pre-condition"
        api_key_source = "cli_fallback"
    set_api_key(resolved_api_key, api_key_source=api_key_source)
    set_entrypoint("cli")

    return resolved_api_key


def _handle_download_directory_option(
    download_directory: str = typer.Option(
        ".",
        "--download-directory",
        prompt=(
            "What directory do you want to download the data to? Defaults to the "
            "current directory."
        ),
        confirmation_prompt=True,
        help="Directory to download the data to. Defaults to the current directory.",
        show_default=True,
    ),
):
    def download_directory_callback() -> str:
        return cast(
            str,
            typer.prompt(
                (
                    "Paste, type, or confirm the download directory you want to "
                    "download files to. Defaults to the current directory."
                ),
                default=".",
            ),
        )

    if isinstance(download_directory, str) and download_directory:
        download_directory = Path(download_directory)

    resolved_download_directory, resolved_source = resolve_download_directory(
        potentially_provided_value=download_directory,
        callback_if_missing=download_directory_callback,
        invalid_exception_class=RuntimeError,
    )
    assert resolved_source in ("provided", "environment", "callback"), "Post-condition"
    download_directory_source: Literal["cli_args", "cli_fallback", "environment"]
    if resolved_source == "provided":
        download_directory_source = "cli_args"
    elif resolved_source == "environment":
        download_directory_source = "environment"
    else:
        assert resolved_source == "callback", "Pre-condition"
        download_directory_source = "cli_fallback"
    set_download_directory(
        resolved_download_directory, download_directory_source=download_directory_source
    )
    set_entrypoint("cli")

    return resolved_download_directory


@app.callback()
def main(
    *,
    api_key: str = _shared_api_key_option,
    download_directory: str = _shared_download_directory_option,
    print_debug_info: bool = _shared_print_debug_info_option,
):
    set_entrypoint("cli")

    _handle_api_key_option(api_key)
    _handle_download_directory_option(download_directory)

    if print_debug_info:
        rprint("--- Initial Debug Info ---")
        prefix = "Main Global Callback Main Context:"
        rprint(f"{prefix} context={main_context}")
        rprint(f"{prefix} entrypoint={main_context.entrypoint}")
        rprint(f"{prefix} _api_key={main_context.api_key}")
        rprint(f"{prefix} api_key_source={main_context.api_key_source}")
        rprint(f"{prefix} api_key_repr_preview={main_context.api_key_repr_preview}")
        rprint("---            ---")


@app.command()
def download(dataset: str = typer.Argument(..., help="The dataset to download.")):
    rprint("Hello from `download`!")
    api_key = main_context.api_key
    download_directory = main_context.download_directory
    rprint(f"api_key={api_key}")
    rprint(f"download_directory={download_directory}")
