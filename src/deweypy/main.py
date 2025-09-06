from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, cast

import httpx
import typer
from rich import print as rprint

from deweypy.app import app
from deweypy.auth import (
    resolve_api_key,
    set_api_key,
)
from deweypy.context import main_context, set_entrypoint
from deweypy.download import (
    DatasetDownloader,
    resolve_download_directory,
    set_download_directory,
)
from deweypy.download.speed_testing import run_download_speed_test
from deweypy.download.speedy import run_speedy_download

_shared_api_key_option = typer.Option(
    None, "--api-key", help="Your Dewey API Key.", show_default=False
)
_shared_download_directory_option = typer.Option(
    f".{os.sep}dewey-downloads",
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
        f".{os.sep}dewey-downloads",
        "--download-directory",
        prompt=(
            f"What directory do you want to download the data to? Defaults to the "
            f".{os.sep}dewey-downloads directory (the dewey-downloads folder within the "
            "current directory)."
        ),
        confirmation_prompt=True,
        help=(
            "Directory to download the data to. Defaults to the dewey-downloads folder "
            "within the current directory."
        ),
        show_default=True,
    ),
):
    def download_directory_callback() -> str:
        return cast(
            str,
            typer.prompt(
                (
                    "Paste, type, or confirm the download directory you want to "
                    "download files to. Defaults to the dewey-downloads folder within "
                    "the current directory."
                ),
                default=f".{os.sep}dewey-downloads",
            ),
        )

    download_directory_path: Path | None = None
    if isinstance(download_directory, str) and download_directory:
        download_directory_path = Path(download_directory)

    resolved_download_directory, resolved_source = resolve_download_directory(
        potentially_provided_value=download_directory_path,
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
def download(
    ds_or_folder_id: str = typer.Argument(..., help="Dataset or Folder ID."),
    partition_key_after: str | None = typer.Option(None, help="Partition key after."),
    partition_key_before: str | None = typer.Option(None, help="Partition key before."),
    skip_existing: bool = typer.Option(True, help="Skip existing files?"),
):
    rprint("Hello from `download`!")

    downloader = DatasetDownloader(
        ds_or_folder_id,
        partition_key_after=partition_key_after,
        partition_key_before=partition_key_before,
        skip_existing=skip_existing,
    )
    downloader.download()


@app.command()
def speedy_download(
    ds_or_folder_id: str = typer.Argument(..., help="Dataset or Folder ID."),
    partition_key_after: str | None = typer.Option(None, help="Partition key after."),
    partition_key_before: str | None = typer.Option(None, help="Partition key before."),
    skip_existing: bool = typer.Option(True, help="Skip existing files?"),
    num_workers: int | None = typer.Option(
        -1, help="Number of async workers. Use -1 to get 'auto' behavior."
    ),
    buffer_chunk_size: int | None = typer.Option(
        -1, help="Async file download buffer chunk size. Use -1 to get 'auto' behavior."
    ),
):
    rprint("Hello from `speedy_download`!")

    parsed_num_workers: int | Literal["auto"] | None = num_workers
    if num_workers in (None, "None", "none", "NULL", "NULL", "null"):
        parsed_num_workers = None
    elif num_workers in ("auto", "Auto", "AUTO", "", "-1", -1):
        parsed_num_workers = "auto"
    else:
        parsed_num_workers = int(num_workers)  # type: ignore[arg-type]

    parsed_buffer_chunk_size: int | Literal["auto"] | None = buffer_chunk_size
    if buffer_chunk_size in (None, "None", "none", "NULL", "NULL", "null"):
        parsed_buffer_chunk_size = None
    elif buffer_chunk_size in ("auto", "Auto", "AUTO", "", "-1", -1):
        parsed_buffer_chunk_size = "auto"
    else:
        parsed_buffer_chunk_size = int(buffer_chunk_size)  # type: ignore[arg-type]

    run_speedy_download(
        ds_or_folder_id,
        partition_key_after=partition_key_after,
        partition_key_before=partition_key_before,
        skip_existing=skip_existing,
        num_workers=parsed_num_workers,
        buffer_chunk_size=parsed_buffer_chunk_size,
    )


@app.command()
def speed_test(
    url: str = typer.Argument(..., help="URL to download repeatedly for timing."),
    n: int = typer.Argument(10, help="Number of sequential full downloads to perform."),
):
    """
    Run a synchronous HTTP download speed test for a given URL
    (currently required to be a Dewey URL).

    Uses a regular `httpx` `Client` (non-async), with full downloads (no streaming).
    """
    with httpx.Client(http2=True, timeout=60.0) as client:
        run_download_speed_test(client, url, num_downloads=n)
