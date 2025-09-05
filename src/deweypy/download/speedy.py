from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Literal

from rich import print as rprint

from deweypy.download import AsyncDatasetDownloader

SHOULD_DEBUG_SPEEDY: bool = os.getenv("DEWEY_SPEEDY_DEBUG") in (
    True,
    "True",
    "true",
    1,
    "1",
    "Yes",
    "yes",
    "Y",
    "y",
)

if SHOULD_DEBUG_SPEEDY:
    logging.basicConfig(level=logging.DEBUG)


def run_speedy_download(
    ds_or_folder_id: str,
    *,
    partition_key_after: str | None = None,
    partition_key_before: str | None = None,
    skip_existing: bool = True,
    num_workers: int | Literal["auto"] | None = "auto",
    buffer_chunk_size: int | Literal["auto"] | None = "auto",
):
    found_winloop: bool = False
    found_uvloop: bool = False
    # https://github.com/Vizonex/Winloop?tab=readme-ov-file#how-to-use-winloop-when-uvloop-is-not-available
    if sys.platform in ("win32", "cygwin", "cli"):
        try:
            # If on Windows, use `winloop` (https://github.com/Vizonex/Winloop)
            # if available.
            from winloop import (  # pyright: ignore[reportMissingImports]
                run as loop_run_fn,
            )

            found_winloop = True
        except ImportError:
            # Otherwise, fall back to `asyncio` `run.`
            from asyncio import run as loop_run_fn
    else:
        try:
            # If on Linux/macOs/non-Windows, etc., use `uvloop` if available.
            from uvloop import run as loop_run_fn

            found_uvloop = True
        except ImportError:
            # Otherwise, fall back to `asyncio` `run.`
            from asyncio import run as loop_run_fn

    running_loop = None
    try:
        running_loop = asyncio.get_running_loop()
    except Exception:
        pass

    async def run():
        downloader = AsyncDatasetDownloader(
            ds_or_folder_id,
            partition_key_after=partition_key_after,
            partition_key_before=partition_key_before,
            skip_existing=skip_existing,
            num_workers=num_workers,
            buffer_chunk_size=buffer_chunk_size,
        )
        await downloader.download_all()

    if running_loop is None:
        rprint("No running loop found, using `loop_run_fn` to run the coroutine.")
        if found_winloop:
            rprint("Using `winloop` to run the coroutine.")
        elif found_uvloop:
            rprint("Using `uvloop` to run the coroutine.")
        else:
            rprint("Using standard `asyncio` loop to run the coroutine.")
        if SHOULD_DEBUG_SPEEDY:
            loop_run_fn(run(), debug=True)
        else:
            loop_run_fn(run())
    else:
        rprint(
            "Running loop found, using `running_loop.run_until_complete` to run the "
            "coroutine."
        )
        running_loop.run_until_complete(run())
