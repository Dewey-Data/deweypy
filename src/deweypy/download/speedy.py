from __future__ import annotations

import asyncio
import sys

from rich import print as rprint

from deweypy.download import AsyncDatasetDownloader


def run_speedy_download(
    ds_or_folder_id: str,
    *,
    partition_key_after: str | None = None,
    partition_key_before: str | None = None,
    skip_existing: bool = True,
):
    async def run():
        downloader = AsyncDatasetDownloader(
            ds_or_folder_id,
            partition_key_after=partition_key_after,
            partition_key_before=partition_key_before,
            skip_existing=skip_existing,
        )
        await downloader.download_all()

    # https://github.com/Vizonex/Winloop?tab=readme-ov-file#how-to-use-winloop-when-uvloop-is-not-available
    if sys.platform in ("win32", "cygwin", "cli"):
        try:
            # If on Windows, use `winloop` (https://github.com/Vizonex/Winloop)
            # if available.
            from winloop import (  # pyright: ignore[reportMissingImports]
                run as loop_run_fn,
            )
        except ImportError:
            # Otherwise, fall back to `asyncio` `run.`
            from asyncio import run as loop_run_fn
    else:
        try:
            # If on Linux/macOs/non-Windows, etc., use `uvloop` if available.
            from uvloop import run as loop_run_fn
        except ImportError:
            # Otherwise, fall back to `asyncio` `run.`
            from asyncio import run as loop_run_fn

    running_loop = None
    try:
        running_loop = asyncio.get_running_loop()
    except Exception:
        pass

    if running_loop is None:
        rprint("No running loop found, using `loop_run_fn` to run the coroutine.")
        loop_run_fn(run())
    else:
        rprint(
            "Running loop found, using `running_loop.run_until_complete` to run the "
            "coroutine."
        )
        running_loop.run_until_complete(run())
