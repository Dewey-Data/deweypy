from __future__ import annotations

from typing import NoReturn

import httpx
from rich import print as rprint


def potentially_augment_error(exception: Exception, *, outputter=rprint) -> None:
    o = outputter

    if isinstance(exception, httpx.HTTPStatusError):
        response = exception.response

        if response is not None and response.status_code == 401:
            o("[red]Looks like you might be missing an API Key?[/red]")

        if response is not None and response.status_code == 403:
            try:
                data = response.json()
            except Exception:
                pass
            else:
                if isinstance(data, dict) and "detail" in data:
                    o(f"[red]403 (Not Permitted) More Info: {data['detail']}[/red]")
                elif isinstance(data, dict) and "message" in data:
                    o(f"[red]403 (Not Permitted) More Info: {data['message']}[/red]")
