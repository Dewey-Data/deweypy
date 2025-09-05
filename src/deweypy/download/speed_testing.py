from __future__ import annotations

import time

import httpx
from rich import print as rprint

from deweypy.context import main_context


def run_download_speed_test(
    client: httpx.Client,
    url: str,
    num_downloads: int = 10,
    use_api_key: bool = False,
) -> tuple[list[float], list[int], list[int], float, float, float]:
    """Run a simple synchronous download speed test using the provided client.

    - Executes `num_downloads` sequential GET requests to `url` (following redirects).
    - Measures elapsed time per request using `time.perf_counter()`.
    - Performs full-body downloads (no streaming).

    Returns `(durations_seconds, status_codes, payload_sizes_bytes)`.
    """
    if num_downloads <= 0:
        raise ValueError("`num_downloads` must be a positive integer.")

    durations_seconds: list[float] = []
    status_codes: list[int] = []
    payload_sizes_bytes: list[int] = []

    for run_index in range(1, num_downloads + 1):
        start_perf = time.perf_counter()
        headers: dict[str, str] = {}
        if use_api_key:
            headers["X-API-Key"] = main_context.api_key
        response = client.get(url, follow_redirects=True)
        # Ensure the content is fully loaded into memory before stopping the timer.
        content = response.content
        end_perf = time.perf_counter()
        duration_seconds = end_perf - start_perf

        durations_seconds.append(duration_seconds)
        status_codes.append(response.status_code)
        payload_sizes_bytes.append(len(content))

        rprint(
            f"#{run_index}: {duration_seconds * 1000:.2f} ms | status={response.status_code} | bytes={len(content)}"
        )

    avg_seconds = sum(durations_seconds) / len(durations_seconds)
    min_seconds = min(durations_seconds)
    max_seconds = max(durations_seconds)
    rprint(
        " ".join(
            [
                f"n={num_downloads};",
                f"avg={avg_seconds * 1000:.2f} ms;",
                f"min={min_seconds * 1000:.2f} ms;",
                f"max={max_seconds * 1000:.2f} ms",
            ]
        )
    )

    return (durations_seconds, status_codes, payload_sizes_bytes)
