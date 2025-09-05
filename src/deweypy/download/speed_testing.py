from __future__ import annotations

import time

import httpx
from rich import print as rprint


def run_download_speed_test(
    client: httpx.Client,
    url: str,
    num_downloads: int = 10,
    use_api_key: bool = False,
) -> tuple[list[float], list[int], list[int]]:
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

    total_bytes: int = 0
    total_seconds: float = 0.0

    rprint("Unwrapping final URL...")
    try:
        dewey_response = client.get(
            url,
            follow_redirects=False,
            timeout=httpx.Timeout(30.0),
        )
        status_code = dewey_response.status_code
        if not status_code or status_code < 200 or status_code >= 400:
            dewey_response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error downloading {url}: {e}") from e
    if dewey_response.status_code not in (301, 302):
        raise RuntimeError(
            "Expecting a 301 or 302 redirect from Dewey at the time of writing."
        )
    final_url = dewey_response.headers.get("Location")
    if not final_url or not isinstance(final_url, str):
        raise RuntimeError(f"Expected a string URL for the final URL, got {final_url}.")
    rprint("Unwrapped final URL.")

    for run_index in range(1, num_downloads + 1):
        start_perf = time.perf_counter()
        response = client.get(final_url, follow_redirects=True)
        # Ensure the content is fully loaded into memory before stopping the timer.
        content = response.content
        end_perf = time.perf_counter()
        duration_seconds = end_perf - start_perf

        durations_seconds.append(duration_seconds)
        status_codes.append(response.status_code)
        payload_sizes_bytes.append(len(content))
        total_bytes += len(content)
        total_seconds += duration_seconds

        mb_per_s = (
            (len(content) / 1_000_000.0 / duration_seconds)
            if duration_seconds > 0
            else 0.0
        )

        rprint(
            f"#{run_index}: {duration_seconds * 1000:.2f} ms | status={response.status_code} "
            f"| bytes={len(content)} | {mb_per_s:.2f} MB/s"
        )

    avg_seconds = sum(durations_seconds) / len(durations_seconds)
    min_seconds = min(durations_seconds)
    max_seconds = max(durations_seconds)
    avg_mb_per_s = (
        (total_bytes / 1_000_000.0 / total_seconds) if total_seconds > 0 else 0.0
    )
    rprint(
        " ".join(
            [
                f"n={num_downloads};",
                f"avg={avg_seconds * 1000:.2f} ms;",
                f"min={min_seconds * 1000:.2f} ms;",
                f"max={max_seconds * 1000:.2f} ms;",
                f"avg={avg_mb_per_s:.2f} MB/s",
            ]
        )
    )

    return (durations_seconds, status_codes, payload_sizes_bytes)
