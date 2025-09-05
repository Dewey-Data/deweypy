from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from functools import cached_property
from pathlib import Path
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import httpx
from httpx._types import (
    AuthTypes,
    CookieTypes,
    HeaderTypes,
    ProxyTypes,
    RequestContent,
    RequestData,
    RequestFiles,
    TimeoutTypes,
)
from rich import print as rprint
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from deweypy.context import MainContext, main_context
from deweypy.download.types import (
    APIMethod,
    GetMetadataDict,
)

if TYPE_CHECKING:
    import ssl  # pragma: no cover


class DatasetDownloader:
    def __init__(
        self,
        identifier: str,
        *,
        partition_key_after: str | None = None,
        partition_key_before: str | None = None,
        skip_existing: bool = True,
    ):
        self.identifier = identifier
        self.partition_key_after = partition_key_after
        self.partition_key_before = partition_key_before
        self.skip_existing = skip_existing

    @property
    def context(self) -> MainContext:
        return main_context

    @cached_property
    def base_url(self) -> str:
        identifier = self.identifier
        if "api.deweydata.io" in identifier:
            return identifier.removesuffix("/")
        return f"/v1/external/data/{identifier}"

    @cached_property
    def metadata(self) -> GetMetadataDict:
        response = api_request("GET", f"{self.base_url}/metadata")
        return cast(GetMetadataDict, response.json())

    @cached_property
    def sub_folder_path_str(self) -> str:
        # NOTE/TODO: Backend should send Dataset slug.
        return "dataset-slug"

    @property
    def max_workers(self) -> int:
        """Calculate optimal number of worker threads based on CPU count."""
        cpu_count = os.cpu_count() or 1
        # Use at least 2 * cpu_count threads, but cap at 4 threads maximum
        return min(4, max(2 * cpu_count, 2))

    def _download_single_file(
        self,
        download_info: tuple[str, str, int, Path, int, int],
        progress: Progress,
        overall_task,
        progress_lock: Lock,
    ) -> tuple[str, str, Path, bool]:
        """Download a single file with progress tracking.

        Args:
            download_info: (link, original_file_name, file_size_bytes, new_file_path, page_num, record_num)
            progress: Rich Progress instance
            overall_task: Overall progress task ID
            progress_lock: Thread lock for progress updates

        Returns:
            (original_file_name, new_file_name, new_file_path, success)
        """
        (
            link,
            original_file_name,
            file_size_bytes,
            new_file_path,
            page_num,
            record_num,
        ) = download_info
        new_file_name = original_file_name

        # Check if file exists and should be skipped
        if new_file_path.exists() and self.skip_existing:
            with progress_lock:
                progress.update(overall_task, advance=file_size_bytes)
            return (original_file_name, new_file_name, new_file_path, True)

        # Create individual file task
        with progress_lock:
            file_task = progress.add_task(
                f"Downloading {original_file_name}",
                total=file_size_bytes,
                filename=original_file_name,
            )

        try:
            # Download the file
            with make_client() as client:
                with (
                    client.stream(
                        "GET",
                        link,
                        follow_redirects=True,
                        timeout=httpx.Timeout(120.0),
                    ) as r,
                    open(new_file_path, "wb") as f,
                ):
                    for raw_bytes in r.iter_bytes():
                        chunk_size = len(raw_bytes)
                        f.write(raw_bytes)
                        # Update progress bars (thread-safe)
                        with progress_lock:
                            progress.update(file_task, advance=chunk_size)
                            progress.update(overall_task, advance=chunk_size)

            # Remove individual file task when complete
            with progress_lock:
                progress.remove_task(file_task)

            return (original_file_name, new_file_name, new_file_path, True)

        except Exception as e:
            # Remove individual file task on error
            with progress_lock:
                progress.remove_task(file_task)
            rprint(f"[red]Error downloading {original_file_name}: {e}[/red]")
            return (original_file_name, new_file_name, new_file_path, False)

    def download(self):
        metadata = self.metadata
        rprint(f"Metadata: {metadata}")

        partition_column = metadata["partition_column"]
        partition_aggregation = metadata["partition_aggregation"]
        rprint(f"Partition column: {partition_column}")
        rprint(f"Partition aggregation: {partition_aggregation}")

        rprint(f"API Key: {main_context.api_key_repr_preview}")

        root_path = self.context.download_directory
        download_directory = root_path / self.sub_folder_path_str
        if not download_directory.exists():
            rprint(f"Creating download directory {download_directory}...")
            download_directory.mkdir(parents=True)

        rprint(f"Downloading to {download_directory}...")

        base_endpoint = (
            f"https://api.deweydata.io/api/v1/external/data/{self.identifier}"
        )
        rprint(f"Base endpoint: {base_endpoint}")

        # `{(page_number, overall_record_number): (original_file_name, new_file_name, full_new_file_path)}`
        downloaded_file_paths: dict[tuple[int, int], tuple[str, str, Path]] = {}

        partition_key_after = self.partition_key_after
        partition_key_before = self.partition_key_before
        query_params: dict[str, Any] = {}
        if partition_key_after:
            query_params["partition_key_after"] = partition_key_after
        if partition_key_before:
            query_params["partition_key_before"] = partition_key_before
        rprint(f"Base query params: {query_params}")

        skip_existing = self.skip_existing
        rprint(f"Skip existing: {skip_existing}")

        # Get total files and size from metadata for progress tracking
        total_files = metadata["total_files"]
        total_size = metadata["total_size"]

        # Calculate optimal number of worker threads
        max_workers = self.max_workers
        rprint(f"Using {max_workers} worker threads for downloads")

        # Create progress bar
        progress = Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            DownloadColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            "•",
            TimeElapsedColumn(),
        )

        # Thread lock for progress updates
        progress_lock = Lock()

        # Collect all download tasks first
        download_tasks: list[tuple[str, str, int, Path, int, int]] = []

        current_page_number: int = 1
        current_overall_record_number: int = 1
        rprint("--- Collecting Files ---")

        while True:
            rprint(f"Fetching page {current_page_number}...")
            next_query_params = query_params | {"page": current_page_number}
            data_response = api_request(
                "GET", f"{self.base_url}/files", params=next_query_params
            )
            response_data = data_response.json()
            total_pages: int = response_data["total_pages"]
            rprint(f"Fetched page {current_page_number}...")

            for download_link_info in response_data["download_links"]:
                link: str = download_link_info["link"]
                original_file_name: str = download_link_info["file_name"]
                file_size_bytes: int = download_link_info["file_size_bytes"]
                new_file_name = original_file_name
                new_file_path = download_directory / new_file_name

                # Add to download tasks
                download_tasks.append(
                    (
                        link,
                        original_file_name,
                        file_size_bytes,
                        new_file_path,
                        current_page_number,
                        current_overall_record_number,
                    )
                )
                current_overall_record_number += 1

            current_page_number += 1
            if current_page_number > total_pages:
                break

        rprint(f"Collected {len(download_tasks)} files to download")

        # Now download files using thread pool
        with progress:
            # Add overall progress task
            overall_task = progress.add_task(
                "Overall Progress", total=total_size, filename="Overall"
            )

            # Track results
            files_processed = 0
            successful_downloads = 0
            failed_downloads = 0

            # Use ThreadPoolExecutor for concurrent downloads
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_task = {
                    executor.submit(
                        self._download_single_file,
                        task,
                        progress,
                        overall_task,
                        progress_lock,
                    ): task
                    for task in download_tasks
                }

                # Process completed downloads
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    (
                        link,
                        original_file_name,
                        file_size_bytes,
                        new_file_path,
                        page_num,
                        record_num,
                    ) = task

                    try:
                        result_file_name, result_new_name, result_path, success = (
                            future.result()
                        )

                        # Update tracking
                        files_processed += 1
                        if success:
                            successful_downloads += 1
                            downloaded_file_paths[(page_num, record_num)] = (
                                result_file_name,
                                result_new_name,
                                result_path,
                            )
                            # For successful downloads, bytes are already tracked in _download_single_file
                        else:
                            failed_downloads += 1

                    except Exception as e:
                        failed_downloads += 1
                        rprint(
                            f"[red]Unexpected error with {original_file_name}: {e}[/red]"
                        )

            # Calculate final stats
            # Note: total_bytes_downloaded is tracked via progress updates, not directly here
            # We can get it from the progress task if needed
            bytes_remaining = total_size - progress.tasks[overall_task].completed

            rprint("\n[bold green]Download Complete![/bold green]")
            rprint(f"Files processed: {files_processed}/{total_files}")
            rprint(f"Successful downloads: {successful_downloads}")
            rprint(f"Failed downloads: {failed_downloads}")
            rprint(f"Bytes downloaded: {int(progress.tasks[overall_task].completed):,}")
            rprint(f"Bytes remaining: {int(bytes_remaining):,}")

        rprint("Data is downloaded!")


def api_request(
    method: APIMethod,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    content: RequestContent | None = None,
    data: RequestData | None = None,
    files: RequestFiles | None = None,
    json: Any | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    auth: AuthTypes | None = None,
    proxy: ProxyTypes | None = None,
    timeout: TimeoutTypes | None = httpx.Timeout(30.0),
    follow_redirects: bool = False,
    verify: ssl.SSLContext | str | bool = True,
    trust_env: bool = True,
    client: httpx.Client | None = None,
    **kwargs: Any,
) -> httpx.Response:
    assert path.startswith("/"), "Current pre-condition"

    if "api.deweydata.io" in path:
        url = path
    else:
        url = f"https://api.deweydata.io/api{path}"

    timeout_to_use = timeout if timeout is not None else httpx.Timeout(30.0)
    headers_to_use: dict[str, str] = {
        "Content-Type": "application/json",
        # NOTE/TODO: Once we have this versioned, we can include more info on
        # the User-Agent here.
        "User-Agent": "deweypy/0.0.1a1",
        "X-API-Key": main_context.api_key,
        **(headers or {}),  # type: ignore[dict-item]
    }

    _client_to_use = (
        make_client(
            headers=headers_to_use,
            cookies=cookies,
            proxy=proxy,
            timeout=timeout_to_use,
            verify=verify,
            trust_env=trust_env,
        )
        if client is None
        else client
    )

    with _client_to_use if client is None else nullcontext(client) as client_to_use:
        response = client_to_use.request(
            method,
            url,
            params=params,
            content=content,
            data=data,
            files=files,
            json=json,
            headers=headers_to_use,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            **kwargs,
        )

    response.raise_for_status()

    return response


def make_client(
    *,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    proxy: ProxyTypes | None = None,
    timeout: TimeoutTypes | None = httpx.Timeout(30.0),
    verify: ssl.SSLContext | str | bool = True,
    trust_env: bool = True,
    **kwargs: Any,
) -> httpx.Client:
    timeout_to_use = timeout if timeout is not None else httpx.Timeout(30.0)
    headers_to_use: dict[str, str] = {
        # NOTE/TODO: Once we have this versioned, we can include more info on
        # the User-Agent here.
        "User-Agent": "deweypy/0.0.1a1",
        "X-API-Key": main_context.api_key,
        **(headers or {}),  # type: ignore[dict-item]
    }

    return httpx.Client(
        cookies=cookies,
        proxy=proxy,
        verify=verify,
        timeout=timeout_to_use,
        trust_env=trust_env,
        headers=headers_to_use,
        **kwargs,
    )
