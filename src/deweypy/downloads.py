from __future__ import annotations

import os
from collections.abc import Callable
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypedDict, cast

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

if TYPE_CHECKING:
    import ssl  # pragma: no cover


def set_download_directory(
    download_directory: Path | None,
    *,
    download_directory_source: Literal[
        "cli_args", "cli_fallback", "environment", "manually_set"
    ] = "manually_set",
) -> None:
    if not download_directory:
        raise ValueError("Download directory cannot be empty.")
    if not download_directory.exists():
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
) -> tuple[Path, Literal["provided", "environment", "callback"]]:
    # If `potentially_provided_value` is non-empty, then use it.
    if cli_download_directory := potentially_provided_value:
        sanity_check_download_directory_value(
            cli_download_directory, invalid_exception_class=invalid_exception_class
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
        )
        return (Path(env_download_directory), "environment")

    callback_download_directory = callback_if_missing()
    if isinstance(callback_download_directory, str) and callback_download_directory:
        callback_download_directory = Path(callback_download_directory)
    sanity_check_download_directory_value(
        callback_download_directory, invalid_exception_class=invalid_exception_class
    )
    assert callback_download_directory, "Post-condition"
    return (callback_download_directory, "callback")


def sanity_check_download_directory_value(
    download_directory: Path | None,
    *,
    invalid_exception_class: type[RuntimeError] | type[ValueError] = ValueError,
    empty_message: str = "The Download Directory must be provided.",
    does_not_exist_message: str = "The Download Directory must exist and be a valid folder.",
    not_a_directory_message: str = "The Download Directory must be a directory.",
) -> str:
    if download_directory in ("", None):
        raise invalid_exception_class(empty_message)
    if not download_directory.exists():
        raise invalid_exception_class(does_not_exist_message)
    if not download_directory.is_dir():
        raise invalid_exception_class(not_a_directory_message)
    return download_directory


class DownloadItemDict(TypedDict):
    link: str
    partition_key: str | None
    file_name: str
    file_extension: Literal[".csv", ".csv.gz", ".parquet", ".parquet.gz"]
    file_size_bytes: int
    modified_at: str  # ISO-8601 formatted datetime string
    external_id: str


class ZippedDownloadItemDict(DownloadItemDict):
    is_zip_file: Literal[True]


class GetMetadataDict(TypedDict):
    total_files: int
    total_size: int
    total_partitions: int
    partition_type: Literal["DATE", "CATEGORICAL"] | None
    partition_column: str | None
    partition_aggregation: Literal["DAY", "MONTH"] | None
    min_partition_key: str | None
    max_partition_key: str | None


class GetFilesDict(TypedDict):
    download_links: list[DownloadItemDict]
    page: int
    number_of_files_for_page: int
    avg_file_size_for_page: int | float | None
    partition_column: str | None
    total_files: int
    total_pages: int
    total_size: int
    expires_at: str  # ISO-8601 formatted datetime string
    zip_file: ZippedDownloadItemDict | None


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

        with make_client() as client, progress:
            # Add overall progress task
            overall_task = progress.add_task(
                "Overall Progress", total=total_size, filename="Overall"
            )

            # Track files processed and bytes downloaded
            files_processed = 0
            total_bytes_downloaded = 0

            current_page_number: int = 1
            current_overall_record_number: int = 1
            rprint("--- Files ---")
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

                    if new_file_path.exists() and skip_existing:
                        downloaded_file_paths[
                            (current_page_number, current_overall_record_number)
                        ] = (
                            original_file_name,
                            new_file_name,
                            new_file_path,
                        )
                        current_overall_record_number += 1
                        files_processed += 1
                        total_bytes_downloaded += file_size_bytes

                        # Update progress for skipped file
                        progress.update(overall_task, advance=file_size_bytes)

                        rprint(f"Skipping {original_file_name} -> {new_file_name}...")
                        continue

                    # Add individual file task
                    file_task = progress.add_task(
                        f"File {files_processed + 1}/{total_files}",
                        total=file_size_bytes,
                        filename=original_file_name,
                    )

                    rprint(f"Downloading {original_file_name} -> {new_file_name}...")
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
                            # Update progress bars
                            progress.update(file_task, advance=chunk_size)
                            progress.update(overall_task, advance=chunk_size)
                            total_bytes_downloaded += chunk_size

                    # Remove individual file task when complete
                    progress.remove_task(file_task)

                    rprint(f"Downloaded {original_file_name} -> {new_file_name}...")
                    downloaded_file_paths[
                        (current_page_number, current_overall_record_number)
                    ] = (
                        original_file_name,
                        new_file_name,
                        new_file_path,
                    )
                    current_overall_record_number += 1
                    files_processed += 1

                current_page_number += 1
                if current_page_number > total_pages:
                    break

            # Show final stats
            bytes_remaining = total_size - total_bytes_downloaded
            rprint("\n[bold green]Download Complete![/bold green]")
            rprint(f"Files processed: {files_processed}/{total_files}")
            rprint(f"Bytes downloaded: {total_bytes_downloaded:,}")
            rprint(f"Bytes remaining: {bytes_remaining:,}")

        rprint("Data is downloaded!")


APIMethod: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]


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
        "User-Agent": "deweypy/0.0.0",
        "X-API-Key": main_context.api_key,
        **(headers or {}),  # type: ignore[dict-item]
    }

    client_to_use = (
        httpx.Client(
            cookies=cookies,
            proxy=proxy,
            verify=verify,
            timeout=timeout_to_use,
            trust_env=trust_env,
        )
        if client is None
        else client
    )

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
        "User-Agent": "deweypy/0.0.0",
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
