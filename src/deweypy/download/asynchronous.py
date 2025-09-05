from __future__ import annotations

import asyncio
from contextlib import nullcontext
from os import stat_result
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    TypeAlias,
    cast,
)

import aiofiles
import httpx
from aiopath import AsyncPath
from async_property import async_cached_property
from attrs import define
from culsans import (
    AsyncQueue as TwoColoredAsyncQueue,
)
from culsans import (
    Queue as TwoColoredQueue,
)
from culsans import QueueEmpty
from culsans import (
    SyncQueue as TwoColoredSyncQueue,
)
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
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from deweypy.context import MainContext, main_context
from deweypy.download.types import (
    APIMethod,
    DescribedDatasetDict,
    GetFilesDict,
    GetMetadataDict,
)

if TYPE_CHECKING:
    import ssl  # pragma: no cover


async def async_api_request(
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
    client: httpx.AsyncClient | None = None,
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

    _client_to_use = (
        make_async_client(
            headers=headers_to_use,
            cookies=cookies,
            proxy=proxy,
            timeout=timeout_to_use,
            verify=verify,
            trust_env=trust_env,
            http2=True,
        )
        if client is None
        else client
    )

    async with (
        _client_to_use if client is None else nullcontext(client)
    ) as client_to_use:
        response = await client_to_use.request(
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


def make_async_client(
    *,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    proxy: ProxyTypes | None = None,
    timeout: TimeoutTypes | None = httpx.Timeout(30.0),
    verify: ssl.SSLContext | str | bool = True,
    trust_env: bool = True,
    http2: bool = True,
    **kwargs: Any,
) -> httpx.AsyncClient:
    timeout_to_use = timeout if timeout is not None else httpx.Timeout(30.0)
    headers_to_use: dict[str, str] = {
        # NOTE/TODO: Once we have this versioned, we can include more info on
        # the User-Agent here.
        "User-Agent": "deweypy/0.0.0",
        "X-API-Key": main_context.api_key,
        **(headers or {}),  # type: ignore[dict-item]
    }

    return httpx.AsyncClient(
        cookies=cookies,
        proxy=proxy,
        verify=verify,
        timeout=timeout_to_use,
        trust_env=trust_env,
        headers=headers_to_use,
        http2=http2,
        **kwargs,
    )


@define(kw_only=True, slots=True)
class DownloadSingleFileInfo:
    link: str
    original_file_name: str
    file_size_bytes: int
    new_file_path: AsyncPath
    page_num: int
    record_num: int


@define(kw_only=True, slots=True)
class DownloadSingleFileResult:
    original_file_name: str
    new_file_name: str
    new_file_path: AsyncPath
    did_skip: bool


@define(kw_only=True, slots=True)
class MessageProgressAddTask:
    key: str
    message: str
    total: int
    filename: str


@define(kw_only=True, slots=True)
class MessageProgressUpdateTask:
    key: str
    advance: int


@define(kw_only=True, slots=True)
class MessageProgressRemoveTask:
    key: str


@define(slots=True)
class MessageLog:
    rprint: str


@define(slots=True)
class MessageWorkDone:
    pass


@define(kw_only=True, slots=True)
class MessageFetchFile:
    info: DownloadSingleFileInfo


LogQueueRecordType: TypeAlias = (
    MessageProgressAddTask
    | MessageProgressUpdateTask
    | MessageProgressRemoveTask
    | MessageLog
    | MessageWorkDone
)
TwoColoredLogQueueType: TypeAlias = TwoColoredQueue[LogQueueRecordType]
TwoColoredAsyncLogQueueType: TypeAlias = TwoColoredAsyncQueue[LogQueueRecordType]
TwoColoredSyncLogQueueType: TypeAlias = TwoColoredSyncQueue[LogQueueRecordType]

WorkQueueRecordType: TypeAlias = MessageFetchFile
WorkQueueType: TypeAlias = asyncio.Queue[WorkQueueRecordType]


class AsyncDatasetDownloader:
    DEFAULT_NUM_WORKERS: ClassVar[int | Literal["auto"]] = 10

    def __init__(
        self,
        identifier: str,
        *,
        partition_key_after: str | None = None,
        partition_key_before: str | None = None,
        skip_existing: bool = True,
        num_workers: int | Literal["auto"] | None = None,
    ):
        self.identifier = identifier
        self.partition_key_after = partition_key_after
        self.partition_key_before = partition_key_before
        self.skip_existing = skip_existing

        self._num_workers = (
            self.DEFAULT_NUM_WORKERS if num_workers is None else num_workers
        )

    @property
    def context(self) -> MainContext:
        return main_context

    @property
    def base_url(self) -> str:
        identifier = self.identifier
        if "api.deweydata.io" in identifier:
            return identifier.removesuffix("/")
        return f"/v1/external/data/{identifier}"

    @property
    def num_workers(self) -> int:
        if self._num_workers == "auto":
            return 10
        return self._num_workers

    @async_cached_property
    async def metadata(self) -> GetMetadataDict:
        response = await async_api_request("GET", f"{self.base_url}/metadata")
        return cast(GetMetadataDict, response.json())

    @async_cached_property
    async def description(self) -> DescribedDatasetDict:
        response = await async_api_request("GET", f"{self.base_url}/description")
        return cast(DescribedDatasetDict, response.json())

    @async_cached_property
    async def sub_folder_path_str(self) -> str:
        # NOTE/TODO: Backend should send Dataset slug.
        return "dataset-slug"

    async def download_all(self):
        log_queue: TwoColoredLogQueueType = TwoColoredQueue()
        logging_async_queue: TwoColoredAsyncLogQueueType = log_queue.async_q
        logging_sync_queue: TwoColoredSyncLogQueueType = log_queue.sync_q
        async_work_queue: WorkQueueType = asyncio.Queue()
        overall_queue_key = "overall"

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

        loop = asyncio.get_running_loop()
        if loop is None:
            raise RuntimeError("Expecting a running/working event loop at this point.")

        worker_numbers: tuple[int, ...] = tuple(range(1, self.num_workers + 1))
        page_fetch_counter: dict[int, list[int]] = {}
        worker_busy_events: dict[int, asyncio.Event] = {
            worker_number: asyncio.Event() for worker_number in worker_numbers
        }
        all_pages_fetched_event = asyncio.Event()
        queue_done_event = asyncio.Event()

        def do_logging_work():
            return self._do_logging_work(
                queue=logging_sync_queue,
                progress=progress,
                overall_queue_key=overall_queue_key,
            )

        async with asyncio.TaskGroup() as tg, make_async_client() as client:
            logging_work_coroutine = asyncio.to_thread(do_logging_work)
            tg.create_task(logging_work_coroutine)

            tg.create_task(
                self._download_all(
                    client=client,
                    log_queue=logging_async_queue,
                    work_queue=async_work_queue,
                    overall_queue_key=overall_queue_key,
                    page_fetch_counter=page_fetch_counter,
                    worker_busy_events=worker_busy_events,
                    all_pages_fetched_event=all_pages_fetched_event,
                )
            )

            for worker_number in worker_numbers:
                tg.create_task(
                    self._do_async_work(
                        worker_number=worker_number,
                        client=client,
                        log_queue=logging_async_queue,
                        work_queue=async_work_queue,
                        overall_queue_key=overall_queue_key,
                        page_fetch_counter=page_fetch_counter,
                        busy_event=worker_busy_events[worker_number],
                        queue_done_event=queue_done_event,
                    )
                )

            tg.create_task(
                self._ensure_all_async_work_done_and_queue_empty(
                    log_queue=logging_async_queue,
                    work_queue=async_work_queue,
                    overall_queue_key=overall_queue_key,
                    worker_busy_events=worker_busy_events,
                    all_pages_fetched_event=all_pages_fetched_event,
                    queue_done_event=queue_done_event,
                )
            )

    async def _download_all(
        self,
        *,
        client: httpx.AsyncClient,
        log_queue: TwoColoredAsyncLogQueueType,
        work_queue: WorkQueueType,
        overall_queue_key: str,
        page_fetch_counter: dict[int, list[int]],
        worker_busy_events: dict[int, asyncio.Event],
        all_pages_fetched_event: asyncio.Event,
    ):
        Log = MessageLog
        AddProgress = MessageProgressAddTask

        metadata = await self.metadata
        await log_queue.put(Log(f"Metadata: {metadata}"))

        partition_column = metadata["partition_column"]
        await log_queue.put(Log(f"Partition column: {partition_column}"))

        partition_aggregation = metadata["partition_aggregation"]
        await log_queue.put(Log(f"Partition aggregation: {partition_aggregation}"))

        await log_queue.put(Log(f"API Key: {main_context.api_key_repr_preview}"))

        _root_path = self.context.download_directory
        sub_folder_path_str = await self.sub_folder_path_str
        _download_directory = _root_path / sub_folder_path_str
        download_directory = AsyncPath(_download_directory)
        if not await download_directory.exists():
            await log_queue.put(
                Log(f"Creating download directory {download_directory}...")
            )
            await download_directory.mkdir(parents=True)
            await log_queue.put(
                Log(f"Created download directory {download_directory}...")
            )

        await log_queue.put(Log(f"Downloading to {download_directory}..."))

        base_endpoint = (
            f"https://api.deweydata.io/api/v1/external/data/{self.identifier}"
        )
        await log_queue.put(Log(f"Base endpoint: {base_endpoint}"))

        partition_key_after = self.partition_key_after
        partition_key_before = self.partition_key_before
        query_params: dict[str, Any] = {}
        if partition_key_after:
            query_params["partition_key_after"] = partition_key_after
        if partition_key_before:
            query_params["partition_key_before"] = partition_key_before
        await log_queue.put(Log(f"Base query params: {query_params}"))

        skip_existing = self.skip_existing
        await log_queue.put(Log(f"Skip existing: {skip_existing}"))

        total_files = metadata["total_files"]
        await log_queue.put(Log(f"Total files: {total_files:,}"))

        total_size = metadata["total_size"]
        await log_queue.put(Log(f"Total size: {total_size:,}"))

        num_workers = self.num_workers
        await log_queue.put(Log(f"Using {num_workers:,} async workers for downloads"))

        current_page_number: int = 1
        current_overall_record_number: int = 1
        total_pages: int | None = None

        page_to_records_needing_fetch: dict[int, set[int]] = {}

        def has_more_pages_to_fetch() -> bool | None:
            if total_pages is None:
                return None
            if current_page_number > total_pages:
                return False
            return True

        def is_ready_to_fetch_next_page() -> bool | None:
            if total_pages is None:
                return None
            keys_set = set(page_fetch_counter.keys())
            if len(keys_set) <= 1:
                return True
            return False

        async def roll_up_page_fetch_counter():
            # The record numbers needed to be fetched for the page number (dict key).
            needing_fetch: dict[int, set[int]] = page_to_records_needing_fetch
            # The record numbers that have been fetched for the page number (dict key).
            fetched: dict[int, list[int]] = page_fetch_counter

            for page_fetched, record_nums_fetched in fetched.items():
                if page_fetched not in needing_fetch:
                    continue
                # For the record numbers that have been fetched, remove them from the
                # set of record numbers needed to be fetched.
                for record_num_fetched in record_nums_fetched:
                    needing_fetch[page_fetched].discard(record_num_fetched)

            for page_needing_fetch, record_nums_needing_fetch in needing_fetch.items():
                # If the set of record numbers needed to be fetched is empty, then the
                # page has been fully downloaded.
                if not record_nums_needing_fetch:
                    del needing_fetch[page_needing_fetch]
                    del fetched[page_needing_fetch]
                    await log_queue.put(
                        Log(f"Page {page_needing_fetch} marked as fully downloaded.")
                    )

        started_overall_progress: bool = False
        while True:
            await log_queue.put(Log(f"Fetching page {current_page_number}..."))

            next_query_params = query_params | {"page": current_page_number}
            # TODO (Dewey Team): Make sure this is wrapped with retries and exponential
            # backoff, etc.
            data_response = await async_api_request(
                "GET",
                f"{self.base_url}/files",
                params=next_query_params,
                timeout=httpx.Timeout(30.0),
                client=client,
            )
            response_data = data_response.json()
            batch = cast(GetFilesDict, response_data)

            total_pages = batch["total_pages"]
            assert isinstance(total_pages, int), "Pre-condition"

            if not started_overall_progress:
                await log_queue.put(
                    AddProgress(
                        key=overall_queue_key,
                        message="Overall Progress",
                        total=total_size,
                        filename="Overall",
                    )
                )
                started_overall_progress = True

            page_to_records_needing_fetch.setdefault(current_page_number, set())
            _current_overall_record_number = current_overall_record_number
            for __ in batch:
                page_to_records_needing_fetch[current_page_number].add(
                    _current_overall_record_number
                )
                _current_overall_record_number += 1
            del _current_overall_record_number

            page_fetch_counter[current_page_number] = []

            for raw_link_info in batch["download_links"]:
                link = raw_link_info["link"]
                original_file_name = raw_link_info["file_name"]
                file_size_bytes = raw_link_info["file_size_bytes"]
                new_file_name = original_file_name
                new_file_path = AsyncPath(_download_directory / new_file_name)

                await work_queue.put(
                    MessageFetchFile(
                        info=DownloadSingleFileInfo(
                            link=link,
                            original_file_name=original_file_name,
                            file_size_bytes=file_size_bytes,
                            new_file_path=new_file_path,
                            page_num=current_page_number,
                            record_num=current_overall_record_number,
                        )
                    )
                )

                current_overall_record_number += 1

            await asyncio.sleep(0.150)  # 150ms

            current_page_number += 1

            if not has_more_pages_to_fetch():
                break

            while True:
                await roll_up_page_fetch_counter()

                if not is_ready_to_fetch_next_page():
                    await asyncio.sleep(0.150)  # 150ms
                    continue

                break

        await log_queue.put(
            Log(f"[green]All pages {total_pages}/{total_pages} fetched[/green].")
        )

        all_pages_fetched_event.set()

    async def _download_single_file(
        self,
        *,
        client: httpx.AsyncClient,
        info: DownloadSingleFileInfo,
        log_queue: TwoColoredAsyncLogQueueType,
        overall_queue_key: str,
        page_fetch_counter: dict[int, list[int]],
    ) -> DownloadSingleFileResult:
        AddProgress = MessageProgressAddTask
        UpdateProgress = MessageProgressUpdateTask
        RemoveProgress = MessageProgressRemoveTask
        Log = MessageLog

        link = info.link
        original_file_name = info.original_file_name
        file_size_bytes = info.file_size_bytes
        new_file_path = info.new_file_path
        page_num = info.page_num
        record_num = info.record_num
        queue_key = f"p{page_num}-r{record_num}-{link}"

        does_new_file_path_already_exist = await new_file_path.exists()
        if does_new_file_path_already_exist and self.skip_existing:
            existing_file_stats = await new_file_path.stat()
            existing_file_stats = cast(stat_result, existing_file_stats)
            file_size_bytes_from_existing_file = existing_file_stats.st_size
            if file_size_bytes_from_existing_file != file_size_bytes:
                await log_queue.put(
                    Log(
                        f"[yellow](page_num={page_num}, record_num={record_num}) File size "
                        f"did not match for file (so re-downloading): "
                        f"{original_file_name}[/yellow]"
                    )
                )
                try:
                    await new_file_path.unlink()
                except FileNotFoundError:
                    pass
            else:
                page_fetch_counter[page_num].append(record_num)
                return DownloadSingleFileResult(
                    original_file_name=original_file_name,
                    new_file_name=original_file_name,
                    new_file_path=new_file_path,
                    did_skip=True,
                )

        await log_queue.put(
            AddProgress(
                key=queue_key,
                message=f"Downloading {original_file_name}",
                total=file_size_bytes,
                filename=original_file_name,
            )
        )

        file_amount_advanced_here: int = 0
        total_amount_advanced_here: int = 0

        try:
            # TODO (Dewey Team): Make sure this is wrapped with retries and exponential
            # backoff, etc.
            async with (
                client.stream(
                    "GET",
                    link,
                    follow_redirects=True,
                    timeout=httpx.Timeout(180.0),
                ) as r,
                aiofiles.open(new_file_path, "wb") as f,
            ):
                async for raw_bytes in r.aiter_bytes():
                    chunk_size = len(raw_bytes)
                    await f.write(raw_bytes)
                    await log_queue.put(
                        UpdateProgress(key=queue_key, advance=chunk_size)
                    )
                    file_amount_advanced_here += chunk_size
                    await log_queue.put(
                        UpdateProgress(key=overall_queue_key, advance=chunk_size)
                    )
                    total_amount_advanced_here += chunk_size
        except Exception as e:
            await log_queue.put(
                Log(f"[red]Error downloading {original_file_name}: {e}[/red]")
            )
            await log_queue.put(
                UpdateProgress(
                    key=overall_queue_key, advance=-1 * file_amount_advanced_here
                )
            )
            await log_queue.put(
                UpdateProgress(
                    key=overall_queue_key, advance=-1 * total_amount_advanced_here
                )
            )
            # TODO (Dewey Team): Make sure other stuff is wrapped with retries and
            # exponential backoff, etc.
            raise RuntimeError(f"Error downloading {original_file_name}: {e}") from e
        else:
            page_fetch_counter[page_num].append(record_num)
        finally:
            await log_queue.put(RemoveProgress(key=queue_key))

        return DownloadSingleFileResult(
            original_file_name=original_file_name,
            new_file_name=original_file_name,
            new_file_path=new_file_path,
            did_skip=False,
        )

    async def _do_async_work(
        self,
        *,
        worker_number: int,
        client: httpx.AsyncClient,
        log_queue: TwoColoredAsyncLogQueueType,
        work_queue: WorkQueueType,
        overall_queue_key: str,
        page_fetch_counter: dict[int, list[int]],
        busy_event: asyncio.Event,
        queue_done_event: asyncio.Event,
    ) -> None:
        # This says that the worker is busy.
        busy_event.clear()

        Log = MessageLog

        consecutive_empty_entries: int = 0

        await log_queue.put(Log(f"Async worker number {worker_number} started."))

        while True:
            entry = None
            try:
                entry = work_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            else:
                # This says that the worker is busy (it has an entry to process).
                busy_event.clear()

            if entry is None:
                consecutive_empty_entries += 1

                if consecutive_empty_entries >= 3:
                    # This says that the worker is not busy (it has no entries to
                    # process and hasn't for at least 2-3 sleeping iterations).
                    busy_event.set()
                    # If the `queue_done_event` is set, then we're done, so we can break
                    # out of the loop.
                    if queue_done_event.is_set():
                        break

                # Sleep for ~15ms.
                await asyncio.sleep(0.015)  # 15ms

                # Go back to the start of the loop and try to grab the next entry.
                continue
            else:
                consecutive_empty_entries = 0

            match entry:
                case MessageFetchFile(info=info):
                    try:
                        await self._download_single_file(
                            client=client,
                            info=info,
                            log_queue=log_queue,
                            overall_queue_key=overall_queue_key,
                            page_fetch_counter=page_fetch_counter,
                        )
                    finally:
                        work_queue.task_done()
                case _:
                    try:
                        await log_queue.put(  # type: ignore[unreachable]
                            Log(f"[red]Unexpected message: {entry}[/red]")
                        )
                    finally:
                        work_queue.task_done()

        await log_queue.put(
            Log(f"[green]Async worker number {worker_number} done.[/green]")
        )

    async def _ensure_all_async_work_done_and_queue_empty(
        self,
        *,
        log_queue: TwoColoredAsyncLogQueueType,
        work_queue: WorkQueueType,
        overall_queue_key: str,
        worker_busy_events: dict[int, asyncio.Event],
        all_pages_fetched_event: asyncio.Event,
        queue_done_event: asyncio.Event,
        ensure_queue_empty_for_at_least_seconds: float = 3.0,
    ) -> None:
        # First, wait for all the pages to be fetched.
        await all_pages_fetched_event.wait()

        # Then, wait for all of the workers to be done.
        for worker_busy_event in worker_busy_events.values():
            await worker_busy_event.wait()

        # Finally, we'll wait for the queue to be empty for at least
        # `ensure_queue_empty_for_at_least_seconds`.
        while True:
            # Wait for the work queue to be empty.
            await work_queue.join()

            # Wait half the `ensure_queue_empty_for_at_least_seconds` time.
            elapsed: float = 0.0
            await asyncio.sleep(ensure_queue_empty_for_at_least_seconds / 2)
            elapsed += ensure_queue_empty_for_at_least_seconds / 2
            # If the queue is empty, then we'll keep going.
            if work_queue.empty():
                pass
            else:
                # Otherwise, we go back to the start of the loop.
                continue

            # Wait the remaining half of the `ensure_queue_empty_for_at_least_seconds`
            # time.
            await asyncio.sleep(ensure_queue_empty_for_at_least_seconds / 2)
            elapsed += ensure_queue_empty_for_at_least_seconds / 2
            # If the queue is empty, then we're done.
            if work_queue.empty():
                break
            else:
                # Otherwise, we go back to the start of the loop.
                continue

        # Finally, we double check at the very end that all of the workers are done.
        # There are possible edge cases with the queue being empty but workers having
        # picked up stuff in the meantime, etc.
        for final_worker_busy_event in worker_busy_events.values():
            await final_worker_busy_event.wait()
        # We'll also go through all of the busy events twice as a defensive/safety
        # measure to make sure all the workers really are done.
        for final_worker_busy_event in worker_busy_events.values():
            await final_worker_busy_event.wait()

        # Set the `queue_done_event` to signal that the queue is done.
        queue_done_event.set()

        # Sleep ~15ms.
        await asyncio.sleep(0.015)  # 15ms

        # Do one more double pass of worker busy events before sending
        # `MessageWorkDone(...)`.
        for final_worker_busy_event in worker_busy_events.values():
            await final_worker_busy_event.wait()
        # Sleep ~3ms.
        await asyncio.sleep(0.003)  # 3ms
        for final_worker_busy_event in worker_busy_events.values():
            await final_worker_busy_event.wait()

        # Remove the overall progress bar.
        await log_queue.put(MessageProgressRemoveTask(key=overall_queue_key))

        # Put the `MessageWorkDone(...)` message into the queue to tell the logging
        # thread (and any other threads down the line if needed) that the work is done.
        await log_queue.put(MessageWorkDone())

        # Finally, wait for the logging queue to be empty.
        await log_queue.join()

    def _do_logging_work(
        self,
        *,
        queue: TwoColoredSyncLogQueueType,
        progress: Progress,
        overall_queue_key: str,
    ) -> None:
        is_work_done: bool = False

        key_to_task_id: dict[str, TaskID] = {}
        overall_task_id: TaskID | None = None

        while True:
            try:
                next_entry = queue.get(timeout=1.5)  # 1.5s
            except QueueEmpty:
                if is_work_done:
                    break
                else:
                    continue

            match next_entry:
                case MessageProgressAddTask(
                    key=key, message=message, total=total, filename=filename
                ):
                    if key == overall_queue_key:
                        overall_task_id = progress.add_task(
                            message, total=total, filename=filename
                        )
                    else:
                        key_to_task_id[key] = progress.add_task(
                            message, total=total, filename=filename
                        )
                case MessageProgressUpdateTask(key=key, advance=advance):
                    if key == overall_queue_key:
                        if overall_task_id is not None:
                            progress.update(overall_task_id, advance=advance)
                    else:
                        progress.update(key_to_task_id[key], advance=advance)
                case MessageProgressRemoveTask(key=key):
                    if key == overall_queue_key:
                        if overall_task_id is not None:
                            progress.remove_task(overall_task_id)
                        overall_task_id = None
                    else:
                        progress.remove_task(key_to_task_id[key])
                        key_to_task_id.pop(key, None)
                case MessageLog(rprint=rprint_value):
                    rprint(rprint_value)
                case MessageWorkDone():
                    is_work_done = True
                case MessageFetchFile():
                    raise RuntimeError("Not expecting this message type here.")
                case _:
                    rprint(f"[red]Unexpected message: {next_entry}[/red]")  # type: ignore[unreachable]
