from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
from tenacity import (
    AsyncRetrying,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

if TYPE_CHECKING:
    from tenacity import RetryCallState


# Exceptions that indicate a transient/retriable failure during downloads.
# These are network-level or protocol-level errors where retrying may succeed.
RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    # httpx exceptions for connection and protocol issues.
    httpx.RemoteProtocolError,  # Includes ContentLengthError
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.PoolTimeout,
    # Also catch generic network errors that might bubble up.
    ConnectionError,
    TimeoutError,
)

RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429})
RETRYABLE_STATUS_CODE_MIN: int = 500


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of attempts (including the initial attempt).
            Default is 10.
        base_delay_seconds: Base delay for exponential backoff. Default is 1.0.
        max_delay_seconds: Maximum delay between retries. Default is 120.0.
        retryable_exceptions: Tuple of exception types that should trigger a retry.
    """

    max_attempts: int = 10
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 120.0
    retryable_exceptions: tuple[type[BaseException], ...] = RETRYABLE_EXCEPTIONS

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            object.__setattr__(self, "max_attempts", 1)


# Default retry configuration.
DEFAULT_RETRY_CONFIG = RetryConfig()


def is_retryable_status_code(status_code: int) -> bool:
    return (
        status_code in RETRYABLE_STATUS_CODES
        or status_code >= RETRYABLE_STATUS_CODE_MIN
    )


def _is_retryable_exception(
    exception: BaseException,
    config: RetryConfig,
) -> bool:
    if isinstance(exception, httpx.HTTPStatusError):
        return is_retryable_status_code(exception.response.status_code)
    return isinstance(exception, config.retryable_exceptions)


def get_retry_info(retry_state: RetryCallState) -> tuple[int, BaseException | None]:
    """Extract attempt number and exception from retry state.

    Args:
        retry_state: The tenacity RetryCallState object.

    Returns:
        A tuple of (attempt_number, exception_or_none).
    """
    attempt = retry_state.attempt_number
    exception = None
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
    return (attempt, exception)


def sync_retrying(config: RetryConfig | None = None) -> Retrying:
    """Create a synchronous Retrying context manager.

    Use this in a for loop to retry a block of code with exponential backoff:

        for attempt in sync_retrying(config):
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    # Log retry attempt...
                # Your code that might fail...

    Args:
        config: Retry configuration. Uses DEFAULT_RETRY_CONFIG if not provided.

    Returns:
        A tenacity Retrying instance configured for sync use.
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    return Retrying(
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_random_exponential(
            multiplier=config.base_delay_seconds,
            exp_base=2,
            max=config.max_delay_seconds,
        ),
        retry=retry_if_exception(lambda exc: _is_retryable_exception(exc, config)),
        reraise=True,
    )


def async_retrying(config: RetryConfig | None = None) -> AsyncRetrying:
    """Create an asynchronous Retrying context manager.

    Use this in an async for loop to retry a block of code with exponential backoff:

        async for attempt in async_retrying(config):
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    # Log retry attempt...
                # Your async code that might fail...

    Args:
        config: Retry configuration. Uses DEFAULT_RETRY_CONFIG if not provided.

    Returns:
        A tenacity AsyncRetrying instance configured for async use.
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    return AsyncRetrying(
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_random_exponential(
            multiplier=config.base_delay_seconds,
            exp_base=2,
            max=config.max_delay_seconds,
        ),
        retry=retry_if_exception(lambda exc: _is_retryable_exception(exc, config)),
        reraise=True,
    )


def format_retry_message(
    *,
    attempt: int,
    max_attempts: int,
    file_name: str,
    exception: BaseException | None,
    page_num: int | None = None,
    record_num: int | None = None,
) -> str:
    """Format a user-friendly retry message for display.

    Args:
        attempt: Current attempt number (1-indexed).
        max_attempts: Maximum number of attempts.
        file_name: Name of the file being downloaded.
        exception: The exception that caused the retry, if any.
        page_num: Optional page number for context.
        record_num: Optional record number for context.

    Returns:
        A formatted string suitable for rich console output.
    """
    # Build the location context if available.
    location = ""
    if page_num is not None and record_num is not None:
        location = f"(page_num={page_num}, record_num={record_num}) "

    # Get a short exception description.
    error_desc = ""
    if exception is not None:
        exc_name = type(exception).__name__
        exc_msg = str(exception)
        # Truncate long exception messages.
        if len(exc_msg) > 100:
            exc_msg = exc_msg[:97] + "..."
        error_desc = f": {exc_name}"
        if exc_msg:
            error_desc = f": {exc_name} - {exc_msg}"

    return (
        f"[yellow]{location}Retry {attempt}/{max_attempts} for "
        f"{file_name}{error_desc}[/yellow]"
    )
