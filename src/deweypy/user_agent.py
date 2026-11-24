from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

# Used when `deweypy` is imported without installed package metadata (for example,
# straight from a source checkout on `sys.path`). Dewey's backend and download proxy
# parse the version from the `User-Agent`, and this placeholder fits their pattern.
UNKNOWN_VERSION = "unknown"


def get_deweypy_version() -> str:
    """Return the installed `deweypy` version, or `UNKNOWN_VERSION` without metadata."""
    try:
        return version("deweypy")
    except PackageNotFoundError:
        return UNKNOWN_VERSION


def build_user_agent(deweypy_version: str | None = None) -> str:
    """Return the `User-Agent` header value sent on every Dewey API request.

    Keep this as the bare `deweypy/<version>` form. Older Dewey backends only read
    the version when the whole `User-Agent` is exactly that form.
    """
    return f"deweypy/{deweypy_version or get_deweypy_version()}"


__version__ = get_deweypy_version()
USER_AGENT = build_user_agent(__version__)
