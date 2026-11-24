from __future__ import annotations

import re
from importlib.metadata import version

# Used when the installed package metadata is missing or unreadable (for example,
# `deweypy` imported straight from a source checkout on `sys.path`).
UNKNOWN_VERSION = "unknown"

# Dewey's backend and download proxy read the client version from the `User-Agent`
# with this character set and length. Older backends also require the whole
# `User-Agent` to be exactly `deweypy/<version>`, so the version sent on the wire
# must fit this pattern on its own.
_WIRE_VERSION_RE = re.compile(r"[0-9A-Za-z.\-_]{1,15}")


def get_deweypy_version() -> str:
    """Return the installed `deweypy` version, or `UNKNOWN_VERSION`.

    This runs at import time, so it never raises: missing, unreadable or malformed
    package metadata all fall back to `UNKNOWN_VERSION`.
    """
    try:
        installed_version = version("deweypy")
    except Exception:
        return UNKNOWN_VERSION
    if not isinstance(installed_version, str) or not installed_version:
        return UNKNOWN_VERSION
    return installed_version


def get_wire_version(deweypy_version: str) -> str:
    """Return the version to send in the `User-Agent`.

    The local version label (`+...`) is dropped. Anything that still doesn't fit
    `_WIRE_VERSION_RE` (e.g. an epoch, or a long dev version) is sent as
    `UNKNOWN_VERSION` rather than being cut short or rejected by the server.
    """
    public_version = deweypy_version.split("+", 1)[0]
    if _WIRE_VERSION_RE.fullmatch(public_version):
        return public_version
    return UNKNOWN_VERSION


def build_user_agent(deweypy_version: str) -> str:
    """Return the `User-Agent` header value sent on Dewey requests.

    Keep this as the bare `deweypy/<version>` form. Older Dewey backends only read
    the version when the whole `User-Agent` is exactly that form.
    """
    return f"deweypy/{get_wire_version(deweypy_version)}"


__version__ = get_deweypy_version()
USER_AGENT = build_user_agent(__version__)
