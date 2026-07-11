"""Find a free TCP port on localhost.

Wrapping this in a tiny module (a) keeps the logic out of ``main.py``
where it would be hard to test, and (b) lets tests reuse the helper to
find unused ports for upstream Flask fixtures in the proxy tests.

Why a fresh socket each call?  We bind to port 0 (the OS picks one),
close the socket, and return the port.  There's an inherent race —
the port could be taken between our ``close()`` and the next bind —
but for a dev server it's good enough.  If the caller hits
``OSError`` they retry or surface the error.
"""
from __future__ import annotations

import socket
from typing import Iterable


def find_free_port(
    *,
    host: str = "127.0.0.1",
    avoid: Iterable[int] = (),
    start: int = 5050,
    end: int = 65535,
) -> int:
    """Return the lowest TCP port on ``host`` in ``[start, end]`` that is free.

    Skips any port in ``avoid`` (useful when you've already claimed one
    and want a second one distinct from the first).

    Raises:
        RuntimeError: if no port in the range is free.
    """
    avoid_set = set(avoid)
    for port in range(start, end + 1):
        if port in avoid_set:
            continue
        if _is_free(host, port):
            return port
    raise RuntimeError(
        f"No free TCP port on {host} in [{start}, {end}] (avoided={sorted(avoid_set)})"
    )


def _is_free(host: str, port: int) -> bool:
    """True iff a TCP listener can bind ``(host, port)`` right now.

    We bind briefly and immediately close; this is the standard
    "find a free port" trick.  We deliberately do NOT set ``SO_REUSEADDR``
    — that flag would let us bind to a port another process already owns,
    so a probe with REUSEADDR would return True even when the port is
    in use by a long-lived listener (the case we care about).
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
        except OSError:
            return False
        return True