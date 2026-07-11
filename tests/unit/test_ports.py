"""Unit tests for ``web/ports.py`` — port-finder helper."""
from __future__ import annotations

import socket

import pytest

from web.ports import find_free_port


def _claim_a_port() -> int:
    """Bind briefly so we have a known-busy port to test against."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestFindFreePort:
    def test_returns_int_in_range(self):
        port = find_free_port(start=10000, end=11000)
        assert isinstance(port, int)
        assert 10000 <= port <= 11000

    def test_default_range_starts_at_5050(self):
        # When no explicit range is given, prefer 5050+ — 5000 is famously
        # taken by macOS Control Center.
        port = find_free_port()
        assert port >= 5050

    def test_skips_avoided_ports(self):
        # Pick a fresh port first; pretend we want a second one *not* equal to it.
        first = find_free_port(start=12000, end=12500)
        # Bind the first so it's busy AND in the avoid set; the finder must skip it.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", first))
            s.listen()
            try:
                second = find_free_port(avoid={first}, start=12000, end=12500)
                assert second != first
            finally:
                s.close()

    def test_raises_when_range_exhausted(self):
        # Find two free ports, bind them, then ask the finder for a port in
        # a window that contains only those two — every iteration must fail.
        a = find_free_port(start=20000, end=29000)
        b = find_free_port(avoid={a}, start=20000, end=29000)
        if a > b:
            a, b = b, a
        busy_socks = []
        try:
            for p in (a, b):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(("127.0.0.1", p))
                s.listen()
                busy_socks.append(s)
            with pytest.raises(RuntimeError) as exc:
                find_free_port(start=a, end=b)
            assert "No free TCP port" in str(exc.value)
        finally:
            for s in busy_socks:
                s.close()

    def test_works_when_avoid_is_empty(self):
        port = find_free_port(avoid=(), start=13000, end=13100)
        assert 13000 <= port <= 13100