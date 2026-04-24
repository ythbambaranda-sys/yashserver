from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yserver.server as yserver_server


class _FakeTransport:
    def __init__(self) -> None:
        self.abort_called = False

    def abort(self) -> None:
        self.abort_called = True


class _HangingWriter:
    def __init__(self) -> None:
        self.closed = False
        self.transport = _FakeTransport()
        self._never = asyncio.Future()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        await self._never


class _FastWriter:
    def __init__(self) -> None:
        self.closed = False
        self.transport = _FakeTransport()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class TestShutdownBehavior(unittest.IsolatedAsyncioTestCase):
    async def test_close_writer_quietly_aborts_when_wait_hangs(self) -> None:
        writer = _HangingWriter()
        await asyncio.wait_for(yserver_server._close_writer_quietly(writer, timeout_seconds=0.1), timeout=0.4)
        self.assertTrue(writer.closed)
        self.assertTrue(writer.transport.abort_called)

    async def test_close_writer_quietly_no_abort_when_clean(self) -> None:
        writer = _FastWriter()
        await asyncio.wait_for(yserver_server._close_writer_quietly(writer, timeout_seconds=0.1), timeout=0.4)
        self.assertTrue(writer.closed)
        self.assertFalse(writer.transport.abort_called)


if __name__ == "__main__":
    unittest.main()
