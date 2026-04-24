from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yserver.sync as ysync


class _HangingServer:
    def __init__(self) -> None:
        self._server = object()
        self.stop_calls = 0

    async def run(self) -> None:
        await asyncio.Future()

    async def stop(self) -> None:
        self.stop_calls += 1
        self._server = None


class _AlreadyClosedServer:
    def __init__(self) -> None:
        self._server = object()
        self.stop_calls = 0

    async def run(self) -> None:
        self._server = None
        return None

    async def stop(self) -> None:
        self.stop_calls += 1
        self._server = None


class TestSyncRunner(unittest.IsolatedAsyncioTestCase):
    async def test_run_servers_stops_on_cancellation(self) -> None:
        server = _HangingServer()
        task = asyncio.create_task(ysync._run_servers([server]))
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(server.stop_calls, 1)

    async def test_run_servers_does_not_double_stop_closed_server(self) -> None:
        server = _AlreadyClosedServer()
        await ysync._run_servers([server])
        self.assertEqual(server.stop_calls, 0)


if __name__ == "__main__":
    unittest.main()
