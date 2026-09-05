from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver.sync as ysync


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


class TestSyncServerSettings(unittest.TestCase):
    def test_setddosprot_available_on_sync_servers(self) -> None:
        tcp = ysync.YSyncServer()
        http = ysync.YSyncHttpServer(ddosprot=False)
        ws = ysync.YSyncWebSocketServer(ddosprot=False)

        self.assertTrue(tcp.async_server.ddosprot)
        self.assertFalse(http.async_server.ddosprot)
        self.assertFalse(ws.async_server.ddosprot)

        self.assertIs(tcp.setddosprot(4, rate_limit_window_seconds=1.0), tcp)
        self.assertIs(http.setddosprot(True), http)
        self.assertIs(ws.setddosprot(True), ws)

        self.assertTrue(tcp.async_server.ddosprot)
        self.assertEqual(tcp.async_server._rate_limiter.limit, 4)
        self.assertEqual(tcp.async_server._rate_limiter.window_seconds, 1.0)
        self.assertTrue(http.async_server.ddosprot)
        self.assertTrue(ws.async_server.ddosprot)


if __name__ == "__main__":
    unittest.main()
