from __future__ import annotations

import asyncio
import json
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver.core import ServerState  # noqa: E402


async def _open(port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection("127.0.0.1", port)


async def _close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


class TestTcpRouting(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)

        @self.server.route("ping")
        async def ping(_conn, payload, _srv):
            return {"pong": payload}

        @self.server.route("boom")
        async def boom(_conn, _payload, _srv):
            raise RuntimeError("handler exploded")

        await self.server.start()
        self.port = self.server.bound_port

    async def asyncTearDown(self) -> None:
        await self.server.stop()

    async def test_route_round_trip(self) -> None:
        reader, writer = await _open(self.port)
        writer.write(b"ping hello\n")
        await writer.drain()
        self.assertEqual(json.loads(await reader.readline()), {"pong": "hello"})
        await _close(writer)

    async def test_unknown_command_is_reported(self) -> None:
        reader, writer = await _open(self.port)
        writer.write(b"nope x\n")
        await writer.drain()
        self.assertEqual(json.loads(await reader.readline())["error"], "unknown-command")
        await _close(writer)

    async def test_handler_error_does_not_kill_the_connection(self) -> None:
        reader, writer = await _open(self.port)
        writer.write(b"boom\n")
        await writer.drain()
        self.assertEqual(json.loads(await reader.readline())["error"], "handler-failed")

        writer.write(b"ping still-alive\n")
        await writer.drain()
        self.assertEqual(json.loads(await reader.readline()), {"pong": "still-alive"})
        await _close(writer)

    async def test_client_registry_tracks_connections(self) -> None:
        reader, writer = await _open(self.port)
        writer.write(b"ping x\n")
        await writer.drain()
        await reader.readline()
        self.assertEqual(len(self.server.clients), 1)
        self.assertEqual(self.server.use_tool("client_count"), 1)

        await _close(writer)
        for _ in range(50):
            if not self.server.clients:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(len(self.server.clients), 0)

    async def test_broadcast_reaches_every_peer_and_can_exclude(self) -> None:
        peers = [await _open(self.port) for _ in range(3)]
        for _, writer in peers:
            writer.write(b"ping x\n")
            await writer.drain()
        for reader, _ in peers:
            await reader.readline()

        delivered = await self.server.broadcast({"note": "all"})
        self.assertEqual(delivered, 3)
        for reader, _ in peers:
            self.assertEqual(json.loads(await reader.readline()), {"note": "all"})

        skipped = next(iter(self.server.clients))
        delivered = await self.server.broadcast({"note": "some"}, exclude=skipped)
        self.assertEqual(delivered, 2)

        for _, writer in peers:
            await _close(writer)

    async def test_metrics_are_recorded(self) -> None:
        reader, writer = await _open(self.port)
        writer.write(b"ping x\n")
        await writer.drain()
        await reader.readline()
        counters = self.server.metrics.snapshot()["counters"]
        self.assertGreaterEqual(counters["messages_received"], 1)
        self.assertGreaterEqual(counters["connections_opened"], 1)
        await _close(writer)


class TestTcpProtections(unittest.IsolatedAsyncioTestCase):
    async def test_oversized_message_is_rejected_not_buffered(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False, max_line_bytes=128)

        @server.route("x")
        async def handler(_conn, payload, _srv):
            return {"len": len(payload)}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            writer.write(b"x " + b"z" * 4096 + b"\n")
            await writer.drain()
            self.assertIn("message-too-large", (await reader.readline()).decode())
            await _close(writer)
        finally:
            await server.stop()

    async def test_rate_limit_blocks_excess_messages(self) -> None:
        server = yashserver.YTcpServer(
            host="127.0.0.1",
            port=0,
            rate_limit_per_window=2,
            rate_limit_window_seconds=60.0,
        )

        @server.route("ping")
        async def ping(_conn, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            for _ in range(3):
                writer.write(b"ping\n")
            await writer.drain()

            self.assertEqual(json.loads(await reader.readline()), {"ok": True})
            self.assertEqual(json.loads(await reader.readline()), {"ok": True})
            third = json.loads(await reader.readline())
            self.assertEqual(third["error"], "rate-limit exceeded")
            await _close(writer)
        finally:
            await server.stop()

    async def test_max_connections_refuses_extra_peers(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False, max_connections=1)

        @server.route("ping")
        async def ping(_conn, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            writer.write(b"ping\n")
            await writer.drain()
            await reader.readline()

            reader2, writer2 = await _open(server.bound_port)
            # The second peer is accepted by the OS then dropped immediately.
            self.assertEqual(await reader2.read(), b"")
            self.assertEqual(server.metrics.counter("connections_refused"), 1)
            await _close(writer)
            await _close(writer2)
        finally:
            await server.stop()

    async def test_idle_timeout_closes_silent_peers(self) -> None:
        server = yashserver.YTcpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            idle_timeout_seconds=0.15,
        )

        @server.route("ping")
        async def ping(_conn, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            self.assertEqual(json.loads(line)["error"], "idle-timeout")
            self.assertEqual(server.metrics.counter("idle_timeouts"), 1)
            await _close(writer)
        finally:
            await server.stop()


class TestTcpRawProtocol(unittest.IsolatedAsyncioTestCase):
    async def test_custom_binary_protocol(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)

        @server.on_connection
        async def handle(conn, _srv):
            size = struct.unpack("!I", await conn.readexactly(4))[0]
            body = await conn.readexactly(size)
            await conn.send_bytes(struct.pack("!I", size) + body[::-1])

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            writer.write(struct.pack("!I", 5) + b"abcde")
            await writer.drain()
            self.assertEqual(struct.unpack("!I", await reader.readexactly(4))[0], 5)
            self.assertEqual(await reader.readexactly(5), b"edcba")
            await _close(writer)
        finally:
            await server.stop()

    async def test_stream_iterates_until_peer_closes(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)
        received: list[int] = []

        @server.on_connection
        async def handle(conn, _srv):
            total = 0
            async for chunk in conn.stream(chunk_size=16):
                total += len(chunk)
            received.append(total)

        await server.start()
        try:
            _reader, writer = await _open(server.bound_port)
            writer.write(b"a" * 100)
            await writer.drain()
            await _close(writer)
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
            self.assertEqual(received, [100])
        finally:
            await server.stop()


class TestTcpLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_state_transitions_and_idempotence(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0)
        self.assertIs(server.state, ServerState.STOPPED)

        await server.start()
        self.assertIs(server.state, ServerState.RUNNING)
        self.assertTrue(server.is_running)
        await server.start()  # second start is a no-op
        self.assertIs(server.state, ServerState.RUNNING)

        await server.stop()
        self.assertIs(server.state, ServerState.STOPPED)
        await server.stop()  # second stop is a no-op
        self.assertIs(server.state, ServerState.STOPPED)

    async def test_async_context_manager(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0)
        async with server:
            self.assertTrue(server.is_running)
            self.assertIsNotNone(server.bound_port)
        self.assertIs(server.state, ServerState.STOPPED)

    async def test_graceful_shutdown_closes_peers(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)

        @server.route("ping")
        async def ping(_conn, _payload, _srv):
            return {"ok": True}

        await server.start()
        reader, writer = await _open(server.bound_port)
        writer.write(b"ping\n")
        await writer.drain()
        await reader.readline()

        await asyncio.wait_for(server.stop(timeout=1.0), timeout=5.0)
        self.assertEqual(server.clients, {})
        self.assertEqual(await reader.read(), b"")
        await _close(writer)

    async def test_run_stops_cleanly_when_cancelled(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0)
        task = asyncio.create_task(server.run())
        for _ in range(50):
            if server.is_running:
                break
            await asyncio.sleep(0.02)
        self.assertTrue(server.is_running)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertIs(server.state, ServerState.STOPPED)

    async def test_every_runs_periodic_work_and_stops_with_the_server(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0)
        ticks: list[int] = []

        await server.start()
        server.every(0.02, lambda: ticks.append(1))
        await asyncio.sleep(0.12)
        await server.stop()

        self.assertGreater(len(ticks), 1)
        before = len(ticks)
        await asyncio.sleep(0.08)
        self.assertEqual(len(ticks), before)


class TestTcpPlugins(unittest.IsolatedAsyncioTestCase):
    async def test_plugin_can_transform_and_drop_messages(self) -> None:
        class Rewriter(yashserver.ServerPlugin):
            name = "rewriter"

            async def on_tcp_message(self, _client, message, _server):
                if message.startswith("drop"):
                    return None
                return message.replace("SHOUT", "ping")

        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)
        server.add_plugin(Rewriter())

        @server.route("ping")
        async def ping(_conn, payload, _srv):
            return {"pong": payload}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            writer.write(b"drop me\nSHOUT hey\n")
            await writer.drain()
            # The dropped line produces no reply, so the next reply is the ping.
            self.assertEqual(json.loads(await reader.readline()), {"pong": "hey"})
            await _close(writer)
        finally:
            await server.stop()

    async def test_connection_stats_plugin_counts_traffic(self) -> None:
        stats = yashserver.ConnectionStatsPlugin()
        server = yashserver.YTcpServer(host="127.0.0.1", port=0, ddosprot=False)
        server.add_plugin(stats)

        @server.route("ping")
        async def ping(_conn, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            reader, writer = await _open(server.bound_port)
            writer.write(b"ping\n")
            await writer.drain()
            await reader.readline()
            snapshot = stats.snapshot()
            self.assertEqual(snapshot["tcp_connections_opened"], 1)
            self.assertEqual(snapshot["tcp_messages"], 1)
            await _close(writer)
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
