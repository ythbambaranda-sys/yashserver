"""Cross-protocol tests: shared abstractions and running servers together."""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver.core import BaseServer, ServerState  # noqa: E402
from yashserver.core import listener_is_dead as yserver_core_listener_is_dead  # noqa: E402


async def _request_ok(port: int) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET /ok HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=5.0)
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    return head.decode("latin-1"), body


def _all_servers() -> list[BaseServer]:
    return [
        yashserver.YTcpServer(host="127.0.0.1", port=0),
        yashserver.YUdpServer(host="127.0.0.1", port=0),
        yashserver.YHttpServer(host="127.0.0.1", port=0),
        yashserver.YWebSocketServer(host="127.0.0.1", port=0),
    ]



def _kill_listener(server: asyncio.AbstractServer) -> None:
    """Put the server into the state the guard exists to recover from.

    The guard's contract is "the listener is gone while we are still RUNNING,
    so rebind it". ``close()`` produces exactly that state, and it is the only
    portable way to produce it: closing the socket behind asyncio's back leaves
    the selector event loop holding a registration for a freed fd, so the
    rebound listener collides with the stale entry and Linux never recovers.
    That corruption is an artefact of how the test cheats, not something a real
    dead listener does, so the test must not induce it.
    """

    server.close()


class TestSharedAbstractions(unittest.IsolatedAsyncioTestCase):
    """Every transport gets the same lifecycle, plugin, tool and metric API."""

    def test_every_server_derives_from_the_same_base(self) -> None:
        for server in _all_servers():
            with self.subTest(protocol=server.protocol):
                self.assertIsInstance(server, BaseServer)

    def test_shared_surface_is_present_everywhere(self) -> None:
        shared = [
            "start",
            "stop",
            "run",
            "add_plugin",
            "register_tool",
            "use_tool",
            "setddosprot",
            "create_task",
            "every",
            "metrics",
            "config",
            "logger",
            "state",
        ]
        for server in _all_servers():
            for name in shared:
                with self.subTest(protocol=server.protocol, member=name):
                    self.assertTrue(hasattr(server, name))

    def test_protocol_specific_surface_stays_protocol_specific(self) -> None:
        tcp = yashserver.YTcpServer(host="127.0.0.1", port=0)
        udp = yashserver.YUdpServer(host="127.0.0.1", port=0)
        http = yashserver.YHttpServer(host="127.0.0.1", port=0)
        ws = yashserver.YWebSocketServer(host="127.0.0.1", port=0)

        # UDP is connectionless: it exposes endpoints, never a client registry
        # or per-connection operations.
        self.assertTrue(hasattr(udp, "known_endpoints"))
        self.assertFalse(hasattr(udp, "clients"))
        self.assertFalse(hasattr(udp, "disconnect"))

        # TCP has connections it can address and close.
        self.assertTrue(hasattr(tcp, "clients"))
        self.assertTrue(hasattr(tcp, "disconnect"))
        self.assertFalse(hasattr(tcp, "known_endpoints"))

        # HTTP speaks requests, routes and middleware.
        for name in ("get", "post", "middleware", "static", "mount_websocket"):
            self.assertTrue(hasattr(http, name), name)

        # WebSocket speaks messages, rooms and broadcasts.
        for name in ("join_room", "leave_room", "broadcast_to_room", "rooms", "send_stream"):
            self.assertTrue(hasattr(ws, name), name)
        self.assertFalse(hasattr(http, "join_room"))

    async def test_tools_and_metrics_work_uniformly(self) -> None:
        for server in _all_servers():
            with self.subTest(protocol=server.protocol):
                async with server:
                    self.assertGreaterEqual(server.use_tool("uptime_seconds"), 0.0)
                    self.assertEqual(server.use_tool("state"), "running")
                    self.assertIsInstance(server.use_tool("metrics"), dict)
                    self.assertIsInstance(server.use_tool("now"), str)

                    server.register_tool("double", lambda value: value * 2)
                    self.assertEqual(server.use_tool("double", 21), 42)
                    with self.assertRaises(KeyError):
                        server.use_tool("no-such-tool")

    async def test_lifecycle_hooks_fire_on_every_protocol(self) -> None:
        class Tracker(yashserver.ServerPlugin):
            name = "tracker"

            def __init__(self) -> None:
                self.events: list[str] = []

            async def on_startup(self, _server):
                self.events.append("startup")

            async def on_shutdown(self, _server):
                self.events.append("shutdown")

        for server in _all_servers():
            with self.subTest(protocol=server.protocol):
                tracker = Tracker()
                server.add_plugin(tracker)
                await server.start()
                await server.stop()
                self.assertEqual(tracker.events, ["startup", "shutdown"])

    async def test_setddosprot_behaves_the_same_everywhere(self) -> None:
        for server in _all_servers():
            with self.subTest(protocol=server.protocol):
                self.assertIs(server.setddosprot(False), server)
                self.assertFalse(server.ddosprot)

                server.setddosprot(7, rate_limit_window_seconds=2.0)
                self.assertTrue(server.ddosprot)
                self.assertEqual(server._rate_limiter.limit, 7)
                self.assertEqual(server._rate_limiter.window_seconds, 2.0)

                server.setddosprot(0)
                self.assertFalse(server.ddosprot)

    async def test_repr_is_informative(self) -> None:
        server = yashserver.YHttpServer(host="127.0.0.1", port=0)
        self.assertIn("stopped", repr(server))
        async with server:
            self.assertIn("running", repr(server))
            self.assertIn("http", repr(server))


class TestGracefulShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_in_flight_request_is_allowed_to_finish(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False, shutdown_drain_seconds=3.0)
        started = asyncio.Event()

        @app.get("/slow")
        async def slow(_request, _server):
            started.set()
            await asyncio.sleep(0.3)
            return {"finished": True}

        await app.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
        writer.write(b"GET /slow HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=2.0)

        # Shutting down mid-request must not cut the response off.
        await app.stop()
        response = await asyncio.wait_for(reader.read(), timeout=2.0)
        self.assertIn(b"200 OK", response)
        self.assertIn(b'"finished":true', response)
        writer.close()

    async def test_drain_deadline_is_respected(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)

        @app.get("/forever")
        async def forever(_request, _server):
            await asyncio.sleep(30)
            return "never"

        await app.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
        writer.write(b"GET /forever HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        await writer.drain()
        await asyncio.sleep(0.2)

        # A wedged handler must not block shutdown past the drain deadline.
        began = time.monotonic()
        await asyncio.wait_for(app.stop(timeout=0.3), timeout=5.0)
        self.assertLess(time.monotonic() - began, 3.0)
        self.assertIs(app.state, ServerState.STOPPED)
        writer.close()

    async def test_idle_connections_do_not_delay_shutdown(self) -> None:
        # A kept-alive HTTP connection and an idle TCP peer have no work in
        # flight, so shutdown must not sit through the whole drain window
        # waiting for them.
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False, shutdown_drain_seconds=5.0)
        tcp = yashserver.YTcpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            shutdown_drain_seconds=5.0,
            idle_timeout_seconds=None,
        )

        @app.get("/p")
        async def ping(_request, _server):
            return {"ok": True}

        @tcp.route("ping")
        async def tcp_ping(_client, _payload, _server):
            return {"ok": True}

        await app.start()
        await tcp.start()

        http_reader, http_writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
        http_writer.write(b"GET /p HTTP/1.1\r\nHost: t\r\n\r\n")
        await http_writer.drain()
        await http_reader.readuntil(b"\r\n\r\n")

        tcp_reader, tcp_writer = await asyncio.open_connection("127.0.0.1", tcp.bound_port)
        tcp_writer.write(b"ping\n")
        await tcp_writer.drain()
        await tcp_reader.readline()

        began = time.monotonic()
        await asyncio.wait_for(asyncio.gather(app.stop(), tcp.stop()), timeout=10.0)
        elapsed = time.monotonic() - began
        self.assertLess(elapsed, 2.0, f"idle connections held shutdown for {elapsed:.2f}s")

        http_writer.close()
        tcp_writer.close()

    async def test_dead_listener_is_detected_and_rebound(self) -> None:
        # CPython's Windows proactor accept loop closes the *listening* socket
        # when a single accept() fails, which a client resetting during the
        # handshake is enough to cause. The server object still looks healthy
        # while silently accepting nothing, so the guard has to notice.
        # Killing the socket directly reproduces that state on any platform.
        app = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            listener_check_seconds=0.1,
        )

        @app.get("/ok")
        async def ok(_request, _server):
            return "fine"

        await app.start()
        port = app.bound_port
        try:
            head, _body = await _request_ok(port)
            self.assertIn("200 OK", head)

            _kill_listener(app._server)
            self.assertTrue(yserver_core_listener_is_dead(app._server))

            for _ in range(60):
                if app.metrics.counter("listener_restarts"):
                    break
                await asyncio.sleep(0.05)

            self.assertEqual(app.metrics.counter("listener_restarts"), 1)
            self.assertEqual(app.bound_port, port, "must rebind the same port")

            head, _body = await _request_ok(port)
            self.assertIn("200 OK", head)
        finally:
            await app.stop()

    async def test_listener_guard_can_be_turned_off(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False, auto_restart_listener=False)

        @app.get("/ok")
        async def ok(_request, _server):
            return "fine"

        await app.start()
        try:
            _kill_listener(app._server)
            await asyncio.sleep(0.3)
            self.assertEqual(app.metrics.counter("listener_restarts"), 0)
        finally:
            await app.stop()

    async def test_stop_releases_the_port(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        await app.start()
        port = app.bound_port
        await app.stop()

        # Re-binding the same port proves the listener really went away.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        finally:
            probe.close()

    async def test_background_tasks_are_cancelled_on_stop(self) -> None:
        server = yashserver.YTcpServer(host="127.0.0.1", port=0)
        await server.start()

        running = asyncio.Event()

        async def worker():
            running.set()
            await asyncio.sleep(60)

        task = server.create_task(worker(), name="worker")
        await asyncio.wait_for(running.wait(), timeout=2.0)
        await server.stop()
        self.assertTrue(task.cancelled() or task.done())


class TestRunMany(unittest.TestCase):
    def test_run_many_runs_all_protocols_together(self) -> None:
        http = yashserver.YSyncHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        udp = yashserver.YSyncUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        tcp = yashserver.YSyncServer(host="127.0.0.1", port=0)

        @http.get("/ping")
        def ping(_request, _server):
            return {"ok": True}

        @udp.route("ping")
        def udp_ping(_endpoint, payload, _server):
            return {"udp": payload}

        @tcp.route("ping")
        def tcp_ping(_client, payload, _server):
            return {"tcp": payload}

        # run_many owns its own event loop, so it runs on a daemon thread that
        # ends with the test process.
        thread = threading.Thread(target=lambda: yashserver.run_many(http, udp, tcp), daemon=True)
        thread.start()

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if all(server.async_server.is_running for server in (http, udp, tcp)):
                break
            time.sleep(0.05)
        self.assertTrue(http.async_server.is_running)
        self.assertTrue(udp.async_server.is_running)
        self.assertTrue(tcp.async_server.is_running)

        # HTTP
        with socket.create_connection(("127.0.0.1", http.async_server.bound_port), timeout=5) as sock:
            sock.sendall(b"GET /ping HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
        self.assertIn(b'{"ok":true}', data)

        # UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(5)
            sock.sendto(b"ping hi", ("127.0.0.1", udp.async_server.bound_port))
            payload, _addr = sock.recvfrom(4096)
        self.assertEqual(json.loads(payload), {"udp": "hi"})

        # TCP
        with socket.create_connection(("127.0.0.1", tcp.async_server.bound_port), timeout=5) as sock:
            sock.sendall(b"ping hey\n")
            line = sock.makefile("rb").readline()
        self.assertEqual(json.loads(line), {"tcp": "hey"})

    def test_sync_wrappers_expose_the_shared_surface(self) -> None:
        wrappers = [
            yashserver.YSyncServer(host="127.0.0.1", port=0),
            yashserver.YSyncUdpServer(host="127.0.0.1", port=0),
            yashserver.YSyncHttpServer(host="127.0.0.1", port=0),
            yashserver.YSyncWebSocketServer(host="127.0.0.1", port=0),
        ]
        for wrapper in wrappers:
            with self.subTest(wrapper=type(wrapper).__name__):
                self.assertIsInstance(wrapper.async_server, BaseServer)
                self.assertIsInstance(wrapper.tools, dict)
                self.assertIsInstance(wrapper.metrics, yashserver.Metrics)
                self.assertIs(wrapper.setddosprot(True), wrapper)
                self.assertTrue(callable(wrapper.run))


class TestPackageSurface(unittest.TestCase):
    def test_public_names_are_importable(self) -> None:
        for name in yashserver.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(yashserver, name), name)

    def test_legacy_import_path_still_works(self) -> None:
        import yashserver.server as legacy

        for name in (
            "YServer",
            "YHttpServer",
            "YWebSocketServer",
            "YUdpServer",
            "TcpClient",
            "WebSocketClient",
            "HttpRequest",
            "WsMessage",
            "DDOS_BLOCK_MESSAGE",
            "WS_GUID",
            "_close_writer_quietly",
            "_extract_bearer_token",
            "_format_peer_name",
            "_is_numeric_ddos_limit",
            "_SlidingWindowRateLimiter",
            "_load_ddos_block_page_bytes",
        ):
            with self.subTest(name=name):
                self.assertTrue(hasattr(legacy, name), name)

    def test_legacy_rate_limiter_keeps_its_positional_signature(self) -> None:
        from yashserver.server import _SlidingWindowRateLimiter

        limiter = _SlidingWindowRateLimiter(2, 60.0)
        self.assertEqual([limiter.allow("a") for _ in range(3)], [True, True, False])

    def test_legacy_aliases_point_at_the_new_classes(self) -> None:
        self.assertIs(yashserver.YServer, yashserver.YTcpServer)
        self.assertIs(yashserver.TcpClient, yashserver.TcpConnection)
        self.assertIs(yashserver.WebSocketClient, yashserver.WebSocketConnection)

    def test_version_is_one_zero(self) -> None:
        self.assertTrue(yashserver.__version__.startswith("1."))


if __name__ == "__main__":
    unittest.main()
