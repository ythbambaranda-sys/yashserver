from __future__ import annotations

import asyncio
import json
import socket
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import UdpDatagram, UdpEndpoint  # noqa: E402
from yashserver.core import ConfigError  # noqa: E402


class _Client:
    """A plain UDP socket driven from the event loop.

    Blocking recvfrom would starve the server's own loop, so reads go through
    ``loop.sock_recv``.
    """

    def __init__(self, family: int = socket.AF_INET) -> None:
        self.socket = socket.socket(family, socket.SOCK_DGRAM)
        self.socket.setblocking(False)

    def send(self, data: bytes, host: str, port: int) -> None:
        self.socket.sendto(data, (host, port))

    async def recv(self, timeout: float = 2.0) -> bytes:
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(loop.sock_recv(self.socket, 65535), timeout=timeout)

    def close(self) -> None:
        self.socket.close()


class TestUdpRouting(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)

        @self.server.route("echo")
        async def echo(endpoint, payload, _srv):
            return {"echo": payload, "from": endpoint.key}

        @self.server.route("boom")
        async def boom(_endpoint, _payload, _srv):
            raise RuntimeError("handler exploded")

        await self.server.start()
        self.port = self.server.bound_port
        self.client = _Client()

    async def asyncTearDown(self) -> None:
        self.client.close()
        await self.server.stop()

    async def test_command_routing_replies_to_sender(self) -> None:
        self.client.send(b"echo hello", "127.0.0.1", self.port)
        reply = json.loads(await self.client.recv())
        self.assertEqual(reply["echo"], "hello")

    async def test_unknown_command_is_reported(self) -> None:
        self.client.send(b"nope x", "127.0.0.1", self.port)
        reply = json.loads(await self.client.recv())
        self.assertEqual(reply["error"], "unknown-command")

    async def test_handler_error_does_not_stop_the_server(self) -> None:
        self.client.send(b"boom", "127.0.0.1", self.port)
        await asyncio.sleep(0.15)
        self.assertGreaterEqual(self.server.metrics.counter("errors"), 1)

        self.client.send(b"echo still-up", "127.0.0.1", self.port)
        self.assertEqual(json.loads(await self.client.recv())["echo"], "still-up")

    async def test_endpoints_are_tracked_but_are_not_connections(self) -> None:
        self.client.send(b"echo x", "127.0.0.1", self.port)
        await self.client.recv()

        endpoints = self.server.known_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertIsInstance(endpoints[0], UdpEndpoint)
        self.assertEqual(endpoints[0].host, "127.0.0.1")
        # The API deliberately offers no connect/disconnect/close for a peer.
        self.assertFalse(hasattr(endpoints[0], "close"))
        self.assertEqual(self.server.use_tool("endpoint_count"), 1)

    async def test_raw_datagram_handler_takes_precedence(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        seen: list[UdpDatagram] = []

        @server.on_datagram
        async def handle(datagram, _srv):
            seen.append(datagram)
            return b"raw:" + datagram.data

        await server.start()
        try:
            client = _Client()
            client.send(b"anything", "127.0.0.1", server.bound_port)
            self.assertEqual(await client.recv(), b"raw:anything")
            self.assertEqual(len(seen), 1)
            self.assertEqual(len(seen[0]), 8)
            client.close()
        finally:
            await server.stop()


class TestUdpPacketHandling(unittest.IsolatedAsyncioTestCase):
    async def test_oversized_datagram_is_dropped(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False, max_packet_size=512)

        @server.route("echo")
        async def echo(_endpoint, payload, _srv):
            return {"len": len(payload)}

        await server.start()
        try:
            client = _Client()
            client.send(b"echo " + b"z" * 1000, "127.0.0.1", server.bound_port)
            await asyncio.sleep(0.2)
            self.assertEqual(server.metrics.counter("dropped_oversized"), 1)

            client.send(b"echo small", "127.0.0.1", server.bound_port)
            self.assertEqual(json.loads(await client.recv())["len"], 5)
            client.close()
        finally:
            await server.stop()

    async def test_oversized_send_is_rejected_rather_than_truncated(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, max_packet_size=512)
        await server.start()
        try:
            with self.assertRaises(ValueError):
                await server.send_to(UdpEndpoint("127.0.0.1", 9), b"y" * 1000)
            self.assertEqual(server.metrics.counter("send_rejected_oversized"), 1)
        finally:
            await server.stop()

    async def test_send_to_reports_handoff_not_delivery(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0)
        await server.start()
        try:
            # Nothing is listening on port 9, yet the send still succeeds:
            # UDP cannot tell us whether anything received it.
            self.assertTrue(await server.send_to(UdpEndpoint("127.0.0.1", 9), b"into the void"))
        finally:
            await server.stop()

    async def test_rate_limit_drops_excess_datagrams(self) -> None:
        server = yashserver.YUdpServer(
            host="127.0.0.1",
            port=0,
            rate_limit_per_window=2,
            rate_limit_window_seconds=60.0,
        )

        @server.route("ping")
        async def ping(_endpoint, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            client = _Client()
            for _ in range(5):
                client.send(b"ping", "127.0.0.1", server.bound_port)
            await asyncio.sleep(0.25)
            self.assertEqual(server.metrics.counter("dropped_rate_limited"), 3)
            client.close()
        finally:
            await server.stop()

    async def test_configuration_is_validated(self) -> None:
        with self.assertRaises(ConfigError):
            yashserver.YUdpServer(host="127.0.0.1", port=0, family="ipx")
        with self.assertRaises(ConfigError):
            yashserver.YUdpServer(host="127.0.0.1", port=0, max_packet_size=999999)
        with self.assertRaises(ConfigError):
            yashserver.YUdpServer(host="127.0.0.1", port=0, backpressure_policy="explode")
        with self.assertRaises(ConfigError):
            yashserver.YUdpServer(host="127.0.0.1", port=0, not_a_real_option=1)


class TestUdpEndpointModel(unittest.TestCase):
    def test_ipv4_and_ipv6_keys(self) -> None:
        v4 = UdpEndpoint("10.0.0.1", 5000)
        self.assertEqual(v4.key, "10.0.0.1:5000")
        self.assertEqual(v4.as_addr(), ("10.0.0.1", 5000))
        self.assertFalse(v4.is_ipv6)

        v6 = UdpEndpoint.from_addr(("::1", 5000, 0, 0))
        self.assertTrue(v6.is_ipv6)
        self.assertEqual(v6.key, "[::1]:5000")
        self.assertEqual(v6.as_addr(), ("::1", 5000, 0, 0))

    def test_endpoints_are_hashable_values(self) -> None:
        # Endpoints are values, so they work as dict keys for per-peer state.
        first = UdpEndpoint("10.0.0.1", 5000)
        second = UdpEndpoint("10.0.0.1", 5000)
        self.assertEqual(first, second)
        self.assertEqual(len({first, second}), 1)

    def test_datagram_decoding_helpers(self) -> None:
        datagram = UdpDatagram(data=b'{"a":1}', endpoint=UdpEndpoint("127.0.0.1", 1))
        self.assertEqual(datagram.json(), {"a": 1})
        self.assertEqual(datagram.text(), '{"a":1}')
        self.assertEqual(len(datagram), 7)
        self.assertIsNone(UdpDatagram(data=b"nope", endpoint=UdpEndpoint("h", 1)).json())


class TestUdpPeerLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_idle_endpoints_are_pruned(self) -> None:
        server = yashserver.YUdpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            peer_idle_seconds=0.2,
        )

        @server.route("ping")
        async def ping(_endpoint, _payload, _srv):
            return {"ok": True}

        await server.start()
        try:
            client = _Client()
            client.send(b"ping", "127.0.0.1", server.bound_port)
            await client.recv()
            self.assertEqual(len(server.known_endpoints()), 1)

            for _ in range(60):
                if not server.known_endpoints():
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(server.known_endpoints(), [])
            client.close()
        finally:
            await server.stop()

    async def test_tracked_peers_are_capped(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False, max_tracked_peers=3)
        await server.start()
        try:
            clients = [_Client() for _ in range(8)]
            for client in clients:
                client.send(b"noop", "127.0.0.1", server.bound_port)
            await asyncio.sleep(0.3)
            self.assertLessEqual(len(server.known_endpoints()), 3)
            for client in clients:
                client.close()
        finally:
            await server.stop()

    async def test_broadcast_fans_out_to_known_endpoints(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)

        @server.route("hello")
        async def hello(_endpoint, _payload, _srv):
            return {"hi": True}

        await server.start()
        try:
            clients = [_Client() for _ in range(3)]
            for client in clients:
                client.send(b"hello", "127.0.0.1", server.bound_port)
                await client.recv()

            sent = await server.broadcast({"news": "hi all"})
            self.assertEqual(sent, 3)
            for client in clients:
                self.assertEqual(json.loads(await client.recv()), {"news": "hi all"})
                client.close()
        finally:
            await server.stop()


class TestUdpPlugins(unittest.IsolatedAsyncioTestCase):
    async def test_plugin_can_transform_and_drop_datagrams(self) -> None:
        class Filter(yashserver.ServerPlugin):
            name = "udp-filter"

            async def on_udp_datagram(self, datagram, _server):
                if datagram.data.startswith(b"drop"):
                    return None
                datagram.data = datagram.data.replace(b"SHOUT", b"echo")
                return datagram

        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        stats = yashserver.ConnectionStatsPlugin()
        server.add_plugin(Filter()).add_plugin(stats)

        @server.route("echo")
        async def echo(_endpoint, payload, _srv):
            return {"echo": payload}

        await server.start()
        try:
            client = _Client()
            client.send(b"drop this", "127.0.0.1", server.bound_port)
            await asyncio.sleep(0.15)
            self.assertEqual(server.metrics.counter("dropped_by_plugin"), 1)

            client.send(b"SHOUT hey", "127.0.0.1", server.bound_port)
            self.assertEqual(json.loads(await client.recv())["echo"], "hey")
            # A datagram dropped by an earlier plugin never reaches later
            # ones, so the stats plugin only counted the surviving datagram.
            self.assertEqual(stats.udp_datagrams, 1)
            self.assertEqual(stats.udp_endpoints_seen, 1)
            client.close()
        finally:
            await server.stop()


@unittest.skipUnless(socket.has_ipv6, "IPv6 unavailable")
class TestUdpIPv6(unittest.IsolatedAsyncioTestCase):
    async def test_ipv6_server_answers_ipv6_clients(self) -> None:
        server = yashserver.YUdpServer(host="::1", port=0, ddosprot=False)

        @server.route("ping")
        async def ping(endpoint, _payload, _srv):
            return {"ipv6": endpoint.is_ipv6}

        try:
            await server.start()
        except Exception as error:  # pragma: no cover - platform dependent
            self.skipTest(f"IPv6 loopback unavailable: {error}")

        try:
            client = _Client(socket.AF_INET6)
            client.send(b"ping", "::1", server.bound_port)
            self.assertEqual(json.loads(await client.recv()), {"ipv6": True})
            client.close()
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
