"""UDP reliability under a hostile network, without needing root.

``tc netem`` is the real instrument for this, but it needs root and a Linux
kernel, so it cannot run in an ordinary test suite. This module puts a
userspace relay between the two endpoints and has it drop, delay, duplicate
and reorder datagrams instead. That is not a substitute for kernel-level
impairment -- see ``scripts/udp_netem_test.py`` for that -- but it is
deterministic, seeded and reproducible, which the kernel version is not.

The distinction matters and is kept explicit: this file proves the protocol
logic handles loss and reordering; netem proves it survives a real network
stack.
"""

from __future__ import annotations

import asyncio
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402


class ImpairedRelay:
    """A UDP relay that damages traffic in the documented ways.

    Sits between a client and a server: everything the client sends is
    forwarded to the server, and replies come back, with loss, duplication,
    reordering and delay applied in each direction.
    """

    def __init__(
        self,
        target: tuple[str, int],
        *,
        loss: float = 0.0,
        duplicate: float = 0.0,
        reorder: float = 0.0,
        delay_seconds: float = 0.0,
        jitter_seconds: float = 0.0,
        seed: int = 1234,
    ) -> None:
        self.target = target
        self.loss = loss
        self.duplicate = duplicate
        self.reorder = reorder
        self.delay_seconds = delay_seconds
        self.jitter_seconds = jitter_seconds
        self.random = random.Random(seed)
        self.transport: asyncio.DatagramTransport | None = None
        self._peer: tuple[str, int] | None = None
        self._tasks: set[asyncio.Task] = set()
        self.stats = {"forwarded": 0, "dropped": 0, "duplicated": 0, "reordered": 0}

    @property
    def port(self) -> int:
        assert self.transport is not None
        return self.transport.get_extra_info("sockname")[1]

    async def start(self, host: str = "127.0.0.1") -> "ImpairedRelay":
        loop = asyncio.get_running_loop()
        relay = self

        class _Protocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                relay.transport = transport

            def datagram_received(self, data, addr):
                relay._handle(data, addr)

        await loop.create_datagram_endpoint(_Protocol, local_addr=(host, 0))
        return self

    def _handle(self, data: bytes, addr: tuple[str, int]) -> None:
        # Traffic from the server goes back to the client; everything else is
        # from the client and goes to the server.
        if addr[1] == self.target[1] and addr[0] == self.target[0]:
            destination = self._peer
        else:
            self._peer = addr
            destination = self.target
        if destination is None:
            return

        if self.random.random() < self.loss:
            self.stats["dropped"] += 1
            return

        copies = 1
        if self.random.random() < self.duplicate:
            copies = 2
            self.stats["duplicated"] += 1

        delay = self.delay_seconds
        if self.jitter_seconds:
            delay += self.random.uniform(0, self.jitter_seconds)
        if self.random.random() < self.reorder:
            # Hold this one back so a later datagram overtakes it.
            delay += max(self.jitter_seconds, 0.02) * 3
            self.stats["reordered"] += 1

        for _ in range(copies):
            if delay > 0:
                task = asyncio.get_running_loop().create_task(
                    self._send_later(data, destination, delay)
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            else:
                self._forward(data, destination)

    async def _send_later(self, data: bytes, destination, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            self._forward(data, destination)
        except asyncio.CancelledError:
            pass

    def _forward(self, data: bytes, destination) -> None:
        if self.transport is None or self.transport.is_closing():
            return
        try:
            self.transport.sendto(data, destination)
            self.stats["forwarded"] += 1
        except Exception:
            pass

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.transport is not None:
            self.transport.close()


class _ReliableFixture(unittest.IsolatedAsyncioTestCase):
    """A reliable channel talking to an echo server through a bad network."""

    async def build(self, **impairment):
        self.server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.received: list[bytes] = []
        server_channel = yashserver.ReliableUdpChannel(
            self.server, retry_interval_seconds=0.05, max_retries=25, **self._channel_kwargs
        )

        @server_channel.on_message
        async def _on_message(payload, _endpoint):
            self.received.append(payload)

        self.server.on_datagram(server_channel.handle_datagram)
        await self.server.start()
        self.server_channel = server_channel

        self.relay = await ImpairedRelay(
            ("127.0.0.1", self.server.bound_port), **impairment
        ).start()

        self.client = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.client_channel = yashserver.ReliableUdpChannel(
            self.client, retry_interval_seconds=0.05, max_retries=25, **self._channel_kwargs
        )
        self.client.on_datagram(self.client_channel.handle_datagram)
        await self.client.start()
        self.failures: list = []
        self.client_channel.on_delivery_failed(
            lambda *args: self.failures.append(args)
        )
        return yashserver.UdpEndpoint("127.0.0.1", self.relay.port)

    _channel_kwargs: dict = {}

    async def teardown_all(self) -> None:
        await self.relay.stop()
        await self.client.stop()
        await self.server.stop()


class TestUnorderedDelivery(_ReliableFixture):
    _channel_kwargs = {"ordered": False}

    async def test_every_message_arrives_despite_30_percent_loss(self) -> None:
        endpoint = await self.build(loss=0.30, seed=7)
        try:
            for i in range(40):
                await self.client_channel.send(endpoint, f"msg-{i:03d}".encode())
            for _ in range(200):
                if len(set(self.received)) >= 40:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(
                {f"msg-{i:03d}".encode() for i in range(40)},
                set(self.received),
                f"relay stats: {self.relay.stats}",
            )
            self.assertEqual(self.failures, [])
            self.assertGreater(self.relay.stats["dropped"], 0, "the test dropped nothing")
        finally:
            await self.teardown_all()

    async def test_duplicates_are_suppressed(self) -> None:
        endpoint = await self.build(duplicate=0.5, seed=11)
        try:
            for i in range(30):
                await self.client_channel.send(endpoint, f"dup-{i:03d}".encode())
            for _ in range(200):
                if len(set(self.received)) >= 30:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(len(set(self.received)), 30)
            # Duplicate suppression means the handler saw each exactly once,
            # even though the network delivered some of them twice.
            self.assertEqual(
                len(self.received), 30, f"duplicates leaked: {self.relay.stats}"
            )
            self.assertGreater(self.relay.stats["duplicated"], 0)
        finally:
            await self.teardown_all()

    async def test_survives_loss_duplication_reordering_and_jitter_together(self) -> None:
        endpoint = await self.build(
            loss=0.20, duplicate=0.20, reorder=0.20, delay_seconds=0.005,
            jitter_seconds=0.02, seed=99,
        )
        try:
            for i in range(50):
                await self.client_channel.send(endpoint, f"mix-{i:03d}".encode())
            for _ in range(300):
                if len(set(self.received)) >= 50:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(
                {f"mix-{i:03d}".encode() for i in range(50)},
                set(self.received),
                f"relay stats: {self.relay.stats}",
            )
            self.assertEqual(len(self.received), 50, "duplicates reached the handler")
        finally:
            await self.teardown_all()


class TestOrderedDelivery(_ReliableFixture):
    _channel_kwargs = {"ordered": True, "reorder_window": 128, "reorder_timeout_seconds": 3.0}

    async def test_order_is_restored_after_reordering(self) -> None:
        endpoint = await self.build(reorder=0.35, jitter_seconds=0.02, seed=5)
        try:
            for i in range(40):
                await self.client_channel.send(endpoint, f"ord-{i:03d}".encode())
            for _ in range(300):
                if len(self.received) >= 40:
                    break
                await asyncio.sleep(0.05)
            expected = [f"ord-{i:03d}".encode() for i in range(40)]
            self.assertEqual(
                self.received, expected, f"out of order; relay stats: {self.relay.stats}"
            )
            self.assertGreater(self.relay.stats["reordered"], 0)
        finally:
            await self.teardown_all()

    async def test_order_holds_with_loss_and_reordering_together(self) -> None:
        endpoint = await self.build(
            loss=0.15, reorder=0.25, jitter_seconds=0.02, seed=21
        )
        try:
            for i in range(40):
                await self.client_channel.send(endpoint, f"both-{i:03d}".encode())
            for _ in range(400):
                if len(self.received) >= 40:
                    break
                await asyncio.sleep(0.05)
            expected = [f"both-{i:03d}".encode() for i in range(40)]
            self.assertEqual(
                self.received, expected, f"out of order; relay stats: {self.relay.stats}"
            )
        finally:
            await self.teardown_all()


class TestGivingUp(_ReliableFixture):
    _channel_kwargs = {"ordered": False}

    async def test_total_loss_is_reported_rather_than_retried_forever(self) -> None:
        # UDP is not magic: when nothing gets through, the honest outcome is a
        # reported failure, not an infinite retry loop.
        self.server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        await self.server.start()
        self.relay = await ImpairedRelay(
            ("127.0.0.1", self.server.bound_port), loss=1.0
        ).start()
        self.client = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        channel = yashserver.ReliableUdpChannel(
            self.client, retry_interval_seconds=0.02, max_retries=3
        )
        self.client.on_datagram(channel.handle_datagram)
        await self.client.start()
        failures: list = []
        channel.on_delivery_failed(lambda *args: failures.append(args))
        try:
            await channel.send(yashserver.UdpEndpoint("127.0.0.1", self.relay.port), b"lost")
            for _ in range(200):
                if failures:
                    break
                await asyncio.sleep(0.05)
            self.assertTrue(failures, "a permanently undeliverable message was never reported")
            self.assertEqual(channel.pending_count, 0)
        finally:
            await self.relay.stop()
            await self.client.stop()
            await self.server.stop()


if __name__ == "__main__":
    unittest.main()
