"""Tests for the optional application-level reliability helper.

These exist to prove the helper does what it claims (at-least-once delivery,
de-duplication, optional ordering, and an honest give-up signal) and nothing
more. UDP itself is still unreliable underneath.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import ReliableUdpChannel, UdpEndpoint  # noqa: E402
from yashserver.udp import _RELIABLE_HEADER, _RELIABLE_MAGIC, _TYPE_DATA  # noqa: E402


def _data_frame(seq: int, payload: bytes) -> bytes:
    return _RELIABLE_HEADER.pack(_RELIABLE_MAGIC, 1, _TYPE_DATA, seq) + payload


async def _until(predicate, timeout: float = 3.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


class _Pair:
    """Two UDP servers, each with a reliable channel, pointed at each other."""

    def __init__(self, **channel_options) -> None:
        self.sender = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.receiver = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        options = {"retry_interval_seconds": 0.05, **channel_options}
        self.sender_channel = ReliableUdpChannel(self.sender, **options)
        self.receiver_channel = ReliableUdpChannel(self.receiver, **options)
        self.sender.on_datagram(self.sender_channel.handle_datagram)
        self.receiver.on_datagram(self.receiver_channel.handle_datagram)
        self.received: list[bytes] = []
        self.receiver_channel.on_message(lambda payload, _endpoint: self.received.append(payload))

    async def start(self) -> "UdpEndpoint":
        await self.sender.start()
        await self.receiver.start()
        return UdpEndpoint("127.0.0.1", self.receiver.bound_port)

    async def stop(self) -> None:
        await self.sender.stop()
        await self.receiver.stop()


class TestReliableDelivery(unittest.IsolatedAsyncioTestCase):
    async def test_messages_are_delivered_and_acknowledged(self) -> None:
        pair = _Pair()
        target = await pair.start()
        try:
            for index in range(5):
                await pair.sender_channel.send(target, f"msg{index}".encode())

            self.assertTrue(await _until(lambda: len(pair.received) == 5))
            self.assertEqual(pair.received, [f"msg{i}".encode() for i in range(5)])

            # Every message was acknowledged, so nothing stays pending.
            self.assertTrue(await _until(lambda: pair.sender_channel.pending_count == 0))
            self.assertEqual(pair.sender.metrics.counter("reliable_acked"), 5)
        finally:
            await pair.stop()

    async def test_ordered_mode_delivers_in_sequence(self) -> None:
        pair = _Pair(ordered=True)
        target = await pair.start()
        try:
            for index in range(10):
                await pair.sender_channel.send(target, str(index).encode())
            self.assertTrue(await _until(lambda: len(pair.received) == 10))
            self.assertEqual(pair.received, [str(i).encode() for i in range(10)])
        finally:
            await pair.stop()

    async def test_out_of_order_arrivals_are_reordered(self) -> None:
        pair = _Pair(ordered=True)
        target = await pair.start()
        try:
            # Deliver sequence 2 before sequence 1, bypassing the sender side.
            await pair.sender.send_to(target, _data_frame(2, b"second"))
            await asyncio.sleep(0.15)
            self.assertEqual(pair.received, [], "seq 2 must wait for seq 1")

            await pair.sender.send_to(target, _data_frame(1, b"first"))
            self.assertTrue(await _until(lambda: len(pair.received) == 2))
            self.assertEqual(pair.received, [b"first", b"second"])
        finally:
            await pair.stop()

    async def test_reorder_window_does_not_stall_forever_on_a_lost_packet(self) -> None:
        pair = _Pair(ordered=True, reorder_window=3)
        target = await pair.start()
        try:
            # Sequence 1 never arrives. Once the buffer exceeds the window the
            # channel gives up on the gap rather than stalling the stream.
            for seq in range(2, 8):
                await pair.sender.send_to(target, _data_frame(seq, f"p{seq}".encode()))
            self.assertTrue(await _until(lambda: len(pair.received) >= 4))
            self.assertEqual(pair.receiver.metrics.counter("reliable_reorder_gaps"), 1)
        finally:
            await pair.stop()

    async def test_duplicates_are_suppressed(self) -> None:
        pair = _Pair()
        target = await pair.start()
        try:
            frame = _data_frame(1, b"once")
            for _ in range(3):
                await pair.sender.send_to(target, frame)
            self.assertTrue(await _until(lambda: pair.receiver.metrics.counter("reliable_duplicates") == 2))
            self.assertEqual(pair.received, [b"once"])
        finally:
            await pair.stop()

    async def test_unacknowledged_message_is_retransmitted(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        channel = ReliableUdpChannel(server, retry_interval_seconds=0.05, max_retries=10)
        await server.start()
        try:
            # Port 1 will never acknowledge, so retransmission must kick in.
            await channel.send(UdpEndpoint("127.0.0.1", 1), b"hello")
            self.assertTrue(await _until(lambda: server.metrics.counter("reliable_retransmits") >= 2))
            self.assertEqual(channel.pending_count, 1)
        finally:
            await server.stop()

    async def test_give_up_is_reported_after_max_retries(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
        channel = ReliableUdpChannel(server, retry_interval_seconds=0.03, max_retries=2)
        failures: list[bytes] = []
        channel.on_delivery_failed(lambda payload, _endpoint: failures.append(payload))

        await server.start()
        try:
            await channel.send(UdpEndpoint("127.0.0.1", 1), b"nobody-home")
            self.assertTrue(await _until(lambda: bool(failures)))
            self.assertEqual(failures, [b"nobody-home"])
            self.assertEqual(channel.pending_count, 0)
            self.assertEqual(server.metrics.counter("reliable_delivery_failed"), 1)
        finally:
            await server.stop()


class TestReliableFraming(unittest.IsolatedAsyncioTestCase):
    async def test_foreign_datagrams_are_ignored_not_delivered(self) -> None:
        pair = _Pair()
        target = await pair.start()
        try:
            await pair.sender.send_to(target, b"not-a-reliable-frame")
            await asyncio.sleep(0.15)
            self.assertEqual(pair.received, [])
            self.assertEqual(pair.receiver.metrics.counter("reliable_malformed"), 1)
        finally:
            await pair.stop()

    async def test_payload_larger_than_a_datagram_is_rejected(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0, max_packet_size=512)
        channel = ReliableUdpChannel(server)
        await server.start()
        try:
            with self.assertRaises(ValueError) as caught:
                await channel.send(UdpEndpoint("127.0.0.1", 1), b"x" * 512)
            self.assertIn("reliable limit", str(caught.exception))
        finally:
            await server.stop()

    async def test_invalid_options_are_rejected(self) -> None:
        server = yashserver.YUdpServer(host="127.0.0.1", port=0)
        with self.assertRaises(yashserver.ConfigError):
            ReliableUdpChannel(server, retry_interval_seconds=0)
        with self.assertRaises(yashserver.ConfigError):
            ReliableUdpChannel(server, max_retries=-1)

    def test_header_is_nine_bytes(self) -> None:
        # magic(3) + version(1) + type(1) + seq(4). Documented as the overhead
        # subtracted from max_packet_size, so it must not drift.
        self.assertEqual(_RELIABLE_HEADER.size, 9)
        self.assertEqual(len(_data_frame(1, b"")), 9)
        magic, version, kind, seq = _RELIABLE_HEADER.unpack_from(_data_frame(7, b"x"))
        self.assertEqual((magic, version, kind, seq), (b"YRL", 1, _TYPE_DATA, 7))


if __name__ == "__main__":
    unittest.main()
