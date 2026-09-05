from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import CloseCode  # noqa: E402


def client_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    first = (0x80 if fin else 0x00) | (opcode & 0x0F)
    mask = os.urandom(4)
    length = len(payload)
    if length < 126:
        header = bytes([first, 0x80 | length])
    elif length < 65536:
        header = bytes([first, 0x80 | 126]) + struct.pack("!H", length)
    else:
        header = bytes([first, 0x80 | 127]) + struct.pack("!Q", length)
    masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return header + mask + masked


def unmasked_frame(opcode: int, payload: bytes) -> bytes:
    """A frame without the client mask, which the server must reject."""

    return bytes([0x80 | opcode, len(payload)]) + payload


async def read_frame(reader: asyncio.StreamReader) -> tuple[int, bool, bytes]:
    first, second = await reader.readexactly(2)
    fin = bool(first >> 7)
    opcode = first & 0x0F
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    return opcode, fin, await reader.readexactly(length)


async def connect(
    port: int,
    path: str = "/chat",
    extra_headers: str = "",
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    writer.write(
        (
            f"GET {path} HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n{extra_headers}\r\n"
        ).encode("utf-8")
    )
    await writer.drain()
    response = await reader.readuntil(b"\r\n\r\n")
    return reader, writer, response


async def send_json(writer: asyncio.StreamWriter, payload: dict) -> None:
    writer.write(client_frame(0x1, json.dumps(payload).encode("utf-8")))
    await writer.drain()


class TestRooms(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
        )

        @self.server.route("/chat")
        async def chat(session, message, server):
            data = json.loads(message)
            action = data.get("action")
            if action == "join":
                server.join_room(session, data["room"])
                return {"joined": data["room"], "size": len(server.room_members(data["room"]))}
            if action == "leave":
                server.leave_room(session, data["room"])
                return {"left": data["room"]}
            if action == "say":
                delivered = await server.broadcast_to_room(
                    data["room"], {"said": data["text"]}, exclude=session.id
                )
                return {"delivered": delivered}
            return {"echo": data}

        await self.server.start()
        self.port = self.server.bound_port
        self.writers: list[asyncio.StreamWriter] = []

    async def asyncTearDown(self) -> None:
        for writer in self.writers:
            writer.close()
        await self.server.stop()

    async def _member(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer, _ = await connect(self.port)
        self.writers.append(writer)
        return reader, writer

    async def test_join_tracks_membership_both_ways(self) -> None:
        reader, writer = await self._member()
        await send_json(writer, {"action": "join", "room": "lobby"})
        _op, _fin, payload = await read_frame(reader)
        self.assertEqual(json.loads(payload), {"joined": "lobby", "size": 1})

        self.assertEqual(self.server.rooms(), {"lobby": 1})
        session = next(iter(self.server.clients.values()))
        self.assertTrue(session.in_room("lobby"))
        self.assertEqual(session.rooms, {"lobby"})

    async def test_room_broadcast_reaches_members_only(self) -> None:
        first_reader, first_writer = await self._member()
        second_reader, second_writer = await self._member()
        outsider_reader, outsider_writer = await self._member()

        for writer in (first_writer, second_writer):
            await send_json(writer, {"action": "join", "room": "lobby"})
        await read_frame(first_reader)
        await read_frame(second_reader)

        await send_json(outsider_writer, {"action": "join", "room": "other"})
        await read_frame(outsider_reader)

        await send_json(first_writer, {"action": "say", "room": "lobby", "text": "hello"})
        _op, _fin, ack = await read_frame(first_reader)
        self.assertEqual(json.loads(ack), {"delivered": 1})

        _op, _fin, heard = await read_frame(second_reader)
        self.assertEqual(json.loads(heard), {"said": "hello"})

        # The outsider gets nothing; prove it by round-tripping something else.
        await send_json(outsider_writer, {"ping": True})
        _op, _fin, payload = await read_frame(outsider_reader)
        self.assertEqual(json.loads(payload), {"echo": {"ping": True}})

    async def test_leaving_a_room_removes_it_when_empty(self) -> None:
        reader, writer = await self._member()
        await send_json(writer, {"action": "join", "room": "lobby"})
        await read_frame(reader)
        self.assertEqual(self.server.rooms(), {"lobby": 1})

        await send_json(writer, {"action": "leave", "room": "lobby"})
        await read_frame(reader)
        self.assertEqual(self.server.rooms(), {})

    async def test_disconnect_removes_the_session_from_its_rooms(self) -> None:
        first_reader, first_writer = await self._member()
        second_reader, second_writer = await self._member()
        for reader, writer in ((first_reader, first_writer), (second_reader, second_writer)):
            await send_json(writer, {"action": "join", "room": "lobby"})
            await read_frame(reader)
        self.assertEqual(self.server.rooms(), {"lobby": 2})

        second_writer.close()
        for _ in range(50):
            if self.server.rooms() == {"lobby": 1}:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.server.rooms(), {"lobby": 1})
        self.assertEqual(len(self.server.clients), 1)

    async def test_broadcast_to_unknown_room_delivers_nothing(self) -> None:
        reader, writer = await self._member()
        await send_json(writer, {"action": "say", "room": "ghost", "text": "hi"})
        _op, _fin, payload = await read_frame(reader)
        self.assertEqual(json.loads(payload), {"delivered": 0})

    async def test_global_broadcast_and_room_tool(self) -> None:
        first_reader, _first_writer = await self._member()
        second_reader, _second_writer = await self._member()
        for _ in range(50):
            if len(self.server.clients) == 2:
                break
            await asyncio.sleep(0.02)

        delivered = await self.server.broadcast({"announce": "hello"})
        self.assertEqual(delivered, 2)
        for reader in (first_reader, second_reader):
            _op, _fin, payload = await read_frame(reader)
            self.assertEqual(json.loads(payload), {"announce": "hello"})

        self.assertEqual(self.server.use_tool("client_count"), 2)


class TestProtocolConformance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
            max_message_size_bytes=4096,
        )

        @self.server.route("/chat")
        async def chat(_session, message, _server):
            if isinstance(message, bytes):
                return {"binary": len(message)}
            return {"text": message}

        await self.server.start()
        self.port = self.server.bound_port

    async def asyncTearDown(self) -> None:
        await self.server.stop()

    async def _expect_close(self, reader: asyncio.StreamReader) -> int:
        while True:
            opcode, _fin, payload = await asyncio.wait_for(read_frame(reader), timeout=2.0)
            if opcode == 0x8:
                return struct.unpack("!H", payload[:2])[0]

    async def test_ping_is_answered_with_pong(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x9, b"beat"))
        await writer.drain()
        opcode, _fin, payload = await read_frame(reader)
        self.assertEqual(opcode, 0xA)
        self.assertEqual(payload, b"beat")
        writer.close()

    async def test_close_frame_gets_a_close_frame_back(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x8, struct.pack("!H", CloseCode.NORMAL)))
        await writer.drain()
        opcode, _fin, payload = await read_frame(reader)
        self.assertEqual(opcode, 0x8)
        self.assertEqual(struct.unpack("!H", payload[:2])[0], CloseCode.NORMAL)
        writer.close()

    async def test_invalid_utf8_text_closes_with_1007(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x1, b"\xff\xfe\xfd"))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.INVALID_PAYLOAD)
        writer.close()

    async def test_unmasked_client_frame_is_a_protocol_error(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(unmasked_frame(0x1, b"hi"))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.PROTOCOL_ERROR)
        writer.close()

    async def test_reserved_bits_are_a_protocol_error(self) -> None:
        reader, writer, _ = await connect(self.port)
        frame = bytearray(client_frame(0x1, b"hi"))
        frame[0] |= 0x40  # RSV1 with no extension negotiated
        writer.write(bytes(frame))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.PROTOCOL_ERROR)
        writer.close()

    async def test_oversized_control_frame_is_a_protocol_error(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x9, b"x" * 126))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.PROTOCOL_ERROR)
        writer.close()

    async def test_fragmented_control_frame_is_a_protocol_error(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x9, b"x", fin=False))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.PROTOCOL_ERROR)
        writer.close()

    async def test_unexpected_continuation_is_a_protocol_error(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x0, b"orphan"))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.PROTOCOL_ERROR)
        writer.close()

    async def test_message_over_the_limit_closes_with_1009(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x1, b"x" * 5000))
        await writer.drain()
        self.assertEqual(await self._expect_close(reader), CloseCode.MESSAGE_TOO_BIG)
        writer.close()

    async def test_fragments_are_reassembled(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x1, b"hel", fin=False))
        writer.write(client_frame(0x0, b"lo", fin=True))
        await writer.drain()
        _op, _fin, payload = await read_frame(reader)
        self.assertEqual(json.loads(payload), {"text": "hello"})
        writer.close()

    async def test_binary_messages_route_as_bytes(self) -> None:
        reader, writer, _ = await connect(self.port)
        writer.write(client_frame(0x2, b"\x00\x01\x02\x03"))
        await writer.drain()
        _op, _fin, payload = await read_frame(reader)
        self.assertEqual(json.loads(payload), {"binary": 4})
        writer.close()

    async def test_handshake_requires_the_upgrade_headers(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(b"GET /chat HTTP/1.1\r\nHost: test\r\n\r\n")
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        writer.close()


class TestStreamingAndKeepalive(unittest.IsolatedAsyncioTestCase):
    async def test_send_stream_emits_fragments(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
        )

        @server.route("/s")
        async def handler(session, _message, srv):
            await srv.send_stream(session, [b"one", b"two", b"three"], binary=True)
            return None

        await server.start()
        try:
            reader, writer, _ = await connect(server.bound_port, "/s")
            writer.write(client_frame(0x1, b"go"))
            await writer.drain()

            opcode, fin, payload = await read_frame(reader)
            self.assertEqual((opcode, fin, payload), (0x2, False, b"one"))
            opcode, fin, payload = await read_frame(reader)
            self.assertEqual((opcode, fin, payload), (0x0, False, b"two"))
            opcode, fin, payload = await read_frame(reader)
            self.assertEqual((opcode, fin, payload), (0x0, False, b"three"))
            opcode, fin, payload = await read_frame(reader)
            self.assertEqual((opcode, fin, payload), (0x0, True, b""))
            writer.close()
        finally:
            await server.stop()

    async def test_missing_pong_drops_the_connection(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=0.05,
            ping_timeout_seconds=0.1,
        )

        @server.route("/p")
        async def handler(_session, message, _server):
            return message

        await server.start()
        try:
            reader, writer, _ = await connect(server.bound_port, "/p")
            # Read frames but never answer the ping.
            deadline = asyncio.get_running_loop().time() + 3.0
            closed = False
            while asyncio.get_running_loop().time() < deadline:
                try:
                    opcode, _fin, _payload = await asyncio.wait_for(read_frame(reader), timeout=1.0)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                    closed = True
                    break
                if opcode == 0x8:
                    closed = True
                    break
            self.assertTrue(closed)
            self.assertGreaterEqual(server.metrics.counter("ping_timeouts"), 1)
            writer.close()
        finally:
            await server.stop()

    async def test_idle_timeout_closes_silent_connections(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
            idle_timeout_seconds=0.15,
        )

        @server.route("/i")
        async def handler(_session, message, _server):
            return message

        await server.start()
        try:
            reader, writer, _ = await connect(server.bound_port, "/i")
            opcode, _fin, payload = await asyncio.wait_for(read_frame(reader), timeout=2.0)
            self.assertEqual(opcode, 0x8)
            self.assertEqual(struct.unpack("!H", payload[:2])[0], CloseCode.GOING_AWAY)
            writer.close()
        finally:
            await server.stop()

    async def test_shutdown_sends_going_away(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
        )

        @server.route("/x")
        async def handler(_session, message, _server):
            return message

        await server.start()
        reader, writer, _ = await connect(server.bound_port, "/x")
        writer.write(client_frame(0x1, b"hi"))
        await writer.drain()
        await read_frame(reader)

        await server.stop()
        opcode, _fin, payload = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        self.assertEqual(opcode, 0x8)
        self.assertEqual(struct.unpack("!H", payload[:2])[0], CloseCode.GOING_AWAY)
        writer.close()


class TestSubprotocolsAndLimits(unittest.IsolatedAsyncioTestCase):
    async def test_subprotocol_negotiation(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
            subprotocols=("chat.v2", "chat.v1"),
        )

        @server.route("/s")
        async def handler(session, _message, _server):
            return {"subprotocol": session.subprotocol}

        await server.start()
        try:
            reader, writer, response = await connect(
                server.bound_port,
                "/s",
                "Sec-WebSocket-Protocol: chat.v9, chat.v1\r\n",
            )
            self.assertIn(b"Sec-WebSocket-Protocol: chat.v1", response)
            writer.write(client_frame(0x1, b"hi"))
            await writer.drain()
            _op, _fin, payload = await read_frame(reader)
            self.assertEqual(json.loads(payload), {"subprotocol": "chat.v1"})
            writer.close()
        finally:
            await server.stop()

    async def test_max_connections_is_enforced(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
            max_connections=1,
        )

        @server.route("/x")
        async def handler(_session, message, _server):
            return message

        await server.start()
        try:
            _reader, writer, response = await connect(server.bound_port, "/x")
            self.assertIn(b"101", response)

            _reader2, writer2, response2 = await connect(server.bound_port, "/x")
            self.assertIn(b"503", response2)
            writer.close()
            writer2.close()
        finally:
            await server.stop()

    async def test_auth_rejects_before_upgrade(self) -> None:
        server = yashserver.YWebSocketServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            ping_interval_seconds=None,
            auth_token="letmein",
        )

        @server.route("/x")
        async def handler(_session, message, _server):
            return message

        await server.start()
        try:
            _reader, writer, response = await connect(server.bound_port, "/x")
            self.assertIn(b"401 Unauthorized", response)
            writer.close()

            _reader, writer, response = await connect(server.bound_port, "/x?token=letmein")
            self.assertIn(b"101 Switching Protocols", response)
            writer.close()
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
