from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yserver


def _make_client_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
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


async def _read_server_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    first, second = await reader.readexactly(2)
    opcode = first & 0x0F
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    payload = await reader.readexactly(length)
    return opcode, payload


class TestWebSocketServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = yserver.YWebSocketServer(host="127.0.0.1", port=0, rate_limit_per_window=2, rate_limit_window_seconds=60)

        @self.server.route("/chat")
        async def chat(_session, message, _server):
            if isinstance(message, bytes):
                return {"type": "binary", "size": len(message)}
            payload = yserver.ServerTools.from_json(message, default={})
            return {"type": "text", "text": payload.get("text", message)}

        await self.server.start()
        assert self.server._server is not None
        self.port = int(self.server._server.sockets[0].getsockname()[1])

    async def asyncTearDown(self) -> None:
        await self.server.stop()

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET /chat HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        writer.write(request.encode("utf-8"))
        await writer.drain()
        response = await reader.readuntil(b"\r\n\r\n")
        self.assertIn(b"101 Switching Protocols", response)
        return reader, writer

    async def test_fragmented_text_message_is_reassembled(self) -> None:
        reader, writer = await self._connect()
        payload = b'{"text":"hello-world"}'
        half = len(payload) // 2
        writer.write(_make_client_frame(0x1, payload[:half], fin=False))
        writer.write(_make_client_frame(0x0, payload[half:], fin=True))
        await writer.drain()

        _opcode, response_payload = await _read_server_frame(reader)
        response = json.loads(response_payload.decode("utf-8"))
        self.assertEqual(response["type"], "text")
        self.assertEqual(response["text"], "hello-world")
        writer.close()
        await writer.wait_closed()

    async def test_binary_message_route(self) -> None:
        reader, writer = await self._connect()
        writer.write(_make_client_frame(0x2, b"\x01\x02\x03", fin=True))
        await writer.drain()

        _opcode, response_payload = await _read_server_frame(reader)
        response = json.loads(response_payload.decode("utf-8"))
        self.assertEqual(response["type"], "binary")
        self.assertEqual(response["size"], 3)
        writer.close()
        await writer.wait_closed()

    async def test_rate_limit_returns_error_payload(self) -> None:
        reader, writer = await self._connect()
        for _ in range(3):
            writer.write(_make_client_frame(0x1, b'{"text":"hello"}', fin=True))
            await writer.drain()

        # first 2 are normal responses
        await _read_server_frame(reader)
        await _read_server_frame(reader)
        _opcode, third_payload = await _read_server_frame(reader)
        third = json.loads(third_payload.decode("utf-8"))
        self.assertEqual(third.get("type"), "error")
        self.assertIn("rate-limit", third.get("message", ""))
        writer.close()
        await writer.wait_closed()

    async def test_auth_rejects_missing_token(self) -> None:
        await self.server.stop()
        self.server = yserver.YWebSocketServer(host="127.0.0.1", port=0, auth_token="secret-token")

        @self.server.route("/chat")
        async def _chat(_session, message, _server):
            return message

        await self.server.start()
        assert self.server._server is not None
        self.port = int(self.server._server.sockets[0].getsockname()[1])

        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET /chat HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        writer.write(request.encode("utf-8"))
        await writer.drain()
        response = await reader.read()
        self.assertIn(b"401 Unauthorized", response)
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()

