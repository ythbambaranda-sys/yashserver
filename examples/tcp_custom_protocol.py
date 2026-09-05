"""A custom length-prefixed binary TCP protocol, plus a client for it.

Command routing is convenient, but real TCP services often speak their own
framing. ``@server.on_connection`` hands you the socket and gets out of the
way, while still giving you connection lifecycle, timeouts, backpressure,
graceful shutdown, TLS and metrics.

Wire format, big-endian:

    uint8   opcode        1 = ECHO, 2 = SUM, 3 = TIME
    uint32  payload_len
    bytes   payload

Run:

    python examples/tcp_custom_protocol.py            # server
    python examples/tcp_custom_protocol.py client     # exercise it
"""

from __future__ import annotations

import asyncio
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver

PORT = 9200
HEADER = struct.Struct("!BI")
MAX_PAYLOAD = 1 << 20  # 1 MiB, enforced so a peer cannot ask us to allocate more

OP_ECHO, OP_SUM, OP_TIME = 1, 2, 3


def frame(opcode: int, payload: bytes) -> bytes:
    return HEADER.pack(opcode, len(payload)) + payload


server = yashserver.YTcpServer(
    host="127.0.0.1",
    port=PORT,
    # Custom protocols usually want their own timeouts and buffer sizes.
    idle_timeout_seconds=120.0,
    write_timeout_seconds=15.0,
    write_buffer_high_bytes=512 * 1024,
    max_connections=5_000,
)
server.add_plugin(yashserver.LoggingPlugin())


@server.on_connection
async def handle(connection, srv):
    """One coroutine per peer, owning the socket for its lifetime."""

    while connection.is_open:
        try:
            header = await asyncio.wait_for(connection.readexactly(HEADER.size), timeout=120.0)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            return  # peer closed, or went quiet

        opcode, length = HEADER.unpack(header)
        if length > MAX_PAYLOAD:
            # Never trust a declared length. Refuse and hang up.
            srv.metrics.incr("oversized_frames")
            await connection.send_bytes(frame(0, b"payload too large"))
            await connection.close()
            return

        payload = await connection.readexactly(length)
        srv.metrics.incr("frames_received")

        if opcode == OP_ECHO:
            await connection.send_bytes(frame(OP_ECHO, payload))
        elif opcode == OP_SUM:
            numbers = struct.unpack(f"!{len(payload) // 4}I", payload) if payload else ()
            await connection.send_bytes(frame(OP_SUM, struct.pack("!Q", sum(numbers))))
        elif opcode == OP_TIME:
            await connection.send_bytes(frame(OP_TIME, str(time.time()).encode()))
        else:
            await connection.send_bytes(frame(0, f"unknown opcode {opcode}".encode()))


async def run_client() -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", PORT)

    async def call(opcode: int, payload: bytes) -> tuple[int, bytes]:
        writer.write(frame(opcode, payload))
        await writer.drain()
        code, length = HEADER.unpack(await reader.readexactly(HEADER.size))
        return code, await reader.readexactly(length)

    print("ECHO ->", await call(OP_ECHO, b"hello binary world"))
    print("SUM  ->", struct.unpack("!Q", (await call(OP_SUM, struct.pack("!4I", 1, 2, 3, 4)))[1])[0])
    print("TIME ->", (await call(OP_TIME, b""))[1].decode())

    # A 4 MiB frame is refused rather than allocated.
    writer.write(HEADER.pack(OP_ECHO, 4 << 20))
    await writer.drain()
    code, length = HEADER.unpack(await reader.readexactly(HEADER.size))
    print("OVERSIZED ->", (await reader.readexactly(length)).decode())

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "client":
        asyncio.run(run_client())
    else:
        print(f"Length-prefixed TCP protocol on 127.0.0.1:{PORT}")
        print("Try it with:  python examples/tcp_custom_protocol.py client")
        yashserver.run_many(server)
