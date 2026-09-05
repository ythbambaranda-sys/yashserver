"""Reproducible throughput and shutdown benchmarks for every transport.

    python examples/benchmark.py

Numbers are for one machine and one Python build, and both the client and the
server run in this process, so treat them as a regression guard rather than a
competitive claim. What they are good for: noticing when a change makes
something an order of magnitude slower.
"""

from __future__ import annotations

import asyncio
import base64
import os
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver


def report(label: str, count: int, seconds: float, unit: str = "ops") -> None:
    print(f"  {label:<34} {count:>7,} {unit} in {seconds:5.2f}s  =  {count / seconds:>9,.0f} {unit}/s")


async def bench_http(requests: int = 2000, connections: int = 20) -> None:
    app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)

    @app.get("/p")
    async def ping(_request, _server):
        return {"ok": True}

    await app.start()
    port = app.bound_port
    per_connection = requests // connections

    async def worker() -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for _ in range(per_connection):
            writer.write(b"GET /p HTTP/1.1\r\nHost: b\r\n\r\n")
            await writer.drain()
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            length = next(
                int(line.split(":")[1])
                for line in head.split("\r\n")
                if line.lower().startswith("content-length")
            )
            await reader.readexactly(length)
        writer.close()

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(connections)))
    elapsed = time.perf_counter() - started
    await app.stop()
    report(f"HTTP keep-alive ({connections} conns)", per_connection * connections, elapsed, "req")


async def bench_websocket(messages: int = 5000) -> None:
    server = yashserver.YWebSocketServer(host="127.0.0.1", port=0, ddosprot=False, ping_interval_seconds=None)

    @server.route("/e")
    async def echo(_session, message, _server):
        return message

    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    writer.write(
        f"GET /e HTTP/1.1\r\nHost: b\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    await writer.drain()
    await reader.readuntil(b"\r\n\r\n")

    payload = b"x" * 64
    mask = os.urandom(4)
    frame = bytes([0x81, 0x80 | len(payload)]) + mask + bytes(
        value ^ mask[index % 4] for index, value in enumerate(payload)
    )

    started = time.perf_counter()
    for _ in range(messages):
        writer.write(frame)
        await writer.drain()
        _first, second = await reader.readexactly(2)
        await reader.readexactly(second & 0x7F)
    elapsed = time.perf_counter() - started
    writer.close()
    await server.stop()
    report("WebSocket echo round-trip", messages, elapsed, "msg")


async def bench_udp(datagrams: int = 5000) -> None:
    server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)

    @server.on_datagram
    async def echo(datagram, _server):
        return datagram.data

    await server.start()
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    address = ("127.0.0.1", server.bound_port)

    started = time.perf_counter()
    for _ in range(datagrams):
        sock.sendto(b"y" * 64, address)
        await loop.sock_recv(sock, 2048)
    elapsed = time.perf_counter() - started
    sock.close()
    await server.stop()
    report("UDP echo round-trip", datagrams, elapsed, "dgram")


async def bench_broadcast(clients: int = 200, rounds: int = 20) -> None:
    server = yashserver.YWebSocketServer(host="127.0.0.1", port=0, ddosprot=False, ping_interval_seconds=None)

    @server.route("/b")
    async def sink(_session, _message, _server):
        return None

    await server.start()
    connections = []
    for _ in range(clients):
        reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        writer.write(
            f"GET /b HTTP/1.1\r\nHost: b\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
        )
        await writer.drain()
        await reader.readuntil(b"\r\n\r\n")
        connections.append((reader, writer))

    started = time.perf_counter()
    for _ in range(rounds):
        await server.broadcast({"tick": 1})
    elapsed = time.perf_counter() - started
    for _reader, writer in connections:
        writer.close()
    await server.stop()
    report(f"WebSocket broadcast ({clients} clients)", rounds * clients, elapsed, "send")


async def bench_shutdown(peers: int = 500) -> None:
    server = yashserver.YTcpServer(
        host="127.0.0.1",
        port=0,
        ddosprot=False,
        max_connections=None,
        idle_timeout_seconds=None,
    )

    @server.route("ping")
    async def ping(_client, _payload, _server):
        return {"ok": True}

    await server.start()
    connections = []
    for _ in range(peers):
        connections.append(await asyncio.open_connection("127.0.0.1", server.bound_port))
    for _reader, writer in connections:
        writer.write(b"ping\n")
    for reader, _writer in connections:
        await reader.readline()

    live = len(server.clients)
    started = time.perf_counter()
    await server.stop()
    elapsed = time.perf_counter() - started
    for _reader, writer in connections:
        writer.close()
    print(f"  {'TCP graceful shutdown':<34} {live:>7,} live peers closed in {elapsed:5.2f}s")


async def main() -> None:
    print(f"yashserver {yashserver.__version__} on Python {sys.version.split()[0]}")
    print("(client and server share this process, so these are relative numbers)\n")
    await bench_http()
    await bench_websocket()
    await bench_udp()
    await bench_broadcast()
    await bench_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
