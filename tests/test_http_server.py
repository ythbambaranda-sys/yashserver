from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver

DDOS_BLOCK_MESSAGE = "<red_gradiat>Yashserver<orange_gradiate> <plain_black> blocked you for suspecting of DDOSing<plain_black>"


async def _http_get(port: int, path: str, *, token: str | None = None) -> str:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    headers = [
        f"GET {path} HTTP/1.1",
        "Host: 127.0.0.1",
        "Connection: close",
    ]
    if token:
        headers.append(f"Authorization: Bearer {token}")
    headers.append("")
    headers.append("")
    writer.write("\r\n".join(headers).encode("utf-8"))
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    return response.decode("utf-8", errors="replace")


class TestHttpServer(unittest.IsolatedAsyncioTestCase):
    async def test_http_auth_and_rate_limit(self) -> None:
        server = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            auth_token="secret-token",
            rate_limit_per_window=1,
            rate_limit_window_seconds=60.0,
        )

        @server.get("/ok")
        async def ok(_request, _server):
            return yashserver.YHttpServer.text("ok")

        await server.start()
        assert server._server is not None
        port = int(server._server.sockets[0].getsockname()[1])

        try:
            unauthorized = await _http_get(port, "/ok")
            self.assertIn("401 Unauthorized", unauthorized)

            authorized = await _http_get(port, "/ok?token=secret-token")
            self.assertIn("200 OK", authorized)

            rate_limited = await _http_get(port, "/ok?token=secret-token")
            self.assertIn("429 Too Many Requests", rate_limited)
            self.assertIn(DDOS_BLOCK_MESSAGE, rate_limited)
        finally:
            await server.stop()

    async def test_http_setddosprot_controls_rate_limit(self) -> None:
        server = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            rate_limit_per_window=1,
            rate_limit_window_seconds=60.0,
            ddosprot=False,
        )

        @server.get("/ok")
        async def ok(_request, _server):
            return yashserver.YHttpServer.text("ok")

        server.setddosprot(True)
        await server.start()
        assert server._server is not None
        port = int(server._server.sockets[0].getsockname()[1])

        try:
            first = await _http_get(port, "/ok")
            self.assertIn("200 OK", first)

            second = await _http_get(port, "/ok")
            self.assertIn("429 Too Many Requests", second)
            self.assertIn(DDOS_BLOCK_MESSAGE, second)
        finally:
            await server.stop()

    async def test_http_setddosprot_numeric_limit(self) -> None:
        server = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=True,
        )
        server.setddosprot(4, rate_limit_window_seconds=1.0)

        @server.get("/ok")
        async def ok(_request, _server):
            return yashserver.YHttpServer.text("ok")

        await server.start()
        assert server._server is not None
        port = int(server._server.sockets[0].getsockname()[1])

        try:
            for _ in range(4):
                allowed = await _http_get(port, "/ok")
                self.assertIn("200 OK", allowed)

            blocked = await _http_get(port, "/ok")
            self.assertIn("429 Too Many Requests", blocked)
            self.assertIn(DDOS_BLOCK_MESSAGE, blocked)
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
