from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import HttpError, HttpResponse  # noqa: E402


def _split(raw: bytes) -> tuple[str, bytes]:
    head, _, body = raw.partition(b"\r\n\r\n")
    return head.decode("latin-1"), body


def _header(head: str, name: str) -> str | None:
    for line in head.split("\r\n")[1:]:
        key, _, value = line.partition(":")
        if key.strip().lower() == name.lower():
            return value.strip()
    return None


async def _request(port: int, raw: bytes) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    response = await asyncio.wait_for(reader.read(), timeout=5.0)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return _split(response)


def _get(path: str, extra: str = "") -> bytes:
    return f"GET {path} HTTP/1.1\r\nHost: test\r\n{extra}Connection: close\r\n\r\n".encode()


class _AppCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.configure(self.app)
        await self.app.start()
        self.port = self.app.bound_port

    async def asyncTearDown(self) -> None:
        await self.app.stop()

    def configure(self, app: yashserver.YHttpServer) -> None:  # pragma: no cover - overridden
        pass


class TestRouting(_AppCase):
    def configure(self, app):
        @app.get("/users/{uid}")
        async def user(request, _server):
            return {"uid": request.param("uid")}

        @app.get("/users/{uid}/posts/{pid}")
        async def post(request, _server):
            return {"uid": request.param("uid"), "pid": request.param("pid")}

        @app.get("/assets/{rest:path}")
        async def asset(request, _server):
            return {"rest": request.param("rest")}

        @app.post("/users/{uid}")
        async def create(request, _server):
            return 201, {"created": request.param("uid")}

        @app.get("/plain")
        async def plain(_request, _server):
            return "hello"

        @app.get("/nothing")
        async def nothing(_request, _server):
            return None

    async def test_single_path_parameter(self) -> None:
        head, body = await _request(self.port, _get("/users/42"))
        self.assertIn("200 OK", head)
        self.assertEqual(json.loads(body), {"uid": "42"})

    async def test_multiple_path_parameters(self) -> None:
        _head, body = await _request(self.port, _get("/users/7/posts/9"))
        self.assertEqual(json.loads(body), {"uid": "7", "pid": "9"})

    async def test_catch_all_parameter_keeps_slashes(self) -> None:
        _head, body = await _request(self.port, _get("/assets/img/logo/big.png"))
        self.assertEqual(json.loads(body), {"rest": "img/logo/big.png"})

    async def test_percent_encoded_parameters_are_decoded(self) -> None:
        _head, body = await _request(self.port, _get("/users/ada%20lovelace"))
        self.assertEqual(json.loads(body), {"uid": "ada lovelace"})

    async def test_method_selects_the_handler(self) -> None:
        head, body = await _request(
            self.port,
            b"POST /users/3 HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        self.assertIn("201 Created", head)
        self.assertEqual(json.loads(body), {"created": "3"})

    async def test_unknown_path_is_404(self) -> None:
        head, _body = await _request(self.port, _get("/missing"))
        self.assertIn("404 Not Found", head)

    async def test_wrong_method_is_405_with_allow(self) -> None:
        head, _body = await _request(
            self.port,
            b"DELETE /users/1 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        self.assertIn("405 Method Not Allowed", head)
        allow = _header(head, "Allow") or ""
        self.assertIn("GET", allow)
        self.assertIn("POST", allow)

    async def test_head_uses_the_get_route_without_a_body(self) -> None:
        head, body = await _request(
            self.port,
            b"HEAD /users/9 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        self.assertIn("200 OK", head)
        self.assertEqual(body, b"")
        # The advertised length still matches what GET would have returned.
        self.assertEqual(_header(head, "Content-Length"), str(len(b'{"uid":"9"}')))

    async def test_return_types_are_normalised(self) -> None:
        head, body = await _request(self.port, _get("/plain"))
        self.assertEqual(body, b"hello")
        self.assertIn("text/plain", _header(head, "Content-Type") or "")

        head, body = await _request(self.port, _get("/nothing"))
        self.assertIn("204 No Content", head)
        self.assertEqual(body, b"")
        self.assertIsNone(_header(head, "Content-Length"))


class TestBodies(_AppCase):
    def configure(self, app):
        @app.post("/echo")
        async def echo(request, _server):
            return {"len": len(request.body), "text": request.text()}

        @app.post("/json")
        async def as_json(request, _server):
            return {"received": request.json()}

        @app.post("/form")
        async def as_form(request, _server):
            return request.form()

        @app.post("/upload", stream=True)
        async def upload(request, _server):
            total = 0
            chunks = 0
            async for chunk in request.stream(chunk_size=8):
                total += len(chunk)
                chunks += 1
            return {"bytes": total, "chunks": chunks}

    async def test_content_length_body(self) -> None:
        raw = b"POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
        _head, body = await _request(self.port, raw)
        self.assertEqual(json.loads(body), {"len": 5, "text": "hello"})

    async def test_chunked_request_body(self) -> None:
        raw = (
            b"POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        )
        _head, body = await _request(self.port, raw)
        self.assertEqual(json.loads(body), {"len": 11, "text": "hello world"})

    async def test_json_and_form_helpers(self) -> None:
        payload = b'{"a": 1}'
        raw = (
            b"POST /json HTTP/1.1\r\nHost: t\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(payload)).encode() + b"\r\nConnection: close\r\n\r\n" + payload
        )
        _head, body = await _request(self.port, raw)
        self.assertEqual(json.loads(body), {"received": {"a": 1}})

        form = b"name=ada&lang=py"
        raw = (
            b"POST /form HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(form)).encode()
            + b"\r\nConnection: close\r\n\r\n" + form
        )
        _head, body = await _request(self.port, raw)
        self.assertEqual(json.loads(body), {"name": ["ada"], "lang": ["py"]})

    async def test_streaming_route_never_buffers_the_body(self) -> None:
        payload = b"0123456789" * 3
        raw = (
            b"POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(payload)).encode()
            + b"\r\nConnection: close\r\n\r\n" + payload
        )
        _head, body = await _request(self.port, raw)
        result = json.loads(body)
        self.assertEqual(result["bytes"], 30)
        self.assertGreater(result["chunks"], 1)

    async def test_streaming_routes_are_not_capped_by_max_body_bytes(self) -> None:
        # max_body_bytes protects memory for buffered bodies. A streaming body
        # never lands in memory, so applying that cap to it would make large
        # uploads impossible for no benefit.
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False, max_body_bytes=1024)

        @app.post("/stream", stream=True)
        async def streamed(request, _server):
            total = 0
            async for chunk in request.stream(chunk_size=4096):
                total += len(chunk)
            return {"bytes": total}

        @app.post("/buffered")
        async def buffered(request, _server):
            return {"bytes": len(request.body)}

        await app.start()
        try:
            payload = b"x" * (256 * 1024)  # 256x the buffered cap
            raw = (
                b"POST /stream HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n" + payload
            )
            head, body = await _request(app.bound_port, raw)
            self.assertIn("200 OK", head)
            self.assertEqual(json.loads(body), {"bytes": len(payload)})

            # The buffered route still enforces the cap.
            raw = (
                b"POST /buffered HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n" + payload
            )
            head, _body = await _request(app.bound_port, raw)
            self.assertIn("413", head)
        finally:
            await app.stop()

    async def test_streaming_routes_can_still_be_capped_explicitly(self) -> None:
        app = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            ddosprot=False,
            max_stream_body_bytes=1024,
        )

        @app.post("/stream", stream=True)
        async def streamed(request, _server):
            total = 0
            async for chunk in request.stream(chunk_size=256):
                total += len(chunk)
            return {"bytes": total}

        await app.start()
        try:
            payload = b"x" * 8192
            raw = (
                b"POST /stream HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n" + payload
            )
            head, _body = await _request(app.bound_port, raw)
            self.assertIn("413", head)
        finally:
            await app.stop()

    async def test_expect_100_continue_is_answered_on_streaming_routes(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)

        @app.post("/stream", stream=True)
        async def streamed(request, _server):
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
            return {"bytes": total}

        await app.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
            writer.write(
                b"POST /stream HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n"
                b"Expect: 100-continue\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
            interim = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2.0)
            self.assertIn(b"100 Continue", interim)

            writer.write(b"hello")
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), timeout=2.0)
            self.assertIn(b'{"bytes":5}', response)
            writer.close()
        finally:
            await app.stop()

    async def test_body_over_the_limit_is_413(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False, max_body_bytes=16)

        @app.post("/echo")
        async def echo(request, _server):
            return {"len": len(request.body)}

        await app.start()
        try:
            payload = b"x" * 64
            raw = (
                b"POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n" + payload
            )
            head, _body = await _request(app.bound_port, raw)
            self.assertIn("413", head)
        finally:
            await app.stop()


class TestStreamingResponses(_AppCase):
    def configure(self, app):
        @app.get("/stream")
        async def stream(_request, _server):
            async def produce():
                for index in range(4):
                    yield f"part{index};".encode()

            return HttpResponse.stream_response(produce(), content_type="text/plain")

    async def test_chunked_transfer_encoding(self) -> None:
        head, body = await _request(self.port, _get("/stream"))
        self.assertEqual(_header(head, "Transfer-Encoding"), "chunked")
        self.assertIsNone(_header(head, "Content-Length"))
        self.assertIn(b"part0;", body)
        self.assertIn(b"part3;", body)
        self.assertTrue(body.endswith(b"0\r\n\r\n"))

    async def test_streamed_body_is_skipped_for_head(self) -> None:
        head, body = await _request(
            self.port,
            b"HEAD /stream HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        self.assertIn("200 OK", head)
        self.assertEqual(body, b"")


class TestFileTransfers(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = Path(tempfile.mkdtemp())
        self.payload = bytes(range(256)) * 500  # 128000 bytes
        (self.directory / "big.bin").write_bytes(self.payload)
        (self.directory / "note.txt").write_text("hello file")

        self.app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.app.static("/files", self.directory)
        await self.app.start()
        self.port = self.app.bound_port

    async def asyncTearDown(self) -> None:
        await self.app.stop()

    async def test_whole_file_is_streamed(self) -> None:
        head, body = await _request(self.port, _get("/files/big.bin"))
        self.assertIn("200 OK", head)
        self.assertEqual(_header(head, "Content-Length"), str(len(self.payload)))
        self.assertEqual(body, self.payload)
        self.assertEqual(_header(head, "Accept-Ranges"), "bytes")

    async def test_range_request_returns_partial_content(self) -> None:
        head, body = await _request(self.port, _get("/files/big.bin", "Range: bytes=100-199\r\n"))
        self.assertIn("206 Partial Content", head)
        self.assertEqual(_header(head, "Content-Range"), f"bytes 100-199/{len(self.payload)}")
        self.assertEqual(body, self.payload[100:200])

    async def test_suffix_range(self) -> None:
        head, body = await _request(self.port, _get("/files/big.bin", "Range: bytes=-50\r\n"))
        self.assertIn("206 Partial Content", head)
        self.assertEqual(body, self.payload[-50:])

    async def test_unsatisfiable_range_is_416(self) -> None:
        head, _body = await _request(self.port, _get("/files/big.bin", "Range: bytes=999999-\r\n"))
        self.assertIn("416", head)

    async def test_etag_produces_304(self) -> None:
        head, _body = await _request(self.port, _get("/files/note.txt"))
        etag = _header(head, "ETag")
        self.assertIsNotNone(etag)

        head, body = await _request(self.port, _get("/files/note.txt", f"If-None-Match: {etag}\r\n"))
        self.assertIn("304 Not Modified", head)
        self.assertEqual(body, b"")

    async def test_content_type_is_guessed(self) -> None:
        head, _body = await _request(self.port, _get("/files/note.txt"))
        self.assertIn("text/plain", _header(head, "Content-Type") or "")

    async def test_path_traversal_is_refused(self) -> None:
        head, _body = await _request(self.port, _get("/files/../../etc/passwd"))
        # Either the traversal is rejected outright or it simply does not
        # resolve to a file; what must never happen is a 200 with content.
        self.assertFalse("200 OK" in head, head)

    async def test_missing_file_is_404(self) -> None:
        head, _body = await _request(self.port, _get("/files/nope.bin"))
        self.assertIn("404", head)


class TestKeepAlive(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)

        @self.app.get("/n")
        async def counter(_request, _server):
            return {"ok": True}

        @self.app.post("/sink")
        async def sink(request, _server):
            return {"len": len(request.body)}

        await self.app.start()
        self.port = self.app.bound_port

    async def asyncTearDown(self) -> None:
        await self.app.stop()

    async def _read_one(self, reader: asyncio.StreamReader) -> tuple[str, bytes]:
        head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
        length = int(_header(head, "Content-Length") or 0)
        return head, await reader.readexactly(length)

    async def test_multiple_requests_on_one_connection(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        try:
            for _ in range(3):
                writer.write(b"GET /n HTTP/1.1\r\nHost: t\r\n\r\n")
                await writer.drain()
                head, body = await self._read_one(reader)
                self.assertIn("200 OK", head)
                self.assertEqual(_header(head, "Connection"), "keep-alive")
                self.assertEqual(json.loads(body), {"ok": True})
            self.assertEqual(self.app.metrics.counter("requests"), 3)
            self.assertEqual(self.app.metrics.counter("connections_opened"), 1)
        finally:
            writer.close()

    async def test_unread_body_does_not_desync_the_next_request(self) -> None:
        # /n ignores the request body, so the server must drain it before
        # parsing the following request off the same connection.
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        try:
            writer.write(
                b"POST /sink HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello"
                b"GET /n HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
            head, body = await self._read_one(reader)
            self.assertEqual(json.loads(body), {"len": 5})
            head, body = await self._read_one(reader)
            self.assertIn("200 OK", head)
            self.assertEqual(json.loads(body), {"ok": True})
        finally:
            writer.close()

    async def test_connection_close_is_honoured(self) -> None:
        head, _body = await _request(self.port, _get("/n"))
        self.assertEqual(_header(head, "Connection"), "close")


class TestMiddlewareAndErrors(_AppCase):
    def configure(self, app):
        self.order: list[str] = []

        @app.middleware
        async def outer(request, call_next):
            self.order.append("outer-in")
            response = await call_next(request)
            self.order.append("outer-out")
            return response.set_header("X-Outer", "1")

        @app.middleware
        async def inner(request, call_next):
            self.order.append("inner-in")
            request.state["user"] = "ada"
            response = await call_next(request)
            self.order.append("inner-out")
            return response.set_header("X-Inner", "1")

        @app.get("/who")
        async def who(request, _server):
            return {"user": request.state.get("user")}

        @app.get("/teapot")
        async def teapot(_request, _server):
            raise HttpError(418, "no coffee here")

        @app.get("/crash")
        async def crash(_request, _server):
            raise RuntimeError("unexpected")

        @app.get("/gate")
        async def gate(_request, _server):
            return {"never": True}

        @app.middleware
        async def guard(request, call_next):
            if request.path == "/gate" and not request.query("pass"):
                raise HttpError(403, "denied by middleware")
            return await call_next(request)

        @app.error_handler(404)
        async def not_found(request, _error):
            return 404, {"custom": True, "path": request.path}

    async def test_middleware_runs_outermost_first_and_shares_state(self) -> None:
        head, body = await _request(self.port, _get("/who"))
        self.assertEqual(json.loads(body), {"user": "ada"})
        self.assertEqual(_header(head, "X-Outer"), "1")
        self.assertEqual(_header(head, "X-Inner"), "1")
        self.assertEqual(
            self.order,
            ["outer-in", "inner-in", "inner-out", "outer-out"],
        )

    async def test_middleware_can_reject_a_request(self) -> None:
        head, body = await _request(self.port, _get("/gate"))
        self.assertIn("403 Forbidden", head)
        self.assertIn("denied by middleware", body.decode())

        head, body = await _request(self.port, _get("/gate?pass=1"))
        self.assertIn("200 OK", head)

    async def test_http_error_maps_to_its_status(self) -> None:
        head, body = await _request(self.port, _get("/teapot"))
        self.assertIn("418", head)
        payload = json.loads(body)
        self.assertEqual(payload["status"], 418)
        self.assertEqual(payload["detail"], "no coffee here")

    async def test_unexpected_exception_is_500_and_is_reported(self) -> None:
        head, _body = await _request(self.port, _get("/crash"))
        self.assertIn("500 Internal Server Error", head)
        self.assertGreaterEqual(self.app.metrics.counter("errors"), 1)

    async def test_custom_error_handler(self) -> None:
        head, body = await _request(self.port, _get("/does-not-exist"))
        self.assertIn("404 Not Found", head)
        self.assertEqual(json.loads(body), {"custom": True, "path": "/does-not-exist"})


class TestProtocolHandling(_AppCase):
    def configure(self, app):
        @app.get("/ok")
        async def ok(_request, _server):
            return "fine"

    async def test_malformed_request_line_is_400(self) -> None:
        head, _body = await _request(self.port, b"GARBAGE\r\n\r\n")
        self.assertIn("400 Bad Request", head)

    async def test_invalid_content_length_is_400(self) -> None:
        raw = b"POST /ok HTTP/1.1\r\nHost: t\r\nContent-Length: abc\r\nConnection: close\r\n\r\n"
        head, _body = await _request(self.port, raw)
        self.assertIn("400 Bad Request", head)

    async def test_oversized_headers_are_431(self) -> None:
        raw = b"GET /ok HTTP/1.1\r\nHost: t\r\nX-Big: " + b"a" * 40000 + b"\r\nConnection: close\r\n\r\n"
        head, _body = await _request(self.port, raw)
        self.assertIn("431", head)

    async def test_repeated_headers_are_joined(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)

        @app.get("/h")
        async def show(request, _server):
            return {"accept": request.header("accept")}

        await app.start()
        try:
            raw = b"GET /h HTTP/1.1\r\nHost: t\r\nAccept: a\r\nAccept: b\r\nConnection: close\r\n\r\n"
            _head, body = await _request(app.bound_port, raw)
            self.assertEqual(json.loads(body), {"accept": "a, b"})
        finally:
            await app.stop()

    async def test_standard_response_headers(self) -> None:
        head, _body = await _request(self.port, _get("/ok"))
        self.assertEqual(_header(head, "Server"), "yashserver")
        self.assertIsNotNone(_header(head, "Date"))


class TestAuthAndLimits(unittest.IsolatedAsyncioTestCase):
    async def test_exempt_paths_bypass_auth(self) -> None:
        app = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            auth_token="secret",
            auth_exempt_paths={"/health"},
            ddosprot=False,
        )

        @app.get("/health")
        async def health(_request, _server):
            return {"ok": True}

        @app.get("/private")
        async def private(_request, _server):
            return {"ok": True}

        await app.start()
        try:
            head, _body = await _request(app.bound_port, _get("/health"))
            self.assertIn("200 OK", head)

            head, _body = await _request(app.bound_port, _get("/private"))
            self.assertIn("401 Unauthorized", head)
            self.assertEqual(_header(head, "WWW-Authenticate"), "Bearer")

            head, _body = await _request(app.bound_port, _get("/private?token=secret"))
            self.assertIn("200 OK", head)
        finally:
            await app.stop()

    async def test_custom_auth_validator(self) -> None:
        async def validator(context):
            return context["headers"].get("x-api-key") == "letmein"

        app = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            auth=yashserver.AuthConfig(validator=validator),
            ddosprot=False,
        )

        @app.get("/x")
        async def handler(_request, _server):
            return {"ok": True}

        await app.start()
        try:
            head, _body = await _request(app.bound_port, _get("/x"))
            self.assertIn("401", head)
            head, _body = await _request(app.bound_port, _get("/x", "X-Api-Key: letmein\r\n"))
            self.assertIn("200 OK", head)
        finally:
            await app.stop()

    async def test_rate_limit_sets_retry_after(self) -> None:
        app = yashserver.YHttpServer(
            host="127.0.0.1",
            port=0,
            rate_limit_per_window=1,
            rate_limit_window_seconds=60.0,
        )

        @app.get("/x")
        async def handler(_request, _server):
            return "ok"

        await app.start()
        try:
            head, _body = await _request(app.bound_port, _get("/x"))
            self.assertIn("200 OK", head)

            head, body = await _request(app.bound_port, _get("/x"))
            self.assertIn("429 Too Many Requests", head)
            self.assertIsNotNone(_header(head, "Retry-After"))
            self.assertIn(b"Blocked by Yashserver", body)
        finally:
            await app.stop()


if __name__ == "__main__":
    unittest.main()
