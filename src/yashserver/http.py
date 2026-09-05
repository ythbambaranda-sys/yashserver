"""Async HTTP/1.1 server.

Covers what a real HTTP application needs: keep-alive connections, path
parameters, middleware, JSON APIs, streaming request and response bodies,
range-aware large-file transfers, authentication, rate limiting and correct
status/error handling.

The response contract is deliberately forgiving. A handler may return:

===========================  ==========================================
returned value               becomes
===========================  ==========================================
``None``                     ``204 No Content``
``str``                      ``200`` ``text/plain``
``bytes``                    ``200`` ``application/octet-stream``
``dict`` / ``list`` / other  ``200`` ``application/json``
``(status, body)``           that status
``(status, body, headers)``  that status with extra headers
:class:`HttpResponse`        used as-is (including streaming bodies)
===========================  ==========================================
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import ssl
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Sequence
from urllib.parse import parse_qs, unquote, urlparse

from .core import (
    AuthConfig,
    BaseServer,
    ConfigError,
    RateLimitConfig,
    ServerConfig,
    ServerState,
    TLSConfig,
    close_listener_quietly,
    close_writer_quietly,
    format_peer_name,
    maybe_await,
    resolve_ssl_context,
)
from .tools import ServerTools

__all__ = [
    "HttpConfig",
    "HttpError",
    "HttpRequest",
    "HttpResponse",
    "YHttpServer",
    "file_response",
]

DDOS_BLOCK_MESSAGE = (
    "<red_gradiat>Yashserver<orange_gradiate> <plain_black> blocked you for suspecting of DDOSing<plain_black>"
)
_DDOS_BLOCK_PAGE_PATH = Path(__file__).with_name("ddosprotblockedmsg.html")
_DDOS_BLOCK_PAGE_CANONICAL = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Blocked by Yashserver</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: linear-gradient(135deg, #ffefe6, #ffe2d9 50%, #ffd4c7);
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: #2e1b16;
    }}
    .panel {{
      width: min(760px, 92vw);
      background: #fff8f5;
      border: 2px solid #f4b49f;
      border-radius: 18px;
      box-shadow: 0 14px 38px rgba(160, 64, 24, 0.2);
      padding: 34px 38px;
      text-align: center;
    }}
    h1 {{
      margin: 0 0 14px;
      font-size: 26px;
      letter-spacing: 0.2px;
    }}
    p {{
      margin: 0;
      font-size: 18px;
      line-height: 1.45;
      word-break: break-word;
    }}
  </style>
</head>
<body>
  <section class="panel">
    <h1>Blocked by Yashserver</h1>
    <p>{DDOS_BLOCK_MESSAGE}</p>
  </section>
</body>
</html>
"""


def _load_ddos_block_page_bytes() -> bytes:
    try:
        content = _DDOS_BLOCK_PAGE_PATH.read_text(encoding="utf-8")
    except OSError:
        content = _DDOS_BLOCK_PAGE_CANONICAL
    if content != _DDOS_BLOCK_PAGE_CANONICAL:
        content = _DDOS_BLOCK_PAGE_CANONICAL
    return content.encode("utf-8")


def status_reason(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Unknown"


def http_date(when: float | None = None) -> str:
    return formatdate(timeval=when, usegmt=True)


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class HttpError(Exception):
    """Raise from a handler or middleware to produce a specific status."""

    def __init__(
        self,
        status: int,
        detail: str | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = int(status)
        self.detail = detail or status_reason(status)
        self.headers = headers or {}
        super().__init__(f"{self.status} {self.detail}")

    def to_response(self) -> "HttpResponse":
        return HttpResponse.json_response(
            {"error": status_reason(self.status), "detail": self.detail, "status": self.status},
            status=self.status,
            headers=self.headers,
        )


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@dataclass
class HttpConfig(ServerConfig):
    """HTTP-specific options."""

    #: Reuse a connection for more than one request.
    keep_alive: bool = True
    #: Idle seconds allowed between requests on a kept-alive connection.
    keep_alive_timeout_seconds: float = 5.0
    #: Requests served per connection before it is closed.
    max_requests_per_connection: int = 100
    #: Largest request head (request line plus headers) accepted.
    max_header_bytes: int = 16 * 1024
    #: Largest body buffered into ``request.body``, in bytes. This limit
    #: protects memory, because a buffered body is held in RAM in full.
    max_body_bytes: int = 16 * 1024 * 1024
    #: Largest body accepted on a route declared ``stream=True``. A streamed
    #: body is never held in memory, so this is a policy limit rather than a
    #: memory one; ``None`` (the default) means no limit.
    max_stream_body_bytes: int | None = None
    #: Seconds allowed to read one complete request head plus body.
    request_timeout_seconds: float = 60.0
    #: Seconds a single response write may block before the peer is dropped.
    write_timeout_seconds: float = 60.0
    #: Concurrent connection ceiling. ``None`` for no cap.
    max_connections: int | None = 10_000
    #: Chunk size for streamed responses and file transfers.
    stream_chunk_size: int = 64 * 1024
    #: After rejecting a request whose body is still arriving, keep absorbing
    #: it for this long so the client can read the status instead of getting a
    #: connection reset. 0 disables.
    lingering_close_seconds: float = 2.0
    lingering_close_max_bytes: int = 16 * 1024 * 1024
    #: Value of the ``Server`` response header.
    server_header: str = "yashserver"
    #: asyncio write-buffer watermarks; this is where backpressure comes from.
    write_buffer_high_bytes: int = 256 * 1024
    write_buffer_low_bytes: int = 64 * 1024
    backlog: int = 128


# ---------------------------------------------------------------------------
# request
# ---------------------------------------------------------------------------


class _BodyReader:
    """Reads a request body, whether length-delimited or chunked."""

    __slots__ = ("_reader", "_remaining", "_chunked", "_done", "_max_bytes", "_consumed")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        *,
        content_length: int | None,
        chunked: bool,
        max_bytes: int | None,
    ) -> None:
        self._reader = reader
        self._remaining = content_length
        self._chunked = chunked
        self._done = not chunked and not content_length
        self._max_bytes = max_bytes
        self._consumed = 0

    @property
    def exhausted(self) -> bool:
        return self._done

    @property
    def consumed(self) -> int:
        return self._consumed

    async def chunks(self, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        if self._done:
            return
        if self._chunked:
            async for chunk in self._read_chunked():
                yield chunk
            return

        remaining = self._remaining or 0
        while remaining > 0:
            piece = await self._reader.read(min(chunk_size, remaining))
            if not piece:
                self._done = True
                raise HttpError(400, "request body ended early")
            remaining -= len(piece)
            self._remaining = remaining
            self._track(len(piece))
            yield piece
        self._done = True

    async def _read_chunked(self) -> AsyncIterator[bytes]:
        while True:
            try:
                size_line = await self._reader.readuntil(b"\r\n")
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as error:
                raise HttpError(400, "malformed chunked body") from error

            size_text = size_line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError as error:
                raise HttpError(400, "malformed chunk size") from error

            if size == 0:
                # Consume the trailer section.
                while True:
                    line = await self._reader.readuntil(b"\r\n")
                    if line in (b"\r\n", b"\n"):
                        break
                self._done = True
                return

            self._track(size)
            data = await self._reader.readexactly(size)
            await self._reader.readexactly(2)  # trailing CRLF
            yield data

    def set_limit(self, max_bytes: int | None) -> None:
        """Set the ceiling for this body. ``None`` means unlimited.

        Called once routing has decided whether the route streams: a buffered
        body is capped to protect memory, a streamed one need not be.
        """

        self._max_bytes = max_bytes
        if max_bytes is not None and self._consumed > max_bytes:
            raise HttpError(413, f"request body exceeds {max_bytes} bytes")

    def _track(self, size: int) -> None:
        self._consumed += size
        if self._max_bytes is not None and self._consumed > self._max_bytes:
            raise HttpError(413, f"request body exceeds {self._max_bytes} bytes")

    async def read_all(self) -> bytes:
        buffer = bytearray()
        async for chunk in self.chunks():
            buffer.extend(chunk)
        return bytes(buffer)

    async def discard(self, limit: int = 1024 * 1024) -> bool:
        """Drain an unread body so the connection can be reused.

        Returns ``False`` when the body is too large to be worth draining, in
        which case the caller should close the connection instead.
        """

        if self._done:
            return True
        drained = 0
        try:
            async for chunk in self.chunks():
                drained += len(chunk)
                if drained > limit:
                    return False
        except Exception:
            return False
        return True


class HttpRequest:
    """One HTTP request."""

    __slots__ = (
        "method",
        "path",
        "raw_target",
        "query_params",
        "headers",
        "body",
        "remote_addr",
        "http_version",
        "path_params",
        "received_at",
        "id",
        "state",
        "_body_reader",
        "_body_loaded",
    )

    def __init__(
        self,
        method: str,
        path: str,
        query_params: dict[str, list[str]] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        remote_addr: str = "unknown",
        *,
        raw_target: str = "",
        http_version: str = "HTTP/1.1",
        path_params: dict[str, str] | None = None,
        body_reader: _BodyReader | None = None,
    ) -> None:
        self.method = method
        self.path = path
        self.raw_target = raw_target or path
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.body = body
        self.remote_addr = remote_addr
        self.http_version = http_version
        self.path_params: dict[str, str] = path_params or {}
        self.received_at = datetime.now(timezone.utc)
        self.id = uuid.uuid4().hex
        #: Free-form per-request scratch space for middleware (auth identity,
        #: trace ids, whatever your application needs downstream).
        self.state: dict[str, Any] = {}
        self._body_reader = body_reader
        self._body_loaded = body_reader is None

    # -- header helpers --------------------------------------------------

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    def query(self, name: str, default: str | None = None) -> str | None:
        values = self.query_params.get(name)
        return values[0] if values else default

    def param(self, name: str, default: str | None = None) -> str | None:
        return self.path_params.get(name, default)

    @property
    def content_type(self) -> str:
        return (self.headers.get("content-type") or "").split(";", 1)[0].strip().lower()

    @property
    def content_length(self) -> int | None:
        raw = self.headers.get("content-length")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    @property
    def is_secure(self) -> bool:
        return bool(self.state.get("tls"))

    @property
    def wants_keep_alive(self) -> bool:
        connection = (self.headers.get("connection") or "").lower()
        if self.http_version == "HTTP/1.0":
            return "keep-alive" in connection
        return "close" not in connection

    # -- body helpers ----------------------------------------------------

    async def stream(self, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        """Iterate the request body without buffering it.

        Only meaningful on routes registered with ``stream=True``; buffered
        routes have already read the body into :attr:`body`.
        """

        if self._body_loaded:
            if self.body:
                yield self.body
            return
        assert self._body_reader is not None
        async for chunk in self._body_reader.chunks(chunk_size):
            yield chunk
        self._body_loaded = True

    async def read_body(self) -> bytes:
        """Buffer and return the whole body, reading it if needed."""

        if not self._body_loaded and self._body_reader is not None:
            self.body = await self._body_reader.read_all()
            self._body_loaded = True
        return self.body

    def text(self, errors: str = "replace") -> str:
        return self.body.decode("utf-8", errors=errors)

    def json(self, default: Any = None) -> Any:
        return ServerTools.from_json(self.text(), default=default)

    def form(self) -> dict[str, list[str]]:
        """Parse an ``application/x-www-form-urlencoded`` body."""

        return parse_qs(self.text(), keep_blank_values=True)

    def __repr__(self) -> str:
        return f"<HttpRequest {self.method} {self.path} from {self.remote_addr}>"


# ---------------------------------------------------------------------------
# response
# ---------------------------------------------------------------------------

@dataclass
class HttpResponse:
    """An HTTP response, optionally with a streaming body.

    Set ``body`` to an async iterator (or any iterable of ``bytes``) to stream;
    the server then uses chunked transfer encoding unless you also set a
    ``Content-Length``.
    """

    status: int = 200
    body: Any = b""
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str | None = None
    #: Force streaming on or off. ``None`` infers it from ``body``.
    streaming: bool | None = None

    def __post_init__(self) -> None:
        self.headers = {key: value for key, value in (self.headers or {}).items()}
        if self.content_type and not self._has_header("content-type"):
            self.headers["Content-Type"] = self.content_type

    def _has_header(self, name: str) -> bool:
        lowered = name.lower()
        return any(key.lower() == lowered for key in self.headers)

    def set_header(self, name: str, value: str) -> "HttpResponse":
        for key in list(self.headers):
            if key.lower() == name.lower():
                del self.headers[key]
        self.headers[name] = value
        return self

    def get_header(self, name: str) -> str | None:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None

    @property
    def is_streaming(self) -> bool:
        """Whether the body is produced incrementally rather than in one piece.

        Inferred from the body being an async iterator or an iterator. A
        ``dict`` or ``list`` is data to serialise as JSON, not a stream to
        iterate, so only genuine iterators qualify.
        """

        if self.streaming is not None:
            return self.streaming
        body = self.body
        if body is None or isinstance(body, (bytes, bytearray, str)):
            return False
        return hasattr(body, "__aiter__") or hasattr(body, "__next__")

    # -- constructors -----------------------------------------------------

    @classmethod
    def text_response(
        cls, content: str, status: int = 200, headers: dict[str, str] | None = None
    ) -> "HttpResponse":
        return cls(status=status, body=content, headers=headers or {}, content_type="text/plain; charset=utf-8")

    @classmethod
    def html_response(
        cls, content: str, status: int = 200, headers: dict[str, str] | None = None
    ) -> "HttpResponse":
        return cls(status=status, body=content, headers=headers or {}, content_type="text/html; charset=utf-8")

    @classmethod
    def json_response(
        cls, data: Any, status: int = 200, headers: dict[str, str] | None = None
    ) -> "HttpResponse":
        return cls(
            status=status,
            body=ServerTools.to_json(data),
            headers=headers or {},
            content_type="application/json; charset=utf-8",
        )

    @classmethod
    def redirect(cls, location: str, status: int = 302) -> "HttpResponse":
        return cls(status=status, body=b"", headers={"Location": location})

    @classmethod
    def stream_response(
        cls,
        chunks: Any,
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/octet-stream",
    ) -> "HttpResponse":
        return cls(
            status=status,
            body=chunks,
            headers=headers or {},
            content_type=content_type,
            streaming=True,
        )


def _parse_range(header: str, size: int) -> tuple[int, int] | None:
    """Parse a single-range ``Range: bytes=a-b`` header."""

    if not header.lower().startswith("bytes="):
        return None
    spec = header[6:].split(",", 1)[0].strip()
    if "-" not in spec:
        return None
    start_text, _, end_text = spec.partition("-")
    try:
        if not start_text:
            length = int(end_text)
            if length <= 0:
                return None
            start = max(0, size - length)
            end = size - 1
        else:
            start = int(start_text)
            end = int(end_text) if end_text else size - 1
    except ValueError:
        return None
    if start > end or start >= size:
        return None
    return start, min(end, size - 1)


def file_response(
    path: str | os.PathLike[str],
    request: HttpRequest | None = None,
    *,
    chunk_size: int = 64 * 1024,
    content_type: str | None = None,
    download_name: str | None = None,
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    """Stream a file from disk, honouring ``Range`` and conditional requests.

    The file is read in ``chunk_size`` pieces, so serving a 4 GB video costs
    one chunk of memory, not four gigabytes.
    """

    file_path = Path(path)
    try:
        stat = file_path.stat()
    except OSError as error:
        raise HttpError(404, f"file not found: {file_path.name}") from error
    if not file_path.is_file():
        raise HttpError(404, f"not a file: {file_path.name}")

    size = stat.st_size
    etag = f'"{stat.st_mtime_ns:x}-{size:x}"'
    last_modified = http_date(stat.st_mtime)
    guessed = content_type or mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"

    base_headers = {
        "Content-Type": guessed,
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Last-Modified": last_modified,
    }
    if download_name:
        base_headers["Content-Disposition"] = f'attachment; filename="{download_name}"'
    if headers:
        base_headers.update(headers)

    if request is not None and _not_modified(request, etag, stat.st_mtime):
        return HttpResponse(status=304, body=b"", headers=base_headers)

    start, end = 0, size - 1
    status = 200
    if request is not None:
        raw_range = request.header("range")
        if raw_range:
            parsed = _parse_range(raw_range, size)
            if parsed is None:
                return HttpResponse(
                    status=416,
                    body=b"",
                    headers={**base_headers, "Content-Range": f"bytes */{size}"},
                )
            start, end = parsed
            status = 206
            base_headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    length = 0 if size == 0 else end - start + 1
    base_headers["Content-Length"] = str(length)

    async def produce() -> AsyncIterator[bytes]:
        remaining = length
        loop = asyncio.get_running_loop()
        handle = await loop.run_in_executor(None, lambda: open(file_path, "rb"))
        try:
            if start:
                await loop.run_in_executor(None, handle.seek, start)
            while remaining > 0:
                piece = await loop.run_in_executor(None, handle.read, min(chunk_size, remaining))
                if not piece:
                    return
                remaining -= len(piece)
                yield piece
        finally:
            await loop.run_in_executor(None, handle.close)

    return HttpResponse(status=status, body=produce(), headers=base_headers)


def _not_modified(request: HttpRequest, etag: str, mtime: float) -> bool:
    if_none_match = request.header("if-none-match")
    if if_none_match:
        candidates = [value.strip() for value in if_none_match.split(",")]
        if etag in candidates or "*" in candidates:
            return True
    if_modified_since = request.header("if-modified-since")
    if if_modified_since:
        try:
            since = parsedate_to_datetime(if_modified_since)
        except (TypeError, ValueError):
            return False
        if since is not None and int(mtime) <= int(since.timestamp()):
            return True
    return False


# ---------------------------------------------------------------------------
# routing
# ---------------------------------------------------------------------------

HttpHandler = Callable[[HttpRequest, "YHttpServer"], Awaitable[Any] | Any]
Middleware = Callable[[HttpRequest, Callable[[HttpRequest], Awaitable[HttpResponse]]], Awaitable[Any] | Any]


@dataclass(slots=True)
class _DynamicRoute:
    method: str
    pattern: str
    segments: tuple[tuple[bool, str], ...]
    handler: HttpHandler
    stream: bool
    catch_all: bool


def _compile(pattern: str) -> tuple[tuple[tuple[bool, str], ...], bool]:
    """Turn ``/users/{id}/posts`` into matchable segments.

    ``{name}`` matches one segment; ``{name:path}`` matches the rest.
    """

    segments: list[tuple[bool, str]] = []
    catch_all = False
    for raw in pattern.strip("/").split("/"):
        if raw.startswith("{") and raw.endswith("}"):
            inner = raw[1:-1]
            name, _, kind = inner.partition(":")
            if kind == "path":
                catch_all = True
                segments.append((True, name))
                break
            segments.append((True, name))
        else:
            segments.append((False, raw))
    return tuple(segments), catch_all


class _Router:
    def __init__(self) -> None:
        self.static: dict[tuple[str, str], HttpHandler] = {}
        self.static_stream: set[tuple[str, str]] = set()
        self.dynamic: list[_DynamicRoute] = []

    def add(self, method: str, pattern: str, handler: HttpHandler, *, stream: bool = False) -> None:
        normalized_method = (method or "GET").strip().upper()
        normalized_path = pattern.strip() or "/"
        if "{" not in normalized_path:
            self.static[(normalized_method, normalized_path)] = handler
            if stream:
                self.static_stream.add((normalized_method, normalized_path))
            else:
                self.static_stream.discard((normalized_method, normalized_path))
            return
        segments, catch_all = _compile(normalized_path)
        self.dynamic = [
            route
            for route in self.dynamic
            if not (route.method == normalized_method and route.pattern == normalized_path)
        ]
        self.dynamic.append(
            _DynamicRoute(
                method=normalized_method,
                pattern=normalized_path,
                segments=segments,
                handler=handler,
                stream=stream,
                catch_all=catch_all,
            )
        )

    def resolve(self, method: str, path: str) -> tuple[HttpHandler, dict[str, str], bool] | None:
        for candidate in (method, "ANY"):
            handler = self.static.get((candidate, path))
            if handler is not None:
                return handler, {}, (candidate, path) in self.static_stream

        parts = [segment for segment in path.strip("/").split("/") if segment]
        for route in self.dynamic:
            if route.method not in (method, "ANY"):
                continue
            params = self._match(route, parts)
            if params is not None:
                return route.handler, params, route.stream

        for candidate in (method, "ANY"):
            handler = self.static.get((candidate, "*"))
            if handler is not None:
                return handler, {}, (candidate, "*") in self.static_stream
        return None

    @staticmethod
    def _match(route: _DynamicRoute, parts: Sequence[str]) -> dict[str, str] | None:
        segments = route.segments
        if route.catch_all:
            if len(parts) < len(segments):
                return None
        elif len(parts) != len(segments):
            return None

        params: dict[str, str] = {}
        for index, (is_param, value) in enumerate(segments):
            if is_param and route.catch_all and index == len(segments) - 1:
                params[value] = "/".join(parts[index:])
                return params
            if is_param:
                params[value] = unquote(parts[index])
            elif parts[index] != value:
                return None
        return params

    def allowed_methods(self, path: str) -> set[str]:
        methods = {method for (method, pattern) in self.static if pattern == path}
        parts = [segment for segment in path.strip("/").split("/") if segment != ""]
        for route in self.dynamic:
            if self._match(route, parts) is not None:
                methods.add(route.method)
        methods.discard("ANY")
        return methods


# ---------------------------------------------------------------------------
# server
# ---------------------------------------------------------------------------


class YHttpServer(BaseServer):
    """Async HTTP/1.1 server with routing, middleware, TLS and streaming."""

    protocol = "http"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        ssl_context: ssl.SSLContext | None = None,
        auth_token: str | None = None,
        auth_exempt_paths: set[str] | None = None,
        rate_limit_per_window: int | None = 600,
        rate_limit_window_seconds: float = 60.0,
        ddosprot: bool = True,
        *,
        tls: TLSConfig | None = None,
        auth: AuthConfig | None = None,
        config: HttpConfig | None = None,
        logger: logging.Logger | None = None,
        **options: Any,
    ) -> None:
        auth_config = auth or AuthConfig(token=auth_token, exempt_paths=set(auth_exempt_paths or set()))
        if auth is not None and auth_token:
            auth_config.token = auth_token

        resolved = config or HttpConfig(
            host=host,
            port=port,
            ssl_context=resolve_ssl_context(ssl_context, tls),
            auth=auth_config,
            rate_limit=RateLimitConfig(limit=rate_limit_per_window, window_seconds=rate_limit_window_seconds),
            ddosprot=ddosprot,
        )
        for key, value in options.items():
            if not hasattr(resolved, key):
                raise ConfigError(f"unknown HTTP option: {key}")
            setattr(resolved, key, value)

        super().__init__(resolved, logger=logger)
        self.config: HttpConfig = resolved

        self._router = _Router()
        self._middleware: list[Middleware] = []
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[asyncio.Task[Any]] = set()
        #: Connections currently serving a request, as opposed to sitting idle
        #: between keep-alive requests. Only these are drained on shutdown.
        self._busy: set[asyncio.Task[Any]] = set()
        #: The port actually bound, remembered so a rebind lands on it again.
        self._listen_port: int | None = None
        self._active_connections = 0
        self._websocket_mounts: list[tuple[str, Any]] = []
        self._error_handlers: dict[int, Callable[[HttpRequest, HttpError], Any]] = {}

    # -- back-compat surface ------------------------------------------------

    @property
    def routes(self) -> dict[tuple[str, str], HttpHandler]:
        """Static routes, keyed ``(METHOD, path)``, as older versions exposed."""

        return self._router.static

    @property
    def auth_exempt_paths(self) -> set[str]:
        return self.config.auth.exempt_paths

    @auth_exempt_paths.setter
    def auth_exempt_paths(self, value: set[str]) -> None:
        self.config.auth.exempt_paths = set(value or set())

    def _bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return self.config.port or None
        return int(self._server.sockets[0].getsockname()[1])

    # -- routing -------------------------------------------------------------

    def add_route(
        self,
        path: str,
        handler: HttpHandler,
        method: str = "GET",
        *,
        stream: bool = False,
    ) -> None:
        self._router.add(method, path, handler, stream=stream)

    def route(
        self, path: str, method: str = "GET", *, stream: bool = False
    ) -> Callable[[HttpHandler], HttpHandler]:
        def decorator(handler: HttpHandler) -> HttpHandler:
            self.add_route(path, handler, method=method, stream=stream)
            return handler

        return decorator

    def get(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="GET", **kwargs)

    def post(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="POST", **kwargs)

    def put(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="PUT", **kwargs)

    def patch(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="PATCH", **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="DELETE", **kwargs)

    def head(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="HEAD", **kwargs)

    def options(self, path: str, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="OPTIONS", **kwargs)

    def static(self, url_prefix: str, directory: str | os.PathLike[str]) -> None:
        """Serve files from ``directory`` under ``url_prefix``.

        Paths are resolved and confirmed to stay inside the directory, so
        ``../`` cannot escape the served root.
        """

        root = Path(directory).resolve()
        prefix = "/" + url_prefix.strip("/")
        pattern = f"{prefix}/{{asset:path}}" if prefix != "/" else "/{asset:path}"

        async def serve(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            relative = request.path_params.get("asset", "")
            target = (root / relative).resolve()
            if not target.is_relative_to(root):
                raise HttpError(403, "path traversal rejected")
            return file_response(target, request, chunk_size=self.config.stream_chunk_size)

        self.add_route(pattern, serve, method="GET")
        self.add_route(pattern, serve, method="HEAD")

    def serve_folder(
        self,
        url_path: str,
        directory: str | os.PathLike[str],
        *,
        fmt: str = "tar.gz",
        download_name: str | None = None,
        exclude: Callable[[Path], bool] | None = None,
    ) -> None:
        """Offer a whole folder as a single streamed archive download.

        The archive is never built on disk or held in memory: it is produced
        by a worker thread and streamed out as it is made, so a folder much
        larger than RAM transfers in constant memory and a slow client applies
        backpressure to the producer.

        The client may pick a format with ``?format=zip`` (or ``tar``,
        ``tar.gz``, ``tar.bz2``, ``tar.xz``). RAR is not offered because
        creating RAR needs proprietary software.

        ::

            app.serve_folder("/backup", "/srv/data")
        """

        from .archive import WRITABLE_FORMATS, ArchiveError, folder_archive_stream

        root = Path(directory).resolve()
        route = "/" + url_path.strip("/")
        default_format = fmt

        async def serve(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            if not root.is_dir():
                raise HttpError(404, "no such folder")
            chosen = (request.query("format") or default_format).lower()
            if chosen not in WRITABLE_FORMATS:
                raise HttpError(
                    400,
                    f"unsupported archive format {chosen!r}; "
                    f"choose one of {', '.join(WRITABLE_FORMATS)}",
                )
            name = download_name or f"{root.name or 'folder'}.{chosen}"
            try:
                chunks = folder_archive_stream(
                    root,
                    fmt=chosen,
                    chunk_size=self.config.stream_chunk_size,
                    exclude=exclude,
                )
            except ArchiveError as error:
                raise HttpError(400, str(error)) from error
            # Length is unknown up front, so this goes out chunked.
            return HttpResponse.stream_response(
                chunks,
                content_type="application/octet-stream",
                headers={"Content-Disposition": f'attachment; filename="{name}"'},
            )

        self.add_route(route, serve, method="GET")

    def accept_folder(
        self,
        url_path: str,
        destination: str | os.PathLike[str],
        *,
        policy: Any = None,
        max_upload_bytes: int | None = None,
    ) -> None:
        """Accept an uploaded archive and extract it safely under ``destination``.

        The upload is streamed to a temporary file rather than buffered, then
        extracted under an :class:`~yashserver.archive.ArchivePolicy`. Anything
        the policy refuses -- traversal, links, special files, zip bombs --
        produces ``422`` and leaves nothing behind.

        Each upload lands in its own subdirectory of ``destination`` so
        concurrent uploads cannot interleave into one tree.
        """

        from .archive import ArchivePolicy, ArchiveError, UnsafeArchiveError, extract_archive

        root = Path(destination).resolve()
        route = "/" + url_path.strip("/")
        active_policy = policy or ArchivePolicy()
        ceiling = max_upload_bytes

        async def receive(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            import tempfile
            import uuid as _uuid

            root.mkdir(parents=True, exist_ok=True)
            staging = Path(tempfile.mkdtemp(prefix="upload-", dir=str(root)))
            archive_path = staging / "upload.bin"
            received = 0
            try:
                with open(archive_path, "wb") as handle:
                    async for chunk in request.stream(chunk_size=self.config.stream_chunk_size):
                        received += len(chunk)
                        if ceiling is not None and received > ceiling:
                            raise HttpError(413, f"upload exceeds {ceiling} bytes")
                        handle.write(chunk)
                if received == 0:
                    raise HttpError(400, "empty upload")

                target = staging / "extracted"
                try:
                    report = await asyncio.to_thread(
                        extract_archive, archive_path, target, policy=active_policy
                    )
                except UnsafeArchiveError as error:
                    raise HttpError(422, f"archive rejected: {error}") from error
                except ArchiveError as error:
                    raise HttpError(400, f"unreadable archive: {error}") from error
                finally:
                    archive_path.unlink(missing_ok=True)

                payload = report.as_dict()
                payload["received_bytes"] = received
                payload["destination"] = str(target)
                return HttpResponse.json_response(payload, status=201)
            except BaseException:
                # A failed upload must not leave a partial tree behind.
                import shutil

                shutil.rmtree(staging, ignore_errors=True)
                raise

        self.add_route(route, receive, method="POST", stream=True)

    def resumable_uploads(
        self,
        url_prefix: str,
        directory: str | os.PathLike[str],
        *,
        max_upload_bytes: int | None = None,
        session_ttl_seconds: float = 24 * 3600.0,
        max_sessions: int = 1000,
        on_complete: Callable[..., Any] | None = None,
    ) -> Any:
        """Serve resumable, integrity-checked uploads under ``url_prefix``.

        A transfer that dies partway can be continued instead of restarted:
        ``HEAD`` the session to learn how much landed, then ``PATCH`` from that
        offset. Returns the :class:`~yashserver.upload.UploadStore` so an
        application can inspect or clean up sessions itself.

        ::

            app.resumable_uploads("/uploads", "/srv/uploads",
                                  max_upload_bytes=100 * 1024**3)

        See :mod:`yashserver.upload` for the protocol and the integrity model.
        """

        from .upload import UploadError, UploadStore

        store = UploadStore(
            directory,
            max_upload_bytes=max_upload_bytes,
            max_sessions=max_sessions,
            session_ttl_seconds=session_ttl_seconds,
        )
        prefix = "/" + url_prefix.strip("/")

        def _fail(error: "UploadError") -> HttpError:
            return HttpError(error.status, error.detail)

        def _headers(session: Any) -> dict[str, str]:
            headers = {
                "Upload-Offset": str(session.offset),
                # A cached offset would make a client resume from the wrong
                # place, which is the one mistake this protocol must not allow.
                "Cache-Control": "no-store",
            }
            if session.length is not None:
                headers["Upload-Length"] = str(session.length)
            return headers

        async def create(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            raw_length = request.header("upload-length")
            length: int | None = None
            if raw_length is not None:
                try:
                    length = int(raw_length)
                except ValueError as error:
                    raise HttpError(400, "Upload-Length must be an integer") from error
            try:
                session = store.create(
                    length=length,
                    checksum=request.header("upload-checksum"),
                    filename=request.header("upload-filename"),
                )
            except UploadError as error:
                raise _fail(error) from error
            headers = _headers(session)
            headers["Location"] = f"{prefix}/{session.id}"
            return HttpResponse.json_response(session.as_dict(), status=201, headers=headers)

        async def status(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            try:
                session = store.get(request.path_params.get("upload_id", ""))
            except UploadError as error:
                raise _fail(error) from error
            return HttpResponse.json_response(session.as_dict(), headers=_headers(session))

        async def head(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            try:
                session = store.get(request.path_params.get("upload_id", ""))
            except UploadError as error:
                raise _fail(error) from error
            return HttpResponse(status=200, body=b"", headers=_headers(session))

        async def patch(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            upload_id = request.path_params.get("upload_id", "")
            raw_offset = request.header("upload-offset")
            if raw_offset is None:
                raise HttpError(400, "PATCH requires an Upload-Offset header")
            try:
                offset = int(raw_offset)
            except ValueError as error:
                raise HttpError(400, "Upload-Offset must be an integer") from error

            try:
                async with store.lock_for(upload_id):
                    session = await store.append(
                        upload_id,
                        request.stream(chunk_size=self.config.stream_chunk_size),
                        offset=offset,
                        chunk_checksum=request.header("upload-checksum"),
                    )
                    if session.is_complete:
                        session = store.finalize(upload_id)
                        if on_complete is not None:
                            await maybe_await(on_complete(session, self))
            except UploadError as error:
                raise _fail(error) from error

            headers = _headers(session)
            if session.completed:
                return HttpResponse.json_response(session.as_dict(), status=200, headers=headers)
            return HttpResponse(status=204, body=b"", headers=headers)

        async def discard(request: HttpRequest, _server: "YHttpServer") -> HttpResponse:
            store.delete(request.path_params.get("upload_id", ""))
            return HttpResponse(status=204, body=b"")

        item = f"{prefix}/{{upload_id}}"
        self.add_route(prefix, create, method="POST")
        self.add_route(item, status, method="GET")
        self.add_route(item, head, method="HEAD")
        self.add_route(item, patch, method="PATCH", stream=True)
        self.add_route(item, discard, method="DELETE")
        return store

    def middleware(self, handler: Middleware) -> Middleware:
        """Register middleware, outermost first.

        ::

            @app.middleware
            async def timing(request, call_next):
                started = time.monotonic()
                response = await call_next(request)
                response.set_header("X-Elapsed", f"{time.monotonic()-started:.4f}")
                return response
        """

        self._middleware.append(handler)
        return handler

    def error_handler(self, status: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Customise the response for one status code."""

        def decorator(handler: Callable[[HttpRequest, HttpError], Any]) -> Callable[..., Any]:
            self._error_handlers[int(status)] = handler
            return handler

        return decorator

    def mount_websocket(self, ws_server: Any, path_prefix: str = "/") -> None:
        """Serve a :class:`~yashserver.websocket.YWebSocketServer` on this port.

        Requests carrying ``Upgrade: websocket`` under ``path_prefix`` are
        handed to the WebSocket server instead of the HTTP router, so a web
        app and its socket can share one origin and one certificate.
        """

        self._websocket_mounts.append((path_prefix.rstrip("/") or "/", ws_server))

    # -- lifecycle ------------------------------------------------------------

    async def _start_impl(self) -> None:
        self._server = await self._listen(self.config.port)
        self._listen_port = self._bound_port()
        self._start_listener_guard()

    async def _listen(self, port: int) -> asyncio.AbstractServer:
        return await asyncio.start_server(
            self._handle_connection,
            self.config.host,
            port,
            ssl=self.config.ssl_context,
            backlog=self.config.backlog,
            limit=max(self.config.max_header_bytes + 1024, 64 * 1024),
        )

    async def _rebind_listener(self) -> None:
        old = self._server
        self._server = None
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        self._server = await self._listen(self._listen_port or self.config.port)

    async def _serve_impl(self) -> None:
        if self._server is None:
            return
        async with self._server:
            await self._server.serve_forever()

    async def _stop_impl(self, drain_deadline: float) -> None:
        # `wait_closed()` also waits for every in-flight handler, so it runs
        # last; awaiting it here would deadlock on kept-alive connections.
        server = self._server
        self._server = None
        close_listener_quietly(server)

        # Let requests that are mid-flight finish. A kept-alive connection
        # idling between requests has nothing to drain, so it must not hold
        # shutdown open for the whole drain window.
        remaining = drain_deadline - time.monotonic()
        if self._busy and remaining > 0:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._busy), return_exceptions=True),
                    timeout=remaining,
                )
            except Exception:
                pass
        for task in list(self._connections):
            task.cancel()
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)
        self._connections.clear()

        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except Exception:
                pass

    # -- connection handling ---------------------------------------------------

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            await self._serve_connection(reader, writer)
        finally:
            if task is not None:
                self._connections.discard(task)

    async def _serve_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        max_connections = self.config.max_connections
        if max_connections is not None and self._active_connections >= max_connections:
            self.metrics.incr("connections_refused")
            await self._write_raw(writer, 503, b"Server too busy", {"Content-Type": "text/plain; charset=utf-8"})
            await close_writer_quietly(writer, timeout_seconds=0.5)
            return

        self._active_connections += 1
        self.metrics.incr("connections_opened")
        self.metrics.gauge("connections_active", self._active_connections)
        remote_addr = format_peer_name(writer.get_extra_info("peername"))
        self._tune_transport(writer)

        served = 0
        keep_alive = self.config.keep_alive
        try:
            while keep_alive or served == 0:
                if self.state is ServerState.STOPPING:
                    break
                timeout = (
                    self.config.request_timeout_seconds
                    if served == 0
                    else self.config.keep_alive_timeout_seconds
                )
                try:
                    head = await asyncio.wait_for(self._read_head(reader), timeout=timeout)
                except asyncio.TimeoutError:
                    if served:
                        break  # idle keep-alive connection; just close it
                    self.metrics.incr("request_timeouts")
                    await self._write_raw(writer, 408, b"Request Timeout", {"Content-Type": "text/plain; charset=utf-8"})
                    break
                except HttpError as error:
                    await self._write_response(writer, error.to_response(), method="GET", close=True)
                    break

                if head is None:
                    break  # clean EOF

                served += 1
                self.metrics.incr("requests")

                task = asyncio.current_task()
                if task is not None:
                    self._busy.add(task)
                try:
                    should_continue = await self._serve_request(reader, writer, head, remote_addr)
                finally:
                    if task is not None:
                        self._busy.discard(task)
                if not should_continue:
                    break
                if served >= self.config.max_requests_per_connection:
                    break
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._report_error(error, {"stage": "http-connection", "remote_addr": remote_addr})
        finally:
            self._active_connections -= 1
            self.metrics.gauge("connections_active", max(0, self._active_connections))
            self.metrics.incr("connections_closed")
            await close_writer_quietly(writer, timeout_seconds=1.0)

    def _tune_transport(self, writer: asyncio.StreamWriter) -> None:
        try:
            writer.transport.set_write_buffer_limits(
                high=self.config.write_buffer_high_bytes,
                low=self.config.write_buffer_low_bytes,
            )
        except (AttributeError, NotImplementedError):
            pass

    async def _read_head(self, reader: asyncio.StreamReader) -> tuple[str, str, str, dict[str, str]] | None:
        try:
            header_bytes = await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError as error:
            raise HttpError(431, "request headers too large") from error

        if len(header_bytes) > self.config.max_header_bytes:
            raise HttpError(431, "request headers too large")

        try:
            header_text = header_bytes.decode("latin-1")
        except UnicodeDecodeError as error:
            raise HttpError(400, "malformed request headers") from error

        lines = header_text.split("\r\n")
        if not lines or not lines[0]:
            return None
        request_line = lines[0].split(" ")
        if len(request_line) < 3:
            raise HttpError(400, "malformed request line")
        method, raw_target, version = request_line[0], request_line[1], request_line[2]
        if not version.startswith("HTTP/"):
            raise HttpError(400, "unsupported protocol")

        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            # Repeated headers join with a comma, per RFC 9110.
            headers[key] = f"{headers[key]}, {value}" if key in headers else value

        return method.upper(), raw_target, version.strip(), headers

    async def _serve_request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        head: tuple[str, str, str, dict[str, str]],
        remote_addr: str,
    ) -> bool:
        method, raw_target, version, headers = head
        started = time.monotonic()

        parsed = urlparse(raw_target)
        path = unquote(parsed.path) or "/"
        query_params = dict(parse_qs(parsed.query, keep_blank_values=True))

        # WebSocket upgrade handoff, before anything HTTP-specific happens.
        if self._websocket_mounts and "websocket" in (headers.get("upgrade") or "").lower():
            handled = await self._handoff_websocket(reader, writer, path, query_params, headers, remote_addr)
            if handled:
                return False

        chunked = "chunked" in (headers.get("transfer-encoding") or "").lower()
        content_length: int | None = None
        if not chunked:
            raw_length = headers.get("content-length")
            if raw_length is not None:
                try:
                    content_length = int(raw_length)
                except ValueError:
                    await self._write_response(
                        writer, HttpError(400, "invalid Content-Length").to_response(), method=method, close=True
                    )
                    return False
                if content_length < 0:
                    await self._write_response(
                        writer, HttpError(400, "invalid Content-Length").to_response(), method=method, close=True
                    )
                    return False

        body_reader = _BodyReader(
            reader,
            content_length=content_length,
            chunked=chunked,
            max_bytes=self.config.max_body_bytes,
        )

        request = HttpRequest(
            method=method,
            path=path,
            query_params=query_params,
            headers=headers,
            body=b"",
            remote_addr=remote_addr,
            raw_target=raw_target,
            http_version=version,
            body_reader=body_reader,
        )
        request.state["tls"] = writer.get_extra_info("ssl_object") is not None

        close_after = not (self.config.keep_alive and request.wants_keep_alive)

        try:
            response = await self._produce_response(request, body_reader, writer)
        except HttpError as error:
            response = await self._render_error(request, error)
            close_after = True
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._report_error(error, {"stage": "http-dispatch", "method": method, "path": path})
            response = await self._render_error(request, HttpError(500, "Internal Server Error"))
            close_after = True

        # A response must not be followed by an unread request body, or the
        # next request on this connection would parse leftover bytes.
        if not close_after and not body_reader.exhausted:
            if not await body_reader.discard():
                close_after = True

        self.metrics.incr(f"status_{response.status // 100}xx")
        self.metrics.observe("request_seconds", time.monotonic() - started)

        try:
            await self._write_response(writer, response, method=method, close=close_after)
        except (ConnectionResetError, BrokenPipeError, TimeoutError, asyncio.TimeoutError):
            return False

        if close_after and not body_reader.exhausted:
            # The client is probably still uploading a body we rejected (413,
            # 401, 404...). Closing on it now would reset the connection and
            # the status we just wrote would never be read. Absorb a little
            # more first so the response actually lands.
            await self._linger(reader)

        return not close_after

    async def _linger(self, reader: asyncio.StreamReader) -> None:
        """Briefly absorb an unwanted request body before closing."""

        seconds = self.config.lingering_close_seconds
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        drained = 0
        while drained < self.config.lingering_close_max_bytes:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=remaining)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                return
            if not chunk:
                return
            drained += len(chunk)

    async def _produce_response(
        self,
        request: HttpRequest,
        body_reader: _BodyReader,
        writer: asyncio.StreamWriter,
    ) -> HttpResponse:
        if not await self.config.auth.authorize(
            headers=request.headers,
            query_params=request.query_params,
            path=request.path,
            remote_addr=request.remote_addr,
        ):
            self.metrics.incr("unauthorized")
            raise HttpError(401, "missing or invalid credentials", headers={"WWW-Authenticate": "Bearer"})

        if not self._allow_request(request.remote_addr):
            retry_after = self._rate_limiter.retry_after_seconds(request.remote_addr)
            return HttpResponse(
                status=429,
                body=_load_ddos_block_page_bytes(),
                headers={
                    "Content-Type": "text/html; charset=utf-8",
                    "Retry-After": str(retry_after),
                },
            )

        await self._notify_plugins("on_http_request", request, self)

        async def dispatch(current: HttpRequest) -> HttpResponse:
            return await self._dispatch(current, body_reader, writer)

        handler_chain = dispatch
        for middleware in reversed(self._middleware):
            handler_chain = self._wrap_middleware(middleware, handler_chain)

        return _normalize_response(await handler_chain(request))

    def _wrap_middleware(
        self,
        middleware: Middleware,
        next_handler: Callable[[HttpRequest], Awaitable[HttpResponse]],
    ) -> Callable[[HttpRequest], Awaitable[HttpResponse]]:
        async def wrapped(request: HttpRequest) -> HttpResponse:
            async def call_next(current: HttpRequest) -> HttpResponse:
                return _normalize_response(await next_handler(current))

            return _normalize_response(await maybe_await(middleware(request, call_next)))

        return wrapped

    async def _dispatch(
        self,
        request: HttpRequest,
        body_reader: _BodyReader,
        writer: asyncio.StreamWriter,
    ) -> HttpResponse:
        resolved = self._router.resolve(request.method, request.path)
        if resolved is None and request.method == "HEAD":
            # A HEAD with no dedicated route is answered by its GET route,
            # headers only. _write_response drops the body.
            resolved = self._router.resolve("GET", request.path)

        if resolved is None:
            allowed = self._router.allowed_methods(request.path)
            if allowed:
                advertised = set(allowed)
                if "GET" in advertised:
                    advertised.add("HEAD")
                raise HttpError(
                    405,
                    f"{request.method} not allowed for {request.path}",
                    headers={"Allow": ", ".join(sorted(advertised))},
                )
            raise HttpError(404, f"no route for {request.path}")

        handler, path_params, streaming = resolved
        request.path_params = path_params

        # The buffered cap exists to protect memory, so it must not be applied
        # to a streaming route, whose body reaches the handler in chunks and is
        # never held in full.
        body_reader.set_limit(
            self.config.max_stream_body_bytes if streaming else self.config.max_body_bytes
        )

        if request.header("expect", "").lower() == "100-continue":
            writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            await writer.drain()

        if not streaming:
            request.body = await body_reader.read_all()

        return _normalize_response(await maybe_await(handler(request, self)))

    async def _render_error(self, request: HttpRequest, error: HttpError) -> HttpResponse:
        custom = self._error_handlers.get(error.status)
        if custom is not None:
            try:
                return _normalize_response(await maybe_await(custom(request, error)))
            except Exception as nested:
                await self._report_error(nested, {"stage": "http-error-handler", "status": error.status})
        return error.to_response()

    # -- WebSocket handoff -------------------------------------------------------

    async def _handoff_websocket(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        path: str,
        query_params: dict[str, list[str]],
        headers: dict[str, str],
        remote_addr: str,
    ) -> bool:
        for prefix, ws_server in self._websocket_mounts:
            if prefix != "/" and not path.startswith(prefix):
                continue
            self.metrics.incr("websocket_upgrades")
            await ws_server.serve_upgrade(
                reader,
                writer,
                path=path,
                query_params=query_params,
                headers=headers,
                remote_addr=remote_addr,
            )
            return True
        return False

    # -- writing ------------------------------------------------------------------

    async def _write_raw(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        await self._write_response(
            writer,
            HttpResponse(status=status, body=body, headers=headers or {}),
            method="GET",
            close=True,
        )

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        response: HttpResponse,
        *,
        method: str,
        close: bool,
    ) -> None:
        body_allowed = method != "HEAD" and response.status not in (204, 304) and not (100 <= response.status < 200)
        streaming = response.is_streaming

        headers: dict[str, str] = {
            "Server": self.config.server_header,
            "Date": http_date(),
        }
        headers.update(response.headers)
        headers["Connection"] = "close" if close else "keep-alive"

        explicit_length = None
        for key, value in headers.items():
            if key.lower() == "content-length":
                explicit_length = value

        payload = b""
        if streaming:
            if explicit_length is None:
                headers["Transfer-Encoding"] = "chunked"
        else:
            payload = _encode_body(response.body)
            # HEAD advertises the length GET would have returned, but sends
            # no body. 204/304 carry no length at all.
            if explicit_length is None:
                headers["Content-Length"] = str(len(payload))
            if not body_allowed:
                payload = b""

        if response.status in (204, 304) or 100 <= response.status < 200:
            headers.pop("Content-Length", None)
            headers.pop("Transfer-Encoding", None)

        head = [f"HTTP/1.1 {response.status} {status_reason(response.status)}"]
        head.extend(f"{key}: {value}" for key, value in headers.items())
        head.append("")
        head.append("")
        await self._write_bytes(writer, "\r\n".join(head).encode("latin-1"))

        if not body_allowed:
            if streaming:
                await _close_iterator(response.body)
            return

        if not streaming:
            if payload:
                await self._write_bytes(writer, payload)
            return

        use_chunked = headers.get("Transfer-Encoding") == "chunked"
        try:
            async for chunk in _iterate_body(response.body):
                if not chunk:
                    continue
                if use_chunked:
                    await self._write_bytes(writer, f"{len(chunk):x}\r\n".encode("latin-1") + chunk + b"\r\n")
                else:
                    await self._write_bytes(writer, chunk)
            if use_chunked:
                await self._write_bytes(writer, b"0\r\n\r\n")
        except (ConnectionResetError, BrokenPipeError, TimeoutError, asyncio.TimeoutError):
            raise
        except Exception as error:
            await self._report_error(error, {"stage": "http-stream-body", "status": response.status})
            # The head is already on the wire, so the only honest signal left
            # is to cut the connection rather than finish the chunked stream.
            raise ConnectionResetError("streaming body failed") from error

    async def _write_bytes(self, writer: asyncio.StreamWriter, data: bytes) -> None:
        writer.write(data)
        try:
            await asyncio.wait_for(writer.drain(), timeout=self.config.write_timeout_seconds)
        except asyncio.TimeoutError as error:
            self.metrics.incr("write_timeouts")
            raise TimeoutError("client did not read the response in time") from error
        self.metrics.incr("bytes_sent", len(data))

    # -- static response helpers (kept from the 0.x API) -----------------------

    @staticmethod
    def html(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        output_headers = {"Content-Type": "text/html; charset=utf-8"}
        if headers:
            output_headers.update(headers)
        return status, content, output_headers

    @staticmethod
    def text(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        output_headers = {"Content-Type": "text/plain; charset=utf-8"}
        if headers:
            output_headers.update(headers)
        return status, content, output_headers

    @staticmethod
    def json(data: Any, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        output_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            output_headers.update(headers)
        return status, ServerTools.to_json(data), output_headers

    @staticmethod
    def file(path: str | os.PathLike[str], request: HttpRequest | None = None, **kwargs: Any) -> HttpResponse:
        return file_response(path, request, **kwargs)

    @staticmethod
    def _status_reason(status: int) -> str:
        return status_reason(status)


# ---------------------------------------------------------------------------
# body helpers
# ---------------------------------------------------------------------------


def _encode_body(body: Any) -> bytes:
    if body is None:
        return b""
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    if isinstance(body, str):
        return body.encode("utf-8")
    return ServerTools.to_json(body).encode("utf-8")


async def _iterate_body(body: Any) -> AsyncIterator[bytes]:
    if hasattr(body, "__aiter__"):
        async for chunk in body:
            yield _encode_body(chunk)
        return
    for chunk in body:
        yield _encode_body(chunk)


async def _close_iterator(body: Any) -> None:
    closer = getattr(body, "aclose", None)
    if closer is not None:
        try:
            await closer()
        except Exception:
            pass


def _normalize_response(result: Any) -> HttpResponse:
    """Turn whatever a handler returned into an :class:`HttpResponse`."""

    if isinstance(result, HttpResponse):
        return result

    if result is None:
        return HttpResponse(status=204, body=b"", headers={"Content-Type": "text/plain; charset=utf-8"})

    if isinstance(result, tuple):
        if len(result) == 2:
            status, body = result
            headers: dict[str, str] = {}
        elif len(result) == 3:
            status, body, headers = result
        else:
            raise ValueError("tuple responses must be (status, body) or (status, body, headers)")
        response = HttpResponse(status=int(status), body=body, headers=dict(headers or {}))
        if not response._has_header("content-type") and not response.is_streaming:
            response.headers["Content-Type"] = _guess_content_type(body)
        return response

    if isinstance(result, (bytes, bytearray)):
        return HttpResponse(status=200, body=bytes(result), content_type="application/octet-stream")

    if isinstance(result, str):
        return HttpResponse(status=200, body=result, content_type="text/plain; charset=utf-8")

    if hasattr(result, "__aiter__"):
        return HttpResponse(
            status=200,
            body=result,
            content_type="application/octet-stream",
            streaming=True,
        )

    return HttpResponse.json_response(result)


def _guess_content_type(body: Any) -> str:
    if isinstance(body, (bytes, bytearray)):
        return "application/octet-stream"
    if isinstance(body, str):
        return "text/plain; charset=utf-8"
    return "application/json; charset=utf-8"
