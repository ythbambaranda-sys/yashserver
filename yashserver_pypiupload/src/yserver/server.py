from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import ssl
import struct
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from .plugin import ServerPlugin
from .tools import ServerTools

TcpHandler = Callable[["TcpClient", str, "YServer"], Awaitable[Any] | Any]
WsMessage = str | bytes
WsHandler = Callable[["WebSocketClient", WsMessage, "YWebSocketServer"], Awaitable[Any] | Any]
HttpHandler = Callable[["HttpRequest", "YHttpServer"], Awaitable[Any] | Any]

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


@dataclass(slots=True)
class TcpClient:
    id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    address: str
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class WebSocketClient:
    id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    path: str
    query_params: dict[str, list[str]] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    remote_addr: str = "unknown"
    authenticated: bool = False
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class HttpRequest:
    method: str
    path: str
    query_params: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes
    remote_addr: str = "unknown"


class _SlidingWindowRateLimiter:
    def __init__(self, limit: int | None, window_seconds: float) -> None:
        self.limit = limit
        self.window_seconds = max(1.0, float(window_seconds))
        self._events: dict[str, deque[float]] = {}

    def allow(self, key: str) -> bool:
        if self.limit is None or self.limit <= 0:
            return True

        now = time.monotonic()
        bucket = self._events.setdefault(key, deque())
        threshold = now - self.window_seconds
        while bucket and bucket[0] < threshold:
            bucket.popleft()

        if len(bucket) >= self.limit:
            return False

        bucket.append(now)
        return True

    def retry_after_seconds(self, key: str) -> int:
        if self.limit is None or self.limit <= 0:
            return 0
        bucket = self._events.get(key)
        if not bucket:
            return 0
        now = time.monotonic()
        remaining = (bucket[0] + self.window_seconds) - now
        if remaining <= 0:
            return 0
        return int(remaining) + 1


def _extract_bearer_token(raw_authorization: str | None) -> str | None:
    if not raw_authorization:
        return None
    value = raw_authorization.strip()
    if not value.lower().startswith("bearer "):
        return None
    token = value[7:].strip()
    return token or None


def _format_peer_name(peer_name: Any) -> str:
    if isinstance(peer_name, tuple) and peer_name:
        return str(peer_name[0])
    return str(peer_name) if peer_name else "unknown"


async def _close_writer_quietly(writer: asyncio.StreamWriter, *, timeout_seconds: float = 1.0) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=max(0.1, float(timeout_seconds)))
        return
    except Exception:
        pass

    transport = getattr(writer, "transport", None) or getattr(writer, "_transport", None)
    if transport is not None:
        try:
            transport.abort()
        except Exception:
            return


class YServer:
    """Line-delimited asyncio TCP server with plugins, routes, and tools."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        delimiter: str = "\n",
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.delimiter = delimiter
        self.ssl_context = ssl_context
        self.plugins: list[ServerPlugin] = []
        self.clients: dict[str, TcpClient] = {}
        self.routes: dict[str, TcpHandler] = {}
        self.tools: dict[str, Callable[..., Any]] = {}
        self.started_at: datetime | None = None

        self._server: asyncio.AbstractServer | None = None
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._is_shutting_down = False
        self._register_default_tools()

    def add_plugin(self, plugin: ServerPlugin) -> "YServer":
        self.plugins.append(plugin)
        return self

    def add_route(self, command: str, handler: TcpHandler) -> None:
        normalized = command.strip().lower()
        if not normalized:
            raise ValueError("command cannot be empty")
        self.routes[normalized] = handler

    def route(self, command: str) -> Callable[[TcpHandler], TcpHandler]:
        def decorator(handler: TcpHandler) -> TcpHandler:
            self.add_route(command, handler)
            return handler

        return decorator

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        key = name.strip().lower()
        if not key:
            raise ValueError("tool name cannot be empty")
        self.tools[key] = tool

    def tool(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.register_tool(name, func)
            return func

        return decorator

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        key = name.strip().lower()
        if key not in self.tools:
            raise KeyError(f"tool not found: {name}")
        return self.tools[key](*args, **kwargs)

    async def start(self) -> None:
        if self._server is not None:
            return
        self.started_at = datetime.now(timezone.utc)
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            ssl=self.ssl_context,
        )
        await self._notify_plugins("on_startup", self)

    async def run(self) -> None:
        await self.start()
        if self._server is None:
            return
        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        try:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

            for task in list(self._background_tasks):
                task.cancel()
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

            client_writers = [client.writer for client in self.clients.values()]
            if client_writers:
                await asyncio.gather(
                    *(_close_writer_quietly(writer) for writer in client_writers),
                    return_exceptions=True,
                )
            self.clients.clear()
            await self._notify_plugins("on_shutdown", self)
        finally:
            self._is_shutting_down = False

    async def send(self, client_or_id: TcpClient | str, payload: Any) -> None:
        client = client_or_id if isinstance(client_or_id, TcpClient) else self.clients[client_or_id]
        encoded = self._encode_payload(payload)
        if not encoded.endswith(self.delimiter.encode("utf-8")):
            encoded += self.delimiter.encode("utf-8")
        client.writer.write(encoded)
        await client.writer.drain()

    async def broadcast(self, payload: Any, exclude: str | None = None) -> None:
        for client_id, client in list(self.clients.items()):
            if exclude and client_id == exclude:
                continue
            try:
                await self.send(client, payload)
            except Exception as error:  # pragma: no cover - runtime network behavior
                await self._notify_plugins(
                    "on_error",
                    error,
                    {"stage": "broadcast", "client_id": client_id},
                    self,
                )

    def create_task(self, coro: Awaitable[Any], *, name: str | None = None) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def every(self, seconds: float, callback: Callable[[], Awaitable[Any] | Any]) -> asyncio.Task[Any]:
        async def runner() -> None:
            while True:
                await asyncio.sleep(seconds)
                result = callback()
                if inspect.isawaitable(result):
                    await result

        callback_name = getattr(callback, "__name__", "callback")
        return self.create_task(runner(), name=f"every-{callback_name}")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info("peername")
        client = TcpClient(
            id=uuid.uuid4().hex,
            reader=reader,
            writer=writer,
            address=_format_peer_name(peer_name),
        )
        self.clients[client.id] = client
        await self._notify_plugins("on_tcp_connect", client, self)

        try:
            while not reader.at_eof():
                raw = await reader.readline()
                if not raw:
                    break

                message = raw.decode("utf-8", errors="replace").strip()
                if not message:
                    continue

                transformed = await self._apply_tcp_message_plugins(client, message)
                if transformed is None:
                    continue

                command, payload = ServerTools.command_parts(transformed)
                if not command:
                    continue

                handler = self.routes.get(command)
                if handler is None:
                    await self.send(client, {"error": "unknown-command", "command": command})
                    continue

                try:
                    result = handler(client, payload, self)
                    if inspect.isawaitable(result):
                        result = await result
                except Exception as error:
                    await self._notify_plugins(
                        "on_error",
                        error,
                        {"stage": "tcp-handler", "command": command, "client_id": client.id},
                        self,
                    )
                    await self.send(client, {"error": "handler-failed", "detail": str(error)})
                    continue

                if result is not None:
                    await self.send(client, result)
        except Exception as error:
            await self._notify_plugins("on_error", error, {"stage": "tcp-read", "client_id": client.id}, self)
        finally:
            self.clients.pop(client.id, None)
            await self._notify_plugins("on_tcp_disconnect", client, self)
            await _close_writer_quietly(writer)

    async def _apply_tcp_message_plugins(self, client: TcpClient, message: str) -> str | None:
        transformed: str | None = message
        for plugin in self.plugins:
            if transformed is None:
                return None
            transformed = await plugin.on_tcp_message(client, transformed, self)
        return transformed

    async def _notify_plugins(self, hook_name: str, *args: Any) -> None:
        for plugin in self.plugins:
            hook = getattr(plugin, hook_name, None)
            if hook is None:
                continue
            try:
                result = hook(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # pragma: no cover - plugin errors should not crash server
                if hook_name == "on_error":
                    continue
                try:
                    await plugin.on_error(
                        RuntimeError(f"plugin hook failed: {plugin.name}.{hook_name}"),
                        {"stage": "plugin-hook", "hook": hook_name},
                        self,
                    )
                except Exception:
                    continue

    def _encode_payload(self, payload: Any) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            return payload.encode("utf-8")
        return ServerTools.to_json(payload).encode("utf-8")

    def _register_default_tools(self) -> None:
        self.register_tool("now", ServerTools.utc_now)
        self.register_tool("client_count", lambda: len(self.clients))
        self.register_tool(
            "list_clients",
            lambda: [{"id": client.id, "address": client.address} for client in self.clients.values()],
        )

        def uptime_seconds() -> float:
            if self.started_at is None:
                return 0.0
            return (datetime.now(timezone.utc) - self.started_at).total_seconds()

        self.register_tool("uptime_seconds", uptime_seconds)


class YWebSocketServer:
    """Independent WebSocket server with TLS/auth/rate-limit support."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9001,
        ssl_context: ssl.SSLContext | None = None,
        auth_token: str | None = None,
        rate_limit_per_window: int | None = 300,
        rate_limit_window_seconds: float = 60.0,
        max_message_size_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.auth_token = auth_token
        self.max_message_size_bytes = max(1024, int(max_message_size_bytes))
        self.plugins: list[ServerPlugin] = []
        self.clients: dict[str, WebSocketClient] = {}
        self.routes: dict[str, WsHandler] = {}
        self.tools: dict[str, Callable[..., Any]] = {}
        self.started_at: datetime | None = None

        self._server: asyncio.AbstractServer | None = None
        self._is_shutting_down = False
        self._rate_limiter = _SlidingWindowRateLimiter(rate_limit_per_window, rate_limit_window_seconds)
        self._register_default_tools()

    def add_plugin(self, plugin: ServerPlugin) -> "YWebSocketServer":
        self.plugins.append(plugin)
        return self

    def add_route(self, path: str, handler: WsHandler) -> None:
        normalized = path.strip() or "/"
        self.routes[normalized] = handler

    def route(self, path: str) -> Callable[[WsHandler], WsHandler]:
        def decorator(handler: WsHandler) -> WsHandler:
            self.add_route(path, handler)
            return handler

        return decorator

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        key = name.strip().lower()
        if not key:
            raise ValueError("tool name cannot be empty")
        self.tools[key] = tool

    def tool(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.register_tool(name, func)
            return func

        return decorator

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        key = name.strip().lower()
        if key not in self.tools:
            raise KeyError(f"tool not found: {name}")
        return self.tools[key](*args, **kwargs)

    async def start(self) -> None:
        if self._server is not None:
            return
        self.started_at = datetime.now(timezone.utc)
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
            ssl=self.ssl_context,
        )
        await self._notify_plugins("on_startup", self)

    async def run(self) -> None:
        await self.start()
        if self._server is None:
            return
        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        try:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

            session_writers = [session.writer for session in self.clients.values()]
            if session_writers:
                await asyncio.gather(
                    *(_close_writer_quietly(writer) for writer in session_writers),
                    return_exceptions=True,
                )
            self.clients.clear()
            await self._notify_plugins("on_shutdown", self)
        finally:
            self._is_shutting_down = False

    async def send(self, session_or_id: WebSocketClient | str, payload: Any) -> None:
        session = session_or_id if isinstance(session_or_id, WebSocketClient) else self.clients[session_or_id]
        if isinstance(payload, bytes):
            await self._write_frame(session.writer, 0x2, payload)
            return
        if isinstance(payload, str):
            await self._write_frame(session.writer, 0x1, payload.encode("utf-8"))
            return
        await self._write_frame(session.writer, 0x1, ServerTools.to_json(payload).encode("utf-8"))

    async def broadcast(self, payload: Any, exclude: str | None = None) -> None:
        for session_id in list(self.clients):
            if exclude and session_id == exclude:
                continue
            try:
                await self.send(session_id, payload)
            except Exception as error:  # pragma: no cover - runtime network behavior
                await self._notify_plugins(
                    "on_error",
                    error,
                    {"stage": "broadcast", "session_id": session_id},
                    self,
                )

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info("peername")
        remote_addr = _format_peer_name(peer_name)
        session: WebSocketClient | None = None
        try:
            handshake = await self._read_ws_handshake(reader)
            if handshake is None:
                return

            path, query_params, headers = handshake
            if not self._is_authorized(headers, query_params):
                await self._write_http_error(
                    writer,
                    401,
                    "Unauthorized",
                    headers={"WWW-Authenticate": "Bearer"},
                )
                return

            accept_value = self._build_accept_value(headers["sec-websocket-key"])
            writer.write(
                (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept_value}\r\n"
                    "\r\n"
                ).encode("utf-8")
            )
            await writer.drain()

            session = WebSocketClient(
                id=uuid.uuid4().hex,
                reader=reader,
                writer=writer,
                path=path,
                query_params=query_params,
                headers=headers,
                remote_addr=remote_addr,
                authenticated=True,
            )
            self.clients[session.id] = session
            await self._notify_plugins("on_ws_connect", session, self)
            continuation_opcode: int | None = None
            continuation_payload = bytearray()

            while True:
                frame = await self._read_frame(reader)
                if frame is None:
                    break
                opcode, fin, payload = frame

                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    await self._write_frame(writer, 0xA, payload)
                    continue
                if opcode == 0xA:
                    continue

                if opcode == 0x0:
                    if continuation_opcode is None:
                        continue
                    continuation_payload.extend(payload)
                    if len(continuation_payload) > self.max_message_size_bytes:
                        await self._safe_send_error(session, "message too large")
                        continuation_opcode = None
                        continuation_payload.clear()
                        continue
                    if not fin:
                        continue
                    opcode = continuation_opcode
                    payload = bytes(continuation_payload)
                    continuation_opcode = None
                    continuation_payload.clear()
                elif opcode in (0x1, 0x2):
                    if not fin:
                        continuation_opcode = opcode
                        continuation_payload = bytearray(payload)
                        if len(continuation_payload) > self.max_message_size_bytes:
                            await self._safe_send_error(session, "message too large")
                            continuation_opcode = None
                            continuation_payload.clear()
                        continue
                    if len(payload) > self.max_message_size_bytes:
                        await self._safe_send_error(session, "message too large")
                        continue
                else:
                    continue

                if not self._rate_limiter.allow(session.remote_addr):
                    await self._safe_send_error(session, "rate-limit exceeded")
                    continue

                if opcode == 0x1:
                    message = payload.decode("utf-8", errors="replace")
                    transformed = await self._apply_ws_message_plugins(session, message)
                    if transformed is None:
                        continue
                    routed_message: WsMessage = transformed
                else:
                    transformed_binary = await self._apply_ws_binary_message_plugins(session, payload)
                    if transformed_binary is None:
                        continue
                    routed_message = transformed_binary

                handler = self.routes.get(session.path) or self.routes.get("*")
                if handler is None:
                    await self.send(session, {"error": "unknown-route", "path": session.path})
                    continue

                try:
                    result = handler(session, routed_message, self)
                    if inspect.isawaitable(result):
                        result = await result
                except Exception as error:
                    await self._notify_plugins(
                        "on_error",
                        error,
                        {"stage": "ws-handler", "path": session.path, "session_id": session.id},
                        self,
                    )
                    await self.send(session, {"error": "handler-failed", "detail": str(error)})
                    continue

                if result is not None:
                    await self.send(session, result)
        except Exception as error:
            await self._notify_plugins(
                "on_error",
                error,
                {"stage": "ws-read", "session_id": session.id if session else None, "remote_addr": remote_addr},
                self,
            )
        finally:
            if session is not None:
                self.clients.pop(session.id, None)
                await self._notify_plugins("on_ws_disconnect", session, self)
            await _close_writer_quietly(writer)

    async def _read_ws_handshake(
        self,
        reader: asyncio.StreamReader,
    ) -> tuple[str, dict[str, list[str]], dict[str, str]] | None:
        try:
            header_bytes = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            return None

        try:
            header_text = header_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return None

        lines = header_text.split("\r\n")
        if not lines or len(lines[0].split(" ")) < 3:
            return None

        method, target, _version = lines[0].split(" ", 2)
        if method.upper() != "GET":
            return None

        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()

        if headers.get("upgrade", "").lower() != "websocket":
            return None
        if "upgrade" not in headers.get("connection", "").lower():
            return None
        if "sec-websocket-key" not in headers:
            return None

        parsed = urlparse(target)
        path = parsed.path or "/"
        query_params = {key: values for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
        return path, query_params, headers

    async def _read_frame(self, reader: asyncio.StreamReader) -> tuple[int, bool, bytes] | None:
        try:
            header = await reader.readexactly(2)
        except asyncio.IncompleteReadError:
            return None

        first, second = header
        fin = (first >> 7) & 1
        opcode = first & 0x0F
        masked = (second >> 7) & 1
        payload_length = second & 0x7F

        if payload_length == 126:
            payload_length = struct.unpack("!H", await reader.readexactly(2))[0]
        elif payload_length == 127:
            payload_length = struct.unpack("!Q", await reader.readexactly(8))[0]

        if payload_length > self.max_message_size_bytes:
            raise ValueError("frame too large")

        if not masked:
            raise ValueError("client frames must be masked")

        mask = await reader.readexactly(4)
        payload = await reader.readexactly(payload_length)
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))

        return opcode, bool(fin), payload

    async def _write_frame(self, writer: asyncio.StreamWriter, opcode: int, payload: bytes) -> None:
        first = 0x80 | (opcode & 0x0F)
        length = len(payload)
        if length < 126:
            header = bytes((first, length))
        elif length < 65536:
            header = bytes((first, 126)) + struct.pack("!H", length)
        else:
            header = bytes((first, 127)) + struct.pack("!Q", length)
        writer.write(header + payload)
        await writer.drain()

    async def _safe_send_error(self, session: WebSocketClient, message: str) -> None:
        try:
            await self.send(session, {"type": "error", "message": message})
        except Exception:
            return

    async def _apply_ws_message_plugins(self, session: WebSocketClient, message: str) -> str | None:
        transformed: str | None = message
        for plugin in self.plugins:
            if transformed is None:
                return None
            transformed = await plugin.on_ws_message(session, transformed, self)
        return transformed

    async def _apply_ws_binary_message_plugins(self, session: WebSocketClient, data: bytes) -> bytes | None:
        transformed: bytes | None = data
        for plugin in self.plugins:
            if transformed is None:
                return None
            transformed = await plugin.on_ws_binary_message(session, transformed, self)
        return transformed

    async def _notify_plugins(self, hook_name: str, *args: Any) -> None:
        for plugin in self.plugins:
            hook = getattr(plugin, hook_name, None)
            if hook is None:
                continue
            try:
                result = hook(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # pragma: no cover - plugin errors should not crash server
                if hook_name == "on_error":
                    continue
                try:
                    await plugin.on_error(
                        RuntimeError(f"plugin hook failed: {plugin.name}.{hook_name}"),
                        {"stage": "plugin-hook", "hook": hook_name},
                        self,
                    )
                except Exception:
                    continue

    def _register_default_tools(self) -> None:
        self.register_tool("now", ServerTools.utc_now)
        self.register_tool("client_count", lambda: len(self.clients))
        self.register_tool(
            "list_clients",
            lambda: [{"id": session.id, "path": session.path} for session in self.clients.values()],
        )

        def uptime_seconds() -> float:
            if self.started_at is None:
                return 0.0
            return (datetime.now(timezone.utc) - self.started_at).total_seconds()

        self.register_tool("uptime_seconds", uptime_seconds)

    def _is_authorized(self, headers: dict[str, str], query_params: dict[str, list[str]]) -> bool:
        if not self.auth_token:
            return True

        query_token = query_params.get("token", [""])[0] if query_params.get("token") else ""
        header_token = headers.get("x-yserver-token", "")
        bearer_token = _extract_bearer_token(headers.get("authorization"))
        supplied = query_token or header_token or (bearer_token or "")
        if not supplied:
            return False
        return hmac.compare_digest(supplied, self.auth_token)

    async def _write_http_error(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        reason = "Unauthorized" if status == 401 else "Bad Request"
        payload = body.encode("utf-8")
        response_headers = {
            "Server": "yserver",
            "Connection": "close",
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(payload)),
        }
        if headers:
            response_headers.update(headers)

        lines = [f"HTTP/1.1 {status} {reason}"]
        for key, value in response_headers.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("")
        try:
            writer.write("\r\n".join(lines).encode("utf-8") + payload)
            await writer.drain()
        except Exception:
            await _close_writer_quietly(writer)
            return
        await _close_writer_quietly(writer)

    @staticmethod
    def _build_accept_value(client_key: str) -> str:
        digest = hashlib.sha1((client_key + WS_GUID).encode("utf-8")).digest()
        return base64.b64encode(digest).decode("utf-8")


class YHttpServer:
    """Minimal async HTTP server with TLS/auth/rate-limit support."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        ssl_context: ssl.SSLContext | None = None,
        auth_token: str | None = None,
        auth_exempt_paths: set[str] | None = None,
        rate_limit_per_window: int | None = 600,
        rate_limit_window_seconds: float = 60.0,
    ) -> None:
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.auth_token = auth_token
        self.auth_exempt_paths = set(auth_exempt_paths or set())
        self.plugins: list[ServerPlugin] = []
        self.routes: dict[tuple[str, str], HttpHandler] = {}
        self.tools: dict[str, Callable[..., Any]] = {}
        self.started_at: datetime | None = None

        self._server: asyncio.AbstractServer | None = None
        self._is_shutting_down = False
        self._rate_limiter = _SlidingWindowRateLimiter(rate_limit_per_window, rate_limit_window_seconds)
        self._register_default_tools()

    def add_plugin(self, plugin: ServerPlugin) -> "YHttpServer":
        self.plugins.append(plugin)
        return self

    def add_route(self, path: str, handler: HttpHandler, method: str = "GET") -> None:
        normalized_method = method.strip().upper() or "GET"
        normalized_path = path.strip() or "/"
        self.routes[(normalized_method, normalized_path)] = handler

    def route(self, path: str, method: str = "GET") -> Callable[[HttpHandler], HttpHandler]:
        def decorator(handler: HttpHandler) -> HttpHandler:
            self.add_route(path, handler, method=method)
            return handler

        return decorator

    def get(self, path: str) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="GET")

    def post(self, path: str) -> Callable[[HttpHandler], HttpHandler]:
        return self.route(path, method="POST")

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        key = name.strip().lower()
        if not key:
            raise ValueError("tool name cannot be empty")
        self.tools[key] = tool

    def tool(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.register_tool(name, func)
            return func

        return decorator

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        key = name.strip().lower()
        if key not in self.tools:
            raise KeyError(f"tool not found: {name}")
        return self.tools[key](*args, **kwargs)

    async def start(self) -> None:
        if self._server is not None:
            return
        self.started_at = datetime.now(timezone.utc)
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
            ssl=self.ssl_context,
        )
        await self._notify_plugins("on_startup", self)

    async def run(self) -> None:
        await self.start()
        if self._server is None:
            return
        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        try:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            await self._notify_plugins("on_shutdown", self)
        finally:
            self._is_shutting_down = False

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info("peername")
        remote_addr = _format_peer_name(peer_name)
        try:
            request = await self._read_http_request(reader, remote_addr=remote_addr)
            if request is None:
                await self._send_response(writer, 400, b"Bad Request", {"Content-Type": "text/plain; charset=utf-8"})
                return

            if not self._is_authorized(request):
                await self._send_response(
                    writer,
                    401,
                    b"Unauthorized",
                    {
                        "Content-Type": "text/plain; charset=utf-8",
                        "WWW-Authenticate": "Bearer",
                    },
                )
                return

            if not self._rate_limiter.allow(request.remote_addr):
                retry_after = self._rate_limiter.retry_after_seconds(request.remote_addr)
                await self._send_response(
                    writer,
                    429,
                    b"Too Many Requests",
                    {
                        "Content-Type": "text/plain; charset=utf-8",
                        "Retry-After": str(retry_after),
                    },
                )
                return

            await self._notify_plugins("on_http_request", request, self)
            handler = self._resolve_handler(request.method, request.path)

            if handler is None:
                await self._send_response(
                    writer,
                    404,
                    b"Not Found",
                    {"Content-Type": "text/plain; charset=utf-8"},
                )
                return

            try:
                result = handler(request, self)
                if inspect.isawaitable(result):
                    result = await result
            except Exception as error:
                await self._notify_plugins(
                    "on_error",
                    error,
                    {"stage": "http-handler", "method": request.method, "path": request.path},
                    self,
                )
                await self._send_response(
                    writer,
                    500,
                    b"Internal Server Error",
                    {"Content-Type": "text/plain; charset=utf-8"},
                )
                return

            status, body, headers = self._normalize_response(result)
            await self._send_response(writer, status, body, headers)
        except Exception as error:
            await self._notify_plugins("on_error", error, {"stage": "http-read"}, self)
            await self._send_response(
                writer,
                500,
                b"Internal Server Error",
                {"Content-Type": "text/plain; charset=utf-8"},
            )
        finally:
            await _close_writer_quietly(writer)

    async def _read_http_request(self, reader: asyncio.StreamReader, remote_addr: str) -> HttpRequest | None:
        try:
            header_bytes = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            return None

        try:
            header_text = header_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return None

        header_lines = header_text.split("\r\n")
        if not header_lines or len(header_lines[0].split(" ")) < 3:
            return None

        method, raw_target, _http_version = header_lines[0].split(" ", 2)
        headers: dict[str, str] = {}
        for line in header_lines[1:]:
            if not line:
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()

        raw_content_length = headers.get("content-length", "0") or "0"
        try:
            content_length = int(raw_content_length)
        except ValueError:
            return None

        body = b""
        if content_length > 0:
            body = await reader.readexactly(content_length)

        parsed = urlparse(raw_target)
        query_params = {key: values for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
        return HttpRequest(
            method=method.upper(),
            path=parsed.path or "/",
            query_params=query_params,
            headers=headers,
            body=body,
            remote_addr=remote_addr,
        )

    def _resolve_handler(self, method: str, path: str) -> HttpHandler | None:
        return (
            self.routes.get((method, path))
            or self.routes.get(("ANY", path))
            or self.routes.get((method, "*"))
            or self.routes.get(("ANY", "*"))
        )

    def _normalize_response(self, result: Any) -> tuple[int, bytes, dict[str, str]]:
        if result is None:
            return 204, b"", {"Content-Type": "text/plain; charset=utf-8"}

        if isinstance(result, tuple):
            if len(result) == 2:
                status, body = result
                headers: dict[str, str] = {}
            elif len(result) == 3:
                status, body, headers = result
            else:
                raise ValueError("tuple responses must be (status, body) or (status, body, headers)")
            status_code = int(status)
            return status_code, self._encode_http_body(body), dict(headers or {})

        if isinstance(result, bytes):
            return 200, result, {"Content-Type": "application/octet-stream"}

        if isinstance(result, str):
            return 200, result.encode("utf-8"), {"Content-Type": "text/plain; charset=utf-8"}

        return 200, ServerTools.to_json(result).encode("utf-8"), {"Content-Type": "application/json; charset=utf-8"}

    async def _send_response(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        reason = self._status_reason(status)
        response_headers = {
            "Server": "yserver",
            "Connection": "close",
            "Content-Length": str(len(body)),
        }
        if headers:
            response_headers.update(headers)

        lines = [f"HTTP/1.1 {status} {reason}"]
        for key, value in response_headers.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("")
        try:
            writer.write("\r\n".join(lines).encode("utf-8") + body)
            await writer.drain()
        except Exception:
            return

    async def _notify_plugins(self, hook_name: str, *args: Any) -> None:
        for plugin in self.plugins:
            hook = getattr(plugin, hook_name, None)
            if hook is None:
                continue
            try:
                result = hook(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # pragma: no cover - plugin errors should not crash server
                if hook_name == "on_error":
                    continue
                try:
                    await plugin.on_error(
                        RuntimeError(f"plugin hook failed: {plugin.name}.{hook_name}"),
                        {"stage": "plugin-hook", "hook": hook_name},
                        self,
                    )
                except Exception:
                    continue

    def _register_default_tools(self) -> None:
        self.register_tool("now", ServerTools.utc_now)

        def uptime_seconds() -> float:
            if self.started_at is None:
                return 0.0
            return (datetime.now(timezone.utc) - self.started_at).total_seconds()

        self.register_tool("uptime_seconds", uptime_seconds)

    def _is_authorized(self, request: HttpRequest) -> bool:
        if request.path in self.auth_exempt_paths:
            return True
        if not self.auth_token:
            return True

        query_token = request.query_params.get("token", [""])[0] if request.query_params.get("token") else ""
        header_token = request.headers.get("x-yserver-token", "")
        bearer_token = _extract_bearer_token(request.headers.get("authorization"))
        supplied = query_token or header_token or (bearer_token or "")
        if not supplied:
            return False
        return hmac.compare_digest(supplied, self.auth_token)

    @staticmethod
    def _encode_http_body(body: Any) -> bytes:
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode("utf-8")
        return ServerTools.to_json(body).encode("utf-8")

    @staticmethod
    def _status_reason(status: int) -> str:
        reasons = {
            200: "OK",
            201: "Created",
            202: "Accepted",
            204: "No Content",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            429: "Too Many Requests",
            500: "Internal Server Error",
        }
        return reasons.get(status, "OK")

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
