"""Native WebSocket server (RFC 6455), no external dependencies.

Speaks WebSocket concepts directly: connections, text and binary messages,
rooms, broadcasts, close codes and ping/pong keepalive. Large payloads can be
streamed out as fragmented frames instead of being built in memory.

It can run standalone on its own port, or be mounted on a
:class:`~yashserver.http.YHttpServer` via ``mount_websocket`` so an app and its
socket share one origin and one certificate.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
import ssl
import struct
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable
from urllib.parse import parse_qs, unquote, urlparse

from .core import (
    AuthConfig,
    BaseServer,
    ConfigError,
    RateLimitConfig,
    ServerConfig,
    TLSConfig,
    close_listener_quietly,
    close_writer_quietly,
    format_peer_name,
    maybe_await,
    resolve_ssl_context,
)
from .tools import ServerTools

__all__ = [
    "CloseCode",
    "WebSocketClient",
    "WebSocketConfig",
    "WebSocketConnection",
    "WsMessage",
    "YWebSocketServer",
]

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
DDOS_BLOCK_MESSAGE = (
    "<red_gradiat>Yashserver<orange_gradiate> <plain_black> blocked you for suspecting of DDOSing<plain_black>"
)

WsMessage = "str | bytes"

# Opcodes
OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class CloseCode:
    """RFC 6455 close codes used by this server."""

    NORMAL = 1000
    GOING_AWAY = 1001
    PROTOCOL_ERROR = 1002
    UNSUPPORTED_DATA = 1003
    INVALID_PAYLOAD = 1007
    POLICY_VIOLATION = 1008
    MESSAGE_TOO_BIG = 1009
    INTERNAL_ERROR = 1011


@dataclass
class WebSocketConfig(ServerConfig):
    """WebSocket-specific options."""

    #: Largest complete message accepted, after reassembling fragments.
    max_message_size_bytes: int = 8 * 1024 * 1024
    #: Largest single frame accepted. Guards against a declared-huge frame.
    max_frame_size_bytes: int = 8 * 1024 * 1024
    #: Seconds between keepalive pings. ``None`` disables them.
    ping_interval_seconds: float | None = 20.0
    #: Seconds to wait for a pong before dropping the connection.
    ping_timeout_seconds: float = 20.0
    #: Close a connection that sends nothing for this long. ``None`` disables.
    idle_timeout_seconds: float | None = 300.0
    #: Seconds a single frame write may block before the peer is dropped.
    write_timeout_seconds: float = 30.0
    #: Fragment size used by :meth:`YWebSocketServer.send_stream`.
    stream_chunk_size: int = 64 * 1024
    #: Concurrent connection ceiling. ``None`` for no cap.
    max_connections: int | None = 10_000
    #: Seconds allowed for the opening handshake.
    handshake_timeout_seconds: float = 10.0
    #: Advertised subprotocols; the first client match is selected.
    subprotocols: tuple[str, ...] = ()
    write_buffer_high_bytes: int = 512 * 1024
    write_buffer_low_bytes: int = 128 * 1024
    backlog: int = 128


class WebSocketConnection:
    """One live WebSocket connection.

    Writes are serialised by a per-connection lock, because two tasks
    interleaving frames on one socket produces a protocol violation that is
    miserable to debug.
    """

    __slots__ = (
        "id",
        "reader",
        "writer",
        "path",
        "query_params",
        "headers",
        "remote_addr",
        "authenticated",
        "connected_at",
        "subprotocol",
        "rooms",
        "state",
        "messages_received",
        "messages_sent",
        "last_seen",
        "_server",
        "_send_lock",
        "_pong_waiter",
        "_close_sent",
        "__weakref__",
    )

    def __init__(
        self,
        *,
        id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        path: str,
        query_params: dict[str, list[str]] | None = None,
        headers: dict[str, str] | None = None,
        remote_addr: str = "unknown",
        authenticated: bool = False,
        connected_at: datetime | None = None,
        server: "YWebSocketServer | None" = None,
        subprotocol: str | None = None,
    ) -> None:
        self.id = id
        self.reader = reader
        self.writer = writer
        self.path = path
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.remote_addr = remote_addr
        self.authenticated = authenticated
        self.connected_at = connected_at or datetime.now(timezone.utc)
        self.subprotocol = subprotocol
        self.rooms: set[str] = set()
        self.state = "open"
        self.messages_received = 0
        self.messages_sent = 0
        self.last_seen = time.monotonic()
        self._server = server
        self._send_lock = asyncio.Lock()
        self._pong_waiter: asyncio.Future[bytes] | None = None
        self._close_sent = False

    @property
    def is_open(self) -> bool:
        return self.state == "open" and not self.writer.is_closing()

    @property
    def tls(self) -> bool:
        return self.writer.get_extra_info("ssl_object") is not None

    def query(self, name: str, default: str | None = None) -> str | None:
        values = self.query_params.get(name)
        return values[0] if values else default

    # -- writing ---------------------------------------------------------

    async def send(self, payload: Any) -> None:
        """Send one message. ``bytes`` go binary, everything else goes text."""

        if isinstance(payload, (bytes, bytearray)):
            await self._write_frame(OP_BINARY, bytes(payload))
        elif isinstance(payload, str):
            await self._write_frame(OP_TEXT, payload.encode("utf-8"))
        else:
            await self._write_frame(OP_TEXT, ServerTools.to_json(payload).encode("utf-8"))
        self.messages_sent += 1

    async def send_text(self, text: str) -> None:
        await self._write_frame(OP_TEXT, text.encode("utf-8"))
        self.messages_sent += 1

    async def send_bytes(self, data: bytes) -> None:
        await self._write_frame(OP_BINARY, bytes(data))
        self.messages_sent += 1

    async def send_stream(self, chunks: Any, *, binary: bool = True) -> None:
        """Send one logical message as a sequence of fragmented frames.

        Lets you push a large file or a live feed without ever holding the
        whole message in memory. Accepts sync or async iterables of bytes.
        """

        first = True
        async with self._send_lock:
            async for chunk in _aiter(chunks):
                data = chunk if isinstance(chunk, (bytes, bytearray)) else str(chunk).encode("utf-8")
                if not data:
                    continue
                opcode = (OP_BINARY if binary else OP_TEXT) if first else OP_CONTINUATION
                await self._write_locked(opcode, bytes(data), fin=False)
                first = False
            # An empty final frame terminates the message.
            opcode = (OP_BINARY if binary else OP_TEXT) if first else OP_CONTINUATION
            await self._write_locked(opcode, b"", fin=True)
        self.messages_sent += 1

    async def ping(self, data: bytes = b"") -> None:
        await self._write_frame(OP_PING, data[:125])

    async def _write_frame(self, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        if not self.is_open and opcode != OP_CLOSE:
            raise ConnectionError(f"websocket {self.id} is closed")
        async with self._send_lock:
            await self._write_locked(opcode, payload, fin=fin)

    async def _write_locked(self, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        header = _build_frame_header(opcode, len(payload), fin=fin)
        self.writer.write(header + payload)
        timeout = self._server.config.write_timeout_seconds if self._server else 30.0
        try:
            await asyncio.wait_for(self.writer.drain(), timeout=timeout)
        except asyncio.TimeoutError as error:
            self.state = "stalled"
            raise TimeoutError(f"websocket {self.id} did not drain within {timeout}s") from error

    # -- rooms -------------------------------------------------------------

    def in_room(self, room: str) -> bool:
        return room in self.rooms

    # -- closing -----------------------------------------------------------

    async def close(self, code: int = CloseCode.NORMAL, reason: str = "") -> None:
        """Send a close frame and shut the connection down."""

        if self.state == "closed":
            return
        self.state = "closing"
        if not self._close_sent:
            self._close_sent = True
            try:
                payload = struct.pack("!H", code) + reason.encode("utf-8")[:123]
                await asyncio.wait_for(self._write_frame(OP_CLOSE, payload), timeout=2.0)
            except Exception:
                pass
        self.state = "closed"
        await close_writer_quietly(self.writer, timeout_seconds=1.0)

    def __repr__(self) -> str:
        return f"<WebSocketConnection {self.id[:8]} {self.path} {self.state}>"


#: Historical name. ``WebSocketClient`` and ``WebSocketConnection`` are the same class.
WebSocketClient = WebSocketConnection

WsHandler = Callable[[WebSocketConnection, Any, "YWebSocketServer"], Awaitable[Any] | Any]


def _build_frame_header(opcode: int, length: int, *, fin: bool = True) -> bytes:
    first = (0x80 if fin else 0x00) | (opcode & 0x0F)
    if length < 126:
        return bytes((first, length))
    if length < 65536:
        return bytes((first, 126)) + struct.pack("!H", length)
    return bytes((first, 127)) + struct.pack("!Q", length)


async def _aiter(source: Any) -> AsyncIterator[Any]:
    if hasattr(source, "__aiter__"):
        async for item in source:
            yield item
        return
    for item in source:
        yield item


class YWebSocketServer(BaseServer):
    """WebSocket server with rooms, broadcasts, auth, rate limiting and TLS."""

    protocol = "ws"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9001,
        ssl_context: ssl.SSLContext | None = None,
        auth_token: str | None = None,
        rate_limit_per_window: int | None = 300,
        rate_limit_window_seconds: float = 60.0,
        ddosprot: bool = True,
        max_message_size_bytes: int = 8 * 1024 * 1024,
        *,
        tls: TLSConfig | None = None,
        auth: AuthConfig | None = None,
        config: WebSocketConfig | None = None,
        logger: logging.Logger | None = None,
        **options: Any,
    ) -> None:
        auth_config = auth or AuthConfig(token=auth_token)
        if auth is not None and auth_token:
            auth_config.token = auth_token

        resolved = config or WebSocketConfig(
            host=host,
            port=port,
            ssl_context=resolve_ssl_context(ssl_context, tls),
            auth=auth_config,
            rate_limit=RateLimitConfig(limit=rate_limit_per_window, window_seconds=rate_limit_window_seconds),
            ddosprot=ddosprot,
            max_message_size_bytes=max(1024, int(max_message_size_bytes)),
            max_frame_size_bytes=max(1024, int(max_message_size_bytes)),
        )
        for key, value in options.items():
            if not hasattr(resolved, key):
                raise ConfigError(f"unknown WebSocket option: {key}")
            setattr(resolved, key, value)

        super().__init__(resolved, logger=logger)
        self.config: WebSocketConfig = resolved

        self.clients: dict[str, WebSocketConnection] = {}
        self.routes: dict[str, WsHandler] = {}
        self._rooms: dict[str, set[str]] = {}
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[asyncio.Task[Any]] = set()
        #: The port actually bound, remembered so a rebind lands on it again.
        self._listen_port: int | None = None
        self._register_ws_tools()

    # -- configuration passthroughs -----------------------------------------

    @property
    def max_message_size_bytes(self) -> int:
        return self.config.max_message_size_bytes

    @max_message_size_bytes.setter
    def max_message_size_bytes(self, value: int) -> None:
        self.config.max_message_size_bytes = max(1024, int(value))

    def _bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return self.config.port or None
        return int(self._server.sockets[0].getsockname()[1])

    # -- routing -------------------------------------------------------------

    def add_route(self, path: str, handler: WsHandler) -> None:
        self.routes[path.strip() or "/"] = handler

    def route(self, path: str) -> Callable[[WsHandler], WsHandler]:
        def decorator(handler: WsHandler) -> WsHandler:
            self.add_route(path, handler)
            return handler

        return decorator

    # -- lifecycle -----------------------------------------------------------

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
        # `wait_closed()` also waits for every connection handler, so it runs
        # last; awaiting it here would deadlock on live sockets.
        server = self._server
        self._server = None
        close_listener_quietly(server)

        # Tell peers why they are being disconnected instead of just vanishing.
        sessions = list(self.clients.values())
        if sessions:
            await asyncio.gather(
                *(session.close(CloseCode.GOING_AWAY, "server shutting down") for session in sessions),
                return_exceptions=True,
            )

        remaining = drain_deadline - time.monotonic()
        if self._connections and remaining > 0:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._connections), return_exceptions=True),
                    timeout=remaining,
                )
            except Exception:
                pass
        for task in list(self._connections):
            task.cancel()
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)
        self._connections.clear()
        self.clients.clear()
        self._rooms.clear()

        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except Exception:
                pass

    # -- sending ---------------------------------------------------------------

    async def send(self, session_or_id: WebSocketConnection | str, payload: Any) -> None:
        session = self._resolve(session_or_id)
        await session.send(payload)
        self.metrics.incr("messages_sent")

    async def send_stream(
        self,
        session_or_id: WebSocketConnection | str,
        chunks: Any,
        *,
        binary: bool = True,
    ) -> None:
        """Stream one large message out as fragments."""

        session = self._resolve(session_or_id)
        await session.send_stream(chunks, binary=binary)
        self.metrics.incr("messages_sent")

    async def broadcast(self, payload: Any, exclude: str | None = None) -> int:
        """Send to every open connection. Returns the delivered count."""

        return await self._fan_out(list(self.clients.values()), payload, exclude)

    async def broadcast_to_room(self, room: str, payload: Any, exclude: str | None = None) -> int:
        """Send to every member of ``room``. Returns the delivered count."""

        members = self.room_members(room)
        return await self._fan_out(members, payload, exclude)

    async def _fan_out(
        self,
        sessions: Iterable[WebSocketConnection],
        payload: Any,
        exclude: str | None,
    ) -> int:
        targets = [session for session in sessions if session.is_open and session.id != exclude]
        if not targets:
            return 0

        async def deliver(session: WebSocketConnection) -> bool:
            try:
                await session.send(payload)
                return True
            except Exception as error:
                await self._report_error(error, {"stage": "broadcast", "session_id": session.id})
                if isinstance(error, (TimeoutError, asyncio.TimeoutError, ConnectionError)):
                    self.metrics.incr("stalled_disconnects")
                    await session.close(CloseCode.POLICY_VIOLATION, "write timeout")
                return False

        results = await asyncio.gather(*(deliver(session) for session in targets))
        delivered = sum(1 for ok in results if ok)
        self.metrics.incr("broadcasts")
        self.metrics.incr("messages_sent", delivered)
        return delivered

    def _resolve(self, session_or_id: WebSocketConnection | str) -> WebSocketConnection:
        if isinstance(session_or_id, WebSocketConnection):
            return session_or_id
        session = self.clients.get(session_or_id)
        if session is None:
            raise KeyError(f"unknown session: {session_or_id}")
        return session

    # -- rooms -------------------------------------------------------------------

    def join_room(self, session_or_id: WebSocketConnection | str, room: str) -> None:
        session = self._resolve(session_or_id)
        session.rooms.add(room)
        self._rooms.setdefault(room, set()).add(session.id)
        self.metrics.gauge("rooms", len(self._rooms))

    def leave_room(self, session_or_id: WebSocketConnection | str, room: str) -> None:
        session = self._resolve(session_or_id)
        session.rooms.discard(room)
        members = self._rooms.get(room)
        if members is not None:
            members.discard(session.id)
            if not members:
                del self._rooms[room]
        self.metrics.gauge("rooms", len(self._rooms))

    def room_members(self, room: str) -> list[WebSocketConnection]:
        return [
            session
            for session in (self.clients.get(member_id) for member_id in self._rooms.get(room, set()))
            if session is not None
        ]

    def rooms(self) -> dict[str, int]:
        return {room: len(members) for room, members in self._rooms.items()}

    def _drop_from_rooms(self, session: WebSocketConnection) -> None:
        for room in list(session.rooms):
            members = self._rooms.get(room)
            if members is not None:
                members.discard(session.id)
                if not members:
                    del self._rooms[room]
        session.rooms.clear()

    async def disconnect(
        self,
        session_or_id: WebSocketConnection | str,
        code: int = CloseCode.NORMAL,
        reason: str = "",
    ) -> None:
        await self._resolve(session_or_id).close(code, reason)

    # -- connection handling -------------------------------------------------------

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            handshake = await self._read_handshake(reader)
            if handshake is None:
                await close_writer_quietly(writer, timeout_seconds=0.5)
                return
            path, query_params, headers = handshake
            remote_addr = format_peer_name(writer.get_extra_info("peername"))
            await self.serve_upgrade(
                reader,
                writer,
                path=path,
                query_params=query_params,
                headers=headers,
                remote_addr=remote_addr,
                _own_task=False,
            )
        finally:
            if task is not None:
                self._connections.discard(task)

    async def serve_upgrade(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        path: str,
        query_params: dict[str, list[str]],
        headers: dict[str, str],
        remote_addr: str,
        _own_task: bool = True,
    ) -> None:
        """Complete a handshake on an already-read request head.

        Used both by this server's own listener and by
        :meth:`~yashserver.http.YHttpServer.mount_websocket`.
        """

        task = asyncio.current_task()
        if _own_task and task is not None:
            self._connections.add(task)

        session: WebSocketConnection | None = None
        try:
            max_connections = self.config.max_connections
            if max_connections is not None and len(self.clients) >= max_connections:
                self.metrics.incr("connections_refused")
                await self._write_http_error(writer, 503, "Server too busy")
                return

            if "sec-websocket-key" not in headers:
                await self._write_http_error(writer, 400, "Bad Request")
                return

            if not await self.config.auth.authorize(
                headers=headers,
                query_params=query_params,
                path=path,
                remote_addr=remote_addr,
            ):
                self.metrics.incr("unauthorized")
                await self._write_http_error(writer, 401, "Unauthorized", {"WWW-Authenticate": "Bearer"})
                return

            if not self._allow_request(remote_addr):
                await self._write_http_error(writer, 429, "Too Many Requests", {"Retry-After": "1"})
                return

            subprotocol = self._select_subprotocol(headers)
            accept_value = self._build_accept_value(headers["sec-websocket-key"])
            response_lines = [
                "HTTP/1.1 101 Switching Protocols",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Accept: {accept_value}",
            ]
            if subprotocol:
                response_lines.append(f"Sec-WebSocket-Protocol: {subprotocol}")
            response_lines.extend(("", ""))
            writer.write("\r\n".join(response_lines).encode("utf-8"))
            await writer.drain()

            self._tune_transport(writer)

            session = WebSocketConnection(
                id=uuid.uuid4().hex,
                reader=reader,
                writer=writer,
                path=path,
                query_params=query_params,
                headers=headers,
                remote_addr=remote_addr,
                authenticated=True,
                server=self,
                subprotocol=subprotocol,
            )
            self.clients[session.id] = session
            self.metrics.incr("connections_opened")
            self.metrics.gauge("connections_active", len(self.clients))
            await self._notify_plugins("on_ws_connect", session, self)

            keepalive = None
            if self.config.ping_interval_seconds:
                keepalive = asyncio.ensure_future(self._keepalive(session))
            try:
                await self._read_loop(session)
            finally:
                if keepalive is not None:
                    keepalive.cancel()
                    await asyncio.gather(keepalive, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as error:
            await self._report_error(
                error,
                {"stage": "ws-read", "session_id": session.id if session else None, "remote_addr": remote_addr},
            )
        except Exception as error:
            await self._report_error(
                error,
                {"stage": "ws-read", "session_id": session.id if session else None, "remote_addr": remote_addr},
            )
        finally:
            if session is not None:
                self.clients.pop(session.id, None)
                self._drop_from_rooms(session)
                self.metrics.incr("connections_closed")
                self.metrics.gauge("connections_active", len(self.clients))
                await self._notify_plugins("on_ws_disconnect", session, self)
                session.state = "closed"
            await close_writer_quietly(writer, timeout_seconds=1.0)
            if _own_task and task is not None:
                self._connections.discard(task)

    def _tune_transport(self, writer: asyncio.StreamWriter) -> None:
        try:
            writer.transport.set_write_buffer_limits(
                high=self.config.write_buffer_high_bytes,
                low=self.config.write_buffer_low_bytes,
            )
        except (AttributeError, NotImplementedError):
            pass

    def _select_subprotocol(self, headers: dict[str, str]) -> str | None:
        offered = self.config.subprotocols
        if not offered:
            return None
        requested = [value.strip() for value in (headers.get("sec-websocket-protocol") or "").split(",")]
        for candidate in requested:
            if candidate in offered:
                return candidate
        return None

    async def _keepalive(self, session: WebSocketConnection) -> None:
        interval = self.config.ping_interval_seconds or 20.0
        while session.is_open:
            await asyncio.sleep(interval)
            if not session.is_open:
                return
            loop = asyncio.get_running_loop()
            waiter: asyncio.Future[bytes] = loop.create_future()
            session._pong_waiter = waiter
            token = os.urandom(4)
            try:
                await session.ping(token)
            except Exception:
                return
            try:
                await asyncio.wait_for(waiter, timeout=self.config.ping_timeout_seconds)
            except asyncio.TimeoutError:
                self.metrics.incr("ping_timeouts")
                await session.close(CloseCode.GOING_AWAY, "ping timeout")
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                return
            finally:
                session._pong_waiter = None

    async def _read_loop(self, session: WebSocketConnection) -> None:
        continuation_opcode: int | None = None
        continuation_payload = bytearray()
        idle_timeout = self.config.idle_timeout_seconds

        while session.is_open:
            try:
                if idle_timeout is None:
                    frame = await self._read_frame(session.reader)
                else:
                    frame = await asyncio.wait_for(self._read_frame(session.reader), timeout=idle_timeout)
            except asyncio.TimeoutError:
                self.metrics.incr("idle_timeouts")
                await session.close(CloseCode.GOING_AWAY, "idle timeout")
                return
            except _ProtocolViolation as error:
                await session.close(error.code, error.reason)
                return
            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
                return

            if frame is None:
                return
            opcode, fin, payload = frame
            session.last_seen = time.monotonic()

            # Control frames
            if opcode == OP_CLOSE:
                code, reason = _parse_close_payload(payload)
                await self._notify_plugins("on_ws_close", session, code, reason, self)
                await session.close(code if code else CloseCode.NORMAL, "")
                return
            if opcode == OP_PING:
                try:
                    await session._write_frame(OP_PONG, payload)
                except Exception:
                    return
                continue
            if opcode == OP_PONG:
                waiter = session._pong_waiter
                if waiter is not None and not waiter.done():
                    waiter.set_result(payload)
                continue

            # Data frames
            if opcode == OP_CONTINUATION:
                if continuation_opcode is None:
                    await session.close(CloseCode.PROTOCOL_ERROR, "unexpected continuation")
                    return
                continuation_payload.extend(payload)
                if len(continuation_payload) > self.config.max_message_size_bytes:
                    self.metrics.incr("oversized_messages")
                    await session.close(CloseCode.MESSAGE_TOO_BIG, "message too large")
                    return
                if not fin:
                    continue
                opcode = continuation_opcode
                payload = bytes(continuation_payload)
                continuation_opcode = None
                continuation_payload = bytearray()
            elif opcode in (OP_TEXT, OP_BINARY):
                if continuation_opcode is not None:
                    await session.close(CloseCode.PROTOCOL_ERROR, "interleaved data frames")
                    return
                if not fin:
                    continuation_opcode = opcode
                    continuation_payload = bytearray(payload)
                    if len(continuation_payload) > self.config.max_message_size_bytes:
                        self.metrics.incr("oversized_messages")
                        await session.close(CloseCode.MESSAGE_TOO_BIG, "message too large")
                        return
                    continue
                if len(payload) > self.config.max_message_size_bytes:
                    self.metrics.incr("oversized_messages")
                    await session.close(CloseCode.MESSAGE_TOO_BIG, "message too large")
                    return
            else:
                await session.close(CloseCode.PROTOCOL_ERROR, f"unknown opcode {opcode}")
                return

            session.messages_received += 1
            self.metrics.incr("messages_received")

            if not self._allow_request(session.remote_addr):
                await self._safe_send(session, {"type": "error", "message": DDOS_BLOCK_MESSAGE})
                continue

            await self._route_message(session, opcode, payload)

    async def _route_message(self, session: WebSocketConnection, opcode: int, payload: bytes) -> None:
        if opcode == OP_TEXT:
            try:
                # RFC 6455: a text frame that is not valid UTF-8 is a protocol
                # error, not something to silently mangle.
                message: Any = payload.decode("utf-8")
            except UnicodeDecodeError:
                self.metrics.incr("invalid_utf8")
                await session.close(CloseCode.INVALID_PAYLOAD, "invalid utf-8")
                return
            transformed = await self._apply_plugins("on_ws_message", session, message)
        else:
            transformed = await self._apply_plugins("on_ws_binary_message", session, payload)

        if transformed is None:
            return

        handler = self.routes.get(session.path) or self.routes.get("*")
        if handler is None:
            await self._safe_send(session, {"error": "unknown-route", "path": session.path})
            return

        started = time.monotonic()
        try:
            result = await maybe_await(handler(session, transformed, self))
        except Exception as error:
            await self._report_error(
                error,
                {"stage": "ws-handler", "path": session.path, "session_id": session.id},
            )
            await self._safe_send(session, {"error": "handler-failed", "detail": str(error)})
            return
        finally:
            self.metrics.observe("handler_seconds", time.monotonic() - started)

        if result is not None:
            await self._safe_send(session, result)

    async def _safe_send(self, session: WebSocketConnection, payload: Any) -> None:
        try:
            await session.send(payload)
            self.metrics.incr("messages_sent")
        except Exception:
            return

    async def _apply_plugins(self, hook_name: str, session: WebSocketConnection, message: Any) -> Any:
        current = message
        for plugin in self.plugins:
            if current is None:
                return None
            hook = getattr(plugin, hook_name, None)
            if hook is None:
                continue
            try:
                current = await maybe_await(hook(session, current, self))
            except Exception as error:
                await self._report_error(
                    error,
                    {"stage": f"{hook_name}-plugin", "plugin": getattr(plugin, "name", "?")},
                )
        return current

    # -- framing ------------------------------------------------------------------

    async def _read_handshake(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, dict[str, list[str]], dict[str, str]] | None:
        try:
            header_bytes = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=self.config.handshake_timeout_seconds,
            )
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
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
        path = unquote(parsed.path) or "/"
        query_params = dict(parse_qs(parsed.query, keep_blank_values=True))
        return path, query_params, headers

    async def _read_frame(self, reader: asyncio.StreamReader) -> tuple[int, bool, bytes] | None:
        try:
            header = await reader.readexactly(2)
        except asyncio.IncompleteReadError:
            return None

        first, second = header
        fin = bool((first >> 7) & 1)
        reserved = (first >> 4) & 0x07
        opcode = first & 0x0F
        masked = (second >> 7) & 1
        payload_length = second & 0x7F

        if reserved:
            # No extensions are negotiated, so RSV bits must be zero.
            raise _ProtocolViolation(CloseCode.PROTOCOL_ERROR, "reserved bits set")

        is_control = opcode >= 0x8
        if is_control:
            if payload_length > 125:
                raise _ProtocolViolation(CloseCode.PROTOCOL_ERROR, "control frame too long")
            if not fin:
                raise _ProtocolViolation(CloseCode.PROTOCOL_ERROR, "fragmented control frame")

        if payload_length == 126:
            payload_length = struct.unpack("!H", await reader.readexactly(2))[0]
        elif payload_length == 127:
            payload_length = struct.unpack("!Q", await reader.readexactly(8))[0]

        if payload_length > self.config.max_frame_size_bytes:
            # Refuse before allocating: a client can claim a 2^63-byte frame.
            raise _ProtocolViolation(CloseCode.MESSAGE_TOO_BIG, "frame too large")

        if not masked:
            raise _ProtocolViolation(CloseCode.PROTOCOL_ERROR, "client frames must be masked")

        mask = await reader.readexactly(4)
        payload = bytearray(await reader.readexactly(payload_length))
        for index in range(payload_length):
            payload[index] ^= mask[index & 3]

        self.metrics.incr("bytes_received", payload_length)
        return opcode, fin, bytes(payload)

    async def _write_http_error(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        from .http import status_reason

        payload = body.encode("utf-8")
        response_headers = {
            "Server": "yashserver",
            "Connection": "close",
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(payload)),
        }
        if headers:
            response_headers.update(headers)

        lines = [f"HTTP/1.1 {status} {status_reason(status)}"]
        lines.extend(f"{key}: {value}" for key, value in response_headers.items())
        lines.extend(("", ""))
        try:
            writer.write("\r\n".join(lines).encode("utf-8") + payload)
            await writer.drain()
        except Exception:
            pass
        await close_writer_quietly(writer, timeout_seconds=0.5)

    @staticmethod
    def _build_accept_value(client_key: str) -> str:
        digest = hashlib.sha1((client_key + WS_GUID).encode("utf-8")).digest()
        return base64.b64encode(digest).decode("utf-8")

    # -- tools -----------------------------------------------------------------------

    def _register_ws_tools(self) -> None:
        self.register_tool("client_count", lambda: len(self.clients))
        self.register_tool(
            "list_clients",
            lambda: [
                {"id": session.id, "path": session.path, "rooms": sorted(session.rooms)}
                for session in self.clients.values()
            ],
        )
        self.register_tool("rooms", self.rooms)


class _ProtocolViolation(Exception):
    def __init__(self, code: int, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(f"{code}: {reason}")


def _parse_close_payload(payload: bytes) -> tuple[int, str]:
    if len(payload) < 2:
        return CloseCode.NORMAL, ""
    code = struct.unpack("!H", payload[:2])[0]
    reason = payload[2:].decode("utf-8", errors="replace")
    return code, reason
