"""Async TCP server.

Two ways to use it, both on the same server object:

* **Command routing** (the historical yserver API): messages are delimited
  text lines, the first word selects a route, the rest is the payload.
* **Raw connection handling**: register :meth:`YTcpServer.on_connection` and
  own the socket yourself for a custom binary protocol.

Both get connection lifecycle management, configurable timeouts,
backpressure, graceful disconnects, and TLS.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable

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
    "TcpClient",
    "TcpConfig",
    "TcpConnection",
    "YTcpServer",
]

DDOS_BLOCK_MESSAGE = (
    "<red_gradiat>Yashserver<orange_gradiate> <plain_black> blocked you for suspecting of DDOSing<plain_black>"
)


@dataclass
class TcpConfig(ServerConfig):
    """TCP-specific options on top of :class:`~yashserver.core.ServerConfig`."""

    #: Message delimiter for command routing.
    delimiter: str = "\n"
    #: Largest single delimited message accepted, in bytes. Protects against a
    #: peer that opens a connection and never sends a delimiter.
    max_line_bytes: int = 64 * 1024
    #: Seconds to wait for the next message before closing an idle peer.
    #: ``None`` disables the idle timeout.
    idle_timeout_seconds: float | None = 300.0
    #: Seconds a single ``drain()`` may block before the peer is considered
    #: stalled and disconnected. This is the backpressure escape hatch.
    write_timeout_seconds: float = 30.0
    #: Seconds allowed for the TLS handshake and first byte.
    connect_timeout_seconds: float = 30.0
    #: Refuse new connections past this many concurrent peers. ``None`` for no cap.
    max_connections: int | None = 10_000
    #: asyncio write-buffer watermarks, in bytes. When the kernel buffer plus
    #: this much data is outstanding, ``drain()`` blocks, which is how
    #: backpressure reaches your handler.
    write_buffer_high_bytes: int = 256 * 1024
    write_buffer_low_bytes: int = 64 * 1024
    #: Listen backlog.
    backlog: int = 128
    #: Disable Nagle's algorithm. Right for interactive/RPC traffic.
    tcp_nodelay: bool = True


class TcpConnection:
    """One live TCP peer.

    Wraps the asyncio reader/writer pair with a send lock (so concurrent
    tasks cannot interleave writes), a write timeout (so one stalled peer
    cannot pin a broadcast forever), and cooperative close.
    """

    __slots__ = (
        "id",
        "reader",
        "writer",
        "address",
        "peer",
        "connected_at",
        "state",
        "bytes_sent",
        "bytes_received",
        "messages_received",
        "_server",
        "_send_lock",
        "_closing",
        "__weakref__",
    )

    def __init__(
        self,
        *,
        id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        address: str,
        peer: tuple[str, int] | None = None,
        server: "YTcpServer | None" = None,
        connected_at: datetime | None = None,
    ) -> None:
        self.id = id
        self.reader = reader
        self.writer = writer
        self.address = address
        self.peer = peer
        self.connected_at = connected_at or datetime.now(timezone.utc)
        self.state = "open"
        self.bytes_sent = 0
        self.bytes_received = 0
        self.messages_received = 0
        self._server = server
        self._send_lock = asyncio.Lock()
        self._closing = False

    # -- introspection ---------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self.state == "open" and not self.writer.is_closing()

    @property
    def peer_host(self) -> str:
        return self.address

    @property
    def peer_port(self) -> int | None:
        return self.peer[1] if self.peer else None

    @property
    def tls(self) -> bool:
        return self.writer.get_extra_info("ssl_object") is not None

    def peer_certificate(self) -> dict[str, Any] | None:
        """The verified client certificate, when mutual TLS is in use."""

        ssl_object = self.writer.get_extra_info("ssl_object")
        return ssl_object.getpeercert() if ssl_object is not None else None

    # -- writing ---------------------------------------------------------

    async def send_bytes(self, data: bytes, *, timeout: float | None = None) -> None:
        """Write raw bytes, applying backpressure and the write timeout."""

        if not self.is_open:
            raise ConnectionError(f"connection {self.id} is closed")
        limit = timeout if timeout is not None else self._write_timeout()
        async with self._send_lock:
            self.writer.write(data)
            try:
                if limit is None:
                    await self.writer.drain()
                else:
                    await asyncio.wait_for(self.writer.drain(), timeout=limit)
            except asyncio.TimeoutError as error:
                self.state = "stalled"
                raise TimeoutError(f"peer {self.address} did not drain within {limit}s") from error
            self.bytes_sent += len(data)

    async def send(self, payload: Any, *, timeout: float | None = None) -> None:
        """Write ``payload`` as one delimited message.

        ``bytes`` are sent as-is, ``str`` is UTF-8 encoded, anything else is
        JSON encoded. A trailing delimiter is added when missing.
        """

        delimiter = self._delimiter().encode("utf-8")
        encoded = _encode_payload(payload)
        if delimiter and not encoded.endswith(delimiter):
            encoded += delimiter
        await self.send_bytes(encoded, timeout=timeout)

    # -- reading ---------------------------------------------------------

    async def read(self, n: int = -1) -> bytes:
        return await self.reader.read(n)

    async def readexactly(self, n: int) -> bytes:
        return await self.reader.readexactly(n)

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        return await self.reader.readuntil(separator)

    async def stream(self, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        """Iterate incoming bytes until the peer closes.

        Handy for custom protocols and for piping an upload to disk without
        buffering it in memory.
        """

        while True:
            chunk = await self.reader.read(chunk_size)
            if not chunk:
                return
            self.bytes_received += len(chunk)
            yield chunk

    # -- closing ---------------------------------------------------------

    async def close(self, *, drain: bool = True, timeout: float = 1.0) -> None:
        """Close politely: flush what is queued, then shut the socket down."""

        if self._closing:
            return
        self._closing = True
        self.state = "closed"
        if drain:
            try:
                await asyncio.wait_for(self.writer.drain(), timeout=max(0.05, timeout))
            except Exception:
                pass
        await close_writer_quietly(self.writer, timeout_seconds=timeout)

    def _delimiter(self) -> str:
        return self._server.config.delimiter if self._server is not None else "\n"

    def _write_timeout(self) -> float | None:
        if self._server is None:
            return None
        return self._server.config.write_timeout_seconds

    def __repr__(self) -> str:
        return f"<TcpConnection {self.id[:8]} {self.address} {self.state}>"


#: Historical name. ``TcpClient`` and ``TcpConnection`` are the same class.
TcpClient = TcpConnection


TcpHandler = Callable[[TcpConnection, str, "YTcpServer"], Awaitable[Any] | Any]
ConnectionHandler = Callable[[TcpConnection, "YTcpServer"], Awaitable[Any] | Any]


def _encode_payload(payload: Any) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return ServerTools.to_json(payload).encode("utf-8")


class YTcpServer(BaseServer):
    """Async TCP server with plugins, routes, tools and TLS."""

    protocol = "tcp"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        delimiter: str = "\n",
        ssl_context: ssl.SSLContext | None = None,
        rate_limit_per_window: int | None = 600,
        rate_limit_window_seconds: float = 60.0,
        ddosprot: bool = True,
        *,
        tls: TLSConfig | None = None,
        auth: AuthConfig | None = None,
        config: TcpConfig | None = None,
        logger: logging.Logger | None = None,
        **options: Any,
    ) -> None:
        resolved = config or TcpConfig(
            host=host,
            port=port,
            delimiter=delimiter,
            ssl_context=resolve_ssl_context(ssl_context, tls),
            auth=auth or AuthConfig(),
            rate_limit=RateLimitConfig(limit=rate_limit_per_window, window_seconds=rate_limit_window_seconds),
            ddosprot=ddosprot,
        )
        for key, value in options.items():
            if not hasattr(resolved, key):
                raise ConfigError(f"unknown TCP option: {key}")
            setattr(resolved, key, value)

        super().__init__(resolved, logger=logger)
        self.config: TcpConfig = resolved

        self.clients: dict[str, TcpConnection] = {}
        self.routes: dict[str, TcpHandler] = {}
        self._connection_handler: ConnectionHandler | None = None
        self._server: asyncio.AbstractServer | None = None
        self._active: set[asyncio.Task[Any]] = set()
        #: Connections currently executing handler code, as opposed to sitting
        #: idle waiting for their next message. Only these are worth draining
        #: on shutdown.
        self._busy: set[asyncio.Task[Any]] = set()
        #: The port actually bound, remembered so a rebind lands on it again.
        self._listen_port: int | None = None
        self._register_tcp_tools()

    # -- configuration passthroughs --------------------------------------

    @property
    def delimiter(self) -> str:
        return self.config.delimiter

    @delimiter.setter
    def delimiter(self, value: str) -> None:
        self.config.delimiter = value

    def _bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return self.config.port or None
        return int(self._server.sockets[0].getsockname()[1])

    # -- routing ----------------------------------------------------------

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

    def on_connection(self, handler: ConnectionHandler) -> ConnectionHandler:
        """Take over the raw connection instead of using command routing.

        Use this for custom binary protocols::

            @server.on_connection
            async def handle(conn, server):
                header = await conn.readexactly(4)
                ...
        """

        self._connection_handler = handler
        return handler

    # -- lifecycle ---------------------------------------------------------

    async def _start_impl(self) -> None:
        self._server = await self._listen(self.config.port)
        self._listen_port = self._bound_port()
        self._start_listener_guard()

    async def _listen(self, port: int) -> asyncio.AbstractServer:
        return await asyncio.start_server(
            self._handle_client,
            self.config.host,
            port,
            ssl=self.config.ssl_context,
            backlog=self.config.backlog,
            limit=max(self.config.max_line_bytes + 1024, 64 * 1024),
        )

    async def _rebind_listener(self) -> None:
        old = self._server
        self._server = None
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        # Rebind the port we were actually on, so a server started on port 0
        # comes back where its clients expect it.
        self._server = await self._listen(self._listen_port or self.config.port)

    async def _serve_impl(self) -> None:
        if self._server is None:
            return
        async with self._server:
            await self._server.serve_forever()

    async def _stop_impl(self, drain_deadline: float) -> None:
        # 1. Stop accepting. Note that `wait_closed()` also waits for every
        #    handler task, so it must come last or shutdown deadlocks on an
        #    idle connection that is simply waiting for its next message.
        server = self._server
        self._server = None
        close_listener_quietly(server)

        # 2. Let handlers that are mid-work finish, up to the drain deadline.
        #    Connections merely waiting for their next message have nothing to
        #    drain, so they must not hold shutdown open.
        await self._drain_busy(drain_deadline)

        # 3. Close whatever is left.
        connections = list(self.clients.values())
        if connections:
            await asyncio.gather(
                *(connection.close(drain=False, timeout=0.5) for connection in connections),
                return_exceptions=True,
            )
        self.clients.clear()

        for task in list(self._active):
            task.cancel()
        if self._active:
            await asyncio.gather(*self._active, return_exceptions=True)
        self._active.clear()

        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except Exception:
                pass

    async def _drain_busy(self, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not self._busy:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*list(self._busy), return_exceptions=True),
                timeout=remaining,
            )
        except Exception:
            return

    # -- sending -----------------------------------------------------------

    async def send(self, client_or_id: TcpConnection | str, payload: Any) -> None:
        connection = self._resolve(client_or_id)
        await connection.send(payload)

    async def send_bytes(self, client_or_id: TcpConnection | str, data: bytes) -> None:
        connection = self._resolve(client_or_id)
        await connection.send_bytes(data)

    async def broadcast(self, payload: Any, exclude: str | None = None) -> int:
        """Send to every connected peer. Returns how many writes succeeded.

        Writes run concurrently so one slow peer does not hold up the rest;
        a peer that blows its write timeout is disconnected rather than
        allowed to grow an unbounded buffer.
        """

        targets = [
            connection
            for client_id, connection in list(self.clients.items())
            if not (exclude and client_id == exclude) and connection.is_open
        ]
        if not targets:
            return 0

        async def deliver(connection: TcpConnection) -> bool:
            try:
                await connection.send(payload)
                return True
            except Exception as error:
                await self._report_error(error, {"stage": "broadcast", "client_id": connection.id})
                await self._disconnect_stalled(connection, error)
                return False

        results = await asyncio.gather(*(deliver(connection) for connection in targets))
        delivered = sum(1 for ok in results if ok)
        self.metrics.incr("broadcasts")
        self.metrics.incr("messages_sent", delivered)
        return delivered

    async def disconnect(self, client_or_id: TcpConnection | str, *, drain: bool = True) -> None:
        """Close one peer gracefully."""

        connection = self._resolve(client_or_id)
        await connection.close(drain=drain)

    def _resolve(self, client_or_id: TcpConnection | str) -> TcpConnection:
        if isinstance(client_or_id, TcpConnection):
            return client_or_id
        connection = self.clients.get(client_or_id)
        if connection is None:
            raise KeyError(f"unknown client: {client_or_id}")
        return connection

    async def _disconnect_stalled(self, connection: TcpConnection, error: BaseException) -> None:
        if isinstance(error, (TimeoutError, asyncio.TimeoutError, ConnectionError)):
            self.metrics.incr("stalled_disconnects")
            await connection.close(drain=False, timeout=0.2)

    # -- connection handling ------------------------------------------------

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._active.add(task)
        try:
            await self._serve_connection(reader, writer)
        finally:
            if task is not None:
                self._active.discard(task)

    async def _serve_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        address = format_peer_name(writer.get_extra_info("peername"))

        max_connections = self.config.max_connections
        if max_connections is not None and len(self.clients) >= max_connections:
            self.metrics.incr("connections_refused")
            await close_writer_quietly(writer, timeout_seconds=0.5)
            return

        self._tune_transport(writer)

        peer_name = writer.get_extra_info("peername")
        connection = TcpConnection(
            id=uuid.uuid4().hex,
            reader=reader,
            writer=writer,
            address=address,
            peer=tuple(peer_name[:2]) if isinstance(peer_name, tuple) and len(peer_name) >= 2 else None,
            server=self,
        )
        self.clients[connection.id] = connection
        self.metrics.incr("connections_opened")
        self.metrics.gauge("connections_active", len(self.clients))
        await self._notify_plugins("on_tcp_connect", connection, self)

        try:
            if self._connection_handler is not None:
                # User code owns the socket, so there is no idle state we can
                # detect: treat the whole connection as work in progress.
                with self._mark_busy():
                    await maybe_await(self._connection_handler(connection, self))
            else:
                await self._run_message_loop(connection)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as error:
            await self._report_error(error, {"stage": "tcp-read", "client_id": connection.id})
        except Exception as error:
            await self._report_error(error, {"stage": "tcp-read", "client_id": connection.id})
        finally:
            self.clients.pop(connection.id, None)
            self.metrics.incr("connections_closed")
            self.metrics.gauge("connections_active", len(self.clients))
            await self._notify_plugins("on_tcp_disconnect", connection, self)
            await connection.close(drain=False, timeout=0.5)

    def _tune_transport(self, writer: asyncio.StreamWriter) -> None:
        transport = writer.transport
        try:
            transport.set_write_buffer_limits(
                high=self.config.write_buffer_high_bytes,
                low=self.config.write_buffer_low_bytes,
            )
        except (AttributeError, NotImplementedError):
            pass
        if self.config.tcp_nodelay:
            socket_object = writer.get_extra_info("socket")
            if socket_object is not None:
                try:
                    import socket as socket_module

                    socket_object.setsockopt(socket_module.IPPROTO_TCP, socket_module.TCP_NODELAY, 1)
                except OSError:
                    pass

    async def _run_message_loop(self, connection: TcpConnection) -> None:
        delimiter = self.config.delimiter.encode("utf-8") or b"\n"
        idle_timeout = self.config.idle_timeout_seconds

        while not connection.reader.at_eof() and connection.is_open:
            try:
                if idle_timeout is None:
                    raw = await connection.reader.readuntil(delimiter)
                else:
                    raw = await asyncio.wait_for(connection.reader.readuntil(delimiter), timeout=idle_timeout)
            except asyncio.TimeoutError:
                self.metrics.incr("idle_timeouts")
                await self._safe_send(connection, {"error": "idle-timeout"})
                return
            except asyncio.IncompleteReadError as error:
                raw = error.partial
                if not raw:
                    return
            except asyncio.LimitOverrunError:
                # No delimiter within the buffer limit: the peer is either
                # broken or hostile. Do not let it grow our memory.
                self.metrics.incr("oversized_messages")
                await self._safe_send(connection, {"error": "message-too-large"})
                return
            except (ConnectionResetError, BrokenPipeError):
                return

            if not raw:
                return

            connection.bytes_received += len(raw)
            if len(raw) > self.config.max_line_bytes:
                self.metrics.incr("oversized_messages")
                await self._safe_send(connection, {"error": "message-too-large"})
                continue

            message = raw.decode("utf-8", errors="replace").strip()
            if not message:
                continue

            connection.messages_received += 1
            self.metrics.incr("messages_received")

            transformed = await self._apply_message_plugins(connection, message)
            if transformed is None:
                continue

            if not self._allow_request(connection.address):
                await self._safe_send(
                    connection,
                    {"error": "rate-limit exceeded", "message": DDOS_BLOCK_MESSAGE},
                )
                continue

            await self._dispatch(connection, transformed)

    async def _dispatch(self, connection: TcpConnection, message: str) -> None:
        command, payload = ServerTools.command_parts(message)
        if not command:
            return

        handler = self.routes.get(command) or self.routes.get("*")
        if handler is None:
            await self._safe_send(connection, {"error": "unknown-command", "command": command})
            return

        started = time.monotonic()
        try:
            with self._mark_busy():
                result = await maybe_await(handler(connection, payload, self))
        except Exception as error:
            await self._report_error(
                error,
                {"stage": "tcp-handler", "command": command, "client_id": connection.id},
            )
            await self._safe_send(connection, {"error": "handler-failed", "detail": str(error)})
            return
        finally:
            self.metrics.observe("handler_seconds", time.monotonic() - started)

        if result is not None:
            await self._safe_send(connection, result)

    @contextmanager
    def _mark_busy(self) -> Any:
        """Mark the current connection task as doing work, not idling."""

        task = asyncio.current_task()
        if task is None:
            yield
            return
        self._busy.add(task)
        try:
            yield
        finally:
            self._busy.discard(task)

    async def _safe_send(self, connection: TcpConnection, payload: Any) -> None:
        try:
            await connection.send(payload)
            self.metrics.incr("messages_sent")
        except Exception as error:
            await self._disconnect_stalled(connection, error)

    async def _apply_message_plugins(self, connection: TcpConnection, message: str) -> str | None:
        transformed: str | None = message
        for plugin in self.plugins:
            if transformed is None:
                return None
            hook = getattr(plugin, "on_tcp_message", None)
            if hook is None:
                continue
            try:
                transformed = await maybe_await(hook(connection, transformed, self))
            except Exception as error:
                await self._report_error(error, {"stage": "tcp-message-plugin", "plugin": getattr(plugin, "name", "?")})
        return transformed

    # -- tools --------------------------------------------------------------

    def _register_tcp_tools(self) -> None:
        self.register_tool("client_count", lambda: len(self.clients))
        self.register_tool(
            "list_clients",
            lambda: [
                {"id": connection.id, "address": connection.address, "state": connection.state}
                for connection in self.clients.values()
            ],
        )


#: Historical name kept as the primary export.
YServer = YTcpServer
