"""Async UDP server.

UDP is connectionless, so this API talks about **endpoints** (a remote
address you have heard from, or intend to send to) rather than connections.
There is no ``accept``, no ``disconnect``, and no stream.

What UDP does not give you, and what this module will not pretend to give
you either:

* **No delivery guarantee.** A datagram you send may never arrive.
* **No ordering.** Datagrams can arrive in a different order than sent.
* **No deduplication.** A datagram can arrive more than once.
* **No connection state.** "Known peers" here just means "addresses we have
  received something from recently"; nothing tells you they are still there.

If your application needs any of that, opt in explicitly with
:class:`ReliableUdpChannel`, which layers sequence numbers, acknowledgements,
retransmission and de-duplication *on top of* UDP. It is a helper, not a
transport, and it is off by default.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .core import (
    AuthConfig,
    BaseServer,
    ConfigError,
    RateLimitConfig,
    ServerConfig,
    maybe_await,
)
from .tools import ServerTools

__all__ = [
    "ReliableUdpChannel",
    "UdpConfig",
    "UdpDatagram",
    "UdpEndpoint",
    "YUdpServer",
]


# ---------------------------------------------------------------------------
# endpoints and datagrams
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class UdpEndpoint:
    """A remote address. Not a connection: nothing here is guaranteed live."""

    host: str
    port: int
    #: ``socket.AF_INET`` or ``socket.AF_INET6``.
    family: int = socket.AF_INET
    #: IPv6 scope id, when the address carries one.
    scope_id: int = 0

    @property
    def key(self) -> str:
        if self.family == socket.AF_INET6:
            return f"[{self.host}]:{self.port}"
        return f"{self.host}:{self.port}"

    @property
    def is_ipv6(self) -> bool:
        return self.family == socket.AF_INET6

    def as_addr(self) -> tuple[Any, ...]:
        """The tuple asyncio wants for ``sendto``."""

        if self.family == socket.AF_INET6:
            return (self.host, self.port, 0, self.scope_id)
        return (self.host, self.port)

    @classmethod
    def from_addr(cls, addr: tuple[Any, ...]) -> "UdpEndpoint":
        if len(addr) >= 4:
            return cls(host=str(addr[0]), port=int(addr[1]), family=socket.AF_INET6, scope_id=int(addr[3]))
        return cls(host=str(addr[0]), port=int(addr[1]), family=socket.AF_INET)

    def __str__(self) -> str:
        return self.key


@dataclass(slots=True)
class UdpDatagram:
    """One received datagram."""

    data: bytes
    endpoint: UdpEndpoint
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def text(self, errors: str = "replace") -> str:
        return self.data.decode("utf-8", errors=errors)

    def json(self, default: Any = None) -> Any:
        return ServerTools.from_json(self.text(), default=default)

    def __len__(self) -> int:
        return len(self.data)


@dataclass(slots=True)
class _PeerRecord:
    endpoint: UdpEndpoint
    first_seen: float
    last_seen: float
    datagrams_received: int = 0
    bytes_received: int = 0


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@dataclass
class UdpConfig(ServerConfig):
    """UDP-specific options."""

    #: Largest datagram accepted or sent, in bytes. Datagrams larger than this
    #: are dropped on receive and rejected on send. 65507 is the IPv4 payload
    #: ceiling; staying under the path MTU (~1200 bytes is a safe internet
    #: default) avoids IP fragmentation, which multiplies loss.
    max_packet_size: int = 65507
    #: ``"auto"`` picks a family from ``host``; ``"ipv4"``/``"ipv6"`` force one.
    family: str = "auto"
    #: Accept IPv4-mapped traffic on an IPv6 socket where the OS allows it.
    dual_stack: bool = True
    #: Allow sending to broadcast addresses.
    allow_broadcast: bool = False
    #: ``SO_REUSEPORT`` where supported (not on Windows).
    reuse_port: bool = False
    #: Drop a peer from the known-peers table after this many idle seconds.
    peer_idle_seconds: float = 300.0
    #: Hard cap on tracked peers so a spoofed-source flood cannot exhaust memory.
    max_tracked_peers: int = 50_000
    #: Cap on handler tasks in flight. Past this, datagrams are dropped rather
    #: than queued, because queueing UDP under overload only adds latency.
    max_concurrent_handlers: int = 2_000
    #: What to do when the OS send buffer is full: ``"drop"`` (UDP-appropriate,
    #: the default) or ``"wait"`` to apply backpressure to the sender.
    backpressure_policy: str = "drop"
    #: Seconds ``"wait"`` will block before giving up on a send.
    send_wait_seconds: float = 1.0

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.family not in ("auto", "ipv4", "ipv6"):
            raise ConfigError("family must be 'auto', 'ipv4' or 'ipv6'")
        if self.backpressure_policy not in ("drop", "wait"):
            raise ConfigError("backpressure_policy must be 'drop' or 'wait'")
        if not 1 <= int(self.max_packet_size) <= 65507:
            raise ConfigError("max_packet_size must be between 1 and 65507")


DatagramHandler = Callable[[UdpDatagram, "YUdpServer"], Awaitable[Any] | Any]
CommandHandler = Callable[[UdpEndpoint, str, "YUdpServer"], Awaitable[Any] | Any]


# ---------------------------------------------------------------------------
# asyncio protocol
# ---------------------------------------------------------------------------


class _UdpProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: "YUdpServer") -> None:
        self._server = server

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._server._attach_transport(transport)  # type: ignore[arg-type]

    def datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
        self._server._datagram_received(data, addr)

    def error_received(self, exc: Exception) -> None:
        # ICMP port-unreachable and friends. Normal on UDP; never fatal.
        self._server._transport_error(exc)

    def connection_lost(self, exc: Exception | None) -> None:
        self._server._transport_lost(exc)

    def pause_writing(self) -> None:
        self._server._set_paused(True)

    def resume_writing(self) -> None:
        self._server._set_paused(False)


# ---------------------------------------------------------------------------
# server
# ---------------------------------------------------------------------------


class YUdpServer(BaseServer):
    """Async UDP server built around endpoints, not connections.

    Two handler styles, same server:

    * ``@server.on_datagram`` receives every :class:`UdpDatagram` raw.
    * ``@server.route("ping")`` splits the datagram as ``command payload``
      text, matching the TCP server's command style.

    Returning a value from either handler sends a reply to the sender. There
    is, of course, no guarantee the reply arrives.
    """

    protocol = "udp"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9002,
        *,
        rate_limit_per_window: int | None = 2000,
        rate_limit_window_seconds: float = 60.0,
        ddosprot: bool = True,
        auth: AuthConfig | None = None,
        config: UdpConfig | None = None,
        logger: logging.Logger | None = None,
        **options: Any,
    ) -> None:
        resolved = config or UdpConfig(
            host=host,
            port=port,
            auth=auth or AuthConfig(),
            rate_limit=RateLimitConfig(limit=rate_limit_per_window, window_seconds=rate_limit_window_seconds),
            ddosprot=ddosprot,
        )
        for key, value in options.items():
            if not hasattr(resolved, key):
                raise ConfigError(f"unknown UDP option: {key}")
            setattr(resolved, key, value)
        resolved.__post_init__()

        super().__init__(resolved, logger=logger)
        self.config: UdpConfig = resolved

        self.routes: dict[str, CommandHandler] = {}
        self._datagram_handler: DatagramHandler | None = None
        self._transport: asyncio.DatagramTransport | None = None
        self._protocol: _UdpProtocol | None = None
        self._peers: dict[str, _PeerRecord] = {}
        self._inflight: set[asyncio.Task[Any]] = set()
        self._paused = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        self._prune_task: asyncio.Task[Any] | None = None
        self._closed_event = asyncio.Event()
        self._register_udp_tools()

    # -- introspection -----------------------------------------------------

    def _bound_port(self) -> int | None:
        if self._transport is None:
            return self.config.port or None
        sock = self._transport.get_extra_info("socket")
        if sock is None:
            return self.config.port or None
        try:
            return int(sock.getsockname()[1])
        except (OSError, IndexError):
            return self.config.port or None

    @property
    def transport(self) -> asyncio.DatagramTransport | None:
        return self._transport

    def known_endpoints(self) -> list[UdpEndpoint]:
        """Addresses seen recently. Presence here does not mean reachable."""

        return [record.endpoint for record in self._peers.values()]

    def peer_stats(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        return [
            {
                "endpoint": record.endpoint.key,
                "datagrams_received": record.datagrams_received,
                "bytes_received": record.bytes_received,
                "idle_seconds": round(now - record.last_seen, 3),
            }
            for record in self._peers.values()
        ]

    # -- routing -----------------------------------------------------------

    def on_datagram(self, handler: DatagramHandler) -> DatagramHandler:
        """Handle every datagram yourself, before any command parsing."""

        self._datagram_handler = handler
        return handler

    def add_route(self, command: str, handler: CommandHandler) -> None:
        normalized = command.strip().lower()
        if not normalized:
            raise ValueError("command cannot be empty")
        self.routes[normalized] = handler

    def route(self, command: str) -> Callable[[CommandHandler], CommandHandler]:
        def decorator(handler: CommandHandler) -> CommandHandler:
            self.add_route(command, handler)
            return handler

        return decorator

    # -- lifecycle ---------------------------------------------------------

    def _resolve_family(self) -> int:
        if self.config.family == "ipv4":
            return socket.AF_INET
        if self.config.family == "ipv6":
            return socket.AF_INET6
        host = self.config.host
        if ":" in host:
            return socket.AF_INET6
        return socket.AF_INET

    def _make_socket(self) -> socket.socket:
        family = self._resolve_family()
        sock = socket.socket(family, socket.SOCK_DGRAM)
        sock.setblocking(False)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError:
            pass

        if family == socket.AF_INET6:
            # Dual stack lets one socket answer IPv4-mapped clients too.
            try:
                sock.setsockopt(
                    socket.IPPROTO_IPV6,
                    socket.IPV6_V6ONLY,
                    0 if self.config.dual_stack else 1,
                )
            except (OSError, AttributeError):
                pass

        if self.config.reuse_port and hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        if self.config.allow_broadcast:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            except OSError:
                pass

        try:
            sock.bind((self.config.host, self.config.port))
        except OSError as error:
            sock.close()
            raise ConfigError(f"could not bind UDP {self.config.host}:{self.config.port}: {error}") from error
        return sock

    async def _start_impl(self) -> None:
        loop = asyncio.get_running_loop()
        sock = self._make_socket()
        self._closed_event.clear()
        transport, protocol = await loop.create_datagram_endpoint(lambda: _UdpProtocol(self), sock=sock)
        self._transport = transport  # type: ignore[assignment]
        self._protocol = protocol  # type: ignore[assignment]
        self._prune_task = self.create_task(self._prune_peers_loop(), name="udp-prune-peers")

    async def _stop_impl(self, drain_deadline: float) -> None:
        transport = self._transport
        self._transport = None
        self._protocol = None
        if transport is not None:
            # A datagram transport only finishes closing once its pending
            # writes drain. A send queued to an unreachable peer can leave one
            # outstanding indefinitely, so escalate to abort() rather than let
            # shutdown hang on it.
            transport.close()
            if not await self._wait_closed(0.25):
                try:
                    transport.abort()
                except Exception:
                    pass
                await self._wait_closed(0.25)

        remaining = drain_deadline - time.monotonic()
        if self._inflight and remaining > 0:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._inflight), return_exceptions=True),
                    timeout=remaining,
                )
            except Exception:
                pass
        for task in list(self._inflight):
            task.cancel()
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
        self._inflight.clear()
        self._peers.clear()
        self._resume_event.set()

    async def _wait_closed(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._closed_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # -- transport callbacks ------------------------------------------------

    def _attach_transport(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def _transport_error(self, error: Exception) -> None:
        # Typically ICMP unreachable for a previous send. Record it and move on.
        self.metrics.incr("transport_errors")
        self.create_task(
            self._report_error(error, {"stage": "udp-transport"}),
            name="udp-transport-error",
        )

    def _transport_lost(self, error: Exception | None) -> None:
        self._transport = None
        self._closed_event.set()
        if error is not None:
            self.metrics.incr("transport_errors")

    def _set_paused(self, paused: bool) -> None:
        self._paused = paused
        if paused:
            self._resume_event.clear()
            self.metrics.incr("send_pauses")
        else:
            self._resume_event.set()

    # -- receive path -------------------------------------------------------

    def _datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
        self.metrics.incr("datagrams_received")
        self.metrics.incr("bytes_received", len(data))

        if len(data) > self.config.max_packet_size:
            self.metrics.incr("dropped_oversized")
            return

        endpoint = UdpEndpoint.from_addr(addr)

        if not self._allow_request(endpoint.host):
            self.metrics.incr("dropped_rate_limited")
            return

        if len(self._inflight) >= self.config.max_concurrent_handlers:
            # Under overload, dropping is the honest UDP behaviour. Queueing
            # would only deliver stale datagrams late.
            self.metrics.incr("dropped_overload")
            return

        self._touch_peer(endpoint, len(data))
        datagram = UdpDatagram(data=data, endpoint=endpoint)

        task = asyncio.ensure_future(self._process(datagram))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    def _touch_peer(self, endpoint: UdpEndpoint, size: int) -> None:
        now = time.monotonic()
        record = self._peers.get(endpoint.key)
        if record is None:
            if len(self._peers) >= self.config.max_tracked_peers:
                self._evict_oldest_peer()
            record = _PeerRecord(endpoint=endpoint, first_seen=now, last_seen=now)
            self._peers[endpoint.key] = record
            self.create_task(
                self._notify_plugins("on_udp_endpoint_seen", endpoint, self),
                name="udp-endpoint-seen",
            )
        record.last_seen = now
        record.datagrams_received += 1
        record.bytes_received += size
        self.metrics.gauge("known_endpoints", len(self._peers))

    def _evict_oldest_peer(self) -> None:
        if not self._peers:
            return
        oldest = min(self._peers, key=lambda key: self._peers[key].last_seen)
        self._peers.pop(oldest, None)

    async def _prune_peers_loop(self) -> None:
        interval = max(1.0, min(30.0, self.config.peer_idle_seconds / 4))
        while True:
            await asyncio.sleep(interval)
            cutoff = time.monotonic() - self.config.peer_idle_seconds
            stale = [key for key, record in self._peers.items() if record.last_seen < cutoff]
            for key in stale:
                record = self._peers.pop(key, None)
                if record is not None:
                    await self._notify_plugins("on_udp_endpoint_expired", record.endpoint, self)
            if stale:
                self.metrics.gauge("known_endpoints", len(self._peers))

    async def _process(self, datagram: UdpDatagram) -> None:
        started = time.monotonic()
        try:
            transformed = await self._apply_datagram_plugins(datagram)
            if transformed is None:
                self.metrics.incr("dropped_by_plugin")
                return

            if self._datagram_handler is not None:
                result = await maybe_await(self._datagram_handler(transformed, self))
                if result is not None:
                    await self.send_to(transformed.endpoint, result)
                return

            if not self.routes:
                return

            command, payload = ServerTools.command_parts(transformed.text())
            if not command:
                return
            handler = self.routes.get(command) or self.routes.get("*")
            if handler is None:
                await self.send_to(transformed.endpoint, {"error": "unknown-command", "command": command})
                return

            result = await maybe_await(handler(transformed.endpoint, payload, self))
            if result is not None:
                await self.send_to(transformed.endpoint, result)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._report_error(
                error,
                {"stage": "udp-handler", "endpoint": datagram.endpoint.key},
            )
        finally:
            self.metrics.observe("handler_seconds", time.monotonic() - started)

    async def _apply_datagram_plugins(self, datagram: UdpDatagram) -> UdpDatagram | None:
        current: UdpDatagram | None = datagram
        for plugin in self.plugins:
            if current is None:
                return None
            hook = getattr(plugin, "on_udp_datagram", None)
            if hook is None:
                continue
            try:
                current = await maybe_await(hook(current, self))
            except Exception as error:
                await self._report_error(
                    error,
                    {"stage": "udp-datagram-plugin", "plugin": getattr(plugin, "name", "?")},
                )
        return current

    # -- send path ----------------------------------------------------------

    def encode(self, payload: Any) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8")
        return ServerTools.to_json(payload).encode("utf-8")

    async def send_to(self, endpoint: UdpEndpoint | tuple[str, int], payload: Any) -> bool:
        """Send one datagram. Returns whether it was handed to the OS.

        ``True`` means the kernel accepted the bytes, **not** that the peer
        received them. UDP cannot tell you that.
        """

        transport = self._transport
        if transport is None:
            return False

        target = endpoint if isinstance(endpoint, UdpEndpoint) else UdpEndpoint(endpoint[0], int(endpoint[1]))
        data = self.encode(payload)

        if len(data) > self.config.max_packet_size:
            self.metrics.incr("send_rejected_oversized")
            raise ValueError(
                f"datagram of {len(data)} bytes exceeds max_packet_size={self.config.max_packet_size}"
            )

        if self._paused:
            if self.config.backpressure_policy == "drop":
                self.metrics.incr("send_dropped_backpressure")
                return False
            try:
                await asyncio.wait_for(self._resume_event.wait(), timeout=self.config.send_wait_seconds)
            except asyncio.TimeoutError:
                self.metrics.incr("send_dropped_backpressure")
                return False

        try:
            transport.sendto(data, target.as_addr())
        except OSError as error:
            self.metrics.incr("send_errors")
            await self._report_error(error, {"stage": "udp-send", "endpoint": target.key})
            return False

        self.metrics.incr("datagrams_sent")
        self.metrics.incr("bytes_sent", len(data))
        return True

    async def broadcast(self, payload: Any, exclude: str | None = None) -> int:
        """Send to every recently seen endpoint. Returns the send count.

        This is a fan-out to known addresses, not an IP broadcast. For a real
        IP broadcast set ``allow_broadcast=True`` and ``send_to`` the
        broadcast address directly.
        """

        data = self.encode(payload)
        sent = 0
        for record in list(self._peers.values()):
            if exclude and record.endpoint.key == exclude:
                continue
            if await self.send_to(record.endpoint, data):
                sent += 1
        return sent

    # -- tools --------------------------------------------------------------

    def _register_udp_tools(self) -> None:
        self.register_tool("endpoint_count", lambda: len(self._peers))
        self.register_tool("list_endpoints", lambda: [endpoint.key for endpoint in self.known_endpoints()])
        self.register_tool("peer_stats", self.peer_stats)


# ---------------------------------------------------------------------------
# optional application-level reliability
# ---------------------------------------------------------------------------

_RELIABLE_MAGIC = b"YRL"
_RELIABLE_VERSION = 1
_TYPE_DATA = 0x01
_TYPE_ACK = 0x02
#: magic(3) + version(1) + type(1) + seq(4)
_RELIABLE_HEADER = struct.Struct("!3sBBI")
_RELIABLE_HEADER_SIZE = _RELIABLE_HEADER.size


@dataclass(slots=True)
class _Outbound:
    seq: int
    payload: bytes
    endpoint: UdpEndpoint
    attempts: int
    next_retry_at: float


class ReliableUdpChannel:
    """Optional at-least-once delivery over UDP, with ordering on request.

    This is an **application-level** helper. It does not change UDP; it adds a
    9-byte header carrying a sequence number, acknowledges what it receives,
    retransmits what goes unacknowledged, and drops duplicates.

    What you get:

    * at-least-once delivery within ``max_retries`` (then a reported failure)
    * duplicate suppression per endpoint
    * optional in-order delivery, buffering up to ``reorder_window`` datagrams

    What you still do not get: congestion control, flow control, or any of the
    other reasons TCP exists. If you find yourself wanting those, use TCP.

    Usage::

        channel = ReliableUdpChannel(server)
        server.on_datagram(channel.handle_datagram)

        @channel.on_message
        async def handle(payload, endpoint):
            ...

        await channel.send(endpoint, b"important")
    """

    def __init__(
        self,
        server: YUdpServer,
        *,
        retry_interval_seconds: float = 0.25,
        max_retries: int = 5,
        ordered: bool = False,
        reorder_window: int = 64,
        reorder_timeout_seconds: float = 1.0,
        dedupe_window: int = 1024,
        backoff_multiplier: float = 1.5,
    ) -> None:
        if retry_interval_seconds <= 0:
            raise ConfigError("retry_interval_seconds must be > 0")
        if max_retries < 0:
            raise ConfigError("max_retries must be >= 0")

        self.server = server
        self.retry_interval_seconds = float(retry_interval_seconds)
        self.max_retries = int(max_retries)
        self.ordered = bool(ordered)
        self.reorder_window = max(1, int(reorder_window))
        self.reorder_timeout_seconds = max(0.0, float(reorder_timeout_seconds))
        self.dedupe_window = max(1, int(dedupe_window))
        self.backoff_multiplier = max(1.0, float(backoff_multiplier))

        self._next_seq: dict[str, int] = {}
        self._pending: dict[tuple[str, int], _Outbound] = {}
        self._seen: dict[str, set[int]] = {}
        self._seen_order: dict[str, list[int]] = {}
        self._expected: dict[str, int] = {}
        self._buffered: dict[str, dict[int, bytes]] = {}
        self._gap_since: dict[str, float] = {}

        self._message_handler: Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any] | None = None
        self._failure_handler: Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any] | None = None
        self._retry_task: asyncio.Task[Any] | None = None

    # -- handlers -----------------------------------------------------------

    def on_message(
        self, handler: Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any]
    ) -> Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any]:
        """Called once per unique payload, in order when ``ordered=True``."""

        self._message_handler = handler
        return handler

    def on_delivery_failed(
        self, handler: Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any]
    ) -> Callable[[bytes, UdpEndpoint], Awaitable[Any] | Any]:
        """Called when a payload went unacknowledged past ``max_retries``."""

        self._failure_handler = handler
        return handler

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Begin the retransmission loop. Safe to call more than once."""

        if self._retry_task is None or self._retry_task.done():
            self._retry_task = self.server.create_task(self._retry_loop(), name="udp-reliable-retry")

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    # -- send ---------------------------------------------------------------

    async def send(self, endpoint: UdpEndpoint, payload: bytes | str | Any) -> int:
        """Send reliably and return the sequence number used."""

        self.start()
        data = self.server.encode(payload)
        max_payload = self.server.config.max_packet_size - _RELIABLE_HEADER_SIZE
        if len(data) > max_payload:
            raise ValueError(
                f"payload of {len(data)} bytes exceeds the reliable limit of {max_payload} "
                f"(max_packet_size minus a {_RELIABLE_HEADER_SIZE}-byte header)"
            )

        seq = self._next_seq.get(endpoint.key, 1)
        self._next_seq[endpoint.key] = (seq + 1) & 0xFFFFFFFF or 1

        frame = _RELIABLE_HEADER.pack(_RELIABLE_MAGIC, _RELIABLE_VERSION, _TYPE_DATA, seq) + data
        self._pending[(endpoint.key, seq)] = _Outbound(
            seq=seq,
            payload=frame,
            endpoint=endpoint,
            attempts=1,
            next_retry_at=time.monotonic() + self.retry_interval_seconds,
        )
        await self.server.send_to(endpoint, frame)
        self.server.metrics.incr("reliable_sent")
        return seq

    # -- receive ------------------------------------------------------------

    async def handle_datagram(self, datagram: UdpDatagram, _server: YUdpServer | None = None) -> None:
        """Feed this to ``server.on_datagram``."""

        parsed = self._parse(datagram.data)
        if parsed is None:
            self.server.metrics.incr("reliable_malformed")
            return
        kind, seq, payload = parsed

        if kind == _TYPE_ACK:
            if self._pending.pop((datagram.endpoint.key, seq), None) is not None:
                self.server.metrics.incr("reliable_acked")
            return

        # DATA: acknowledge first so the sender can stop retransmitting even
        # if our handler is slow or throws.
        ack = _RELIABLE_HEADER.pack(_RELIABLE_MAGIC, _RELIABLE_VERSION, _TYPE_ACK, seq)
        await self.server.send_to(datagram.endpoint, ack)

        if self._is_duplicate(datagram.endpoint.key, seq):
            self.server.metrics.incr("reliable_duplicates")
            return
        self._remember(datagram.endpoint.key, seq)

        if not self.ordered:
            await self._deliver(payload, datagram.endpoint)
            return

        await self._deliver_in_order(datagram.endpoint, seq, payload)

    async def _deliver_in_order(self, endpoint: UdpEndpoint, seq: int, payload: bytes) -> None:
        key = endpoint.key
        now = time.monotonic()
        # Senders always start at sequence 1, so a new stream is expected to
        # begin there. Anchoring to the first sequence that happens to arrive
        # would deliver an out-of-order opener immediately, which is exactly
        # what ordered mode exists to prevent.
        expected = self._expected.setdefault(key, 1)
        buffered = self._buffered.setdefault(key, {})

        if seq < expected:
            return  # already delivered, or skipped past
        buffered[seq] = payload

        if expected not in buffered:
            # Head-of-line gap. Wait for the missing datagram, but not
            # forever: give up once too much has piled up behind it or it has
            # been outstanding too long, and resume from the oldest datagram
            # still held. Stalling a live stream on a lost packet would be
            # worse than skipping it.
            gap_started = self._gap_since.setdefault(key, now)
            too_many = len(buffered) > self.reorder_window
            too_long = self.reorder_timeout_seconds > 0 and (now - gap_started) > self.reorder_timeout_seconds
            if too_many or too_long:
                self.server.metrics.incr("reliable_reorder_gaps")
                expected = min(buffered)
                self._expected[key] = expected
                self._gap_since.pop(key, None)

        while expected in buffered:
            await self._deliver(buffered.pop(expected), endpoint)
            expected += 1
        self._expected[key] = expected

        if buffered:
            self._gap_since.setdefault(key, now)
        else:
            self._gap_since.pop(key, None)

    async def _deliver(self, payload: bytes, endpoint: UdpEndpoint) -> None:
        self.server.metrics.incr("reliable_delivered")
        if self._message_handler is None:
            return
        try:
            await maybe_await(self._message_handler(payload, endpoint))
        except Exception as error:
            await self.server._report_error(error, {"stage": "reliable-udp-handler", "endpoint": endpoint.key})

    # -- retransmission -----------------------------------------------------

    async def _retry_loop(self) -> None:
        while True:
            await asyncio.sleep(self.retry_interval_seconds / 2)
            if not self._pending:
                continue
            now = time.monotonic()
            for key, outbound in list(self._pending.items()):
                if outbound.next_retry_at > now:
                    continue
                if outbound.attempts > self.max_retries:
                    self._pending.pop(key, None)
                    self.server.metrics.incr("reliable_delivery_failed")
                    if self._failure_handler is not None:
                        try:
                            await maybe_await(
                                self._failure_handler(outbound.payload[_RELIABLE_HEADER_SIZE:], outbound.endpoint)
                            )
                        except Exception:
                            pass
                    continue
                outbound.attempts += 1
                delay = self.retry_interval_seconds * (self.backoff_multiplier ** (outbound.attempts - 1))
                outbound.next_retry_at = now + delay
                await self.server.send_to(outbound.endpoint, outbound.payload)
                self.server.metrics.incr("reliable_retransmits")

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _parse(data: bytes) -> tuple[int, int, bytes] | None:
        if len(data) < _RELIABLE_HEADER_SIZE:
            return None
        magic, version, kind, seq = _RELIABLE_HEADER.unpack_from(data)
        if magic != _RELIABLE_MAGIC or version != _RELIABLE_VERSION:
            return None
        if kind not in (_TYPE_DATA, _TYPE_ACK):
            return None
        return kind, seq, data[_RELIABLE_HEADER_SIZE:]

    def _is_duplicate(self, key: str, seq: int) -> bool:
        return seq in self._seen.get(key, ())

    def _remember(self, key: str, seq: int) -> None:
        seen = self._seen.setdefault(key, set())
        order = self._seen_order.setdefault(key, [])
        seen.add(seq)
        order.append(seq)
        while len(order) > self.dedupe_window:
            seen.discard(order.pop(0))
