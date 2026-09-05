"""Shared foundations for every yashserver transport.

This module holds the concepts that are genuinely common across TCP, UDP,
HTTP and WebSocket servers:

* configuration (:class:`ServerConfig` and friends)
* TLS setup with secure defaults (:class:`TLSConfig`)
* authentication (:class:`AuthConfig`)
* rate limiting (:class:`RateLimitConfig`, :class:`SlidingWindowRateLimiter`)
* metrics (:class:`Metrics`)
* server lifecycle, plugins, tools and graceful shutdown (:class:`BaseServer`)

Everything protocol specific lives in the transport modules. ``BaseServer``
deliberately knows nothing about connections, datagrams, requests or frames.
"""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import ssl
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable, Sequence

__all__ = [
    "AuthConfig",
    "BaseServer",
    "ConfigError",
    "Metrics",
    "RateLimitConfig",
    "ServerConfig",
    "ServerState",
    "SlidingWindowRateLimiter",
    "TLSConfig",
    "YServerError",
    "close_listener_quietly",
    "close_writer_quietly",
    "format_peer_name",
    "maybe_await",
    "resolve_ssl_context",
]


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class YServerError(Exception):
    """Base class for every error raised by yashserver itself."""


class ConfigError(YServerError, ValueError):
    """Raised when a server is configured with impossible options."""


# ---------------------------------------------------------------------------
# lifecycle state
# ---------------------------------------------------------------------------


class ServerState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


async def maybe_await(value: Any) -> Any:
    """Await ``value`` when it is awaitable, otherwise return it unchanged."""

    if inspect.isawaitable(value):
        return await value
    return value


def format_peer_name(peer_name: Any) -> str:
    """Render an asyncio peer name as a stable rate-limit / logging key."""

    if isinstance(peer_name, tuple) and peer_name:
        return str(peer_name[0])
    return str(peer_name) if peer_name else "unknown"


def close_listener_quietly(server: Any) -> None:
    """Close an ``asyncio`` server, tolerating an already-dead listening socket.

    The listening socket can be gone before shutdown runs — that is exactly the
    failure the listener guard watches for — and asking asyncio to close it
    again raises. Shutdown must not depend on the listener still being healthy.
    """

    if server is None:
        return
    try:
        server.close()
    except OSError:
        pass


async def close_writer_quietly(writer: Any, *, timeout_seconds: float = 1.0) -> None:
    """Close a stream writer, aborting the transport if the peer stalls.

    A half-open or wedged TLS peer can make ``wait_closed()`` hang forever,
    which would block graceful shutdown. Abort after ``timeout_seconds``.
    """

    try:
        writer.close()
    except Exception:
        return
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


def listener_is_dead(server: Any) -> bool:
    """Has an ``asyncio.AbstractServer`` lost its listening socket?

    This is not a hypothetical. CPython's Windows proactor accept loop treats
    a failed ``accept()`` as fatal to the *listener*: it closes the listening
    socket and stops accepting. A single client that resets its connection
    during the handshake is enough to trigger it, so a burst of connect-then-
    reset takes a server offline while the ``Server`` object still looks
    perfectly healthy.

    Detecting it is cheap: a closed socket reports ``fileno() == -1``.
    """

    if server is None:
        return False
    sockets = getattr(server, "sockets", None)
    if not sockets:
        return True
    try:
        return all(sock.fileno() == -1 for sock in sockets)
    except Exception:
        return True


def is_numeric_limit(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _constant_time_match(supplied: str, candidates: Sequence[str]) -> bool:
    """Compare ``supplied`` against every candidate without an early exit."""

    matched = False
    for candidate in candidates:
        if hmac.compare_digest(supplied, candidate):
            matched = True
    return matched


def extract_bearer_token(raw_authorization: str | None) -> str | None:
    if not raw_authorization:
        return None
    value = raw_authorization.strip()
    if not value.lower().startswith("bearer "):
        return None
    token = value[7:].strip()
    return token or None


# ---------------------------------------------------------------------------
# TLS
# ---------------------------------------------------------------------------


_TLS_VERSIONS: dict[str, ssl.TLSVersion] = {
    "1.0": ssl.TLSVersion.TLSv1,
    "1.1": ssl.TLSVersion.TLSv1_1,
    "1.2": ssl.TLSVersion.TLSv1_2,
    "1.3": ssl.TLSVersion.TLSv1_3,
    "tlsv1": ssl.TLSVersion.TLSv1,
    "tlsv1.1": ssl.TLSVersion.TLSv1_1,
    "tlsv1.2": ssl.TLSVersion.TLSv1_2,
    "tlsv1.3": ssl.TLSVersion.TLSv1_3,
}


def _resolve_tls_version(value: "str | ssl.TLSVersion | None") -> ssl.TLSVersion | None:
    if value is None:
        return None
    if isinstance(value, ssl.TLSVersion):
        return value
    key = str(value).strip().lower()
    if key not in _TLS_VERSIONS:
        raise ConfigError(f"unsupported TLS version: {value!r} (use one of 1.0, 1.1, 1.2, 1.3)")
    return _TLS_VERSIONS[key]


@dataclass
class TLSConfig:
    """Declarative TLS configuration with secure defaults.

    TLS 1.2 is the floor, compression is disabled, and ephemeral keys are not
    reused across handshakes. Client certificates are optional and off by
    default; turning them on requires a CA bundle so the server cannot
    silently accept certificates it has no way to verify.
    """

    certfile: str
    keyfile: str | None = None
    password: str | None = None
    cafile: str | None = None
    capath: str | None = None
    require_client_cert: bool = False
    minimum_version: "str | ssl.TLSVersion" = "1.2"
    maximum_version: "str | ssl.TLSVersion | None" = None
    ciphers: str | None = None
    alpn_protocols: Sequence[str] | None = None

    def create_server_context(self) -> ssl.SSLContext:
        if self.require_client_cert and not (self.cafile or self.capath):
            raise ConfigError("require_client_cert=True needs cafile or capath to verify clients")

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        try:
            context.load_cert_chain(
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=self.password,
            )
        except (OSError, ssl.SSLError) as error:
            raise ConfigError(f"could not load TLS certificate/key: {error}") from error

        minimum = _resolve_tls_version(self.minimum_version)
        if minimum is not None:
            context.minimum_version = minimum
        maximum = _resolve_tls_version(self.maximum_version)
        if maximum is not None:
            context.maximum_version = maximum

        context.options |= ssl.OP_NO_COMPRESSION
        context.options |= ssl.OP_SINGLE_DH_USE
        context.options |= ssl.OP_SINGLE_ECDH_USE

        if self.ciphers:
            try:
                context.set_ciphers(self.ciphers)
            except ssl.SSLError as error:
                raise ConfigError(f"invalid cipher string: {error}") from error

        if self.cafile or self.capath:
            try:
                context.load_verify_locations(cafile=self.cafile, capath=self.capath)
            except (OSError, ssl.SSLError) as error:
                raise ConfigError(f"could not load CA bundle: {error}") from error

        context.verify_mode = ssl.CERT_REQUIRED if self.require_client_cert else ssl.CERT_NONE

        if self.alpn_protocols:
            context.set_alpn_protocols(list(self.alpn_protocols))

        return context


def resolve_ssl_context(
    ssl_context: ssl.SSLContext | None,
    tls: TLSConfig | None,
) -> ssl.SSLContext | None:
    """Accept either a ready-made context or a :class:`TLSConfig`."""

    if ssl_context is not None and tls is not None:
        raise ConfigError("pass either ssl_context or tls, not both")
    if tls is not None:
        return tls.create_server_context()
    return ssl_context


# ---------------------------------------------------------------------------
# rate limiting
# ---------------------------------------------------------------------------


@dataclass
class RateLimitConfig:
    """Sliding-window rate limit.

    A ``limit`` of ``None`` or ``<= 0`` disables limiting. ``max_tracked_keys``
    bounds memory: the limiter never grows past that many distinct keys.
    """

    limit: int | None = None
    window_seconds: float = 60.0
    max_tracked_keys: int = 100_000

    def __post_init__(self) -> None:
        self.window_seconds = max(0.001, float(self.window_seconds))
        self.max_tracked_keys = max(1, int(self.max_tracked_keys))

    @property
    def enabled(self) -> bool:
        return self.limit is not None and self.limit > 0


class SlidingWindowRateLimiter:
    """Per-key sliding window counter with bounded memory.

    The naive version keeps one deque per key forever, which turns any
    internet-facing server into a slow memory leak. This one sweeps expired
    keys as it goes and evicts the least recently seen key once the tracking
    cap is reached.
    """

    __slots__ = ("_config", "_events", "_last_seen", "_calls_since_sweep")

    #: How many ``allow()`` calls pass between stale-key sweeps.
    SWEEP_INTERVAL = 512

    def __init__(self, config: RateLimitConfig | None = None) -> None:
        self._config = config or RateLimitConfig()
        self._events: dict[str, deque[float]] = {}
        self._last_seen: dict[str, float] = {}
        self._calls_since_sweep = 0

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    @property
    def limit(self) -> int | None:
        return self._config.limit

    @property
    def window_seconds(self) -> float:
        return self._config.window_seconds

    def reconfigure(self, config: RateLimitConfig) -> None:
        self._config = config
        self._events.clear()
        self._last_seen.clear()

    def allow(self, key: str) -> bool:
        config = self._config
        if not config.enabled:
            return True

        now = time.monotonic()
        self._maybe_sweep(now)

        bucket = self._events.get(key)
        if bucket is None:
            if len(self._events) >= config.max_tracked_keys:
                self._evict_oldest()
            bucket = deque()
            self._events[key] = bucket

        self._last_seen[key] = now
        threshold = now - config.window_seconds
        while bucket and bucket[0] < threshold:
            bucket.popleft()

        limit = config.limit or 0
        if len(bucket) >= limit:
            return False

        bucket.append(now)
        return True

    def retry_after_seconds(self, key: str) -> int:
        if not self._config.enabled:
            return 0
        bucket = self._events.get(key)
        if not bucket:
            return 0
        remaining = (bucket[0] + self._config.window_seconds) - time.monotonic()
        if remaining <= 0:
            return 0
        return int(remaining) + 1

    def remaining(self, key: str) -> int | None:
        """How many more events ``key`` may send inside the current window."""

        limit = self._config.limit
        if not self._config.enabled or limit is None:
            return None
        bucket = self._events.get(key)
        if not bucket:
            return limit
        threshold = time.monotonic() - self._config.window_seconds
        active = sum(1 for stamp in bucket if stamp >= threshold)
        return max(0, limit - active)

    def forget(self, key: str) -> None:
        self._events.pop(key, None)
        self._last_seen.pop(key, None)

    def tracked_keys(self) -> int:
        return len(self._events)

    def _maybe_sweep(self, now: float) -> None:
        self._calls_since_sweep += 1
        if self._calls_since_sweep < self.SWEEP_INTERVAL:
            return
        self._calls_since_sweep = 0
        cutoff = now - self._config.window_seconds
        stale = [key for key, seen in self._last_seen.items() if seen < cutoff]
        for key in stale:
            self.forget(key)

    def _evict_oldest(self) -> None:
        if not self._last_seen:
            self._events.clear()
            return
        oldest = min(self._last_seen, key=self._last_seen.__getitem__)
        self.forget(oldest)


# ---------------------------------------------------------------------------
# authentication
# ---------------------------------------------------------------------------


@dataclass
class AuthConfig:
    """Shared token authentication.

    Tokens may arrive as a query parameter, a custom header, or an
    ``Authorization: Bearer`` header. ``validator`` replaces token checking
    entirely when an application needs something richer (JWT, session lookup,
    a database call); it receives a context dict and may be async.
    """

    token: str | None = None
    tokens: Sequence[str] = ()
    header_name: str = "x-yserver-token"
    query_param: str = "token"
    allow_bearer: bool = True
    exempt_paths: set[str] = field(default_factory=set)
    validator: Callable[[dict[str, Any]], Awaitable[bool] | bool] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.token or self.tokens or self.validator)

    def all_tokens(self) -> tuple[str, ...]:
        values: list[str] = []
        if self.token:
            values.append(self.token)
        values.extend(value for value in self.tokens if value)
        return tuple(values)

    def supplied_token(
        self,
        headers: dict[str, str] | None = None,
        query_params: dict[str, list[str]] | None = None,
    ) -> str | None:
        headers = headers or {}
        query_params = query_params or {}

        values = query_params.get(self.query_param) or []
        query_token = values[0] if values else ""
        header_token = headers.get(self.header_name.lower(), "")
        bearer_token = extract_bearer_token(headers.get("authorization")) if self.allow_bearer else None
        return query_token or header_token or bearer_token or None

    async def authorize(
        self,
        *,
        headers: dict[str, str] | None = None,
        query_params: dict[str, list[str]] | None = None,
        path: str | None = None,
        remote_addr: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        if path is not None and path in self.exempt_paths:
            return True
        if not self.enabled:
            return True

        supplied = self.supplied_token(headers, query_params)

        if self.validator is not None:
            context: dict[str, Any] = {
                "token": supplied,
                "headers": headers or {},
                "query_params": query_params or {},
                "path": path,
                "remote_addr": remote_addr,
            }
            if extra:
                context.update(extra)
            return bool(await maybe_await(self.validator(context)))

        if not supplied:
            return False
        return _constant_time_match(supplied, self.all_tokens())


# ---------------------------------------------------------------------------
# metrics
# ---------------------------------------------------------------------------


class Metrics:
    """Tiny in-process metrics sink: counters, gauges and summaries.

    Deliberately dependency free and cheap enough to call on every message.
    Export it however you like (an HTTP route, a plugin, a log line).
    """

    __slots__ = ("_counters", "_gauges", "_summaries")

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._summaries: dict[str, dict[str, float]] = {}

    def incr(self, name: str, value: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + value

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = float(value)

    def observe(self, name: str, value: float) -> None:
        numeric = float(value)
        summary = self._summaries.get(name)
        if summary is None:
            self._summaries[name] = {"count": 1.0, "sum": numeric, "min": numeric, "max": numeric}
            return
        summary["count"] += 1.0
        summary["sum"] += numeric
        summary["min"] = min(summary["min"], numeric)
        summary["max"] = max(summary["max"], numeric)

    def counter(self, name: str) -> int:
        return self._counters.get(name, 0)

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._summaries.clear()

    def snapshot(self) -> dict[str, Any]:
        summaries: dict[str, dict[str, Any]] = {}
        for name, summary in self._summaries.items():
            count = summary["count"] or 1.0
            summaries[name] = {
                "count": int(summary["count"]),
                "sum": summary["sum"],
                "min": summary["min"],
                "max": summary["max"],
                "avg": summary["sum"] / count,
            }
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "summaries": summaries,
        }


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@dataclass
class ServerConfig:
    """Options every yashserver transport understands."""

    host: str = "127.0.0.1"
    port: int = 0
    ssl_context: ssl.SSLContext | None = None
    auth: AuthConfig = field(default_factory=AuthConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    ddosprot: bool = True
    #: How long graceful shutdown waits for in-flight work before forcing.
    shutdown_drain_seconds: float = 5.0
    #: Watch the listening socket and rebind it if it dies underneath us.
    #: See :func:`listener_is_dead` for why this is not paranoia.
    auto_restart_listener: bool = True
    #: How often the listener is checked, in seconds.
    listener_check_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.port is not None and not 0 <= int(self.port) <= 65535:
            raise ConfigError(f"port out of range: {self.port}")
        self.shutdown_drain_seconds = max(0.0, float(self.shutdown_drain_seconds))


# ---------------------------------------------------------------------------
# base server
# ---------------------------------------------------------------------------


class BaseServer:
    """Lifecycle, plugins, tools, metrics and graceful shutdown.

    Subclasses implement :meth:`_start_impl` and :meth:`_stop_impl` and are
    otherwise free to expose whatever API suits their protocol. Nothing here
    assumes a connection-oriented transport, which is what lets UDP share this
    base without pretending to have connections.
    """

    #: Short protocol name used for logger names and metric prefixes.
    protocol: str = "base"

    def __init__(self, config: ServerConfig, *, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(f"yashserver.{self.protocol}")
        self.plugins: list[Any] = []
        self.tools: dict[str, Callable[..., Any]] = {}
        self.metrics = Metrics()
        self.started_at: datetime | None = None
        self.state = ServerState.STOPPED

        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._rate_limiter = SlidingWindowRateLimiter(config.rate_limit)
        self._register_base_tools()

    # -- configuration passthroughs -------------------------------------

    @property
    def host(self) -> str:
        return self.config.host

    @host.setter
    def host(self, value: str) -> None:
        self.config.host = value

    @property
    def port(self) -> int:
        return self.config.port

    @port.setter
    def port(self, value: int) -> None:
        self.config.port = int(value)

    @property
    def ssl_context(self) -> ssl.SSLContext | None:
        return self.config.ssl_context

    @ssl_context.setter
    def ssl_context(self, value: ssl.SSLContext | None) -> None:
        self.config.ssl_context = value

    @property
    def ddosprot(self) -> bool:
        return self.config.ddosprot

    @ddosprot.setter
    def ddosprot(self, value: bool) -> None:
        self.config.ddosprot = bool(value)

    @property
    def auth(self) -> AuthConfig:
        return self.config.auth

    @property
    def auth_token(self) -> str | None:
        return self.config.auth.token

    @auth_token.setter
    def auth_token(self, value: str | None) -> None:
        self.config.auth.token = value

    @property
    def is_running(self) -> bool:
        return self.state is ServerState.RUNNING

    @property
    def bound_port(self) -> int | None:
        """The port actually bound; differs from ``port`` when 0 was requested."""

        return self._bound_port()

    def _bound_port(self) -> int | None:  # pragma: no cover - overridden
        return self.config.port or None

    # -- plugins ---------------------------------------------------------

    def add_plugin(self, plugin: Any) -> "BaseServer":
        self.plugins.append(plugin)
        return self

    async def _notify_plugins(self, hook_name: str, *args: Any) -> None:
        for plugin in self.plugins:
            hook = getattr(plugin, hook_name, None)
            if hook is None:
                continue
            try:
                await maybe_await(hook(*args))
            except Exception:  # plugin errors must never take the server down
                if hook_name == "on_error":
                    continue
                try:
                    await maybe_await(
                        plugin.on_error(
                            RuntimeError(f"plugin hook failed: {getattr(plugin, 'name', plugin)}.{hook_name}"),
                            {"stage": "plugin-hook", "hook": hook_name},
                            self,
                        )
                    )
                except Exception:
                    continue

    async def _report_error(self, error: BaseException, context: dict[str, Any]) -> None:
        self.metrics.incr("errors")
        await self._notify_plugins("on_error", error, context, self)

    # -- tools -----------------------------------------------------------

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

    def uptime_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    def _register_base_tools(self) -> None:
        from .tools import ServerTools

        self.register_tool("now", ServerTools.utc_now)
        self.register_tool("uptime_seconds", self.uptime_seconds)
        self.register_tool("metrics", self.metrics.snapshot)
        self.register_tool("state", lambda: self.state.value)

    # -- rate limiting ----------------------------------------------------

    def setddosprot(
        self,
        ddosprot: bool | int | float,
        rate_limit_window_seconds: float | None = None,
    ) -> "BaseServer":
        """Enable/disable rate limiting, or set a numeric limit directly.

        ``setddosprot(True/False)`` toggles. ``setddosprot(20)`` means twenty
        events per second; pass ``rate_limit_window_seconds`` for a different
        window.
        """

        if is_numeric_limit(ddosprot):
            limit = int(ddosprot)
            if limit <= 0:
                self.config.ddosprot = False
                return self
            window = 1.0 if rate_limit_window_seconds is None else float(rate_limit_window_seconds)
            self.config.rate_limit = RateLimitConfig(
                limit=limit,
                window_seconds=window,
                max_tracked_keys=self.config.rate_limit.max_tracked_keys,
            )
            self._rate_limiter.reconfigure(self.config.rate_limit)
            self.config.ddosprot = True
            return self

        self.config.ddosprot = bool(ddosprot)
        return self

    def _allow_request(self, key: str) -> bool:
        if not self.config.ddosprot:
            return True
        allowed = self._rate_limiter.allow(key)
        if not allowed:
            self.metrics.incr("rate_limited")
        return allowed

    # -- background tasks -------------------------------------------------

    def create_task(self, coro: Awaitable[Any], *, name: str | None = None) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coro)
        if name is not None:
            try:
                task.set_name(name)
            except AttributeError:  # pragma: no cover - non-Task futures
                pass
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def every(self, seconds: float, callback: Callable[[], Awaitable[Any] | Any]) -> asyncio.Task[Any]:
        """Run ``callback`` every ``seconds`` until the server stops."""

        async def runner() -> None:
            while True:
                await asyncio.sleep(seconds)
                try:
                    await maybe_await(callback())
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await self._report_error(error, {"stage": "every", "interval": seconds})

        callback_name = getattr(callback, "__name__", "callback")
        return self.create_task(runner(), name=f"every-{callback_name}")

    # -- listener supervision ---------------------------------------------

    def _listener(self) -> Any:
        """The underlying ``asyncio.AbstractServer``, if this transport has one."""

        return getattr(self, "_server", None)

    async def _rebind_listener(self) -> None:  # pragma: no cover - overridden
        """Recreate a listener that died. Stream transports override this."""

        raise NotImplementedError

    def _start_listener_guard(self) -> None:
        """Watch for a listener that dies underneath us and bring it back.

        Without this, one hostile (or merely unlucky) client can silently end
        a server's ability to accept connections. See :func:`listener_is_dead`.
        """

        if not self.config.auto_restart_listener:
            return
        self.create_task(self._listener_guard_loop(), name=f"{self.protocol}-listener-guard")

    async def _listener_guard_loop(self) -> None:
        interval = max(0.1, float(self.config.listener_check_seconds))
        while True:
            await asyncio.sleep(interval)
            if self.state is not ServerState.RUNNING:
                continue
            if not listener_is_dead(self._listener()):
                continue

            # Report before attempting the rebind, so a listener death is
            # visible even if bringing it back then fails.
            await self._report_error(
                RuntimeError("listening socket died; rebinding"),
                {"stage": "listener-guard", "protocol": self.protocol},
            )
            try:
                await self._rebind_listener()
            except Exception as error:
                self.metrics.incr("listener_restart_failures")
                await self._report_error(error, {"stage": "listener-rebind", "protocol": self.protocol})
            else:
                # Counted only once the listener is genuinely back, so the
                # metric means "accepting again" rather than "about to try".
                # Anything waiting on it can then trust the port is bound.
                self.metrics.incr("listener_restarts")
                self.logger.warning("%s listener died and was rebound", type(self).__name__)

    async def _cancel_background_tasks(self) -> None:
        tasks = [task for task in self._background_tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    # -- lifecycle --------------------------------------------------------

    async def _start_impl(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    async def _stop_impl(self, drain_deadline: float) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    async def _serve_impl(self) -> None:
        """Stay up until cancelled. Subclasses may override with serve_forever."""

        forever: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await forever

    async def start(self) -> None:
        if self.state in (ServerState.RUNNING, ServerState.STARTING):
            return
        self.state = ServerState.STARTING
        try:
            self.started_at = datetime.now(timezone.utc)
            await self._start_impl()
        except BaseException:
            self.state = ServerState.STOPPED
            self.started_at = None
            raise
        self.state = ServerState.RUNNING
        self.metrics.incr("starts")
        await self._notify_plugins("on_startup", self)

    async def run(self) -> None:
        """Start and serve until cancelled, then shut down gracefully."""

        await self.start()
        try:
            await self._serve_impl()
        finally:
            await self.stop()

    async def stop(self, *, timeout: float | None = None) -> None:
        """Stop accepting new work and drain what is in flight."""

        if self.state in (ServerState.STOPPED, ServerState.STOPPING):
            return
        self.state = ServerState.STOPPING
        drain = self.config.shutdown_drain_seconds if timeout is None else max(0.0, float(timeout))
        deadline = time.monotonic() + drain
        try:
            await self._stop_impl(deadline)
            await self._cancel_background_tasks()
            await self._notify_plugins("on_shutdown", self)
        finally:
            self.state = ServerState.STOPPED
            self.metrics.incr("stops")

    async def __aenter__(self) -> "BaseServer":
        await self.start()
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.stop()

    def __repr__(self) -> str:
        where = f"{self.host}:{self.bound_port or self.port}"
        return f"<{type(self).__name__} {self.protocol} {where} {self.state.value}>"


async def gather_with_deadline(awaitables: Iterable[Any], deadline: float) -> None:
    """Await everything, but never past ``deadline`` (a monotonic timestamp)."""

    pending = list(awaitables)
    if not pending:
        return
    remaining = max(0.01, deadline - time.monotonic())
    try:
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=remaining)
    except Exception:
        return
