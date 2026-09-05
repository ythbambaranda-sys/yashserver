"""Backwards-compatible facade over the transport modules.

Before 1.0 every server lived in this one file. The implementations now live
in :mod:`yashserver.tcp`, :mod:`yashserver.udp`, :mod:`yashserver.http` and
:mod:`yashserver.websocket`, sharing :mod:`yashserver.core`. Everything that was
importable from ``yashserver.server`` still is, including the private helpers
that existing code and tests reach for.

New code should import from :mod:`yashserver` directly.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .core import (
    AuthConfig,
    BaseServer,
    Metrics,
    RateLimitConfig,
    ServerConfig,
    ServerState,
    TLSConfig,
    close_writer_quietly,
    extract_bearer_token,
    format_peer_name,
    is_numeric_limit,
)
from .core import SlidingWindowRateLimiter as _BaseSlidingWindowRateLimiter
from .http import (
    DDOS_BLOCK_MESSAGE,
    HttpConfig,
    HttpError,
    HttpRequest,
    HttpResponse,
    YHttpServer,
    _load_ddos_block_page_bytes,  # noqa: F401  re-exported for 0.x compatibility
    file_response,
)
from .tcp import TcpClient, TcpConfig, TcpConnection, YServer, YTcpServer
from .udp import ReliableUdpChannel, UdpConfig, UdpDatagram, UdpEndpoint, YUdpServer
from .websocket import (
    WS_GUID,
    CloseCode,
    WebSocketClient,
    WebSocketConfig,
    WebSocketConnection,
    YWebSocketServer,
)

__all__ = [
    "AuthConfig",
    "BaseServer",
    "CloseCode",
    "DDOS_BLOCK_MESSAGE",
    "HttpConfig",
    "HttpError",
    "HttpRequest",
    "HttpResponse",
    "Metrics",
    "RateLimitConfig",
    "ReliableUdpChannel",
    "ServerConfig",
    "ServerState",
    "TLSConfig",
    "TcpClient",
    "TcpConfig",
    "TcpConnection",
    "UdpConfig",
    "UdpDatagram",
    "UdpEndpoint",
    "WS_GUID",
    "WebSocketClient",
    "WebSocketConfig",
    "WebSocketConnection",
    "WsMessage",
    "YHttpServer",
    "YServer",
    "YTcpServer",
    "YUdpServer",
    "YWebSocketServer",
    "file_response",
]

# Handler type aliases, unchanged from 0.x.
WsMessage = str | bytes
TcpHandler = Callable[[TcpConnection, str, YServer], Awaitable[Any] | Any]
WsHandler = Callable[[WebSocketConnection, WsMessage, YWebSocketServer], Awaitable[Any] | Any]
HttpHandler = Callable[[HttpRequest, YHttpServer], Awaitable[Any] | Any]
UdpHandler = Callable[[UdpEndpoint, str, YUdpServer], Awaitable[Any] | Any]


class _SlidingWindowRateLimiter(_BaseSlidingWindowRateLimiter):
    """0.x-style constructor: positional ``limit`` and ``window_seconds``."""

    def __init__(self, limit: int | None, window_seconds: float) -> None:
        super().__init__(RateLimitConfig(limit=limit, window_seconds=max(1.0, float(window_seconds))))


# Private helpers some downstream code imports by their old names.
_close_writer_quietly = close_writer_quietly
_extract_bearer_token = extract_bearer_token
_format_peer_name = format_peer_name
_is_numeric_ddos_limit = is_numeric_limit
