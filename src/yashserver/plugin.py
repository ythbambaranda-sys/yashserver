from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any


class ServerPlugin:
    """Base plugin hooks, shared by every transport.

    Every hook is optional and every hook is a no-op by default, so a plugin
    only overrides what it cares about. Hooks that return a value
    (``on_*_message``, ``on_udp_datagram``) can transform or drop traffic by
    returning a replacement or ``None``.
    """

    name = "server-plugin"

    async def on_startup(self, server: Any) -> None:
        """Called when a server starts."""

    async def on_shutdown(self, server: Any) -> None:
        """Called when a server stops."""

    async def on_tcp_connect(self, client: Any, server: Any) -> None:
        """Called when a TCP client connects."""

    async def on_tcp_disconnect(self, client: Any, server: Any) -> None:
        """Called when a TCP client disconnects."""

    async def on_tcp_message(self, client: Any, message: str, server: Any) -> str | None:
        """
        Called before TCP routing.
        Return a new message string to transform it.
        Return None to drop the message.
        """

        return message

    async def on_ws_connect(self, session: Any, server: Any) -> None:
        """Called when a WebSocket client connects."""

    async def on_ws_disconnect(self, session: Any, server: Any) -> None:
        """Called when a WebSocket client disconnects."""

    async def on_ws_message(self, session: Any, message: str, server: Any) -> str | None:
        """
        Called before WebSocket routing.
        Return a new message string to transform it.
        Return None to drop the message.
        """

        return message

    async def on_ws_binary_message(self, session: Any, data: bytes, server: Any) -> bytes | None:
        """
        Called before WebSocket routing for binary payloads.
        Return new bytes to transform it.
        Return None to drop the message.
        """

        return data

    async def on_udp_datagram(self, datagram: Any, server: Any) -> "Any | None":
        """
        Called before UDP routing.
        Return a new datagram to transform it.
        Return None to drop it.
        """

        return datagram

    async def on_udp_endpoint_seen(self, endpoint: Any, server: Any) -> None:
        """Called the first time a UDP endpoint is heard from."""

    async def on_udp_endpoint_expired(self, endpoint: Any, server: Any) -> None:
        """Called when a UDP endpoint is dropped from the known-peers table."""

    async def on_ws_close(self, session: Any, code: int, reason: str, server: Any) -> None:
        """Called when a WebSocket peer sends a close frame."""

    async def on_http_request(self, request: Any, server: Any) -> None:
        """Called before an HTTP route is executed."""

    async def on_error(self, error: BaseException, context: dict[str, Any], server: Any) -> None:
        """Called when server code catches an exception."""


class LoggingPlugin(ServerPlugin):
    """Simple stdout/logger plugin for visibility while developing."""

    name = "logging-plugin"

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("yashserver")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    async def on_startup(self, server: Any) -> None:
        self.logger.info("%s starting", type(server).__name__)

    async def on_shutdown(self, server: Any) -> None:
        self.logger.info("%s stopped", type(server).__name__)

    async def on_tcp_connect(self, client: Any, server: Any) -> None:
        self.logger.info("tcp connect %s (%s)", client.id, client.address)

    async def on_tcp_disconnect(self, client: Any, server: Any) -> None:
        self.logger.info("tcp disconnect %s", client.id)

    async def on_ws_connect(self, session: Any, server: Any) -> None:
        self.logger.info("ws connect %s (%s)", session.id, session.path)

    async def on_udp_endpoint_seen(self, endpoint: Any, server: Any) -> None:
        self.logger.info("udp endpoint %s", endpoint)

    async def on_ws_disconnect(self, session: Any, server: Any) -> None:
        self.logger.info("ws disconnect %s", session.id)

    async def on_error(self, error: BaseException, context: dict[str, Any], server: Any) -> None:
        # Network churn is normal in browser/server dev loops (refreshes, tab close, cert retries).
        # Keep terminal signal high by not treating expected disconnects as hard errors.
        noisy = (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, asyncio.IncompleteReadError)
        if isinstance(error, noisy):
            self.logger.info("%s: network disconnect (%s)", type(server).__name__, context)
            return
        if isinstance(error, ssl.SSLError):
            lowered = str(error).lower()
            noisy_ssl_markers = (
                "unknown ca",
                "certificate verify failed",
                "wrong version number",
                "tlsv1 alert",
                "sslv3 alert",
                "application data after close notify",
            )
            if any(marker in lowered for marker in noisy_ssl_markers):
                self.logger.info("%s: tls handshake/disconnect (%s)", type(server).__name__, context)
                return
        self.logger.error("%s: %s (%s)", type(server).__name__, error, context)
