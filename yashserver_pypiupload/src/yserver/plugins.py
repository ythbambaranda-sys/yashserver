from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .plugin import ServerPlugin


@dataclass
class ConnectionStatsPlugin(ServerPlugin):
    """Tracks message volume and connection events."""

    name: str = "connection-stats"
    tcp_connections_opened: int = 0
    ws_connections_opened: int = 0
    tcp_messages: int = 0
    ws_messages: int = 0
    ws_binary_messages: int = 0

    async def on_tcp_connect(self, client: Any, server: Any) -> None:
        self.tcp_connections_opened += 1

    async def on_ws_connect(self, session: Any, server: Any) -> None:
        self.ws_connections_opened += 1

    async def on_tcp_message(self, client: Any, message: str, server: Any) -> str | None:
        self.tcp_messages += 1
        return message

    async def on_ws_message(self, session: Any, message: str, server: Any) -> str | None:
        self.ws_messages += 1
        return message

    async def on_ws_binary_message(self, session: Any, data: bytes, server: Any) -> bytes | None:
        self.ws_binary_messages += 1
        return data

    def snapshot(self) -> dict[str, int]:
        return {
            "tcp_connections_opened": self.tcp_connections_opened,
            "ws_connections_opened": self.ws_connections_opened,
            "tcp_messages": self.tcp_messages,
            "ws_messages": self.ws_messages,
            "ws_binary_messages": self.ws_binary_messages,
        }
