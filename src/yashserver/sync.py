"""Blocking wrappers for code that does not want to write asyncio.

Each wrapper owns an async server underneath (``.async_server``) and forwards
to it. Handlers may still be ``async def`` — they are awaited for you.
"""

from __future__ import annotations

import asyncio
import inspect
import ssl
from typing import Any, Callable

from .core import AuthConfig, TLSConfig
from .http import HttpRequest, YHttpServer
from .plugin import ServerPlugin
from .tcp import TcpConnection, YTcpServer
from .udp import UdpEndpoint, YUdpServer
from .websocket import WebSocketConnection, YWebSocketServer

__all__ = [
    "YSyncHttpServer",
    "YSyncServer",
    "YSyncTcpServer",
    "YSyncUdpServer",
    "YSyncWebSocketServer",
    "run_many",
]


def _as_async_server(server: Any) -> Any:
    async_server = getattr(server, "async_server", None)
    return async_server if async_server is not None else server


async def _run_servers(async_servers: list[Any]) -> None:
    tasks: list[asyncio.Task[Any]] = []
    try:
        for index, server in enumerate(async_servers):
            tasks.append(asyncio.create_task(server.run(), name=f"yashserver-run-{index}"))
        if tasks:
            await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # First Ctrl+C cancels the main task. Treat that as graceful shutdown.
        pass
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5.0)
            except Exception:
                pass

        stop_awaitables: list[Any] = []
        for server in async_servers:
            stop = getattr(server, "stop", None)
            if stop is None:
                continue
            # run() already stops its own server, so only chase the ones that
            # are still up. BaseServer reports that through `state`; anything
            # else is judged by whether it still holds a listener.
            state = getattr(server, "state", None)
            if state is not None:
                if getattr(state, "value", state) in ("stopped", "stopping"):
                    continue
            elif getattr(server, "_server", None) is None:
                continue
            try:
                result = stop()
                if inspect.isawaitable(result):
                    stop_awaitables.append(result)
            except Exception:
                continue
        if stop_awaitables:
            try:
                await asyncio.wait_for(asyncio.gather(*stop_awaitables, return_exceptions=True), timeout=5.0)
            except Exception:
                pass


def run_many(*servers: Any) -> None:
    """Run several yashserver instances together, across protocols.

    ::

        yashserver.run_many(http_server, ws_server, udp_server)

    Ctrl+C shuts all of them down gracefully.
    """

    async_servers = [_as_async_server(server) for server in servers]
    try:
        asyncio.run(_run_servers(async_servers))
    except KeyboardInterrupt:
        # Fallback for environments that raise KeyboardInterrupt directly.
        return


class _SyncBase:
    """Shared forwarding for the blocking wrappers."""

    async_server: Any

    @property
    def tools(self) -> dict[str, Callable[..., Any]]:
        return self.async_server.tools

    @property
    def metrics(self) -> Any:
        return self.async_server.metrics

    @property
    def config(self) -> Any:
        return self.async_server.config

    def add_plugin(self, plugin: ServerPlugin) -> "_SyncBase":
        self.async_server.add_plugin(plugin)
        return self

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        self.async_server.register_tool(name, tool)

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self.async_server.use_tool(name, *args, **kwargs)

    def setddosprot(
        self,
        ddosprot: bool | int | float,
        rate_limit_window_seconds: float | None = None,
    ) -> "_SyncBase":
        self.async_server.setddosprot(ddosprot, rate_limit_window_seconds=rate_limit_window_seconds)
        return self

    def run(self) -> None:
        run_many(self)

    def _bind(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap a user handler so it receives *this* wrapper as the server."""

        async def wrapped(*args: Any) -> Any:
            result = func(*args[:-1], self)
            if inspect.isawaitable(result):
                result = await result
            return result

        return wrapped


class YSyncServer(_SyncBase):
    """Sync wrapper for :class:`~yashserver.tcp.YTcpServer`."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        delimiter: str = "\n",
        ssl_context: ssl.SSLContext | None = None,
        *,
        tls: TLSConfig | None = None,
        **options: Any,
    ) -> None:
        self.async_server = YTcpServer(
            host=host,
            port=port,
            delimiter=delimiter,
            ssl_context=ssl_context,
            tls=tls,
            **options,
        )

    def route(self, command: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.async_server.add_route(command, self._bind(func))
            return func

        return decorator

    def on_connection(self, func: Callable[..., Any]) -> Callable[..., Any]:
        self.async_server.on_connection(self._bind(func))
        return func

    def send(self, client_or_id: TcpConnection | str, payload: Any) -> Any:
        return self.async_server.send(client_or_id, payload)

    def broadcast(self, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast(payload, exclude=exclude)


#: Clearer alias; ``YSyncServer`` is the historical name.
YSyncTcpServer = YSyncServer


class YSyncUdpServer(_SyncBase):
    """Sync wrapper for :class:`~yashserver.udp.YUdpServer`.

    Same UDP caveats apply: sending is best effort, and nothing here implies
    a connection.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9002,
        **options: Any,
    ) -> None:
        self.async_server = YUdpServer(host=host, port=port, **options)

    def route(self, command: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.async_server.add_route(command, self._bind(func))
            return func

        return decorator

    def on_datagram(self, func: Callable[..., Any]) -> Callable[..., Any]:
        self.async_server.on_datagram(self._bind(func))
        return func

    def send_to(self, endpoint: UdpEndpoint | tuple[str, int], payload: Any) -> Any:
        return self.async_server.send_to(endpoint, payload)

    def broadcast(self, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast(payload, exclude=exclude)

    def known_endpoints(self) -> list[UdpEndpoint]:
        return self.async_server.known_endpoints()


class YSyncWebSocketServer(_SyncBase):
    """Sync wrapper for :class:`~yashserver.websocket.YWebSocketServer`."""

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
        **options: Any,
    ) -> None:
        self.async_server = YWebSocketServer(
            host=host,
            port=port,
            ssl_context=ssl_context,
            auth_token=auth_token,
            rate_limit_per_window=rate_limit_per_window,
            rate_limit_window_seconds=rate_limit_window_seconds,
            ddosprot=ddosprot,
            max_message_size_bytes=max_message_size_bytes,
            tls=tls,
            auth=auth,
            **options,
        )

    def route(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.async_server.add_route(path, self._bind(func))
            return func

        return decorator

    def send(self, session_or_id: WebSocketConnection | str, payload: Any) -> Any:
        return self.async_server.send(session_or_id, payload)

    def broadcast(self, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast(payload, exclude=exclude)

    def broadcast_to_room(self, room: str, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast_to_room(room, payload, exclude=exclude)

    def join_room(self, session_or_id: WebSocketConnection | str, room: str) -> None:
        self.async_server.join_room(session_or_id, room)

    def leave_room(self, session_or_id: WebSocketConnection | str, room: str) -> None:
        self.async_server.leave_room(session_or_id, room)

    def rooms(self) -> dict[str, int]:
        return self.async_server.rooms()


class YSyncHttpServer(_SyncBase):
    """Sync wrapper for :class:`~yashserver.http.YHttpServer`."""

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
        **options: Any,
    ) -> None:
        self.async_server = YHttpServer(
            host=host,
            port=port,
            ssl_context=ssl_context,
            auth_token=auth_token,
            auth_exempt_paths=auth_exempt_paths,
            rate_limit_per_window=rate_limit_per_window,
            rate_limit_window_seconds=rate_limit_window_seconds,
            ddosprot=ddosprot,
            tls=tls,
            auth=auth,
            **options,
        )

    def route(
        self,
        path: str,
        method: str = "GET",
        *,
        stream: bool = False,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.async_server.add_route(path, self._bind(func), method=method, stream=stream)
            return func

        return decorator

    def get(self, path: str, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, method="GET", **kwargs)

    def post(self, path: str, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, method="POST", **kwargs)

    def put(self, path: str, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, method="PUT", **kwargs)

    def patch(self, path: str, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, method="PATCH", **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, method="DELETE", **kwargs)

    def middleware(self, func: Callable[..., Any]) -> Callable[..., Any]:
        self.async_server.middleware(func)
        return func

    def static(self, url_prefix: str, directory: str) -> None:
        self.async_server.static(url_prefix, directory)

    def mount_websocket(self, ws_server: Any, path_prefix: str = "/") -> None:
        self.async_server.mount_websocket(_as_async_server(ws_server), path_prefix)

    @staticmethod
    def html(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.html(content, status=status, headers=headers)

    @staticmethod
    def text(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.text(content, status=status, headers=headers)

    @staticmethod
    def json(data: Any, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.json(data, status=status, headers=headers)

    @staticmethod
    def file(path: str, request: HttpRequest | None = None, **kwargs: Any) -> Any:
        return YHttpServer.file(path, request, **kwargs)
