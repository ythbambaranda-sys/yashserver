from __future__ import annotations

import asyncio
import inspect
import ssl
from typing import Any, Callable

from .plugin import ServerPlugin
from .server import HttpRequest, TcpClient, WebSocketClient, WsMessage, YHttpServer, YServer, YWebSocketServer


def _as_async_server(server: Any) -> Any:
    async_server = getattr(server, "async_server", None)
    return async_server if async_server is not None else server


async def _run_servers(async_servers: list[Any]) -> None:
    tasks: list[asyncio.Task[Any]] = []
    try:
        for index, server in enumerate(async_servers):
            tasks.append(asyncio.create_task(server.run(), name=f"yserver-run-{index}"))
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
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
            except Exception:
                pass

        stop_awaitables: list[Any] = []
        for server in async_servers:
            stop = getattr(server, "stop", None)
            if stop is None:
                continue
            internal_server = getattr(server, "_server", None)
            if internal_server is None:
                continue
            try:
                result = stop()
                if inspect.isawaitable(result):
                    stop_awaitables.append(result)
            except Exception:
                continue
        if stop_awaitables:
            try:
                await asyncio.wait_for(asyncio.gather(*stop_awaitables, return_exceptions=True), timeout=2.0)
            except Exception:
                pass


def run_many(*servers: Any) -> None:
    """Run multiple yserver instances together without writing asyncio code."""

    async_servers = [_as_async_server(server) for server in servers]
    try:
        asyncio.run(_run_servers(async_servers))
    except KeyboardInterrupt:
        # Fallback for environments that raise KeyboardInterrupt directly.
        return


class YSyncServer:
    """Sync wrapper for YServer."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        delimiter: str = "\n",
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.async_server = YServer(host=host, port=port, delimiter=delimiter, ssl_context=ssl_context)

    @property
    def tools(self) -> dict[str, Callable[..., Any]]:
        return self.async_server.tools

    def add_plugin(self, plugin: ServerPlugin) -> "YSyncServer":
        self.async_server.add_plugin(plugin)
        return self

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        self.async_server.register_tool(name, tool)

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self.async_server.use_tool(name, *args, **kwargs)

    def route(self, command: str) -> Callable[[Callable[[TcpClient, str, "YSyncServer"], Any]], Callable[[TcpClient, str, "YSyncServer"], Any]]:
        def decorator(func: Callable[[TcpClient, str, "YSyncServer"], Any]) -> Callable[[TcpClient, str, "YSyncServer"], Any]:
            async def wrapped(client: TcpClient, payload: str, _server: YServer) -> Any:
                result = func(client, payload, self)
                if inspect.isawaitable(result):
                    result = await result
                return result

            self.async_server.add_route(command, wrapped)
            return func

        return decorator

    def send(self, client_or_id: TcpClient | str, payload: Any) -> Any:
        return self.async_server.send(client_or_id, payload)

    def broadcast(self, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast(payload, exclude=exclude)

    def run(self) -> None:
        run_many(self)


class YSyncWebSocketServer:
    """Sync wrapper for YWebSocketServer."""

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
        self.async_server = YWebSocketServer(
            host=host,
            port=port,
            ssl_context=ssl_context,
            auth_token=auth_token,
            rate_limit_per_window=rate_limit_per_window,
            rate_limit_window_seconds=rate_limit_window_seconds,
            max_message_size_bytes=max_message_size_bytes,
        )

    @property
    def tools(self) -> dict[str, Callable[..., Any]]:
        return self.async_server.tools

    def add_plugin(self, plugin: ServerPlugin) -> "YSyncWebSocketServer":
        self.async_server.add_plugin(plugin)
        return self

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        self.async_server.register_tool(name, tool)

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self.async_server.use_tool(name, *args, **kwargs)

    def route(
        self,
        path: str,
    ) -> Callable[
        [Callable[[WebSocketClient, WsMessage, "YSyncWebSocketServer"], Any]],
        Callable[[WebSocketClient, WsMessage, "YSyncWebSocketServer"], Any],
    ]:
        def decorator(
            func: Callable[[WebSocketClient, WsMessage, "YSyncWebSocketServer"], Any],
        ) -> Callable[[WebSocketClient, WsMessage, "YSyncWebSocketServer"], Any]:
            async def wrapped(session: WebSocketClient, message: WsMessage, _server: YWebSocketServer) -> Any:
                result = func(session, message, self)
                if inspect.isawaitable(result):
                    result = await result
                return result

            self.async_server.add_route(path, wrapped)
            return func

        return decorator

    def send(self, session_or_id: WebSocketClient | str, payload: Any) -> Any:
        return self.async_server.send(session_or_id, payload)

    def broadcast(self, payload: Any, exclude: str | None = None) -> Any:
        return self.async_server.broadcast(payload, exclude=exclude)

    def run(self) -> None:
        run_many(self)


class YSyncHttpServer:
    """Sync wrapper for YHttpServer."""

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
        self.async_server = YHttpServer(
            host=host,
            port=port,
            ssl_context=ssl_context,
            auth_token=auth_token,
            auth_exempt_paths=auth_exempt_paths,
            rate_limit_per_window=rate_limit_per_window,
            rate_limit_window_seconds=rate_limit_window_seconds,
        )

    @property
    def tools(self) -> dict[str, Callable[..., Any]]:
        return self.async_server.tools

    def add_plugin(self, plugin: ServerPlugin) -> "YSyncHttpServer":
        self.async_server.add_plugin(plugin)
        return self

    def register_tool(self, name: str, tool: Callable[..., Any]) -> None:
        self.async_server.register_tool(name, tool)

    def use_tool(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self.async_server.use_tool(name, *args, **kwargs)

    def route(
        self,
        path: str,
        method: str = "GET",
    ) -> Callable[[Callable[[HttpRequest, "YSyncHttpServer"], Any]], Callable[[HttpRequest, "YSyncHttpServer"], Any]]:
        def decorator(func: Callable[[HttpRequest, "YSyncHttpServer"], Any]) -> Callable[[HttpRequest, "YSyncHttpServer"], Any]:
            async def wrapped(request: HttpRequest, _server: YHttpServer) -> Any:
                result = func(request, self)
                if inspect.isawaitable(result):
                    result = await result
                return result

            self.async_server.add_route(path, wrapped, method=method)
            return func

        return decorator

    def get(self, path: str) -> Callable[[Callable[[HttpRequest, "YSyncHttpServer"], Any]], Callable[[HttpRequest, "YSyncHttpServer"], Any]]:
        return self.route(path, method="GET")

    def post(self, path: str) -> Callable[[Callable[[HttpRequest, "YSyncHttpServer"], Any]], Callable[[HttpRequest, "YSyncHttpServer"], Any]]:
        return self.route(path, method="POST")

    @staticmethod
    def html(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.html(content, status=status, headers=headers)

    @staticmethod
    def text(content: str, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.text(content, status=status, headers=headers)

    @staticmethod
    def json(data: Any, status: int = 200, headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
        return YHttpServer.json(data, status=status, headers=headers)

    def run(self) -> None:
        run_many(self)
