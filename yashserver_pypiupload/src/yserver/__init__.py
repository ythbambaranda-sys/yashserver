"""yserver: make asyncio server development simple."""

from .database import (
    DatabaseClient,
    DatabaseConfigError,
    DatabaseError,
    MissingDependencyError,
    UnsupportedDatabaseError,
    build_sqlalchemy_url,
    connect_database,
    database_support_matrix,
    list_supported_databases,
)
from .plugin import LoggingPlugin, ServerPlugin
from .plugins import ConnectionStatsPlugin
from .server import HttpRequest, TcpClient, WebSocketClient, WsMessage, YHttpServer, YServer, YWebSocketServer
from .sync import YSyncHttpServer, YSyncServer, YSyncWebSocketServer, run_many
from .tools import ServerTools

__all__ = [
    "build_sqlalchemy_url",
    "connect_database",
    "database_support_matrix",
    "list_supported_databases",
    "ConnectionStatsPlugin",
    "DatabaseClient",
    "DatabaseConfigError",
    "DatabaseError",
    "LoggingPlugin",
    "MissingDependencyError",
    "ServerPlugin",
    "ServerTools",
    "HttpRequest",
    "UnsupportedDatabaseError",
    "WsMessage",
    "TcpClient",
    "WebSocketClient",
    "YHttpServer",
    "YServer",
    "YSyncHttpServer",
    "YSyncServer",
    "YSyncWebSocketServer",
    "YWebSocketServer",
    "run_many",
]
