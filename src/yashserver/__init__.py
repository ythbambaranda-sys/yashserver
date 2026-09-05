"""yashserver: multi-protocol asyncio server toolkit.

One import gets you every transport::

    import yashserver

    tcp  = yashserver.YTcpServer(port=9000)         # streaming, custom protocols
    udp  = yashserver.YUdpServer(port=9002)         # datagrams, endpoints
    http = yashserver.YHttpServer(port=8080)        # REST, files, streaming
    ws   = yashserver.YWebSocketServer(port=9001)   # messages, rooms, broadcast

    yashserver.run_many(tcp, udp, http, ws)

Shared concerns (lifecycle, auth, rate limiting, plugins, metrics, config,
graceful shutdown) come from one core. Protocol concepts stay protocol
specific: UDP talks about endpoints, HTTP about requests and status codes,
WebSocket about messages and rooms.
"""

__version__ = "1.0.1"

from .core import (
    AuthConfig,
    BaseServer,
    ConfigError,
    Metrics,
    RateLimitConfig,
    ServerConfig,
    ServerState,
    SlidingWindowRateLimiter,
    TLSConfig,
    YServerError,
)
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
from .archive import (
    ArchiveError,
    ArchiveFormat,
    ArchivePolicy,
    ExtractReport,
    UnsafeArchiveError,
    create_archive,
    detect_format,
    extract_archive,
    folder_archive_stream,
    iter_folder_entries,
    safe_member_path,
)
from .http import (
    HttpConfig,
    HttpError,
    HttpRequest,
    HttpResponse,
    YHttpServer,
    file_response,
)
from .plugin import LoggingPlugin, ServerPlugin
from .plugins import ConnectionStatsPlugin
from .sync import (
    YSyncHttpServer,
    YSyncServer,
    YSyncTcpServer,
    YSyncUdpServer,
    YSyncWebSocketServer,
    run_many,
)
from .tcp import TcpClient, TcpConfig, TcpConnection, YServer, YTcpServer
from .tools import ServerTools
from .udp import ReliableUdpChannel, UdpConfig, UdpDatagram, UdpEndpoint, YUdpServer
from .upload import (
    UploadError,
    UploadSession,
    UploadStore,
    file_digest,
    verify_file_digest,
)
from .websocket import (
    CloseCode,
    WebSocketClient,
    WebSocketConfig,
    WebSocketConnection,
    WsMessage,
    YWebSocketServer,
)

__all__ = [
    # resumable uploads / integrity
    "UploadError",
    "UploadSession",
    "UploadStore",
    "file_digest",
    "verify_file_digest",
    # archive / folder transfer
    "ArchiveError",
    "ArchiveFormat",
    "ArchivePolicy",
    "ExtractReport",
    "UnsafeArchiveError",
    "create_archive",
    "detect_format",
    "extract_archive",
    "folder_archive_stream",
    "iter_folder_entries",
    "safe_member_path",

    # core
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
    # tcp
    "TcpClient",
    "TcpConfig",
    "TcpConnection",
    "YServer",
    "YTcpServer",
    # udp
    "ReliableUdpChannel",
    "UdpConfig",
    "UdpDatagram",
    "UdpEndpoint",
    "YUdpServer",
    # http
    "HttpConfig",
    "HttpError",
    "HttpRequest",
    "HttpResponse",
    "YHttpServer",
    "file_response",
    # websocket
    "CloseCode",
    "WebSocketClient",
    "WebSocketConfig",
    "WebSocketConnection",
    "WsMessage",
    "YWebSocketServer",
    # sync wrappers
    "YSyncHttpServer",
    "YSyncServer",
    "YSyncTcpServer",
    "YSyncUdpServer",
    "YSyncWebSocketServer",
    "run_many",
    # plugins and tools
    "ConnectionStatsPlugin",
    "LoggingPlugin",
    "ServerPlugin",
    "ServerTools",
    # databases
    "DatabaseClient",
    "DatabaseConfigError",
    "DatabaseError",
    "MissingDependencyError",
    "UnsupportedDatabaseError",
    "build_sqlalchemy_url",
    "connect_database",
    "database_support_matrix",
    "list_supported_databases",
]
