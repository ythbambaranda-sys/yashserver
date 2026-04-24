from __future__ import annotations

import os
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yserver

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_HTML_PATH = PROJECT_ROOT / "test.html"

MAX_CHUNK_SIZE = 256 * 1024
MAX_FILE_SIZE = 256 * 1024 * 1024
MAX_STORED_FILES = 100
FILE_TTL_SECONDS = 60 * 60
TRANSFER_TTL_SECONDS = 10 * 60
TOKEN_ENV_VAR = "YSERVER_TOKEN"
MONGO_URI_ENV_VAR = "YSERVER_MONGO_URI"
MONGO_HOST_ENV_VAR = "YSERVER_MONGO_HOST"
MONGO_PORT_ENV_VAR = "YSERVER_MONGO_PORT"
MONGO_DB_ENV_VAR = "YSERVER_MONGO_DB"
MONGO_COLLECTION_ENV_VAR = "YSERVER_MONGO_COLLECTION"
MONGO_GRIDFS_BUCKET_ENV_VAR = "YSERVER_MONGO_GRIDFS_BUCKET"
MONGO_GRIDFS_CHUNK_SIZE_ENV_VAR = "YSERVER_MONGO_GRIDFS_CHUNK_SIZE"
MONGO_TEXT_ENABLED_ENV_VAR = "YSERVER_MONGO_TEXT_ENABLED"
MONGO_DIRECT_ENV_VAR = "YSERVER_MONGO_DIRECT"
MONGO_SERVER_SELECTION_TIMEOUT_MS_ENV_VAR = "YSERVER_MONGO_SERVER_SELECTION_TIMEOUT_MS"
MONGO_CONNECT_TIMEOUT_MS_ENV_VAR = "YSERVER_MONGO_CONNECT_TIMEOUT_MS"
MONGO_SOCKET_TIMEOUT_MS_ENV_VAR = "YSERVER_MONGO_SOCKET_TIMEOUT_MS"
MONGO_WRITE_CONCERN_ENV_VAR = "YSERVER_MONGO_WRITE_CONCERN"
MONGO_JOURNAL_ENV_VAR = "YSERVER_MONGO_JOURNAL"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, "")
    if not value.strip():
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _parse_write_concern(raw_value: str, default: int | str) -> int | str:
    value = (raw_value or "").strip()
    if not value:
        return default
    if value.isdigit():
        return int(value)
    return value

AUTH_TOKEN = os.getenv(TOKEN_ENV_VAR, "")
AUTH_ENABLED = bool(AUTH_TOKEN.strip())
MONGO_TEXT_ENABLED = os.getenv(MONGO_TEXT_ENABLED_ENV_VAR, "1").strip().lower() not in ("0", "false", "no", "off")
MONGO_HOST = os.getenv(MONGO_HOST_ENV_VAR, "localhost").strip() or "localhost"
MONGO_PORT = int(os.getenv(MONGO_PORT_ENV_VAR, "27018").strip() or "27018")
MONGO_DB = os.getenv(MONGO_DB_ENV_VAR, "yserver_chat").strip() or "yserver_chat"
MONGO_COLLECTION = os.getenv(MONGO_COLLECTION_ENV_VAR, "messages").strip() or "messages"
MONGO_GRIDFS_BUCKET = os.getenv(MONGO_GRIDFS_BUCKET_ENV_VAR, "fs").strip() or "fs"
MONGO_GRIDFS_CHUNK_SIZE = int(os.getenv(MONGO_GRIDFS_CHUNK_SIZE_ENV_VAR, str(512 * 1024)).strip() or str(512 * 1024))
MONGO_FILES_COLLECTION = f"{MONGO_GRIDFS_BUCKET}.files"
MONGO_FILE_CHUNKS_COLLECTION = f"{MONGO_GRIDFS_BUCKET}.chunks"
LEGACY_MONGO_FILES_COLLECTION = "files"
LEGACY_MONGO_FILE_CHUNKS_COLLECTION = "file_chunks"
MONGO_URI = os.getenv(MONGO_URI_ENV_VAR, "").strip() or f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
MONGO_DIRECT = _env_bool(MONGO_DIRECT_ENV_VAR, True)
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv(MONGO_SERVER_SELECTION_TIMEOUT_MS_ENV_VAR, "1200").strip() or "1200")
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv(MONGO_CONNECT_TIMEOUT_MS_ENV_VAR, "1200").strip() or "1200")
MONGO_SOCKET_TIMEOUT_MS = int(os.getenv(MONGO_SOCKET_TIMEOUT_MS_ENV_VAR, "3000").strip() or "3000")
MONGO_WRITE_CONCERN = _parse_write_concern(os.getenv(MONGO_WRITE_CONCERN_ENV_VAR, "1"), 1)
MONGO_JOURNAL = _env_bool(MONGO_JOURNAL_ENV_VAR, False)


@dataclass(slots=True)
class IncomingTransfer:
    transfer_id: str
    sender_session_id: str
    sender_name: str
    file_name: str
    mime_type: str
    expected_size: int
    total_chunks: int
    chunks: dict[int, bytes] = field(default_factory=dict)
    received_bytes: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class StoredFile:
    file_id: str
    sender_name: str
    file_name: str
    mime_type: str
    data: bytes
    created_at: float = field(default_factory=time.time)


incoming_transfers: dict[str, IncomingTransfer] = {}
stored_files: dict[str, StoredFile] = {}
saved_text_messages = 0
saved_file_messages = 0
mongo_write_errors = 0
mongo_file_write_errors = 0
mongo_connect_attempts = 0
mongo_connect_failures = 0
mongo_last_error = ""
mongo_last_saved_at = ""
mongo_last_connected_at = ""

mongo_client: yserver.DatabaseClient | None = None
mongo_gridfs_bucket: Any | None = None
mongo_connect_error = ""
mongo_gridfs_error = ""


def _connect_mongo(*, force: bool = False) -> bool:
    global mongo_client
    global mongo_gridfs_bucket
    global mongo_connect_error
    global mongo_gridfs_error
    global mongo_connect_attempts
    global mongo_connect_failures
    global mongo_last_error
    global mongo_last_connected_at

    if not MONGO_TEXT_ENABLED:
        return False
    if mongo_client is not None and not force:
        return True
    if force and mongo_client is not None:
        try:
            mongo_client.close()
        except Exception:
            pass
        mongo_client = None
        mongo_gridfs_bucket = None

    mongo_connect_attempts += 1
    try:
        mongo_client = yserver.connect_database(
            "mongodb",
            uri=MONGO_URI,
            database=MONGO_DB,
            serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
            socketTimeoutMS=MONGO_SOCKET_TIMEOUT_MS,
            directConnection=MONGO_DIRECT,
            retryWrites=True,
            w=MONGO_WRITE_CONCERN,
            journal=MONGO_JOURNAL,
        )
        mongo_client.ping()
        mongo_gridfs_bucket = None
        mongo_connect_error = ""
        mongo_gridfs_error = ""
        mongo_last_error = ""
        mongo_last_connected_at = yserver.ServerTools.utc_now()
        return True
    except Exception as error:
        mongo_connect_failures += 1
        mongo_connect_error = str(error)
        mongo_gridfs_error = str(error)
        mongo_last_error = str(error)
        mongo_client = None
        mongo_gridfs_bucket = None
        return False


def _get_gridfs_bucket(*, force: bool = False) -> Any | None:
    global mongo_gridfs_bucket
    global mongo_gridfs_error
    global mongo_last_error

    if mongo_gridfs_bucket is not None and not force:
        return mongo_gridfs_bucket
    if not _connect_mongo(force=force):
        return None
    try:
        gridfs_module = __import__("gridfs")
        assert mongo_client is not None
        mongo_raw_client = mongo_client.raw()
        mongo_database = mongo_raw_client[MONGO_DB]
        mongo_gridfs_bucket = gridfs_module.GridFSBucket(
            mongo_database,
            bucket_name=MONGO_GRIDFS_BUCKET,
        )
        mongo_gridfs_error = ""
        return mongo_gridfs_bucket
    except Exception as error:
        mongo_gridfs_error = str(error)
        mongo_last_error = str(error)
        mongo_gridfs_bucket = None
        return None


if MONGO_TEXT_ENABLED:
    _connect_mongo()

http_server = yserver.YSyncHttpServer(
    host="127.0.0.1",
    port=8080,
    auth_token=AUTH_TOKEN if AUTH_ENABLED else None,
    auth_exempt_paths={"/", "/test.html", "/api/config"} if AUTH_ENABLED else None,
    rate_limit_per_window=800,
    rate_limit_window_seconds=60.0,
)
ws_server = yserver.YSyncWebSocketServer(
    host="127.0.0.1",
    port=9001,
    auth_token=AUTH_TOKEN if AUTH_ENABLED else None,
    rate_limit_per_window=2500,
    rate_limit_window_seconds=60.0,
    max_message_size_bytes=MAX_CHUNK_SIZE + 1024,
)

stats_plugin = yserver.ConnectionStatsPlugin()
log_plugin = yserver.LoggingPlugin()

http_server.add_plugin(log_plugin)
ws_server.add_plugin(log_plugin)
ws_server.add_plugin(stats_plugin)


def load_page_html() -> str:
    return TEST_HTML_PATH.read_text(encoding="utf-8")


def _save_text_message_to_mongo(
    *,
    session: yserver.WebSocketClient,
    sender: str,
    text: str,
    source: str,
) -> None:
    global saved_text_messages
    global mongo_write_errors
    global mongo_last_error
    global mongo_last_saved_at
    global mongo_client

    if not _connect_mongo():
        mongo_write_errors += 1
        return

    document = {
        "type": "text",
        "sender": sender,
        "text": text,
        "source": source,
        "session_id": session.id,
        "path": session.path,
        "remote_addr": session.remote_addr,
        "created_at": yserver.ServerTools.utc_now(),
    }

    for attempt in range(2):
        try:
            assert mongo_client is not None
            mongo_client.insert_one(MONGO_COLLECTION, document)
            saved_text_messages += 1
            mongo_last_saved_at = yserver.ServerTools.utc_now()
            mongo_last_error = ""
            return
        except Exception as error:
            mongo_write_errors += 1
            mongo_last_error = str(error)
            if mongo_client is not None:
                try:
                    mongo_client.close()
                except Exception:
                    pass
            mongo_client = None
            if attempt == 0 and _connect_mongo(force=True):
                continue
            return


def _save_file_to_mongo(
    *,
    file_id: str,
    sender: str,
    file_name: str,
    mime_type: str,
    data: bytes,
) -> None:
    global saved_file_messages
    global mongo_file_write_errors
    global mongo_last_error
    global mongo_last_saved_at
    global mongo_client
    global mongo_gridfs_bucket

    if not _connect_mongo():
        mongo_file_write_errors += 1
        return

    for attempt in range(2):
        try:
            bucket = _get_gridfs_bucket(force=(attempt > 0))
            if bucket is None:
                raise RuntimeError(mongo_gridfs_error or "GridFS bucket unavailable")

            metadata = {
                "app": "yserver",
                "type": "file",
                "file_id": file_id,
                "sender": sender,
                "mime": mime_type,
                "size": len(data),
                "is_image": mime_type.startswith("image/"),
                "created_at": yserver.ServerTools.utc_now(),
            }
            upload_with_id = getattr(bucket, "upload_from_stream_with_id", None)
            if callable(upload_with_id):
                upload_with_id(
                    file_id,
                    file_name,
                    data,
                    chunk_size_bytes=MONGO_GRIDFS_CHUNK_SIZE,
                    metadata=metadata,
                )
            else:
                stream = bucket.open_upload_stream_with_id(
                    file_id,
                    file_name,
                    chunk_size_bytes=MONGO_GRIDFS_CHUNK_SIZE,
                    metadata=metadata,
                )
                try:
                    stream.write(data)
                finally:
                    stream.close()

            saved_file_messages += 1
            mongo_last_saved_at = yserver.ServerTools.utc_now()
            mongo_last_error = ""
            return
        except Exception as error:
            mongo_file_write_errors += 1
            mongo_last_error = str(error)
            if mongo_client is not None:
                try:
                    mongo_client.close()
                except Exception:
                    pass
            mongo_client = None
            mongo_gridfs_bucket = None
            if attempt == 0 and _connect_mongo(force=True):
                continue
            return


def _load_file_from_legacy_mongo(file_id: str) -> StoredFile | None:
    if not _connect_mongo():
        return None
    try:
        assert mongo_client is not None
        documents = mongo_client.find(
            LEGACY_MONGO_FILES_COLLECTION,
            {"type": "file", "file_id": file_id},
            limit=1,
            sort=[("_id", -1)],
        )
        if not documents:
            return None
        document = documents[0]
        storage_mode = str(document.get("storage", "inline")).strip().lower()
        data: bytes
        if storage_mode == "chunked":
            raw_chunk_count = int(document.get("chunk_count", 0) or 0)
            chunks = mongo_client.find(
                LEGACY_MONGO_FILE_CHUNKS_COLLECTION,
                {"type": "file_chunk", "file_id": file_id},
                limit=max(1, raw_chunk_count if raw_chunk_count > 0 else 200000),
                sort=[("chunk_index", 1)],
            )
            if not chunks:
                return None
            assembled = bytearray()
            for chunk_doc in chunks:
                raw_chunk = chunk_doc.get("data", b"")
                if isinstance(raw_chunk, bytes):
                    assembled.extend(raw_chunk)
                elif isinstance(raw_chunk, bytearray):
                    assembled.extend(bytes(raw_chunk))
                elif isinstance(raw_chunk, memoryview):
                    assembled.extend(raw_chunk.tobytes())
                else:
                    try:
                        assembled.extend(bytes(raw_chunk))
                    except Exception:
                        return None
            data = bytes(assembled)
        else:
            raw_data = document.get("data", b"")
            if isinstance(raw_data, bytes):
                data = raw_data
            elif isinstance(raw_data, bytearray):
                data = bytes(raw_data)
            elif isinstance(raw_data, memoryview):
                data = raw_data.tobytes()
            else:
                try:
                    data = bytes(raw_data)
                except Exception:
                    return None
        return StoredFile(
            file_id=file_id,
            sender_name=str(document.get("sender", "unknown")),
            file_name=str(document.get("name", "download.bin")),
            mime_type=str(document.get("mime", "application/octet-stream")),
            data=data,
        )
    except Exception:
        return None


def _load_file_from_mongo(file_id: str) -> StoredFile | None:
    if not _connect_mongo():
        return None
    try:
        bucket = _get_gridfs_bucket()
        if bucket is None:
            return _load_file_from_legacy_mongo(file_id)
        stream = bucket.open_download_stream(file_id)
        try:
            metadata = getattr(stream, "metadata", {}) or {}
            data = stream.read()
            return StoredFile(
                file_id=file_id,
                sender_name=str(metadata.get("sender", "unknown")),
                file_name=str(getattr(stream, "filename", "") or metadata.get("name", "download.bin")),
                mime_type=str(metadata.get("mime", "application/octet-stream")),
                data=bytes(data),
            )
        finally:
            stream.close()
    except Exception:
        return _load_file_from_legacy_mongo(file_id)


def _cleanup_expired_state() -> None:
    now = time.time()
    expired_transfer_ids = [
        transfer_id
        for transfer_id, transfer in incoming_transfers.items()
        if (now - transfer.updated_at) > TRANSFER_TTL_SECONDS
    ]
    for transfer_id in expired_transfer_ids:
        incoming_transfers.pop(transfer_id, None)

    expired_file_ids = [
        file_id
        for file_id, stored_file in stored_files.items()
        if (now - stored_file.created_at) > FILE_TTL_SECONDS
    ]
    for file_id in expired_file_ids:
        stored_files.pop(file_id, None)

    if len(stored_files) > MAX_STORED_FILES:
        sorted_files = sorted(stored_files.values(), key=lambda item: item.created_at)
        for overflow_file in sorted_files[: len(stored_files) - MAX_STORED_FILES]:
            stored_files.pop(overflow_file.file_id, None)


def _sanitize_download_name(name: str) -> str:
    safe = "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_", ".", " ")).strip()
    return safe[:180] or "download.bin"


def _parse_chunk_packet(payload: bytes) -> tuple[str, int, int, bytes] | None:
    if len(payload) < 10:
        return None

    version = payload[0]
    transfer_id_len = payload[1]
    if version != 1:
        return None
    if transfer_id_len <= 0 or transfer_id_len > 64:
        return None

    header_size = 2 + transfer_id_len + 4 + 4
    if len(payload) < header_size:
        return None

    transfer_id_bytes = payload[2 : 2 + transfer_id_len]
    try:
        transfer_id = transfer_id_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return None

    chunk_index = int.from_bytes(payload[2 + transfer_id_len : 2 + transfer_id_len + 4], byteorder="big")
    total_chunks = int.from_bytes(payload[2 + transfer_id_len + 4 : 2 + transfer_id_len + 8], byteorder="big")
    chunk_data = payload[header_size:]

    return transfer_id, chunk_index, total_chunks, chunk_data


async def _broadcast_completed_transfer(transfer: IncomingTransfer, server: yserver.YSyncWebSocketServer) -> None:
    assembled = bytearray()
    for chunk_index in range(transfer.total_chunks):
        chunk = transfer.chunks.get(chunk_index)
        if chunk is None:
            return
        assembled.extend(chunk)

    if len(assembled) != transfer.expected_size:
        return

    file_id = secrets.token_urlsafe(12)
    payload_bytes = bytes(assembled)
    stored_files[file_id] = StoredFile(
        file_id=file_id,
        sender_name=transfer.sender_name,
        file_name=transfer.file_name,
        mime_type=transfer.mime_type,
        data=payload_bytes,
    )
    _save_file_to_mongo(
        file_id=file_id,
        sender=transfer.sender_name,
        file_name=transfer.file_name,
        mime_type=transfer.mime_type,
        data=payload_bytes,
    )
    _cleanup_expired_state()

    await server.broadcast(
        {
            "type": "file",
            "sender": transfer.sender_name,
            "name": transfer.file_name,
            "mime": transfer.mime_type,
            "size": transfer.expected_size,
            "url": f"/download/{file_id}",
        }
    )


async def _handle_binary_chunk(
    session: yserver.WebSocketClient,
    payload: bytes,
    server: yserver.YSyncWebSocketServer,
) -> dict[str, Any] | None:
    parsed = _parse_chunk_packet(payload)
    if parsed is None:
        return {"type": "error", "message": "invalid chunk packet format"}

    transfer_id, chunk_index, total_chunks, chunk_data = parsed
    transfer = incoming_transfers.get(transfer_id)
    if transfer is None:
        return {"type": "error", "message": f"unknown transfer id: {transfer_id}"}
    if transfer.sender_session_id != session.id:
        return {"type": "error", "message": "transfer ownership mismatch"}
    if total_chunks != transfer.total_chunks:
        return {"type": "error", "message": "transfer chunk-count mismatch"}
    if chunk_index < 0 or chunk_index >= transfer.total_chunks:
        return {"type": "error", "message": "chunk index out of range"}
    if len(chunk_data) > MAX_CHUNK_SIZE:
        return {"type": "error", "message": "chunk too large"}

    transfer.updated_at = time.time()
    if chunk_index not in transfer.chunks:
        transfer.chunks[chunk_index] = chunk_data
        transfer.received_bytes += len(chunk_data)
    elif transfer.chunks[chunk_index] != chunk_data:
        return {"type": "error", "message": "chunk checksum mismatch"}

    await server.send(
        session,
        {
            "type": "chunk_ack",
            "transfer_id": transfer_id,
            "chunk_index": chunk_index,
            "received_chunks": len(transfer.chunks),
            "total_chunks": transfer.total_chunks,
        },
    )

    if len(transfer.chunks) == transfer.total_chunks:
        incoming_transfers.pop(transfer_id, None)
        await _broadcast_completed_transfer(transfer, server)
    return None


@http_server.get("/")
def home(_request, _server):
    return yserver.YSyncHttpServer.html(load_page_html(), headers={"Cache-Control": "no-store"})


@http_server.get("/test.html")
def test_page(_request, _server):
    return yserver.YSyncHttpServer.html(load_page_html(), headers={"Cache-Control": "no-store"})


@http_server.get("/api/stats")
def stats(_request, _server):
    _cleanup_expired_state()
    snapshot = stats_plugin.snapshot()
    snapshot.update(
        {
            "active_transfers": len(incoming_transfers),
            "stored_files": len(stored_files),
            "mongo_text_enabled": MONGO_TEXT_ENABLED,
            "mongo_connected": mongo_client is not None,
            "mongo_db": MONGO_DB,
            "mongo_collection": MONGO_COLLECTION,
            "mongo_files_collection": MONGO_FILES_COLLECTION,
            "mongo_file_chunks_collection": MONGO_FILE_CHUNKS_COLLECTION,
            "mongo_gridfs_bucket": MONGO_GRIDFS_BUCKET,
            "mongo_write_concern": MONGO_WRITE_CONCERN,
            "mongo_journal": MONGO_JOURNAL,
            "mongo_connect_attempts": mongo_connect_attempts,
            "mongo_connect_failures": mongo_connect_failures,
            "mongo_saved_text_messages": saved_text_messages,
            "mongo_saved_file_messages": saved_file_messages,
            "mongo_write_errors": mongo_write_errors,
            "mongo_file_write_errors": mongo_file_write_errors,
            "mongo_last_connected_at": mongo_last_connected_at,
            "mongo_last_saved_at": mongo_last_saved_at,
            "mongo_last_error": mongo_last_error,
        }
    )
    return yserver.YSyncHttpServer.json(snapshot)


@http_server.get("/api/config")
def config(_request, _server):
    return yserver.YSyncHttpServer.json(
        {
            "ws_url": "ws://127.0.0.1:9001/chat",
            "auth_required": AUTH_ENABLED,
            "token_env_var": TOKEN_ENV_VAR,
            "chunk_size": MAX_CHUNK_SIZE,
            "max_file_size": MAX_FILE_SIZE,
            "mongo_text_enabled": MONGO_TEXT_ENABLED,
            "mongo_connected": mongo_client is not None,
            "mongo_db": MONGO_DB,
            "mongo_collection": MONGO_COLLECTION,
            "mongo_files_collection": MONGO_FILES_COLLECTION,
            "mongo_file_chunks_collection": MONGO_FILE_CHUNKS_COLLECTION,
            "mongo_gridfs_bucket": MONGO_GRIDFS_BUCKET,
            "mongo_write_concern": MONGO_WRITE_CONCERN,
            "mongo_journal": MONGO_JOURNAL,
        },
        headers={"Cache-Control": "no-store"},
    )


@http_server.get("/api/mongo/recent")
def mongo_recent(request, _server):
    if not MONGO_TEXT_ENABLED:
        return yserver.YSyncHttpServer.json({"error": "mongo-text-disabled"}, status=503)
    if not _connect_mongo():
        return yserver.YSyncHttpServer.json({"error": "mongo-unavailable", "detail": mongo_connect_error}, status=503)

    limit_raw = request.query_params.get("limit", ["20"])[0] if request.query_params.get("limit") else "20"
    try:
        limit = max(1, min(200, int(limit_raw)))
    except ValueError:
        limit = 20

    try:
        assert mongo_client is not None
        documents = mongo_client.find(
            MONGO_COLLECTION,
            {"type": "text"},
            limit=limit,
            sort=[("_id", -1)],
        )
        return yserver.YSyncHttpServer.json(
            {
                "mongo_db": MONGO_DB,
                "mongo_collection": MONGO_COLLECTION,
                "count": len(documents),
                "documents": documents,
            },
            headers={"Cache-Control": "no-store"},
        )
    except Exception as error:
        return yserver.YSyncHttpServer.json({"error": "mongo-query-failed", "detail": str(error)}, status=500)


@http_server.get("/api/mongo/recent-files")
def mongo_recent_files(request, _server):
    if not MONGO_TEXT_ENABLED:
        return yserver.YSyncHttpServer.json({"error": "mongo-text-disabled"}, status=503)
    if not _connect_mongo():
        return yserver.YSyncHttpServer.json({"error": "mongo-unavailable", "detail": mongo_connect_error}, status=503)

    limit_raw = request.query_params.get("limit", ["20"])[0] if request.query_params.get("limit") else "20"
    try:
        limit = max(1, min(200, int(limit_raw)))
    except ValueError:
        limit = 20

    try:
        assert mongo_client is not None
        gridfs_documents = mongo_client.find(
            MONGO_FILES_COLLECTION,
            {"metadata.app": "yserver", "metadata.type": "file"},
            limit=limit,
            sort=[("uploadDate", -1)],
        )
        summarized: list[dict[str, Any]] = []
        seen_file_ids: set[str] = set()
        for document in gridfs_documents:
            metadata = document.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            size = int(document.get("length", metadata.get("size", 0)) or 0)
            chunk_size = int(document.get("chunkSize", MONGO_GRIDFS_CHUNK_SIZE) or MONGO_GRIDFS_CHUNK_SIZE)
            chunk_count = (size + chunk_size - 1) // chunk_size if chunk_size > 0 else 0
            file_id = str(document.get("_id", ""))
            if not file_id:
                continue
            seen_file_ids.add(file_id)
            summarized.append(
                {
                    "file_id": file_id,
                    "sender": str(metadata.get("sender", "unknown")),
                    "name": str(document.get("filename", "download.bin")),
                    "mime": str(metadata.get("mime", "application/octet-stream")),
                    "size": size,
                    "storage": "gridfs",
                    "chunk_count": int(chunk_count),
                    "is_image": bool(metadata.get("is_image", False)),
                    "created_at": str(metadata.get("created_at", document.get("uploadDate", ""))),
                }
            )

        legacy_documents = mongo_client.find(
            LEGACY_MONGO_FILES_COLLECTION,
            {"type": "file"},
            limit=limit,
            sort=[("_id", -1)],
        )
        for document in legacy_documents:
            file_id = str(document.get("file_id", ""))
            if not file_id or file_id in seen_file_ids:
                continue
            seen_file_ids.add(file_id)
            summarized.append(
                {
                    "file_id": file_id,
                    "sender": str(document.get("sender", "unknown")),
                    "name": str(document.get("name", "download.bin")),
                    "mime": str(document.get("mime", "application/octet-stream")),
                    "size": int(document.get("size", 0)),
                    "storage": str(document.get("storage", "legacy-inline")),
                    "chunk_count": int(document.get("chunk_count", 0) or 0),
                    "is_image": bool(document.get("is_image", False)),
                    "created_at": str(document.get("created_at", "")),
                }
            )

        summarized = summarized[:limit]
        return yserver.YSyncHttpServer.json(
            {
                "mongo_db": MONGO_DB,
                "mongo_files_collection": MONGO_FILES_COLLECTION,
                "mongo_file_chunks_collection": MONGO_FILE_CHUNKS_COLLECTION,
                "mongo_gridfs_bucket": MONGO_GRIDFS_BUCKET,
                "count": len(summarized),
                "documents": summarized,
            },
            headers={"Cache-Control": "no-store"},
        )
    except Exception as error:
        return yserver.YSyncHttpServer.json({"error": "mongo-query-failed", "detail": str(error)}, status=500)


@http_server.get("*")
def downloads_or_404(request, _server):
    if request.path.startswith("/download/"):
        file_id = request.path.replace("/download/", "", 1).strip()
        if not file_id:
            return yserver.YSyncHttpServer.text("Not Found", status=404)
        _cleanup_expired_state()
        stored_file = stored_files.get(file_id)
        if stored_file is None:
            stored_file = _load_file_from_mongo(file_id)
            if stored_file is None:
                return yserver.YSyncHttpServer.text("Not Found", status=404)
            stored_files[file_id] = stored_file

        safe_name = _sanitize_download_name(stored_file.file_name)
        return (
            200,
            stored_file.data,
            {
                "Content-Type": stored_file.mime_type or "application/octet-stream",
                "Content-Disposition": f'attachment; filename="{safe_name}"',
                "Cache-Control": "no-store",
            },
        )
    return yserver.YSyncHttpServer.text("Not Found", status=404)


@ws_server.route("/chat")
def chat(
    session: yserver.WebSocketClient,
    message: yserver.WsMessage,
    server: yserver.YSyncWebSocketServer,
):
    _cleanup_expired_state()
    sender = session.id[:6]

    if isinstance(message, bytes):
        return _handle_binary_chunk(session, message, server)

    payload = yserver.ServerTools.from_json(message, default=None)
    if not isinstance(payload, dict):
        text = message.strip()
        if not text:
            return None
        _save_text_message_to_mongo(
            session=session,
            sender=sender,
            text=text,
            source="plain-text",
        )
        return server.broadcast({"type": "text", "sender": sender, "text": text})

    command = str(payload.get("cmd", "text")).strip().lower()
    if command == "text":
        text = str(payload.get("text", "")).strip()
        if not text:
            return None
        _save_text_message_to_mongo(
            session=session,
            sender=sender,
            text=text,
            source="json-cmd-text",
        )
        return server.broadcast({"type": "text", "sender": sender, "text": text})

    if command == "file_start":
        transfer_id = str(payload.get("transfer_id", "")).strip()
        file_name = str(payload.get("name", "file.bin")).strip()[:180] or "file.bin"
        mime_type = str(payload.get("mime", "application/octet-stream")).strip() or "application/octet-stream"
        try:
            expected_size = int(payload.get("size", 0))
            total_chunks = int(payload.get("total_chunks", 0))
        except (TypeError, ValueError):
            return {"type": "error", "message": "invalid file_start size or total_chunks"}

        if not transfer_id or len(transfer_id) > 64:
            return {"type": "error", "message": "invalid transfer_id"}
        if expected_size <= 0 or expected_size > MAX_FILE_SIZE:
            return {"type": "error", "message": "file size out of bounds"}
        if total_chunks <= 0 or total_chunks > 25000:
            return {"type": "error", "message": "invalid total_chunks"}

        incoming_transfers[transfer_id] = IncomingTransfer(
            transfer_id=transfer_id,
            sender_session_id=session.id,
            sender_name=sender,
            file_name=file_name,
            mime_type=mime_type,
            expected_size=expected_size,
            total_chunks=total_chunks,
        )
        return {
            "type": "file_start_ack",
            "transfer_id": transfer_id,
            "chunk_size": MAX_CHUNK_SIZE,
        }

    if command == "file_cancel":
        transfer_id = str(payload.get("transfer_id", "")).strip()
        transfer = incoming_transfers.get(transfer_id)
        if transfer and transfer.sender_session_id == session.id:
            incoming_transfers.pop(transfer_id, None)
            return {"type": "file_cancelled", "transfer_id": transfer_id}
        return {"type": "error", "message": "unable to cancel transfer"}

    return {"type": "error", "message": f"unknown command: {command}"}


@ws_server.route("*")
def fallback(_session, _message, _server):
    return {"error": "connect using ws://127.0.0.1:9001/chat"}


if __name__ == "__main__":
    print("Open this in your browser: http://127.0.0.1:8080")
    print("WebSocket endpoint: ws://127.0.0.1:9001/chat")
    if AUTH_ENABLED:
        print(f"Auth enabled. Use token from env var: {TOKEN_ENV_VAR}")
    if MONGO_TEXT_ENABLED:
        if mongo_client is not None:
            print(
                "Mongo persistence enabled: "
                f"{MONGO_URI} ({MONGO_DB}.{MONGO_COLLECTION}, "
                f"{MONGO_DB}.{MONGO_FILES_COLLECTION}, {MONGO_DB}.{MONGO_FILE_CHUNKS_COLLECTION}) "
                f"bucket={MONGO_GRIDFS_BUCKET} chunk={MONGO_GRIDFS_CHUNK_SIZE} "
                f"w={MONGO_WRITE_CONCERN} journal={MONGO_JOURNAL}"
            )
        else:
            print(f"Mongo text persistence unavailable: {mongo_connect_error}")
    else:
        print("Mongo text persistence disabled by env.")
    try:
        yserver.run_many(http_server, ws_server)
    except OSError as error:
        error_text = str(error).lower()
        if (
            getattr(error, "winerror", None) == 10048
            or getattr(error, "errno", None) == 10048
            or "address already in use" in error_text
            or "winerror 10048" in error_text
        ):
            print("Port already in use. Stop the previous yserver process, then run again.")
            print("Tip: close old terminals or kill python.exe using port 8080/9001.")
        else:
            print(f"Server failed to start: {error}")
    except KeyboardInterrupt:
        print("Servers stopped")
    finally:
        if mongo_client is not None:
            try:
                mongo_client.close()
            except Exception:
                pass
        mongo_gridfs_bucket = None
