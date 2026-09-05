from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import yashserver

TEST_HTML_PATH = PROJECT_ROOT / "test.html"

http_server = yashserver.YSyncHttpServer(host="127.0.0.1", port=8080)
ws_server = yashserver.YSyncWebSocketServer(host="127.0.0.1", port=9001)
stats_plugin = yashserver.ConnectionStatsPlugin()
ws_server.add_plugin(stats_plugin)

# Hard-coded DDOS policy: 4 reloads per second per remote address.
http_server.setddosprot(4, rate_limit_window_seconds=1.0)


def _load_test_html() -> str:
    return TEST_HTML_PATH.read_text(encoding="utf-8")


@http_server.get("/")
def home(_request, _server):
    return yashserver.YSyncHttpServer.html(_load_test_html(), headers={"Cache-Control": "no-store"})


@http_server.get("/test.html")
def test_html(_request, _server):
    return yashserver.YSyncHttpServer.html(_load_test_html(), headers={"Cache-Control": "no-store"})


@http_server.get("/api/config")
def config(_request, _server):
    return yashserver.YSyncHttpServer.json(
        {
            "ws_url": "ws://127.0.0.1:9001/chat",
            "auth_required": False,
            "token_env_var": "YSERVER_TOKEN",
            "chunk_size": 262144,
            "max_file_size": 268435456,
        },
        headers={"Cache-Control": "no-store"},
    )


@http_server.get("/api/stats")
def stats(_request, _server):
    snapshot = stats_plugin.snapshot()
    snapshot.update(
        {
            "active_transfers": 0,
            "stored_files": 0,
        }
    )
    return yashserver.YSyncHttpServer.json(snapshot, headers={"Cache-Control": "no-store"})


@http_server.get("*")
def not_found(_request, _server):
    return yashserver.YSyncHttpServer.text("Not Found", status=404)


@ws_server.route("/chat")
def chat(session: yashserver.WebSocketClient, message: yashserver.WsMessage, server: yashserver.YSyncWebSocketServer):
    if isinstance(message, bytes):
        return {"type": "error", "message": "Binary uploads are disabled in this demo server."}

    payload = yashserver.ServerTools.from_json(message, default={})
    if not isinstance(payload, dict):
        text = str(message).strip()
        if not text:
            return None
        return server.broadcast({"type": "text", "sender": session.id[:6], "text": text})

    command = str(payload.get("cmd", "text")).strip().lower()
    if command == "text":
        text = str(payload.get("text", "")).strip()
        if not text:
            return None
        return server.broadcast({"type": "text", "sender": session.id[:6], "text": text})
    if command == "file_start":
        return {"type": "error", "message": "File upload is disabled in this demo server."}
    if command == "file_cancel":
        return {"type": "file_cancelled", "transfer_id": str(payload.get("transfer_id", ""))}
    return {"type": "error", "message": f"unknown command: {command}"}


@ws_server.route("*")
def ws_fallback(_session, _message, _server):
    return {"error": "connect using ws://127.0.0.1:9001/chat"}


if __name__ == "__main__":
    print("Serving test.html on http://127.0.0.1:8080")
    print("WebSocket endpoint: ws://127.0.0.1:9001/chat")
    print("Hard-coded DDOS protection: 4 reloads per second per client IP")
    try:
        yashserver.run_many(http_server, ws_server)
    except KeyboardInterrupt:
        print("Servers stopped")
