"""All four transports on one process, sharing plugins, auth and metrics.

Run:

    python examples/multiprotocol_server.py

Then:

    curl http://127.0.0.1:8080/api/status
    curl http://127.0.0.1:8080/metrics
    printf 'ping hello\\n' | nc 127.0.0.1 9000          # TCP
    echo -n 'ping hello' | nc -u -w1 127.0.0.1 9002     # UDP

The WebSocket endpoint is mounted on the HTTP server, so a browser page served
from http://127.0.0.1:8080 can open ws://127.0.0.1:8080/ws on the same origin.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver

# One plugin instance observes every protocol.
stats = yashserver.ConnectionStatsPlugin()
logging_plugin = yashserver.LoggingPlugin()


# ---------------------------------------------------------------------------
# HTTP: the REST surface and the metrics endpoint
# ---------------------------------------------------------------------------

http = yashserver.YHttpServer(host="127.0.0.1", port=8080, ddosprot=True)
http.add_plugin(stats).add_plugin(logging_plugin)


@http.middleware
async def request_id(request, call_next):
    response = await call_next(request)
    return response.set_header("X-Request-Id", request.id)


@http.get("/api/status")
async def status(_request, server):
    return {
        "protocol": "http",
        "uptime_seconds": round(server.uptime_seconds(), 2),
        "state": server.state.value,
    }


@http.get("/api/echo/{value}")
async def echo(request, _server):
    return {"you_said": request.param("value")}


@http.get("/metrics")
async def metrics(_request, _server):
    return {
        "http": http.metrics.snapshot(),
        "tcp": tcp.metrics.snapshot(),
        "udp": udp.metrics.snapshot(),
        "websocket": ws.metrics.snapshot(),
        "plugin": stats.snapshot(),
    }


# ---------------------------------------------------------------------------
# WebSocket: rooms, mounted on the HTTP port
# ---------------------------------------------------------------------------

ws = yashserver.YWebSocketServer(host="127.0.0.1", port=9001, ddosprot=True)
ws.add_plugin(stats).add_plugin(logging_plugin)


@ws.route("/ws")
async def socket(session, message, server):
    payload = yashserver.ServerTools.from_json(message, default={}) if isinstance(message, str) else {}

    if payload.get("join"):
        server.join_room(session, payload["join"])
        return {"joined": payload["join"], "members": len(server.room_members(payload["join"]))}

    if payload.get("room") and payload.get("text"):
        delivered = await server.broadcast_to_room(
            payload["room"],
            {"from": session.id[:8], "text": payload["text"]},
            exclude=session.id,
        )
        return {"delivered": delivered}

    return {"echo": payload or message}


http.mount_websocket(ws, "/ws")


# ---------------------------------------------------------------------------
# TCP: a line-based command protocol
# ---------------------------------------------------------------------------

tcp = yashserver.YTcpServer(host="127.0.0.1", port=9000, ddosprot=True)
tcp.add_plugin(stats).add_plugin(logging_plugin)


@tcp.route("ping")
async def tcp_ping(_client, payload, _server):
    return {"pong": payload}


@tcp.route("who")
async def tcp_who(client, _payload, server):
    return {"you": client.id[:8], "peers": len(server.clients)}


@tcp.route("shout")
async def tcp_shout(client, payload, server):
    delivered = await server.broadcast({"shout": payload}, exclude=client.id)
    return {"delivered": delivered}


# ---------------------------------------------------------------------------
# UDP: endpoints, not connections
# ---------------------------------------------------------------------------

udp = yashserver.YUdpServer(host="127.0.0.1", port=9002, ddosprot=True, max_packet_size=1200)
udp.add_plugin(stats).add_plugin(logging_plugin)


@udp.route("ping")
async def udp_ping(endpoint, payload, _server):
    # The reply is best effort. UDP cannot tell us whether it arrives.
    return {"pong": payload, "you_are": endpoint.key}


@udp.route("peers")
async def udp_peers(_endpoint, _payload, server):
    return {"recently_seen": [str(endpoint) for endpoint in server.known_endpoints()]}


if __name__ == "__main__":
    print("HTTP      http://127.0.0.1:8080")
    print("WebSocket ws://127.0.0.1:8080/ws  (also on its own port 9001)")
    print("TCP       127.0.0.1:9000  (line protocol, try: ping hello)")
    print("UDP       127.0.0.1:9002  (datagrams, try: ping hello)")
    print("Ctrl+C to stop all four gracefully.")
    yashserver.run_many(http, ws, tcp, udp)
