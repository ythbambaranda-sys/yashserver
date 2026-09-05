# WebSocket

A native RFC 6455 implementation with no external dependencies.

```python
import yashserver

ws = yashserver.YWebSocketServer(host="0.0.0.0", port=9001)

@ws.route("/chat")
async def chat(session, message, server):
    return {"echo": message}

yashserver.run_many(ws)
```

## Messages

Handlers receive `str` for text frames and `bytes` for binary frames. Fragments
are reassembled before your handler sees them.

```python
@ws.route("/chat")
async def chat(session, message, server):
    if isinstance(message, bytes):
        return {"binary": len(message)}
    payload = yashserver.ServerTools.from_json(message, default={})
    return {"text": payload.get("text")}
```

Returning a value sends it back: `bytes` go out as a binary frame, `str` as
text, anything else as JSON text.

```python
await session.send({"a": 1})     # JSON text frame
await session.send_text("hi")
await session.send_bytes(b"\x00\x01")
await session.close(yashserver.CloseCode.NORMAL, "goodbye")
```

## Rooms

```python
server.join_room(session, "lobby")
server.leave_room(session, "lobby")

server.room_members("lobby")     # list of connections
server.rooms()                   # {"lobby": 3, "game-7": 12}
session.rooms                    # {"lobby"}
session.in_room("lobby")

delivered = await server.broadcast_to_room("lobby", {"msg": "hi"}, exclude=session.id)
delivered = await server.broadcast({"msg": "to everyone"})
```

Rooms are cleaned up automatically: a disconnecting session is removed from
every room it was in, and an empty room disappears. Fan-out runs concurrently,
and a peer that blows its write timeout is closed rather than allowed to stall
the broadcast.

## Streaming large messages

```python
async def produce():
    with open("video.mp4", "rb") as handle:
        while chunk := handle.read(64 * 1024):
            yield chunk

await server.send_stream(session, produce(), binary=True)
```

The message goes out as fragmented frames, so the browser receives one logical
message while your process never holds more than a chunk.

## The connection object

```python
session.id
session.path            # "/chat"
session.query("token")  # from the handshake query string
session.headers
session.remote_addr
session.connected_at
session.subprotocol
session.is_open
session.tls
session.messages_received, session.messages_sent
```

## Lifecycle events

```python
class Presence(yashserver.ServerPlugin):
    async def on_ws_connect(self, session, server): ...
    async def on_ws_disconnect(self, session, server): ...
    async def on_ws_close(self, session, code, reason, server): ...
    async def on_ws_message(self, session, message, server):
        return message      # transform, or None to drop
    async def on_ws_binary_message(self, session, data, server):
        return data
```

## Keepalive and timeouts

```python
yashserver.YWebSocketServer(
    ping_interval_seconds=20.0,     # None disables keepalive pings
    ping_timeout_seconds=20.0,      # no pong in time → connection dropped
    idle_timeout_seconds=300.0,     # nothing received → 1001 going away
    write_timeout_seconds=30.0,
    handshake_timeout_seconds=10.0,
)
```

Ping/pong detects peers that have vanished without a TCP close — the common
case behind NAT, on mobile, and with a laptop lid.

## Protocol conformance

Violations close the connection with the right RFC 6455 code instead of being
silently tolerated:

| Situation | Close code |
|---|---|
| Text frame that is not valid UTF-8 | `1007` invalid payload |
| Unmasked client frame | `1002` protocol error |
| Reserved bits set with no extension | `1002` protocol error |
| Control frame over 125 bytes, or fragmented | `1002` protocol error |
| Continuation with nothing to continue | `1002` protocol error |
| Message over `max_message_size_bytes` | `1009` message too big |
| Server shutting down | `1001` going away |

A frame that *declares* a huge length is rejected before anything is
allocated, so a client cannot make the server reserve gigabytes by lying in a
header.

## Authentication

Checked during the handshake, before the upgrade, so an unauthorised client
never reaches a route:

```python
ws = yashserver.YWebSocketServer(auth_token="secret")
```

Accepted as `?token=secret`, `X-Yserver-Token:` or `Authorization: Bearer`.
Browsers cannot set headers on a WebSocket, so the query parameter is usually
the practical choice. Failures get an HTTP `401` rather than a socket that
opens and then dies.

For richer schemes:

```python
async def validate(context):
    return await session_is_valid(context["token"])

ws = yashserver.YWebSocketServer(auth=yashserver.AuthConfig(validator=validate))
```

## Subprotocols

```python
ws = yashserver.YWebSocketServer(subprotocols=("chat.v2", "chat.v1"))
# the first client-offered match is selected and echoed in the handshake
```

## Same port as your web app

```python
app = yashserver.YHttpServer(port=8080)
app.mount_websocket(ws, "/ws")
yashserver.run_many(app, ws)
```

A page from `http://host:8080` opens `ws://host:8080/ws` — one origin, one
certificate. See `examples/websocket_chat_rooms.py`.

## Configuration

```python
yashserver.YWebSocketServer(
    host="0.0.0.0",
    port=9001,
    tls=yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem"),  # wss://
    auth_token=None,
    max_message_size_bytes=8 * 1024 * 1024,
    max_frame_size_bytes=8 * 1024 * 1024,
    max_connections=10_000,
    ping_interval_seconds=20.0,
    idle_timeout_seconds=300.0,
    rate_limit_per_window=300,
    rate_limit_window_seconds=60.0,
    subprotocols=(),
)
```

## Metrics

```python
ws.metrics.snapshot()
# counters: connections_opened/closed/refused, messages_received/sent,
#           broadcasts, oversized_messages, invalid_utf8, ping_timeouts,
#           idle_timeouts, stalled_disconnects, unauthorized, rate_limited
# gauges:   connections_active, rooms
# summaries: handler_seconds
```
