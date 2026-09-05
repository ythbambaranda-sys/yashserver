# TCP

```python
import yashserver

server = yashserver.YTcpServer(host="0.0.0.0", port=9000)

@server.route("ping")
async def ping(connection, payload, srv):
    return {"pong": payload}

yashserver.run_many(server)
```

`YServer` is the historical name and remains an alias for `YTcpServer`.

## Two ways to handle a connection

**Command routing** — messages are delimited text, the first word selects the
route, the rest is the payload:

```python
@server.route("login")
async def login(connection, payload, srv):
    return {"welcome": payload}

@server.route("*")                     # fallback
async def unknown(connection, payload, srv):
    return {"error": "unknown command"}
```

**Raw connections** — for a protocol of your own design. You own the socket;
the framework still gives you lifecycle, timeouts, backpressure, TLS, metrics
and graceful shutdown:

```python
@server.on_connection
async def handle(connection, srv):
    header = await connection.readexactly(4)
    size = struct.unpack("!I", header)[0]
    body = await connection.readexactly(size)
    await connection.send_bytes(process(body))
```

Registering `on_connection` replaces command routing entirely. See
`examples/tcp_custom_protocol.py`.

## The connection object

```python
connection.id                 # stable id for this peer
connection.address            # remote host
connection.peer_port
connection.connected_at
connection.is_open
connection.tls                # served over TLS
connection.peer_certificate() # client cert under mutual TLS
connection.bytes_sent, connection.bytes_received, connection.messages_received

await connection.send({"a": 1})      # JSON + delimiter
await connection.send_bytes(b"raw")  # exactly these bytes
await connection.read(4096)
await connection.readexactly(8)
await connection.readuntil(b"\r\n")
await connection.close(drain=True)   # flush, then close
```

Iterate an incoming stream without buffering it:

```python
async for chunk in connection.stream(chunk_size=64 * 1024):
    handle.write(chunk)
```

## Broadcasting

```python
delivered = await server.broadcast({"notice": "server restarting"})
delivered = await server.broadcast(payload, exclude=connection.id)
```

Writes run concurrently, so one slow peer does not hold up the rest. A peer
that blows its write timeout is disconnected rather than allowed to grow an
unbounded buffer in your process. The return value is how many writes
succeeded.

## Backpressure and timeouts

```python
yashserver.YTcpServer(
    write_buffer_high_bytes=256 * 1024,  # drain() blocks past this
    write_buffer_low_bytes=64 * 1024,
    write_timeout_seconds=30.0,          # stalled peer is dropped
    idle_timeout_seconds=300.0,          # silent peer is closed; None disables
    max_connections=10_000,              # extra peers refused
    max_line_bytes=64 * 1024,            # message with no delimiter
    tcp_nodelay=True,                    # off for bulk transfer
)
```

`write_buffer_high_bytes` is where backpressure comes from: once that much
data is outstanding, `send()` blocks inside `drain()` and your handler slows to
the peer's pace. `write_timeout_seconds` bounds that wait so a peer that stops
reading entirely cannot pin a task forever.

`max_line_bytes` matters against hostile peers: without it, a client that opens
a connection and sends bytes without ever sending a delimiter would grow your
buffer indefinitely. Over the limit, the peer gets `message-too-large` and the
connection closes.

## TLS

```python
server = yashserver.YTcpServer(
    port=9443,
    tls=yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem"),
)
```

See [tls.md](../tls.md), including mutual TLS and reading the client
certificate via `connection.peer_certificate()`.

## Auth and rate limiting

TCP has no headers to carry a token, so authentication is a protocol decision
— typically a `login` command before anything else is accepted:

```python
AUTHENTICATED: set[str] = set()

@server.route("login")
async def login(connection, payload, srv):
    if not verify(payload):
        await connection.close()
        return None
    AUTHENTICATED.add(connection.id)
    return {"ok": True}

@server.route("secret")
async def secret(connection, payload, srv):
    if connection.id not in AUTHENTICATED:
        return {"error": "login first"}
    return {"data": "..."}
```

Rate limiting is per remote address and built in:

```python
yashserver.YTcpServer(rate_limit_per_window=600, rate_limit_window_seconds=60.0)
server.setddosprot(20)        # 20 messages/second
server.setddosprot(False)     # off
```

## Plugin hooks

```python
class Audit(yashserver.ServerPlugin):
    async def on_tcp_connect(self, client, server): ...
    async def on_tcp_disconnect(self, client, server): ...
    async def on_tcp_message(self, client, message, server):
        return message          # transform, or None to drop
```

## Metrics

```python
server.metrics.snapshot()
# counters: connections_opened/closed/refused, messages_received/sent,
#           oversized_messages, idle_timeouts, stalled_disconnects,
#           rate_limited, broadcasts, errors
# gauges:   connections_active
# summaries: handler_seconds
```
