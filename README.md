# yashserver

A multi-protocol asyncio server toolkit for Python. TCP, UDP, HTTP and
WebSocket, with TLS, in one dependency-free package.

```python
import yashserver

tcp  = yashserver.YTcpServer(port=9000)         # streaming, custom protocols
udp  = yashserver.YUdpServer(port=9002)         # datagrams, endpoints
http = yashserver.YHttpServer(port=8080)        # REST, files, streaming
ws   = yashserver.YWebSocketServer(port=9001)   # messages, rooms, broadcast

yashserver.run_many(tcp, udp, http, ws)
```

Core networking is standard library only. Database connectors are optional and
imported lazily.

## Install

```bash
pip install yashserver
```

For the optional database drivers:

```bash
pip install "yashserver[db]"
```

## One core, four protocols

Everything that is genuinely the same across transports is shared: server
lifecycle, start/stop, authentication, rate limiting, logging, plugins,
metrics, configuration and graceful shutdown.

Everything that is not the same stays protocol-specific. TCP and UDP do not
pretend to have the same connection model, HTTP speaks in requests and status
codes, and WebSocket speaks in messages and rooms.

```python
# Same on every transport
await server.start() / await server.stop() / await server.run()
async with server: ...
server.add_plugin(...)   server.register_tool(...)   server.every(5.0, tick)
server.metrics.snapshot()   server.state   server.bound_port
```

```python
# Deliberately different
tcp.clients              # connections you can address and close
udp.known_endpoints()    # addresses heard from recently — not connections
http.get("/users/{id}")  # routes, methods, middleware, status codes
ws.broadcast_to_room(…)  # messages, rooms, close codes
```

See [docs/architecture.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/architecture.md).

## TCP

```python
server = yashserver.YTcpServer(port=9000)

@server.route("ping")
async def ping(connection, payload, srv):
    return {"pong": payload}
```

Or own the socket for a protocol of your own:

```python
@server.on_connection
async def handle(connection, srv):
    size = struct.unpack("!I", await connection.readexactly(4))[0]
    body = await connection.readexactly(size)
    await connection.send_bytes(process(body))
```

Multiple simultaneous clients, connection lifecycle, streaming, configurable
timeouts, backpressure with write timeouts, graceful disconnects, TLS.
→ [docs/protocols/tcp.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/protocols/tcp.md)

## UDP

```python
server = yashserver.YUdpServer(port=9002, max_packet_size=1200)

@server.route("ping")
async def ping(endpoint, payload, srv):
    return {"pong": payload, "you_are": endpoint.key}
```

UDP is connectionless, so the API models **endpoints**, not sessions. There is
no `disconnect()`, because there is nothing to disconnect.

yashserver does not pretend UDP is reliable: sends are best effort, datagrams can
arrive out of order or twice or never, and the server counts loss and overload
rather than hiding them. When you need guarantees for *some* messages,
`ReliableUdpChannel` adds acknowledgements, retransmission, de-duplication and
optional ordering on top — opt in per message:

```python
channel = ReliableUdpChannel(server, ordered=True)
server.on_datagram(channel.handle_datagram)
await channel.send(endpoint, b"this one matters")
```

IPv4 and IPv6 (dual stack), configurable packet sizes, bounded peer tracking.
→ [docs/protocols/udp.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/protocols/udp.md)

## HTTP

```python
app = yashserver.YHttpServer(port=8080, auth_token="secret",
                          auth_exempt_paths={"/health"})

@app.get("/api/users/{uid}")
async def user(request, srv):
    return {"id": request.param("uid")}

@app.middleware
async def timing(request, call_next):
    response = await call_next(request)
    return response.set_header("X-Request-Id", request.id)

@app.post("/upload", stream=True)
async def upload(request, srv):
    async for chunk in request.stream():
        write(chunk)                       # never buffered in memory

@app.get("/video/{name}")
async def video(request, srv):
    return yashserver.file_response(f"media/{request.param('name')}", request)
```

HTTP/1.1 with keep-alive, path parameters, JSON APIs, streaming requests and
responses, range-aware large-file transfers, authentication, rate limiting,
middleware, and correct status handling (405 with `Allow`, 413, 431, 416, 304).
→ [docs/protocols/http.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/protocols/http.md)

## WebSocket

```python
ws = yashserver.YWebSocketServer(port=9001)

@ws.route("/chat")
async def chat(session, message, server):
    server.join_room(session, "lobby")
    await server.broadcast_to_room("lobby", {"said": message}, exclude=session.id)
    return {"ok": True}
```

Text and binary messages, rooms, broadcast and per-client sends, connection
lifecycle events, authentication at handshake time, ping/pong keepalive, and
streamed sends for large payloads. RFC 6455 conformance including proper close
codes.
→ [docs/protocols/websocket.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/protocols/websocket.md)

Serve it on the same port as your web app, so a page and its socket share one
origin and one certificate:

```python
app.mount_websocket(ws, "/ws")
```

## TLS

```python
tls = yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem", minimum_version="1.2")

http = yashserver.YHttpServer(port=8443, tls=tls)   # https
ws   = yashserver.YWebSocketServer(port=9443, tls=tls)  # wss
tcp  = yashserver.YTcpServer(port=9000, tls=tls)
```

TLS 1.2 floor, compression off, no ephemeral key reuse, and misconfiguration
that fails loudly instead of quietly starting something insecure. Mutual TLS is
supported and requires a CA bundle.

TLS is not offered on UDP: DTLS is not in the standard library, so there is no
`ssl_context` there rather than a broken one.
→ [docs/tls.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/tls.md)

## Not async? Fine

```python
import yashserver

app = yashserver.YSyncHttpServer(port=8080)

@app.get("/")
def index(request, server):
    return {"hello": "world"}

app.run()
```

`YSyncServer`, `YSyncUdpServer`, `YSyncHttpServer` and `YSyncWebSocketServer`
wrap the async servers; handlers may be sync or async.

## Shared concerns

**Plugins** — one plugin can observe every protocol:

```python
class Audit(yashserver.ServerPlugin):
    async def on_startup(self, server): ...
    async def on_tcp_message(self, client, message, server): return message
    async def on_udp_datagram(self, datagram, server): return datagram
    async def on_ws_connect(self, session, server): ...
    async def on_http_request(self, request, server): ...
    async def on_error(self, error, context, server): ...

for server in (tcp, udp, http, ws):
    server.add_plugin(Audit())
```

**Metrics** — counters, gauges and summaries on every server:

```python
@app.get("/metrics")
async def metrics(request, server):
    return {"http": http.metrics.snapshot(), "udp": udp.metrics.snapshot()}
```

**Auth and rate limiting** — shared configuration, applied where each protocol
can apply it:

```python
yashserver.AuthConfig(token="secret", exempt_paths={"/health"})
yashserver.AuthConfig(validator=my_async_jwt_check)

server.setddosprot(20)      # 20 events/second
server.setddosprot(False)   # off
```

**Graceful shutdown** — Ctrl+C stops every server cleanly. In-flight work is
drained up to `shutdown_drain_seconds`; connections merely sitting idle do not
delay shutdown; WebSocket peers get a `1001 going away` close frame rather than
a socket that vanishes.

## Examples

| File | What it shows |
|---|---|
| `examples/multiprotocol_server.py` | All four transports in one process, shared plugins and metrics |
| `examples/http_rest_api.py` | REST API: path params, middleware, auth, CORS, error handlers |
| `examples/http_file_server.py` | Streaming uploads and downloads, ranges, traversal protection |
| `examples/websocket_chat_rooms.py` | Chat with rooms, served on one port with its web page |
| `examples/udp_game_server.py` | Game server: unreliable state ticks, reliable join/chat |
| `examples/tcp_custom_protocol.py` | Length-prefixed binary protocol with a client |
| `examples/benchmark.py` | Throughput and shutdown benchmarks |
| `examples/task1_browser_server.py` | The original browser demo (chunked binary upload, chat) |
| `examples/task2_database_support.py` | Database connectors |

## Databases

```python
import yashserver

db = yashserver.connect_database("sqlite", database=":memory:")
db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
print(db.fetch_all("SELECT * FROM users"))
db.close()
```

Supported: MySQL, PostgreSQL, SQL Server, Oracle, SQLite, MariaDB, MongoDB,
Redis, Cassandra, DynamoDB, Firebase, Couchbase, Snowflake, BigQuery,
Redshift, ClickHouse, Elasticsearch, Neo4j, InfluxDB, DuckDB. SQL backends go
through SQLAlchemy; drivers are optional and imported only when used.

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

269 tests covering the core abstractions, per-protocol behaviour and
conformance, the UDP reliability helper, archives and folder transfer,
resumable uploads, cross-protocol lifecycle and graceful shutdown.

## Upgrading from 0.x

**The import name changed: `import yserver` becomes `import yashserver`.**
That is the one change every 0.x codebase must make. Beyond it the API is
unchanged. See [docs/migration.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/docs/migration.md) for the rename and for
the three behaviour changes (HTTP keep-alive is on, invalid UTF-8 in a
WebSocket text frame now closes with 1007, rate-limiter memory is bounded).

## License

MIT — see [LICENSE.md](https://github.com/ythbambaranda-sys/yashserver/blob/main/LICENSE.md).
