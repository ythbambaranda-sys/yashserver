# Migrating from 0.x to 1.0

**1.0 renames the import package.** This is a deliberate breaking change and
the one edit every 0.x codebase must make:

```python
import yserver          # 0.x
import yashserver       # 1.0
```

```bash
pip install yashserver
```

Beyond that rename the public API is preserved — same classes, same
constructor arguments, same helpers, including the private helpers that
downstream code sometimes imported.

How to read the rest of this document:

* **The import rename** — required. Nothing works until you do it.
* **Nothing else to change** — once renamed, existing 0.x code runs unmodified.
* **Behaviour that changed** — informational, but not optional reading. Five
  externally visible behaviours differ, and each could affect a deployment.
* **New features** — optional. Adopt them when you need them.

## The import rename

The distribution and the import package now match:

| step    | 0.x                                               | 1.0                      |
| ------- | ------------------------------------------------- | ------------------------ |
| install | `pip install yashserver` or `pip install yserver` | `pip install yashserver` |
| import  | `import yserver`                                  | `import yashserver`      |

Every submodule moves with it:

| 0.x                 | 1.0                    |
| ------------------- | ---------------------- |
| `yserver.server`    | `yashserver.server`    |
| `yserver.core`      | `yashserver.core`      |
| `yserver.tcp`       | `yashserver.tcp`       |
| `yserver.udp`       | `yashserver.udp`       |
| `yserver.http`      | `yashserver.http`      |
| `yserver.websocket` | `yashserver.websocket` |
| `yserver.database`  | `yashserver.database`  |

Do the rename as a **whole-word** find-and-replace of `yserver` with
`yashserver` in your editor or IDE, with *Match whole word* enabled, then read
the diff before committing. Whole-word matching matters: a plain substring
replace turns an already-renamed `yashserver` into `yashyashserver` on a
second pass.

To see which files are affected first, list them. Both commands below are
read-only and modify nothing:

```bash
grep -rlwi --include='*.py' --include='*.toml' --include='*.cfg' \
     --include='*.txt' --include='*.md' yserver .
```

```powershell
Get-ChildItem -Recurse -Include *.py,*.toml,*.cfg,*.txt,*.md |
    Select-String -Pattern '\byserver\b' |
    Select-Object -Unique Path
```

Both searches are case-insensitive, so they will also surface two names that
must **not** be renamed:

- `YServer` — a retained legacy class alias, see
  [API renames](#api-renames-legacy-class-names-retained)
- `X-Yserver-Token` — the authentication header, deliberately unchanged

Environment variables such as `YSERVER_TOKEN` are not matched, because the
underscore makes them a different word.

Deliberately not offered here: piping either list into an in-place `sed`.
Rewriting files in bulk hits everything that matched, including files you did
not mean to touch — lock files, test fixtures, vendored code, anything under
`.git/` — and a match inside a binary file corrupts it with no warning. An
editor-driven replace is reviewable and reversible; a bulk rewrite is neither.

**There is no `yserver` compatibility shim, by design.** `import yserver`
raises `ModuleNotFoundError` under 1.0. Shipping a shim would mean the
`yashserver` distribution installing a top-level `yserver` package, which
collides with the separate `yserver` distribution already on PyPI: two
distributions owning the same import path, with whichever installed last
silently winning. A clean break is less trouble than that ambiguity.

If you must run old and new side by side, install them into separate virtual
environments.

## Nothing else to change

Once the import is renamed, these all behave as before:

```python
import yashserver

app = yashserver.YSyncServer(port=9000)

@app.route("ping")
def ping(client, payload, server):
    return {"reply": "pong"}

app.run()
```

* `YServer`, `YHttpServer`, `YWebSocketServer` — same constructors. Keyword
  arguments remain per-class; see the table below.
* `YSyncServer`, `YSyncHttpServer`, `YSyncWebSocketServer`, `run_many`.
* `TcpClient`, `WebSocketClient`, `HttpRequest`, `WsMessage`.
* `ServerPlugin`, `LoggingPlugin`, `ConnectionStatsPlugin`, `ServerTools`.
* `setddosprot()`, `.clients`, `.routes`, `.tools`, `.plugins`, `.started_at`,
  `._server`.
* `YHttpServer.html` / `.text` / `.json` static helpers, and the
  `(status, body)` / `(status, body, headers)` return forms.
* `import yashserver.server` — still there, including `_close_writer_quietly`,
  `_SlidingWindowRateLimiter`, `_extract_bearer_token`, `_format_peer_name`,
  `_is_numeric_ddos_limit`, `DDOS_BLOCK_MESSAGE` and `WS_GUID`.
* The whole `database` module.

### Constructor keyword arguments

These were never shared across all three classes, and still are not. An
argument that a class did not accept in 0.x raises `ConfigError` in 1.0 rather
than being silently ignored.

| keyword                     | `YTcpServer` / `YServer` | `YHttpServer` | `YWebSocketServer` | `YUdpServer` |
| --------------------------- | :----------------------: | :-----------: | :----------------: | :----------: |
| `ssl_context`               |           yes            |      yes      |        yes         |     yes      |
| `ddosprot`                  |           yes            |      yes      |        yes         |     yes      |
| `rate_limit_per_window`     |           yes            |      yes      |        yes         |     yes      |
| `rate_limit_window_seconds` |           yes            |      yes      |        yes         |     yes      |
| `auth_token`                |            no            |      yes      |        yes         |      no      |
| `auth_exempt_paths`         |            no            |      yes      |         no         |      no      |
| `max_message_size_bytes`    |            no            |       no      |        yes         |      no      |
| `delimiter`                 |           yes            |       no      |         no         |      no      |

`delimiter` accepts `str` or `bytes`; 0.x passed `str` and that still works.

## Behaviour that changed

Five changes are visible from outside, beyond the import rename. Each is
worth checking against your deployment.

**HTTP connections are now kept alive.** 0.x sent `Connection: close` on every response. 1.0 reuses connections, which is what HTTP/1.1 clients expect. A client that asks for `Connection: close` still gets it. To restore the old behaviour everywhere:

```python
app = yashserver.YHttpServer(keep_alive=False)
```

**A WebSocket text frame that is not valid UTF-8 now closes the connection with `1007`.** 0.x decoded it with `errors="replace"` and passed the mangled string to your handler. The new behaviour is what RFC 6455 requires. If you relied on receiving mangled text, send those payloads as binary frames instead.

**Rate limiter memory is bounded.** 0.x kept one bucket per source address forever, which leaked slowly on any internet-facing server. 1.0 sweeps expired keys and caps tracking at `max_tracked_keys` (100,000 by default). Limiting decisions are unchanged.

**Identity strings follow the rename.** The HTTP and WebSocket `Server`
response header now reads `yashserver` instead of `yserver`, and loggers moved
from `yserver.*` to `yashserver.*` — adjust any logging configuration keyed on
the old names. Override the header with
`YHttpServer(server_header="...")` if you depended on the old value.

**The authentication header name deliberately did *not* change.** It is still
`x-yserver-token`:

```python
AuthConfig(token="secret")            # clients still send X-Yserver-Token
AuthConfig(token="secret", header_name="x-yashserver-token")   # opt in
```

Renaming it would have broken authentication for every existing client at
runtime — a far worse failure than an import error, and unrelated to the
import rename. Set `header_name` explicitly if you want the new spelling.

## New features

Optional. Adopt what you need.

### UDP

```python
udp = yashserver.YUdpServer(port=9002, max_packet_size=1200)

@udp.route("ping")
async def ping(endpoint, payload, server):
    return {"pong": payload}
```

Routed datagrams carry `command payload` as text, matching the TCP server's
command style. See [protocols/udp.md](protocols/udp.md).

The API addresses **endpoints**, not connections, because UDP has neither.
`YUdpServer` exposes `known_endpoints` and has no `.clients` registry and no
`.disconnect()`; an endpoint is an address the server has heard from, not a
session it holds open. Delivery remains best-effort — see
`ReliableUdpChannel` for opt-in acknowledgement, retry and ordering.

### Path parameters and middleware

```python
@app.get("/users/{uid}/posts/{pid}")
async def post(request, server):
    return {"uid": request.param("uid"), "pid": request.param("pid")}

@app.middleware
async def timing(request, call_next):
    response = await call_next(request)
    return response.set_header("X-Elapsed", "...")
```

### Streaming and large files

```python
@app.post("/upload", stream=True)           # body never buffered
async def upload(request, server):
    async for chunk in request.stream():
        ...

@app.get("/video")                          # Range, ETag, chunked reads
async def video(request, server):
    return yashserver.file_response("big.mp4", request)
```

Streaming routes are not capped by `max_body_bytes`, which exists to protect
memory for buffered bodies. Set `max_stream_body_bytes` if you want a policy
limit on streamed uploads; it is unlimited by default.

### Folder transfer and archives

```python
app.serve_folder("/backup", "/srv/data")            # streamed tar.gz/zip/tar
app.accept_folder("/upload", "/srv/incoming")       # extracted under policy
```

ZIP, TAR, TAR.GZ, TAR.BZ2 and TAR.XZ are read and written; RAR is read-only.
See [archives.md](archives.md).

### Resumable uploads

```python
app.resumable_uploads("/uploads", "/srv/uploads")
```

`HEAD` the session for its `Upload-Offset`, then `PATCH` from there, so an
interrupted transfer resumes instead of restarting. Optional per-chunk and
whole-file checksums are verified server-side.

### WebSocket rooms

```python
server.join_room(session, "lobby")
await server.broadcast_to_room("lobby", {"msg": "hi"}, exclude=session.id)
```

### One port for a page and its socket

```python
app.mount_websocket(ws, "/ws")
```

### Metrics

```python
@app.get("/metrics")
async def metrics(request, server):
    return server.metrics.snapshot()
```

### Declarative TLS

```python
tls = yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem", minimum_version="1.3")
app = yashserver.YHttpServer(port=8443, tls=tls)
```

`ssl_context=` still works exactly as before.

## API renames (legacy class names retained)

| 0.x               | 1.0                                                | status                                    |
| ----------------- | -------------------------------------------------- | ----------------------------------------- |
| `YServer`         | `YTcpServer`                                       | alias, same class                         |
| `TcpClient`       | `TcpConnection`                                    | alias, same class                         |
| `WebSocketClient` | `WebSocketConnection`                              | alias, same class                         |
| `yserver.server`  | `yashserver.tcp` / `.udp` / `.http` / `.websocket` | split; facade kept as `yashserver.server` |

## New configuration you may want to set

1.0 adds limits that did not exist in 0.x. The defaults shown here are the
actual defaults, but on a public server it is worth being deliberate:

```python
yashserver.YHttpServer(
    max_connections=10_000,
    max_header_bytes=16 * 1024,
    max_body_bytes=16 * 1024 * 1024,
    keep_alive_timeout_seconds=5.0,
    shutdown_drain_seconds=5.0,
)
```

## Checking your upgrade

```bash
python -m unittest discover -s tests -p "test_*.py"
```

This runs the complete suite; every test is a `unittest.TestCase`, so no extra
dependency is required. `python -m pytest tests` collects the same tests and
additionally reports subtest counts, if you prefer that output and have pytest
installed — it is not a project dependency.

The suite includes the original 0.x tests unchanged, plus compatibility tests
covering the parts of the 0.x surface that *are* retained:

* the legacy class aliases resolve to the new classes — `YServer is
  YTcpServer`, `TcpClient is TcpConnection`, `WebSocketClient is
  WebSocketConnection`
* the `yashserver.server` facade still re-exports the legacy names and the
  private helpers (`_close_writer_quietly`, `_SlidingWindowRateLimiter`,
  `_extract_bearer_token`, `_format_peer_name`, `_is_numeric_ddos_limit`,
  `DDOS_BLOCK_MESSAGE`, `WS_GUID`)
* `_SlidingWindowRateLimiter` keeps its 0.x positional signature
* every name in `__all__` is importable

The old `yserver` import path is **not** among them, and is not retained:
`import yserver` raises `ModuleNotFoundError` under 1.0. That is the
intentional breaking change described at the top of this document.
