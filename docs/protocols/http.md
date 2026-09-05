# HTTP

```python
import yashserver

app = yashserver.YHttpServer(host="0.0.0.0", port=8080)

@app.get("/api/users/{user_id}")
async def user(request, server):
    return {"id": request.param("user_id")}

yashserver.run_many(app)
```

HTTP/1.1 with keep-alive, path parameters, middleware, streaming in both
directions, range-aware file transfers, authentication and rate limiting.

## Responses

A handler may return whatever is most natural:

| return                         | becomes                          |
| ------------------------------ | -------------------------------- |
| `None`                         | `204 No Content`                 |
| `"text"`                       | `200` `text/plain`               |
| `b"bytes"`                     | `200` `application/octet-stream` |
| `{"a": 1}`, lists, dataclasses | `200` `application/json`         |
| `(status, body)`               | that status                      |
| `(status, body, headers)`      | that status with extra headers   |
| `HttpResponse(...)`            | used as-is                       |

```python
@app.get("/json")
async def json_route(request, server):
    return {"ok": True}

@app.post("/create")
async def create(request, server):
    return 201, {"id": 7}, {"Location": "/things/7"}

@app.get("/page")
async def page(request, server):
    return yashserver.HttpResponse.html_response("<h1>hi</h1>")

@app.get("/away")
async def away(request, server):
    return yashserver.HttpResponse.redirect("/elsewhere")
```

## Routing

```python
@app.get("/health")                       # exact
@app.get("/users/{uid}")                  # one segment
@app.get("/users/{uid}/posts/{pid}")      # several
@app.get("/files/{path:path}")            # rest of the path, slashes included
@app.route("/any", method="ANY")          # any method
@app.route("/legacy", method="GET")       # explicit
```

`get`, `post`, `put`, `patch`, `delete`, `head` and `options` are all
available. Parameters are percent-decoded and reachable as
`request.param("uid")` or `request.path_params["uid"]`.

* A path that exists but not for that method returns **405** with an `Allow`
  header, not 404.
* `HEAD` falls back to the `GET` route automatically, returning headers with
  the `Content-Length` `GET` would have sent, and no body.

## Requests

```python
@app.post("/thing")
async def thing(request, server):
    request.method            # "POST"
    request.path              # "/thing"
    request.path_params       # {"id": "7"}
    request.query("q", "")    # first value of ?q=
    request.query_params      # {"q": ["a", "b"]}
    request.header("accept")  # case-insensitive; repeats joined with ", "
    request.body              # bytes (buffered routes)
    request.text()            # decoded
    request.json()            # parsed, or None
    request.form()            # urlencoded form
    request.content_type
    request.is_secure         # served over TLS
    request.remote_addr
    request.id                # unique per request
    request.state             # dict for middleware to pass things down
```

## Streaming

**Response** — return an async iterator, or use `stream_response`. Chunked
transfer encoding is used automatically unless you set a `Content-Length`.

```python
@app.get("/events")
async def events(request, server):
    async def produce():
        for index in range(1000):
            yield f"data: {index}\n\n".encode()
            await asyncio.sleep(1)
    return yashserver.HttpResponse.stream_response(produce(), content_type="text/event-stream")
```

**Request** — declare the route with `stream=True` and the body is never
buffered, so upload size is bounded by your disk rather than by memory.

```python
@app.post("/upload/{name}", stream=True)
async def upload(request, server):
    total = 0
    with open(request.param("name"), "wb") as handle:
        async for chunk in request.stream(chunk_size=256 * 1024):
            handle.write(chunk)
            total += len(chunk)
    return {"bytes": total}
```

Buffered routes read the body up front and reject anything over
`max_body_bytes` with **413**. `Expect: 100-continue` is answered before the
body is read. `Transfer-Encoding: chunked` request bodies are decoded on both
paths.

## Files and large transfers

```python
app.static("/assets", "./public")     # ranges, ETag, traversal protection

@app.get("/video/{name}")
async def video(request, server):
    return yashserver.file_response(f"./media/{request.param('name')}", request)
```

`file_response` reads the file in `stream_chunk_size` pieces, so a 4 GB video
costs one chunk of memory. It handles:

* `Range: bytes=0-1023` → **206** with `Content-Range` (and `bytes=-500` for a
  suffix); an unsatisfiable range → **416**
* `ETag` and `Last-Modified`, with `If-None-Match` / `If-Modified-Since` → **304**
* `Content-Type` guessed from the extension
* `download_name=` to force a save dialog

`app.static()` resolves every path and confirms it stays inside the served
directory, so `../` cannot escape.

## Middleware

Registered outermost first; each receives the request and the next handler.

```python
@app.middleware
async def timing(request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    return response.set_header("X-Elapsed-Ms", f"{(time.perf_counter()-started)*1000:.2f}")

@app.middleware
async def require_admin(request, call_next):
    if request.path.startswith("/admin") and not is_admin(request):
        raise yashserver.HttpError(403, "admins only")
    request.state["user"] = lookup_user(request)
    return await call_next(request)
```

Middleware can short-circuit by returning a response or raising `HttpError`,
and can pass data to handlers through `request.state`.

## Errors

```python
raise yashserver.HttpError(404, "no such book")
raise yashserver.HttpError(429, "slow down", headers={"Retry-After": "30"})
```

`HttpError` becomes a JSON body carrying `error`, `detail` and `status`. Any
other exception becomes **500** and is reported to `on_error` plugin hooks.
Override the rendering per status:

```python
@app.error_handler(404)
async def not_found(request, error):
    return 404, {"error": "not_found", "path": request.path}
```

Malformed requests are handled properly rather than crashing the connection:
bad request line or `Content-Length` → **400**, oversized headers → **431**,
no request within the timeout → **408**, over the connection cap → **503**.

## Keep-alive

Connections are reused by default. An unread request body is drained before the
next request is parsed, so a handler that ignores its body cannot desynchronise
the connection.

```python
yashserver.YHttpServer(
    keep_alive=True,
    keep_alive_timeout_seconds=5.0,      # idle time allowed between requests
    max_requests_per_connection=100,
)
```

## Auth and rate limiting

```python
app = yashserver.YHttpServer(
    auth_token="secret",
    auth_exempt_paths={"/health", "/metrics"},
    rate_limit_per_window=600,
    rate_limit_window_seconds=60.0,
    ddosprot=True,
)
```

Tokens are accepted as `?token=`, `X-Yserver-Token:` or
`Authorization: Bearer`, and compared in constant time. For anything richer,
supply a validator:

```python
async def validate(context):
    return await check_jwt(context["token"], context["path"])

app = yashserver.YHttpServer(auth=yashserver.AuthConfig(validator=validate))
```

Rate-limited requests get **429** with `Retry-After`.

## Mounting a WebSocket on the same port

```python
ws = yashserver.YWebSocketServer(port=9001)

@ws.route("/ws")
async def socket(session, message, server):
    return {"echo": message}

app.mount_websocket(ws, "/ws")   # now ws://host:8080/ws works
```

A page served from `http://host:8080` can open `ws://host:8080/ws` — one
origin, one certificate, no CORS.

## Configuration

```python
yashserver.YHttpServer(
    host="0.0.0.0",
    port=8080,
    tls=yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem"),
    keep_alive=True,
    keep_alive_timeout_seconds=5.0,
    max_requests_per_connection=100,
    max_header_bytes=16 * 1024,
    max_body_bytes=16 * 1024 * 1024,
    request_timeout_seconds=60.0,
    write_timeout_seconds=60.0,
    max_connections=10_000,
    stream_chunk_size=64 * 1024,
    server_header="yashserver",
    shutdown_drain_seconds=5.0,
)
```

## Metrics

```python
app.metrics.snapshot()
# counters: requests, status_2xx/4xx/5xx, connections_opened/closed/refused,
#           unauthorized, rate_limited, request_timeouts, write_timeouts,
#           websocket_upgrades, bytes_sent, errors
# summaries: request_seconds
```
