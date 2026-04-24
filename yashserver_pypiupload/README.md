# yashserver

`yashserver` is an independent Python server library focused on simple APIs:
- `YServer` for line-based TCP command servers
- `YWebSocketServer` for WebSocket routes
- `YHttpServer` for lightweight HTML/API serving
- `YSyncServer`, `YSyncWebSocketServer`, `YSyncHttpServer` for non-async usage
- Built-in TLS (`ssl_context`) support
- Optional token auth and sliding-window rate limiting
- Plugin hooks for lifecycle, traffic, and error handling

Core networking is standard library based. Database connectors are optional and loaded only when used.

## Install

```bash
pip install -e .
```

From PyPI:

```bash
pip install yashserver
```

For database drivers (optional):

```bash
pip install -e ".[db]"
```

## Task 1 Demo (Browser + HTML)

Run:

```bash
python examples/task1_browser_server.py
```

Then open:

`http://127.0.0.1:8080`

You will see a small HTML page served by `YHttpServer`.
Live chat is enabled by `YWebSocketServer` with no external dependencies.
The page code is in `test.html`, served on `/` and `/test.html`.

Demo highlights:
- Chunked **binary** file upload (no base64 transfer path), including videos
- ACK/retry chunk handling in the browser client
- Server-side file storage + `/download/<id>` links
- Optional auth token via env var `YSERVER_TOKEN`
- Optional MongoDB text-message persistence

Mongo text persistence env vars for `examples/task1_browser_server.py`:
- `YSERVER_MONGO_TEXT_ENABLED` (`1` or `0`)
- `YSERVER_MONGO_URI` (example: `mongodb://localhost:27018`)
- `YSERVER_MONGO_DB` (default: `yserver_chat`)
- `YSERVER_MONGO_COLLECTION` (default: `messages`)
- `YSERVER_MONGO_GRIDFS_BUCKET` (default: `fs`, creates `<bucket>.files` + `<bucket>.chunks`)
- `YSERVER_MONGO_GRIDFS_CHUNK_SIZE` (default: `524288` bytes)
- `YSERVER_MONGO_WRITE_CONCERN` (default: `1` for fast local visibility in Compass)
- `YSERVER_MONGO_JOURNAL` (`1` or `0`, default: `0`)
- or set `YSERVER_MONGO_HOST` + `YSERVER_MONGO_PORT` instead of a full URI

## Minimal Sync Example

```python
import yserver

app = yserver.YSyncServer(port=9000)

@app.route("ping")
def ping(client, payload, server):
    return {"reply": "pong"}

app.run()
```

You can use a single import and access all public helpers from `yserver.*`, for example:
`yserver.ConnectionStatsPlugin`, `yserver.LoggingPlugin`, `yserver.ServerTools`,
`yserver.YSyncHttpServer`, `yserver.YSyncWebSocketServer`, and `yserver.run_many`.

## Database Support

Single import API:

```python
import yserver

db = yserver.connect_database("sqlite", database=":memory:")
db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
print(db.fetch_all("SELECT * FROM users"))
db.close()
```

Also available through tools:

```python
db = yserver.ServerTools.connect_database("sqlite", database="app.db")
print(yserver.ServerTools.supported_databases())
```

Supported backends:
- MySQL
- PostgreSQL
- Microsoft SQL Server
- Oracle Database
- SQLite
- MariaDB
- MongoDB
- Redis
- Cassandra
- DynamoDB
- Firebase Realtime Database
- Couchbase
- Snowflake
- Google BigQuery
- Amazon Redshift
- ClickHouse
- Elasticsearch
- Neo4j
- InfluxDB
- DuckDB

Quick DB demo script:

```bash
python examples/task2_database_support.py
```

MongoDB Compass quick test (default local port `27018`):

```bash
python examples/task3_mongodb_compass_test.py
```

Compass connection string:

`mongodb://localhost:27018`

Notes:
- SQL backends use SQLAlchemy (`url=` or DSN-style config).
- SQLite is supported via stdlib `sqlite3`.
- Non-SQL backends use their ecosystem drivers (optional install).

## TLS / HTTPS

```python
import yserver

ssl_ctx = yserver.ServerTools.create_server_ssl_context(
    certfile="cert.pem",
    keyfile="key.pem",
)

http = yserver.YSyncHttpServer(port=8443, ssl_context=ssl_ctx)
ws = yserver.YSyncWebSocketServer(port=9443, ssl_context=ssl_ctx)
```

For the demo script (`examples/task1_browser_server.py`), you can enable TLS by env vars:

```bash
set YSERVER_TLS_CERT=cert.pem
set YSERVER_TLS_KEY=key.pem
python examples/task1_browser_server.py
```

If `tls/cert.pem` and `tls/key.pem` exist in the project root, the demo auto-enables HTTPS/WSS.

## Auth + Rate Limit

```python
import yserver

ws = yserver.YSyncWebSocketServer(
    auth_token="my-secret-token",
    rate_limit_per_window=300,
    rate_limit_window_seconds=60.0,
)

http = yserver.YSyncHttpServer(
    auth_token="my-secret-token",
    rate_limit_per_window=800,
    rate_limit_window_seconds=60.0,
)
```

Token can be provided as:
- Query param: `?token=...`
- Header: `x-yserver-token: ...`
- Header: `Authorization: Bearer ...`

## Test Suite

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
