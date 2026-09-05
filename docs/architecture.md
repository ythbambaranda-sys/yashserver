# Architecture

yashserver 1.0 supports four transports. The design problem is that they are
genuinely different, and pretending otherwise produces an API that is wrong
for all of them. So the split is deliberate:

**Shared, because it is the same problem everywhere:** server lifecycle,
start/stop, authentication, rate limiting, logging, plugins, metrics,
configuration, background tasks, and graceful shutdown.

**Not shared, because it is not the same thing:** connections, datagrams,
requests, and messages.

```
                         ┌──────────────────────────┐
                         │      yashserver.core        │
                         │                          │
                         │  BaseServer              │  lifecycle, state machine
                         │  ServerConfig            │  configuration
                         │  AuthConfig              │  tokens, validators
                         │  RateLimitConfig         │  sliding window
                         │  SlidingWindowRateLimiter│  bounded memory
                         │  TLSConfig               │  secure defaults
                         │  Metrics                 │  counters/gauges/summaries
                         └────────────┬─────────────┘
                                      │
        ┌──────────────┬──────────────┼──────────────┬──────────────┐
        │              │              │              │              │
 ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────────┐   │
 │ yashserver.tcp │ │yashserver.udp │ │yashserver.http│ │yashserver.websocket│  │
 │             │ │            │ │            │ │                │   │
 │YTcpServer   │ │YUdpServer  │ │YHttpServer │ │YWebSocketServer│   │
 │TcpConnection│ │UdpEndpoint │ │HttpRequest │ │WebSocketConn.  │   │
 │             │ │UdpDatagram │ │HttpResponse│ │rooms, broadcast│   │
 │connections, │ │endpoints,  │ │requests,   │ │messages,       │   │
 │streams      │ │datagrams   │ │routes,     │ │rooms           │   │
 │             │ │            │ │middleware  │ │                │   │
 └─────────────┘ └────────────┘ └─────┬──────┘ └───────┬────────┘   │
                                      │                │            │
                                      └── mount_websocket ──────────┘
                                          (one port, one certificate)
```

`yashserver.server` remains as a facade re-exporting everything, so the 0.x
*module layout* is preserved. The top-level import name changed in 1.0,
though: `import yserver` becomes `import yashserver`. See
[migration.md](migration.md).

## What `BaseServer` provides

`BaseServer` knows about lifecycle and cross-cutting concerns, and nothing
about wire formats. It has no concept of a connection, which is precisely what
lets UDP inherit from it without lying.

```python
await server.start()          # bind, then fire on_startup
await server.run()            # start, serve until cancelled, then stop
await server.stop()           # stop accepting, drain, then close

async with server:            # start/stop as a context manager
    ...

server.state                  # STOPPED | STARTING | RUNNING | STOPPING
server.bound_port             # the real port, even when you asked for 0
server.metrics.snapshot()     # counters, gauges, summaries
server.add_plugin(plugin)
server.register_tool("name", fn)
server.every(5.0, callback)   # periodic work, cancelled on stop
```

Subclasses implement only `_start_impl`, `_stop_impl` and (optionally)
`_serve_impl`.

## Where the APIs deliberately diverge

| Concept | TCP | UDP | HTTP | WebSocket |
|---|---|---|---|---|
| Peer | `TcpConnection` | `UdpEndpoint` (a value, not a session) | `HttpRequest.remote_addr` | `WebSocketConnection` |
| Registry | `server.clients` | `server.known_endpoints()` | none — requests are transient | `server.clients` |
| Disconnect | `connection.close()` | *does not exist* | `Connection: close` | `session.close(code, reason)` |
| Delivery | ordered, reliable | best effort | request/response | ordered, reliable |
| Fan-out | `broadcast()` | `broadcast()` to recently seen addresses | n/a | `broadcast()`, `broadcast_to_room()` |
| Grouping | none | none | routes | rooms |

The UDP server has **no** `clients` dict, **no** `disconnect()` and **no**
per-peer close, because none of those exist in UDP. `known_endpoints()` means
"addresses we heard from recently", and the name says exactly that much.

## Graceful shutdown

`stop()` runs the same three phases everywhere:

1. **Stop accepting.** The listener closes immediately, so no new work arrives.
2. **Drain.** Handlers that are *mid-work* get up to `shutdown_drain_seconds`
   to finish. Connections merely sitting idle — a kept-alive HTTP connection
   between requests, a TCP peer waiting for its next command — have nothing to
   drain and are not waited on.
3. **Close.** Everything still open is closed; WebSocket peers get a
   `1001 going away` close frame first, rather than a socket that just vanishes.

Anything still stuck past the deadline is cancelled, then the transport is
aborted. Shutdown is bounded even when a peer is wedged.

## Backpressure

Backpressure is real on the stream transports, and honest about being absent
on UDP:

* **TCP and WebSocket** set asyncio write-buffer watermarks, so `drain()`
  blocks once too much is outstanding. Every write also carries a timeout: a
  peer that stops reading is disconnected rather than allowed to grow an
  unbounded buffer in your process. Broadcasts run concurrently so one slow
  peer cannot stall the rest.
* **HTTP** uses the same watermarks, streams response bodies chunk by chunk,
  and streams request bodies on routes declared with `stream=True`.
* **UDP** has no flow control to apply. When the OS send buffer fills, the
  configured `backpressure_policy` either drops (the default, and the honest
  UDP answer) or waits briefly. Under receive overload, datagrams past
  `max_concurrent_handlers` are dropped rather than queued, because delivering
  a stale datagram late is worse than not delivering it.

## Bounded memory

Every unbounded thing an untrusted peer could grow has a cap:

| Risk | Bound |
|---|---|
| Rate-limiter buckets per source IP | `max_tracked_keys`, plus stale-key sweeping |
| UDP peers from spoofed sources | `max_tracked_peers`, plus idle expiry |
| TCP message with no delimiter | `max_line_bytes` |
| HTTP request headers | `max_header_bytes` → `431` |
| HTTP request body | `max_body_bytes` → `413`, or stream it |
| WebSocket message / frame | `max_message_size_bytes` → close `1009` |
| Concurrent connections | `max_connections` |
| In-flight UDP handlers | `max_concurrent_handlers` |

## Testing

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

269 tests covering the core abstractions, each transport's protocol
behaviour, the reliability helper, archives and folder transfer, resumable
uploads, and cross-protocol lifecycle.

```bash
python examples/benchmark.py
```
