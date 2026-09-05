# UDP

```python
import yashserver

server = yashserver.YUdpServer(host="0.0.0.0", port=9002, max_packet_size=1200)

@server.route("ping")
async def ping(endpoint, payload, srv):
    return {"pong": payload, "you_are": endpoint.key}

yashserver.run_many(server)
```

## What UDP does not give you

This is the important part, so it comes first. yashserver does **not** paper over
any of it:

* **No delivery guarantee.** A datagram you send may never arrive. `send_to()`
  returning `True` means the kernel accepted the bytes — nothing more.
* **No ordering.** Datagrams can arrive in a different order than sent.
* **No de-duplication.** A datagram can arrive more than once.
* **No connections.** There is no accept, no disconnect, and no way to learn
  that a peer has gone. Silence is the only signal, and silence is ambiguous.
* **No TLS.** DTLS is not in Python's standard library, so `YUdpServer` has no
  `ssl_context`. Encrypt payloads at the application layer, or use TCP.

If you need delivery guarantees for *everything*, use TCP. If you need them for
*some* messages, see [Opt-in reliability](#opt-in-reliability) below — that is
usually the right answer for games and telemetry.

## Endpoints, not connections

The API models remote addresses as values:

```python
@server.route("hello")
async def hello(endpoint, payload, srv):
    endpoint.host      # "203.0.113.7"
    endpoint.port      # 51234
    endpoint.key       # "203.0.113.7:51234"  (or "[::1]:51234" for IPv6)
    endpoint.is_ipv6
```

`UdpEndpoint` is frozen and hashable, so it works directly as a dict key for
your own per-peer state:

```python
scores: dict[UdpEndpoint, int] = {}
scores[endpoint] = scores.get(endpoint, 0) + 1
```

`server.known_endpoints()` lists addresses heard from recently. It is **not** a
connection list: an entry means "this address sent us something within
`peer_idle_seconds`", and nothing about whether anyone is still there. Entries
expire on their own and are capped by `max_tracked_peers` so a spoofed-source
flood cannot exhaust memory.

## Two handler styles

Command routing, matching the TCP server:

```python
@server.route("move")
async def move(endpoint, payload, srv):
    ...

@server.route("*")          # fallback for unmatched commands
async def other(endpoint, payload, srv):
    ...
```

Raw datagrams, for binary protocols:

```python
@server.on_datagram
async def handle(datagram, srv):
    datagram.data          # bytes, exactly as received
    datagram.endpoint      # UdpEndpoint
    datagram.text()        # decoded
    datagram.json()        # parsed, or None
    len(datagram)          # size in bytes
    return b"reply"        # optional; sent back best effort
```

A raw handler takes precedence over routes. Returning a value from either
sends a reply to the sender — with no guarantee it arrives.

## Sending

```python
await server.send_to(endpoint, {"tick": 1})     # dict → JSON
await server.send_to(endpoint, b"raw bytes")
await server.send_to(("10.0.0.5", 9999), "text")

sent = await server.broadcast({"news": "hi"})   # fan out to known endpoints
```

`broadcast()` is a fan-out to recently seen addresses, not an IP broadcast. For
a real IP broadcast, set `allow_broadcast=True` and send to the broadcast
address directly.

## Packet size

```python
server = yashserver.YUdpServer(port=9002, max_packet_size=1200)
```

`max_packet_size` is enforced in both directions: larger incoming datagrams are
dropped (counted as `dropped_oversized`), and an oversized `send_to()` raises
`ValueError` rather than silently truncating.

**Pick a value under the path MTU.** 65507 is the IPv4 payload ceiling, but
anything over roughly 1200 bytes on the open internet will be IP-fragmented,
and a fragmented datagram is lost entirely if any single fragment is lost. 1200
is a safe default; on a controlled LAN you can go higher.

## IPv4 and IPv6

```python
yashserver.YUdpServer(host="0.0.0.0")               # IPv4
yashserver.YUdpServer(host="::")                    # IPv6, dual-stack by default
yashserver.YUdpServer(host="::", dual_stack=False)  # IPv6 only
yashserver.YUdpServer(host="0.0.0.0", family="ipv4")
```

`family="auto"` (the default) picks from the host string. Dual stack lets one
IPv6 socket also serve IPv4-mapped clients where the OS supports it.

## Loss, reordering and overload

The server does not hide any of these — it counts them, so you can see what
your network is doing:

```python
server.metrics.snapshot()["counters"]
# datagrams_received, datagrams_sent, bytes_received, bytes_sent,
# dropped_oversized, dropped_rate_limited, dropped_overload,
# dropped_by_plugin, send_errors, send_dropped_backpressure, transport_errors
```

`dropped_overload` means datagrams arrived faster than handlers could run and
were dropped past `max_concurrent_handlers`. Queueing them would only deliver
stale data late, so dropping is the correct UDP behaviour — but a rising
counter means you are over capacity.

`transport_errors` typically means ICMP port-unreachable for something you
sent. It is normal on UDP and never fatal.

## Opt-in reliability

`ReliableUdpChannel` adds at-least-once delivery, de-duplication and optional
ordering **on top of** UDP. It is a helper, not a transport, and it is off by
default.

```python
from yashserver import ReliableUdpChannel

channel = ReliableUdpChannel(
    server,
    retry_interval_seconds=0.25,
    max_retries=5,
    ordered=False,          # True to deliver in sequence
    reorder_window=64,      # datagrams buffered behind a gap
    reorder_timeout_seconds=1.0,
)
server.on_datagram(channel.handle_datagram)
channel.start()

@channel.on_message
async def handle(payload: bytes, endpoint):
    ...

@channel.on_delivery_failed
async def gave_up(payload: bytes, endpoint):
    ...  # unacknowledged past max_retries

await channel.send(endpoint, b"this one matters")
```

**What it gives you:** a 9-byte header (`YRL` + version + type + 32-bit
sequence), acknowledgement of every DATA frame, retransmission with exponential
backoff, duplicate suppression per endpoint, and — with `ordered=True` —
in-sequence delivery.

**What it does not give you:** congestion control, flow control, or connection
establishment. If you want those, you want TCP.

Two details worth knowing:

* Ordered mode expects a stream to start at sequence 1. If a gap does not
  close within `reorder_window` datagrams or `reorder_timeout_seconds`, the
  channel skips it and resumes rather than stalling forever
  (`reliable_reorder_gaps` counts this).
* The header costs 9 bytes, so the largest reliable payload is
  `max_packet_size - 9`. Oversized sends raise rather than fragment.

### Mixing reliable and unreliable traffic

This is the pattern most games want, and it is why the helper is opt-in per
message rather than per server:

```python
@server.on_datagram
async def dispatch(datagram, srv):
    if datagram.data.startswith(b"YRL"):
        await channel.handle_datagram(datagram, srv)   # join, chat, scores
        return None
    handle_position_update(datagram.json())            # superseded next tick
    return None
```

See `examples/udp_game_server.py` for a complete working version.

## Configuration

```python
yashserver.YUdpServer(
    host="0.0.0.0",
    port=9002,
    max_packet_size=1200,
    family="auto",                 # "auto" | "ipv4" | "ipv6"
    dual_stack=True,
    allow_broadcast=False,
    reuse_port=False,              # SO_REUSEPORT where supported
    peer_idle_seconds=300.0,
    max_tracked_peers=50_000,
    max_concurrent_handlers=2_000,
    backpressure_policy="drop",    # "drop" | "wait"
    send_wait_seconds=1.0,
    rate_limit_per_window=2000,
    rate_limit_window_seconds=60.0,
    ddosprot=True,
)
```

Unknown options raise `ConfigError` at construction rather than being silently
ignored.

## Plugin hooks

```python
class Filter(yashserver.ServerPlugin):
    async def on_udp_datagram(self, datagram, server):
        return None if is_junk(datagram) else datagram   # None drops it

    async def on_udp_endpoint_seen(self, endpoint, server): ...
    async def on_udp_endpoint_expired(self, endpoint, server): ...
```
