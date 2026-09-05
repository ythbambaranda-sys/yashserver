# TLS

TLS is available on every TCP-based transport: TCP, HTTP (`https://`) and
WebSocket (`wss://`).

It is **not** available on UDP. DTLS is not in Python's standard library, so
`YUdpServer` has no `ssl_context` parameter rather than a broken one. Encrypt
UDP payloads at the application layer, or carry sensitive traffic over TCP.

## Basic use

```python
import yashserver

tls = yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem")

http = yashserver.YHttpServer(port=8443, tls=tls)
ws   = yashserver.YWebSocketServer(port=9443, tls=tls)
tcp  = yashserver.YTcpServer(port=9000, tls=tls)

yashserver.run_many(http, ws, tcp)
```

One `TLSConfig` can be shared; each server builds its own `SSLContext`.

## Secure defaults

`TLSConfig` applies these without being asked:

* **TLS 1.2 minimum.** SSLv3, TLS 1.0 and TLS 1.1 are off.
* **Compression disabled** (`OP_NO_COMPRESSION`), which closes off CRIME.
* **No ephemeral key reuse** (`OP_SINGLE_DH_USE`, `OP_SINGLE_ECDH_USE`).
* **Python's default cipher suite**, which excludes the known-weak ones.

Misconfiguration fails loudly at construction with `ConfigError`, rather than
starting a server that is quietly insecure:

```python
yashserver.TLSConfig(certfile="missing.pem", keyfile="missing.pem").create_server_context()
# ConfigError: could not load TLS certificate/key: ...

yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem", minimum_version="1.9")
# ConfigError: unsupported TLS version: '1.9' (use one of 1.0, 1.1, 1.2, 1.3)
```

## Options

```python
yashserver.TLSConfig(
    certfile="cert.pem",
    keyfile="key.pem",
    password=None,               # for an encrypted private key
    cafile=None,                 # CA bundle for verifying client certificates
    capath=None,
    require_client_cert=False,   # mutual TLS
    minimum_version="1.2",       # "1.0" | "1.1" | "1.2" | "1.3"
    maximum_version=None,
    ciphers=None,                # OpenSSL cipher string
    alpn_protocols=None,         # e.g. ["http/1.1"]
)
```

### TLS 1.3 only

```python
tls = yashserver.TLSConfig(certfile="cert.pem", keyfile="key.pem", minimum_version="1.3")
```

Modern clients only, and the simplest thing to reason about — worth doing for
an internal service where you control both ends.

### Mutual TLS

Common for internal microservices, where both sides prove who they are:

```python
tls = yashserver.TLSConfig(
    certfile="server.pem",
    keyfile="server.key",
    cafile="ca.pem",
    require_client_cert=True,
)
```

`require_client_cert=True` without a `cafile` or `capath` raises `ConfigError`,
because a server that demands certificates it cannot verify is worse than one
that never asked. Read the verified certificate in a handler:

```python
@server.on_connection
async def handle(connection, srv):
    certificate = connection.peer_certificate()
    subject = dict(item[0] for item in certificate["subject"])
    ...
```

Over HTTP, the same is reachable through the underlying socket, and
`request.is_secure` tells you the request arrived over TLS.

## Bringing your own context

If you need something `TLSConfig` does not express, pass a plain
`ssl.SSLContext`:

```python
import ssl

context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
context.load_cert_chain("cert.pem", "key.pem")
context.set_alpn_protocols(["http/1.1"])

app = yashserver.YHttpServer(port=8443, ssl_context=context)
```

Passing both `ssl_context` and `tls` raises `ConfigError` rather than silently
preferring one.

The helper carried over from 0.x is unchanged:

```python
context = yashserver.ServerTools.create_server_ssl_context("cert.pem", "key.pem")
```

## Certificates for development

The repository deliberately ships no certificate or private key. A key in
version control cannot be rotated, trips secret scanners, and is a bad habit
even when it is only valid for `localhost`. Generate your own:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout tls/key.pem -out tls/cert.pem -subj "/CN=localhost"
```

`.gitignore` excludes `*.pem` and `*.key`, so a certificate generated here
will not be committed by accident. The TLS tests generate their own
throwaway pair at runtime and skip if `openssl` is unavailable.

Browsers will warn about a self-signed certificate; that is expected. For
anything public, use a real certificate (Let's Encrypt or your organisation's
CA).

## Terminating TLS elsewhere

Running behind nginx, Caddy or a cloud load balancer that terminates TLS is
perfectly reasonable — run yashserver plain on localhost in that case. If you do,
remember that `request.remote_addr` is then the proxy's address; read the
forwarded header your proxy sets, and only trust it because you control that
proxy:

```python
@app.middleware
async def real_ip(request, call_next):
    forwarded = request.header("x-forwarded-for")
    if forwarded:
        request.state["client_ip"] = forwarded.split(",")[0].strip()
    return await call_next(request)
```

Rate limiting keys off `remote_addr`, so behind a proxy every request looks
like one client. Either rate limit at the proxy, or key your own limiter off
the forwarded address you trust.
