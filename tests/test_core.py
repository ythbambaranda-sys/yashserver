from __future__ import annotations

import atexit
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from yashserver.core import (  # noqa: E402
    AuthConfig,
    ConfigError,
    Metrics,
    RateLimitConfig,
    ServerConfig,
    SlidingWindowRateLimiter,
    TLSConfig,
    extract_bearer_token,
    format_peer_name,
)

_TLS_PAIR: list = []


def temporary_tls_pair() -> tuple[Path, Path] | None:
    """Generate a throwaway self-signed cert and key for this test session.

    No static private key is kept in the repository. Committing one trips
    secret scanners and is a poor habit even when the key is only ever valid
    for localhost -- and a key in version control cannot be rotated. The pair
    is written to a temporary directory that is removed when the process
    exits, and is generated once per session because RSA keygen is slow.

    Returns ``None`` when no generator is available, in which case the TLS
    tests skip rather than fail.
    """

    if _TLS_PAIR:
        return _TLS_PAIR[0]

    result: tuple[Path, Path] | None = None
    openssl = shutil.which("openssl")
    if openssl is not None:
        holder = tempfile.TemporaryDirectory(prefix="yashserver-tls-")
        atexit.register(holder.cleanup)
        certfile = Path(holder.name) / "cert.pem"
        keyfile = Path(holder.name) / "key.pem"
        completed = subprocess.run(
            [
                openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(keyfile), "-out", str(certfile),
                "-days", "1", "-subj", "/CN=localhost",
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0 and certfile.is_file() and keyfile.is_file():
            result = (certfile, keyfile)

    _TLS_PAIR.append(result)
    return result


class TestRateLimiter(unittest.TestCase):
    def test_allows_up_to_limit_then_blocks(self) -> None:
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=3, window_seconds=60))
        self.assertEqual([limiter.allow("a") for _ in range(5)], [True, True, True, False, False])
        self.assertEqual(limiter.remaining("a"), 0)
        self.assertGreater(limiter.retry_after_seconds("a"), 0)

    def test_disabled_when_limit_is_none_or_zero(self) -> None:
        for limit in (None, 0, -5):
            limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=limit))
            self.assertTrue(all(limiter.allow("k") for _ in range(100)))
            self.assertIsNone(limiter.remaining("k"))
            self.assertEqual(limiter.retry_after_seconds("k"), 0)

    def test_window_expiry_frees_capacity(self) -> None:
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=1, window_seconds=0.05))
        self.assertTrue(limiter.allow("a"))
        self.assertFalse(limiter.allow("a"))
        time.sleep(0.08)
        self.assertTrue(limiter.allow("a"))

    def test_keys_are_independent(self) -> None:
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=1, window_seconds=60))
        self.assertTrue(limiter.allow("a"))
        self.assertTrue(limiter.allow("b"))
        self.assertFalse(limiter.allow("a"))

    def test_tracked_keys_are_capped(self) -> None:
        # An internet-facing limiter must not grow one bucket per source IP
        # forever; the oldest key is evicted once the cap is reached.
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=10, window_seconds=60, max_tracked_keys=8))
        for index in range(200):
            limiter.allow(f"host-{index}")
        self.assertLessEqual(limiter.tracked_keys(), 8)

    def test_stale_keys_are_swept(self) -> None:
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=5, window_seconds=0.01))
        burst = SlidingWindowRateLimiter.SWEEP_INTERVAL
        for index in range(burst):
            limiter.allow(f"host-{index}")
        self.assertEqual(limiter.tracked_keys(), burst)

        # Once those keys age past the window, the next sweep discards them.
        time.sleep(0.05)
        for _ in range(burst):
            limiter.allow("still-here")
        self.assertEqual(limiter.tracked_keys(), 1)

    def test_reconfigure_resets_state(self) -> None:
        limiter = SlidingWindowRateLimiter(RateLimitConfig(limit=1, window_seconds=60))
        self.assertTrue(limiter.allow("a"))
        self.assertFalse(limiter.allow("a"))
        limiter.reconfigure(RateLimitConfig(limit=5, window_seconds=60))
        self.assertTrue(limiter.allow("a"))
        self.assertEqual(limiter.limit, 5)


class TestAuthConfig(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_auth_allows_everything(self) -> None:
        auth = AuthConfig()
        self.assertFalse(auth.enabled)
        self.assertTrue(await auth.authorize(headers={}, query_params={}))

    async def test_accepts_query_header_and_bearer(self) -> None:
        auth = AuthConfig(token="s3cret")
        self.assertTrue(await auth.authorize(query_params={"token": ["s3cret"]}))
        self.assertTrue(await auth.authorize(headers={"x-yserver-token": "s3cret"}))
        self.assertTrue(await auth.authorize(headers={"authorization": "Bearer s3cret"}))
        self.assertFalse(await auth.authorize(headers={"authorization": "Bearer wrong"}))
        self.assertFalse(await auth.authorize(headers={}))

    async def test_multiple_tokens_and_rotation(self) -> None:
        auth = AuthConfig(token="new", tokens=("old",))
        self.assertTrue(await auth.authorize(headers={"x-yserver-token": "new"}))
        self.assertTrue(await auth.authorize(headers={"x-yserver-token": "old"}))
        self.assertFalse(await auth.authorize(headers={"x-yserver-token": "other"}))

    async def test_exempt_paths_skip_auth(self) -> None:
        auth = AuthConfig(token="s3cret", exempt_paths={"/health"})
        self.assertTrue(await auth.authorize(path="/health"))
        self.assertFalse(await auth.authorize(path="/private"))

    async def test_custom_validator_replaces_token_check(self) -> None:
        async def validator(context: dict) -> bool:
            return context["token"] == "abc" and context["path"] == "/ok"

        auth = AuthConfig(validator=validator)
        self.assertTrue(auth.enabled)
        self.assertTrue(await auth.authorize(headers={"x-yserver-token": "abc"}, path="/ok"))
        self.assertFalse(await auth.authorize(headers={"x-yserver-token": "abc"}, path="/no"))

    def test_bearer_extraction(self) -> None:
        self.assertEqual(extract_bearer_token("Bearer abc"), "abc")
        self.assertEqual(extract_bearer_token("bearer  abc  "), "abc")
        self.assertIsNone(extract_bearer_token("Basic abc"))
        self.assertIsNone(extract_bearer_token("Bearer "))
        self.assertIsNone(extract_bearer_token(None))


@unittest.skipUnless(
    temporary_tls_pair() is not None,
    "no openssl available to generate a temporary test certificate",
)
class TestTLSConfig(unittest.TestCase):
    def _config(self, **kwargs) -> TLSConfig:
        pair = temporary_tls_pair()
        assert pair is not None  # guarded by the class-level skip
        certfile, keyfile = pair
        return TLSConfig(certfile=str(certfile), keyfile=str(keyfile), **kwargs)

    def test_secure_defaults(self) -> None:
        context = self._config().create_server_context()
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
        self.assertTrue(context.options & ssl.OP_NO_COMPRESSION)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)

    def test_explicit_versions(self) -> None:
        context = self._config(minimum_version="1.3").create_server_context()
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_3)

    def test_unknown_version_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            self._config(minimum_version="1.9").create_server_context()

    def test_client_certs_require_a_ca(self) -> None:
        with self.assertRaises(ConfigError):
            self._config(require_client_cert=True).create_server_context()

    def test_missing_certificate_is_reported_clearly(self) -> None:
        with self.assertRaises(ConfigError):
            TLSConfig(certfile="does-not-exist.pem", keyfile="nope.pem").create_server_context()

    def test_alpn_protocols_are_set(self) -> None:
        # Should not raise; ALPN has no public getter to assert against.
        self._config(alpn_protocols=["http/1.1"]).create_server_context()


class TestMetrics(unittest.TestCase):
    def test_counters_gauges_and_summaries(self) -> None:
        metrics = Metrics()
        metrics.incr("requests")
        metrics.incr("requests", 4)
        metrics.gauge("connections", 3)
        for value in (1.0, 3.0, 2.0):
            metrics.observe("latency", value)

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["counters"]["requests"], 5)
        self.assertEqual(metrics.counter("requests"), 5)
        self.assertEqual(snapshot["gauges"]["connections"], 3.0)
        latency = snapshot["summaries"]["latency"]
        self.assertEqual((latency["count"], latency["min"], latency["max"]), (3, 1.0, 3.0))
        self.assertAlmostEqual(latency["avg"], 2.0)

    def test_unknown_counter_is_zero_and_reset_clears(self) -> None:
        metrics = Metrics()
        self.assertEqual(metrics.counter("nothing"), 0)
        metrics.incr("x")
        metrics.reset()
        self.assertEqual(metrics.snapshot()["counters"], {})


class TestServerConfig(unittest.TestCase):
    def test_rejects_impossible_port(self) -> None:
        with self.assertRaises(ConfigError):
            ServerConfig(port=99999)

    def test_drain_seconds_cannot_be_negative(self) -> None:
        self.assertEqual(ServerConfig(shutdown_drain_seconds=-1).shutdown_drain_seconds, 0.0)

    def test_peer_name_formatting(self) -> None:
        self.assertEqual(format_peer_name(("10.0.0.1", 5555)), "10.0.0.1")
        self.assertEqual(format_peer_name(None), "unknown")
        self.assertEqual(format_peer_name("pipe"), "pipe")


if __name__ == "__main__":
    unittest.main()
