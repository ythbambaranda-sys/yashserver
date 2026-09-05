"""Resumable uploads and integrity verification."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver.upload import UploadError, UploadStore, file_digest, parse_checksum  # noqa: E402


async def _send(port: int, raw: bytes) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=30.0)
    writer.close()
    head, _, body = data.partition(b"\r\n\r\n")
    return head.decode("latin-1"), body


def _header(head: str, name: str) -> str | None:
    for line in head.split("\r\n")[1:]:
        key, _, value = line.partition(":")
        if key.strip().lower() == name.lower():
            return value.strip()
    return None


class _Base(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)
        self.app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        self.store = self.app.resumable_uploads("/uploads", self.tmp / "uploads")
        await self.app.start()
        self.port = self.app.bound_port

    async def asyncTearDown(self) -> None:
        await self.app.stop()
        self._dir.cleanup()

    async def create(self, length: int | None, checksum: str | None = None) -> str:
        headers = b"POST /uploads HTTP/1.1\r\nHost: t\r\nConnection: close\r\n"
        if length is not None:
            headers += b"Upload-Length: " + str(length).encode() + b"\r\n"
        if checksum:
            headers += b"Upload-Checksum: " + checksum.encode() + b"\r\n"
        head, body = await _send(self.port, headers + b"\r\n")
        self.assertIn("201", head)
        return json.loads(body)["id"]

    async def patch(
        self, upload_id: str, offset: int, payload: bytes, checksum: str | None = None
    ) -> tuple[str, bytes]:
        raw = (
            f"PATCH /uploads/{upload_id} HTTP/1.1\r\nHost: t\r\n"
            f"Upload-Offset: {offset}\r\nContent-Length: {len(payload)}\r\n"
        ).encode()
        if checksum:
            raw += b"Upload-Checksum: " + checksum.encode() + b"\r\n"
        raw += b"Connection: close\r\n\r\n" + payload
        return await _send(self.port, raw)

    async def head(self, upload_id: str) -> tuple[str, bytes]:
        return await _send(
            self.port,
            f"HEAD /uploads/{upload_id} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n".encode(),
        )


class TestResumableUpload(_Base):
    async def test_upload_in_one_chunk(self) -> None:
        payload = os.urandom(50_000)
        upload_id = await self.create(len(payload))
        head, body = await self.patch(upload_id, 0, payload)
        self.assertIn("200", head)
        report = json.loads(body)
        self.assertTrue(report["completed"])
        self.assertEqual(report["offset"], len(payload))
        stored = Path(report["metadata"]["stored_path"])
        self.assertEqual(stored.read_bytes(), payload)

    async def test_upload_in_many_chunks(self) -> None:
        payload = os.urandom(200_000)
        upload_id = await self.create(len(payload))
        step = 40_000
        for offset in range(0, len(payload), step):
            head, _ = await self.patch(upload_id, offset, payload[offset : offset + step])
            self.assertTrue("204" in head or "200" in head, head.splitlines()[0])
        head, body = await self.head(upload_id)
        self.assertEqual(_header(head, "Upload-Offset"), str(len(payload)))

    async def test_interrupted_upload_resumes_from_the_reported_offset(self) -> None:
        payload = os.urandom(300_000)
        upload_id = await self.create(len(payload))

        # Two chunks land, then the "client" dies.
        await self.patch(upload_id, 0, payload[:100_000])
        await self.patch(upload_id, 100_000, payload[100_000:180_000])

        # A new client asks where it got to, and carries on from there.
        head, _ = await self.head(upload_id)
        offset = int(_header(head, "Upload-Offset"))
        self.assertEqual(offset, 180_000)

        head, body = await self.patch(upload_id, offset, payload[offset:])
        self.assertIn("200", head)
        report = json.loads(body)
        self.assertTrue(report["completed"])
        stored = Path(report["metadata"]["stored_path"])
        self.assertEqual(stored.read_bytes(), payload)
        self.assertEqual(
            hashlib.sha256(stored.read_bytes()).hexdigest(),
            hashlib.sha256(payload).hexdigest(),
        )

    async def test_wrong_offset_is_refused_rather_than_written(self) -> None:
        payload = os.urandom(20_000)
        upload_id = await self.create(len(payload))
        await self.patch(upload_id, 0, payload[:5_000])

        # Too far ahead: would leave a hole.
        head, _ = await self.patch(upload_id, 9_000, payload[9_000:])
        self.assertIn("409", head)
        # Too far back: would duplicate bytes.
        head, _ = await self.patch(upload_id, 0, payload)
        self.assertIn("409", head)

        # The offset is untouched by the rejected attempts.
        head, _ = await self.head(upload_id)
        self.assertEqual(_header(head, "Upload-Offset"), "5000")

    async def test_missing_offset_header_is_a_400(self) -> None:
        upload_id = await self.create(100)
        head, _ = await _send(
            self.port,
            f"PATCH /uploads/{upload_id} HTTP/1.1\r\nHost: t\r\nContent-Length: 4\r\n"
            f"Connection: close\r\n\r\ndata".encode(),
        )
        self.assertIn("400", head)

    async def test_writing_past_the_declared_length_is_refused(self) -> None:
        upload_id = await self.create(1_000)
        head, _ = await self.patch(upload_id, 0, os.urandom(2_000))
        self.assertIn("400", head)
        head, _ = await self.head(upload_id)
        self.assertEqual(_header(head, "Upload-Offset"), "0")

    async def test_unknown_upload_is_404(self) -> None:
        head, _ = await self.head("f" * 32)
        self.assertIn("404", head)

    async def test_upload_id_cannot_be_used_for_traversal(self) -> None:
        for bad in ("..", "../../etc/passwd", "abc", "../" + "a" * 32):
            with self.subTest(bad=bad):
                head, _ = await _send(
                    self.port,
                    f"GET /uploads/{bad} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n".encode(),
                )
                self.assertTrue(
                    "404" in head or "400" in head, f"{bad} -> {head.splitlines()[0]}"
                )

    async def test_delete_abandons_the_upload(self) -> None:
        upload_id = await self.create(1_000)
        await self.patch(upload_id, 0, os.urandom(500))
        head, _ = await _send(
            self.port,
            f"DELETE /uploads/{upload_id} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n".encode(),
        )
        self.assertIn("204", head)
        head, _ = await self.head(upload_id)
        self.assertIn("404", head)

    async def test_oversized_upload_is_refused_at_creation(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.resumable_uploads("/uploads", self.tmp / "capped", max_upload_bytes=1_000)
        await app.start()
        try:
            head, _ = await _send(
                app.bound_port,
                b"POST /uploads HTTP/1.1\r\nHost: t\r\nUpload-Length: 999999\r\n"
                b"Connection: close\r\n\r\n",
            )
            self.assertIn("413", head)
        finally:
            await app.stop()


class TestIntegrity(_Base):
    async def test_whole_file_checksum_is_verified(self) -> None:
        payload = os.urandom(80_000)
        digest = hashlib.sha256(payload).hexdigest()
        upload_id = await self.create(len(payload), checksum=f"sha256 {digest}")
        head, body = await self.patch(upload_id, 0, payload)
        self.assertIn("200", head)
        self.assertTrue(json.loads(body)["completed"])

    async def test_whole_file_checksum_mismatch_is_rejected(self) -> None:
        payload = os.urandom(80_000)
        wrong = hashlib.sha256(b"something else entirely").hexdigest()
        upload_id = await self.create(len(payload), checksum=f"sha256 {wrong}")
        head, body = await self.patch(upload_id, 0, payload)
        self.assertIn("422", head)
        self.assertIn(b"mismatch", body)

    async def test_checksum_survives_a_resumed_upload(self) -> None:
        # The running digest is rebuilt from disk when a session is resumed by
        # a process that never saw the earlier chunks. Dropping the in-memory
        # state simulates a server restart mid-upload.
        payload = os.urandom(250_000)
        digest = hashlib.sha256(payload).hexdigest()
        upload_id = await self.create(len(payload), checksum=f"sha256 {digest}")

        await self.patch(upload_id, 0, payload[:120_000])
        self.store._digests.clear()  # as if the server had restarted

        head, body = await self.patch(upload_id, 120_000, payload[120_000:])
        self.assertIn("200", head)
        self.assertTrue(json.loads(body)["completed"])

    async def test_corruption_after_a_restart_is_still_caught(self) -> None:
        payload = os.urandom(150_000)
        digest = hashlib.sha256(payload).hexdigest()
        upload_id = await self.create(len(payload), checksum=f"sha256 {digest}")
        await self.patch(upload_id, 0, payload[:70_000])
        self.store._digests.clear()

        corrupted = bytearray(payload[70_000:])
        corrupted[0] ^= 0xFF
        head, _ = await self.patch(upload_id, 70_000, bytes(corrupted))
        self.assertIn("422", head)

    async def test_per_chunk_checksum_rolls_back_a_bad_chunk(self) -> None:
        payload = os.urandom(60_000)
        upload_id = await self.create(len(payload))
        good = payload[:30_000]
        await self.patch(
            upload_id, 0, good, checksum=f"sha256 {hashlib.sha256(good).hexdigest()}"
        )

        # Claim a digest the chunk does not have.
        bad = payload[30_000:]
        head, _ = await self.patch(
            upload_id,
            30_000,
            bad,
            checksum=f"sha256 {hashlib.sha256(b'lies').hexdigest()}",
        )
        self.assertIn("422", head)

        # Rolled back, so retrying the same chunk honestly succeeds.
        head, _ = await self.head(upload_id)
        self.assertEqual(_header(head, "Upload-Offset"), "30000")
        head, body = await self.patch(
            upload_id, 30_000, bad, checksum=f"sha256 {hashlib.sha256(bad).hexdigest()}"
        )
        self.assertIn("200", head)
        stored = Path(json.loads(body)["metadata"]["stored_path"])
        self.assertEqual(stored.read_bytes(), payload)

    async def test_an_empty_checksum_header_means_no_checksum(self) -> None:
        # An empty header value is equivalent to not sending the header at
        # all, which is the ordinary HTTP reading; it is not a malformed
        # checksum.
        head, _ = await _send(
            self.port,
            b"POST /uploads HTTP/1.1\r\nHost: t\r\nUpload-Length: 10\r\n"
            b"Upload-Checksum:   \r\nConnection: close\r\n\r\n",
        )
        self.assertIn("201", head)

    async def test_malformed_checksum_header_is_a_400(self) -> None:
        for value in ("sha256", "notanalgo abcdef12", "sha256 zzzz"):
            with self.subTest(value=value):
                head, _ = await _send(
                    self.port,
                    b"POST /uploads HTTP/1.1\r\nHost: t\r\nUpload-Length: 10\r\n"
                    b"Upload-Checksum: " + value.encode() + b"\r\nConnection: close\r\n\r\n",
                )
                self.assertIn("400", head)


class TestUploadStoreUnit(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)
        self.store = UploadStore(self.tmp)

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_file_digest_matches_hashlib(self) -> None:
        payload = os.urandom(300_000)
        path = self.tmp / "x.bin"
        path.write_bytes(payload)
        self.assertEqual(file_digest(path), hashlib.sha256(payload).hexdigest())
        self.assertEqual(file_digest(path, "sha1"), hashlib.sha1(payload).hexdigest())

    def test_file_digest_can_hash_a_prefix(self) -> None:
        payload = os.urandom(100_000)
        path = self.tmp / "x.bin"
        path.write_bytes(payload)
        self.assertEqual(
            file_digest(path, length=40_000), hashlib.sha256(payload[:40_000]).hexdigest()
        )

    def test_parse_checksum(self) -> None:
        self.assertEqual(parse_checksum("sha256 " + "a" * 64), ("sha256", "a" * 64))
        self.assertEqual(parse_checksum("SHA256 " + "A" * 64), ("sha256", "a" * 64))
        for bad in ("", "sha256", "bogus " + "a" * 64, "sha256 nothex"):
            with self.subTest(bad=bad):
                with self.assertRaises(UploadError):
                    parse_checksum(bad)

    def test_session_survives_a_reload(self) -> None:
        session = self.store.create(length=100, filename="x.bin")
        reloaded = UploadStore(self.tmp).get(session.id)
        self.assertEqual(reloaded.id, session.id)
        self.assertEqual(reloaded.length, 100)
        self.assertEqual(reloaded.filename, "x.bin")

    def test_offset_never_exceeds_what_is_on_disk(self) -> None:
        # The sidecar is written after the data, so a crash between the two
        # leaves it behind the file, never ahead. Simulate the opposite --
        # a sidecar claiming more than exists -- and confirm the file wins.
        session = self.store.create(length=1000)
        session.offset = 500
        self.store._save(session)
        self.assertEqual(self.store.get(session.id).offset, 0)

    def test_expired_sessions_are_purged(self) -> None:
        store = UploadStore(self.tmp, session_ttl_seconds=0.0)
        session = store.create(length=10)
        self.assertEqual(store.purge_expired(), 1)
        with self.assertRaises(UploadError):
            store.get(session.id)

    def test_bad_session_ids_are_rejected(self) -> None:
        for bad in ("", "..", "../../etc/passwd", "xyz", "A" * 32, "a" * 31):
            with self.subTest(bad=bad):
                with self.assertRaises(UploadError):
                    self.store.get(bad)


if __name__ == "__main__":
    unittest.main()
