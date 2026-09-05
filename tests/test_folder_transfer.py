"""Whole-folder transfer over HTTP, including hostile uploads."""

from __future__ import annotations

import asyncio
import io
import json
import os
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import archive as A  # noqa: E402


async def _raw(port: int, payload: bytes, *, read_all: bool = True) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(payload)
    await writer.drain()
    data = await asyncio.wait_for(reader.read() if read_all else reader.read(65536), timeout=60.0)
    writer.close()
    head, _, body = data.partition(b"\r\n\r\n")
    return head.decode("latin-1"), body


def _dechunk(body: bytes) -> bytes:
    """Decode a chunked response body."""

    out = bytearray()
    rest = body
    while True:
        line, _, rest = rest.partition(b"\r\n")
        if not line:
            break
        size = int(line.split(b";")[0], 16)
        if size == 0:
            break
        out += rest[:size]
        rest = rest[size + 2 :]
    return bytes(out)


class TestFolderDownload(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)
        self.root = self.tmp / "data"
        (self.root / "sub").mkdir(parents=True)
        (self.root / "a.txt").write_text("alpha")
        (self.root / "sub" / "b.bin").write_bytes(os.urandom(50_000))
        (self.root / "sub" / "c.txt").write_text("gamma")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def contents(self, root: Path) -> dict[str, bytes]:
        return {str(n): p.read_bytes() for p, n in A.iter_folder_entries(root)}

    async def test_folder_downloads_as_a_streamed_archive(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.serve_folder("/backup", self.root)
        await app.start()
        try:
            for fmt in ("tar.gz", "zip", "tar", "tar.bz2", "tar.xz"):
                with self.subTest(fmt=fmt):
                    head, body = await _raw(
                        app.bound_port,
                        f"GET /backup?format={fmt} HTTP/1.1\r\nHost: t\r\n"
                        f"Connection: close\r\n\r\n".encode(),
                    )
                    self.assertIn("200 OK", head)
                    self.assertIn("attachment", head)
                    # Unknown length up front, so it must be chunked.
                    self.assertIn("Transfer-Encoding: chunked", head)
                    blob = _dechunk(body)

                    arc = self.tmp / f"dl.{fmt}"
                    arc.write_bytes(blob)
                    out = self.tmp / f"out_{fmt.replace('.', '_')}"
                    A.extract_archive(arc, out)
                    self.assertEqual(self.contents(out), self.contents(self.root))
        finally:
            await app.stop()

    async def test_unsupported_format_is_rejected(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.serve_folder("/backup", self.root)
        await app.start()
        try:
            head, _ = await _raw(
                app.bound_port,
                b"GET /backup?format=rar HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
            )
            self.assertIn("400", head)
        finally:
            await app.stop()

    async def test_missing_folder_is_404(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.serve_folder("/gone", self.tmp / "does-not-exist")
        await app.start()
        try:
            head, _ = await _raw(
                app.bound_port, b"GET /gone HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
            )
            self.assertIn("404", head)
        finally:
            await app.stop()

    async def test_client_disconnecting_mid_download_does_not_break_the_server(self) -> None:
        big = self.root / "big.bin"
        big.write_bytes(os.urandom(8 * 1024 * 1024))
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.serve_folder("/backup", self.root)
        await app.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
            writer.write(b"GET /backup?format=tar HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            await writer.drain()
            await reader.read(1024)
            writer.transport.abort()  # vanish mid-transfer
            await asyncio.sleep(0.3)

            # The server must still serve everyone else.
            head, _ = await _raw(
                app.bound_port,
                b"GET /backup?format=tar HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
            )
            self.assertIn("200 OK", head)
        finally:
            await app.stop()


class TestFolderUpload(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)
        self.dest = self.tmp / "incoming"

    def tearDown(self) -> None:
        self._dir.cleanup()

    async def _post(self, port: int, blob: bytes) -> tuple[str, bytes]:
        return await _raw(
            port,
            b"POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: "
            + str(len(blob)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + blob,
        )

    def _good_zip(self) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("a.txt", "alpha")
            zf.writestr("sub/b.txt", "beta")
        return buffer.getvalue()

    async def test_valid_archive_is_extracted(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            head, body = await self._post(app.bound_port, self._good_zip())
            self.assertIn("201", head)
            report = json.loads(body)
            self.assertEqual(report["entries"], 2)
            self.assertEqual(report["format"], "zip")
            extracted = Path(report["destination"])
            self.assertEqual((extracted / "a.txt").read_text(), "alpha")
            self.assertEqual((extracted / "sub" / "b.txt").read_text(), "beta")
        finally:
            await app.stop()

    async def test_traversal_archive_is_rejected_with_422(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("../../pwned.txt", "owned")

        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            head, body = await self._post(app.bound_port, buffer.getvalue())
            self.assertIn("422", head)
            self.assertIn(b"rejected", body)
            # Nothing planted anywhere.
            self.assertFalse((self.tmp / "pwned.txt").exists())
            self.assertFalse((self.tmp.parent / "pwned.txt").exists())
        finally:
            await app.stop()

    async def test_zip_bomb_is_rejected(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("bomb.bin", b"\x00" * (64 * 1024 * 1024))

        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            head, _body = await self._post(app.bound_port, buffer.getvalue())
            self.assertIn("422", head)
        finally:
            await app.stop()

    async def test_symlink_archive_is_rejected(self) -> None:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as tf:
            info = tarfile.TarInfo("escape")
            info.type = tarfile.SYMTYPE
            info.linkname = "../../../../etc/passwd"
            tf.addfile(info)

        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            head, _body = await self._post(app.bound_port, buffer.getvalue())
            self.assertIn("422", head)
        finally:
            await app.stop()

    async def test_garbage_upload_is_rejected_cleanly(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            head, _body = await self._post(app.bound_port, b"this is not an archive at all")
            self.assertIn("400", head)
        finally:
            await app.stop()

    async def test_oversized_upload_is_rejected(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest, max_upload_bytes=1024)
        await app.start()
        try:
            head, _body = await self._post(app.bound_port, self._good_zip() + b"\x00" * 4096)
            self.assertIn("413", head)
        finally:
            await app.stop()

    async def test_rejected_uploads_leave_nothing_behind(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("../../pwned.txt", "owned")

        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            await self._post(app.bound_port, buffer.getvalue())
            await self._post(app.bound_port, b"garbage")
            leftovers = list(self.dest.glob("*")) if self.dest.exists() else []
            self.assertEqual(leftovers, [], f"staging not cleaned: {leftovers}")
        finally:
            await app.stop()

    async def test_concurrent_uploads_do_not_interleave(self) -> None:
        app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
        app.accept_folder("/upload", self.dest)
        await app.start()
        try:
            def payload(tag: str) -> bytes:
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, "w") as zf:
                    zf.writestr(f"{tag}.txt", tag * 100)
                return buffer.getvalue()

            results = await asyncio.gather(
                *(self._post(app.bound_port, payload(f"t{i}")) for i in range(6))
            )
            destinations = set()
            for head, body in results:
                self.assertIn("201", head)
                report = json.loads(body)
                self.assertEqual(report["entries"], 1)
                destinations.add(report["destination"])
            # Each upload got its own tree.
            self.assertEqual(len(destinations), 6)
        finally:
            await app.stop()


class TestRoundTripOverHttp(unittest.IsolatedAsyncioTestCase):
    async def test_download_a_folder_then_upload_it_back(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            source = tmp / "source"
            (source / "nested" / "deeper").mkdir(parents=True)
            (source / "one.txt").write_text("one")
            (source / "nested" / "two.bin").write_bytes(os.urandom(30_000))
            (source / "nested" / "deeper" / "three.txt").write_text("three")

            app = yashserver.YHttpServer(host="127.0.0.1", port=0, ddosprot=False)
            app.serve_folder("/download", source)
            app.accept_folder("/upload", tmp / "landing")
            await app.start()
            try:
                head, body = await _raw(
                    app.bound_port,
                    b"GET /download?format=tar.gz HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
                )
                self.assertIn("200 OK", head)
                blob = _dechunk(body)

                head, body = await _raw(
                    app.bound_port,
                    b"POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: "
                    + str(len(blob)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + blob,
                )
                self.assertIn("201", head)
                report = json.loads(body)
                landed = Path(report["destination"])

                original = {str(n): p.read_bytes() for p, n in A.iter_folder_entries(source)}
                restored = {str(n): p.read_bytes() for p, n in A.iter_folder_entries(landed)}
                self.assertEqual(restored, original)
                self.assertEqual(report["entries"], 3)
            finally:
                await app.stop()


if __name__ == "__main__":
    unittest.main()
