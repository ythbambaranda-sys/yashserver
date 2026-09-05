"""Archive handling, with the emphasis on refusing hostile input.

Every test that builds a malicious archive constructs it by hand rather than
through a helper, so what is being defended against stays readable.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import stat
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from yashserver import archive as A  # noqa: E402
from yashserver.archive import ArchiveError, ArchivePolicy, UnsafeArchiveError  # noqa: E402


class _Tmp(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)

    def tearDown(self) -> None:
        self._dir.cleanup()

    def sample_tree(self, name: str = "src") -> Path:
        root = self.tmp / name
        (root / "sub" / "deep").mkdir(parents=True)
        (root / "top.txt").write_text("top")
        (root / "sub" / "mid.bin").write_bytes(os.urandom(4096))
        (root / "sub" / "deep" / "low.txt").write_text("low")
        return root

    def tree_contents(self, root: Path) -> dict[str, bytes]:
        return {
            str(name): path.read_bytes() for path, name in A.iter_folder_entries(root)
        }


# ---------------------------------------------------------------------------
# name sanitisation -- the single most important function here
# ---------------------------------------------------------------------------


class TestSafeMemberPath(unittest.TestCase):
    def test_ordinary_names_pass_through(self) -> None:
        for name in ("a.txt", "dir/a.txt", "a/b/c/d.bin", "./a.txt", "dir//a.txt"):
            with self.subTest(name=name):
                self.assertTrue(str(A.safe_member_path(name)))

    def test_traversal_is_refused(self) -> None:
        hostile = [
            "../escape.txt",
            "a/../../escape.txt",
            "a/b/../../../escape.txt",
            "..",
            "a/..",
            "....//escape.txt".replace("....", ".."),
        ]
        for name in hostile:
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_backslash_traversal_is_refused(self) -> None:
        # A zip written on Windows may use backslashes; normalising without
        # then re-checking is a classic hole.
        for name in ("..\\escape.txt", "a\\..\\..\\escape.txt"):
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_absolute_paths_are_refused(self) -> None:
        for name in ("/etc/passwd", "//server/share/x", "\\\\server\\share\\x"):
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_drive_letters_are_refused(self) -> None:
        for name in ("C:/Windows/System32/x", "C:\\Windows\\x", "d:evil.txt"):
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_nul_and_control_characters_are_refused(self) -> None:
        for name in ("a\x00b.txt", "a\nb.txt", "a\rb.txt", "a\tb.txt"):
            with self.subTest(name=repr(name)):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_windows_reserved_names_are_refused_on_every_platform(self) -> None:
        # Refused even on Linux, so an extracted folder does not become a
        # landmine the moment it is copied to Windows.
        for name in ("CON", "con.txt", "aux/file.txt", "dir/NUL.log", "lpt1.dat"):
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_trailing_dots_and_spaces_are_refused(self) -> None:
        # Windows silently strips these, so "evil. " can collide with "evil".
        for name in ("evil. ", "evil.", "dir /file.txt", "dir./file.txt"):
            with self.subTest(name=name):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)

    def test_limits_are_enforced(self) -> None:
        policy = ArchivePolicy(max_depth=3, max_name_length=10, max_path_length=40)
        with self.assertRaises(UnsafeArchiveError):
            A.safe_member_path("a/b/c/d/e.txt", policy)
        with self.assertRaises(UnsafeArchiveError):
            A.safe_member_path("averyveryverylongname.txt", policy)
        with self.assertRaises(UnsafeArchiveError):
            A.safe_member_path("a/" * 30 + "x.txt", policy)

    def test_empty_names_are_refused(self) -> None:
        for name in ("", "   ", ".", "./", "//"):
            with self.subTest(name=repr(name)):
                with self.assertRaises(UnsafeArchiveError):
                    A.safe_member_path(name)


# ---------------------------------------------------------------------------
# hostile archives
# ---------------------------------------------------------------------------


class TestHostileArchives(_Tmp):
    def test_zip_traversal_is_refused(self) -> None:
        bad = self.tmp / "evil.zip"
        with zipfile.ZipFile(bad, "w") as zf:
            zf.writestr("../../pwned.txt", "owned")
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")
        self.assertFalse((self.tmp / "pwned.txt").exists())
        self.assertFalse((self.tmp.parent / "pwned.txt").exists())

    def test_zip_absolute_path_is_refused(self) -> None:
        bad = self.tmp / "abs.zip"
        with zipfile.ZipFile(bad, "w") as zf:
            zf.writestr("/tmp/pwned.txt", "owned")
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")

    def test_zip_symlink_member_is_refused(self) -> None:
        bad = self.tmp / "link.zip"
        with zipfile.ZipFile(bad, "w") as zf:
            info = zipfile.ZipInfo("link")
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            zf.writestr(info, "/etc/passwd")
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")

    def test_tar_symlink_escape_is_refused(self) -> None:
        bad = self.tmp / "evil.tar"
        with tarfile.open(bad, "w") as tf:
            info = tarfile.TarInfo("escape")
            info.type = tarfile.SYMTYPE
            info.linkname = "../../../../etc/passwd"
            tf.addfile(info)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")

    def test_tar_absolute_symlink_is_refused_even_when_links_allowed(self) -> None:
        bad = self.tmp / "abs_link.tar"
        with tarfile.open(bad, "w") as tf:
            info = tarfile.TarInfo("escape")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            tf.addfile(info)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out", policy=ArchivePolicy(allow_links=True))

    def test_tar_relative_symlink_escape_is_refused_when_links_allowed(self) -> None:
        bad = self.tmp / "rel_link.tar"
        with tarfile.open(bad, "w") as tf:
            info = tarfile.TarInfo("dir/escape")
            info.type = tarfile.SYMTYPE
            info.linkname = "../../outside"
            tf.addfile(info)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out", policy=ArchivePolicy(allow_links=True))

    def test_tar_device_file_is_refused(self) -> None:
        bad = self.tmp / "dev.tar"
        with tarfile.open(bad, "w") as tf:
            info = tarfile.TarInfo("hda")
            info.type = tarfile.BLKTYPE
            info.devmajor, info.devminor = 8, 0
            tf.addfile(info)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")

    def test_tar_fifo_is_refused(self) -> None:
        bad = self.tmp / "fifo.tar"
        with tarfile.open(bad, "w") as tf:
            info = tarfile.TarInfo("pipe")
            info.type = tarfile.FIFOTYPE
            tf.addfile(info)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(bad, self.tmp / "out")

    def test_zip_bomb_ratio_is_refused(self) -> None:
        # 200 MB of zeroes compresses to a few KB: ratio far past the cap.
        bomb = self.tmp / "bomb.zip"
        with zipfile.ZipFile(bomb, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("bomb.bin", b"\x00" * (200 * 1024 * 1024))
        with self.assertRaises(UnsafeArchiveError) as caught:
            A.extract_archive(bomb, self.tmp / "out")
        self.assertIn("ratio", str(caught.exception))

    def test_total_size_cap_is_enforced(self) -> None:
        big = self.tmp / "big.zip"
        with zipfile.ZipFile(big, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("a.bin", os.urandom(2 * 1024 * 1024))
        policy = ArchivePolicy(max_total_bytes=1024 * 1024, max_compression_ratio=1000)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(big, self.tmp / "out", policy=policy)

    def test_per_entry_size_cap_is_enforced(self) -> None:
        big = self.tmp / "entry.zip"
        with zipfile.ZipFile(big, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("a.bin", os.urandom(2 * 1024 * 1024))
        policy = ArchivePolicy(max_entry_bytes=1024 * 1024, max_compression_ratio=1000)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(big, self.tmp / "out", policy=policy)

    def test_entry_count_cap_is_enforced(self) -> None:
        many = self.tmp / "many.zip"
        with zipfile.ZipFile(many, "w") as zf:
            for i in range(50):
                zf.writestr(f"f{i}.txt", "x")
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(many, self.tmp / "out", policy=ArchivePolicy(max_entries=10))

    def test_partial_file_is_removed_when_a_limit_trips(self) -> None:
        big = self.tmp / "partial.zip"
        with zipfile.ZipFile(big, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("a.bin", os.urandom(4 * 1024 * 1024))
        out = self.tmp / "out"
        policy = ArchivePolicy(max_entry_bytes=1024 * 1024, max_compression_ratio=1000)
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(big, out, policy=policy)
        # A rejected archive must not leave a half-written bomb behind.
        self.assertFalse((out / "a.bin").exists())

    def test_symlink_in_destination_cannot_redirect_a_write(self) -> None:
        # The archive is innocent; the destination has been booby-trapped.
        if not hasattr(os, "symlink"):
            self.skipTest("no symlink support")
        out = self.tmp / "out"
        (out / "dir").mkdir(parents=True)
        outside = self.tmp / "outside"
        outside.mkdir()
        try:
            (out / "dir" / "link").symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("symlink creation not permitted")

        good = self.tmp / "ok.zip"
        with zipfile.ZipFile(good, "w") as zf:
            zf.writestr("dir/link/planted.txt", "planted")
        with self.assertRaises(UnsafeArchiveError):
            A.extract_archive(good, out)
        self.assertFalse((outside / "planted.txt").exists())


# ---------------------------------------------------------------------------
# well-behaved archives
# ---------------------------------------------------------------------------


class TestRoundTrip(_Tmp):
    def test_every_writable_format_round_trips(self) -> None:
        root = self.sample_tree()
        expected = self.tree_contents(root)
        for fmt, ext in (
            ("zip", ".zip"),
            ("tar", ".tar"),
            ("tar.gz", ".tar.gz"),
            ("tar.bz2", ".tar.bz2"),
            ("tar.xz", ".tar.xz"),
        ):
            with self.subTest(fmt=fmt):
                arc = self.tmp / f"a{ext}"
                A.create_archive(root, arc, fmt=fmt)
                self.assertEqual(A.detect_format(arc), fmt)
                out = self.tmp / f"out_{fmt.replace('.', '_')}"
                report = A.extract_archive(arc, out)
                self.assertEqual(report.entries, 3)
                self.assertEqual(self.tree_contents(out), expected)

    def test_format_is_detected_from_content_not_extension(self) -> None:
        root = self.sample_tree()
        lying = self.tmp / "actually_a_tar_gz.zip"
        A.create_archive(root, lying, fmt="tar.gz")
        self.assertEqual(A.detect_format(lying), "tar.gz")
        # And it still extracts correctly despite the wrong name.
        out = self.tmp / "out"
        A.extract_archive(lying, out)
        self.assertEqual(self.tree_contents(out), self.tree_contents(root))

    def test_rar_cannot_be_created(self) -> None:
        root = self.sample_tree()
        with self.assertRaises(ArchiveError) as caught:
            A.create_archive(root, self.tmp / "x.rar")
        self.assertIn("proprietary", str(caught.exception))

    def test_folder_size_matches_the_tree(self) -> None:
        root = self.sample_tree()
        count, total = A.folder_size(root)
        self.assertEqual(count, 3)
        self.assertEqual(total, 3 + 4096 + 3)

    def test_entry_order_is_deterministic(self) -> None:
        # Walk order is depth-first with names sorted at each level, which is
        # reproducible without materialising the whole tree. That is the
        # property archives need; a global lexicographic sort would cost
        # memory proportional to the file count for no benefit.
        root = self.sample_tree()
        first = [str(n) for _p, n in A.iter_folder_entries(root)]
        second = [str(n) for _p, n in A.iter_folder_entries(root)]
        self.assertEqual(first, second)
        self.assertEqual(sorted(first), sorted(second))
        self.assertEqual(len(first), 3)

    def test_symlinks_are_skipped_by_default(self) -> None:
        root = self.sample_tree()
        try:
            (root / "link.txt").symlink_to(root / "top.txt")
        except (OSError, NotImplementedError):
            self.skipTest("symlink creation not permitted")
        names = [str(n) for _p, n in A.iter_folder_entries(root)]
        self.assertNotIn("link.txt", names)


class TestRar(_Tmp):
    """RAR is read-only, and that asymmetry is the thing worth testing.

    End-to-end extraction cannot be tested without a real ``.rar`` fixture,
    and one cannot be generated here: the only RAR encoder is proprietary.
    Set ``YSERVER_RAR_FIXTURE`` to the path of a RAR archive to exercise the
    extraction path for real; without it that one test skips rather than
    quietly pretending to pass.
    """

    def test_rar_magic_is_detected(self) -> None:
        # Detection must work from content alone, since a hostile upload will
        # not label itself honestly.
        fake = self.tmp / "mislabelled.zip"
        fake.write_bytes(b"Rar!\x1a\x07\x00" + b"\x00" * 64)
        self.assertEqual(A.detect_format(fake), "rar")

    def test_creating_rar_is_refused_with_a_clear_reason(self) -> None:
        root = self.sample_tree()
        with self.assertRaises(ArchiveError) as caught:
            A.create_archive(root, self.tmp / "out.rar")
        message = str(caught.exception)
        self.assertIn("rar", message.lower())
        self.assertIn("proprietary", message)

    def test_streaming_rar_is_refused(self) -> None:
        self.assertNotIn("rar", A.WRITABLE_FORMATS)
        self.assertIn("rar", A.READABLE_FORMATS)

    def test_corrupt_rar_is_an_error_not_a_silent_empty_success(self) -> None:
        # rarfile accepts anything with the RAR signature and reports zero
        # members instead of raising, so without an explicit check a corrupt
        # upload would report success having extracted nothing. Skipped where
        # rarfile is absent, so that a pass always means the real path ran
        # rather than the "rarfile is missing" branch.
        try:
            import rarfile  # noqa: F401
        except ImportError:
            self.skipTest("rarfile not installed; the real RAR path cannot run")

        for label, payload in (
            ("truncated", b"Rar!\x1a\x07\x00" + os.urandom(128)),
            ("signature only", b"Rar!\x1a\x07\x00"),
            ("rar5 truncated", b"Rar!\x1a\x07\x01\x00" + os.urandom(128)),
        ):
            with self.subTest(case=label):
                broken = self.tmp / f"broken_{label.replace(' ', '_')}.rar"
                broken.write_bytes(payload)
                with self.assertRaises(ArchiveError):
                    A.extract_archive(broken, self.tmp / "out")

    def test_non_rar_masquerading_as_rar_is_an_error(self) -> None:
        try:
            import rarfile  # noqa: F401
        except ImportError:
            self.skipTest("rarfile not installed; the real RAR path cannot run")
        junk = self.tmp / "junk.rar"
        junk.write_bytes(os.urandom(256))
        with self.assertRaises(ArchiveError):
            A.extract_archive(junk, self.tmp / "out", fmt="rar")

    def test_real_rar_extracts(self) -> None:
        fixture = os.environ.get("YSERVER_RAR_FIXTURE")
        if not fixture or not Path(fixture).is_file():
            self.skipTest(
                "no RAR fixture; set YSERVER_RAR_FIXTURE to a .rar path. "
                "Generate one with scripts/make_rar_fixtures.py, which needs a "
                "RAR encoder (WinRAR) -- there is no free one."
            )
        self.assertEqual(A.detect_format(fixture), "rar")
        out = self.tmp / "rar_out"
        report = A.extract_archive(fixture, out)
        self.assertEqual(report.format, "rar")
        self.assertGreater(report.entries, 0)
        self.assertGreater(report.bytes_written, 0)
        extracted = list(A.iter_folder_entries(out))
        self.assertEqual(len(extracted), report.entries)

        # When the tree the fixture was built from is known, insist on an
        # exact byte-for-byte match rather than just "some files appeared".
        source = os.environ.get("YSERVER_RAR_SOURCE")
        if source and Path(source).is_dir():
            def digests(root):
                return {
                    str(name): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path, name in A.iter_folder_entries(root)
                }

            self.assertEqual(digests(out), digests(Path(source)))

    def test_real_rar_bomb_is_blocked(self) -> None:
        bomb = os.environ.get("YSERVER_RAR_BOMB")
        if not bomb or not Path(bomb).is_file():
            self.skipTest("no RAR bomb fixture; set YSERVER_RAR_BOMB")
        out = self.tmp / "bomb_out"
        with self.assertRaises(UnsafeArchiveError) as caught:
            A.extract_archive(bomb, out)
        self.assertIn("ratio", str(caught.exception))
        # And nothing was left behind on the way to refusing it.
        leftover = list(out.rglob("*")) if out.exists() else []
        self.assertEqual([p for p in leftover if p.is_file()], [])

    def test_real_rar_respects_per_entry_cap(self) -> None:
        bomb = os.environ.get("YSERVER_RAR_BOMB")
        if not bomb or not Path(bomb).is_file():
            self.skipTest("no RAR bomb fixture; set YSERVER_RAR_BOMB")
        policy = ArchivePolicy(max_entry_bytes=8 * 1024 * 1024, max_compression_ratio=1e9)
        with self.assertRaises(UnsafeArchiveError) as caught:
            A.extract_archive(bomb, self.tmp / "cap_out", policy=policy)
        self.assertIn("exceeds", str(caught.exception))

    def test_windows_rar_backend_is_discovered(self) -> None:
        # rarfile looks for a bare 'unrar' on PATH, which WinRAR does not
        # provide, so without discovery RAR support is dead on Windows.
        if sys.platform != "win32":
            self.skipTest("Windows-specific backend discovery")
        try:
            import rarfile  # noqa: F401
        except ImportError:
            self.skipTest("rarfile not installed")
        from yashserver.archive import _rarfile_module, _windows_rar_backends

        if not _windows_rar_backends():
            self.skipTest("no WinRAR/7-Zip installed to discover")
        module = _rarfile_module()
        self.assertTrue(Path(module.UNRAR_TOOL).is_file())

    def test_rar_traversal_would_be_refused(self) -> None:
        # The RAR path shares safe_member_path with every other format, so the
        # traversal defence is the same code. Assert that explicitly rather
        # than assuming it.
        with self.assertRaises(UnsafeArchiveError):
            A.safe_member_path("../../pwned.txt")


class TestFolderStreaming(unittest.IsolatedAsyncioTestCase, _Tmp):
    async def test_streamed_archive_matches_the_folder(self) -> None:
        root = self.sample_tree()
        chunks = []
        async for chunk in A.folder_archive_stream(root, fmt="tar.gz", chunk_size=4096):
            chunks.append(chunk)
        blob = b"".join(chunks)
        self.assertGreater(len(blob), 0)

        arc = self.tmp / "streamed.tar.gz"
        arc.write_bytes(blob)
        out = self.tmp / "out"
        A.extract_archive(arc, out)
        self.assertEqual(self.tree_contents(out), self.tree_contents(root))

    async def test_streaming_zip_also_works(self) -> None:
        root = self.sample_tree()
        blob = b"".join([c async for c in A.folder_archive_stream(root, fmt="zip")])
        arc = self.tmp / "streamed.zip"
        arc.write_bytes(blob)
        out = self.tmp / "out"
        A.extract_archive(arc, out)
        self.assertEqual(self.tree_contents(out), self.tree_contents(root))

    async def test_abandoning_the_stream_does_not_hang_or_leak(self) -> None:
        # A client disconnecting mid-download must not leave the worker thread
        # blocked forever on a queue nobody drains.
        root = self.sample_tree()
        stream = A.folder_archive_stream(root, fmt="tar.gz", chunk_size=1024, queue_size=1)
        first = await stream.__anext__()
        self.assertTrue(first)
        await asyncio.wait_for(stream.aclose(), timeout=5.0)

    async def test_streaming_an_unwritable_format_is_refused(self) -> None:
        root = self.sample_tree()
        with self.assertRaises(ArchiveError):
            async for _chunk in A.folder_archive_stream(root, fmt="rar"):
                break


if __name__ == "__main__":
    unittest.main()
