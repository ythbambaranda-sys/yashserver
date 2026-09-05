"""Real-world whole-folder transfer benchmark.

Points it at a large folder (the CS2 install, in the case this was built for)
and verifies that YashServer can move it correctly, in bounded memory, with
every byte accounted for.

    python scripts/folder_transfer_benchmark.py --source "<folder>" \
        --workdir C:\\yashserver-testdata --phases stream,archive,extract

Safety rules it enforces itself:

* the source folder is opened read-only and never written to
* everything generated goes under ``--workdir`` and nowhere else
* free space is re-checked before every phase that writes, against that
  phase's own requirement plus a margin
* phases run sequentially and clean up, so one copy exists at a time unless
  a phase genuinely needs two
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import shutil
import sys
import tarfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402
from yashserver import archive as A  # noqa: E402

GIB = 1024 ** 3


# ---------------------------------------------------------------------------
# process memory, without a third-party dependency
# ---------------------------------------------------------------------------

if sys.platform == "win32":
    import ctypes
    import ctypes.wintypes

    class _PMC(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.wintypes.DWORD),
            ("PageFaultCount", ctypes.wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _psapi = ctypes.WinDLL("psapi", use_last_error=True)
    _k32.GetCurrentProcess.restype = ctypes.c_void_p
    _psapi.GetProcessMemoryInfo.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_PMC),
        ctypes.wintypes.DWORD,
    ]

    def _mem() -> tuple[float, float]:
        pmc = _PMC()
        pmc.cb = ctypes.sizeof(pmc)
        _psapi.GetProcessMemoryInfo(_k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
        return pmc.WorkingSetSize / 1024 / 1024, pmc.PeakWorkingSetSize / 1024 / 1024

else:

    def _mem() -> tuple[float, float]:
        current = peak = 0.0
        with open("/proc/self/status") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    current = int(line.split()[1]) / 1024
                elif line.startswith("VmHWM:"):
                    peak = int(line.split()[1]) / 1024
        return current, peak


def rss() -> float:
    return _mem()[0]


def peak_rss() -> float:
    return _mem()[1]


def free_gib(path: Path) -> float:
    """Free space on the volume holding ``path``.

    Walks up to the first ancestor that exists, so this works before the
    workdir has been created.
    """

    probe = Path(path).resolve()
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return shutil.disk_usage(probe).free / GIB


def require_space(path: Path, needed_gib: float, margin_gib: float, label: str) -> None:
    available = free_gib(path)
    if available < needed_gib + margin_gib:
        raise SystemExit(
            f"REFUSING {label}: needs {needed_gib:.1f} GiB + {margin_gib:.1f} GiB margin, "
            f"but only {available:.1f} GiB is free on {path.anchor or path}"
        )
    print(
        f"  space check: {available:.1f} GiB free, need {needed_gib:.1f} GiB "
        f"(+{margin_gib:.1f} margin) -- OK"
    )


# ---------------------------------------------------------------------------
# hashing
# ---------------------------------------------------------------------------


def hash_tree(root: Path, label: str, chunk: int = 8 << 20) -> dict[str, str]:
    """SHA-256 every file under ``root``, keyed by archive-relative name."""

    digests: dict[str, str] = {}
    total = 0
    started = time.time()
    for absolute, name in A.iter_folder_entries(root):
        digest = hashlib.sha256()
        try:
            with open(absolute, "rb") as handle:
                while block := handle.read(chunk):
                    digest.update(block)
                    total += len(block)
        except OSError as error:
            print(f"    unreadable: {name} ({error})")
            continue
        digests[str(name)] = digest.hexdigest()
    elapsed = max(time.time() - started, 1e-9)
    print(
        f"  {label}: {len(digests):,} files, {total/GIB:.2f} GiB hashed in "
        f"{elapsed:.0f}s ({total/elapsed/1024/1024:.0f} MB/s)"
    )
    return digests


def compare(expected: dict[str, str], actual: dict[str, str]) -> bool:
    if expected == actual:
        print(f"  INTEGRITY: MATCH ({len(expected):,} files, every SHA-256 identical)")
        return True
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    differing = sorted(k for k in set(expected) & set(actual) if expected[k] != actual[k])
    print("  INTEGRITY: MISMATCH")
    print(f"    missing   : {len(missing):,}  {missing[:3]}")
    print(f"    unexpected: {len(extra):,}  {extra[:3]}")
    print(f"    differing : {len(differing):,}  {differing[:3]}")
    return False


# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------


async def phase_stream(source: Path, expected: dict[str, str], fmt: str) -> bool:
    """Serve the folder over HTTP and verify it without ever touching disk.

    The client hashes members straight out of the tar stream, so this proves a
    66 GiB folder transfers correctly in constant memory with no scratch space.
    """

    print("\n=== PHASE 1: HTTP folder stream, verified in flight (0 bytes written) ===")
    app = yashserver.YHttpServer(
        host="127.0.0.1",
        port=0,
        ddosprot=False,
        stream_chunk_size=1 << 20,
        write_timeout_seconds=1800.0,
    )
    app.serve_folder("/folder", source, fmt=fmt)
    await app.start()
    baseline = rss()
    print(f"  baseline RSS {baseline:.1f} MB")

    reader, writer = await asyncio.open_connection("127.0.0.1", app.bound_port)
    writer.write(
        f"GET /folder?format={fmt} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n".encode()
    )
    await writer.drain()
    head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
    if "200 OK" not in head:
        print(f"  FAILED: {head.splitlines()[0]}")
        await app.stop()
        return False

    # De-chunk into a pipe that tarfile reads as a non-seekable stream.
    loop = asyncio.get_running_loop()
    read_fd, write_fd = os.pipe()
    received = {"bytes": 0}
    high_water = {"rss": baseline}

    async def pump() -> None:
        with os.fdopen(write_fd, "wb") as sink:
            while True:
                line = await reader.readuntil(b"\r\n")
                size = int(line.split(b";")[0], 16)
                if size == 0:
                    await reader.readuntil(b"\r\n")
                    break
                payload = await reader.readexactly(size)
                await reader.readexactly(2)
                received["bytes"] += len(payload)
                await loop.run_in_executor(None, sink.write, payload)
                if received["bytes"] % (4 << 30) < (1 << 20):
                    high_water["rss"] = max(high_water["rss"], rss())

    def consume() -> dict[str, str]:
        digests: dict[str, str] = {}
        with os.fdopen(read_fd, "rb") as tap:
            with tarfile.open(fileobj=tap, mode="r|*") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    handle = archive.extractfile(member)
                    if handle is None:
                        continue
                    digest = hashlib.sha256()
                    while block := handle.read(8 << 20):
                        digest.update(block)
                    digests[member.name] = digest.hexdigest()
        return digests

    started = time.time()
    consumer = loop.run_in_executor(None, consume)
    try:
        await pump()
        streamed = await consumer
    finally:
        writer.close()
    elapsed = max(time.time() - started, 1e-9)

    print(
        f"  streamed {received['bytes']/GIB:.2f} GiB in {elapsed:.0f}s "
        f"({received['bytes']/elapsed/1024/1024:.0f} MB/s)"
    )
    print(f"  RSS during: {high_water['rss']:.1f} MB (+{high_water['rss']-baseline:.1f} MB)")
    ok = compare(expected, streamed)
    await app.stop()
    return ok


def phase_archive(source: Path, workdir: Path, source_gib: float) -> Path | None:
    print("\n=== PHASE 2: create an archive on disk ===")
    require_space(workdir, source_gib, 50.0, "archive creation")
    target = workdir / "cs2.tar"
    started = time.time()
    size = A.create_archive(source, target, fmt="tar")
    elapsed = max(time.time() - started, 1e-9)
    print(
        f"  wrote {target.name}: {size/GIB:.2f} GiB in {elapsed:.0f}s "
        f"({size/elapsed/1024/1024:.0f} MB/s)"
    )
    print(f"  detect_format -> {A.detect_format(target)}")
    print(f"  peak RSS so far {peak_rss():.1f} MB")
    return target


def phase_extract(archive_path: Path, workdir: Path, expected: dict[str, str], source_gib: float) -> bool:
    print("\n=== PHASE 3: extract the archive and verify every file ===")
    require_space(workdir, source_gib, 50.0, "extraction")
    destination = workdir / "extracted"
    # Real game data, but still extracted under the hostile-input policy --
    # the point is that a legitimate 66 GiB folder is not tripped up by the
    # defences meant for malicious archives.
    policy = A.ArchivePolicy(
        max_entries=200_000,
        max_entry_bytes=16 * GIB,
        max_total_bytes=200 * GIB,
    )
    started = time.time()
    report = A.extract_archive(archive_path, destination, policy=policy)
    elapsed = max(time.time() - started, 1e-9)
    print(
        f"  extracted {report.entries:,} files ({report.bytes_written/GIB:.2f} GiB) "
        f"in {elapsed:.0f}s ({report.bytes_written/elapsed/1024/1024:.0f} MB/s)"
    )
    print(f"  directories created: {report.directories:,}")
    print(f"  peak RSS so far {peak_rss():.1f} MB")
    actual = hash_tree(destination, "extracted tree")
    return compare(expected, actual)


def cleanup(workdir: Path, source: Path) -> None:
    print("\n=== CLEANUP ===")
    resolved_work = workdir.resolve()
    resolved_source = source.resolve()
    # Paranoia: never delete anything that is, or contains, the source.
    if resolved_source == resolved_work or resolved_work in resolved_source.parents:
        raise SystemExit(f"REFUSING to delete {resolved_work}: it holds the source folder")
    before = free_gib(workdir if workdir.exists() else workdir.parent)
    if resolved_work.exists():
        shutil.rmtree(resolved_work, ignore_errors=True)
    after = free_gib(resolved_work.parent)
    print(f"  removed {resolved_work}")
    print(f"  free space {before:.1f} GiB -> {after:.1f} GiB (reclaimed {after-before:.1f} GiB)")
    print(f"  source untouched: {resolved_source} exists={resolved_source.is_dir()}")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--phases", default="stream,archive,extract")
    parser.add_argument("--format", default="tar")
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    args = parser.parse_args()

    source = Path(args.source)
    workdir = Path(args.workdir)
    phases = {p.strip() for p in args.phases.split(",") if p.strip()}

    if not source.is_dir():
        raise SystemExit(f"no such folder: {source}")

    print("=" * 66)
    print(f" source : {source}")
    print(f" workdir: {workdir}")
    print(f" phases : {', '.join(sorted(phases))}")
    print("=" * 66)

    count, total = A.folder_size(source)
    source_gib = total / GIB
    print(f"  source: {count:,} files, {source_gib:.2f} GiB")
    print(f"  free  : {free_gib(workdir):.1f} GiB")

    workdir.mkdir(parents=True, exist_ok=True)

    expected = hash_tree(source, "source tree")
    if len(expected) != count:
        print(f"  note: {count-len(expected)} files were unreadable and are excluded")

    results: dict[str, bool] = {}
    archive_path: Path | None = None
    try:
        if "stream" in phases:
            results["stream"] = await phase_stream(source, expected, args.format)
        if "archive" in phases:
            archive_path = phase_archive(source, workdir, source_gib)
            results["archive"] = archive_path is not None
        if "extract" in phases:
            if archive_path is None:
                archive_path = workdir / "cs2.tar"
            if not archive_path.is_file():
                print("\n  skipping extract: no archive present")
            else:
                results["extract"] = phase_extract(archive_path, workdir, expected, source_gib)
    finally:
        if not args.keep:
            cleanup(workdir, source)

    print("\n" + "=" * 66)
    for name, ok in results.items():
        print(f"  {name:<10} {'PASS' if ok else 'FAIL'}")
    print(f"  peak RSS across the whole run: {peak_rss():.1f} MB")
    print("=" * 66)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
