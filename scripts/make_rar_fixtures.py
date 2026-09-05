"""Generate the RAR test fixtures.

RAR cannot be created by free software, so these fixtures are not checked in
and cannot be produced in CI. This script drives a locally installed RAR
encoder (WinRAR's ``Rar.exe``, or ``rar`` on Unix) to build them.

    python scripts/make_rar_fixtures.py --outdir C:\\yashserver-testdata\\rarfixtures

Then run the RAR tests against them::

    set YSERVER_RAR_FIXTURE=...\\fixture_rar5.rar
    set YSERVER_RAR_SOURCE=...\\src
    set YSERVER_RAR_BOMB=...\\bomb.rar
    python -m unittest tests.test_archive

Note that WinRAR is needed only to *produce* these fixtures. yashserver itself
never requires a RAR encoder -- it only reads RAR, via unar/bsdtar/UnRAR.
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import subprocess
import sys
from pathlib import Path


def find_encoder() -> str:
    """Locate a RAR encoder, or explain why there isn't one."""

    on_path = shutil.which("rar") or shutil.which("Rar")
    if on_path:
        return on_path
    for variable in ("ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"):
        root = os.environ.get(variable)
        if not root:
            continue
        candidate = Path(root) / "WinRAR" / "Rar.exe"
        if candidate.is_file():
            return str(candidate)
    raise SystemExit(
        "No RAR encoder found. Install WinRAR (Windows) or the proprietary "
        "'rar' binary (Unix). There is no free RAR encoder, which is exactly "
        "why yashserver only reads RAR and never writes it."
    )


def build_source(root: Path) -> None:
    """A small tree with varied content: text, incompressible, compressible."""

    if root.exists():
        shutil.rmtree(root)
    (root / "sub" / "deep").mkdir(parents=True)
    (root / "alpha.txt").write_bytes(b"alpha content for rar fixture")
    (root / "sub" / "beta.txt").write_bytes(b"beta nested content")
    rng = random.Random(1234)
    (root / "binary.bin").write_bytes(bytes(rng.getrandbits(8) for _ in range(200_000)))
    rng2 = random.Random(99)
    (root / "sub" / "deep" / "gamma.bin").write_bytes(
        bytes(rng2.getrandbits(8) for _ in range(100_000))
    )
    # Legitimately very compressible: proves the ratio check does not
    # false-positive on honest data.
    (root / "sub" / "zeros.bin").write_bytes(b"\x00" * 2_000_000)


def run_rar(encoder: str, args: list[str], cwd: Path) -> None:
    result = subprocess.run(
        [encoder, *args], cwd=str(cwd), capture_output=True, text=True
    )
    if result.returncode != 0:
        raise SystemExit(
            f"rar failed ({result.returncode}): {result.stdout[-400:]} {result.stderr[-400:]}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", required=True)
    parser.add_argument(
        "--bomb-mb", type=int, default=300, help="uncompressed size of the bomb fixture"
    )
    args = parser.parse_args()

    encoder = find_encoder()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    print(f"encoder: {encoder}")

    source = outdir / "src"
    build_source(source)
    print(f"source tree: {source}")

    good = outdir / "fixture_rar5.rar"
    good.unlink(missing_ok=True)
    run_rar(encoder, ["a", "-r", "-ep1", "-y", str(good), "."], source)
    print(f"  {good.name}: {good.stat().st_size:,} bytes")

    # A bomb, to prove the ratio and size caps fire on RAR too.
    bomb_src = outdir / "_bomb_src"
    if bomb_src.exists():
        shutil.rmtree(bomb_src)
    bomb_src.mkdir(parents=True)
    block = b"\x00" * (1024 * 1024)
    with open(bomb_src / "zeros.bin", "wb") as handle:
        for _ in range(args.bomb_mb):
            handle.write(block)
    bomb = outdir / "bomb.rar"
    bomb.unlink(missing_ok=True)
    run_rar(encoder, ["a", "-ep1", "-m5", "-y", str(bomb), "."], bomb_src)
    shutil.rmtree(bomb_src)
    uncompressed = args.bomb_mb * 1024 * 1024
    print(
        f"  {bomb.name}: {bomb.stat().st_size:,} bytes from {uncompressed:,} "
        f"({uncompressed / bomb.stat().st_size:,.0f}:1)"
    )

    print("\nexport these before running the tests:")
    prefix = "set" if sys.platform == "win32" else "export"
    joiner = "=" if sys.platform == "win32" else "="
    print(f"  {prefix} YSERVER_RAR_FIXTURE{joiner}{good}")
    print(f"  {prefix} YSERVER_RAR_SOURCE{joiner}{source}")
    print(f"  {prefix} YSERVER_RAR_BOMB{joiner}{bomb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
