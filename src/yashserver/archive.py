"""Archive handling and whole-folder transfer.

Supported formats
=================

============  ======  ======  =========================================
format        read    write   notes
============  ======  ======  =========================================
zip           yes     yes     stdlib
tar           yes     yes     stdlib
tar.gz / tgz  yes     yes     stdlib
tar.bz2       yes     yes     stdlib
tar.xz        yes     yes     stdlib
rar           yes     **no**  needs ``rarfile`` + a system extractor
============  ======  ======  =========================================

RAR is deliberately read-only. The only RAR *encoder* is proprietary
software, so writing RAR cannot be supported without a non-free dependency.
Extraction works with the free ``unar``/``bsdtar`` tools that ``rarfile``
drives, so reading is the honest ceiling.

Threat model
============

An archive is attacker-controlled input, and every field in it is a lie until
checked. This module defends against:

* **path traversal** -- ``../``, absolute paths, Windows drive letters and UNC
  paths, backslash separators, and names that only escape after normalisation
* **link escapes** -- symlink and hardlink members pointing outside the
  destination (rejected outright by default)
* **special files** -- devices, FIFOs and sockets, which have no business in a
  transferred folder
* **zip bombs** -- caps on entry count, per-entry size, total uncompressed
  size and compression ratio, all enforced *while* decompressing rather than
  by trusting the header, because the header is attacker-controlled too
* **name-based attacks** -- NUL bytes, control characters, over-long names,
  excessive nesting, and Windows reserved device names such as ``CON``

Limits live in :class:`ArchivePolicy`. The defaults are deliberately strict;
raise them consciously for a trusted workload.
"""

from __future__ import annotations

import asyncio
import io
import os
import posixpath
import stat
import tarfile
import threading
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, AsyncIterator, Callable, Iterable, Iterator, Sequence

from .core import YServerError

__all__ = [
    "ArchiveError",
    "ArchiveFormat",
    "ArchivePolicy",
    "ExtractReport",
    "UnsafeArchiveError",
    "create_archive",
    "detect_format",
    "extract_archive",
    "folder_archive_stream",
    "iter_folder_entries",
    "safe_member_path",
]


class ArchiveError(YServerError):
    """An archive could not be read, written or understood."""


class UnsafeArchiveError(ArchiveError):
    """An archive was rejected because extracting it would be unsafe.

    Carries the offending member so a caller can log precisely what was
    refused without having to re-parse the archive.
    """

    def __init__(self, reason: str, member: str | None = None) -> None:
        self.reason = reason
        self.member = member
        super().__init__(f"{reason}" + (f" (member: {member!r})" if member else ""))


# ---------------------------------------------------------------------------
# formats
# ---------------------------------------------------------------------------


class ArchiveFormat(str):
    """Known archive formats, as plain strings so they serialise trivially."""

    ZIP = "zip"
    TAR = "tar"
    TAR_GZ = "tar.gz"
    TAR_BZ2 = "tar.bz2"
    TAR_XZ = "tar.xz"
    RAR = "rar"


#: Formats this module can create. RAR is absent on purpose -- see module docs.
WRITABLE_FORMATS = (
    ArchiveFormat.ZIP,
    ArchiveFormat.TAR,
    ArchiveFormat.TAR_GZ,
    ArchiveFormat.TAR_BZ2,
    ArchiveFormat.TAR_XZ,
)
READABLE_FORMATS = WRITABLE_FORMATS + (ArchiveFormat.RAR,)

_TAR_MODES = {
    ArchiveFormat.TAR: "",
    ArchiveFormat.TAR_GZ: "gz",
    ArchiveFormat.TAR_BZ2: "bz2",
    ArchiveFormat.TAR_XZ: "xz",
}

_SUFFIXES: tuple[tuple[str, str], ...] = (
    (".tar.gz", ArchiveFormat.TAR_GZ),
    (".tgz", ArchiveFormat.TAR_GZ),
    (".tar.bz2", ArchiveFormat.TAR_BZ2),
    (".tbz2", ArchiveFormat.TAR_BZ2),
    (".tar.xz", ArchiveFormat.TAR_XZ),
    (".txz", ArchiveFormat.TAR_XZ),
    (".tar", ArchiveFormat.TAR),
    (".zip", ArchiveFormat.ZIP),
    (".rar", ArchiveFormat.RAR),
)

#: Magic numbers, checked before the filename is believed.
_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"Rar!\x1a\x07", ArchiveFormat.RAR),
    (b"PK\x03\x04", ArchiveFormat.ZIP),
    (b"PK\x05\x06", ArchiveFormat.ZIP),  # empty zip
    (b"\x1f\x8b", ArchiveFormat.TAR_GZ),
    (b"BZh", ArchiveFormat.TAR_BZ2),
    (b"\xfd7zXZ\x00", ArchiveFormat.TAR_XZ),
)


def detect_format(path: str | os.PathLike[str], *, sniff: bool = True) -> str:
    """Work out an archive's format.

    Content is trusted over the filename: a ``.zip`` that is really a RAR is
    reported as RAR, because the extension is attacker-controlled whenever the
    archive is. Falls back to the suffix when there is no usable magic (plain
    ``.tar`` has none at offset 0).
    """

    path = Path(path)
    if sniff:
        try:
            with open(path, "rb") as handle:
                head = handle.read(262)
        except OSError:
            head = b""
        for magic, fmt in _MAGIC:
            if head.startswith(magic):
                return fmt
        # POSIX tar keeps "ustar" at offset 257.
        if len(head) >= 262 and head[257:262] == b"ustar":
            return ArchiveFormat.TAR

    name = path.name.lower()
    for suffix, fmt in _SUFFIXES:
        if name.endswith(suffix):
            return fmt
    raise ArchiveError(f"cannot determine archive format for {path.name!r}")


# ---------------------------------------------------------------------------
# policy
# ---------------------------------------------------------------------------


@dataclass
class ArchivePolicy:
    """Limits applied while reading an archive.

    Defaults suit untrusted input. Every size is in bytes.
    """

    #: Maximum number of members.
    max_entries: int = 100_000
    #: Maximum uncompressed size of any single member.
    max_entry_bytes: int = 8 * 1024 ** 3
    #: Maximum uncompressed size of the whole archive.
    max_total_bytes: int = 64 * 1024 ** 3
    #: Maximum uncompressed:compressed ratio, the classic zip-bomb signal.
    #: Checked per entry once an entry is big enough for the ratio to mean
    #: something, and again across the archive as a whole.
    max_compression_ratio: float = 200.0
    #: Ratio checking starts only after this much output, so that tiny highly
    #: compressible files do not trip it.
    ratio_check_threshold: int = 4 * 1024 * 1024
    #: Maximum path depth of a member.
    max_depth: int = 64
    #: Maximum length of a single path component.
    max_name_length: int = 255
    #: Maximum length of a member's whole path.
    max_path_length: int = 4096
    #: Follow symlink/hardlink members. Off by default: a link is the easiest
    #: way out of a destination directory.
    allow_links: bool = False
    #: Permit device/FIFO/socket members. Off by default.
    allow_special_files: bool = False
    #: Reject names that are reserved device names on Windows, regardless of
    #: the host, so an archive cannot be extracted on Linux and then break when
    #: the folder is later copied to Windows.
    reject_windows_reserved: bool = True
    #: Read/write buffer.
    chunk_size: int = 1024 * 1024

    def validate(self) -> None:
        if self.max_entries <= 0 or self.max_entry_bytes <= 0 or self.max_total_bytes <= 0:
            raise ValueError("archive limits must be positive")
        if self.max_compression_ratio <= 1:
            raise ValueError("max_compression_ratio must be greater than 1")


# Windows refuses these as filenames whatever the extension.
_WINDOWS_RESERVED = frozenset(
    ["con", "prn", "aux", "nul"]
    + [f"com{i}" for i in range(1, 10)]
    + [f"lpt{i}" for i in range(1, 10)]
)


def safe_member_path(name: str, policy: ArchivePolicy | None = None) -> PurePosixPath:
    """Turn an archive member name into a safe relative path, or refuse it.

    Refusing is the point. This never "repairs" a hostile name by stripping
    ``..`` -- silently rewriting an attacker's path is how you end up
    extracting somewhere surprising. Anything suspicious raises
    :class:`UnsafeArchiveError`.
    """

    policy = policy or ArchivePolicy()

    if not name or not name.strip():
        raise UnsafeArchiveError("empty member name", name)
    if len(name) > policy.max_path_length:
        raise UnsafeArchiveError("member path too long", name)
    if "\x00" in name:
        raise UnsafeArchiveError("member name contains a NUL byte", name)
    if any(ord(ch) < 32 for ch in name):
        raise UnsafeArchiveError("member name contains control characters", name)

    # Zip entries in the wild use both separators; normalise before judging.
    normalised = name.replace("\\", "/")

    if normalised.startswith("/"):
        raise UnsafeArchiveError("absolute member path", name)
    # C:\..., C:/... and UNC \\server\share
    if len(normalised) >= 2 and normalised[1] == ":" and normalised[0].isalpha():
        raise UnsafeArchiveError("member path has a drive letter", name)
    if normalised.startswith("//"):
        raise UnsafeArchiveError("UNC member path", name)

    parts: list[str] = []
    for part in normalised.split("/"):
        if part in ("", "."):
            continue  # harmless noise
        if part == "..":
            raise UnsafeArchiveError("member path escapes the destination", name)
        if len(part) > policy.max_name_length:
            raise UnsafeArchiveError("member path component too long", name)
        # Trailing dots and spaces are silently stripped by Windows, which
        # turns "evil. " into "evil" and can collide with a real file.
        if part != part.rstrip(". "):
            raise UnsafeArchiveError("member name has trailing dots or spaces", name)
        if policy.reject_windows_reserved:
            stem = part.split(".", 1)[0].lower()
            if stem in _WINDOWS_RESERVED:
                raise UnsafeArchiveError(f"member uses reserved device name {stem!r}", name)
        parts.append(part)

    if not parts:
        raise UnsafeArchiveError("member path is empty after normalisation", name)
    if len(parts) > policy.max_depth:
        raise UnsafeArchiveError("member path is nested too deeply", name)

    candidate = PurePosixPath(*parts)
    # Belt and braces: normalising must not have reintroduced an escape.
    if posixpath.normpath(str(candidate)).startswith(("..", "/")):
        raise UnsafeArchiveError("member path escapes after normalisation", name)
    return candidate


def _resolve_within(destination: Path, relative: PurePosixPath, member: str) -> Path:
    """Join and confirm the result really is inside ``destination``.

    ``safe_member_path`` has already vetted the name; this catches what a name
    alone cannot -- an existing symlink *in the destination* redirecting the
    write elsewhere.
    """

    target = (destination / Path(*relative.parts)).resolve()
    root = destination.resolve()
    if target != root and root not in target.parents:
        raise UnsafeArchiveError("member resolves outside the destination", member)
    return target


# ---------------------------------------------------------------------------
# extraction
# ---------------------------------------------------------------------------


@dataclass
class ExtractReport:
    """What an extraction actually did."""

    entries: int = 0
    bytes_written: int = 0
    directories: int = 0
    skipped: list[str] = field(default_factory=list)
    format: str = ""
    destination: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "destination": self.destination,
            "entries": self.entries,
            "directories": self.directories,
            "bytes_written": self.bytes_written,
            "skipped": list(self.skipped),
        }


class _Accountant:
    """Tracks totals during extraction and trips the limits."""

    def __init__(self, policy: ArchivePolicy, compressed_size: int) -> None:
        self.policy = policy
        self.compressed_size = max(1, compressed_size)
        self.total_out = 0
        self.entries = 0

    def add_entry(self, member: str) -> None:
        self.entries += 1
        if self.entries > self.policy.max_entries:
            raise UnsafeArchiveError(
                f"archive has more than {self.policy.max_entries} entries", member
            )

    def add_bytes(self, count: int, member: str, entry_total: int, entry_packed: int) -> None:
        self.total_out += count
        if entry_total > self.policy.max_entry_bytes:
            raise UnsafeArchiveError(
                f"member exceeds {self.policy.max_entry_bytes} bytes", member
            )
        if self.total_out > self.policy.max_total_bytes:
            raise UnsafeArchiveError(
                f"archive expands beyond {self.policy.max_total_bytes} bytes", member
            )
        # Per-entry ratio, once the entry is large enough for it to be meaningful.
        if entry_total >= self.policy.ratio_check_threshold and entry_packed > 0:
            ratio = entry_total / entry_packed
            if ratio > self.policy.max_compression_ratio:
                raise UnsafeArchiveError(
                    f"member compression ratio {ratio:.0f}:1 exceeds "
                    f"{self.policy.max_compression_ratio:.0f}:1",
                    member,
                )
        # Whole-archive ratio.
        if self.total_out >= self.policy.ratio_check_threshold:
            ratio = self.total_out / self.compressed_size
            if ratio > self.policy.max_compression_ratio:
                raise UnsafeArchiveError(
                    f"archive compression ratio {ratio:.0f}:1 exceeds "
                    f"{self.policy.max_compression_ratio:.0f}:1",
                    member,
                )


def _write_stream(
    source: Any,
    target: Path,
    *,
    member: str,
    packed: int,
    accountant: _Accountant,
    policy: ArchivePolicy,
) -> int:
    """Copy a member to disk, enforcing limits as the bytes arrive.

    The declared size is never trusted; only what actually decompresses counts.
    A partial file is removed if a limit trips mid-write, so a rejected archive
    does not leave half a bomb on disk.
    """

    written = 0
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(target, "wb") as out:
            while True:
                chunk = source.read(policy.chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                accountant.add_bytes(len(chunk), member, written, packed)
                out.write(chunk)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return written


def _extract_zip(
    path: Path, destination: Path, policy: ArchivePolicy, report: ExtractReport
) -> None:
    with zipfile.ZipFile(path) as archive:
        accountant = _Accountant(policy, path.stat().st_size)
        for info in archive.infolist():
            accountant.add_entry(info.filename)
            relative = safe_member_path(info.filename, policy)

            # Zip stores unix mode in the top 16 bits of external_attr.
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode) and not policy.allow_links:
                raise UnsafeArchiveError("symlink member", info.filename)

            target = _resolve_within(destination, relative, info.filename)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                report.directories += 1
                continue

            with archive.open(info) as source:
                written = _write_stream(
                    source,
                    target,
                    member=info.filename,
                    packed=info.compress_size,
                    accountant=accountant,
                    policy=policy,
                )
            report.entries += 1
            report.bytes_written += written


def _extract_tar(
    path: Path, destination: Path, policy: ArchivePolicy, report: ExtractReport, fmt: str
) -> None:
    mode = f"r:{_TAR_MODES.get(fmt, '')}" if fmt in _TAR_MODES else "r:*"
    with tarfile.open(path, mode) as archive:
        accountant = _Accountant(policy, path.stat().st_size)
        for member in archive:
            accountant.add_entry(member.name)
            relative = safe_member_path(member.name, policy)
            target = _resolve_within(destination, relative, member.name)

            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                report.directories += 1
                continue

            if member.issym() or member.islnk():
                if not policy.allow_links:
                    raise UnsafeArchiveError("link member", member.name)
                # Even when links are allowed, the target must stay inside.
                link = member.linkname.replace("\\", "/")
                if link.startswith("/"):
                    raise UnsafeArchiveError("absolute link target", member.name)
                combined = posixpath.normpath(
                    posixpath.join(posixpath.dirname(str(relative)), link)
                )
                if combined.startswith(".."):
                    raise UnsafeArchiveError("link target escapes destination", member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                if member.issym():
                    target.symlink_to(link)
                else:
                    source_path = _resolve_within(
                        destination, safe_member_path(member.linkname, policy), member.name
                    )
                    os.link(source_path, target)
                report.entries += 1
                continue

            if not member.isfile():
                if not policy.allow_special_files:
                    raise UnsafeArchiveError(
                        "special file member (device, fifo or socket)", member.name
                    )
                report.skipped.append(member.name)
                continue

            source = archive.extractfile(member)
            if source is None:
                report.skipped.append(member.name)
                continue
            with source:
                written = _write_stream(
                    source,
                    target,
                    member=member.name,
                    # tar members are compressed archive-wide, not per entry,
                    # so per-entry ratio is not meaningful here; the
                    # archive-wide ratio still applies.
                    packed=0,
                    accountant=accountant,
                    policy=policy,
                )
            report.entries += 1
            report.bytes_written += written


def _windows_rar_backends() -> list[tuple[str, str]]:
    """Locate a RAR extractor on Windows, as ``(rarfile attribute, path)``.

    ``rarfile`` looks for a bare ``unrar`` on ``PATH``, which is a Unix
    assumption: WinRAR installs ``UnRAR.exe`` into Program Files and does not
    put it on ``PATH``. Without this, RAR support is dead on Windows even when
    WinRAR is installed.
    """

    roots: list[Path] = []
    for variable in ("ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"):
        value = os.environ.get(variable)
        if value:
            roots.append(Path(value))

    # WinRAR records its install location in the registry; prefer that over
    # guessing, since it may not be under Program Files.
    try:
        import winreg  # type: ignore

        for hive, key in (
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WinRAR"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\WinRAR"),
            (winreg.HKEY_CURRENT_USER, r"SOFTWARE\WinRAR"),
        ):
            try:
                with winreg.OpenKey(hive, key) as handle:
                    for name in ("exe64", "exe32", ""):
                        try:
                            value, _kind = winreg.QueryValueEx(handle, name)
                        except OSError:
                            continue
                        if value:
                            roots.append(Path(value).parent.parent)
                            roots.append(Path(value).parent)
            except OSError:
                continue
    except ImportError:
        pass

    candidates: list[tuple[str, Path]] = []
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        candidates.append(("UNRAR_TOOL", root / "WinRAR" / "UnRAR.exe"))
        candidates.append(("UNRAR_TOOL", root / "UnRAR.exe"))
        candidates.append(("SEVENZIP_TOOL", root / "7-Zip" / "7z.exe"))
    return [(attribute, str(path)) for attribute, path in candidates if path.is_file()]


def _rarfile_module():
    try:
        import rarfile  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise ArchiveError(
            "reading RAR needs the 'rarfile' package (pip install rarfile) plus a "
            "system extractor such as 'unar', 'bsdtar' or WinRAR's UnRAR.exe"
        ) from exc

    # The happy path: a tool is already on PATH, which is the norm on Linux.
    try:
        rarfile.tool_setup()
        return rarfile
    except Exception:
        pass

    for attribute, path in _windows_rar_backends():
        previous = getattr(rarfile, attribute, None)
        setattr(rarfile, attribute, path)
        try:
            rarfile.tool_setup(force=True)
            return rarfile
        except Exception:
            setattr(rarfile, attribute, previous)
            continue

    raise ArchiveError(
        "no working RAR extractor found. Install one of: 'unar' or 'bsdtar' "
        "(Linux/macOS), or WinRAR/UnRAR.exe (Windows). RAR extraction shells "
        "out to one of these because there is no free RAR decoder library."
    )


def _extract_rar(
    path: Path, destination: Path, policy: ArchivePolicy, report: ExtractReport
) -> None:
    rarfile = _rarfile_module()
    try:
        archive = rarfile.RarFile(path)
    except Exception as exc:  # rarfile raises a family of its own errors
        raise ArchiveError(f"cannot open RAR archive: {exc}") from exc
    with archive:
        try:
            members = archive.infolist()
        except Exception as exc:
            raise ArchiveError(f"cannot read RAR index: {exc}") from exc
        if not members:
            # rarfile accepts any file carrying the RAR signature and reports
            # it as empty rather than raising, so a truncated or corrupt
            # archive would otherwise "extract" successfully with nothing in
            # it -- the worst kind of silent failure for a file transfer. A
            # real RAR always has at least one member, so treat zero as
            # corrupt. This does reject a deliberately empty RAR, which is a
            # trade worth making.
            raise ArchiveError(
                "RAR archive has no readable entries; it is truncated or corrupt"
            )
        accountant = _Accountant(policy, path.stat().st_size)
        for info in members:
            accountant.add_entry(info.filename)
            relative = safe_member_path(info.filename, policy)
            target = _resolve_within(destination, relative, info.filename)
            if info.isdir():
                target.mkdir(parents=True, exist_ok=True)
                report.directories += 1
                continue
            try:
                source = archive.open(info)
            except Exception as exc:
                raise ArchiveError(f"cannot read RAR member {info.filename!r}: {exc}") from exc
            with source:
                written = _write_stream(
                    source,
                    target,
                    member=info.filename,
                    packed=getattr(info, "compress_size", 0) or 0,
                    accountant=accountant,
                    policy=policy,
                )
            report.entries += 1
            report.bytes_written += written


def extract_archive(
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    policy: ArchivePolicy | None = None,
    fmt: str | None = None,
) -> ExtractReport:
    """Extract ``path`` into ``destination``, refusing anything unsafe.

    Raises :class:`UnsafeArchiveError` on the first member that violates
    ``policy``; extraction stops there rather than continuing with the rest.
    """

    policy = policy or ArchivePolicy()
    policy.validate()
    archive_path = Path(path)
    if not archive_path.is_file():
        raise ArchiveError(f"no such archive: {archive_path}")

    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)

    resolved = fmt or detect_format(archive_path)
    report = ExtractReport(format=resolved, destination=str(destination_path))

    if resolved == ArchiveFormat.ZIP:
        _extract_zip(archive_path, destination_path, policy, report)
    elif resolved in _TAR_MODES:
        _extract_tar(archive_path, destination_path, policy, report, resolved)
    elif resolved == ArchiveFormat.RAR:
        _extract_rar(archive_path, destination_path, policy, report)
    else:
        raise ArchiveError(f"unsupported archive format: {resolved}")
    return report


# ---------------------------------------------------------------------------
# folder walking and archive creation
# ---------------------------------------------------------------------------


def iter_folder_entries(
    root: str | os.PathLike[str],
    *,
    follow_symlinks: bool = False,
    exclude: Callable[[Path], bool] | None = None,
) -> Iterator[tuple[Path, PurePosixPath]]:
    """Yield ``(absolute_path, archive_name)`` for every file under ``root``.

    Order is depth-first with names sorted at each level, which makes
    archives reproducible without materialising the whole tree in memory.
    Symlinks are skipped unless
    ``follow_symlinks`` is set, and even then anything pointing outside
    ``root`` is skipped rather than followed out of the tree.
    """

    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise ArchiveError(f"not a directory: {root_path}")

    for current, dirnames, filenames in os.walk(root_path, followlinks=follow_symlinks):
        dirnames.sort()
        filenames.sort()
        current_path = Path(current)
        for name in filenames:
            absolute = current_path / name
            if absolute.is_symlink() and not follow_symlinks:
                continue
            try:
                resolved = absolute.resolve()
                if follow_symlinks and root_path not in resolved.parents:
                    continue
                if not absolute.is_file():
                    continue
            except OSError:
                continue
            if exclude is not None and exclude(absolute):
                continue
            relative = absolute.relative_to(root_path)
            yield absolute, PurePosixPath(*relative.parts)


def folder_size(root: str | os.PathLike[str]) -> tuple[int, int]:
    """Return ``(file_count, total_bytes)`` for a folder. Cheap pre-flight."""

    count = 0
    total = 0
    for absolute, _name in iter_folder_entries(root):
        try:
            total += absolute.stat().st_size
            count += 1
        except OSError:
            continue
    return count, total


class _StreamSink(io.RawIOBase):
    """A write-only file object that hands finished chunks to a callback."""

    def __init__(self, emit: Callable[[bytes], None]) -> None:
        self._emit = emit
        self._position = 0

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:  # type: ignore[override]
        payload = bytes(data)
        if payload:
            self._position += len(payload)
            self._emit(payload)
        return len(payload)

    def tell(self) -> int:
        # zipfile asks for this; it must reflect bytes emitted so far.
        return self._position


def _build_archive_into(
    sink: Any,
    root: Path,
    fmt: str,
    *,
    exclude: Callable[[Path], bool] | None,
    chunk_size: int,
) -> None:
    """Write an archive of ``root`` into the file-like ``sink``."""

    if fmt == ArchiveFormat.ZIP:
        # ZIP_STORED keeps this streamable and is the right call for game
        # assets, which are already compressed.
        with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
            for absolute, name in iter_folder_entries(root, exclude=exclude):
                zf.write(absolute, str(name))
    elif fmt in _TAR_MODES:
        mode = f"w|{_TAR_MODES[fmt]}"  # the stream form: never seeks
        with tarfile.open(fileobj=sink, mode=mode, bufsize=chunk_size) as tf:
            for absolute, name in iter_folder_entries(root, exclude=exclude):
                try:
                    info = tf.gettarinfo(str(absolute), arcname=str(name))
                except OSError:
                    continue
                if not info.isfile():
                    continue
                try:
                    with open(absolute, "rb") as handle:
                        tf.addfile(info, handle)
                except OSError:
                    continue
    else:
        raise ArchiveError(f"cannot create {fmt} archives")


def create_archive(
    root: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    fmt: str | None = None,
    exclude: Callable[[Path], bool] | None = None,
    chunk_size: int = 1024 * 1024,
) -> int:
    """Archive a folder to a file. Returns the archive's size in bytes."""

    root_path = Path(root)
    destination_path = Path(destination)
    resolved = fmt or _format_from_name(destination_path.name)
    if resolved not in WRITABLE_FORMATS:
        raise ArchiveError(
            f"cannot create {resolved} archives"
            + (" (RAR encoding requires proprietary software)" if resolved == ArchiveFormat.RAR else "")
        )
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with open(destination_path, "wb") as handle:
        _build_archive_into(handle, root_path, resolved, exclude=exclude, chunk_size=chunk_size)
    return destination_path.stat().st_size


def _format_from_name(name: str) -> str:
    lowered = name.lower()
    for suffix, fmt in _SUFFIXES:
        if lowered.endswith(suffix):
            return fmt
    raise ArchiveError(f"cannot infer archive format from {name!r}")


# ---------------------------------------------------------------------------
# streaming a folder over the network
# ---------------------------------------------------------------------------


_SENTINEL = object()


async def folder_archive_stream(
    root: str | os.PathLike[str],
    *,
    fmt: str = ArchiveFormat.TAR_GZ,
    chunk_size: int = 1024 * 1024,
    queue_size: int = 8,
    exclude: Callable[[Path], bool] | None = None,
) -> AsyncIterator[bytes]:
    """Stream a folder as an archive, without building it on disk first.

    The archive is produced by a worker thread and handed over through a
    bounded queue, which is what makes this usable for a folder far larger
    than memory: file I/O and compression stay off the event loop, and a slow
    client applies real backpressure once ``queue_size`` chunks are in flight,
    because the worker blocks on a full queue.
    """

    if fmt not in WRITABLE_FORMATS:
        raise ArchiveError(f"cannot stream {fmt} archives")

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(1, queue_size))
    root_path = Path(root)
    # Signals the worker to stop early when the consumer goes away.
    cancelled = threading.Event()

    def emit(payload: bytes) -> None:
        if cancelled.is_set():
            raise _StreamAborted()
        # Block the worker until the consumer keeps up. This is the backpressure.
        asyncio.run_coroutine_threadsafe(queue.put(payload), loop).result()

    def worker() -> None:
        try:
            _build_archive_into(
                _StreamSink(emit), root_path, fmt, exclude=exclude, chunk_size=chunk_size
            )
        except _StreamAborted:
            pass
        except BaseException as error:  # surfaced to the consumer below
            asyncio.run_coroutine_threadsafe(queue.put(error), loop).result()
        finally:
            try:
                asyncio.run_coroutine_threadsafe(queue.put(_SENTINEL), loop).result()
            except Exception:
                pass

    task = loop.run_in_executor(None, worker)
    try:
        while True:
            item = await queue.get()
            if item is _SENTINEL:
                break
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        cancelled.set()
        # Drain so a blocked worker can finish and the thread is not leaked.
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            await task
        except Exception:
            pass


class _StreamAborted(Exception):
    """Raised inside the worker thread when the consumer disconnects."""
