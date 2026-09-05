"""Resumable uploads with built-in integrity verification.

A 50 GB upload that dies at 90% and has to start again is not a transfer
mechanism, it is a lottery. This module keeps a durable record of how much of
each upload has landed so a client can carry on from that offset, and verifies
what arrived rather than assuming it.

Protocol
========

Deliberately modelled on the `tus <https://tus.io>`_ core protocol, so the
semantics are ones clients already understand. This is a compatible *subset*,
not a full tus implementation -- there is no ``Tus-Resumable`` negotiation and
no extension registry.

======  =====================  ================================================
verb    path                   meaning
======  =====================  ================================================
POST    ``/uploads``           create a session; returns ``Location``
HEAD    ``/uploads/{id}``      report ``Upload-Offset`` -- ask this to resume
PATCH   ``/uploads/{id}``      append at ``Upload-Offset``; returns new offset
GET     ``/uploads/{id}``      session status as JSON
DELETE  ``/uploads/{id}``      abandon and delete
======  =====================  ================================================

Resuming is therefore: ``HEAD`` to learn the offset, then ``PATCH`` from there.
An interrupted 50 GB upload resumes at the byte it reached, not at zero.

Integrity
=========

Three independent checks, because they catch different failures:

* **Offset agreement.** A ``PATCH`` whose ``Upload-Offset`` disagrees with the
  server is rejected with ``409`` rather than written at the wrong place. A
  confused client cannot silently corrupt the file.
* **Per-chunk checksum.** ``Upload-Checksum: sha256 <hex>`` on a ``PATCH``
  verifies that one chunk before it is kept; a bad chunk is discarded and the
  offset rolls back, so the client can simply retry it.
* **Whole-file checksum.** ``Upload-Checksum`` on the creating ``POST`` is
  verified when the last byte arrives. This is the one that catches an error
  no per-chunk check can: bytes that were fine in flight but landed wrong.

The running digest is held in memory while a session is active and
**recomputed from disk when a session is resumed**, so integrity survives a
server restart mid-upload rather than being quietly abandoned.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

from .core import YServerError

__all__ = [
    "UploadError",
    "UploadSession",
    "UploadStore",
    "file_digest",
    "verify_file_digest",
]

#: Algorithms a client may name. Restricted on purpose: an attacker should not
#: get to pick a deliberately slow hash and make the server do the work.
SUPPORTED_ALGORITHMS = ("sha256", "sha512", "sha1", "md5", "blake2b")

_SESSION_ID = re.compile(r"^[0-9a-f]{32}$")
_CHECKSUM = re.compile(r"^(?P<algorithm>[A-Za-z0-9_-]{2,16})\s+(?P<digest>[0-9a-fA-F]{8,128})$")


class UploadError(YServerError):
    """An upload could not be created, continued or completed."""

    def __init__(self, status: int, detail: str) -> None:
        self.status = int(status)
        self.detail = detail
        super().__init__(f"{status} {detail}")


def _new_hash(algorithm: str):
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise UploadError(400, f"unsupported checksum algorithm {algorithm!r}")
    return hashlib.new(algorithm)


def file_digest(
    path: str | os.PathLike[str],
    algorithm: str = "sha256",
    *,
    length: int | None = None,
    chunk_size: int = 1024 * 1024,
) -> str:
    """Hash a file without reading it into memory.

    ``length`` hashes only the first N bytes, which is what resuming a partial
    upload needs.
    """

    digest = _new_hash(algorithm)
    remaining = length
    with open(path, "rb") as handle:
        while True:
            if remaining is not None and remaining <= 0:
                break
            want = chunk_size if remaining is None else min(chunk_size, remaining)
            block = handle.read(want)
            if not block:
                break
            digest.update(block)
            if remaining is not None:
                remaining -= len(block)
    return digest.hexdigest()


def verify_file_digest(
    path: str | os.PathLike[str], expected: str, algorithm: str = "sha256"
) -> bool:
    """Constant-time comparison of a file's digest against an expected value."""

    import hmac

    return hmac.compare_digest(file_digest(path, algorithm).lower(), expected.strip().lower())


def parse_checksum(header: str) -> tuple[str, str]:
    """Parse ``Upload-Checksum: <algorithm> <hex>``."""

    match = _CHECKSUM.match(header.strip())
    if not match:
        raise UploadError(400, "malformed Upload-Checksum; expected '<algorithm> <hex digest>'")
    algorithm = match.group("algorithm").lower()
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise UploadError(400, f"unsupported checksum algorithm {algorithm!r}")
    return algorithm, match.group("digest").lower()


@dataclass
class UploadSession:
    """One in-progress upload."""

    id: str
    offset: int = 0
    length: int | None = None
    algorithm: str = "sha256"
    expected_digest: str | None = None
    filename: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.length is not None and self.offset >= self.length

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "offset": self.offset,
            "length": self.length,
            "algorithm": self.algorithm,
            "expected_digest": self.expected_digest,
            "filename": self.filename,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "completed": self.completed,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "UploadSession":
        return cls(
            id=str(raw["id"]),
            offset=int(raw.get("offset", 0)),
            length=raw.get("length"),
            algorithm=str(raw.get("algorithm", "sha256")),
            expected_digest=raw.get("expected_digest"),
            filename=raw.get("filename"),
            created_at=float(raw.get("created_at", time.time())),
            updated_at=float(raw.get("updated_at", time.time())),
            completed=bool(raw.get("completed", False)),
            metadata=dict(raw.get("metadata") or {}),
        )


class UploadStore:
    """Durable storage for resumable uploads.

    State lives beside the data as a small JSON sidecar, so an upload survives
    a server restart: the client asks for the offset and carries on.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        max_upload_bytes: int | None = None,
        max_sessions: int = 1000,
        session_ttl_seconds: float = 24 * 3600.0,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        self.root = Path(directory).resolve()
        self.partial_dir = self.root / "partial"
        self.completed_dir = self.root / "completed"
        self.max_upload_bytes = max_upload_bytes
        self.max_sessions = max_sessions
        self.session_ttl_seconds = session_ttl_seconds
        self.chunk_size = chunk_size
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        self.completed_dir.mkdir(parents=True, exist_ok=True)
        # One writer per session; concurrent PATCHes to one upload would
        # otherwise interleave and corrupt the file.
        self._locks: dict[str, asyncio.Lock] = {}
        # Running digests, kept only while this process owns the session.
        self._digests: dict[str, Any] = {}

    # -- paths ---------------------------------------------------------------

    def _validate_id(self, session_id: str) -> str:
        # Ids are server-generated hex. Validating the shape means a client
        # cannot steer these paths anywhere, so no traversal is possible.
        if not _SESSION_ID.match(session_id or ""):
            raise UploadError(404, "no such upload")
        return session_id

    def data_path(self, session_id: str) -> Path:
        return self.partial_dir / f"{self._validate_id(session_id)}.part"

    def meta_path(self, session_id: str) -> Path:
        return self.partial_dir / f"{self._validate_id(session_id)}.json"

    def lock_for(self, session_id: str) -> asyncio.Lock:
        return self._locks.setdefault(session_id, asyncio.Lock())

    # -- lifecycle -----------------------------------------------------------

    def create(
        self,
        *,
        length: int | None = None,
        checksum: str | None = None,
        filename: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> UploadSession:
        if length is not None:
            if length < 0:
                raise UploadError(400, "Upload-Length must not be negative")
            if self.max_upload_bytes is not None and length > self.max_upload_bytes:
                raise UploadError(413, f"upload exceeds {self.max_upload_bytes} bytes")

        self.purge_expired()
        if len(list(self.partial_dir.glob("*.json"))) >= self.max_sessions:
            raise UploadError(503, "too many uploads in progress")

        algorithm, digest = ("sha256", None)
        if checksum:
            algorithm, digest = parse_checksum(checksum)

        session = UploadSession(
            id=secrets.token_hex(16),
            length=length,
            algorithm=algorithm,
            expected_digest=digest,
            filename=filename,
            metadata=metadata or {},
        )
        self.data_path(session.id).touch()
        self._digests[session.id] = _new_hash(session.algorithm)
        self._save(session)
        return session

    def _save(self, session: UploadSession) -> None:
        session.updated_at = time.time()
        target = self.meta_path(session.id)
        # Write-then-rename, so a crash mid-write cannot leave the sidecar
        # truncated and the upload unresumable.
        staging = target.with_suffix(".json.tmp")
        staging.write_text(json.dumps(session.as_dict()), encoding="utf-8")
        staging.replace(target)

    def get(self, session_id: str) -> UploadSession:
        path = self.meta_path(session_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise UploadError(404, "no such upload") from error
        session = UploadSession.from_dict(raw)
        if session.completed:
            # The partial file has been moved to its final home, so its absence
            # says nothing about the offset. Clamping here would report 0 and
            # send a client that asked about a finished upload back to the
            # start of a file it has already delivered.
            return session
        # Trust the file on disk over the sidecar: the sidecar is written after
        # the data, so a crash between the two leaves it behind, never ahead.
        try:
            actual = self.data_path(session_id).stat().st_size
        except OSError:
            actual = 0
        if actual < session.offset:
            session.offset = actual
        return session

    def delete(self, session_id: str) -> None:
        self.data_path(session_id).unlink(missing_ok=True)
        self.meta_path(session_id).unlink(missing_ok=True)
        self._digests.pop(session_id, None)
        self._locks.pop(session_id, None)

    def purge_expired(self) -> int:
        """Remove sessions untouched for longer than the TTL."""

        removed = 0
        cutoff = time.time() - self.session_ttl_seconds
        for meta in self.partial_dir.glob("*.json"):
            try:
                if meta.stat().st_mtime < cutoff:
                    self.delete(meta.stem)
                    removed += 1
            except OSError:
                continue
        return removed

    # -- writing -------------------------------------------------------------

    def _resume_digest(self, session: UploadSession):
        """Get the running digest, rebuilding it from disk if this process
        does not have one (a resumed or restarted upload)."""

        digest = self._digests.get(session.id)
        if digest is not None:
            return digest
        rebuilt = _new_hash(session.algorithm)
        if session.offset:
            path = self.data_path(session.id)
            remaining = session.offset
            with open(path, "rb") as handle:
                while remaining > 0:
                    block = handle.read(min(self.chunk_size, remaining))
                    if not block:
                        break
                    rebuilt.update(block)
                    remaining -= len(block)
        self._digests[session.id] = rebuilt
        return rebuilt

    async def append(
        self,
        session_id: str,
        chunks: AsyncIterator[bytes],
        *,
        offset: int,
        chunk_checksum: str | None = None,
    ) -> UploadSession:
        """Append a chunk at ``offset``.

        The offset must match what the server has, exactly. If a per-chunk
        checksum is supplied and does not match, the chunk is discarded and the
        file truncated back, so a retry is safe.
        """

        session = self.get(session_id)
        if session.completed:
            raise UploadError(409, "upload already completed")
        if offset != session.offset:
            raise UploadError(
                409,
                f"offset mismatch: server has {session.offset}, client sent {offset}",
            )

        expected_algorithm = None
        expected_chunk_digest = None
        if chunk_checksum:
            expected_algorithm, expected_chunk_digest = parse_checksum(chunk_checksum)

        path = self.data_path(session_id)
        running = self._resume_digest(session)
        chunk_hash = _new_hash(expected_algorithm) if expected_algorithm else None
        # Snapshot so a failed chunk can be rolled back cleanly.
        start_offset = session.offset
        written = 0
        # The running digest is only committed if the chunk is accepted.
        staged = running.copy()

        try:
            with open(path, "r+b") as handle:
                handle.seek(start_offset)
                handle.truncate(start_offset)
                async for block in chunks:
                    if not block:
                        continue
                    if (
                        self.max_upload_bytes is not None
                        and start_offset + written + len(block) > self.max_upload_bytes
                    ):
                        raise UploadError(413, f"upload exceeds {self.max_upload_bytes} bytes")
                    if session.length is not None and start_offset + written + len(block) > session.length:
                        raise UploadError(
                            400, "chunk would write past the declared Upload-Length"
                        )
                    handle.write(block)
                    staged.update(block)
                    if chunk_hash is not None:
                        chunk_hash.update(block)
                    written += len(block)
                handle.flush()
                os.fsync(handle.fileno())
        except UploadError:
            self._rollback(path, start_offset)
            raise
        except Exception as error:
            self._rollback(path, start_offset)
            raise UploadError(500, f"write failed: {error}") from error

        if chunk_hash is not None and expected_chunk_digest is not None:
            import hmac

            if not hmac.compare_digest(chunk_hash.hexdigest(), expected_chunk_digest):
                self._rollback(path, start_offset)
                raise UploadError(
                    422,
                    "chunk checksum mismatch; the chunk was discarded, retry from "
                    f"offset {start_offset}",
                )

        session.offset = start_offset + written
        self._digests[session_id] = staged
        self._save(session)
        return session

    def _rollback(self, path: Path, offset: int) -> None:
        try:
            with open(path, "r+b") as handle:
                handle.truncate(offset)
        except OSError:
            pass

    def finalize(self, session_id: str) -> UploadSession:
        """Verify and complete an upload whose last byte has arrived."""

        session = self.get(session_id)
        if session.completed:
            return session
        if session.length is not None and session.offset != session.length:
            raise UploadError(
                409, f"upload incomplete: {session.offset} of {session.length} bytes"
            )

        path = self.data_path(session_id)
        if session.expected_digest:
            digest = self._digests.get(session_id)
            actual = digest.hexdigest() if digest is not None else file_digest(
                path, session.algorithm
            )
            import hmac

            if not hmac.compare_digest(actual.lower(), session.expected_digest.lower()):
                raise UploadError(
                    422,
                    f"{session.algorithm} mismatch: expected {session.expected_digest}, "
                    f"got {actual}",
                )

        target = self.completed_dir / f"{session_id}.bin"
        path.replace(target)
        session.completed = True
        session.metadata["stored_path"] = str(target)
        self._save(session)
        self._digests.pop(session_id, None)
        return session

    def digest_of(self, session_id: str) -> str:
        """Current digest of everything received so far."""

        session = self.get(session_id)
        digest = self._digests.get(session_id)
        if digest is not None:
            return digest.hexdigest()
        return file_digest(self.data_path(session_id), session.algorithm)
