"""A file server: large downloads, resumable ranges, and streaming uploads.

Nothing here buffers a whole file in memory, in either direction, so serving a
multi-gigabyte video costs one chunk of RAM rather than a gigabyte of it.

    python examples/http_file_server.py

    curl http://127.0.0.1:8083/                       # listing
    curl -O http://127.0.0.1:8083/files/<name>        # download
    curl -r 0-99 http://127.0.0.1:8083/files/<name>   # resume / partial
    curl -T bigfile.bin http://127.0.0.1:8083/upload/bigfile.bin
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver
from yashserver import HttpError, HttpResponse, file_response

STORAGE = Path(tempfile.gettempdir()) / "yserver_files"
STORAGE.mkdir(exist_ok=True)

app = yashserver.YHttpServer(
    host="127.0.0.1",
    port=8083,
    # Uploads stream, so this cap only applies to buffered routes.
    max_body_bytes=8 * 1024 * 1024,
    stream_chunk_size=256 * 1024,
    # Big transfers over slow links need a generous write timeout.
    write_timeout_seconds=300.0,
    ddosprot=False,
)


@app.get("/")
async def index(_request, _server):
    files = sorted(path for path in STORAGE.iterdir() if path.is_file())
    return {
        "storage": str(STORAGE),
        "files": [{"name": path.name, "bytes": path.stat().st_size} for path in files],
    }


# `static` streams files with Range, ETag and traversal protection built in.
app.static("/files", STORAGE)


@app.get("/download/{name}")
async def download(request, _server):
    """Same as /files, but forces a save dialog."""

    target = _safe_path(request.param("name"))
    return file_response(target, request, download_name=target.name)


@app.put("/upload/{name}", stream=True)
@app.post("/upload/{name}", stream=True)
async def upload(request, _server):
    """Stream an upload straight to disk, hashing as it goes.

    ``stream=True`` means the body is never buffered, so the size of the
    upload is bounded by your disk, not by memory.
    """

    target = _safe_path(request.param("name"))
    digest = hashlib.sha256()
    written = 0

    partial = target.with_suffix(target.suffix + ".part")
    with partial.open("wb") as handle:
        async for chunk in request.stream(chunk_size=256 * 1024):
            handle.write(chunk)
            digest.update(chunk)
            written += len(chunk)
    partial.replace(target)

    return 201, {"name": target.name, "bytes": written, "sha256": digest.hexdigest()}


@app.get("/generate/{megabytes}")
async def generate(request, _server):
    """Stream synthetic data without ever building it in memory."""

    try:
        megabytes = min(1024, max(1, int(request.param("megabytes") or 1)))
    except ValueError:
        raise HttpError(400, "megabytes must be an integer") from None

    async def produce():
        block = b"y" * (64 * 1024)
        for _ in range(megabytes * 16):
            yield block

    return HttpResponse.stream_response(
        produce(),
        headers={"Content-Length": str(megabytes * 1024 * 1024)},
        content_type="application/octet-stream",
    )


@app.delete("/files/{name}")
async def delete(request, _server):
    target = _safe_path(request.param("name"))
    if not target.is_file():
        raise HttpError(404, f"no such file: {target.name}")
    target.unlink()
    return None


def _safe_path(name: str | None) -> Path:
    """Resolve a name inside STORAGE, refusing anything that escapes it."""

    candidate = (STORAGE / (name or "")).resolve()
    if not candidate.is_relative_to(STORAGE.resolve()):
        raise HttpError(403, "path traversal rejected")
    return candidate


if __name__ == "__main__":
    print(f"File server on http://127.0.0.1:8083   storage: {STORAGE}")
    yashserver.run_many(app)
