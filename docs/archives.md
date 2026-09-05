# Archives and folder transfer

yashserver can move a whole directory over HTTP as a single streamed archive, and
can accept one back, extracting it under a policy designed for input you do
not trust.

## Formats

| format | read | write | notes |
|---|---|---|---|
| `zip` | yes | yes | stdlib |
| `tar` | yes | yes | stdlib |
| `tar.gz` / `tgz` | yes | yes | stdlib |
| `tar.bz2` | yes | yes | stdlib |
| `tar.xz` | yes | yes | stdlib |
| `rar` | yes | **no** | needs `rarfile` plus `unar` or `bsdtar` |

**RAR is read-only, permanently.** The only RAR encoder is proprietary
software, so writing RAR cannot be supported without a non-free dependency.
Extraction works with the free tools `rarfile` drives, so reading is the
honest ceiling. `create_archive(..., fmt="rar")` raises rather than pretending.

### Setting up RAR reading

Install `rarfile` plus one extractor:

| platform | extractor | notes |
|---|---|---|
| Linux / macOS | `unar` or `bsdtar` | free, in Debian main; found on `PATH` |
| Windows | WinRAR's `UnRAR.exe` | found automatically, see below |

`rarfile` looks for a bare `unrar` on `PATH`, which is a Unix assumption —
WinRAR installs `UnRAR.exe` into Program Files and does not add it to `PATH`,
so RAR reading would simply not work on Windows. yashserver therefore discovers
the backend itself: it tries `PATH` first, then WinRAR's registry entries and
the standard Program Files locations, and falls back to 7-Zip. If nothing is
found the error names exactly what to install.

WinRAR is needed only to *create* RAR files, which yashserver never does. Reading
requires only the free extractors.

Two RAR-specific behaviours worth knowing:

* **A corrupt RAR is an error, not an empty success.** `rarfile` accepts any
  file carrying the RAR signature and reports zero members rather than
  raising, so a truncated upload would otherwise "extract" successfully with
  nothing in it. Zero members is treated as corrupt. This does reject a
  deliberately empty RAR — a trade worth making for a file transfer.
* **RAR4 creation is gone from WinRAR 7.x**, so the bundled fixture generator
  produces RAR5 only. RAR4 reading still works through the same extractors but
  is not covered by the test fixtures.

Format is detected from **content, not filename** — a `.zip` that is really a
RAR is treated as RAR, because whenever an archive is attacker-controlled so
is its extension.

## Serving a folder

```python
app = yashserver.YHttpServer(port=8080)
app.serve_folder("/backup", "/srv/data")
```

```
GET /backup                 -> tar.gz  (the default)
GET /backup?format=zip      -> zip
GET /backup?format=tar      -> tar
```

The archive is never built on disk or held in memory. A worker thread produces
it and hands chunks to the event loop through a bounded queue, so:

* memory stays flat regardless of folder size
* a slow client applies real backpressure — the producer blocks on a full
  queue rather than buffering ahead of the client
* compression and file I/O stay off the event loop

Because the length is unknown up front, the response is chunked.

For already-compressed payloads (game assets, media, existing archives) choose
`tar`: gzip costs CPU and saves nothing. `zip` is written with `ZIP_STORED`
for the same reason.

## Accepting a folder

```python
app.accept_folder("/upload", "/srv/incoming", max_upload_bytes=50 * 1024**3)
```

The body is streamed to a temporary file rather than buffered, then extracted
under an `ArchivePolicy`. Each upload gets its own subdirectory, so concurrent
uploads cannot interleave. Responses:

| status | meaning |
|---|---|
| `201` | extracted; body is a JSON report (entries, bytes, destination) |
| `400` | empty or unreadable archive |
| `413` | larger than `max_upload_bytes` |
| `422` | the policy refused it — traversal, links, special files, a bomb |

A rejected upload leaves nothing behind: the staging directory is removed
whatever the failure.

## Threat model

Every field in an archive is a lie until checked. `ArchivePolicy` defends
against:

**Path traversal.** `../`, absolute paths, Windows drive letters, UNC paths,
and backslash separators — checked *after* normalisation, since normalising
without re-checking is the classic hole. Hostile names are **refused, never
repaired**: silently stripping `..` from an attacker's path is how you end up
extracting somewhere surprising.

**Link escapes.** Symlink and hardlink members are rejected outright by
default. With `allow_links=True` they are still validated to land inside the
destination.

**Booby-trapped destinations.** Even a well-formed archive is refused if an
existing symlink *in the destination* would redirect the write outside it.

**Special files.** Devices, FIFOs and sockets are refused.

**Zip bombs.** Caps on entry count, per-entry size, total uncompressed size,
and compression ratio — all enforced *while decompressing*, never from the
declared header size. If a limit trips mid-write the partial file is deleted,
so a rejected archive cannot leave half a bomb on disk.

**Name-based attacks.** NUL bytes, control characters, over-long names or
paths, excessive nesting, trailing dots and spaces (which Windows silently
strips, letting `evil. ` collide with `evil`), and Windows reserved device
names like `CON` or `LPT1`. Reserved names are refused **on every platform**,
so a folder extracted on Linux does not become a landmine when later copied to
Windows.

## Policy

```python
from yashserver import ArchivePolicy, extract_archive

policy = ArchivePolicy(
    max_entries=100_000,
    max_entry_bytes=8 * 1024**3,
    max_total_bytes=64 * 1024**3,
    max_compression_ratio=200.0,
    max_depth=64,
    allow_links=False,
    allow_special_files=False,
)
report = extract_archive("incoming.zip", "/srv/out", policy=policy)
print(report.entries, report.bytes_written)
```

Defaults suit untrusted input. Raise them consciously: a legitimate 66 GiB
folder needs `max_total_bytes` and `max_entry_bytes` lifted, which is a
deliberate decision rather than something the library should assume.

`UnsafeArchiveError` carries the offending `member`, so a rejection can be
logged precisely without re-parsing the archive.

## Direct API

```python
from yashserver import (
    create_archive, extract_archive, detect_format,
    folder_archive_stream, iter_folder_entries, safe_member_path,
)

create_archive("/srv/data", "/tmp/backup.tar.gz")
detect_format("/tmp/backup.tar.gz")           # -> "tar.gz"
extract_archive("/tmp/backup.tar.gz", "/srv/restored")

async for chunk in folder_archive_stream("/srv/data", fmt="tar"):
    ...
```

`iter_folder_entries` walks depth-first with names sorted at each level, so
archives are reproducible without materialising the tree in memory.

---

# Resumable uploads

See `yashserver.upload` for the full protocol. In brief:

```python
store = app.resumable_uploads("/uploads", "/srv/uploads",
                              max_upload_bytes=100 * 1024**3)
```

| verb | path | meaning |
|---|---|---|
| `POST` | `/uploads` | create a session; returns `Location` |
| `HEAD` | `/uploads/{id}` | report `Upload-Offset` — ask this to resume |
| `PATCH` | `/uploads/{id}` | append at `Upload-Offset` |
| `GET` | `/uploads/{id}` | session status as JSON |
| `DELETE` | `/uploads/{id}` | abandon and delete |

Resuming is: `HEAD` for the offset, then `PATCH` from there. An interrupted
50 GB upload resumes at the byte it reached, not at zero.

Three independent integrity checks, because they catch different failures:

* **Offset agreement** — a `PATCH` whose offset disagrees with the server is
  rejected with `409` rather than written at the wrong place.
* **Per-chunk checksum** — `Upload-Checksum: sha256 <hex>` on a `PATCH`. A bad
  chunk is discarded and the file truncated back, so a retry is safe.
* **Whole-file checksum** — `Upload-Checksum` on the creating `POST`, verified
  when the last byte lands. Catches what no per-chunk check can: bytes that
  were fine in flight but landed wrong.

The running digest is rebuilt from disk when a session is resumed, so
integrity survives a server restart mid-upload rather than being abandoned.
Session ids are server-generated hex and validated on every lookup, so a
client cannot steer those paths anywhere.
