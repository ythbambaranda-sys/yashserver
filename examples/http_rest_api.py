"""A REST API: path parameters, middleware, auth, JSON, and proper errors.

    python examples/http_rest_api.py

    curl http://127.0.0.1:8081/health
    curl -H 'Authorization: Bearer demo-token' http://127.0.0.1:8081/api/books
    curl -H 'Authorization: Bearer demo-token' http://127.0.0.1:8081/api/books/1
    curl -X POST -H 'Authorization: Bearer demo-token' -H 'Content-Type: application/json' \\
         -d '{"title":"Dune","author":"Herbert"}' http://127.0.0.1:8081/api/books
    curl -X DELETE -H 'Authorization: Bearer demo-token' http://127.0.0.1:8081/api/books/1
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver
from yashserver import HttpError

TOKEN = "demo-token"

app = yashserver.YHttpServer(
    host="127.0.0.1",
    port=8081,
    auth_token=TOKEN,
    # Health checks and probes must work without credentials.
    auth_exempt_paths={"/health"},
    rate_limit_per_window=300,
    rate_limit_window_seconds=60.0,
)

BOOKS: dict[int, dict] = {
    1: {"id": 1, "title": "The Mythical Man-Month", "author": "Brooks"},
    2: {"id": 2, "title": "Structure and Interpretation", "author": "Abelson"},
}
_next_id = 3


@app.middleware
async def timing(request, call_next):
    """Add latency and request-id headers to every response."""

    started = time.perf_counter()
    response = await call_next(request)
    response.set_header("X-Elapsed-Ms", f"{(time.perf_counter() - started) * 1000:.2f}")
    response.set_header("X-Request-Id", request.id)
    return response


@app.middleware
async def cors(request, call_next):
    if request.method == "OPTIONS":
        return yashserver.HttpResponse(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Authorization, Content-Type",
            },
        )
    response = await call_next(request)
    return response.set_header("Access-Control-Allow-Origin", "*")


@app.get("/health")
async def health(_request, server):
    return {"status": "ok", "uptime_seconds": round(server.uptime_seconds(), 2)}


@app.get("/api/books")
async def list_books(request, _server):
    author = request.query("author")
    books = list(BOOKS.values())
    if author:
        books = [book for book in books if book["author"].lower() == author.lower()]
    return {"count": len(books), "items": books}


@app.get("/api/books/{book_id}")
async def get_book(request, _server):
    book = _lookup(request.param("book_id"))
    return book


@app.post("/api/books")
async def create_book(request, _server):
    global _next_id
    payload = request.json()
    if not isinstance(payload, dict) or not payload.get("title"):
        raise HttpError(422, "title is required")

    book = {"id": _next_id, "title": payload["title"], "author": payload.get("author", "unknown")}
    BOOKS[_next_id] = book
    _next_id += 1
    return 201, book, {"Location": f"/api/books/{book['id']}"}


@app.put("/api/books/{book_id}")
async def replace_book(request, _server):
    book = _lookup(request.param("book_id"))
    payload = request.json()
    if not isinstance(payload, dict):
        raise HttpError(422, "expected a JSON object")
    book.update({"title": payload.get("title", book["title"]), "author": payload.get("author", book["author"])})
    return book


@app.delete("/api/books/{book_id}")
async def delete_book(request, _server):
    book = _lookup(request.param("book_id"))
    BOOKS.pop(book["id"])
    return None  # 204 No Content


@app.error_handler(404)
async def not_found(request, error):
    return 404, {"error": "not_found", "path": request.path, "detail": error.detail}


def _lookup(raw_id: str | None) -> dict:
    try:
        book_id = int(raw_id or "")
    except ValueError:
        raise HttpError(400, "book id must be an integer") from None
    book = BOOKS.get(book_id)
    if book is None:
        raise HttpError(404, f"no book with id {book_id}")
    return book


if __name__ == "__main__":
    print("REST API on http://127.0.0.1:8081")
    print(f"Authorization: Bearer {TOKEN}   (/health needs no token)")
    yashserver.run_many(app)
