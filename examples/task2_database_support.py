from __future__ import annotations

import yashserver


def main() -> None:
    print("Supported database backends:")
    for name in yashserver.list_supported_databases():
        print(f"- {name}")

    db = yashserver.connect_database("sqlite", database=":memory:")
    try:
        db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        db.execute("INSERT INTO notes (id, body) VALUES (?, ?)", (1, "hello from yashserver db support"))
        rows = db.fetch_all("SELECT id, body FROM notes")
        print("SQLite demo rows:", rows)
    finally:
        db.close()


if __name__ == "__main__":
    main()
