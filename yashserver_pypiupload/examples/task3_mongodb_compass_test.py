from __future__ import annotations

import os

import yserver

MONGO_HOST = os.getenv("YSERVER_MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("YSERVER_MONGO_PORT", "27018"))
MONGO_DB = os.getenv("YSERVER_MONGO_DB", "yserver_test")
MONGO_COLLECTION = os.getenv("YSERVER_MONGO_COLLECTION", "messages")


def main() -> None:
    mongo_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
    print(f"MongoDB URI: {mongo_uri}")
    print(f"Database: {MONGO_DB}")
    print(f"Collection: {MONGO_COLLECTION}")

    try:
        client = yserver.connect_database(
            "mongodb",
            uri=mongo_uri,
            database=MONGO_DB,
        )
    except yserver.MissingDependencyError as error:
        print(str(error))
        print("Tip: run `pip install pymongo` (or `pip install -e \".[db]\"`).")
        return

    try:
        try:
            client.ping()
        except Exception as error:
            text = str(error)
            if "ECONNREFUSED" in text or "connection refused" in text.lower():
                print(f"Cannot connect to MongoDB at {mongo_uri}.")
                print("Make sure `mongod` is running and listening on that host/port.")
                print("If your service is on 27017, set env var: YSERVER_MONGO_PORT=27017")
                return
            raise
        print("Connected to MongoDB.")

        inserted_id = client.insert_one(
            MONGO_COLLECTION,
            {
                "type": "mongo-test",
                "message": "hello from yserver",
                "created_at": yserver.ServerTools.utc_now(),
            },
        )
        print(f"Inserted document id: {inserted_id}")

        recent = client.find(
            MONGO_COLLECTION,
            {"type": "mongo-test"},
            limit=5,
            sort=[("_id", -1)],
        )
        print("Recent test documents:")
        for document in recent:
            print(document)
    finally:
        client.close()
        print("MongoDB connection closed.")


if __name__ == "__main__":
    main()
