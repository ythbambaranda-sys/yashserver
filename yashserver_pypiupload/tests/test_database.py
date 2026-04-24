from __future__ import annotations

import importlib
import unittest
from pathlib import Path
from unittest import mock

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yserver


class TestDatabaseSupport(unittest.TestCase):
    def test_supported_database_list_contains_requested_engines(self) -> None:
        expected = {
            "mysql",
            "postgresql",
            "microsoft sql server",
            "oracle database",
            "sqlite",
            "mariadb",
            "mongodb",
            "redis",
            "cassandra",
            "dynamodb",
            "firebase realtime database",
            "couchbase",
            "snowflake",
            "google bigquery",
            "amazon redshift",
            "clickhouse",
            "elasticsearch",
            "neo4j",
            "influxdb",
            "duckdb",
        }
        supported = {name.lower() for name in yserver.list_supported_databases()}
        self.assertTrue(expected.issubset(supported))

    def test_sqlite_basic_round_trip(self) -> None:
        db = yserver.connect_database("sqlite", database=":memory:")
        try:
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
            row = db.fetch_one("SELECT id, name FROM users WHERE id = ?", (1,))
            assert row is not None
            self.assertEqual(row["id"], 1)
            self.assertEqual(row["name"], "Ada")

            rows = db.fetch_all("SELECT id, name FROM users")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["name"], "Ada")
            self.assertTrue(db.ping())
        finally:
            db.close()

    def test_unknown_backend_raises(self) -> None:
        with self.assertRaises(yserver.UnsupportedDatabaseError):
            yserver.connect_database("not-real-db")

    def test_missing_dependency_error_is_clear(self) -> None:
        original_import = importlib.import_module

        def fake_import(name: str, package: str | None = None):
            if name == "pymongo":
                raise ImportError("No module named pymongo")
            return original_import(name, package)

        with mock.patch("yserver.database.importlib.import_module", side_effect=fake_import):
            with self.assertRaises(yserver.MissingDependencyError):
                yserver.connect_database("mongodb", uri="mongodb://127.0.0.1:27017", database="test")

    def test_server_tools_database_helpers(self) -> None:
        supported = yserver.ServerTools.supported_databases()
        self.assertIn("SQLite", supported)
        db = yserver.ServerTools.connect_database("sqlite", database=":memory:")
        try:
            self.assertTrue(db.ping())
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
