from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yserver


class TestServerTools(unittest.TestCase):
    def test_chunk_bytes(self) -> None:
        data = b"abcdefghij"
        chunks = yserver.ServerTools.chunk_bytes(data, 3)
        self.assertEqual(chunks, [b"abc", b"def", b"ghi", b"j"])

    def test_retry_sync(self) -> None:
        attempts = {"count": 0}

        def flaky() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise ValueError("try again")
            return "ok"

        value = yserver.ServerTools.retry(flaky, attempts=3, delay_seconds=0.0)
        self.assertEqual(value, "ok")
        self.assertEqual(attempts["count"], 3)

    def test_retry_async(self) -> None:
        attempts = {"count": 0}

        async def flaky_async() -> str:
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise ValueError("retry")
            return "done"

        async def run_test() -> str:
            return await yserver.ServerTools.retry_async(flaky_async, attempts=2, delay_seconds=0.0)

        value = asyncio.run(run_test())
        self.assertEqual(value, "done")
        self.assertEqual(attempts["count"], 2)


if __name__ == "__main__":
    unittest.main()

