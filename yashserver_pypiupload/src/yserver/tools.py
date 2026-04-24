from __future__ import annotations

import asyncio
import json
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable


class ServerTools:
    """Small helper utilities for handlers and plugins."""

    @staticmethod
    def to_json(data: Any, *, compact: bool = True) -> str:
        if compact:
            return json.dumps(data, separators=(",", ":"), ensure_ascii=True)
        return json.dumps(data, indent=2, sort_keys=True, ensure_ascii=True)

    @staticmethod
    def from_json(raw: str, *, default: Any = None) -> Any:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default

    @staticmethod
    def command_parts(line: str) -> tuple[str, str]:
        stripped = line.strip()
        if not stripped:
            return "", ""
        command, _, payload = stripped.partition(" ")
        return command.lower(), payload.strip()

    @staticmethod
    def utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def chunk_bytes(data: bytes, chunk_size: int) -> list[bytes]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        return [data[index : index + chunk_size] for index in range(0, len(data), chunk_size)]

    @staticmethod
    def create_server_ssl_context(
        certfile: str,
        keyfile: str,
        *,
        cafile: str | None = None,
        require_client_cert: bool = False,
    ) -> ssl.SSLContext:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        if cafile:
            context.load_verify_locations(cafile=cafile)
        if require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_NONE
        return context

    @staticmethod
    def retry(
        func: Callable[[], Any],
        *,
        attempts: int = 3,
        delay_seconds: float = 0.25,
        backoff_multiplier: float = 2.0,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
    ) -> Any:
        if attempts <= 0:
            raise ValueError("attempts must be > 0")
        wait = max(0.0, delay_seconds)
        for attempt in range(1, attempts + 1):
            try:
                return func()
            except retry_on:
                if attempt >= attempts:
                    raise
                if wait > 0:
                    time.sleep(wait)
                wait *= max(1.0, backoff_multiplier)
        raise RuntimeError("retry reached unexpected state")

    @staticmethod
    async def retry_async(
        func: Callable[[], Awaitable[Any] | Any],
        *,
        attempts: int = 3,
        delay_seconds: float = 0.25,
        backoff_multiplier: float = 2.0,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
    ) -> Any:
        if attempts <= 0:
            raise ValueError("attempts must be > 0")
        wait = max(0.0, delay_seconds)
        for attempt in range(1, attempts + 1):
            try:
                value = func()
                if asyncio.iscoroutine(value):
                    return await value
                return value
            except retry_on:
                if attempt >= attempts:
                    raise
                if wait > 0:
                    await asyncio.sleep(wait)
                wait *= max(1.0, backoff_multiplier)
        raise RuntimeError("retry_async reached unexpected state")

    @staticmethod
    def connect_database(backend: str, **config: Any) -> Any:
        from .database import connect_database

        return connect_database(backend, **config)

    @staticmethod
    def supported_databases() -> list[str]:
        from .database import list_supported_databases

        return list_supported_databases()
