"""Producer side of the SQLite-backed message queue shared with the Node.js fulfillment worker.

Messages carry string headers next to the body, like Kafka or AMQP headers, so W3C trace
context (``traceparent``, ``tracestate``) travels with each message. The schema is the same
as ``spec/queue.sql``; a test fails if the two drift apart.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  headers TEXT NOT NULL,
  body TEXT NOT NULL,
  enqueued_at REAL NOT NULL,
  state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'claimed', 'done', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  claimed_at REAL,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS messages_by_state ON messages (topic, state, id);
"""

MAX_HEADERS = 16
MAX_HEADER_KEY = 64
MAX_HEADER_VALUE = 1024
MAX_BODY_BYTES = 64 * 1024
_TOPIC = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_HEADER_KEY = re.compile(r"^[a-z0-9_.-]+$")


class MessageRejectedError(ValueError):
    """A message that breaks the queue's limits; nothing is written."""


def validate_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Check header count, key format and sizes; return a plain dict copy."""
    if len(headers) > MAX_HEADERS:
        raise MessageRejectedError(f"more than {MAX_HEADERS} headers")
    out: dict[str, str] = {}
    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise MessageRejectedError("header keys and values must be strings")
        if len(key) > MAX_HEADER_KEY or not _HEADER_KEY.match(key):
            raise MessageRejectedError(f"invalid header key {key[:MAX_HEADER_KEY]!r}")
        if len(value) > MAX_HEADER_VALUE:
            raise MessageRejectedError(f"header {key!r} longer than {MAX_HEADER_VALUE} characters")
        out[key] = value
    return out


class SqliteQueue:
    """Appends messages to a SQLite file; one short-lived connection per call keeps it thread-safe."""

    def __init__(self, path: str | Path, busy_timeout_s: float = 5.0) -> None:
        """Open (and if needed create) the queue database at path."""
        self._path = str(path)
        self._timeout = busy_timeout_s
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        """Open a connection that waits for locks instead of failing."""
        conn = sqlite3.connect(self._path, timeout=self._timeout)
        conn.execute(f"PRAGMA busy_timeout={int(self._timeout * 1000)}")
        return conn

    def publish(self, topic: str, headers: Mapping[str, str], body: Mapping[str, Any]) -> int:
        """Validate and append one message; return its id."""
        if not _TOPIC.match(topic):
            raise MessageRejectedError(f"invalid topic {topic[:64]!r}")
        clean = validate_headers(headers)
        payload = json.dumps(body, separators=(",", ":"), allow_nan=False)
        if len(payload.encode()) > MAX_BODY_BYTES:
            raise MessageRejectedError(f"body larger than {MAX_BODY_BYTES} bytes")
        conn = self._connect()
        try:
            with conn:
                cur = conn.execute(
                    "INSERT INTO messages (topic, headers, body, enqueued_at) VALUES (?, ?, ?, ?)",
                    (topic, json.dumps(clean, separators=(",", ":")), payload, time.time()),
                )
            if cur.lastrowid is None:  # pragma: no cover - sqlite always sets it after INSERT
                raise RuntimeError("insert returned no row id")
            return cur.lastrowid
        finally:
            conn.close()

    def rows(self) -> list[dict[str, Any]]:
        """Return every message (for tests and debugging)."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute("SELECT * FROM messages ORDER BY id")]
        finally:
            conn.close()
