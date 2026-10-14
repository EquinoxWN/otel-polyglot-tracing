"""SQLite queue producer: schema contract, header limits, concurrent publishers."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from pathlib import Path

import pytest

from otel_polyglot_tracing.sqlite_queue import (
    MAX_BODY_BYTES,
    MAX_HEADER_VALUE,
    MAX_HEADERS,
    SCHEMA,
    MessageRejectedError,
    SqliteQueue,
)

SPEC = Path(__file__).resolve().parents[2] / "spec" / "queue.sql"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def normalise(sql: str) -> str:
    """Collapse whitespace so formatting differences do not matter."""
    return re.sub(r"\s+", " ", sql).strip()


def test_schema_matches_the_shared_spec() -> None:
    assert normalise(SCHEMA) == normalise(SPEC.read_text(encoding="utf-8"))


def test_publish_stores_headers_and_body(tmp_path: Path) -> None:
    q = SqliteQueue(tmp_path / "q.db")
    mid = q.publish("orders", {"traceparent": TRACEPARENT, "tracestate": "k=v"}, {"order_id": "a", "n": 1})
    [row] = q.rows()
    assert row["id"] == mid
    assert row["topic"] == "orders"
    assert json.loads(row["headers"]) == {"traceparent": TRACEPARENT, "tracestate": "k=v"}
    assert json.loads(row["body"]) == {"order_id": "a", "n": 1}
    assert (row["state"], row["attempts"], row["claimed_at"]) == ("ready", 0, None)


def test_database_uses_wal_mode(tmp_path: Path) -> None:
    SqliteQueue(tmp_path / "q.db")
    with sqlite3.connect(tmp_path / "q.db") as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


@pytest.mark.parametrize(
    ("topic", "headers", "body"),
    [
        ("Orders", {}, {}),
        ("", {}, {}),
        ("orders; DROP TABLE messages", {}, {}),
        ("orders", {f"h{i}": "v" for i in range(MAX_HEADERS + 1)}, {}),
        ("orders", {"Traceparent": TRACEPARENT}, {}),
        ("orders", {"bad key": "v"}, {}),
        ("orders", {"traceparent": "x" * (MAX_HEADER_VALUE + 1)}, {}),
        ("orders", {"traceparent": 1}, {}),
        ("orders", {}, {"pad": "x" * MAX_BODY_BYTES}),
    ],
)
def test_messages_breaking_limits_are_rejected(
    tmp_path: Path, topic: str, headers: dict[str, str], body: dict[str, str]
) -> None:
    q = SqliteQueue(tmp_path / "q.db")
    with pytest.raises(MessageRejectedError):
        q.publish(topic, headers, body)
    assert q.rows() == []


def test_non_finite_numbers_are_not_valid_json(tmp_path: Path) -> None:
    q = SqliteQueue(tmp_path / "q.db")
    with pytest.raises(ValueError, match="JSON"):
        q.publish("orders", {}, {"x": float("nan")})


def test_concurrent_publishers_get_unique_ids(tmp_path: Path) -> None:
    q = SqliteQueue(tmp_path / "q.db")
    got: list[int] = []
    lock = threading.Lock()

    def publish_many(worker: int) -> None:
        """Publish 25 messages from one thread."""
        for i in range(25):
            mid = q.publish("orders", {"traceparent": TRACEPARENT}, {"worker": worker, "i": i})
            with lock:
                got.append(mid)

    threads = [threading.Thread(target=publish_many, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(got) == len(set(got)) == 200
    assert len(q.rows()) == 200
