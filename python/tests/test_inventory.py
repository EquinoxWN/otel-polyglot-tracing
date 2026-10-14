"""Inventory service: trace continuation over HTTP, business span, producer span into the queue."""

from __future__ import annotations

import json
import random
import sqlite3
import threading
from collections.abc import Iterator

import pytest
from conftest import Harness
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import SpanKind, StatusCode

from otel_polyglot_tracing.inventory import Outcome, Stock

ORDER = {"order_id": "0123456789abcdef", "sku": "SKU-1", "quantity": 2}
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"


def ids(span: ReadableSpan) -> tuple[str, str]:
    """Trace and span id of a span as lowercase hex."""
    ctx = span.get_span_context()
    assert ctx is not None
    return f"{ctx.trace_id:032x}", f"{ctx.span_id:016x}"


def parent_id(span: ReadableSpan) -> str | None:
    """Parent span id as hex, or None for a root span."""
    return None if span.parent is None else f"{span.parent.span_id:016x}"


def test_reservation_continues_the_trace_and_injects_the_producer_context(harness: Harness) -> None:
    resp = harness.client.post("/reservations", json=ORDER, headers={"traceparent": f"00-{TRACE_ID}-{PARENT_ID}-01"})
    assert resp.status_code == 202, resp.text
    assert resp.json()["order_id"] == ORDER["order_id"]

    server = harness.span("POST /reservations")
    reserve = harness.span("inventory.reserve")
    producer = harness.span("publish orders")
    assert server.kind is SpanKind.SERVER
    assert producer.kind is SpanKind.PRODUCER
    assert ids(server)[0] == TRACE_ID
    assert parent_id(server) == PARENT_ID
    assert parent_id(reserve) == ids(server)[1]
    assert parent_id(producer) == ids(server)[1]
    assert reserve.attributes is not None
    assert reserve.attributes["order.id"] == ORDER["order_id"]
    assert producer.attributes is not None
    assert producer.attributes["messaging.destination.name"] == "orders"
    assert producer.attributes["messaging.operation.type"] == "send"

    [row] = harness.queue.rows()
    headers = json.loads(row["headers"])
    trace_id, producer_id = ids(producer)
    assert headers["traceparent"] == f"00-{trace_id}-{producer_id}-01"
    assert json.loads(row["body"]) == ORDER
    assert producer.attributes["messaging.message.id"] == str(row["id"])
    assert harness.stock.level("SKU-1") == 98


def test_out_of_stock_marks_the_business_span_and_publishes_nothing(harness: Harness) -> None:
    resp = harness.client.post("/reservations", json={**ORDER, "sku": "SOLD-OUT"})
    assert resp.status_code == 409
    reserve = harness.span("inventory.reserve")
    assert reserve.status.status_code is StatusCode.ERROR
    assert reserve.status.description == Outcome.INSUFFICIENT.value
    assert not [s for s in harness.spans() if s.kind is SpanKind.PRODUCER]
    assert harness.queue.rows() == []


def test_unknown_sku_is_404(harness: Harness) -> None:
    assert harness.client.post("/reservations", json={**ORDER, "sku": "NOPE"}).status_code == 404
    assert harness.span("inventory.reserve").status.status_code is StatusCode.ERROR
    assert harness.queue.rows() == []


@pytest.mark.parametrize(
    "body",
    [
        {**ORDER, "extra": 1},
        {**ORDER, "order_id": "0123456789ABCDEF"},
        {**ORDER, "order_id": "short"},
        {**ORDER, "sku": "sku-1"},
        {**ORDER, "sku": "A" * 33},
        {**ORDER, "quantity": 0},
        {**ORDER, "quantity": 101},
        {**ORDER, "quantity": "2"},
        {**ORDER, "quantity": 2.5},
        {"order_id": ORDER["order_id"], "sku": "SKU-1"},
    ],
)
def test_invalid_requests_are_rejected_before_any_side_effect(harness: Harness, body: dict[str, object]) -> None:
    assert harness.client.post("/reservations", json=body).status_code == 422
    assert harness.queue.rows() == []
    assert harness.stock.level("SKU-1") == 100


def test_oversized_bodies_are_rejected_with_413(harness: Harness) -> None:
    big = json.dumps({**ORDER, "pad": "x" * 5000})
    resp = harness.client.post("/reservations", content=big, headers={"content-type": "application/json"})
    assert resp.status_code == 413

    def chunks() -> Iterator[bytes]:
        """Stream the body without a Content-Length header."""
        yield big[:3000].encode()
        yield big[3000:].encode()

    resp = harness.client.post("/reservations", content=chunks(), headers={"content-type": "application/json"})
    assert resp.status_code == 413
    assert harness.queue.rows() == []


def test_baseline_mode_publishes_without_trace_headers(baseline_harness: Harness) -> None:
    resp = baseline_harness.client.post("/reservations", json=ORDER)
    assert resp.status_code == 202
    [row] = baseline_harness.queue.rows()
    assert json.loads(row["headers"]) == {}


def test_queue_failure_returns_503_and_releases_the_stock(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_: object) -> int:
        """Simulate a locked or missing queue file."""
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(harness.queue, "publish", broken)
    assert harness.client.post("/reservations", json=ORDER).status_code == 503
    assert harness.stock.level("SKU-1") == 100


def test_malformed_traceparent_starts_a_new_valid_trace(harness: Harness) -> None:
    resp = harness.client.post("/reservations", json=ORDER, headers={"traceparent": "00-zzzz-not-valid"})
    assert resp.status_code == 202
    server = harness.span("POST /reservations")
    assert server.parent is None
    headers = json.loads(harness.queue.rows()[0]["headers"])
    assert headers["traceparent"].split("-")[1] == ids(server)[0]


def test_random_trace_contexts_round_trip_through_the_queue(harness: Harness) -> None:
    rng = random.Random(57)
    for i in range(100):
        trace_id = f"{rng.getrandbits(128) | 1:032x}"
        span_id = f"{rng.getrandbits(64) | 1:016x}"
        order = {**ORDER, "order_id": f"{i:016x}", "quantity": 1}
        resp = harness.client.post("/reservations", json=order, headers={"traceparent": f"00-{trace_id}-{span_id}-01"})
        assert resp.status_code == 202
        row = harness.queue.rows()[-1]
        _, sent_trace, sent_parent, flags = json.loads(row["headers"])["traceparent"].split("-")
        assert sent_trace == trace_id
        assert sent_parent != span_id
        assert flags == "01"


def test_healthz_is_not_traced(harness: Harness) -> None:
    assert harness.client.get("/healthz").json() == {"status": "ok"}
    assert harness.spans() == []


def test_concurrent_reservations_never_oversell() -> None:
    stock = Stock({"SKU-1": 50})
    results: list[Outcome] = []
    lock = threading.Lock()

    def worker() -> None:
        """Try ten single-unit reservations."""
        for _ in range(10):
            outcome = stock.reserve("SKU-1", 1)
            with lock:
                results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(Outcome.RESERVED) == 50
    assert results.count(Outcome.INSUFFICIENT) == 110
    assert stock.level("SKU-1") == 0
