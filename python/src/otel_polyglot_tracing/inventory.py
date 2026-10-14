"""Inventory service (FastAPI): reserves stock, then publishes the order to the queue.

Spans: FastAPI instrumentation creates the SERVER span from the incoming ``traceparent``;
``inventory.reserve`` is a custom business span; ``publish orders`` is a PRODUCER span whose
context is injected into the message headers so the Node.js worker continues the same trace.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from opentelemetry import propagate, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import SpanKind, Status, StatusCode, TracerProvider
from opentelemetry.util.types import AttributeValue
from pydantic import BaseModel, ConfigDict, Field
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from otel_polyglot_tracing.sqlite_queue import MessageRejectedError, SqliteQueue

TOPIC = "orders"
MAX_REQUEST_BYTES = 4 * 1024
DEFAULT_STOCK: Mapping[str, int] = {"SKU-1": 1_000_000, "SKU-2": 1_000_000, "SOLD-OUT": 0}


class ReservationRequest(BaseModel):
    """Body of POST /reservations, sent by the Go gateway."""

    model_config = ConfigDict(extra="forbid", strict=True)

    order_id: Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
    sku: Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9-]{0,31}$")]
    quantity: Annotated[int, Field(ge=1, le=100)]


class Outcome(StrEnum):
    """Result of a stock reservation."""

    RESERVED = "reserved"
    UNKNOWN_SKU = "unknown_sku"
    INSUFFICIENT = "insufficient_stock"


class Stock:
    """Thread-safe in-memory stock levels."""

    def __init__(self, levels: Mapping[str, int]) -> None:
        """Start from a copy of the given levels."""
        self._levels = dict(levels)
        self._lock = threading.Lock()

    def reserve(self, sku: str, quantity: int) -> Outcome:
        """Take quantity units if available."""
        with self._lock:
            have = self._levels.get(sku)
            if have is None:
                return Outcome.UNKNOWN_SKU
            if have < quantity:
                return Outcome.INSUFFICIENT
            self._levels[sku] = have - quantity
            return Outcome.RESERVED

    def release(self, sku: str, quantity: int) -> None:
        """Give back units from a reservation that could not be completed."""
        with self._lock:
            self._levels[sku] = self._levels.get(sku, 0) + quantity

    def level(self, sku: str) -> int | None:
        """Current units for sku, or None if unknown."""
        with self._lock:
            return self._levels.get(sku)


class BodyLimitMiddleware:
    """Read at most max_bytes of request body; larger bodies get 413 before the app sees them."""

    def __init__(self, app: ASGIApp, max_bytes: int = MAX_REQUEST_BYTES) -> None:
        """Wrap app."""
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Buffer the (small) body under the limit, then replay it to the app."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        length = dict(scope["headers"]).get(b"content-length")
        if length is not None and (not length.isdigit() or int(length) > self.max_bytes):
            await _too_large(send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] != "http.request":
                return  # client went away
            body += message.get("body", b"")
            if len(body) > self.max_bytes:
                await _too_large(send)
                return
            if not message.get("more_body", False):
                break
        replayed = False

        async def replay() -> Message:
            """Hand the buffered body over once, then wait for the real disconnect."""
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": bytes(body), "more_body": False}

        await self.app(scope, replay, send)


async def _too_large(send: Send) -> None:
    """Send a 413 JSON response."""
    body = b'{"error":"request body too large"}'
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


def create_app(
    queue: SqliteQueue,
    stock: Stock,
    tracer_provider: TracerProvider | None = None,
    *,
    propagate_queue_context: bool = True,
) -> FastAPI:
    """Build the instrumented inventory app.

    ``propagate_queue_context=False`` publishes messages without trace headers. It exists only to
    measure the baseline (a trace that breaks at the queue) in the end-to-end test.
    """
    app = FastAPI(title="inventory", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(BodyLimitMiddleware)
    tracer = trace.get_tracer(__name__, tracer_provider=tracer_provider)

    def publish(req: ReservationRequest) -> int:
        """Append the reserved order to the queue inside a PRODUCER span."""
        attributes: dict[str, AttributeValue] = {
            "messaging.system": "sqlite",
            "messaging.destination.name": TOPIC,
            "messaging.operation.type": "send",
            "messaging.operation.name": "publish",
            "order.id": req.order_id,
        }
        with tracer.start_as_current_span(f"publish {TOPIC}", kind=SpanKind.PRODUCER, attributes=attributes) as span:
            headers: dict[str, str] = {}
            if propagate_queue_context:
                propagate.inject(headers)
            message_id = queue.publish(TOPIC, headers, req.model_dump())
            span.set_attribute("messaging.message.id", str(message_id))
            return message_id

    @app.post("/reservations")
    def reserve(req: ReservationRequest) -> JSONResponse:
        """Reserve stock for one order and hand it to fulfillment."""
        attributes: dict[str, AttributeValue] = {
            "order.id": req.order_id,
            "order.sku": req.sku,
            "order.quantity": req.quantity,
        }
        with tracer.start_as_current_span("inventory.reserve", attributes=attributes) as span:
            outcome = stock.reserve(req.sku, req.quantity)
            span.set_attribute("inventory.outcome", outcome.value)
            if outcome is not Outcome.RESERVED:
                span.set_status(Status(StatusCode.ERROR, outcome.value))
        if outcome is Outcome.UNKNOWN_SKU:
            return JSONResponse({"error": "unknown sku"}, status_code=404)
        if outcome is Outcome.INSUFFICIENT:
            return JSONResponse({"error": "out of stock"}, status_code=409)
        try:
            message_id = publish(req)
        except (sqlite3.Error, MessageRejectedError):
            stock.release(req.sku, req.quantity)
            return JSONResponse({"error": "queue unavailable"}, status_code=503)
        return JSONResponse({"order_id": req.order_id, "message_id": message_id}, status_code=202)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Liveness probe (not traced)."""
        return {"status": "ok"}

    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=tracer_provider,
        excluded_urls="healthz",
        exclude_spans=["receive", "send"],
    )
    return app
