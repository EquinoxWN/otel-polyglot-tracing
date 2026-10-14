"""Shared fixtures: an in-memory span exporter, a queue in a temp dir and a test client."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from otel_polyglot_tracing.inventory import Stock, create_app
from otel_polyglot_tracing.sqlite_queue import SqliteQueue


class Harness:
    """A test client wired to a span exporter, a queue and stock levels."""

    def __init__(self, tmp: Path, *, propagate_queue_context: bool = True) -> None:
        """Build the app with fresh state."""
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.queue = SqliteQueue(tmp / "queue.db")
        self.stock = Stock({"SKU-1": 100, "SOLD-OUT": 0})
        app = create_app(self.queue, self.stock, self.provider, propagate_queue_context=propagate_queue_context)
        self.client = TestClient(app)

    def spans(self) -> list[ReadableSpan]:
        """Finished spans so far."""
        return list(self.exporter.get_finished_spans())

    def span(self, name: str) -> ReadableSpan:
        """The single finished span with this name."""
        found = [s for s in self.spans() if s.name == name]
        assert len(found) == 1, f"want one {name!r} span, got {[s.name for s in self.spans()]}"
        return found[0]


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    """A harness with queue trace propagation on."""
    h = Harness(tmp_path)
    yield h
    h.provider.shutdown()


@pytest.fixture
def baseline_harness(tmp_path: Path) -> Iterator[Harness]:
    """A harness that publishes without trace headers (the broken baseline)."""
    h = Harness(tmp_path, propagate_queue_context=False)
    yield h
    h.provider.shutdown()
