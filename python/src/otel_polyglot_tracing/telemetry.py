"""OpenTelemetry setup for the inventory service: OTLP/HTTP export, W3C propagation."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup(service_name: str) -> TracerProvider:
    """Create a batching OTLP/HTTP tracer provider and install it globally.

    The exporter reads the standard OTEL_EXPORTER_OTLP_* variables and the batch processor reads
    OTEL_BSP_*, exactly like the Go and Node.js services. The global propagator defaults to W3C
    trace context plus baggage.
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider
