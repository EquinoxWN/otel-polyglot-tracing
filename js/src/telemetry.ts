// OpenTelemetry setup for the worker: OTLP/HTTP protobuf export, W3C propagation, fetch spans.
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { UndiciInstrumentation } from "@opentelemetry/instrumentation-undici";
import { defaultResource, resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor, NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

/**
 * Create and register a batching tracer provider; fetch() calls get CLIENT spans automatically.
 * The exporter reads OTEL_EXPORTER_OTLP_* and the batch processor OTEL_BSP_*, as in Go and Python.
 */
export function setupTelemetry(serviceName: string): NodeTracerProvider {
  const provider = new NodeTracerProvider({
    resource: defaultResource().merge(resourceFromAttributes({ "service.name": serviceName })),
    spanProcessors: [new BatchSpanProcessor(new OTLPTraceExporter())],
  });
  // Installs the AsyncLocalStorage context manager and the W3C trace-context + baggage propagator.
  provider.register();
  registerInstrumentations({ tracerProvider: provider, instrumentations: [new UndiciInstrumentation()] });
  return provider;
}
