// Package telemetry builds the OpenTelemetry tracer provider and propagator used by the gateway.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Propagator returns the W3C trace-context and baggage propagator shared by all three services.
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// Setup creates an OTLP/HTTP-exporting tracer provider and installs it and the propagator globally.
//
// The exporter reads the standard OTEL_EXPORTER_OTLP_* variables and the batch processor reads
// OTEL_BSP_*, so the services are configured the same way in Go, Python and Node.js.
func Setup(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(Propagator())
	return tp, nil
}
