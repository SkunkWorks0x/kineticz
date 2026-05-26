package arize

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation library name used for all spans Kineticz
// emits. Phoenix groups spans by this name in the UI.
const TracerName = "kineticz"

// NewTracerProvider builds an OpenTelemetry TracerProvider that ships spans
// to Phoenix Cloud via OTLP HTTP. endpoint must be the full collector URL
// (PHOENIX_COLLECTOR_ENDPOINT); apiKey is the Phoenix API key
// (PHOENIX_API_KEY). The returned shutdown function flushes pending spans
// and must be deferred by the caller before process exit.
//
// `[unverified]` against current Phoenix Cloud auth conventions; the
// "api_key" header name follows examples in the Phoenix docs but may need
// to be "authorization: Bearer <key>" depending on tenant configuration.
func NewTracerProvider(ctx context.Context, endpoint, apiKey string) (*sdktrace.TracerProvider, func(context.Context) error, error) {
	headers := map[string]string{}
	if apiKey != "" {
		headers["api_key"] = apiKey
	}
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(endpoint),
	}
	if len(headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(headers))
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("arize: build OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(TracerName)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("arize: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, tp.Shutdown, nil
}

// Tracer returns the Kineticz tracer for stage span creation. Pipeline stages
// call this to wrap their work in spans for Phoenix observability.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}
