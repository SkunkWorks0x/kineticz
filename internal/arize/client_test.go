package arize

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestNewTracerProvider_ConstructsAndShutsDown(t *testing.T) {
	// httptest server stands in for Phoenix Cloud; the exporter writes here.
	srv := httptest.NewServer(nil)
	defer srv.Close()

	ctx := context.Background()
	tp, shutdown, err := NewTracerProvider(ctx, srv.URL, "test-api-key")
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	if tp == nil {
		t.Fatal("TracerProvider is nil")
	}

	tracer := Tracer()
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestTracer_ReturnsNonNil(t *testing.T) {
	if Tracer() == nil {
		t.Fatal("Tracer() returned nil")
	}
}
