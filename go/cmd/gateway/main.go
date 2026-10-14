// Command gateway runs the Go order gateway: POST /orders reserves stock in the Python inventory
// service, and the Node.js fulfillment worker reports shipments back to it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/EquinoxWN/otel-polyglot-tracing/go/internal/gateway"
	"github.com/EquinoxWN/otel-polyglot-tracing/go/internal/telemetry"
)

// main runs the gateway and exits non-zero on startup failure.
func main() {
	if err := run(); err != nil {
		log.Printf("gateway: %v", err)
		os.Exit(1)
	}
}

// run reads configuration from the environment, serves until SIGINT/SIGTERM and flushes spans.
func run() error {
	addr := envOr("GATEWAY_ADDR", "127.0.0.1:8080")
	inventoryURL := envOr("INVENTORY_URL", "http://127.0.0.1:8081")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tp, err := telemetry.Setup(ctx, "gateway")
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("gateway: flush spans: %v", err)
		}
	}()

	srv, err := gateway.New(gateway.Config{
		InventoryURL:   inventoryURL,
		TracerProvider: tp,
		Propagator:     otel.GetTextMapPropagator(),
	})
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	// The e2e harness reads this line to learn the port when GATEWAY_ADDR ends in :0.
	fmt.Printf("gateway listening on http://%s\n", ln.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.Serve(ln) }()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

// envOr returns the environment variable or a default.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
