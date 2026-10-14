package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope of the gateway's custom spans.
const TracerName = "github.com/EquinoxWN/otel-polyglot-tracing/go/internal/gateway"

// Config wires the gateway to its tracer provider, propagator and the inventory service.
type Config struct {
	InventoryURL   string
	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator
	Timeout        time.Duration // per inventory call; 5 s when zero
	NewID          func() (string, error)
}

// Server holds the gateway's HTTP handlers and state.
type Server struct {
	inventory *url.URL
	client    *http.Client
	tracer    trace.Tracer
	tp        trace.TracerProvider
	prop      propagation.TextMapPropagator
	orders    *store
	newID     func() (string, error)
}

// New validates cfg and builds a Server.
func New(cfg Config) (*Server, error) {
	u, err := ParseServiceURL(cfg.InventoryURL)
	if err != nil {
		return nil, fmt.Errorf("inventory URL: %w", err)
	}
	if cfg.TracerProvider == nil || cfg.Propagator == nil {
		return nil, errors.New("tracer provider and propagator are required")
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	newID := cfg.NewID
	if newID == nil {
		newID = randomOrderID
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	transport := otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(cfg.TracerProvider),
		otelhttp.WithPropagators(cfg.Propagator),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	return &Server{
		inventory: u,
		client:    &http.Client{Transport: transport, Timeout: timeout},
		tracer:    cfg.TracerProvider.Tracer(TracerName),
		tp:        cfg.TracerProvider,
		prop:      cfg.Propagator,
		orders:    newStore(maxOrdersKept),
		newID:     newID,
	}, nil
}

// ParseServiceURL accepts only absolute http(s) URLs with a host and no query or fragment.
func ParseServiceURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("%q must be an http(s) URL with a host and nothing else", raw)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return u, nil
}

// Handler returns the routed, instrumented HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, otelhttp.NewHandler(h, pattern,
			otelhttp.WithTracerProvider(s.tp),
			otelhttp.WithPropagators(s.prop),
		))
	}
	route("POST /orders", s.createOrder)
	route("GET /orders/{id}", s.getOrder)
	route("POST /orders/{id}/shipment", s.recordShipment)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// createOrder validates the order, reserves stock in the inventory service and remembers it.
func (s *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	order, err := s.validate(ctx, r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := s.newID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create order id")
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("order.id", id))

	status, err := s.reserve(ctx, id, order)
	switch {
	case err != nil:
		writeOrderError(w, id, http.StatusBadGateway, "inventory unavailable")
	case status == http.StatusAccepted:
		s.orders.add(OrderRecord{ID: id, SKU: order.SKU, Quantity: order.Quantity, Status: StatusAccepted})
		writeJSON(w, http.StatusAccepted, map[string]string{"order_id": id, "status": StatusAccepted})
	case status == http.StatusConflict:
		writeOrderError(w, id, http.StatusConflict, "out of stock")
	case status == http.StatusNotFound:
		writeOrderError(w, id, http.StatusNotFound, "unknown sku")
	default:
		writeOrderError(w, id, http.StatusBadGateway, fmt.Sprintf("inventory answered %d", status))
	}
}

// validate decodes the order inside the order.validate business span.
func (s *Server) validate(ctx context.Context, body io.Reader) (Order, error) {
	_, span := s.tracer.Start(ctx, "order.validate")
	defer span.End()
	order, err := DecodeOrder(body)
	if err != nil {
		span.SetStatus(codes.Error, "invalid order")
		span.RecordError(err)
		return Order{}, err
	}
	span.SetAttributes(attribute.String("order.sku", order.SKU), attribute.Int("order.quantity", order.Quantity))
	return order, nil
}

// reserve asks the inventory service to reserve stock and returns its HTTP status.
func (s *Server) reserve(ctx context.Context, id string, order Order) (int, error) {
	payload, err := json.Marshal(map[string]any{"order_id": id, "sku": order.SKU, "quantity": order.Quantity})
	if err != nil {
		return 0, err
	}
	target := s.inventory.JoinPath("reservations")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		trace.SpanFromContext(ctx).RecordError(err)
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, nil
}

// getOrder returns the current state of one order.
func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ValidOrderID(id) {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}
	rec, ok := s.orders.get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown order")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// recordShipment marks an order shipped inside the order.mark_shipped business span.
func (s *Server) recordShipment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ValidOrderID(id) {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}
	_, span := s.tracer.Start(r.Context(), "order.mark_shipped", trace.WithAttributes(attribute.String("order.id", id)))
	defer span.End()
	shipment, err := DecodeShipment(r.Body)
	if err != nil {
		span.SetStatus(codes.Error, "invalid shipment")
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, ok := s.orders.markShipped(id, shipment.TrackingNumber); !ok {
		span.SetStatus(codes.Error, "unknown order")
		writeError(w, http.StatusNotFound, "unknown order")
		return
	}
	span.SetAttributes(attribute.String("shipment.tracking_number", shipment.TrackingNumber))
	w.WriteHeader(http.StatusNoContent)
}

// randomOrderID returns 16 lowercase hex characters from crypto/rand.
func randomOrderID() (string, error) {
	b := make([]byte, orderIDHexSize/2)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// writeJSON sends v as a JSON response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends {"error": msg}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeOrderError sends {"error": msg, "order_id": id} so callers can find the order's trace.
func writeOrderError(w http.ResponseWriter, id string, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "order_id": id})
}
