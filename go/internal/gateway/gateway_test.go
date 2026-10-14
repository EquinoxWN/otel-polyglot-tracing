package gateway

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// fakeInventory records what the gateway sends and answers with a fixed status.
type fakeInventory struct {
	mu      sync.Mutex
	status  int
	calls   atomic.Int32
	headers []http.Header
	bodies  []map[string]any
}

// ServeHTTP records the request and replies with f.status.
func (f *fakeInventory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.headers = append(f.headers, r.Header.Clone())
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()
	w.WriteHeader(f.status)
}

// newGateway builds a gateway with an in-memory span recorder against inventoryURL.
func newGateway(t *testing.T, inventoryURL string) (*Server, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	srv, err := New(Config{InventoryURL: inventoryURL, TracerProvider: tp, Propagator: propagation.TraceContext{}})
	if err != nil {
		t.Fatal(err)
	}
	return srv, rec
}

// startInventory runs a fake inventory answering status.
func startInventory(t *testing.T, status int) (*fakeInventory, string) {
	t.Helper()
	inv := &fakeInventory{status: status}
	ts := httptest.NewServer(inv)
	t.Cleanup(ts.Close)
	return inv, ts.URL
}

// do sends one request to the gateway handler.
func do(h http.Handler, method, path, body string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// spanNamed returns the single ended span with the given name.
func spanNamed(t *testing.T, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %q span, got %d", name, len(found))
	}
	return found[0]
}

func TestCreateOrderPropagatesTraceContextToInventory(t *testing.T) {
	inv, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)

	w := do(srv.Handler(), http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":2}`, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || !ValidOrderID(resp["order_id"]) {
		t.Fatalf("bad response %s (%v)", w.Body, err)
	}

	server := spanNamed(t, rec, "POST /orders")
	validate := spanNamed(t, rec, "order.validate")
	client := spanNamed(t, rec, "POST /reservations")
	if server.SpanKind() != trace.SpanKindServer || client.SpanKind() != trace.SpanKindClient {
		t.Fatalf("kinds: server %v client %v", server.SpanKind(), client.SpanKind())
	}
	if validate.Parent().SpanID() != server.SpanContext().SpanID() || client.Parent().SpanID() != server.SpanContext().SpanID() {
		t.Fatal("order.validate and the client span must be children of the server span")
	}
	if !hasAttr(server, "order.id", resp["order_id"]) {
		t.Fatal("server span lacks order.id")
	}

	want := fmt.Sprintf("00-%s-%s-01", client.SpanContext().TraceID(), client.SpanContext().SpanID())
	if got := inv.headers[0].Get("Traceparent"); got != want {
		t.Fatalf("inventory got traceparent %q, want %q", got, want)
	}
	if inv.bodies[0]["order_id"] != resp["order_id"] || inv.bodies[0]["sku"] != "SKU-1" || inv.bodies[0]["quantity"] != float64(2) {
		t.Fatalf("inventory body %v", inv.bodies[0])
	}
}

func TestIncomingTraceparentIsContinued(t *testing.T) {
	inv, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)
	const traceID, parentID = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"

	h := http.Header{"Traceparent": {"00-" + traceID + "-" + parentID + "-01"}}
	if w := do(srv.Handler(), http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":1}`, h); w.Code != http.StatusAccepted {
		t.Fatalf("status %d", w.Code)
	}
	server := spanNamed(t, rec, "POST /orders")
	if server.SpanContext().TraceID().String() != traceID || server.Parent().SpanID().String() != parentID {
		t.Fatalf("server span %s parent %s", server.SpanContext().TraceID(), server.Parent().SpanID())
	}
	if !strings.Contains(inv.headers[0].Get("Traceparent"), traceID) {
		t.Fatal("trace id not forwarded to inventory")
	}
}

func TestMalformedTraceparentStartsNewTrace(t *testing.T) {
	_, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)
	for _, tp := range []string{"garbage", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", strings.Repeat("a", 4096)} {
		w := do(srv.Handler(), http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":1}`, http.Header{"Traceparent": {tp}})
		if w.Code != http.StatusAccepted {
			t.Fatalf("%q: status %d", tp[:min(len(tp), 20)], w.Code)
		}
	}
	for _, s := range rec.Ended() {
		if s.Name() == "POST /orders" && s.Parent().IsValid() {
			t.Fatalf("malformed traceparent must give a root span, got parent %s", s.Parent().SpanID())
		}
	}
}

func TestInvalidOrdersNeverReachInventory(t *testing.T) {
	cases := map[string]string{
		"not json":          `sku=SKU-1`,
		"empty body":        ``,
		"unknown field":     `{"sku":"SKU-1","quantity":1,"price":0}`,
		"lowercase sku":     `{"sku":"sku-1","quantity":1}`,
		"sku too long":      `{"sku":"` + strings.Repeat("A", 33) + `","quantity":1}`,
		"sku leading dash":  `{"sku":"-A","quantity":1}`,
		"quantity zero":     `{"sku":"SKU-1","quantity":0}`,
		"quantity too big":  `{"sku":"SKU-1","quantity":101}`,
		"quantity string":   `{"sku":"SKU-1","quantity":"1"}`,
		"quantity fraction": `{"sku":"SKU-1","quantity":1.5}`,
		"trailing data":     `{"sku":"SKU-1","quantity":1}{}`,
		"oversized body":    `{"sku":"SKU-1","quantity":1,"x":"` + strings.Repeat("a", MaxBodyBytes) + `"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			inv, url := startInventory(t, http.StatusAccepted)
			srv, rec := newGateway(t, url)
			w := do(srv.Handler(), http.MethodPost, "/orders", body, nil)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status %d", w.Code)
			}
			if inv.calls.Load() != 0 {
				t.Fatal("invalid order reached inventory")
			}
			if s := spanNamed(t, rec, "order.validate"); s.Status().Code != codes.Error {
				t.Fatal("order.validate must be marked as an error")
			}
		})
	}
}

func TestInventoryOutcomesMapToGatewayStatus(t *testing.T) {
	cases := []struct {
		inventory, want int
		serverError     bool
	}{
		{http.StatusConflict, http.StatusConflict, false},
		{http.StatusNotFound, http.StatusNotFound, false},
		{http.StatusInternalServerError, http.StatusBadGateway, true},
		{http.StatusTeapot, http.StatusBadGateway, true},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.inventory), func(t *testing.T) {
			_, url := startInventory(t, c.inventory)
			srv, rec := newGateway(t, url)
			if w := do(srv.Handler(), http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":1}`, nil); w.Code != c.want {
				t.Fatalf("status %d, want %d", w.Code, c.want)
			}
			if got := spanNamed(t, rec, "POST /orders").Status().Code == codes.Error; got != c.serverError {
				t.Fatalf("server span error = %v, want %v", got, c.serverError)
			}
			if spanNamed(t, rec, "POST /reservations").Status().Code != codes.Error {
				t.Fatal("a non-2xx inventory answer must mark the client span as an error")
			}
		})
	}
}

func TestUnreachableInventoryIsBadGateway(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close() // nothing listens there any more
	srv, rec := newGateway(t, url)
	if w := do(srv.Handler(), http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":1}`, nil); w.Code != http.StatusBadGateway {
		t.Fatalf("status %d", w.Code)
	}
	if spanNamed(t, rec, "POST /reservations").Status().Code != codes.Error {
		t.Fatal("client span must record the connection error")
	}
}

func TestShipmentIsRecordedOnceAndIdempotent(t *testing.T) {
	_, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)
	h := srv.Handler()
	var created map[string]string
	_ = json.Unmarshal(do(h, http.MethodPost, "/orders", `{"sku":"SKU-2","quantity":3}`, nil).Body.Bytes(), &created)
	id := created["order_id"]

	for range 2 { // at-least-once delivery may report the same shipment twice
		if w := do(h, http.MethodPost, "/orders/"+id+"/shipment", `{"tracking_number":"TRK123"}`, nil); w.Code != http.StatusNoContent {
			t.Fatalf("shipment status %d: %s", w.Code, w.Body)
		}
	}
	var got OrderRecord
	_ = json.Unmarshal(do(h, http.MethodGet, "/orders/"+id, "", nil).Body.Bytes(), &got)
	if got.Status != StatusShipped || got.TrackingNumber != "TRK123" || got.Quantity != 3 {
		t.Fatalf("order after shipment: %+v", got)
	}
	marks := 0
	for _, s := range rec.Ended() {
		if s.Name() == "order.mark_shipped" {
			marks++
			if !hasAttr(s, "order.id", id) || s.Parent().SpanID() == (trace.SpanID{}) {
				t.Fatal("order.mark_shipped must carry order.id and sit under the server span")
			}
		}
	}
	if marks != 2 {
		t.Fatalf("want 2 order.mark_shipped spans, got %d", marks)
	}

	cases := []struct{ path, body string }{
		{"/orders/0123456789abcdef/shipment", `{"tracking_number":"TRK1"}`},
		{"/orders/NOT-AN-ID/shipment", `{"tracking_number":"TRK1"}`},
		{"/orders/" + id + "/shipment", `{"tracking_number":"lower"}`},
		{"/orders/" + id + "/shipment", `{"tracking_number":"TRK1","extra":1}`},
	}
	wantCodes := []int{http.StatusNotFound, http.StatusBadRequest, http.StatusBadRequest, http.StatusBadRequest}
	for i, c := range cases {
		if w := do(h, http.MethodPost, c.path, c.body, nil); w.Code != wantCodes[i] {
			t.Errorf("%s %s: status %d, want %d", c.path, c.body, w.Code, wantCodes[i])
		}
	}
	if w := do(h, http.MethodGet, "/orders/0123456789abcdef", "", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown order: %d", w.Code)
	}
}

func TestHealthzIsNotTraced(t *testing.T) {
	_, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)
	if w := do(srv.Handler(), http.MethodGet, "/healthz", "", nil); w.Code != http.StatusOK {
		t.Fatalf("healthz %d", w.Code)
	}
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("healthz produced %d spans", n)
	}
}

// TestRandomTraceContextsRoundTrip sends 200 random valid traceparents (fixed seed) and checks
// that each trace ID reaches the inventory unchanged with the client span as the new parent.
func TestRandomTraceContextsRoundTrip(t *testing.T) {
	inv, url := startInventory(t, http.StatusAccepted)
	srv, rec := newGateway(t, url)
	h := srv.Handler()
	rng := rand.New(rand.NewPCG(57, 7))
	for i := range 200 {
		traceID := randomHex(rng, 16)
		parentID := randomHex(rng, 8)
		hdr := http.Header{"Traceparent": {"00-" + traceID + "-" + parentID + "-01"}}
		if w := do(h, http.MethodPost, "/orders", `{"sku":"SKU-1","quantity":1}`, hdr); w.Code != http.StatusAccepted {
			t.Fatalf("iteration %d: status %d", i, w.Code)
		}
		got := inv.headers[i].Get("Traceparent")
		parts := strings.Split(got, "-")
		if len(parts) != 4 || parts[1] != traceID || parts[2] == parentID {
			t.Fatalf("iteration %d: sent trace %s parent %s, inventory got %q", i, traceID, parentID, got)
		}
	}
	clientSpans := 0
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			clientSpans++
		}
	}
	if clientSpans != 200 {
		t.Fatalf("want 200 client spans, got %d", clientSpans)
	}
}

func TestStoreEvictsOldest(t *testing.T) {
	s := newStore(3)
	for _, id := range []string{"a", "b", "c", "d"} {
		s.add(OrderRecord{ID: id})
	}
	if _, ok := s.get("a"); ok {
		t.Fatal("oldest order should be evicted")
	}
	for _, id := range []string{"b", "c", "d"} {
		if _, ok := s.get(id); !ok {
			t.Fatalf("order %s missing", id)
		}
	}
	if _, ok := s.markShipped("a", "T"); ok {
		t.Fatal("evicted order cannot be shipped")
	}
}

func TestParseServiceURL(t *testing.T) {
	good := []string{"http://127.0.0.1:8081", "https://inventory.internal/base/"}
	bad := []string{"", "127.0.0.1:8081", "ftp://host", "http://", "http://h/?q=1", "http://h/#f", "http://u:p@h", "file:///etc/passwd"}
	for _, raw := range good {
		if _, err := ParseServiceURL(raw); err != nil {
			t.Errorf("%q rejected: %v", raw, err)
		}
	}
	for _, raw := range bad {
		if _, err := ParseServiceURL(raw); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
	if _, err := New(Config{InventoryURL: "http://h"}); err == nil {
		t.Error("New must require a tracer provider and propagator")
	}
}

// FuzzDecodeOrder checks that no input panics and every accepted order satisfies the limits.
func FuzzDecodeOrder(f *testing.F) {
	for _, seed := range []string{`{"sku":"SKU-1","quantity":1}`, `{"sku":"A","quantity":100}`, `{}`, `[]`, `{"sku":"\u0000","quantity":-1}`, `{"quantity":1e400}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		o, err := DecodeOrder(strings.NewReader(body))
		if err != nil {
			return
		}
		if !skuPattern.MatchString(o.SKU) || o.Quantity < 1 || o.Quantity > MaxQuantity || len(body) > MaxBodyBytes {
			t.Fatalf("accepted invalid order %+v from %q", o, body)
		}
	})
}

// hasAttr reports whether the span carries key=value.
func hasAttr(s sdktrace.ReadOnlySpan, key, value string) bool {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key && kv.Value.Emit() == value {
			return true
		}
	}
	return false
}

// randomHex returns n random bytes as hex, never all zero.
func randomHex(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	b[0] |= 1
	return hex.EncodeToString(b)
}
