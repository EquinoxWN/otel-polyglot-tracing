//go:build e2e

// Package e2e runs the Go gateway, the Python inventory service and the Node.js worker as local
// processes, sends real orders and checks the traces they export to an in-process OTLP receiver.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/otel-polyglot-tracing/go/internal/otlpstub"
)

// expectedSpan is one span every successful order must produce, with its parent.
type expectedSpan struct {
	key, service, kind, name, parent string
}

// The trace of one successful order: 11 spans, three languages, one queue hop.
var orderTrace = []expectedSpan{
	{"gw.server", "gateway", "SERVER", "POST /orders", ""},
	{"gw.validate", "gateway", "INTERNAL", "order.validate", "gw.server"},
	{"gw.client", "gateway", "CLIENT", "POST /reservations", "gw.server"},
	{"inv.server", "inventory", "SERVER", "POST /reservations", "gw.client"},
	{"inv.reserve", "inventory", "INTERNAL", "inventory.reserve", "inv.server"},
	{"inv.publish", "inventory", "PRODUCER", "publish orders", "inv.server"},
	{"ful.process", "fulfillment", "CONSUMER", "process orders", "inv.publish"},
	{"ful.ship", "fulfillment", "INTERNAL", "fulfillment.ship", "ful.process"},
	{"ful.client", "fulfillment", "CLIENT", "", "ful.ship"}, // fetch span; name set by the undici instrumentation
	{"gw.shipment", "gateway", "SERVER", "POST /orders/{id}/shipment", "ful.client"},
	{"gw.mark", "gateway", "INTERNAL", "order.mark_shipped", "gw.shipment"},
}

var languages = map[string]string{"gateway": "go", "inventory": "python", "fulfillment": "nodejs"}

// placeOrder posts one order and returns the order id and HTTP status.
func placeOrder(t *testing.T, gatewayURL, sku string, header http.Header) (string, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sku": sku, "quantity": 1})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, gatewayURL+"/orders", bytes.NewReader(body))
	if err != nil {
		t.Error(err)
		return "", 0
	}
	req.Header = header.Clone()
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return "", 0
	}
	defer resp.Body.Close()
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out["order_id"], resp.StatusCode
}

// traceOf returns the spans of the trace whose gateway server span carries orderID.
func traceOf(spans []otlpstub.Span, orderID string) []otlpstub.Span {
	for _, s := range spans {
		if s.Name == "POST /orders" && s.Attributes["order.id"] == orderID {
			return otlpstub.ByTrace(spans)[s.TraceID]
		}
	}
	return nil
}

// match finds each expected span in one trace; it returns the spans by key and any problems.
func match(trace []otlpstub.Span, want []expectedSpan) (map[string]otlpstub.Span, []string) {
	got := map[string]otlpstub.Span{}
	var problems []string
	for _, w := range want {
		var found []otlpstub.Span
		for _, s := range trace {
			if s.Service == w.service && s.Kind == w.kind && (w.name == "" || s.Name == w.name) {
				found = append(found, s)
			}
		}
		if len(found) != 1 {
			problems = append(problems, fmt.Sprintf("%s: want 1 %s %s %q span, found %d", w.key, w.service, w.kind, w.name, len(found)))
			continue
		}
		got[w.key] = found[0]
	}
	for _, w := range want {
		s, ok := got[w.key]
		if !ok {
			continue
		}
		if lang := languages[w.service]; s.Language != lang {
			problems = append(problems, fmt.Sprintf("%s: language %q, want %q", w.key, s.Language, lang))
		}
		if w.parent != "" {
			if p, ok := got[w.parent]; ok && s.ParentSpanID != p.SpanID {
				problems = append(problems, fmt.Sprintf("%s: parent %s, want %s (%s)", w.key, s.ParentSpanID, p.SpanID, w.parent))
			}
		}
	}
	if len(trace) != len(want) {
		problems = append(problems, fmt.Sprintf("trace has %d spans, want %d", len(trace), len(want)))
	}
	return got, problems
}

// complete reports whether every order's trace has all expected spans.
func complete(ids []string, n int) func([]otlpstub.Span) bool {
	return func(spans []otlpstub.Span) bool {
		for _, id := range ids {
			if len(traceOf(spans, id)) < n {
				return false
			}
		}
		return true
	}
}

// percentile returns the p-th percentile (nearest rank) of ds.
func percentile(ds []time.Duration, p float64) time.Duration {
	s := slices.Clone(ds)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s))*p+0.5) - 1
	return s[max(0, min(i, len(s)-1))]
}

func TestOneTraceSpansAllThreeLanguagesAndTheQueue(t *testing.T) {
	s := startStack(t, true)
	n := 20
	if v, err := strconv.Atoi(os.Getenv("E2E_ORDERS")); err == nil && v > 0 && v <= 500 {
		n = v
	}

	// n concurrent orders, so context leaking between requests or messages would show up.
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			id, code := placeOrder(t, s.gatewayURL, "SKU-1", nil)
			if code != http.StatusAccepted {
				t.Errorf("order %d: status %d", i, code)
			}
			ids[i] = id
		})
	}
	wg.Wait()

	// One order arrives with the caller's own trace context; the gateway must continue it.
	const callerTrace, callerSpan = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	callerID, code := placeOrder(t, s.gatewayURL, "SKU-2",
		http.Header{"Traceparent": {"00-" + callerTrace + "-" + callerSpan + "-01"}})
	if code != http.StatusAccepted {
		t.Fatalf("caller-context order: status %d", code)
	}
	// One order fails in the inventory service; its trace must say so.
	soldOutID, code := placeOrder(t, s.gatewayURL, "SOLD-OUT", nil)
	if code != http.StatusConflict {
		t.Fatalf("sold-out order: status %d", code)
	}
	if t.Failed() {
		t.FailNow()
	}

	all := append(slices.Clone(ids), callerID)
	spans := s.waitForSpans(t, "complete order traces", func(sp []otlpstub.Span) bool {
		return complete(all, len(orderTrace))(sp) && len(traceOf(sp, soldOutID)) >= 5
	})
	time.Sleep(500 * time.Millisecond) // let any stray span arrive before checking exact counts
	spans = s.receiver.Spans()

	traceIDs := map[string]bool{}
	var e2e, lag []time.Duration
	completeTraces := 0
	for _, id := range all {
		trace := traceOf(spans, id)
		got, problems := match(trace, orderTrace)
		for _, sp := range trace {
			if v, ok := sp.Attributes["order.id"]; ok && v != id {
				problems = append(problems, fmt.Sprintf("span %s carries order.id %s in the trace of %s", sp.Name, v, id))
			}
			lag = append(lag, sp.Received.Sub(sp.End))
		}
		if len(problems) > 0 {
			t.Errorf("order %s:\n  %v\n%s", id, problems, otlpstub.Render(otlpstub.Tree(trace)))
			continue
		}
		completeTraces++
		traceIDs[got["gw.server"].TraceID] = true
		e2e = append(e2e, got["gw.mark"].End.Sub(got["gw.server"].Start))

		// The functional result matches the trace: the gateway stored the tracking number.
		resp, err := http.Get(s.gatewayURL + "/orders/" + id)
		if err != nil {
			t.Fatal(err)
		}
		var rec struct{ Status, TrackingNumber string }
		var raw map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&raw)
		resp.Body.Close()
		rec.Status, _ = raw["status"].(string)
		rec.TrackingNumber, _ = raw["tracking_number"].(string)
		if rec.Status != "shipped" || rec.TrackingNumber != got["ful.ship"].Attributes["shipment.tracking_number"] {
			t.Errorf("order %s: gateway has %+v, ship span says %s", id, rec, got["ful.ship"].Attributes["shipment.tracking_number"])
		}
	}
	if len(traceIDs) != len(all) {
		t.Errorf("%d orders share %d trace ids", len(all), len(traceIDs))
	}

	caller := traceOf(spans, callerID)
	got, _ := match(caller, orderTrace)
	if got["gw.server"].TraceID != callerTrace || got["gw.server"].ParentSpanID != callerSpan {
		t.Errorf("caller context not continued: trace %s parent %s", got["gw.server"].TraceID, got["gw.server"].ParentSpanID)
	}

	soldOut := traceOf(spans, soldOutID)
	var errorSpans []string
	for _, sp := range soldOut {
		if sp.Service == "fulfillment" || sp.Kind == "PRODUCER" {
			t.Errorf("sold-out order reached the queue: %s %s", sp.Service, sp.Name)
		}
		if sp.StatusCode == "ERROR" && sp.Kind == "INTERNAL" {
			errorSpans = append(errorSpans, sp.Service+"/"+sp.Name)
		}
	}
	if !slices.Equal(errorSpans, []string{"inventory/inventory.reserve"}) {
		t.Errorf("sold-out trace: business error spans %v, want only inventory/inventory.reserve", errorSpans)
	}

	t.Logf("sample trace (order %s):\n%s", ids[0], otlpstub.Render(otlpstub.Tree(traceOf(spans, ids[0]))))
	t.Logf("sold-out trace (order %s):\n%s", soldOutID, otlpstub.Render(otlpstub.Tree(soldOut)))
	t.Logf("RESULT complete traces %d/%d, distinct trace ids %d, spans per trace %d, languages per trace 3, "+
		"caller traceparent continued, sold-out error on %v; order to shipment p50 %v max %v; export lag max %v",
		completeTraces, len(all), len(traceIDs), len(orderTrace), errorSpans,
		percentile(e2e, 0.5).Round(time.Millisecond), percentile(e2e, 1).Round(time.Millisecond),
		percentile(lag, 1).Round(time.Millisecond))
}

// TestBaselineWithoutQueuePropagationBreaksTheTrace measures the common bug: the producer does not
// inject trace context into message headers, so every order splits into two unrelated traces.
// It also proves the main test would catch that bug.
func TestBaselineWithoutQueuePropagationBreaksTheTrace(t *testing.T) {
	s := startStack(t, false)
	const n = 5
	ids := make([]string, n)
	for i := range n {
		id, code := placeOrder(t, s.gatewayURL, "SKU-1", nil)
		if code != http.StatusAccepted {
			t.Fatalf("status %d", code)
		}
		ids[i] = id
	}
	consumerOf := func(spans []otlpstub.Span, id string) (otlpstub.Span, bool) {
		for _, sp := range spans {
			if sp.Kind == "CONSUMER" && sp.Attributes["order.id"] == id {
				return sp, true
			}
		}
		return otlpstub.Span{}, false
	}
	spans := s.waitForSpans(t, "both halves of every order", func(sp []otlpstub.Span) bool {
		for _, id := range ids {
			c, ok := consumerOf(sp, id)
			if !ok || len(traceOf(sp, id)) < 6 || len(otlpstub.ByTrace(sp)[c.TraceID]) < 5 {
				return false
			}
		}
		return true
	})
	connected, orphans := 0, 0
	for _, id := range ids {
		front := traceOf(spans, id)
		if _, problems := match(front, orderTrace); len(problems) == 0 {
			connected++
		}
		c, _ := consumerOf(spans, id)
		if c.ParentSpanID == "" && c.TraceID != front[0].TraceID {
			orphans++
		}
	}
	if connected != 0 || orphans != n {
		t.Fatalf("baseline: connected %d, orphan consumer traces %d", connected, orphans)
	}
	c, _ := consumerOf(spans, ids[0])
	t.Logf("front half (order %s):\n%s", ids[0], otlpstub.Render(otlpstub.Tree(traceOf(spans, ids[0]))))
	t.Logf("back half, a separate trace:\n%s", otlpstub.Render(otlpstub.Tree(otlpstub.ByTrace(spans)[c.TraceID])))
	t.Logf("BASELINE without queue propagation: connected traces %d/%d, orphan consumer traces %d/%d", connected, n, orphans, n)
}
