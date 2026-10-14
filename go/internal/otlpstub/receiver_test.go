package otlpstub

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// exportWith sends one parent and one child span through the real Go OTLP/HTTP exporter.
func exportWith(t *testing.T, url string, opts ...otlptracehttp.Option) (parent, child trace.SpanContext) {
	t.Helper()
	ctx := t.Context()
	exp, err := otlptracehttp.New(ctx, append([]otlptracehttp.Option{otlptracehttp.WithEndpointURL(url + TracesPath)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	res, err := resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(attribute.String("service.name", "unit")))
	if err != nil {
		t.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithResource(res))
	tracer := tp.Tracer("test")
	pctx, p := tracer.Start(ctx, "parent", trace.WithSpanKind(trace.SpanKindServer))
	_, c := tracer.Start(pctx, "child", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("s", "v"), attribute.Int("i", 7), attribute.Bool("b", true), attribute.Float64("f", 1.5)))
	c.SetStatus(codes.Error, "boom")
	c.End()
	p.End()
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	return p.SpanContext(), c.SpanContext()
}

func TestReceiverDecodesTheGoExporter(t *testing.T) {
	for name, opts := range map[string][]otlptracehttp.Option{
		"plain": nil,
		"gzip":  {otlptracehttp.WithCompression(otlptracehttp.GzipCompression)},
	} {
		t.Run(name, func(t *testing.T) {
			rec := NewReceiver(100)
			ts := httptest.NewServer(rec)
			defer ts.Close()
			parent, child := exportWith(t, ts.URL, opts...)

			spans := rec.Spans()
			if len(spans) != 2 {
				t.Fatalf("want 2 spans, got %d", len(spans))
			}
			byName := map[string]Span{}
			for _, s := range spans {
				byName[s.Name] = s
			}
			p, c := byName["parent"], byName["child"]
			if p.TraceID != parent.TraceID().String() || p.SpanID != parent.SpanID().String() || p.ParentSpanID != "" {
				t.Fatalf("parent decoded as %+v", p)
			}
			if c.TraceID != p.TraceID || c.ParentSpanID != p.SpanID || c.SpanID != child.SpanID().String() {
				t.Fatalf("child decoded as %+v", c)
			}
			if p.Kind != "SERVER" || c.Kind != "PRODUCER" || c.StatusCode != "ERROR" || c.StatusMessage != "boom" || p.StatusCode != "UNSET" {
				t.Fatalf("kind/status wrong: %+v / %+v", p, c)
			}
			if c.Service != "unit" || c.Language != "go" {
				t.Fatalf("resource: service %q language %q", c.Service, c.Language)
			}
			want := map[string]string{"s": "v", "i": "7", "b": "true", "f": "1.5"}
			for k, v := range want {
				if c.Attributes[k] != v {
					t.Errorf("attribute %s = %q, want %q", k, c.Attributes[k], v)
				}
			}
			if c.Duration() < 0 || c.Received.IsZero() {
				t.Fatal("timestamps missing")
			}
		})
	}
}

// requestBody marshals one export request holding n spans of one trace.
func requestBody(t *testing.T, n int, traceIDLen int) []byte {
	t.Helper()
	var spans []*tracepb.Span
	for i := range n {
		spans = append(spans, &tracepb.Span{
			TraceId: bytes.Repeat([]byte{1}, traceIDLen),
			SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, byte(i + 1)},
			Name:    "s",
		})
	}
	b, err := proto.Marshal(&collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// gzipped compresses b.
func gzipped(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.Bytes()
}

func TestReceiverRejectsBadRequests(t *testing.T) {
	valid := requestBody(t, 1, 16)
	bomb := gzipped(bytes.Repeat([]byte{0}, MaxDecompressedBytes+1024))
	cases := []struct {
		name, method, path, ctype, encoding string
		body                                []byte
		want                                int
	}{
		{"wrong method", http.MethodGet, TracesPath, "application/x-protobuf", "", nil, http.StatusMethodNotAllowed},
		{"wrong path", http.MethodPost, "/v1/metrics", "application/x-protobuf", "", valid, http.StatusNotFound},
		{"json body", http.MethodPost, TracesPath, "application/json", "", []byte(`{}`), http.StatusUnsupportedMediaType},
		{"no content type", http.MethodPost, TracesPath, "", "", valid, http.StatusUnsupportedMediaType},
		{"unknown encoding", http.MethodPost, TracesPath, "application/x-protobuf", "br", valid, http.StatusUnsupportedMediaType},
		{"corrupt protobuf", http.MethodPost, TracesPath, "application/x-protobuf", "", []byte{0xff, 0xff, 0xff}, http.StatusBadRequest},
		{"bad gzip", http.MethodPost, TracesPath, "application/x-protobuf", "gzip", []byte("not gzip"), http.StatusBadRequest},
		{"body too large", http.MethodPost, TracesPath, "application/x-protobuf", "", make([]byte, MaxBodyBytes+1), http.StatusRequestEntityTooLarge},
		{"gzip bomb", http.MethodPost, TracesPath, "application/x-protobuf", "gzip", bomb, http.StatusRequestEntityTooLarge},
		{"short trace id", http.MethodPost, TracesPath, "application/x-protobuf", "", requestBody(t, 1, 15), http.StatusBadRequest},
		{"valid", http.MethodPost, TracesPath, "application/x-protobuf; charset=binary", "", valid, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := NewReceiver(10)
			req := httptest.NewRequest(c.method, c.path, bytes.NewReader(c.body))
			if c.ctype != "" {
				req.Header.Set("Content-Type", c.ctype)
			}
			if c.encoding != "" {
				req.Header.Set("Content-Encoding", c.encoding)
			}
			w := httptest.NewRecorder()
			rec.ServeHTTP(w, req)
			if w.Code != c.want {
				t.Fatalf("status %d, want %d (%s)", w.Code, c.want, strings.TrimSpace(w.Body.String()))
			}
			if wantSpans := map[bool]int{true: 1, false: 0}[c.want == http.StatusOK]; len(rec.Spans()) != wantSpans {
				t.Fatalf("stored %d spans", len(rec.Spans()))
			}
		})
	}
}

func TestReceiverCapacityReportsPartialSuccess(t *testing.T) {
	rec := NewReceiver(2)
	req := httptest.NewRequest(http.MethodPost, TracesPath, bytes.NewReader(requestBody(t, 3, 16)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	rec.ServeHTTP(w, req)
	var resp collectortrace.ExportTraceServiceResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 || rec.Rejected() != 1 || len(rec.Spans()) != 2 {
		t.Fatalf("rejected %d / %d, stored %d", resp.GetPartialSuccess().GetRejectedSpans(), rec.Rejected(), len(rec.Spans()))
	}
}

func TestWaitForWakesOnNewSpansAndHonoursDeadline(t *testing.T) {
	rec := NewReceiver(10)
	go func() {
		time.Sleep(20 * time.Millisecond)
		rec.add([]Span{{Name: "late"}})
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	spans, err := rec.WaitFor(ctx, func(s []Span) bool { return len(s) == 1 })
	if err != nil || spans[0].Name != "late" {
		t.Fatalf("WaitFor: %v %v", spans, err)
	}
	short, cancel2 := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel2()
	if _, err := rec.WaitFor(short, func(s []Span) bool { return len(s) == 2 }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
}

func TestTreeKeepsBrokenLinksVisible(t *testing.T) {
	t0 := time.Unix(100, 0)
	spans := []Span{
		{TraceID: "t", SpanID: "b", ParentSpanID: "a", Name: "second", Start: t0.Add(2 * time.Millisecond), End: t0.Add(3 * time.Millisecond)},
		{TraceID: "t", SpanID: "a", Name: "root", Service: "gateway", Language: "go", Start: t0, End: t0.Add(5 * time.Millisecond)},
		{TraceID: "t", SpanID: "c", ParentSpanID: "a", Name: "first", Start: t0.Add(time.Millisecond), End: t0.Add(2 * time.Millisecond), StatusCode: "ERROR", StatusMessage: "out of stock"},
		{TraceID: "t", SpanID: "d", ParentSpanID: "missing", Name: "orphan", Start: t0, End: t0},
	}
	roots := Tree(spans)
	if len(roots) != 2 {
		t.Fatalf("want root + orphan, got %d roots", len(roots))
	}
	if roots[0].Name != "root" && roots[1].Name != "root" {
		t.Fatal("root missing")
	}
	var root *Node
	for _, r := range roots {
		if r.Name == "root" {
			root = r
		}
	}
	if len(root.Children) != 2 || root.Children[0].Name != "first" || root.Children[1].Name != "second" {
		t.Fatalf("children not ordered by start: %+v", root.Children)
	}
	out := Render(roots)
	for _, want := range []string{"gateway (go)", "root", "  ", "ERROR out of stock", "orphan"} {
		if !strings.Contains(out, want) {
			t.Fatalf("render lacks %q:\n%s", want, out)
		}
	}
	if got := len(ByTrace(append(spans, Span{TraceID: "u", SpanID: "z"}))); got != 2 {
		t.Fatalf("ByTrace groups %d traces", got)
	}
}
