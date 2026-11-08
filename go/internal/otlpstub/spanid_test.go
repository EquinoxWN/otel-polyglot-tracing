package otlpstub

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// A span whose span id is not 8 bytes is refused like a short trace id.
func TestReceiverRejectsAShortSpanID(t *testing.T) {
	b, err := proto.Marshal(&collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{
			TraceId: bytes.Repeat([]byte{1}, 16), SpanId: []byte{0, 0, 0, 0, 0, 0, 1}, Name: "s",
		}}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := NewReceiver(10)
	req := httptest.NewRequest(http.MethodPost, TracesPath, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	rec.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || len(rec.Spans()) != 0 {
		t.Fatalf("status %d, %d spans stored", w.Code, len(rec.Spans()))
	}
}
