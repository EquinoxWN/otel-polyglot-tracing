// Package otlpstub is a minimal OTLP/HTTP trace receiver for tests: it decodes protobuf export
// requests from the Go, Python and Node.js SDKs and keeps the spans in memory for assertions.
package otlpstub

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Size limits that keep a misbehaving exporter from exhausting memory.
const (
	MaxBodyBytes         = 4 << 20
	MaxDecompressedBytes = 16 << 20
	TracesPath           = "/v1/traces"
)

// Span is one received span, flattened to what the tests assert on.
type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          string
	Service       string
	Language      string
	Start         time.Time
	End           time.Time
	StatusCode    string
	StatusMessage string
	Attributes    map[string]string
	Received      time.Time
}

// Duration is the span's wall-clock length.
func (s Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// Receiver is an http.Handler implementing the OTLP/HTTP protobuf traces endpoint.
type Receiver struct {
	mu       sync.Mutex
	spans    []Span
	maxSpans int
	changed  chan struct{} // closed and replaced whenever spans are added
	rejected int
	now      func() time.Time
}

// NewReceiver returns a receiver that stores at most maxSpans spans and rejects the rest.
func NewReceiver(maxSpans int) *Receiver {
	return &Receiver{maxSpans: maxSpans, changed: make(chan struct{}), now: time.Now}
}

// ServeHTTP accepts POST /v1/traces with an application/x-protobuf body, optionally gzip-encoded.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != TracesPath {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if mt, _, err := mime.ParseMediaType(req.Header.Get("Content-Type")); err != nil || mt != "application/x-protobuf" {
		http.Error(w, "only application/x-protobuf is supported", http.StatusUnsupportedMediaType)
		return
	}
	body, status, err := readBody(w, req)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	spans, err := Decode(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rejected := r.add(spans)
	resp := &collectortrace.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectortrace.ExportTracePartialSuccess{
			RejectedSpans: int64(rejected),
			ErrorMessage:  "receiver is full",
		}
	}
	out, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(out)
}

// readBody reads the bounded, optionally gzip-compressed request body.
func readBody(w http.ResponseWriter, req *http.Request) ([]byte, int, error) {
	var src io.Reader = http.MaxBytesReader(w, req.Body, MaxBodyBytes)
	switch req.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(src)
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("bad gzip body: %w", err)
		}
		defer zr.Close()
		src = zr
	default:
		return nil, http.StatusUnsupportedMediaType, errors.New("unsupported content encoding")
	}
	body, err := io.ReadAll(io.LimitReader(src, MaxDecompressedBytes+1))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		return nil, http.StatusRequestEntityTooLarge, errors.New("body too large")
	case err != nil:
		return nil, http.StatusBadRequest, fmt.Errorf("read body: %w", err)
	case len(body) > MaxDecompressedBytes:
		return nil, http.StatusRequestEntityTooLarge, errors.New("decompressed body too large")
	}
	return body, http.StatusOK, nil
}

// Decode turns a protobuf ExportTraceServiceRequest into flat spans.
func Decode(body []byte) ([]Span, error) {
	var req collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("decode ExportTraceServiceRequest: %w", err)
	}
	var out []Span
	for _, rs := range req.GetResourceSpans() {
		res := attributes(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				if len(sp.GetTraceId()) != 16 || len(sp.GetSpanId()) != 8 {
					return nil, errors.New("span with invalid trace or span id length")
				}
				out = append(out, Span{
					TraceID:       hex.EncodeToString(sp.GetTraceId()),
					SpanID:        hex.EncodeToString(sp.GetSpanId()),
					ParentSpanID:  hexOrEmpty(sp.GetParentSpanId()),
					Name:          sp.GetName(),
					Kind:          kindName(sp.GetKind()),
					Service:       res["service.name"],
					Language:      res["telemetry.sdk.language"],
					Start:         time.Unix(0, int64(sp.GetStartTimeUnixNano())),
					End:           time.Unix(0, int64(sp.GetEndTimeUnixNano())),
					StatusCode:    statusName(sp.GetStatus().GetCode()),
					StatusMessage: sp.GetStatus().GetMessage(),
					Attributes:    attributes(sp.GetAttributes()),
				})
			}
		}
	}
	return out, nil
}

// add stores spans up to the capacity and returns how many were rejected.
func (r *Receiver) add(spans []Span) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	room := max(r.maxSpans-len(r.spans), 0)
	accepted := min(room, len(spans))
	for _, s := range spans[:accepted] {
		s.Received = now
		r.spans = append(r.spans, s)
	}
	rejected := len(spans) - accepted
	r.rejected += rejected
	if accepted > 0 {
		close(r.changed)
		r.changed = make(chan struct{})
	}
	return rejected
}

// Spans returns a copy of every span received so far.
func (r *Receiver) Spans() []Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Span(nil), r.spans...)
}

// Rejected returns how many spans were refused because the receiver was full.
func (r *Receiver) Rejected() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rejected
}

// WaitFor blocks until cond holds for the received spans or ctx ends.
func (r *Receiver) WaitFor(ctx context.Context, cond func([]Span) bool) ([]Span, error) {
	for {
		r.mu.Lock()
		spans := append([]Span(nil), r.spans...)
		changed := r.changed
		r.mu.Unlock()
		if cond(spans) {
			return spans, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return spans, ctx.Err()
		}
	}
}

// attributes flattens OTLP key-values to strings.
func attributes(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.GetKey()] = valueString(kv.GetValue())
	}
	return out
}

// valueString renders one OTLP AnyValue.
func valueString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

// hexOrEmpty encodes a parent span id, treating absent or all-zero ids as no parent.
func hexOrEmpty(b []byte) string {
	for _, c := range b {
		if c != 0 {
			return hex.EncodeToString(b)
		}
	}
	return ""
}

// kindName maps the OTLP span kind to SERVER, CLIENT, PRODUCER, CONSUMER or INTERNAL.
func kindName(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	default:
		return "UNSPECIFIED"
	}
}

// statusName maps the OTLP status code to UNSET, OK or ERROR.
func statusName(c tracepb.Status_StatusCode) string {
	switch c {
	case tracepb.Status_STATUS_CODE_OK:
		return "OK"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}
