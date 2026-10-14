//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/otel-polyglot-tracing/go/internal/otlpstub"
)

// gatewayBinary is built once by TestMain into a temporary directory.
var gatewayBinary string

// TestMain builds the gateway once, runs the tests and removes the build directory.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "otel-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	gatewayBinary = filepath.Join(dir, "gateway")
	if runtime.GOOS == "windows" {
		gatewayBinary += ".exe"
	}
	build := exec.Command("go", "build", "-o", gatewayBinary, "./cmd/gateway")
	build.Dir = ".."
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	code := 1
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build gateway:", err)
	} else {
		code = m.Run()
	}
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// lineWatcher collects a process's output and reports the first line matching ready.
type lineWatcher struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	partial []byte
	ready   *regexp.Regexp
	found   chan string
}

// Write buffers output and checks each complete line against the ready pattern.
func (w *lineWatcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	w.partial = append(w.partial, p...)
	for {
		i := bytes.IndexByte(w.partial, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(w.partial[:i]), "\r")
		w.partial = w.partial[i+1:]
		if m := w.ready.FindStringSubmatch(line); m != nil && w.found != nil {
			w.found <- m[len(m)-1]
			w.found = nil
		}
	}
	return len(p), nil
}

// String returns everything the process printed.
func (w *lineWatcher) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// service is one running process of the stack.
type service struct {
	name string
	cmd  *exec.Cmd
	out  *lineWatcher
}

// stack is the three services plus the OTLP receiver they export to.
type stack struct {
	receiver   *otlpstub.Receiver
	gatewayURL string
	services   []*service
}

// startStack starts the receiver, the Python inventory, the Go gateway and the Node.js worker,
// all bound to 127.0.0.1, and stops every process when the test ends.
func startStack(t *testing.T, propagateQueue bool) *stack {
	t.Helper()
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	workerJS := filepath.Join(repo, "js", "dist", "src", "main.js")
	if _, err := os.Stat(workerJS); err != nil {
		t.Fatalf("build the worker first (cd js && npm run build): %v", err)
	}

	s := &stack{receiver: otlpstub.NewReceiver(50_000)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: s.receiver, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	queueDB := filepath.Join(t.TempDir(), "queue.db")
	propagation := "on"
	if !propagateQueue {
		propagation = "off"
	}
	common := []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://" + ln.Addr().String(),
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=100",
		"OTEL_TRACES_SAMPLER=parentbased_always_on",
		"QUEUE_DB=" + queueDB,
	}

	inventory := s.start(t, "inventory", pythonBin(repo), []string{"-m", "otel_polyglot_tracing"},
		append(common, "INVENTORY_ADDR=127.0.0.1:0", "QUEUE_TRACE_PROPAGATION="+propagation),
		`listening on (http://\S+)`)
	s.gatewayURL = s.start(t, "gateway", gatewayBinary, nil,
		append(common, "GATEWAY_ADDR=127.0.0.1:0", "INVENTORY_URL="+inventory),
		`listening on (http://\S+)`)
	s.start(t, "fulfillment", envOr("NODE", "node"), []string{workerJS},
		append(common, "GATEWAY_URL="+s.gatewayURL, "POLL_MS=20"),
		`(fulfillment worker polling)`)
	return s
}

// start launches one service and waits until it prints its ready line; it returns the match.
func (s *stack) start(t *testing.T, name, bin string, args, env []string, ready string) string {
	t.Helper()
	out := &lineWatcher{ready: regexp.MustCompile(ready), found: make(chan string, 1)}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.WaitDelay = 5 * time.Second
	prepareTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	done := make(chan struct{}) // closed when the process has exited
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	s.services = append(s.services, &service{name: name, cmd: cmd, out: out})
	t.Cleanup(func() {
		killTree(cmd)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("%s did not exit after kill", name)
		}
		if t.Failed() {
			t.Logf("--- %s output ---\n%s", name, out.String())
		}
	})
	select {
	case m := <-out.found:
		return m
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not become ready in 30 s:\n%s", name, out.String())
	case <-done:
		t.Fatalf("%s exited early (%v):\n%s", name, waitErr, out.String())
	}
	return ""
}

// waitForSpans blocks until cond holds or 60 s pass.
func (s *stack) waitForSpans(t *testing.T, what string, cond func([]otlpstub.Span) bool) []otlpstub.Span {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	spans, err := s.receiver.WaitFor(ctx, cond)
	if err != nil {
		for id, trace := range otlpstub.ByTrace(spans) {
			t.Logf("trace %s (%d spans):\n%s", id, len(trace), otlpstub.Render(otlpstub.Tree(trace)))
		}
		t.Fatalf("waiting for %s: %v (%d spans received)", what, err, len(spans))
	}
	return spans
}

// pythonBin picks $PYTHON, else the repo's .venv interpreter, else python on PATH.
func pythonBin(repo string) string {
	if v := os.Getenv("PYTHON"); v != "" {
		return v
	}
	venv := filepath.Join(repo, ".venv", "bin", "python")
	if runtime.GOOS == "windows" {
		venv = filepath.Join(repo, ".venv", "Scripts", "python.exe")
	}
	if _, err := os.Stat(venv); err == nil {
		return venv
	}
	return "python"
}

// envOr returns the environment variable or a default.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
