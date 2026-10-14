package otlpstub

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Node is a span with its children, ordered by start time.
type Node struct {
	Span
	Children []*Node
}

// ByTrace groups spans by trace ID.
func ByTrace(spans []Span) map[string][]Span {
	out := make(map[string][]Span)
	for _, s := range spans {
		out[s.TraceID] = append(out[s.TraceID], s)
	}
	return out
}

// Tree links one trace's spans into parent/child nodes. Spans whose parent is missing from the
// trace (a broken link) become extra roots, so a broken propagation is visible, not hidden.
func Tree(spans []Span) []*Node {
	nodes := make(map[string]*Node, len(spans))
	for _, s := range spans {
		nodes[s.SpanID] = &Node{Span: s}
	}
	var roots []*Node
	for _, s := range spans {
		n := nodes[s.SpanID]
		if p, ok := nodes[s.ParentSpanID]; ok && s.ParentSpanID != "" && p != n {
			p.Children = append(p.Children, n)
		} else {
			roots = append(roots, n)
		}
	}
	sortNodes(roots)
	return roots
}

// sortNodes orders siblings by start time, recursively.
func sortNodes(ns []*Node) {
	sort.SliceStable(ns, func(i, j int) bool { return ns[i].Start.Before(ns[j].Start) })
	for _, n := range ns {
		sortNodes(n.Children)
	}
}

// Render prints a trace as an indented tree: service (language), kind, name, duration, status.
func Render(roots []*Node) string {
	var b strings.Builder
	var walk func(n *Node, depth int)
	walk = func(n *Node, depth int) {
		status := ""
		if n.StatusCode == "ERROR" {
			status = "  ERROR " + n.StatusMessage
		}
		fmt.Fprintf(&b, "%s%-22s %-9s %-30s %9s%s\n",
			strings.Repeat("  ", depth), n.Service+" ("+n.Language+")", n.Kind, n.Name,
			n.Duration().Round(100*time.Microsecond), status)
		for _, c := range n.Children {
			walk(c, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, 0)
	}
	return b.String()
}
