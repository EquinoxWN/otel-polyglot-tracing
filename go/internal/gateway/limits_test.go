package gateway

import (
	"errors"
	"strings"
	"testing"
)

// A valid order padded with whitespace to one byte over the limit is refused, not truncated.
func TestOrderOneByteOverTheBodyLimitIsRefused(t *testing.T) {
	order := `{"sku":"SKU-1","quantity":1}`
	if _, err := DecodeOrder(strings.NewReader(order + strings.Repeat(" ", MaxBodyBytes-len(order)))); err != nil {
		t.Fatalf("a body of exactly %d bytes must be accepted: %v", MaxBodyBytes, err)
	}
	_, err := DecodeOrder(strings.NewReader(order + strings.Repeat(" ", MaxBodyBytes+1-len(order))))
	if !errors.Is(err, ErrInvalidOrder) || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("err %v", err)
	}
}
