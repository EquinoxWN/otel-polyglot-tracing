// Package gateway is the Go front door: it accepts orders, reserves stock in the Python
// inventory service over HTTP and records shipments reported by the Node.js worker.
package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// Limits on what a client may send; everything else is rejected before any downstream call.
const (
	MaxBodyBytes   = 4 << 10
	MaxQuantity    = 100
	maxOrdersKept  = 10_000
	orderIDHexSize = 16
)

var (
	skuPattern      = regexp.MustCompile(`^[A-Z0-9][A-Z0-9-]{0,31}$`)
	orderIDPattern  = regexp.MustCompile(`^[0-9a-f]{16}$`)
	trackingPattern = regexp.MustCompile(`^[A-Z0-9]{1,32}$`)
)

// ErrInvalidOrder wraps every validation failure of a client request.
var ErrInvalidOrder = errors.New("invalid order")

// Order is the body of POST /orders.
type Order struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// Shipment is the body of POST /orders/{id}/shipment.
type Shipment struct {
	TrackingNumber string `json:"tracking_number"`
}

// DecodeOrder parses and validates one order from at most MaxBodyBytes of JSON.
func DecodeOrder(r io.Reader) (Order, error) {
	var o Order
	if err := decodeStrict(r, &o); err != nil {
		return Order{}, err
	}
	if !skuPattern.MatchString(o.SKU) {
		return Order{}, fmt.Errorf("%w: sku must match %s", ErrInvalidOrder, skuPattern)
	}
	if o.Quantity < 1 || o.Quantity > MaxQuantity {
		return Order{}, fmt.Errorf("%w: quantity must be 1..%d", ErrInvalidOrder, MaxQuantity)
	}
	return o, nil
}

// DecodeShipment parses and validates a shipment notification.
func DecodeShipment(r io.Reader) (Shipment, error) {
	var s Shipment
	if err := decodeStrict(r, &s); err != nil {
		return Shipment{}, err
	}
	if !trackingPattern.MatchString(s.TrackingNumber) {
		return Shipment{}, fmt.Errorf("%w: tracking_number must match %s", ErrInvalidOrder, trackingPattern)
	}
	return s, nil
}

// ValidOrderID reports whether id has the gateway's order-id format.
func ValidOrderID(id string) bool { return orderIDPattern.MatchString(id) }

// decodeStrict reads one JSON object with no unknown fields and nothing after it.
func decodeStrict(r io.Reader, v any) error {
	data, err := io.ReadAll(io.LimitReader(r, MaxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("%w: read body: %v", ErrInvalidOrder, err)
	}
	if len(data) > MaxBodyBytes {
		return fmt.Errorf("%w: body larger than %d bytes", ErrInvalidOrder, MaxBodyBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidOrder, err)
	}
	if err := dec.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after JSON object", ErrInvalidOrder)
	}
	return nil
}
