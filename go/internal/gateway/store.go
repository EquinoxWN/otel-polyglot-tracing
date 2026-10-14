package gateway

import "sync"

// Order states as returned by GET /orders/{id}.
const (
	StatusAccepted = "accepted"
	StatusShipped  = "shipped"
)

// OrderRecord is what the gateway remembers about one order.
type OrderRecord struct {
	ID             string `json:"order_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	Status         string `json:"status"`
	TrackingNumber string `json:"tracking_number,omitempty"`
}

// store keeps the most recent orders in memory, dropping the oldest beyond its capacity.
type store struct {
	mu       sync.Mutex
	capacity int
	orders   map[string]OrderRecord
	order    []string // insertion order, oldest first
}

// newStore returns an empty store holding at most capacity orders.
func newStore(capacity int) *store {
	return &store{capacity: capacity, orders: make(map[string]OrderRecord, capacity)}
}

// add records a new accepted order, evicting the oldest when full.
func (s *store) add(rec OrderRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.orders[rec.ID]; !ok {
		if len(s.order) >= s.capacity {
			delete(s.orders, s.order[0])
			s.order = s.order[1:]
		}
		s.order = append(s.order, rec.ID)
	}
	s.orders[rec.ID] = rec
}

// get returns the record for id.
func (s *store) get(id string) (OrderRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.orders[id]
	return rec, ok
}

// markShipped sets the tracking number; repeating it is harmless (at-least-once delivery).
func (s *store) markShipped(id, tracking string) (OrderRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.orders[id]
	if !ok {
		return OrderRecord{}, false
	}
	rec.Status, rec.TrackingNumber = StatusShipped, tracking
	s.orders[id] = rec
	return rec, true
}
