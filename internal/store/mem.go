package store

import (
	"sort"
	"sync"
	"time"

	"crm-bisync/internal/clock"
)

// Mem is an in-memory Store. It backs the whole test suite and the chaos
// harness, so it is held to the same conformance suite as the file store.
type Mem struct {
	mu     sync.Mutex
	clock  clock.Clock
	closed bool
	data   map[string]map[string]Entry
}

// NewMem returns an empty in-memory store. A nil clock means the wall clock.
func NewMem(c clock.Clock) *Mem {
	if c == nil {
		c = clock.Real{}
	}
	return &Mem{clock: c, data: make(map[string]map[string]Entry)}
}

// Get implements Store.
func (m *Mem) Get(collection, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, ErrClosed
	}
	e, ok := m.data[collection][key]
	if !ok || !live(e.ExpiresAt, m.clock.Now()) {
		return nil, false, nil
	}
	return append([]byte(nil), e.Value...), true, nil
}

// Put implements Store.
func (m *Mem) Put(collection, key string, value []byte, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.put(collection, key, value, expiresAt)
	return nil
}

func (m *Mem) put(collection, key string, value []byte, expiresAt time.Time) {
	c, ok := m.data[collection]
	if !ok {
		c = make(map[string]Entry)
		m.data[collection] = c
	}
	c[key] = Entry{
		Key:       key,
		Value:     append([]byte(nil), value...),
		ExpiresAt: expiresAt,
	}
}

// Delete implements Store.
func (m *Mem) Delete(collection, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	delete(m.data[collection], key)
	return nil
}

// Take implements Store.
func (m *Mem) Take(collection, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, ErrClosed
	}
	e, ok := m.data[collection][key]
	if !ok {
		return nil, false, nil
	}
	delete(m.data[collection], key)
	if !live(e.ExpiresAt, m.clock.Now()) {
		return nil, false, nil
	}
	return append([]byte(nil), e.Value...), true, nil
}

// List implements Store.
func (m *Mem) List(collection string, limit int) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	now := m.clock.Now()

	keys := make([]string, 0, len(m.data[collection]))
	for k, e := range m.data[collection] {
		if live(e.ExpiresAt, now) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		e := m.data[collection][k]
		out = append(out, Entry{
			Key:       k,
			Value:     append([]byte(nil), e.Value...),
			ExpiresAt: e.ExpiresAt,
		})
	}
	return out, nil
}

// Len implements Store.
func (m *Mem) Len(collection string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	now := m.clock.Now()
	n := 0
	for _, e := range m.data[collection] {
		if live(e.ExpiresAt, now) {
			n++
		}
	}
	return n, nil
}

// Sweep implements Store.
func (m *Mem) Sweep(now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	dropped := 0
	for _, c := range m.data {
		for k, e := range c {
			if !live(e.ExpiresAt, now) {
				delete(c, k)
				dropped++
			}
		}
	}
	return dropped, nil
}

// Close implements Store.
func (m *Mem) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.data = nil
	return nil
}

var _ Store = (*Mem)(nil)
