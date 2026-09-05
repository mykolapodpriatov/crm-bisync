package store

import (
	"sort"
	"sync"
	"time"

	"crm-bisync/internal/clock"
)

// Overlay is a writable view of a store whose writes are thrown away.
//
// It is what makes `plan` worth trusting. A dry run that reimplemented the
// pipeline would be a second program that agrees with the first only by
// coincidence; a dry run that reads the real state and writes into a layer
// nobody keeps is the same program with its effects discarded.
//
// Discarding is not the same as ignoring. Writes are visible to later reads
// through the overlay, so a plan covering many records sees the world its own
// earlier decisions would have produced, which is the only way the second
// record's plan can be right about the first record's link.
type Overlay struct {
	base Store

	mu sync.Mutex
	// over holds everything the run wrote.
	over *Mem
	// deleted marks keys the run removed, so a delete hides a base entry
	// rather than falling through to it.
	deleted map[string]bool
	closed  bool
}

// NewOverlay wraps a store. The base is never written to.
func NewOverlay(base Store, c clock.Clock) *Overlay {
	return &Overlay{
		base:    base,
		over:    NewMem(c),
		deleted: map[string]bool{},
	}
}

func tomb(collection, key string) string { return collection + "\x00" + key }

// Get implements Store.
func (o *Overlay) Get(collection, key string) ([]byte, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.get(collection, key)
}

// get is the unlocked form, for Take.
func (o *Overlay) get(collection, key string) ([]byte, bool, error) {
	if o.closed {
		return nil, false, ErrClosed
	}
	if v, ok, err := o.over.Get(collection, key); err != nil || ok {
		return v, ok, err
	}
	if o.deleted[tomb(collection, key)] {
		return nil, false, nil
	}
	return o.base.Get(collection, key)
}

// Put implements Store.
func (o *Overlay) Put(collection, key string, value []byte, expiresAt time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	delete(o.deleted, tomb(collection, key))
	return o.over.Put(collection, key, value, expiresAt)
}

// Delete implements Store.
func (o *Overlay) Delete(collection, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	o.deleted[tomb(collection, key)] = true
	return o.over.Delete(collection, key)
}

// Take implements Store.
func (o *Overlay) Take(collection, key string) ([]byte, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	value, ok, err := o.get(collection, key)
	if err != nil || !ok {
		return nil, false, err
	}
	o.deleted[tomb(collection, key)] = true
	if err := o.over.Delete(collection, key); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// List implements Store.
func (o *Overlay) List(collection string, limit int) ([]Entry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, ErrClosed
	}

	merged := map[string]Entry{}

	baseEntries, err := o.base.List(collection, 0)
	if err != nil {
		return nil, err
	}
	for _, e := range baseEntries {
		if o.deleted[tomb(collection, e.Key)] {
			continue
		}
		merged[e.Key] = e
	}

	overEntries, err := o.over.List(collection, 0)
	if err != nil {
		return nil, err
	}
	for _, e := range overEntries {
		merged[e.Key] = e
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, merged[k])
	}
	return out, nil
}

// Len implements Store.
func (o *Overlay) Len(collection string) (int, error) {
	entries, err := o.List(collection, 0)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

// Sweep implements Store.
//
// It only sweeps the overlay. The base belongs to the running engine, and a
// dry run has no business reclaiming its rows.
func (o *Overlay) Sweep(now time.Time) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return 0, ErrClosed
	}
	return o.over.Sweep(now)
}

// Close implements Store. It releases the overlay and leaves the base open,
// because the base is somebody else's.
func (o *Overlay) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	return o.over.Close()
}

var _ Store = (*Overlay)(nil)
