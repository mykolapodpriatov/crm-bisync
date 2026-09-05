package fake

import (
	"fmt"
	"sort"

	"crm-bisync/internal/model"
)

// The methods in this file are the test driver, not part of
// connector.Connector. They are how a test plays the part of a person editing
// records inside the CRM, which is the other half of every bidirectional sync
// scenario: the engine writes, and meanwhile a human somewhere else writes too.

// ExternalUpsert applies a change as if a user made it in the CRM's own UI.
//
// It is deliberately not Upsert: it bypasses the fault gate and the
// idempotency cache, because a person clicking save is not one of our API
// calls and must not consume our injected failures.
func (f *Fake) ExternalUpsert(kind, remoteID string, fields map[string]any) model.Record {
	f.mu.Lock()
	defer f.mu.Unlock()

	bucket, ok := f.data[kind]
	if !ok {
		bucket = make(map[string]*record)
		f.data[kind] = bucket
	}
	if remoteID == "" {
		f.nextID++
		remoteID = fmt.Sprintf("%s-%d", f.name, f.nextID)
	}
	rec, ok := bucket[remoteID]
	if !ok {
		rec = &record{id: remoteID, fields: make(map[string]any)}
		bucket[remoteID] = rec
	}
	rec.deleted = false
	for k, v := range fields {
		rec.fields[k] = v
	}
	rec.updatedAt = f.stamp()
	rec.version++

	out := f.toModel(kind, rec)
	f.emit(model.ChangeEvent{Kind: kind, RemoteID: remoteID, Record: ptr(out)})
	return out
}

// ExternalDelete removes a record as a user would.
func (f *Fake) ExternalDelete(kind, remoteID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rec, ok := f.data[kind][remoteID]
	if !ok {
		return
	}
	if f.caps.SoftDelete {
		rec.deleted = true
		rec.updatedAt = f.stamp()
	} else {
		delete(f.data[kind], remoteID)
	}
	f.emit(model.ChangeEvent{Kind: kind, RemoteID: remoteID, Deleted: true})
}

// Records returns every live record of a kind, sorted by ID, for assertions.
func (f *Fake) Records(kind string) []model.Record {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]model.Record, 0, len(f.data[kind]))
	for _, rec := range f.data[kind] {
		if rec.deleted {
			continue
		}
		out = append(out, f.toModel(kind, rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RemoteID < out[j].RemoteID })
	return out
}

// Count reports how many live records of a kind exist. The convergence test
// uses it as the no-duplicates assertion.
func (f *Fake) Count(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, rec := range f.data[kind] {
		if !rec.deleted {
			n++
		}
	}
	return n
}

// Field reads one field of one record, or false when the record is gone.
func (f *Fake) Field(kind, remoteID, name string) (any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.data[kind][remoteID]
	if !ok || rec.deleted {
		return nil, false
	}
	v, ok := rec.fields[name]
	return v, ok
}

// SetFaults replaces the fault configuration mid-run, which is how the chaos
// harness stops injecting faults and lets the engine quiesce before the
// convergence assertions run.
func (f *Fake) SetFaults(faults Faults) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.f = faults
}

// Faults returns the current fault configuration.
func (f *Fake) Faults() Faults {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f
}
