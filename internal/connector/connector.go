// Package connector is the boundary between the engine and a real CRM.
//
// The engine never learns which CRM it is talking to. It learns what a
// connector can do, from Caps, and adapts. That is deliberate: connectors
// genuinely differ, and a design that pretends otherwise pushes the difference
// into the engine later, as a surprise, usually in production.
package connector

import (
	"context"
	"net/http"
	"time"

	"crm-bisync/internal/model"
)

// Caps describes what a connector's peer can actually do.
//
// Every field here exists because some real CRM does not have the property,
// and the engine has to behave differently when it does not.
type Caps struct {
	// NativeIdempotency is true when the peer honours an idempotency key we
	// supply. When false the engine falls back to a read-before-write plus a
	// link-table check, which costs an extra request per write.
	NativeIdempotency bool

	// Webhooks is true when the peer can push changes. When false the engine
	// relies on polling alone, and the watermark is the only safety net.
	Webhooks bool

	// SoftDelete is true when a deleted record stays readable in some archived
	// form. When false a delete is only ever observed as an absence, so the
	// engine cannot tell "deleted" from "no longer visible to this token".
	SoftDelete bool

	// ETags is true when records carry a version token the engine can use for
	// optimistic concurrency instead of trusting timestamps.
	ETags bool

	// MaxPageSize is the largest page the peer will return. Zero means the
	// connector picks.
	MaxPageSize int

	// ModifiedAtIsExact is false when the peer's modification timestamps are
	// coarse or lag the actual write. The engine widens the watermark overlap
	// window when it is false, because a coarse timestamp means a record
	// modified inside the last tick may report the previous tick.
	ModifiedAtIsExact bool
}

// Page is one page of a delta read.
type Page struct {
	Records []model.Record
	// Deleted holds remote IDs the peer reports as deleted. Only a connector
	// whose Caps.SoftDelete is true can populate this: on a peer that hard
	// deletes, a removed record is indistinguishable from one that merely
	// stopped being visible to our token, and guessing is how a sync deletes
	// live customer data.
	Deleted []string
	// Cursor is empty when this was the last page. It is opaque to the engine
	// and only ever handed back to the same connector.
	Cursor string
}

// HasMore reports whether another page follows.
func (p Page) HasMore() bool { return p.Cursor != "" }

// FieldSpec is one field on a remote object.
type FieldSpec struct {
	Name     string
	Type     string
	ReadOnly bool
	Required bool
}

// Schema is what Describe reports. Mapping validation compares it against the
// configured field map at startup, so a rename fails loudly at boot instead of
// quietly at 3am.
type Schema struct {
	Kind   string
	Fields []FieldSpec
}

// Field returns the named field.
func (s Schema) Field(name string) (FieldSpec, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return FieldSpec{}, false
}

// Names returns every field name in the schema.
func (s Schema) Names() []string {
	out := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		out = append(out, f.Name)
	}
	return out
}

// WriteResult is what an Upsert produced.
type WriteResult struct {
	RemoteID string
	// Created distinguishes an insert from an update. The engine records it so
	// a replayed idempotency key can report the original outcome rather than
	// claiming a second create.
	Created   bool
	Version   string
	UpdatedAt time.Time
}

// Connector is one side of a sync.
//
// Implementations must be safe for concurrent use: the worker pool calls them
// from several goroutines, and the rate limiter sits above them rather than
// inside them.
type Connector interface {
	Name() string
	Capabilities() Caps

	// Describe reports the remote schema for one object kind.
	Describe(ctx context.Context, kind string) (Schema, error)

	// ListChanged returns records modified at or after since. An empty cursor
	// starts at the first page; the returned Page.Cursor asks for the next.
	ListChanged(ctx context.Context, kind string, since time.Time, cursor string) (Page, error)

	// Get fetches one record. It returns an error classified as KindNotFound
	// when the record does not exist.
	Get(ctx context.Context, kind, remoteID string) (model.Record, error)

	// Upsert creates or updates a record. key is the engine's idempotency key:
	// the same key with the same payload must not produce a second record,
	// whether the peer enforces that natively or the connector does.
	Upsert(ctx context.Context, kind string, rec model.Record, key string) (WriteResult, error)

	// Delete removes a record. Deleting an absent record is not an error,
	// because a retry after a successful delete must not fail.
	Delete(ctx context.Context, kind, remoteID, key string) error

	// VerifyWebhook authenticates an inbound request and parses it into
	// change events. It must reject a bad signature and a stale timestamp, and
	// it must not trust anything in the body before doing so.
	VerifyWebhook(r *http.Request, body []byte) ([]model.ChangeEvent, error)
}
