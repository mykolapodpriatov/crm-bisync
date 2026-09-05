// Package store is the durable state the engine restarts from.
//
// Everything the pipeline needs to survive a crash lives here: watermarks,
// the link table, snapshot hashes, the origin write log, the idempotency
// replay cache, webhook delivery IDs, the dead-letter queue and the review
// queue.
//
// Those are seven different shapes with one shape of access, so the interface
// is a small expiring key-value store over named collections, and the typed
// accessors in typed.go are written once on top of it. That keeps the two
// implementations (in-memory and append-only file) honest against each other:
// there is exactly one behaviour to get right, and the conformance suite in
// store_test.go runs against both.
package store

import (
	"errors"
	"time"
)

// ErrClosed is returned by every method once Close has been called.
var ErrClosed = errors.New("store: closed")

// Entry is one live record in a collection.
type Entry struct {
	Key   string
	Value []byte
	// ExpiresAt is the zero time when the record does not expire.
	ExpiresAt time.Time
}

// Store is an expiring key-value store over named collections.
//
// Implementations must be safe for concurrent use: the worker pool shares one
// store. Values are opaque bytes; the typed accessors own their encoding.
type Store interface {
	// Get returns the value, or ok == false when the key is absent or has
	// expired. An expired record is never visible, whether or not Sweep has
	// run yet.
	Get(collection, key string) (value []byte, ok bool, err error)

	// Put writes a value. A zero expiresAt means the record never expires.
	// Writing a key that already exists replaces it.
	Put(collection, key string, value []byte, expiresAt time.Time) error

	// Delete removes a key. Deleting an absent key is not an error.
	Delete(collection, key string) error

	// Take atomically returns and removes a value.
	//
	// This exists for the origin write log, where entries are single-use: two
	// concurrent inbound events carrying the same origin tag must not both be
	// suppressed, because only one of them can be the echo of our write. A
	// Get followed by a Delete would let both through the gap.
	Take(collection, key string) (value []byte, ok bool, err error)

	// List returns up to limit live entries in key order. A limit of zero or
	// less returns everything. Key order is required so the DLQ and review
	// queue list stably rather than in map order.
	List(collection string, limit int) ([]Entry, error)

	// Len reports how many live entries a collection holds.
	Len(collection string) (int, error)

	// Sweep drops every expired record and reports how many it dropped.
	// Expiry is already invisible to readers; Sweep is what stops the origin
	// log and the delivery table from growing without bound.
	Sweep(now time.Time) (int, error)

	// Close releases the store. It is safe to call more than once.
	Close() error
}

// Collections used by the engine. They are constants rather than free strings
// so a typo is a compile error instead of a silently empty read.
const (
	CollWatermarks = "watermarks"
	CollLinks      = "links"
	CollSnapshots  = "snapshots"
	CollOrigins    = "origins"
	CollIdem       = "idem"
	CollDeliveries = "deliveries"
	CollDLQ        = "dlq"
	CollReview     = "review"
	// CollKeys indexes deterministic identity keys to the records that hold
	// them, so matching a record against its peer is a local lookup rather
	// than a remote search. Not every CRM can search by an arbitrary field,
	// and the ones that can charge an API call for it.
	CollKeys = "keys"
	// CollKeyOwner records which keys a record currently occupies, so that a
	// record whose email changes releases the old key instead of leaving a
	// stale entry that would match somebody else.
	CollKeyOwner = "key_owner"
)

// AllCollections is every collection the engine uses, for sweeps and for the
// conformance suite.
var AllCollections = []string{
	CollWatermarks,
	CollLinks,
	CollSnapshots,
	CollOrigins,
	CollIdem,
	CollDeliveries,
	CollDLQ,
	CollReview,
	CollKeys,
	CollKeyOwner,
}

// live reports whether a record with the given expiry is visible at now. A
// zero expiry never expires.
func live(expiresAt, now time.Time) bool {
	return expiresAt.IsZero() || expiresAt.After(now)
}
