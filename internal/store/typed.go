package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// Typed wraps a Store with the shapes the engine actually stores. Keeping the
// encoding here rather than in the two Store implementations means there is
// one place where a key format can go wrong, and it is covered by tests that
// run against both backends.
type Typed struct {
	S Store
}

// NewTyped wraps a store.
func NewTyped(s Store) *Typed { return &Typed{S: s} }

// Ref names one record on one side of a sync.
type Ref struct {
	Connector string `json:"connector"`
	Kind      string `json:"kind"`
	RemoteID  string `json:"remote_id"`
}

func (r Ref) key() string {
	return r.Connector + "\x00" + r.Kind + "\x00" + r.RemoteID
}

// String renders a ref for logs and CLI output.
func (r Ref) String() string {
	return fmt.Sprintf("%s:%s/%s", r.Connector, r.Kind, r.RemoteID)
}

// Link records that two remote records are the same thing.
//
// This is the only irreplaceable data the engine owns. Watermarks, hashes and
// queues can all be rebuilt by resyncing. Losing link rows means duplicating
// every record, so links are stored in both directions and never expire.
type Link struct {
	Left     Ref       `json:"left"`
	Right    Ref       `json:"right"`
	LinkedAt time.Time `json:"linked_at"`
}

// Peer returns the other side of the link, given one side.
func (l Link) Peer(r Ref) (Ref, bool) {
	switch r.key() {
	case l.Left.key():
		return l.Right, true
	case l.Right.key():
		return l.Left, true
	default:
		return Ref{}, false
	}
}

// PutLink writes the link under both of its refs, so a lookup from either
// side is a single Get.
func (t *Typed) PutLink(l Link) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return fmt.Errorf("store: encode link: %w", err)
	}
	if err := t.S.Put(CollLinks, l.Left.key(), raw, time.Time{}); err != nil {
		return err
	}
	return t.S.Put(CollLinks, l.Right.key(), raw, time.Time{})
}

// GetLink looks up the link for one side.
func (t *Typed) GetLink(r Ref) (Link, bool, error) {
	raw, ok, err := t.S.Get(CollLinks, r.key())
	if err != nil || !ok {
		return Link{}, false, err
	}
	var l Link
	if err := json.Unmarshal(raw, &l); err != nil {
		return Link{}, false, fmt.Errorf("store: decode link: %w", err)
	}
	return l, true, nil
}

// DeleteLink removes both directions.
func (t *Typed) DeleteLink(l Link) error {
	if err := t.S.Delete(CollLinks, l.Left.key()); err != nil {
		return err
	}
	return t.S.Delete(CollLinks, l.Right.key())
}

// Watermark is the high-water mark of a delta read.
func (t *Typed) Watermark(connector, kind string) (time.Time, bool, error) {
	raw, ok, err := t.S.Get(CollWatermarks, connector+"\x00"+kind)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	var unixNano int64
	if err := json.Unmarshal(raw, &unixNano); err != nil {
		return time.Time{}, false, fmt.Errorf("store: decode watermark: %w", err)
	}
	return time.Unix(0, unixNano).UTC(), true, nil
}

// SetWatermark advances the mark. The engine only calls this once a page has
// durably landed, so a crash re-reads a page rather than skipping one.
func (t *Typed) SetWatermark(connector, kind string, at time.Time) error {
	raw, err := json.Marshal(at.UnixNano())
	if err != nil {
		return fmt.Errorf("store: encode watermark: %w", err)
	}
	return t.S.Put(CollWatermarks, connector+"\x00"+kind, raw, time.Time{})
}

// Snapshot is a record as it stood at the last successful sync.
//
// It holds the values as well as the digest, and the values are what make
// field-level conflict resolution possible at all: merging two edits is a
// three-way merge, and a three-way merge needs the base. Storing only the
// digest answers "did this record change" but never "which field changed",
// so every field of a changed record looks contested and the merge degrades
// into an escalation.
//
// The cost is bounded: only mapped fields are stored, and only one version.
type Snapshot struct {
	Hash   string         `json:"hash"`
	Values map[string]any `json:"values,omitempty"`
	At     time.Time      `json:"at"`
}

// Snapshot returns the record as it stood at the last successful sync.
func (t *Typed) Snapshot(r Ref) (Snapshot, bool, error) {
	raw, ok, err := t.S.Get(CollSnapshots, r.key())
	if err != nil || !ok {
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return Snapshot{}, false, fmt.Errorf("store: decode snapshot: %w", err)
	}
	return snap, true, nil
}

// SetSnapshot records the record as it now stands.
func (t *Typed) SetSnapshot(r Ref, snap Snapshot) error {
	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("store: encode snapshot: %w", err)
	}
	return t.S.Put(CollSnapshots, r.key(), raw, time.Time{})
}

// Origin is one entry in the write log echo suppression consumes.
type Origin struct {
	Ref       Ref       `json:"ref"`
	Hash      string    `json:"hash"`
	WrittenAt time.Time `json:"written_at"`
}

// OriginTag is the key an origin entry is filed under. Folding the payload
// digest in is what makes suppression work on CRMs that do not report who
// made a change: we recognise our write by what it said, not by who said it.
func OriginTag(r Ref, hash string) string {
	return r.key() + "\x00" + hash
}

// PutOrigin records that we wrote this exact payload, for expiresAt.
func (t *Typed) PutOrigin(o Origin, expiresAt time.Time) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return fmt.Errorf("store: encode origin: %w", err)
	}
	return t.S.Put(CollOrigins, OriginTag(o.Ref, o.Hash), raw, expiresAt)
}

// TakeOrigin consumes an entry, reporting whether this change was ours.
//
// Entries are single-use on purpose. If our write is followed by a genuine
// second change with the same payload, only the first inbound event is our
// echo; suppressing both would drop a real change.
func (t *Typed) TakeOrigin(r Ref, hash string) (Origin, bool, error) {
	raw, ok, err := t.S.Take(CollOrigins, OriginTag(r, hash))
	if err != nil || !ok {
		return Origin{}, false, err
	}
	var o Origin
	if err := json.Unmarshal(raw, &o); err != nil {
		return Origin{}, false, fmt.Errorf("store: decode origin: %w", err)
	}
	return o, true, nil
}

// IdemResult is what a write produced, remembered so a retry can skip it.
//
// InFlight marks intent recorded before the call went out. It is the only way
// to tell "this write never happened" from "this write may have landed and we
// lost the response", and those two need different recovery.
type IdemResult struct {
	RemoteID  string    `json:"remote_id"`
	Created   bool      `json:"created"`
	InFlight  bool      `json:"in_flight,omitempty"`
	WrittenAt time.Time `json:"written_at"`
}

// Idem looks up a previous result for an idempotency key.
func (t *Typed) Idem(key string) (IdemResult, bool, error) {
	raw, ok, err := t.S.Get(CollIdem, key)
	if err != nil || !ok {
		return IdemResult{}, false, err
	}
	var r IdemResult
	if err := json.Unmarshal(raw, &r); err != nil {
		return IdemResult{}, false, fmt.Errorf("store: decode idem: %w", err)
	}
	return r, true, nil
}

// PutIdem remembers a write result until expiresAt.
func (t *Typed) PutIdem(key string, r IdemResult, expiresAt time.Time) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("store: encode idem: %w", err)
	}
	return t.S.Put(CollIdem, key, raw, expiresAt)
}

// SeenDelivery records a webhook delivery ID and reports whether it had
// already been seen. A CRM redelivering a webhook is normal, not an error.
func (t *Typed) SeenDelivery(connector, deliveryID string, at, expiresAt time.Time) (bool, error) {
	key := connector + "\x00" + deliveryID
	_, ok, err := t.S.Get(CollDeliveries, key)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	raw, err := json.Marshal(at.UnixNano())
	if err != nil {
		return false, fmt.Errorf("store: encode delivery: %w", err)
	}
	return false, t.S.Put(CollDeliveries, key, raw, expiresAt)
}

// QueueItem is one entry in the dead-letter or review queue. Both carry the
// same envelope: an ID that sorts by time, the payload, and enough context to
// act on it without going back to the logs.
type QueueItem struct {
	ID        string          `json:"id"`
	Sync      string          `json:"sync"`
	Ref       Ref             `json:"ref"`
	Reason    string          `json:"reason"`
	CreatedAt time.Time       `json:"created_at"`
	Attempts  int             `json:"attempts,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// NewQueueID builds an ID that sorts chronologically, so List returns oldest
// first without the store needing a secondary index.
func NewQueueID(at time.Time, seq uint64) string {
	return fmt.Sprintf("%020d-%010d", at.UTC().UnixNano(), seq)
}

// PutQueueItem writes to CollDLQ or CollReview.
func (t *Typed) PutQueueItem(collection string, item QueueItem) error {
	raw, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("store: encode queue item: %w", err)
	}
	return t.S.Put(collection, item.ID, raw, time.Time{})
}

// GetQueueItem reads one item.
func (t *Typed) GetQueueItem(collection, id string) (QueueItem, bool, error) {
	raw, ok, err := t.S.Get(collection, id)
	if err != nil || !ok {
		return QueueItem{}, false, err
	}
	var item QueueItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return QueueItem{}, false, fmt.Errorf("store: decode queue item: %w", err)
	}
	return item, true, nil
}

// ListQueueItems returns items oldest first.
func (t *Typed) ListQueueItems(collection string, limit int) ([]QueueItem, error) {
	entries, err := t.S.List(collection, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueueItem, 0, len(entries))
	for _, e := range entries {
		var item QueueItem
		if err := json.Unmarshal(e.Value, &item); err != nil {
			return nil, fmt.Errorf("store: decode queue item %s: %w", e.Key, err)
		}
		out = append(out, item)
	}
	return out, nil
}

// DeleteQueueItem removes one item.
func (t *Typed) DeleteQueueItem(collection, id string) error {
	return t.S.Delete(collection, id)
}
