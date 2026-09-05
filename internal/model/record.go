// Package model holds the canonical shapes the engine moves between
// connectors. Nothing here knows about HubSpot, Twenty or HTTP.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// Record is one object on one side of a sync, already translated into
// canonical field names by the mapper.
type Record struct {
	Kind      string         `json:"kind"`
	RemoteID  string         `json:"remote_id"`
	Fields    map[string]any `json:"fields"`
	UpdatedAt time.Time      `json:"updated_at"`
	// Version is the peer's ETag or version token. Empty when the connector
	// reports Caps.ETags == false.
	Version string `json:"version,omitempty"`
}

// ChangeEvent is a notification that something changed on one side. Record may
// be nil: several CRMs send notification-only webhooks, so the pipeline has to
// be able to re-fetch rather than assume a payload is attached.
type ChangeEvent struct {
	Source     string    `json:"source"`
	Kind       string    `json:"kind"`
	RemoteID   string    `json:"remote_id"`
	DeliveryID string    `json:"delivery_id,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
	Deleted    bool      `json:"deleted,omitempty"`
	Record     *Record   `json:"record,omitempty"`
}

// Hydrated reports whether the event already carries the record body.
func (e ChangeEvent) Hydrated() bool { return e.Record != nil }

// SnapshotHash is a stable digest of the mapped fields of a record.
//
// It is the load-bearing primitive for two separate mechanisms, which is why
// it lives in model rather than in either of them:
//
//   - conflict detection compares the current hash against the hash stored at
//     the last successful sync, which is what distinguishes "changed since we
//     synced" from "changed in this request";
//   - echo suppression folds the hash into the origin tag, so a write we
//     caused is recognised even when the peer does not report an actor.
//
// Only the named fields take part, in sorted order, so an unmapped field
// churning on the peer cannot invalidate the hash and cause a write storm.
func SnapshotHash(fields map[string]any, names []string) string {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	h := sha256.New()
	for _, name := range sorted {
		// Length-prefix every part so ("ab","c") cannot collide with ("a","bc").
		writeChunk(h, name)
		v, ok := fields[name]
		if !ok {
			writeChunk(h, "\x00absent")
			continue
		}
		writeChunk(h, canonicalValue(v))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeChunk(h interface{ Write([]byte) (int, error) }, s string) {
	// Hash writers never return an error, so the length prefix plus payload
	// can be written without error handling.
	_, _ = h.Write([]byte(strconv.Itoa(len(s))))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(s))
}

// canonicalValue renders a field value so that values which are equal for sync
// purposes render identically. JSON round-trips turn integers into float64 and
// timestamps into strings, so 1 and 1.0 must not look like a change.
func canonicalValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "\x00null"
	case string:
		return "s" + t
	case bool:
		return "b" + strconv.FormatBool(t)
	case time.Time:
		return "t" + t.UTC().Format(time.RFC3339Nano)
	case int:
		return numeric(float64(t))
	case int32:
		return numeric(float64(t))
	case int64:
		return numeric(float64(t))
	case float32:
		return numeric(float64(t))
	case float64:
		return numeric(t)
	default:
		return "x" + fmt.Sprintf("%v", t)
	}
}

// numeric renders whole floats without a fractional part, so the float64 that
// survives a JSON round-trip hashes the same as the int it started as.
func numeric(f float64) string {
	if f == float64(int64(f)) {
		return "n" + strconv.FormatInt(int64(f), 10)
	}
	return "n" + strconv.FormatFloat(f, 'g', -1, 64)
}
