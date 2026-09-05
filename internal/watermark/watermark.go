// Package watermark tracks how far a delta read has got.
//
// Two failure modes shape everything here, and both lose records silently:
//
//  1. Advancing the mark to "now" after a poll. A record modified while the
//     poll was running gets a timestamp before now and is never read again.
//  2. Advancing the mark before the work durably lands. A crash between the
//     read and the write means the next poll starts after records that were
//     never applied.
//
// So the mark only ever moves to the highest timestamp actually observed, only
// after the caller says the page committed, and every read reaches back past
// it by an overlap window. The overlap makes re-reading normal, which is
// exactly why every write is idempotent: the two mechanisms were designed
// together rather than one bolted onto the other.
package watermark

import (
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

// DefaultOverlap is how far back a read reaches when nothing else is known.
const DefaultOverlap = 2 * time.Minute

// CoarseOverlap is the floor applied when a peer's timestamps are not exact.
//
// A peer that rounds modification times down to the second reports a record
// modified at 12:00:00.900 as 12:00:00, so a mark set from an exact clock can
// step past it. The overlap has to exceed the granularity, and by enough to
// cover the peer's own write latency as well.
const CoarseOverlap = 5 * time.Minute

// PeerConfig describes one peer's timing behaviour.
type PeerConfig struct {
	// Overlap is the configured window. Zero means DefaultOverlap.
	Overlap time.Duration
	// ExactTimestamps is false when the peer rounds or lags its modification
	// times, which widens the window to at least CoarseOverlap.
	ExactTimestamps bool
}

// Tracker reads and advances watermarks.
type Tracker struct {
	typed *store.Typed
	clock clock.Clock
	peers map[string]PeerConfig
}

// New builds a tracker.
func New(typed *store.Typed, c clock.Clock, peers map[string]PeerConfig) *Tracker {
	if c == nil {
		c = clock.Real{}
	}
	if peers == nil {
		peers = map[string]PeerConfig{}
	}
	return &Tracker{typed: typed, clock: c, peers: peers}
}

// Overlap is the window a peer's reads reach back by.
func (t *Tracker) Overlap(connector string) time.Duration {
	cfg := t.peers[connector]

	overlap := cfg.Overlap
	if overlap <= 0 {
		overlap = DefaultOverlap
	}
	if !cfg.ExactTimestamps && overlap < CoarseOverlap {
		// Widening silently would hide a misconfiguration, so this is a floor
		// rather than a replacement: an operator who set a larger window keeps
		// it, and one who set a smaller one gets a window that actually works.
		overlap = CoarseOverlap
	}
	return overlap
}

// Since returns the timestamp a delta read should start from.
//
// The second return value is false when no mark exists yet, which means a full
// backfill rather than an incremental read. Callers need the difference:
// backfill is paged and resumable and may take a long time, and treating it as
// an ordinary poll is how a first run times out halfway through.
func (t *Tracker) Since(connector, kind string) (time.Time, bool, error) {
	mark, ok, err := t.typed.Watermark(connector, kind)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	return mark.Add(-t.Overlap(connector)), true, nil
}

// Mark returns the stored high-water mark, without the overlap applied.
func (t *Tracker) Mark(connector, kind string) (time.Time, bool, error) {
	return t.typed.Watermark(connector, kind)
}

// Advance commits a new high-water mark.
//
// highest is the largest modification time in the page that just landed, not
// the current time. Using the clock here is the classic way to lose a record
// modified during the read.
//
// The mark never moves backwards. A page whose records are all older than the
// mark is normal, since the overlap window guarantees re-reads, and rewinding
// on one would make every poll re-read progressively more.
func (t *Tracker) Advance(connector, kind string, highest time.Time) error {
	if highest.IsZero() {
		return nil
	}
	// A peer whose clock runs ahead of ours would otherwise push the mark into
	// the future and blind the sync until real time caught up.
	if now := t.clock.Now(); highest.After(now) {
		highest = now
	}

	current, ok, err := t.typed.Watermark(connector, kind)
	if err != nil {
		return err
	}
	if ok && !highest.After(current) {
		return nil
	}
	return t.typed.SetWatermark(connector, kind, highest)
}

// Highest returns the largest of a set of record timestamps, for handing to
// Advance. It ignores zero times, so a peer that omits a modification time on
// some records cannot drag the mark to the epoch.
func Highest(times ...time.Time) time.Time {
	var out time.Time
	for _, t := range times {
		if t.IsZero() {
			continue
		}
		if out.IsZero() || t.After(out) {
			out = t
		}
	}
	return out
}

// Lag reports how far behind a peer's mark is, which is the one number worth
// alerting on: it goes wrong quietly, and it is the first sign that a sync has
// stopped keeping up rather than stopped working.
func (t *Tracker) Lag(connector, kind string) (time.Duration, bool, error) {
	mark, ok, err := t.typed.Watermark(connector, kind)
	if err != nil || !ok {
		return 0, false, err
	}
	return t.clock.Now().Sub(mark), true, nil
}

// Reset clears a mark, forcing the next read to be a full backfill.
func (t *Tracker) Reset(connector, kind string) error {
	return t.typed.S.Delete(store.CollWatermarks, connector+"\x00"+kind)
}
