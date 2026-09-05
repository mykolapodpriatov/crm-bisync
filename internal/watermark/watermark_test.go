package watermark_test

import (
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
	"crm-bisync/internal/watermark"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func newTracker(t *testing.T, peers map[string]watermark.PeerConfig) (*watermark.Tracker, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	return watermark.New(store.NewTyped(store.NewMem(c)), c, peers), c
}

func exact(overlap time.Duration) map[string]watermark.PeerConfig {
	return map[string]watermark.PeerConfig{
		"hubspot": {Overlap: overlap, ExactTimestamps: true},
	}
}

// No mark means a full backfill, and the caller needs to know: backfill is
// paged and slow, and treating it as an ordinary poll is how a first run dies
// halfway through.
func TestNoMarkMeansBackfill(t *testing.T) {
	tr, _ := newTracker(t, exact(time.Minute))

	since, ok, err := tr.Since("hubspot", "contact")
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if ok {
		t.Fatalf("Since reported a mark that was never set: %v", since)
	}
}

func TestSinceReachesBackByTheOverlap(t *testing.T) {
	tr, _ := newTracker(t, exact(90*time.Second))

	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	since, ok, err := tr.Since("hubspot", "contact")
	if err != nil || !ok {
		t.Fatalf("Since = %v,%v,%v", since, ok, err)
	}
	if want := epoch.Add(-90 * time.Second); !since.Equal(want) {
		t.Fatalf("Since = %v, want %v", since, want)
	}
}

// A peer that rounds timestamps down reports a record modified at 12:00:00.9
// as 12:00:00, so a mark taken from an exact clock steps straight past it.
// The window has to be wide enough to catch that, and an operator's too-small
// setting must not be honoured into a data loss.
func TestCoarseTimestampsWidenTheWindow(t *testing.T) {
	tr, _ := newTracker(t, map[string]watermark.PeerConfig{
		"twenty": {Overlap: 10 * time.Second, ExactTimestamps: false},
	})

	if got := tr.Overlap("twenty"); got != watermark.CoarseOverlap {
		t.Fatalf("Overlap = %s, want the %s floor", got, watermark.CoarseOverlap)
	}
}

// A floor, not a replacement: an operator who deliberately set a wide window
// keeps it.
func TestAWiderConfiguredOverlapSurvives(t *testing.T) {
	tr, _ := newTracker(t, map[string]watermark.PeerConfig{
		"twenty": {Overlap: time.Hour, ExactTimestamps: false},
	})

	if got := tr.Overlap("twenty"); got != time.Hour {
		t.Fatalf("Overlap = %s, want 1h", got)
	}
}

func TestUnknownConnectorGetsTheDefaultOverlap(t *testing.T) {
	tr, _ := newTracker(t, nil)

	// An unregistered peer is assumed inexact, because assuming exactness is
	// the assumption that loses records.
	if got := tr.Overlap("pipedrive"); got != watermark.CoarseOverlap {
		t.Fatalf("Overlap = %s, want %s", got, watermark.CoarseOverlap)
	}
}

// The mark must never rewind. A page whose records are all older than the mark
// is normal, because the overlap guarantees re-reads, and rewinding on one
// would make every poll re-read progressively more.
func TestAdvanceNeverMovesBackwards(t *testing.T) {
	tr, _ := newTracker(t, exact(time.Minute))

	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if err := tr.Advance("hubspot", "contact", epoch.Add(-time.Hour)); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	mark, _, err := tr.Mark("hubspot", "contact")
	if err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if !mark.Equal(epoch) {
		t.Fatalf("mark rewound to %v", mark)
	}
}

// A peer whose clock runs ahead would otherwise push the mark into the future
// and blind the sync until real time caught up.
func TestAdvanceClampsAFutureTimestamp(t *testing.T) {
	tr, c := newTracker(t, exact(time.Minute))

	if err := tr.Advance("hubspot", "contact", epoch.Add(time.Hour)); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	mark, _, err := tr.Mark("hubspot", "contact")
	if err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if !mark.Equal(c.Now()) {
		t.Fatalf("mark = %v, want it clamped to now (%v)", mark, c.Now())
	}
}

func TestAdvanceIgnoresAZeroTimestamp(t *testing.T) {
	tr, _ := newTracker(t, exact(time.Minute))

	if err := tr.Advance("hubspot", "contact", time.Time{}); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if _, ok, _ := tr.Mark("hubspot", "contact"); ok {
		t.Fatal("a zero timestamp created a mark")
	}
}

// The scenario the whole package exists for: a record modified while the poll
// was running, on a peer whose clock is behind ours. Advancing to "now" would
// step over it; advancing to the highest observed timestamp plus the overlap
// does not.
func TestARecordModifiedDuringThePollIsStillRead(t *testing.T) {
	tr, c := newTracker(t, exact(2*time.Minute))

	// The poll returns records up to 12:00:00 and takes 30 seconds.
	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	c.Advance(30 * time.Second)

	// Meanwhile a record was modified at 12:00:10, before the mark was written
	// but after the page was assembled.
	modified := epoch.Add(10 * time.Second)

	since, _, err := tr.Since("hubspot", "contact")
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if !modified.After(since) {
		t.Fatalf("a record modified at %v would be skipped by a read from %v", modified, since)
	}
}

func TestHighestIgnoresZeroTimes(t *testing.T) {
	a := epoch
	b := epoch.Add(time.Minute)

	if got := watermark.Highest(a, time.Time{}, b); !got.Equal(b) {
		t.Fatalf("Highest = %v, want %v", got, b)
	}
	if got := watermark.Highest(time.Time{}, time.Time{}); !got.IsZero() {
		t.Fatalf("Highest of nothing = %v, want the zero time", got)
	}
	if got := watermark.Highest(); !got.IsZero() {
		t.Fatalf("Highest() = %v, want the zero time", got)
	}
}

// Lag is the number worth alerting on: it goes wrong quietly, and it is the
// first sign a sync stopped keeping up rather than stopped working.
func TestLag(t *testing.T) {
	tr, c := newTracker(t, exact(time.Minute))

	if _, ok, err := tr.Lag("hubspot", "contact"); ok || err != nil {
		t.Fatalf("Lag on an unset mark = %v,%v", ok, err)
	}
	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	c.Advance(7 * time.Minute)
	lag, ok, err := tr.Lag("hubspot", "contact")
	if err != nil || !ok {
		t.Fatalf("Lag = %v,%v", ok, err)
	}
	if lag != 7*time.Minute {
		t.Fatalf("Lag = %s, want 7m", lag)
	}
}

func TestResetForcesABackfill(t *testing.T) {
	tr, _ := newTracker(t, exact(time.Minute))

	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if err := tr.Reset("hubspot", "contact"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, ok, _ := tr.Since("hubspot", "contact"); ok {
		t.Fatal("the mark survived a reset")
	}
}

// Marks are per (connector, kind); one advancing must not move another.
func TestMarksAreScoped(t *testing.T) {
	tr, _ := newTracker(t, exact(time.Minute))

	if err := tr.Advance("hubspot", "contact", epoch); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if _, ok, _ := tr.Mark("hubspot", "company"); ok {
		t.Fatal("advancing contact moved company")
	}
	if _, ok, _ := tr.Mark("twenty", "contact"); ok {
		t.Fatal("advancing one connector moved the other")
	}
}
