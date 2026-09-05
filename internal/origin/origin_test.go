package origin_test

import (
	"sync"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/origin"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func newSuppressor(t *testing.T, window time.Duration) (*origin.Suppressor, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	typed := store.NewTyped(store.NewMem(c))
	return origin.New(typed, c, map[string]time.Duration{"twenty": window}), c
}

func ref(id string) store.Ref {
	return store.Ref{Connector: "twenty", Kind: "contact", RemoteID: id}
}

func TestOurOwnWriteComesBackAsAnEcho(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)

	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	v, err := s.Classify(ref("a"), "hash-1")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v != origin.Echo {
		t.Fatalf("verdict = %s, want echo", v)
	}
	if v.Processed() {
		t.Fatal("an echo was allowed down the pipeline")
	}
}

// Single-use on purpose. If our write is followed by a genuine second change
// carrying the same payload, only the first inbound event is our echo, and
// suppressing both would drop a real change.
func TestAnEchoEntryIsConsumedOnce(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)
	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	if v, _ := s.Classify(ref("a"), "hash-1"); v != origin.Echo {
		t.Fatalf("first classify = %s, want echo", v)
	}
	v, err := s.Classify(ref("a"), "hash-1")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v == origin.Echo {
		t.Fatal("the same origin entry suppressed twice")
	}
	if !v.Processed() {
		t.Fatalf("verdict %s stopped a real change", v)
	}
}

// This is the case actor filtering gets wrong. An automation on the peer
// rewrites the record milliseconds after our write, often under our own user.
// Filtering on who wrote it would drop that change; filtering on what was
// written does not.
func TestAPeerRewritingOurPayloadIsARealChange(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)
	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	v, err := s.Classify(ref("a"), "hash-2")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v != origin.NearMiss {
		t.Fatalf("verdict = %s, want near_miss", v)
	}
	if !v.Processed() {
		t.Fatal("a near miss stopped a real change")
	}
	// The counter is the point: a rising near-miss rate is the early warning
	// that a peer automation is fighting the sync.
	if s.Stats().NearMiss != 1 {
		t.Fatalf("NearMiss = %d, want 1", s.Stats().NearMiss)
	}
}

func TestAChangeToARecordWeNeverTouchedIsForeign(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)

	v, err := s.Classify(ref("z"), "hash-1")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v != origin.Foreign {
		t.Fatalf("verdict = %s, want foreign", v)
	}
	if s.Stats().Foreign != 1 {
		t.Fatalf("Foreign = %d, want 1", s.Stats().Foreign)
	}
}

// The window has to exceed the peer's delivery latency. A delivery that
// arrives after the entry expired is the start of a loop, and this test pins
// the behaviour so the failure is at least legible when it happens.
func TestADeliveryPastTheWindowIsNoLongerRecognised(t *testing.T) {
	s, c := newSuppressor(t, 2*time.Minute)
	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	c.Advance(3 * time.Minute)
	v, err := s.Classify(ref("a"), "hash-1")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v == origin.Echo {
		t.Fatal("an entry past its window still suppressed")
	}
}

func TestWindowIsPerConnectorWithADefault(t *testing.T) {
	s, _ := newSuppressor(t, 90*time.Second)

	if got := s.Window("twenty"); got != 90*time.Second {
		t.Fatalf("Window(twenty) = %s, want 1m30s", got)
	}
	if got := s.Window("hubspot"); got != 5*time.Minute {
		t.Fatalf("Window(hubspot) = %s, want the 5m default", got)
	}
}

// Two workers classifying the same tag must not both be told it was theirs,
// or one genuine change disappears. Run under -race.
func TestConcurrentClassifyYieldsExactlyOneEcho(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)
	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	const racers = 16
	var wg sync.WaitGroup
	verdicts := make(chan origin.Verdict, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.Classify(ref("a"), "hash-1")
			if err == nil {
				verdicts <- v
			}
		}()
	}
	wg.Wait()
	close(verdicts)

	echoes := 0
	for v := range verdicts {
		if v == origin.Echo {
			echoes++
		}
	}
	if echoes != 1 {
		t.Fatalf("%d goroutines were told the change was theirs, want exactly 1", echoes)
	}
}

func TestSweepDropsExpiredEntries(t *testing.T) {
	s, c := newSuppressor(t, time.Minute)
	if err := s.Record(ref("a"), "hash-1"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	c.Advance(2 * time.Minute)
	n, err := s.Sweep()
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// The payload entry and the per-record marker both go.
	if n != 2 {
		t.Fatalf("Sweep dropped %d entries, want 2", n)
	}
	if s.Stats().Swept != 2 {
		t.Fatalf("Swept = %d, want 2", s.Stats().Swept)
	}
}

// The near-miss lookup must not depend on scanning the write log, because
// that would make every inbound event cost a pass over every recent write.
func TestNearMissDetectionSurvivesManyRecordedWrites(t *testing.T) {
	s, _ := newSuppressor(t, 5*time.Minute)
	for i := 0; i < 200; i++ {
		if err := s.Record(store.Ref{Connector: "twenty", Kind: "contact", RemoteID: string(rune('a' + i%26))}, "h"+string(rune('a'+i%26))); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	v, err := s.Classify(ref("a"), "a-different-hash")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if v != origin.NearMiss {
		t.Fatalf("verdict = %s, want near_miss", v)
	}
}
