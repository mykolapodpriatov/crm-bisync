package ratelimit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/ratelimit"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func newBucket(t *testing.T, rate float64, burst int) (*ratelimit.Bucket, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	return ratelimit.New(c, rate, burst), c
}

func TestBurstIsSpentThenRefilled(t *testing.T) {
	b, c := newBucket(t, 2, 3)

	for i := 0; i < 3; i++ {
		if wait := b.Reserve(); wait != 0 {
			t.Fatalf("call %d waited %s inside the burst", i, wait)
		}
	}
	if wait := b.Reserve(); wait <= 0 {
		t.Fatal("the call past the burst did not wait")
	}

	// At two per second, a second buys two tokens.
	c.Advance(time.Second)
	b2, c2 := newBucket(t, 2, 3)
	for i := 0; i < 3; i++ {
		b2.Reserve()
	}
	c2.Advance(time.Second)
	if wait := b2.Reserve(); wait != 0 {
		t.Fatalf("a refilled token still waited %s", wait)
	}
}

func TestReserveQueuesCallersRatherThanReleasingThemTogether(t *testing.T) {
	b, _ := newBucket(t, 1, 1)

	b.Reserve()
	first := b.Reserve()
	second := b.Reserve()

	if first <= 0 || second <= 0 {
		t.Fatalf("waits = %s and %s, want both positive", first, second)
	}
	// If both waited the same amount they would fire together and the peer
	// would see a burst at exactly the moment we were trying to avoid one.
	if second <= first {
		t.Fatalf("second waiter (%s) did not queue behind the first (%s)", second, first)
	}
}

func TestAllowDoesNotBlock(t *testing.T) {
	b, c := newBucket(t, 1, 1)

	if !b.Allow() {
		t.Fatal("the first call was not allowed")
	}
	if b.Allow() {
		t.Fatal("a second call was allowed with an empty bucket")
	}
	c.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("a refilled token was not allowed")
	}
}

// A 429 does not just cost a wait. It means the configured rate was wrong, and
// continuing at it spends the next minute repeating the mistake.
func TestRejectionShrinksTheRate(t *testing.T) {
	b, _ := newBucket(t, 8, 8)

	b.Reject(0)
	if got := b.Rate(); got != 4 {
		t.Fatalf("rate after one rejection = %v, want 4", got)
	}
	b.Reject(0)
	if got := b.Rate(); got != 2 {
		t.Fatalf("rate after two rejections = %v, want 2", got)
	}
}

// The floor matters: below it the sync is effectively stopped, and stopping
// quietly is worse than being slow.
func TestTheRateHasAFloor(t *testing.T) {
	b, _ := newBucket(t, 8, 8)

	for i := 0; i < 20; i++ {
		b.Reject(0)
	}
	if got, want := b.Rate(), 1.0; got != want {
		t.Fatalf("rate = %v, want the floor %v", got, want)
	}
}

func TestRetryAfterIsHonouredOverTheBucket(t *testing.T) {
	b, c := newBucket(t, 100, 100)

	b.Reject(30 * time.Second)
	if got := b.BlockedFor(); got != 30*time.Second {
		t.Fatalf("BlockedFor = %s, want 30s", got)
	}
	// Even with a full bucket, the peer's answer wins.
	if wait := b.Reserve(); wait < 29*time.Second {
		t.Fatalf("Reserve waited %s despite a 30s Retry-After", wait)
	}

	c.Advance(31 * time.Second)
	if got := b.BlockedFor(); got != 0 {
		t.Fatalf("BlockedFor = %s after the block elapsed", got)
	}
}

// A peer that says nothing is the common case, which is why the bucket needs
// its own backoff at all.
func TestRejectionWithoutAHintStillBacksOff(t *testing.T) {
	b, _ := newBucket(t, 4, 4)

	b.Reject(0)
	if got := b.BlockedFor(); got <= 0 {
		t.Fatal("a rejection with no Retry-After did not back off")
	}
	// The backoff scales with the reduced rate, so repeated pushback gets
	// progressively more patient rather than hammering at a fixed interval.
	first := b.BlockedFor()
	b.Reject(0)
	if b.BlockedFor() <= first {
		t.Fatalf("the second backoff (%s) was not longer than the first (%s)",
			b.BlockedFor(), first)
	}
}

// Everything in the bucket was earned at a rate the peer has just called too
// high, so it is not spendable.
func TestRejectionEmptiesTheBucket(t *testing.T) {
	b, c := newBucket(t, 10, 10)

	b.Reject(0)
	c.Advance(b.BlockedFor() + time.Millisecond)
	if b.Allow() {
		// One token may have refilled during the block, which is fine; what
		// must not survive is the whole pre-rejection burst.
		if b.Allow() {
			t.Fatal("the burst survived a rejection")
		}
	}
}

func TestSustainedSuccessWalksTheRateBackUp(t *testing.T) {
	b, _ := newBucket(t, 8, 8)

	b.Reject(0)
	reduced := b.Rate()

	for i := 0; i < 19; i++ {
		b.Success()
	}
	if b.Rate() != reduced {
		t.Fatalf("the rate recovered after %d successes, too eagerly", 19)
	}

	b.Success()
	if b.Rate() <= reduced {
		t.Fatalf("the rate did not recover: %v", b.Rate())
	}
	if b.Rate() > 8 {
		t.Fatalf("the rate recovered past its configured value: %v", b.Rate())
	}
}

// A rejection mid-recovery has to reset the streak, or a peer that rejects one
// call in twenty gets treated as healthy.
func TestARejectionResetsTheRecoveryStreak(t *testing.T) {
	b, _ := newBucket(t, 8, 8)

	b.Reject(0)
	for i := 0; i < 19; i++ {
		b.Success()
	}
	b.Reject(0)
	rate := b.Rate()

	for i := 0; i < 19; i++ {
		b.Success()
	}
	if b.Rate() != rate {
		t.Fatalf("the streak was not reset: rate moved to %v", b.Rate())
	}
}

func TestSuccessNeverExceedsTheConfiguredRate(t *testing.T) {
	b, _ := newBucket(t, 5, 5)

	for i := 0; i < 500; i++ {
		b.Success()
	}
	if got := b.Rate(); got != 5 {
		t.Fatalf("rate = %v, want the configured 5", got)
	}
}

func TestWaitHonoursContextCancellation(t *testing.T) {
	b, _ := newBucket(t, 1, 1)
	b.Reserve()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx); err == nil {
		t.Fatal("Wait ignored a cancelled context")
	}
}

func TestStats(t *testing.T) {
	b, _ := newBucket(t, 1, 1)

	b.Reserve()
	b.Reserve()
	b.Reject(time.Second)

	s := b.Stats()
	if s.Granted != 2 {
		t.Fatalf("Granted = %d, want 2", s.Granted)
	}
	if s.Waited != 1 {
		t.Fatalf("Waited = %d, want 1", s.Waited)
	}
	if s.WaitTotal <= 0 {
		t.Fatalf("WaitTotal = %s, want a positive total", s.WaitTotal)
	}
	if s.Rejections != 1 {
		t.Fatalf("Rejections = %d, want 1", s.Rejections)
	}
	if s.Configured != 1 {
		t.Fatalf("Configured = %v, want 1", s.Configured)
	}
}

// Every worker touching one connector shares one bucket, because the peer's
// quota is per account. Run under -race.
func TestBucketIsSafeForConcurrentUse(t *testing.T) {
	b, _ := newBucket(t, 1000, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); b.Reserve() }()
		go func() { defer wg.Done(); b.Reject(0) }()
		go func() { defer wg.Done(); b.Success(); _ = b.Stats() }()
	}
	wg.Wait()
}

// A connector nobody registered must not end up unlimited. Returning nil would
// push a nil check into every call site, and one of them would eventually be
// missing, pointed at somebody's production CRM.
func TestSetGivesAnUnregisteredConnectorALimiterAnyway(t *testing.T) {
	set := ratelimit.NewSet(clock.NewManual(epoch))
	set.Add("hubspot", 8, 16)

	if set.For("hubspot") == nil {
		t.Fatal("a registered connector has no bucket")
	}
	stray := set.For("never-registered")
	if stray == nil {
		t.Fatal("an unregistered connector got no limiter at all")
	}
	if stray.Rate() <= 0 {
		t.Fatalf("the default limiter has rate %v", stray.Rate())
	}
	first, second := set.For("hubspot"), set.For("hubspot")
	if first != second {
		t.Fatal("the set handed out two buckets for one connector")
	}
}
