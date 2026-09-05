// Package ratelimit keeps the engine inside a peer's quota.
//
// The bucket is per connector and shared across pollers, hydration reads and
// writes, because the peer's limit is per account rather than per code path.
// A separate limiter per worker is the same mistake as no limiter at all,
// arrived at with more code.
//
// It adapts. A 429 does not just cost a wait: it means the configured rate was
// wrong, and continuing at that rate spends the next minute making the same
// mistake. So a rejection shrinks the rate, and sustained success walks it
// back up.
package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"

	"crm-bisync/internal/clock"
)

// Tuning constants. They are deliberately conservative: recovering slowly
// costs throughput, recovering quickly costs another 429, and a 429 is the
// expensive one because it wastes a request and delays the whole batch.
const (
	// shrinkFactor is how much of the rate survives a rejection.
	shrinkFactor = 0.5
	// minRateFraction is the floor, as a fraction of the configured rate.
	// Below this the sync is effectively stopped, and stopping quietly is
	// worse than being slow.
	minRateFraction = 0.125
	// recoverFactor is how much the rate grows per successful step.
	recoverFactor = 1.25
	// recoverAfter is how many consecutive successes earn a step up.
	recoverAfter = 20
)

// Stats is what the bucket has been doing.
type Stats struct {
	Granted    int64
	Waited     int64
	WaitTotal  time.Duration
	Rejections int64
	// Rate is the current effective rate, which is below Configured whenever
	// the peer has recently pushed back.
	Rate       float64
	Configured float64
}

// Bucket is a token bucket with adaptive rate.
//
// Safe for concurrent use: every worker touching one connector shares one.
type Bucket struct {
	clock clock.Clock

	mu         sync.Mutex
	configured float64
	rate       float64
	burst      float64
	tokens     float64
	last       time.Time
	// blockedUntil honours a peer-supplied Retry-After, which overrides the
	// bucket entirely: the peer has told us the answer.
	blockedUntil time.Time
	successes    int
	stats        Stats
}

// New builds a bucket. A non-positive rate or burst is corrected to something
// usable rather than rejected, because a misconfigured limiter that refuses to
// start is worse than one that runs conservatively.
func New(c clock.Clock, ratePerSec float64, burst int) *Bucket {
	if c == nil {
		c = clock.Real{}
	}
	if ratePerSec <= 0 {
		ratePerSec = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &Bucket{
		clock:      c,
		configured: ratePerSec,
		rate:       ratePerSec,
		burst:      float64(burst),
		tokens:     float64(burst),
		last:       c.Now(),
	}
}

// refill adds the tokens earned since the last call. The caller holds the lock.
func (b *Bucket) refill(now time.Time) {
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.last = now
	// The cap is the burst, but the floor is whatever debt the queue has run
	// up: refilling must not forgive a wait a caller has already been told to
	// take.
	b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
}

// Reserve consumes a token and reports how long the caller must wait before
// using it. Zero means go now.
//
// Splitting reservation from waiting is what makes the timing testable: a test
// asserts on the returned duration instead of measuring wall-clock sleeps.
func (b *Bucket) Reserve() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.refill(now)

	var wait time.Duration
	if b.blockedUntil.After(now) {
		wait = b.blockedUntil.Sub(now)
	}

	// The balance is allowed to go negative, and that is what makes callers
	// queue. If it were clamped at zero, two callers arriving at an empty
	// bucket would both be told to wait one token's worth, then fire together,
	// producing exactly the burst the limiter exists to prevent.
	b.tokens--
	if b.tokens < 0 {
		owed := time.Duration(-b.tokens / b.rate * float64(time.Second))
		if owed > wait {
			wait = owed
		}
	}

	b.stats.Granted++
	if wait > 0 {
		b.stats.Waited++
		b.stats.WaitTotal += wait
	}
	return wait
}

// Wait reserves a token and blocks for it, honouring ctx.
func (b *Bucket) Wait(ctx context.Context) error {
	wait := b.Reserve()
	if wait <= 0 {
		return ctx.Err()
	}
	return b.clock.Sleep(ctx, wait)
}

// Allow reports whether a token is available right now, without waiting.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.refill(now)
	if b.blockedUntil.After(now) || b.tokens < 1 {
		return false
	}
	b.tokens--
	b.stats.Granted++
	return true
}

// Reject records that the peer pushed back.
//
// retryAfter is what the peer asked for, or zero when it said nothing. Zero is
// the common case and is why the bucket has its own backoff at all.
func (b *Bucket) Reject(retryAfter time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.stats.Rejections++
	b.successes = 0

	// The configured rate was wrong. Continuing at it spends the next minute
	// repeating the mistake, so shrink first and ask questions later.
	floor := b.configured * minRateFraction
	b.rate = math.Max(floor, b.rate*shrinkFactor)

	// Everything already in the bucket was earned at a rate the peer has just
	// told us is too high, so it is not spendable. Existing debt is kept: the
	// callers already queued behind it are still queued.
	if b.tokens > 0 {
		b.tokens = 0
	}
	b.last = now

	wait := retryAfter
	if wait <= 0 {
		// No hint from the peer: back off by one token's worth at the reduced
		// rate, which scales with how hard we have already been pushed back.
		wait = time.Duration(float64(time.Second) / b.rate)
	}
	until := now.Add(wait)
	if until.After(b.blockedUntil) {
		b.blockedUntil = until
	}
}

// Success records a call the peer accepted, which is what earns the rate back.
func (b *Bucket) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rate >= b.configured {
		return
	}
	b.successes++
	if b.successes < recoverAfter {
		return
	}
	b.successes = 0
	b.rate = math.Min(b.configured, b.rate*recoverFactor)
}

// Rate is the current effective rate.
func (b *Bucket) Rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// BlockedFor reports how long the peer has asked us to stay away.
func (b *Bucket) BlockedFor() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d := b.blockedUntil.Sub(b.clock.Now()); d > 0 {
		return d
	}
	return 0
}

// Stats returns a snapshot.
func (b *Bucket) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := b.stats
	out.Rate = b.rate
	out.Configured = b.configured
	return out
}

// Set is a bucket per connector, which is the unit a quota actually applies to.
type Set struct {
	mu      sync.Mutex
	clock   clock.Clock
	buckets map[string]*Bucket
}

// NewSet builds an empty set.
func NewSet(c clock.Clock) *Set {
	if c == nil {
		c = clock.Real{}
	}
	return &Set{clock: c, buckets: make(map[string]*Bucket)}
}

// Add registers a connector's limits.
func (s *Set) Add(connector string, ratePerSec float64, burst int) *Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := New(s.clock, ratePerSec, burst)
	s.buckets[connector] = b
	return b
}

// For returns a connector's bucket, creating a permissive default if the
// connector was never registered.
//
// Returning nil would push a nil check into every call site and one of them
// would eventually be missing, which is an unlimited caller pointed at
// somebody's production CRM.
func (s *Set) For(connector string) *Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()

	if b, ok := s.buckets[connector]; ok {
		return b
	}
	b := New(s.clock, 5, 10)
	s.buckets[connector] = b
	return b
}
