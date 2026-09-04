// Package clock provides the time source the engine depends on.
//
// Every timing decision in crm-bisync (echo-suppression windows, watermark
// overlap, retry backoff, rate-limit refill) is a correctness concern, so
// tests must be able to drive time directly instead of sleeping. Production
// code takes a Clock; tests take Manual.
package clock

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Clock is the time source used throughout the engine.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives once, after d has elapsed on this
	// clock. The returned channel is buffered, so a caller that stops
	// listening never blocks the clock.
	After(d time.Duration) <-chan time.Time
	// Sleep blocks for d or until ctx is done, whichever happens first. It
	// returns ctx.Err() when it was cut short.
	Sleep(ctx context.Context, d time.Duration) error
}

// Real is the wall-clock implementation.
type Real struct{}

// Now reports the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// After wraps time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Sleep waits for d, honouring ctx cancellation.
func (Real) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// Manual is a Clock whose time only moves when a test advances it. Safe for
// concurrent use, which matters because the engine's workers all share one
// clock and the suite runs under -race.
type Manual struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

// NewManual returns a Manual clock positioned at start.
func NewManual(start time.Time) *Manual {
	return &Manual{now: start}
}

// Now reports the clock's current position.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After registers a waiter that fires once the clock reaches now+d. A
// non-positive d fires immediately, matching time.After.
func (m *Manual) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- m.now
		return ch
	}
	m.waiters = append(m.waiters, &waiter{at: m.now.Add(d), ch: ch})
	return ch
}

// Sleep waits on After, honouring ctx cancellation.
func (m *Manual) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-m.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Advance moves the clock forward by d and fires every waiter whose deadline
// the move crossed, in deadline order. Firing in order matters: a backoff test
// that expects retry 1 before retry 2 would otherwise be flaky.
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now

	var due []*waiter
	kept := m.waiters[:0]
	for _, w := range m.waiters {
		if !w.at.After(now) {
			due = append(due, w)
			continue
		}
		kept = append(kept, w)
	}
	m.waiters = kept
	m.mu.Unlock()

	sort.SliceStable(due, func(i, j int) bool { return due[i].at.Before(due[j].at) })
	for _, w := range due {
		w.ch <- w.at
	}
}

// Pending reports how many waiters have not fired yet. Tests use it to assert
// that the engine actually stopped scheduling work.
func (m *Manual) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.waiters)
}

var (
	_ Clock = Real{}
	_ Clock = (*Manual)(nil)
)
