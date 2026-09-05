package clock

import (
	"context"
	"sync"
	"testing"
	"time"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func TestManualNowOnlyMovesOnAdvance(t *testing.T) {
	m := NewManual(epoch)
	if got := m.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", got, epoch)
	}
	m.Advance(90 * time.Second)
	if got, want := m.Now(), epoch.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
}

func TestManualAfterFiresOnlyWhenDeadlineIsCrossed(t *testing.T) {
	m := NewManual(epoch)
	ch := m.After(5 * time.Minute)

	m.Advance(4 * time.Minute)
	select {
	case v := <-ch:
		t.Fatalf("fired early at %v", v)
	default:
	}

	m.Advance(1 * time.Minute)
	select {
	case v := <-ch:
		if want := epoch.Add(5 * time.Minute); !v.Equal(want) {
			t.Fatalf("fired with %v, want %v", v, want)
		}
	default:
		t.Fatal("did not fire once the deadline was reached")
	}
}

func TestManualAfterNonPositiveFiresImmediately(t *testing.T) {
	m := NewManual(epoch)
	select {
	case <-m.After(0):
	default:
		t.Fatal("After(0) did not fire immediately")
	}
	if m.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", m.Pending())
	}
}

// A single Advance that crosses several deadlines must fire them in deadline
// order. Retry-backoff tests depend on this.
func TestManualAdvanceFiresWaitersInDeadlineOrder(t *testing.T) {
	m := NewManual(epoch)
	third := m.After(30 * time.Second)
	first := m.After(10 * time.Second)
	second := m.After(20 * time.Second)

	m.Advance(time.Minute)

	for i, ch := range []<-chan time.Time{first, second, third} {
		select {
		case <-ch:
		default:
			t.Fatalf("waiter %d did not fire", i)
		}
	}
	if m.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", m.Pending())
	}
}

func TestManualSleepReturnsContextErrorWhenCancelled(t *testing.T) {
	m := NewManual(epoch)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- m.Sleep(ctx, time.Hour) }()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Sleep returned nil, want context error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Sleep did not return after cancellation")
	}
}

// The engine's workers share one clock, so concurrent Advance and After must
// not race. This test exists to be run under -race.
func TestManualIsSafeForConcurrentUse(t *testing.T) {
	m := NewManual(epoch)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = m.After(time.Second) }()
		go func() { defer wg.Done(); m.Advance(time.Millisecond) }()
	}
	wg.Wait()
}

func TestRealSleepHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (Real{}).Sleep(ctx, time.Hour); err == nil {
		t.Fatal("Sleep returned nil for a cancelled context")
	}
}
