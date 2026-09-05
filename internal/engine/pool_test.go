package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"crm-bisync/internal/conflict"
	"crm-bisync/internal/model"
)

func TestTheWorkerPoolProcessesEverything(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := h.engine.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.engine.Stop()

	const records = 25
	for i := 0; i < records; i++ {
		h.left.ExternalUpsert("contact", "", map[string]any{
			"email":     fmt.Sprintf("user%02d@example.com", i),
			"firstname": fmt.Sprintf("User %02d", i),
		})
	}

	if _, err := h.engine.PollAll(ctx); err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	waitIdle(t, h)

	if got := h.right.Count("contact"); got != records {
		t.Fatalf("right has %d records, want %d", got, records)
	}
}

// The race the keyed mutex closes. The same change arriving several times at
// once must produce one record, not one per worker that saw it.
func TestConcurrentDuplicateEventsProduceOneRecord(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})

	if err := h.engine.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.engine.Stop()

	event := model.ChangeEvent{
		Source: "left", Kind: "contact", RemoteID: rec.RemoteID, Record: &rec,
	}

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.engine.Submit(h.syncName(), event)
		}()
	}
	wg.Wait()
	waitIdle(t, h)

	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("twelve copies of one change produced %d records", got)
	}
	if got := h.engine.Stats().Created; got != 1 {
		t.Fatalf("Created = %d, want 1", got)
	}
}

// Two people editing different fields of one contact is the case field-level
// resolution exists for, and on a shared record it is most of the time.
func TestFieldLevelMergeThroughTheEngine(t *testing.T) {
	h := newHarness(t, harnessOptions{policy: conflict.FieldLevel})

	left := h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})
	h.cycle()
	right := h.right.Records("contact")[0]

	// One edits the address, the other edits the name.
	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"email": "new@example.com"})
	h.right.ExternalUpsert("contact", right.RemoteID, map[string]any{"first_name": "Annabel"})

	h.cycle()

	if got := h.field(h.right, "email"); got != "new@example.com" {
		t.Fatalf("the left edit did not reach the right: %v", got)
	}
	if got := h.field(h.left, "firstname"); got != "Annabel" {
		t.Fatalf("the right edit did not reach the left: %v", got)
	}
	if len(h.reviews()) != 0 {
		t.Fatalf("a mergeable pair of edits was escalated: %+v", h.reviews())
	}
}

// Both edited the same field, so there is nothing to merge and the whole
// record is escalated rather than half applied.
func TestFieldLevelEscalatesACollisionThroughTheEngine(t *testing.T) {
	h := newHarness(t, harnessOptions{policy: conflict.FieldLevel})

	left := h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})
	h.cycle()
	right := h.right.Records("contact")[0]

	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "Left"})
	h.right.ExternalUpsert("contact", right.RemoteID, map[string]any{"first_name": "Right"})

	h.cycle()

	if len(h.reviews()) == 0 {
		t.Fatal("a genuine collision was resolved silently")
	}
	if h.field(h.left, "firstname") != "Left" || h.field(h.right, "first_name") != "Right" {
		t.Fatalf("a side was overwritten while the question was open: left %v, right %v",
			h.field(h.left, "firstname"), h.field(h.right, "first_name"))
	}
}

// Stopping must release everything waiting on a task, including work that was
// queued and never started. That is safe because the watermark never advanced
// past it: the next poll reads it again.
func TestStopReleasesQueuedWork(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		h.left.ExternalUpsert("contact", "", map[string]any{
			"email": fmt.Sprintf("user%02d@example.com", i),
		})
	}
	results, err := h.engine.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}

	h.engine.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, r := range results {
			_ = r.Commit(ctx, h.engine)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Commit hung on work that shutdown abandoned")
	}
	if h.engine.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d after Stop", h.engine.QueueDepth())
	}
}

func TestStopIsIdempotent(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.engine.Stop()
	h.engine.Stop()
}

// waitIdle blocks until the engine has nothing left, with a bound so that a
// hang is a failure rather than a stuck test run.
func waitIdle(t *testing.T, h *harness) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.engine.WaitIdle(ctx); err != nil {
		t.Fatalf("WaitIdle: %v (depth %d)", err, h.engine.QueueDepth())
	}
}
