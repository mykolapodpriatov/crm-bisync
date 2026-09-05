package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
	"crm-bisync/internal/store"
)

// Backoff bounds. The floor exists so that full jitter cannot produce a retry
// that fires immediately and spins; the ceiling so that a peer which has been
// down for an hour is still checked on something like a human timescale.
const (
	minBackoff  = 100 * time.Millisecond
	baseBackoff = 250 * time.Millisecond
	maxBackoff  = 30 * time.Second
)

// Submit queues a change for processing and returns its handle.
//
// The handle is what pollers wait on before advancing a watermark: a mark that
// moves past work still in flight is a mark that skips records when the
// process restarts, and the in-process queue is not durable.
func (e *Engine) Submit(syncName string, ev model.ChangeEvent) *task {
	t := &task{Sync: syncName, Event: ev, done: make(chan struct{})}
	e.count(func(s *Stats) { s.Ingested++ })
	if !e.queue.push(t) {
		return t
	}
	return t
}

// Drain processes every task that is ready now, and keeps going until none is.
//
// It is safe to call while workers are running, where it simply acts as one
// more worker. Tests use it on its own, which is what makes the pipeline
// deterministic to exercise: nothing happens until Drain is called, and
// nothing that is waiting out a backoff happens until the clock is moved.
func (e *Engine) Drain(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		t, ok := e.queue.next(e.clock.Now())
		if !ok {
			return
		}
		e.runTask(ctx, t)
	}
}

// runTask processes one task and decides what happens to it afterwards.
func (e *Engine) runTask(ctx context.Context, t *task) {
	outcome, err := e.process(ctx, t)
	if err == nil {
		e.log.Debug("processed",
			"sync", t.Sync, "record", t.Event.RemoteID, "outcome", string(outcome))
		e.queue.complete(t)
		return
	}

	// A cancelled context is a shutdown, not a failure of this task. Retrying
	// it would be pointless and dead-lettering it would be a lie about why it
	// did not run.
	if ctx.Err() != nil {
		e.queue.complete(t)
		return
	}

	t.Attempts++
	if retryable(err) && t.Attempts < e.cfg.MaxAttempts {
		t.NotBefore = e.clock.Now().Add(e.backoff(t.Attempts, connector.RetryAfterOf(err)))
		e.count(func(s *Stats) { s.Retried++ })
		e.log.Debug("retrying",
			"sync", t.Sync, "record", t.Event.RemoteID,
			"attempt", t.Attempts, "err", err)
		e.queue.requeue(t)
		return
	}

	if dlErr := e.deadLetter(e.byName[t.Sync], t, err); dlErr != nil {
		e.log.Error("could not dead-letter", "sync", t.Sync, "err", dlErr)
	}
	e.queue.complete(t)
}

// backoff returns how long to wait before attempt number n.
//
// Full jitter rather than plain exponential: several workers failing against
// one peer at the same moment would otherwise all come back at the same
// moment, which is how a struggling peer gets a synchronised second wave
// exactly when it can least take one.
func (e *Engine) backoff(attempts int, hint time.Duration) time.Duration {
	window := baseBackoff
	for i := 1; i < attempts && window < maxBackoff; i++ {
		window *= 2
	}
	if window > maxBackoff {
		window = maxBackoff
	}

	e.rndMu.Lock()
	jittered := time.Duration(e.rnd.Int63n(int64(window) + 1))
	e.rndMu.Unlock()

	if jittered < minBackoff {
		jittered = minBackoff
	}
	// The peer's own answer beats ours whenever it gave one.
	if hint > jittered {
		return hint
	}
	return jittered
}

// Start brings up the worker pool.
//
// It validates every mapping against both peers' live schemas first. A sync
// whose mapping names a field the peer no longer has should refuse to start
// rather than discover it one record at a time.
func (e *Engine) Start(ctx context.Context) error {
	if err := e.Validate(ctx); err != nil {
		return err
	}

	e.startOnce.Do(func() {
		workers := e.cfg.Workers
		if workers < 1 {
			workers = 1
		}
		for i := 0; i < workers; i++ {
			e.workersWG.Add(1)
			go e.worker(ctx)
		}
	})
	return nil
}

// Stop shuts the pool down and waits for in-flight work to finish.
//
// Anything queued but not started is abandoned rather than processed, and its
// handle is released so nothing waiting on it hangs. That is safe because the
// watermark never advanced past it: the next poll reads it again.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopped)
		e.queue.close()
	})
	e.workersWG.Wait()
}

// worker is one goroutine of the pool.
func (e *Engine) worker(ctx context.Context) {
	defer e.workersWG.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopped:
			return
		default:
		}

		t, ok := e.queue.next(e.clock.Now())
		if ok {
			e.runTask(ctx, t)
			continue
		}

		// Nothing ready. Wait for new work, for the soonest retry to come due,
		// or for shutdown, whichever happens first.
		var timer <-chan time.Time
		if deadline, has := e.queue.nextDeadline(); has {
			wait := deadline.Sub(e.clock.Now())
			if wait < 0 {
				wait = 0
			}
			timer = e.clock.After(wait)
		}

		select {
		case <-ctx.Done():
			return
		case <-e.stopped:
			return
		case <-e.queue.wake:
		case <-timer:
		}
	}
}

// WaitIdle blocks until the queue holds nothing, or ctx ends.
func (e *Engine) WaitIdle(ctx context.Context) error {
	for {
		if e.queue.Depth() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.queue.empty:
		}
	}
}

// QueueDepth reports how much work is queued or in flight.
func (e *Engine) QueueDepth() int { return e.queue.Depth() }

// Sweep drops expired origin entries and delivery records.
//
// Expiry is already invisible to the readers, so this only exists to stop the
// tables growing without bound. The daemon calls it on a timer.
func (e *Engine) Sweep() (int, error) { return e.origin.Sweep() }

// DeadLetters returns dead-lettered work, oldest first.
func (e *Engine) DeadLetters(limit int) ([]store.QueueItem, error) {
	return e.typed.ListQueueItems(store.CollDLQ, limit)
}

// Reviews returns queued decisions, oldest first.
func (e *Engine) Reviews(limit int) ([]store.QueueItem, error) {
	return e.typed.ListQueueItems(store.CollReview, limit)
}

// Discard removes an item from the dead-letter or review queue.
func (e *Engine) Discard(collection, id string) error {
	return e.typed.DeleteQueueItem(collection, id)
}

// Replay puts a dead-lettered event back on the queue.
//
// The attempt count restarts, because the operator replaying it has usually
// changed something, and carrying the old count forward would spend the whole
// budget on the first failure.
func (e *Engine) Replay(id string) error {
	item, ok, err := e.typed.GetQueueItem(store.CollDLQ, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("engine: no dead-lettered item %q", id)
	}

	var payload struct {
		Event model.ChangeEvent `json:"event"`
	}
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return fmt.Errorf("engine: decode dead-lettered event: %w", err)
	}
	if _, ok := e.byName[item.Sync]; !ok {
		return fmt.Errorf("engine: item %q belongs to sync %q, which is no longer configured", id, item.Sync)
	}

	e.Submit(item.Sync, payload.Event)
	return e.typed.DeleteQueueItem(store.CollDLQ, id)
}

// Watermark reports a peer's high-water mark.
func (e *Engine) Watermark(connectorName, kind string) (time.Time, bool, error) {
	return e.marks.Mark(connectorName, kind)
}

// Link returns the pair a record belongs to.
func (e *Engine) Link(ref store.Ref) (store.Link, bool, error) {
	return e.typed.GetLink(ref)
}
