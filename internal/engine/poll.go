package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"crm-bisync/internal/mapping"
	"crm-bisync/internal/model"
	"crm-bisync/internal/store"
	"crm-bisync/internal/watermark"
)

// PollResult is one completed read of one peer, before its watermark moves.
//
// Poll and Commit are separate because the mark must not advance until the
// work the page produced has reached a terminal state. Doing both in one call
// would either block the poller for as long as the slowest retry, or advance
// the mark over work that a restart would then never see again.
type PollResult struct {
	Sync    *Sync
	Peer    *Peer
	Highest time.Time
	Events  []model.ChangeEvent
	Tasks   []*task
}

// Submit queues the events this poll found.
//
// Reading and queueing are separate steps so that PollAll can read every peer
// before any of them is processed. That ordering is not cosmetic: identity
// matching runs off a local index that is populated as records are seen, so a
// first pass which starts matching before the far side has been read finds
// nothing to match against and creates a second copy of every record that
// already existed on both sides.
func (r *PollResult) Submit(e *Engine) {
	for _, ev := range r.Events {
		r.Tasks = append(r.Tasks, e.Submit(r.Sync.Name, ev))
	}
}

// Poll reads one peer's changes since its watermark and queues them.
func (e *Engine) Poll(ctx context.Context, s *Sync, p *Peer) (*PollResult, error) {
	since, known, err := e.marks.Since(p.Name, s.Kind)
	if err != nil {
		return nil, err
	}
	if !known {
		// No mark yet, so this is a backfill: read from the beginning. It is
		// the same code path, just a much larger one, which is why paging and
		// resumption are not optional here.
		since = time.Time{}
		e.log.Info("backfilling", "sync", s.Name, "peer", p.Name, "kind", s.Kind)
	}

	result := &PollResult{Sync: s, Peer: p}
	cursor := ""
	now := e.clock.Now()

	for pages := 0; ; pages++ {
		if err := p.Bucket.Wait(ctx); err != nil {
			return nil, err
		}
		page, err := p.Conn.ListChanged(ctx, s.Kind, since, cursor)
		if err != nil {
			e.noteFailure(p, err)
			return nil, fmt.Errorf("polling %s for %s: %w", p.Name, s.Kind, err)
		}
		p.Bucket.Success()

		for i := range page.Records {
			rec := page.Records[i]
			result.Highest = watermark.Highest(result.Highest, rec.UpdatedAt)

			// Index on sight. Polling a peer is the moment we learn what it
			// holds, and indexing is local, so there is no reason to wait for
			// the record's turn in the queue to record it.
			if canonical, err := s.Mapper.ToCanonical(p.Side, rec); err == nil {
				ref := store.Ref{Connector: p.Name, Kind: s.Kind, RemoteID: rec.RemoteID}
				if err := s.Identity.Index(ref, canonical); err != nil {
					return nil, err
				}
			}

			result.Events = append(result.Events, model.ChangeEvent{
				Source:     p.Name,
				Kind:       s.Kind,
				RemoteID:   rec.RemoteID,
				ObservedAt: now,
				Record:     &rec,
			})
		}
		for _, id := range page.Deleted {
			result.Events = append(result.Events, model.ChangeEvent{
				Source:     p.Name,
				Kind:       s.Kind,
				RemoteID:   id,
				ObservedAt: now,
				Deleted:    true,
			})
		}

		if !page.HasMore() {
			break
		}
		if pages > maxPages {
			// A connector whose cursor never terminates would otherwise turn
			// one poll into an unbounded loop holding the peer's whole quota.
			return nil, fmt.Errorf("polling %s for %s: paging did not terminate after %d pages",
				p.Name, s.Kind, maxPages)
		}
		cursor = page.Cursor
	}

	return result, nil
}

// maxPages bounds one poll, so a misbehaving cursor cannot loop forever.
const maxPages = 10_000

// Commit waits for the polled work to reach a terminal state and then advances
// the watermark.
//
// Terminal includes dead-lettered: that work is durable, so the mark may pass
// it. It does not include a retry still waiting out its backoff, which is why
// a crash mid-retry costs a re-read rather than a lost record.
func (r *PollResult) Commit(ctx context.Context, e *Engine) error {
	for _, t := range r.Tasks {
		select {
		case <-t.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return e.marks.Advance(r.Peer.Name, r.Sync.Kind, r.Highest)
}

// readablePeers returns the peers of a sync whose changes are worth reading.
//
// A peer we never write from is a peer we do not need to poll: in a one-way
// sync the target's changes are, by configuration, not our business.
func (s *Sync) readablePeers() []*Peer {
	var out []*Peer
	for _, p := range []*Peer{s.Left, s.Right} {
		if mapping.WritesTo(s.Mapper.Direction(), s.other(p).Side) {
			out = append(out, p)
		}
	}
	return out
}

// PollAll reads every peer of every sync and queues what it finds.
func (e *Engine) PollAll(ctx context.Context) ([]*PollResult, error) {
	var results []*PollResult
	var errs []error

	for _, s := range e.syncs {
		for _, p := range s.readablePeers() {
			result, err := e.Poll(ctx, s, p)
			if err != nil {
				// One unreachable peer must not stop the others. The mark for
				// this one simply does not move, so nothing is lost.
				errs = append(errs, err)
				continue
			}
			results = append(results, result)
		}
	}

	// Everything is read before anything is queued. See PollResult.Submit.
	for _, r := range results {
		r.Submit(e)
	}
	return results, errors.Join(errs...)
}

// Cycle is one full pass: read every peer, process what it found, then move
// the marks. It blocks until the work is terminal, which is what makes the
// mark safe to move.
func (e *Engine) Cycle(ctx context.Context) error {
	results, pollErr := e.PollAll(ctx)

	e.Drain(ctx)

	var errs []error
	if pollErr != nil {
		errs = append(errs, pollErr)
	}
	for _, r := range results {
		if err := r.Commit(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
