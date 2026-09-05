package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"crm-bisync/internal/conflict"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/idem"
	"crm-bisync/internal/identity"
	"crm-bisync/internal/mapping"
	"crm-bisync/internal/model"
	"crm-bisync/internal/origin"
	"crm-bisync/internal/store"
)

// Outcome is what happened to one change.
type Outcome string

// The outcomes, which double as metric labels.
const (
	OutcomeEcho       Outcome = "echo"
	OutcomeNoChange   Outcome = "no_change"
	OutcomeCreated    Outcome = "created"
	OutcomeUpdated    Outcome = "updated"
	OutcomeDeleted    Outcome = "deleted"
	OutcomeReviewed   Outcome = "reviewed"
	OutcomeSkipped    Outcome = "skipped"
	OutcomeDeadLetter Outcome = "dead_letter"
)

// Stats counts what the engine has done.
type Stats struct {
	Ingested     int64
	Echoes       int64
	NearMisses   int64
	Created      int64
	Updated      int64
	Deleted      int64
	Reviewed     int64
	Skipped      int64
	NoChange     int64
	Retried      int64
	DeadLettered int64
}

// errRecheck asks for another attempt because a previous one may have landed.
//
// It is retryable on purpose: the retry re-resolves identity from scratch, and
// by then a poll may have surfaced the record the lost response created. That
// is exactly what "re-resolve before writing" means, expressed with machinery
// the engine already has rather than a special case in the write path.
var errRecheck = errors.New("a previous attempt may have landed; re-resolving")

// seq numbers queue items so their IDs sort chronologically even within a
// nanosecond.
var seq atomic.Uint64

// count mutates the stats under the lock.
func (e *Engine) count(f func(*Stats)) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	f(&e.stats)
}

// Stats returns a snapshot of the counters.
func (e *Engine) Stats() Stats {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	return e.stats
}

// process runs one change through the pipeline.
//
// The order is the design. Every stage is a package that was tested on its
// own; what makes them a system is which one runs first and what is written
// before the network call rather than after it.
func (e *Engine) process(ctx context.Context, t *task) (Outcome, error) {
	s, ok := e.byName[t.Sync]
	if !ok {
		return OutcomeSkipped, permanent(fmt.Errorf("no sync named %q", t.Sync))
	}
	from, ok := s.peerByName(t.Event.Source)
	if !ok {
		return OutcomeSkipped, permanent(fmt.Errorf("%s: no peer named %q", s.Name, t.Event.Source))
	}
	to := s.other(from)

	// Direction gate, first and cheapest. A sync that never writes to the far
	// side has nothing to do with a change on the near one, and in a one-way
	// sync this is also what stops our own writes coming back: the echo
	// arrives from the target, and the target is a side we never write from.
	if !mapping.WritesTo(s.Mapper.Direction(), to.Side) {
		return OutcomeSkipped, nil
	}

	if t.Event.Deleted {
		return e.processDelete(ctx, s, from, to, t.Event)
	}

	rec, err := e.hydrate(ctx, s, from, t.Event)
	if err != nil {
		return OutcomeSkipped, err
	}
	if rec == nil {
		// The record is gone. A peer that reports a change and then cannot
		// produce the record is describing a deletion, whatever it called it.
		return e.processDelete(ctx, s, from, to, t.Event)
	}

	canonical, err := s.Mapper.ToCanonical(from.Side, *rec)
	if err != nil {
		return OutcomeSkipped, permanent(err)
	}
	hash := model.SnapshotHash(canonical, s.canonical)
	fromRef := store.Ref{Connector: from.Name, Kind: s.Kind, RemoteID: rec.RemoteID}

	// Everything from here to the end of the bookkeeping is one logical
	// record's turn. See keyedMutex for the race this closes.
	unlock := e.locks.lock(e.lockKey(s, canonical, fromRef))
	defer unlock()

	verdict, err := e.origin.Classify(fromRef, hash)
	if err != nil {
		return OutcomeSkipped, err
	}
	if verdict == origin.NearMiss {
		e.count(func(st *Stats) { st.NearMisses++ })
		e.log.Info("peer rewrote our payload",
			"sync", s.Name, "ref", fromRef.String())
	}
	if !verdict.Processed() {
		e.count(func(st *Stats) { st.Echoes++ })
		return OutcomeEcho, nil
	}

	// Index before resolving, so that the far side can find this record on a
	// later pass even if this one ends in a review or a skip.
	if err := s.Identity.Index(fromRef, canonical); err != nil {
		return OutcomeSkipped, err
	}

	res, err := s.Identity.Resolve(fromRef, to.Name, canonical)
	if err != nil {
		return OutcomeSkipped, err
	}
	switch res.Outcome {
	case identity.Review:
		// Keyed on the record itself: an ambiguous match has no pair yet, so
		// one unresolved question per source record is exactly right.
		return e.queueReview(s, fromRef.String(), fromRef, res.Reason, map[string]any{
			"candidates": refStrings(res.Candidates),
			"values":     canonical,
		})
	case identity.Skip:
		e.count(func(st *Stats) { st.Skipped++ })
		return OutcomeSkipped, nil
	}

	targetRef := store.Ref{Connector: to.Name, Kind: s.Kind}
	if res.Outcome == identity.Linked || res.Outcome == identity.Matched {
		targetRef = res.Peer
	}

	if targetRef.RemoteID == "" {
		return e.create(ctx, s, from, to, fromRef, canonical, hash)
	}
	return e.reconcile(ctx, s, from, to, fromRef, targetRef, canonical, hash, rec.UpdatedAt)
}

// hydrate returns the record body, fetching it when the event carried none.
//
// The plan for this engine said the echo filter should run before hydration,
// so that an event we caused never costs a rate-limited read. That holds for a
// peer that sends record bodies, and it cannot hold for one that only says
// "record X changed": the payload digest is what recognises our own write, and
// there is no payload to hash until it has been fetched. The read is not
// wasted work so much as the price of a peer that tells us less.
func (e *Engine) hydrate(ctx context.Context, s *Sync, from *Peer, ev model.ChangeEvent) (*model.Record, error) {
	if ev.Hydrated() {
		return ev.Record, nil
	}
	rec, err := e.get(ctx, from, s.Kind, ev.RemoteID)
	if connector.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

// sides orders a near/far pair into the mapping's left and right.
func sides[T any](from *Peer, near, far T) (T, T) {
	if from.Side == mapping.Left {
		return near, far
	}
	return far, near
}

// create writes a record the far side does not have yet.
func (e *Engine) create(
	ctx context.Context,
	s *Sync,
	from, to *Peer,
	fromRef store.Ref,
	canonical map[string]any,
	fromHash string,
) (Outcome, error) {
	targetRef := store.Ref{Connector: to.Name, Kind: s.Kind}

	result, wrote, err := e.write(ctx, s, to, targetRef, fromRef, canonical, nil)
	if err != nil {
		return OutcomeSkipped, err
	}
	if !wrote {
		return OutcomeNoChange, nil
	}
	targetRef.RemoteID = result.RemoteID

	leftRef, rightRef := sides(from, fromRef, targetRef)
	// The far side now holds what we sent, as far as we can know without
	// asking it again.
	leftValues, rightValues := sides(from, canonical, canonical)
	if err := e.commitPair(s, leftRef, rightRef, leftValues, rightValues); err != nil {
		return OutcomeSkipped, err
	}

	e.count(func(st *Stats) { st.Created++ })
	return OutcomeCreated, nil
}

// reconcile decides what to do about a pair that is already linked.
func (e *Engine) reconcile(
	ctx context.Context,
	s *Sync,
	from, to *Peer,
	fromRef, targetRef store.Ref,
	canonical map[string]any,
	fromHash string,
	updatedAt time.Time,
) (Outcome, error) {
	fromSnap, _, err := e.typed.Snapshot(fromRef)
	if err != nil {
		return OutcomeSkipped, err
	}
	toSnap, _, err := e.typed.Snapshot(targetRef)
	if err != nil {
		return OutcomeSkipped, err
	}

	nearState := conflict.State{
		Fields:     canonical,
		Hash:       fromHash,
		Base:       fromSnap.Values,
		SyncedHash: fromSnap.Hash,
		UpdatedAt:  updatedAt,
		Present:    true,
	}
	farState := conflict.State{
		Fields:     toSnap.Values,
		Hash:       toSnap.Hash,
		Base:       toSnap.Values,
		SyncedHash: toSnap.Hash,
		Present:    true,
	}

	// Read before write, but only on a bidirectional sync.
	//
	// This is what makes conflict detection real rather than nominal. Without
	// the far side's current state the only situation that can ever be
	// observed is "the near side changed", and every concurrent edit is
	// silently overwritten. It costs one request per write, which is the price
	// of not losing somebody's edit.
	//
	// A one-way sync skips it, because there the far side is not supposed to
	// be edited by anyone and the operator has said so in configuration. That
	// is a decision rather than a guess.
	if s.Mapper.Direction() == mapping.Bidirectional {
		current, err := e.get(ctx, to, s.Kind, targetRef.RemoteID)
		switch {
		case connector.IsNotFound(err):
			// The link points at a record that no longer exists. Dropping the
			// link and creating is the only reading that does not quietly stop
			// syncing this pair forever.
			if err := e.dropLink(s, fromRef, targetRef); err != nil {
				return OutcomeSkipped, err
			}
			return e.create(ctx, s, from, to, fromRef, canonical, fromHash)
		case err != nil:
			return OutcomeSkipped, err
		}

		farCanonical, err := s.Mapper.ToCanonical(to.Side, current)
		if err != nil {
			return OutcomeSkipped, permanent(err)
		}
		farState.Fields = farCanonical
		farState.Hash = model.SnapshotHash(farCanonical, s.canonical)
		farState.UpdatedAt = current.UpdatedAt
	}

	leftRef, rightRef := sides(from, fromRef, targetRef)
	leftState, rightState := sides(from, nearState, farState)

	res := s.Conflict.Resolve(leftState, rightState, s.canonical)

	if res.Action == conflict.Escalate {
		// Keyed on the pair, not on the record that happened to surface it.
		// Both sides' events reach this line for one disagreement, and two
		// rows describing one decision is two people resolving it, or one
		// person resolving it twice.
		pair := leftRef.String() + " <-> " + rightRef.String()
		return e.queueReview(s, pair, fromRef, res.Reason, map[string]any{
			"situation": res.Situation.String(),
			"policy":    res.Policy,
			"diffs":     res.Diffs,
			"peer":      targetRef.String(),
		})
	}

	leftFinal, rightFinal := leftState.Fields, rightState.Fields
	switch res.Action {
	case conflict.WriteLeft:
		if _, _, err := e.write(ctx, s, s.Left, leftRef, rightRef, res.Values, leftState.Fields); err != nil {
			return OutcomeSkipped, err
		}
		leftFinal = merge(leftState.Fields, res.Values)

	case conflict.WriteRight:
		if _, _, err := e.write(ctx, s, s.Right, rightRef, leftRef, res.Values, rightState.Fields); err != nil {
			return OutcomeSkipped, err
		}
		rightFinal = merge(rightState.Fields, res.Values)

	case conflict.WriteBoth:
		// Each side is missing something the other has. The two writes touch
		// disjoint fields by construction, so neither is conditional on the
		// other and the order does not matter.
		if _, _, err := e.write(ctx, s, s.Left, leftRef, rightRef, res.LeftValues, leftState.Fields); err != nil {
			return OutcomeSkipped, err
		}
		if _, _, err := e.write(ctx, s, s.Right, rightRef, leftRef, res.RightValues, rightState.Fields); err != nil {
			return OutcomeSkipped, err
		}
		leftFinal = merge(leftState.Fields, res.LeftValues)
		rightFinal = merge(rightState.Fields, res.RightValues)
	}

	// Committed even when nothing was written. That is the pass that records
	// the base for a pair which has only just been matched, and without it
	// every later pass would see two sides that both "changed" since a sync
	// that never happened, and escalate for ever.
	if err := e.commitPair(s, leftRef, rightRef, leftFinal, rightFinal); err != nil {
		return OutcomeSkipped, err
	}

	if res.Action == conflict.Nothing {
		e.count(func(st *Stats) { st.NoChange++ })
		return OutcomeNoChange, nil
	}
	e.count(func(st *Stats) { st.Updated++ })
	return OutcomeUpdated, nil
}

// write performs one idempotent, rate-limited, echo-tagged write.
//
// base is the target's known current canonical state, used to predict what the
// record will look like afterwards. That prediction is what the origin tag is
// computed from, because the peer will echo its whole record and not the
// subset we sent.
func (e *Engine) write(
	ctx context.Context,
	s *Sync,
	target *Peer,
	targetRef, sourceRef store.Ref,
	values map[string]any,
	base map[string]any,
) (connector.WriteResult, bool, error) {
	fields := s.Mapper.FromCanonical(target.Side, values)
	if len(fields) == 0 {
		// Everything the resolution wanted written is read-only on this side,
		// or narrowed away by a per-field direction. Not an error, and not a
		// request either.
		return connector.WriteResult{}, false, nil
	}

	if e.plan != nil {
		// A dry run stops here. Everything after this point is the network and
		// the bookkeeping that follows it; everything before it is the
		// decision, which is exactly what a plan is meant to show.
		action := ActionUpdate
		if targetRef.RemoteID == "" {
			action = ActionCreate
		}
		// The direction filter has already run, so the plan reports the
		// fields that would land rather than the ones that were offered. A
		// plan showing a read-only or narrowed field is a plan that lies
		// about the one thing anybody reads it for.
		id := e.plan.record(PlannedChange{
			Sync:   s.Name,
			Action: action,
			Source: sourceRef,
			Target: targetRef,
			Fields: namedValues(only(values, s.Mapper.WritableNames(target.Side))),
		})
		if targetRef.RemoteID == "" {
			// A synthetic identifier, so that the rest of the pass links and
			// snapshots against something and a later record's plan is right
			// about this one.
			return connector.WriteResult{RemoteID: id, Created: true}, true, nil
		}
		return connector.WriteResult{RemoteID: targetRef.RemoteID}, true, nil
	}

	predicted := merge(base, values)
	payloadHash := model.SnapshotHash(predicted, s.canonical)
	key := idem.Key(s.Name, s.Kind, sourceRef, targetRef, payloadHash)

	previous, decision, err := e.idem.Begin(key)
	if err != nil {
		return connector.WriteResult{}, false, err
	}
	switch decision {
	case idem.Replayed:
		return connector.WriteResult{
			RemoteID: previous.RemoteID,
			Created:  previous.Created,
		}, true, nil
	case idem.Recheck:
		if targetRef.RemoteID == "" {
			// A create whose outcome we never learned. Retrying blind is how a
			// dropped connection becomes a second contact, so this waits for a
			// poll to surface the record and for identity to match it.
			return connector.WriteResult{}, false, errRecheck
		}
		// The target is known, so this is an update, and an update carrying
		// the same payload is safe to repeat. Clear the stale mark and take a
		// fresh one.
		if err := e.idem.Abandon(key); err != nil {
			return connector.WriteResult{}, false, err
		}
		if _, _, err := e.idem.Begin(key); err != nil {
			return connector.WriteResult{}, false, err
		}
	}

	// The origin tag is written before the call, not after, whenever the
	// target is already known.
	//
	// Recording afterwards leaves a window in which the peer's webhook arrives
	// before we have said the write was ours, and an unrecognised echo is an
	// unbounded loop. Recording beforehand can only fail the other way: if the
	// write does not land, an unmatched tag sits in the log until its window
	// closes, and the one thing it could suppress in the meantime is a change
	// carrying exactly the payload we were trying to write.
	if targetRef.RemoteID != "" {
		if err := e.recordOrigin(targetRef, payloadHash); err != nil {
			return connector.WriteResult{}, false, err
		}
	}

	if err := target.Bucket.Wait(ctx); err != nil {
		return connector.WriteResult{}, false, err
	}

	record := model.Record{Kind: s.Kind, RemoteID: targetRef.RemoteID, Fields: fields}
	result, err := target.Conn.Upsert(ctx, s.Kind, record, key)
	if err != nil {
		e.noteFailure(target, err)
		if connector.Answered(err) {
			// The peer answered, and the answer was not success, so the write
			// did not land. Leaving the mark would turn every later attempt
			// into a recheck for a write that never happened, and the record
			// would never sync again.
			if abandonErr := e.idem.Abandon(key); abandonErr != nil {
				e.log.Warn("could not clear the in-flight mark",
					"sync", s.Name, "err", abandonErr)
			}
		}
		return connector.WriteResult{}, false, err
	}
	target.Bucket.Success()

	// A create could not pre-record its tag, because the identifier did not
	// exist yet. The keyed mutex is what keeps the echo of this create from
	// being processed before the next two lines run.
	if targetRef.RemoteID == "" {
		created := store.Ref{Connector: target.Name, Kind: s.Kind, RemoteID: result.RemoteID}
		if err := e.recordOrigin(created, payloadHash); err != nil {
			return connector.WriteResult{}, false, err
		}
	}

	if err := e.idem.Commit(key, store.IdemResult{
		RemoteID:  result.RemoteID,
		Created:   result.Created,
		WrittenAt: e.clock.Now(),
	}); err != nil {
		return connector.WriteResult{}, false, err
	}

	return result, true, nil
}

// recordOrigin writes both the payload tag and the per-record marker.
func (e *Engine) recordOrigin(ref store.Ref, payloadHash string) error {
	return e.origin.Record(ref, payloadHash)
}

// commitPair records everything that makes the next pass cheap and correct:
// the link, both snapshots, and both sides' identity keys.
//
// The snapshots are the state each side is in after the resolution was
// applied, which is what the next comparison has to be made against. On the
// far side that is a prediction, and it can be wrong when the peer defaults or
// rewrites a mapped field on write. When it is wrong the peer's echo arrives
// as a near miss and one reconciling pass follows, which is correct behaviour;
// the near-miss counter is what makes it visible rather than mysterious.
func (e *Engine) commitPair(
	s *Sync,
	leftRef, rightRef store.Ref,
	leftValues, rightValues map[string]any,
) error {
	now := e.clock.Now()

	if err := e.typed.PutLink(store.Link{Left: leftRef, Right: rightRef, LinkedAt: now}); err != nil {
		return err
	}

	for _, side := range []struct {
		ref    store.Ref
		values map[string]any
	}{
		{leftRef, leftValues},
		{rightRef, rightValues},
	} {
		if err := e.typed.SetSnapshot(side.ref, store.Snapshot{
			Hash:   model.SnapshotHash(side.values, s.canonical),
			Values: side.values,
			At:     now,
		}); err != nil {
			return err
		}
		if err := s.Identity.Index(side.ref, side.values); err != nil {
			return err
		}
	}
	return nil
}

// processDelete propagates a deletion.
//
// Deletion needs no echo suppression, and gets none: a delete event carries no
// payload to hash. It terminates on the link instead. We delete the far
// record and drop the link, so the far side's own delete event finds no link
// and stops there. One round trip, always.
func (e *Engine) processDelete(
	ctx context.Context,
	s *Sync,
	from, to *Peer,
	ev model.ChangeEvent,
) (Outcome, error) {
	fromRef := store.Ref{Connector: from.Name, Kind: s.Kind, RemoteID: ev.RemoteID}

	unlock := e.locks.lock(s.Name + "\x00ref\x00" + fromRef.String())
	defer unlock()

	link, ok, err := e.typed.GetLink(fromRef)
	if err != nil {
		return OutcomeSkipped, err
	}
	if !ok {
		e.count(func(st *Stats) { st.Skipped++ })
		return OutcomeSkipped, nil
	}
	peer, ok := link.Peer(fromRef)
	if !ok {
		return OutcomeSkipped, permanent(fmt.Errorf("link for %s names no peer", fromRef))
	}

	if e.plan != nil {
		e.plan.record(PlannedChange{
			Sync:   s.Name,
			Action: ActionDelete,
			Source: fromRef,
			Target: peer,
		})
		if err := e.dropLink(s, fromRef, peer); err != nil {
			return OutcomeSkipped, err
		}
		e.count(func(st *Stats) { st.Deleted++ })
		return OutcomeDeleted, nil
	}

	key := idem.Key(s.Name, s.Kind, fromRef, peer, "delete")
	_, decision, err := e.idem.Begin(key)
	if err != nil {
		return OutcomeSkipped, err
	}
	if decision == idem.Proceed {
		if err := to.Bucket.Wait(ctx); err != nil {
			return OutcomeSkipped, err
		}
		if err := to.Conn.Delete(ctx, s.Kind, peer.RemoteID, key); err != nil {
			e.noteFailure(to, err)
			return OutcomeSkipped, err
		}
		to.Bucket.Success()
		if err := e.idem.Commit(key, store.IdemResult{RemoteID: peer.RemoteID}); err != nil {
			return OutcomeSkipped, err
		}
	}

	if err := e.dropLink(s, fromRef, peer); err != nil {
		return OutcomeSkipped, err
	}

	e.count(func(st *Stats) { st.Deleted++ })
	return OutcomeDeleted, nil
}

// dropLink removes a pair and everything derived from it.
func (e *Engine) dropLink(s *Sync, a, b store.Ref) error {
	link, ok, err := e.typed.GetLink(a)
	if err != nil {
		return err
	}
	if ok {
		if err := e.typed.DeleteLink(link); err != nil {
			return err
		}
	}
	for _, ref := range []store.Ref{a, b} {
		if err := s.Identity.Deindex(ref); err != nil {
			return err
		}
		if err := e.store.Delete(store.CollSnapshots, refKey(ref)); err != nil {
			return err
		}
	}
	return nil
}

// get reads one record, under the peer's rate limit.
func (e *Engine) get(ctx context.Context, p *Peer, kind, remoteID string) (model.Record, error) {
	if err := p.Bucket.Wait(ctx); err != nil {
		return model.Record{}, err
	}
	rec, err := p.Conn.Get(ctx, kind, remoteID)
	if err != nil {
		e.noteFailure(p, err)
		return model.Record{}, err
	}
	p.Bucket.Success()
	return rec, nil
}

// noteFailure feeds a failure back into the peer's bucket.
func (e *Engine) noteFailure(p *Peer, err error) {
	if connector.KindOf(err) == connector.KindRateLimited {
		p.Bucket.Reject(connector.RetryAfterOf(err))
	}
}

// queueReview records a decision the engine refused to make.
//
// subject identifies the question rather than the event that raised it, so a
// disagreement seen from both sides is one row.
func (e *Engine) queueReview(s *Sync, subject string, ref store.Ref, reason string, payload map[string]any) (Outcome, error) {
	id := reviewID(s.Name, subject)

	// The first sighting keeps its timestamp. A question that has been open
	// since Tuesday should not look like it arrived just now every time
	// something notices it again.
	if _, exists, err := e.typed.GetQueueItem(store.CollReview, id); err != nil {
		return OutcomeSkipped, err
	} else if exists {
		return OutcomeReviewed, nil
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("engine: encode review payload: %w", err)
	}
	if err := e.typed.PutQueueItem(store.CollReview, store.QueueItem{
		ID:        id,
		Sync:      s.Name,
		Ref:       ref,
		Reason:    reason,
		CreatedAt: e.clock.Now(),
		Payload:   raw,
	}); err != nil {
		return OutcomeSkipped, err
	}
	e.count(func(st *Stats) { st.Reviewed++ })
	e.log.Info("queued for review", "sync", s.Name, "ref", ref.String(), "reason", reason)
	return OutcomeReviewed, nil
}

// deadLetter records work that exhausted its attempts or can never succeed.
func (e *Engine) deadLetter(s *Sync, t *task, cause error) error {
	ref := store.Ref{Connector: t.Event.Source, Kind: t.Event.Kind, RemoteID: t.Event.RemoteID}
	payload := map[string]any{
		"event": t.Event,
		"error": cause.Error(),
		"kind":  connector.KindOf(cause).String(),
	}
	if err := e.enqueueItem(store.CollDLQ, s, ref, cause.Error(), t.Attempts, payload); err != nil {
		return err
	}
	e.count(func(st *Stats) { st.DeadLettered++ })
	e.log.Error("dead-lettered", "sync", t.Sync, "ref", ref.String(),
		"attempts", t.Attempts, "err", cause)
	return nil
}

// enqueueItem writes to the dead-letter or review collection.
func (e *Engine) enqueueItem(
	collection string,
	s *Sync,
	ref store.Ref,
	reason string,
	attempts int,
	payload map[string]any,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("engine: encode queue payload: %w", err)
	}
	name := ""
	if s != nil {
		name = s.Name
	}
	now := e.clock.Now()
	return e.typed.PutQueueItem(collection, store.QueueItem{
		ID:        store.NewQueueID(now, seq.Add(1)),
		Sync:      name,
		Ref:       ref,
		Reason:    reason,
		CreatedAt: now,
		Attempts:  attempts,
		Payload:   raw,
	})
}

// reviewID is a stable identifier for one open question.
func reviewID(sync, subject string) string {
	sum := sha256.Sum256([]byte(sync + "\x00" + subject))
	return "review-" + hex.EncodeToString(sum[:16])
}

// only copies the named fields out of a value set.
func only(values map[string]any, names []string) map[string]any {
	out := make(map[string]any, len(names))
	for _, name := range names {
		if v, ok := values[name]; ok {
			out[name] = v
		}
	}
	return out
}

// lockKey names the logical record a task is about.
//
// The identity fingerprint rather than either remote ID, because the two
// events that race are a change on one side and the echo of the record it
// created on the other, and at that moment the fingerprint is the only thing
// they have in common.
func (e *Engine) lockKey(s *Sync, canonical map[string]any, ref store.Ref) string {
	keys := s.Identity.Keys()
	if len(keys) > 0 {
		fingerprint := model.SnapshotHash(canonical, keys)
		if fingerprint != model.SnapshotHash(map[string]any{}, keys) {
			return s.Name + "\x00id\x00" + fingerprint
		}
	}
	// No identity keys configured, or none of them has a value. Falling back
	// to the record itself still serialises repeat events for one record,
	// which is the weaker guarantee but better than none.
	return s.Name + "\x00ref\x00" + ref.String()
}

// merge overlays values on top of a base, without mutating either.
func merge(base, values map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(values))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range values {
		out[k] = v
	}
	return out
}

// refKey mirrors the store's internal key format for a ref.
func refKey(r store.Ref) string {
	return r.Connector + "\x00" + r.Kind + "\x00" + r.RemoteID
}

func refStrings(refs []store.Ref) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.String())
	}
	return out
}

// permanentError marks a failure retrying cannot fix.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

func permanent(err error) error { return permanentError{err} }

// retryable reports whether another attempt could plausibly succeed.
func retryable(err error) bool {
	var p permanentError
	if errors.As(err, &p) {
		return false
	}
	if errors.Is(err, errRecheck) {
		return true
	}
	return connector.Retryable(err)
}
