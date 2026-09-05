package engine

import (
	"context"
	"fmt"
	"time"

	"crm-bisync/internal/store"
)

// Check is one thing doctor looked at.
type Check struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	OK      bool   `json:"ok"`
	Detail  string `json:"detail"`
}

// Diagnosis is everything doctor looked at, in the order it looked.
type Diagnosis struct {
	Checks []Check `json:"checks"`
}

// OK reports whether every check passed.
func (d Diagnosis) OK() bool {
	for _, c := range d.Checks {
		if !c.OK {
			return false
		}
	}
	return true
}

// Failures returns only the checks that did not pass.
func (d Diagnosis) Failures() []Check {
	var out []Check
	for _, c := range d.Checks {
		if !c.OK {
			out = append(out, c)
		}
	}
	return out
}

func (d *Diagnosis) pass(name, subject, detail string) {
	d.Checks = append(d.Checks, Check{Name: name, Subject: subject, OK: true, Detail: detail})
}

func (d *Diagnosis) fail(name, subject, detail string) {
	d.Checks = append(d.Checks, Check{Name: name, Subject: subject, OK: false, Detail: detail})
}

// Doctor checks everything that can be checked without writing to a CRM.
//
// It is safe to run against production, and it is meant to be: the point is to
// find the renamed property, the expired token and the unreadable state
// directory before a sync does, and at a moment when somebody is looking.
func (e *Engine) Doctor(ctx context.Context) Diagnosis {
	var d Diagnosis

	e.checkStore(&d)
	e.checkConnectors(ctx, &d)
	e.checkMappings(ctx, &d)
	e.checkProgress(&d)
	e.checkQueues(&d)

	return d
}

// checkStore proves the state directory is usable, by using it.
//
// Reading is not enough: a store that opens and cannot be written to fails at
// the first successful sync, which is the worst possible moment, because by
// then the write has already landed in somebody's CRM.
func (e *Engine) checkStore(d *Diagnosis) {
	const probe = "doctor-probe"
	if err := e.store.Put(store.CollIdem, probe, []byte("ok"), e.clock.Now().Add(time.Minute)); err != nil {
		d.fail("store", e.cfg.StateDir, fmt.Sprintf("cannot write: %v", err))
		return
	}
	if _, ok, err := e.store.Take(store.CollIdem, probe); err != nil || !ok {
		d.fail("store", e.cfg.StateDir, fmt.Sprintf("wrote a probe and could not read it back: ok=%v err=%v", ok, err))
		return
	}
	d.pass("store", e.cfg.StateDir, "readable and writable")
}

// checkConnectors asks every peer to describe every object it takes part in.
func (e *Engine) checkConnectors(ctx context.Context, d *Diagnosis) {
	type target struct {
		peer *Peer
		kind string
	}
	seen := map[string]bool{}
	var targets []target

	for _, s := range e.syncs {
		for _, p := range []*Peer{s.Left, s.Right} {
			key := p.Name + "\x00" + s.Kind
			if seen[key] {
				continue
			}
			seen[key] = true
			targets = append(targets, target{p, s.Kind})
		}
	}

	for _, t := range targets {
		subject := fmt.Sprintf("%s/%s", t.peer.Name, t.kind)
		schema, err := t.peer.Conn.Describe(ctx, t.kind)
		if err != nil {
			// This is where a wrong or expired credential surfaces, and it is
			// deliberately not distinguished from an unreachable peer: both
			// mean the sync cannot run, and the error text says which.
			d.fail("connector", subject, err.Error())
			continue
		}
		d.pass("connector", subject, fmt.Sprintf("%d fields", len(schema.Fields)))
	}
}

// checkMappings validates every field map against both live schemas.
func (e *Engine) checkMappings(ctx context.Context, d *Diagnosis) {
	for _, s := range e.syncs {
		left, leftErr := s.Left.Conn.Describe(ctx, s.Kind)
		right, rightErr := s.Right.Conn.Describe(ctx, s.Kind)
		if leftErr != nil || rightErr != nil {
			// Already reported by checkConnectors; saying it twice would make
			// one broken token look like two problems.
			continue
		}
		if err := s.Mapper.Validate(left, right); err != nil {
			d.fail("mapping", s.Name, err.Error())
			continue
		}
		d.pass("mapping", s.Name, fmt.Sprintf("%d fields, %s", len(s.Mapper.Fields()), s.Mapper.Direction()))
	}
}

// checkProgress reports how far behind each peer is.
//
// Lag is the number that goes wrong quietly. A sync that has stopped keeping
// up looks exactly like one that is working, until somebody asks why a contact
// from this morning is not there.
func (e *Engine) checkProgress(d *Diagnosis) {
	seen := map[string]bool{}
	for _, s := range e.syncs {
		for _, p := range s.readablePeers() {
			key := p.Name + "\x00" + s.Kind
			if seen[key] {
				continue
			}
			seen[key] = true

			subject := fmt.Sprintf("%s/%s", p.Name, s.Kind)
			lag, known, err := e.marks.Lag(p.Name, s.Kind)
			switch {
			case err != nil:
				d.fail("watermark", subject, err.Error())
			case !known:
				d.pass("watermark", subject, "not set yet; the next run backfills")
			default:
				d.pass("watermark", subject, fmt.Sprintf("%s behind", lag.Round(time.Second)))
			}
		}
	}
}

// checkQueues reports work that is waiting for a person.
func (e *Engine) checkQueues(d *Diagnosis) {
	for _, q := range []struct {
		collection string
		name       string
		detail     string
	}{
		{store.CollDLQ, "dead-letter queue", "items that exhausted their attempts"},
		{store.CollReview, "review queue", "decisions waiting for a person"},
	} {
		n, err := e.store.Len(q.collection)
		switch {
		case err != nil:
			d.fail("queue", q.name, err.Error())
		case n == 0:
			d.pass("queue", q.name, "empty")
		default:
			// Not an error in the sense that anything is broken, and still a
			// finding: work is sitting there and nothing else will move it.
			d.fail("queue", q.name, fmt.Sprintf("%d %s", n, q.detail))
		}
	}
}
