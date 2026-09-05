// Package origin decides whether an inbound change is one this engine caused.
//
// It is the mechanism the whole repository stands on. If a write we perform
// can be re-ingested as a foreign change, the engine writes forever: our write
// to B fires B's webhook, which we apply to A, which fires A's webhook, and so
// on until somebody notices the API bill.
//
// The naive fix is to ignore changes whose actor is our integration user. It
// does not work. Several CRMs do not report an actor on webhooks at all, and
// where they do, an automation on the peer can rewrite the record milliseconds
// after our write, under our own actor: filtering on the actor drops that
// genuine change. So this filters on the payload instead. We recognise our own
// write by what it said, not by who said it.
package origin

import (
	"sync"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

// recentMarker is the pseudo-hash under which a per-record "we wrote something
// here lately" entry is filed. It starts with a character a hex digest never
// contains, so it cannot collide with a real payload hash.
const recentMarker = "*recent"

// Verdict is what the suppressor decided about an inbound change.
type Verdict int

// The possible verdicts.
const (
	// Foreign means the change is real and must be processed.
	Foreign Verdict = iota
	// Echo means this engine caused the change, and it stops here.
	Echo
	// NearMiss means the record is one we wrote to inside the window, but the
	// payload differs. It is treated exactly like Foreign, because it is a
	// real change: the peer rewrote what we sent. It is reported separately
	// because a rising near-miss count is the early warning that a peer's
	// automation is fighting the sync, which turns a healthy loop into a slow
	// write storm long before anybody notices the traffic.
	NearMiss
)

// String renders the verdict for logs and metric labels.
func (v Verdict) String() string {
	switch v {
	case Echo:
		return "echo"
	case NearMiss:
		return "near_miss"
	default:
		return "foreign"
	}
}

// Processed reports whether a verdict lets the change continue down the
// pipeline. Only a confirmed echo stops.
func (v Verdict) Processed() bool { return v != Echo }

// Stats counts what the suppressor has seen. The engine exposes these as
// metrics; the near-miss count is the one worth alerting on.
type Stats struct {
	Foreign   int64
	Echoes    int64
	NearMiss  int64
	Recorded  int64
	Swept     int64
	Unmatched int64
}

// Suppressor records outbound writes and recognises their echoes.
//
// Safe for concurrent use: the worker pool classifies from several goroutines.
type Suppressor struct {
	typed  *store.Typed
	clock  clock.Clock
	window map[string]time.Duration

	mu    sync.Mutex
	stats Stats
}

// New builds a suppressor. window holds the echo window per connector name,
// because peers differ in how long they take to deliver a webhook, and the
// window has to comfortably exceed that: an entry that expires before our own
// write comes back is the start of a loop.
func New(typed *store.Typed, c clock.Clock, window map[string]time.Duration) *Suppressor {
	if c == nil {
		c = clock.Real{}
	}
	return &Suppressor{typed: typed, clock: c, window: window}
}

// Window reports the echo window for a connector.
func (s *Suppressor) Window(connector string) time.Duration {
	if d, ok := s.window[connector]; ok && d > 0 {
		return d
	}
	return 5 * time.Minute
}

// Record notes that we wrote this exact payload to this exact record.
//
// hash is the snapshot digest of the mapped fields, which is what makes the
// entry recognisable without the peer telling us who made the change.
func (s *Suppressor) Record(ref store.Ref, hash string) error {
	now := s.clock.Now()
	expires := now.Add(s.Window(ref.Connector))

	if err := s.typed.PutOrigin(store.Origin{
		Ref:       ref,
		Hash:      hash,
		WrittenAt: now,
	}, expires); err != nil {
		return err
	}
	// A second, per-record entry with the same lifetime. Classify needs to
	// answer "did we write anything to this record lately" in one lookup;
	// scanning the write log for a matching prefix would make every inbound
	// event cost a pass over the whole log.
	if err := s.typed.PutOrigin(store.Origin{
		Ref:       ref,
		Hash:      recentMarker,
		WrittenAt: now,
	}, expires); err != nil {
		return err
	}

	s.count(func(st *Stats) { st.Recorded++ })
	return nil
}

// count mutates the stats under the lock.
func (s *Suppressor) count(f func(*Stats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.stats)
}

// Classify decides what an inbound change is.
//
// The entry is consumed, so it is single-use. That matters: if our write is
// followed by a genuine second change carrying the same payload, only the
// first inbound event is our echo, and suppressing both would drop a real
// change. Consumption is atomic in the store, so two workers racing the same
// tag cannot both be told it was theirs.
func (s *Suppressor) Classify(ref store.Ref, hash string) (Verdict, error) {
	_, ok, err := s.typed.TakeOrigin(ref, hash)
	if err != nil {
		return Foreign, err
	}
	if ok {
		s.count(func(st *Stats) { st.Echoes++ })
		return Echo, nil
	}

	// No exact match. Did we write something else to this record recently?
	// If so the peer changed what we sent, which is a real change and also a
	// signal worth counting.
	recent, err := s.wroteRecently(ref)
	if err != nil {
		return Foreign, err
	}
	if recent {
		s.count(func(st *Stats) { st.NearMiss++ })
		return NearMiss, nil
	}

	s.count(func(st *Stats) { st.Foreign++ })
	return Foreign, nil
}

// wroteRecently reports whether we wrote anything to this record inside its
// echo window, whatever the payload was.
func (s *Suppressor) wroteRecently(ref store.Ref) (bool, error) {
	_, ok, err := s.typed.S.Get(store.CollOrigins, store.OriginTag(ref, recentMarker))
	return ok, err
}

// Sweep drops expired entries and reports how many went.
//
// Expiry is already invisible to Classify, so this exists only to stop the
// write log growing without bound. The count it returns is worth watching:
// entries that expire unmatched are writes whose echo never arrived, which
// either means the peer does not echo at all or that the window is too short.
func (s *Suppressor) Sweep() (int, error) {
	n, err := s.typed.S.Sweep(s.clock.Now())
	if err != nil {
		return 0, err
	}
	s.count(func(st *Stats) { st.Swept += int64(n) })
	return n, nil
}

// Stats returns a snapshot of the counters.
func (s *Suppressor) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
