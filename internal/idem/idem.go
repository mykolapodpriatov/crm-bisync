// Package idem makes writes safe to repeat.
//
// Retries are not an exception here, they are the design. The watermark
// overlap window guarantees records get re-read, webhooks get redelivered, and
// a transient failure gets another attempt. All of that is only safe because
// every write carries a deterministic key, so the two mechanisms were designed
// together rather than one bolted onto the other.
package idem

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

// Decision is what the cache tells the caller to do about a write.
type Decision int

// The possible decisions.
const (
	// Proceed means no attempt has been made: make the call.
	Proceed Decision = iota
	// Replayed means an identical write already landed. The cached result is
	// returned and no call is made.
	Replayed
	// Recheck means a previous attempt was started and never finished, so the
	// write may or may not have landed. The caller must re-resolve identity
	// before writing, because blindly retrying a create is how a lost response
	// turns into a duplicate contact.
	Recheck
)

// String renders the decision for logs and metric labels.
func (d Decision) String() string {
	switch d {
	case Replayed:
		return "replayed"
	case Recheck:
		return "recheck"
	default:
		return "proceed"
	}
}

// Key derives the idempotency key for one write.
//
// It is deterministic across process restarts, which is the whole point: a
// key regenerated after a crash has to match the one the pre-crash attempt
// used, or the replay cache never hits.
//
// The source ref is part of the key even though the write goes to the target.
// On a create the target has no ID yet, so without the source two different
// records creating on the same peer would derive the same key and the second
// would be swallowed as a replay of the first.
func Key(sync, kind string, source, target store.Ref, payloadHash string) string {
	h := sha256.New()
	for _, part := range []string{
		sync, kind,
		source.Connector, source.RemoteID,
		target.Connector, target.RemoteID,
		payloadHash,
	} {
		// Length-prefixed, so ("ab","c") and ("a","bc") cannot derive the
		// same key and collapse two different writes into one.
		_, _ = h.Write([]byte(strconv.Itoa(len(part))))
		_, _ = h.Write([]byte(":"))
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Cache remembers what writes produced, so a repeat can skip the network.
type Cache struct {
	typed *store.Typed
	clock clock.Clock
	ttl   time.Duration
}

// DefaultTTL is how long a write result is remembered.
//
// It has to comfortably exceed the retry budget plus the watermark overlap,
// because those are what generate the repeats it exists to absorb. It does
// not need to be long: once the peer's next poll surfaces the record, the
// link table answers instead.
const DefaultTTL = time.Hour

// NewCache builds a cache. A zero ttl means DefaultTTL.
func NewCache(typed *store.Typed, c clock.Clock, ttl time.Duration) *Cache {
	if c == nil {
		c = clock.Real{}
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Cache{typed: typed, clock: c, ttl: ttl}
}

// Begin records the intent to write and reports what the caller should do.
//
// Recording intent before the call is what makes a lost response recoverable.
// Without it, a crash between "the peer created the record" and "we wrote the
// result down" is indistinguishable from "the write never happened", and the
// retry creates a second record.
func (c *Cache) Begin(key string) (store.IdemResult, Decision, error) {
	prev, ok, err := c.typed.Idem(key)
	if err != nil {
		return store.IdemResult{}, Proceed, err
	}
	switch {
	case ok && !prev.InFlight:
		return prev, Replayed, nil
	case ok && prev.InFlight:
		return store.IdemResult{}, Recheck, nil
	}

	now := c.clock.Now()
	mark := store.IdemResult{InFlight: true, WrittenAt: now}
	if err := c.typed.PutIdem(key, mark, now.Add(c.ttl)); err != nil {
		return store.IdemResult{}, Proceed, err
	}
	return store.IdemResult{}, Proceed, nil
}

// Commit records what the write produced.
func (c *Cache) Commit(key string, res store.IdemResult) error {
	now := c.clock.Now()
	res.InFlight = false
	if res.WrittenAt.IsZero() {
		res.WrittenAt = now
	}
	return c.typed.PutIdem(key, res, now.Add(c.ttl))
}

// Abandon clears the in-flight mark after a failure the caller knows did not
// land, such as a request the peer rejected outright.
//
// It is deliberately not called on a timeout or a dropped connection: those
// are exactly the cases where the write may have landed, and clearing the mark
// would turn the next attempt into a blind create.
func (c *Cache) Abandon(key string) error {
	return c.typed.S.Delete(store.CollIdem, key)
}

// Lookup reports a previously committed result without recording intent.
func (c *Cache) Lookup(key string) (store.IdemResult, bool, error) {
	res, ok, err := c.typed.Idem(key)
	if err != nil || !ok || res.InFlight {
		return store.IdemResult{}, false, err
	}
	return res, true, nil
}
