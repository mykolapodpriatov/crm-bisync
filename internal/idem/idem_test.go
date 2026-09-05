package idem_test

import (
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/idem"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func newCache(t *testing.T, ttl time.Duration) (*idem.Cache, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	return idem.NewCache(store.NewTyped(store.NewMem(c)), c, ttl), c
}

func left(id string) store.Ref {
	return store.Ref{Connector: "hubspot", Kind: "contact", RemoteID: id}
}

func right(id string) store.Ref {
	return store.Ref{Connector: "twenty", Kind: "contact", RemoteID: id}
}

// The key has to survive a restart, because a key regenerated after a crash
// must match the one the pre-crash attempt used or the cache never hits.
func TestKeyIsDeterministic(t *testing.T) {
	a := idem.Key("contact-sync", "contact", left("1"), right("a"), "hash")
	b := idem.Key("contact-sync", "contact", left("1"), right("a"), "hash")
	if a != b {
		t.Fatalf("the same write derived two keys: %s and %s", a, b)
	}
}

func TestKeyChangesWithEveryInput(t *testing.T) {
	base := idem.Key("contact-sync", "contact", left("1"), right("a"), "hash")

	variants := map[string]string{
		"sync":     idem.Key("other-sync", "contact", left("1"), right("a"), "hash"),
		"kind":     idem.Key("contact-sync", "company", left("1"), right("a"), "hash"),
		"source":   idem.Key("contact-sync", "contact", left("2"), right("a"), "hash"),
		"target":   idem.Key("contact-sync", "contact", left("1"), right("b"), "hash"),
		"payload":  idem.Key("contact-sync", "contact", left("1"), right("a"), "other"),
		"peername": idem.Key("contact-sync", "contact", left("1"), store.Ref{Connector: "pipedrive", Kind: "contact", RemoteID: "a"}, "hash"),
	}
	for name, key := range variants {
		if key == base {
			t.Errorf("changing the %s did not change the key", name)
		}
	}
}

// On a create the target has no ID yet. Without the source in the key, two
// different records creating on the same peer derive the same key, and the
// second is swallowed as a replay of the first.
func TestTwoCreatesFromDifferentSourcesGetDifferentKeys(t *testing.T) {
	empty := store.Ref{Connector: "twenty", Kind: "contact"}

	a := idem.Key("contact-sync", "contact", left("1"), empty, "hash")
	b := idem.Key("contact-sync", "contact", left("2"), empty, "hash")
	if a == b {
		t.Fatal("two creates from different records share an idempotency key")
	}
}

// Length prefixing: without it, ("ab","c") and ("a","bc") derive the same key.
func TestKeyPartsCannotBeRepacked(t *testing.T) {
	a := idem.Key("ab", "c", left("1"), right("a"), "h")
	b := idem.Key("a", "bc", left("1"), right("a"), "h")
	if a == b {
		t.Fatal("key parts are not delimited")
	}
}

func TestFirstAttemptProceeds(t *testing.T) {
	c, _ := newCache(t, 0)

	_, decision, err := c.Begin("k")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if decision != idem.Proceed {
		t.Fatalf("decision = %s, want proceed", decision)
	}
}

func TestACommittedWriteReplaysWithoutACall(t *testing.T) {
	c, _ := newCache(t, 0)

	if _, _, err := c.Begin("k"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := c.Commit("k", store.IdemResult{RemoteID: "twenty-1", Created: true}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	res, decision, err := c.Begin("k")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if decision != idem.Replayed {
		t.Fatalf("decision = %s, want replayed", decision)
	}
	if res.RemoteID != "twenty-1" {
		t.Fatalf("replay lost the remote ID: %+v", res)
	}
	// A replay must report the original outcome. Saying Created = false would
	// make the engine think it updated something it actually inserted.
	if !res.Created {
		t.Fatal("a replay reported Created = false")
	}
}

// The case the in-flight mark exists for: the peer created the record and the
// response never came back. Without the mark this is indistinguishable from
// "the write never happened", and the retry creates a second record.
func TestAnUnfinishedAttemptAsksForARecheck(t *testing.T) {
	c, _ := newCache(t, 0)

	if _, _, err := c.Begin("k"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// No Commit: the process died, or the connection dropped mid-write.

	_, decision, err := c.Begin("k")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if decision != idem.Recheck {
		t.Fatalf("decision = %s, want recheck", decision)
	}
}

// Abandon is for a write the caller knows did not land, such as one the peer
// rejected outright. It must not be used on a timeout, and the distinction is
// the reason it is a separate call rather than part of error handling.
func TestAbandonClearsTheMarkSoTheNextAttemptProceeds(t *testing.T) {
	c, _ := newCache(t, 0)

	if _, _, err := c.Begin("k"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := c.Abandon("k"); err != nil {
		t.Fatalf("Abandon: %v", err)
	}

	_, decision, err := c.Begin("k")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if decision != idem.Proceed {
		t.Fatalf("decision = %s, want proceed", decision)
	}
}

func TestLookupIgnoresAnInFlightMark(t *testing.T) {
	c, _ := newCache(t, 0)

	if _, _, err := c.Begin("k"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, ok, err := c.Lookup("k"); err != nil || ok {
		t.Fatalf("Lookup returned an in-flight mark as a result: ok=%v err=%v", ok, err)
	}

	if err := c.Commit("k", store.IdemResult{RemoteID: "x"}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, ok, err := c.Lookup("k"); err != nil || !ok {
		t.Fatalf("Lookup missed a committed result: ok=%v err=%v", ok, err)
	}
}

// The cache is a retry absorber, not a permanent record: once the peer's next
// poll surfaces the record, the link table answers instead.
func TestResultsExpire(t *testing.T) {
	c, clk := newCache(t, 10*time.Minute)

	if _, _, err := c.Begin("k"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := c.Commit("k", store.IdemResult{RemoteID: "x"}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	clk.Advance(11 * time.Minute)
	_, decision, err := c.Begin("k")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if decision != idem.Proceed {
		t.Fatalf("decision = %s after expiry, want proceed", decision)
	}
}

// The realistic loop: attempt, transient failure, retry with the same key.
// Exactly one write should reach the peer.
func TestRetryLoopMakesOneWrite(t *testing.T) {
	c, _ := newCache(t, 0)
	key := idem.Key("contact-sync", "contact", left("1"), right("a"), "hash")

	writes := 0
	attempt := func(succeed bool) idem.Decision {
		t.Helper()
		_, decision, err := c.Begin(key)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if decision != idem.Proceed {
			return decision
		}
		writes++
		if succeed {
			if err := c.Commit(key, store.IdemResult{RemoteID: "twenty-1"}); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
		return decision
	}

	// A transient failure: the call went out, nothing came back.
	attempt(false)
	// The retry must not blindly write again.
	if d := attempt(true); d != idem.Recheck {
		t.Fatalf("retry decision = %s, want recheck", d)
	}
	if writes != 1 {
		t.Fatalf("%d writes reached the peer, want 1", writes)
	}
}
