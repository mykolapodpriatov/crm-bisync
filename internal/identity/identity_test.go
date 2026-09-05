package identity_test

import (
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/identity"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

const (
	leftConn  = "hubspot"
	rightConn = "twenty"
)

func newResolver(t *testing.T, onAmbiguous string) (*identity.Resolver, *store.Typed) {
	t.Helper()
	typed := store.NewTyped(store.NewMem(clock.NewManual(epoch)))
	return identity.New(typed, "contact", []string{"email", "domain"}, onAmbiguous), typed
}

func ref(conn, id string) store.Ref {
	return store.Ref{Connector: conn, Kind: "contact", RemoteID: id}
}

func index(t *testing.T, r *identity.Resolver, ref store.Ref, fields map[string]any) {
	t.Helper()
	if err := r.Index(ref, fields); err != nil {
		t.Fatalf("Index(%s): %v", ref, err)
	}
}

func resolve(t *testing.T, r *identity.Resolver, from store.Ref, peer string, fields map[string]any) identity.Resolution {
	t.Helper()
	res, err := r.Resolve(from, peer, fields)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", from, err)
	}
	return res
}

// Tier one. Once two records are linked, nothing else is consulted, which is
// what keeps steady state to a single lookup.
func TestAnExistingLinkWins(t *testing.T) {
	r, typed := newResolver(t, identity.OnAmbiguousReview)
	from, peer := ref(leftConn, "1"), ref(rightConn, "a")

	if err := typed.PutLink(store.Link{Left: from, Right: peer, LinkedAt: epoch}); err != nil {
		t.Fatalf("PutLink: %v", err)
	}
	// A decoy that would win on the deterministic key if the link did not.
	index(t, r, ref(rightConn, "b"), map[string]any{"email": "ann@example.com"})

	res := resolve(t, r, from, rightConn, map[string]any{"email": "ann@example.com"})
	if res.Outcome != identity.Linked {
		t.Fatalf("outcome = %s, want linked", res.Outcome)
	}
	if res.Peer != peer {
		t.Fatalf("peer = %s, want %s", res.Peer, peer)
	}
}

// Tier two. One candidate on a normalised key is a match.
func TestASingleDeterministicMatch(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	index(t, r, ref(rightConn, "a"), map[string]any{"email": "ann@example.com"})

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "ann@example.com"})
	if res.Outcome != identity.Matched {
		t.Fatalf("outcome = %s, want matched", res.Outcome)
	}
	if res.Peer != ref(rightConn, "a") {
		t.Fatalf("peer = %s", res.Peer)
	}
	if res.Reason == "" {
		t.Fatal("a match with no reason is unreviewable")
	}
}

// Tier three. Two candidates is exactly the case where guessing merges two
// real customers, so the default writes nothing.
func TestAmbiguityGoesToReviewByDefault(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	index(t, r, ref(rightConn, "a"), map[string]any{"email": "ann@example.com"})
	index(t, r, ref(rightConn, "b"), map[string]any{"domain": "example.com"})

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{
		"email":  "ann@example.com",
		"domain": "example.com",
	})
	if res.Outcome != identity.Review {
		t.Fatalf("outcome = %s, want review", res.Outcome)
	}
	if len(res.Candidates) != 2 {
		t.Fatalf("got %d candidates, want 2", len(res.Candidates))
	}
	if res.Peer != (store.Ref{}) {
		t.Fatal("an ambiguous resolution named a peer, which the caller might write to")
	}
}

func TestAmbiguityPolicies(t *testing.T) {
	for policy, want := range map[string]identity.Outcome{
		identity.OnAmbiguousReview: identity.Review,
		identity.OnAmbiguousCreate: identity.Create,
		identity.OnAmbiguousSkip:   identity.Skip,
	} {
		r, _ := newResolver(t, policy)
		index(t, r, ref(rightConn, "a"), map[string]any{"email": "ann@example.com"})
		index(t, r, ref(rightConn, "b"), map[string]any{"email": "ann@example.com"})

		res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "ann@example.com"})
		if res.Outcome != want {
			t.Errorf("policy %q gave %s, want %s", policy, res.Outcome, want)
		}
	}
}

// No match is not ambiguity. Review is for "which of these two is it", so a
// record with no counterpart still creates under the review policy.
func TestNoMatchCreatesEvenUnderReview(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "new@example.com"})
	if res.Outcome != identity.Create {
		t.Fatalf("outcome = %s, want create", res.Outcome)
	}
}

func TestNoMatchSkipsUnderTheSkipPolicy(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousSkip)

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "new@example.com"})
	if res.Outcome != identity.Skip {
		t.Fatalf("outcome = %s, want skip", res.Outcome)
	}
}

// A contact whose email changes must stop answering to the old address.
// A stale index entry makes the next record with that address match the wrong
// person, which is fuzzy matching's failure reached more slowly.
func TestReindexingReleasesTheOldKey(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	peer := ref(rightConn, "a")

	index(t, r, peer, map[string]any{"email": "old@example.com"})
	index(t, r, peer, map[string]any{"email": "new@example.com"})

	stale := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "old@example.com"})
	if stale.Outcome != identity.Create {
		t.Fatalf("the released address still matched: %s -> %s", stale.Outcome, stale.Peer)
	}

	current := resolve(t, r, ref(leftConn, "2"), rightConn, map[string]any{"email": "new@example.com"})
	if current.Outcome != identity.Matched || current.Peer != peer {
		t.Fatalf("the current address did not match: %s -> %s", current.Outcome, current.Peer)
	}
}

func TestDeindexRemovesARecordFromMatching(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	peer := ref(rightConn, "a")
	index(t, r, peer, map[string]any{"email": "ann@example.com"})

	if err := r.Deindex(peer); err != nil {
		t.Fatalf("Deindex: %v", err)
	}
	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "ann@example.com"})
	if res.Outcome != identity.Create {
		t.Fatalf("a deindexed record still matched: %s", res.Outcome)
	}
}

// An empty value is not a key. Indexing blank emails would make every record
// without one a candidate for every other one.
func TestEmptyAndMissingValuesDoNotIndex(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	index(t, r, ref(rightConn, "a"), map[string]any{"email": ""})
	index(t, r, ref(rightConn, "b"), map[string]any{})

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": ""})
	if res.Outcome != identity.Create {
		t.Fatalf("a blank key matched %d candidates: %s", len(res.Candidates), res.Outcome)
	}
}

// The index is per connector: the same email on our own side is not a match
// for us, or every record would match itself.
func TestKeysAreScopedToTheirConnector(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	index(t, r, ref(leftConn, "1"), map[string]any{"email": "ann@example.com"})

	res := resolve(t, r, ref(leftConn, "1"), rightConn, map[string]any{"email": "ann@example.com"})
	if res.Outcome != identity.Create {
		t.Fatalf("a record matched itself across the sync: %s -> %s", res.Outcome, res.Peer)
	}
}

func TestIndexingTheSameRecordTwiceIsIdempotent(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	peer := ref(rightConn, "a")
	fields := map[string]any{"email": "ann@example.com"}

	index(t, r, peer, fields)
	index(t, r, peer, fields)

	res := resolve(t, r, ref(leftConn, "1"), rightConn, fields)
	if res.Outcome != identity.Matched {
		t.Fatalf("outcome = %s with %d candidates, want a single match",
			res.Outcome, len(res.Candidates))
	}
}

// Two keys pointing at the same record is one candidate, not two.
func TestTwoKeysOnOneRecordIsASingleMatch(t *testing.T) {
	r, _ := newResolver(t, identity.OnAmbiguousReview)
	peer := ref(rightConn, "a")
	fields := map[string]any{"email": "ann@example.com", "domain": "example.com"}

	index(t, r, peer, fields)

	res := resolve(t, r, ref(leftConn, "1"), rightConn, fields)
	if res.Outcome != identity.Matched || res.Peer != peer {
		t.Fatalf("outcome = %s, peer = %s", res.Outcome, res.Peer)
	}
}
