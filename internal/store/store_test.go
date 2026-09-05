package store_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

// factory builds a store and the clock that drives its expiry. Every test in
// this file runs against both implementations, because the point of having an
// interface is that the two behave identically.
type factory struct {
	name string
	open func(t *testing.T) (store.Store, *clock.Manual)
}

func factories() []factory {
	return []factory{
		{
			name: "mem",
			open: func(t *testing.T) (store.Store, *clock.Manual) {
				t.Helper()
				c := clock.NewManual(epoch)
				return store.NewMem(c), c
			},
		},
		{
			name: "file",
			open: func(t *testing.T) (store.Store, *clock.Manual) {
				t.Helper()
				c := clock.NewManual(epoch)
				s, err := store.OpenFile(t.TempDir(), store.FileOptions{Clock: c})
				if err != nil {
					t.Fatalf("OpenFile: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s, c
			},
		},
	}
}

func eachStore(t *testing.T, run func(t *testing.T, s store.Store, c *clock.Manual)) {
	t.Helper()
	for _, f := range factories() {
		t.Run(f.name, func(t *testing.T) {
			s, c := f.open(t)
			run(t, s, c)
		})
	}
}

func mustPut(t *testing.T, s store.Store, coll, key, val string, exp time.Time) {
	t.Helper()
	if err := s.Put(coll, key, []byte(val), exp); err != nil {
		t.Fatalf("Put(%s,%s): %v", coll, key, err)
	}
}

func mustGet(t *testing.T, s store.Store, coll, key string) (string, bool) {
	t.Helper()
	v, ok, err := s.Get(coll, key)
	if err != nil {
		t.Fatalf("Get(%s,%s): %v", coll, key, err)
	}
	return string(v), ok
}

func TestPutGetDelete(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		if _, ok := mustGet(t, s, store.CollLinks, "absent"); ok {
			t.Fatal("an absent key reported ok")
		}

		mustPut(t, s, store.CollLinks, "k", "v1", time.Time{})
		if v, ok := mustGet(t, s, store.CollLinks, "k"); !ok || v != "v1" {
			t.Fatalf("Get = %q,%v, want v1,true", v, ok)
		}

		mustPut(t, s, store.CollLinks, "k", "v2", time.Time{})
		if v, _ := mustGet(t, s, store.CollLinks, "k"); v != "v2" {
			t.Fatalf("Put did not replace: %q", v)
		}

		if err := s.Delete(store.CollLinks, "k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, ok := mustGet(t, s, store.CollLinks, "k"); ok {
			t.Fatal("key survived Delete")
		}
		if err := s.Delete(store.CollLinks, "k"); err != nil {
			t.Fatalf("deleting an absent key is an error: %v", err)
		}
	})
}

// Collections must not leak into each other, otherwise a link key and a
// watermark key could collide.
func TestCollectionsAreIsolated(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		mustPut(t, s, store.CollLinks, "k", "link", time.Time{})
		mustPut(t, s, store.CollIdem, "k", "idem", time.Time{})

		if v, _ := mustGet(t, s, store.CollLinks, "k"); v != "link" {
			t.Fatalf("links got %q", v)
		}
		if v, _ := mustGet(t, s, store.CollIdem, "k"); v != "idem" {
			t.Fatalf("idem got %q", v)
		}
	})
}

// An expired record must be invisible immediately, not only after Sweep. The
// origin log depends on this: an entry that outlived its echo window has to
// stop suppressing even if nothing has swept yet.
func TestExpiryHidesRecordsBeforeSweep(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, c *clock.Manual) {
		mustPut(t, s, store.CollOrigins, "tag", "v", epoch.Add(5*time.Minute))

		if _, ok := mustGet(t, s, store.CollOrigins, "tag"); !ok {
			t.Fatal("record was not visible before expiry")
		}
		c.Advance(5*time.Minute + time.Nanosecond)
		if _, ok := mustGet(t, s, store.CollOrigins, "tag"); ok {
			t.Fatal("expired record is still visible")
		}
		if n, err := s.Len(store.CollOrigins); err != nil || n != 0 {
			t.Fatalf("Len = %d,%v, want 0", n, err)
		}
		got, err := s.List(store.CollOrigins, 0)
		if err != nil || len(got) != 0 {
			t.Fatalf("List returned %d expired entries (%v)", len(got), err)
		}
	})
}

func TestSweepDropsOnlyExpired(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, c *clock.Manual) {
		mustPut(t, s, store.CollOrigins, "gone", "v", epoch.Add(time.Minute))
		mustPut(t, s, store.CollOrigins, "stays", "v", epoch.Add(time.Hour))
		mustPut(t, s, store.CollLinks, "forever", "v", time.Time{})

		c.Advance(2 * time.Minute)
		n, err := s.Sweep(c.Now())
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if n != 1 {
			t.Fatalf("Sweep dropped %d, want 1", n)
		}
		if _, ok := mustGet(t, s, store.CollOrigins, "stays"); !ok {
			t.Fatal("Sweep dropped a live record")
		}
		if _, ok := mustGet(t, s, store.CollLinks, "forever"); !ok {
			t.Fatal("Sweep dropped a record that never expires")
		}
	})
}

// Take has to be atomic. Two inbound events carrying the same origin tag must
// not both be suppressed: only one of them can be the echo of our write.
func TestTakeIsSingleUse(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		mustPut(t, s, store.CollOrigins, "tag", "mine", time.Time{})

		v, ok, err := s.Take(store.CollOrigins, "tag")
		if err != nil || !ok || string(v) != "mine" {
			t.Fatalf("first Take = %q,%v,%v", v, ok, err)
		}
		if _, ok, _ := s.Take(store.CollOrigins, "tag"); ok {
			t.Fatal("the same entry was taken twice")
		}
	})
}

func TestTakeOfExpiredEntryReportsMiss(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, c *clock.Manual) {
		mustPut(t, s, store.CollOrigins, "tag", "mine", epoch.Add(time.Minute))
		c.Advance(2 * time.Minute)

		if _, ok, err := s.Take(store.CollOrigins, "tag"); ok || err != nil {
			t.Fatalf("Take of an expired entry = %v,%v, want false,nil", ok, err)
		}
	})
}

// Exactly one of N concurrent takers may win. This is the assertion that keeps
// echo suppression correct under the worker pool, and it is why the interface
// has Take rather than leaving callers to Get then Delete.
func TestConcurrentTakeHasExactlyOneWinner(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		mustPut(t, s, store.CollOrigins, "tag", "mine", time.Time{})

		const racers = 16
		var wg sync.WaitGroup
		wins := make(chan struct{}, racers)
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, ok, err := s.Take(store.CollOrigins, "tag"); err == nil && ok {
					wins <- struct{}{}
				}
			}()
		}
		wg.Wait()
		close(wins)

		if n := len(wins); n != 1 {
			t.Fatalf("%d goroutines took the same entry, want exactly 1", n)
		}
	})
}

func TestListIsKeyOrderedAndLimited(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		for _, k := range []string{"c", "a", "d", "b"} {
			mustPut(t, s, store.CollDLQ, k, k, time.Time{})
		}

		all, err := s.List(store.CollDLQ, 0)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		var got string
		for _, e := range all {
			got += e.Key
		}
		if got != "abcd" {
			t.Fatalf("List order = %q, want abcd", got)
		}

		two, err := s.List(store.CollDLQ, 2)
		if err != nil {
			t.Fatalf("List(limit): %v", err)
		}
		if len(two) != 2 || two[0].Key != "a" || two[1].Key != "b" {
			t.Fatalf("limited List = %v", two)
		}
	})
}

// Callers must not be able to mutate the store by holding on to a slice they
// were given, or by reusing the buffer they passed in.
func TestValuesAreCopiedInAndOut(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		buf := []byte("original")
		if err := s.Put(store.CollLinks, "k", buf, time.Time{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		copy(buf, "MUTATED!")

		got, _, err := s.Get(store.CollLinks, "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(got) != "original" {
			t.Fatalf("mutating the caller's buffer changed the store: %q", got)
		}

		copy(got, "clobber!")
		again, _, _ := s.Get(store.CollLinks, "k")
		if string(again) != "original" {
			t.Fatalf("mutating the returned slice changed the store: %q", again)
		}
	})
}

func TestClosedStoreRejectsEverything(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, _, err := s.Get(store.CollLinks, "k"); err == nil {
			t.Fatal("Get succeeded on a closed store")
		}
		if err := s.Put(store.CollLinks, "k", nil, time.Time{}); err == nil {
			t.Fatal("Put succeeded on a closed store")
		}
	})
}

func TestConcurrentMixedAccess(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 25; j++ {
					k := fmt.Sprintf("k%d-%d", i, j)
					mustPutNoHelper(s, k)
					_, _, _ = s.Get(store.CollLinks, k)
					_, _ = s.List(store.CollLinks, 5)
					_, _ = s.Len(store.CollLinks)
				}
			}()
		}
		wg.Wait()
	})
}

func mustPutNoHelper(s store.Store, k string) {
	// Deliberately not a test helper: it runs on a non-test goroutine, where
	// calling t.Fatalf would be a race of its own.
	_ = s.Put(store.CollLinks, k, []byte("v"), time.Time{})
}

// -- typed accessors -------------------------------------------------------

func TestTypedLinkIsReachableFromBothSides(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		left := store.Ref{Connector: "hubspot", Kind: "contact", RemoteID: "1"}
		right := store.Ref{Connector: "twenty", Kind: "contact", RemoteID: "a"}
		link := store.Link{Left: left, Right: right, LinkedAt: epoch}

		if err := ty.PutLink(link); err != nil {
			t.Fatalf("PutLink: %v", err)
		}
		for _, from := range []store.Ref{left, right} {
			got, ok, err := ty.GetLink(from)
			if err != nil || !ok {
				t.Fatalf("GetLink(%s) = %v,%v", from, ok, err)
			}
			peer, ok := got.Peer(from)
			if !ok {
				t.Fatalf("Peer(%s) not found in %+v", from, got)
			}
			if peer == from {
				t.Fatalf("Peer(%s) returned the same side", from)
			}
		}

		if err := ty.DeleteLink(link); err != nil {
			t.Fatalf("DeleteLink: %v", err)
		}
		if _, ok, _ := ty.GetLink(left); ok {
			t.Fatal("link survived DeleteLink")
		}
	})
}

// A ref must not be able to collide with another by concatenation, or two
// different records would share a link row.
func TestRefKeysCannotCollide(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		a := store.Ref{Connector: "hub", Kind: "spot", RemoteID: "1"}
		b := store.Ref{Connector: "hubspot", Kind: "", RemoteID: "1"}

		if err := ty.SetSnapshot(a, store.Snapshot{Hash: "hash-a"}); err != nil {
			t.Fatalf("SetSnapshot: %v", err)
		}
		if err := ty.SetSnapshot(b, store.Snapshot{Hash: "hash-b"}); err != nil {
			t.Fatalf("SetSnapshot: %v", err)
		}
		got, _, err := ty.Snapshot(a)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if got.Hash != "hash-a" {
			t.Fatalf("refs collided: got %q", got.Hash)
		}
	})
}

func TestTypedWatermarkRoundTrip(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		if _, ok, err := ty.Watermark("hubspot", "contact"); ok || err != nil {
			t.Fatalf("unset watermark = %v,%v", ok, err)
		}
		if err := ty.SetWatermark("hubspot", "contact", epoch); err != nil {
			t.Fatalf("SetWatermark: %v", err)
		}
		got, ok, err := ty.Watermark("hubspot", "contact")
		if err != nil || !ok || !got.Equal(epoch) {
			t.Fatalf("Watermark = %v,%v,%v", got, ok, err)
		}
	})
}

func TestTypedOriginIsSingleUseAndExpires(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, c *clock.Manual) {
		ty := store.NewTyped(s)
		ref := store.Ref{Connector: "twenty", Kind: "contact", RemoteID: "a"}
		o := store.Origin{Ref: ref, Hash: "h1", WrittenAt: epoch}

		if err := ty.PutOrigin(o, epoch.Add(5*time.Minute)); err != nil {
			t.Fatalf("PutOrigin: %v", err)
		}
		if _, ok, err := ty.TakeOrigin(ref, "h1"); !ok || err != nil {
			t.Fatalf("first TakeOrigin = %v,%v", ok, err)
		}
		if _, ok, _ := ty.TakeOrigin(ref, "h1"); ok {
			t.Fatal("origin entry was consumed twice")
		}

		// A different payload for the same record is a real change, not an echo.
		if err := ty.PutOrigin(store.Origin{Ref: ref, Hash: "h2"}, epoch.Add(5*time.Minute)); err != nil {
			t.Fatalf("PutOrigin: %v", err)
		}
		if _, ok, _ := ty.TakeOrigin(ref, "h3"); ok {
			t.Fatal("a different payload was suppressed as our echo")
		}

		c.Advance(6 * time.Minute)
		if _, ok, _ := ty.TakeOrigin(ref, "h2"); ok {
			t.Fatal("an entry past its echo window still suppressed")
		}
	})
}

func TestTypedIdemRoundTrip(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, c *clock.Manual) {
		ty := store.NewTyped(s)
		want := store.IdemResult{RemoteID: "42", Created: true, WrittenAt: epoch}
		if err := ty.PutIdem("key", want, epoch.Add(time.Hour)); err != nil {
			t.Fatalf("PutIdem: %v", err)
		}
		got, ok, err := ty.Idem("key")
		if err != nil || !ok || got.RemoteID != "42" || !got.Created {
			t.Fatalf("Idem = %+v,%v,%v", got, ok, err)
		}
		c.Advance(2 * time.Hour)
		if _, ok, _ := ty.Idem("key"); ok {
			t.Fatal("idem entry outlived its expiry")
		}
	})
}

func TestTypedDeliveryDedupe(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		seen, err := ty.SeenDelivery("hubspot", "d1", epoch, epoch.Add(time.Hour))
		if err != nil || seen {
			t.Fatalf("first delivery reported seen=%v (%v)", seen, err)
		}
		seen, err = ty.SeenDelivery("hubspot", "d1", epoch, epoch.Add(time.Hour))
		if err != nil || !seen {
			t.Fatalf("redelivery reported seen=%v (%v)", seen, err)
		}
		// The same ID from a different connector is a different delivery.
		seen, err = ty.SeenDelivery("twenty", "d1", epoch, epoch.Add(time.Hour))
		if err != nil || seen {
			t.Fatalf("cross-connector delivery collided: seen=%v (%v)", seen, err)
		}
	})
}

func TestTypedQueueItemsListOldestFirst(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		for i := 0; i < 3; i++ {
			at := epoch.Add(time.Duration(i) * time.Second)
			item := store.QueueItem{
				ID:        store.NewQueueID(at, uint64(i)),
				Sync:      "contact",
				Reason:    fmt.Sprintf("reason-%d", i),
				CreatedAt: at,
				Payload:   json.RawMessage(`{"n":` + fmt.Sprint(i) + `}`),
			}
			if err := ty.PutQueueItem(store.CollDLQ, item); err != nil {
				t.Fatalf("PutQueueItem: %v", err)
			}
		}

		items, err := ty.ListQueueItems(store.CollDLQ, 0)
		if err != nil {
			t.Fatalf("ListQueueItems: %v", err)
		}
		if len(items) != 3 {
			t.Fatalf("got %d items, want 3", len(items))
		}
		for i, item := range items {
			if item.Reason != fmt.Sprintf("reason-%d", i) {
				t.Fatalf("item %d out of order: %s", i, item.Reason)
			}
		}

		if err := ty.DeleteQueueItem(store.CollDLQ, items[0].ID); err != nil {
			t.Fatalf("DeleteQueueItem: %v", err)
		}
		if _, ok, _ := ty.GetQueueItem(store.CollDLQ, items[0].ID); ok {
			t.Fatal("item survived delete")
		}
	})
}

// Queue IDs must sort chronologically as strings, because that is what makes
// List return oldest first without a secondary index.
func TestQueueIDsSortChronologically(t *testing.T) {
	early := store.NewQueueID(epoch, 0)
	late := store.NewQueueID(epoch.Add(time.Nanosecond), 0)
	sameInstant := store.NewQueueID(epoch, 1)

	if !(early < late) {
		t.Fatalf("%q should sort before %q", early, late)
	}
	if !(early < sameInstant) {
		t.Fatalf("%q should sort before %q", early, sameInstant)
	}
}

// The snapshot carries the values, not just the digest, because field-level
// conflict resolution is a three-way merge and a three-way merge needs a base.
func TestSnapshotRoundTripsItsValues(t *testing.T) {
	eachStore(t, func(t *testing.T, s store.Store, _ *clock.Manual) {
		ty := store.NewTyped(s)
		ref := store.Ref{Connector: "hubspot", Kind: "contact", RemoteID: "1"}

		want := store.Snapshot{
			Hash:   "abc",
			Values: map[string]any{"email": "ann@example.com", "score": float64(42)},
			At:     epoch,
		}
		if err := ty.SetSnapshot(ref, want); err != nil {
			t.Fatalf("SetSnapshot: %v", err)
		}

		got, ok, err := ty.Snapshot(ref)
		if err != nil || !ok {
			t.Fatalf("Snapshot = %v,%v", ok, err)
		}
		if got.Hash != want.Hash || !got.At.Equal(want.At) {
			t.Fatalf("Snapshot = %+v, want %+v", got, want)
		}
		if got.Values["email"] != "ann@example.com" || got.Values["score"] != float64(42) {
			t.Fatalf("values did not round-trip: %#v", got.Values)
		}
	})
}
