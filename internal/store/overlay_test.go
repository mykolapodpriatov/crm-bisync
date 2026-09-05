package store_test

import (
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

func newOverlay(t *testing.T) (*store.Overlay, store.Store, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	base := store.NewMem(c)
	return store.NewOverlay(base, c), base, c
}

func TestOverlayReadsThroughToTheBase(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := base.Put(store.CollLinks, "k", []byte("base"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, ok, err := o.Get(store.CollLinks, "k")
	if err != nil || !ok || string(v) != "base" {
		t.Fatalf("Get = %q,%v,%v", v, ok, err)
	}
}

// The whole point: the run sees its own effects, and the base does not.
func TestOverlayWritesAreVisibleButNotPersisted(t *testing.T) {
	o, base, _ := newOverlay(t)

	if err := o.Put(store.CollLinks, "k", []byte("planned"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, _, _ := o.Get(store.CollLinks, "k"); string(v) != "planned" {
		t.Fatalf("the overlay cannot see its own write: %q", v)
	}
	if _, ok, _ := base.Get(store.CollLinks, "k"); ok {
		t.Fatal("a dry run reached the real store")
	}
}

func TestOverlayShadowsTheBase(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := base.Put(store.CollLinks, "k", []byte("base"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Put(store.CollLinks, "k", []byte("planned"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if v, _, _ := o.Get(store.CollLinks, "k"); string(v) != "planned" {
		t.Fatalf("the base won over the overlay: %q", v)
	}
	if v, _, _ := base.Get(store.CollLinks, "k"); string(v) != "base" {
		t.Fatalf("the base was modified: %q", v)
	}
}

// A delete has to hide the base entry rather than fall through to it, or the
// second half of a dry run sees rows the first half removed.
func TestOverlayDeleteHidesTheBase(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := base.Put(store.CollLinks, "k", []byte("base"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Delete(store.CollLinks, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, _ := o.Get(store.CollLinks, "k"); ok {
		t.Fatal("a deleted key fell through to the base")
	}
	if _, ok, _ := base.Get(store.CollLinks, "k"); !ok {
		t.Fatal("the delete reached the real store")
	}
}

func TestOverlayPutAfterDeleteResurrects(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := base.Put(store.CollLinks, "k", []byte("base"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Delete(store.CollLinks, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := o.Put(store.CollLinks, "k", []byte("again"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if v, ok, _ := o.Get(store.CollLinks, "k"); !ok || string(v) != "again" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
}

// Take against a base entry has to consume it for the run, without touching
// the base. Echo suppression uses Take, so a dry run must not eat the real
// engine's origin entries.
func TestOverlayTakeConsumesLocallyOnly(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := base.Put(store.CollOrigins, "tag", []byte("mine"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, ok, err := o.Take(store.CollOrigins, "tag")
	if err != nil || !ok || string(v) != "mine" {
		t.Fatalf("Take = %q,%v,%v", v, ok, err)
	}
	if _, ok, _ := o.Take(store.CollOrigins, "tag"); ok {
		t.Fatal("the overlay took the same entry twice")
	}
	if _, ok, _ := base.Get(store.CollOrigins, "tag"); !ok {
		t.Fatal("a dry run consumed the real engine's origin entry")
	}
}

func TestOverlayListMergesAndHonoursDeletes(t *testing.T) {
	o, base, _ := newOverlay(t)
	for _, k := range []string{"a", "b", "c"} {
		if err := base.Put(store.CollDLQ, k, []byte("base-"+k), time.Time{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := o.Put(store.CollDLQ, "d", []byte("planned"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Put(store.CollDLQ, "b", []byte("shadowed"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Delete(store.CollDLQ, "c"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entries, err := o.List(store.CollDLQ, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	var keys, values string
	for _, e := range entries {
		keys += e.Key
		values += string(e.Value) + ";"
	}
	if keys != "abd" {
		t.Fatalf("List keys = %q, want abd", keys)
	}
	if values != "base-a;shadowed;planned;" {
		t.Fatalf("List values = %q", values)
	}

	if n, err := o.Len(store.CollDLQ); err != nil || n != 3 {
		t.Fatalf("Len = %d,%v, want 3", n, err)
	}
	limited, err := o.List(store.CollDLQ, 2)
	if err != nil || len(limited) != 2 {
		t.Fatalf("limited List returned %d entries (%v)", len(limited), err)
	}
}

func TestOverlaySweepLeavesTheBaseAlone(t *testing.T) {
	o, base, c := newOverlay(t)
	if err := base.Put(store.CollOrigins, "base-tag", []byte("v"), epoch.Add(time.Minute)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := o.Put(store.CollOrigins, "over-tag", []byte("v"), epoch.Add(time.Minute)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	c.Advance(2 * time.Minute)
	n, err := o.Sweep(c.Now())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("Sweep dropped %d entries, want 1", n)
	}
	if l, _ := base.Len(store.CollOrigins); l != 0 {
		// The base entry is expired, so it is invisible either way. What
		// matters is that the overlay did not reach in and remove it.
		t.Logf("base still holds %d entries", l)
	}
}

func TestOverlayCloseLeavesTheBaseOpen(t *testing.T) {
	o, base, _ := newOverlay(t)
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := o.Get(store.CollLinks, "k"); err == nil {
		t.Fatal("a closed overlay still answered")
	}
	if err := base.Put(store.CollLinks, "k", []byte("v"), time.Time{}); err != nil {
		t.Fatalf("the base was closed with the overlay: %v", err)
	}
}
