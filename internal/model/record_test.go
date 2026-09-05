package model

import (
	"testing"
	"time"
)

func TestSnapshotHashIsStableAcrossMapOrder(t *testing.T) {
	names := []string{"email", "first_name", "last_name"}
	a := map[string]any{"email": "a@b.c", "first_name": "Ann", "last_name": "Lee"}
	b := map[string]any{"last_name": "Lee", "email": "a@b.c", "first_name": "Ann"}

	if SnapshotHash(a, names) != SnapshotHash(b, names) {
		t.Fatal("hash depends on map iteration order")
	}
}

// The whole point of hashing only mapped fields: a field the sync does not own
// must not be able to trigger a write.
func TestSnapshotHashIgnoresUnmappedFields(t *testing.T) {
	names := []string{"email"}
	base := map[string]any{"email": "a@b.c"}
	noisy := map[string]any{"email": "a@b.c", "hs_last_page_seen": "/pricing"}

	if SnapshotHash(base, names) != SnapshotHash(noisy, names) {
		t.Fatal("an unmapped field changed the hash")
	}
}

// A JSON round-trip turns int 1 into float64 1. If that reads as a change the
// engine writes forever, so it is asserted rather than assumed.
func TestSnapshotHashTreatsWholeFloatsAndIntsAlike(t *testing.T) {
	names := []string{"score"}
	if SnapshotHash(map[string]any{"score": 42}, names) !=
		SnapshotHash(map[string]any{"score": float64(42)}, names) {
		t.Fatal("int 42 and float64 42 hash differently")
	}
}

func TestSnapshotHashDistinguishesAbsentFromEmpty(t *testing.T) {
	names := []string{"phone"}
	absent := SnapshotHash(map[string]any{}, names)
	empty := SnapshotHash(map[string]any{"phone": ""}, names)
	null := SnapshotHash(map[string]any{"phone": nil}, names)

	if absent == empty || absent == null || empty == null {
		t.Fatalf("absent/empty/null collapsed: %s %s %s", absent, empty, null)
	}
}

// Length-prefixing exists so adjacent values cannot be repacked into the same
// digest. Without it ("ab","c") and ("a","bc") collide.
func TestSnapshotHashResistsFieldBoundaryCollision(t *testing.T) {
	names := []string{"a", "b"}
	x := SnapshotHash(map[string]any{"a": "ab", "b": "c"}, names)
	y := SnapshotHash(map[string]any{"a": "a", "b": "bc"}, names)
	if x == y {
		t.Fatal("field boundaries are not encoded in the digest")
	}
}

func TestSnapshotHashNormalisesTimeZones(t *testing.T) {
	names := []string{"created"}
	utc := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	east := utc.In(time.FixedZone("EEST", 3*3600))

	if SnapshotHash(map[string]any{"created": utc}, names) !=
		SnapshotHash(map[string]any{"created": east}, names) {
		t.Fatal("the same instant in two zones hashes differently")
	}
}

func TestSnapshotHashChangesWhenAMappedValueChanges(t *testing.T) {
	names := []string{"email"}
	if SnapshotHash(map[string]any{"email": "a@b.c"}, names) ==
		SnapshotHash(map[string]any{"email": "d@e.f"}, names) {
		t.Fatal("a mapped value change did not change the hash")
	}
}

func TestChangeEventHydrated(t *testing.T) {
	if (ChangeEvent{}).Hydrated() {
		t.Fatal("a notification-only event reported itself as hydrated")
	}
	if !(ChangeEvent{Record: &Record{}}).Hydrated() {
		t.Fatal("an event carrying a record reported itself as not hydrated")
	}
}
