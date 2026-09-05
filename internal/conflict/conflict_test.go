package conflict_test

import (
	"strings"
	"testing"
	"time"

	"crm-bisync/internal/conflict"
	"crm-bisync/internal/model"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

var fields = []string{"email", "first_name", "phone"}

// state builds a side, deriving the digests the way the engine does so the
// tests cannot drift from what the pipeline will actually store.
func state(now, base map[string]any, updatedAt time.Time) conflict.State {
	s := conflict.State{
		Fields:    now,
		Hash:      model.SnapshotHash(now, fields),
		Present:   true,
		UpdatedAt: updatedAt,
	}
	if base != nil {
		s.Base = base
		s.SyncedHash = model.SnapshotHash(base, fields)
	}
	return s
}

func resolver(t *testing.T, policy string, perField map[string]string) *conflict.Resolver {
	t.Helper()
	r, err := conflict.New(policy, perField, 0)
	if err != nil {
		t.Fatalf("New(%s): %v", policy, err)
	}
	return r
}

func TestUnknownPoliciesAreRejected(t *testing.T) {
	if _, err := conflict.New("coin_flip", nil, 0); err == nil {
		t.Fatal("New accepted an unknown default policy")
	}
	if _, err := conflict.New(conflict.Review, map[string]string{"email": "coin_flip"}, 0); err == nil {
		t.Fatal("New accepted an unknown per-field policy")
	}
}

// A sync that was never told what to do must not pick a winner on its own.
func TestTheDefaultPolicyIsReview(t *testing.T) {
	r := resolver(t, "", nil)

	base := map[string]any{"email": "a@b.c"}
	res := r.Resolve(
		state(map[string]any{"email": "left@b.c"}, base, epoch),
		state(map[string]any{"email": "right@b.c"}, base, epoch),
		fields,
	)
	if res.Action != conflict.Escalate {
		t.Fatalf("action = %s, want escalate", res.Action)
	}
}

// Most polls see records that did not change, and that path must be free.
func TestNeitherSideChanged(t *testing.T) {
	r := resolver(t, conflict.LeftWins, nil)
	same := map[string]any{"email": "a@b.c"}

	res := r.Resolve(state(same, same, epoch), state(same, same, epoch), fields)
	if res.Situation != conflict.Unchanged || res.Action != conflict.Nothing {
		t.Fatalf("situation = %s, action = %s", res.Situation, res.Action)
	}
}

func TestOnlyOneSideChangedIsNotAConflict(t *testing.T) {
	r := resolver(t, conflict.Review, nil)
	base := map[string]any{"email": "a@b.c"}

	left := r.Resolve(
		state(map[string]any{"email": "new@b.c"}, base, epoch),
		state(base, base, epoch),
		fields,
	)
	if left.Situation != conflict.LeftChanged || left.Action != conflict.WriteRight {
		t.Fatalf("left-only: situation = %s, action = %s", left.Situation, left.Action)
	}
	if left.Values["email"] != "new@b.c" {
		t.Fatalf("left-only carried %v", left.Values)
	}

	right := r.Resolve(
		state(base, base, epoch),
		state(map[string]any{"email": "new@b.c"}, base, epoch),
		fields,
	)
	if right.Situation != conflict.RightChanged || right.Action != conflict.WriteLeft {
		t.Fatalf("right-only: situation = %s, action = %s", right.Situation, right.Action)
	}
	// Even under the review policy: review is for conflicts, and one side
	// moving is not one.
	if right.Action == conflict.Escalate {
		t.Fatal("a one-sided change was escalated")
	}
}

func TestSourceOfTruthPolicies(t *testing.T) {
	base := map[string]any{"email": "a@b.c"}
	left := state(map[string]any{"email": "left@b.c"}, base, epoch)
	right := state(map[string]any{"email": "right@b.c"}, base, epoch)

	leftWins := resolver(t, conflict.LeftWins, nil).Resolve(left, right, fields)
	if leftWins.Action != conflict.WriteRight || leftWins.Values["email"] != "left@b.c" {
		t.Fatalf("left_wins gave %s with %v", leftWins.Action, leftWins.Values)
	}

	rightWins := resolver(t, conflict.RightWins, nil).Resolve(left, right, fields)
	if rightWins.Action != conflict.WriteLeft || rightWins.Values["email"] != "right@b.c" {
		t.Fatalf("right_wins gave %s with %v", rightWins.Action, rightWins.Values)
	}
}

func TestNewestWinsPicksTheLaterSide(t *testing.T) {
	r := resolver(t, conflict.NewestWins, nil)
	base := map[string]any{"email": "a@b.c"}

	res := r.Resolve(
		state(map[string]any{"email": "left@b.c"}, base, epoch.Add(time.Minute)),
		state(map[string]any{"email": "right@b.c"}, base, epoch),
		fields,
	)
	if res.Action != conflict.WriteRight || res.Values["email"] != "left@b.c" {
		t.Fatalf("action = %s, values = %v", res.Action, res.Values)
	}
	if !strings.Contains(res.Reason, "left side changed") {
		t.Fatalf("reason does not say which side won: %q", res.Reason)
	}
}

// Believing a millisecond means whichever peer's clock drifts forward wins
// every conflict, forever, and nobody sees it happening.
func TestNewestWinsRefusesToDecideInsideTheClockTolerance(t *testing.T) {
	r, err := conflict.New(conflict.NewestWins, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base := map[string]any{"email": "a@b.c"}

	res := r.Resolve(
		state(map[string]any{"email": "left@b.c"}, base, epoch.Add(time.Second)),
		state(map[string]any{"email": "right@b.c"}, base, epoch),
		fields,
	)
	if res.Action != conflict.Escalate {
		t.Fatalf("action = %s, want escalate", res.Action)
	}
	if !strings.Contains(res.Reason, "tolerance") {
		t.Fatalf("reason does not explain the tolerance: %q", res.Reason)
	}
}

// The case field-level exists for: two people edited different fields of the
// same contact, which on a shared record is most of the time.
func TestFieldLevelMergesNonOverlappingEdits(t *testing.T) {
	r := resolver(t, conflict.FieldLevel, nil)
	base := map[string]any{"email": "a@b.c", "first_name": "Ann", "phone": "123"}

	res := r.Resolve(
		state(map[string]any{"email": "new@b.c", "first_name": "Ann", "phone": "123"}, base, epoch),
		state(map[string]any{"email": "a@b.c", "first_name": "Anna", "phone": "123"}, base, epoch),
		fields,
	)

	if res.Action != conflict.WriteBoth {
		t.Fatalf("action = %s, want write_both (%s)", res.Action, res.Reason)
	}
	if res.RightValues["email"] != "new@b.c" {
		t.Fatalf("the left edit did not reach the right side: %v", res.RightValues)
	}
	if res.LeftValues["first_name"] != "Anna" {
		t.Fatalf("the right edit did not reach the left side: %v", res.LeftValues)
	}
	// Nothing untouched should be written anywhere.
	if _, ok := res.RightValues["phone"]; ok {
		t.Fatal("an unedited field was written")
	}
}

func TestFieldLevelEscalatesAGenuineCollision(t *testing.T) {
	r := resolver(t, conflict.FieldLevel, nil)
	base := map[string]any{"email": "a@b.c", "first_name": "Ann"}

	res := r.Resolve(
		state(map[string]any{"email": "left@b.c", "first_name": "Annabel"}, base, epoch),
		state(map[string]any{"email": "right@b.c", "first_name": "Ann"}, base, epoch),
		fields,
	)

	if res.Action != conflict.Escalate {
		t.Fatalf("action = %s, want escalate", res.Action)
	}
	if !strings.Contains(res.Reason, "email") {
		t.Fatalf("reason does not name the contested field: %q", res.Reason)
	}
	// Applying the mergeable half and escalating the rest would leave the
	// record in a state neither person edited, and make the queued diff a lie
	// about what is on each side.
	if res.LeftValues != nil || res.RightValues != nil {
		t.Fatal("a partial merge was handed back alongside an escalation")
	}
}

// Without a base there is nothing to merge against, and guessing which side
// edited which field is the guess this package exists not to make.
func TestFieldLevelWithoutABaseEscalates(t *testing.T) {
	r := resolver(t, conflict.FieldLevel, nil)

	res := r.Resolve(
		state(map[string]any{"email": "left@b.c"}, nil, epoch),
		state(map[string]any{"email": "right@b.c"}, nil, epoch),
		fields,
	)
	if res.Action != conflict.Escalate {
		t.Fatalf("action = %s, want escalate", res.Action)
	}
	if !strings.Contains(res.Reason, "base") {
		t.Fatalf("reason does not explain the missing base: %q", res.Reason)
	}
}

// Both digests moved but no mapped field did, which means something unmapped
// changed. Writing here would be a write with nothing to say.
func TestFieldLevelWritesNothingWhenNoMappedFieldMoved(t *testing.T) {
	r := resolver(t, conflict.FieldLevel, nil)
	base := map[string]any{"email": "a@b.c"}
	same := map[string]any{"email": "a@b.c"}

	left := state(same, base, epoch)
	right := state(same, base, epoch)
	// Force both digests to look moved without changing a mapped value.
	left.SyncedHash = "stale"
	right.SyncedHash = "stale"

	res := r.Resolve(left, right, fields)
	if res.Action != conflict.Nothing {
		t.Fatalf("action = %s, want nothing (%s)", res.Action, res.Reason)
	}
}

func TestDiffsAreReportedForEveryDisagreement(t *testing.T) {
	r := resolver(t, conflict.Review, nil)
	base := map[string]any{"email": "a@b.c", "first_name": "Ann"}

	// Left edited only the email, right edited only the name. Both sides moved
	// at the record level, but no single field was touched twice.
	res := r.Resolve(
		state(map[string]any{"email": "left@b.c", "first_name": "Ann"}, base, epoch),
		state(map[string]any{"email": "a@b.c", "first_name": "Anna"}, base, epoch),
		fields,
	)

	if len(res.Diffs) != 2 {
		t.Fatalf("got %d diffs, want 2: %+v", len(res.Diffs), res.Diffs)
	}
	// Sorted, so plan output and queued conflicts read the same way twice.
	if res.Diffs[0].Field != "email" || res.Diffs[1].Field != "first_name" {
		t.Fatalf("diffs are not in field order: %+v", res.Diffs)
	}
	if !res.Diffs[0].LeftChanged || res.Diffs[0].RightChanged {
		t.Fatalf("email: left edited it, right did not: %+v", res.Diffs[0])
	}
	if res.Diffs[1].LeftChanged || !res.Diffs[1].RightChanged {
		t.Fatalf("first_name: right edited it, left did not: %+v", res.Diffs[1])
	}
}

// A JSON round-trip turning 1 into 1.0 must not read as a disagreement, or
// every poll produces a conflict out of nothing.
func TestNumericRoundTripsAreNotDisagreements(t *testing.T) {
	r := resolver(t, conflict.Review, nil)
	names := []string{"score"}

	res := r.Resolve(
		conflict.State{Fields: map[string]any{"score": 42}, Hash: "h", SyncedHash: "h"},
		conflict.State{Fields: map[string]any{"score": float64(42)}, Hash: "h", SyncedHash: "h"},
		names,
	)
	if len(res.Diffs) != 0 {
		t.Fatalf("int 42 and float64 42 were reported as different: %+v", res.Diffs)
	}
}

// A resolution must never carry a field the mapping does not own, or the
// engine writes something nobody configured.
func TestResolvedValuesAreLimitedToMappedFields(t *testing.T) {
	r := resolver(t, conflict.LeftWins, nil)
	base := map[string]any{"email": "a@b.c"}

	res := r.Resolve(
		state(map[string]any{"email": "left@b.c", "unmapped": "x"}, base, epoch),
		state(map[string]any{"email": "right@b.c"}, base, epoch),
		fields,
	)
	if _, ok := res.Values["unmapped"]; ok {
		t.Fatalf("an unmapped field reached the resolution: %v", res.Values)
	}
}

func TestPolicyForFallsBackToTheDefault(t *testing.T) {
	r := resolver(t, conflict.NewestWins, map[string]string{"email": conflict.LeftWins})

	if got := r.PolicyFor("email"); got != conflict.LeftWins {
		t.Fatalf("PolicyFor(email) = %s", got)
	}
	if got := r.PolicyFor("phone"); got != conflict.NewestWins {
		t.Fatalf("PolicyFor(phone) = %s", got)
	}
}

func TestEveryPolicyIsValid(t *testing.T) {
	for _, p := range conflict.Policies() {
		if !conflict.IsValidPolicy(p) {
			t.Errorf("%s is listed but not valid", p)
		}
		if _, err := conflict.New(p, nil, 0); err != nil {
			t.Errorf("New(%s): %v", p, err)
		}
	}
	if conflict.IsValidPolicy("coin_flip") {
		t.Error("coin_flip is valid")
	}
}
