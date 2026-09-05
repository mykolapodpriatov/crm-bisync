package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/connector/fake"
	"crm-bisync/internal/mapping"
	"crm-bisync/internal/store"
)

// serverErrorEvery makes every nth call to a peer fail transiently.
func serverErrorEvery(n int) fake.Faults {
	return fake.Faults{ServerErrorEvery: n}
}

// rejectValue makes a peer refuse one value outright, the way an
// account-level validation rule does.
func rejectValue(field, value string) fake.Faults {
	return fake.Faults{RejectField: field, RejectValue: value}
}

func TestCreateFlowsToTheOtherSide(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "Ann@Example.com", "firstname": "Ann",
	})

	h.cycle()

	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("right has %d records, want 1", got)
	}
	// The transform ran on the way through: the value stored on the far side
	// is the normalised one, which is also what identity matching compares.
	if got := h.field(h.right, "email"); got != "ann@example.com" {
		t.Fatalf("right email = %v, want ann@example.com", got)
	}
	if got := h.field(h.right, "first_name"); got != "Ann" {
		t.Fatalf("right first_name = %v, want Ann", got)
	}
	if h.engine.Stats().Created != 1 {
		t.Fatalf("Created = %d, want 1", h.engine.Stats().Created)
	}
}

// The assertion the whole repository is for. After the last external edit the
// engine has to stop writing, and it has to stop on its own.
func TestWritesStopAfterTheLastEdit(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})

	h.cycle()
	settled := h.writes()

	h.cycles(6)

	if got := h.writes(); got != settled {
		t.Fatalf("%d further writes happened with nothing to sync (%d then %d)",
			got-settled, settled, got)
	}
	if h.left.Count("contact") != 1 || h.right.Count("contact") != 1 {
		t.Fatalf("record counts drifted: left %d, right %d",
			h.left.Count("contact"), h.right.Count("contact"))
	}
}

// A record already on both sides must be linked rather than created again.
func TestExistingRecordsAreMatchedNotDuplicated(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "first_name": "Ann"})

	h.cycles(3)

	if h.left.Count("contact") != 1 || h.right.Count("contact") != 1 {
		t.Fatalf("matching created a duplicate: left %d, right %d",
			h.left.Count("contact"), h.right.Count("contact"))
	}
	link, ok, err := h.engine.Link(refFor("left", h.left.Records("contact")[0].RemoteID))
	if err != nil || !ok {
		t.Fatalf("the two records were never linked: %v %v", ok, err)
	}
	if link.Right.Connector != "right" {
		t.Fatalf("link points at %s", link.Right)
	}
}

func TestUpdatesPropagate(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	rec := h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()

	h.left.ExternalUpsert("contact", rec.RemoteID, map[string]any{"firstname": "Annabel"})
	h.cycle()

	if got := h.field(h.right, "first_name"); got != "Annabel" {
		t.Fatalf("right first_name = %v, want Annabel", got)
	}
}

func TestUpdatesPropagateBothWays(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()

	remote := h.right.Records("contact")[0]
	h.right.ExternalUpsert("contact", remote.RemoteID, map[string]any{"first_name": "Anna"})
	h.cycle()

	if got := h.field(h.left, "firstname"); got != "Anna" {
		t.Fatalf("left firstname = %v, want Anna", got)
	}
}

// Both sides edited the same field. newest_wins with a two-second tolerance
// cannot tell which came first, so nothing is written and a person is asked.
func TestSimultaneousEditsGoToReview(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	left := h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()

	right := h.right.Records("contact")[0]
	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "Left"})
	h.right.ExternalUpsert("contact", right.RemoteID, map[string]any{"first_name": "Right"})

	h.cycle()

	reviews := h.reviews()
	if len(reviews) == 0 {
		t.Fatal("simultaneous edits were resolved silently instead of queued")
	}
	if !strings.Contains(reviews[0].Reason, "tolerance") {
		t.Fatalf("the queued reason does not explain itself: %q", reviews[0].Reason)
	}
	// Neither side may have been overwritten while the question is open.
	if h.field(h.left, "firstname") != "Left" {
		t.Fatalf("left was overwritten: %v", h.field(h.left, "firstname"))
	}
	if h.field(h.right, "first_name") != "Right" {
		t.Fatalf("right was overwritten: %v", h.field(h.right, "first_name"))
	}
}

// Two peer records answering to one email is exactly the case where guessing
// merges two customers, so nothing is written.
func TestAmbiguousMatchesGoToReview(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "first_name": "One"})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "first_name": "Two"})
	h.cycle()

	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	before := h.right.Count("contact")
	h.cycle()

	if got := h.right.Count("contact"); got != before {
		t.Fatalf("an ambiguous match wrote anyway: %d records, was %d", got, before)
	}
	found := false
	for _, item := range h.reviews() {
		if strings.Contains(item.Reason, "match on") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no ambiguity was queued: %+v", h.reviews())
	}
}

func TestDeletePropagatesAndStops(t *testing.T) {
	caps := connector.Caps{
		NativeIdempotency: true, Webhooks: true, SoftDelete: true, ModifiedAtIsExact: true,
	}
	h := newHarness(t, harnessOptions{caps: &caps})

	rec := h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()
	if h.right.Count("contact") != 1 {
		t.Fatalf("setup: right has %d records", h.right.Count("contact"))
	}

	h.left.ExternalDelete("contact", rec.RemoteID)
	h.cycle()

	if got := h.right.Count("contact"); got != 0 {
		t.Fatalf("right still has %d records after the delete", got)
	}

	// The far side's own delete event finds no link and stops there, so the
	// deletes do not bounce. One round trip, always.
	deletes := h.left.Calls("delete") + h.right.Calls("delete")
	h.cycles(4)
	if got := h.left.Calls("delete") + h.right.Calls("delete"); got != deletes {
		t.Fatalf("deletes kept bouncing: %d then %d", deletes, got)
	}
}

// A one-way sync must never write to its source, whatever happens on the far
// side. This is also what stops our own writes coming back on such a sync.
func TestAOneWaySyncNeverWritesBack(t *testing.T) {
	h := newHarness(t, harnessOptions{direction: mapping.LeftToRight})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()

	remote := h.right.Records("contact")[0]
	h.right.ExternalUpsert("contact", remote.RemoteID, map[string]any{"first_name": "Edited on the far side"})
	h.cycles(3)

	if got := h.field(h.left, "firstname"); got != "Ann" {
		t.Fatalf("a one-way sync wrote back to its source: %v", got)
	}
	if h.left.Calls("upsert") != 0 {
		t.Fatalf("%d writes reached the source of a one-way sync", h.left.Calls("upsert"))
	}
}

func TestATransientFailureIsRetriedAndThenSucceeds(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	ctx := context.Background()
	results, err := h.engine.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	// The fault is injected after the read, so it lands on the write and not
	// on the poll. A poll that fails is a different scenario with a different
	// answer: the watermark simply does not move.
	h.right.SetFaults(serverErrorEvery(1))
	h.engine.Drain(ctx)

	if h.right.Count("contact") != 0 {
		t.Fatal("the injected failure did not fire")
	}
	if h.engine.Stats().Retried == 0 {
		t.Fatal("the failure was not retried")
	}

	// The mark must not have moved past work that has not landed.
	if _, ok, _ := h.engine.Watermark("left", "contact"); ok {
		t.Fatal("the watermark advanced past a record still being retried")
	}

	// Let the peer recover, wait out the backoff, and drain again.
	h.right.SetFaults(fake.Faults{})
	h.clock.Advance(time.Minute)
	h.engine.Drain(ctx)

	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("the retry did not land: right has %d records", got)
	}
	for _, r := range results {
		if err := r.Commit(ctx, h.engine); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	if _, ok, _ := h.engine.Watermark("left", "contact"); !ok {
		t.Fatal("the watermark did not advance once the work landed")
	}
}

// A validation rule in the customer's CRM account is not something a retry can
// fix, so it goes straight to the dead-letter queue with the reason attached.
func TestAPermanentRejectionIsDeadLetteredImmediately(t *testing.T) {
	h := newHarness(t, harnessOptions{
		rightFaults: rejectValue("email", "blocked@example.com"),
	})
	h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "blocked@example.com", "firstname": "Ann",
	})

	h.cycle()

	items := h.dlq()
	if len(items) != 1 {
		t.Fatalf("got %d dead-lettered items, want 1", len(items))
	}
	if !strings.Contains(items[0].Reason, "validation rule") {
		t.Fatalf("the reason does not explain itself: %q", items[0].Reason)
	}
	if items[0].Attempts != 1 {
		t.Fatalf("a permanent failure was attempted %d times", items[0].Attempts)
	}
}

func TestExhaustedRetriesAreDeadLettered(t *testing.T) {
	h := newHarness(t, harnessOptions{maxAttempts: 3})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	ctx := context.Background()
	if _, err := h.engine.PollAll(ctx); err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	h.right.SetFaults(serverErrorEvery(1))
	for i := 0; i < 6; i++ {
		h.engine.Drain(ctx)
		h.clock.Advance(time.Minute)
	}

	items := h.dlq()
	if len(items) != 1 {
		t.Fatalf("got %d dead-lettered items, want 1", len(items))
	}
	if items[0].Attempts != 3 {
		t.Fatalf("attempts = %d, want the configured 3", items[0].Attempts)
	}
	if h.engine.Stats().DeadLettered != 1 {
		t.Fatalf("DeadLettered = %d", h.engine.Stats().DeadLettered)
	}
}

// A replayed item starts its attempt budget again, because whoever replayed it
// has usually changed something.
func TestReplayingADeadLetteredItem(t *testing.T) {
	h := newHarness(t, harnessOptions{maxAttempts: 2})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	ctx := context.Background()
	if _, err := h.engine.PollAll(ctx); err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	h.right.SetFaults(serverErrorEvery(1))
	for i := 0; i < 4; i++ {
		h.engine.Drain(ctx)
		h.clock.Advance(time.Minute)
	}
	items := h.dlq()
	if len(items) != 1 {
		t.Fatalf("setup: %d dead-lettered items", len(items))
	}

	h.right.SetFaults(fake.Faults{})
	if err := h.engine.Replay(items[0].ID); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	h.engine.Drain(ctx)

	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("the replay did not land: right has %d records", got)
	}
	if len(h.dlq()) != 0 {
		t.Fatal("the replayed item stayed in the dead-letter queue")
	}
}

func TestValidateRejectsAMappingTheSchemaDoesNotSupport(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	faults := fake.Faults{}
	faults.RenameField.Kind = "contact"
	faults.RenameField.From = "first_name"
	faults.RenameField.To = "given_name"
	faults.RenameField.AfterCalls = 0
	h.right.SetFaults(faults)

	err := h.engine.Validate(context.Background())
	if err == nil {
		t.Fatal("Validate accepted a mapping naming a field the peer no longer has")
	}
	if !strings.Contains(err.Error(), "first_name") {
		t.Fatalf("the error does not name the field: %v", err)
	}
}

func refFor(connectorName, remoteID string) store.Ref {
	return store.Ref{Connector: connectorName, Kind: "contact", RemoteID: remoteID}
}

// A record whose linked counterpart vanished must not be resurrected.
//
// This is what reconcile's read-before-write turns into if a 404 is read as
// "something is wrong, recreate" instead of "the far side deleted this":
// an update already queued for the near side, processed after the far side
// has genuinely been deleted, brings the deleted contact back to life on the
// side that legitimately removed it. Found by the chaos harness's "no loss"
// invariant on the very first real run.
func TestReconcileDeletesRatherThanResurrectsWhenFarSideVanishes(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	left := h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})
	h.cycle()
	right := h.right.Records("contact")[0]

	// Queue an update on the near side, and delete the far side's record
	// before that update is ever processed. Both are real edits an operator
	// could make within the same moment.
	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "Annabel"})
	h.right.ExternalDelete("contact", right.RemoteID)

	h.cycles(3)

	if h.right.Count("contact") != 0 {
		t.Fatalf("the deleted record was resurrected on the right: %d records", h.right.Count("contact"))
	}
	if h.left.Count("contact") != 0 {
		t.Fatalf("the delete did not propagate to the left: %d records", h.left.Count("contact"))
	}
}

// An event describing an old value must not overwrite a newer one that some
// other event already applied for the same record.
//
// This is what a redelivered webhook, or a poll re-reading its overlap
// window, looks like once a different event for the same pair has already
// landed. Found by the chaos harness: two genuinely concurrent edits kept
// alternating forever because each side's stale, already-applied event kept
// being reprocessed as if it were new.
func TestAStaleEventDoesNotOverwriteANewerOne(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	left := h.left.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "firstname": "Ann",
	})
	h.cycle()
	// h.cycle syncs by polling, not by webhook delivery, so the webhook the
	// initial create queued is still sitting there; clear it before the
	// webhooks under test are queued.
	h.left.DrainWebhooks()

	// The webhook for this edit is captured now, with "Old" embedded in it,
	// and deliberately held back rather than delivered immediately.
	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "Old"})
	stale := h.left.DrainWebhooks()
	if len(stale) != 1 {
		t.Fatalf("setup: got %d webhooks, want 1", len(stale))
	}

	// A second, later edit supersedes it, and this one is allowed to sync
	// normally.
	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "New"})
	h.cycle()

	if got := h.field(h.right, "first_name"); got != "New" {
		t.Fatalf("the newer edit did not land: %v", got)
	}

	// The stale webhook, describing "Old", arrives only now.
	h.post(stale[0].Request("/webhook/left"))
	h.engine.Drain(context.Background())

	if got := h.field(h.right, "first_name"); got != "New" {
		t.Fatalf("a stale, already-superseded event overwrote the newer value: got %v, want New", got)
	}
	if got := h.field(h.left, "firstname"); got != "New" {
		t.Fatalf("the stale event changed the source of truth itself: got %v, want New", got)
	}
}
