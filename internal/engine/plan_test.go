package engine_test

import (
	"context"
	"strings"
	"testing"

	"crm-bisync/internal/connector/fake"
	"crm-bisync/internal/engine"
	"crm-bisync/internal/store"
)

func TestPlanReportsWritesWithoutMakingThem(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	report, err := engine.Plan(context.Background(), h.opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if len(report.Changes) != 1 {
		t.Fatalf("got %d planned changes, want 1: %+v", len(report.Changes), report.Changes)
	}
	change := report.Changes[0]
	if change.Action != engine.ActionCreate {
		t.Fatalf("action = %q, want create", change.Action)
	}
	if change.Target.Connector != "right" {
		t.Fatalf("target = %s", change.Target)
	}

	// The whole point: nothing happened.
	if h.right.Calls("upsert") != 0 {
		t.Fatalf("a dry run made %d writes", h.right.Calls("upsert"))
	}
	if h.right.Count("contact") != 0 {
		t.Fatal("a dry run created a record")
	}
	if n, _ := h.store.Len(store.CollLinks); n != 0 {
		t.Fatalf("a dry run wrote %d links into the real store", n)
	}
}

// A dry run must not consume the real engine's state either. Echo suppression
// takes origin entries, and a plan that ate them would make the next real pass
// process its own writes as foreign changes.
func TestPlanLeavesTheRealStateAlone(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()

	before := map[string]int{}
	for _, collection := range store.AllCollections {
		n, err := h.store.Len(collection)
		if err != nil {
			t.Fatalf("Len(%s): %v", collection, err)
		}
		before[collection] = n
	}

	if _, err := engine.Plan(context.Background(), h.opts); err != nil {
		t.Fatalf("Plan: %v", err)
	}

	for collection, want := range before {
		got, err := h.store.Len(collection)
		if err != nil {
			t.Fatalf("Len(%s): %v", collection, err)
		}
		if got != want {
			t.Errorf("%s holds %d entries after a dry run, was %d", collection, got, want)
		}
	}
}

// Plan is worth trusting only if it is the same program. This runs it, then
// runs the engine for real, and requires the two to agree.
func TestPlanAgreesWithTheRun(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "bo@example.com", "firstname": "Bo"})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "cass@example.com", "first_name": "Cass"})

	report, err := engine.Plan(context.Background(), h.opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	planned := map[string]int{}
	for _, c := range report.Changes {
		planned[c.Target.Connector]++
	}

	h.cycle()

	actual := map[string]int{
		"left":  h.left.Calls("upsert"),
		"right": h.right.Calls("upsert"),
	}
	for side, want := range planned {
		if actual[side] != want {
			t.Errorf("plan said %d writes to %s, the run made %d", want, side, actual[side])
		}
	}
	if h.left.Count("contact") != 3 || h.right.Count("contact") != 3 {
		t.Fatalf("after the run: left %d, right %d, want 3 each",
			h.left.Count("contact"), h.right.Count("contact"))
	}
}

// A plan only shows fields that would land. One showing a read-only or
// direction-narrowed field lies about the single thing it is read for.
func TestPlanOmitsFieldsThatWouldNotBeWritten(t *testing.T) {
	h := newHarness(t, harnessOptions{direction: "right_to_left"})
	h.right.ExternalUpsert("contact", "", map[string]any{
		"email": "ann@example.com", "first_name": "Ann",
	})

	report, err := engine.Plan(context.Background(), h.opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(report.Changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(report.Changes))
	}
	for _, f := range report.Changes[0].Fields {
		if f.Field != "email" && f.Field != "first_name" {
			t.Fatalf("unexpected field in the plan: %s", f.Field)
		}
	}
}

func TestPlanReportsQueuedDecisions(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Left"})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "first_name": "Right"})

	report, err := engine.Plan(context.Background(), h.opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(report.Reviews) != 1 {
		t.Fatalf("got %d queued decisions, want 1: %+v", len(report.Reviews), report.Reviews)
	}
	if len(report.Changes) != 0 {
		t.Fatalf("a plan that needs a decision also planned %d writes", len(report.Changes))
	}
	if report.Empty() {
		t.Fatal("a plan with a queued decision reported itself as empty")
	}
}

// One disagreement is one decision, however many events surface it. Both
// sides' events reach the escalation, and two rows would mean two people
// resolving one thing, or one person resolving it twice.
func TestOneConflictIsOneReviewItem(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	left := h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycle()
	right := h.right.Records("contact")[0]

	h.left.ExternalUpsert("contact", left.RemoteID, map[string]any{"firstname": "Left"})
	h.right.ExternalUpsert("contact", right.RemoteID, map[string]any{"first_name": "Right"})

	h.cycles(3)

	if got := len(h.reviews()); got != 1 {
		for _, item := range h.reviews() {
			t.Logf("  %s %s", item.ID, item.Ref)
		}
		t.Fatalf("one disagreement produced %d review items", got)
	}
}

func TestPlanOfASettledPairIsEmpty(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	h.cycles(2)

	report, err := engine.Plan(context.Background(), h.opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !report.Empty() {
		t.Fatalf("a settled pair still planned work: %+v", report.Changes)
	}
}

func TestDoctorPassesOnAHealthySetup(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	d := h.engine.Doctor(context.Background())
	if !d.OK() {
		t.Fatalf("doctor found problems in a healthy setup: %+v", d.Failures())
	}
	if len(d.Checks) == 0 {
		t.Fatal("doctor checked nothing")
	}
}

// The 3am rename, found at a moment when somebody is looking.
func TestDoctorReportsARenamedField(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	faults := fake.Faults{}
	faults.RenameField.Kind = "contact"
	faults.RenameField.From = "first_name"
	faults.RenameField.To = "given_name"
	h.right.SetFaults(faults)

	d := h.engine.Doctor(context.Background())
	if d.OK() {
		t.Fatal("doctor passed a mapping naming a field the peer no longer has")
	}

	joined := failureText(d)
	if !strings.Contains(joined, "first_name") {
		t.Fatalf("the finding does not name the field: %s", joined)
	}
}

// An unreachable peer is one finding, not two: the mapping check is skipped
// rather than reporting the same broken token again under another name.
func TestDoctorReportsAnUnreachablePeerOnce(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.right.SetFaults(fake.Faults{ServerErrorEvery: 1})

	d := h.engine.Doctor(context.Background())
	if d.OK() {
		t.Fatal("doctor passed an unreachable peer")
	}

	connectorFailures := 0
	mappingFailures := 0
	for _, c := range d.Failures() {
		switch c.Name {
		case "connector":
			connectorFailures++
		case "mapping":
			mappingFailures++
		}
	}
	if connectorFailures == 0 {
		t.Fatal("the unreachable peer was not reported")
	}
	if mappingFailures != 0 {
		t.Fatalf("one broken peer produced %d mapping findings as well", mappingFailures)
	}
}

func TestDoctorReportsQueuedWork(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Left"})
	h.right.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "first_name": "Right"})
	h.cycle()

	d := h.engine.Doctor(context.Background())
	if d.OK() {
		t.Fatal("doctor passed with a decision waiting")
	}
	if !strings.Contains(failureText(d), "review queue") {
		t.Fatalf("the review queue was not reported: %s", failureText(d))
	}
}

func failureText(d engine.Diagnosis) string {
	var b strings.Builder
	for _, c := range d.Failures() {
		b.WriteString(c.Name + " " + c.Subject + ": " + c.Detail + "\n")
	}
	return b.String()
}
