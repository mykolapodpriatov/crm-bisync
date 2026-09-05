package report_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crm-bisync/internal/engine"
	"crm-bisync/internal/report"
	"crm-bisync/internal/store"
)

// update rewrites the golden files. Run with -update after deliberately
// changing the output, and read the diff before committing it: these files are
// the only place the shape of the output is reviewed.
var update = flag.Bool("update", false, "rewrite the golden files")

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func golden(t *testing.T, name string, got string) {
	t.Helper()

	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("output changed.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

func ref(connector, id string) store.Ref {
	return store.Ref{Connector: connector, Kind: "contact", RemoteID: id}
}

func TestPlanOutput(t *testing.T) {
	r := &engine.PlanReport{
		Changes: []engine.PlannedChange{
			{
				Sync:   "contact:hubspot-twenty",
				Action: engine.ActionCreate,
				Source: ref("hubspot", "hs-2"),
				Target: ref("twenty", "planned-1"),
				Fields: []engine.NamedVal{
					{Field: "email", Value: "bo@northwind.test"},
					{Field: "first_name", Value: "Bo"},
					{Field: "score", Value: 42},
					{Field: "phone", Value: nil},
				},
			},
			{
				Sync:   "contact:hubspot-twenty",
				Action: engine.ActionUpdate,
				Source: ref("twenty", "tw-4"),
				Target: ref("hubspot", "hs-7"),
				Fields: []engine.NamedVal{
					{Field: "first_name", Value: "Cass"},
				},
			},
			{
				Sync:   "contact:hubspot-twenty",
				Action: engine.ActionDelete,
				Source: ref("hubspot", "hs-9"),
				Target: ref("twenty", "tw-9"),
			},
		},
		Reviews: []store.QueueItem{{
			ID:        "review-abc",
			Ref:       ref("hubspot", "hs-1"),
			Reason:    "both sides changed 1s apart, inside the 2s clock tolerance, so which is newer is not knowable",
			CreatedAt: epoch,
		}},
		DeadLetters: []store.QueueItem{{
			ID:        "dlq-1",
			Ref:       ref("twenty", "tw-3"),
			Reason:    `property "email" failed a validation rule`,
			CreatedAt: epoch,
			Attempts:  6,
		}},
	}

	var out bytes.Buffer
	report.Plan(&out, r)
	golden(t, "plan.txt", out.String())
}

func TestEmptyPlanOutput(t *testing.T) {
	var out bytes.Buffer
	report.Plan(&out, &engine.PlanReport{})
	golden(t, "plan-empty.txt", out.String())
}

// A plan that could not read one peer is still worth most of what it cost, so
// it prints what it has and says plainly that it is incomplete.
func TestIncompletePlanOutput(t *testing.T) {
	r := &engine.PlanReport{
		Errors: []string{"polling twenty for contact: twenty.list: auth: bad credentials"},
	}

	var out bytes.Buffer
	report.Plan(&out, r)
	golden(t, "plan-incomplete.txt", out.String())
}

func TestDoctorOutput(t *testing.T) {
	d := engine.Diagnosis{Checks: []engine.Check{
		{Name: "store", Subject: "./state", OK: true, Detail: "readable and writable"},
		{Name: "connector", Subject: "hubspot/contact", OK: true, Detail: "4 fields"},
		{Name: "connector", Subject: "twenty/contact", OK: false, Detail: "twenty.describe: auth: token expired"},
		{Name: "mapping", Subject: "contact:hubspot-twenty", OK: true, Detail: "4 fields, bidirectional"},
		{Name: "watermark", Subject: "hubspot/contact", OK: true, Detail: "3m0s behind"},
		{Name: "queue", Subject: "dead-letter queue", OK: false, Detail: "2 items that exhausted their attempts"},
	}}

	var out bytes.Buffer
	report.Doctor(&out, d)
	golden(t, "doctor.txt", out.String())
}

func TestHealthyDoctorOutput(t *testing.T) {
	d := engine.Diagnosis{Checks: []engine.Check{
		{Name: "store", Subject: "./state", OK: true, Detail: "readable and writable"},
		{Name: "queue", Subject: "review queue", OK: true, Detail: "empty"},
	}}

	var out bytes.Buffer
	report.Doctor(&out, d)
	golden(t, "doctor-healthy.txt", out.String())
}

func TestDoctorWithNothingConfigured(t *testing.T) {
	var out bytes.Buffer
	report.Doctor(&out, engine.Diagnosis{})
	golden(t, "doctor-empty.txt", out.String())
}

// The JSON form is what a script reads, so it has to stay stable too.
func TestPlanJSONIsStable(t *testing.T) {
	r := &engine.PlanReport{Changes: []engine.PlannedChange{{
		Sync:   "contact:hubspot-twenty",
		Action: engine.ActionCreate,
		Source: ref("hubspot", "hs-2"),
		Target: ref("twenty", "planned-1"),
		Fields: []engine.NamedVal{{Field: "email", Value: "bo@northwind.test"}},
	}}}

	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	golden(t, "plan.json", string(encoded)+"\n")
}
