package driver_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/driver"
	"crm-bisync/internal/engine"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func TestUnknownDriverSaysWhatIsAvailable(t *testing.T) {
	_, err := driver.Build(config.Connector{Name: "hubspot", Driver: "salesforce"}, clock.NewManual(epoch))
	if err == nil {
		t.Fatal("Build accepted a driver that does not exist")
	}
	// The message has to be actionable: somebody reading it is usually one
	// typo or one missing build tag away from being right.
	for _, want := range []string{"hubspot", "salesforce", "fake"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error is missing %q: %v", want, err)
		}
	}
}

func TestFakeDriverReadsItsFixture(t *testing.T) {
	conn, err := driver.Build(config.Connector{
		Name:    "hubspot",
		Driver:  "fake",
		BaseDir: filepath.Join("..", "..", "examples"),
		Options: map[string]string{"file": "hubspot.json"},
	}, clock.NewManual(epoch))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	schema, err := conn.Describe(context.Background(), "contact")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if _, ok := schema.Field("email"); !ok {
		t.Fatalf("the fixture's schema was not loaded: %v", schema.Names())
	}

	page, err := conn.ListChanged(context.Background(), "contact", time.Time{}, "")
	if err != nil {
		t.Fatalf("ListChanged: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("got %d seeded records, want 3", len(page.Records))
	}
}

// A fixture path is resolved against the configuration file, so that running
// the binary from somewhere else does not change what it reads.
func TestFixturePathsResolveAgainstTheConfig(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "examples", "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, c := range cfg.Connectors {
		if c.BaseDir == "" {
			t.Fatalf("connector %q did not learn where its configuration lives", c.Name)
		}
	}
	if _, err := driver.BuildAll(cfg, clock.NewManual(epoch)); err != nil {
		t.Fatalf("BuildAll from a different working directory: %v", err)
	}
}

// The shipped example is what the README shows, so a change that quietly makes
// it produce something else should fail here rather than in a screenshot.
func TestShippedExampleProducesTheDocumentedPlan(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "examples", "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	c := clock.NewManual(epoch)
	connectors, err := driver.BuildAll(cfg, c)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}

	report, err := engine.Plan(context.Background(), engine.Options{
		Config:     cfg,
		Store:      store.NewMem(c),
		Clock:      c,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Connectors: connectors,
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("the example plan reported errors: %v", report.Errors)
	}

	creates := 0
	for _, change := range report.Changes {
		if change.Action != engine.ActionCreate {
			t.Errorf("unexpected %s in the example plan: %+v", change.Action, change)
		}
		creates++
	}
	if creates != 3 {
		t.Fatalf("the example plans %d creates, want 3", creates)
	}
	if len(report.Reviews) != 1 {
		t.Fatalf("the example queues %d decisions, want 1", len(report.Reviews))
	}
	if len(report.DeadLetters) != 0 {
		t.Fatalf("the example would fail %d writes", len(report.DeadLetters))
	}

	// The read-only left-hand field must not appear in a write going left.
	for _, change := range report.Changes {
		if change.Target.Connector != "hubspot" {
			continue
		}
		for _, f := range change.Fields {
			if f.Field == "created_at" {
				t.Error("a left_to_right field was planned for a write to the left")
			}
		}
	}
}
