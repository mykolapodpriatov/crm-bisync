package engine_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/conflict"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/connector/fake"
	"crm-bisync/internal/engine"
	"crm-bisync/internal/mapping"
	"crm-bisync/internal/store"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

// harness is two fake CRMs with an engine between them.
type harness struct {
	t      *testing.T
	clock  *clock.Manual
	left   *fake.Fake
	right  *fake.Fake
	engine *engine.Engine
	store  store.Store
}

type harnessOptions struct {
	direction    string
	policy       string
	perField     map[string]string
	onAmbiguous  string
	leftFaults   fake.Faults
	rightFaults  fake.Faults
	caps         *connector.Caps
	maxAttempts  int
	identityKeys []string
}

func leftSchema() map[string]connector.Schema {
	return map[string]connector.Schema{
		"contact": {Kind: "contact", Fields: []connector.FieldSpec{
			{Name: "email"},
			{Name: "firstname"},
		}},
	}
}

func rightSchema() map[string]connector.Schema {
	return map[string]connector.Schema{
		"contact": {Kind: "contact", Fields: []connector.FieldSpec{
			{Name: "email"},
			{Name: "first_name"},
		}},
	}
}

func newHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()

	if opts.direction == "" {
		opts.direction = mapping.Bidirectional
	}
	if opts.policy == "" {
		opts.policy = conflict.NewestWins
	}
	if opts.onAmbiguous == "" {
		opts.onAmbiguous = "review"
	}
	if opts.maxAttempts == 0 {
		opts.maxAttempts = 4
	}
	if opts.identityKeys == nil {
		opts.identityKeys = []string{"email"}
	}
	caps := connector.Caps{
		NativeIdempotency: true,
		Webhooks:          true,
		ETags:             true,
		ModifiedAtIsExact: true,
	}
	if opts.caps != nil {
		caps = *opts.caps
	}

	c := clock.NewManual(epoch)
	h := &harness{
		t:     t,
		clock: c,
		store: store.NewMem(c),
		left: fake.New(fake.Options{
			Name: "left", Clock: c, Caps: caps, Secret: "left-secret",
			Schemas: leftSchema(), Faults: opts.leftFaults, Seed: 1,
		}),
		right: fake.New(fake.Options{
			Name: "right", Clock: c, Caps: caps, Secret: "right-secret",
			Schemas: rightSchema(), Faults: opts.rightFaults, Seed: 2,
		}),
	}

	cfg := &config.Config{
		LogLevel:    "error",
		Listen:      "127.0.0.1:0",
		Workers:     2,
		MaxAttempts: opts.maxAttempts,
		Connectors: []config.Connector{
			{
				Name: "left", Driver: "fake",
				EchoWindow: config.Duration(5 * time.Minute),
				Overlap:    config.Duration(time.Minute),
				RateLimit:  config.RateLimit{RatePerSec: 10000, Burst: 10000},
			},
			{
				Name: "right", Driver: "fake",
				EchoWindow: config.Duration(5 * time.Minute),
				Overlap:    config.Duration(time.Minute),
				RateLimit:  config.RateLimit{RatePerSec: 10000, Burst: 10000},
			},
		},
		Syncs: []config.Sync{{
			Kind:      "contact",
			Left:      "left",
			Right:     "right",
			Direction: opts.direction,
			Identity: config.Identity{
				Keys:        opts.identityKeys,
				OnAmbiguous: opts.onAmbiguous,
			},
			Conflict: config.Conflict{Default: opts.policy, PerField: opts.perField},
			Fields: []config.FieldPair{
				{Canonical: "email", Left: "email", Right: "email", Transform: mapping.TransformEmailNormalize},
				{Canonical: "first_name", Left: "firstname", Right: "first_name"},
			},
		}},
	}

	e, err := engine.New(engine.Options{
		Config: cfg,
		Store:  h.store,
		Clock:  c,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Connectors: map[string]connector.Connector{
			"left":  h.left,
			"right": h.right,
		},
		Seed: 7,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	h.engine = e
	return h
}

// cycle runs one full pass: poll both peers, process, advance marks.
func (h *harness) cycle() {
	h.t.Helper()
	if err := h.engine.Cycle(context.Background()); err != nil {
		h.t.Fatalf("Cycle: %v", err)
	}
	// Peers stamp their records from the same clock, so without a nudge every
	// record in a scenario shares one timestamp and the ordering the engine
	// sees stops resembling anything real.
	h.clock.Advance(time.Second)
}

// cycles runs n passes, which is how the termination assertions are made.
func (h *harness) cycles(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.cycle()
	}
}

// syncName is the single sync this harness configures.
func (h *harness) syncName() string {
	h.t.Helper()
	return h.engine.Syncs()[0].Name
}

// writes counts every upsert either peer has been asked for, which is the
// number the termination tests watch.
func (h *harness) writes() int {
	return h.left.Calls("upsert") + h.right.Calls("upsert")
}

func (h *harness) dlq() []store.QueueItem {
	h.t.Helper()
	items, err := h.engine.DeadLetters(0)
	if err != nil {
		h.t.Fatalf("DeadLetters: %v", err)
	}
	return items
}

func (h *harness) reviews() []store.QueueItem {
	h.t.Helper()
	items, err := h.engine.Reviews(0)
	if err != nil {
		h.t.Fatalf("Reviews: %v", err)
	}
	return items
}

// field reads one field of the single record on a peer.
func (h *harness) field(f *fake.Fake, name string) any {
	h.t.Helper()
	records := f.Records("contact")
	if len(records) != 1 {
		h.t.Fatalf("expected exactly one record on %s, got %d", f.Name(), len(records))
	}
	return records[0].Fields[name]
}
