// Package chaos runs the engine against two misbehaving CRMs and reports what
// happened.
//
// Everything else in this repository is a claim. This is the evidence. A sync
// that works when nothing goes wrong is not interesting; the question is
// whether it still converges when webhooks are redelivered and reordered, when
// the peer returns 429 halfway through a batch, when its clock is wrong and its
// timestamps are coarse, and when the process is restarted in the middle.
//
// The harness is a normal package rather than a test file because the same
// scenarios are exposed as `crm-bisync chaos`, so that a change to the engine
// can be argued with by running it rather than by reading it.
package chaos

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sort"
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

// Options configures one run.
type Options struct {
	// Seed makes a run reproducible. Every failure prints it, because a
	// property test whose failures cannot be replayed is a rumour.
	Seed int64
	// Rounds is how many batches of edits are applied.
	Rounds int
	// OpsPerRound is how many edits each batch makes.
	OpsPerRound int
	// Faults is the misbehaviour both peers exhibit while edits are running.
	// It is switched off before the run is allowed to settle, because the
	// question is whether the engine converges once the world stops fighting
	// it, not whether it converges while a peer is still returning 503.
	Faults fake.Faults
	// Restart rebuilds the engine halfway through, discarding whatever was in
	// its queue. State that survives is exactly the state on disk, which is
	// what a real restart leaves behind.
	Restart bool
	// Policy is the conflict policy under test.
	Policy string
	// SettleRounds is how many quiet passes are run after the edits stop.
	SettleRounds int
	// EditAdvance and SettleAdvance are how far the clock moves per pass.
	// They matter: a retry waits out a backoff, and a clock that barely moves
	// would leave the run finishing with work that was never given its turn,
	// which would look exactly like a convergence failure.
	EditAdvance   time.Duration
	SettleAdvance time.Duration
	// Logger is where the engine's own logging goes. Nil discards it.
	Logger *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.Rounds <= 0 {
		o.Rounds = 6
	}
	if o.OpsPerRound <= 0 {
		o.OpsPerRound = 4
	}
	if o.Policy == "" {
		o.Policy = conflict.LeftWins
	}
	if o.SettleRounds <= 0 {
		o.SettleRounds = 8
	}
	if o.EditAdvance <= 0 {
		o.EditAdvance = 5 * time.Second
	}
	if o.SettleAdvance <= 0 {
		o.SettleAdvance = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Result is what a run produced.
type Result struct {
	Options Options
	Stats   engine.Stats

	// Live is every address the edit stream left in existence, sorted.
	Live []string
	// Deleted is every address it removed, sorted.
	Deleted []string

	// WritesDuringEdits and WritesWhileSettling separate the work the edits
	// caused from the work the engine invented afterwards. The second number
	// reaching zero is the termination property.
	WritesDuringEdits   int
	WritesWhileSettling int
	// SettledAfter is how many quiet rounds it took for writes to stop.
	SettledAfter int

	Left        *fake.Fake
	Right       *fake.Fake
	Store       store.Store
	Reviews     []store.QueueItem
	DeadLetters []store.QueueItem
}

// runner holds the machinery of one run.
type runner struct {
	opts   Options
	rnd    *rand.Rand
	clock  *clock.Manual
	store  store.Store
	left   *fake.Fake
	right  *fake.Fake
	engine *engine.Engine
	cfg    *config.Config

	// live tracks the addresses the edit stream believes exist, which is the
	// model the no-duplicate and no-loss assertions are made against.
	live    map[string]bool
	deleted map[string]bool
	nextID  int
}

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

// Run executes one scenario.
func Run(ctx context.Context, opts Options) (*Result, error) {
	opts.applyDefaults()

	r := &runner{
		opts:    opts,
		rnd:     rand.New(rand.NewSource(opts.Seed)),
		clock:   clock.NewManual(epoch),
		live:    map[string]bool{},
		deleted: map[string]bool{},
	}
	r.store = store.NewMem(r.clock)

	caps := connector.Caps{
		NativeIdempotency: true,
		Webhooks:          true,
		SoftDelete:        true,
		ETags:             true,
		ModifiedAtIsExact: opts.Faults.TimestampGranularity == 0,
	}
	r.left = fake.New(fake.Options{
		Name: "left", Clock: r.clock, Caps: caps, Secret: "left-secret",
		Schemas: leftSchema(), Faults: opts.Faults, Seed: opts.Seed,
	})
	r.right = fake.New(fake.Options{
		Name: "right", Clock: r.clock, Caps: caps, Secret: "right-secret",
		Schemas: rightSchema(), Faults: opts.Faults, Seed: opts.Seed + 1,
	})
	r.cfg = buildConfig(opts.Policy)

	if err := r.build(); err != nil {
		return nil, err
	}

	restartAt := -1
	if opts.Restart {
		restartAt = opts.Rounds / 2
	}

	for round := 0; round < opts.Rounds; round++ {
		if round == restartAt {
			// A restart loses the queue and keeps the store, which is exactly
			// what a crash leaves behind.
			r.engine.Stop()
			if err := r.build(); err != nil {
				return nil, err
			}
		}
		r.applyEdits(opts.OpsPerRound)
		if err := r.tick(ctx, opts.EditAdvance); err != nil {
			return nil, err
		}
	}

	result := &Result{Options: opts}
	result.WritesDuringEdits = r.writes()

	// The world stops fighting, and the engine is given quiet rounds to
	// finish. Anything it writes from here on it invented.
	r.left.SetFaults(fake.Faults{})
	r.right.SetFaults(fake.Faults{})

	before := r.writes()
	for round := 1; round <= opts.SettleRounds; round++ {
		if err := r.tick(ctx, opts.SettleAdvance); err != nil {
			return nil, err
		}
		if r.writes() == before {
			if result.SettledAfter == 0 {
				result.SettledAfter = round
			}
		} else {
			// Still working, so the count of quiet rounds starts again.
			result.SettledAfter = 0
			before = r.writes()
		}
	}
	result.WritesWhileSettling = r.writes() - result.WritesDuringEdits

	r.engine.Stop()

	result.Stats = r.engine.Stats()
	result.Left, result.Right, result.Store = r.left, r.right, r.store
	result.Live, result.Deleted = keys(r.live), keys(r.deleted)

	var err error
	if result.Reviews, err = r.engine.Reviews(0); err != nil {
		return nil, err
	}
	if result.DeadLetters, err = r.engine.DeadLetters(0); err != nil {
		return nil, err
	}
	return result, nil
}

// build constructs an engine over the current store, as a fresh process would.
func (r *runner) build() error {
	e, err := engine.New(engine.Options{
		Config: r.cfg,
		Store:  r.store,
		Clock:  r.clock,
		Logger: r.opts.Logger,
		Connectors: map[string]connector.Connector{
			"left":  r.left,
			"right": r.right,
		},
		Seed: r.opts.Seed,
	})
	if err != nil {
		return err
	}
	r.engine = e
	return nil
}

// tick is one pass of the world: deliver whatever the peers queued, poll,
// process, and move time forward.
func (r *runner) tick(ctx context.Context, advance time.Duration) error {
	r.deliver()
	if err := r.engine.Cycle(ctx); err != nil {
		// A peer that refused to answer is part of the scenario, not a
		// failure of it. The mark simply does not move.
		_ = err
	}
	r.deliver()
	r.engine.Drain(ctx)

	// Deliveries the drain caused, fed straight back in. This is the loop the
	// termination property is about.
	r.deliver()
	r.engine.Drain(ctx)

	r.clock.Advance(advance)
	return nil
}

// deliver posts every due webhook at the engine's handler.
func (r *runner) deliver() {
	handler := r.engine.WebhookHandler()
	for _, peer := range []struct {
		f    *fake.Fake
		path string
	}{{r.left, "/webhook/left"}, {r.right, "/webhook/right"}} {
		for _, d := range peer.f.DrainWebhooks() {
			handler.ServeHTTP(&discardWriter{}, d.Request(peer.path))
		}
	}
}

// applyEdits makes n changes, as people would.
func (r *runner) applyEdits(n int) {
	for i := 0; i < n; i++ {
		side, name := r.left, "left"
		if r.rnd.Intn(2) == 0 {
			side, name = r.right, "right"
		}

		switch {
		case len(r.live) == 0 || r.rnd.Intn(100) < 40:
			r.create(side, name)
		case r.rnd.Intn(100) < 15:
			r.delete(side)
		default:
			r.update(side, name)
		}
	}
}

func (r *runner) create(side *fake.Fake, name string) {
	r.nextID++
	email := fmt.Sprintf("person%03d@example.test", r.nextID)
	side.ExternalUpsert("contact", "", r.fields(side, email, fmt.Sprintf("%s-%d", name, r.nextID)))
	r.live[email] = true
	delete(r.deleted, email)
}

func (r *runner) update(side *fake.Fake, name string) {
	email := r.pickLive()
	if email == "" {
		return
	}
	rec := r.find(side, email)
	if rec == "" {
		// The address exists in the world but not yet on this side. Editing
		// what is not there is not an edit.
		return
	}
	side.ExternalUpsert("contact", rec, map[string]any{
		r.firstNameField(side): fmt.Sprintf("%s-%d", name, r.rnd.Intn(1000)),
	})
}

func (r *runner) delete(side *fake.Fake) {
	email := r.pickLive()
	if email == "" {
		return
	}
	rec := r.find(side, email)
	if rec == "" {
		return
	}
	side.ExternalDelete("contact", rec)
	delete(r.live, email)
	r.deleted[email] = true
}

// fields builds a record in the side's own field names.
func (r *runner) fields(side *fake.Fake, email, firstName string) map[string]any {
	if side == r.left {
		return map[string]any{"email": email, "firstname": firstName}
	}
	return map[string]any{"email": email, "first_name": firstName}
}

func (r *runner) firstNameField(side *fake.Fake) string {
	if side == r.left {
		return "firstname"
	}
	return "first_name"
}

// find returns the remote ID holding an address on one side.
func (r *runner) find(side *fake.Fake, email string) string {
	for _, rec := range side.Records("contact") {
		if fmt.Sprint(rec.Fields["email"]) == email {
			return rec.RemoteID
		}
	}
	return ""
}

func (r *runner) pickLive() string {
	emails := keys(r.live)
	if len(emails) == 0 {
		return ""
	}
	return emails[r.rnd.Intn(len(emails))]
}

func (r *runner) writes() int {
	return r.left.Calls("upsert") + r.right.Calls("upsert") +
		r.left.Calls("delete") + r.right.Calls("delete")
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// discardWriter swallows the handler's response, which nothing here reads.
// The webhook path's job is to queue work, and whether it queued any is
// visible in what the engine then does.
type discardWriter struct{ header http.Header }

func (d *discardWriter) Header() http.Header {
	if d.header == nil {
		d.header = http.Header{}
	}
	return d.header
}
func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*discardWriter) WriteHeader(int)             {}

func leftSchema() map[string]connector.Schema {
	return map[string]connector.Schema{"contact": {Kind: "contact", Fields: []connector.FieldSpec{
		{Name: "email"}, {Name: "firstname"},
	}}}
}

func rightSchema() map[string]connector.Schema {
	return map[string]connector.Schema{"contact": {Kind: "contact", Fields: []connector.FieldSpec{
		{Name: "email"}, {Name: "first_name"},
	}}}
}

func buildConfig(policy string) *config.Config {
	return &config.Config{
		LogLevel:    "error",
		Listen:      "127.0.0.1:0",
		Workers:     2,
		MaxAttempts: 12,
		Connectors: []config.Connector{
			{
				Name: "left", Driver: "fake",
				EchoWindow: config.Duration(10 * time.Minute),
				Overlap:    config.Duration(2 * time.Minute),
				RateLimit:  config.RateLimit{RatePerSec: 100000, Burst: 100000},
			},
			{
				Name: "right", Driver: "fake",
				EchoWindow: config.Duration(10 * time.Minute),
				Overlap:    config.Duration(2 * time.Minute),
				RateLimit:  config.RateLimit{RatePerSec: 100000, Burst: 100000},
			},
		},
		Syncs: []config.Sync{{
			Kind:      "contact",
			Left:      "left",
			Right:     "right",
			Direction: mapping.Bidirectional,
			Identity:  config.Identity{Keys: []string{"email"}, OnAmbiguous: "review"},
			Conflict:  config.Conflict{Default: policy},
			Fields: []config.FieldPair{
				{Canonical: "email", Left: "email", Right: "email", Transform: mapping.TransformEmailNormalize},
				{Canonical: "first_name", Left: "firstname", Right: "first_name"},
			},
		}},
	}
}
