package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

// PlannedChange is one write a dry run would have made.
type PlannedChange struct {
	Sync   string     `json:"sync"`
	Action string     `json:"action"`
	Source store.Ref  `json:"source"`
	Target store.Ref  `json:"target"`
	Fields []NamedVal `json:"fields,omitempty"`
}

// NamedVal is one field of a planned write, kept as a sorted slice rather than
// a map so that two runs of plan over the same state render identically.
type NamedVal struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

// The actions a plan can contain.
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

// planRecorder collects what a dry run would do.
type planRecorder struct {
	mu      sync.Mutex
	changes []PlannedChange
	next    int
}

// record adds a planned change and returns the identifier a create would get.
func (p *planRecorder) record(c PlannedChange) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.next++
	id := fmt.Sprintf("planned-%d", p.next)
	if c.Action == ActionCreate {
		c.Target.RemoteID = id
	}
	p.changes = append(p.changes, c)
	return id
}

func (p *planRecorder) all() []PlannedChange {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PlannedChange(nil), p.changes...)
}

// namedValues renders a value map as a sorted slice.
func namedValues(values map[string]any) []NamedVal {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]NamedVal, 0, len(names))
	for _, name := range names {
		out = append(out, NamedVal{Field: name, Value: values[name]})
	}
	return out
}

// PlannedChanges returns what a dry run decided to do.
func (e *Engine) PlannedChanges() []PlannedChange {
	if e.plan == nil {
		return nil
	}
	return e.plan.all()
}

// PlanReport is the whole answer to "what would this do".
type PlanReport struct {
	Changes     []PlannedChange   `json:"changes"`
	Reviews     []store.QueueItem `json:"reviews"`
	DeadLetters []store.QueueItem `json:"dead_letters"`
	// Errors are the failures the dry run itself hit, such as a peer that
	// could not be read. They are reported rather than returned, because a
	// plan that covered three syncs and failed on the fourth is still worth
	// most of what it cost.
	Errors []string `json:"errors,omitempty"`
}

// Plan performs a full dry run.
//
// It is the same engine over an overlay of the real store, with the writes
// recorded instead of sent. A dry run that reimplemented the pipeline would be
// a second program agreeing with the first by coincidence; this one cannot
// disagree, because it is the first program.
func Plan(ctx context.Context, opts Options) (*PlanReport, error) {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}

	overlay := store.NewOverlay(opts.Store, opts.Clock)
	defer overlay.Close()

	planOpts := opts
	planOpts.Store = overlay
	planOpts.DryRun = true

	e, err := New(planOpts)
	if err != nil {
		return nil, err
	}
	if err := e.Validate(ctx); err != nil {
		return nil, err
	}

	report := &PlanReport{}
	if err := e.Cycle(ctx); err != nil {
		report.Errors = append(report.Errors, err.Error())
	}

	report.Changes = e.PlannedChanges()
	if report.Reviews, err = e.Reviews(0); err != nil {
		return nil, err
	}
	if report.DeadLetters, err = e.DeadLetters(0); err != nil {
		return nil, err
	}
	return report, nil
}

// Empty reports whether a plan would change nothing.
func (r *PlanReport) Empty() bool {
	return len(r.Changes) == 0 && len(r.Reviews) == 0 && len(r.DeadLetters) == 0
}
