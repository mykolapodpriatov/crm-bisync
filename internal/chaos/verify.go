package chaos

import (
	"fmt"
	"sort"

	"crm-bisync/internal/model"
)

// Violation is one invariant that did not hold.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) String() string { return v.Invariant + ": " + v.Detail }

// The invariants, named so a failure says which one broke rather than only
// that something did.
const (
	InvariantConvergence  = "convergence"
	InvariantTermination  = "termination"
	InvariantNoDuplicates = "no duplicates"
	InvariantNoLoss       = "no loss"
	InvariantNoFailures   = "no failures"
)

// Verify checks everything the run is supposed to guarantee.
//
// It returns every violation rather than the first, because a run that broke
// three invariants and a run that broke one are different bugs, and knowing
// which is which before opening the debugger is most of the work.
func (r *Result) Verify() []Violation {
	var out []Violation

	out = append(out, r.checkFailures()...)
	out = append(out, r.checkDuplicates()...)
	out = append(out, r.checkNoLoss()...)
	out = append(out, r.checkConvergence()...)
	out = append(out, r.checkTermination()...)

	return out
}

// checkFailures reports work that never landed.
//
// With the fault rates these scenarios use, exhausting a dozen attempts is
// effectively impossible, so a dead letter means something other than bad luck
// and every assertion after it is standing on sand.
func (r *Result) checkFailures() []Violation {
	var out []Violation
	for _, item := range r.DeadLetters {
		out = append(out, Violation{
			Invariant: InvariantNoFailures,
			Detail:    fmt.Sprintf("%s: %s", item.Ref.String(), item.Reason),
		})
	}
	return out
}

// checkDuplicates requires one record per address per side.
//
// This is the idempotency assertion. A repeated webhook, a re-read page or a
// retried write that produced a second contact shows up here and nowhere else.
func (r *Result) checkDuplicates() []Violation {
	var out []Violation

	for _, side := range r.sides() {
		counts := map[string]int{}
		for _, rec := range side.records() {
			counts[side.email(rec)]++
		}
		for _, email := range sortedKeys(counts) {
			if counts[email] > 1 {
				out = append(out, Violation{
					Invariant: InvariantNoDuplicates,
					Detail:    fmt.Sprintf("%s holds %d records for %s", side.name, counts[email], email),
				})
			}
		}
	}
	return out
}

// checkNoLoss requires every address the edits left alive to exist on both
// sides, and every address they removed to exist on neither.
func (r *Result) checkNoLoss() []Violation {
	var out []Violation

	for _, side := range r.sides() {
		present := side.byEmail()
		for _, email := range r.Live {
			if _, ok := present[email]; !ok {
				out = append(out, Violation{
					Invariant: InvariantNoLoss,
					Detail:    fmt.Sprintf("%s is missing from %s", email, side.name),
				})
			}
		}
		for _, email := range r.Deleted {
			if _, ok := present[email]; ok {
				out = append(out, Violation{
					Invariant: InvariantNoLoss,
					Detail:    fmt.Sprintf("%s was deleted but survives on %s", email, side.name),
				})
			}
		}
	}
	return out
}

// checkConvergence requires the two sides to agree, or to have said why not.
//
// A pair the engine refused to decide about is allowed to differ: that is the
// engine working, not failing. Anything else differing is the sync having
// stopped short of agreement without telling anybody.
func (r *Result) checkConvergence() []Violation {
	var out []Violation

	reviewed := r.reviewedEmails()
	left, right := r.sides()[0].byEmail(), r.sides()[1].byEmail()

	for _, email := range r.Live {
		l, lok := left[email]
		rr, rok := right[email]
		if !lok || !rok {
			// Already reported as loss; saying it twice would make one problem
			// look like two.
			continue
		}
		if reviewed[email] {
			continue
		}
		if l != rr {
			out = append(out, Violation{
				Invariant: InvariantConvergence,
				Detail: fmt.Sprintf("%s: left has %q, right has %q, and nothing was queued for review",
					email, l, rr),
			})
		}
	}
	return out
}

// checkTermination requires the writing to have stopped.
//
// This is the anti-echo-loop assertion, and it is the reason the settle phase
// exists at all. A sync that converges and keeps writing is not converged, it
// is just briefly agreeing on its way round the loop again.
func (r *Result) checkTermination() []Violation {
	if r.SettledAfter > 0 {
		return nil
	}
	return []Violation{{
		Invariant: InvariantTermination,
		Detail: fmt.Sprintf("still writing after %d quiet rounds (%d writes while settling)",
			r.Options.SettleRounds, r.WritesWhileSettling),
	}}
}

// reviewedEmails resolves queued decisions back to the addresses they concern.
func (r *Result) reviewedEmails() map[string]bool {
	out := map[string]bool{}
	for _, item := range r.Reviews {
		for _, side := range r.sides() {
			if side.name != item.Ref.Connector {
				continue
			}
			for _, rec := range side.records() {
				if rec.RemoteID == item.Ref.RemoteID {
					out[side.email(rec)] = true
				}
			}
		}
	}
	return out
}

// sideView is one peer, with the field names it happens to use.
type sideView struct {
	name       string
	records    func() []model.Record
	firstField string
}

func (s sideView) email(rec model.Record) string {
	return fmt.Sprint(rec.Fields["email"])
}

// byEmail maps each address to the first name held against it, which is the
// only mapped field that varies in these scenarios.
func (s sideView) byEmail() map[string]string {
	out := map[string]string{}
	for _, rec := range s.records() {
		out[s.email(rec)] = fmt.Sprint(rec.Fields[s.firstField])
	}
	return out
}

func (r *Result) sides() []sideView {
	return []sideView{
		{name: "left", records: func() []model.Record { return r.Left.Records("contact") }, firstField: "firstname"},
		{name: "right", records: func() []model.Record { return r.Right.Records("contact") }, firstField: "first_name"},
	}
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
