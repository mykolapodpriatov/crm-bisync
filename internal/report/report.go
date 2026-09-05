// Package report renders what the engine decided, for people.
//
// It is a separate package because the rendering is worth testing on its own:
// plan output is the first and often the only thing anybody reads about this
// program, and a golden test is the cheapest way to notice that it stopped
// being readable.
package report

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"crm-bisync/internal/engine"
)

// Doctor writes a diagnosis.
func Doctor(w io.Writer, d engine.Diagnosis) {
	if len(d.Checks) == 0 {
		fmt.Fprintln(w, "Nothing to check: no syncs are configured.")
		return
	}

	width := 0
	for _, c := range d.Checks {
		if n := len(c.Name) + len(c.Subject) + 1; n > width {
			width = n
		}
	}

	for _, c := range d.Checks {
		mark := "ok  "
		if !c.OK {
			mark = "FAIL"
		}
		label := c.Name
		if c.Subject != "" {
			label += " " + c.Subject
		}
		fmt.Fprintf(w, "%s  %-*s  %s\n", mark, width, label, c.Detail)
	}

	fmt.Fprintln(w)
	if failures := d.Failures(); len(failures) > 0 {
		fmt.Fprintf(w, "%d of %d checks failed.\n", len(failures), len(d.Checks))
		return
	}
	fmt.Fprintf(w, "All %d checks passed.\n", len(d.Checks))
}

// Plan writes a dry run.
func Plan(w io.Writer, r *engine.PlanReport) {
	if r.Empty() {
		if len(r.Errors) > 0 {
			// Saying the sides agree when a peer could not be read would be a
			// claim the plan has no basis for.
			fmt.Fprintln(w, "Nothing to report from the peers that answered.")
		} else {
			fmt.Fprintln(w, "Nothing to do: both sides already agree.")
		}
		writeErrors(w, r.Errors)
		return
	}

	bySync := map[string][]engine.PlannedChange{}
	var names []string
	for _, c := range r.Changes {
		if _, ok := bySync[c.Sync]; !ok {
			names = append(names, c.Sync)
		}
		bySync[c.Sync] = append(bySync[c.Sync], c)
	}
	sort.Strings(names)

	for _, name := range names {
		fmt.Fprintf(w, "%s\n", name)
		for _, c := range bySync[name] {
			writeChange(w, c)
		}
		fmt.Fprintln(w)
	}

	if len(r.Reviews) > 0 {
		fmt.Fprintf(w, "Waiting for a decision (%d)\n", len(r.Reviews))
		for _, item := range r.Reviews {
			fmt.Fprintf(w, "  %s\n    %s\n", item.Ref.String(), item.Reason)
		}
		fmt.Fprintln(w)
	}

	if len(r.DeadLetters) > 0 {
		fmt.Fprintf(w, "Would fail (%d)\n", len(r.DeadLetters))
		for _, item := range r.DeadLetters {
			fmt.Fprintf(w, "  %s\n    %s\n", item.Ref.String(), item.Reason)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "%s.\n", summarise(r))
	writeErrors(w, r.Errors)
}

// writeChange renders one planned write, with its fields underneath it.
func writeChange(w io.Writer, c engine.PlannedChange) {
	switch c.Action {
	case engine.ActionCreate:
		fmt.Fprintf(w, "  create on %s\n", c.Target.Connector)
		fmt.Fprintf(w, "    from %s\n", c.Source.String())
	case engine.ActionDelete:
		fmt.Fprintf(w, "  delete %s\n", c.Target.String())
		fmt.Fprintf(w, "    because %s was deleted\n", c.Source.String())
		return
	default:
		fmt.Fprintf(w, "  update %s\n", c.Target.String())
		fmt.Fprintf(w, "    from %s\n", c.Source.String())
	}

	for _, f := range c.Fields {
		fmt.Fprintf(w, "    %s = %s\n", f.Field, render(f.Value))
	}
}

// summarise counts the plan in one sentence, because the first thing anybody
// wants from a plan is whether it is small.
func summarise(r *engine.PlanReport) string {
	counts := map[string]int{}
	for _, c := range r.Changes {
		counts[c.Action]++
	}

	var parts []string
	for _, action := range []string{engine.ActionCreate, engine.ActionUpdate, engine.ActionDelete} {
		if n := counts[action]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d to %s", n, action))
		}
	}
	if len(r.Reviews) > 0 {
		parts = append(parts, fmt.Sprintf("%d waiting for a decision", len(r.Reviews)))
	}
	if len(r.DeadLetters) > 0 {
		parts = append(parts, fmt.Sprintf("%d that would fail", len(r.DeadLetters)))
	}
	if len(parts) == 0 {
		return "Nothing to do"
	}
	return strings.Join(parts, ", ")
}

func writeErrors(w io.Writer, errs []string) {
	if len(errs) == 0 {
		return
	}
	fmt.Fprintf(w, "\nThe plan is incomplete. %d peer error(s):\n", len(errs))
	for _, e := range errs {
		for _, line := range strings.Split(e, "\n") {
			fmt.Fprintf(w, "  %s\n", line)
		}
	}
}

// render prints a value the way a person reads it, quoting strings so that a
// trailing space or an empty value is visible rather than invisible.
func render(v any) string {
	switch t := v.(type) {
	case nil:
		return "(unset)"
	case string:
		return fmt.Sprintf("%q", t)
	default:
		return fmt.Sprint(t)
	}
}
