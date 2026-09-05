// Package conflict decides what happens when both peers changed a record
// since the last time they agreed.
//
// The load-bearing idea is the stored snapshot digest. Without it there is no
// way to tell "this side changed" from "this side merely reports a value", so
// every sync degrades into last-writer-wins by accident and nobody finds out
// until a customer's phone number goes backwards. With it, the four cases are
// distinguishable and only one of them is genuinely a conflict.
package conflict

import (
	"fmt"
	"sort"
	"time"

	"crm-bisync/internal/mapping"
	"crm-bisync/internal/model"
)

// Policies, as they appear in config.json.
const (
	// LeftWins and RightWins name a side as the source of truth. Boring, and
	// the right answer for most real deployments.
	LeftWins  = "left_wins"
	RightWins = "right_wins"
	// NewestWins trusts modification times, which means trusting two clocks
	// that are not ours. The resolver applies a skew tolerance rather than
	// believing a millisecond.
	NewestWins = "newest_wins"
	// FieldLevel merges edits that do not overlap and escalates the ones that
	// do, which is the only policy that can lose nothing when two people edit
	// different fields of the same record.
	FieldLevel = "field_level"
	// Review writes nothing and queues both versions.
	Review = "review"
)

// Policies lists every valid policy.
func Policies() []string {
	return []string{LeftWins, RightWins, NewestWins, FieldLevel, Review}
}

// IsValidPolicy reports whether a policy exists.
func IsValidPolicy(p string) bool {
	for _, known := range Policies() {
		if known == p {
			return true
		}
	}
	return false
}

// Situation classifies what happened since the last agreement.
type Situation int

// The four cases.
const (
	// Unchanged means neither side moved. Nothing to do, and this is the
	// common case on any real sync: most polls see records that did not change.
	Unchanged Situation = iota
	// LeftChanged means only the left side moved.
	LeftChanged
	// RightChanged means only the right side moved.
	RightChanged
	// BothChanged is the only genuine conflict.
	BothChanged
)

// String renders the situation for logs and metric labels.
func (s Situation) String() string {
	switch s {
	case LeftChanged:
		return "left_changed"
	case RightChanged:
		return "right_changed"
	case BothChanged:
		return "both_changed"
	default:
		return "unchanged"
	}
}

// Action is what the caller should do.
type Action int

// The possible actions.
const (
	// Nothing means no write.
	Nothing Action = iota
	// WriteLeft means apply the resolved values to the left peer.
	WriteLeft
	// WriteRight means apply the resolved values to the right peer.
	WriteRight
	// WriteBoth is only produced by field-level merging, where each side is
	// missing something the other has.
	WriteBoth
	// Escalate means write nothing and queue the conflict for a human.
	Escalate
)

// String renders the action for logs and metric labels.
func (a Action) String() string {
	switch a {
	case WriteLeft:
		return "write_left"
	case WriteRight:
		return "write_right"
	case WriteBoth:
		return "write_both"
	case Escalate:
		return "escalate"
	default:
		return "nothing"
	}
}

// Side names a peer.
type Side = mapping.Side

// FieldDiff is one field the two sides disagree about.
type FieldDiff struct {
	Field string
	Left  any
	Right any
	// LeftChanged and RightChanged say which side moved since the last
	// agreement, which is what distinguishes a merge from a conflict.
	LeftChanged  bool
	RightChanged bool
	// Winner is the side whose value the resolution picked, or Escalate's
	// zero value when nobody won.
	Winner Action
}

// State is one side's view of a record.
type State struct {
	// Fields are the canonical values as they are now.
	Fields map[string]any
	// Hash is the digest of Fields.
	Hash string
	// Base is this side's record as it stood at the last successful sync,
	// with SyncedHash its digest. Both are empty when the pair has never
	// synced.
	//
	// The values matter as much as the digest. Field-level resolution is a
	// three-way merge and a three-way merge needs the base: with only a
	// digest, a changed record makes every one of its fields look contested
	// and the merge degrades into an escalation.
	Base       map[string]any
	SyncedHash string
	// UpdatedAt is the peer's modification time, used only by NewestWins.
	UpdatedAt time.Time
	// Present is false when the record does not exist on this side.
	Present bool
}

// Changed reports whether this side moved since the last agreement.
func (s State) Changed() bool {
	return s.Hash != s.SyncedHash
}

// HasBase reports whether this side has a recorded base to merge against.
func (s State) HasBase() bool {
	return s.SyncedHash != ""
}

// fieldChanged reports whether one field moved since the last agreement.
func (s State) fieldChanged(name string) bool {
	current, hasCurrent := s.Fields[name]
	base, hasBase := s.Base[name]
	if hasCurrent != hasBase {
		return true
	}
	return !equal(current, base)
}

// Resolution is the resolver's answer.
type Resolution struct {
	Situation Situation
	Action    Action
	Policy    string
	// Values are the canonical fields to write. For WriteBoth they are the
	// merged set, and each side receives the fields it is missing.
	Values map[string]any
	// LeftValues and RightValues are set for WriteBoth.
	LeftValues  map[string]any
	RightValues map[string]any
	// Diffs is every field the sides disagree about, in field order. It is
	// what plan renders and what the review queue stores.
	Diffs  []FieldDiff
	Reason string
}

// Resolver applies a policy.
type Resolver struct {
	defaultPolicy string
	perField      map[string]string
	// skew is how far apart two peers' clocks may be before NewestWins stops
	// trusting the difference. Believing a millisecond means letting whichever
	// peer's clock drifts forward win every conflict forever.
	skew time.Duration
}

// DefaultSkew is the tolerance applied to NewestWins.
const DefaultSkew = 2 * time.Second

// New builds a resolver. An empty default policy means Review, which is the
// only safe thing to assume: a sync that was never told what to do must not
// pick a winner on its own.
func New(defaultPolicy string, perField map[string]string, skew time.Duration) (*Resolver, error) {
	if defaultPolicy == "" {
		defaultPolicy = Review
	}
	if !IsValidPolicy(defaultPolicy) {
		return nil, fmt.Errorf("conflict: unknown policy %q", defaultPolicy)
	}
	for field, policy := range perField {
		if !IsValidPolicy(policy) {
			return nil, fmt.Errorf("conflict: unknown policy %q for field %q", policy, field)
		}
	}
	if skew <= 0 {
		skew = DefaultSkew
	}
	return &Resolver{defaultPolicy: defaultPolicy, perField: perField, skew: skew}, nil
}

// PolicyFor returns the policy governing a field.
func (r *Resolver) PolicyFor(field string) string {
	if p, ok := r.perField[field]; ok {
		return p
	}
	return r.defaultPolicy
}

// Resolve decides what to do about one linked pair.
func (r *Resolver) Resolve(left, right State, fields []string) Resolution {
	situation := classify(left, right)

	res := Resolution{
		Situation: situation,
		Policy:    r.defaultPolicy,
		Diffs:     diff(left, right, fields),
	}

	// Digests can say both sides moved while the sides still agree: an
	// unmapped field changed, or two people made the same edit, or a pair has
	// only just been matched and has no recorded base at all. There is nothing
	// to write and nothing to decide, and escalating here would put a question
	// with no answer in front of a person.
	if len(res.Diffs) == 0 {
		res.Action = Nothing
		res.Reason = "the two sides already agree on every mapped field"
		return res
	}

	switch situation {
	case Unchanged:
		res.Action = Nothing
		res.Reason = "neither side changed since the last sync"
		return res

	case LeftChanged:
		res.Action = WriteRight
		res.Values = pick(left.Fields, fields)
		res.Reason = "only the left side changed"
		return res

	case RightChanged:
		res.Action = WriteLeft
		res.Values = pick(right.Fields, fields)
		res.Reason = "only the right side changed"
		return res
	}

	return r.resolveConflict(left, right, fields, res)
}

// resolveConflict handles the case both sides moved.
func (r *Resolver) resolveConflict(left, right State, fields []string, res Resolution) Resolution {
	// Field level is the only policy that looks at fields rather than sides,
	// so it takes its own path.
	if r.defaultPolicy == FieldLevel {
		return r.merge(left, right, fields, res)
	}

	res.Policy = r.defaultPolicy
	switch r.defaultPolicy {
	case LeftWins:
		res.Action = WriteRight
		res.Values = pick(left.Fields, fields)
		res.Reason = "both sides changed; the left side is the source of truth"
	case RightWins:
		res.Action = WriteLeft
		res.Values = pick(right.Fields, fields)
		res.Reason = "both sides changed; the right side is the source of truth"
	case NewestWins:
		return r.newest(left, right, fields, res)
	default:
		res.Action = Escalate
		res.Reason = "both sides changed and the policy is to ask"
	}
	return res
}

// newest applies NewestWins, with the skew guard.
func (r *Resolver) newest(left, right State, fields []string, res Resolution) Resolution {
	gap := left.UpdatedAt.Sub(right.UpdatedAt)
	if gap < 0 {
		gap = -gap
	}

	// Within the tolerance the two edits are simultaneous as far as anyone can
	// tell, and picking one would be a coin flip dressed up as a policy.
	if gap <= r.skew {
		res.Action = Escalate
		res.Reason = fmt.Sprintf(
			"both sides changed %s apart, inside the %s clock tolerance, so which is newer is not knowable",
			gap, r.skew)
		return res
	}

	if left.UpdatedAt.After(right.UpdatedAt) {
		res.Action = WriteRight
		res.Values = pick(left.Fields, fields)
		res.Reason = fmt.Sprintf("the left side changed %s later", gap)
		return res
	}
	res.Action = WriteLeft
	res.Values = pick(right.Fields, fields)
	res.Reason = fmt.Sprintf("the right side changed %s later", gap)
	return res
}

// merge applies FieldLevel: keep every edit that does not collide, escalate
// only the fields that do.
//
// This is the only policy that can lose nothing when two people edit different
// fields of the same record, which on a shared contact is most of the time.
func (r *Resolver) merge(left, right State, fields []string, res Resolution) Resolution {
	res.Policy = FieldLevel

	// Without a base there is nothing to merge against, and guessing which
	// side edited which field is exactly the guess this package exists not to
	// make.
	if !left.HasBase() || !right.HasBase() {
		res.Action = Escalate
		res.Reason = "both sides changed and there is no recorded base to merge against"
		return res
	}

	res.LeftValues = map[string]any{}
	res.RightValues = map[string]any{}

	var contested []string
	for i, d := range res.Diffs {
		switch {
		case d.LeftChanged && d.RightChanged:
			// Both edited the same field to different values. Nothing here can
			// merge that, so it is the one thing a human has to look at.
			contested = append(contested, d.Field)
		case d.LeftChanged:
			res.RightValues[d.Field] = d.Left
			res.Diffs[i].Winner = WriteRight
		case d.RightChanged:
			res.LeftValues[d.Field] = d.Right
			res.Diffs[i].Winner = WriteLeft
		}
	}

	if len(contested) > 0 {
		// Partially applying a merge and escalating the rest would leave the
		// record in a state neither person edited, and make the queued diff a
		// lie about what is on each side.
		res.Action = Escalate
		res.LeftValues = nil
		res.RightValues = nil
		res.Reason = fmt.Sprintf("both sides edited %s", joinFields(contested))
		return res
	}

	switch {
	case len(res.LeftValues) > 0 && len(res.RightValues) > 0:
		res.Action = WriteBoth
		res.Reason = "both sides changed, in fields that do not overlap"
	case len(res.RightValues) > 0:
		res.Action = WriteRight
		res.Values = res.RightValues
		res.RightValues = nil
		res.LeftValues = nil
		res.Reason = "both sides changed, but only left-side edits need applying"
	case len(res.LeftValues) > 0:
		res.Action = WriteLeft
		res.Values = res.LeftValues
		res.LeftValues = nil
		res.RightValues = nil
		res.Reason = "both sides changed, but only right-side edits need applying"
	default:
		// The digests differ but no mapped field does, which means something
		// unmapped moved. Writing here would be a write with nothing to say.
		res.Action = Nothing
		res.LeftValues = nil
		res.RightValues = nil
		res.Reason = "the digests differ but no mapped field does"
	}
	return res
}

// classify works out which sides moved.
func classify(left, right State) Situation {
	l, r := left.Changed(), right.Changed()
	switch {
	case l && r:
		return BothChanged
	case l:
		return LeftChanged
	case r:
		return RightChanged
	default:
		return Unchanged
	}
}

// diff lists every field whose values differ, with which side moved.
func diff(left, right State, fields []string) []FieldDiff {
	names := append([]string(nil), fields...)
	sort.Strings(names)

	var out []FieldDiff
	for _, name := range names {
		lv, lok := left.Fields[name]
		rv, rok := right.Fields[name]
		if lok == rok && equal(lv, rv) {
			continue
		}
		out = append(out, FieldDiff{
			Field:        name,
			Left:         lv,
			Right:        rv,
			LeftChanged:  left.fieldChanged(name),
			RightChanged: right.fieldChanged(name),
		})
	}
	return out
}

// equal compares two canonical values the way the digest does, so that a
// JSON round-trip turning 1 into 1.0 is not reported as a difference.
func equal(a, b any) bool {
	return model.SnapshotHash(map[string]any{"v": a}, []string{"v"}) ==
		model.SnapshotHash(map[string]any{"v": b}, []string{"v"})
}

// pick copies only the named fields, so a resolution can never carry a value
// the mapping does not own.
func pick(fields map[string]any, names []string) map[string]any {
	out := make(map[string]any, len(names))
	for _, name := range names {
		if v, ok := fields[name]; ok {
			out[name] = v
		}
	}
	return out
}

func joinFields(names []string) string {
	sort.Strings(names)
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
