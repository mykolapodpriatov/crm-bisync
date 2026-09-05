// Package identity decides which record on one peer is the same thing as a
// record on the other.
//
// Three tiers, in strict order, and it never guesses:
//
//  1. The link table. Once two records are linked, that is the answer, and in
//     steady state this is the only tier that runs.
//  2. Deterministic keys, such as a normalised email. Exact equality on a
//     normalised value, never similarity.
//  3. Ambiguity handling. More than one candidate, or none, is a decision the
//     operator configured, defaulting to a review queue.
//
// There is deliberately no fuzzy matching. Probabilistic matching running
// unattended is how two real customers become one record, and nobody can
// un-merge that from a log afterwards.
package identity

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"crm-bisync/internal/store"
)

// Outcome is what the resolver decided.
type Outcome int

// The possible outcomes.
const (
	// Linked means an existing link answered it.
	Linked Outcome = iota
	// Matched means a deterministic key found exactly one counterpart. The
	// caller is expected to write the link so the next lookup is tier one.
	Matched
	// Create means nothing matched and the peer should get a new record.
	Create
	// Review means the caller must not write anything: a human decides.
	Review
	// Skip means nothing matched and the operator asked to do nothing.
	Skip
)

// String renders the outcome for logs and metrics labels.
func (o Outcome) String() string {
	switch o {
	case Linked:
		return "linked"
	case Matched:
		return "matched"
	case Create:
		return "create"
	case Review:
		return "review"
	case Skip:
		return "skip"
	default:
		return "unknown"
	}
}

// Resolution is the resolver's answer.
type Resolution struct {
	Outcome Outcome
	// Peer is set for Linked and Matched.
	Peer store.Ref
	// Candidates is set for Review, and holds every record that matched.
	Candidates []store.Ref
	// Reason is a short human-readable explanation, carried into the review
	// queue so somebody reading it later does not have to reconstruct why.
	Reason string
}

// Ambiguity policies, as they appear in config.json.
const (
	OnAmbiguousReview = "review"
	OnAmbiguousCreate = "create"
	OnAmbiguousSkip   = "skip"
)

// Resolver matches records for one sync.
type Resolver struct {
	typed       *store.Typed
	kind        string
	keys        []string
	onAmbiguous string
}

// New builds a resolver. keys are canonical field names, already normalised by
// the mapper's transforms, which is what makes exact comparison meaningful.
func New(typed *store.Typed, kind string, keys []string, onAmbiguous string) *Resolver {
	if onAmbiguous == "" {
		onAmbiguous = OnAmbiguousReview
	}
	return &Resolver{typed: typed, kind: kind, keys: keys, onAmbiguous: onAmbiguous}
}

// keyEntry is the stored set of records holding one key value.
type keyEntry struct {
	Refs []store.Ref `json:"refs"`
}

// ownerEntry is the set of key strings a record currently occupies.
type ownerEntry struct {
	Keys []string `json:"keys"`
}

// keyString encodes one indexed key.
func keyString(connector, kind, name string, value any) string {
	return connector + "\x00" + kind + "\x00" + name + "\x00" + fmt.Sprint(value)
}

func ownerKey(ref store.Ref) string {
	return ref.Connector + "\x00" + ref.Kind + "\x00" + ref.RemoteID
}

// Index records the deterministic keys a record currently holds.
//
// It releases whatever the record held before, so that a contact whose email
// changes stops answering to the old address. Leaving the stale entry behind
// would make the next record with that address match the wrong person, which
// is the same failure as fuzzy matching, arrived at more slowly.
func (r *Resolver) Index(ref store.Ref, canonical map[string]any) error {
	if err := r.releaseKeys(ref); err != nil {
		return err
	}

	var held []string
	for _, name := range r.keys {
		v, ok := canonical[name]
		if !ok || isEmpty(v) {
			// A record with no value for a key simply does not take part in
			// matching on it. Indexing the empty string would make every
			// record with a blank email a candidate for every other one.
			continue
		}
		k := keyString(ref.Connector, ref.Kind, name, v)
		if err := r.addRef(k, ref); err != nil {
			return err
		}
		held = append(held, k)
	}

	if len(held) == 0 {
		return nil
	}
	sort.Strings(held)
	raw, err := json.Marshal(ownerEntry{Keys: held})
	if err != nil {
		return fmt.Errorf("identity: encode key owner: %w", err)
	}
	return r.typed.S.Put(store.CollKeyOwner, ownerKey(ref), raw, time.Time{})
}

// Deindex removes a record from the key index, for a delete.
func (r *Resolver) Deindex(ref store.Ref) error {
	return r.releaseKeys(ref)
}

func (r *Resolver) releaseKeys(ref store.Ref) error {
	raw, ok, err := r.typed.S.Get(store.CollKeyOwner, ownerKey(ref))
	if err != nil || !ok {
		return err
	}
	var owner ownerEntry
	if err := json.Unmarshal(raw, &owner); err != nil {
		return fmt.Errorf("identity: decode key owner: %w", err)
	}
	for _, k := range owner.Keys {
		if err := r.removeRef(k, ref); err != nil {
			return err
		}
	}
	return r.typed.S.Delete(store.CollKeyOwner, ownerKey(ref))
}

func (r *Resolver) addRef(key string, ref store.Ref) error {
	entry, err := r.loadKey(key)
	if err != nil {
		return err
	}
	for _, existing := range entry.Refs {
		if existing == ref {
			return nil
		}
	}
	entry.Refs = append(entry.Refs, ref)
	sort.Slice(entry.Refs, func(i, j int) bool {
		return entry.Refs[i].RemoteID < entry.Refs[j].RemoteID
	})
	return r.saveKey(key, entry)
}

func (r *Resolver) removeRef(key string, ref store.Ref) error {
	entry, err := r.loadKey(key)
	if err != nil {
		return err
	}
	kept := entry.Refs[:0]
	for _, existing := range entry.Refs {
		if existing != ref {
			kept = append(kept, existing)
		}
	}
	entry.Refs = kept
	if len(entry.Refs) == 0 {
		return r.typed.S.Delete(store.CollKeys, key)
	}
	return r.saveKey(key, entry)
}

func (r *Resolver) loadKey(key string) (keyEntry, error) {
	raw, ok, err := r.typed.S.Get(store.CollKeys, key)
	if err != nil || !ok {
		return keyEntry{}, err
	}
	var entry keyEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return keyEntry{}, fmt.Errorf("identity: decode key entry: %w", err)
	}
	return entry, nil
}

func (r *Resolver) saveKey(key string, entry keyEntry) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("identity: encode key entry: %w", err)
	}
	return r.typed.S.Put(store.CollKeys, key, raw, time.Time{})
}

// Resolve finds the counterpart of `from` on the peer connector.
func (r *Resolver) Resolve(from store.Ref, peerConnector string, canonical map[string]any) (Resolution, error) {
	// Tier one: an existing link is the answer, full stop.
	if link, ok, err := r.typed.GetLink(from); err != nil {
		return Resolution{}, err
	} else if ok {
		if peer, ok := link.Peer(from); ok {
			return Resolution{Outcome: Linked, Peer: peer, Reason: "linked"}, nil
		}
	}

	// Tier two: deterministic keys, exact equality on normalised values.
	candidates, matchedOn, err := r.candidates(peerConnector, canonical)
	if err != nil {
		return Resolution{}, err
	}

	switch len(candidates) {
	case 1:
		return Resolution{
			Outcome: Matched,
			Peer:    candidates[0],
			Reason:  fmt.Sprintf("matched on %s", matchedOn),
		}, nil
	case 0:
		return r.noMatch(), nil
	}

	// Tier three: more than one candidate. The operator decides, and the
	// default refuses to.
	res := Resolution{
		Candidates: candidates,
		Reason: fmt.Sprintf("%d records on %s match on %s",
			len(candidates), peerConnector, matchedOn),
	}
	switch r.onAmbiguous {
	case OnAmbiguousCreate:
		res.Outcome = Create
	case OnAmbiguousSkip:
		res.Outcome = Skip
	default:
		res.Outcome = Review
	}
	return res, nil
}

func (r *Resolver) noMatch() Resolution {
	if r.onAmbiguous == OnAmbiguousSkip {
		return Resolution{Outcome: Skip, Reason: "no match"}
	}
	// No match is not ambiguous, so it creates even under the review policy.
	// Review exists for "which of these two is it", not for "there is none".
	return Resolution{Outcome: Create, Reason: "no match"}
}

// candidates gathers every peer record matching on any configured key.
func (r *Resolver) candidates(peerConnector string, canonical map[string]any) ([]store.Ref, string, error) {
	seen := make(map[store.Ref]bool)
	var out []store.Ref
	var matchedOn []string

	for _, name := range r.keys {
		v, ok := canonical[name]
		if !ok || isEmpty(v) {
			continue
		}
		entry, err := r.loadKey(keyString(peerConnector, r.kind, name, v))
		if err != nil {
			return nil, "", err
		}
		if len(entry.Refs) == 0 {
			continue
		}
		matchedOn = append(matchedOn, name)
		for _, ref := range entry.Refs {
			if seen[ref] {
				continue
			}
			seen[ref] = true
			out = append(out, ref)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].RemoteID < out[j].RemoteID })
	if len(matchedOn) == 0 {
		return nil, "", nil
	}
	return out, joinNames(matchedOn), nil
}

func joinNames(names []string) string {
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

// isEmpty reports whether a canonical value should take part in matching.
func isEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	default:
		return false
	}
}
