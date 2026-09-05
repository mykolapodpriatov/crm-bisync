// Package mapping turns a configured field map into something executable.
//
// Two ideas carry the package. First, the engine works in canonical fields, so
// neither connector's field names leak into the pipeline and a sync between
// two CRMs is not written from the point of view of either. Second, only
// mapped fields are ever read or written: that is what makes it safe to point
// this at a CRM with two hundred fields nobody here owns.
package mapping

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
)

// Side names one end of a sync.
type Side int

// The two sides.
const (
	Left Side = iota
	Right
)

// String renders the side for errors and logs.
func (s Side) String() string {
	if s == Left {
		return "left"
	}
	return "right"
}

// Other returns the opposite side.
func (s Side) Other() Side {
	if s == Left {
		return Right
	}
	return Left
}

// Directions, as they appear in config.json.
//
// They are named after the sides rather than "push" and "pull" on purpose.
// This engine syncs two peers that are both CRMs; "push" only means something
// when one end is privileged, and calling one of two equal peers the push
// target is how the direction of a field ends up backwards in review.
const (
	LeftToRight   = "left_to_right"
	RightToLeft   = "right_to_left"
	Bidirectional = "bidirectional"
)

const (
	bitToRight = 1
	bitToLeft  = 2
)

var directionBits = map[string]int{
	LeftToRight:   bitToRight,
	RightToLeft:   bitToLeft,
	Bidirectional: bitToRight | bitToLeft,
}

// Directions lists every valid direction.
func Directions() []string {
	return []string{LeftToRight, RightToLeft, Bidirectional}
}

// IsValidDirection reports whether a direction exists.
func IsValidDirection(d string) bool {
	_, ok := directionBits[d]
	return ok
}

// Narrow combines a sync's direction with a per-field override.
//
// An override may narrow what the sync does and never widen it, so this is an
// intersection rather than a table of cases. An empty intersection means the
// field could never sync, which is a configuration error to report rather than
// a direction to fall back to: silently ignoring it is how somebody spends an
// afternoon wondering why one field never moves.
func Narrow(syncDirection, fieldDirection string) (string, bool) {
	base, ok := directionBits[syncDirection]
	if !ok {
		return "", false
	}
	if fieldDirection == "" {
		return syncDirection, true
	}
	over, ok := directionBits[fieldDirection]
	if !ok {
		return "", false
	}

	switch base & over {
	case bitToRight:
		return LeftToRight, true
	case bitToLeft:
		return RightToLeft, true
	case bitToRight | bitToLeft:
		return Bidirectional, true
	default:
		return "", false
	}
}

// WritesTo reports whether a direction carries changes into the given side.
func WritesTo(direction string, side Side) bool {
	bit := bitToRight
	if side == Left {
		bit = bitToLeft
	}
	return directionBits[direction]&bit != 0
}

// Field is one canonical field bound to a path on each side.
type Field struct {
	Canonical string
	// Left and Right are dotted paths, so a connector whose records nest
	// values can be mapped without a bespoke adapter. Describe must report
	// the same dotted paths, which is the contract that lets Validate check
	// a mapping without knowing anything about the connector.
	Left      string
	Right     string
	Transform string
	// Direction is the effective direction, already narrowed against the
	// sync's own. It is never empty on a built Mapper.
	Direction string
}

// Path returns the field's path on one side.
func (f Field) Path(side Side) string {
	if side == Left {
		return f.Left
	}
	return f.Right
}

// Spec is the configuration a Mapper is built from.
//
// It is a plain struct rather than the config type on purpose: this package
// must not import config, so that config can import this one for the
// direction and transform vocabularies instead of restating them. One list,
// one place for it to be wrong.
type Spec struct {
	Kind      string
	Direction string
	Fields    []FieldSpec
}

// FieldSpec is one configured field pair.
type FieldSpec struct {
	Canonical string
	Left      string
	Right     string
	Transform string
	Direction string
}

// Mapper executes one sync's field map.
type Mapper struct {
	kind      string
	direction string
	fields    []Field
	byName    map[string]Field
}

// New builds a mapper from configuration.
//
// It fails rather than dropping anything it cannot make sense of, because a
// mapper that quietly ignored half its fields would produce a sync that looks
// like it is working.
func New(s Spec) (*Mapper, error) {
	if !IsValidDirection(s.Direction) {
		return nil, fmt.Errorf("mapping: unknown direction %q", s.Direction)
	}

	m := &Mapper{
		kind:      s.Kind,
		direction: s.Direction,
		byName:    make(map[string]Field, len(s.Fields)),
	}

	var errs []error
	for i, f := range s.Fields {
		where := fmt.Sprintf("fields[%d] (%s)", i, f.Canonical)

		if f.Canonical == "" {
			errs = append(errs, fmt.Errorf("%s: canonical name is empty", where))
			continue
		}
		if _, dup := m.byName[f.Canonical]; dup {
			errs = append(errs, fmt.Errorf("%s: %q is mapped twice", where, f.Canonical))
			continue
		}
		if f.Left == "" || f.Right == "" {
			errs = append(errs, fmt.Errorf("%s: both sides must name a field", where))
			continue
		}
		if !IsKnown(f.Transform) {
			errs = append(errs, fmt.Errorf("%s: unknown transform %q", where, f.Transform))
			continue
		}

		direction, ok := Narrow(s.Direction, f.Direction)
		if !ok {
			errs = append(errs, fmt.Errorf(
				"%s: direction %q cannot be combined with the sync's %q, so this field would never sync",
				where, f.Direction, s.Direction))
			continue
		}

		field := Field{
			Canonical: f.Canonical,
			Left:      f.Left,
			Right:     f.Right,
			Transform: f.Transform,
			Direction: direction,
		}
		m.fields = append(m.fields, field)
		m.byName[f.Canonical] = field
	}

	if len(m.fields) == 0 && len(errs) == 0 {
		errs = append(errs, errors.New("mapping: the sync maps no fields, so it would write nothing"))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

// Kind is the object type this mapper covers.
func (m *Mapper) Kind() string { return m.kind }

// Direction is the sync's direction.
func (m *Mapper) Direction() string { return m.direction }

// Fields returns the mapped fields in configured order.
func (m *Mapper) Fields() []Field {
	return append([]Field(nil), m.fields...)
}

// CanonicalNames returns the canonical field names, sorted.
//
// This is exactly the set the snapshot digest is computed over, which is what
// keeps an unmapped field churning on either peer from looking like a change.
func (m *Mapper) CanonicalNames() []string {
	names := make([]string, 0, len(m.fields))
	for _, f := range m.fields {
		names = append(names, f.Canonical)
	}
	sort.Strings(names)
	return names
}

// ToCanonical reads a record from one side into canonical fields.
//
// A field the record does not carry is left out rather than written as nil,
// so that "the peer did not return this field" stays distinguishable from
// "the peer says this field is empty". Conflict resolution needs that
// difference; a partial response would otherwise look like a deletion.
func (m *Mapper) ToCanonical(side Side, rec model.Record) (map[string]any, error) {
	out := make(map[string]any, len(m.fields))
	var errs []error

	for _, f := range m.fields {
		v, ok := getPath(rec.Fields, f.Path(side))
		if !ok {
			continue
		}
		tv, err := Apply(f.Transform, v)
		if err != nil {
			errs = append(errs, fmt.Errorf("field %q: %w", f.Canonical, err))
			continue
		}
		out[f.Canonical] = tv
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

// FromCanonical renders canonical fields into the field names of one side.
//
// Only fields whose effective direction writes into that side are included,
// and only fields the caller actually supplied. Everything else on the target
// record is left untouched.
func (m *Mapper) FromCanonical(side Side, canonical map[string]any) map[string]any {
	out := make(map[string]any)
	for _, f := range m.fields {
		if !WritesTo(f.Direction, side) {
			continue
		}
		v, ok := canonical[f.Canonical]
		if !ok {
			continue
		}
		setPath(out, f.Path(side), v)
	}
	return out
}

// WritableNames returns the canonical fields that may be written into a side.
func (m *Mapper) WritableNames(side Side) []string {
	var names []string
	for _, f := range m.fields {
		if WritesTo(f.Direction, side) {
			names = append(names, f.Canonical)
		}
	}
	sort.Strings(names)
	return names
}

// Validate checks the mapping against both peers' live schemas.
//
// This runs at startup and in doctor. It is the difference between a renamed
// property failing at boot with the field name in the message, and failing at
// three in the morning as a write that half-lands.
func (m *Mapper) Validate(left, right connector.Schema) error {
	var errs []error

	for side, schema := range map[Side]connector.Schema{Left: left, Right: right} {
		if schema.Kind != "" && schema.Kind != m.kind {
			errs = append(errs, fmt.Errorf(
				"%s: schema describes %q but the sync is for %q", side, schema.Kind, m.kind))
		}
		for _, f := range m.fields {
			path := f.Path(side)
			spec, ok := schema.Field(path)
			if !ok {
				errs = append(errs, fmt.Errorf(
					"field %q: %s has no field %q (it has %s)",
					f.Canonical, side, path, strings.Join(schema.Names(), ", ")))
				continue
			}
			// Writing a read-only field is rejected at startup rather than
			// discovered as a permanent write failure per record later.
			if spec.ReadOnly && WritesTo(f.Direction, side) {
				errs = append(errs, fmt.Errorf(
					"field %q: %s field %q is read-only, but the direction %q writes to it",
					f.Canonical, side, path, f.Direction))
			}
		}
	}

	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errors.Join(errs...)
}

// getPath reads a dotted path out of a record's fields.
func getPath(fields map[string]any, path string) (any, bool) {
	if fields == nil {
		return nil, false
	}
	// The flat case is the common one, and a connector is free to use a name
	// that contains a dot, so it wins over traversal.
	if v, ok := fields[path]; ok {
		return v, true
	}

	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		return nil, false
	}

	var cur any = fields
	for _, part := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// setPath writes a dotted path into a record's fields, creating the
// intermediate maps it needs.
func setPath(fields map[string]any, path string, v any) {
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		fields[path] = v
		return
	}

	cur := fields
	for _, part := range parts[:len(parts)-1] {
		next, ok := cur[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			cur[part] = next
		}
		cur = next
	}
	cur[parts[len(parts)-1]] = v
}
