package mapping_test

import (
	"reflect"
	"strings"
	"testing"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/mapping"
	"crm-bisync/internal/model"
)

func spec(direction string, fields ...mapping.FieldSpec) mapping.Spec {
	return mapping.Spec{Kind: "contact", Direction: direction, Fields: fields}
}

func contactSpec() mapping.Spec {
	return spec(mapping.Bidirectional,
		mapping.FieldSpec{
			Canonical: "email", Left: "email", Right: "emails.primaryEmail",
			Transform: mapping.TransformEmailNormalize,
		},
		mapping.FieldSpec{Canonical: "first_name", Left: "firstname", Right: "name.firstName"},
		mapping.FieldSpec{
			Canonical: "created_at", Left: "createdate", Right: "createdAt",
			Direction: mapping.LeftToRight,
		},
	)
}

func TestNarrow(t *testing.T) {
	cases := []struct {
		sync, field string
		want        string
		ok          bool
	}{
		{mapping.Bidirectional, "", mapping.Bidirectional, true},
		{mapping.LeftToRight, "", mapping.LeftToRight, true},
		{mapping.Bidirectional, mapping.LeftToRight, mapping.LeftToRight, true},
		{mapping.Bidirectional, mapping.RightToLeft, mapping.RightToLeft, true},
		{mapping.LeftToRight, mapping.LeftToRight, mapping.LeftToRight, true},
		// A field may narrow, never widen.
		{mapping.LeftToRight, mapping.Bidirectional, mapping.LeftToRight, true},
		{mapping.RightToLeft, mapping.Bidirectional, mapping.RightToLeft, true},
		// And never contradict: nothing left to sync is an error.
		{mapping.LeftToRight, mapping.RightToLeft, "", false},
		{mapping.RightToLeft, mapping.LeftToRight, "", false},
		{mapping.Bidirectional, "sideways", "", false},
		{"sideways", "", "", false},
	}
	for _, c := range cases {
		got, ok := mapping.Narrow(c.sync, c.field)
		if got != c.want || ok != c.ok {
			t.Errorf("Narrow(%q, %q) = %q,%v, want %q,%v",
				c.sync, c.field, got, ok, c.want, c.ok)
		}
	}
}

func TestWritesTo(t *testing.T) {
	if !mapping.WritesTo(mapping.LeftToRight, mapping.Right) {
		t.Error("left_to_right should write to the right")
	}
	if mapping.WritesTo(mapping.LeftToRight, mapping.Left) {
		t.Error("left_to_right must not write to the left")
	}
	if !mapping.WritesTo(mapping.Bidirectional, mapping.Left) ||
		!mapping.WritesTo(mapping.Bidirectional, mapping.Right) {
		t.Error("bidirectional should write to both sides")
	}
	if mapping.WritesTo("sideways", mapping.Left) {
		t.Error("an unknown direction must not grant permission to write")
	}
}

func TestNewRejectsBadSpecsAndReportsAllOfThem(t *testing.T) {
	_, err := mapping.New(spec(mapping.LeftToRight,
		mapping.FieldSpec{Canonical: "", Left: "a", Right: "b"},
		mapping.FieldSpec{Canonical: "email", Left: "", Right: "b"},
		mapping.FieldSpec{Canonical: "name", Left: "a", Right: "b", Transform: "shout"},
		mapping.FieldSpec{Canonical: "phone", Left: "a", Right: "b", Direction: mapping.RightToLeft},
	))
	if err == nil {
		t.Fatal("New accepted a broken spec")
	}
	for _, want := range []string{"canonical name is empty", "both sides", "unknown transform", "never sync"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

func TestNewRejectsDuplicateCanonicalField(t *testing.T) {
	_, err := mapping.New(spec(mapping.Bidirectional,
		mapping.FieldSpec{Canonical: "email", Left: "email", Right: "a"},
		mapping.FieldSpec{Canonical: "email", Left: "work_email", Right: "b"},
	))
	if err == nil || !strings.Contains(err.Error(), "mapped twice") {
		t.Fatalf("want a duplicate error, got %v", err)
	}
}

func TestNewRejectsAnEmptyFieldList(t *testing.T) {
	if _, err := mapping.New(spec(mapping.Bidirectional)); err == nil {
		t.Fatal("New accepted a sync that maps nothing")
	}
}

func TestToCanonicalAppliesTransformsAndNestedPaths(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := m.ToCanonical(mapping.Right, model.Record{
		Kind: "contact",
		Fields: map[string]any{
			"emails":    map[string]any{"primaryEmail": "  Ann@Example.COM "},
			"name":      map[string]any{"firstName": "Ann"},
			"createdAt": "2026-01-01",
			// An unmapped field must not appear in the canonical form.
			"lastActivityAt": "2026-09-01",
		},
	})
	if err != nil {
		t.Fatalf("ToCanonical: %v", err)
	}

	want := map[string]any{
		"email":      "ann@example.com",
		"first_name": "Ann",
		"created_at": "2026-01-01",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ToCanonical = %#v, want %#v", got, want)
	}
}

// A field the peer did not return has to stay absent rather than become nil.
// Conflict resolution reads the difference: a partial response would otherwise
// look exactly like the peer clearing a value.
func TestToCanonicalOmitsFieldsTheRecordDoesNotCarry(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := m.ToCanonical(mapping.Left, model.Record{
		Kind:   "contact",
		Fields: map[string]any{"email": "ann@example.com"},
	})
	if err != nil {
		t.Fatalf("ToCanonical: %v", err)
	}
	if _, present := got["first_name"]; present {
		t.Fatal("a field the record did not carry appeared in the canonical form")
	}
	if len(got) != 1 {
		t.Fatalf("got %d canonical fields, want 1: %#v", len(got), got)
	}
}

func TestFromCanonicalBuildsNestedPaths(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := m.FromCanonical(mapping.Right, map[string]any{
		"email":      "ann@example.com",
		"first_name": "Ann",
		"created_at": "2026-01-01",
	})

	want := map[string]any{
		"emails":    map[string]any{"primaryEmail": "ann@example.com"},
		"name":      map[string]any{"firstName": "Ann"},
		"createdAt": "2026-01-01",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FromCanonical = %#v, want %#v", got, want)
	}
}

// created_at is left_to_right, so it must never be written back to the left.
// A read-only remote value coming home would overwrite the real creation date
// with whatever the peer believes it is.
func TestFromCanonicalHonoursPerFieldDirection(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := m.FromCanonical(mapping.Left, map[string]any{
		"email":      "ann@example.com",
		"created_at": "2026-01-01",
	})

	if _, present := got["createdate"]; present {
		t.Fatal("a left_to_right field was written back to the left")
	}
	if got["email"] != "ann@example.com" {
		t.Fatalf("email was not written: %#v", got)
	}
}

func TestFromCanonicalOnlyWritesSuppliedFields(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := m.FromCanonical(mapping.Right, map[string]any{"first_name": "Ann"})
	if len(got) != 1 {
		t.Fatalf("FromCanonical invented fields: %#v", got)
	}
}

func TestCanonicalAndWritableNames(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if want := []string{"created_at", "email", "first_name"}; !reflect.DeepEqual(m.CanonicalNames(), want) {
		t.Fatalf("CanonicalNames = %v, want %v", m.CanonicalNames(), want)
	}
	if want := []string{"created_at", "email", "first_name"}; !reflect.DeepEqual(m.WritableNames(mapping.Right), want) {
		t.Fatalf("WritableNames(right) = %v, want %v", m.WritableNames(mapping.Right), want)
	}
	if want := []string{"email", "first_name"}; !reflect.DeepEqual(m.WritableNames(mapping.Left), want) {
		t.Fatalf("WritableNames(left) = %v, want %v", m.WritableNames(mapping.Left), want)
	}
}

func schema(kind string, fields ...connector.FieldSpec) connector.Schema {
	return connector.Schema{Kind: kind, Fields: fields}
}

func TestValidateAcceptsAMappingThatMatchesBothSchemas(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	left := schema("contact",
		connector.FieldSpec{Name: "email"},
		connector.FieldSpec{Name: "firstname"},
		connector.FieldSpec{Name: "createdate", ReadOnly: true},
	)
	right := schema("contact",
		connector.FieldSpec{Name: "emails.primaryEmail"},
		connector.FieldSpec{Name: "name.firstName"},
		connector.FieldSpec{Name: "createdAt"},
	)

	if err := m.Validate(left, right); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// This is the rename that would otherwise break a write at three in the
// morning. It should fail at startup, naming the field and the side.
func TestValidateReportsAMissingField(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	left := schema("contact",
		connector.FieldSpec{Name: "email"},
		connector.FieldSpec{Name: "first_name"},
		connector.FieldSpec{Name: "createdate"},
	)
	right := schema("contact",
		connector.FieldSpec{Name: "emails.primaryEmail"},
		connector.FieldSpec{Name: "name.firstName"},
		connector.FieldSpec{Name: "createdAt"},
	)

	err = m.Validate(left, right)
	if err == nil {
		t.Fatal("Validate accepted a mapping naming a field the peer does not have")
	}
	for _, want := range []string{"first_name", "left", "firstname"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

// A read-only field that the direction writes to is a permanent write failure
// per record. Catching it once at startup is cheaper than in every DLQ entry.
func TestValidateRejectsWritingAReadOnlyField(t *testing.T) {
	m, err := mapping.New(spec(mapping.Bidirectional,
		mapping.FieldSpec{Canonical: "created_at", Left: "createdate", Right: "createdAt"},
	))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	left := schema("contact", connector.FieldSpec{Name: "createdate"})
	right := schema("contact", connector.FieldSpec{Name: "createdAt", ReadOnly: true})

	err = m.Validate(left, right)
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("want a read-only error, got %v", err)
	}
}

// The same field being read-only is fine when nothing writes to it, which is
// exactly what a per-field direction is for.
func TestValidateAllowsReadingAReadOnlyField(t *testing.T) {
	m, err := mapping.New(spec(mapping.Bidirectional,
		mapping.FieldSpec{
			Canonical: "created_at", Left: "createdate", Right: "createdAt",
			Direction: mapping.RightToLeft,
		},
	))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	left := schema("contact", connector.FieldSpec{Name: "createdate"})
	right := schema("contact", connector.FieldSpec{Name: "createdAt", ReadOnly: true})

	if err := m.Validate(left, right); err != nil {
		t.Fatalf("Validate rejected a read-only field that is only ever read: %v", err)
	}
}

func TestValidateReportsAKindMismatch(t *testing.T) {
	m, err := mapping.New(contactSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = m.Validate(schema("company"), schema("contact"))
	if err == nil || !strings.Contains(err.Error(), "company") {
		t.Fatalf("want a kind mismatch error, got %v", err)
	}
}

// A connector is free to use a field name containing a dot. The flat lookup
// must win over path traversal, or such a field becomes unreachable.
func TestFlatFieldNameWithADotBeatsTraversal(t *testing.T) {
	m, err := mapping.New(spec(mapping.Bidirectional,
		mapping.FieldSpec{Canonical: "email", Left: "properties.email", Right: "email"},
	))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := m.ToCanonical(mapping.Left, model.Record{
		Fields: map[string]any{"properties.email": "ann@example.com"},
	})
	if err != nil {
		t.Fatalf("ToCanonical: %v", err)
	}
	if got["email"] != "ann@example.com" {
		t.Fatalf("a flat field whose name contains a dot was unreachable: %#v", got)
	}
}
