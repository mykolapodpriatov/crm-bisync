package mapping_test

import (
	"testing"

	"crm-bisync/internal/mapping"
)

func TestNormalizeEmail(t *testing.T) {
	cases := map[string]string{
		"  Ann@Example.COM  ": "ann@example.com",
		"ann@example.com.":    "ann@example.com",
		"ann@WWW.Example.com": "ann@example.com",
		"ann":                 "ann",
		"":                    "",
		// The local part keeps its dots and its plus tag. See below.
		"a.n.n+crm@example.com": "a.n.n+crm@example.com",
		// Only the last @ separates local from domain.
		"weird@name@example.com": "weird@name@example.com",
	}
	for in, want := range cases {
		if got := mapping.NormalizeEmail(in); got != want {
			t.Errorf("NormalizeEmail(%q) = %q, want %q", in, got, want)
		}
	}
}

// Folding plus-addressing is Gmail's rule, not the internet's, and this
// function feeds identity matching. Folding by default would merge two real
// people into one record, which is not recoverable from a log.
func TestPlusAddressingIsNotFoldedByDefault(t *testing.T) {
	tagged := mapping.NormalizeEmail("ann+crm@example.com")
	plain := mapping.NormalizeEmail("ann@example.com")

	if tagged == plain {
		t.Fatal("normalisation folded a plus tag, which would merge two mailboxes")
	}

	// A site that knows its users are on Gmail can opt in.
	if mapping.FoldPlusAddress(tagged) != plain {
		t.Fatalf("FoldPlusAddress(%q) = %q, want %q",
			tagged, mapping.FoldPlusAddress(tagged), plain)
	}
}

func TestNormalizeDomain(t *testing.T) {
	cases := map[string]string{
		"  Example.COM  ":            "example.com",
		"www.example.com":            "example.com",
		"https://www.Example.com/":   "example.com",
		"http://example.com/pricing": "example.com",
		"example.com.":               "example.com",
		"example.com:8443":           "example.com",
		"user@example.com":           "example.com",
		"example.com?utm=1":          "example.com",
		"":                           "",
	}
	for in, want := range cases {
		if got := mapping.NormalizeDomain(in); got != want {
			t.Errorf("NormalizeDomain(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestApplyTransforms(t *testing.T) {
	cases := []struct {
		transform string
		in, want  any
	}{
		{mapping.TransformNone, "  As Is  ", "  As Is  "},
		{mapping.TransformTrim, "  padded  ", "padded"},
		{mapping.TransformLowercase, "  SHOUT  ", "shout"},
		{mapping.TransformEmailNormalize, " Ann@Example.com ", "ann@example.com"},
		{mapping.TransformEmailFoldPlus, "Ann+crm@Example.com", "ann@example.com"},
		{mapping.TransformDomainNormalize, "https://WWW.example.com/x", "example.com"},
	}
	for _, c := range cases {
		got, err := mapping.Apply(c.transform, c.in)
		if err != nil {
			t.Fatalf("Apply(%s): %v", c.transform, err)
		}
		if got != c.want {
			t.Errorf("Apply(%s, %v) = %v, want %v", c.transform, c.in, got, c.want)
		}
	}
}

// A transform named on a numeric field must leave it alone. Coercing it to a
// string would change the snapshot digest and make every poll look like a
// change.
func TestApplyLeavesNonStringsAlone(t *testing.T) {
	got, err := mapping.Apply(mapping.TransformLowercase, 42)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got != 42 {
		t.Fatalf("Apply turned 42 into %v", got)
	}
}

func TestApplyRejectsUnknownTransform(t *testing.T) {
	if _, err := mapping.Apply("shout", "x"); err == nil {
		t.Fatal("Apply accepted an unknown transform")
	}
	if mapping.IsKnown("shout") {
		t.Fatal("IsKnown accepted an unknown transform")
	}
	if !mapping.IsKnown(mapping.TransformNone) {
		t.Fatal("the empty transform should be known")
	}
}
