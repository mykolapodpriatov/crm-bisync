package mapping

import (
	"fmt"
	"strings"
)

// Transform identifiers, as they appear in config.json.
const (
	TransformNone            = ""
	TransformEmailNormalize  = "email_normalize"
	TransformEmailFoldPlus   = "email_fold_plus"
	TransformDomainNormalize = "domain_normalize"
	TransformTrim            = "trim"
	TransformLowercase       = "lowercase"
)

// Known lists every transform, for config validation and for doctor.
func Known() []string {
	return []string{
		TransformNone,
		TransformEmailNormalize,
		TransformEmailFoldPlus,
		TransformDomainNormalize,
		TransformTrim,
		TransformLowercase,
	}
}

// IsKnown reports whether a transform identifier exists.
func IsKnown(name string) bool {
	for _, k := range Known() {
		if k == name {
			return true
		}
	}
	return false
}

// Apply runs a transform over a value.
//
// Transforms only ever touch strings. A transform named on a non-string field
// leaves the value alone rather than coercing it, because coercing a number to
// a string here would change the snapshot digest and make every poll look like
// a change.
func Apply(name string, v any) (any, error) {
	if name == TransformNone {
		return v, nil
	}
	if !IsKnown(name) {
		return nil, fmt.Errorf("unknown transform %q", name)
	}
	s, ok := v.(string)
	if !ok {
		return v, nil
	}

	switch name {
	case TransformTrim:
		return strings.TrimSpace(s), nil
	case TransformLowercase:
		return strings.ToLower(strings.TrimSpace(s)), nil
	case TransformEmailNormalize:
		return NormalizeEmail(s), nil
	case TransformEmailFoldPlus:
		return FoldPlusAddress(NormalizeEmail(s)), nil
	case TransformDomainNormalize:
		return NormalizeDomain(s), nil
	default:
		return nil, fmt.Errorf("unknown transform %q", name)
	}
}

// NormalizeEmail canonicalises an address for comparison.
//
// It trims, lower-cases, and drops the trailing dot a fully qualified domain
// may carry. It deliberately does NOT fold plus-addressing or strip dots from
// the local part.
//
// Those two are the famous Gmail rules, and they are Gmail's, not the
// internet's. "ann+crm@example.com" and "ann@example.com" are two different
// mailboxes at most providers, and this function feeds deterministic identity
// matching: folding them would silently merge two real people into one record,
// which is not something anyone can un-merge from a log. A site that knows its
// user base is Gmail can opt in with the email_fold_plus transform.
//
// The local part is lower-cased even though RFC 5321 says it is technically
// case-sensitive, because no CRM in practice treats Ann@ and ann@ as two
// contacts, and refusing to match them would create duplicates on every import.
func NormalizeEmail(s string) string {
	s = strings.TrimSpace(s)
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return strings.ToLower(s)
	}
	local, domain := s[:at], s[at+1:]
	return strings.ToLower(local) + "@" + NormalizeDomain(domain)
}

// FoldPlusAddress drops a plus tag from the local part. See NormalizeEmail for
// why this is opt-in rather than part of normalisation.
func FoldPlusAddress(s string) string {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	local, domain := s[:at], s[at+1:]
	if plus := strings.Index(local, "+"); plus >= 0 {
		local = local[:plus]
	}
	return local + "@" + domain
}

// NormalizeDomain canonicalises a hostname for comparison: trimmed,
// lower-cased, without a scheme, a path, a leading "www." or a trailing dot.
//
// Company matching runs on this, and a CRM will happily hold
// "https://www.Example.com/" and "example.com" as two accounts that are one
// company.
func NormalizeDomain(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))

	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		// A userinfo prefix, as in user@host. Keep the host.
		s = s[i+1:]
	}
	if i := strings.Index(s, ":"); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSuffix(s, ".")
	s = strings.TrimPrefix(s, "www.")
	return s
}
