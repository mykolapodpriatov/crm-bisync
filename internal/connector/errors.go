package connector

import (
	"errors"
	"fmt"
	"time"
)

// Kind classifies a connector failure so the engine can decide what to do
// without parsing error strings or knowing which CRM produced it.
type Kind int

const (
	// KindUnknown is an unclassified failure. Treated as transient, because
	// giving up on an unknown error loses data and retrying it does not.
	KindUnknown Kind = iota
	// KindTransient is a network failure or a 5xx: retry with backoff.
	KindTransient
	// KindRateLimited means back off for RetryAfter and resume the same batch.
	KindRateLimited
	// KindPermanent is a request the peer will never accept, such as a value
	// that fails a validation rule. Retrying is pointless; it goes to the DLQ.
	KindPermanent
	// KindNotFound means the record is gone.
	KindNotFound
	// KindAuth means the credentials are wrong or expired. Retrying makes it
	// worse, and it needs a human, so it is surfaced rather than buried.
	KindAuth
)

// String renders the kind for logs and metrics labels.
func (k Kind) String() string {
	switch k {
	case KindTransient:
		return "transient"
	case KindRateLimited:
		return "rate_limited"
	case KindPermanent:
		return "permanent"
	case KindNotFound:
		return "not_found"
	case KindAuth:
		return "auth"
	default:
		return "unknown"
	}
}

// Error is a classified connector failure.
//
// Returning one is a statement that the peer answered and the answer was not
// success, which means the request was not applied. A connector that does not
// know whether the request was applied, because it timed out or the connection
// dropped after the bytes went out, must return the raw error instead. The
// engine reads the difference: an answered failure clears the in-flight mark
// and retries cleanly, while an unanswered one keeps it and forces identity to
// be re-resolved, because a blind retry of a create that may have landed is
// how a dropped connection becomes a duplicate contact.
type Error struct {
	Connector string
	Op        string
	Kind      Kind
	// RetryAfter is set on KindRateLimited when the peer told us how long to
	// wait. Zero means it did not, and the engine falls back to backoff.
	RetryAfter time.Duration
	Err        error
}

// Errorf builds a classified error.
func Errorf(conn, op string, kind Kind, format string, args ...any) *Error {
	return &Error{Connector: conn, Op: op, Kind: kind, Err: fmt.Errorf(format, args...)}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s.%s: %s: %v", e.Connector, e.Op, e.Kind, e.Err)
}

// Unwrap exposes the cause.
func (e *Error) Unwrap() error { return e.Err }

// KindOf classifies any error. An unclassified error is transient, so an
// unexpected failure is retried rather than silently dropped.
func KindOf(err error) Kind {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind
	}
	if err == nil {
		return KindUnknown
	}
	return KindTransient
}

// RetryAfterOf reports the peer-supplied wait, or zero.
func RetryAfterOf(err error) time.Duration {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.RetryAfter
	}
	return 0
}

// Retryable reports whether trying again could plausibly succeed.
func Retryable(err error) bool {
	switch KindOf(err) {
	case KindTransient, KindRateLimited, KindUnknown:
		return true
	default:
		return false
	}
}

// IsNotFound is a convenience for the common branch.
func IsNotFound(err error) bool { return KindOf(err) == KindNotFound }

// Answered reports whether the peer replied.
//
// True means the request was not applied and may be retried from scratch.
// False means the outcome is unknown, and a write must not be repeated
// without first re-establishing what the peer holds.
func Answered(err error) bool {
	var ce *Error
	return errors.As(err, &ce)
}
