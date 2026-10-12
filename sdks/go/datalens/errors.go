package datalens

import (
	"fmt"
	"time"
)

type ErrorType string

const (
	// APIError means the server answered with an error status.
	APIError ErrorType = "api"
	// HTTPError means the request did not complete.
	HTTPError ErrorType = "http"
	// DecodeError means a success body was not the expected JSON.
	DecodeError ErrorType = "decode"
	// InconsistentError means pages of one query disagreed on a block hash.
	InconsistentError ErrorType = "inconsistent"
)

// Error is every failure this package returns.
type Error struct {
	Type ErrorType
	// Kind is the stable error.kind, such as forbidden, or http for a body that is not a
	// datalens error.
	Kind       string
	Message    string
	Status     int
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string {
	switch e.Type {
	case APIError:
		return fmt.Sprintf("datalens %d %s: %s", e.Status, e.Kind, e.Message)
	default:
		return fmt.Sprintf("datalens %s: %s", e.Type, e.Message)
	}
}

func (e *Error) Unwrap() error { return e.Err }

// Retryable reports rate limits (429), server errors, transport failures, and a page set
// that disagreed on a block hash. 400 and 403 are not retried.
func (e *Error) Retryable() bool {
	switch e.Type {
	case APIError:
		return e.Status == 429 || e.Status >= 500
	case HTTPError, InconsistentError:
		return true
	default:
		return false
	}
}
