// Package domain holds the entities, statuses, state machine and sentinel
// errors of the bulk document processing service. It depends on nothing else.
package domain

import "errors"

// Sentinel errors. Callers wrap them with %w and tests assert with errors.Is.
var ErrInvalidInput = errors.New("invalid input")
var ErrBatchNotFound = errors.New("batch not found")
var ErrDocumentNotFound = errors.New("document not found")
var ErrBatchExists = errors.New("batch already exists")

// ErrNotProcessable is returned when a document cannot be claimed because it
// is not PENDING (already claimed, finished or out of attempts).
var ErrNotProcessable = errors.New("document is not processable")

// ErrInvalidTransition is returned when an operation would move a document
// through a transition the state machine forbids.
var ErrInvalidTransition = errors.New("invalid status transition")

// ErrSubmitFailed marks a document whose Submit call failed; the document has
// already been recorded as FAILED when this error is returned.
var ErrSubmitFailed = errors.New("submit to principal failed")
