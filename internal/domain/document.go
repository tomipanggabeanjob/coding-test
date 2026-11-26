package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// DefaultMaxAttempts is the retry budget: 1 initial attempt + 2 retries.
const DefaultMaxAttempts = 3

// Batch groups documents submitted together.
type Batch struct {
	ID        string    `json:"id"`
	BatchNo   string    `json:"batch_no"`
	CreatedAt time.Time `json:"created_at"`
}

// DocumentInput is what a client supplies when creating a batch.
type DocumentInput struct {
	ExternalRef string          `json:"external_ref"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// Document is a single unit of work sent to the Principal API.
type Document struct {
	ID           string          `json:"id"`
	BatchID      string          `json:"batch_id"`
	ExternalRef  string          `json:"external_ref"`
	Status       DocStatus       `json:"status"`
	AttemptCount int             `json:"attempt_count"`
	MaxAttempts  int             `json:"max_attempts"`
	LockedAt     *time.Time      `json:"locked_at,omitempty"`
	NextRetryAt  *time.Time      `json:"next_retry_at,omitempty"`
	LastError    string          `json:"last_error,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// RequestID is sent to Principal so it can deduplicate repeated submissions of
// the same attempt. Releasing a claim decrements AttemptCount, so a re-claimed
// document reuses the ID of the attempt that never completed.
func (d Document) RequestID() string {
	return RequestID(d.ID, d.AttemptCount)
}

// RequestID builds the deduplication key for a document attempt.
func RequestID(docID string, attempt int) string {
	return fmt.Sprintf("%s:%d", docID, attempt)
}

// Retryable reports whether a FAILED document still has attempts left.
func (d Document) Retryable() bool {
	return d.Status == StatusFailed && d.AttemptCount < d.MaxAttempts
}

// Callback is an asynchronous result reported by Principal.
type Callback struct {
	// CallbackID is the idempotency key assigned by Principal.
	CallbackID string          `json:"callback_id"`
	DocumentID string          `json:"document_id"`
	Result     DocStatus       `json:"result"`
	ReceivedAt time.Time       `json:"received_at"`
	RawBody    json.RawMessage `json:"-"`
}

// CallbackOutcome says what SaveCallbackAndApply did with a callback.
type CallbackOutcome string

const (
	// CallbackApplied: new callback that moved the document out of PROCESSING.
	CallbackApplied CallbackOutcome = "APPLIED"
	// CallbackDuplicate: the CallbackID was already seen; no effect at all.
	CallbackDuplicate CallbackOutcome = "DUPLICATE"
	// CallbackIgnored: new callback recorded for audit, but the document was
	// no longer PROCESSING (late callback), so nothing changed.
	CallbackIgnored CallbackOutcome = "IGNORED"
)

// BatchStatusView is a consistent snapshot of a batch's progress.
type BatchStatusView struct {
	BatchID string            `json:"batch_id"`
	BatchNo string            `json:"batch_no"`
	Status  BatchStatus       `json:"status"`
	Total   int               `json:"total"`
	Counts  map[DocStatus]int `json:"counts"`
	// RetryableFailed counts FAILED documents with attempt_count < max_attempts.
	RetryableFailed int `json:"retryable_failed"`
	// Started is true once any document has been claimed at least once.
	Started bool `json:"started"`
}
