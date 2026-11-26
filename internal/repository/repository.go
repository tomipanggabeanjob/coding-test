// Package repository defines the storage contract used by the service layer.
//
// Every method that changes a document is an atomic, guarded update: the
// guard (expected status, expected attempt) is evaluated inside the same
// critical section as the write, so callers never need check-then-act logic.
package repository

import (
	"context"
	"time"

	"bulkdoc/internal/domain"
)

type Repository interface {
	// CreateBatch stores the batch and all documents (PENDING, attempt 0,
	// max attempts domain.DefaultMaxAttempts) atomically.
	// Errors: domain.ErrBatchExists, domain.ErrInvalidInput.
	CreateBatch(ctx context.Context, batchNo string, docs []domain.DocumentInput) (domain.Batch, []domain.Document, error)

	// GetBatch returns domain.ErrBatchNotFound for unknown batches.
	GetBatch(ctx context.Context, batchID string) (domain.Batch, error)

	// ClaimPending atomically moves up to limit PENDING documents of the batch
	// (oldest first) to PROCESSING, increments attempt_count and sets locked_at.
	// Concurrent callers never receive the same document. Returns
	// domain.ErrBatchNotFound for unknown batches.
	ClaimPending(ctx context.Context, batchID string, limit int) ([]domain.Document, error)

	// ClaimDocument is ClaimPending for one specific document. It returns
	// domain.ErrDocumentNotFound or domain.ErrNotProcessable (not PENDING).
	ClaimDocument(ctx context.Context, docID string) (domain.Document, error)

	// MarkRequestAccepted records that Principal accepted the request: the
	// document stays PROCESSING and the lease restarts. It is a no-op unless the
	// document is still PROCESSING on the given attempt (a fast callback may
	// already have settled it).
	MarkRequestAccepted(ctx context.Context, docID string, attempt int) error

	// MarkRequestFailed moves a PROCESSING document (on the given attempt) to
	// FAILED. A positive retryAfter sets next_retry_at. No-op when the document
	// already left PROCESSING, so a result that arrived first is never overwritten.
	MarkRequestFailed(ctx context.Context, docID string, attempt int, reason string, retryAfter time.Duration) error

	// ReleaseClaim returns a PROCESSING document (on the given attempt) to
	// PENDING and gives the attempt back. Used when Submit never ran or was canceled.
	ReleaseClaim(ctx context.Context, docID string, attempt int) error

	// SaveCallbackAndApply is the single atomic place for callback idempotency
	// and the conditional status update:
	//   - unknown document: domain.ErrDocumentNotFound
	//   - CallbackID already stored: CallbackDuplicate, no effect
	//   - document PENDING: domain.ErrInvalidTransition, nothing stored
	//   - document PROCESSING: callback stored, document moved to cb.Result (CallbackApplied)
	//   - document SUCCESS or FAILED: callback stored for audit only (CallbackIgnored)
	SaveCallbackAndApply(ctx context.Context, cb domain.Callback) (domain.CallbackOutcome, error)

	// RetryFailed moves FAILED documents with attempt_count < max_attempts and
	// no pending backoff back to PENDING. Returns how many moved.
	RetryFailed(ctx context.Context, batchID string) (int, error)

	// RecoverStuck fails PROCESSING documents whose lease (locked_at) is older
	// than lease, so RetryFailed can pick them up. Returns how many moved.
	RecoverStuck(ctx context.Context, lease time.Duration) (int, error)

	// BatchStatus returns a consistent snapshot of the batch's progress.
	BatchStatus(ctx context.Context, batchID string) (domain.BatchStatusView, error)

	GetDocument(ctx context.Context, docID string) (domain.Document, error)

	// ListDocuments returns the batch's documents ordered by ID.
	ListDocuments(ctx context.Context, batchID string) ([]domain.Document, error)
}
