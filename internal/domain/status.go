package domain

// DocStatus is the lifecycle state of a single document.
type DocStatus string

const (
	StatusPending    DocStatus = "PENDING"
	StatusProcessing DocStatus = "PROCESSING"
	StatusSuccess    DocStatus = "SUCCESS"
	StatusFailed     DocStatus = "FAILED"
)

// AllDocStatuses lists every status in a stable order.
func AllDocStatuses() []DocStatus {
	return []DocStatus{StatusPending, StatusProcessing, StatusSuccess, StatusFailed}
}

// Valid reports whether s is one of the four known statuses.
func (s DocStatus) Valid() bool {
	switch s {
	case StatusPending, StatusProcessing, StatusSuccess, StatusFailed:
		return true
	}
	return false
}

// CanTransition encodes the state machine. SUCCESS is terminal.
//
//	PENDING    -> PROCESSING  worker claims the document
//	PROCESSING -> SUCCESS     callback SUCCESS
//	PROCESSING -> FAILED      callback FAILED, Submit error, lease expiry
//	PROCESSING -> PENDING     context canceled before/while submitting (claim released)
//	FAILED     -> PENDING     RetryFailed while attempts remain
func CanTransition(from, to DocStatus) bool {
	switch from {
	case StatusPending:
		return to == StatusProcessing
	case StatusProcessing:
		return to == StatusSuccess || to == StatusFailed || to == StatusPending
	case StatusFailed:
		return to == StatusPending
	}
	return false
}

// BatchStatus is the derived status of a batch.
type BatchStatus string

const (
	BatchPending    BatchStatus = "PENDING"
	BatchInProgress BatchStatus = "IN_PROGRESS"
	BatchCompleted  BatchStatus = "COMPLETED"
)

// DeriveBatchStatus computes the batch status from a consistent snapshot:
//   - COMPLETED: no PENDING, no PROCESSING and no retryable FAILED documents.
//   - PENDING: nothing has been started yet (every document PENDING, no attempts).
//   - IN_PROGRESS: anything else.
func DeriveBatchStatus(v BatchStatusView) BatchStatus {
	if v.Counts[StatusPending] == 0 && v.Counts[StatusProcessing] == 0 && v.RetryableFailed == 0 {
		return BatchCompleted
	}
	if !v.Started && v.Counts[StatusPending] == v.Total {
		return BatchPending
	}
	return BatchInProgress
}
