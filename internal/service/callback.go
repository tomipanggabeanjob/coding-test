package service

import (
	"context"
	"fmt"
	"strings"

	"bulkdoc/internal/domain"
)

// HandleCallback validates and applies an asynchronous result from Principal.
//
// Idempotency and the "SUCCESS is terminal" rule are enforced by the
// repository inside one atomic section (SaveCallbackAndApply), not by
// application timing:
//   - the same CallbackID again is a no-op reported as CallbackDuplicate (nil error);
//   - a callback for a document that is no longer PROCESSING (a late FAILED after
//     SUCCESS, for instance) is stored for audit and ignored (CallbackIgnored);
//   - a callback for a PENDING document should never happen and is rejected with
//     domain.ErrInvalidTransition;
//   - an unknown document yields domain.ErrDocumentNotFound.
func (s *Service) HandleCallback(ctx context.Context, cb domain.Callback) (domain.CallbackOutcome, error) {
	if strings.TrimSpace(cb.CallbackID) == "" {
		return "", fmt.Errorf("%w: callback_id is required", domain.ErrInvalidInput)
	}
	if strings.TrimSpace(cb.DocumentID) == "" {
		return "", fmt.Errorf("%w: document_id is required", domain.ErrInvalidInput)
	}
	if cb.Result != domain.StatusSuccess && cb.Result != domain.StatusFailed {
		return "", fmt.Errorf("%w: result must be SUCCESS or FAILED, got %q", domain.ErrInvalidInput, cb.Result)
	}

	outcome, err := s.repo.SaveCallbackAndApply(ctx, cb)
	if err != nil {
		return "", fmt.Errorf("handle callback %q: %w", cb.CallbackID, err)
	}
	switch outcome {
	case domain.CallbackApplied:
		s.metrics.callbacksApplied.Add(1)
	case domain.CallbackDuplicate:
		s.metrics.callbacksDuplicate.Add(1)
	case domain.CallbackIgnored:
		s.metrics.callbacksIgnored.Add(1)
	}
	s.logger.InfoContext(ctx, "callback handled",
		"callback_id", cb.CallbackID, "document_id", cb.DocumentID, "result", cb.Result, "outcome", outcome)
	return outcome, nil
}
