package service

import (
	"context"
	"fmt"
	"time"
)

// RetryFailed moves FAILED documents of the batch that still have attempts left
// (attempt_count < max_attempts) and no pending backoff back to PENDING, and
// returns how many moved. Documents out of attempts stay FAILED for good.
func (s *Service) RetryFailed(ctx context.Context, batchID string) (int, error) {
	n, err := s.repo.RetryFailed(ctx, batchID)
	if err != nil {
		return 0, fmt.Errorf("retry failed documents of batch %s: %w", batchID, err)
	}
	s.logger.InfoContext(ctx, "failed documents re-queued", "batch_id", batchID, "count", n)
	return n, nil
}

// RecoverStuck fails PROCESSING documents whose lease is older than lease
// (callback never arrived, or the process died between claim and Submit), so
// RetryFailed can re-queue them while attempts remain.
func (s *Service) RecoverStuck(ctx context.Context, lease time.Duration) (int, error) {
	n, err := s.repo.RecoverStuck(ctx, lease)
	if err != nil {
		return 0, fmt.Errorf("recover stuck documents: %w", err)
	}
	if n > 0 {
		s.logger.WarnContext(ctx, "expired leases recovered", "count", n, "lease", lease)
	}
	return n, nil
}
