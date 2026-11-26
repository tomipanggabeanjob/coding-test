package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal"
	"golang.org/x/sync/errgroup"
)

const maxStoredErrorLen = 500

// ProcessBatch submits every PENDING document of the batch to Principal.
//
// It runs a small pool of workers; each worker atomically claims one document
// at a time and submits it. Real Submit concurrency is bounded by the
// Service-wide semaphore, so concurrent ProcessBatch calls on different (or the
// same) batches together never exceed the limit. ProcessBatch waits for every
// worker it started before returning, so no goroutine outlives the call.
//
// A failed Submit is recorded on the document (FAILED) and does not stop the
// batch. On cancellation or deadline the error wraps ctx.Err(); claimed but
// unsent documents are put back to PENDING.
func (s *Service) ProcessBatch(ctx context.Context, batchID string) error {
	if _, err := s.repo.GetBatch(ctx, batchID); err != nil {
		return fmt.Errorf("process batch %s: %w", batchID, err)
	}
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < s.workers; i++ {
		g.Go(func() error { return s.batchWorker(gctx, batchID) })
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("process batch %s: %w", batchID, err)
	}
	return nil
}

func (s *Service) batchWorker(ctx context.Context, batchID string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		docs, err := s.repo.ClaimPending(ctx, batchID, 1)
		if err != nil {
			return err
		}
		if len(docs) == 0 {
			return nil
		}
		if err := s.submit(ctx, docs[0]); err != nil {
			if errors.Is(err, domain.ErrSubmitFailed) {
				continue // recorded as FAILED; keep working through the batch
			}
			return err
		}
	}
}

// ProcessDocument claims one document by ID and submits it. Only a PENDING
// document can be claimed, so concurrent calls for the same ID result in exactly
// one Submit; the others get domain.ErrNotProcessable.
//
// When Submit fails the document is recorded as FAILED and the returned error
// wraps domain.ErrSubmitFailed (and the underlying cause).
func (s *Service) ProcessDocument(ctx context.Context, docID string) error {
	doc, err := s.repo.ClaimDocument(ctx, docID)
	if err != nil {
		return fmt.Errorf("process document %s: %w", docID, err)
	}
	return s.submit(ctx, doc)
}

// submit drives one claimed document through Submit and records the outcome.
func (s *Service) submit(ctx context.Context, doc domain.Document) error {
	sent, err := s.send(ctx, doc)

	// The semaphore is already released: repository writes below never hold it.
	switch {
	case !sent:
		s.releaseClaim(ctx, doc, "canceled before submit")
		return fmt.Errorf("process document %s: not submitted: %w", doc.ID, err)

	case err == nil:
		s.metrics.submits.Add(1)
		cctx, cancel := s.cleanupContext(ctx)
		defer cancel()
		if aerr := s.repo.MarkRequestAccepted(cctx, doc.ID, doc.AttemptCount); aerr != nil {
			return fmt.Errorf("process document %s: record accepted request: %w", doc.ID, aerr)
		}
		return nil

	case errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled):
		// The caller gave up mid-flight; the outcome at Principal is unknown. The
		// attempt is handed back so the next claim reuses the same RequestID and
		// Principal can deduplicate.
		s.releaseClaim(ctx, doc, "canceled during submit")
		return fmt.Errorf("process document %s: submit interrupted: %w", doc.ID, err)

	default:
		s.metrics.submits.Add(1)
		s.metrics.submitFailures.Add(1)
		cctx, cancel := s.cleanupContext(ctx)
		defer cancel()
		reason := truncate(err.Error(), maxStoredErrorLen)
		retryAfter := s.retryDelay(doc.AttemptCount)
		if ferr := s.repo.MarkRequestFailed(cctx, doc.ID, doc.AttemptCount, reason, retryAfter); ferr != nil {
			return fmt.Errorf("process document %s: record failed request: %w", doc.ID, errors.Join(ferr, err))
		}
		s.logger.WarnContext(ctx, "submit failed", "document_id", doc.ID, "attempt", doc.AttemptCount, "error", err)
		return fmt.Errorf("%w: document %s attempt %d: %w", domain.ErrSubmitFailed, doc.ID, doc.AttemptCount, err)
	}
}

// send takes a slot of the global semaphore, calls Submit and gives the slot
// back. sent reports whether Submit actually ran.
func (s *Service) send(ctx context.Context, doc domain.Document) (sent bool, err error) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-s.sem }()

	// select picks randomly when both cases are ready; never submit on a dead context.
	if err := ctx.Err(); err != nil {
		return false, err
	}

	callCtx := ctx
	if s.submitTimeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, s.submitTimeout)
		defer cancel()
	}
	s.metrics.inFlight.Add(1)
	defer s.metrics.inFlight.Add(-1)

	return true, s.principal.Submit(callCtx, principal.SubmitRequest{
		RequestID:   doc.RequestID(),
		DocumentID:  doc.ID,
		BatchID:     doc.BatchID,
		ExternalRef: doc.ExternalRef,
		Attempt:     doc.AttemptCount,
		Payload:     doc.Payload,
	})
}

// releaseClaim puts a claimed document back to PENDING using a context that
// survives the cancellation of ctx, so a canceled request never strands a
// document in PROCESSING.
func (s *Service) releaseClaim(ctx context.Context, doc domain.Document, why string) {
	cctx, cancel := s.cleanupContext(ctx)
	defer cancel()
	s.metrics.released.Add(1)
	if err := s.repo.ReleaseClaim(cctx, doc.ID, doc.AttemptCount); err != nil {
		s.logger.ErrorContext(ctx, "release claim failed; lease recovery will reclaim the document",
			"document_id", doc.ID, "reason", why, "error", err)
	}
}

// cleanupContext detaches from ctx's cancellation but keeps its values.
func (s *Service) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.cleanupTimeout)
}

func (s *Service) retryDelay(attempt int) time.Duration {
	if s.backoff == nil {
		return 0
	}
	return s.backoff(attempt)
}

// truncate cuts msg to at most n bytes without leaving a broken UTF-8 rune
// (Postgres rejects invalid UTF-8).
func truncate(msg string, n int) string {
	if len(msg) <= n {
		return msg
	}
	return strings.ToValidUTF8(msg[:n], "")
}
