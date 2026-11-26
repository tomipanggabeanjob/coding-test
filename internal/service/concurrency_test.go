package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/service"

	"go.uber.org/goleak"
)

// Test plan item 1: never more than 3 Submit calls in flight.
func TestProcessBatch_GlobalConcurrencyLimit(t *testing.T) {
	const docsInBatch = 25
	// 10 workers would happily submit 10 documents at once; only the shared
	// semaphore keeps the in-flight count at 3.
	mock := principaltest.New(principaltest.WithBarrier(3))
	svc, repo := newService(t, mock, service.WithBatchWorkers(10))
	b, docs := createBatch(t, svc, "B1", docsInBatch)

	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessBatch(context.Background(), b.ID) }()

	waitClosed(t, mock.Reached(), "3 concurrent Submit calls")
	mock.Release()
	if err := waitErr(t, errc, "ProcessBatch"); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	if got := mock.MaxInFlight(); got > 3 {
		t.Fatalf("max in flight = %d, limit is 3", got)
	}
	if got := mock.MaxInFlight(); got != 3 {
		t.Fatalf("max in flight = %d, want exactly 3 (limit never reached)", got)
	}
	if got := mock.TotalCalls(); got != docsInBatch {
		t.Fatalf("Submit called %d times, want %d", got, docsInBatch)
	}
	for _, d := range docs {
		if got := getDoc(t, repo, d.ID); got.Status != domain.StatusProcessing || got.AttemptCount != 1 {
			t.Errorf("document %s = %s/%d, want PROCESSING/1 (accepted, waiting for callback)", d.ID, got.Status, got.AttemptCount)
		}
	}
	if m := svc.Metrics(); m.InFlight != 0 || m.Submits != docsInBatch {
		t.Errorf("metrics = %+v", m)
	}
}

// Test plan item 1b: the limit is shared by concurrent ProcessBatch calls.
func TestConcurrentBatches_SharedLimit(t *testing.T) {
	mock := principaltest.New(principaltest.WithBarrier(3))
	svc, _ := newService(t, mock)
	b1, _ := createBatch(t, svc, "B1", 15)
	b2, _ := createBatch(t, svc, "B2", 15)

	errc := make(chan error, 2)
	for _, id := range []string{b1.ID, b2.ID} {
		go func() { errc <- svc.ProcessBatch(context.Background(), id) }()
	}

	waitClosed(t, mock.Reached(), "3 concurrent Submit calls")
	mock.Release()
	for i := 0; i < 2; i++ {
		if err := waitErr(t, errc, "ProcessBatch"); err != nil {
			t.Fatalf("ProcessBatch: %v", err)
		}
	}
	if got := mock.MaxInFlight(); got != 3 {
		t.Fatalf("max in flight across two batches = %d, want 3", got)
	}
	if got := mock.TotalCalls(); got != 30 {
		t.Fatalf("Submit called %d times, want 30", got)
	}
}

func TestWithMaxInFlight_ConfigurableLimit(t *testing.T) {
	mock := principaltest.New(principaltest.WithBarrier(5))
	svc, _ := newService(t, mock, service.WithMaxInFlight(5))
	b, _ := createBatch(t, svc, "B1", 20)

	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessBatch(context.Background(), b.ID) }()
	waitClosed(t, mock.Reached(), "5 concurrent Submit calls")
	mock.Release()
	if err := waitErr(t, errc, "ProcessBatch"); err != nil {
		t.Fatal(err)
	}
	if got := mock.MaxInFlight(); got != 5 {
		t.Fatalf("max in flight = %d, want 5", got)
	}
}

// The service must never hold a lock (or the semaphore's slot count) in a way
// that blocks other use cases while Submit is running.
func TestSubmit_DoesNotBlockOtherOperations(t *testing.T) {
	mock := principaltest.New(principaltest.WithBarrier(1))
	svc, _ := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 2)
	ctx := context.Background()

	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessDocument(ctx, docs[0].ID) }()
	waitClosed(t, mock.Reached(), "Submit to be in flight")

	// Submit is blocked right now. Everything else must still make progress.
	done := make(chan error, 1)
	go func() {
		if _, err := svc.GetBatchStatus(ctx, b.ID); err != nil {
			done <- err
			return
		}
		if _, err := svc.RetryFailed(ctx, b.ID); err != nil {
			done <- err
			return
		}
		_, _, err := svc.CreateBatch(ctx, "B2", []domain.DocumentInput{{ExternalRef: "x"}})
		done <- err
	}()
	if err := waitErr(t, done, "operations during a blocked Submit"); err != nil {
		t.Fatalf("operation failed while Submit was in flight: %v", err)
	}

	mock.Release()
	if err := waitErr(t, errc, "ProcessDocument"); err != nil {
		t.Fatal(err)
	}
}

// Test plan item 6: no data races across mixed concurrent operations. The test
// body checks invariants; the race detector does the rest.
func TestMixedOperations_Race(t *testing.T) {
	const (
		batches    = 3
		docsPerB   = 10
		iterations = 20
	)
	mock := principaltest.New(principaltest.WithBehavior(principaltest.FailFirst(1)))
	svc, repo := newService(t, mock)
	ctx := context.Background()

	type created struct {
		batch domain.Batch
		docs  []domain.Document
	}
	var all []created
	for i := 0; i < batches; i++ {
		b, docs := createBatch(t, svc, fmt.Sprintf("B%d", i), docsPerB)
		all = append(all, created{b, docs})
	}

	var wg sync.WaitGroup
	errs := make(chan error, 1024)
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}

	for _, c := range all {
		wg.Add(3)
		go func() { // processor
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := svc.ProcessBatch(ctx, c.batch.ID); err != nil {
					report(fmt.Errorf("ProcessBatch: %w", err))
				}
			}
		}()
		go func() { // retrier
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if _, err := svc.RetryFailed(ctx, c.batch.ID); err != nil {
					report(fmt.Errorf("RetryFailed: %w", err))
				}
			}
		}()
		go func() { // single-document processing racing with ProcessBatch
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				for _, d := range c.docs {
					err := svc.ProcessDocument(ctx, d.ID)
					if err != nil && !errors.Is(err, domain.ErrNotProcessable) && !errors.Is(err, domain.ErrSubmitFailed) {
						report(fmt.Errorf("ProcessDocument: %w", err))
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() { // callbacks racing with everything else
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			for _, c := range all {
				for j, d := range c.docs {
					result := domain.StatusSuccess
					if (i+j)%3 == 0 {
						result = domain.StatusFailed
					}
					cb := domain.Callback{CallbackID: fmt.Sprintf("cb-%d-%s", i, d.ID), DocumentID: d.ID, Result: result}
					if _, err := svc.HandleCallback(ctx, cb); err != nil && !errors.Is(err, domain.ErrInvalidTransition) {
						report(fmt.Errorf("HandleCallback: %w", err))
					}
				}
			}
		}
	}()

	wg.Add(1)
	go func() { // readers: counts must always add up, SUCCESS must never be left
		defer wg.Done()
		succeeded := make(map[string]bool)
		for i := 0; i < iterations*5; i++ {
			for _, c := range all {
				view, err := svc.GetBatchStatus(ctx, c.batch.ID)
				if err != nil {
					report(fmt.Errorf("GetBatchStatus: %w", err))
					continue
				}
				sum := 0
				for _, n := range view.Counts {
					sum += n
				}
				if sum != view.Total || view.Total != docsPerB {
					report(fmt.Errorf("inconsistent view %+v", view))
				}
				docs, err := repo.ListDocuments(ctx, c.batch.ID)
				if err != nil {
					report(err)
					continue
				}
				for _, d := range docs {
					if succeeded[d.ID] && d.Status != domain.StatusSuccess {
						report(fmt.Errorf("document %s left SUCCESS and became %s", d.ID, d.Status))
					}
					if d.Status == domain.StatusSuccess {
						succeeded[d.ID] = true
					}
				}
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	for _, c := range all {
		for _, d := range listDocs(t, repo, c.batch.ID) {
			if d.AttemptCount > d.MaxAttempts {
				t.Errorf("document %s attempts %d exceed max %d", d.ID, d.AttemptCount, d.MaxAttempts)
			}
			if d.Status == domain.StatusProcessing && d.LockedAt == nil {
				t.Errorf("document %s is PROCESSING without a lease", d.ID)
			}
		}
	}
}

// Test plan item 7: cancellation returns context.Canceled, leaks no goroutine
// and leaves no document stuck in PROCESSING.
func TestProcessBatch_ContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const total = 20
	// 3 calls are stuck inside Submit; 7 more workers hold claimed documents
	// while waiting for a semaphore slot. Cancel must release all of them.
	mock := principaltest.New(principaltest.WithBarrier(3))
	svc, repo := newService(t, mock, service.WithBatchWorkers(10))
	b, docs := createBatch(t, svc, "B1", total)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessBatch(ctx, b.ID) }()

	waitClosed(t, mock.Reached(), "3 concurrent Submit calls")
	cancel()

	err := waitErr(t, errc, "ProcessBatch to return after cancel")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if mock.InFlight() != 0 {
		t.Errorf("%d Submit calls still in flight after ProcessBatch returned", mock.InFlight())
	}
	for _, d := range docs {
		got := getDoc(t, repo, d.ID)
		if got.Status != domain.StatusPending || got.AttemptCount != 0 || got.LockedAt != nil {
			t.Errorf("document %s = %s attempts=%d locked=%v; want PENDING/0/unlocked", d.ID, got.Status, got.AttemptCount, got.LockedAt)
		}
	}
	view, err := svc.GetBatchStatus(context.Background(), b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Counts[domain.StatusProcessing] != 0 || view.Status != domain.BatchPending {
		t.Errorf("view after cancel = %+v", view)
	}
	if m := svc.Metrics(); m.ClaimsReleased < 3 || m.InFlight != 0 {
		t.Errorf("metrics = %+v", m)
	}

	// The released documents are fully usable again (the barrier no longer holds calls).
	mock.Release()
	if err := svc.ProcessBatch(context.Background(), b.ID); err != nil {
		t.Fatalf("ProcessBatch after cancel: %v", err)
	}
	if got := mock.TotalCalls(); got < total {
		t.Errorf("only %d Submit calls after re-processing %d documents", got, total)
	}
}

func TestProcessBatch_AlreadyCanceledContext(t *testing.T) {
	mock := principaltest.New()
	svc, repo := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.ProcessBatch(ctx, b.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if mock.TotalCalls() != 0 {
		t.Errorf("Submit called %d times on a canceled context", mock.TotalCalls())
	}
	for _, d := range docs {
		if got := getDoc(t, repo, d.ID); got.Status != domain.StatusPending {
			t.Errorf("document %s = %s, want PENDING", d.ID, got.Status)
		}
	}
}

func TestProcessDocument_CancelDuringSubmitReleasesClaim(t *testing.T) {
	mock := principaltest.New(principaltest.WithBarrier(1))
	svc, repo := newService(t, mock)
	_, docs := createBatch(t, svc, "B1", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessDocument(ctx, docs[0].ID) }()
	waitClosed(t, mock.Reached(), "Submit to be in flight")
	cancel()

	err := waitErr(t, errc, "ProcessDocument")
	if !errors.Is(err, context.Canceled) || errors.Is(err, domain.ErrSubmitFailed) {
		t.Fatalf("err = %v, want context.Canceled and not ErrSubmitFailed", err)
	}
	got := getDoc(t, repo, docs[0].ID)
	if got.Status != domain.StatusPending || got.AttemptCount != 0 {
		t.Fatalf("document = %s attempts=%d, want PENDING/0", got.Status, got.AttemptCount)
	}

	// The interrupted attempt is retried under the same RequestID, so Principal
	// can deduplicate it if it did receive the first request.
	mock2 := principaltest.New()
	svc2 := newServiceWithRepo(t, repo, mock2)
	if err := svc2.ProcessDocument(context.Background(), docs[0].ID); err != nil {
		t.Fatal(err)
	}
	first, second := mock.Requests()[0], mock2.Requests()[0]
	if first.RequestID != second.RequestID {
		t.Errorf("request IDs differ across the released attempt: %s vs %s", first.RequestID, second.RequestID)
	}
}
