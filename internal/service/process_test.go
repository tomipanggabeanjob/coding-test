package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/service"
)

// Test plan item 3: concurrent ProcessDocument calls for the same ID result in
// exactly one Submit.
func TestProcessDocument_SingleClaimUnderRace(t *testing.T) {
	const callers = 30
	mock := principaltest.New()
	svc, repo := newService(t, mock)
	_, docs := createBatch(t, svc, "B1", 1)
	id := docs[0].ID

	start := make(chan struct{})
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = svc.ProcessDocument(context.Background(), id)
		}()
	}
	close(start)
	wg.Wait()

	if got := mock.Calls(id); got != 1 {
		t.Fatalf("Submit called %d times, want exactly 1", got)
	}
	var ok, notProcessable int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, domain.ErrNotProcessable):
			notProcessable++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 1 || notProcessable != callers-1 {
		t.Errorf("ok=%d notProcessable=%d, want 1 and %d", ok, notProcessable, callers-1)
	}
	if got := getDoc(t, repo, id); got.Status != domain.StatusProcessing || got.AttemptCount != 1 {
		t.Errorf("document = %s/%d, want PROCESSING/1", got.Status, got.AttemptCount)
	}
}

// Test plan item 8: SUCCESS documents are never reprocessed.
func TestProcessBatch_SuccessNotReprocessed(t *testing.T) {
	ctx := context.Background()
	mock := principaltest.New()
	svc, repo := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 6)

	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	// 4 documents succeed, 2 fail.
	for i, d := range docs {
		result := domain.StatusSuccess
		if i >= 4 {
			result = domain.StatusFailed
		}
		if _, err := svc.HandleCallback(ctx, cbFor(d.ID, result)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := svc.RetryFailed(ctx, b.ID); err != nil || n != 2 {
		t.Fatalf("RetryFailed = %d, %v; want 2", n, err)
	}

	// Second and third ProcessBatch: only the two retried documents are submitted.
	for i := 0; i < 2; i++ {
		if err := svc.ProcessBatch(ctx, b.ID); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range docs {
		want := 1
		if i >= 4 {
			want = 2
		}
		if got := mock.Calls(d.ID); got != want {
			t.Errorf("document %d: Submit called %d times, want %d", i, got, want)
		}
	}
	for _, d := range docs[:4] {
		if got := getDoc(t, repo, d.ID); got.Status != domain.StatusSuccess || got.AttemptCount != 1 {
			t.Errorf("SUCCESS document %s changed: %s/%d", d.ID, got.Status, got.AttemptCount)
		}
	}
	// A SUCCESS document cannot even be claimed by ID.
	if err := svc.ProcessDocument(ctx, docs[0].ID); !errors.Is(err, domain.ErrNotProcessable) {
		t.Errorf("ProcessDocument(SUCCESS) err = %v, want ErrNotProcessable", err)
	}
}

// Test plan item B: a Submit timeout fails the document and frees the slot.
func TestSubmit_Timeout(t *testing.T) {
	slowOnly := func(ctx context.Context, req principal.SubmitRequest) error {
		if req.ExternalRef == "slow" {
			return principaltest.BlockUntilDone(ctx, req)
		}
		return nil
	}

	t.Run("SubmitTimeoutOption", func(t *testing.T) {
		mock := principaltest.New(principaltest.WithBehavior(slowOnly))
		// One slot only: if the timed-out call kept it, the follow-up would hang.
		svc, repo := newService(t, mock, service.WithMaxInFlight(1), service.WithSubmitTimeout(10*time.Millisecond))
		_, docs, err := svc.CreateBatch(context.Background(), "B1", []domain.DocumentInput{{ExternalRef: "slow"}, {ExternalRef: "fast"}})
		if err != nil {
			t.Fatal(err)
		}

		err = svc.ProcessDocument(context.Background(), docs[0].ID)
		if !errors.Is(err, domain.ErrSubmitFailed) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want ErrSubmitFailed wrapping DeadlineExceeded", err)
		}
		got := getDoc(t, repo, docs[0].ID)
		if got.Status != domain.StatusFailed || got.AttemptCount != 1 || got.LastError == "" || got.LockedAt != nil {
			t.Errorf("timed-out document = %+v", got)
		}

		if err := svc.ProcessDocument(context.Background(), docs[1].ID); err != nil {
			t.Fatalf("follow-up Submit after a timeout: %v", err)
		}
	})

	t.Run("CallerDeadline", func(t *testing.T) {
		const slowRef = "B1-doc-001" // first document created by createBatch
		entered := make(chan struct{})
		behavior := func(ctx context.Context, req principal.SubmitRequest) error {
			if req.ExternalRef != slowRef {
				return nil
			}
			close(entered)
			return principaltest.BlockUntilDone(ctx, req)
		}
		mock := principaltest.New(principaltest.WithBehavior(behavior))
		svc, repo := newService(t, mock, service.WithMaxInFlight(1))
		_, docs := createBatch(t, svc, "B1", 2)

		ctx := newManualDeadlineCtx()
		errc := make(chan error, 1)
		go func() { errc <- svc.ProcessDocument(ctx, docs[0].ID) }()
		waitClosed(t, entered, "Submit to start")
		ctx.expire() // the caller's deadline passes while Submit is in flight

		err := waitErr(t, errc, "ProcessDocument")
		if !errors.Is(err, domain.ErrSubmitFailed) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want ErrSubmitFailed wrapping DeadlineExceeded", err)
		}
		if got := getDoc(t, repo, docs[0].ID); got.Status != domain.StatusFailed {
			t.Fatalf("document = %s, want FAILED", got.Status)
		}
		// Cleanup used a detached context, and the slot is free again.
		if err := svc.ProcessDocument(context.Background(), docs[1].ID); err != nil {
			t.Fatalf("follow-up after caller deadline: %v", err)
		}
	})
}

func TestProcessDocument_DeadlineBeforeSubmitReleasesClaim(t *testing.T) {
	// Holding the only slot guarantees the document is still waiting for a slot
	// when its deadline passes.
	mock := principaltest.New(principaltest.WithBarrier(1))
	svc, repo := newService(t, mock, service.WithMaxInFlight(1))
	_, docs := createBatch(t, svc, "B1", 2)

	holder := make(chan error, 1)
	go func() { holder <- svc.ProcessDocument(context.Background(), docs[0].ID) }()
	waitClosed(t, mock.Reached(), "slot holder to be in flight")

	ctx := newManualDeadlineCtx()
	errc := make(chan error, 1)
	go func() { errc <- svc.ProcessDocument(ctx, docs[1].ID) }()
	// The second document is claimed, then blocks on the semaphore.
	waitUntil(t, "second document to be claimed", func() bool {
		select {
		case err := <-errc:
			t.Fatalf("ProcessDocument returned early: %v", err)
		default:
		}
		return getDoc(t, repo, docs[1].ID).Status == domain.StatusProcessing
	})
	ctx.expire()

	err := waitErr(t, errc, "ProcessDocument")
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, domain.ErrSubmitFailed) {
		t.Fatalf("err = %v, want DeadlineExceeded and not ErrSubmitFailed (Submit never ran)", err)
	}
	if got := getDoc(t, repo, docs[1].ID); got.Status != domain.StatusPending || got.AttemptCount != 0 {
		t.Errorf("document = %s/%d, want PENDING/0", got.Status, got.AttemptCount)
	}
	if mock.Calls(docs[1].ID) != 0 {
		t.Error("Submit ran for a document whose deadline had passed")
	}

	mock.Release()
	if err := waitErr(t, holder, "slot holder"); err != nil {
		t.Fatal(err)
	}
}

func TestProcessDocument_SubmitErrorMarksFailed(t *testing.T) {
	mock := principaltest.New(principaltest.WithBehavior(principaltest.Fail))
	svc, repo := newService(t, mock)
	_, docs := createBatch(t, svc, "B1", 1)

	err := svc.ProcessDocument(context.Background(), docs[0].ID)
	if !errors.Is(err, domain.ErrSubmitFailed) || !errors.Is(err, principaltest.ErrRejected) {
		t.Fatalf("err = %v, want ErrSubmitFailed wrapping the principal error", err)
	}
	got := getDoc(t, repo, docs[0].ID)
	if got.Status != domain.StatusFailed || got.AttemptCount != 1 {
		t.Errorf("document = %s/%d, want FAILED/1", got.Status, got.AttemptCount)
	}
	if m := svc.Metrics(); m.SubmitFailures != 1 {
		t.Errorf("metrics = %+v", m)
	}
}

func TestProcessDocument_SendsRequestID(t *testing.T) {
	mock := principaltest.New(principaltest.WithBehavior(principaltest.Fail))
	svc, _ := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 1)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := svc.ProcessBatch(ctx, b.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.RetryFailed(ctx, b.ID); err != nil {
			t.Fatal(err)
		}
	}
	reqs := mock.Requests()
	if len(reqs) != 2 {
		t.Fatalf("got %d requests, want 2", len(reqs))
	}
	for i, r := range reqs {
		want := domain.RequestID(docs[0].ID, i+1)
		if r.RequestID != want || r.Attempt != i+1 || r.DocumentID != docs[0].ID || r.BatchID != b.ID {
			t.Errorf("request %d = %+v, want RequestID %s", i, r, want)
		}
	}
}

func TestNotFoundErrors(t *testing.T) {
	svc, _ := newService(t, principaltest.New())
	ctx := context.Background()
	if err := svc.ProcessBatch(ctx, "999"); !errors.Is(err, domain.ErrBatchNotFound) {
		t.Errorf("ProcessBatch: %v", err)
	}
	if err := svc.ProcessDocument(ctx, "999"); !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("ProcessDocument: %v", err)
	}
	if _, err := svc.RetryFailed(ctx, "999"); !errors.Is(err, domain.ErrBatchNotFound) {
		t.Errorf("RetryFailed: %v", err)
	}
	if _, err := svc.GetBatchStatus(ctx, "999"); !errors.Is(err, domain.ErrBatchNotFound) {
		t.Errorf("GetBatchStatus: %v", err)
	}
}
