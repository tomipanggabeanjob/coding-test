package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal/principaltest"
)

// processed returns a service whose batch of n documents was submitted
// successfully, so every document is PROCESSING and waiting for a callback.
func processed(t *testing.T, n int) (*testEnv, []domain.Document) {
	t.Helper()
	mock := principaltest.New()
	svc, repo := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", n)
	if err := svc.ProcessBatch(context.Background(), b.ID); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	return &testEnv{svc: svc, repo: repo, mock: mock, batch: b}, docs
}

// Test plan item 4: callbacks are idempotent, sequentially and concurrently.
func TestHandleCallback_Duplicate(t *testing.T) {
	ctx := context.Background()

	t.Run("Sequential", func(t *testing.T) {
		env, docs := processed(t, 1)
		cb := cbFor(docs[0].ID, domain.StatusSuccess)

		outcomes := map[domain.CallbackOutcome]int{}
		for i := 0; i < 5; i++ {
			out, err := env.svc.HandleCallback(ctx, cb)
			if err != nil {
				t.Fatalf("call %d: %v", i, err)
			}
			outcomes[out]++
		}
		if outcomes[domain.CallbackApplied] != 1 || outcomes[domain.CallbackDuplicate] != 4 {
			t.Fatalf("outcomes = %v, want 1 APPLIED and 4 DUPLICATE", outcomes)
		}
		if got := getDoc(t, env.repo, docs[0].ID); got.Status != domain.StatusSuccess {
			t.Errorf("status = %s, want SUCCESS", got.Status)
		}
		if env.repo.CallbackCount() != 1 || env.repo.AppliedCount() != 1 {
			t.Errorf("callback rows=%d side effects=%d, want 1 and 1", env.repo.CallbackCount(), env.repo.AppliedCount())
		}
	})

	t.Run("Parallel", func(t *testing.T) {
		env, docs := processed(t, 1)
		cb := cbFor(docs[0].ID, domain.StatusSuccess)

		const callers = 20
		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		outcomes := map[domain.CallbackOutcome]int{}
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				out, err := env.svc.HandleCallback(ctx, cb)
				if err != nil {
					t.Errorf("HandleCallback: %v", err)
					return
				}
				mu.Lock()
				outcomes[out]++
				mu.Unlock()
			}()
		}
		close(start)
		wg.Wait()

		if outcomes[domain.CallbackApplied] != 1 || outcomes[domain.CallbackDuplicate] != callers-1 {
			t.Fatalf("outcomes = %v, want 1 APPLIED and %d DUPLICATE", outcomes, callers-1)
		}
		if got := getDoc(t, env.repo, docs[0].ID); got.Status != domain.StatusSuccess {
			t.Errorf("status = %s, want SUCCESS", got.Status)
		}
		if env.repo.CallbackCount() != 1 || env.repo.AppliedCount() != 1 {
			t.Errorf("callback rows=%d side effects=%d, want 1 and 1", env.repo.CallbackCount(), env.repo.AppliedCount())
		}
		if m := env.svc.Metrics(); m.CallbacksApplied != 1 || m.CallbacksDuplicate != callers-1 {
			t.Errorf("metrics = %+v", m)
		}
	})

	t.Run("DuplicateStaysDuplicateAfterRetry", func(t *testing.T) {
		env, docs := processed(t, 1)
		failed := cbFor(docs[0].ID, domain.StatusFailed)
		if out, err := env.svc.HandleCallback(ctx, failed); err != nil || out != domain.CallbackApplied {
			t.Fatalf("FAILED callback = %s, %v", out, err)
		}
		if n, err := env.svc.RetryFailed(ctx, env.batch.ID); err != nil || n != 1 {
			t.Fatalf("RetryFailed = %d, %v", n, err)
		}
		// Document is PENDING again; a redelivery of the old callback is harmless.
		if out, err := env.svc.HandleCallback(ctx, failed); err != nil || out != domain.CallbackDuplicate {
			t.Fatalf("redelivery = %s, %v; want DUPLICATE", out, err)
		}
	})
}

// Test plan item 5: a late FAILED callback never overwrites SUCCESS.
func TestHandleCallback_LateFailedKeepsSuccess(t *testing.T) {
	ctx := context.Background()
	env, docs := processed(t, 1)
	id := docs[0].ID

	if out, err := env.svc.HandleCallback(ctx, domain.Callback{CallbackID: "cb-A", DocumentID: id, Result: domain.StatusSuccess}); err != nil || out != domain.CallbackApplied {
		t.Fatalf("SUCCESS callback = %s, %v", out, err)
	}
	out, err := env.svc.HandleCallback(ctx, domain.Callback{CallbackID: "cb-B", DocumentID: id, Result: domain.StatusFailed})
	if err != nil {
		t.Fatalf("late FAILED callback must be accepted as a no-op, got %v", err)
	}
	if out != domain.CallbackIgnored {
		t.Errorf("outcome = %s, want IGNORED", out)
	}
	got := getDoc(t, env.repo, id)
	if got.Status != domain.StatusSuccess || got.LastError != "" {
		t.Fatalf("document = %s (%q), want SUCCESS untouched", got.Status, got.LastError)
	}
	if env.repo.CallbackCount() != 2 || env.repo.AppliedCount() != 1 {
		t.Errorf("rows=%d applied=%d; the late callback is kept for audit but not applied", env.repo.CallbackCount(), env.repo.AppliedCount())
	}
}

// SUCCESS and FAILED racing for the same PROCESSING document: whichever the
// repository serialises first wins, and the loser is a no-op, never an overwrite.
func TestHandleCallback_ConflictingCallbacksRace(t *testing.T) {
	ctx := context.Background()
	env, docs := processed(t, 1)
	id := docs[0].ID

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[domain.CallbackOutcome]int{}
	for i := 0; i < 10; i++ {
		result := domain.StatusSuccess
		if i%2 == 0 {
			result = domain.StatusFailed
		}
		cb := domain.Callback{CallbackID: fmt.Sprintf("cb-%d", i), DocumentID: id, Result: result}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out, err := env.svc.HandleCallback(ctx, cb)
			if err != nil {
				t.Errorf("HandleCallback: %v", err)
				return
			}
			mu.Lock()
			outcomes[out]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if outcomes[domain.CallbackApplied] != 1 || outcomes[domain.CallbackIgnored] != 9 {
		t.Fatalf("outcomes = %v, want exactly 1 APPLIED and 9 IGNORED", outcomes)
	}
	if got := getDoc(t, env.repo, id); got.Status != domain.StatusSuccess && got.Status != domain.StatusFailed {
		t.Errorf("status = %s", got.Status)
	}
}

func TestHandleCallback_Validation(t *testing.T) {
	env, docs := processed(t, 1)
	ctx := context.Background()
	id := docs[0].ID

	tests := []struct {
		name string
		cb   domain.Callback
		want error
	}{
		{"missing callback id", domain.Callback{DocumentID: id, Result: domain.StatusSuccess}, domain.ErrInvalidInput},
		{"missing document id", domain.Callback{CallbackID: "x", Result: domain.StatusSuccess}, domain.ErrInvalidInput},
		{"bad result", domain.Callback{CallbackID: "x", DocumentID: id, Result: "DONE"}, domain.ErrInvalidInput},
		{"processing is not a result", domain.Callback{CallbackID: "x", DocumentID: id, Result: domain.StatusProcessing}, domain.ErrInvalidInput},
		{"unknown document", domain.Callback{CallbackID: "x", DocumentID: "999", Result: domain.StatusSuccess}, domain.ErrDocumentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := env.svc.HandleCallback(ctx, tt.cb); !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}
	if got := getDoc(t, env.repo, id); got.Status != domain.StatusProcessing {
		t.Errorf("rejected callbacks changed the document to %s", got.Status)
	}
}

func TestHandleCallback_ForPendingDocumentRejected(t *testing.T) {
	svc, repo := newService(t, principaltest.New())
	_, docs := createBatch(t, svc, "B1", 1)

	_, err := svc.HandleCallback(context.Background(), cbFor(docs[0].ID, domain.StatusSuccess))
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
	if got := getDoc(t, repo, docs[0].ID); got.Status != domain.StatusPending {
		t.Errorf("status = %s, want PENDING", got.Status)
	}
	if repo.CallbackCount() != 0 {
		t.Error("a rejected callback must not be stored")
	}
}
