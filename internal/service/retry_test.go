package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/service"
)

// Test plan item 2: a document is submitted at most 3 times (1 + 2 retries).
func TestRetry_MaxThreeAttempts(t *testing.T) {
	ctx := context.Background()
	mock := principaltest.New(principaltest.WithBehavior(principaltest.Fail))
	svc, repo := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 5)

	rounds := 0
	for {
		if err := svc.ProcessBatch(ctx, b.ID); err != nil {
			t.Fatalf("ProcessBatch: %v", err)
		}
		moved, err := svc.RetryFailed(ctx, b.ID)
		if err != nil {
			t.Fatalf("RetryFailed: %v", err)
		}
		rounds++
		if moved == 0 {
			break
		}
		if rounds > 10 {
			t.Fatal("retry loop did not terminate")
		}
	}
	if rounds != 3 {
		t.Errorf("rounds = %d, want 3", rounds)
	}
	for _, d := range docs {
		if got := mock.Calls(d.ID); got != 3 {
			t.Errorf("document %s: Submit called %d times, want 3", d.ID, got)
		}
		if got := getDoc(t, repo, d.ID); got.Status != domain.StatusFailed || got.AttemptCount != 3 {
			t.Errorf("document %s = %s/%d, want FAILED/3", d.ID, got.Status, got.AttemptCount)
		}
	}

	// One more round must not cause a 4th Submit.
	before := mock.TotalCalls()
	if moved, err := svc.RetryFailed(ctx, b.ID); err != nil || moved != 0 {
		t.Fatalf("extra RetryFailed = %d, %v; want 0", moved, err)
	}
	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if after := mock.TotalCalls(); after != before {
		t.Fatalf("Submit called again after attempts were exhausted (%d -> %d)", before, after)
	}
	if err := svc.ProcessDocument(ctx, docs[0].ID); !errors.Is(err, domain.ErrNotProcessable) {
		t.Errorf("ProcessDocument(exhausted) err = %v, want ErrNotProcessable", err)
	}

	view, err := svc.GetBatchStatus(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.BatchCompleted || view.Counts[domain.StatusFailed] != 5 {
		t.Errorf("view = %+v; exhausted FAILED documents mean the batch is COMPLETED", view)
	}
}

func TestRetry_SucceedsOnSecondAttempt(t *testing.T) {
	ctx := context.Background()
	mock := principaltest.New(principaltest.WithBehavior(principaltest.FailFirst(1)))
	svc, repo := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 3)

	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := svc.RetryFailed(ctx, b.ID); n != 3 {
		t.Fatalf("RetryFailed = %d, want 3", n)
	}
	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		if got := getDoc(t, repo, d.ID); got.Status != domain.StatusProcessing || got.AttemptCount != 2 {
			t.Errorf("document %s = %s/%d, want PROCESSING/2", d.ID, got.Status, got.AttemptCount)
		}
	}
}

func TestRetry_BackoffDelaysRetry(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	repo := memory.New(memory.WithClock(clock.Now))
	mock := principaltest.New(principaltest.WithBehavior(principaltest.Fail))
	svc := newServiceWithRepo(t, repo, mock, service.WithBackoff(service.ExponentialBackoff(time.Minute, time.Hour, nil)))
	b, docs := createBatch(t, svc, "B1", 1)

	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := svc.RetryFailed(ctx, b.ID); n != 0 {
		t.Fatalf("RetryFailed during backoff moved %d documents", n)
	}
	clock.Advance(time.Minute)
	if n, _ := svc.RetryFailed(ctx, b.ID); n != 1 {
		t.Fatalf("RetryFailed after backoff moved %d, want 1", n)
	}
	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	// Attempt 2 waits twice as long.
	clock.Advance(time.Minute)
	if n, _ := svc.RetryFailed(ctx, b.ID); n != 0 {
		t.Fatalf("second backoff should be 2 minutes, but %d documents moved after 1", n)
	}
	clock.Advance(time.Minute)
	if n, _ := svc.RetryFailed(ctx, b.ID); n != 1 {
		t.Fatalf("RetryFailed after second backoff moved %d, want 1", n)
	}
	if got := mock.Calls(docs[0].ID); got != 2 {
		t.Errorf("Submit calls = %d, want 2", got)
	}
}

func TestExponentialBackoff(t *testing.T) {
	plain := service.ExponentialBackoff(time.Second, 10*time.Second, nil)
	for attempt, want := range map[int]time.Duration{
		0: 0, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 30: 10 * time.Second,
	} {
		if got := plain(attempt); got != want {
			t.Errorf("attempt %d: got %s, want %s", attempt, got, want)
		}
	}
	if got := service.ExponentialBackoff(0, time.Hour, nil)(3); got != 0 {
		t.Errorf("zero base must disable backoff, got %s", got)
	}

	low := service.ExponentialBackoff(time.Second, time.Minute, func() float64 { return 0 })
	high := service.ExponentialBackoff(time.Second, time.Minute, func() float64 { return 0.999 })
	if got := low(3); got != 2*time.Second { // 4s/2
		t.Errorf("jitter 0: got %s, want 2s", got)
	}
	if got := high(3); got < 3900*time.Millisecond || got >= 4*time.Second {
		t.Errorf("jitter ~1: got %s, want just under 4s", got)
	}
}

func TestRecoverStuck_ThenRetry(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	repo := memory.New(memory.WithClock(clock.Now))
	mock := principaltest.New()
	svc := newServiceWithRepo(t, repo, mock)
	b, docs := createBatch(t, svc, "B1", 2)

	if err := svc.ProcessBatch(ctx, b.ID); err != nil { // both accepted, callbacks never arrive
		t.Fatal(err)
	}
	if _, err := svc.HandleCallback(ctx, cbFor(docs[0].ID, domain.StatusSuccess)); err != nil {
		t.Fatal(err)
	}

	clock.Advance(9 * time.Minute)
	if n, err := svc.RecoverStuck(ctx, 10*time.Minute); err != nil || n != 0 {
		t.Fatalf("RecoverStuck inside lease = %d, %v; want 0", n, err)
	}
	clock.Advance(2 * time.Minute)
	if n, err := svc.RecoverStuck(ctx, 10*time.Minute); err != nil || n != 1 {
		t.Fatalf("RecoverStuck after lease = %d, %v; want 1 (SUCCESS document untouched)", n, err)
	}
	if got := getDoc(t, repo, docs[0].ID); got.Status != domain.StatusSuccess {
		t.Errorf("SUCCESS document changed to %s", got.Status)
	}
	if got := getDoc(t, repo, docs[1].ID); got.Status != domain.StatusFailed {
		t.Fatalf("stuck document = %s, want FAILED", got.Status)
	}

	if n, _ := svc.RetryFailed(ctx, b.ID); n != 1 {
		t.Fatalf("RetryFailed = %d, want 1", n)
	}
	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if got := mock.Calls(docs[1].ID); got != 2 {
		t.Errorf("stuck document submitted %d times, want 2", got)
	}
}
