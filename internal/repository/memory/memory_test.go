package memory_test

import (
	"context"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/repository"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/repository/repotest"
)

func TestMemoryRepositoryContract(t *testing.T) {
	repotest.Run(t, func(*testing.T) repository.Repository { return memory.New() })
}

// fakeClock lets tests move time forward deterministically.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func TestRetryFailedBackoffWithFakeClock(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	repo := memory.New(memory.WithClock(clock.Now))

	b, docs, err := repo.CreateBatch(ctx, "B", []domain.DocumentInput{{ExternalRef: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := repo.ClaimDocument(ctx, docs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkRequestFailed(ctx, d.ID, d.AttemptCount, "boom", time.Minute); err != nil {
		t.Fatal(err)
	}

	clock.Advance(59 * time.Second)
	if n, _ := repo.RetryFailed(ctx, b.ID); n != 0 {
		t.Fatalf("moved %d before backoff elapsed", n)
	}
	clock.Advance(time.Second)
	if n, _ := repo.RetryFailed(ctx, b.ID); n != 1 {
		t.Fatalf("moved %d after backoff elapsed, want 1", n)
	}
}

func TestRecoverStuckWithFakeClock(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	repo := memory.New(memory.WithClock(clock.Now))

	_, docs, err := repo.CreateBatch(ctx, "B", []domain.DocumentInput{{ExternalRef: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimDocument(ctx, docs[0].ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Minute)
	if n, _ := repo.RecoverStuck(ctx, 10*time.Minute); n != 0 {
		t.Fatalf("recovered %d inside the lease", n)
	}
	clock.Advance(2 * time.Minute)
	if n, _ := repo.RecoverStuck(ctx, 10*time.Minute); n != 1 {
		t.Fatalf("recovered %d after lease expiry, want 1", n)
	}
}
