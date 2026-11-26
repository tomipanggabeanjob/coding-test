package service_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/service"
)

// guardTimeout only bounds how long a broken implementation can hang a test;
// no test relies on it elapsing.
const guardTimeout = 30 * time.Second

// testEnv bundles a service with its fakes.
type testEnv struct {
	svc   *service.Service
	repo  *memory.Repo
	mock  *principaltest.Mock
	batch domain.Batch
}

func newService(t testing.TB, p principal.Client, opts ...service.Option) (*service.Service, *memory.Repo) {
	t.Helper()
	repo := memory.New()
	svc, err := service.NewService(repo, p, opts...)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, repo
}

func newServiceWithRepo(t testing.TB, repo *memory.Repo, p principal.Client, opts ...service.Option) *service.Service {
	t.Helper()
	svc, err := service.NewService(repo, p, opts...)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func createBatch(t testing.TB, svc *service.Service, batchNo string, n int) (domain.Batch, []domain.Document) {
	t.Helper()
	inputs := make([]domain.DocumentInput, n)
	for i := range inputs {
		inputs[i] = domain.DocumentInput{ExternalRef: fmt.Sprintf("%s-doc-%03d", batchNo, i+1)}
	}
	b, docs, err := svc.CreateBatch(context.Background(), batchNo, inputs)
	if err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	return b, docs
}

func getDoc(t testing.TB, repo *memory.Repo, id string) domain.Document {
	t.Helper()
	d, err := repo.GetDocument(context.Background(), id)
	if err != nil {
		t.Fatalf("GetDocument(%s): %v", id, err)
	}
	return d
}

func listDocs(t testing.TB, repo *memory.Repo, batchID string) []domain.Document {
	t.Helper()
	docs, err := repo.ListDocuments(context.Background(), batchID)
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	return docs
}

func waitClosed(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(guardTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitUntil spins (yielding the processor, never sleeping) until cond holds.
// It is used only to observe state that has no channel to wait on.
func waitUntil(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(guardTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func waitErr(t testing.TB, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(guardTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// cbFor builds a SUCCESS/FAILED callback with a unique ID per (doc, result).
func cbFor(docID string, result domain.DocStatus) domain.Callback {
	return domain.Callback{
		CallbackID: fmt.Sprintf("cb-%s-%s", docID, result),
		DocumentID: docID,
		Result:     result,
	}
}

// fakeClock is a goroutine-safe manual clock for the memory repository.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// manualDeadlineCtx is a context whose deadline "fires" when the test says so,
// which makes deadline-exceeded paths deterministic (no timers involved).
type manualDeadlineCtx struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func newManualDeadlineCtx() *manualDeadlineCtx {
	return &manualDeadlineCtx{Context: context.Background(), done: make(chan struct{})}
}

func (c *manualDeadlineCtx) Done() <-chan struct{} { return c.done }

func (c *manualDeadlineCtx) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *manualDeadlineCtx) expire() { c.once.Do(func() { close(c.done) }) }
