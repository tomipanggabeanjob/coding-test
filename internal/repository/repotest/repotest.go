// Package repotest holds a contract test suite that every repository.Repository
// implementation (memory, postgres) must pass, so their behaviour cannot diverge.
package repotest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/repository"
)

// Factory returns a fresh, empty repository for one subtest.
type Factory func(t *testing.T) repository.Repository

func inputs(n int) []domain.DocumentInput {
	in := make([]domain.DocumentInput, n)
	for i := range in {
		in[i] = domain.DocumentInput{ExternalRef: fmt.Sprintf("doc-%03d", i+1), Payload: []byte(`{"n":1}`)}
	}
	return in
}

func newBatch(t *testing.T, repo repository.Repository, n int) (domain.Batch, []domain.Document) {
	t.Helper()
	b, docs, err := repo.CreateBatch(context.Background(), "BATCH-1", inputs(n))
	if err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	return b, docs
}

func claimOne(t *testing.T, repo repository.Repository, docID string) domain.Document {
	t.Helper()
	d, err := repo.ClaimDocument(context.Background(), docID)
	if err != nil {
		t.Fatalf("ClaimDocument(%s): %v", docID, err)
	}
	return d
}

func mustStatus(t *testing.T, repo repository.Repository, docID string, want domain.DocStatus) domain.Document {
	t.Helper()
	d, err := repo.GetDocument(context.Background(), docID)
	if err != nil {
		t.Fatalf("GetDocument(%s): %v", docID, err)
	}
	if d.Status != want {
		t.Fatalf("document %s status = %s, want %s", docID, d.Status, want)
	}
	return d
}

// runParallel starts n goroutines that all begin at the same instant.
func runParallel(n int, fn func(i int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

// Run executes the whole contract suite.
func Run(t *testing.T, newRepo Factory) {
	cases := []struct {
		name string
		fn   func(t *testing.T, repo repository.Repository)
	}{
		{"CreateBatch", testCreateBatch},
		{"CreateBatchDuplicateBatchNo", testCreateBatchDuplicate},
		{"CreateBatchInvalidDocuments", testCreateBatchInvalid},
		{"UnknownBatch", testUnknownBatch},
		{"ClaimPendingOrderAndAttempts", testClaimPending},
		{"ClaimPendingConcurrentNoDoubleClaim", testClaimPendingConcurrent},
		{"ClaimDocumentSingleWinner", testClaimDocument},
		{"MarkRequestFailedIsGuarded", testMarkRequestFailedGuarded},
		{"MarkRequestAcceptedKeepsProcessing", testMarkRequestAccepted},
		{"ReleaseClaimGivesAttemptBack", testReleaseClaim},
		{"CallbackApplied", testCallbackApplied},
		{"CallbackDuplicateSequential", testCallbackDuplicate},
		{"CallbackDuplicateAfterRetry", testCallbackDuplicateAfterRetry},
		{"CallbackDuplicateConcurrent", testCallbackDuplicateConcurrent},
		{"CallbackLateFailedKeepsSuccess", testCallbackLateFailed},
		{"CallbackForPendingRejected", testCallbackPending},
		{"CallbackUnknownDocument", testCallbackUnknownDocument},
		{"CallbackInvalidResult", testCallbackInvalidResult},
		{"RetryFailedRespectsMaxAttempts", testRetryFailedMaxAttempts},
		{"RetryFailedRespectsBackoff", testRetryFailedBackoff},
		{"RecoverStuck", testRecoverStuck},
		{"BatchStatusCounts", testBatchStatus},
		{"CanceledContext", testCanceledContext},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newRepo(t)) })
	}
}

func testCreateBatch(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 3)
	if b.ID == "" || b.BatchNo != "BATCH-1" {
		t.Fatalf("unexpected batch %+v", b)
	}
	if len(docs) != 3 {
		t.Fatalf("got %d docs, want 3", len(docs))
	}
	for _, d := range docs {
		if d.ID == "" || d.BatchID != b.ID || d.Status != domain.StatusPending ||
			d.AttemptCount != 0 || d.MaxAttempts != domain.DefaultMaxAttempts {
			t.Errorf("unexpected new document %+v", d)
		}
	}
	got, err := repo.GetBatch(ctx, b.ID)
	if err != nil || got.BatchNo != "BATCH-1" {
		t.Fatalf("GetBatch = %+v, %v", got, err)
	}
	listed, err := repo.ListDocuments(ctx, b.ID)
	if err != nil || len(listed) != 3 {
		t.Fatalf("ListDocuments = %d docs, %v", len(listed), err)
	}
	for i := range listed {
		if listed[i].ID != docs[i].ID || listed[i].ExternalRef != fmt.Sprintf("doc-%03d", i+1) {
			t.Errorf("ListDocuments order/identity mismatch at %d: %+v", i, listed[i])
		}
	}
	if string(listed[0].Payload) != `{"n": 1}` && string(listed[0].Payload) != `{"n":1}` {
		t.Errorf("payload not preserved: %s", listed[0].Payload)
	}
}

func testCreateBatchDuplicate(t *testing.T, repo repository.Repository) {
	newBatch(t, repo, 1)
	_, _, err := repo.CreateBatch(context.Background(), "BATCH-1", inputs(1))
	if !errors.Is(err, domain.ErrBatchExists) {
		t.Fatalf("err = %v, want ErrBatchExists", err)
	}
}

func testCreateBatchInvalid(t *testing.T, repo repository.Repository) {
	dup := []domain.DocumentInput{{ExternalRef: "a"}, {ExternalRef: "a"}}
	if _, _, err := repo.CreateBatch(context.Background(), "B-DUP", dup); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("duplicate external_ref: err = %v, want ErrInvalidInput", err)
	}
	// A failed CreateBatch must not leave a half-created batch behind.
	if _, _, err := repo.CreateBatch(context.Background(), "B-DUP", inputs(1)); err != nil {
		t.Fatalf("batch number must be reusable after a failed create: %v", err)
	}
}

func testUnknownBatch(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"GetBatch":     func() error { _, err := repo.GetBatch(ctx, "999"); return err },
		"ClaimPending": func() error { _, err := repo.ClaimPending(ctx, "999", 1); return err },
		"RetryFailed":  func() error { _, err := repo.RetryFailed(ctx, "999"); return err },
		"BatchStatus":  func() error { _, err := repo.BatchStatus(ctx, "999"); return err },
		"List":         func() error { _, err := repo.ListDocuments(ctx, "999"); return err },
		"NonNumeric":   func() error { _, err := repo.GetBatch(ctx, "not-a-number"); return err },
	} {
		if err := call(); !errors.Is(err, domain.ErrBatchNotFound) {
			t.Errorf("%s: err = %v, want ErrBatchNotFound", name, err)
		}
	}
	if _, err := repo.GetDocument(ctx, "999"); !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("GetDocument: err = %v, want ErrDocumentNotFound", err)
	}
	if _, err := repo.GetDocument(ctx, "not-a-number"); !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("GetDocument non-numeric: err = %v, want ErrDocumentNotFound", err)
	}
}

func testClaimPending(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 5)

	claimed, err := repo.ClaimPending(ctx, b.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 || claimed[0].ID != docs[0].ID || claimed[1].ID != docs[1].ID {
		t.Fatalf("claimed %+v, want the two oldest documents", claimed)
	}
	for _, d := range claimed {
		if d.Status != domain.StatusProcessing || d.AttemptCount != 1 || d.LockedAt == nil {
			t.Errorf("claimed document not in expected state: %+v", d)
		}
	}
	rest, err := repo.ClaimPending(ctx, b.ID, 10)
	if err != nil || len(rest) != 3 {
		t.Fatalf("second claim = %d docs, %v; want 3", len(rest), err)
	}
	none, err := repo.ClaimPending(ctx, b.ID, 10)
	if err != nil || len(none) != 0 {
		t.Fatalf("third claim = %d docs, %v; want none", len(none), err)
	}
	if _, err := repo.ClaimPending(ctx, b.ID, 0); !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("limit 0: err = %v, want ErrInvalidInput", err)
	}
}

func testClaimPendingConcurrent(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	const total = 40
	b, _ := newBatch(t, repo, total)

	var mu sync.Mutex
	seen := make(map[string]int)
	runParallel(8, func(int) {
		for {
			docs, err := repo.ClaimPending(ctx, b.ID, 3)
			if err != nil {
				t.Errorf("ClaimPending: %v", err)
				return
			}
			if len(docs) == 0 {
				return
			}
			mu.Lock()
			for _, d := range docs {
				seen[d.ID]++
			}
			mu.Unlock()
		}
	})
	if len(seen) != total {
		t.Fatalf("claimed %d distinct documents, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("document %s claimed %d times", id, n)
		}
	}
}

func testClaimDocument(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	id := docs[0].ID

	var wins, notProcessable int
	var mu sync.Mutex
	runParallel(16, func(int) {
		_, err := repo.ClaimDocument(ctx, id)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err == nil:
			wins++
		case errors.Is(err, domain.ErrNotProcessable):
			notProcessable++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	})
	if wins != 1 || notProcessable != 15 {
		t.Fatalf("wins=%d notProcessable=%d, want 1 and 15", wins, notProcessable)
	}
	if _, err := repo.ClaimDocument(ctx, "999"); !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("unknown document: err = %v, want ErrDocumentNotFound", err)
	}
}

func testMarkRequestFailedGuarded(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 2)

	d := claimOne(t, repo, docs[0].ID)
	// Wrong attempt: no-op.
	if err := repo.MarkRequestFailed(ctx, d.ID, d.AttemptCount+1, "boom", 0); err != nil {
		t.Fatal(err)
	}
	mustStatus(t, repo, d.ID, domain.StatusProcessing)

	if err := repo.MarkRequestFailed(ctx, d.ID, d.AttemptCount, "boom", 0); err != nil {
		t.Fatal(err)
	}
	failed := mustStatus(t, repo, d.ID, domain.StatusFailed)
	if failed.LastError != "boom" || failed.LockedAt != nil || failed.AttemptCount != 1 {
		t.Errorf("unexpected failed document: %+v", failed)
	}

	// A result that already settled the document is never overwritten.
	s := claimOne(t, repo, docs[1].ID)
	if _, err := repo.SaveCallbackAndApply(ctx, domain.Callback{CallbackID: "c-1", DocumentID: s.ID, Result: domain.StatusSuccess}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkRequestFailed(ctx, s.ID, s.AttemptCount, "late submit error", 0); err != nil {
		t.Fatal(err)
	}
	mustStatus(t, repo, s.ID, domain.StatusSuccess)

	if err := repo.MarkRequestFailed(ctx, "999", 1, "x", 0); !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("unknown document: err = %v, want ErrDocumentNotFound", err)
	}
}

func testMarkRequestAccepted(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)
	if err := repo.MarkRequestAccepted(ctx, d.ID, d.AttemptCount); err != nil {
		t.Fatal(err)
	}
	got := mustStatus(t, repo, d.ID, domain.StatusProcessing)
	if got.LockedAt == nil {
		t.Error("PROCESSING document must keep a lease timestamp")
	}
}

func testReleaseClaim(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)

	if err := repo.ReleaseClaim(ctx, d.ID, d.AttemptCount+1); err != nil { // stale attempt: no-op
		t.Fatal(err)
	}
	mustStatus(t, repo, d.ID, domain.StatusProcessing)

	if err := repo.ReleaseClaim(ctx, d.ID, d.AttemptCount); err != nil {
		t.Fatal(err)
	}
	got := mustStatus(t, repo, d.ID, domain.StatusPending)
	if got.AttemptCount != 0 || got.LockedAt != nil {
		t.Errorf("released document: %+v", got)
	}
	again := claimOne(t, repo, d.ID)
	if again.RequestID() != d.RequestID() {
		t.Errorf("a released claim must reuse its request ID: %s vs %s", again.RequestID(), d.RequestID())
	}
}

func cb(id, docID string, result domain.DocStatus) domain.Callback {
	return domain.Callback{CallbackID: id, DocumentID: docID, Result: result}
}

func testCallbackApplied(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 2)
	ok := claimOne(t, repo, docs[0].ID)
	bad := claimOne(t, repo, docs[1].ID)

	out, err := repo.SaveCallbackAndApply(ctx, cb("c-ok", ok.ID, domain.StatusSuccess))
	if err != nil || out != domain.CallbackApplied {
		t.Fatalf("SUCCESS callback = %s, %v", out, err)
	}
	got := mustStatus(t, repo, ok.ID, domain.StatusSuccess)
	if got.LockedAt != nil {
		t.Error("SUCCESS document must release its lease")
	}

	out, err = repo.SaveCallbackAndApply(ctx, cb("c-bad", bad.ID, domain.StatusFailed))
	if err != nil || out != domain.CallbackApplied {
		t.Fatalf("FAILED callback = %s, %v", out, err)
	}
	failed := mustStatus(t, repo, bad.ID, domain.StatusFailed)
	if failed.AttemptCount != 1 || failed.LastError == "" {
		t.Errorf("failed document: %+v", failed)
	}
}

func testCallbackDuplicate(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)

	first, err := repo.SaveCallbackAndApply(ctx, cb("c-1", d.ID, domain.StatusSuccess))
	if err != nil || first != domain.CallbackApplied {
		t.Fatalf("first = %s, %v", first, err)
	}
	for i := 0; i < 4; i++ {
		out, err := repo.SaveCallbackAndApply(ctx, cb("c-1", d.ID, domain.StatusSuccess))
		if err != nil || out != domain.CallbackDuplicate {
			t.Fatalf("repeat %d = %s, %v; want DUPLICATE", i, out, err)
		}
	}
	mustStatus(t, repo, d.ID, domain.StatusSuccess)
}

func testCallbackDuplicateAfterRetry(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)
	if out, err := repo.SaveCallbackAndApply(ctx, cb("c-f", d.ID, domain.StatusFailed)); err != nil || out != domain.CallbackApplied {
		t.Fatalf("FAILED = %s, %v", out, err)
	}
	if n, err := repo.RetryFailed(ctx, b.ID); err != nil || n != 1 {
		t.Fatalf("RetryFailed = %d, %v", n, err)
	}
	// The document is PENDING again, yet a redelivery of the old callback is a
	// harmless duplicate rather than an invalid transition.
	out, err := repo.SaveCallbackAndApply(ctx, cb("c-f", d.ID, domain.StatusFailed))
	if err != nil || out != domain.CallbackDuplicate {
		t.Fatalf("redelivery = %s, %v; want DUPLICATE", out, err)
	}
	mustStatus(t, repo, d.ID, domain.StatusPending)
}

func testCallbackDuplicateConcurrent(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)

	var mu sync.Mutex
	outcomes := make(map[domain.CallbackOutcome]int)
	runParallel(20, func(int) {
		out, err := repo.SaveCallbackAndApply(ctx, cb("c-dup", d.ID, domain.StatusSuccess))
		if err != nil {
			t.Errorf("SaveCallbackAndApply: %v", err)
			return
		}
		mu.Lock()
		outcomes[out]++
		mu.Unlock()
	})
	if outcomes[domain.CallbackApplied] != 1 || outcomes[domain.CallbackDuplicate] != 19 || len(outcomes) != 2 {
		t.Fatalf("outcomes = %v, want exactly 1 APPLIED and 19 DUPLICATE", outcomes)
	}
	mustStatus(t, repo, d.ID, domain.StatusSuccess)
}

func testCallbackLateFailed(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)

	if out, err := repo.SaveCallbackAndApply(ctx, cb("c-a", d.ID, domain.StatusSuccess)); err != nil || out != domain.CallbackApplied {
		t.Fatalf("SUCCESS = %s, %v", out, err)
	}
	out, err := repo.SaveCallbackAndApply(ctx, cb("c-b", d.ID, domain.StatusFailed))
	if err != nil || out != domain.CallbackIgnored {
		t.Fatalf("late FAILED = %s, %v; want IGNORED", out, err)
	}
	mustStatus(t, repo, d.ID, domain.StatusSuccess)
}

func testCallbackPending(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 1)
	_, err := repo.SaveCallbackAndApply(ctx, cb("c-1", docs[0].ID, domain.StatusSuccess))
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
	mustStatus(t, repo, docs[0].ID, domain.StatusPending)

	// The rejected callback must not have been stored: once the document is
	// really PROCESSING the very same callback is applied, not a duplicate.
	d := claimOne(t, repo, docs[0].ID)
	out, err := repo.SaveCallbackAndApply(ctx, cb("c-1", d.ID, domain.StatusSuccess))
	if err != nil || out != domain.CallbackApplied {
		t.Fatalf("retry of rejected callback = %s, %v; want APPLIED", out, err)
	}
}

func testCallbackUnknownDocument(t *testing.T, repo repository.Repository) {
	for _, id := range []string{"999", "not-a-number"} {
		_, err := repo.SaveCallbackAndApply(context.Background(), cb("c-1", id, domain.StatusSuccess))
		if !errors.Is(err, domain.ErrDocumentNotFound) {
			t.Errorf("document %q: err = %v, want ErrDocumentNotFound", id, err)
		}
	}
}

func testCallbackInvalidResult(t *testing.T, repo repository.Repository) {
	_, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)
	_, err := repo.SaveCallbackAndApply(context.Background(), cb("c-1", d.ID, domain.StatusPending))
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
}

func testRetryFailedMaxAttempts(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 2)
	id := docs[0].ID

	for attempt := 1; attempt <= domain.DefaultMaxAttempts; attempt++ {
		d := claimOne(t, repo, id)
		if d.AttemptCount != attempt {
			t.Fatalf("attempt = %d, want %d", d.AttemptCount, attempt)
		}
		if err := repo.MarkRequestFailed(ctx, id, attempt, "boom", 0); err != nil {
			t.Fatal(err)
		}
		moved, err := repo.RetryFailed(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if attempt == domain.DefaultMaxAttempts {
			want = 0
		}
		if moved != want {
			t.Fatalf("after attempt %d RetryFailed moved %d, want %d", attempt, moved, want)
		}
	}
	final := mustStatus(t, repo, id, domain.StatusFailed)
	if final.AttemptCount != domain.DefaultMaxAttempts {
		t.Errorf("attempt_count = %d, want %d", final.AttemptCount, domain.DefaultMaxAttempts)
	}
	if _, err := repo.ClaimDocument(ctx, id); !errors.Is(err, domain.ErrNotProcessable) {
		t.Errorf("claiming an exhausted FAILED document: err = %v, want ErrNotProcessable", err)
	}
}

func testRetryFailedBackoff(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 1)
	d := claimOne(t, repo, docs[0].ID)
	if err := repo.MarkRequestFailed(ctx, d.ID, d.AttemptCount, "boom", time.Hour); err != nil {
		t.Fatal(err)
	}
	failed := mustStatus(t, repo, d.ID, domain.StatusFailed)
	if failed.NextRetryAt == nil {
		t.Fatal("next_retry_at must be set when retryAfter > 0")
	}
	moved, err := repo.RetryFailed(ctx, b.ID)
	if err != nil || moved != 0 {
		t.Fatalf("RetryFailed during backoff = %d, %v; want 0", moved, err)
	}
}

func testRecoverStuck(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	_, docs := newBatch(t, repo, 2)
	stuck := claimOne(t, repo, docs[0].ID)

	n, err := repo.RecoverStuck(ctx, time.Hour)
	if err != nil || n != 0 {
		t.Fatalf("RecoverStuck(1h) = %d, %v; want 0 (lease still valid)", n, err)
	}
	n, err = repo.RecoverStuck(ctx, 0)
	if err != nil || n != 1 {
		t.Fatalf("RecoverStuck(0) = %d, %v; want 1", n, err)
	}
	got := mustStatus(t, repo, stuck.ID, domain.StatusFailed)
	if got.LastError == "" || got.LockedAt != nil {
		t.Errorf("recovered document: %+v", got)
	}
	mustStatus(t, repo, docs[1].ID, domain.StatusPending) // PENDING documents are never touched
}

func testBatchStatus(t *testing.T, repo repository.Repository) {
	ctx := context.Background()
	b, docs := newBatch(t, repo, 4)

	view, err := repo.BatchStatus(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Total != 4 || view.Counts[domain.StatusPending] != 4 || view.Status != domain.BatchPending || view.Started {
		t.Fatalf("fresh batch view = %+v", view)
	}
	for _, s := range domain.AllDocStatuses() {
		if _, ok := view.Counts[s]; !ok {
			t.Errorf("status %s missing from counts (zero counts must be present)", s)
		}
	}

	a := claimOne(t, repo, docs[0].ID)
	f := claimOne(t, repo, docs[1].ID)
	p := claimOne(t, repo, docs[2].ID)
	_ = p
	if _, err := repo.SaveCallbackAndApply(ctx, cb("c-a", a.ID, domain.StatusSuccess)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SaveCallbackAndApply(ctx, cb("c-f", f.ID, domain.StatusFailed)); err != nil {
		t.Fatal(err)
	}
	view, err = repo.BatchStatus(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[domain.DocStatus]int{
		domain.StatusPending: 1, domain.StatusProcessing: 1, domain.StatusSuccess: 1, domain.StatusFailed: 1,
	}
	for s, n := range want {
		if view.Counts[s] != n {
			t.Errorf("count[%s] = %d, want %d", s, view.Counts[s], n)
		}
	}
	if view.RetryableFailed != 1 || !view.Started || view.Status != domain.BatchInProgress {
		t.Errorf("view = %+v", view)
	}
}

func testCanceledContext(t *testing.T, repo repository.Repository) {
	b, docs := newBatch(t, repo, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.ClaimPending(ctx, b.ID, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("ClaimPending: err = %v, want context.Canceled", err)
	}
	if _, err := repo.ClaimDocument(ctx, docs[0].ID); !errors.Is(err, context.Canceled) {
		t.Errorf("ClaimDocument: err = %v, want context.Canceled", err)
	}
	mustStatus(t, repo, docs[0].ID, domain.StatusPending)
}
