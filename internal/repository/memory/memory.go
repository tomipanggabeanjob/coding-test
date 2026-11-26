// Package memory is a mutex-based, in-process implementation of
// repository.Repository. The mutex only ever guards in-memory state changes; it
// is never held across a network call.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/repository"
)

var _ repository.Repository = (*Repo)(nil)

type Repo struct {
	mu        sync.Mutex
	now       func() time.Time
	nextBatch int
	nextDoc   int
	batches   map[string]domain.Batch
	batchNos  map[string]string
	docs      map[string]*domain.Document
	order     map[string][]string
	callbacks map[string]domain.Callback
	applied   int
}

type Option func(*Repo)

// WithClock injects the time source (tests use a fake clock).
func WithClock(now func() time.Time) Option {
	return func(r *Repo) { r.now = now }
}

func New(opts ...Option) *Repo {
	r := &Repo{
		now:       time.Now,
		batches:   make(map[string]domain.Batch),
		batchNos:  make(map[string]string),
		docs:      make(map[string]*domain.Document),
		order:     make(map[string][]string),
		callbacks: make(map[string]domain.Callback),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// CallbackCount is the number of stored callback rows (test helper).
func (r *Repo) CallbackCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.callbacks)
}

// AppliedCount is how many callbacks changed a document (test helper): the
// "side effect" counter used to prove idempotency.
func (r *Repo) AppliedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

func ctxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory repository: %w", err)
	}
	return nil
}

func clone(d *domain.Document) domain.Document {
	c := *d
	c.Payload = bytes.Clone(d.Payload)
	if d.LockedAt != nil {
		t := *d.LockedAt
		c.LockedAt = &t
	}
	if d.NextRetryAt != nil {
		t := *d.NextRetryAt
		c.NextRetryAt = &t
	}
	return c
}

func (r *Repo) CreateBatch(ctx context.Context, batchNo string, inputs []domain.DocumentInput) (domain.Batch, []domain.Document, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.Batch{}, nil, err
	}
	seen := make(map[string]struct{}, len(inputs))
	for _, in := range inputs {
		if in.ExternalRef == "" {
			return domain.Batch{}, nil, fmt.Errorf("%w: empty external_ref", domain.ErrInvalidInput)
		}
		if _, dup := seen[in.ExternalRef]; dup {
			return domain.Batch{}, nil, fmt.Errorf("%w: duplicate external_ref %q", domain.ErrInvalidInput, in.ExternalRef)
		}
		seen[in.ExternalRef] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.batchNos[batchNo]; exists {
		return domain.Batch{}, nil, fmt.Errorf("batch %q: %w", batchNo, domain.ErrBatchExists)
	}
	now := r.now()
	r.nextBatch++
	batch := domain.Batch{ID: strconv.Itoa(r.nextBatch), BatchNo: batchNo, CreatedAt: now}
	r.batches[batch.ID] = batch
	r.batchNos[batchNo] = batch.ID

	created := make([]domain.Document, 0, len(inputs))
	ids := make([]string, 0, len(inputs))
	for _, in := range inputs {
		r.nextDoc++
		d := &domain.Document{
			ID:          strconv.Itoa(r.nextDoc),
			BatchID:     batch.ID,
			ExternalRef: in.ExternalRef,
			Status:      domain.StatusPending,
			MaxAttempts: domain.DefaultMaxAttempts,
			Payload:     bytes.Clone(in.Payload),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		r.docs[d.ID] = d
		ids = append(ids, d.ID)
		created = append(created, clone(d))
	}
	r.order[batch.ID] = ids
	return batch, created, nil
}

func (r *Repo) GetBatch(ctx context.Context, batchID string) (domain.Batch, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.Batch{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batches[batchID]
	if !ok {
		return domain.Batch{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	return b, nil
}

func claimable(d *domain.Document) bool {
	return d.Status == domain.StatusPending && d.AttemptCount < d.MaxAttempts
}

func claim(d *domain.Document, now time.Time) {
	d.Status = domain.StatusProcessing
	d.AttemptCount++
	locked := now
	d.LockedAt = &locked
	d.NextRetryAt = nil
	d.UpdatedAt = now
}

func (r *Repo) ClaimPending(ctx context.Context, batchID string, limit int) ([]domain.Document, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: claim limit must be positive", domain.ErrInvalidInput)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ids, ok := r.order[batchID]
	if !ok {
		return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	now := r.now()
	var claimed []domain.Document
	for _, id := range ids {
		if len(claimed) == limit {
			break
		}
		d := r.docs[id]
		if !claimable(d) {
			continue
		}
		claim(d, now)
		claimed = append(claimed, clone(d))
	}
	return claimed, nil
}

func (r *Repo) ClaimDocument(ctx context.Context, docID string) (domain.Document, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.Document{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[docID]
	if !ok {
		return domain.Document{}, fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	if !claimable(d) {
		return domain.Document{}, fmt.Errorf("document %q is %s with %d/%d attempts: %w",
			docID, d.Status, d.AttemptCount, d.MaxAttempts, domain.ErrNotProcessable)
	}
	claim(d, r.now())
	return clone(d), nil
}

// guarded runs fn on the document only while it is PROCESSING on the given attempt.
func (r *Repo) guarded(ctx context.Context, docID string, attempt int, fn func(d *domain.Document, now time.Time)) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[docID]
	if !ok {
		return fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	if d.Status == domain.StatusProcessing && d.AttemptCount == attempt {
		fn(d, r.now())
	}
	return nil
}

func (r *Repo) MarkRequestAccepted(ctx context.Context, docID string, attempt int) error {
	return r.guarded(ctx, docID, attempt, func(d *domain.Document, now time.Time) {
		locked := now
		d.LockedAt = &locked
		d.LastError = ""
		d.UpdatedAt = now
	})
}

func (r *Repo) MarkRequestFailed(ctx context.Context, docID string, attempt int, reason string, retryAfter time.Duration) error {
	return r.guarded(ctx, docID, attempt, func(d *domain.Document, now time.Time) {
		d.Status = domain.StatusFailed
		d.LastError = reason
		d.LockedAt = nil
		d.NextRetryAt = nil
		if retryAfter > 0 {
			next := now.Add(retryAfter)
			d.NextRetryAt = &next
		}
		d.UpdatedAt = now
	})
}

func (r *Repo) ReleaseClaim(ctx context.Context, docID string, attempt int) error {
	return r.guarded(ctx, docID, attempt, func(d *domain.Document, now time.Time) {
		d.Status = domain.StatusPending
		d.AttemptCount--
		d.LockedAt = nil
		d.UpdatedAt = now
	})
}

func (r *Repo) SaveCallbackAndApply(ctx context.Context, cb domain.Callback) (domain.CallbackOutcome, error) {
	if err := ctxErr(ctx); err != nil {
		return "", err
	}
	if cb.Result != domain.StatusSuccess && cb.Result != domain.StatusFailed {
		return "", fmt.Errorf("%w: callback result must be SUCCESS or FAILED, got %q", domain.ErrInvalidInput, cb.Result)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[cb.DocumentID]
	if !ok {
		return "", fmt.Errorf("document %q: %w", cb.DocumentID, domain.ErrDocumentNotFound)
	}
	if _, seen := r.callbacks[cb.CallbackID]; seen {
		return domain.CallbackDuplicate, nil
	}
	if d.Status == domain.StatusPending {
		return "", fmt.Errorf("callback %q for document %q in status %s: %w",
			cb.CallbackID, cb.DocumentID, d.Status, domain.ErrInvalidTransition)
	}

	now := r.now()
	cb.ReceivedAt = now
	cb.RawBody = bytes.Clone(cb.RawBody)
	r.callbacks[cb.CallbackID] = cb

	// Only PROCESSING may change: SUCCESS stays terminal, late FAILED callbacks are no-ops.
	if d.Status != domain.StatusProcessing {
		return domain.CallbackIgnored, nil
	}
	d.Status = cb.Result
	d.LockedAt = nil
	d.NextRetryAt = nil
	d.LastError = ""
	if cb.Result == domain.StatusFailed {
		d.LastError = "principal reported FAILED"
	}
	d.UpdatedAt = now
	r.applied++
	return domain.CallbackApplied, nil
}

func (r *Repo) RetryFailed(ctx context.Context, batchID string) (int, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ids, ok := r.order[batchID]
	if !ok {
		return 0, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	now := r.now()
	moved := 0
	for _, id := range ids {
		d := r.docs[id]
		if !d.Retryable() {
			continue
		}
		if d.NextRetryAt != nil && d.NextRetryAt.After(now) {
			continue
		}
		d.Status = domain.StatusPending
		d.NextRetryAt = nil
		d.UpdatedAt = now
		moved++
	}
	return moved, nil
}

func (r *Repo) RecoverStuck(ctx context.Context, lease time.Duration) (int, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	cutoff := now.Add(-lease)
	recovered := 0
	for _, d := range r.docs {
		if d.Status != domain.StatusProcessing || d.LockedAt == nil || !d.LockedAt.Before(cutoff) {
			continue
		}
		d.Status = domain.StatusFailed
		d.LastError = "lease expired"
		d.LockedAt = nil
		d.UpdatedAt = now
		recovered++
	}
	return recovered, nil
}

func (r *Repo) BatchStatus(ctx context.Context, batchID string) (domain.BatchStatusView, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.BatchStatusView{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batches[batchID]
	if !ok {
		return domain.BatchStatusView{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	view := domain.BatchStatusView{
		BatchID: b.ID,
		BatchNo: b.BatchNo,
		Counts:  make(map[domain.DocStatus]int, 4),
	}
	for _, s := range domain.AllDocStatuses() {
		view.Counts[s] = 0
	}
	for _, id := range r.order[batchID] {
		d := r.docs[id]
		view.Total++
		view.Counts[d.Status]++
		if d.Retryable() {
			view.RetryableFailed++
		}
		if d.AttemptCount > 0 {
			view.Started = true
		}
	}
	view.Status = domain.DeriveBatchStatus(view)
	return view, nil
}

func (r *Repo) GetDocument(ctx context.Context, docID string) (domain.Document, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.Document{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[docID]
	if !ok {
		return domain.Document{}, fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	return clone(d), nil
}

func (r *Repo) ListDocuments(ctx context.Context, batchID string) ([]domain.Document, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ids, ok := r.order[batchID]
	if !ok {
		return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	docs := make([]domain.Document, 0, len(ids))
	for _, id := range ids {
		docs = append(docs, clone(r.docs[id]))
	}
	return docs, nil
}
