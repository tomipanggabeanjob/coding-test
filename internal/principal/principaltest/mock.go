// Package principaltest provides a fully controllable fake of the Principal
// client for tests: in-flight tracking, a barrier that holds calls until N are
// in flight, scripted behaviours and per-document call counters.
package principaltest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"bulkdoc/internal/principal"
)

// ErrRejected is the error scripted behaviours return.
var ErrRejected = errors.New("principaltest: submit rejected")

// Behavior decides the outcome of one Submit call.
type Behavior func(ctx context.Context, req principal.SubmitRequest) error

// Accept always accepts the request.
func Accept(context.Context, principal.SubmitRequest) error { return nil }

// Fail always fails with ErrRejected.
func Fail(context.Context, principal.SubmitRequest) error { return ErrRejected }

// BlockUntilDone blocks until the context ends and returns its error.
func BlockUntilDone(ctx context.Context, _ principal.SubmitRequest) error {
	<-ctx.Done()
	return ctx.Err()
}

// FailFirst fails the first n calls of every document, then accepts.
func FailFirst(n int) Behavior {
	var mu sync.Mutex
	seen := make(map[string]int)
	return func(_ context.Context, req principal.SubmitRequest) error {
		mu.Lock()
		seen[req.DocumentID]++
		c := seen[req.DocumentID]
		mu.Unlock()
		if c <= n {
			return ErrRejected
		}
		return nil
	}
}

type Mock struct {
	behavior Behavior

	inFlight    atomic.Int32
	maxInFlight atomic.Int32

	barrierAt   int32
	reached     chan struct{}
	reachedOnce sync.Once
	release     chan struct{}
	releaseOnce sync.Once

	mu       sync.Mutex
	calls    map[string]int
	requests []principal.SubmitRequest
}

var _ principal.Client = (*Mock)(nil)

type Option func(*Mock)

// WithBehavior sets the scripted outcome (default: Accept).
func WithBehavior(b Behavior) Option { return func(m *Mock) { m.behavior = b } }

// WithBarrier makes every Submit wait until Release is called. Reached() is
// closed as soon as n calls are in flight at the same time.
func WithBarrier(n int) Option {
	return func(m *Mock) {
		m.barrierAt = int32(n)
		m.reached = make(chan struct{})
		m.release = make(chan struct{})
	}
}

func New(opts ...Option) *Mock {
	m := &Mock{behavior: Accept, calls: make(map[string]int)}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Mock) Submit(ctx context.Context, req principal.SubmitRequest) error {
	n := m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	for {
		cur := m.maxInFlight.Load()
		if n <= cur || m.maxInFlight.CompareAndSwap(cur, n) {
			break
		}
	}

	m.mu.Lock()
	m.calls[req.DocumentID]++
	m.requests = append(m.requests, req)
	m.mu.Unlock()

	if m.release != nil {
		if n >= m.barrierAt {
			m.reachedOnce.Do(func() { close(m.reached) })
		}
		select {
		case <-m.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.behavior(ctx, req)
}

// Reached is closed once the barrier's number of concurrent calls was seen.
func (m *Mock) Reached() <-chan struct{} { return m.reached }

// Release lets every held (and future) call continue. Safe to call repeatedly.
func (m *Mock) Release() {
	if m.release != nil {
		m.releaseOnce.Do(func() { close(m.release) })
	}
}

func (m *Mock) InFlight() int    { return int(m.inFlight.Load()) }
func (m *Mock) MaxInFlight() int { return int(m.maxInFlight.Load()) }

// Calls is how many times Submit ran for the document.
func (m *Mock) Calls(docID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[docID]
}

// TotalCalls is the number of Submit calls across all documents.
func (m *Mock) TotalCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

// Requests returns a copy of every request received, in arrival order.
func (m *Mock) Requests() []principal.SubmitRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]principal.SubmitRequest(nil), m.requests...)
}
