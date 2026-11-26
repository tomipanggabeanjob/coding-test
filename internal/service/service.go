// Package service implements the six use cases of the bulk document processing
// service: CreateBatch, ProcessBatch, ProcessDocument, HandleCallback,
// RetryFailed and GetBatchStatus.
//
// Concurrency model: a single semaphore owned by the Service value caps the
// number of in-flight Principal.Submit calls across *all* batches. It is held
// only while Submit runs, never while waiting on the repository, and no lock
// of any kind is held across a network call.
package service

import (
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"bulkdoc/internal/principal"
	"bulkdoc/internal/repository"
)

// DefaultMaxInFlight is the Principal limit per instance.
const DefaultMaxInFlight = 3

const defaultCleanupTimeout = 5 * time.Second

// Backoff returns how long a document must wait before it may be retried after
// the given (1-based) failed attempt. Zero means "retry immediately".
type Backoff func(attempt int) time.Duration

// ExponentialBackoff doubles the delay on every attempt up to max. When jitter
// is non-nil (returning values in [0,1)) the result is spread over
// [delay/2, delay) so retries of many documents do not synchronise.
func ExponentialBackoff(base, max time.Duration, jitter func() float64) Backoff {
	return func(attempt int) time.Duration {
		if base <= 0 || attempt < 1 {
			return 0
		}
		d := base
		for i := 1; i < attempt && d < max; i++ {
			d *= 2
		}
		if d > max {
			d = max
		}
		if jitter != nil {
			d = d/2 + time.Duration(jitter()*float64(d/2))
		}
		return d
	}
}

// Metrics are cheap process-local counters. They live on the Service value,
// not in package state.
type Metrics struct {
	submits            atomic.Int64
	submitFailures     atomic.Int64
	released           atomic.Int64
	callbacksApplied   atomic.Int64
	callbacksDuplicate atomic.Int64
	callbacksIgnored   atomic.Int64
	inFlight           atomic.Int64
}

// MetricsSnapshot is a point-in-time copy of the counters.
type MetricsSnapshot struct {
	Submits            int64 `json:"submits"`
	SubmitFailures     int64 `json:"submit_failures"`
	ClaimsReleased     int64 `json:"claims_released"`
	CallbacksApplied   int64 `json:"callbacks_applied"`
	CallbacksDuplicate int64 `json:"callbacks_duplicate"`
	CallbacksIgnored   int64 `json:"callbacks_ignored"`
	InFlight           int64 `json:"in_flight"`
}

type Service struct {
	repo           repository.Repository
	principal      principal.Client
	sem            chan struct{}
	workers        int
	submitTimeout  time.Duration
	cleanupTimeout time.Duration
	backoff        Backoff
	logger         *slog.Logger
	metrics        Metrics
}

// Option customises a Service.
type Option func(*Service) error

// WithMaxInFlight sets the global Submit concurrency limit (default 3).
func WithMaxInFlight(n int) Option {
	return func(s *Service) error {
		if n < 1 {
			return fmt.Errorf("max in flight must be >= 1, got %d", n)
		}
		s.sem = make(chan struct{}, n)
		return nil
	}
}

// WithBatchWorkers sets how many documents one ProcessBatch call keeps claimed
// at once (default: the in-flight limit). The global semaphore still bounds
// real Submit concurrency whatever this is set to.
func WithBatchWorkers(n int) Option {
	return func(s *Service) error {
		if n < 1 {
			return fmt.Errorf("batch workers must be >= 1, got %d", n)
		}
		s.workers = n
		return nil
	}
}

// WithSubmitTimeout bounds each Submit call (0 = only the caller's context).
// A timeout counts as a failed attempt.
func WithSubmitTimeout(d time.Duration) Option {
	return func(s *Service) error {
		if d < 0 {
			return errors.New("submit timeout must not be negative")
		}
		s.submitTimeout = d
		return nil
	}
}

// WithBackoff sets the retry delay policy (default: none).
func WithBackoff(b Backoff) Option {
	return func(s *Service) error {
		s.backoff = b
		return nil
	}
}

// WithLogger sets the structured logger (default: discard).
func WithLogger(l *slog.Logger) Option {
	return func(s *Service) error {
		if l != nil {
			s.logger = l
		}
		return nil
	}
}

// WithCleanupTimeout bounds the repository writes that must still happen after
// the caller's context is gone (releasing claims, recording failures).
func WithCleanupTimeout(d time.Duration) Option {
	return func(s *Service) error {
		if d <= 0 {
			return errors.New("cleanup timeout must be positive")
		}
		s.cleanupTimeout = d
		return nil
	}
}

// NewService wires the service. Dependencies are injected; there is no
// package-level state.
func NewService(repo repository.Repository, p principal.Client, opts ...Option) (*Service, error) {
	if repo == nil {
		return nil, errors.New("repository is required")
	}
	if p == nil {
		return nil, errors.New("principal client is required")
	}
	s := &Service{
		repo:           repo,
		principal:      p,
		sem:            make(chan struct{}, DefaultMaxInFlight),
		cleanupTimeout: defaultCleanupTimeout,
		logger:         slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("invalid service option: %w", err)
		}
	}
	if s.workers == 0 {
		s.workers = cap(s.sem)
	}
	return s, nil
}

// Metrics returns a snapshot of the process-local counters.
func (s *Service) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Submits:            s.metrics.submits.Load(),
		SubmitFailures:     s.metrics.submitFailures.Load(),
		ClaimsReleased:     s.metrics.released.Load(),
		CallbacksApplied:   s.metrics.callbacksApplied.Load(),
		CallbacksDuplicate: s.metrics.callbacksDuplicate.Load(),
		CallbacksIgnored:   s.metrics.callbacksIgnored.Load(),
		InFlight:           s.metrics.inFlight.Load(),
	}
}
