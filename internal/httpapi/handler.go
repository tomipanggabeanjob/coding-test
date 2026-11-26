// Package httpapi exposes the service over HTTP: batch creation, processing,
// retry, status, and the Principal callback endpoint. Handlers stay thin: decode,
// call one service method, map sentinel errors to status codes.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/service"
)

const maxBodyBytes = 4 << 20

type Handler struct {
	svc    *service.Service
	logger *slog.Logger
	mux    *http.ServeMux

	// Background ProcessBatch runs are tied to bgCtx so Shutdown can stop them.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup
}

func NewHandler(svc *service.Service, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	bgCtx, cancel := context.WithCancel(context.Background())
	h := &Handler{svc: svc, logger: logger, mux: http.NewServeMux(), bgCtx: bgCtx, bgCancel: cancel}
	h.mux.HandleFunc("POST /batches", h.createBatch)
	h.mux.HandleFunc("GET /batches/{id}", h.getBatch)
	h.mux.HandleFunc("GET /batches/{id}/documents", h.listDocuments)
	h.mux.HandleFunc("POST /batches/{id}/process", h.processBatch)
	h.mux.HandleFunc("POST /batches/{id}/retry", h.retryFailed)
	h.mux.HandleFunc("POST /callbacks", h.callback)
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	h.mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, svc.Metrics())
	})
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// Wait blocks until every background ProcessBatch started so far has finished.
func (h *Handler) Wait() { h.bg.Wait() }

// Shutdown cancels background processing (claimed-but-unsent documents go back
// to PENDING) and waits for it to finish or for ctx to expire.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.bgCancel()
	done := make(chan struct{})
	go func() {
		h.bg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("background processing still running: %w", ctx.Err())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorBody struct {
	Error string `json:"error"`
}

// writeError maps sentinel errors to HTTP status codes. Unexpected errors are
// logged and reported without internal details.
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, errorBody{err.Error()})
	case errors.Is(err, domain.ErrBatchNotFound), errors.Is(err, domain.ErrDocumentNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{err.Error()})
	case errors.Is(err, domain.ErrBatchExists),
		errors.Is(err, domain.ErrInvalidTransition),
		errors.Is(err, domain.ErrNotProcessable):
		writeJSON(w, http.StatusConflict, errorBody{err.Error()})
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusServiceUnavailable, errorBody{"request canceled or timed out"})
	default:
		h.logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{"internal error"})
	}
}

// decode reads a JSON body (size-limited). It returns the raw bytes as well, so
// callbacks can be stored verbatim for audit.
func decode(w http.ResponseWriter, r *http.Request, v any) ([]byte, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read body: %v", domain.ErrInvalidInput, err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return nil, fmt.Errorf("%w: malformed JSON: %v", domain.ErrInvalidInput, err)
	}
	return raw, nil
}

type createBatchRequest struct {
	BatchNo   string                 `json:"batch_no"`
	Documents []domain.DocumentInput `json:"documents"`
}

type createBatchResponse struct {
	Batch     domain.Batch      `json:"batch"`
	Documents []domain.Document `json:"documents"`
}

func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if _, err := decode(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	batch, docs, err := h.svc.CreateBatch(r.Context(), req.BatchNo, req.Documents)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createBatchResponse{Batch: batch, Documents: docs})
}

func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	view, err := h.svc.GetBatchStatus(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) listDocuments(w http.ResponseWriter, r *http.Request) {
	docs, err := h.svc.ListDocuments(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, docs)
}

// processBatch answers 202 immediately and processes in the background: a batch
// can take long, and the work must outlive the HTTP request.
func (h *Handler) processBatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.svc.GetBatchStatus(r.Context(), id); err != nil {
		h.writeError(w, r, err)
		return
	}
	h.bg.Add(1)
	go func() {
		defer h.bg.Done()
		if err := h.svc.ProcessBatch(h.bgCtx, id); err != nil {
			h.logger.Warn("batch processing ended with error", "batch_id", id, "error", err)
			return
		}
		h.logger.Info("batch processing finished", "batch_id", id)
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"batch_id": id, "status": "processing started"})
}

func (h *Handler) retryFailed(w http.ResponseWriter, r *http.Request) {
	n, err := h.svc.RetryFailed(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"retried": n})
}

type callbackRequest struct {
	CallbackID string           `json:"callback_id"`
	DocumentID string           `json:"document_id"`
	Result     domain.DocStatus `json:"result"`
}

// callback receives Principal's asynchronous result:
//
//	200 accepted (first delivery) or duplicate (idempotent)
//	400 invalid body
//	404 unknown document
//	409 document cannot take a result (still PENDING)
//	500 internal error
func (h *Handler) callback(w http.ResponseWriter, r *http.Request) {
	var req callbackRequest
	raw, err := decode(w, r, &req)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	outcome, err := h.svc.HandleCallback(r.Context(), domain.Callback{
		CallbackID: req.CallbackID,
		DocumentID: req.DocumentID,
		Result:     req.Result,
		RawBody:    raw,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"outcome": outcome})
}
