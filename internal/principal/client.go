// Package principal defines the client contract for the external Principal API
// and an HTTP implementation. Principal accepts a request synchronously and
// reports the real result later through a callback.
package principal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SubmitRequest is one attempt to process one document.
type SubmitRequest struct {
	// RequestID is "<documentID>:<attempt>"; Principal can use it to deduplicate
	// repeated submissions of the same attempt.
	RequestID   string          `json:"request_id"`
	DocumentID  string          `json:"document_id"`
	BatchID     string          `json:"batch_id"`
	ExternalRef string          `json:"external_ref"`
	Attempt     int             `json:"attempt"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// Client submits documents to Principal.
type Client interface {
	// Submit returns nil when Principal accepted the request (the result arrives
	// by callback) or a wrapped error otherwise. It must honour ctx.
	Submit(ctx context.Context, req SubmitRequest) error
}

// StatusError is returned by HTTPClient for non-2xx answers.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("principal responded with HTTP %d: %s", e.Code, e.Body)
}

// Permanent reports whether retrying the same request is pointless (4xx other
// than 408/429). The service currently treats every error as a failed attempt.
func (e *StatusError) Permanent() bool {
	return e.Code >= 400 && e.Code < 500 && e.Code != http.StatusRequestTimeout && e.Code != http.StatusTooManyRequests
}

// HTTPClient implements Client over HTTP: POST {baseURL}/submit.
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

var _ Client = (*HTTPClient)(nil)

// NewHTTPClient builds a client. A nil hc uses a client with no overall
// timeout; callers bound each call with the context passed to Submit.
func NewHTTPClient(baseURL string, hc *http.Client) *HTTPClient {
	if hc == nil {
		hc = &http.Client{}
	}
	return &HTTPClient{baseURL: strings.TrimRight(baseURL, "/"), http: hc}
}

func (c *HTTPClient) Submit(ctx context.Context, req SubmitRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("encode submit request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/submit", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build submit request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.RequestID)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("submit %s: %w", req.RequestID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("submit %s: %w", req.RequestID, &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(msg))})
}
