//go:build e2e

// Package e2e is a black-box test against the running docker compose stack:
// app (Postgres-backed) + mock principal. Run it with
//
//	docker compose run --build --rm e2e
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"bulkdoc/internal/domain"
)

const settleTimeout = 90 * time.Second

func base(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type client struct {
	t    *testing.T
	app  string
	prin string
}

func (c *client) call(method, url string, in, out any) int {
	c.t.Helper()
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			c.t.Fatal(err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			c.t.Fatalf("%s %s: decode: %v", method, url, err)
		}
	}
	return resp.StatusCode
}

func (c *client) status(batchID string) domain.BatchStatusView {
	c.t.Helper()
	var v domain.BatchStatusView
	if code := c.call("GET", c.app+"/batches/"+batchID, nil, &v); code != 200 {
		c.t.Fatalf("GET batch: %d", code)
	}
	return v
}

// settle waits until nothing is PENDING or PROCESSING (callbacks are asynchronous).
func (c *client) settle(batchID string) domain.BatchStatusView {
	c.t.Helper()
	deadline := time.After(settleTimeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		v := c.status(batchID)
		if v.Counts[domain.StatusPending] == 0 && v.Counts[domain.StatusProcessing] == 0 {
			return v
		}
		select {
		case <-deadline:
			c.t.Fatalf("batch did not settle: %+v", v)
		case <-tick.C:
		}
	}
}

type principalStats struct {
	MaxInFlight   int            `json:"max_in_flight"`
	Requests      map[string]int `json:"requests"`
	Deduped       int            `json:"deduped"`
	CallbacksSent int            `json:"callbacks_sent"`
}

func TestEndToEnd(t *testing.T) {
	c := &client{t: t, app: base("APP_URL", "http://localhost:8080"), prin: base("PRINCIPAL_URL", "http://localhost:9090")}
	if code := c.call("POST", c.prin+"/reset", nil, nil); code != 200 {
		t.Fatalf("reset mock principal: %d", code)
	}

	const okDocs = 15
	docs := []map[string]any{}
	for i := 1; i <= okDocs; i++ {
		docs = append(docs, map[string]any{"external_ref": fmt.Sprintf("ok-%02d", i), "payload": map[string]int{"n": i}})
	}
	docs = append(docs,
		map[string]any{"external_ref": "fail-1"},
		map[string]any{"external_ref": "fail-2"},
		map[string]any{"external_ref": "reject-1"},
	)
	total := len(docs)

	batchNo := fmt.Sprintf("E2E-%d", time.Now().UnixNano())
	var created struct {
		Batch     domain.Batch      `json:"batch"`
		Documents []domain.Document `json:"documents"`
	}
	if code := c.call("POST", c.app+"/batches", map[string]any{"batch_no": batchNo, "documents": docs}, &created); code != http.StatusCreated {
		t.Fatalf("create batch: %d", code)
	}
	id := created.Batch.ID

	t.Run("duplicate batch number is a conflict", func(t *testing.T) {
		code := c.call("POST", c.app+"/batches", map[string]any{"batch_no": batchNo, "documents": docs[:1]}, nil)
		if code != http.StatusConflict {
			t.Errorf("status = %d, want 409", code)
		}
	})

	if v := c.status(id); v.Status != domain.BatchPending || v.Total != total {
		t.Fatalf("fresh batch: %+v", v)
	}

	// Rounds: process, wait for callbacks, retry. Failing documents get exactly 3 attempts.
	for round := 1; round <= domain.DefaultMaxAttempts; round++ {
		if code := c.call("POST", c.app+"/batches/"+id+"/process", nil, nil); code != http.StatusAccepted {
			t.Fatalf("round %d process: %d", round, code)
		}
		v := c.settle(id)
		var retried map[string]int
		if code := c.call("POST", c.app+"/batches/"+id+"/retry", nil, &retried); code != 200 {
			t.Fatalf("round %d retry: %d", round, code)
		}
		wantRetried := 3
		if round == domain.DefaultMaxAttempts {
			wantRetried = 0 // attempts exhausted
		}
		if retried["retried"] != wantRetried {
			t.Fatalf("round %d: retried %d documents, want %d (view %+v)", round, retried["retried"], wantRetried, v)
		}
		if round == 1 && v.Counts[domain.StatusSuccess] != okDocs {
			t.Fatalf("round 1: %d SUCCESS, want %d", v.Counts[domain.StatusSuccess], okDocs)
		}
	}

	final := c.status(id)
	if final.Status != domain.BatchCompleted || final.Counts[domain.StatusSuccess] != okDocs ||
		final.Counts[domain.StatusFailed] != 3 || final.RetryableFailed != 0 {
		t.Fatalf("final view: %+v", final)
	}

	var list []domain.Document
	if code := c.call("GET", c.app+"/batches/"+id+"/documents", nil, &list); code != 200 || len(list) != total {
		t.Fatalf("documents: %d, %d items", code, len(list))
	}
	byRef := map[string]domain.Document{}
	for _, d := range list {
		byRef[d.ExternalRef] = d
	}

	t.Run("principal saw the right number of submissions", func(t *testing.T) {
		var st principalStats
		if code := c.call("GET", c.prin+"/stats", nil, &st); code != 200 {
			t.Fatalf("stats: %d", code)
		}
		if st.MaxInFlight > 3 {
			t.Errorf("principal saw %d concurrent submissions; the limit is 3", st.MaxInFlight)
		}
		if st.MaxInFlight < 2 {
			t.Errorf("principal saw only %d concurrent submissions; expected the pool to run concurrently", st.MaxInFlight)
		}
		t.Logf("max concurrent submissions at principal: %d", st.MaxInFlight)
		for ref, n := range st.Requests {
			want := 1 // SUCCESS documents are never reprocessed
			if ref == "fail-1" || ref == "fail-2" || ref == "reject-1" {
				want = domain.DefaultMaxAttempts
			}
			if n != want {
				t.Errorf("%s: principal received %d submissions, want %d", ref, n, want)
			}
		}
		if st.Deduped != 0 {
			t.Errorf("principal deduplicated %d requests; every attempt should have a fresh RequestID", st.Deduped)
		}
		if st.CallbacksSent < okDocs {
			t.Errorf("only %d callbacks delivered", st.CallbacksSent)
		}
	})

	t.Run("final document states", func(t *testing.T) {
		for ref, d := range byRef {
			switch ref {
			case "fail-1", "fail-2", "reject-1":
				if d.Status != domain.StatusFailed || d.AttemptCount != 3 {
					t.Errorf("%s = %s/%d, want FAILED/3", ref, d.Status, d.AttemptCount)
				}
			default:
				if d.Status != domain.StatusSuccess || d.AttemptCount != 1 {
					t.Errorf("%s = %s/%d, want SUCCESS/1", ref, d.Status, d.AttemptCount)
				}
			}
		}
	})

	t.Run("callback endpoint status codes", func(t *testing.T) {
		ok := byRef["ok-01"].ID
		cb := func(id, doc, result string) map[string]string {
			return map[string]string{"callback_id": id, "document_id": doc, "result": result}
		}
		var out map[string]string

		// Late FAILED after SUCCESS is accepted but never overwrites it.
		if code := c.call("POST", c.app+"/callbacks", cb("late-failed", ok, "FAILED"), &out); code != 200 || out["outcome"] != "IGNORED" {
			t.Errorf("late FAILED: %d %v", code, out)
		}
		if code := c.call("POST", c.app+"/callbacks", cb("late-failed", ok, "FAILED"), &out); code != 200 || out["outcome"] != "DUPLICATE" {
			t.Errorf("repeated callback: %d %v", code, out)
		}
		if code := c.call("POST", c.app+"/callbacks", cb("x", "99999999", "SUCCESS"), nil); code != 404 {
			t.Errorf("unknown document: %d, want 404", code)
		}
		if code := c.call("POST", c.app+"/callbacks", cb("", ok, "SUCCESS"), nil); code != 400 {
			t.Errorf("missing callback_id: %d, want 400", code)
		}
		var d domain.Document
		var docs []domain.Document
		c.call("GET", c.app+"/batches/"+id+"/documents", nil, &docs)
		for _, x := range docs {
			if x.ID == ok {
				d = x
			}
		}
		if d.Status != domain.StatusSuccess {
			t.Errorf("SUCCESS document became %s", d.Status)
		}
	})

	t.Run("duplicate callbacks were absorbed", func(t *testing.T) {
		var m struct {
			Submits            int `json:"submits"`
			CallbacksApplied   int `json:"callbacks_applied"`
			CallbacksDuplicate int `json:"callbacks_duplicate"`
		}
		if code := c.call("GET", c.app+"/metrics", nil, &m); code != 200 {
			t.Fatalf("metrics: %d", code)
		}
		// The mock sends every callback twice: one APPLIED, one DUPLICATE.
		if m.CallbacksApplied < okDocs || m.CallbacksDuplicate < okDocs {
			t.Errorf("metrics = %+v, want at least %d applied and %d duplicate callbacks", m, okDocs, okDocs)
		}
	})
}
