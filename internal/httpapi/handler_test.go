package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/httpapi"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/service"
)

type env struct {
	srv  *httptest.Server
	h    *httpapi.Handler
	repo *memory.Repo
	mock *principaltest.Mock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	repo := memory.New()
	mock := principaltest.New()
	svc, err := service.NewService(repo, mock)
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.NewHandler(svc, nil)
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		_ = h.Shutdown(context.Background())
	})
	return &env{srv: srv, h: h, repo: repo, mock: mock}
}

func (e *env) do(t *testing.T, method, path, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return resp.StatusCode
}

func (e *env) createBatch(t *testing.T, no string, n int) createResp {
	t.Helper()
	var docs []map[string]any
	for i := 0; i < n; i++ {
		docs = append(docs, map[string]any{"external_ref": no + "-" + string(rune('a'+i)), "payload": map[string]int{"n": i}})
	}
	body, _ := json.Marshal(map[string]any{"batch_no": no, "documents": docs})
	var out createResp
	if code := e.do(t, "POST", "/batches", string(body), &out); code != http.StatusCreated {
		t.Fatalf("create batch: status %d", code)
	}
	return out
}

type createResp struct {
	Batch     domain.Batch      `json:"batch"`
	Documents []domain.Document `json:"documents"`
}

func TestCreateBatchStatusCodes(t *testing.T) {
	e := newEnv(t)
	e.createBatch(t, "B1", 2)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"duplicate batch number", `{"batch_no":"B1","documents":[{"external_ref":"x"}]}`, http.StatusConflict},
		{"no documents", `{"batch_no":"B2","documents":[]}`, http.StatusBadRequest},
		{"missing batch number", `{"documents":[{"external_ref":"x"}]}`, http.StatusBadRequest},
		{"malformed json", `{"batch_no":`, http.StatusBadRequest},
		{"duplicate external ref", `{"batch_no":"B3","documents":[{"external_ref":"x"},{"external_ref":"x"}]}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out errorResp
			if code := e.do(t, "POST", "/batches", tt.body, &out); code != tt.want {
				t.Errorf("status = %d, want %d (%s)", code, tt.want, out.Error)
			}
		})
	}
}

type errorResp struct {
	Error string `json:"error"`
}

func TestCallbackEndpointStatusCodes(t *testing.T) {
	e := newEnv(t)
	created := e.createBatch(t, "B1", 2)
	if code := e.do(t, "POST", "/batches/"+created.Batch.ID+"/process", "", nil); code != http.StatusAccepted {
		t.Fatalf("process: status %d", code)
	}
	e.h.Wait() // both documents are now PROCESSING (accepted by the mock principal)
	first, second := created.Documents[0].ID, created.Documents[1].ID

	post := func(body string) (int, map[string]any) {
		var out map[string]any
		code := e.do(t, "POST", "/callbacks", body, &out)
		return code, out
	}
	cb := func(id, doc, result string) string {
		b, _ := json.Marshal(map[string]string{"callback_id": id, "document_id": doc, "result": result})
		return string(b)
	}

	if code, out := post(cb("c1", first, "SUCCESS")); code != 200 || out["outcome"] != "APPLIED" {
		t.Errorf("first delivery: %d %v", code, out)
	}
	if code, out := post(cb("c1", first, "SUCCESS")); code != 200 || out["outcome"] != "DUPLICATE" {
		t.Errorf("duplicate delivery must be 200: %d %v", code, out)
	}
	if code, out := post(cb("c2", first, "FAILED")); code != 200 || out["outcome"] != "IGNORED" {
		t.Errorf("late FAILED after SUCCESS: %d %v", code, out)
	}
	if code, _ := post(cb("c3", "9999", "SUCCESS")); code != 404 {
		t.Errorf("unknown document: %d, want 404", code)
	}
	for name, body := range map[string]string{
		"malformed":      `{"callback_id":`,
		"no callback id": cb("", second, "SUCCESS"),
		"no document id": cb("c4", "", "SUCCESS"),
		"bad result":     cb("c4", second, "DONE"),
	} {
		if code, _ := post(body); code != 400 {
			t.Errorf("%s: %d, want 400", name, code)
		}
	}

	doc, err := e.repo.GetDocument(context.Background(), first)
	if err != nil || doc.Status != domain.StatusSuccess {
		t.Errorf("document = %v, %v; want SUCCESS", doc.Status, err)
	}
	if doc2, _ := e.repo.GetDocument(context.Background(), second); doc2.Status != domain.StatusProcessing {
		t.Errorf("rejected callbacks changed the second document to %s", doc2.Status)
	}
}

func TestCallbackForPendingDocumentIsConflict(t *testing.T) {
	e := newEnv(t)
	created := e.createBatch(t, "B1", 1)
	body, _ := json.Marshal(map[string]string{"callback_id": "c1", "document_id": created.Documents[0].ID, "result": "SUCCESS"})
	if code := e.do(t, "POST", "/callbacks", string(body), nil); code != http.StatusConflict {
		t.Errorf("status = %d, want 409", code)
	}
}

func TestBatchLifecycleOverHTTP(t *testing.T) {
	e := newEnv(t)
	created := e.createBatch(t, "B1", 3)
	id := created.Batch.ID

	var view domain.BatchStatusView
	if code := e.do(t, "GET", "/batches/"+id, "", &view); code != 200 || view.Status != domain.BatchPending || view.Total != 3 {
		t.Fatalf("initial view: %d %+v", code, view)
	}
	if code := e.do(t, "POST", "/batches/"+id+"/process", "", nil); code != http.StatusAccepted {
		t.Fatalf("process: %d", code)
	}
	e.h.Wait()
	if e.mock.TotalCalls() != 3 {
		t.Errorf("Submit called %d times, want 3", e.mock.TotalCalls())
	}

	var docs []domain.Document
	if code := e.do(t, "GET", "/batches/"+id+"/documents", "", &docs); code != 200 || len(docs) != 3 {
		t.Fatalf("documents: %d %v", code, docs)
	}
	for _, d := range docs {
		body, _ := json.Marshal(map[string]string{"callback_id": "cb-" + d.ID, "document_id": d.ID, "result": "SUCCESS"})
		if code := e.do(t, "POST", "/callbacks", string(body), nil); code != 200 {
			t.Fatalf("callback: %d", code)
		}
	}
	if code := e.do(t, "GET", "/batches/"+id, "", &view); code != 200 || view.Status != domain.BatchCompleted || view.Counts[domain.StatusSuccess] != 3 {
		t.Errorf("final view: %d %+v", code, view)
	}

	var retried map[string]int
	if code := e.do(t, "POST", "/batches/"+id+"/retry", "", &retried); code != 200 || retried["retried"] != 0 {
		t.Errorf("retry: %d %v", code, retried)
	}
}

func TestUnknownBatchIs404(t *testing.T) {
	e := newEnv(t)
	for _, c := range []struct{ method, path string }{
		{"GET", "/batches/999"}, {"GET", "/batches/999/documents"},
		{"POST", "/batches/999/process"}, {"POST", "/batches/999/retry"},
		{"GET", "/batches/not-a-number"},
	} {
		if code := e.do(t, c.method, c.path, "", nil); code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", c.method, c.path, code)
		}
	}
}

func TestHealthAndMetrics(t *testing.T) {
	e := newEnv(t)
	if code := e.do(t, "GET", "/healthz", "", nil); code != 200 {
		t.Errorf("healthz = %d", code)
	}
	var m service.MetricsSnapshot
	if code := e.do(t, "GET", "/metrics", "", &m); code != 200 {
		t.Errorf("metrics = %d", code)
	}
}

func TestShutdownCancelsBackgroundProcessing(t *testing.T) {
	repo := memory.New()
	mock := principaltest.New(principaltest.WithBarrier(1)) // Submit hangs until released or canceled
	svc, err := service.NewService(repo, mock)
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.NewHandler(svc, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	body, _ := json.Marshal(map[string]any{"batch_no": "B1", "documents": []map[string]string{{"external_ref": "a"}}})
	resp, err := http.Post(srv.URL+"/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var created createResp
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()

	resp, err = http.Post(srv.URL+"/batches/"+created.Batch.ID+"/process", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("process: %v %v", resp, err)
	}
	resp.Body.Close()
	select {
	case <-mock.Reached(): // Submit is in flight
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for Submit to start")
	}

	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	doc, err := repo.GetDocument(context.Background(), created.Documents[0].ID)
	if err != nil || doc.Status != domain.StatusPending || doc.AttemptCount != 0 {
		t.Errorf("document after shutdown = %+v, %v; want PENDING/0 (claim released)", doc, err)
	}
}
