// Command mockprincipal is a stand-in for the external Principal API, used by
// the docker compose stack and the end-to-end test.
//
// Behaviour is driven by the document's external_ref prefix:
//
//	reject*  -> HTTP 500 on submit (no callback)
//	fail*    -> accepts, then sends a FAILED callback
//	anything else -> accepts, then sends a SUCCESS callback
//
// Submit holds the request for SUBMIT_LATENCY, so GET /stats reports the real
// maximum number of concurrent submissions the service produced. Requests are
// deduplicated by their Idempotency-Key (the RequestID), and every callback is
// delivered twice when DUPLICATE_CALLBACKS=true to exercise idempotency.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"bulkdoc/internal/principal"
)

type stats struct {
	MaxInFlight   int            `json:"max_in_flight"`
	Requests      map[string]int `json:"requests"`
	Deduped       int            `json:"deduped"`
	CallbacksSent int            `json:"callbacks_sent"`
}

type server struct {
	callbackURL string
	latency     time.Duration
	duplicate   bool
	client      *http.Client
	logger      *slog.Logger

	mu       sync.Mutex
	inFlight int
	seen     map[string]bool
	st       stats

	callbacks sync.WaitGroup
}

func newServer(callbackURL string, latency time.Duration, duplicate bool, logger *slog.Logger) *server {
	s := &server{callbackURL: callbackURL, latency: latency, duplicate: duplicate, logger: logger,
		client: &http.Client{Timeout: 5 * time.Second}}
	s.reset()
	return s
}

func (s *server) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = make(map[string]bool)
	s.st = stats{Requests: make(map[string]int)}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /submit", s.submit)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, http.StatusOK, s.st)
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) {
		s.reset()
		writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) submit(w http.ResponseWriter, r *http.Request) {
	var req principal.SubmitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = req.RequestID
	}

	s.mu.Lock()
	s.inFlight++
	if s.inFlight > s.st.MaxInFlight {
		s.st.MaxInFlight = s.inFlight
	}
	s.st.Requests[req.ExternalRef]++
	duplicate := s.seen[key]
	if duplicate {
		s.st.Deduped++
	}
	s.seen[key] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()

	select {
	case <-time.After(s.latency):
	case <-r.Context().Done():
		return
	}

	switch {
	case strings.HasPrefix(req.ExternalRef, "reject"):
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "principal rejected the document"})
		return
	case duplicate:
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "duplicate request ignored"})
		return
	}

	result := "SUCCESS"
	if strings.HasPrefix(req.ExternalRef, "fail") {
		result = "FAILED"
	}
	s.callbacks.Add(1)
	go func() {
		defer s.callbacks.Done()
		s.sendCallback("cb-"+key, req.DocumentID, result)
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *server) sendCallback(callbackID, documentID, result string) {
	body, _ := json.Marshal(map[string]string{"callback_id": callbackID, "document_id": documentID, "result": result})
	deliveries := 1
	if s.duplicate {
		deliveries = 2
	}
	for i := 0; i < deliveries; i++ {
		if err := s.deliver(body); err != nil {
			s.logger.Error("callback delivery failed", "callback_id", callbackID, "error", err)
			continue
		}
		s.mu.Lock()
		s.st.CallbacksSent++
		s.mu.Unlock()
	}
}

// deliver retries a few times: the service may still be starting up.
func (s *server) deliver(body []byte) error {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		resp, err := s.client.Post(s.callbackURL, "application/json", bytes.NewReader(body))
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		last = fmt.Errorf("callback endpoint answered HTTP %d", resp.StatusCode)
	}
	return last
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe /healthz and exit")
	flag.Parse()

	addr := getenv("LISTEN_ADDR", ":9090")
	if *healthcheck {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		resp, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://127.0.0.1:" + port + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}

	latency, err := time.ParseDuration(getenv("SUBMIT_LATENCY", "100ms"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "SUBMIT_LATENCY:", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	s := newServer(getenv("CALLBACK_URL", "http://localhost:8080/callbacks"), latency, getenv("DUPLICATE_CALLBACKS", "true") == "true", logger)
	srv := &http.Server{Addr: addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	logger.Info("mock principal listening", "addr", addr, "latency", latency, "callback_url", s.callbackURL)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
	s.callbacks.Wait()
}
