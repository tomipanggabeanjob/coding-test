package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"bulkdoc/internal/domain"
	"bulkdoc/internal/principal/principaltest"
	"bulkdoc/internal/repository/memory"
	"bulkdoc/internal/service"
)

func TestCreateBatch_Validation(t *testing.T) {
	svc, _ := newService(t, principaltest.New())
	ctx := context.Background()
	ok := []domain.DocumentInput{{ExternalRef: "a"}}

	tests := []struct {
		name    string
		batchNo string
		docs    []domain.DocumentInput
	}{
		{"empty batch number", "", ok},
		{"blank batch number", "   ", ok},
		{"no documents", "B1", nil},
		{"empty external ref", "B1", []domain.DocumentInput{{ExternalRef: ""}}},
		{"duplicate external ref", "B1", []domain.DocumentInput{{ExternalRef: "a"}, {ExternalRef: "a"}}},
		{"invalid payload", "B1", []domain.DocumentInput{{ExternalRef: "a", Payload: []byte("{nope")}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := svc.CreateBatch(ctx, tt.batchNo, tt.docs)
			if !errors.Is(err, domain.ErrInvalidInput) {
				t.Errorf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestCreateBatch_DuplicateBatchNo(t *testing.T) {
	svc, _ := newService(t, principaltest.New())
	createBatch(t, svc, "B1", 1)
	_, _, err := svc.CreateBatch(context.Background(), "B1", []domain.DocumentInput{{ExternalRef: "z"}})
	if !errors.Is(err, domain.ErrBatchExists) {
		t.Fatalf("err = %v, want ErrBatchExists", err)
	}
}

func TestCreateBatch_StoresPendingDocuments(t *testing.T) {
	svc, repo := newService(t, principaltest.New())
	b, docs := createBatch(t, svc, "  B1  ", 3)
	if b.BatchNo != "B1" {
		t.Errorf("batch number %q was not trimmed", b.BatchNo)
	}
	for _, d := range docs {
		got := getDoc(t, repo, d.ID)
		if got.Status != domain.StatusPending || got.AttemptCount != 0 || got.MaxAttempts != 3 {
			t.Errorf("document %s = %+v", d.ID, got)
		}
	}
}

func TestGetBatchStatus_Lifecycle(t *testing.T) {
	ctx := context.Background()
	mock := principaltest.New(principaltest.WithBehavior(principaltest.FailFirst(1)))
	svc, _ := newService(t, mock)
	b, docs := createBatch(t, svc, "B1", 3)

	assertView := func(step string, want domain.BatchStatus, pending, processing, success, failed int) {
		t.Helper()
		v, err := svc.GetBatchStatus(ctx, b.ID)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		got := [4]int{v.Counts[domain.StatusPending], v.Counts[domain.StatusProcessing], v.Counts[domain.StatusSuccess], v.Counts[domain.StatusFailed]}
		if v.Status != want || got != [4]int{pending, processing, success, failed} || v.Total != 3 {
			t.Errorf("%s: view = %+v, want %s %v", step, v, want, [4]int{pending, processing, success, failed})
		}
	}

	assertView("created", domain.BatchPending, 3, 0, 0, 0)

	if err := svc.ProcessBatch(ctx, b.ID); err != nil { // every document's first Submit fails
		t.Fatal(err)
	}
	assertView("after first round", domain.BatchInProgress, 0, 0, 0, 3)

	if n, _ := svc.RetryFailed(ctx, b.ID); n != 3 {
		t.Fatalf("RetryFailed = %d", n)
	}
	if err := svc.ProcessBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	assertView("accepted, waiting for callbacks", domain.BatchInProgress, 0, 3, 0, 0)

	for _, d := range docs {
		if _, err := svc.HandleCallback(ctx, cbFor(d.ID, domain.StatusSuccess)); err != nil {
			t.Fatal(err)
		}
	}
	assertView("all succeeded", domain.BatchCompleted, 0, 0, 3, 0)
}

func TestNewService_RejectsBadConfiguration(t *testing.T) {
	repo := memory.New()
	mock := principaltest.New()
	if _, err := service.NewService(nil, mock); err == nil {
		t.Error("nil repository must be rejected")
	}
	if _, err := service.NewService(repo, nil); err == nil {
		t.Error("nil principal client must be rejected")
	}
	for name, opt := range map[string]service.Option{
		"max in flight":   service.WithMaxInFlight(0),
		"batch workers":   service.WithBatchWorkers(0),
		"submit timeout":  service.WithSubmitTimeout(-1),
		"cleanup timeout": service.WithCleanupTimeout(0),
	} {
		if _, err := service.NewService(repo, mock, opt); err == nil {
			t.Errorf("%s: invalid option accepted", name)
		}
	}
}

func BenchmarkProcessBatch(b *testing.B) {
	for _, docs := range []int{100, 1000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			ctx := context.Background()
			svc, _ := newService(b, principaltest.New())
			inputs := make([]domain.DocumentInput, docs)
			for i := range inputs {
				inputs[i] = domain.DocumentInput{ExternalRef: fmt.Sprintf("doc-%d", i)}
			}

			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				b.StopTimer()
				batch, _, err := svc.CreateBatch(ctx, fmt.Sprintf("bench-%d", i), inputs)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := svc.ProcessBatch(ctx, batch.ID); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(docs)*float64(b.N)/b.Elapsed().Seconds(), "docs/s")
		})
	}
}
