package domain

import "testing"

func TestCanTransition(t *testing.T) {
	allowed := map[[2]DocStatus]bool{
		{StatusPending, StatusProcessing}: true,
		{StatusProcessing, StatusSuccess}: true,
		{StatusProcessing, StatusFailed}:  true,
		{StatusProcessing, StatusPending}: true,
		{StatusFailed, StatusPending}:     true,
	}
	for _, from := range AllDocStatuses() {
		for _, to := range AllDocStatuses() {
			want := allowed[[2]DocStatus{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestCanTransition_SuccessIsTerminal(t *testing.T) {
	for _, to := range AllDocStatuses() {
		if CanTransition(StatusSuccess, to) {
			t.Errorf("SUCCESS -> %s must be rejected", to)
		}
	}
	if CanTransition("BOGUS", StatusPending) {
		t.Error("unknown source status must be rejected")
	}
}

func TestDeriveBatchStatus(t *testing.T) {
	counts := func(p, pr, s, f int) map[DocStatus]int {
		return map[DocStatus]int{StatusPending: p, StatusProcessing: pr, StatusSuccess: s, StatusFailed: f}
	}
	tests := []struct {
		name string
		view BatchStatusView
		want BatchStatus
	}{
		{"nothing started", BatchStatusView{Total: 3, Counts: counts(3, 0, 0, 0)}, BatchPending},
		{"some processing", BatchStatusView{Total: 3, Counts: counts(1, 2, 0, 0), Started: true}, BatchInProgress},
		{"all success", BatchStatusView{Total: 3, Counts: counts(0, 0, 3, 0), Started: true}, BatchCompleted},
		{"failed with retries left", BatchStatusView{Total: 3, Counts: counts(0, 0, 2, 1), Started: true, RetryableFailed: 1}, BatchInProgress},
		{"failed exhausted", BatchStatusView{Total: 3, Counts: counts(0, 0, 2, 1), Started: true}, BatchCompleted},
		{"retried back to pending after attempts", BatchStatusView{Total: 2, Counts: counts(2, 0, 0, 0), Started: true}, BatchInProgress},
		{"empty batch", BatchStatusView{Counts: counts(0, 0, 0, 0)}, BatchCompleted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeriveBatchStatus(tt.view); got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	d := Document{ID: "42", AttemptCount: 2}
	if got := d.RequestID(); got != "42:2" {
		t.Errorf("RequestID = %q, want 42:2", got)
	}
}
