package retryx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func okCheck(context.Context) error { return nil }

func mustHealth(t *testing.T, cfg HealthConfig) *Health {
	t.Helper()
	if cfg.Check == nil {
		cfg.Check = okCheck
	}
	h, err := NewHealth(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestNewHealthRequiresCheck(t *testing.T) {
	if _, err := NewHealth(HealthConfig{}); err == nil {
		t.Fatal("expected an error when Check is nil")
	}
}

// Consecutive-failure / consecutive-success thresholds of the probe state machine.
func TestHealthProbeThresholds(t *testing.T) {
	h := mustHealth(t, HealthConfig{FailureThreshold: 3, SuccessThreshold: 2})
	boom := errors.New("boom")

	h.recordProbe(boom, 0)
	h.recordProbe(boom, 0)
	if h.State() != StateHealthy {
		t.Fatal("must stay healthy below the failure threshold")
	}
	h.recordProbe(nil, 0) // a success resets the streak
	h.recordProbe(boom, 0)
	h.recordProbe(boom, 0)
	if h.State() != StateHealthy {
		t.Fatal("success in between must reset the failure streak")
	}
	h.recordProbe(boom, 0)
	if h.State() != StateUnhealthy {
		t.Fatal("3 consecutive failures must mark it unhealthy")
	}

	h.recordProbe(nil, 0)
	if h.State() != StateUnhealthy {
		t.Fatal("one success is not enough to recover (SuccessThreshold=2)")
	}
	h.recordProbe(nil, 0)
	if h.State() != StateHealthy {
		t.Fatal("2 consecutive successes must recover")
	}
}

// HoldDown keeps the gate closed even when probes are green.
func TestHealthHoldDown(t *testing.T) {
	h := mustHealth(t, HealthConfig{FailureThreshold: 1, SuccessThreshold: 1, HoldDown: 80 * time.Millisecond})
	h.recordProbe(errors.New("boom"), 0)
	h.recordProbe(nil, 0)
	if h.State() != StateUnhealthy {
		t.Fatal("hold-down must keep it unhealthy")
	}
	time.Sleep(100 * time.Millisecond)
	h.recordProbe(nil, 0)
	if h.State() != StateHealthy {
		t.Fatal("must recover once the hold-down has elapsed")
	}
}

// Passive signals from real traffic trip the gate; after that no more requests
// are sent, neither by the retrying call nor by new calls.
func TestPassiveTripStopsRetriesAndFailsFast(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	h := mustHealth(t, HealthConfig{PassiveThreshold: 2})
	rec := &Recorder{}
	client := newTestClient(
		WithMaxAttempts(6),
		WithHealth(h, GatePolicy{RetryWait: -1}), // negative: do not wait, give up
		WithObserver(rec),
	)

	// Call 1: two 503s trip the gate; attempts 3..6 must never be sent.
	_, err := client.Get(srv.URL)
	if !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("err = %v, want ErrUnhealthy", err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("server saw %d requests, want 2 (gate must stop the retries)", got)
	}
	if h.State() != StateUnhealthy {
		t.Fatal("gate should be unhealthy")
	}

	// Call 2: rejected before anything is sent.
	rec.Reset()
	_, err = client.Get(srv.URL)
	if !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("err = %v, want ErrUnhealthy", err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("server saw %d requests, want still 2", got)
	}
	events := rec.Events()
	last := events[len(events)-1]
	if last.Kind != EventCallDone || last.Result != ResultUnhealthy || last.Attempts != 0 {
		t.Fatalf("last event = %+v, want call_done/downstream_unhealthy/0 attempts", last)
	}
	var sawGate bool
	for _, e := range events {
		if e.Kind == EventGate && e.Gate == GateRejected && e.GateAt == GateAtStart {
			sawGate = true
		}
	}
	if !sawGate {
		t.Fatalf("no gate/rejected/start event in %+v", events)
	}
}

// A call that arrives while the downstream is down waits for the recovery
// signal (instead of failing) and is sent exactly once, after recovery.
func TestCallWaitsForRecovery(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := mustHealth(t, HealthConfig{FailureThreshold: 1, SuccessThreshold: 1})
	h.recordProbe(errors.New("down"), 0)
	if h.State() != StateUnhealthy {
		t.Fatal("setup: expected unhealthy")
	}

	rec := &Recorder{}
	client := newTestClient(
		WithHealth(h, GatePolicy{StartWait: 2 * time.Second, ReleaseJitter: -1}),
		WithObserver(rec),
	)

	go func() {
		time.Sleep(60 * time.Millisecond)
		h.recordProbe(nil, 0) // the downstream comes back
	}()

	start := time.Now()
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("returned after %v: it did not wait for recovery", elapsed)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server saw %d requests, want 1", got)
	}
	var waited bool
	for _, e := range rec.Events() {
		if e.Kind == EventGate && e.Gate == GateWaited && e.GateAt == GateAtStart && e.Wait > 0 {
			waited = true
		}
	}
	if !waited {
		t.Fatalf("no gate/waited event in %+v", rec.Events())
	}
}

// If the downstream does not recover within the wait budget, give up with ErrUnhealthy.
func TestWaitTimesOut(t *testing.T) {
	h := mustHealth(t, HealthConfig{FailureThreshold: 1})
	h.recordProbe(errors.New("down"), 0)

	client := newTestClient(WithHealth(h, GatePolicy{StartWait: 50 * time.Millisecond}))
	start := time.Now()
	_, err := client.Get("http://127.0.0.1:1/never-called")
	if !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("err = %v, want ErrUnhealthy", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("gave up before the wait budget elapsed")
	}
}

// The real periodic loop: probes a /health endpoint, flips to unhealthy when it
// fails, and back to healthy when it recovers.
func TestProbeLoopFollowsHealthEndpoint(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	plain := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	h := mustHealth(t, HealthConfig{
		Check:              HTTPCheck(plain, srv.URL),
		Interval:           15 * time.Millisecond,
		RecheckInterval:    10 * time.Millisecond,
		MaxRecheckInterval: 40 * time.Millisecond,
		Timeout:            time.Second,
		FailureThreshold:   2,
		SuccessThreshold:   2,
	})
	h.Start(context.Background())
	defer h.Stop()

	healthy.Store(false)
	waitFor(t, 3*time.Second, "gate to become unhealthy", func() bool { return h.State() == StateUnhealthy })
	if h.LastError() == nil {
		t.Error("LastError should explain why it is unhealthy")
	}

	healthy.Store(true)
	waitFor(t, 3*time.Second, "gate to recover", func() bool { return h.State() == StateHealthy })
}
