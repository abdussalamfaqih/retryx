package retryx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func fastBackoff(*Attempt) time.Duration { return time.Millisecond }

// dropConnection closes the TCP connection without answering: from the client's
// side the outcome is unknown (the server may or may not have processed it).
func dropConnection(w http.ResponseWriter) {
	if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
		_ = conn.Close()
	}
}

func newTestClient(opts ...Option) *http.Client {
	base := &http.Transport{DisableKeepAlives: true}
	opts = append([]Option{WithBackoff(fastBackoff)}, opts...)
	return &http.Client{Transport: New(base, opts...)}
}

// The same idempotency key must be sent on every retry of one logical call.
func TestIdempotencyKeyStableAcrossRetries(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		n := len(keys)
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	client := newTestClient(Before(IdempotencyKey("Idempotency-Key", RandomKey, KeyPerCall)))
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(`{"amount":100}`))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(keys))
	}
	if keys[0] == "" || keys[0] != keys[1] || keys[1] != keys[2] {
		t.Fatalf("idempotency key not stable across retries: %v", keys)
	}
}

// The server creates the order but the connection dies before we see the reply.
// The Reconcile probe finds the order, so we must NOT POST a second time.
func TestReconcileFindsAlreadyCreated(t *testing.T) {
	var mu sync.Mutex
	posts, created := 0, 0
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		posts++
		created++
		mu.Unlock()
		dropConnection(w)
	})
	mux.HandleFunc("/orders/last", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		c := created
		mu.Unlock()
		if c == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"id":"o-1"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	plain := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}} // no retry layer here
	probe := func(ctx context.Context, a *Attempt) (*http.Response, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/orders/last", nil)
		r, err := plain.Do(req)
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		switch r.StatusCode {
		case http.StatusOK:
			return NewResponse(a.Call.Original, http.StatusCreated, nil, []byte(`{"id":"o-1","reconciled":true}`)), nil
		case http.StatusNotFound:
			return nil, nil
		}
		return nil, fmt.Errorf("probe: unexpected status %d", r.StatusCode)
	}

	rec := &Recorder{}
	client := newTestClient(Reconcile(Reconciler(probe, false)), WithObserver(rec))
	ctx := WithOperation(context.Background(), "create_order")
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/orders", strings.NewReader(`{"sku":"x"}`))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want synthetic 201", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatalf("server received %d POSTs, want exactly 1 (no duplicate)", posts)
	}

	// Observability: start -> attempt(unknown) -> wait -> hook(reconcile) -> done(reconciled)
	wantKinds := []EventKind{EventCallStart, EventAttempt, EventWait, EventHook, EventCallDone}
	events := rec.Events()
	if len(events) != len(wantKinds) {
		t.Fatalf("got %d events, want %d: %+v", len(events), len(wantKinds), events)
	}
	for i, k := range wantKinds {
		if events[i].Kind != k {
			t.Fatalf("event[%d] = %v, want %v", i, events[i].Kind, k)
		}
	}
	if events[1].Outcome != OutcomeUnknown {
		t.Errorf("attempt outcome = %v, want unknown", events[1].Outcome)
	}
	if events[3].Phase != PhaseReconcile || events[3].Verdict != "succeed" {
		t.Errorf("hook event = %v/%v, want reconcile/succeed", events[3].Phase, events[3].Verdict)
	}
	done := events[4]
	if done.Result != ResultReconciled || done.Attempts != 1 {
		t.Errorf("done = result %q attempts %d, want reconciled/1", done.Result, done.Attempts)
	}
	if done.Labels.Operation != "create_order" {
		t.Errorf("operation label = %q, want create_order", done.Labels.Operation)
	}
}

// A misbehaving observer must never break the request or starve other observers.
func TestObserverPanicIsIsolated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := &Recorder{}
	boom := ObserverFunc(func(context.Context, Event) { panic("observer bug") })
	client := newTestClient(WithObserver(boom, rec))

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	events := rec.Events()
	if len(events) == 0 || events[len(events)-1].Kind != EventCallDone {
		t.Fatalf("recorder did not receive the full event stream: %+v", events)
	}
	if events[len(events)-1].Result != ResultSuccess {
		t.Errorf("result = %q, want success", events[len(events)-1].Result)
	}
}

// Unknown outcome + non-idempotent POST + no key + no reconciler => fail closed.
func TestUnsafePostIsNotBlindlyRetried(t *testing.T) {
	var mu sync.Mutex
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		posts++
		mu.Unlock()
		dropConnection(w)
	}))
	defer srv.Close()

	client := newTestClient()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(`{}`))
	_, err := client.Do(req)
	if !errors.Is(err, ErrUnsafeToRetry) {
		t.Fatalf("err = %v, want ErrUnsafeToRetry", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatalf("server received %d POSTs, want 1", posts)
	}
}
