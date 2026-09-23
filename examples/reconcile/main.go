// Reconcile before retry: the downstream has NO idempotency support. A POST
// creates the order, but the connection dies before we see the reply. Instead
// of blindly re-sending (duplicate order!), retryx asks the downstream whether
// the order already exists, and returns it if so.
//
//	go run ./examples/reconcile
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/abdussalamfaqih/retryx"
)

func main() {
	var (
		mu      sync.Mutex
		posts   int
		created int
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		posts++
		created++
		mu.Unlock()
		// The order IS created, but the reply is lost: the caller cannot know.
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	})
	mux.HandleFunc("/orders/last", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		c := created
		mu.Unlock()
		if c == 0 {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"id":"order_1","reconciled":true}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The probe must use a client WITHOUT the retry layer.
	plain := &http.Client{Timeout: time.Second}

	// Probe contract:
	//   (resp, nil) -> it already happened: return resp to the caller, do not retry
	//   (nil, nil)  -> definitely not there: safe to retry
	//   (nil, err)  -> cannot tell: abort rather than risk a duplicate
	probe := func(ctx context.Context, a *retryx.Attempt) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/orders/last", nil)
		if err != nil {
			return nil, err
		}
		resp, err := plain.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			body, _ := io.ReadAll(resp.Body)
			// Translate "GET 200" into the 201 the original POST would have returned.
			return retryx.NewResponse(a.Call.Original, http.StatusCreated, nil, body), nil
		case http.StatusNotFound:
			return nil, nil
		}
		return nil, fmt.Errorf("probe: unexpected status %d", resp.StatusCode)
	}

	client := &http.Client{Transport: retryx.New(&http.Transport{DisableKeepAlives: true},
		retryx.WithBackoff(retryx.ExponentialJitter(20*time.Millisecond, 100*time.Millisecond)),
		retryx.Reconcile(retryx.Reconciler(probe, false)), // false = fail closed if the probe is inconclusive
		retryx.WithObserver(retryx.ObserverFunc(func(_ context.Context, e retryx.Event) {
			switch e.Kind {
			case retryx.EventAttempt:
				fmt.Printf("attempt %d: outcome=%s error_class=%s\n", e.Attempt, e.Outcome, e.ErrClass)
			case retryx.EventCallDone:
				fmt.Printf("call done: result=%s attempts_sent=%d\n", e.Result, e.Attempts)
			}
		})),
	)}

	resp, err := client.Post(srv.URL+"/orders", "application/json", strings.NewReader(`{"sku":"x"}`))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("response: %d %s\n", resp.StatusCode, body)
	fmt.Printf("POSTs the server received: %d (no duplicate)\n", posts)
}
