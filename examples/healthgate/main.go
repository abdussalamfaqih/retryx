// Health gate: while the downstream is down, no requests are sent to it.
//
//   - "fail-fast" callers are rejected immediately (nothing is sent);
//
//   - "patient" callers wait for the recovery signal and are then released with
//     random jitter, so recovery is not a thundering herd.
//
//     go run ./examples/healthgate
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abdussalamfaqih/retryx"
)

func main() {
	var up atomic.Bool
	var apiHits atomic.Int64
	up.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		apiHits.Add(1)
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "ok")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Active probing (plus passive feedback from real calls).
	health, err := retryx.NewHealth(retryx.HealthConfig{
		Check:              retryx.HTTPCheck(&http.Client{Timeout: time.Second}, srv.URL+"/health"),
		Labels:             retryx.Labels{Target: "demo"},
		Interval:           100 * time.Millisecond,
		RecheckInterval:    100 * time.Millisecond,
		MaxRecheckInterval: 400 * time.Millisecond,
		FailureThreshold:   2,
		SuccessThreshold:   2,
		PassiveThreshold:   3,
	})
	if err != nil {
		panic(err)
	}
	health.Start(ctx)
	defer health.Stop()

	backoff := retryx.ExponentialJitter(20*time.Millisecond, 50*time.Millisecond)

	// Interactive traffic: fail fast when the downstream is down.
	failFast := &http.Client{Transport: retryx.New(nil,
		retryx.WithBackoff(backoff),
		retryx.WithHealth(health, retryx.GatePolicy{}),
	)}
	// Background traffic: wait up to 5s for recovery, released over 300ms.
	patient := &http.Client{Transport: retryx.New(nil,
		retryx.WithBackoff(backoff),
		retryx.WithHealth(health, retryx.GatePolicy{StartWait: 5 * time.Second, ReleaseJitter: 300 * time.Millisecond}),
	)}

	fmt.Println("1) downstream healthy:        ", get(failFast, srv.URL+"/api"))

	up.Store(false)
	fmt.Println("2) downstream goes down")
	waitUntil(func() bool { return health.State() == retryx.StateUnhealthy })
	fmt.Println("   gate is closed; reason:", health.LastError())

	before := apiHits.Load()
	var wg sync.WaitGroup
	var rejected atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if get(failFast, srv.URL+"/api") == rejectedMsg {
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("3) 50 concurrent calls: %d rejected by the gate, requests that reached the API: %d\n",
		rejected.Load(), apiHits.Load()-before)

	fmt.Println("4) 5 patient callers arrive and wait for recovery")
	start := time.Now()
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res := get(patient, srv.URL+"/api")
			fmt.Printf("   caller %d: %s after %v\n", id, res, time.Since(start).Round(10*time.Millisecond))
		}(i)
	}
	time.Sleep(300 * time.Millisecond)
	up.Store(true)
	fmt.Println("   downstream recovers")
	wg.Wait()
}

const rejectedMsg = "rejected by the health gate (nothing sent)"

func get(c *http.Client, url string) string {
	resp, err := c.Get(url)
	if err != nil {
		if errors.Is(err, retryx.ErrUnhealthy) {
			return rejectedMsg
		}
		return "error: " + err.Error()
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Status
}

func waitUntil(cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}
