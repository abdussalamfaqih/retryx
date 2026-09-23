// Retry budget: when a downstream is struggling, per-call retries multiply the
// load (100 callers x 4 attempts = 400 requests). A Budget shared by all calls
// caps the extra load: each retry spends a token, each success refunds a little.
//
//	go run ./examples/budget
//
// Expect roughly 400 requests without a budget and ~110 with one (100 first
// attempts + 10 retries).
package main

import (
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
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable) // the downstream is struggling
	}))
	defer srv.Close()

	const callers = 100
	fast := func(*retryx.Attempt) time.Duration { return time.Millisecond }

	run := func(name string, extra ...retryx.Option) {
		hits.Store(0)
		opts := append([]retryx.Option{retryx.WithMaxAttempts(4), retryx.WithBackoff(fast)}, extra...)
		client := &http.Client{Transport: retryx.New(nil, opts...)}

		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.Get(srv.URL)
				if err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		wg.Wait()
		fmt.Printf("%-12s %d callers -> %3d requests reached the downstream (%.1fx)\n",
			name, callers, hits.Load(), float64(hits.Load())/callers)
	}

	run("no budget")
	// 10 tokens to spend on retries, +0.1 token refunded per success.
	run("with budget", retryx.WithBudget(retryx.NewBudget(10, 0.1)))
}
