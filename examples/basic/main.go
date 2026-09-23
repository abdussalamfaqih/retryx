// Basic: the smallest useful setup. The server first says "slow down"
// (429 + Retry-After), then fails once (503), then succeeds. retryx honours
// Retry-After (it wins over the computed backoff when it is longer) and retries.
//
//	go run ./examples/basic
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/abdussalamfaqih/retryx"
)

func main() {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "1") // "come back in 1 second"
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			fmt.Fprint(w, "hello")
		}
	}))
	defer srv.Close()

	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithMaxAttempts(5),
		retryx.WithAttemptTimeout(2*time.Second),
		retryx.WithBackoff(retryx.ExponentialJitter(50*time.Millisecond, time.Second)),

		// An observer is optional; here it just narrates the call.
		retryx.WithObserver(retryx.ObserverFunc(func(_ context.Context, e retryx.Event) {
			switch e.Kind {
			case retryx.EventAttempt:
				fmt.Printf("attempt %d: status=%d outcome=%s\n", e.Attempt, e.Status, e.Outcome)
			case retryx.EventWait:
				fmt.Printf("  waiting %v (source: %s)\n", e.Wait.Round(time.Millisecond), e.WaitSource)
			case retryx.EventCallDone:
				fmt.Printf("done: result=%s attempts=%d elapsed=%v\n", e.Result, e.Attempts, e.Elapsed.Round(time.Millisecond))
			}
		})),
	)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("response:", resp.StatusCode, string(body))
}
