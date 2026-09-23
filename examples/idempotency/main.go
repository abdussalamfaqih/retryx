// Idempotency: the SAME Idempotency-Key is sent on every retry of one logical
// call, so a server that dedupes on it can never create the payment twice.
//
//	go run ./examples/idempotency
package main

import (
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
		mu   sync.Mutex
		keys []string
	)

	// A payment API that is overloaded for its first two requests.
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
		fmt.Fprint(w, `{"id":"pay_1"}`)
	}))
	defer srv.Close()

	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithMaxAttempts(5),
		retryx.WithBackoff(retryx.ExponentialJitter(20*time.Millisecond, 200*time.Millisecond)),
		retryx.Before(
			// KeyPerCall: generated once, reused by every retry. This also marks the
			// call replay-safe, so even an "unknown outcome" failure may be retried.
			retryx.IdempotencyKey("Idempotency-Key", retryx.RandomKey, retryx.KeyPerCall),
			retryx.AttemptHeader("X-Retry-Attempt"),
		),
	)}

	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{"amount":100}`))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	fmt.Println("response:", resp.StatusCode, string(body))
	fmt.Println("idempotency keys the server saw:")
	mu.Lock()
	defer mu.Unlock()
	for i, k := range keys {
		fmt.Printf("  attempt %d: %s\n", i+1, k)
	}
}
