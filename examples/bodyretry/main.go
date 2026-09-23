// Retry on the response BODY: some APIs answer HTTP 200 with an error payload
// such as {"status":"busy"} (GraphQL errors, job APIs ...). RetryOnBody peeks at
// the body without consuming it, and can turn a "successful" response into a retry.
//
//	go run ./examples/bodyretry
package main

import (
	"bytes"
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
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			fmt.Printf("  server call %d -> 200 {\"status\":\"busy\"}\n", n)
			fmt.Fprint(w, `{"status":"busy"}`)
			return
		}
		fmt.Printf("  server call %d -> 200 {\"status\":\"ok\",\"answer\":42}\n", n)
		fmt.Fprint(w, `{"status":"ok","answer":42}`)
	}))
	defer srv.Close()

	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithBackoff(retryx.ExponentialJitter(10*time.Millisecond, 50*time.Millisecond)),
		retryx.After(retryx.RetryOnBody(4096, func(status int, body []byte) (retryx.Outcome, bool) {
			if bytes.Contains(body, []byte(`"busy"`)) {
				return retryx.OutcomeNotApplied, true // override: treat as "not processed, try again"
			}
			return retryx.OutcomeSuccess, false // no opinion: keep the status-based result
		})),
	)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body) // the peeked body is intact
	fmt.Println("final response:", resp.StatusCode, string(body))
}
