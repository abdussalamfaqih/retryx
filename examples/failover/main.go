// Failover per attempt: a Before hook rewrites the target host, so attempt 1
// goes to the primary region and the retry goes to the secondary one.
//
//	go run ./examples/failover
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/abdussalamfaqih/retryx"
)

func main() {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("  primary   received request (attempt header:", r.Header.Get("X-Retry-Attempt")+") -> 503")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer primary.Close()

	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("  secondary received request (attempt header:", r.Header.Get("X-Retry-Attempt")+") -> 200")
		fmt.Fprint(w, "served by secondary")
	}))
	defer secondary.Close()

	hostOf := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithBackoff(retryx.ExponentialJitter(10*time.Millisecond, 50*time.Millisecond)),
		retryx.Before(
			retryx.FailoverHosts(hostOf(primary), hostOf(secondary)), // attempt n -> hosts[(n-1) % 2]
			retryx.AttemptHeader("X-Retry-Attempt"),
		),
	)}

	fmt.Println("GET", primary.URL+"/data")
	resp, err := client.Get(primary.URL + "/data")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("response:", resp.StatusCode, string(body))
}
