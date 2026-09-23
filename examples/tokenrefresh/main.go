// Token refresh: a Before hook stamps the current token on every attempt; an
// After hook turns the first 401 into "refresh the token and retry", once per
// call. Even a POST is safe to retry here: a 401 is rejected before processing.
//
//	go run ./examples/tokenrefresh
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

type tokenSource struct {
	mu  sync.Mutex
	cur string
}

func (t *tokenSource) get() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cur
}

func (t *tokenSource) refresh() {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Println("  (refreshing token)")
	t.cur = "fresh-token"
}

type refreshedKey struct{} // per-call state key

func main() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		fmt.Printf("  server saw Authorization: %q\n", auth)
		if auth != "Bearer fresh-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"created":true}`)
	}))
	defer srv.Close()

	tokens := &tokenSource{cur: "expired-token"}

	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithBackoff(func(*retryx.Attempt) time.Duration { return 10 * time.Millisecond }),

		retryx.Before(func(_ context.Context, a *retryx.Attempt) retryx.Verdict {
			a.Req.Header.Set("Authorization", "Bearer "+tokens.get())
			return retryx.Continue()
		}),

		retryx.After(func(_ context.Context, a *retryx.Attempt) retryx.Verdict {
			if a.Err != nil || a.Resp.StatusCode != http.StatusUnauthorized {
				return retryx.Continue()
			}
			if _, already := retryx.Value[bool](a.Call, refreshedKey{}); already {
				return retryx.Continue() // refreshed once already: a second 401 is final
			}
			tokens.refresh()
			a.Call.Set(refreshedKey{}, true)
			a.Outcome = retryx.OutcomeNotApplied // override the classifier: retry this one
			return retryx.Continue()
		}),
	)}

	fmt.Println("POST /resource")
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{"name":"x"}`))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("response:", resp.StatusCode, string(body))
}
