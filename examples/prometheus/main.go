// Everything together, with metrics you can point Grafana at.
//
// A fake downstream is 20% flaky and goes fully down for 8s out of every 20s.
// Synthetic traffic runs against it through retryx with:
//
//   - a stable idempotency key,
//
//   - a shared retry budget,
//
//   - a health gate (active probes + passive feedback),
//
//   - Prometheus metrics and slog logs (two observers injected at once).
//
//     cd examples/prometheus && go mod tidy && go run .
//     curl -s localhost:2112/metrics | grep '^retryx_'
//
// Watch retryx_downstream_unhealthy flip to 1 during an outage, and
// retryx_calls_total{result="downstream_unhealthy"} grow while the fake
// downstream receives (almost) nothing.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"example.com/retryx/obs/promobs"
	"example.com/retryx/obs/slogobs"
	"github.com/abdussalamfaqih/retryx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// ---- fake downstream: 20% flaky, periodic full outages ------------------
	var down atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case down.Load():
			w.WriteHeader(http.StatusServiceUnavailable)
		case r.URL.Path != "/health" && rand.Intn(10) < 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()
	go func() {
		for {
			if !sleep(ctx, 12*time.Second) {
				return
			}
			down.Store(true)
			log.Println("!! downstream outage starts (8s)")
			if !sleep(ctx, 8*time.Second) {
				return
			}
			down.Store(false)
			log.Println("!! downstream outage ends")
		}
	}()

	// ---- observers ----------------------------------------------------------
	reg := prometheus.NewRegistry()
	prom := promobs.MustNew(promobs.Options{
		Registerer:  reg,
		ConstLabels: prometheus.Labels{"service": "demo"},
	})
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	observers := retryx.Multi(prom, slogobs.New(logger)) // any number of tools

	// ---- health gate + budget ----------------------------------------------
	target := strings.TrimPrefix(srv.URL, "http://")
	health, err := retryx.NewHealth(retryx.HealthConfig{
		Check:            retryx.HTTPCheck(&http.Client{Timeout: time.Second}, srv.URL+"/health"),
		Labels:           retryx.Labels{Target: target},
		Observer:         observers,
		Interval:         2 * time.Second,
		RecheckInterval:  time.Second,
		PassiveThreshold: 5,
		HoldDown:         3 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	health.Start(ctx)
	defer health.Stop()

	budget := retryx.NewBudget(50, 0.1)
	must(prom.RegisterBudget("demo", budget))
	must(prom.RegisterHealth(target, health))

	// ---- the client ---------------------------------------------------------
	client := &http.Client{Transport: retryx.New(nil,
		retryx.WithMaxAttempts(4),
		retryx.WithBudget(budget),
		retryx.WithHealth(health, retryx.GatePolicy{RetryWait: 3 * time.Second}),
		retryx.WithObserver(observers),
		retryx.Before(retryx.IdempotencyKey("Idempotency-Key", retryx.RandomKey, retryx.KeyPerCall)),
	)}

	go traffic(ctx, client, srv.URL)

	// ---- /metrics -----------------------------------------------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metrics := &http.Server{Addr: ":2112", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metrics.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	fmt.Println("metrics on http://localhost:2112/metrics  (Ctrl-C to stop)")

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = metrics.Shutdown(shutdown)
}

// traffic fires ~20 create_order calls per second.
func traffic(ctx context.Context, client *http.Client, base string) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			go func() {
				cctx, cancel := context.WithTimeout(retryx.WithOperation(ctx, "create_order"), 5*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(cctx, http.MethodPost, base+"/orders", strings.NewReader(`{"sku":"x"}`))
				if err != nil {
					return
				}
				resp, err := client.Do(req)
				if err != nil {
					return // outcome is already visible in the metrics and logs
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
