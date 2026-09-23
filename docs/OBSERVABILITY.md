# retryx observability

The engine knows nothing about Prometheus, OpenTelemetry or any logger. It emits
plain `retryx.Event` values to any number of `retryx.Observer`s.

```
                    ┌──────────────┐
 retryx.Transport ─►│ Multi(...)   │─► promobs   (Prometheus metrics)   ← own go.mod
   emits Events     │ fan-out,     │─► otelobs   (OTel metrics + spans) ← own go.mod
                    │ panic-safe   │─► slogobs   (structured logs)      ← stdlib only
                    └──────────────┘─► your own  (StatsD, Datadog, Kafka ...)
```

The core module has zero third-party dependencies. `promobs` and `otelobs` are
separate Go modules, so importing retryx never drags in client libraries you do not use.

## Wiring

```go
reg := prometheus.NewRegistry()
prom := promobs.MustNew(promobs.Options{
    Registerer:  reg,
    ConstLabels: prometheus.Labels{"service": "checkout"},
})
otelObs, _ := otelobs.New(nil, nil) // nil = use otel globals

budget := retryx.NewBudget(100, 0.1)
_ = prom.RegisterBudget("payments", budget) // retryx_budget_tokens{budget="payments"}

client := &http.Client{Transport: retryx.New(nil,
    retryx.WithBudget(budget),
    retryx.WithObserver(prom, otelObs, slogobs.New(slog.Default())), // any number, any mix
    retryx.Before(retryx.IdempotencyKey("Idempotency-Key", retryx.RandomKey, retryx.KeyPerCall)),
    retryx.Reconcile(retryx.Reconciler(checkOrderExists, false)),
)}

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

// Name the business operation. This becomes the `operation` label.
ctx = retryx.WithOperation(ctx, "create_order")
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
```

Labels default to `target` = URL host and `operation` = `WithOperation` value
(or `unknown`). Override with `retryx.WithLabeler(func(*http.Request) retryx.Labels {...})`,
for example to map hosts to service names.

**Cardinality rule:** `operation` must be a route template or business name
(`create_order`), never a raw path (`/orders/123`) and never an id.

## Event model

| Kind | When | Useful fields |
|---|---|---|
| `EventCallStart` | logical call begins | Labels, Method |
| `EventAttempt` | each HTTP attempt, after classification and After hooks | Attempt, Outcome, Status, StatusClass, ErrClass, Elapsed, ReplaySafe |
| `EventWait` | before sleeping for a retry | Attempt, Wait, WaitSource (`backoff` or `retry_after`) |
| `EventHook` | a hook phase finished | Phase, Verdict, Elapsed |
| `EventCallDone` | always, exactly once per `EventCallStart` | Attempts, Result, Status, ErrClass, Elapsed (incl. waits) |
| `EventGate` | the health gate rejected or delayed a call | Gate (`rejected`/`waited`/`timeout`), GateAt (`start`/`retry`), Wait |
| `EventProbe` | a health probe finished (emitted by `Health`, not by a call) | ProbeOK, ErrClass, Elapsed |
| `EventHealth` | the downstream changed health state (emitted by `Health`) | HealthFrom, HealthTo |

Health and gate metrics, alerts and tuning are documented in `HEALTH.md`.

`Result` is a closed set: `success`, `non_retryable`, `exhausted`,
`unsafe_to_retry`, `body_not_replayable`, `budget_exhausted`,
`retry_after_too_long`, `reconciled`, `fallback`, `short_circuit`,
`hook_aborted`, `canceled`, `body_error`, `panic`, `downstream_unhealthy`.

`ErrClass` is a closed set: `none`, `canceled`, `timeout`, `tls`, `dns`, `dial`,
`conn_reset`, `other`.

## Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `retryx_calls_in_flight` | gauge | target, operation |
| `retryx_calls_total` | counter | target, operation, method, result |
| `retryx_call_duration_seconds` | histogram | target, operation, method, result |
| `retryx_call_attempts` | histogram | target, operation, method |
| `retryx_attempts_total` | counter | target, operation, method, outcome, status_class, error_class |
| `retryx_attempt_duration_seconds` | histogram | target, operation, method, outcome |
| `retryx_retry_wait_seconds` | histogram | target, operation, source |
| `retryx_hook_duration_seconds` | histogram | target, operation, phase, verdict |
| `retryx_budget_tokens` | gauge | budget |
| `retryx_downstream_unhealthy` | gauge | target |
| `retryx_probe_total` | counter | target, result, error_class |
| `retryx_probe_duration_seconds` | histogram | target |
| `retryx_health_transitions_total` | counter | target, from, to |
| `retryx_gate_events_total` | counter | target, operation, at, result |
| `retryx_gate_wait_seconds` | histogram | target, operation, at, result |

## Grafana panels (PromQL)

| Panel | Query |
|---|---|
| Retry ratio (share of attempts that were retries) | `1 - sum(rate(retryx_calls_total[5m])) / sum(rate(retryx_attempts_total[5m]))` |
| Calls that needed >1 attempt (per s) | `sum(rate(retryx_call_attempts_count[5m])) - sum(rate(retryx_call_attempts_bucket{le="1"}[5m]))` |
| Call outcomes | `sum by (result) (rate(retryx_calls_total[5m]))` |
| Attempt outcomes by cause | `sum by (outcome, status_class, error_class) (rate(retryx_attempts_total{outcome!="success"}[5m]))` |
| Duplicates avoided by reconcile | `sum by (target, operation) (rate(retryx_calls_total{result="reconciled"}[5m]))` |
| Refused blind retries | `sum by (target, operation) (rate(retryx_calls_total{result="unsafe_to_retry"}[5m]))` |
| User-visible p95 | `histogram_quantile(0.95, sum by (le) (rate(retryx_call_duration_seconds_bucket{result="success"}[5m])))` |
| Single-attempt p95 | `histogram_quantile(0.95, sum by (le) (rate(retryx_attempt_duration_seconds_bucket[5m])))` |
| Time lost to waiting | `sum(rate(retryx_retry_wait_seconds_sum[5m]))` |
| Server-imposed waits | `sum(rate(retryx_retry_wait_seconds_count{source="retry_after"}[5m]))` |
| Reconcile probe latency | `histogram_quantile(0.95, sum by (le) (rate(retryx_hook_duration_seconds_bucket{phase="reconcile"}[5m])))` |
| Retry budget | `retryx_budget_tokens` |
| In flight | `sum by (target) (retryx_calls_in_flight)` |

Gap between the two p95 panels = latency users pay for retries.

## Alert starters

```yaml
- alert: RetryGiveUps
  expr: sum by (target, operation) (rate(retryx_calls_total{result=~"exhausted|budget_exhausted|hook_aborted"}[5m])) > 0.1
  for: 5m

- alert: BlindRetryRefused          # non-idempotent calls failing with unknown outcome
  expr: sum by (target, operation) (rate(retryx_calls_total{result="unsafe_to_retry"}[10m])) > 0
  for: 10m
  # fix: add an idempotency key or a Reconcile probe for this operation

- alert: RetryBudgetLow
  expr: retryx_budget_tokens < 10
  for: 2m

- alert: RetryStorm
  expr: 1 - sum(rate(retryx_calls_total[5m])) / sum(rate(retryx_attempts_total[5m])) > 0.3
  for: 5m
```

## Writing your own observer

```go
type datadogObserver struct{ c *statsd.Client }

func (d datadogObserver) Observe(_ context.Context, e retryx.Event) {
    if e.Kind != retryx.EventCallDone {
        return
    }
    tags := []string{"target:" + e.Labels.Target, "op:" + e.Labels.Operation, "result:" + e.Result}
    _ = d.c.Incr("retryx.calls", tags, 1)
    _ = d.c.Timing("retryx.call.duration", e.Elapsed, tags, 1)
}
```

Rules: be fast (it runs on the request path), be concurrency-safe, do not block
on I/O (buffer or hand off to a goroutine). A panic is recovered and never
affects the request or other observers.

## Setup

```
cd retryx && go vet ./... && go test ./...
cd obs/promobs && go mod tidy    # adds client_golang + go.sum
cd ../otelobs  && go mod tidy    # adds otel + go.sum
```

Nothing here was compiled in the authoring environment (no Go toolchain), so
expect to fix a typo or a dependency version on first build.
