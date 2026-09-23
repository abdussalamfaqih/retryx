# Examples

Every example is a self-contained `main` package that starts its own fake
downstream (`httptest`), so they run offline with no setup:

```bash
go run ./examples/<name>        # from the repository root
```

The only exception is [`prometheus`](prometheus/), which has its own `go.mod`
because it depends on the Prometheus client library.

## Which one do I need?

| I want to... | Example | Building blocks |
|---|---|---|
| See the smallest useful setup, `Retry-After` handling | [`basic`](basic/main.go) | `WithMaxAttempts`, `WithBackoff`, `WithAttemptTimeout`, `WithObserver` |
| Retry a `POST` safely when the API supports idempotency keys | [`idempotency`](idempotency/main.go) | `IdempotencyKey(KeyPerCall)`, `AttemptHeader` |
| Retry a `POST` safely when the API has **no** idempotency support | [`reconcile`](reconcile/main.go) | `Reconcile`, `Reconciler`, `NewResponse` |
| Stop a retry storm from multiplying load on a struggling service | [`budget`](budget/main.go) | `Budget`, `WithBudget` |
| Stop calling a service that is down; recover without a thundering herd | [`healthgate`](healthgate/main.go) | `Health`, `HTTPCheck`, `WithHealth`, `GatePolicy` |
| Not lose requests during an outage (queue them, replay later) | [`outbox`](outbox/main.go) | `OnGiveUp`, `ErrUnhealthy`, idempotency key, `NewResponse` |
| Retry when the API answers `200` with an error payload | [`bodyretry`](bodyretry/main.go) | `After`, `RetryOnBody`, `PeekBody` |
| Send the retry to another region / replica | [`failover`](failover/main.go) | `Before`, `FailoverHosts` |
| Refresh an expired auth token and retry once | [`tokenrefresh`](tokenrefresh/main.go) | `Before`, `After`, `Call.Set` / `Value[T]`, outcome override |
| Get Prometheus metrics for Grafana (and logs) from all of the above | [`prometheus`](prometheus/main.go) | `promobs`, `slogobs`, `Multi`, `RegisterBudget`, `RegisterHealth` |

## What to look for

| Example | In the output |
|---|---|
| `basic` | attempt 1 is `429` and the next wait is labelled `retry_after` (about 1s, longer than the computed backoff); attempt 2 is `503`; attempt 3 succeeds |
| `idempotency` | the server logs the **same** key for every attempt |
| `reconcile` | one attempt with `outcome=unknown`, then `result=reconciled`, and the server received exactly **1** POST |
| `budget` | roughly 4x amplification without a budget (about 400 requests for 100 callers) versus about 1.1x with it |
| `healthgate` | after the outage starts, 50 concurrent calls are rejected and (almost) none reach the API; the patient callers finish after recovery, spread over the jitter window |
| `outbox` | the caller gets `202 Accepted` while the downstream is down; the replay creates the charge once, and a second replay returns `replayed`, so the server creates exactly **1** charge |
| `bodyretry` | two `200 busy` responses are retried; the third `200 ok` reaches the caller with its body intact |
| `failover` | the primary answers `503`, the retry lands on the secondary |
| `tokenrefresh` | the server sees the expired token, answers `401`, the token is refreshed once, and the retry succeeds |
| `prometheus` | `curl -s localhost:2112/metrics \| grep '^retryx_'` (see below) |

## The Prometheus / Grafana example

```bash
cd examples/prometheus
go mod tidy          # adds github.com/prometheus/client_golang and go.sum
go run .
curl -s localhost:2112/metrics | grep '^retryx_'
```

A fake downstream is 20% flaky and goes completely down for 8s out of every 20s
while synthetic traffic runs through retryx. Point a Prometheus at
`localhost:2112` and, in Grafana, try:

```promql
retryx_downstream_unhealthy                                                     # outage timeline
sum by (result) (rate(retryx_calls_total[1m]))                                  # what happened to calls
sum(rate(retryx_calls_total{result="downstream_unhealthy"}[1m]))                # requests we did NOT send
1 - sum(rate(retryx_calls_total[1m])) / sum(rate(retryx_attempts_total[1m]))    # retry ratio
```

More panels and alerts: [../docs/OBSERVABILITY.md](../docs/OBSERVABILITY.md).

## Writing your own

Copy the example closest to your problem. Two things carry over unchanged:

- probes and reconcile checks must use an `http.Client` **without** the retrying
  transport;
- call `retryx.WithOperation(ctx, "create_order")` (or set `WithLabeler`) so your
  metrics have a meaningful `operation` label.
