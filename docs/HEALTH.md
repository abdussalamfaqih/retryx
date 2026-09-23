# retryx health gating

Goal: when a downstream is down, **stop sending it traffic** and let it recover,
instead of letting every caller run its own retry/backoff loop against it.

## What changes compared to plain backoff

| | Plain backoff | With a health gate |
|---|---|---|
| New call while downstream is down | sent, times out, retried N times | rejected immediately (or waits up to `StartWait`), **nothing sent** |
| Call already retrying when it goes down | sleeps its backoff, sends again, fails again | stops sleeping-and-hammering; waits for the "healthy again" signal |
| Requests hitting a dead service | `callers × attempts` | ~0 (plus a few cheap probes) |
| Downstream comes back | every retry timer fires at random moments, or all at once | waiters are released with random jitter (no thundering herd) |
| Probing the downstream | n/a | probes back off exponentially while it is down |

Backoff still decides *spacing* between attempts while the downstream is up. The
gate decides *whether to try at all*.

## How it works

```
active probes ── Check() every Interval ──┐
 (2s → 4s → 8s … 30s while down)          ├─► Health ──► Healthy ⇄ Unhealthy
passive signals ── real attempts Report() ┘      │
 (N consecutive transport errors/502/503/504)    ▼
                                      Transport consults it:
   new call, Unhealthy  → fail fast with ErrUnhealthy   (or wait StartWait)
   retry,    Unhealthy  → skip the backoff sleep, wait up to RetryWait for recovery,
                          then sleep a random 0..ReleaseJitter and continue
   result label         → "downstream_unhealthy", nothing was sent
```

- **Active** probing finds an outage that has no traffic yet, and is the only thing
  that can declare recovery.
- **Passive** feedback closes the gap between probes: after `PassiveThreshold`
  consecutive failed real attempts the gate closes at once and the probe loop is
  woken immediately.
- The initial state is Healthy (optimistic), so startup traffic is not blocked
  before the first probe.

## Wiring

One downstream:

```go
health, _ := retryx.NewHealth(retryx.HealthConfig{
    Check:  retryx.HTTPCheck(plainClient, "https://payments.example.com/health"),
    Labels: retryx.Labels{Target: "payments.example.com"},
    Observer: prom,            // any retryx.Observer: metrics, logs, traces
    PassiveThreshold: 5,
})
health.Start(ctx)              // stop with health.Stop() or by cancelling ctx

client := &http.Client{Transport: retryx.New(nil,
    retryx.WithHealth(health, retryx.GatePolicy{}),
    retryx.WithObserver(prom),
)}
```

Many downstreams through one client (one gate per host, created lazily):

```go
sel := retryx.HealthByHost(func(host string) retryx.HealthGate {
    h, _ := retryx.NewHealth(retryx.HealthConfig{
        Check:  retryx.HTTPCheck(plainClient, "https://"+host+"/health"),
        Labels: retryx.Labels{Target: host},
        Observer: prom,
        PassiveThreshold: 5,
    })
    h.Start(ctx)
    return h
})
client := &http.Client{Transport: retryx.New(nil, retryx.WithHealthSelector(sel, retryx.GatePolicy{}))}
```

`plainClient` must **not** contain the retrying Transport, or probes would be retried.

## Configuration

`HealthConfig` (only `Check` is required):

| Field | Default | Meaning |
|---|---|---|
| `Interval` | 10s | probe period while healthy |
| `Timeout` | 2s | per-probe timeout |
| `RecheckInterval` | 2s | first re-probe after a failure; also the period while confirming recovery |
| `MaxRecheckInterval` | 30s | cap of the exponential probe backoff while down |
| `FailureThreshold` | 3 | consecutive failed probes → Unhealthy |
| `SuccessThreshold` | 2 | consecutive OK probes → Healthy |
| `PassiveThreshold` | 0 (off) | consecutive failed real attempts → Unhealthy |
| `IsFailure` | `DefaultIsFailure` | what counts as a failed attempt |
| `HoldDown` | 0 | minimum time to stay Unhealthy once tripped |

`GatePolicy`:

| Field | Default | Meaning |
|---|---|---|
| `StartWait` | 0 = fail fast | how long a **new** call waits for recovery |
| `RetryWait` | 30s (negative = don't wait) | how long a call **already retrying** waits |
| `ReleaseJitter` | 1s (negative = release all at once) | spread of the release after recovery |

All waits are also bounded by the request context's deadline.

`DefaultIsFailure` counts transport errors (except the caller cancelling) and
502/503/504. It deliberately ignores plain **500** (usually one broken endpoint;
counting it would let one bad endpoint close the gate for the whole host) and
**429** (the service is alive and asking you to slow down; `Retry-After` handles it).

## Tuning recipes

| Situation | Settings |
|---|---|
| Interactive API, latency matters | `StartWait: 0`, `RetryWait: 2s` (fail fast, retries wait briefly) |
| Background worker / queue consumer | `StartWait: 30s`, `RetryWait: 5m`, `ReleaseJitter: 10s` |
| `/health` can be green while the real API is broken | `PassiveThreshold: 5` and `HoldDown: 15s` (stops rapid flapping) |
| Big fleet of instances | keep `Check` cheap; probes are already jittered ±10% per instance |
| One noisy endpoint on a shared host | custom `IsFailure` (or gate per host+operation via `WithHealthSelector`) |

## Don't lose the work: park it while the gate is closed

`Reason` in an `OnGiveUp` hook says why we stopped. When the downstream is down you
usually want to queue the request, not fail it:

```go
retryx.OnGiveUp(func(ctx context.Context, a *retryx.Attempt) retryx.Verdict {
    if !errors.Is(a.Reason, retryx.ErrUnhealthy) {
        return retryx.Continue()
    }
    // Serialize from a.Call.Original (+ its GetBody); a.Req's body may be consumed.
    if err := outbox.Enqueue(ctx, a.Call.Original); err != nil {
        return retryx.Fail(err)
    }
    return retryx.Succeed(retryx.NewResponse(a.Call.Original, http.StatusAccepted, nil, []byte(`{"queued":true}`)))
}),
```

Together with a stable idempotency key (`KeyPerCall`) the worker that drains the
outbox can replay it safely.

## Plug in something else

The engine only needs `HealthGate`:

```go
type HealthGate interface {
    State() HealthState
    Report(HealthSignal)
}
```

A circuit breaker (e.g. sony/gobreaker), a service-mesh signal or a Consul /
Kubernetes readiness feed can implement it. Optionally implement
`WaitHealthy(ctx) error` (`HealthWaiter`) to wake waiters instantly; otherwise
the engine polls `State()` every 100ms (`WaitUntilHealthy` does the same for you).

## Metrics (Prometheus names; OTel equivalents use dots)

| Metric | Meaning |
|---|---|
| `retryx_downstream_unhealthy{target}` | 1 while the gate is closed (`RegisterHealth`) |
| `retryx_health_transitions_total{target,from,to}` | flapping detector |
| `retryx_probe_total{target,result,error_class}` / `retryx_probe_duration_seconds` | probe success rate and latency |
| `retryx_gate_events_total{target,operation,at,result}` | calls rejected / delayed / timed out by the gate |
| `retryx_gate_wait_seconds{...}` | time callers lost waiting for recovery |
| `retryx_calls_total{result="downstream_unhealthy"}` | calls that sent nothing because of the gate |

```yaml
- alert: DownstreamUnhealthy
  expr: retryx_downstream_unhealthy == 1
  for: 1m

- alert: DownstreamFlapping
  expr: sum by (target) (increase(retryx_health_transitions_total{to="unhealthy"}[30m])) > 4

- alert: ProbeFailing                 # probe errors while the gate is still open
  expr: sum by (target) (rate(retryx_probe_total{result="fail"}[5m])) > 0

- alert: CallsBlockedByGate
  expr: sum by (target, operation) (rate(retryx_calls_total{result="downstream_unhealthy"}[5m])) > 0
  for: 2m
```

Panels: requests avoided during an outage
(`sum(rate(retryx_calls_total{result="downstream_unhealthy"}[1m]))`), outage
timeline (`retryx_downstream_unhealthy`), probe latency p95
(`histogram_quantile(0.95, sum by (le) (rate(retryx_probe_duration_seconds_bucket[5m])))`),
caller time lost waiting (`sum(rate(retryx_gate_wait_seconds_sum[5m]))`).

## Limits worth knowing

- State is per process. With many instances each one probes and decides on its
  own; there is no shared view.
- Health needs an active `Check`; it is not a passive-only circuit breaker. For that,
  plug a breaker in through `HealthGate`.
- A retry budget token is spent before the gate wait; a call that then gives up
  because of the gate does not get it back (minor; the budget refills on success).
- While the gate is closed, a `Reconcile` probe does not run either, because the
  gate wait happens before it. That is intended: a "did it already happen?" check
  is also a request to the dead downstream.
