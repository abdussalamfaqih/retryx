// Package promobs exports retryx events as Prometheus metrics.
//
// Metrics (namespace "retryx" by default; all durations in seconds):
//
//	retryx_calls_in_flight{target,operation}                              gauge
//	retryx_calls_total{target,operation,method,result}                    counter
//	retryx_call_duration_seconds{target,operation,method,result}          histogram  (whole call, incl. waits)
//	retryx_call_attempts{target,operation,method}                         histogram  (attempts per call)
//	retryx_attempts_total{target,operation,method,outcome,status_class,error_class} counter
//	retryx_attempt_duration_seconds{target,operation,method,outcome}      histogram  (one HTTP attempt)
//	retryx_retry_wait_seconds{target,operation,source}                    histogram  (source = backoff|retry_after)
//	retryx_hook_duration_seconds{target,operation,phase,verdict}          histogram  (before|after|reconcile|give_up)
//	retryx_budget_tokens{budget}                                          gauge      (see RegisterBudget)
//	retryx_downstream_unhealthy{target}                                   gauge      (1 = gate closed; see RegisterHealth)
//	retryx_probe_total{target,result,error_class}                         counter    (result = ok|fail)
//	retryx_probe_duration_seconds{target}                                 histogram
//	retryx_health_transitions_total{target,from,to}                       counter
//	retryx_gate_events_total{target,operation,at,result}                  counter    (result = rejected|waited|timeout)
//	retryx_gate_wait_seconds{target,operation,at,result}                  histogram  (time calls spent waiting for recovery)
//
// Useful PromQL for Grafana:
//
//	# Retry ratio: share of HTTP attempts that were retries
//	1 - sum(rate(retryx_calls_total[5m])) / sum(rate(retryx_attempts_total[5m]))
//
//	# Calls per second that needed more than one attempt
//	sum(rate(retryx_call_attempts_count[5m])) - sum(rate(retryx_call_attempts_bucket{le="1"}[5m]))
//
//	# Give-up rate by reason (alert on unsafe_to_retry, budget_exhausted)
//	sum by (result) (rate(retryx_calls_total{result!~"success|non_retryable|canceled"}[5m]))
//
//	# Duplicate-write risk avoided: reconcile probes that found the work already done
//	sum by (target, operation) (rate(retryx_calls_total{result="reconciled"}[5m]))
//
//	# p95 latency users actually experience vs. p95 of a single attempt
//	histogram_quantile(0.95, sum by (le) (rate(retryx_call_duration_seconds_bucket{result="success"}[5m])))
//	histogram_quantile(0.95, sum by (le) (rate(retryx_attempt_duration_seconds_bucket[5m])))
//
//	# Slow probe / hook (reconcile is usually an extra downstream call)
//	histogram_quantile(0.95, sum by (le, phase) (rate(retryx_hook_duration_seconds_bucket[5m])))
//
//	# Retry budget nearly empty => retries are about to be suppressed
//	retryx_budget_tokens < 10
//
// Cardinality: labels are target x operation x method x small closed sets.
// Keep Labels.Operation a route template (e.g. "create_order"), never a raw path.
package promobs

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/abdussalamfaqih/retryx"
)

// Options configures the Prometheus observer. The zero value is usable.
type Options struct {
	Namespace       string                // default "retryx"
	Registerer      prometheus.Registerer // default prometheus.DefaultRegisterer
	ConstLabels     prometheus.Labels     // e.g. {"service": "checkout"}
	DurationBuckets []float64             // seconds; default 5ms..30s
	CountBuckets    []float64             // attempts per call; default 1..10
}

// Observer implements retryx.Observer.
type Observer struct {
	ns          string
	reg         prometheus.Registerer
	constLabels prometheus.Labels

	inflight     *prometheus.GaugeVec
	calls        *prometheus.CounterVec
	callDur      *prometheus.HistogramVec
	callAttempts *prometheus.HistogramVec
	attempts     *prometheus.CounterVec
	attemptDur   *prometheus.HistogramVec
	wait         *prometheus.HistogramVec
	hookDur      *prometheus.HistogramVec
	probes       *prometheus.CounterVec
	probeDur     *prometheus.HistogramVec
	transitions  *prometheus.CounterVec
	gateEvents   *prometheus.CounterVec
	gateWait     *prometheus.HistogramVec
}

var _ retryx.Observer = (*Observer)(nil)

// New creates the collectors and registers them.
func New(o Options) (*Observer, error) {
	if o.Namespace == "" {
		o.Namespace = "retryx"
	}
	if o.Registerer == nil {
		o.Registerer = prometheus.DefaultRegisterer
	}
	if o.DurationBuckets == nil {
		o.DurationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	}
	if o.CountBuckets == nil {
		o.CountBuckets = []float64{1, 2, 3, 4, 5, 7, 10}
	}

	ns, cl := o.Namespace, o.ConstLabels
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help, ConstLabels: cl}, labels)
	}
	hist := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, ConstLabels: cl, Buckets: buckets}, labels)
	}

	ob := &Observer{
		ns: ns, reg: o.Registerer, constLabels: cl,
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "calls_in_flight", Help: "Logical calls currently in progress (including waits between attempts).", ConstLabels: cl,
		}, []string{"target", "operation"}),
		calls: counter("calls_total", "Finished logical calls by final result.",
			"target", "operation", "method", "result"),
		callDur: hist("call_duration_seconds", "Total time of a logical call including retries and waits.", o.DurationBuckets,
			"target", "operation", "method", "result"),
		callAttempts: hist("call_attempts", "HTTP attempts used per logical call.", o.CountBuckets,
			"target", "operation", "method"),
		attempts: counter("attempts_total", "HTTP attempts by classified outcome.",
			"target", "operation", "method", "outcome", "status_class", "error_class"),
		attemptDur: hist("attempt_duration_seconds", "Duration of a single HTTP attempt.", o.DurationBuckets,
			"target", "operation", "method", "outcome"),
		wait: hist("retry_wait_seconds", "Time slept before a retry.", o.DurationBuckets,
			"target", "operation", "source"),
		hookDur: hist("hook_duration_seconds", "Duration of a hook phase (a slow reconcile probe shows up here).", o.DurationBuckets,
			"target", "operation", "phase", "verdict"),
		probes: counter("probe_total", "Health probes by result.",
			"target", "result", "error_class"),
		probeDur: hist("probe_duration_seconds", "Duration of a health probe.", o.DurationBuckets,
			"target"),
		transitions: counter("health_transitions_total", "Downstream health state changes.",
			"target", "from", "to"),
		gateEvents: counter("gate_events_total", "Calls rejected or delayed by the health gate.",
			"target", "operation", "at", "result"),
		gateWait: hist("gate_wait_seconds", "Time calls spent waiting for the downstream to recover.", o.DurationBuckets,
			"target", "operation", "at", "result"),
	}

	for _, c := range []prometheus.Collector{
		ob.inflight, ob.calls, ob.callDur, ob.callAttempts, ob.attempts, ob.attemptDur, ob.wait, ob.hookDur,
		ob.probes, ob.probeDur, ob.transitions, ob.gateEvents, ob.gateWait,
	} {
		if err := o.Registerer.Register(c); err != nil {
			return nil, err
		}
	}
	return ob, nil
}

// MustNew is like New but panics on error.
func MustNew(o Options) *Observer {
	ob, err := New(o)
	if err != nil {
		panic(err)
	}
	return ob
}

// RegisterBudget exposes a shared retry budget as retryx_budget_tokens{budget=name}.
func (o *Observer) RegisterBudget(name string, b *retryx.Budget) error {
	cl := prometheus.Labels{"budget": name}
	for k, v := range o.constLabels {
		cl[k] = v
	}
	return o.reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: o.ns, Name: "budget_tokens", Help: "Retry budget tokens currently available.", ConstLabels: cl,
	}, b.Tokens))
}

// RegisterHealth exposes retryx_downstream_unhealthy{target}: 1 while the gate is
// closed, 0 otherwise. It is read at scrape time, so it never goes stale.
func (o *Observer) RegisterHealth(target string, h retryx.HealthGate) error {
	cl := prometheus.Labels{"target": target}
	for k, v := range o.constLabels {
		cl[k] = v
	}
	return o.reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: o.ns, Name: "downstream_unhealthy", Help: "1 while the downstream is marked unhealthy.", ConstLabels: cl,
	}, func() float64 {
		if h.State() == retryx.StateUnhealthy {
			return 1
		}
		return 0
	}))
}

// Observe implements retryx.Observer.
func (o *Observer) Observe(_ context.Context, e retryx.Event) {
	t, op := e.Labels.Target, e.Labels.Operation
	switch e.Kind {
	case retryx.EventCallStart:
		o.inflight.WithLabelValues(t, op).Inc()

	case retryx.EventAttempt:
		outcome := e.Outcome.String()
		o.attempts.WithLabelValues(t, op, e.Method, outcome, e.StatusClass, e.ErrClass).Inc()
		o.attemptDur.WithLabelValues(t, op, e.Method, outcome).Observe(e.Elapsed.Seconds())

	case retryx.EventWait:
		o.wait.WithLabelValues(t, op, e.WaitSource).Observe(e.Wait.Seconds())

	case retryx.EventHook:
		o.hookDur.WithLabelValues(t, op, e.Phase.String(), e.Verdict).Observe(e.Elapsed.Seconds())

	case retryx.EventProbe:
		result := "ok"
		if !e.ProbeOK {
			result = "fail"
		}
		o.probes.WithLabelValues(t, result, e.ErrClass).Inc()
		o.probeDur.WithLabelValues(t).Observe(e.Elapsed.Seconds())

	case retryx.EventHealth:
		o.transitions.WithLabelValues(t, e.HealthFrom.String(), e.HealthTo.String()).Inc()

	case retryx.EventGate:
		o.gateEvents.WithLabelValues(t, op, e.GateAt, e.Gate).Inc()
		if e.Gate != retryx.GateRejected {
			o.gateWait.WithLabelValues(t, op, e.GateAt, e.Gate).Observe(e.Wait.Seconds())
		}

	case retryx.EventCallDone:
		o.inflight.WithLabelValues(t, op).Dec()
		o.calls.WithLabelValues(t, op, e.Method, e.Result).Inc()
		o.callDur.WithLabelValues(t, op, e.Method, e.Result).Observe(e.Elapsed.Seconds())
		o.callAttempts.WithLabelValues(t, op, e.Method).Observe(float64(e.Attempts))
	}
}
