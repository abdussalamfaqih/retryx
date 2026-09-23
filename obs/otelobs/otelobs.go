// Package otelobs exports retryx events through OpenTelemetry.
//
// Metrics (instrument names use OTel conventions; a Prometheus exporter turns
// dots into underscores and adds unit suffixes):
//
//	retryx.calls.in_flight, retryx.calls, retryx.call.duration,
//	retryx.attempts, retryx.attempt.duration, retryx.retry.wait, retryx.hook.duration
//
// Traces: every HTTP attempt becomes an "retryx.attempt" child span of whatever
// span is in the request context, with the real start/end times reconstructed
// from the event. So a trace of a retried call shows attempt 1 (failed),
// the gap (backoff), attempt 2 ... The final result is added as an event on the
// parent span ("retryx.call_done").
package otelobs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/abdussalamfaqih/retryx"
)

const scope = "github.com/abdussalamfaqih/retryx"

// Observer implements retryx.Observer.
type Observer struct {
	tracer      trace.Tracer
	inflight    metric.Int64UpDownCounter
	calls       metric.Int64Counter
	callDur     metric.Float64Histogram
	attempts    metric.Int64Counter
	attemptDur  metric.Float64Histogram
	wait        metric.Float64Histogram
	hookDur     metric.Float64Histogram
	probes      metric.Int64Counter
	transitions metric.Int64Counter
	gates       metric.Int64Counter
	gateWait    metric.Float64Histogram
	meter       metric.Meter
}

var _ retryx.Observer = (*Observer)(nil)

type builder struct {
	m   metric.Meter
	err error
}

func (b *builder) note(err error) {
	if err != nil && b.err == nil {
		b.err = err
	}
}

func (b *builder) counter(name, desc, unit string) metric.Int64Counter {
	c, err := b.m.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.note(err)
	return c
}

func (b *builder) upDown(name, desc, unit string) metric.Int64UpDownCounter {
	c, err := b.m.Int64UpDownCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.note(err)
	return c
}

func (b *builder) hist(name, desc string) metric.Float64Histogram {
	h, err := b.m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"))
	b.note(err)
	return h
}

// New builds the observer. Nil providers fall back to the OTel globals.
func New(mp metric.MeterProvider, tp trace.TracerProvider) (*Observer, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	b := &builder{m: mp.Meter(scope)}
	o := &Observer{
		tracer:     tp.Tracer(scope),
		inflight:   b.upDown("retryx.calls.in_flight", "Logical calls in progress.", "{call}"),
		calls:      b.counter("retryx.calls", "Finished logical calls by final result.", "{call}"),
		callDur:    b.hist("retryx.call.duration", "Total duration of a logical call including retries."),
		attempts:   b.counter("retryx.attempts", "HTTP attempts by classified outcome.", "{attempt}"),
		attemptDur: b.hist("retryx.attempt.duration", "Duration of one HTTP attempt."),
		wait:       b.hist("retryx.retry.wait", "Time slept before a retry."),
		hookDur:    b.hist("retryx.hook.duration", "Duration of a hook phase."),

		probes:      b.counter("retryx.probes", "Health probes by result.", "{probe}"),
		transitions: b.counter("retryx.health.transitions", "Downstream health state changes.", "{transition}"),
		gates:       b.counter("retryx.gate.events", "Calls rejected or delayed by the health gate.", "{call}"),
		gateWait:    b.hist("retryx.gate.wait", "Time calls spent waiting for the downstream to recover."),
		meter:       b.m,
	}
	if b.err != nil {
		return nil, b.err
	}
	return o, nil
}

// RegisterHealth exposes retryx.downstream.unhealthy{retryx.target}: 1 while the
// gate is closed. It is an observable gauge, read on every collection.
func (o *Observer) RegisterHealth(target string, h retryx.HealthGate) error {
	_, err := o.meter.Int64ObservableGauge("retryx.downstream.unhealthy",
		metric.WithDescription("1 while the downstream is marked unhealthy."),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			var v int64
			if h.State() == retryx.StateUnhealthy {
				v = 1
			}
			obs.Observe(v, metric.WithAttributes(attribute.String("retryx.target", target)))
			return nil
		}),
	)
	return err
}

// Observe implements retryx.Observer.
func (o *Observer) Observe(ctx context.Context, e retryx.Event) {
	base := []attribute.KeyValue{
		attribute.String("retryx.target", e.Labels.Target),
		attribute.String("retryx.operation", e.Labels.Operation),
		attribute.String("http.request.method", e.Method),
	}
	with := func(extra ...attribute.KeyValue) metric.MeasurementOption {
		all := append(append([]attribute.KeyValue(nil), base...), extra...)
		return metric.WithAttributes(all...)
	}

	switch e.Kind {
	case retryx.EventCallStart:
		o.inflight.Add(ctx, 1, with())

	case retryx.EventAttempt:
		attrs := []attribute.KeyValue{
			attribute.String("retryx.outcome", e.Outcome.String()),
			attribute.String("retryx.status_class", e.StatusClass),
			attribute.String("error.type", e.ErrClass),
		}
		o.attempts.Add(ctx, 1, with(attrs...))
		o.attemptDur.Record(ctx, e.Elapsed.Seconds(), with(attribute.String("retryx.outcome", e.Outcome.String())))

		spanAttrs := append(append([]attribute.KeyValue(nil), base...), attrs...)
		spanAttrs = append(spanAttrs,
			attribute.Int("retryx.attempt", e.Attempt),
			attribute.Int("http.response.status_code", e.Status),
			attribute.Bool("retryx.replay_safe", e.ReplaySafe),
		)
		_, span := o.tracer.Start(ctx, "retryx.attempt",
			trace.WithTimestamp(e.Time.Add(-e.Elapsed)),
			trace.WithAttributes(spanAttrs...),
		)
		if e.Outcome != retryx.OutcomeSuccess {
			span.SetStatus(codes.Error, e.Outcome.String())
		}
		span.End(trace.WithTimestamp(e.Time))

	case retryx.EventWait:
		o.wait.Record(ctx, e.Wait.Seconds(), with(attribute.String("retryx.wait_source", e.WaitSource)))

	case retryx.EventHook:
		o.hookDur.Record(ctx, e.Elapsed.Seconds(), with(
			attribute.String("retryx.phase", e.Phase.String()),
			attribute.String("retryx.verdict", e.Verdict),
		))

	case retryx.EventProbe:
		result := "ok"
		if !e.ProbeOK {
			result = "fail"
		}
		o.probes.Add(ctx, 1, metric.WithAttributes(
			attribute.String("retryx.target", e.Labels.Target),
			attribute.String("retryx.result", result),
			attribute.String("error.type", e.ErrClass),
		))

	case retryx.EventHealth:
		o.transitions.Add(ctx, 1, metric.WithAttributes(
			attribute.String("retryx.target", e.Labels.Target),
			attribute.String("retryx.from", e.HealthFrom.String()),
			attribute.String("retryx.to", e.HealthTo.String()),
		))

	case retryx.EventGate:
		attrs := with(
			attribute.String("retryx.gate.at", e.GateAt),
			attribute.String("retryx.gate.result", e.Gate),
		)
		o.gates.Add(ctx, 1, attrs)
		if e.Gate != retryx.GateRejected {
			o.gateWait.Record(ctx, e.Wait.Seconds(), attrs)
		}

	case retryx.EventCallDone:
		o.inflight.Add(ctx, -1, with())
		o.calls.Add(ctx, 1, with(attribute.String("retryx.result", e.Result)))
		o.callDur.Record(ctx, e.Elapsed.Seconds(), with(attribute.String("retryx.result", e.Result)))
		trace.SpanFromContext(ctx).AddEvent("retryx.call_done", trace.WithAttributes(
			attribute.String("retryx.result", e.Result),
			attribute.Int("retryx.attempts", e.Attempts),
		))
	}
}
