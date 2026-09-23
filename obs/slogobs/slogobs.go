// Package slogobs turns retryx events into structured log lines (log/slog).
//
// Noise policy: healthy first-attempt traffic is logged at Debug only. Retries,
// reconciliations and give-ups are logged at Info/Warn/Error, so production
// logs stay quiet until something interesting happens.
package slogobs

import (
	"context"
	"log/slog"

	"github.com/abdussalamfaqih/retryx"
)

type observer struct{ l *slog.Logger }

// New returns a retryx.Observer that logs to l (slog.Default() if nil).
func New(l *slog.Logger) retryx.Observer {
	if l == nil {
		l = slog.Default()
	}
	return observer{l: l}
}

func (o observer) Observe(ctx context.Context, e retryx.Event) {
	base := []slog.Attr{
		slog.String("target", e.Labels.Target),
		slog.String("operation", e.Labels.Operation),
		slog.String("method", e.Method),
	}
	log := func(level slog.Level, msg string, attrs ...slog.Attr) {
		if o.l.Enabled(ctx, level) {
			o.l.LogAttrs(ctx, level, msg, append(base, attrs...)...)
		}
	}

	switch e.Kind {
	case retryx.EventAttempt:
		level := slog.LevelDebug
		if e.Outcome != retryx.OutcomeSuccess {
			level = slog.LevelWarn
		}
		log(level, "retryx attempt",
			slog.Int("attempt", e.Attempt),
			slog.String("outcome", e.Outcome.String()),
			slog.Int("status", e.Status),
			slog.String("error_class", e.ErrClass),
			slog.Bool("replay_safe", e.ReplaySafe),
			slog.Duration("elapsed", e.Elapsed),
		)

	case retryx.EventWait:
		log(slog.LevelInfo, "retryx waiting before retry",
			slog.Int("after_attempt", e.Attempt),
			slog.Duration("wait", e.Wait),
			slog.String("source", e.WaitSource),
		)

	case retryx.EventHook:
		log(slog.LevelDebug, "retryx hook",
			slog.Int("attempt", e.Attempt),
			slog.String("phase", e.Phase.String()),
			slog.String("verdict", e.Verdict),
			slog.Duration("elapsed", e.Elapsed),
		)

	case retryx.EventProbe:
		level := slog.LevelDebug // healthy probes are noise
		if !e.ProbeOK {
			level = slog.LevelInfo
		}
		log(level, "retryx health probe",
			slog.Bool("ok", e.ProbeOK),
			slog.String("error_class", e.ErrClass),
			slog.Duration("elapsed", e.Elapsed),
		)

	case retryx.EventHealth:
		level := slog.LevelInfo
		if e.HealthTo == retryx.StateUnhealthy {
			level = slog.LevelWarn
		}
		log(level, "retryx downstream health changed",
			slog.String("from", e.HealthFrom.String()),
			slog.String("to", e.HealthTo.String()),
		)

	case retryx.EventGate:
		level := slog.LevelDebug // one line per rejected call would flood the logs during an outage
		if e.Gate != retryx.GateRejected {
			level = slog.LevelInfo
		}
		log(level, "retryx health gate",
			slog.String("result", e.Gate),
			slog.String("at", e.GateAt),
			slog.Duration("waited", e.Wait),
		)

	case retryx.EventCallDone:
		level := slog.LevelDebug
		switch {
		case e.Result == retryx.ResultSuccess && e.Attempts > 1,
			e.Result == retryx.ResultReconciled,
			e.Result == retryx.ResultFallback:
			level = slog.LevelInfo // it worked, but only thanks to the retry layer
		case e.Result == retryx.ResultSuccess:
			// healthy: stay at debug
		case e.Result == retryx.ResultNonRetryable, e.Result == retryx.ResultCanceled:
			level = slog.LevelInfo
		case e.Result == retryx.ResultUnhealthy:
			level = slog.LevelWarn // expected during an outage; the health transition is the Error-worthy line
		default:
			level = slog.LevelError // exhausted, unsafe, budget, aborted, canceled ...
		}
		log(level, "retryx call done",
			slog.String("result", e.Result),
			slog.Int("attempts", e.Attempts),
			slog.Int("status", e.Status),
			slog.String("error_class", e.ErrClass),
			slog.Duration("elapsed", e.Elapsed),
		)
	}
}
