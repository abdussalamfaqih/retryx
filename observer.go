package retryx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Observability is decoupled from any metrics library. The engine emits plain
// Event values to an Observer; adapters (Prometheus, OpenTelemetry, slog, your
// own) turn them into metrics, spans or logs. Several observers can be injected
// at once with WithObserver(a, b, c).
//
// Rules for Observer implementations:
//   - be fast and non-blocking: Observe runs inline on the request path;
//   - be safe for concurrent use;
//   - never mutate the Event (it is passed by value, but Labels etc. are shared).
// A panicking observer is recovered and never breaks the request.
// ---------------------------------------------------------------------------

// EventKind identifies what happened.
type EventKind int

const (
	EventCallStart EventKind = iota // a logical call begins
	EventAttempt                    // one attempt finished and was classified
	EventWait                       // about to sleep before the next attempt
	EventHook                       // a hook phase finished
	EventCallDone                   // the logical call finished (always emitted after CallStart)
	EventProbe                      // a health probe finished          (from Health, not from a call)
	EventHealth                     // the downstream changed health state (from Health)
	EventGate                       // the health gate rejected or delayed a call
)

func (k EventKind) String() string {
	switch k {
	case EventCallStart:
		return "call_start"
	case EventAttempt:
		return "attempt"
	case EventWait:
		return "wait"
	case EventHook:
		return "hook"
	case EventCallDone:
		return "call_done"
	case EventProbe:
		return "probe"
	case EventHealth:
		return "health"
	case EventGate:
		return "gate"
	}
	return "invalid"
}

// Phase is a hook phase.
type Phase int

const (
	PhaseBefore Phase = iota
	PhaseAfter
	PhaseReconcile
	PhaseGiveUp
)

func (p Phase) String() string {
	switch p {
	case PhaseBefore:
		return "before"
	case PhaseAfter:
		return "after"
	case PhaseReconcile:
		return "reconcile"
	case PhaseGiveUp:
		return "give_up"
	}
	return "invalid"
}

// Final result labels of a logical call (Event.Result on EventCallDone).
// They are a small closed set, so they are safe as metric label values.
const (
	ResultSuccess           = "success"              // a good response was returned
	ResultNonRetryable      = "non_retryable"        // failure the classifier marked Fatal (e.g. 4xx), returned as-is
	ResultExhausted         = "exhausted"            // max attempts reached
	ResultUnsafe            = "unsafe_to_retry"      // unknown outcome, not replay-safe, no reconciler: failed closed
	ResultBodyNotReplayable = "body_not_replayable"  // body could not be re-sent
	ResultBudget            = "budget_exhausted"     // shared retry budget was empty
	ResultRetryAfterTooLong = "retry_after_too_long" // server asked to wait longer than we allow
	ResultReconciled        = "reconciled"           // Reconcile probe found it already done
	ResultFallback          = "fallback"             // an OnGiveUp hook supplied the response
	ResultShortCircuit      = "short_circuit"        // a Before/After hook supplied the response
	ResultHookAborted       = "hook_aborted"         // a hook returned Fail
	ResultCanceled          = "canceled"             // the caller's context ended
	ResultBodyError         = "body_error"           // could not prepare the request body
	ResultPanic             = "panic"                // a hook panicked (the panic still propagates)
	ResultUnhealthy         = "downstream_unhealthy" // health gate said the downstream is down; nothing (more) was sent
)

func resultForReason(reason error) string {
	switch {
	case errors.Is(reason, ErrRetriesExhausted):
		return ResultExhausted
	case errors.Is(reason, ErrUnsafeToRetry):
		return ResultUnsafe
	case errors.Is(reason, ErrBodyNotReplayable):
		return ResultBodyNotReplayable
	case errors.Is(reason, ErrBudgetExhausted):
		return ResultBudget
	case errors.Is(reason, ErrRetryAfterTooLong):
		return ResultRetryAfterTooLong
	case errors.Is(reason, ErrUnhealthy):
		return ResultUnhealthy
	}
	return ResultExhausted
}

// Wait sources (Event.WaitSource).
const (
	WaitBackoff    = "backoff"
	WaitRetryAfter = "retry_after"
)

// Labels are the low-cardinality dimensions attached to every event.
// Never put raw URLs, paths with ids, or user ids here.
type Labels struct {
	Target    string // downstream service, defaults to the URL host
	Operation string // logical operation, e.g. "create_order"; default "unknown"
}

// Labeler derives Labels from a request. See DefaultLabeler and WithOperation.
type Labeler func(*http.Request) Labels

type operationKey struct{}

// WithOperation tags requests made with the returned context with an operation
// name, which DefaultLabeler exposes as Labels.Operation.
func WithOperation(ctx context.Context, op string) context.Context {
	return context.WithValue(ctx, operationKey{}, op)
}

// DefaultLabeler uses the URL host as Target and the WithOperation value as Operation.
func DefaultLabeler(r *http.Request) Labels {
	op, _ := r.Context().Value(operationKey{}).(string)
	if op == "" {
		op = "unknown"
	}
	return Labels{Target: r.URL.Host, Operation: op}
}

// Event is one observation. Which fields are set depends on Kind:
//
//	EventCallStart  Labels, Method
//	EventAttempt    + Attempt, Outcome, Status, StatusClass, ErrClass, Elapsed (this attempt), ReplaySafe
//	EventWait       + Attempt (the one that just failed), Wait, WaitSource
//	EventHook       + Attempt, Phase, Verdict, Elapsed (hook phase duration)
//	EventCallDone   + Attempts, Result, Status, StatusClass, ErrClass, Elapsed (whole call incl. waits)
//	EventGate       + Gate (rejected|waited|timeout), GateAt (start|retry), Wait (time spent waiting)
//	EventProbe      Labels, ProbeOK, ErrClass, Elapsed        (Method is empty)
//	EventHealth     Labels, HealthFrom, HealthTo              (Method is empty)
type Event struct {
	Kind   EventKind
	Time   time.Time // when the event was emitted (an attempt's start is Time-Elapsed)
	Labels Labels
	Method string

	Attempt     int
	Attempts    int
	Outcome     Outcome
	Status      int    // HTTP status, 0 if none
	StatusClass string // "2xx".."5xx" or "none"
	ErrClass    string // see ErrorClass; "none" if no error
	ReplaySafe  bool
	Elapsed     time.Duration

	Wait       time.Duration
	WaitSource string

	Phase   Phase
	Verdict string // "continue" | "succeed" | "fail"

	Result string

	Gate   string // EventGate: GateRejected | GateWaited | GateTimeout
	GateAt string // EventGate: GateAtStart | GateAtRetry

	ProbeOK    bool        // EventProbe
	HealthFrom HealthState // EventHealth
	HealthTo   HealthState // EventHealth
}

// Observer receives events. Implementations must be fast and concurrency-safe.
type Observer interface {
	Observe(ctx context.Context, e Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, e Event)

func (f ObserverFunc) Observe(ctx context.Context, e Event) { f(ctx, e) }

// Multi fans events out to several observers; one panicking observer does not
// stop the others.
func Multi(obs ...Observer) Observer {
	flat := make(multi, 0, len(obs))
	for _, o := range obs {
		if o != nil {
			flat = append(flat, o)
		}
	}
	return flat
}

type multi []Observer

func (m multi) Observe(ctx context.Context, e Event) {
	for _, o := range m {
		func() {
			defer func() { _ = recover() }()
			o.Observe(ctx, e)
		}()
	}
}

// Recorder is an in-memory Observer for tests and debugging.
type Recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *Recorder) Observe(_ context.Context, e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

// Events returns a copy of everything recorded so far.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

// Reset clears the recording.
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.events = nil
	r.mu.Unlock()
}

// ------------------------------------------------------------ label helpers

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func statusClass(resp *http.Response) string {
	switch c := statusOf(resp); {
	case c == 0:
		return "none"
	case c < 200:
		return "1xx"
	case c < 300:
		return "2xx"
	case c < 400:
		return "3xx"
	case c < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// ErrorClass buckets a transport error into a small closed set of label values:
// none, canceled, timeout, tls, dns, dial, conn_reset, other.
func ErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	var certErr *tls.CertificateVerificationError
	var dnsErr *net.DNSError
	var opErr *net.OpError
	var netErr net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &certErr):
		return "tls"
	case errors.As(err, &dnsErr):
		return "dns"
	case errors.As(err, &opErr) && opErr.Op == "dial":
		return "dial"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET):
		return "conn_reset"
	}
	return "other"
}

func (v Verdict) name() string {
	switch v.kind {
	case vSucceed:
		return "succeed"
	case vFail:
		return "fail"
	}
	return "continue"
}
