// Package retryx is an HTTP retry layer built around one idea that most retry
// libraries lack: a failed attempt is not simply "retry / don't retry".
//
// Every attempt ends in one of four Outcomes:
//
//	Success     - done.
//	NotApplied  - failed, and the server almost certainly did NOT process it
//	              (dial error, 429, 503 ...). Always safe to retry.
//	Unknown     - failed, but the server MAY have processed it (timeout,
//	              connection reset, 500/502/504). Retrying a non-idempotent
//	              call blindly can create duplicates.
//	Fatal       - failed and will never succeed (4xx, TLS verify error ...).
//
// Lifecycle of one logical call (each box is a hook phase you can plug into):
//
//	┌───────────────────────────────────────────────────────────────┐
//	│ for attempt := 1..N                                           │
//	│   Before hooks   ← set idempotency key, re-sign, refresh token│
//	│   send request                                                │
//	│   classify → Outcome                                          │
//	│   After hooks    ← inspect body, override Outcome             │
//	│   Success/Fatal  → return                                     │
//	│   safety gate    ← Unknown + not replay-safe + no reconciler  │
//	│                    → fail closed (no blind duplicate POST)    │
//	│   backoff (+ Retry-After), budget check                       │
//	│   Reconcile hooks← "did it already happen?" probe; may return │
//	│                    a synthetic success and stop retrying      │
//	│ GiveUp hooks     ← fallback response, compensation, DLQ       │
//	└───────────────────────────────────────────────────────────────┘
//
// All hooks share one signature (Hook) and return a Verdict, so a new use case
// is a new function, not a new library feature.
//
// Health gating: attach a HealthGate (see health.go) and calls are not sent
// while the downstream is known to be down; retries wait for recovery instead of
// hammering it, and released waiters are jittered to avoid a thundering herd.
//
// Observability: the engine emits Events (call start/done, attempt, wait, hook)
// to any number of Observers (see observer.go). Adapters live in obs/*.
//
// NOTE: a Reconcile probe must use an http.Client that does NOT contain this
// Transport, otherwise the probe would itself be retried recursively.
package retryx

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ---------------------------------------------------------------- outcomes

// Outcome is the result of classifying one attempt.
type Outcome int

const (
	OutcomeSuccess    Outcome = iota // return the response to the caller
	OutcomeNotApplied                // definitely not processed: safe to retry
	OutcomeUnknown                   // may have been processed: retry only if replay-safe or reconciled
	OutcomeFatal                     // never retry
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeNotApplied:
		return "not_applied"
	case OutcomeUnknown:
		return "unknown"
	case OutcomeFatal:
		return "fatal"
	}
	return "invalid"
}

// Classifier maps a finished attempt (a.Resp / a.Err) to an Outcome.
type Classifier func(a *Attempt) Outcome

// DefaultClassify is a conservative classifier.
func DefaultClassify(a *Attempt) Outcome {
	if a.Err != nil {
		var certErr *tls.CertificateVerificationError
		if errors.As(a.Err, &certErr) || errors.Is(a.Err, context.Canceled) {
			return OutcomeFatal
		}
		var dnsErr *net.DNSError
		if errors.As(a.Err, &dnsErr) {
			if dnsErr.IsNotFound {
				return OutcomeFatal
			}
			return OutcomeNotApplied
		}
		var opErr *net.OpError
		if errors.As(a.Err, &opErr) && opErr.Op == "dial" {
			return OutcomeNotApplied // never reached the server
		}
		// timeouts, resets, EOF, per-attempt deadline: we cannot know.
		return OutcomeUnknown
	}
	code := a.Resp.StatusCode
	switch {
	case code < 400:
		return OutcomeSuccess
	case code == 408, code == 425, code == 429, code == 503:
		return OutcomeNotApplied // server explicitly refused / asked us to come back
	case code == 501, code == 505:
		return OutcomeFatal
	case code >= 500:
		return OutcomeUnknown
	default:
		return OutcomeFatal
	}
}

// ------------------------------------------------------------- call state

// Result is the compact record of a finished attempt kept in Call.History.
type Result struct {
	N       int
	Status  int // 0 if no response
	Err     error
	Outcome Outcome
	Elapsed time.Duration
}

// Call is the state of one logical request, shared by all its attempts.
type Call struct {
	Original *http.Request // the caller's request; never mutated
	History  []Result      // finished attempts so far
	// Idempotent declares that replaying this call is safe (e.g. because an
	// idempotency key was attached). Hooks may set it.
	Idempotent bool

	values   map[any]any
	labels   Labels
	attempts int
}

// Set stores a value for later attempts / hooks (e.g. a downstream resource id).
func (c *Call) Set(key, val any) { c.values[key] = val }

// Value reads a typed value stored with Set.
func Value[T any](c *Call, key any) (T, bool) {
	v, ok := c.values[key]
	if !ok {
		var zero T
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

// Attempt is one try inside a Call. Req is a private clone: mutate it freely.
type Attempt struct {
	*Call
	N       int            // 1-based
	Req     *http.Request  // request for this attempt only
	Resp    *http.Response // nil until sent; after a retry decision the body is already drained/closed
	Err     error
	Outcome Outcome
	Elapsed time.Duration
	Reason  error // set only in GiveUp hooks: why we stopped

	cancel context.CancelFunc
	done   bool
}

// ReplaySafe reports whether sending this request again cannot create a
// duplicate side effect (safe/idempotent method, or hook-declared idempotency).
func (a *Attempt) ReplaySafe() bool {
	if a.Call.Idempotent {
		return true
	}
	switch a.Req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace,
		http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

func (a *Attempt) result() Result {
	r := Result{N: a.N, Err: a.Err, Outcome: a.Outcome, Elapsed: a.Elapsed}
	if a.Resp != nil {
		r.Status = a.Resp.StatusCode
	}
	return r
}

// discard drains and closes the response (so the connection can be reused)
// and releases the per-attempt context. Safe to call more than once.
func (a *Attempt) discard() {
	if a.done {
		return
	}
	a.done = true
	if a.Resp != nil && a.Resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(a.Resp.Body, 4<<10))
		_ = a.Resp.Body.Close()
	}
	if a.cancel != nil {
		a.cancel()
	}
}

// deliver hands this attempt's result to the caller.
func (a *Attempt) deliver() (*http.Response, error) {
	if a.Err != nil {
		a.discard()
		return nil, a.Err
	}
	a.done = true
	if a.cancel != nil { // keep the attempt context alive until the caller closes the body
		a.Resp.Body = &cancelBody{ReadCloser: a.Resp.Body, cancel: a.cancel}
	}
	return a.Resp, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// ------------------------------------------------------------ hooks/verdicts

type verdictKind int

const (
	vContinue verdictKind = iota
	vSucceed
	vFail
)

// Verdict is what a Hook returns. The zero value means "carry on".
type Verdict struct {
	kind verdictKind
	resp *http.Response
	err  error
}

// Continue lets the engine proceed normally.
func Continue() Verdict { return Verdict{} }

// Succeed stops retrying and returns resp to the caller (resp must be non-nil).
// Use it to return a response found by a probe, or a fallback response.
func Succeed(resp *http.Response) Verdict { return Verdict{kind: vSucceed, resp: resp} }

// Fail stops retrying and returns err to the caller.
func Fail(err error) Verdict {
	if err == nil {
		err = errors.New("retryx: aborted by hook")
	}
	return Verdict{kind: vFail, err: err}
}

// Hook is the single extension signature used by every phase.
type Hook func(ctx context.Context, a *Attempt) Verdict

// finish ends the call because a hook returned Succeed or Fail.
// okResult is the result label to report when the hook succeeded.
func finish(a *Attempt, v Verdict, okResult string) (*http.Response, string, error) {
	if v.kind == vSucceed {
		if v.resp == nil {
			a.discard()
			return nil, ResultHookAborted, errors.New("retryx: hook returned Succeed with nil response")
		}
		if v.resp == a.Resp && !a.done {
			resp, err := a.deliver()
			return resp, okResult, err
		}
		a.discard()
		return v.resp, okResult, nil
	}
	a.discard()
	return nil, ResultHookAborted, v.err
}

func runHooks(ctx context.Context, hooks []Hook, a *Attempt) Verdict {
	for _, h := range hooks {
		if v := h(ctx, a); v.kind != vContinue {
			return v
		}
	}
	return Continue()
}

// ------------------------------------------------------------------- errors

var (
	ErrRetriesExhausted  = errors.New("retryx: max attempts reached")
	ErrUnsafeToRetry     = errors.New("retryx: outcome unknown and request is not replay-safe")
	ErrBodyNotReplayable = errors.New("retryx: request body cannot be replayed")
	ErrBodyTooLarge      = errors.New("retryx: request body exceeds buffer limit; set GetBody or raise WithBodyBuffer")
	ErrBudgetExhausted   = errors.New("retryx: retry budget exhausted")
	ErrRetryAfterTooLong = errors.New("retryx: server Retry-After exceeds configured maximum")
	ErrUnhealthy         = errors.New("retryx: downstream is unhealthy")
)

// ExhaustedError is returned when we give up after a transport-level failure.
// (When we give up after an HTTP status, the last response is returned instead,
// like net/http does.) errors.Is(err, ErrUnsafeToRetry) etc. work.
type ExhaustedError struct {
	Attempts int
	History  []Result
	Reason   error
	Err      error
}

func (e *ExhaustedError) Error() string {
	if e.Err == nil || errors.Is(e.Err, e.Reason) { // e.g. nothing was sent: avoid printing the reason twice
		return fmt.Sprintf("retryx: giving up after %d attempt(s): %v", e.Attempts, e.Reason)
	}
	return fmt.Sprintf("retryx: giving up after %d attempt(s) (%v): %v", e.Attempts, e.Reason, e.Err)
}
func (e *ExhaustedError) Unwrap() error { return e.Err }
func (e *ExhaustedError) Is(target error) bool {
	return e.Reason != nil && errors.Is(e.Reason, target)
}

// ------------------------------------------------------------------ backoff

// Backoff returns how long to wait before the next attempt.
type Backoff func(a *Attempt) time.Duration

// ExponentialJitter doubles the delay each attempt up to max, then applies
// "equal jitter" (half fixed, half random) to avoid synchronized retry storms.
func ExponentialJitter(base, max time.Duration) Backoff {
	return func(a *Attempt) time.Duration {
		d := base
		for i := 1; i < a.N && d < max; i++ {
			d *= 2
		}
		if d > max {
			d = max
		}
		half := d / 2
		return half + time.Duration(rand.Int63n(int64(half)+1))
	}
}

func retryAfter(resp *http.Response) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if at, err := http.ParseTime(v); err == nil {
		d := time.Until(at)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ------------------------------------------------------------------- budget

// Budget is a retry budget shared by many calls (gRPC-style token bucket).
// Each retry spends one token; each success refunds `ratio` tokens. When the
// downstream is failing broadly the bucket empties and retries stop, instead of
// multiplying load by MaxAttempts during an outage.
type Budget struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	ratio  float64
}

// NewBudget creates a budget holding up to max tokens, refunding ratio per success.
func NewBudget(max, ratio float64) *Budget { return &Budget{tokens: max, max: max, ratio: ratio} }

// Tokens returns the tokens currently available (export it as a gauge).
func (b *Budget) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}

func (b *Budget) allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (b *Budget) onSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = math.Min(b.max, b.tokens+b.ratio)
	b.mu.Unlock()
}

// ------------------------------------------------------------------- config

type config struct {
	maxAttempts    int
	backoff        Backoff
	classify       Classifier
	attemptTimeout time.Duration
	maxRetryAfter  time.Duration
	bufferBody     int64
	budget         *Budget
	observer       Observer
	gate           GateSelector
	gatePolicy     GatePolicy
	labeler        Labeler
	before         []Hook
	after          []Hook
	reconcile      []Hook
	giveUp         []Hook
}

// Option configures a Transport.
type Option func(*config)

func WithMaxAttempts(n int) Option              { return func(c *config) { c.maxAttempts = n } }
func WithBackoff(b Backoff) Option              { return func(c *config) { c.backoff = b } }
func WithClassifier(f Classifier) Option        { return func(c *config) { c.classify = f } }
func WithAttemptTimeout(d time.Duration) Option { return func(c *config) { c.attemptTimeout = d } }
func WithMaxRetryAfter(d time.Duration) Option  { return func(c *config) { c.maxRetryAfter = d } }
func WithBudget(b *Budget) Option               { return func(c *config) { c.budget = b } }

// WithLabeler overrides how Labels (Target, Operation) are derived per request.
func WithLabeler(l Labeler) Option { return func(c *config) { c.labeler = l } }

// WithObserver injects one or more observers (metrics, tracing, logging ...).
// Repeated use appends.
func WithObserver(o ...Observer) Option {
	return func(c *config) {
		all := o
		if c.observer != nil {
			all = append([]Observer{c.observer}, o...)
		}
		c.observer = Multi(all...)
	}
}

// WithHealth gates every call on g: while g reports unhealthy, no HTTP request
// is sent. See GatePolicy for fail-fast vs wait-for-recovery behaviour.
func WithHealth(g HealthGate, p GatePolicy) Option {
	return WithHealthSelector(func(*http.Request) HealthGate { return g }, p)
}

// WithHealthSelector is like WithHealth but picks the gate per request, e.g. one
// gate per downstream host (see HealthByHost). The selector may return nil to
// skip gating for a request.
func WithHealthSelector(sel GateSelector, p GatePolicy) Option {
	return func(c *config) { c.gate, c.gatePolicy = sel, p }
}

// WithBodyBuffer buffers bodies that have no GetBody, up to limit bytes, so they
// can be replayed. 0 disables buffering (such requests are sent once).
func WithBodyBuffer(limit int64) Option { return func(c *config) { c.bufferBody = limit } }

// Before runs on every attempt (including the first), before sending.
func Before(h ...Hook) Option { return func(c *config) { c.before = append(c.before, h...) } }

// After runs after each attempt is classified; hooks may change a.Outcome.
func After(h ...Hook) Option { return func(c *config) { c.after = append(c.after, h...) } }

// Reconcile runs between attempts, only when the last outcome was Unknown.
func Reconcile(h ...Hook) Option { return func(c *config) { c.reconcile = append(c.reconcile, h...) } }

// OnGiveUp runs once when retrying stops without success (a.Reason says why).
func OnGiveUp(h ...Hook) Option { return func(c *config) { c.giveUp = append(c.giveUp, h...) } }

// --------------------------------------------------------------- transport

// Transport is a retrying http.RoundTripper.
type Transport struct {
	next http.RoundTripper
	cfg  config
}

// New wraps next (nil means http.DefaultTransport).
func New(next http.RoundTripper, opts ...Option) *Transport {
	cfg := config{
		maxAttempts:   4,
		backoff:       ExponentialJitter(100*time.Millisecond, 5*time.Second),
		classify:      DefaultClassify,
		maxRetryAfter: 30 * time.Second,
		bufferBody:    1 << 20,
		labeler:       DefaultLabeler,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}
	cfg.gatePolicy = cfg.gatePolicy.normalized()
	return &Transport{next: next, cfg: cfg}
}

// NewClient returns an *http.Client using a retrying Transport over http.DefaultTransport.
func NewClient(opts ...Option) *http.Client {
	return &http.Client{Transport: New(nil, opts...)}
}

// emit sends an event to the observer, if any. It never panics or blocks the
// caller on observer failure.
func (t *Transport) emit(ctx context.Context, c *Call, e Event) {
	if t.cfg.observer == nil {
		return
	}
	e.Time = time.Now()
	e.Labels = c.labels
	e.Method = c.Original.Method
	if e.Method == "" {
		e.Method = http.MethodGet
	}
	defer func() { _ = recover() }()
	t.cfg.observer.Observe(ctx, e)
}

// runPhase runs the hooks of one phase and reports how long they took.
func (t *Transport) runPhase(ctx context.Context, p Phase, hooks []Hook, a *Attempt) Verdict {
	if len(hooks) == 0 {
		return Continue()
	}
	start := time.Now()
	v := runHooks(ctx, hooks, a)
	if t.cfg.observer != nil {
		t.emit(ctx, a.Call, Event{
			Kind: EventHook, Attempt: a.N, Phase: p, Verdict: v.name(), Elapsed: time.Since(start),
		})
	}
	return v
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	call := &Call{Original: req, values: make(map[any]any)}
	if t.cfg.observer == nil {
		resp, _, err := t.do(ctx, call)
		return resp, err
	}

	call.labels = t.cfg.labeler(req)
	start := time.Now()
	t.emit(ctx, call, Event{Kind: EventCallStart})

	// A panicking hook propagates to the caller, but the call must still be
	// closed in the metrics, otherwise calls_in_flight would drift upwards.
	finished := false
	defer func() {
		if !finished {
			t.emit(ctx, call, Event{
				Kind: EventCallDone, Attempts: call.attempts, Result: ResultPanic,
				StatusClass: "none", ErrClass: "none", Elapsed: time.Since(start),
			})
		}
	}()
	resp, result, err := t.do(ctx, call)
	finished = true
	t.emit(ctx, call, Event{
		Kind:        EventCallDone,
		Attempts:    call.attempts,
		Result:      result,
		Status:      statusOf(resp),
		StatusClass: statusClass(resp),
		ErrClass:    ErrorClass(err),
		Elapsed:     time.Since(start),
	})
	return resp, err
}

// do runs the retry loop. The middle return value is the final Result label.
func (t *Transport) do(ctx context.Context, call *Call) (*http.Response, string, error) {
	getBody, replayable, err := bodyReplayer(call.Original, t.cfg.bufferBody)
	if err != nil {
		return nil, ResultBodyError, err
	}

	var gate HealthGate
	if t.cfg.gate != nil {
		gate = t.cfg.gate(call.Original)
	}

	for n := 1; ; n++ {
		a, err := t.newAttempt(call, n, getBody)
		if err != nil {
			return nil, ResultBodyError, err
		}

		// Health gate at the start of the call: do not even build the request
		// (Before hooks) while the downstream is known to be down.
		if n == 1 && gate != nil && gate.State() == StateUnhealthy {
			if res, err := t.awaitHealth(ctx, call, gate, t.cfg.gatePolicy.StartWait, "start"); err != nil {
				if res == ResultUnhealthy {
					return t.giveUp(ctx, a, ErrUnhealthy)
				}
				a.discard()
				return nil, res, err
			}
		}

		if v := t.runPhase(ctx, PhaseBefore, t.cfg.before, a); v.kind != vContinue {
			return finish(a, v, ResultShortCircuit)
		}

		// The per-attempt timeout covers only the HTTP round trip, not health
		// waits or Before hooks.
		if t.cfg.attemptTimeout > 0 {
			actx, cancel := context.WithTimeout(ctx, t.cfg.attemptTimeout)
			a.cancel = cancel
			a.Req = a.Req.WithContext(actx)
		}
		call.attempts++

		start := time.Now()
		next := t.next
		if next == nil {
			next = http.DefaultTransport
		}
		a.Resp, a.Err = next.RoundTrip(a.Req)
		a.Elapsed = time.Since(start)

		if a.Err != nil && ctx.Err() != nil { // the caller gave up: stop immediately
			a.Outcome = OutcomeFatal
			t.emitAttempt(ctx, a)
			a.discard()
			return nil, ResultCanceled, a.Err
		}

		a.Outcome = t.cfg.classify(a)
		if gate != nil {
			gate.Report(healthSignal(a)) // passive health feedback from real traffic
		}
		v := t.runPhase(ctx, PhaseAfter, t.cfg.after, a)
		call.History = append(call.History, a.result())
		t.emitAttempt(ctx, a) // after hooks, so overridden outcomes are what we report
		if v.kind != vContinue {
			return finish(a, v, ResultShortCircuit)
		}

		switch a.Outcome {
		case OutcomeSuccess:
			t.cfg.budget.onSuccess()
			resp, err := a.deliver()
			return resp, ResultSuccess, err
		case OutcomeFatal:
			resp, err := a.deliver()
			return resp, ResultNonRetryable, err
		}

		// --- safety gates: decide whether we are allowed to try again
		switch {
		case n >= t.cfg.maxAttempts:
			return t.giveUp(ctx, a, ErrRetriesExhausted)
		case !replayable:
			return t.giveUp(ctx, a, ErrBodyNotReplayable)
		case a.Outcome == OutcomeUnknown && !a.ReplaySafe() && len(t.cfg.reconcile) == 0:
			return t.giveUp(ctx, a, ErrUnsafeToRetry) // fail closed: no blind duplicate POST
		case !t.cfg.budget.allow():
			return t.giveUp(ctx, a, ErrBudgetExhausted)
		}

		// --- wait (backoff, or the server's Retry-After if longer)
		delay, source := t.cfg.backoff(a), WaitBackoff
		if ra, ok := retryAfter(a.Resp); ok {
			if ra > t.cfg.maxRetryAfter {
				return t.giveUp(ctx, a, ErrRetryAfterTooLong)
			}
			if ra > delay {
				delay, source = ra, WaitRetryAfter
			}
		}
		if t.cfg.observer != nil {
			t.emit(ctx, call, Event{Kind: EventWait, Attempt: n, Wait: delay, WaitSource: source})
		}
		a.discard()
		// Wait out the backoff; if the downstream is (or becomes) unhealthy,
		// wait for recovery instead of spending attempts on a dead service.
		if res, err := t.pause(ctx, call, gate, delay); err != nil {
			if res == ResultUnhealthy {
				return t.giveUp(ctx, a, ErrUnhealthy)
			}
			return nil, res, err
		}

		// --- reconcile AFTER the wait, so eventually-consistent downstreams
		// have had time to make an in-flight write visible.
		if a.Outcome == OutcomeUnknown {
			if v := t.runPhase(ctx, PhaseReconcile, t.cfg.reconcile, a); v.kind != vContinue {
				return finish(a, v, ResultReconciled)
			}
		}
	}
}

func (t *Transport) emitAttempt(ctx context.Context, a *Attempt) {
	if t.cfg.observer == nil {
		return
	}
	t.emit(ctx, a.Call, Event{
		Kind:        EventAttempt,
		Attempt:     a.N,
		Outcome:     a.Outcome,
		Status:      statusOf(a.Resp),
		StatusClass: statusClass(a.Resp),
		ErrClass:    ErrorClass(a.Err),
		ReplaySafe:  a.ReplaySafe(),
		Elapsed:     a.Elapsed,
	})
}

func (t *Transport) giveUp(ctx context.Context, a *Attempt, reason error) (*http.Response, string, error) {
	a.Reason = reason
	if v := t.runPhase(ctx, PhaseGiveUp, t.cfg.giveUp, a); v.kind != vContinue {
		return finish(a, v, ResultFallback)
	}
	result := resultForReason(reason)
	// No deliverable response: transport error, no request sent yet, or the
	// response was already drained before we decided to stop.
	if a.Err != nil || a.Resp == nil || a.done {
		cause := a.Err
		switch {
		case cause != nil:
		case a.Resp != nil:
			cause = fmt.Errorf("last attempt returned HTTP %d", a.Resp.StatusCode)
		default:
			cause = reason
		}
		a.discard()
		return nil, result, &ExhaustedError{Attempts: a.Call.attempts, History: a.History, Reason: reason, Err: cause}
	}
	resp, err := a.deliver() // status-based failure: hand back the last response
	return resp, result, err
}

func (t *Transport) newAttempt(call *Call, n int, getBody func() (io.ReadCloser, error)) (*Attempt, error) {
	r := call.Original.Clone(call.Original.Context()) // deep-copies headers and URL
	if getBody != nil {
		body, err := getBody()
		if err != nil {
			return nil, err
		}
		r.Body = body
	}
	return &Attempt{Call: call, N: n, Req: r}, nil
}

// bodyReplayer returns a function producing a fresh body per attempt.
// replayable=false means the body can only be sent once.
func bodyReplayer(req *http.Request, limit int64) (func() (io.ReadCloser, error), bool, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, true, nil
	}
	if req.GetBody != nil {
		_ = req.Body.Close() // clones get their bodies from GetBody; the original is unused
		return req.GetBody, true, nil
	}
	if limit <= 0 {
		return nil, false, nil // first attempt uses req.Body as-is
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, limit+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return nil, false, ErrBodyTooLarge
	}
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}, true, nil
}
