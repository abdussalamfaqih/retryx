package retryx

// Health gating: stop sending requests to a downstream that is known to be down.
//
//	                 ┌───────────── active: Check() every Interval ─────────────┐
//	                 │  (probes back off exponentially while the downstream is   │
//	                 │   down, so we do not bombard it with health checks either)│
//	   Health  ◄─────┤                                                           │
//	  (HealthGate)   └───────────── passive: real attempts Report() ────────────┘
//	      │  Healthy ⇄ Unhealthy
//	      ▼
//	  Transport:  new call while Unhealthy   → fail fast (or wait GatePolicy.StartWait)
//	              retry while Unhealthy      → do NOT sleep-and-hammer; wait for the
//	                                           "healthy again" signal (GatePolicy.RetryWait),
//	                                           then release waiters with random jitter
//	                                           so recovery is not a thundering herd.
//
// HealthGate is a tiny interface (State + Report), so a circuit breaker, a
// service-mesh signal or a Consul/Kubernetes check can be plugged in instead of
// Health. Optionally implement HealthWaiter to wake waiters instantly.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// ------------------------------------------------------------------- types

// HealthState is the state of a downstream as seen by a HealthGate.
type HealthState int

const (
	StateHealthy HealthState = iota
	StateUnhealthy
)

func (s HealthState) String() string {
	if s == StateUnhealthy {
		return "unhealthy"
	}
	return "healthy"
}

// HealthSignal is what the engine tells a gate about a finished attempt
// (passive health feedback from real traffic).
type HealthSignal struct {
	Status  int   // HTTP status, 0 if none
	Err     error // transport error, if any
	Outcome Outcome
	Elapsed time.Duration
}

func healthSignal(a *Attempt) HealthSignal {
	return HealthSignal{Status: statusOf(a.Resp), Err: a.Err, Outcome: a.Outcome, Elapsed: a.Elapsed}
}

// DefaultIsFailure decides whether an attempt counts against downstream health:
// transport errors (except the caller cancelling) and 502/503/504. Plain 500 is
// NOT counted: it usually means one broken endpoint, and a per-host gate would
// then block every other endpoint on that host. 429 is not counted either: the
// service is alive and is asking us to slow down (Retry-After handles that).
func DefaultIsFailure(s HealthSignal) bool {
	if s.Err != nil {
		return !errors.Is(s.Err, context.Canceled)
	}
	switch s.Status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// HealthGate is what the Transport consults. Implementations must be safe for
// concurrent use and must not block.
type HealthGate interface {
	State() HealthState
	Report(HealthSignal)
}

// HealthWaiter is optionally implemented by a HealthGate to block until it is
// healthy again (returning ctx.Err() if ctx ends first). Gates that do not
// implement it are polled every 100ms.
type HealthWaiter interface {
	WaitHealthy(ctx context.Context) error
}

// GateSelector picks the gate for a request (nil = do not gate this request).
type GateSelector func(*http.Request) HealthGate

// HealthByHost lazily creates one gate per URL host, e.g.
//
//	retryx.WithHealthSelector(retryx.HealthByHost(func(host string) retryx.HealthGate {
//	    h, _ := retryx.NewHealth(...)
//	    h.Start(ctx)
//	    return h
//	}), retryx.GatePolicy{})
//
// newGate must return an untyped nil (not a typed-nil pointer) to skip gating a host.
func HealthByHost(newGate func(host string) HealthGate) GateSelector {
	var mu sync.Mutex
	gates := make(map[string]HealthGate)
	return func(r *http.Request) HealthGate {
		mu.Lock()
		defer mu.Unlock()
		g, ok := gates[r.URL.Host]
		if !ok {
			g = newGate(r.URL.Host)
			gates[r.URL.Host] = g
		}
		return g
	}
}

// GatePolicy says what to do when the gate reports Unhealthy.
type GatePolicy struct {
	// StartWait is how long a NEW call waits for the downstream to recover
	// before failing with ErrUnhealthy. 0 (default) = fail fast, which protects
	// both the downstream and the caller's latency.
	StartWait time.Duration

	// RetryWait is how long a call that is already retrying waits for recovery.
	// 0 = default 30s; negative = do not wait, give up immediately.
	// The wait is also bounded by the request context's deadline.
	RetryWait time.Duration

	// ReleaseJitter spreads the release of waiting calls uniformly over
	// [0, ReleaseJitter] once the downstream is healthy again, so recovery is not a
	// thundering herd. 0 = default 1s; negative = release everyone at once.
	ReleaseJitter time.Duration
}

func (p GatePolicy) normalized() GatePolicy {
	if p.StartWait < 0 {
		p.StartWait = 0
	}
	switch {
	case p.RetryWait == 0:
		p.RetryWait = 30 * time.Second
	case p.RetryWait < 0:
		p.RetryWait = 0
	}
	switch {
	case p.ReleaseJitter == 0:
		p.ReleaseJitter = time.Second
	case p.ReleaseJitter < 0:
		p.ReleaseJitter = 0
	}
	return p
}

// Gate event results (Event.Gate) and positions (Event.GateAt).
const (
	GateRejected = "rejected" // unhealthy and we were not allowed to wait
	GateWaited   = "waited"   // waited, downstream recovered, call proceeded
	GateTimeout  = "timeout"  // waited, still unhealthy, gave up

	GateAtStart = "start" // before the first attempt
	GateAtRetry = "retry" // between attempts
)

// ------------------------------------------------- Transport-side helpers

// awaitHealth is called when gate is Unhealthy. It returns ("", nil) if the
// call may proceed, otherwise the final Result label and an error.
func (t *Transport) awaitHealth(ctx context.Context, call *Call, gate HealthGate, maxWait time.Duration, at string) (string, error) {
	if maxWait <= 0 {
		t.emitGate(ctx, call, at, GateRejected, 0)
		return ResultUnhealthy, ErrUnhealthy
	}
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, maxWait)
	err := waitHealthy(wctx, gate)
	cancel()
	if err != nil {
		if ctx.Err() != nil { // the caller gave up, not the wait
			return ResultCanceled, ctx.Err()
		}
		t.emitGate(ctx, call, at, GateTimeout, time.Since(start))
		return ResultUnhealthy, ErrUnhealthy
	}
	if j := t.cfg.gatePolicy.ReleaseJitter; j > 0 {
		if err := sleep(ctx, time.Duration(rand.Int63n(int64(j)+1))); err != nil {
			return ResultCanceled, err
		}
	}
	t.emitGate(ctx, call, at, GateWaited, time.Since(start))
	return "", nil
}

// pause is the wait between two attempts. It replaces a plain sleep: if the
// downstream is already known to be down there is no point sleeping the backoff
// and then failing again, so we go straight to waiting for recovery.
func (t *Transport) pause(ctx context.Context, call *Call, gate HealthGate, delay time.Duration) (string, error) {
	if gate != nil && gate.State() == StateUnhealthy {
		return t.awaitHealth(ctx, call, gate, t.cfg.gatePolicy.RetryWait, GateAtRetry)
	}
	if err := sleep(ctx, delay); err != nil {
		return ResultCanceled, err
	}
	if gate != nil && gate.State() == StateUnhealthy { // it tripped while we slept
		return t.awaitHealth(ctx, call, gate, t.cfg.gatePolicy.RetryWait, GateAtRetry)
	}
	return "", nil
}

func (t *Transport) emitGate(ctx context.Context, call *Call, at, result string, waited time.Duration) {
	if t.cfg.observer == nil {
		return
	}
	t.emit(ctx, call, Event{Kind: EventGate, Gate: result, GateAt: at, Wait: waited})
}

func waitHealthy(ctx context.Context, g HealthGate) error {
	if w, ok := g.(HealthWaiter); ok {
		return w.WaitHealthy(ctx)
	}
	return WaitUntilHealthy(ctx, g, 100*time.Millisecond)
}

// WaitUntilHealthy polls g.State() until it is not Unhealthy or ctx ends.
// Useful for implementing HealthWaiter on top of a plain gate.
func WaitUntilHealthy(ctx context.Context, g HealthGate, poll time.Duration) error {
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for g.State() == StateUnhealthy {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// ------------------------------------------------------------------ Health

// Checker probes the downstream. Return nil if it is healthy. It must honour
// ctx (a per-probe timeout is applied). Use a client WITHOUT the retry Transport.
type Checker func(ctx context.Context) error

// HTTPCheck returns a Checker that GETs url and expects a 2xx answer.
// client must not contain the retrying Transport; nil means a plain http.Client.
func HTTPCheck(client *http.Client, url string) Checker {
	if client == nil {
		client = &http.Client{}
	}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("health check: HTTP %d", resp.StatusCode)
		}
		return nil
	}
}

// HealthConfig configures Health. Only Check is required.
type HealthConfig struct {
	Check    Checker
	Labels   Labels   // attached to the Probe/Health events (set Target)
	Observer Observer // receives EventProbe / EventHealth (optional)

	Interval           time.Duration // probe period while healthy            (default 10s)
	Timeout            time.Duration // per-probe timeout                     (default 2s)
	RecheckInterval    time.Duration // first re-probe after a failure, and probe period while confirming recovery (default 2s)
	MaxRecheckInterval time.Duration // cap of the exponential probe backoff while unhealthy (default 30s)

	FailureThreshold int // consecutive failed probes before Unhealthy      (default 3)
	SuccessThreshold int // consecutive OK probes before Healthy again      (default 2)

	// PassiveThreshold trips the gate after that many consecutive failed REAL
	// attempts (see IsFailure), without waiting for the next probe. 0 = off.
	PassiveThreshold int
	IsFailure        func(HealthSignal) bool // default DefaultIsFailure

	// HoldDown is the minimum time the gate stays Unhealthy once tripped, even if
	// probes succeed. Use it when your probe endpoint can be green while the real
	// API is broken (prevents rapid flapping).
	HoldDown time.Duration
}

// Health is a HealthGate combining active probes and passive failure counting.
// The zero state is Healthy (optimistic: do not block traffic before the first probe).
type Health struct {
	cfg HealthConfig

	mu         sync.Mutex
	unhealthy  bool
	since      time.Time
	probeFails int
	probeOKs   int
	callFails  int
	probeWait  time.Duration // current exponential probe delay while unhealthy
	lastErr    error
	ready      chan struct{} // closed while healthy; replaced by an open channel while unhealthy
	kick       chan struct{} // wakes the probe loop early (passive trip)
	cancel     context.CancelFunc
	done       chan struct{}
}

var (
	_ HealthGate   = (*Health)(nil)
	_ HealthWaiter = (*Health)(nil)
)

// NewHealth validates cfg and applies defaults. Call Start to begin probing.
func NewHealth(cfg HealthConfig) (*Health, error) {
	if cfg.Check == nil {
		return nil, errors.New("retryx: HealthConfig.Check is required")
	}
	def := func(p *time.Duration, v time.Duration) {
		if *p <= 0 {
			*p = v
		}
	}
	def(&cfg.Interval, 10*time.Second)
	def(&cfg.Timeout, 2*time.Second)
	def(&cfg.RecheckInterval, 2*time.Second)
	def(&cfg.MaxRecheckInterval, 30*time.Second)
	if cfg.MaxRecheckInterval < cfg.RecheckInterval {
		cfg.MaxRecheckInterval = cfg.RecheckInterval
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = DefaultIsFailure
	}
	h := &Health{cfg: cfg, since: time.Now(), kick: make(chan struct{}, 1)}
	h.ready = make(chan struct{})
	close(h.ready)
	return h, nil
}

// Start launches the probe loop (first probe immediately). It stops when ctx is
// done or Stop is called. Calling Start twice is a no-op; a stopped Health
// cannot be restarted.
func (h *Health) Start(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.done = make(chan struct{})
	go func() {
		defer close(h.done)
		h.loop(ctx)
	}()
}

// Stop ends the probe loop and waits for it to exit.
func (h *Health) Stop() {
	h.mu.Lock()
	cancel, done := h.cancel, h.done
	h.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// State implements HealthGate.
func (h *Health) State() HealthState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return stateOf(h.unhealthy)
}

// LastError is the most recent probe or passive failure that caused (or
// contributed to) an Unhealthy state; useful in logs and admin endpoints.
func (h *Health) LastError() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastErr
}

// WaitHealthy implements HealthWaiter.
func (h *Health) WaitHealthy(ctx context.Context) error {
	for {
		h.mu.Lock()
		ch, bad := h.ready, h.unhealthy
		h.mu.Unlock()
		if !bad {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Report implements HealthGate: passive feedback from real traffic.
// Only failures can trip the gate; recovery is always decided by probes.
func (h *Health) Report(s HealthSignal) {
	if h.cfg.PassiveThreshold <= 0 {
		return
	}
	failed := h.cfg.IsFailure(s)

	h.mu.Lock()
	was := h.unhealthy
	if !h.unhealthy {
		if failed {
			h.callFails++
			if h.callFails >= h.cfg.PassiveThreshold {
				h.tripLocked(time.Now(), passiveCause(s))
				select { // check now instead of waiting up to Interval
				case h.kick <- struct{}{}:
				default:
				}
			}
		} else {
			h.callFails = 0
		}
	}
	is := h.unhealthy
	h.mu.Unlock()

	h.emitChange(was, is)
}

func passiveCause(s HealthSignal) error {
	if s.Err != nil {
		return fmt.Errorf("passive: %w", s.Err)
	}
	return fmt.Errorf("passive: HTTP %d", s.Status)
}

// recordProbe applies one probe result to the state machine.
func (h *Health) recordProbe(err error, elapsed time.Duration) {
	now := time.Now()
	h.mu.Lock()
	was := h.unhealthy
	if err == nil {
		h.probeFails = 0
		h.probeOKs++
		if h.unhealthy && h.probeOKs >= h.cfg.SuccessThreshold && now.Sub(h.since) >= h.cfg.HoldDown {
			h.recoverLocked(now)
		}
	} else {
		h.probeOKs = 0
		h.probeFails++
		h.lastErr = err
		if !h.unhealthy && h.probeFails >= h.cfg.FailureThreshold {
			h.tripLocked(now, err)
		}
		if h.unhealthy { // exponential backoff of the probes themselves
			if h.probeWait == 0 {
				h.probeWait = h.cfg.RecheckInterval
			} else {
				h.probeWait *= 2
				if h.probeWait > h.cfg.MaxRecheckInterval {
					h.probeWait = h.cfg.MaxRecheckInterval
				}
			}
		}
	}
	is := h.unhealthy
	h.mu.Unlock()

	h.emit(Event{Kind: EventProbe, ProbeOK: err == nil, ErrClass: ErrorClass(err), Elapsed: elapsed})
	h.emitChange(was, is)
}

func (h *Health) tripLocked(now time.Time, cause error) {
	h.unhealthy = true
	h.since = now
	h.lastErr = cause
	h.callFails = 0
	h.probeOKs = 0
	h.probeWait = 0
	h.ready = make(chan struct{})
}

func (h *Health) recoverLocked(now time.Time) {
	h.unhealthy = false
	h.since = now
	h.probeFails, h.probeOKs, h.callFails = 0, 0, 0
	h.probeWait = 0
	close(h.ready)
}

func (h *Health) loop(ctx context.Context) {
	wait := time.Duration(0) // first probe right away
	for {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-h.kick:
		case <-timer.C:
		}
		timer.Stop()
		h.probe(ctx)
		wait = h.nextInterval()
	}
}

func (h *Health) probe(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()
	start := time.Now()
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("retryx: health check panicked: %v", r)
			}
		}()
		return h.cfg.Check(pctx)
	}()
	elapsed := time.Since(start)
	if ctx.Err() != nil { // shutting down: a cancelled probe says nothing about the downstream
		return
	}
	h.recordProbe(err, elapsed)
}

// nextInterval: slow while healthy, quick while suspicious or confirming
// recovery, exponentially backed off while confirmed down. ±10% jitter so many
// instances do not probe in lockstep.
func (h *Health) nextInterval() time.Duration {
	h.mu.Lock()
	var d time.Duration
	switch {
	case h.unhealthy && h.probeOKs > 0:
		d = h.cfg.RecheckInterval // recovering: confirm quickly
	case h.unhealthy:
		d = h.probeWait
		if d <= 0 {
			d = h.cfg.RecheckInterval
		}
	case h.probeFails > 0:
		d = h.cfg.RecheckInterval // suspicious: confirm quickly
	default:
		d = h.cfg.Interval
	}
	h.mu.Unlock()
	return d + time.Duration((rand.Float64()*2-1)*0.1*float64(d))
}

func stateOf(unhealthy bool) HealthState {
	if unhealthy {
		return StateUnhealthy
	}
	return StateHealthy
}

func (h *Health) emit(e Event) {
	if h.cfg.Observer == nil {
		return
	}
	e.Time = time.Now()
	e.Labels = h.cfg.Labels
	defer func() { _ = recover() }()
	h.cfg.Observer.Observe(context.Background(), e)
}

func (h *Health) emitChange(was, is bool) {
	if was != is {
		h.emit(Event{Kind: EventHealth, HealthFrom: stateOf(was), HealthTo: stateOf(is)})
	}
}
