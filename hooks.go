package retryx

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// ------------------------------------------------------- idempotency key

// KeyScope controls how long an idempotency key lives.
type KeyScope int

const (
	// KeyPerCall generates the key once and reuses it on every retry. This is
	// what makes replay SAFE: the server can dedupe. It marks the call
	// idempotent, so Unknown outcomes are retried without a reconciler.
	KeyPerCall KeyScope = iota

	// KeyPerAttempt generates a fresh key for every attempt. This does NOT make
	// replay safe (the server sees each attempt as a new operation), so the call
	// is not marked idempotent: pair it with a Reconcile probe. Use it for
	// downstreams that reject a reused key after a partial failure (e.g. 409/422
	// "key already used with different state").
	KeyPerAttempt
)

type idemKey struct{}

// IdempotencyKey is a Before hook that sets header on every attempt.
// A key already present on the caller's request is used for the first attempt.
func IdempotencyKey(header string, gen func() string, scope KeyScope) Hook {
	return func(_ context.Context, a *Attempt) Verdict {
		key, have := Value[string](a.Call, idemKey{})
		switch {
		case scope == KeyPerCall && have:
			// reuse: same logical operation, same key
		case a.N == 1 && a.Req.Header.Get(header) != "":
			key = a.Req.Header.Get(header)
		default:
			key = gen()
		}
		a.Call.Set(idemKey{}, key)
		a.Req.Header.Set(header, key)
		if scope == KeyPerCall {
			a.Call.Idempotent = true
		}
		return Continue()
	}
}

// IdempotencyKeyOf returns the key currently in use for the call.
func IdempotencyKeyOf(c *Call) (string, bool) { return Value[string](c, idemKey{}) }

// RandomKey returns a random UUIDv4 string, suitable as an idempotency key.
func RandomKey() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// ---------------------------------------------------- reconcile (check first)

// Probe asks the downstream "did my previous attempt already take effect?".
//
//	(resp, nil) - yes; resp is what the caller should receive instead
//	(nil, nil)  - definitely not; go ahead and retry
//	(nil, err)  - could not tell
//
// Build resp with NewResponse if you need to translate e.g. a GET 200 into the
// 201 the original POST would have returned.
type Probe func(ctx context.Context, a *Attempt) (*http.Response, error)

// Reconciler turns a Probe into a Reconcile hook. If the probe is inconclusive:
// failOpen=false aborts (safe: never risks a duplicate), failOpen=true retries
// anyway (available: risks a duplicate).
func Reconciler(p Probe, failOpen bool) Hook {
	return func(ctx context.Context, a *Attempt) Verdict {
		resp, err := p(ctx, a)
		switch {
		case err != nil && failOpen:
			return Continue()
		case err != nil:
			return Fail(fmt.Errorf("retryx: reconcile probe inconclusive, refusing to retry: %w", err))
		case resp != nil:
			return Succeed(resp)
		default:
			return Continue()
		}
	}
}

// NewResponse builds a synthetic *http.Response, e.g. for a Probe result.
func NewResponse(req *http.Request, status int, header http.Header, body []byte) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

// -------------------------------------------------------------- small hooks

// AttemptHeader tells the downstream which attempt this is (useful in its logs).
func AttemptHeader(name string) Hook {
	return func(_ context.Context, a *Attempt) Verdict {
		a.Req.Header.Set(name, strconv.Itoa(a.N))
		return Continue()
	}
}

// FailoverHosts sends attempt n to hosts[(n-1) % len(hosts)] (region/replica failover).
func FailoverHosts(hosts ...string) Hook {
	return func(_ context.Context, a *Attempt) Verdict {
		if len(hosts) > 0 {
			h := hosts[(a.N-1)%len(hosts)]
			a.Req.URL.Host = h
			a.Req.Host = h
		}
		return Continue()
	}
}

// PeekBody reads up to limit bytes of resp.Body and puts them back, so later
// readers (and the final caller) still see the full body.
func PeekBody(resp *http.Response, limit int64) ([]byte, error) {
	buf, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	resp.Body = &replayBody{
		Reader: io.MultiReader(bytes.NewReader(buf), resp.Body),
		closer: resp.Body,
	}
	return buf, err
}

type replayBody struct {
	io.Reader
	closer io.Closer
}

func (r *replayBody) Close() error { return r.closer.Close() }

// RetryOnBody is an After hook for APIs that return HTTP 200 with an error
// payload (GraphQL errors, {"status":"busy"}, ...). classify may return a new
// Outcome and true to override the status-based classification.
func RetryOnBody(limit int64, classify func(status int, body []byte) (Outcome, bool)) Hook {
	return func(_ context.Context, a *Attempt) Verdict {
		if a.Err != nil || a.Outcome != OutcomeSuccess {
			return Continue()
		}
		body, err := PeekBody(a.Resp, limit)
		if err != nil {
			a.Outcome = OutcomeUnknown
			return Continue()
		}
		if o, ok := classify(a.Resp.StatusCode, body); ok {
			a.Outcome = o
		}
		return Continue()
	}
}
