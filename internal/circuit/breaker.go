package circuit

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrOpen is returned when a call is rejected because the circuit is open.
var ErrOpen = errors.New("circuit breaker is open")

type state int32

const (
	stateClosed   state = iota // normal operation
	stateOpen                  // failing fast, no calls allowed
	stateHalfOpen              // probe mode, limited calls allowed
)

// Breaker is a thread-safe circuit breaker that transitions between
// Closed → Open → HalfOpen → Closed states using only atomic operations.
//
// Transitions:
//
//	Closed   → Open:      consecutive failures exceed maxFailures
//	Open     → HalfOpen:  timeout elapses since last failure
//	HalfOpen → Closed:    successThreshold successes observed
//	HalfOpen → Open:      any failure resets back to Open
type Breaker struct {
	state        atomic.Int32
	failures     atomic.Int64
	successes    atomic.Int64  // tracked only in HalfOpen
	lastFailedAt atomic.Int64  // unix nanoseconds

	maxFailures      int64
	timeout          time.Duration
	successThreshold int64
}

// New creates a Breaker.
//   - maxFailures: consecutive failures before opening
//   - timeout: duration to stay Open before probing with HalfOpen
//   - successThreshold: successes needed in HalfOpen before closing
func New(maxFailures int64, timeout time.Duration, successThreshold int64) *Breaker {
	return &Breaker{
		maxFailures:      maxFailures,
		timeout:          timeout,
		successThreshold: successThreshold,
	}
}

// Execute calls fn if the circuit allows it and records the outcome.
// Returns ErrOpen immediately if the circuit is open and timeout has not elapsed.
func (b *Breaker) Execute(fn func() error) error {
	if err := b.allow(); err != nil {
		return err
	}
	err := fn()
	b.record(err)
	return err
}

// State returns a human-readable state label. Useful for metrics and health endpoints.
func (b *Breaker) State() string {
	switch state(b.state.Load()) {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func (b *Breaker) allow() error {
	switch state(b.state.Load()) {
	case stateClosed:
		return nil

	case stateOpen:
		since := time.Duration(time.Now().UnixNano() - b.lastFailedAt.Load())
		if since < b.timeout {
			return ErrOpen
		}
		// Attempt transition to HalfOpen; only one goroutine succeeds.
		if b.state.CompareAndSwap(int32(stateOpen), int32(stateHalfOpen)) {
			b.successes.Store(0)
		}
		return nil

	default: // HalfOpen
		return nil
	}
}

func (b *Breaker) record(err error) {
	if err != nil {
		b.failures.Add(1)
		b.lastFailedAt.Store(time.Now().UnixNano())

		cur := state(b.state.Load())
		if cur == stateHalfOpen {
			b.state.Store(int32(stateOpen))
			return
		}
		if b.failures.Load() >= b.maxFailures {
			b.state.Store(int32(stateOpen))
		}
		return
	}

	// success path
	switch state(b.state.Load()) {
	case stateHalfOpen:
		if b.successes.Add(1) >= b.successThreshold {
			b.state.Store(int32(stateClosed))
			b.failures.Store(0)
		}
	case stateClosed:
		b.failures.Store(0)
	}
}
