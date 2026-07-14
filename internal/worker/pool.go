package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/helios/internal/buffer"
	"github.com/helios/pkg/event"
	"github.com/rs/zerolog"
)

// ProcessFunc is the signature every event processor must satisfy.
type ProcessFunc func(ctx context.Context, ev event.Event) error

// Pool runs a fixed set of goroutines that drain a RingBuffer and dispatch
// events to a ProcessFunc. Backpressure is enforced via a semaphore: at most
// maxConcurrent ProcessFunc calls run at the same time regardless of how many
// workers are pulling from the buffer.
type Pool struct {
	rb            *buffer.RingBuffer[event.Event]
	processFn     ProcessFunc
	workerCount   int
	semaphore     chan struct{}
	wg            sync.WaitGroup
	log           zerolog.Logger
}

// New creates a Pool.
//   - workerCount: goroutines pulling from the ring buffer
//   - maxConcurrent: cap on simultaneous ProcessFunc executions (backpressure knob)
func New(
	rb *buffer.RingBuffer[event.Event],
	workerCount, maxConcurrent int,
	fn ProcessFunc,
	log zerolog.Logger,
) *Pool {
	return &Pool{
		rb:          rb,
		processFn:   fn,
		workerCount: workerCount,
		semaphore:   make(chan struct{}, maxConcurrent),
		log:         log,
	}
}

// Start launches all worker goroutines. Returns immediately; workers run until
// ctx is cancelled, then drain remaining buffered events before exiting.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.run(ctx, i)
	}
}

// Wait blocks until all workers have fully exited (after ctx cancellation).
func (p *Pool) Wait() {
	p.wg.Wait()
}

const (
	backoffMin = time.Microsecond
	backoffMax = 2 * time.Millisecond
)

func (p *Pool) run(ctx context.Context, id int) {
	defer p.wg.Done()
	backoff := backoffMin

	for {
		select {
		case <-ctx.Done():
			p.drain()
			return
		default:
		}

		ev, err := p.rb.Dequeue()
		if err != nil {
			if errors.Is(err, buffer.ErrEmpty) {
				time.Sleep(backoff)
				if backoff < backoffMax {
					backoff *= 2
				}
				continue
			}
			p.log.Error().Err(err).Int("worker", id).Msg("dequeue error")
			continue
		}

		backoff = backoffMin // reset on success

		// Acquire semaphore before spawning; blocks if maxConcurrent is reached.
		p.semaphore <- struct{}{}
		go func(e event.Event) {
			defer func() { <-p.semaphore }()
			if err := p.processFn(ctx, e); err != nil {
				p.log.Error().Err(err).Str("event_id", e.ID).Msg("process error")
			}
		}(ev)
	}
}

// drain flushes any events remaining in the buffer after shutdown is signalled.
// Uses a background context so in-flight events are not abandoned mid-write.
func (p *Pool) drain() {
	p.log.Info().Msg("draining buffer before shutdown")
	drainCtx := context.Background()
	for {
		ev, err := p.rb.Dequeue()
		if errors.Is(err, buffer.ErrEmpty) {
			return
		}
		if err != nil {
			return
		}
		if err := p.processFn(drainCtx, ev); err != nil {
			p.log.Error().Err(err).Str("event_id", ev.ID).Msg("drain error")
		}
	}
}
