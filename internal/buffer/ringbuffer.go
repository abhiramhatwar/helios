package buffer

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	ErrFull            = errors.New("buffer is full")
	ErrEmpty           = errors.New("buffer is empty")
	ErrInvalidCapacity = errors.New("capacity must be a power of 2 greater than zero")
)

type slot[T any] struct {
	sequence atomic.Uint64
	value    T
}

// RingBuffer is a lock-free, bounded MPMC (multi-producer, multi-consumer) ring buffer.
//
// Uses Dmitry Vyukov's sequence-based algorithm:
// https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
//
// Each slot carries a sequence number that acts as a rendezvous between
// producers and consumers. The padding fields reduce false sharing between
// the producer and consumer cursors on separate cache lines.
type RingBuffer[T any] struct {
	_          [64]byte       // push enqueuePos off the zero-offset (avoid sharing with slice header)
	enqueuePos atomic.Uint64  // producer cursor
	_          [56]byte       // 64 - sizeof(atomic.Uint64) = 56
	dequeuePos atomic.Uint64  // consumer cursor
	_          [56]byte
	mask       uint64
	slots      []slot[T]
}

// New creates a RingBuffer with the given capacity. Capacity must be a power of 2.
func New[T any](capacity uint64) (*RingBuffer[T], error) {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		return nil, ErrInvalidCapacity
	}
	rb := &RingBuffer[T]{
		mask:  capacity - 1,
		slots: make([]slot[T], capacity),
	}
	for i := uint64(0); i < capacity; i++ {
		rb.slots[i].sequence.Store(i)
	}
	return rb, nil
}

// Enqueue adds value to the buffer. Returns ErrFull if the buffer is at capacity.
// Safe for concurrent use by multiple producers.
func (rb *RingBuffer[T]) Enqueue(value T) error {
	for {
		pos := rb.enqueuePos.Load()
		s := &rb.slots[pos&rb.mask]
		seq := s.sequence.Load()
		diff := int64(seq) - int64(pos)

		switch {
		case diff == 0:
			// Slot is free. Race to claim it.
			if rb.enqueuePos.CompareAndSwap(pos, pos+1) {
				s.value = value
				// Publishing: advance sequence signals this slot is ready for consumers.
				s.sequence.Store(pos + 1)
				return nil
			}
		case diff < 0:
			// Sequence behind pos means the buffer has wrapped and the slot
			// is not yet freed by its consumer → buffer is full.
			return ErrFull
		default:
			// Another producer is ahead; spin.
			runtime.Gosched()
		}
	}
}

// Dequeue removes and returns a value. Returns ErrEmpty if the buffer has no items.
// Safe for concurrent use by multiple consumers.
func (rb *RingBuffer[T]) Dequeue() (T, error) {
	for {
		pos := rb.dequeuePos.Load()
		s := &rb.slots[pos&rb.mask]
		seq := s.sequence.Load()
		diff := int64(seq) - int64(pos+1)

		switch {
		case diff == 0:
			// Slot is ready. Race to consume it.
			if rb.dequeuePos.CompareAndSwap(pos, pos+1) {
				value := s.value
				// Release: advance sequence to pos+cap, marking the slot free for the
				// next producer that wraps around to it.
				s.sequence.Store(pos + rb.mask + 1)
				return value, nil
			}
		case diff < 0:
			// Producer has not published yet → buffer is empty.
			var zero T
			return zero, ErrEmpty
		default:
			// Another consumer is ahead; spin.
			runtime.Gosched()
		}
	}
}

// Len returns an approximate count of items currently in the buffer.
func (rb *RingBuffer[T]) Len() int64 {
	enq := int64(rb.enqueuePos.Load())
	deq := int64(rb.dequeuePos.Load())
	if n := enq - deq; n > 0 {
		return n
	}
	return 0
}

// Cap returns the total capacity of the buffer.
func (rb *RingBuffer[T]) Cap() uint64 {
	return rb.mask + 1
}
