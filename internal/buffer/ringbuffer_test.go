package buffer

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRingBuffer_EnqueueDequeue(t *testing.T) {
	rb, err := New[int](8)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		if err := rb.Enqueue(i); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}

	if err := rb.Enqueue(99); err != ErrFull {
		t.Fatalf("expected ErrFull, got %v", err)
	}

	for i := 0; i < 8; i++ {
		v, err := rb.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(): %v", err)
		}
		if v != i {
			t.Fatalf("expected %d, got %d", i, v)
		}
	}

	if _, err := rb.Dequeue(); err != ErrEmpty {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestRingBuffer_InvalidCapacity(t *testing.T) {
	if _, err := New[int](0); err != ErrInvalidCapacity {
		t.Fatal("expected ErrInvalidCapacity for capacity 0")
	}
	if _, err := New[int](3); err != ErrInvalidCapacity {
		t.Fatal("expected ErrInvalidCapacity for capacity 3")
	}
}

func TestRingBuffer_ConcurrentMPMC(t *testing.T) {
	const (
		capacity  = 1024
		producers = 4
		consumers = 4
		perProd   = 10_000
	)

	rb, _ := New[int](capacity)
	total := int64(producers * perProd)

	var received atomic.Int64
	var wgCons sync.WaitGroup

	// Consumers run until they've collectively received every item.
	for c := 0; c < consumers; c++ {
		wgCons.Add(1)
		go func() {
			defer wgCons.Done()
			for received.Load() < total {
				_, err := rb.Dequeue()
				if err == nil {
					received.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		}()
	}

	var wgProd sync.WaitGroup
	for p := 0; p < producers; p++ {
		wgProd.Add(1)
		go func(base int) {
			defer wgProd.Done()
			for i := 0; i < perProd; i++ {
				for rb.Enqueue(base*perProd+i) == ErrFull {
					runtime.Gosched()
				}
			}
		}(p)
	}

	wgProd.Wait()
	wgCons.Wait()

	if n := received.Load(); n != total {
		t.Fatalf("expected %d items, got %d", total, n)
	}
}

func BenchmarkRingBuffer_Enqueue(b *testing.B) {
	rb, _ := New[int](1 << 16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Enqueue(i)
			rb.Dequeue()
			i++
		}
	})
}
