package circuit

import (
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func TestBreaker_StartsClosedAndAllowsCalls(t *testing.T) {
	b := New(3, time.Second, 2)
	if b.State() != "closed" {
		t.Fatalf("expected closed, got %s", b.State())
	}
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatalf("unexpected error on healthy call: %v", err)
	}
}

func TestBreaker_OpensAfterMaxFailures(t *testing.T) {
	b := New(3, time.Hour, 2)
	for i := 0; i < 3; i++ {
		_ = b.Execute(func() error { return errBoom })
	}
	if b.State() != "open" {
		t.Fatalf("expected open after 3 failures, got %s", b.State())
	}
	if err := b.Execute(func() error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
}

func TestBreaker_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	b := New(1, 20*time.Millisecond, 1)
	_ = b.Execute(func() error { return errBoom })
	if b.State() != "open" {
		t.Fatalf("expected open, got %s", b.State())
	}

	time.Sleep(30 * time.Millisecond)

	// First call after timeout should go through (HalfOpen probe).
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatalf("expected probe call to succeed, got %v", err)
	}
	if b.State() != "closed" {
		t.Fatalf("expected closed after successful probe, got %s", b.State())
	}
}

func TestBreaker_HalfOpenReturnsToOpenOnFailure(t *testing.T) {
	b := New(1, 20*time.Millisecond, 2)
	_ = b.Execute(func() error { return errBoom })

	time.Sleep(30 * time.Millisecond)

	// Probe fails — should snap back to Open immediately.
	_ = b.Execute(func() error { return errBoom })
	if b.State() != "open" {
		t.Fatalf("expected open after half-open failure, got %s", b.State())
	}
}

func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	b := New(3, time.Hour, 1)
	_ = b.Execute(func() error { return errBoom })
	_ = b.Execute(func() error { return errBoom })
	_ = b.Execute(func() error { return nil }) // success resets counter
	if b.State() != "closed" {
		t.Fatalf("expected closed after success, got %s", b.State())
	}
	// 2 more failures should not open (counter was reset).
	_ = b.Execute(func() error { return errBoom })
	_ = b.Execute(func() error { return errBoom })
	if b.State() != "closed" {
		t.Fatalf("expected still closed (only 2 failures since reset), got %s", b.State())
	}
}
