package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// bucket is a token bucket for a single IP address.
type bucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64 // tokens per second
	lastSeen time.Time
}

func newBucket(rate, capacity float64) *bucket {
	return &bucket{
		tokens:   capacity,
		capacity: capacity,
		rate:     rate,
		lastSeen: time.Now(),
	}
}

func (b *bucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.lastSeen = now

	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimiter applies a per-IP token bucket to every request.
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens per second
	capacity float64 // burst capacity
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     rps,
		capacity: float64(burst),
	}
	go rl.cleanup()
	return rl
}

// cleanup removes buckets idle for more than 5 minutes to prevent unbounded growth.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)
		rl.mu.Lock()
		for ip, b := range rl.buckets {
			b.mu.Lock()
			if b.lastSeen.Before(cutoff) {
				delete(rl.buckets, ip)
			}
			b.mu.Unlock()
		}
		rl.mu.Unlock()
	}
}

func (rl *RateLimiter) getBucket(ip string) *bucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.buckets[ip]
	if !ok {
		b = newBucket(rl.rate, rl.capacity)
		rl.buckets[ip] = b
	}
	return b
}

// Limit returns a middleware that enforces the rate limit.
func (rl *RateLimiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		// Prefer X-Forwarded-For when behind a proxy.
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			ip = fwd
		}

		if !rl.getBucket(ip).allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
