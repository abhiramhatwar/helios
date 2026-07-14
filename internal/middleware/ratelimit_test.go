package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestRateLimiter_AllowsRequestsWithinLimit(t *testing.T) {
	rl := NewRateLimiter(10, 5) // 10 rps, burst 5
	handler := rl.Limit(http.HandlerFunc(okHandler))

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		if rw.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i+1, rw.Code)
		}
	}
}

func TestRateLimiter_BlocksWhenBurstExceeded(t *testing.T) {
	rl := NewRateLimiter(1, 3) // 1 rps, burst 3
	handler := rl.Limit(http.HandlerFunc(okHandler))

	var blocked int
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "10.0.0.1:9999"
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		if rw.Code == http.StatusTooManyRequests {
			blocked++
		}
	}
	if blocked == 0 {
		t.Fatal("expected at least one 429 after burst exhausted")
	}
}

func TestRateLimiter_DifferentIPsAreSeparate(t *testing.T) {
	rl := NewRateLimiter(1, 2) // burst 2
	handler := rl.Limit(http.HandlerFunc(okHandler))

	send := func(ip string) int {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = ip + ":1234"
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		return rw.Code
	}

	// Exhaust IP A's burst.
	send("1.1.1.1")
	send("1.1.1.1")
	code := send("1.1.1.1")
	if code != http.StatusTooManyRequests {
		t.Fatalf("IP A should be rate limited, got %d", code)
	}

	// IP B's bucket is independent — should still be OK.
	if code := send("2.2.2.2"); code != http.StatusOK {
		t.Fatalf("IP B should not be rate limited, got %d", code)
	}
}

func TestAPIKeyAuth_PassthroughWhenNoKey(t *testing.T) {
	handler := APIKeyAuth("")(http.HandlerFunc(okHandler))
	req := httptest.NewRequest("GET", "/api/v1/events", nil)
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 with no API key configured, got %d", rw.Code)
	}
}

func TestAPIKeyAuth_RejectsWrongKey(t *testing.T) {
	handler := APIKeyAuth("secret")(http.HandlerFunc(okHandler))
	req := httptest.NewRequest("GET", "/api/v1/events", nil)
	req.Header.Set("X-API-Key", "wrong")
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for wrong key, got %d", rw.Code)
	}
}

func TestAPIKeyAuth_AcceptsCorrectBearerToken(t *testing.T) {
	handler := APIKeyAuth("mytoken")(http.HandlerFunc(okHandler))
	req := httptest.NewRequest("GET", "/api/v1/events", nil)
	req.Header.Set("Authorization", "Bearer mytoken")
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct Bearer token, got %d", rw.Code)
	}
}

func TestAPIKeyAuth_ExemptsHealthRoute(t *testing.T) {
	handler := APIKeyAuth("secret")(http.HandlerFunc(okHandler))
	req := httptest.NewRequest("GET", "/health", nil)
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("/health should be exempt, got %d", rw.Code)
	}
}
