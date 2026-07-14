package middleware

import (
	"net/http"
	"strings"
)

// exempt paths skip authentication entirely.
var exemptPaths = map[string]bool{
	"/health":  true,
	"/metrics": true,
	"/":        true,
	"/ws":      true, // WebSocket — auth handled by the client via query param or subprotocol
}

// APIKeyAuth returns a middleware that requires a valid API key on all
// non-exempt routes. Accepts the key via:
//
//	Authorization: Bearer <key>
//	X-API-Key: <key>
func APIKeyAuth(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Pass-through when no key is configured (dev mode).
			if apiKey == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Exempt paths skip auth.
			path := r.URL.Path
			if exemptPaths[path] || strings.HasPrefix(path, "/app") {
				next.ServeHTTP(w, r)
				return
			}

			key := extractKey(r)
			if key == "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="helios"`)
				http.Error(w, `{"error":"missing API key"}`, http.StatusUnauthorized)
				return
			}
			if key != apiKey {
				http.Error(w, `{"error":"invalid API key"}`, http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func extractKey(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return r.Header.Get("X-API-Key")
}
