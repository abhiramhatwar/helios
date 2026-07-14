package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/helios/internal/buffer"
	"github.com/helios/internal/middleware"
	"github.com/helios/internal/ws"
	"github.com/helios/pkg/event"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Server is the HTTP layer. It owns the mux and injects the ring buffer
// so handlers can enqueue events without touching any other pipeline detail.
type Server struct {
	rb          *buffer.RingBuffer[event.Event]
	hub         *ws.Hub
	log         zerolog.Logger
	mux         *http.ServeMux
	rateLimiter *middleware.RateLimiter
	apiKey      string
}

func New(rb *buffer.RingBuffer[event.Event], hub *ws.Hub, log zerolog.Logger, rps float64, burst int, apiKey string) *Server {
	s := &Server{
		rb:          rb,
		hub:         hub,
		log:         log,
		mux:         http.NewServeMux(),
		rateLimiter: middleware.NewRateLimiter(rps, burst),
		apiKey:      apiKey,
	}
	s.routes()
	return s
}

// Handler returns the root handler, chained: logging → rate limit → auth → mux.
func (s *Server) Handler() http.Handler {
	chain := middleware.APIKeyAuth(s.apiKey)(s.mux)
	chain = s.rateLimiter.Limit(chain)
	return s.logMiddleware(chain)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /api/v1/events", s.ingestOne)
	s.mux.HandleFunc("POST /api/v1/events/batch", s.ingestBatch)
	s.mux.HandleFunc("GET /api/v1/status", s.status)
	s.mux.HandleFunc("GET /health", s.health)
	s.mux.Handle("GET /metrics", promhttp.Handler())
	s.mux.HandleFunc("GET /ws", s.handleWS)
	// Dashboard — serve web/ directory at root.
	s.mux.Handle("/", http.FileServer(http.Dir("web")))
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	ws.ServeWS(s.hub, w, r, s.log)
}

// --- handlers ---

type ingestRequest struct {
	Source  event.Source   `json:"source"`
	Level   event.Level    `json:"level"`
	Message string         `json:"message"`
	Payload map[string]any `json:"payload,omitempty"`
	Tags    []string       `json:"tags,omitempty"`
}

func (s *Server) ingestOne(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errJSON(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Message == "" {
		s.errJSON(w, http.StatusUnprocessableEntity, "message is required")
		return
	}

	ev := newEvent(req)
	if err := s.rb.Enqueue(ev); err != nil {
		if errors.Is(err, buffer.ErrFull) {
			s.errJSON(w, http.StatusServiceUnavailable, "buffer full — apply backpressure")
			return
		}
		s.log.Error().Err(err).Msg("enqueue failed")
		s.errJSON(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.json(w, http.StatusAccepted, map[string]string{"id": ev.ID, "status": "queued"})
}

func (s *Server) ingestBatch(w http.ResponseWriter, r *http.Request) {
	var reqs []ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		s.errJSON(w, http.StatusBadRequest, "invalid JSON array")
		return
	}
	if len(reqs) == 0 {
		s.errJSON(w, http.StatusUnprocessableEntity, "batch must not be empty")
		return
	}

	queued := make([]string, 0, len(reqs))
	for _, req := range reqs {
		if req.Message == "" {
			continue
		}
		ev := newEvent(req)
		if err := s.rb.Enqueue(ev); err != nil {
			if errors.Is(err, buffer.ErrFull) {
				s.errJSON(w, http.StatusServiceUnavailable,
					fmt.Sprintf("buffer full after %d events", len(queued)))
				return
			}
			s.log.Error().Err(err).Msg("batch enqueue failed")
			s.errJSON(w, http.StatusInternalServerError, "internal error")
			return
		}
		queued = append(queued, ev.ID)
	}

	s.json(w, http.StatusAccepted, map[string]any{
		"queued": len(queued),
		"ids":    queued,
	})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	cap := s.rb.Cap()
	length := s.rb.Len()
	s.json(w, http.StatusOK, map[string]any{
		"buffer_len":   length,
		"buffer_cap":   cap,
		"buffer_usage": fmt.Sprintf("%.1f%%", float64(length)/float64(cap)*100),
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	s.json(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- middleware ---

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", rw.status).
			Dur("duration_ms", time.Since(start)).
			Msg("request")
	})
}

// --- helpers ---

func newEvent(req ingestRequest) event.Event {
	return event.Event{
		ID:        uuid.New().String(),
		Source:    req.Source,
		Level:     req.Level,
		Timestamp: time.Now().UTC(),
		Message:   req.Message,
		Payload:   req.Payload,
		Tags:      req.Tags,
	}
}

func (s *Server) json(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) errJSON(w http.ResponseWriter, status int, msg string) {
	s.json(w, status, map[string]string{"error": msg})
}
