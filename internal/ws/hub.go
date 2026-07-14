package ws

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const alertChannel = "helios:alerts"

// Hub manages all active WebSocket connections and broadcasts Redis pub/sub
// messages to every connected client.
type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]struct{}
	redis   *redis.Client
	log     zerolog.Logger
}

func NewHub(redisClient *redis.Client, log zerolog.Logger) *Hub {
	return &Hub{
		clients: make(map[*Client]struct{}),
		redis:   redisClient,
		log:     log,
	}
}

func (h *Hub) register(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	h.log.Debug().Int("total", len(h.clients)).Msg("WS client connected")
}

func (h *Hub) unregister(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	h.log.Debug().Int("total", len(h.clients)).Msg("WS client disconnected")
}

func (h *Hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			// Slow client — drop rather than block.
			h.log.Warn().Msg("WS: slow client, dropping message")
		}
	}
}

// Run subscribes to the Redis alert channel and broadcasts every message
// to all connected WebSocket clients. Blocks until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) {
	sub := h.redis.Subscribe(ctx, alertChannel)
	defer sub.Close()

	h.log.Info().Msg("WebSocket hub running, listening for alerts")

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			h.broadcast([]byte(msg.Payload))
		}
	}
}
