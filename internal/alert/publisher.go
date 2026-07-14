package alert

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/helios/pkg/event"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const (
	alertChannel = "helios:alerts"
	eventTTL     = 24 * time.Hour
)

type Publisher struct {
	client *redis.Client
	log    zerolog.Logger
}

func New(url string, log zerolog.Logger) (*Publisher, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis url: %w", err)
	}
	return &Publisher{client: redis.NewClient(opts), log: log}, nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	return p.client.Ping(ctx).Err()
}

// Cache stores an enriched event in Redis for fast dashboard reads.
func (p *Publisher) Cache(ctx context.Context, ev event.EnrichedEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.client.Set(ctx, "helios:event:"+ev.ID, b, eventTTL).Err()
}

// PublishAlert broadcasts an anomalous event to all WebSocket subscribers.
func (p *Publisher) PublishAlert(ctx context.Context, ev event.EnrichedEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.client.Publish(ctx, alertChannel, string(b)).Err()
}

// Client returns the underlying Redis client for use by gRPC server and WS hub.
func (p *Publisher) Client() *redis.Client {
	return p.client
}

func (p *Publisher) Close() error {
	return p.client.Close()
}
