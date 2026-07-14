package storage

import (
	"context"

	"github.com/helios/pkg/event"
)

type Store interface {
	Save(ctx context.Context, ev event.EnrichedEvent) error
	Close() error
}
