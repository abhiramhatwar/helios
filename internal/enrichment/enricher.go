package enrichment

import (
	"context"

	"github.com/helios/pkg/event"
)

type Enricher interface {
	Enrich(ctx context.Context, ev event.Event) (event.EnrichedEvent, error)
}
