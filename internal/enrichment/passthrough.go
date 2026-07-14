package enrichment

import (
	"context"
	"time"

	"github.com/helios/pkg/event"
)

// Passthrough is a no-op enricher used when no AI API key is configured.
// It returns the event unchanged with a classification of "unknown".
type Passthrough struct{}

func (p *Passthrough) Enrich(_ context.Context, ev event.Event) (event.EnrichedEvent, error) {
	return event.EnrichedEvent{
		Event:          ev,
		Classification: "unknown",
		Summary:        ev.Message,
		AnomalyScore:   0,
		IsAnomaly:      false,
		ProcessedAt:    time.Now(),
	}, nil
}
