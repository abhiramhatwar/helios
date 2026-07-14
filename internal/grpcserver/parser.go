package grpcserver

import (
	"encoding/json"
	"fmt"

	heliosv1 "github.com/helios/proto/gen/proto"
	"github.com/helios/pkg/event"
)

func parseAlert(payload string) (*heliosv1.AlertEvent, error) {
	var ev event.EnrichedEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return nil, fmt.Errorf("unmarshal alert: %w", err)
	}
	return &heliosv1.AlertEvent{
		Id:             ev.ID,
		Source:         string(ev.Source),
		Level:          string(ev.Level),
		Message:        ev.Message,
		Classification: ev.Classification,
		Summary:        ev.Summary,
		AnomalyScore:   ev.AnomalyScore,
		IsAnomaly:      ev.IsAnomaly,
		TimestampUnix:  ev.Timestamp.UnixNano(),
		ProcessedUnix:  ev.ProcessedAt.UnixNano(),
	}, nil
}
