package event

import "time"

type Level string
type Source string

const (
	LevelInfo     Level = "info"
	LevelWarning  Level = "warning"
	LevelError    Level = "error"
	LevelCritical Level = "critical"
)

const (
	SourceHTTP    Source = "http"
	SourceGRPC    Source = "grpc"
	SourceFile    Source = "file"
	SourceMetrics Source = "metrics"
)

// Event is the atomic unit of data flowing through the Helios pipeline.
type Event struct {
	ID        string         `json:"id"`
	Source    Source         `json:"source"`
	Level     Level          `json:"level"`
	Timestamp time.Time      `json:"timestamp"`
	Message   string         `json:"message"`
	Payload   map[string]any `json:"payload,omitempty"`
	Tags      []string       `json:"tags,omitempty"`
}

// EnrichedEvent is produced by the AI enrichment layer.
type EnrichedEvent struct {
	Event
	AnomalyScore   float64   `json:"anomaly_score"`
	Classification string    `json:"classification"`
	Summary        string    `json:"summary"`
	IsAnomaly      bool      `json:"is_anomaly"`
	ProcessedAt    time.Time `json:"processed_at"`
}

// Result is a generic async operation outcome.
type Result[T any] struct {
	Value T
	Err   error
}
