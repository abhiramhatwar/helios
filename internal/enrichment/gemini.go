package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/generative-ai-go/genai"
	"github.com/helios/internal/circuit"
	"github.com/helios/pkg/event"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

type geminiResponse struct {
	Classification string  `json:"classification"`
	Summary        string  `json:"summary"`
	AnomalyScore   float64 `json:"anomaly_score"`
	IsAnomaly      bool    `json:"is_anomaly"`
}

type GeminiEnricher struct {
	client  *genai.Client
	model   *genai.GenerativeModel
	breaker *circuit.Breaker
	log     zerolog.Logger
}

func NewGemini(ctx context.Context, apiKey string, breaker *circuit.Breaker, log zerolog.Logger) (*GeminiEnricher, error) {
	client, err := genai.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		return nil, fmt.Errorf("gemini client: %w", err)
	}
	model := client.GenerativeModel("gemini-2.0-flash")
	model.ResponseMIMEType = "application/json"
	return &GeminiEnricher{client: client, model: model, breaker: breaker, log: log}, nil
}

func (g *GeminiEnricher) Enrich(ctx context.Context, ev event.Event) (event.EnrichedEvent, error) {
	enriched := event.EnrichedEvent{Event: ev, ProcessedAt: time.Now()}

	prompt := fmt.Sprintf(
		`Analyze this system event and classify it. Respond with valid JSON only, no markdown.

Source: %s
Level: %s
Message: %s

{
  "classification": "<infrastructure|security|business|performance|unknown>",
  "summary": "<one sentence: what happened and why it matters>",
  "anomaly_score": <float 0.0-1.0, 1.0 = definitely anomalous>,
  "is_anomaly": <true if anomaly_score > 0.7>
}`,
		ev.Source, ev.Level, ev.Message,
	)

	err := g.breaker.Execute(func() error {
		resp, err := g.model.GenerateContent(ctx, genai.Text(prompt))
		if err != nil {
			return err
		}
		if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
			return fmt.Errorf("empty gemini response")
		}
		raw, ok := resp.Candidates[0].Content.Parts[0].(genai.Text)
		if !ok {
			return fmt.Errorf("unexpected gemini response type")
		}
		var gr geminiResponse
		if err := json.Unmarshal([]byte(raw), &gr); err != nil {
			return fmt.Errorf("parse gemini response: %w", err)
		}
		enriched.Classification = gr.Classification
		enriched.Summary = gr.Summary
		enriched.AnomalyScore = gr.AnomalyScore
		enriched.IsAnomaly = gr.IsAnomaly
		return nil
	})

	if err != nil {
		g.log.Warn().Err(err).Str("event_id", ev.ID).Msg("AI enrichment failed, using defaults")
		enriched.Classification = "unknown"
		enriched.Summary = ev.Message
		enriched.AnomalyScore = 0
		enriched.IsAnomaly = false
	}

	return enriched, nil
}

func (g *GeminiEnricher) Close() error {
	return g.client.Close()
}
