package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	EventsIngested = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "helios_events_ingested_total", Help: "Total events ingested"},
		[]string{"source", "level"},
	)
	EventsEnriched = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "helios_events_enriched_total", Help: "Total events enriched by AI"},
		[]string{"classification"},
	)
	AnomaliesDetected = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "helios_anomalies_detected_total", Help: "Total anomalies detected"},
		[]string{"source"},
	)
	EnrichmentDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "helios_enrichment_duration_seconds",
		Help:    "Latency of AI enrichment calls",
		Buckets: prometheus.DefBuckets,
	})
	CircuitBreakerOpen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "helios_circuit_breaker_open",
		Help: "1 if the AI enrichment circuit breaker is open",
	})
)

func Register() {
	prometheus.MustRegister(
		EventsIngested,
		EventsEnriched,
		AnomaliesDetected,
		EnrichmentDuration,
		CircuitBreakerOpen,
	)
}
