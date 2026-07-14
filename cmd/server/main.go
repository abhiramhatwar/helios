package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/helios/config"
	"github.com/helios/internal/alert"
	"github.com/helios/internal/api"
	"github.com/helios/internal/buffer"
	"github.com/helios/internal/circuit"
	"github.com/helios/internal/detector"
	"github.com/helios/internal/enrichment"
	"github.com/helios/internal/metrics"
	"github.com/helios/internal/storage"
	"github.com/helios/internal/worker"
	"github.com/helios/pkg/event"
	"github.com/rs/zerolog"
)

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("config load failed")
	}

	metrics.Register()

	rb, err := buffer.New[event.Event](cfg.Buffer.Capacity)
	if err != nil {
		log.Fatal().Err(err).Msg("ring buffer init failed")
	}
	log.Info().Uint64("capacity", cfg.Buffer.Capacity).Msg("ring buffer initialised")

	// Background context for long-lived clients (Gemini, Postgres, Redis).
	bgCtx := context.Background()

	// Circuit breaker for Gemini API: open after 5 failures, recover after 30s,
	// require 2 consecutive successes to close.
	aiBreaker := circuit.New(5, 30*time.Second, 2)

	// AI enrichment layer — gracefully degrades if Gemini is unreachable.
	var enricher enrichment.Enricher
	if cfg.Gemini.APIKey == "" {
		log.Warn().Msg("GEMINI_API_KEY not set — AI enrichment disabled, using passthrough")
		enricher = &enrichment.Passthrough{}
	} else {
		ge, err := enrichment.NewGemini(bgCtx, cfg.Gemini.APIKey, aiBreaker, log)
		if err != nil {
			log.Fatal().Err(err).Msg("gemini enricher init failed")
		}
		defer ge.Close()
		enricher = ge
		log.Info().Msg("Gemini 2.0 Flash enricher ready")
	}

	// Anomaly detector: 60 buckets × 10s = 10 min window, z-score threshold 2.0.
	anom := detector.New(60, 10*time.Second, 2.0)
	defer anom.Stop()

	// PostgreSQL store.
	store, err := storage.NewPostgres(bgCtx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres init failed")
	}
	defer store.Close()
	if err := store.Migrate(bgCtx); err != nil {
		log.Fatal().Err(err).Msg("postgres migration failed")
	}
	log.Info().Msg("postgres ready")

	// Redis alert publisher.
	pub, err := alert.New(cfg.Redis.URL, log)
	if err != nil {
		log.Fatal().Err(err).Msg("redis init failed")
	}
	defer pub.Close()
	if err := pub.Ping(bgCtx); err != nil {
		log.Fatal().Err(err).Msg("redis ping failed")
	}
	log.Info().Msg("redis ready")

	// Full event processing pipeline.
	processEvent := func(ctx context.Context, ev event.Event) error {
		metrics.EventsIngested.WithLabelValues(string(ev.Source), string(ev.Level)).Inc()

		// Record in anomaly detector (rate-based spike detection).
		rateAnomaly := anom.Record(string(ev.Source))

		// AI enrichment (classification + summary + anomaly score).
		start := time.Now()
		enriched, err := enricher.Enrich(ctx, ev)
		if err != nil {
			return fmt.Errorf("enrich: %w", err)
		}
		metrics.EnrichmentDuration.Observe(time.Since(start).Seconds())
		metrics.EventsEnriched.WithLabelValues(enriched.Classification).Inc()

		// Merge rate-anomaly signal into AI anomaly score.
		if rateAnomaly && !enriched.IsAnomaly {
			enriched.IsAnomaly = true
			enriched.AnomalyScore = max(enriched.AnomalyScore, 0.75)
		}

		if enriched.IsAnomaly {
			metrics.AnomaliesDetected.WithLabelValues(string(ev.Source)).Inc()
			log.Warn().
				Str("id", ev.ID).
				Str("source", string(ev.Source)).
				Str("classification", enriched.Classification).
				Float64("score", enriched.AnomalyScore).
				Str("summary", enriched.Summary).
				Msg("anomaly detected")
		}

		// Update circuit breaker metric.
		if aiBreaker.State() == "open" {
			metrics.CircuitBreakerOpen.Set(1)
		} else {
			metrics.CircuitBreakerOpen.Set(0)
		}

		// Persist to PostgreSQL.
		if err := store.Save(ctx, enriched); err != nil {
			log.Error().Err(err).Str("event_id", ev.ID).Msg("store save failed")
		}

		// Cache in Redis and publish alert if anomalous.
		if err := pub.Cache(ctx, enriched); err != nil {
			log.Error().Err(err).Str("event_id", ev.ID).Msg("redis cache failed")
		}
		if enriched.IsAnomaly {
			if err := pub.PublishAlert(ctx, enriched); err != nil {
				log.Error().Err(err).Str("event_id", ev.ID).Msg("alert publish failed")
			}
		}

		return nil
	}

	pool := worker.New(rb, cfg.Worker.Count, cfg.Worker.MaxConcurrent, processEvent, log)

	httpSrv := api.New(rb, log)
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      httpSrv.Handler(),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSecs) * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutdown signal received")
		cancel()
	}()

	pool.Start(ctx)
	log.Info().Int("workers", cfg.Worker.Count).Int("max_concurrent", cfg.Worker.MaxConcurrent).Msg("worker pool started")

	go func() {
		log.Info().Str("addr", srv.Addr).Msg("HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}

	pool.Wait()
	log.Info().Msg("helios shutdown complete")
}

