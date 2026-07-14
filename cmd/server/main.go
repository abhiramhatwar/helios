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
	"github.com/helios/internal/api"
	"github.com/helios/internal/buffer"
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

	rb, err := buffer.New[event.Event](cfg.Buffer.Capacity)
	if err != nil {
		log.Fatal().Err(err).Msg("ring buffer init failed")
	}
	log.Info().
		Uint64("capacity", cfg.Buffer.Capacity).
		Msg("ring buffer initialised")

	// processEvent is the core pipeline step. Days 2–7 will replace this
	// stub with AI enrichment, anomaly detection, and alert dispatch.
	processEvent := func(ctx context.Context, ev event.Event) error {
		log.Debug().
			Str("id", ev.ID).
			Str("level", string(ev.Level)).
			Str("source", string(ev.Source)).
			Str("message", ev.Message).
			Msg("event received")
		return nil
	}

	pool := worker.New(
		rb,
		cfg.Worker.Count,
		cfg.Worker.MaxConcurrent,
		processEvent,
		log,
	)

	httpSrv := api.New(rb, log)
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      httpSrv.Handler(),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSecs) * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutdown signal received")
		cancel()
	}()

	pool.Start(ctx)
	log.Info().
		Int("workers", cfg.Worker.Count).
		Int("max_concurrent", cfg.Worker.MaxConcurrent).
		Msg("worker pool started")

	go func() {
		log.Info().Str("addr", srv.Addr).Msg("HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	<-ctx.Done()

	// Graceful HTTP shutdown (give in-flight requests 15 s to complete)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}

	// Drain ring buffer
	pool.Wait()
	log.Info().Msg("helios shutdown complete")
}
