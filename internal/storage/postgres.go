package storage

import (
	"context"
	"fmt"

	"github.com/helios/pkg/event"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS enriched_events (
			id             TEXT PRIMARY KEY,
			source         TEXT NOT NULL,
			level          TEXT NOT NULL,
			message        TEXT NOT NULL,
			classification TEXT NOT NULL DEFAULT 'unknown',
			summary        TEXT NOT NULL DEFAULT '',
			anomaly_score  DOUBLE PRECISION NOT NULL DEFAULT 0,
			is_anomaly     BOOLEAN NOT NULL DEFAULT false,
			timestamp      TIMESTAMPTZ NOT NULL,
			processed_at   TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_ee_source     ON enriched_events(source);
		CREATE INDEX IF NOT EXISTS idx_ee_is_anomaly ON enriched_events(is_anomaly);
		CREATE INDEX IF NOT EXISTS idx_ee_timestamp  ON enriched_events(timestamp DESC);
	`)
	return err
}

func (s *PostgresStore) Save(ctx context.Context, ev event.EnrichedEvent) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO enriched_events
			(id, source, level, message, classification, summary, anomaly_score, is_anomaly, timestamp, processed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO NOTHING
	`,
		ev.ID, string(ev.Source), string(ev.Level), ev.Message,
		ev.Classification, ev.Summary, ev.AnomalyScore, ev.IsAnomaly,
		ev.Timestamp, ev.ProcessedAt,
	)
	return err
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}
