# Helios — Real-time Event Intelligence Engine

A high-throughput event ingestion and anomaly detection system built in Go. Ingests events via HTTP or gRPC, enriches them with AI classification, detects anomalies using statistical analysis, and streams results live to a WebSocket dashboard.

## Architecture

```
Producers (HTTP REST / gRPC client-streaming)
        │
        ▼
┌───────────────────┐
│  Lock-free MPMC   │  Vyukov sequence algorithm
│   Ring Buffer     │  8M+ ops/sec · 0 allocations per op
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Goroutine Worker │  Semaphore backpressure
│      Pool         │  Configurable concurrency
└────────┬──────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌──────────────────┐
│ Anomaly│ │  Gemini 2.0 Flash│  Wrapped in circuit breaker
│Detector│ │   AI Enrichment  │  (Closed → Open → HalfOpen)
│z-score │ └────────┬─────────┘
└────┬───┘          │
     └──────┬───────┘
            ▼
     ┌─────────────┐
     │  PostgreSQL  │  pgx/v5 · auto-migration · idempotent upserts
     └──────┬──────┘
            │
            ▼
     ┌─────────────┐
     │    Redis     │  helios:events  ──► WebSocket hub ──► Browser dashboard
     │   Pub/Sub    │  helios:alerts  ──► gRPC WatchAlerts stream
     └─────────────┘
```

## Key Engineering Decisions

**Lock-free ring buffer** — Custom MPMC queue using atomic sequence numbers (Vyukov algorithm). Zero heap allocations on the hot path, cache-line padded slots to prevent false sharing. Benchmarks at 8M+ ops/sec on a single core.

**Circuit breaker** — Three-state atomic state machine wrapping Gemini API calls. Configurable failure threshold, recovery window, and success count to re-close. System degrades gracefully to passthrough enrichment when the breaker is open — no data is dropped.

**Sliding window anomaly detection** — Per-source ring of 60 ten-second buckets (10-minute history). Z-score threshold of 2.0 flags statistical outliers in event rate. Single background goroutine rotates all source windows; no per-event goroutine spawning.

**Two Redis channels** — `helios:events` fans every enriched event to the WebSocket dashboard. `helios:alerts` carries only anomaly payloads to gRPC subscribers. Fully decouples dashboard and alerting concerns.

**Token bucket rate limiting** — Per-IP buckets with lazy init and a background goroutine that purges buckets idle for 5+ minutes. No external dependency.

## Features

- **Dual ingestion** — REST (`POST /api/v1/events`, batch) and gRPC client-streaming (`IngestStream`)
- **AI enrichment** — Gemini 2.0 Flash classifies each event, produces a summary and anomaly score
- **Anomaly detection** — Z-score over a 10-minute sliding window, per event source
- **Live dashboard** — WebSocket-fed Chart.js dashboard showing events/s, anomalies/s, live feed, anomaly log
- **gRPC alert streaming** — `WatchAlerts` RPC streams anomalies to subscribers, filterable by source and min score
- **Observability** — Prometheus metrics (events ingested, enrichment latency, circuit breaker state) + Grafana auto-provisioned
- **Rate limiting** — 100 req/s per IP, burst 200, 429 with `Retry-After` on exceed
- **API key auth** — Bearer token or `X-API-Key` header; health/metrics/dashboard routes exempt

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| AI | Gemini 2.0 Flash (google/generative-ai-go) |
| Database | PostgreSQL 16 (pgx/v5) |
| Cache / Pub-Sub | Redis 7 (go-redis/v9) |
| RPC | gRPC + Protocol Buffers |
| WebSocket | gorilla/websocket |
| Metrics | Prometheus + Grafana |
| Logging | zerolog (structured JSON) |
| Config | Viper (env vars) |

## Running Locally

**Prerequisites**: Docker, Go 1.22+

```bash
git clone https://github.com/abhiramhatwar/helios.git
cd helios

# Start PostgreSQL, Redis, Prometheus, Grafana
docker compose up -d

# Run without AI enrichment (passthrough mode)
go run ./cmd/server

# Run with AI enrichment
GEMINI_API_KEY=your_key go run ./cmd/server
```

- Dashboard: `http://localhost:8080`
- Grafana: `http://localhost:3000` (admin / helios)
- gRPC: `:9090`

## API

```bash
# Single event
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{"source":"payments","level":"error","message":"gateway timeout","tags":["db","infra"]}'

# Batch
curl -X POST http://localhost:8080/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[{"source":"auth","level":"warn","message":"login failed"},{"source":"api","level":"info","message":"ok"}]'

# Buffer depth
curl http://localhost:8080/api/v1/status

# Health
curl http://localhost:8080/health
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP listen port |
| `POSTGRES_DSN` | `postgres://helios:helios@localhost:5432/helios` | PostgreSQL DSN |
| `REDIS_URL` | `redis://localhost:6379` | Redis URL |
| `GEMINI_API_KEY` | `` | Gemini API key (optional — passthrough if unset) |
| `BUFFER_CAPACITY` | `4096` | Ring buffer size (must be power of 2) |
| `WORKER_COUNT` | `8` | Consumer goroutines |
| `WORKER_MAX_CONCURRENT` | `32` | Max concurrent enrichment calls |
| `RATELIMIT_RPS` | `100` | Requests/sec per IP |
| `RATELIMIT_BURST` | `200` | Burst capacity per IP |
| `AUTH_API_KEY` | `` | API key for protected routes (optional) |

## Benchmarks

```
BenchmarkRingBuffer_Enqueue-10    8,248,914 ops/sec    0 B/op    0 allocs/op
```

## Project Structure

```
helios/
├── cmd/server/          # Entrypoint — wires the full pipeline
├── config/              # Viper config with env var binding
├── internal/
│   ├── buffer/          # Lock-free MPMC ring buffer (generics)
│   ├── worker/          # Goroutine pool with semaphore backpressure
│   ├── circuit/         # Three-state atomic circuit breaker
│   ├── enrichment/      # Gemini enricher + passthrough fallback
│   ├── detector/        # Sliding window z-score anomaly detection
│   ├── storage/         # PostgreSQL store (pgx/v5)
│   ├── alert/           # Redis pub/sub publisher
│   ├── grpcserver/      # gRPC IngestStream + WatchAlerts
│   ├── ws/              # WebSocket hub + client
│   ├── api/             # HTTP handlers
│   └── middleware/      # Token bucket rate limiter + API key auth
├── pkg/event/           # Shared event types
├── proto/               # Protobuf definitions + generated code
└── web/                 # Dashboard (Tailwind CDN + Chart.js, no build step)
```
