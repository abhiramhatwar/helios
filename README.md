# Helios — Real-time Event Intelligence Engine

A high-throughput event stream processing system built in Go that ingests structured events, enriches them with AI in real-time, detects anomalies, and pushes instant alerts to a live dashboard.

## What it does

Every software system produces events — errors, warnings, user actions, payment failures. Helios watches all of them in real-time, uses AI to understand what is happening, and alerts you the moment something goes wrong — before your users notice.

## Architecture

```
Client (HTTP / gRPC)
        │
        ▼
   API Layer (net/http)
        │
        ▼
Lock-Free Ring Buffer (MPMC · Vyukov algorithm · 8M ops/sec)
        │
        ▼
Worker Pool (goroutines + semaphore backpressure)
        │
        ▼
Circuit Breaker (Closed → Open → HalfOpen · atomic state machine)
        │
        ▼
AI Enrichment Layer (Gemini 2.0 Flash · async parallel inference)
        │
        ▼
Anomaly Detector (sliding window · dynamic baseline per source)
        │
   ┌────┴────┐
   ▼         ▼
Alert     Storage
Engine    (PostgreSQL + Redis cache)
   │
   ▼
WebSocket Hub (real-time dashboard push)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| API | net/http (Go standard library) |
| Queue | Custom lock-free MPMC ring buffer |
| Concurrency | goroutines · errgroup · sync/atomic |
| Circuit Breaker | Custom atomic state machine |
| AI | Gemini 2.0 Flash API |
| Vector Search | pgvector (Day 4+) |
| Database | PostgreSQL (pgx v5) |
| Cache + PubSub | Redis |
| Real-time | WebSocket (gorilla/websocket) |
| Metrics | Prometheus |
| Logging | zerolog |
| Config | Viper (env-driven) |
| Infrastructure | Docker + Docker Compose |

## Go Concepts Demonstrated

- Lock-free data structures (`sync/atomic`, CAS operations)
- Goroutine worker pool with semaphore backpressure
- Circuit breaker pattern (atomic state machine)
- Fan-out / fan-in channel orchestration
- Context propagation and cancellation trees
- Interface-driven plugin architecture
- Generics for typed event schemas
- Graceful shutdown with buffer drain
- gRPC streaming (Day 3)

## Getting Started

### Prerequisites

- Go 1.22+
- Docker + Docker Compose

### Run locally

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/helios.git
cd helios

# Start PostgreSQL, Redis, Prometheus
make docker-up

# Copy env file
cp .env.example .env

# Install dependencies
make deps

# Run the server
make run
```

Server starts at `http://localhost:8080`

### Send an event

```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "source": "payment-service",
    "level": "error",
    "message": "payment gateway timeout",
    "payload": { "amount": 5000, "user_id": "u123" }
  }'
```

### Send a batch

```bash
curl -X POST http://localhost:8080/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"source":"auth-service","level":"warning","message":"login attempt failed"},
    {"source":"payment-service","level":"error","message":"gateway timeout"}
  ]'
```

### Check buffer status

```bash
curl http://localhost:8080/api/v1/status
```

### Health check

```bash
curl http://localhost:8080/health
```

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/events` | Ingest a single event |
| POST | `/api/v1/events/batch` | Ingest multiple events |
| GET | `/api/v1/status` | Buffer depth and usage |
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| WS | `/ws` | Real-time alert stream (Day 3) |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP server port |
| `POSTGRES_DSN` | `postgres://helios:helios@localhost:5432/helios` | Database connection |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `BUFFER_CAPACITY` | `4096` | Ring buffer size (must be power of 2) |
| `WORKER_COUNT` | `8` | Number of worker goroutines |
| `WORKER_MAX_CONCURRENT` | `32` | Max concurrent event processors |

## Benchmarks

```
BenchmarkRingBuffer_Enqueue-10    8,248,914 ops/sec    0 B/op    0 allocs/op
```

## Project Status

- [x] Day 1 — Core pipeline (ring buffer, worker pool, circuit breaker, HTTP API)
- [ ] Day 2 — AI enrichment layer + anomaly detection + PostgreSQL + Redis
- [ ] Day 3 — gRPC streaming + WebSocket real-time alerts
- [ ] Day 4 — Live dashboard + Prometheus + Grafana
- [ ] Day 5 — Rate limiting + auth + deployment (Fly.io)
