# Helios — Real-time Event Intelligence Engine

> High-throughput event ingestion, AI-powered enrichment, and anomaly detection — built in Go.

Every software system produces signals: payment failures, auth errors, infra spikes. Helios ingests thousands of those events per second, uses Gemini AI to classify and summarize each one, detects statistical anomalies in real time, and pushes everything live to a WebSocket dashboard and gRPC alert stream.

---

## What it looks like running

```
$ go run ./cmd/server

{"level":"info","buffer_capacity":4096,"message":"ring buffer initialised"}
{"level":"warn","message":"GEMINI_API_KEY not set — AI enrichment disabled, using passthrough"}
{"level":"info","message":"postgres ready"}
{"level":"info","message":"redis ready"}
{"level":"info","workers":8,"message":"worker pool started"}
{"level":"info","addr":"0.0.0.0:8080","message":"HTTP server listening"}
{"level":"info","addr":":9090","message":"gRPC server listening"}

# Send an event:
$ curl -s -X POST http://localhost:8080/api/v1/events \
    -H "Content-Type: application/json" \
    -d '{"source":"payments","level":"error","message":"gateway timeout after 30s"}'

{"id":"a3f2c1d8-...","status":"queued"}

# Server logs the processed result:
{"level":"warn","id":"a3f2c1d8","source":"payments","classification":"infrastructure",
 "score":0.82,"summary":"Payment gateway timed out — potential network or dependency failure",
 "message":"anomaly detected"}
```

---

## Architecture

```
  HTTP REST  ──┐
               ▼
  gRPC Stream ──► [ Lock-free Ring Buffer ] ──► [ Worker Pool ]
                        8M ops/sec                 semaphore backpressure
                        0 allocs/op
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
            [ Anomaly Detector ]   [ Circuit Breaker ]
             sliding z-score        Closed/Open/HalfOpen
             per source                    │
                    │                      ▼
                    └──────► [ Gemini 2.0 Flash ]
                               AI enrichment
                                      │
                              ┌───────┴────────┐
                              ▼                ▼
                        [ PostgreSQL ]    [ Redis Pub/Sub ]
                          pgx/v5              │
                          idempotent    ┌─────┴─────┐
                          upserts       ▼           ▼
                                  helios:events  helios:alerts
                                       │              │
                                       ▼              ▼
                               [ WebSocket Hub ]  [ gRPC WatchAlerts ]
                                 Browser dashboard  Alert subscribers
```

---

## Core Engineering

### Lock-free MPMC Ring Buffer
Custom bounded queue using Dmitry Vyukov's sequence algorithm. Producer and consumer cursors sit on separate cache lines (64-byte padding) to prevent false sharing. Zero heap allocations on the hot path.

```
BenchmarkRingBuffer_Enqueue-10    8,248,914 ops/sec    0 B/op    0 allocs/op
```

### Circuit Breaker
Three-state atomic state machine wrapping every Gemini API call. No locks — all transitions use `sync/atomic` CAS operations.

```
Closed ──(5 failures)──► Open ──(30s timeout)──► HalfOpen
  ▲                                                   │
  └──────────────(2 successes)───────────────────────┘
```

When open, the system falls back to passthrough enrichment. **No events are dropped** — they just flow through without AI classification.

### Sliding Window Anomaly Detection
Each event source gets its own ring of 60 ten-second buckets (10-minute history). A single background goroutine advances all windows on a ticker — no per-event goroutine spawning. A z-score above 2.0 flags the burst as anomalous.

### Two Redis Channels
`helios:events` fans every enriched event to the WebSocket dashboard. `helios:alerts` carries anomaly-only payloads to gRPC subscribers. The two concerns are fully decoupled — the dashboard and alerting pipelines never interfere.

---

## Quick Start

**Requirements**: Go 1.22+, Docker

```bash
# 1. Clone
git clone https://github.com/abhiramhatwar/helios.git
cd helios

# 2. Start infrastructure (PostgreSQL, Redis, Prometheus, Grafana)
docker compose up -d

# 3. Copy config
cp .env.example .env

# 4. Run (passthrough mode — no Gemini key needed)
go run ./cmd/server

# 5. Open the live dashboard
open http://localhost:8080
```

With AI enrichment:
```bash
GEMINI_API_KEY=your_key_here go run ./cmd/server
```

Grafana (auto-provisioned with Prometheus datasource):
```
http://localhost:3000  →  admin / helios
```

---

## API Examples

### Ingest a single event

```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "source": "payments",
    "level": "error",
    "message": "Stripe gateway timeout after 30s",
    "payload": { "amount_cents": 4999, "user_id": "u_abc123" },
    "tags": ["stripe", "latency", "p99"]
  }'
```

Response:
```json
{ "id": "a3f2c1d8-7b2e-4c1f-9d8e-3f2a1b4c5d6e", "status": "queued" }
```

### Ingest a batch

```bash
curl -X POST http://localhost:8080/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[
    { "source": "auth", "level": "warning", "message": "5 failed login attempts from 192.168.1.1" },
    { "source": "api", "level": "info",    "message": "GET /v2/products 200 12ms" },
    { "source": "db",  "level": "error",   "message": "connection pool exhausted (max=50)" }
  ]'
```

Response:
```json
{ "queued": 3, "ids": ["...", "...", "..."] }
```

### Check system status

```bash
curl http://localhost:8080/api/v1/status
```

Response:
```json
{
  "buffer_len": 12,
  "buffer_cap": 4096,
  "buffer_usage": "0.3%",
  "circuit_breaker": "closed"
}
```

### Health check

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

### With API key authentication

```bash
# Using X-API-Key header
curl -X POST http://localhost:8080/api/v1/events \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"source":"app","level":"info","message":"deploy completed"}'

# Using Bearer token
curl -X POST http://localhost:8080/api/v1/events \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"source":"app","level":"info","message":"deploy completed"}'
```

---

## gRPC Streaming

Helios exposes two RPCs on `:9090`:

### Stream events in (client-side streaming)

```protobuf
rpc IngestStream(stream IngestRequest) returns (IngestResponse);
```

A gRPC client can open a persistent stream and push events at high throughput without per-request HTTP overhead. The server returns a summary (`accepted=N dropped=M`) when the client closes.

### Watch anomaly alerts (server-side streaming)

```protobuf
rpc WatchAlerts(WatchRequest) returns (stream AlertEvent);
```

Subscribers receive a real-time stream of anomalies filtered by source name and minimum anomaly score:

```go
// Go client example
client.WatchAlerts(ctx, &heliosv1.WatchRequest{
    Sources:         []string{"payments", "auth"},
    MinAnomalyScore: 0.7,
})
// → receives AlertEvent messages as anomalies are detected
```

---

## Live Dashboard

Connect to `http://localhost:8080` to see the real-time dashboard powered by WebSocket + Chart.js.

| Card | What it shows |
|---|---|
| **Total Events** | Running count + current events/s |
| **Anomalies** | Count + % of total |
| **Circuit Breaker** | CLOSED / OPEN / HALF-OPEN — updates every 5s |
| **Last Event** | Source and timestamp of the most recent event |

The rate chart plots events/s and anomalies/s for the last 60 seconds. The live feed shows the 60 most recent events. The anomaly log shows the last 20 anomalies with a score bar.

WebSocket auto-reconnects with a 2-second backoff if the connection drops.

---

## Prometheus Metrics

Available at `http://localhost:8080/metrics`:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `helios_events_ingested_total` | Counter | `source`, `level` | Total events received |
| `helios_events_enriched_total` | Counter | `classification` | Events processed by AI |
| `helios_anomalies_detected_total` | Counter | `source` | Anomalies flagged |
| `helios_enrichment_duration_seconds` | Histogram | — | AI call latency |
| `helios_circuit_breaker_open` | Gauge | — | 1 when breaker is open |

Grafana is auto-provisioned at `http://localhost:3000` with Prometheus as the default datasource.

---

## Configuration

All settings via environment variables (no config file needed):

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP listen port |
| `POSTGRES_DSN` | `postgres://helios:helios@localhost:5432/helios` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `GEMINI_API_KEY` | *(empty)* | Gemini API key — passthrough mode if unset |
| `BUFFER_CAPACITY` | `4096` | Ring buffer size (must be power of 2) |
| `WORKER_COUNT` | `8` | Consumer goroutines |
| `WORKER_MAX_CONCURRENT` | `32` | Max simultaneous AI enrichment calls |
| `RATELIMIT_RPS` | `100` | Requests per second per IP |
| `RATELIMIT_BURST` | `200` | Burst capacity per IP |
| `AUTH_API_KEY` | *(empty)* | API key for protected routes — disabled if unset |

---

## Running Tests

```bash
# All tests
go test ./...

# With race detector
go test -race ./...

# Specific package
go test ./internal/circuit/...
go test ./internal/detector/...
go test ./internal/middleware/...

# Ring buffer benchmark
go test ./internal/buffer/... -bench=. -benchmem
```

---

## Project Structure

```
helios/
├── cmd/server/           # Entrypoint — wires the full pipeline
├── config/               # Viper config with env var binding
├── internal/
│   ├── buffer/           # Lock-free MPMC ring buffer (generics)
│   │   └── *_test.go     # MPMC concurrency + benchmark tests
│   ├── worker/           # Goroutine pool with semaphore backpressure
│   ├── circuit/          # Three-state atomic circuit breaker
│   │   └── *_test.go     # State machine transition tests
│   ├── enrichment/       # Gemini enricher + passthrough fallback
│   ├── detector/         # Sliding window z-score anomaly detection
│   │   └── *_test.go     # Baseline + spike detection tests
│   ├── storage/          # PostgreSQL store (pgx/v5)
│   ├── alert/            # Redis pub/sub publisher
│   ├── grpcserver/       # gRPC IngestStream + WatchAlerts
│   ├── ws/               # WebSocket hub + client
│   ├── api/              # HTTP handlers + middleware chain
│   └── middleware/       # Token bucket rate limiter + API key auth
│       └── *_test.go     # Rate limit + auth tests
├── pkg/event/            # Shared event and enriched event types
├── proto/                # Protobuf definitions + generated code
├── web/                  # Dashboard (Tailwind CDN + Chart.js, no build step)
├── grafana/              # Grafana provisioning (auto-loaded datasource)
├── Dockerfile            # Multi-stage build (golang:1.24 → alpine:3.20)
└── docker-compose.yml    # PostgreSQL, Redis, Prometheus, Grafana
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22+ |
| AI | Google Gemini 2.0 Flash |
| Database | PostgreSQL 16 (pgx/v5) |
| Cache / Pub-Sub | Redis 7 (go-redis/v9) |
| RPC | gRPC + Protocol Buffers |
| WebSocket | gorilla/websocket |
| Metrics | Prometheus + Grafana |
| Logging | zerolog (structured JSON) |
| Config | Viper (env var binding) |
| Container | Docker + Docker Compose |
