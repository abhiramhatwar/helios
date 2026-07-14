# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o bin/helios ./cmd/server

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/helios .
COPY --from=builder /app/web ./web

# HTTP API
EXPOSE 8080
# gRPC
EXPOSE 9090

CMD ["./helios"]
