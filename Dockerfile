# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

# go.mod requires Go 1.25 (grpc v1.82). GOTOOLCHAIN=auto lets the Go 1.24 image
# download and invoke the 1.25 toolchain automatically during the build.
ENV GOTOOLCHAIN=auto

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
