.PHONY: build run test lint clean docker-up docker-down deps generate

build:
	go build -o bin/helios ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./... -v -race -count=1

bench:
	go test ./internal/buffer/... -bench=. -benchmem

lint:
	golangci-lint run ./...

deps:
	go mod tidy

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

clean:
	rm -rf bin/

generate:
	protoc --go_out=. --go-grpc_out=. proto/*.proto
