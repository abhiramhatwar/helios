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
	PATH="$$PATH:$(shell go env GOPATH)/bin" protoc \
		--go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		proto/event.proto
