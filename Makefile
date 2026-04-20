.PHONY: build run test clean tidy

BINARY=mutual-fund-analytics
BUILD_DIR=./bin

build:
	@echo "Building..."
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/server/
	@echo "Binary at $(BUILD_DIR)/$(BINARY)"

run:
	@echo "Running server..."
	go run ./cmd/server/

tidy:
	go mod tidy

test:
	@echo "Running all tests..."
	go test ./... -v -timeout 60s

test-rate-limiter:
	go test ./internal/ratelimiter/... -v -timeout 30s

test-analytics:
	go test ./internal/analytics/... -v -timeout 30s

test-pipeline:
	go test ./internal/pipeline/... -v -timeout 60s

test-api:
	go test ./internal/api/... -v -timeout 30s

clean:
	rm -rf $(BUILD_DIR)
	rm -f *.db *.db-shm *.db-wal

lint:
	go vet ./...
	@which staticcheck && staticcheck ./... || echo "Install staticcheck: go install honnef.co/go/tools/cmd/staticcheck@latest"

coverage:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
