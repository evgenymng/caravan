BIN := bin/caravan

.PHONY: help build run-receive run-send format lint test test-short test-race clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build the caravan binary into bin/
	go build -o $(BIN) ./cmd/caravan

run-receive: ## Run receiver on :8080 into ./downloads
	go run ./cmd/caravan receive --addr :8080 --dir ./downloads

run-send: ## Send FILE to ADDR (make run-send FILE=./a.bin ADDR=host:8080)
	go run ./cmd/caravan send --file $(FILE) --addr $(ADDR)

format: ## Format all Go code (goimports + gofumpt + golines)
	goimports -w .
	gofumpt -w .
	golines -w -m 80 .

lint: ## Run golangci-lint
	golangci-lint run

test: ## Run all tests (including the 100 MB scenario)
	go test ./...

test-short: ## Run fast tests only
	go test -short ./...

test-race: ## Run all tests with the race detector
	go test -race ./...

clean: ## Remove build artifacts
	rm -rf bin/
