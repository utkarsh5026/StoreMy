# Makefile for StoreMy project

.PHONY: test test-tables test-all test-watch test-watch-tables clean install-tools examples \
        docker-demo docker-import docker-fresh docker-test docker-build docker-clean docker-stop quickstart

# Run all tests
test:
	go test ./... -v

# Run tests for tables package only
test-tables:
	go test ./pkg/tables/... -v

# Run tests with coverage
test-coverage:
	go test ./... -cover

# Watch and run all tests when files change (requires inotify-tools)
test-watch:
	@echo "Watching for changes in Go files..."
	@echo "Will run: go test ./... -v"
	@echo "Press Ctrl+C to stop"
	@echo ""
	@go test ./... -v
	@echo ""
	@while inotifywait -e modify,create,delete -r . --include='.*\.go$$' 2>/dev/null; do \
		echo "Files changed, running tests..."; \
		echo "$$(date): Running go test ./... -v"; \
		go test ./... -v; \
		echo ""; \
	done


# Install required tools
install-tools:
	@echo "Installing file watching tools..."
	sudo apt-get update && sudo apt-get install -y inotify-tools || true
	go install github.com/mitranim/gow@latest || true
	@echo "Tools installation complete!"

# Clean test cache
clean:
	go clean -testcache

# Build the project
build:
	go build -o bin/storemy ./

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# Run linter (requires golangci-lint)
lint:
	golangci-lint run || true

# Run all checks
check: fmt vet test

# Run WAL examples
examples:
	go run pkg/examples/main.go

# Help
help:
	@echo "StoreMy Database - Available Commands"
	@echo ""
	@echo "Docker Commands (Recommended for Testing):"
	@echo "  make docker-demo      - Start database with demo data (easiest way to test)"
	@echo "  make docker-import    - Start database and import sample SQL"
	@echo "  make docker-fresh     - Start empty database"
	@echo "  make docker-test      - Run automated CRUD tests"
	@echo "  make docker-build     - Build Docker image"
	@echo "  make docker-clean     - Stop containers and remove volumes"
	@echo "  make docker-stop      - Stop Docker containers"
	@echo "  make quickstart       - Build and run demo (one command)"
	@echo ""
	@echo "Local Development:"
	@echo "  test              - Run all tests"
	@echo "  test-tables       - Run table tests only"
	@echo "  test-coverage     - Run tests with coverage"
	@echo "  test-watch        - Watch and run all tests on file changes"
	@echo "  test-watch-tables - Watch and run table tests on file changes"
	@echo "  install-tools     - Install file watching tools"
	@echo "  clean             - Clean test cache"
	@echo "  build             - Build the project"
	@echo "  fmt               - Format code"
	@echo "  vet               - Vet code"
	@echo "  lint              - Run linter"
	@echo "  check             - Run fmt, vet, and test"
	@echo "  examples          - Run WAL transaction examples"
	@echo "  help              - Show this help"

# ============================================
# Docker Commands (For Recruiters/Testers)
# ============================================

docker-demo:
	@echo "🚀 Starting StoreMy with demo data..."
	@echo "Use Ctrl+E to execute queries, Ctrl+H for help, Ctrl+Q to quit"
	docker-compose up storemy-demo

docker-import:
	@echo "📥 Starting StoreMy with sample SQL import..."
	docker-compose up storemy-import

docker-fresh:
	@echo "🆕 Starting fresh StoreMy database..."
	docker-compose up storemy-fresh

docker-test:
	@echo "🧪 Running automated CRUD tests..."
	docker-compose run --rm storemy-test

docker-build:
	@echo "🔨 Building Docker image..."
	docker-compose build

docker-clean:
	@echo "🧹 Cleaning up Docker containers and volumes..."
	docker-compose down -v

docker-stop:
	@echo "🛑 Stopping Docker containers..."
	docker-compose down

# Quick start for recruiters
quickstart: docker-build docker-demo