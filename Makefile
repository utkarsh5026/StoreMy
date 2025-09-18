# Makefile for StoreMy project

.PHONY: test test-tables test-all test-watch test-watch-tables clean install-tools

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

# Help
help:
	@echo "Available targets:"
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
	@echo "  help              - Show this help"