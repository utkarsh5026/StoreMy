# Contributing to StoreMy

Thank you for your interest in contributing to StoreMy, a relational database
management system built from scratch in Go.

## Table of Contents

1. [Development Environment](#development-environment)
2. [Project Structure](#project-structure)
3. [Building and Running](#building-and-running)
4. [Testing](#testing)
5. [Linting and Formatting](#linting-and-formatting)
6. [Benchmarking](#benchmarking)
7. [Submitting a Pull Request](#submitting-a-pull-request)
8. [Code Style](#code-style)

---

## Development Environment

### Required Tools

- **Go 1.25 or later** — https://go.dev/dl/
- **golangci-lint** — https://golangci-lint.run/usage/install/
  ```
  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
  ```
- **Docker and Docker Compose** — required for the demo and integration test mode

### Optional Tools

- **gow** — file watcher for automatic test re-runs on any platform
  ```
  go install github.com/mitranim/gow@latest
  ```
- **inotify-tools** — Linux-only alternative used by `make test-watch`

### Windows Development

StoreMy is primarily developed on Windows with CI running on Linux.

- Use Git Bash or WSL for shell scripts
- `.editorconfig` sets LF line endings for all files; ensure your editor
  respects this (VS Code requires the EditorConfig extension; GoLand supports
  it natively)
- `make test-watch` uses `inotifywait` and is Linux-only; use `gow` instead:
  ```
  gow test ./...
  ```

---

## Project Structure

```
storemy/
├── main.go              # Entry point, CLI flags, TUI initialization
├── pkg/
│   ├── catalog/         # Schema catalog (table and column metadata)
│   ├── concurrency/     # Transaction management and two-phase locking
│   ├── database/        # Database facade
│   ├── execution/       # Query execution operators (scan, join, aggregate)
│   ├── indexmanager/    # Index lifecycle (B+tree, hash)
│   ├── log/             # Write-Ahead Log (WAL)
│   ├── memory/          # Buffer pool manager
│   ├── optimizer/       # Cost-based query optimizer
│   ├── parser/          # SQL parser and AST
│   ├── plan/            # Logical and physical plan nodes
│   ├── planner/         # Plan generation from AST
│   ├── storage/         # Heap files and page management
│   ├── tuple/           # Tuple representation
│   ├── types/           # Type system
│   └── ui/              # Terminal UI (charmbracelet)
├── Makefile             # Local development commands
└── docker-compose.yml   # Demo and test containers
```

---

## Building and Running

```bash
# Build the binary
make build

# Run directly
go run .

# Build and run via Docker with demo data
make docker-demo
```

---

## Testing

```bash
# Run all tests
make test

# Run tests with race detection (run this before submitting a PR)
go test -race ./...

# Run tests with coverage report
make test-coverage

# Run tests for a specific package
go test ./pkg/execution/... -v

# Watch for file changes and re-run tests (requires gow)
gow test ./...
```

All tests must pass with race detection enabled before a PR can be merged.

---

## Linting and Formatting

```bash
# Format code
make fmt

# Run go vet
make vet

# Run golangci-lint (requires golangci-lint installed)
make lint

# Run all checks together (fmt + vet + test)
make check
```

golangci-lint is configured in [.golangci.yml](.golangci.yml). CI enforces
lint passing. Run `golangci-lint run --fix` to automatically fix formatting
issues before pushing.

---

## Benchmarking

StoreMy uses a standardized benchmarking framework documented in
[.claude/rules/benchmarking.md](.claude/rules/benchmarking.md). When adding or
modifying performance-sensitive code:

1. Add a `*_bench_test.go` file in the affected package, following the pattern
   in [pkg/execution/aggregation/operator_bench_test.go](pkg/execution/aggregation/operator_bench_test.go)
2. Run benchmarks with:
   ```bash
   go test -v -run TestBenchmarkAndSaveResults ./pkg/<your-package>/...
   ```
3. Results are saved to `.benchmarks/` (gitignored) in JSON and HTML formats
4. Include benchmark results in your PR description for performance-sensitive changes

---

## Submitting a Pull Request

1. Fork the repository and create a branch from `main`:
   ```
   git checkout -b your-feature-name
   ```
2. Make your changes following the code style guidelines below
3. Add or update tests for your changes
4. Ensure all checks pass:
   ```bash
   make check
   go test -race ./...
   golangci-lint run
   ```
5. Push your branch and open a PR against `main`
6. Fill out the PR template completely

PRs are squash-merged. Keep each commit focused on a single logical change.

---

## Code Style

- **Formatting:** All Go code must be `gofmt`-formatted. Run `make fmt` before committing.
- **Imports:** Use `goimports` with local prefix `storemy` (standard library, then third-party, then `storemy/...`).
- **Error handling:** Always return errors; do not discard them with `_` outside of test helpers.
- **Comments:** Exported types and functions must have doc comments. Internal helpers only need comments when the logic is non-obvious.
- **Line length:** Soft limit of 120 characters.
- **No global mutable state:** Pass dependencies through constructors rather than using package-level variables.
