# Makefile for StoreMy (Rust workspace). See CONTRIBUTING.md.

.DEFAULT_GOAL := help

.PHONY: test quick-test ci-test cargo-test test-watch test-watch-rust test-coverage \
	clean rust-clean \
	build \
	fmt rust-fmt rust-fmt-check rust-fmt-install install-hooks uninstall-hooks \
	clippy rust-clippy rust-lint rust-deny rust-machete rust-typos rust-watch rust-audit \
	lint \
	check \
	install-tools help \
	server web dev logs logs-down \
	docker-demo docker-test docker-build docker-clean docker-stop quickstart

# ----- Rust (default; workspace root = repository root) -----

# Nightly pin used ONLY for rustfmt (rustfmt.toml relies on unstable options).
# Build/test/clippy use the stable toolchain from rust-toolchain.toml.
RUSTFMT_TOOLCHAIN := nightly-2026-04-01

rust-fmt-install:
	rustup toolchain install $(RUSTFMT_TOOLCHAIN) --component rustfmt --profile minimal

rust-fmt:
	cargo +$(RUSTFMT_TOOLCHAIN) fmt --all

rust-fmt-check:
	cargo +$(RUSTFMT_TOOLCHAIN) fmt --all -- --check

# ----- Git hooks (tracked in tools/git-hooks) -----
install-hooks:
	git config core.hooksPath tools/git-hooks
	@echo "Git hooks enabled (core.hooksPath = tools/git-hooks)."
	@echo "Use 'SKIP_HOOKS=1 git commit ...' to bypass once."

uninstall-hooks:
	git config --unset core.hooksPath || true
	@echo "Git hooks disabled."

rust-clippy:
	cargo clippy --workspace --all-targets -- -D warnings

rust-deny:
	cargo deny check

rust-machete:
	cargo machete

rust-typos:
	typos

rust-watch:
	bacon

rust-audit: rust-deny rust-machete rust-typos

rust-lint: rust-clippy

rust-test:
	cargo nextest run --workspace

rust-nextest:
	cargo nextest run --workspace

quick-test:
	cargo nextest run -p storemy --lib

ci-test:
	cargo nextest run --workspace --profile ci

cargo-test:
	cargo test --workspace

rust-build:
	cargo build --workspace

rust-build-release:
	cargo build --workspace --release

rust-clean:
	cargo clean

# ----- Dev servers -----

server:
	cargo run --bin storemy-server

web:
	cd web && bun run dev

# logs — start Loki + Grafana + Promtail (logging profile).
# Grafana UI: http://localhost:3000  (admin / storemy)
# Loki API:   http://localhost:3100
logs:
	@echo "Starting logging stack (Loki + Promtail + Grafana)…"
	@mkdir -p data/logs
	docker compose --profile logging up -d
	@echo ""
	@echo "  Grafana → http://localhost:3000  (admin / storemy)"
	@echo "  Open the 'StoreMy — Query Logs' dashboard to watch live logs."

logs-down:
	docker compose --profile logging down

# dev — backend + frontend + live logging stack.
# Logs land in data/logs/storemy.log and are visible in Grafana.
dev:
	@echo "Starting logging stack…"
	@mkdir -p data/logs
	@docker compose --profile logging up -d
	@echo "Starting backend (127.0.0.1:7878) and frontend (localhost:5173)…"
	@echo "  Grafana → http://localhost:3000  (admin / storemy)"
	@cargo run --bin storemy-server 2>&1 | tee data/logs/storemy.log & \
	BACKEND_PID=$$!; \
	echo "Waiting for backend on :7878…"; \
	until bash -c 'echo > /dev/tcp/127.0.0.1/7878' 2>/dev/null; do sleep 0.5; done; \
	echo "Backend ready. Starting Vite…"; \
	cd web && bun run dev; \
	kill $$BACKEND_PID 2>/dev/null; wait; \
	docker compose --profile logging down

# Default developer targets
fmt: rust-fmt
clippy: rust-clippy
test: rust-test
build: rust-build
check: rust-fmt rust-clippy ci-test

lint: rust-clippy

test-watch-rust:
	@echo "Watching db/src for changes (Rust tests)..."
	@echo "Press Ctrl+C to stop"
	@cargo nextest run --workspace
	@while inotifywait -e modify,create,delete -r db/src 2>/dev/null; do \
		echo "$$(date): Running cargo nextest run --workspace"; \
		cargo nextest run --workspace; \
		echo ""; \
	done

test-watch: test-watch-rust

test-coverage:
	cargo nextest run --workspace
	@echo "Tip: install cargo-llvm-cov for LCOV/HTML: cargo llvm-cov test --workspace"

# ----- Housekeeping -----

clean: rust-clean

install-tools:
	@echo "Installing file watching tools..."
	sudo apt-get update && sudo apt-get install -y inotify-tools mold clang || true
	@echo "Optional: cargo install cargo-nextest cargo-watch bacon typos-cli --locked"
	@echo "Tools installation complete!"

help:
	@echo "StoreMy — Rust workspace (see CONTRIBUTING.md)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-demo      - Interactive REPL (docker compose up storemy)"
	@echo "  make docker-test      - Integration tests (docker compose run storemy-test)"
	@echo "  make docker-build     - Build images"
	@echo "  make docker-clean     - Remove containers/volumes"
	@echo "  make docker-stop      - Stop containers"
	@echo "  make quickstart       - docker-build + docker-demo"
	@echo ""
	@echo "Rust (default):"
	@echo "  test / rust-test     - cargo nextest run --workspace"
	@echo "  rust-nextest         - cargo nextest run (install cargo-nextest)"
	@echo "  quick-test           - cargo nextest run -p storemy --lib"
	@echo "  ci-test              - cargo nextest run --workspace --profile ci"
	@echo "  cargo-test           - cargo test --workspace"
	@echo "  fmt / rust-fmt       - cargo +nightly fmt --all (uses pinned nightly)"
	@echo "  rust-fmt-check       - fmt --check (same nightly)"
	@echo "  rust-fmt-install     - install pinned nightly + rustfmt component"
	@echo "  install-hooks        - enable tools/git-hooks (pre-commit fmt+clippy)"
	@echo "  uninstall-hooks      - disable the repo git hooks"
	@echo "  clippy               - clippy -D warnings"
	@echo "  rust-deny            - cargo deny check (licenses/CVEs/dups)"
	@echo "  rust-machete         - unused deps"
	@echo "  rust-typos           - source-code spell check (install: cargo install typos-cli)"
	@echo "  rust-watch           - bacon background checker (install: cargo install bacon)"
	@echo "  rust-audit           - deny + machete + typos"
	@echo "  build                - cargo build --workspace"
	@echo "  check                - fmt + clippy + ci-test"
	@echo "  test-watch           - inotify + cargo nextest on db/src changes"
	@echo "  rust-clean           - cargo clean"
	@echo ""
	@echo "  install-tools, help"
	@echo ""
	@echo "Dev servers:"
	@echo "  server               - cargo run --bin storemy-server (port 7878)"
	@echo "  web                  - bun run dev in web/ (port 5173)"
	@echo "  dev                  - backend + frontend + logging stack (Ctrl+C stops all)"
	@echo "  logs                 - start Loki + Grafana + Promtail only"
	@echo "  logs-down            - stop the logging stack"

# ============================================
# Docker
# ============================================

docker-demo:
	@echo "Starting StoreMy REPL (Rust). Ctrl+C to stop."
	docker compose up storemy

docker-test:
	@echo "Running Rust integration tests in Docker..."
	docker compose run --rm storemy-test

docker-build:
	@echo "Building Docker images..."
	docker compose build

docker-clean:
	@echo "Cleaning up Docker containers and volumes..."
	docker compose down -v

docker-stop:
	@echo "Stopping Docker containers..."
	docker compose down

quickstart: docker-build docker-demo
