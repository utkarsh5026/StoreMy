# Makefile for StoreMy (Rust workspace). See CONTRIBUTING.md.

.DEFAULT_GOAL := help

.PHONY: test test-watch test-watch-rust test-coverage \
	clean rust-clean \
	build \
	fmt rust-fmt rust-fmt-check rust-fmt-install install-hooks uninstall-hooks \
	clippy rust-clippy rust-lint rust-deny rust-machete rust-typos rust-watch rust-audit \
	lint \
	check \
	install-tools help \
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
	cargo test --workspace

rust-nextest:
	cargo nextest run --workspace

rust-build:
	cargo build --workspace

rust-build-release:
	cargo build --workspace --release

rust-clean:
	cargo clean

# Default developer targets
fmt: rust-fmt
clippy: rust-clippy
test: rust-test
build: rust-build
check: rust-fmt rust-clippy rust-test

lint: rust-clippy

test-watch-rust:
	@echo "Watching db/src for changes (Rust tests)..."
	@echo "Press Ctrl+C to stop"
	@cargo test --workspace
	@while inotifywait -e modify,create,delete -r db/src 2>/dev/null; do \
		echo "$$(date): Running cargo test --workspace"; \
		cargo test --workspace; \
		echo ""; \
	done

test-watch: test-watch-rust

test-coverage:
	cargo test --workspace
	@echo "Tip: install cargo-llvm-cov for LCOV/HTML: cargo llvm-cov test --workspace"

# ----- Housekeeping -----

clean: rust-clean

install-tools:
	@echo "Installing file watching tools..."
	sudo apt-get update && sudo apt-get install -y inotify-tools || true
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
	@echo "  test / rust-test     - cargo test --workspace"
	@echo "  rust-nextest         - cargo nextest run (install cargo-nextest)"
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
	@echo "  check                - fmt + clippy + test"
	@echo "  test-watch           - inotify + cargo test on db/src changes"
	@echo "  rust-clean           - cargo clean"
	@echo ""
	@echo "  install-tools, help"

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
