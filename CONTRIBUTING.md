# Contributing to StoreMy

StoreMy is a **Rust-only** workspace. The database implementation lives under [`db/`](db/) (crate `storemy`). There is no secondary language runtime in this repository.

## Rust workflow

From the repository root:

```bash
cargo +nightly-2026-04-01 fmt --all -- --check   # or: make rust-fmt-check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs **cargo-nextest** for tests (`cargo nextest run`). Install locally:

```bash
cargo install cargo-nextest --locked
```

### Clippy

The project treats warnings as errors in CI: `clippy ... -D warnings`. Fix or allow with a short comment and `#[allow(...)]` only when justified.

### Faster links (optional, Linux / WSL)

If [mold](https://github.com/rui314/mold) is installed, you can speed up linking by adding a repo-local Cargo config (not committed), for example `~/.cargo/config.toml` or `.cargo/config.toml` with your platform’s target and:

```toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

Adjust the target triple for your machine (`aarch64-unknown-linux-gnu`, etc.). CI installs mold for the Rust jobs.

### Docker

- `make docker-build` / `docker compose build` — release image with `storemy` and `metrics_exporter`.
- `make docker-demo` — interactive REPL.
- `make docker-test` — `cargo test -p storemy --test integration` inside a dev image.
