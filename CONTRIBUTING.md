# Contributing to StoreMy

StoreMy is a **Rust-only** workspace. The database implementation lives under [`db/`](db/) (crate `storemy`). There is no secondary language runtime in this repository.

## Rust workflow

From the repository root:

```bash
cargo +nightly-2026-04-01 fmt --all -- --check   # or: make rust-fmt-check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --profile ci
```

CI runs **cargo-nextest** for tests (`cargo nextest run`). Install locally:

```bash
cargo install cargo-nextest --locked
```

### Fast local loop

Use the smallest useful command while you are editing, then widen the check before committing:

```bash
cargo check -p storemy                    # fastest compiler feedback
cargo nextest run -p storemy --lib parser # focused unit tests; replace parser with a filter
cargo t                                   # workspace nextest alias
cargo c                                   # workspace clippy alias
```

The Makefile mirrors that split:

```bash
make quick-test # cargo nextest run -p storemy --lib
make test       # cargo nextest run --workspace
make ci-test    # cargo nextest run --workspace --profile ci
make check      # fmt + clippy + ci-test
```

For background feedback, run `bacon`. The default job is a package-level `cargo check`; press `l` for library tests, `t` for workspace tests, `c` for CI-style clippy, and `f` for pedantic clippy.

### Clippy

The project treats warnings as errors in CI: `clippy ... -D warnings`. Fix or allow with a short comment and `#[allow(...)]` only when justified.

### Faster links (optional, Linux / WSL)

If [mold](https://github.com/rui314/mold) and `clang` are installed, you can speed up linking by enabling the guarded block in [`.cargo/config.toml`](.cargo/config.toml), or by adding the same setting to `~/.cargo/config.toml`:

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
