//! Shared test utilities for all `storemy` unit tests.
//!
//! Import with `use crate::test_utils;` and call [`init_tracing`] from your
//! module's test-helper function (e.g. `make_store`, `make_overflow_file`).
//! Because it is guarded by a [`std::sync::OnceLock`] it is safe to call from
//! many parallel tests — the subscriber is initialized exactly once per
//! process.
//!
//! Logging is opt-in at runtime via `RUST_LOG`. Set it before running tests:
//!
//! ```text
//! RUST_LOG=storemy=debug  make test-log
//! RUST_LOG=storemy=trace  make test-log-one TEST=write_and_read_single_chunk
//! ```
//!
//! Without `RUST_LOG` the subscriber is still installed but emits nothing,
//! so the call is always safe.

use std::sync::OnceLock;

static TRACING: OnceLock<()> = OnceLock::new();

/// Initialize the global tracing subscriber for tests.
///
/// Safe to call from multiple test helpers in the same process — the
/// subscriber is set up only on the first call. Subsequent calls are
/// instant no-ops.
pub fn init_tracing() {
    TRACING.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    });
}
