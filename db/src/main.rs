//! `StoreMy` database CLI entrypoint.
//!
//! This module is intentionally small and does only process bootstrapping:
//! - initialize logging/tracing via [`observability::init`]
//! - parse CLI arguments to choose REPL vs one-shot execution
//! - delegate database startup to [`storemy::registry::boot_database`]
//! - hand off control to [`repl`]
//!
//! # CLI modes
//! - **REPL mode** (`cargo run` or `cargo run -- repl`) starts an interactive SQL shell.
//! - **One-shot mode** (`cargo run -- "<sql>"`) executes a single SQL statement and exits.
//!
//! # Data directory
//! On-disk state (WAL, catalog, REPL history) lives under `DATA_DIR` if set, otherwise `./data`.
//!
//! Keeping this logic in `main.rs` helps keep the executable wiring explicit, while
//! parser/executor/storage behavior lives in their respective modules.

use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    process,
};

use storemy::{
    observability,
    registry::{BUFFER_POOL_PAGES, boot_database},
    repl,
};
use tracing::error;

const REPL_HISTORY_FILE_NAME: &str = ".repl_history";

/// Logs `message` with the error value and terminates the process.
fn exit_on_err<T, E: std::fmt::Display>(result: Result<T, E>, message: &str) -> T {
    result.unwrap_or_else(|e| {
        error!(error = %e, "{message}");
        process::exit(1);
    })
}

/// Entrypoint for the `StoreMy` process.
///
/// This function performs the following bootstrapping steps:
/// - Initializes process-wide tracing and logging by calling [`observability::init`].
/// - Parses CLI arguments to determine execution mode: either REPL mode (interactive shell) or
///   one-shot mode (execute a SQL query and exit).
/// - Ensures the on-disk data directory exists, exiting with an error if not.
/// - Delegates all database subsystem construction to [`boot_database`] (WAL, double-write buffer,
///   buffer pool, ARIES recovery, catalog, transaction manager).
/// - Dispatches control to the selected CLI mode:
///     - REPL: launches an interactive prompt (history file persisted in data dir).
///     - One-shot: executes provided SQL string(s) and prints the result.
///
/// # Panics
/// This function will terminate the process on fatal boot errors, such as filesystem or subsystem
/// initialization failures. All such errors are logged via the tracing system.
fn main() {
    let trace_guards = observability::init();

    let mut args = std::env::args().skip(1);
    let mode_or_sql = args.next();

    let _ = writeln!(
        io::stdout(),
        "StoreMy Database v{}",
        env!("CARGO_PKG_VERSION")
    );
    if let Some(path) = trace_guards.chrome_path() {
        let _ = writeln!(
            io::stdout(),
            "  perfetto trace : {} (open at https://ui.perfetto.dev)",
            path.display()
        );
    }
    if let Some(endpoint) = trace_guards.otel_endpoint() {
        let _ = writeln!(
            io::stdout(),
            "  otel exporter  : {endpoint} (Jaeger UI: http://localhost:16686)"
        );
    }
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "StoreMy starting");

    let data_dir = env::var("DATA_DIR").map_or_else(|_| PathBuf::from("./data"), PathBuf::from);
    if let Err(e) = fs::create_dir_all(&data_dir) {
        error!(error = %e, dir = %data_dir.display(), "failed to create data dir");
        process::exit(1);
    }

    let db = exit_on_err(boot_database(&data_dir), "failed to boot database");

    match mode_or_sql.as_deref() {
        None | Some("repl") => repl::run(
            &db,
            &data_dir.join(REPL_HISTORY_FILE_NAME),
            &data_dir,
            BUFFER_POOL_PAGES,
        ),
        Some("--file") => {
            let path = args.next().unwrap_or_else(|| {
                let _ = writeln!(io::stderr(), "usage: storemy --file <path.sql>");
                process::exit(1);
            });
            let content = fs::read_to_string(&path).unwrap_or_else(|e| {
                error!(path = %path, error = %e, "cannot read SQL file");
                process::exit(1);
            });
            repl::execute_script(&db, &content);
        }
        Some(first) => {
            let mut sql = String::from(first);
            for part in args {
                sql.push(' ');
                sql.push_str(&part);
            }
            repl::execute_one_shot(&db, &sql);
        }
    }
}
