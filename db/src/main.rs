//! `StoreMy` database CLI entrypoint.
//!
//! This module is intentionally small and does only process bootstrapping:
//! - initialize logging/tracing via [`observability::init`]
//! - parse CLI arguments to choose REPL vs one-shot execution
//! - create storage/runtime dependencies (WAL, buffer pool, catalog, tx manager)
//! - construct [`Database`] and hand off control to [`repl`]
//!
//! # CLI modes
//! - **REPL mode** (`cargo run` or `cargo run -- repl`) starts an interactive SQL shell.
//! - **One-shot mode** (`cargo run -- "<sql>"`) executes a single SQL statement and exits.
//!
//! Keeping this logic in `main.rs` helps keep the executable wiring explicit, while
//! parser/executor/storage behavior lives in their respective modules.

use std::{path::PathBuf, sync::Arc};

use storemy::{
    buffer_pool::page_store::PageStore, catalog::manager::Catalog, database::Database,
    observability, repl, transaction::TransactionManager, wal::writer::Wal,
};
use tracing::{error, info};

const WAL_FILE_NAME: &str = "wal.log";
const REPL_HISTORY_FILE_NAME: &str = ".repl_history";
const BUFFER_POOL_PAGES: usize = 1028;
const WORKER_THREADS: usize = 4;

/// Entrypoint for the `StoreMy` process.
///
/// This function performs the following bootstrapping steps:
/// - Initializes process-wide tracing and logging by calling [`observability::init`].
/// - Parses CLI arguments to determine execution mode: either REPL mode (interactive shell) or
///   one-shot mode (execute a SQL query and exit).
/// - Ensures the on-disk data directory exists, exiting with an error if not.
/// - Constructs all core database subsystems:
///     - Write-ahead log (`WAL`)
///     - Buffer pool and page store
///     - Catalog store/manager
///     - Transaction manager
///     - Main [`Database`] struct
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

    println!("StoreMy Database v{}", env!("CARGO_PKG_VERSION"));
    if let Some(path) = trace_guards.chrome_path() {
        println!(
            "  perfetto trace : {} (open at https://ui.perfetto.dev)",
            path.display()
        );
    }
    if let Some(endpoint) = trace_guards.otel_endpoint() {
        println!("  otel exporter  : {endpoint} (Jaeger UI: http://localhost:16686)");
    }
    info!(version = env!("CARGO_PKG_VERSION"), "StoreMy starting");

    let data_dir = PathBuf::from("./data");
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        error!(error = %e, dir = %data_dir.display(), "failed to create data dir");
        std::process::exit(1);
    }

    let wal = Arc::new(
        Wal::new(&data_dir.join(WAL_FILE_NAME), 0).unwrap_or_else(|e| {
            error!(error = %e, "failed to create WAL");
            std::process::exit(1);
        }),
    );

    let buffer_pool = Arc::new(PageStore::new(BUFFER_POOL_PAGES, wal.clone()));
    let catalog = Catalog::initialize(&buffer_pool, &wal, &data_dir).unwrap_or_else(|e| {
        error!(error = %e, "failed to initialize catalog");
        std::process::exit(1);
    });
    let txn_mgr = TransactionManager::new(wal, buffer_pool);
    let db = Database::new(Arc::new(catalog), Arc::new(txn_mgr), WORKER_THREADS);

    match mode_or_sql.as_deref() {
        None | Some("repl") => repl::run(
            &db,
            &data_dir.join(REPL_HISTORY_FILE_NAME),
            &data_dir,
            BUFFER_POOL_PAGES,
        ),
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
