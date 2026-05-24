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
//! # Data directory
//! On-disk state (WAL, catalog, REPL history) lives under `DATA_DIR` if set, otherwise `./data`.
//!
//! Keeping this logic in `main.rs` helps keep the executable wiring explicit, while
//! parser/executor/storage behavior lives in their respective modules.

use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

use storemy::{
    buffer_pool::{double_write::DoubleWriteBuffer, page_store::PageStore},
    catalog::manager::Catalog,
    database::Database,
    observability,
    recovery::Aries,
    repl,
    transaction::TransactionManager,
    wal::writer::Wal,
};
use tracing::{error, info};

const WAL_FILE_NAME: &str = "wal.log";
const MASTER_RECORD_FILE_NAME: &str = "master.rec";
const DOUBLE_WRITE_FILE_NAME: &str = "double_write.bin";
const REPL_HISTORY_FILE_NAME: &str = ".repl_history";
const BUFFER_POOL_PAGES: usize = 1028;
/// Number of double-write buffer slots.  Each slot is one page (4 KiB) plus a
/// 512-byte header.  128 slots = ~576 KiB — negligible overhead.
const DWB_SLOTS: usize = 128;
const WORKER_THREADS: usize = 4;

/// Logs `message` with the error value and terminates the process.
fn exit_on_err<T, E: std::fmt::Display>(result: Result<T, E>, message: &str) -> T {
    result.unwrap_or_else(|e| {
        error!(error = %e, "{message}");
        process::exit(1);
    })
}

/// Initializes and loads the buffer pool with a double-write buffer, performing necessary recovery.
///
/// # Arguments
/// * `data_dir` - Path to the directory containing the database files.
/// * `wal` - Shared reference to the write-ahead log.
///
/// This function attempts to open the double-write buffer file (`double_write.bin`) with the
/// configured number of slots. If opening or recovery of torn pages fails, the process will exit
/// with a logged error. On success, a `PageStore` buffer pool is constructed and returned,
/// using the provided WAL and double-write buffer.
///
/// # Panics
/// Exits the process on filesystem or recovery errors.
fn load_buffer_pool(data_dir: &Path, wal: &Arc<Wal>) -> Arc<PageStore> {
    let mut dwb = exit_on_err(
        DoubleWriteBuffer::open(data_dir.join(DOUBLE_WRITE_FILE_NAME), DWB_SLOTS),
        "failed to open double-write buffer",
    );
    exit_on_err(
        dwb.recover_torn_pages(),
        "double-write buffer recovery failed",
    );
    info!("double-write buffer recovery complete");

    let mut buffer_pool = PageStore::new(BUFFER_POOL_PAGES, wal.clone());
    buffer_pool.set_double_write_buffer(dwb);
    Arc::new(buffer_pool)
}

/// Performs ARIES crash recovery for the database, using the WAL and buffer pool.
///
/// # Arguments
/// * `data_dir` - Path to the directory containing the database files.
/// * `buffer_pool` - Shared reference to the page store (buffer pool).
/// * `wal` - Shared reference to the write-ahead log.
///
/// ARIES recovery will scan the WAL log (`wal.log`) and master record (`master.rec`)
/// to undo changes from loser transactions and redo necessary log records, updating the
/// buffer pool state. If recovery fails, the process will exit with a logged error.
///
/// # Panics
/// Exits the process on unrecoverable or corrupt WAL errors.
fn recover_aries(data_dir: &Path, buffer_pool: &Arc<PageStore>, wal: &Arc<Wal>) {
    let recovery = exit_on_err(
        Aries::new(
            data_dir.join(WAL_FILE_NAME),
            data_dir.join(MASTER_RECORD_FILE_NAME),
        )
        .recover(buffer_pool, wal),
        "crash recovery failed — database may be corrupt",
    );
    info!(
        losers_undone = recovery.att.len(),
        dirty_pages   = recovery.dpt.len(),
        redo_lsn      = ?recovery.redo_lsn,
        "ARIES recovery complete"
    );
}

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
    info!(version = env!("CARGO_PKG_VERSION"), "StoreMy starting");

    let data_dir = env::var("DATA_DIR").map_or_else(|_| PathBuf::from("./data"), PathBuf::from);
    if let Err(e) = fs::create_dir_all(&data_dir) {
        error!(error = %e, dir = %data_dir.display(), "failed to create data dir");
        process::exit(1);
    }

    let wal = Arc::new(exit_on_err(
        Wal::new(&data_dir.join(WAL_FILE_NAME), 0),
        "failed to create WAL",
    ));

    let buffer_pool = load_buffer_pool(&data_dir, &wal);
    recover_aries(&data_dir, &buffer_pool, &wal);

    let catalog = Arc::new(exit_on_err(
        Catalog::initialize(&buffer_pool, &wal, &data_dir),
        "failed to initialize catalog",
    ));
    let txn_mgr = Arc::new(TransactionManager::new(wal, buffer_pool));
    let db = Database::new(catalog, txn_mgr, WORKER_THREADS);

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
