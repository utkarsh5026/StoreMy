//! `StoreMy` database CLI entrypoint.
//!
//! Runs the `StoreMy` database in either:
//! - **REPL mode** (`cargo run -- repl`) for interactive SQL input
//! - **One-shot mode** (`cargo run -- "<sql>"`) to execute a single statement and exit
//!
//! The REPL uses line editing and persists history under `./data/.repl_history`.

use std::{path::PathBuf, sync::Arc};

use comfy_table::{Cell, ContentArrangement, Table};
use owo_colors::OwoColorize;
use rustyline::{DefaultEditor, error::ReadlineError};
use storemy::{
    buffer_pool::page_store::PageStore, catalog::manager::Catalog, database::Database,
    transaction::TransactionManager, wal::writer::Wal,
};

/// Default file name for the write-ahead log stored under the data directory.
const WAL_FILE_NAME: &str = "wal.log";

/// REPL history file name stored under the data directory.
const REPL_HISTORY_FILE_NAME: &str = ".repl_history";

/// CLI entrypoint that initializes storage/catalog and runs either REPL or one-shot execution.
///
/// The database stores its files under `./data` relative to the current working directory.
///
/// # Panics
///
/// Does not intentionally panic, but may panic if stdout/stderr writes panic (rare) or if Rust
/// encounters unrecoverable runtime failures.
fn main() {
    let mut args = std::env::args().skip(1);
    let mode_or_sql = args.next();

    println!("StoreMy Database v{}", env!("CARGO_PKG_VERSION"));

    let data_dir = PathBuf::from("./data");
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        eprintln!("failed to create data dir {}: {e}", data_dir.display());
        std::process::exit(1);
    }

    let wal = Arc::new(
        Wal::new(&data_dir.join(WAL_FILE_NAME), 0).unwrap_or_else(|e| {
            eprintln!("failed to create WAL: {e}");
            std::process::exit(1);
        }),
    );
    let bp = Arc::new(PageStore::new(64, wal.clone()));

    let catalog = Catalog::initialize(&bp, &wal, &data_dir).unwrap_or_else(|e| {
        eprintln!("failed to initialize catalog: {e}");
        std::process::exit(1);
    });
    let txn_mgr = TransactionManager::new(wal, bp);

    let db = Database::new(Arc::new(catalog), Arc::new(txn_mgr), 4);

    match mode_or_sql.as_deref() {
        None | Some("repl") => run_repl(&db, &data_dir.join(REPL_HISTORY_FILE_NAME)),
        Some(first) => {
            let mut sql = String::from(first);
            for part in args {
                sql.push(' ');
                sql.push_str(&part);
            }
            execute_once(&db, &sql);
        }
    }
}

/// Executes one SQL statement and prints either a formatted success result or a formatted error.
///
/// This is used by both one-shot CLI mode and the REPL.
///
/// # Panics
///
/// Does not intentionally panic. A panic could still happen if the underlying channel receiver
/// panics due to runtime failures outside this function’s control.
fn execute_once(db: &Database, sql: &str) {
    let rx = db.execute(sql.to_string());
    match rx.recv() {
        Ok(Ok(result)) => {
            let mut t = Table::new();
            t.set_content_arrangement(ContentArrangement::Dynamic);
            t.set_header(vec![
                Cell::new("Result").add_attribute(comfy_table::Attribute::Bold),
            ]);

            t.add_row(vec![Cell::new(result.to_string())]);
            println!("{t}");
        }
        Ok(Err(e)) => {
            eprintln!("{}", format!("error: {e}").red().bold());
        }
        Err(e) => {
            eprintln!("{}", format!("worker disconnected: {e}").red().bold());
        }
    }
}

/// Runs the interactive SQL REPL with line editing and persisted history.
///
/// The REPL reads one line per statement (no multiline input yet), executes it, and prints the
/// result. History is loaded from and saved to `history_path`.
///
/// # Panics
///
/// Exits the process if the line editor cannot be initialized.
fn run_repl(db: &Database, history_path: &std::path::Path) {
    println!(
        "{}",
        r"
╔══════════════════════════════════════════════════════════════╗
║                          StoreMy DB                          ║
║                  a database built in Rust                    ║
╚══════════════════════════════════════════════════════════════╝
"
        .bright_blue()
    );
    println!("{}", "REPL mode. Type `exit` or `quit` to leave.".dimmed());
    println!(
        "{}",
        "Try: `CREATE TABLE users (id UINT64, name STRING)`".dimmed()
    );

    let mut rl = DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!(
            "{}",
            format!("failed to initialize line editor: {e}")
                .red()
                .bold()
        );
        std::process::exit(1);
    });

    let _ = rl.load_history(history_path);

    loop {
        let prompt = format!("{} ", "storemy>".bright_green().bold());
        let line = match rl.readline(&prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => continue, // Ctrl-C
            Err(ReadlineError::Eof) => break,            // Ctrl-D
            Err(e) => {
                eprintln!("{}", format!("failed to read input: {e}").red().bold());
                break;
            }
        };

        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }
        if matches!(sql, "exit" | "quit") {
            break;
        }

        rl.add_history_entry(sql).ok();
        execute_once(db, sql);
    }

    let _ = rl.save_history(history_path);
}
