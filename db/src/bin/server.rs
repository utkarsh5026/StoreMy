//! `storemy-server` — Axum HTTP front-end for the engine.
//!
//! Runs the same boot sequence as the REPL binary ([`crate::main`](../main.rs))
//! — same WAL, buffer pool, catalog, transaction manager, [`Database`] —
//! but instead of starting an interactive prompt it binds an HTTP server.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin storemy-server                        # binds 127.0.0.1:7878
//! BIND_ADDR=0.0.0.0:9000 cargo run --bin storemy-server # custom address
//! DATA_DIR=/tmp/storemy cargo run --bin storemy-server  # custom data dir
//! ```

use std::{path::PathBuf, sync::Arc};

use storemy::{
    buffer_pool::page_store::PageStore, catalog::manager::Catalog, database::Database,
    observability, transaction::TransactionManager, wal::writer::Wal, web,
};
use tracing::{error, info};

const WAL_FILE_NAME: &str = "wal.log";
const BUFFER_POOL_PAGES: usize = 1028;
const WORKER_THREADS: usize = 4;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:7878";

#[tokio::main]
async fn main() {
    let _trace_guards = observability::init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "StoreMy server starting"
    );

    let data_dir =
        std::env::var("DATA_DIR").map_or_else(|_| PathBuf::from("./data"), PathBuf::from);
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
    let db = Arc::new(Database::new(
        Arc::new(catalog),
        Arc::new(txn_mgr),
        WORKER_THREADS,
    ));

    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, %bind, "failed to bind HTTP listener");
            std::process::exit(1);
        }
    };

    let app = web::router(db);
    info!(%bind, "HTTP server ready");
    println!("StoreMy server listening on http://{bind}");

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(error = %e, "server error");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
