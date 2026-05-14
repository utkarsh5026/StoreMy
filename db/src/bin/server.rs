//! `storemy-server` — Axum HTTP front-end for the engine.
//!
//! On startup it initialises a [`DatabaseRegistry`] that scans the root data
//! directory for existing database subdirectories. Each named database lives
//! in its own folder (`DATA_DIR/<name>/`) and gets its own WAL, buffer pool,
//! catalog, and transaction manager.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin storemy-server                        # binds 127.0.0.1:7878
//! BIND_ADDR=0.0.0.0:9000 cargo run --bin storemy-server # custom address
//! DATA_DIR=/tmp/storemy cargo run --bin storemy-server  # custom data dir
//! ```

use std::{path::PathBuf, sync::Arc};

use storemy::{observability, registry::DatabaseRegistry, web};
use tracing::{error, info};

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

    let registry = match DatabaseRegistry::initialize(&data_dir) {
        Ok(r) => {
            let dbs = r.list();
            info!(
                data_dir = %data_dir.display(),
                databases = ?dbs,
                "registry initialized"
            );
            Arc::new(r)
        }
        Err(e) => {
            error!(error = %e, dir = %data_dir.display(), "failed to initialize database registry");
            std::process::exit(1);
        }
    };

    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, %bind, "failed to bind HTTP listener");
            std::process::exit(1);
        }
    };

    let app = web::router(registry);
    info!(%bind, "HTTP server ready");

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
