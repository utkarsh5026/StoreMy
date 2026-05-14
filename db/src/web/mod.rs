//! HTTP layer over the `StoreMy` engine.
//!
//! Owns the JSON shapes (DTOs) the React frontend talks to, and the Axum
//! router/handlers that translate HTTP requests into [`DatabaseRegistry`]
//! calls. The engine itself does not depend on serde — DTOs are explicit
//! mirror types so we can change wire formats without touching core code.
//!
//! # Route layout
//!
//! ```text
//! GET  /api/databases           — list all databases
//! POST /api/databases           — create a new database
//! POST /api/:db/query           — execute a SQL string in database :db
//! GET  /api/:db/tables          — list user tables in :db
//! GET  /api/:db/tables/:name    — describe one table in :db
//! GET  /api/:db/heap/:name      — heap dump for :name in :db
//! ```

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::registry::DatabaseRegistry;

pub mod dto;
pub mod error;
pub mod handlers;

/// State shared with every Axum handler. Clone-cheap (it's an `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<DatabaseRegistry>,
}

/// Builds the Axum router for the `StoreMy` web API.
pub fn router(registry: Arc<DatabaseRegistry>) -> Router {
    let state = AppState { registry };
    Router::new()
        .route("/api/databases", get(handlers::databases::list_databases))
        .route("/api/databases", post(handlers::databases::create_database))
        .route("/api/:db/query", post(handlers::query::run_query))
        .route("/api/:db/tables", get(handlers::catalog::list_tables))
        .route(
            "/api/:db/tables/:name",
            get(handlers::catalog::describe_table),
        )
        .route("/api/:db/heap/:name", get(handlers::heap::dump_heap))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
