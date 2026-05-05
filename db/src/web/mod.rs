//! HTTP layer over the StoreMy engine.
//!
//! Owns the JSON shapes (DTOs) the React frontend talks to, and the Axum
//! router/handlers that translate HTTP requests into [`Database`](crate::database::Database)
//! calls. The engine itself does not depend on serde — DTOs are explicit
//! mirror types so we can change wire formats without touching core code.

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::database::Database;

pub mod dto;
pub mod error;
pub mod handlers;

/// State shared with every Axum handler. Clone-cheap (it's an `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
}

/// Builds the Axum router for the StoreMy web API.
///
/// Routes:
/// - `POST /api/query` — execute a SQL string.
/// - `GET  /api/tables` — list user tables.
/// - `GET  /api/tables/{name}` — describe a single user table.
pub fn router(db: Arc<Database>) -> Router {
    let state = AppState { db };
    Router::new()
        .route("/api/query", post(handlers::query::run_query))
        .route("/api/tables", get(handlers::catalog::list_tables))
        .route("/api/tables/:name", get(handlers::catalog::describe_table))
        .route("/api/heap/:name", get(handlers::heap::dump_heap))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
