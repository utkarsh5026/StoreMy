//! `POST /api/:db/query` — run a single SQL string through the engine.

use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;

use crate::web::{AppState, dto::QueryResultDto, error::ApiError};

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

/// Hands the SQL off to the worker pool and waits for the single reply.
///
/// `Database::execute` returns a synchronous [`std::sync::mpsc::Receiver`]; we
/// `recv` it on a blocking task so the async runtime stays responsive.
pub async fn run_query(
    State(state): State<AppState>,
    Path(db): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResultDto>, ApiError> {
    let sql_preview: String = req
        .sql
        .trim()
        .lines()
        .next()
        .unwrap_or("")
        .chars()
        .take(120)
        .collect();
    tracing::info!(db = %db, sql = %sql_preview, "query");

    let db_handle = state.registry.get(&db).ok_or_else(|| {
        tracing::warn!(db = %db, "database not found");
        ApiError::DatabaseNotFound(db.clone())
    })?;

    let result = tokio::task::spawn_blocking(move || {
        let rx = db_handle.execute(req.sql);
        rx.recv()
    })
    .await
    .map_err(|e| ApiError::JoinError(e.to_string()))?
    .map_err(|_| ApiError::WorkerGone)?;

    match result {
        Ok(r) => {
            tracing::info!(kind = r.kind_name(), summary = %r.log_summary(), "query ok");
            Ok(Json(QueryResultDto::from(&r)))
        }
        Err(e) => {
            tracing::warn!(error = %e, "query error");
            Err(ApiError::Engine(e))
        }
    }
}
