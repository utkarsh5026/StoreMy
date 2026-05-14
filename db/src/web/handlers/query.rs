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
    let db_handle = state
        .registry
        .get(&db)
        .ok_or(ApiError::DatabaseNotFound(db))?;

    let result = tokio::task::spawn_blocking(move || {
        let rx = db_handle.execute(req.sql);
        rx.recv()
    })
    .await
    .map_err(|e| ApiError::JoinError(e.to_string()))?
    .map_err(|_| ApiError::WorkerGone)??;

    Ok(Json(QueryResultDto::from(&result)))
}
