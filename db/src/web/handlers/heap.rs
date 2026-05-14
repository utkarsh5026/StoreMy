//! `GET /api/heap/{table}` — dumps every page of a table's heap file.
//!
//! The heavy lifting (catalog lookup, transaction, page reads) lives on
//! [`crate::database::Database::read_heap_pages`]. The handler is just a
//! shape adapter: bytes in → JSON out.

use axum::{
    Json,
    extract::{Path, State},
};

use crate::web::{AppState, dto::HeapDumpDto, error::ApiError};

pub async fn dump_heap(
    State(state): State<AppState>,
    Path((db, name)): Path<(String, String)>,
) -> Result<Json<HeapDumpDto>, ApiError> {
    let db_handle = state
        .registry
        .get(&db)
        .ok_or(ApiError::DatabaseNotFound(db))?;

    let dump = tokio::task::spawn_blocking(move || {
        let (info, pages) = db_handle.read_heap_pages(&name)?;
        Ok::<HeapDumpDto, crate::engine::EngineError>(HeapDumpDto::build_full(
            &info.name,
            &info.schema,
            pages,
        ))
    })
    .await
    .map_err(|e| ApiError::JoinError(e.to_string()))??;

    Ok(Json(dump))
}
