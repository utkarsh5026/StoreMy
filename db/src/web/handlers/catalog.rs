//! Catalog endpoints — read-only views of the on-disk schema.

use axum::{
    Json,
    extract::{Path, State},
};

use crate::web::{
    AppState,
    dto::{TableInfoDto, TableSummaryDto},
    error::ApiError,
};

/// `GET /api/tables` — list every user table.
pub async fn list_tables(
    State(state): State<AppState>,
) -> Result<Json<Vec<TableSummaryDto>>, ApiError> {
    let db = state.db.clone();
    let tables = tokio::task::spawn_blocking(move || db.list_user_tables())
        .await
        .map_err(|e| ApiError::JoinError(e.to_string()))??;

    Ok(Json(tables.iter().map(TableSummaryDto::from).collect()))
}

/// `GET /api/tables/{name}` — describe one table.
pub async fn describe_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<TableInfoDto>, ApiError> {
    let db = state.db.clone();
    let info = tokio::task::spawn_blocking(move || db.describe_table(&name))
        .await
        .map_err(|e| ApiError::JoinError(e.to_string()))??;

    Ok(Json(TableInfoDto::from(&info)))
}
