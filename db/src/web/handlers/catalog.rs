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

/// `GET /api/:db/tables` — list every user table in the named database.
pub async fn list_tables(
    State(state): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<Vec<TableSummaryDto>>, ApiError> {
    let db_handle = state
        .registry
        .get(&db)
        .ok_or(ApiError::DatabaseNotFound(db))?;

    let tables = tokio::task::spawn_blocking(move || db_handle.list_user_tables())
        .await
        .map_err(|e| ApiError::JoinError(e.to_string()))??;

    Ok(Json(tables.iter().map(TableSummaryDto::from).collect()))
}

/// `GET /api/:db/tables/:name` — describe one table in the named database.
pub async fn describe_table(
    State(state): State<AppState>,
    Path((db, name)): Path<(String, String)>,
) -> Result<Json<TableInfoDto>, ApiError> {
    let db_handle = state
        .registry
        .get(&db)
        .ok_or(ApiError::DatabaseNotFound(db))?;

    let info = tokio::task::spawn_blocking(move || db_handle.describe_table(&name))
        .await
        .map_err(|e| ApiError::JoinError(e.to_string()))??;

    Ok(Json(TableInfoDto::from(&info)))
}
