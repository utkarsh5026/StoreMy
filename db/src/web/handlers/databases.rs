//! `GET /api/databases` and `POST /api/databases` — registry endpoints.

use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;

use crate::web::{AppState, dto::DatabaseSummaryDto, error::ApiError};

/// `GET /api/databases` — list all known databases, each with their table names.
pub async fn list_databases(State(state): State<AppState>) -> Json<Vec<DatabaseSummaryDto>> {
    let names = state.registry.list();
    let summaries = names
        .into_iter()
        .map(|name| {
            let tables = state
                .registry
                .get(&name)
                .map(|db| {
                    db.list_user_tables()
                        .unwrap_or_default()
                        .iter()
                        .map(|t| t.name.to_string())
                        .collect()
                })
                .unwrap_or_default();
            DatabaseSummaryDto { name, tables }
        })
        .collect();
    Json(summaries)
}

#[derive(Debug, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
}

/// `POST /api/databases` — create a new database.
///
/// Returns 201 on success, 409 if a database with that name already exists,
/// 400 if the name is invalid.
pub async fn create_database(
    State(state): State<AppState>,
    Json(req): Json<CreateDatabaseRequest>,
) -> Result<(StatusCode, Json<DatabaseSummaryDto>), ApiError> {
    let registry = state.registry.clone();
    let name = req.name.clone();
    tokio::task::spawn_blocking(move || registry.create(&name))
        .await
        .map_err(|e| ApiError::JoinError(e.to_string()))?
        .map_err(ApiError::Registry)?;

    Ok((
        StatusCode::CREATED,
        Json(DatabaseSummaryDto {
            name: req.name,
            tables: vec![],
        }),
    ))
}
