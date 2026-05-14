//! HTTP error responses.
//!
//! Engine errors carry rich information; we translate them into a uniform
//! JSON shape the frontend can render without knowing Rust types.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::engine::{ConstraintViolation, EngineError};

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Stable kind tag — useful for frontend to switch on.
    pub kind: &'static str,
    pub message: String,
}

#[derive(Debug)]
pub enum ApiError {
    Engine(EngineError),
    /// Worker pool dropped the reply channel before sending — should not
    /// happen in practice. Surfaced as 500.
    WorkerGone,
    /// Tokio failed to join the `spawn_blocking` task that called the engine.
    JoinError(String),
}

impl From<EngineError> for ApiError {
    fn from(e: EngineError) -> Self {
        ApiError::Engine(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, kind, message) = match self {
            ApiError::Engine(e) => engine_error_to_http(&e),
            ApiError::WorkerGone => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "worker_gone",
                "database worker pool shut down".to_string(),
            ),
            ApiError::JoinError(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                format!("background task failed: {m}"),
            ),
        };
        (status, Json(ErrorBody { kind, message })).into_response()
    }
}

/// Map engine errors to HTTP status + a stable string tag.
fn engine_error_to_http(e: &EngineError) -> (StatusCode, &'static str, String) {
    use EngineError as E;
    let (status, kind) = match e {
        E::Parse(_) => (StatusCode::BAD_REQUEST, "parse"),
        E::Unsupported(_) => (StatusCode::BAD_REQUEST, "unsupported"),
        E::TableNotFound(_) => (StatusCode::NOT_FOUND, "table_not_found"),
        E::TableAlreadyExists(_) => (StatusCode::CONFLICT, "table_exists"),
        E::IndexAlreadyExists(_) => (StatusCode::CONFLICT, "index_exists"),
        E::UnknownIndex(_) => (StatusCode::NOT_FOUND, "index_not_found"),
        E::PrimaryKeyAlreadyExists(_) => (StatusCode::CONFLICT, "primary_key_exists"),
        E::MultipleAutoIncrementColumns { .. } => {
            (StatusCode::BAD_REQUEST, "multiple_auto_increment")
        }
        E::InsertIntoAutoIncrementColumn { .. } | E::UpdateAutoIncrementColumn { .. } => {
            (StatusCode::BAD_REQUEST, "auto_increment_column_write")
        }
        E::UnknownColumn { .. } => (StatusCode::BAD_REQUEST, "column_not_found"),
        E::DuplicateColumn { .. } | E::DuplicateInsertColumn { .. } => {
            (StatusCode::BAD_REQUEST, "duplicate_column")
        }
        E::AmbiguousColumn { .. } => (StatusCode::BAD_REQUEST, "ambiguous_column"),
        E::WrongColumnCount { .. } => (StatusCode::BAD_REQUEST, "wrong_column_count"),
        E::MissingColumnDefault { .. } => (StatusCode::BAD_REQUEST, "missing_column_default"),
        E::TypeMismatch { .. } | E::TypeError(_) => (StatusCode::BAD_REQUEST, "type_error"),
        E::Constraint(c) => match c {
            ConstraintViolation::NullViolation { .. } => {
                (StatusCode::BAD_REQUEST, "null_violation")
            }
            ConstraintViolation::UniqueViolation { .. } => {
                (StatusCode::CONFLICT, "unique_violation")
            }
            ConstraintViolation::ForeignKeyViolation { .. } => {
                (StatusCode::CONFLICT, "fk_violation")
            }
            ConstraintViolation::FkParentViolation { .. } => {
                (StatusCode::CONFLICT, "fk_parent_violation")
            }
            ConstraintViolation::CheckViolation { .. } => {
                (StatusCode::BAD_REQUEST, "check_violation")
            }
        },
        E::Catalog(_)
        | E::Transaction(_)
        | E::Storage(_)
        | E::BufferPool(_)
        | E::Index(_)
        | E::Execution(_) => (StatusCode::INTERNAL_SERVER_ERROR, "engine"),
    };
    (status, kind, e.to_string())
}
