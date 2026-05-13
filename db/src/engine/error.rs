use thiserror::Error;

use crate::{
    buffer_pool::page_store::PageStoreError, catalog::CatalogError, execution::ExecutionError,
    heap::file::HeapError, transaction::TransactionError,
};

/// Schema rule violations raised during DML execution.
///
/// Grouped as a sub-type so callers (e.g. the HTTP layer) can match on
/// "was this a constraint violation?" without enumerating every variant.
#[derive(Debug, Error)]
pub enum ConstraintViolation {
    #[error("null value in NOT NULL column '{column}' in table '{table}'")]
    NullViolation { table: String, column: String },

    #[error("duplicate value violates unique constraint '{constraint}'")]
    UniqueViolation { constraint: String },

    #[error(
        "insert violates foreign key constraint '{constraint}': \
         key not found in referenced table"
    )]
    ForeignKeyViolation { constraint: String },

    #[error(
        "delete or update on parent table violates foreign key constraint '{constraint}': \
         child rows still reference the old key"
    )]
    FkParentViolation { constraint: String },
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("unsupported statement: {0}")]
    Unsupported(String),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error(transparent)]
    Storage(#[from] HeapError),

    #[error(transparent)]
    BufferPool(#[from] PageStoreError),

    #[error(transparent)]
    Index(#[from] crate::index::IndexError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    // ── Schema / DDL ──────────────────────────────────────────────────────────
    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),

    #[error("index '{0}' not found")]
    UnknownIndex(String),

    #[error("table '{0}' already has a primary key")]
    PrimaryKeyAlreadyExists(String),

    // ── Column resolution ─────────────────────────────────────────────────────
    #[error("column '{column}' not found in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("column '{column}' appears more than once in the FROM/JOIN scope")]
    DuplicateColumn { table: String, column: String },

    #[error("column '{column}' is ambiguous: exists in multiple tables")]
    AmbiguousColumn { column: String },

    // ── DML / row validation ──────────────────────────────────────────────────
    #[error("column '{column}' appears more than once in INSERT into '{table}'")]
    DuplicateInsertColumn { table: String, column: String },

    #[error("wrong number of values for table '{table}': expected {expected}, got {got}")]
    WrongColumnCount {
        table: String,
        expected: usize,
        got: usize,
    },

    #[error("type mismatch for column '{column}': expected {expected}, got {got}")]
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },

    #[error("type error: {0}")]
    TypeError(String),

    #[error(transparent)]
    Constraint(#[from] ConstraintViolation),
}
