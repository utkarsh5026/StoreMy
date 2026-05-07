//! Row and schema - the data model the executor produces and consumes.
//!
//! Every operator in [`crate::execution`] takes [`Tuple`]s in and produces
//! [`Tuple`]s out, all interpreted relative to a [`TupleSchema`].
//!
//! This module is intentionally split across:
//! - [`schema`] for [`Field`] and [`TupleSchema`]
//! - [`tuple`] for [`Tuple`] and row-oriented operations

use thiserror::Error;

use crate::types::Type;

mod schema;
#[allow(clippy::module_inception)]
mod tuple;

pub use schema::{Field, TupleSchema};
pub use tuple::Tuple;

/// Errors produced by row validation and column lookups.
///
/// Each variant maps to a specific SQL-level cause:
///
/// - `FieldIndexOutOfBounds` - the executor or binder asked for a column past the schema's width.
///   Usually means a `SELECT col` referenced something the resolver did not actually bind.
/// - `NullNotAllowed` - `NULL` written to a `NOT NULL` column (`INSERT` or `UPDATE`).
/// - `TypeMismatch` - value type differs from the column's declared type.
/// - `FieldCountMismatch` - `INSERT ... VALUES (...)` arity does not match the table's column list.
#[derive(Debug, Error)]
pub enum TupleError {
    #[error("field index {index} is out of bounds")]
    FieldIndexOutOfBounds { index: usize },

    #[error("null value not allowed for column '{column}'")]
    NullNotAllowed { column: String },

    #[error("type mismatch in column '{column}': expected {expected}, got {actual}")]
    TypeMismatch {
        column: String,
        expected: Type,
        actual: Type,
    },

    #[error("tuple has {actual} fields, expected {expected}")]
    FieldCountMismatch { expected: usize, actual: usize },
}
