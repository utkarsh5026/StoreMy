//! Binder: turns a parsed [`Statement`] into a resolved [`Bound`] form.
//!
//! Runs between the parser and the executor. Errors here indicate the query
//! is syntactically valid SQL but refers to something that doesn't exist,
//! has a type mismatch, or violates a static rule. The executor can assume
//! every `Bound` value it receives is internally consistent.
use thiserror::Error;

use crate::{
    catalog::{CatalogError, manager::Catalog},
    parser::statements::Statement,
    transaction::Transaction,
};

mod binders;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BindError {
    #[error("unknown table '{0}'")]
    UnknownTable(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("column '{0}' appears more than once")]
    DuplicateColumn(String),

    #[error("primary key references unknown column '{0}'")]
    PrimaryKeyNotInColumns(String),

    #[error("type mismatch for column '{column}': expected {expected}, got {got}")]
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },

    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

impl BindError {
    pub(super) fn unknown_table(table: impl Into<String>) -> Self {
        Self::UnknownTable(table.into())
    }

    pub(super) fn table_already_exists(table: impl Into<String>) -> Self {
        Self::TableAlreadyExists(table.into())
    }

    pub(super) fn duplicate_column(column: impl Into<String>) -> Self {
        Self::DuplicateColumn(column.into())
    }
}

use crate::{FileId, tuple::TupleSchema};

#[non_exhaustive]
pub enum Bound {
    Drop(BoundDrop),
    CreateTable(BoundCreateTable),
}

/// Outcome of binding a `DROP TABLE`. Either the table was resolved (drop it)
/// or it was absent under `IF EXISTS` (no-op).
pub enum BoundDrop {
    /// The table exists and should be dropped. `file_id` is carried so the
    /// executor doesn't have to look it up again.
    Drop { name: String, file_id: FileId },
    /// `IF EXISTS` matched a missing table — the executor has nothing to do.
    NoOp { name: String },
}

/// Outcome of binding a `CREATE TABLE`. Either we fully resolved a new table
/// (build it) or `IF NOT EXISTS` matched an existing one (report it).
pub enum BoundCreateTable {
    New {
        name: String,
        schema: TupleSchema,
        primary_key: Option<Vec<usize>>,
    },
    AlreadyExists {
        name: String,
        file_id: FileId,
    },
}

/// Resolves `stmt` against `catalog` inside `txn`.
///
/// The transaction is required because catalog lookups may read system tables
/// on cache miss. Binding and execution should share the same transaction so
/// that the bound plan reflects the same catalog snapshot the executor sees.
pub fn bind(stmt: Statement, catalog: &Catalog, txn: &Transaction<'_>) -> Result<Bound, BindError> {
    match stmt {
        Statement::Drop(s) => Ok(Bound::Drop(binders::ddl::bind_drop(&s, catalog, txn)?)),
        Statement::CreateTable(s) => Ok(Bound::CreateTable(binders::ddl::bind_create_table(
            &s, catalog, txn,
        )?)),
        Statement::CreateIndex(_) => todo!("bind_create_index"),
        Statement::DropIndex(_) => todo!("bind_drop_index"),
        Statement::Delete(_) => todo!("bind_delete"),
        Statement::Insert(_) => todo!("bind_insert"),
        Statement::Update(_) => todo!("bind_update"),
        Statement::Select(_) => todo!("bind_select"),
        Statement::ShowIndexes(_) => todo!("bind_show_indexes"),
    }
}
