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

mod ddl;
mod dml;
pub mod expr;
mod helpers;
pub mod query;
mod scope;

pub use ddl::{
    BoundAlterTable, BoundCreateIndex, BoundCreateTable, BoundDrop, BoundDropIndex,
    BoundShowIndexes,
};
pub use dml::{BoundDelete, BoundInsert, BoundUpdate};
pub use expr::BoundExpr;
pub(in crate::binder) use helpers::{check_table, ensure_unique_strs, require_column};
pub use query::BoundSelect;

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

    #[error("column '{column}' appears more than once in the FROM/JOIN scope")]
    AmbiguousColumn { column: String },

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

    #[error("null value not allowed for column '{column}'")]
    NullViolation { table: String, column: String },

    #[error("column '{column}' appears more than once in INSERT into '{table}'")]
    DuplicateInsertColumn { table: String, column: String },

    #[error("wrong number of values for table '{table}': expected {expected}, got {got}")]
    WrongColumnCount {
        table: String,
        expected: usize,
        got: usize,
    },

    #[error("unsupported statement: {0}")]
    Unsupported(String),

    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),

    #[error("unknown index '{0}'")]
    UnknownIndex(String),

    #[error("table '{0}' already has a primary key")]
    PrimaryKeyAlreadyExists(String),
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

    pub(super) fn ambiguous_column(column: impl Into<String>) -> Self {
        Self::AmbiguousColumn {
            column: column.into(),
        }
    }

    pub(super) fn unknown_column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self::UnknownColumn {
            table: table.into(),
            column: column.into(),
        }
    }

    pub(super) fn primary_key_already_exists(table: impl Into<String>) -> Self {
        Self::PrimaryKeyAlreadyExists(table.into())
    }

    pub(super) fn wrong_column_count(
        table: impl Into<String>,
        expected: usize,
        got: usize,
    ) -> Self {
        Self::WrongColumnCount {
            table: table.into(),
            expected,
            got,
        }
    }
}

#[non_exhaustive]
pub enum Bound {
    Drop(BoundDrop),
    CreateTable(BoundCreateTable),
    CreateIndex(BoundCreateIndex),
    DropIndex(BoundDropIndex),
    ShowIndexes(BoundShowIndexes),
    Delete(BoundDelete),
    Insert(BoundInsert),
    Update(BoundUpdate),
    Select(BoundSelect),
    AlterTable(BoundAlterTable),
}

impl Bound {
    /// Resolves `stmt` against `catalog` inside `txn`.
    ///
    /// The transaction is required because catalog lookups may read system
    /// tables on cache miss. Binding and execution should share the same
    /// transaction so that the bound plan reflects the same catalog snapshot
    /// the executor sees.
    pub fn bind(
        stmt: Statement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        match stmt {
            Statement::Drop(s) => Ok(Self::Drop(BoundDrop::bind(&s, catalog, txn)?)),
            Statement::CreateTable(s) => {
                Ok(Self::CreateTable(BoundCreateTable::bind(&s, catalog, txn)?))
            }
            Statement::CreateIndex(s) => {
                Ok(Self::CreateIndex(BoundCreateIndex::bind(s, catalog, txn)?))
            }
            Statement::DropIndex(s) => Ok(Self::DropIndex(BoundDropIndex::bind(s, catalog, txn)?)),
            Statement::Delete(s) => Ok(Self::Delete(BoundDelete::bind(s, catalog, txn)?)),
            Statement::Insert(s) => Ok(Self::Insert(BoundInsert::bind(s, catalog, txn)?)),
            Statement::Update(s) => Ok(Self::Update(BoundUpdate::bind(s, catalog, txn)?)),
            Statement::Select(s) => Ok(Self::Select(BoundSelect::bind(s, catalog, txn)?)),
            Statement::ShowIndexes(s) => {
                Ok(Self::ShowIndexes(BoundShowIndexes::bind(&s, catalog, txn)?))
            }
            Statement::AlterTable(s) => {
                Ok(Self::AlterTable(BoundAlterTable::bind(s, catalog, txn)?))
            }
        }
    }

    /// Short, stable label for the bound shape — handy for logging, error
    /// messages, and tests that don't want to match every variant inline.
    ///
    /// The strings here mirror the SQL surface (`CREATE TABLE`, `DROP INDEX`,
    /// …) rather than the variant identifier so they read naturally in
    /// diagnostics aimed at end users.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Drop(_) => "DROP TABLE",
            Self::CreateTable(_) => "CREATE TABLE",
            Self::CreateIndex(_) => "CREATE INDEX",
            Self::DropIndex(_) => "DROP INDEX",
            Self::ShowIndexes(_) => "SHOW INDEXES",
            Self::Delete(_) => "DELETE",
            Self::Insert(_) => "INSERT",
            Self::Update(_) => "UPDATE",
            Self::Select(_) => "SELECT",
            Self::AlterTable(_) => "ALTER TABLE",
        }
    }
}
