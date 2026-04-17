use std::fmt;

use thiserror::Error;

use crate::{
    FileId,
    buffer_pool::page_store::PageStoreError,
    catalog::{CatalogError, manager::Catalog},
    engine::ddl::{create_table, drop_table},
    heap::file::HeapError,
    parser::statements::Statement,
    transaction::{Transaction, TransactionError, TransactionManager},
};

mod ddl;

#[derive(Debug)]
pub enum StatementResult {
    TableCreated {
        name: String,
        file_id: FileId,
        already_exists: bool,
    },
    TableDropped {
        name: String,
    },
    Inserted {
        table: String,
        rows: usize,
    },
}

impl StatementResult {
    pub(super) fn table_created(
        name: impl Into<String>,
        file_id: FileId,
        already_exists: bool,
    ) -> Self {
        Self::TableCreated {
            name: name.into(),
            file_id,
            already_exists,
        }
    }

    pub(super) fn table_dropped(name: impl Into<String>) -> Self {
        Self::TableDropped { name: name.into() }
    }

    pub(super) fn inserted(table: impl Into<String>, rows: usize) -> Self {
        Self::Inserted {
            table: table.into(),
            rows,
        }
    }
}

impl fmt::Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::TableCreated {
                name,
                file_id,
                already_exists,
            } => {
                if *already_exists {
                    write!(
                        f,
                        "CREATE TABLE completed: table '{name}' already exists (IF NOT EXISTS); using existing file {file_id}"
                    )
                } else {
                    write!(
                        f,
                        "CREATE TABLE completed: registered '{name}' in the catalog with backing file {file_id}"
                    )
                }
            }
            StatementResult::TableDropped { name } => write!(
                f,
                "DROP TABLE completed: removed '{name}' from the catalog and released its heap file"
            ),
            StatementResult::Inserted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "INSERT completed: wrote {rows} {row_word} into heap table '{table}'",
                )
            }
        }
    }
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

    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    #[error("wrong number of values for table '{table}': expected {expected}, got {got}")]
    WrongColumnCount {
        table: String,
        expected: usize,
        got: usize,
    },

    #[error("null value in NOT NULL column '{column}'")]
    NullViolation { column: String },

    #[error("type mismatch for column '{column}': expected {expected}, got {got}")]
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },
}

pub(super) fn with_txn<T, F>(f: F, txn_manager: &TransactionManager) -> Result<T, EngineError>
where
    F: FnOnce(&Transaction<'_>) -> Result<T, EngineError>,
{
    let txn = txn_manager.begin()?;
    let out = f(&txn)?;
    txn.commit()?;
    Ok(out)
}

pub fn execute_statement(
    catalog: &Catalog,
    statement: Statement,
    txn_manager: &TransactionManager,
) -> Result<StatementResult, EngineError> {
    match statement {
        Statement::CreateTable(statement) => create_table(catalog, txn_manager, statement),
        Statement::Drop(statement) => drop_table(catalog, txn_manager, statement),
        _ => Err(EngineError::Unsupported(statement.to_string())),
    }
}
