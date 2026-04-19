use std::fmt;

use thiserror::Error;

use crate::{
    FileId,
    buffer_pool::page_store::PageStoreError,
    catalog::{CatalogError, manager::Catalog},
    heap::file::HeapError,
    parser::statements::Statement,
    transaction::{Transaction, TransactionError, TransactionManager},
    tuple::Tuple,
};

mod ddl;
mod dml;

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
    Deleted {
        table: String,
        rows: usize,
    },
    Updated {
        table: String,
        rows: usize,
    },
    Selected {
        table: String,
        rows: Vec<Tuple>,
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

    pub(super) fn deleted(table: impl Into<String>, rows: usize) -> Self {
        Self::Deleted {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn updated(table: impl Into<String>, rows: usize) -> Self {
        Self::Updated {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn selected(table: impl Into<String>, rows: Vec<Tuple>) -> Self {
        Self::Selected {
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
            StatementResult::Deleted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "DELETE completed: removed {rows} {row_word} from heap table '{table}'",
                )
            }
            StatementResult::Updated { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "UPDATE completed: modified {rows} {row_word} in heap table '{table}'",
                )
            }
            StatementResult::Selected { table, rows } => {
                let row_word = if rows.len() == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "SELECT completed: returned {} {row_word} from '{table}'",
                    rows.len()
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

    #[error("type error: {0}")]
    TypeError(String),
}

impl EngineError {
    pub(super) fn column_not_found(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self::ColumnNotFound {
            table: table.into(),
            column: column.into(),
        }
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

    pub(super) fn type_error(message: impl Into<String>) -> Self {
        Self::TypeError(message.into())
    }
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
        Statement::CreateTable(s) => ddl::create_table(catalog, txn_manager, s),
        Statement::Drop(s) => ddl::drop_table(catalog, txn_manager, s),
        Statement::Insert(s) => dml::insert(catalog, txn_manager, s),
        Statement::Delete(s) => dml::delete(catalog, txn_manager, s),
        Statement::Update(s) => dml::update(catalog, txn_manager, s),
        _ => Err(EngineError::Unsupported(statement.to_string())),
    }
}
