use fallible_iterator::FallibleIterator;
use thiserror::Error;

use crate::{
    TransactionId,
    binder::{BindError, Bound},
    buffer_pool::page_store::PageStoreError,
    catalog::{CatalogError, manager::Catalog},
    execution::expression::BooleanExpression,
    heap::file::{HeapError, HeapFile},
    parser::statements::Statement,
    primitives::RecordId,
    transaction::{Transaction, TransactionError, TransactionManager},
    tuple::Tuple,
};

mod ddl;
mod dml;
mod query;
mod result;

pub use result::{ShownIndex, StatementResult};

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
    Bind(#[from] crate::binder::BindError),

    #[error(transparent)]
    Index(#[from] crate::index::IndexError),

    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    #[error("column '{column}' appears more than once in INSERT into '{table}'")]
    DuplicateInsertColumn { table: String, column: String },

    #[error("wrong number of values for table '{table}': expected {expected}, got {got}")]
    WrongColumnCount {
        table: String,
        expected: usize,
        got: usize,
    },

    #[error("null value in NOT NULL column '{column}' in table '{table}'")]
    NullViolation { table: String, column: String },

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
    pub(super) fn type_error(message: impl Into<String>) -> Self {
        Self::TypeError(message.into())
    }

    fn from_update_bind_error(error: BindError) -> Self {
        match error {
            BindError::UnknownColumn { table, column } => Self::ColumnNotFound { table, column },
            other => Self::Bind(other),
        }
    }
}

/// SQL execution entrypoint.
///
/// The engine holds references to the catalog and transaction manager so
/// statement executors can share common binding/transaction shells without
/// threading dependencies through every call.
pub struct Engine<'a> {
    catalog: &'a Catalog,
    txn_manager: &'a TransactionManager,
}

impl<'a> Engine<'a> {
    pub fn new(catalog: &'a Catalog, txn_manager: &'a TransactionManager) -> Self {
        Self {
            catalog,
            txn_manager,
        }
    }

    #[tracing::instrument(
        name = "execute_statement",
        skip_all,
        fields(stmt = stmt.kind_name())
    )]
    pub fn execute_statement(&self, stmt: Statement) -> Result<StatementResult, EngineError> {
        let result = match stmt {
            Statement::CreateTable(_) => self.exec_create_table(stmt),
            Statement::Drop(s) => self.exec_drop_table(s),
            Statement::CreateIndex(s) => self.exec_create_index(s),
            Statement::DropIndex(s) => self.exec_drop_index(s),
            Statement::ShowIndexes(s) => self.exec_show_indexes(s),
            Statement::Insert(_) => self.exec_insert(stmt),
            Statement::Delete(_) => self.exec_delete(stmt),
            Statement::Update(s) => self.exec_update(s),
            _ => Err(EngineError::Unsupported(stmt.to_string())),
        };
        if let Err(e) = &result {
            tracing::warn!(error = %e, "statement failed");
        }
        result
    }

    /// Binds `stmt` inside a fresh transaction, then runs the bound statement.
    ///
    /// This is the shared shell for statement executors with the same shape:
    /// begin a transaction, bind the AST against the catalog, execute the bound
    /// variant, then commit on success.
    fn bind_and_execute<F>(&self, stmt: Statement, run: F) -> Result<StatementResult, EngineError>
    where
        F: FnOnce(&Catalog, Bound, &Transaction<'_>) -> Result<StatementResult, EngineError>,
    {
        let is_update = matches!(&stmt, Statement::Update(_));
        let txn = self.txn_manager.begin()?;
        let bound = Bound::bind(stmt, self.catalog, &txn).map_err(|e| {
            if is_update {
                EngineError::from_update_bind_error(e)
            } else {
                EngineError::Bind(e)
            }
        })?;
        let out = run(self.catalog, bound, &txn)?;
        txn.commit()?;
        Ok(out)
    }
}

/// Collects every heap row visible to `transaction_id`, optionally filtered by `predicate`.
///
/// Rows with no predicate are included. When `predicate` is `Some`, a row is kept only if the
/// expression evaluates to true for that row's tuple.
///
/// # Errors
///
/// Returns [`EngineError`] if the heap scan cannot advance.
///
/// # Examples
///
/// Collect all rows without filtering:
///
/// ```ignore
/// let rows = collect_matching(&heap, txn_id, None)?;
/// // rows contains every (RecordId, Tuple) pair visible to the transaction
/// ```
///
/// Collect only rows where the first column equals `42`:
///
/// ```ignore
/// use storemy::{execution::expression::BooleanExpression, primitives::Predicate, types::Value};
///
/// let predicate = BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(42));
/// let rows = collect_matching(&heap, txn_id, Some(&predicate))?;
/// // rows contains only tuples whose first column is 42
/// ```
pub(super) fn collect_matching(
    heap: &HeapFile,
    transaction_id: TransactionId,
    predicate: Option<&BooleanExpression>,
) -> Result<Vec<(RecordId, Tuple)>, EngineError> {
    let mut scan = heap.scan(transaction_id)?;
    let mut out = Vec::new();
    while let Some((rid, tuple)) = FallibleIterator::next(&mut scan)? {
        let keep = match predicate {
            None => true,
            Some(p) => p
                .eval(&tuple)
                .map_err(|e| EngineError::type_error(e.to_string()))?,
        };
        if keep {
            out.push((rid, tuple));
        }
    }
    Ok(out)
}
