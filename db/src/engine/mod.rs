use crate::{
    binder::Bound,
    catalog::manager::Catalog,
    parser::statements::Statement,
    transaction::{Transaction, TransactionManager},
};

mod dml;
mod error;
mod query;
mod result;

pub use error::{ConstraintViolation, EngineError};
pub use result::{ShownIndex, StatementResult};

mod alter_table;
mod create_index;
mod create_table;
mod delete;
mod drop_index;
mod drop_table;
mod helpers;
mod scope;
mod show_indexes;

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
            Statement::CreateTable(s) => {
                self.with_txn(|txn| Engine::exec_create_table(s, txn, self.catalog))
            }
            Statement::Drop(s) => {
                self.with_txn(|txn| Engine::exec_drop_table(txn, self.catalog, s))
            }
            Statement::CreateIndex(s) => {
                self.with_txn(|txn| Engine::exec_create_index(txn, self.catalog, s))
            }
            Statement::DropIndex(s) => {
                self.with_txn(|txn| Engine::exec_drop_index(txn, self.catalog, s))
            }
            Statement::ShowIndexes(s) => {
                self.with_txn(|txn| Engine::exec_show_indexes(txn, self.catalog, s))
            }
            Statement::Insert(_) => self.exec_insert(stmt),
            Statement::Delete(_) => self.exec_delete(stmt),
            Statement::Update(s) => self.exec_update(s),
            Statement::Select(_) => self.exec_select(stmt),
            Statement::AlterTable(s) => {
                self.with_txn(|txn| Engine::exec_alter_table(txn, self.catalog, s))
            }
        };
        if let Err(e) = &result {
            tracing::warn!(error = %e, "statement failed");
        }
        result
    }

    pub(super) fn with_txn<F>(&self, run: F) -> Result<StatementResult, EngineError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<StatementResult, EngineError>,
    {
        let txn = self.txn_manager.begin()?;
        let result = run(&txn)?;
        txn.commit()?;
        Ok(result)
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
