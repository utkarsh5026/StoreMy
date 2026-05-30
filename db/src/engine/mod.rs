use std::sync::Arc;

use crate::{
    catalog::manager::Catalog,
    parser::statements::Statement,
    transaction::{ActiveTransaction, TransactionManager},
};

mod error;
mod fk;
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
mod insert;
mod scope;
mod show_indexes;
mod tcl;
mod update;

/// Kind tag used when generating a default catalog constraint name.
#[derive(Clone, Copy)]
pub(super) enum ConstraintDefaultKind {
    Unique,
    Check,
    ForeignKey,
}

/// SQL execution entrypoint.
///
/// The engine holds references to the catalog and transaction manager so
/// statement executors can share common binding/transaction shells without
/// threading dependencies through every call.
pub struct Engine<'a> {
    catalog: &'a Catalog,
    txn_manager: &'a Arc<TransactionManager>,
}

impl<'a> Engine<'a> {
    pub fn new(catalog: &'a Catalog, txn_manager: &'a Arc<TransactionManager>) -> Self {
        Self {
            catalog,
            txn_manager,
        }
    }

    /// Dispatches a parsed statement in autocommit mode.
    ///
    /// Each mutating statement runs inside its own implicit transaction via
    /// `with_txn`. `SELECT` is routed to `exec_select`, wrapped the same way.
    ///
    /// Transaction-control statements (`BEGIN`, `COMMIT`, `ROLLBACK`, savepoints)
    /// are rejected here; session-aware callers should use
    /// [`execute_statement_in_session`](Self::execute_statement_in_session) instead.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for TCL statements, or any error
    /// from parsing, catalog I/O, constraint checks, or transaction management.
    #[tracing::instrument(
        name = "execute_statement",
        skip_all,
        fields(stmt = stmt.kind_name())
    )]
    pub fn execute_statement(&self, stmt: Statement) -> Result<StatementResult, EngineError> {
        let result = match stmt {
            Statement::Begin(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_) => Err(EngineError::Unsupported(
                "explicit transaction control is not implemented yet".into(),
            )),
            other => self.with_txn(|txn| Self::dispatch(txn, self.catalog, other)),
        };
        if let Err(e) = &result {
            tracing::warn!(error = %e, "statement failed");
        }
        result
    }

    /// Routes a single DML/DDL statement to its executor under an already-open transaction.
    ///
    /// Both [`Self::execute_statement`] (autocommit) and
    /// [`Self::exec_dml_in_explicit`](crate::engine::Engine::exec_dml_in_explicit)
    /// (explicit transaction) funnel through here so statement dispatch lives in
    /// exactly one place. TCL variants must be filtered out by callers before
    /// reaching this method.
    fn dispatch(
        txn: &ActiveTransaction,
        catalog: &Catalog,
        stmt: Statement,
    ) -> Result<StatementResult, EngineError> {
        match stmt {
            Statement::CreateTable(s) => Self::exec_create_table(txn, catalog, s),
            Statement::Drop(s) => Self::exec_drop_table(txn, catalog, s),
            Statement::CreateIndex(s) => Self::exec_create_index(txn, catalog, s),
            Statement::DropIndex(s) => Self::exec_drop_index(txn, catalog, s),
            Statement::ShowIndexes(s) => Self::exec_show_indexes(txn, catalog, s),
            Statement::Insert(s) => Self::exec_insert(txn, catalog, s),
            Statement::Delete(s) => Self::exec_delete(txn, catalog, s),
            Statement::Update(s) => Self::exec_update(txn, catalog, s),
            Statement::Select(s) => Self::exec_select(txn, catalog, s),
            Statement::AlterTable(s) => Self::exec_alter_table(txn, catalog, s),
            Statement::Begin(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_) => unreachable!("TCL routed before dispatch"),
        }
    }

    pub(super) fn with_txn<F>(&self, run: F) -> Result<StatementResult, EngineError>
    where
        F: FnOnce(&ActiveTransaction) -> Result<StatementResult, EngineError>,
    {
        let txn = self.txn_manager.begin()?;
        match run(&txn) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(run_err) => {
                if let Err(abort_err) = txn.abort() {
                    tracing::error!(error = %abort_err, "failed to write ABORT record after statement error");
                }
                Err(run_err)
            }
        }
    }
}
