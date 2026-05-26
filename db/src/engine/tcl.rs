use std::mem;

use super::{Engine, EngineError, StatementResult};
use crate::{
    parser::statements::{RollbackStatement, Statement},
    transaction::{Session, TxnContext},
};

impl Engine<'_> {
    /// Session-aware execution entry point.
    ///
    /// Routes TCL statements (`BEGIN`/`COMMIT`/`ROLLBACK`) to their dedicated
    /// handlers and dispatches DML/DDL to the right transaction context based
    /// on the session's current [`TxnContext`]:
    ///
    /// - [`TxnContext::Autocommit`]  — delegates to [`Self::execute_statement`], which wraps each
    ///   statement in its own implicit transaction.
    /// - [`TxnContext::Explicit`]    — runs the statement inside the open transaction via
    ///   `exec_dml_in_explicit`; transitions to `Aborted` on failure.
    /// - [`TxnContext::Aborted`]     — rejects every non-TCL statement with
    ///   [`EngineError::TransactionAborted`] until the user issues `ROLLBACK`.
    #[tracing::instrument(
        name = "execute_statement_in_session",
        skip_all,
        fields(stmt = stmt.kind_name())
    )]
    pub fn execute_statement_in_session(
        &self,
        stmt: Statement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        let result = match stmt {
            Statement::Begin(_) => self.exec_begin(session),
            Statement::Commit(_) => Self::exec_commit(session),
            Statement::Rollback(s) => Self::exec_rollback(s, session),

            Statement::Savepoint(_) | Statement::ReleaseSavepoint(_) => Err(
                EngineError::Unsupported("savepoints not yet implemented".into()),
            ),

            stmt => match &session.ctx {
                TxnContext::Autocommit => self.execute_statement(stmt),
                TxnContext::Explicit(_) => self.exec_dml_in_explicit(stmt, session),
                TxnContext::Aborted(_) => Err(EngineError::TransactionAborted),
            },
        };

        if let Err(e) = &result {
            tracing::warn!(error = %e, "statement failed");
        }
        result
    }

    fn exec_begin(&self, session: &mut Session) -> Result<StatementResult, EngineError> {
        if matches!(session.ctx, TxnContext::Autocommit) {
            let txn = self.txn_manager.begin()?;
            session.ctx = TxnContext::Explicit(txn);
            Ok(StatementResult::TransactionStarted)
        } else {
            Ok(StatementResult::Notice {
                message: "there is already a transaction in progress".into(),
            })
        }
    }

    fn exec_commit(session: &mut Session) -> Result<StatementResult, EngineError> {
        let old = mem::replace(&mut session.ctx, TxnContext::Autocommit);
        match old {
            TxnContext::Autocommit => Ok(StatementResult::Notice {
                message: "there is no transaction in progress".into(),
            }),
            TxnContext::Explicit(txn) => {
                txn.commit()?;
                Ok(StatementResult::TransactionCommitted)
            }
            // COMMIT on an aborted transaction acts as ROLLBACK.
            TxnContext::Aborted(txn) => {
                txn.abort()?;
                Ok(StatementResult::TransactionRolledBack)
            }
        }
    }

    /// Aborts the current explicit transaction and returns the session to
    /// [`TxnContext::Autocommit`].
    ///
    /// Works identically for both `Explicit` and `Aborted` contexts — the
    /// `ActiveTransaction`'s drop guard would abort anyway, but calling
    /// `abort()` explicitly lets the WAL error surface to the caller.
    fn exec_rollback(
        _stmt: RollbackStatement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        let old = mem::replace(&mut session.ctx, TxnContext::Autocommit);
        match old {
            TxnContext::Autocommit => Ok(StatementResult::Notice {
                message: "there is no transaction in progress".into(),
            }),
            TxnContext::Explicit(txn) | TxnContext::Aborted(txn) => {
                txn.abort()?;
                Ok(StatementResult::TransactionRolledBack)
            }
        }
    }

    /// Runs a non-TCL statement inside the already-open explicit transaction.
    ///
    /// Uses `mem::replace` to take ownership of the `ActiveTransaction` out of
    /// the session, runs the statement with it, then puts the session into
    /// either `Explicit` (success) or `Aborted` (failure) before returning.
    ///
    /// The result is returned **without** `?`-propagating before the state is
    /// restored — if we propagated early the transaction would be left in an
    /// inconsistent position.
    fn exec_dml_in_explicit(
        &self,
        stmt: Statement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        // Swap Explicit(txn) out; Autocommit is the zero-cost placeholder that
        // owns nothing, so it is always safe to drop if a panic occurs here.
        let old = mem::replace(&mut session.ctx, TxnContext::Autocommit);
        let TxnContext::Explicit(txn) = old else {
            unreachable!("exec_dml_in_explicit must only be called in Explicit state")
        };

        let c = self.catalog;
        let result = match stmt {
            Statement::Insert(s) => Self::exec_insert(&txn, c, s),
            Statement::Delete(s) => Self::exec_delete(&txn, c, s),
            Statement::Update(s) => Self::exec_update(&txn, c, s),
            Statement::Select(s) => Self::exec_select(&txn, c, s),
            Statement::CreateTable(s) => Self::exec_create_table(&txn, c, s),
            Statement::Drop(s) => Self::exec_drop_table(&txn, c, s),
            Statement::CreateIndex(s) => Self::exec_create_index(&txn, c, s),
            Statement::DropIndex(s) => Self::exec_drop_index(&txn, c, s),
            Statement::ShowIndexes(s) => Self::exec_show_indexes(&txn, c, s),
            Statement::AlterTable(s) => Self::exec_alter_table(&txn, c, s),
            Statement::Begin(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_) => {
                unreachable!("TCL is handled before exec_dml_in_explicit is called")
            }
        };

        session.ctx = if result.is_err() {
            TxnContext::Aborted(txn)
        } else {
            TxnContext::Explicit(txn)
        };

        result
    }
}
