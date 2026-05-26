use std::mem;

use super::{Engine, EngineError, StatementResult};
use crate::{
    parser::statements::{
        ReleaseSavepointStatement, RollbackStatement, SavepointStatement, Statement,
    },
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

            Statement::Savepoint(s) => Self::exec_savepoint(s, session),
            Statement::ReleaseSavepoint(s) => Self::exec_release_savepoint(s, session),

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
    fn exec_savepoint(
        stmt: SavepointStatement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        match &mut session.ctx {
            TxnContext::Explicit(txn) => {
                let name = stmt.name;
                txn.savepoint(name.clone())?;
                Ok(StatementResult::NoOp {
                    statement: format!("SAVEPOINT {name}"),
                })
            }
            TxnContext::Autocommit => Ok(StatementResult::Notice {
                message: "SAVEPOINT can only be used inside a transaction".into(),
            }),
            TxnContext::Aborted(_) => Err(EngineError::TransactionAborted),
        }
    }

    fn exec_release_savepoint(
        stmt: ReleaseSavepointStatement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        match &mut session.ctx {
            TxnContext::Explicit(txn) => {
                let name = stmt.name;
                txn.release_savepoint(name.as_str())?;
                Ok(StatementResult::NoOp {
                    statement: format!("RELEASE SAVEPOINT {name}"),
                })
            }
            TxnContext::Autocommit => Ok(StatementResult::Notice {
                message: "RELEASE SAVEPOINT can only be used inside a transaction".into(),
            }),
            TxnContext::Aborted(_) => Err(EngineError::TransactionAborted),
        }
    }

    fn exec_rollback(
        stmt: RollbackStatement,
        session: &mut Session,
    ) -> Result<StatementResult, EngineError> {
        match stmt {
            // Plain ROLLBACK — abort the whole transaction.
            RollbackStatement::Transaction { .. } => {
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
            // ROLLBACK TO [SAVEPOINT] <name> — partial undo back to a marker.
            RollbackStatement::Savepoint { name } => {
                if matches!(session.ctx, TxnContext::Autocommit) {
                    return Ok(StatementResult::Notice {
                        message: "ROLLBACK TO SAVEPOINT: there is no transaction in progress"
                            .into(),
                    });
                }

                // Remember whether we started in Aborted so we can restore it on error.
                let was_aborted = matches!(session.ctx, TxnContext::Aborted(_));

                // Take ownership of the transaction so we can call &mut methods on it.
                let old = mem::replace(&mut session.ctx, TxnContext::Autocommit);
                let mut txn = match old {
                    TxnContext::Explicit(t) | TxnContext::Aborted(t) => t,
                    TxnContext::Autocommit => unreachable!(),
                };

                // Locate the savepoint and trim the in-memory stack.
                // If the name is unknown, put the transaction back unchanged.
                let target_lsn = match txn.truncate_to_savepoint(name.as_str()) {
                    Ok(lsn) => lsn,
                    Err(e) => {
                        session.ctx = if was_aborted {
                            TxnContext::Aborted(txn)
                        } else {
                            TxnContext::Explicit(txn)
                        };
                        return Err(EngineError::Transaction(e));
                    }
                };

                match txn.partial_undo(target_lsn) {
                    Ok(()) => {
                        session.ctx = TxnContext::Explicit(txn);
                        Ok(StatementResult::NoOp {
                            statement: format!("ROLLBACK TO SAVEPOINT {name}"),
                        })
                    }
                    Err(e) => {
                        session.ctx = TxnContext::Aborted(txn);
                        Err(EngineError::Transaction(e))
                    }
                }
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
