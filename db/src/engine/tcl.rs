//! Transaction-control (TCL) execution for a single database session.
//!
//! Maps `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`, and
//! `ROLLBACK TO SAVEPOINT` to [`Session`](crate::transaction::Session) state and
//! WAL-backed [`ActiveTransaction`](crate::transaction::ActiveTransaction)
//! handles. DML and DDL inside an explicit transaction share one transaction until
//! `COMMIT` or `ROLLBACK`.
//!
//! ## Shape
//!
//! - [`Session`](crate::transaction::Session) — per-connection state; holds
//!   [`TxnContext`](crate::transaction::TxnContext).
//! - [`TxnContext::Autocommit`](crate::transaction::TxnContext::Autocommit) — each non-TCL
//!   statement gets its own implicit transaction via [`Engine::execute_statement`].
//! - [`TxnContext::Explicit`](crate::transaction::TxnContext::Explicit) — open handle from `BEGIN`;
//!   subsequent inserts/updates/selects run against it.
//! - [`TxnContext::Aborted`](crate::transaction::TxnContext::Aborted) — a statement failed; only
//!   TCL cleanup (`ROLLBACK`, savepoint ops) is accepted.
//!
//! ## How it works
//!
//! [`Engine::execute_statement_in_session`] is the session-aware entry point (REPL,
//! web API). TCL statements are handled first, regardless of context. Everything
//! else is routed by the current [`TxnContext`](crate::transaction::TxnContext):
//!
//! 1. **Autocommit** — delegate to [`Engine::execute_statement`] (begin → run → commit per
//!    statement).
//! 2. **Explicit** — [`exec_dml_in_explicit`] runs the statement with the open handle; on failure
//!    the session moves to **Aborted**.
//! 3. **Aborted** — reject with [`EngineError::TransactionAborted`] until the user issues
//!    `ROLLBACK` (full or to savepoint).
//!
//! `ROLLBACK TO SAVEPOINT` trims the in-memory savepoint stack, then calls
//! [`ActiveTransaction::partial_undo`](crate::transaction::ActiveTransaction::partial_undo)
//! to walk the WAL backward and restore page images.
//!
//! ## NULL semantics
//!
//! TCL statements do not evaluate expressions; three-valued `NULL` logic does not apply.

use std::mem;

use super::{Engine, EngineError, StatementResult};
use crate::{
    parser::statements::{
        ReleaseSavepointStatement, RollbackStatement, SavepointStatement, Statement,
    },
    transaction::{Session, TxnContext},
};

impl Engine<'_> {
    /// Dispatches one parsed statement using the connection's transaction mode.
    ///
    /// Routes transaction-control SQL to dedicated handlers and runs DML/DDL
    /// against the correct [`TxnContext`].
    ///
    /// # SQL → routing
    ///
    /// ```sql
    /// -- BEGIN;                          → exec_begin → TransactionStarted
    /// -- COMMIT;                         → exec_commit → TransactionCommitted
    /// -- ROLLBACK;                       → exec_rollback (full) → TransactionRolledBack
    /// -- SAVEPOINT s1;                   → exec_savepoint
    /// -- RELEASE SAVEPOINT s1;           → exec_release_savepoint
    /// -- ROLLBACK TO SAVEPOINT s1;       → exec_rollback (partial undo)
    /// -- INSERT INTO users VALUES (...); → execute_statement OR exec_dml_in_explicit
    /// ```
    ///
    /// When [`Session`] is in autocommit, `INSERT` /
    /// `UPDATE` / `DELETE` / `SELECT` / DDL each get an implicit `BEGIN` … `COMMIT`
    /// via [`Self::execute_statement`]. After `BEGIN`, the same statements reuse the
    /// open [`ActiveTransaction`](crate::transaction::ActiveTransaction) until
    /// `COMMIT` or `ROLLBACK`.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TransactionAborted`] — a prior statement failed inside an explicit
    ///   transaction; only `ROLLBACK` / savepoint TCL is allowed until cleanup.
    /// - [`EngineError::Transaction`] — savepoint not found, WAL undo failure, etc.
    /// - Other [`EngineError`] variants — propagated from DML/DDL executors (catalog, constraints,
    ///   type errors) in either autocommit or explicit mode.
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

    /// Opens an explicit transaction when the session is in autocommit.
    ///
    /// ```sql
    /// -- BEGIN;
    /// --   TxnContext::Autocommit → Explicit(new ActiveTransaction)
    ///
    /// -- BEGIN;   -- again while already open
    /// --   Notice: "there is already a transaction in progress"
    /// ```
    ///
    /// `BEGIN TRANSACTION` parses to the same handler; the optional `TRANSACTION`
    /// keyword is not interpreted separately.
    ///
    /// # Errors
    ///
    /// - [`EngineError::Transaction`] — WAL `BEGIN` record could not be written.
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

    /// Commits the open explicit transaction, or surfaces a notice when none is open.
    ///
    /// ```sql
    /// -- COMMIT;   (session in Explicit)
    /// --   txn.commit() → TransactionCommitted → Autocommit
    ///
    /// -- COMMIT;   (session in Autocommit)
    /// --   Notice: "there is no transaction in progress"
    ///
    /// -- COMMIT;   (session in Aborted — failed INSERT, etc.)
    /// --   txn.abort() → TransactionRolledBack → Autocommit
    /// ```
    ///
    /// `COMMIT` on an **aborted** block does not try to commit dirty work; it rolls
    /// back, matching common SQL client behavior after an error.
    ///
    /// # Errors
    ///
    /// - [`EngineError::Transaction`] — WAL `COMMIT` or `ABORT` record could not be written.
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

    /// Records a named savepoint inside the current explicit transaction.
    ///
    /// ```sql
    /// -- SAVEPOINT s1;
    /// --   txn.savepoint("s1") → NoOp { statement: "SAVEPOINT s1" }
    ///
    /// -- SAVEPOINT s1;   (outside BEGIN)
    /// --   Notice: "SAVEPOINT can only be used inside a transaction"
    /// ```
    ///
    /// Redefining `s1` drops nested savepoints pushed after the previous `s1`
    /// (handled in
    /// [`ActiveTransaction::savepoint`](crate::transaction::ActiveTransaction::savepoint)).
    ///
    /// # Errors
    ///
    /// - [`EngineError::TransactionAborted`] — session is in the aborted state.
    /// - [`EngineError::Transaction`] — WAL savepoint record could not be written.
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

    /// Drops a savepoint marker without undoing work (`RELEASE SAVEPOINT`).
    ///
    /// ```sql
    /// -- RELEASE SAVEPOINT s1;
    /// --   txn.release_savepoint("s1") → NoOp { statement: "RELEASE SAVEPOINT s1" }
    ///
    /// -- RELEASE s1;   (PostgreSQL-style spelling — same parsed shape)
    ///
    /// -- RELEASE SAVEPOINT s1;   (no open transaction)
    /// --   Notice: "RELEASE SAVEPOINT can only be used inside a transaction"
    /// ```
    ///
    /// # Errors
    ///
    /// - [`EngineError::TransactionAborted`] — session is in the aborted state.
    /// - [`EngineError::Transaction`] — savepoint name not found on the stack.
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

    /// Ends the transaction or rolls back to a savepoint.
    ///
    /// Handles both full abort and partial undo:
    ///
    /// ```sql
    /// -- ROLLBACK;
    /// --   RollbackStatement::Transaction → abort whole txn → TransactionRolledBack
    ///
    /// -- ROLLBACK TO SAVEPOINT s1;
    /// --   RollbackStatement::Savepoint → truncate stack + partial_undo → NoOp
    ///
    /// -- ROLLBACK;   (no transaction)
    /// --   Notice: "there is no transaction in progress"
    /// ```
    ///
    /// Full `ROLLBACK` works from **Explicit** or **Aborted** and always returns
    /// to autocommit. `ROLLBACK TO SAVEPOINT` keeps the transaction open on success;
    /// on undo failure the session moves to **Aborted** so only cleanup TCL runs.
    ///
    /// Unknown savepoint names return [`EngineError::Transaction`] without changing
    /// autocommit vs. explicit vs. aborted (the handle is put back unchanged).
    ///
    /// # Errors
    ///
    /// - [`EngineError::Transaction`] — unknown savepoint, WAL read/CLR failure during partial
    ///   undo, corrupt before-image size.
    /// - [`EngineError::Transaction`] — WAL `ABORT` record could not be written (full rollback).
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

    /// Runs DML or DDL inside the open explicit transaction started by `BEGIN`.
    ///
    /// ```sql
    /// BEGIN;
    /// INSERT INTO users (id, name) VALUES (1, 'Ada');
    /// UPDATE users SET name = 'Grace' WHERE id = 1;
    /// SELECT * FROM users;
    /// COMMIT;
    /// ```
    ///
    /// Each statement between `BEGIN` and `COMMIT`/`ROLLBACK` maps to one call
    /// here with the same [`ActiveTransaction`](crate::transaction::ActiveTransaction).
    /// On success the session stays **Explicit**; on any `Err` it becomes **Aborted**
    /// (PostgreSQL-style "current transaction is aborted" until `ROLLBACK`).
    ///
    /// Uses `mem::replace` so the transaction handle can be mutably borrowed for
    /// savepoint rollback without leaving the session in a half-updated state. The
    /// result is returned only after `session.ctx` is restored — early `?` would
    /// strand the session in autocommit while the txn is still open.
    ///
    /// # Errors
    ///
    /// Propagates whatever the underlying executor returns (`INSERT`, `UPDATE`,
    /// `DELETE`, `SELECT`, `CREATE TABLE`, etc.). TCL statements never reach this
    /// function.
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

        let result = Self::dispatch(&txn, self.catalog, stmt);

        session.ctx = if result.is_err() {
            TxnContext::Aborted(txn)
        } else {
            TxnContext::Explicit(txn)
        };

        result
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the TCL execution layer in [`super`].
    //!
    //! Each test exercises the session-state machine in isolation: no real tables
    //! are required for the TCL handlers themselves (savepoint undo with no
    //! intervening data records is a natural no-op on the WAL). Tests that need an
    //! `Aborted` session build it directly with `txn_mgr.begin()` rather than
    //! running a failing DML statement, which keeps the fixtures minimal.
    //!
    //! Groups:
    //! - `exec_begin`            — autocommit → Explicit, double-BEGIN, BEGIN in Aborted
    //! - `exec_commit`           — happy path, no-txn notice, COMMIT on aborted acts as rollback
    //! - `exec_rollback`         — full abort from Explicit / Aborted / Autocommit
    //! - `exec_rollback_to_sp`   — autocommit notice, unknown name, success keeps Explicit
    //! - `exec_savepoint`        — Explicit / Autocommit / Aborted states
    //! - `exec_release_savepoint`— Explicit / Autocommit / Aborted / unknown-name
    //! - session routing         — non-TCL rejected in Aborted
    //! - savepoint stack         — release removes nested savepoints; redefine shadows earlier

    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        parser::{
            Parser,
            statements::{
                BeginStatement, CommitStatement, ReleaseSavepointStatement, RollbackStatement,
                SavepointStatement, Statement,
            },
        },
        primitives::NonEmptyString,
        transaction::{Session, TransactionManager, TxnContext},
        wal::writer::Wal,
    };

    // ── shared infrastructure ─────────────────────────────────────────────────

    fn make_setup(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, dir).expect("catalog init failed");
        let txn_mgr = Arc::new(TransactionManager::new(wal, bp, dir.join("wal.log")));
        (catalog, txn_mgr)
    }

    // ── statement constructors ────────────────────────────────────────────────
    //
    // Building Statement values directly is more explicit than parsing SQL in
    // every test and makes the test intent clearer.

    fn begin() -> Statement {
        Statement::Begin(BeginStatement { transaction: false })
    }

    fn commit() -> Statement {
        Statement::Commit(CommitStatement { transaction: false })
    }

    fn rollback() -> Statement {
        Statement::Rollback(RollbackStatement::Transaction { transaction: false })
    }

    fn rollback_to(name: &str) -> Statement {
        Statement::Rollback(RollbackStatement::Savepoint {
            name: NonEmptyString::new(name).unwrap(),
        })
    }

    fn savepoint(name: &str) -> Statement {
        Statement::Savepoint(SavepointStatement {
            name: NonEmptyString::new(name).unwrap(),
        })
    }

    fn release(name: &str) -> Statement {
        Statement::ReleaseSavepoint(ReleaseSavepointStatement {
            name: NonEmptyString::new(name).unwrap(),
        })
    }

    // Shorthand: run a statement and expect success.
    fn run(engine: &Engine<'_>, stmt: Statement, session: &mut Session) -> StatementResult {
        engine
            .execute_statement_in_session(stmt, session)
            .expect("statement should succeed")
    }

    // ── exec_begin ────────────────────────────────────────────────────────────

    /// `BEGIN` in autocommit opens an explicit transaction.
    #[test]
    fn begin_in_autocommit_returns_transaction_started_and_session_becomes_explicit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, begin(), &mut session);

        assert!(
            matches!(result, StatementResult::TransactionStarted),
            "expected TransactionStarted, got: {result:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Explicit(_)),
            "session should be Explicit after BEGIN"
        );
    }

    #[test]
    fn begin_while_explicit_returns_notice_and_keeps_session_explicit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        let result = run(&engine, begin(), &mut session);

        assert!(
            matches!(
                result,
                StatementResult::Notice { ref message }
                    if message.contains("already a transaction in progress")
            ),
            "second BEGIN should warn, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Explicit(_)));
    }

    /// `BEGIN` while the session is in the Aborted state also returns a notice
    /// (the aborted transaction counts as "a transaction in progress").
    #[test]
    fn begin_while_aborted_returns_notice_and_keeps_session_aborted() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let result = run(&engine, begin(), &mut session);

        assert!(
            matches!(result, StatementResult::Notice { .. }),
            "BEGIN in Aborted should return a notice, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Aborted(_)));
    }

    // ── exec_commit ───────────────────────────────────────────────────────────

    /// `COMMIT` on a live explicit transaction durably closes it and returns the
    /// session to autocommit.
    #[test]
    fn commit_explicit_returns_committed_and_resets_session_to_autocommit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        let result = run(&engine, commit(), &mut session);

        assert!(
            matches!(result, StatementResult::TransactionCommitted),
            "expected TransactionCommitted, got: {result:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Autocommit),
            "session should be Autocommit after COMMIT"
        );
    }

    /// `COMMIT` when no transaction is open returns a notice rather than an error.
    #[test]
    fn commit_in_autocommit_returns_notice() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, commit(), &mut session);

        assert!(
            matches!(
                result,
                StatementResult::Notice { ref message }
                    if message.contains("no transaction in progress")
            ),
            "COMMIT with no active txn should warn, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    #[test]
    fn commit_aborted_transaction_acts_as_rollback_and_resets_to_autocommit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let result = run(&engine, commit(), &mut session);

        assert!(
            matches!(result, StatementResult::TransactionRolledBack),
            "COMMIT on Aborted should roll back, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    // ── exec_rollback (full) ──────────────────────────────────────────────────

    /// Plain `ROLLBACK` on a live explicit transaction aborts it.
    #[test]
    fn rollback_explicit_returns_rolled_back_and_resets_to_autocommit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        let result = run(&engine, rollback(), &mut session);

        assert!(
            matches!(result, StatementResult::TransactionRolledBack),
            "expected TransactionRolledBack, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    /// `ROLLBACK` on an aborted-state transaction is the normal cleanup path
    /// after a failed DML statement.
    #[test]
    fn rollback_aborted_returns_rolled_back_and_resets_to_autocommit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let result = run(&engine, rollback(), &mut session);

        assert!(
            matches!(result, StatementResult::TransactionRolledBack),
            "ROLLBACK on Aborted should succeed, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    /// `ROLLBACK` with no transaction open returns a notice.
    #[test]
    fn rollback_in_autocommit_returns_notice() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, rollback(), &mut session);

        assert!(
            matches!(
                result,
                StatementResult::Notice { ref message }
                    if message.contains("no transaction in progress")
            ),
            "ROLLBACK with no active txn should warn, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    // ── exec_rollback_to_savepoint ────────────────────────────────────────────

    /// `ROLLBACK TO SAVEPOINT` with no transaction open returns a notice.
    #[test]
    fn rollback_to_savepoint_in_autocommit_returns_notice() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, rollback_to("sp1"), &mut session);

        assert!(
            matches!(result, StatementResult::Notice { .. }),
            "ROLLBACK TO SAVEPOINT outside txn should warn, got: {result:?}"
        );
        assert!(matches!(session.ctx, TxnContext::Autocommit));
    }

    /// An unknown savepoint name leaves the transaction open and the session in
    /// the Explicit state — it must not silently discard the transaction.
    #[test]
    fn rollback_to_unknown_savepoint_in_explicit_returns_error_and_keeps_session_explicit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);

        let err = engine
            .execute_statement_in_session(rollback_to("no_such"), &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Transaction(_)),
            "unknown savepoint should return a Transaction error, got: {err:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Explicit(_)),
            "session should stay Explicit after unknown savepoint"
        );
    }

    /// An unknown savepoint name in the Aborted state leaves the session in the
    /// Aborted state so the user still needs to run `ROLLBACK` to clean up.
    #[test]
    fn rollback_to_unknown_savepoint_in_aborted_returns_error_and_keeps_session_aborted() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let err = engine
            .execute_statement_in_session(rollback_to("ghost"), &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Transaction(_)),
            "unknown savepoint should return a Transaction error, got: {err:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Aborted(_)),
            "session should stay Aborted after unknown savepoint"
        );
    }

    /// `ROLLBACK TO SAVEPOINT` with no intervening data records is a WAL no-op
    /// (the undo loop hits the Savepoint record and breaks immediately). The
    /// session must stay Explicit.
    #[test]
    fn rollback_to_savepoint_with_no_intervening_work_returns_noop_and_keeps_explicit() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        run(&engine, savepoint("sp"), &mut session);

        let result = run(&engine, rollback_to("sp"), &mut session);

        assert!(
            matches!(result, StatementResult::NoOp { ref statement } if statement.contains("sp")),
            "expected NoOp for savepoint rollback with no work, got: {result:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Explicit(_)),
            "transaction should remain open after successful savepoint rollback"
        );
    }

    // ── exec_savepoint ────────────────────────────────────────────────────────

    /// `SAVEPOINT` inside an explicit transaction returns a `NoOp` that includes
    /// the savepoint name.
    #[test]
    fn savepoint_in_explicit_returns_noop_containing_name() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        let result = run(&engine, savepoint("my_sp"), &mut session);

        assert!(
            matches!(result, StatementResult::NoOp { ref statement } if statement.contains("my_sp")),
            "expected NoOp with savepoint name, got: {result:?}"
        );
    }

    /// `SAVEPOINT` outside a transaction returns a notice.
    #[test]
    fn savepoint_in_autocommit_returns_notice() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, savepoint("s1"), &mut session);

        assert!(
            matches!(
                result,
                StatementResult::Notice { ref message }
                    if message.contains("SAVEPOINT can only be used inside a transaction")
            ),
            "SAVEPOINT outside txn should warn, got: {result:?}"
        );
    }

    /// `SAVEPOINT` in an aborted session is rejected with `TransactionAborted`.
    /// The user must first `ROLLBACK` before issuing new savepoints.
    #[test]
    fn savepoint_in_aborted_session_returns_transaction_aborted_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let err = engine
            .execute_statement_in_session(savepoint("s1"), &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TransactionAborted),
            "SAVEPOINT in Aborted should return TransactionAborted, got: {err:?}"
        );
    }

    // ── exec_release_savepoint ────────────────────────────────────────────────

    /// `RELEASE SAVEPOINT` removes the named marker and returns a `NoOp`.
    #[test]
    fn release_savepoint_in_explicit_returns_noop_containing_name() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        run(&engine, savepoint("rel_sp"), &mut session);

        let result = run(&engine, release("rel_sp"), &mut session);

        assert!(
            matches!(result, StatementResult::NoOp { ref statement } if statement.contains("rel_sp")),
            "expected NoOp with savepoint name, got: {result:?}"
        );
    }

    /// `RELEASE SAVEPOINT` outside a transaction returns a notice.
    #[test]
    fn release_savepoint_in_autocommit_returns_notice() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let result = run(&engine, release("s1"), &mut session);

        assert!(
            matches!(
                result,
                StatementResult::Notice { ref message }
                    if message.contains("RELEASE SAVEPOINT can only be used inside a transaction")
            ),
            "RELEASE SAVEPOINT outside txn should warn, got: {result:?}"
        );
    }

    /// `RELEASE SAVEPOINT` in an aborted session is rejected with
    /// `TransactionAborted`.
    #[test]
    fn release_savepoint_in_aborted_session_returns_transaction_aborted_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        let err = engine
            .execute_statement_in_session(release("s1"), &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TransactionAborted),
            "RELEASE SAVEPOINT in Aborted should return TransactionAborted, got: {err:?}"
        );
    }

    /// `RELEASE SAVEPOINT` on an unknown name returns a `Transaction` error.
    #[test]
    fn release_unknown_savepoint_returns_transaction_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);

        let err = engine
            .execute_statement_in_session(release("does_not_exist"), &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Transaction(_)),
            "RELEASE on unknown savepoint should return Transaction error, got: {err:?}"
        );
    }

    // ── session routing ───────────────────────────────────────────────────────

    /// Any non-TCL statement while the session is Aborted is immediately rejected
    /// with `TransactionAborted` — the statement is not executed at all.
    #[test]
    fn non_tcl_statement_in_aborted_session_returns_transaction_aborted_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        let txn = txn_mgr.begin().unwrap();
        session.ctx = TxnContext::Aborted(txn);

        // Parse any non-TCL statement; the engine must reject it before running it.
        let stmt = Parser::new("CREATE TABLE foo (id INT)")
            .parse_all()
            .unwrap()
            .remove(0);

        let err = engine
            .execute_statement_in_session(stmt, &mut session)
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TransactionAborted),
            "non-TCL in Aborted should return TransactionAborted, got: {err:?}"
        );
        // The session must stay Aborted — no state change from a rejected statement.
        assert!(matches!(session.ctx, TxnContext::Aborted(_)));
    }

    // ── savepoint stack invariants ────────────────────────────────────────────

    /// Releasing `sp1` removes it *and* any savepoints pushed after it (sp2).
    /// Rolling back to either name must subsequently fail.
    #[test]
    fn release_savepoint_removes_it_and_all_later_savepoints_from_the_stack() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        run(&engine, savepoint("sp1"), &mut session);
        run(&engine, savepoint("sp2"), &mut session);

        // Releasing sp1 must also drop sp2 (it was nested after sp1).
        run(&engine, release("sp1"), &mut session);

        let err = engine
            .execute_statement_in_session(rollback_to("sp1"), &mut session)
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Transaction(_)),
            "rolling back to a released savepoint should fail, got: {err:?}"
        );
    }

    /// Declaring the same savepoint name twice replaces the earlier marker.
    /// Rolling back to the name undoes only work since the *second* declaration.
    #[test]
    fn redefining_savepoint_shadows_earlier_marker_and_rollback_to_name_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_setup(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);
        let mut session = Session::default();

        run(&engine, begin(), &mut session);
        run(&engine, savepoint("sp"), &mut session);
        // Redefine — should silently drop the original and push a new marker.
        run(&engine, savepoint("sp"), &mut session);

        let result = run(&engine, rollback_to("sp"), &mut session);

        assert!(
            matches!(result, StatementResult::NoOp { .. }),
            "rollback to redefined savepoint should succeed, got: {result:?}"
        );
        assert!(
            matches!(session.ctx, TxnContext::Explicit(_)),
            "session should remain Explicit after savepoint rollback"
        );
    }
}
