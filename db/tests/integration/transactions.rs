//! End-to-end integration tests for explicit transaction control.
//!
//! These tests exercise `BEGIN` / `COMMIT` / `ROLLBACK`, savepoints, and the
//! abort-isolation state machine through the full pipeline: SQL text → parser
//! → [`Engine::execute_statement_in_session`] → WAL → heap pages.  They are the
//! only layer that can catch bugs where the session-state machine is correct but
//! page-level undo or WAL replay silently fails to restore the before-image.
//!
//! # Why these are integration tests, not unit tests
//!
//! The unit tests in `engine/tcl.rs` verify session-state transitions
//! (`Autocommit → Explicit → Aborted → Autocommit`) with no intervening data
//! records on the WAL.  They cannot catch bugs in:
//! - `partial_undo` restoring actual heap before-images after a `ROLLBACK TO SAVEPOINT`
//! - ROLLBACK reverting real INSERT/UPDATE/DELETE work on disk pages
//! - the abort-isolation fence rejecting DML correctly after a real statement failure
//!
//! # Setup
//!
//! `TestDb` (from `common`) owns the `TempDir`, catalog, and transaction manager.
//! Because `Database::execute()` uses the autocommit path and rejects TCL,
//! session-aware SQL runs through a thin `SessionDb` wrapper that calls
//! `Engine::execute_statement_in_session` directly.
//!
//! Data effects are verified with `TestDb::scan_all` — a heap scan in its own
//! autocommit transaction that is immune to the session under test.

use storemy::{
    Value,
    engine::{Engine, EngineError, StatementResult},
    parser::Parser,
    transaction::Session,
    types::FixedValue,
};

use crate::common::TestDb;

// ── shared infrastructure ─────────────────────────────────────────────────────

/// A `TestDb` paired with a single `Session` so we can run session-aware SQL.
struct SessionDb {
    db: TestDb,
    session: Session,
}

impl SessionDb {
    fn new() -> Self {
        Self {
            db: TestDb::new(),
            session: Session::default(),
        }
    }

    /// Parse and execute one SQL statement in the current session.
    fn run(&mut self, sql: &str) -> Result<StatementResult, EngineError> {
        let mut stmts = Parser::new(sql)
            .parse_all()
            .map_err(|e| EngineError::Parse(e.to_string()))?;
        if stmts.is_empty() {
            return Err(EngineError::Parse("empty input".into()));
        }
        let stmt = stmts.remove(0);
        let engine = Engine::new(&self.db.catalog, &self.db.txn_manager);
        engine.execute_statement_in_session(stmt, &mut self.session)
    }

    /// Same as [`run`] but panics on error — for happy-path statements.
    fn run_ok(&mut self, sql: &str) -> StatementResult {
        self.run(sql)
            .unwrap_or_else(|e| panic!("statement failed: {sql}\n  -> {e}"))
    }
}

/// Collect the first (id) column of a table as a sorted `Vec<i32>`.
///
/// Uses `TestDb::scan_all` which opens its own autocommit transaction, so it
/// sees committed state regardless of what the `Session` under test is doing.
fn scan_ids(db: &TestDb, table: &str) -> Vec<i32> {
    let mut ids: Vec<i32> = db
        .scan_all(table)
        .iter()
        .map(|t| match t.get(0) {
            Some(Value::Fixed(FixedValue::Int32(v))) => *v,
            other => panic!("expected Int32 id column, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

// ── BEGIN / COMMIT ────────────────────────────────────────────────────────────

/// After `BEGIN … INSERT … COMMIT`, the inserted row must be visible to an
/// independent heap scan — the commit must flush the page and record a COMMIT
/// WAL record that makes the change durable.
#[test]
fn committed_insert_is_visible_in_heap_scan() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    assert!(matches!(
        sdb.run_ok("BEGIN"),
        StatementResult::TransactionStarted
    ));
    sdb.run_ok("INSERT INTO t VALUES (42)");
    assert!(matches!(
        sdb.run_ok("COMMIT"),
        StatementResult::TransactionCommitted
    ));

    assert_eq!(scan_ids(&sdb.db, "t"), vec![42]);
}

/// Multiple DML statements inside one explicit transaction — all or nothing.
/// After COMMIT, every row must be present; the heap should not have partially
/// applied the work.
#[test]
fn committed_multi_statement_transaction_all_rows_visible() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)");
    sdb.run_ok("INSERT INTO t VALUES (2)");
    sdb.run_ok("INSERT INTO t VALUES (3)");
    sdb.run_ok("COMMIT");

    assert_eq!(scan_ids(&sdb.db, "t"), vec![1, 2, 3]);
}

/// UPDATE inside BEGIN … COMMIT — the modified value must be durably stored
/// after the transaction closes.
#[test]
fn committed_update_persists_new_value() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT, name STRING)");
    sdb.db.run_ok("INSERT INTO t VALUES (1, 'before')");

    sdb.run_ok("BEGIN");
    sdb.run_ok("UPDATE t SET name = 'after' WHERE id = 1");
    sdb.run_ok("COMMIT");

    let rows = sdb.db.scan_all("t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(1), Some(&Value::varchar("after".into())));
}

/// DELETE inside BEGIN … COMMIT — the deleted row must be gone from the heap
/// after the transaction closes.
#[test]
fn committed_delete_removes_row_permanently() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");
    sdb.db.run_ok("INSERT INTO t VALUES (1), (2), (3)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("DELETE FROM t WHERE id = 2");
    sdb.run_ok("COMMIT");

    assert_eq!(scan_ids(&sdb.db, "t"), vec![1, 3]);
}

// ── BEGIN / ROLLBACK ──────────────────────────────────────────────────────────

/// ROLLBACK after an INSERT must undo the write: the heap must be empty, not
/// contain the inserted row.  This exercises the WAL-backed undo path on an
/// actual heap page, which the unit tests do not touch.
#[test]
fn rolled_back_insert_is_not_visible_in_heap_scan() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (99)");
    assert!(matches!(
        sdb.run_ok("ROLLBACK"),
        StatementResult::TransactionRolledBack
    ));

    assert!(
        scan_ids(&sdb.db, "t").is_empty(),
        "rolled-back insert must leave heap empty"
    );
}

/// ROLLBACK after an UPDATE must restore the original value, not the new one.
#[test]
fn rolled_back_update_restores_original_value() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT, name STRING)");
    sdb.db.run_ok("INSERT INTO t VALUES (1, 'original')");

    sdb.run_ok("BEGIN");
    sdb.run_ok("UPDATE t SET name = 'changed' WHERE id = 1");
    sdb.run_ok("ROLLBACK");

    let rows = sdb.db.scan_all("t");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(1),
        Some(&Value::varchar("original".into())),
        "rollback must restore the original name"
    );
}

/// ROLLBACK after a DELETE must put the deleted rows back in the heap.
#[test]
fn rolled_back_delete_restores_deleted_rows() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");
    sdb.db.run_ok("INSERT INTO t VALUES (1), (2), (3)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("DELETE FROM t");
    sdb.run_ok("ROLLBACK");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1, 2, 3],
        "rolled-back DELETE must restore all rows"
    );
}

/// Multiple writes inside one transaction, all undone by a single ROLLBACK.
/// Verifies that the undo pass walks back through multiple WAL records.
#[test]
fn multi_statement_rollback_undoes_all_writes() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");
    sdb.db.run_ok("INSERT INTO t VALUES (1), (2)"); // committed before BEGIN

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (3)");
    sdb.run_ok("INSERT INTO t VALUES (4)");
    sdb.run_ok("DELETE FROM t WHERE id = 1");
    sdb.run_ok("ROLLBACK");

    // Only the two pre-BEGIN rows should remain
    assert_eq!(scan_ids(&sdb.db, "t"), vec![1, 2]);
}

// ── abort isolation ───────────────────────────────────────────────────────────

/// A DML statement that fails inside an explicit transaction (referencing a
/// non-existent column) must move the session to the Aborted state.  The next
/// non-TCL statement must be rejected with `TransactionAborted`.
#[test]
fn failed_dml_moves_session_to_aborted_and_blocks_further_dml() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    // INSERT into a non-existent table → error, session → Aborted
    let err = sdb
        .run("INSERT INTO ghost_table VALUES (1)")
        .expect_err("INSERT into non-existent table must fail");
    assert!(
        !matches!(err, EngineError::TransactionAborted),
        "first failure should be a real error (TableNotFound/Catalog), not TransactionAborted; got {err:?}"
    );

    // Next DML must be rejected outright, not executed
    let blocked = sdb
        .run("INSERT INTO t VALUES (1)")
        .expect_err("DML after abort must be rejected");
    assert!(
        matches!(blocked, EngineError::TransactionAborted),
        "expected TransactionAborted after session abort, got {blocked:?}"
    );

    // Nothing should have landed in the table
    assert!(
        scan_ids(&sdb.db, "t").is_empty(),
        "aborted transaction must not leave any rows"
    );
}

/// ROLLBACK after the Aborted state clears the session so new work can proceed.
#[test]
fn rollback_after_abort_clears_session_for_new_work() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    let _ = sdb.run("INSERT INTO ghost VALUES (1)"); // induces Aborted
    sdb.run_ok("ROLLBACK"); // clears to Autocommit

    // Should be able to work normally now (autocommit mode)
    sdb.run_ok("INSERT INTO t VALUES (7)");

    assert_eq!(scan_ids(&sdb.db, "t"), vec![7]);
}

/// Work committed before a later-aborted statement must survive.  The abort
/// state machine must not reach back and undo already-committed data.
#[test]
fn committed_work_before_aborted_txn_is_not_rolled_back() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");
    sdb.db.run_ok("INSERT INTO t VALUES (1)"); // autocommit — committed

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (2)");
    let _ = sdb.run("INSERT INTO ghost VALUES (99)"); // abort the explicit txn
    sdb.run_ok("ROLLBACK");

    // id=1 (committed before BEGIN) must survive; id=2 (aborted txn) must not
    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1],
        "committed row must survive; aborted row must not appear"
    );
}

// ── SAVEPOINTS ────────────────────────────────────────────────────────────────

/// Work done before a savepoint must persist; work done after it must be
/// undone when `ROLLBACK TO SAVEPOINT` is called.  This is the core property
/// of `partial_undo` operating on real page before-images.
#[test]
fn savepoint_rollback_undoes_post_savepoint_work_preserves_prior_work() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)"); // before savepoint — must survive
    sdb.run_ok("SAVEPOINT sp");
    sdb.run_ok("INSERT INTO t VALUES (2)"); // after savepoint — must be undone
    sdb.run_ok("INSERT INTO t VALUES (3)"); // after savepoint — must be undone
    sdb.run_ok("ROLLBACK TO SAVEPOINT sp");
    sdb.run_ok("COMMIT");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1],
        "only the pre-savepoint row must survive"
    );
}

/// After `ROLLBACK TO SAVEPOINT`, the transaction must stay open.  New DML
/// submitted after the rollback must be committed successfully.
#[test]
fn savepoint_rollback_keeps_transaction_open_for_continued_work() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)");
    sdb.run_ok("SAVEPOINT sp");
    sdb.run_ok("INSERT INTO t VALUES (2)"); // will be undone
    sdb.run_ok("ROLLBACK TO SAVEPOINT sp");
    // Transaction is still open — can insert more
    sdb.run_ok("INSERT INTO t VALUES (3)");
    sdb.run_ok("COMMIT");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1, 3],
        "pre-savepoint and post-rollback rows must survive; mid-savepoint row must not"
    );
}

/// Nested savepoints: set sp1, do work, set sp2, do more work.
/// `ROLLBACK TO SAVEPOINT sp1` must undo everything since sp1 — including
/// the work done between sp1 and sp2, and the work done after sp2.
#[test]
fn nested_savepoints_outer_rollback_undoes_all_inner_work() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)"); // before sp1 — survives
    sdb.run_ok("SAVEPOINT sp1");
    sdb.run_ok("INSERT INTO t VALUES (2)"); // between sp1 and sp2 — undone
    sdb.run_ok("SAVEPOINT sp2");
    sdb.run_ok("INSERT INTO t VALUES (3)"); // after sp2 — undone
    sdb.run_ok("ROLLBACK TO SAVEPOINT sp1");
    sdb.run_ok("COMMIT");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1],
        "outer savepoint rollback must undo all nested work"
    );
}

/// `ROLLBACK TO SAVEPOINT sp2` (the inner savepoint) must undo only work since
/// sp2, leaving the work between sp1 and sp2 intact.
#[test]
fn nested_savepoints_inner_rollback_undoes_only_inner_work() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)"); // before sp1 — survives
    sdb.run_ok("SAVEPOINT sp1");
    sdb.run_ok("INSERT INTO t VALUES (2)"); // between sp1 and sp2 — survives
    sdb.run_ok("SAVEPOINT sp2");
    sdb.run_ok("INSERT INTO t VALUES (3)"); // after sp2 — undone
    sdb.run_ok("ROLLBACK TO SAVEPOINT sp2");
    sdb.run_ok("COMMIT");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1, 2],
        "inner savepoint rollback must keep work from before sp2"
    );
}

/// After `RELEASE SAVEPOINT`, the marker is gone.  Committing the transaction
/// makes all work (including what was captured by the now-released savepoint)
/// permanent.
#[test]
fn release_savepoint_makes_all_work_visible_after_commit() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)");
    sdb.run_ok("SAVEPOINT sp");
    sdb.run_ok("INSERT INTO t VALUES (2)");
    sdb.run_ok("RELEASE SAVEPOINT sp");
    sdb.run_ok("COMMIT");

    assert_eq!(
        scan_ids(&sdb.db, "t"),
        vec![1, 2],
        "releasing a savepoint does not undo its work — both rows must survive commit"
    );
}

/// `ROLLBACK TO SAVEPOINT` on a released savepoint must return an error.
/// The transaction must stay open (not be silently aborted or reset).
#[test]
fn rollback_to_released_savepoint_returns_error_and_keeps_txn_open() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (1)");
    sdb.run_ok("SAVEPOINT sp");
    sdb.run_ok("RELEASE SAVEPOINT sp");

    let err = sdb
        .run("ROLLBACK TO SAVEPOINT sp")
        .expect_err("rolling back to a released savepoint must fail");
    assert!(
        matches!(err, EngineError::Transaction(_)),
        "expected Transaction error for unknown savepoint, got {err:?}"
    );

    // Transaction should still be alive — commit the work
    sdb.run_ok("COMMIT");
    assert_eq!(scan_ids(&sdb.db, "t"), vec![1]);
}

/// Savepoint declared inside an aborted session is rejected.
/// After `ROLLBACK`, the user can open a fresh transaction and use savepoints normally.
#[test]
fn savepoint_after_abort_is_rejected_rollback_recovers_session() {
    let mut sdb = SessionDb::new();
    sdb.db.run_ok("CREATE TABLE t (id INT)");

    sdb.run_ok("BEGIN");
    let _ = sdb.run("INSERT INTO ghost VALUES (1)"); // → Aborted

    let err = sdb
        .run("SAVEPOINT sp")
        .expect_err("SAVEPOINT in Aborted must be rejected");
    assert!(
        matches!(err, EngineError::TransactionAborted),
        "expected TransactionAborted, got {err:?}"
    );

    sdb.run_ok("ROLLBACK");

    // Fresh transaction + savepoint must work normally now
    sdb.run_ok("BEGIN");
    sdb.run_ok("INSERT INTO t VALUES (5)");
    sdb.run_ok("SAVEPOINT sp");
    sdb.run_ok("INSERT INTO t VALUES (6)");
    sdb.run_ok("ROLLBACK TO SAVEPOINT sp");
    sdb.run_ok("COMMIT");

    assert_eq!(scan_ids(&sdb.db, "t"), vec![5]);
}
