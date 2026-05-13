//! End-to-end integration tests for AUTO_INCREMENT.
//!
//! Verifies the full lifecycle: counter starts at 1, increments per insert,
//! persists across statements, survives DROP COLUMN cleanup, and rejects
//! explicit writes to the auto-increment column.

use storemy::{Value, engine::EngineError};

use crate::common::TestDb;

// ── basic counter behaviour ───────────────────────────────────────────────────

#[test]
fn auto_increment_assigns_sequential_ids_starting_at_one() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL)");

    db.run_ok("INSERT INTO users (name) VALUES ('alice')");
    db.run_ok("INSERT INTO users (name) VALUES ('bob')");
    db.run_ok("INSERT INTO users (name) VALUES ('carol')");

    let rows = db.scan_all("users");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].get(0),
        Some(&Value::Int64(1)),
        "first row id must be 1"
    );
    assert_eq!(
        rows[1].get(0),
        Some(&Value::Int64(2)),
        "second row id must be 2"
    );
    assert_eq!(
        rows[2].get(0),
        Some(&Value::Int64(3)),
        "third row id must be 3"
    );
}

#[test]
fn auto_increment_batch_insert_assigns_contiguous_ids() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, label STRING NOT NULL)");

    // Three rows in a single VALUES list — all get unique, contiguous IDs.
    db.run_ok("INSERT INTO t (label) VALUES ('x'), ('y'), ('z')");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get(0), Some(&Value::Int64(1)));
    assert_eq!(rows[1].get(0), Some(&Value::Int64(2)));
    assert_eq!(rows[2].get(0), Some(&Value::Int64(3)));
}

#[test]
fn auto_increment_counter_continues_across_separate_inserts() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, val STRING NOT NULL)");

    db.run_ok("INSERT INTO t (val) VALUES ('first')");
    db.run_ok("INSERT INTO t (val) VALUES ('second'), ('third')");
    db.run_ok("INSERT INTO t (val) VALUES ('fourth')");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 4);
    let ids: Vec<_> = rows.iter().map(|r| r.get(0).cloned().unwrap()).collect();
    assert_eq!(ids, vec![
        Value::Int64(1),
        Value::Int64(2),
        Value::Int64(3),
        Value::Int64(4),
    ]);
}

// ── non-ai columns get the values the user supplied ──────────────────────────

#[test]
fn auto_increment_non_id_columns_store_user_supplied_values() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE products (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL)");

    db.run_ok("INSERT INTO products (name) VALUES ('widget'), ('gadget')");

    let rows = db.scan_all("products");
    assert_eq!(rows[0].get(1), Some(&Value::String("widget".into())));
    assert_eq!(rows[1].get(1), Some(&Value::String("gadget".into())));
}

#[test]
fn auto_increment_works_when_ai_column_is_not_first() {
    let db = TestDb::new();
    // id is the second column, not the first.
    db.run_ok("CREATE TABLE t (name STRING NOT NULL, id INT NOT NULL AUTO_INCREMENT)");

    db.run_ok("INSERT INTO t (name) VALUES ('alice'), ('bob')");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 2);
    // name is slot 0, id is slot 1.
    assert_eq!(rows[0].get(0), Some(&Value::String("alice".into())));
    assert_eq!(rows[0].get(1), Some(&Value::Int64(1)));
    assert_eq!(rows[1].get(0), Some(&Value::String("bob".into())));
    assert_eq!(rows[1].get(1), Some(&Value::Int64(2)));
}

// ── constraint enforcement ────────────────────────────────────────────────────

#[test]
fn explicit_insert_into_auto_increment_column_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL)");

    let err = db
        .run("INSERT INTO t (id, name) VALUES (99, 'x')")
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InsertIntoAutoIncrementColumn { .. }),
        "expected InsertIntoAutoIncrementColumn, got {err:?}"
    );
}

#[test]
fn positional_insert_into_auto_increment_table_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL)");

    let err = db.run("INSERT INTO t VALUES (1, 'x')").unwrap_err();
    assert!(
        matches!(err, EngineError::InsertIntoAutoIncrementColumn { .. }),
        "expected InsertIntoAutoIncrementColumn, got {err:?}"
    );
}

#[test]
fn update_auto_increment_column_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL)");
    db.run_ok("INSERT INTO t (name) VALUES ('alice')");

    let err = db.run("UPDATE t SET id = 99").unwrap_err();
    assert!(
        matches!(err, EngineError::UpdateAutoIncrementColumn { .. }),
        "expected UpdateAutoIncrementColumn, got {err:?}"
    );
}

#[test]
fn multiple_auto_increment_columns_in_create_table_is_rejected() {
    let db = TestDb::new();
    let err = db
        .run("CREATE TABLE t (a INT NOT NULL AUTO_INCREMENT, b INT NOT NULL AUTO_INCREMENT)")
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MultipleAutoIncrementColumns { .. }),
        "expected MultipleAutoIncrementColumns, got {err:?}"
    );
}

// ── drop column cleans up the ai row ─────────────────────────────────────────

#[test]
fn drop_auto_increment_column_allows_normal_inserts_after() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, name STRING NOT NULL, score INT)");
    db.run_ok("INSERT INTO t (name, score) VALUES ('alice', 10)");

    // Drop the AUTO_INCREMENT column.
    db.run_ok("ALTER TABLE t DROP COLUMN id");

    // After the drop, INSERT no longer needs to omit id — full named insert works.
    db.run_ok("INSERT INTO t (name, score) VALUES ('bob', 20)");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 2);
}
