//! End-to-end tests for INSERT / UPDATE / DELETE.
//!
//! `SELECT` is not yet implemented in the engine, so we verify row state by
//! scanning the underlying heap directly via [`TestDb::scan_all`].

use storemy::{Value, engine::StatementResult};

use crate::common::TestDb;

fn expect_inserted(result: &StatementResult, expected_rows: usize) {
    let StatementResult::Inserted { rows, .. } = result else {
        panic!("expected Inserted, got {result:?}");
    };
    assert_eq!(*rows, expected_rows);
}

fn expect_deleted(result: &StatementResult, expected_rows: usize) {
    let StatementResult::Deleted { rows, .. } = result else {
        panic!("expected Deleted, got {result:?}");
    };
    assert_eq!(*rows, expected_rows);
}

fn expect_updated(result: &StatementResult, expected_rows: usize) {
    let StatementResult::Updated { rows, .. } = result else {
        panic!("expected Updated, got {result:?}");
    };
    assert_eq!(*rows, expected_rows);
}

#[test]
fn insert_single_row_is_visible_in_heap_scan() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, name STRING)");

    expect_inserted(&db.run_ok("INSERT INTO users VALUES (1, 'Alice')"), 1);

    let rows = db.scan_all("users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Int64(1)));
    assert_eq!(rows[0].get(1), Some(&Value::String("Alice".into())));
}

#[test]
fn insert_multiple_rows_in_one_statement() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");

    expect_inserted(
        &db.run_ok("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
        3,
    );
    assert_eq!(db.scan_all("t").len(), 3);
}

#[test]
fn insert_with_explicit_column_list() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");

    expect_inserted(
        &db.run_ok("INSERT INTO t (id, name) VALUES (42, 'answer')"),
        1,
    );

    let rows = db.scan_all("t");
    assert_eq!(rows[0].get(0), Some(&Value::Int64(42)));
    assert_eq!(rows[0].get(1), Some(&Value::String("answer".into())));
}

#[test]
fn delete_without_where_clears_table() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("INSERT INTO t VALUES (1), (2), (3)");

    expect_deleted(&db.run_ok("DELETE FROM t"), 3);
    assert!(db.scan_all("t").is_empty());
}

#[test]
fn delete_with_where_removes_only_matching_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    expect_deleted(&db.run_ok("DELETE FROM t WHERE id = 2"), 1);

    let mut remaining: Vec<i64> = db
        .scan_all("t")
        .iter()
        .map(|t| match t.get(0) {
            Some(Value::Int64(v)) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    remaining.sort_unstable();
    assert_eq!(remaining, vec![1, 3]);
}

#[test]
fn delete_with_no_matches_returns_zero() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("INSERT INTO t VALUES (1), (2)");

    expect_deleted(&db.run_ok("DELETE FROM t WHERE id = 99"), 0);
    assert_eq!(db.scan_all("t").len(), 2);
}

#[test]
fn update_without_where_rewrites_all_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'a'), (2, 'b')");

    expect_updated(&db.run_ok("UPDATE t SET name = 'z'"), 2);

    let names: Vec<_> = db
        .scan_all("t")
        .iter()
        .map(|t| t.get(1).cloned().unwrap())
        .collect();
    assert!(
        names
            .iter()
            .all(|v| matches!(v, Value::String(s) if s == "z"))
    );
}

#[test]
fn update_with_where_only_touches_matching_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    expect_updated(&db.run_ok("UPDATE t SET name = 'X' WHERE id = 2"), 1);

    // Pair (id, name) lookups so order-of-iteration doesn't matter.
    let mut pairs: Vec<(i64, String)> = db
        .scan_all("t")
        .iter()
        .map(|t| {
            let id = match t.get(0) {
                Some(Value::Int64(v)) => *v,
                other => panic!("expected Int64, got {other:?}"),
            };
            let name = match t.get(1) {
                Some(Value::String(s)) => s.clone(),
                other => panic!("expected String, got {other:?}"),
            };
            (id, name)
        })
        .collect();
    pairs.sort_unstable_by_key(|(id, _)| *id);

    assert_eq!(pairs, vec![
        (1, "a".to_string()),
        (2, "X".to_string()),
        (3, "c".to_string()),
    ]);
}

#[test]
fn insert_then_delete_then_reinsert_keeps_consistent_state() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");

    expect_inserted(&db.run_ok("INSERT INTO t VALUES (1), (2), (3)"), 3);
    expect_deleted(&db.run_ok("DELETE FROM t WHERE id > 1"), 2);
    expect_inserted(&db.run_ok("INSERT INTO t VALUES (10), (11)"), 2);

    let mut ids: Vec<i64> = db
        .scan_all("t")
        .iter()
        .map(|t| match t.get(0) {
            Some(Value::Int64(v)) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 10, 11]);
}
