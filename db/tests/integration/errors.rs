//! Tests for error paths through the `Database` facade — parser failures,
//! unsupported statements, and bind-time errors.

use storemy::engine::EngineError;

use crate::common::TestDb;

#[test]
fn select_returns_selected_result() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");

    let result = db.run("SELECT * FROM t").unwrap();
    assert!(
        matches!(result, storemy::engine::StatementResult::Selected { ref table, .. } if table == "t"),
        "expected Selected result, got {result:?}"
    );
}

#[test]
fn garbage_input_surfaces_parse_error() {
    let db = TestDb::new();
    let err = db.run("THIS IS NOT SQL").unwrap_err();
    assert!(
        matches!(err, EngineError::Parse(_)),
        "expected Parse, got {err:?}"
    );
}

#[test]
fn empty_input_surfaces_parse_error() {
    let db = TestDb::new();
    let err = db.run("").unwrap_err();
    assert!(matches!(err, EngineError::Parse(_)));
}

#[test]
fn insert_into_unknown_table_errors() {
    let db = TestDb::new();
    let err = db.run("INSERT INTO ghost VALUES (1)").unwrap_err();
    assert!(
        matches!(err, EngineError::Bind(_)),
        "expected Bind(UnknownTable), got {err:?}"
    );
}

#[test]
fn update_unknown_column_errors() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    let err = db.run("UPDATE t SET nope = 1").unwrap_err();
    assert!(
        matches!(err, EngineError::ColumnNotFound { .. }),
        "expected ColumnNotFound, got {err:?}"
    );
}
