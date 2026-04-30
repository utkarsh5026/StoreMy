//! End-to-end tests for DDL statements via the `Database` facade.

use storemy::engine::{EngineError, StatementResult};

use crate::common::TestDb;

#[test]
fn create_table_then_drop_round_trip() {
    let db = TestDb::new();

    let created = db.run_ok("CREATE TABLE users (id INT, name STRING)");
    let StatementResult::TableCreated {
        name,
        already_exists,
        ..
    } = created
    else {
        panic!("expected TableCreated, got {created:?}");
    };
    assert_eq!(name, "users");
    assert!(!already_exists);

    let dropped = db.run_ok("DROP TABLE users");
    assert!(matches!(dropped, StatementResult::TableDropped { name } if name == "users"));
}

#[test]
fn create_table_if_not_exists_is_idempotent() {
    let db = TestDb::new();

    db.run_ok("CREATE TABLE t (id INT)");
    let again = db.run_ok("CREATE TABLE IF NOT EXISTS t (id INT)");

    let StatementResult::TableCreated { already_exists, .. } = again else {
        panic!("expected TableCreated, got {again:?}");
    };
    assert!(
        already_exists,
        "second CREATE should report already_exists=true"
    );
}

#[test]
fn create_table_duplicate_without_if_not_exists_errors() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE dup (id INT)");

    let err = db.run("CREATE TABLE dup (id INT)").unwrap_err();
    assert!(
        matches!(err, EngineError::Bind(_)),
        "expected Bind(TableAlreadyExists), got {err:?}"
    );
}

#[test]
fn drop_missing_table_errors_without_if_exists() {
    let db = TestDb::new();
    let err = db.run("DROP TABLE ghost").unwrap_err();
    assert!(
        matches!(err, EngineError::Bind(_)),
        "expected Bind(UnknownTable), got {err:?}"
    );
}

#[test]
fn drop_missing_table_with_if_exists_is_noop() {
    let db = TestDb::new();
    let result = db.run_ok("DROP TABLE IF EXISTS ghost");
    assert!(matches!(result, StatementResult::TableDropped { name } if name == "ghost"));
}

#[test]
fn create_table_persists_schema_in_catalog() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE products (id INT, name STRING, price FLOAT)");

    let txn = db.txn_manager.begin().unwrap();
    let info = db.catalog.get_table_info(&txn, "products").unwrap();
    txn.commit().unwrap();

    let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "price"]);
}

#[test]
fn multiple_tables_get_distinct_file_ids() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE a (id INT)");
    db.run_ok("CREATE TABLE b (id INT)");

    let a = db.file_id_of("a");
    let b = db.file_id_of("b");
    assert_ne!(a, b, "distinct tables should map to distinct file ids");
}
