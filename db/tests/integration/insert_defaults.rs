//! End-to-end tests for INSERT … DEFAULT VALUES and partial named-column INSERT.
//!
//! These tests drive the full stack — SQL string → parser → engine → heap —
//! and verify the stored tuple values by scanning the heap directly.

use storemy::{Value, engine::EngineError};

use crate::common::TestDb;

// ── INSERT … DEFAULT VALUES ──────────────────────────────────────────────────

#[test]
fn default_values_fills_every_column_from_schema_defaults() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE settings (theme STRING DEFAULT 'dark', font_size INT DEFAULT 14)");

    db.run_ok("INSERT INTO settings DEFAULT VALUES");

    let rows = db.scan_all("settings");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::String("dark".into())));
    assert_eq!(rows[0].get(1), Some(&Value::Int64(14)));
}

#[test]
fn default_values_uses_null_for_nullable_column_without_default() {
    let db = TestDb::new();
    // `notes` is nullable with no DEFAULT — must become NULL.
    db.run_ok("CREATE TABLE t (score INT DEFAULT 0, notes STRING)");

    db.run_ok("INSERT INTO t DEFAULT VALUES");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Int64(0)));
    assert_eq!(rows[0].get(1), Some(&Value::Null));
}

#[test]
fn default_values_inserts_exactly_one_row() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE counters (n INT DEFAULT 0)");

    // Two separate DEFAULT VALUES statements → two rows.
    db.run_ok("INSERT INTO counters DEFAULT VALUES");
    db.run_ok("INSERT INTO counters DEFAULT VALUES");

    assert_eq!(db.scan_all("counters").len(), 2);
}

#[test]
fn default_values_rejects_not_null_column_without_default() {
    let db = TestDb::new();
    // `id` is NOT NULL with no DEFAULT — DEFAULT VALUES must be rejected.
    db.run_ok("CREATE TABLE t (id INT NOT NULL, label STRING DEFAULT 'x')");

    let err = db.run("INSERT INTO t DEFAULT VALUES").unwrap_err();
    assert!(
        matches!(err, EngineError::MissingColumnDefault { ref column, .. } if column == "id"),
        "expected MissingColumnDefault for 'id', got {err:?}"
    );
}

#[test]
fn default_values_works_with_mixed_types() {
    let db = TestDb::new();
    db.run_ok(
        "CREATE TABLE profile (\
            active  BOOLEAN DEFAULT true, \
            score   FLOAT   DEFAULT 1.5, \
            handle  STRING  DEFAULT 'anon'\
        )",
    );

    db.run_ok("INSERT INTO profile DEFAULT VALUES");

    let rows = db.scan_all("profile");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Bool(true)));
    assert_eq!(rows[0].get(1), Some(&Value::Float64(1.5)));
    assert_eq!(rows[0].get(2), Some(&Value::String("anon".into())));
}

// ── Partial named-column INSERT ───────────────────────────────────────────────

#[test]
fn partial_insert_omitting_defaulted_columns_uses_their_defaults() {
    let db = TestDb::new();
    db.run_ok(
        "CREATE TABLE users (\
            id     INT  NOT NULL, \
            role   STRING  DEFAULT 'viewer', \
            active BOOLEAN DEFAULT true\
        )",
    );

    // Only supply `id`; `role` and `active` must come from their defaults.
    db.run_ok("INSERT INTO users (id) VALUES (42)");

    let rows = db.scan_all("users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Int64(42)));
    assert_eq!(rows[0].get(1), Some(&Value::String("viewer".into())));
    assert_eq!(rows[0].get(2), Some(&Value::Bool(true)));
}

#[test]
fn partial_insert_omitting_nullable_column_stores_null() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL, notes STRING)");

    db.run_ok("INSERT INTO t (id) VALUES (7)");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Int64(7)));
    assert_eq!(rows[0].get(1), Some(&Value::Null));
}

#[test]
fn partial_insert_explicitly_supplied_value_overrides_default() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL, role STRING DEFAULT 'viewer')");

    // Explicitly supply 'admin' — the default must not clobber it.
    db.run_ok("INSERT INTO t (id, role) VALUES (1, 'admin')");

    let rows = db.scan_all("t");
    assert_eq!(rows[0].get(1), Some(&Value::String("admin".into())));
}

#[test]
fn partial_insert_rejects_omitting_not_null_column_without_default() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL, name STRING NOT NULL)");

    // `name` is NOT NULL with no default — omitting it must be an error.
    let err = db.run("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert!(
        matches!(err, EngineError::MissingColumnDefault { ref column, .. } if column == "name"),
        "expected MissingColumnDefault for 'name', got {err:?}"
    );
}

#[test]
fn partial_insert_multiple_rows_all_use_defaults() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT NOT NULL, tag STRING DEFAULT 'new')");

    db.run_ok("INSERT INTO t (id) VALUES (1), (2), (3)");

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.get(1), Some(&Value::String("new".into())));
    }
}
