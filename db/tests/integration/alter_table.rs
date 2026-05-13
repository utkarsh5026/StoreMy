//! End-to-end tests for `ALTER TABLE` — ADD COLUMN, DROP COLUMN, RENAME COLUMN, RENAME TABLE.
//!
//! Schema state is verified through [`storemy::database::Database::describe_table`] and,
//! for INSERT behavior, by checking that the engine accepts or rejects VALUES rows of
//! the right arity after schema mutations.

use storemy::{
    catalog::CatalogError,
    engine::{EngineError, StatementResult},
    types::Value,
};

use crate::common::TestDb;

/// Returns the logical field names for `table` (dropped columns excluded).
fn logical_names(db: &TestDb, table: &str) -> Vec<String> {
    let info = db.db.describe_table(table).expect("describe_table");
    info.schema
        .logical_iter()
        .map(|f| f.name.as_str().to_owned())
        .collect()
}

#[test]
fn add_column_appends_to_schema() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");

    let result = db.run_ok("ALTER TABLE t ADD COLUMN age INT");
    assert!(
        matches!(result, StatementResult::ColumnAdded { ref table, ref column_name }
            if table == "t" && column_name == "age"),
        "unexpected result: {result:?}"
    );

    assert_eq!(logical_names(&db, "t"), ["id", "name", "age"]);
}

#[test]
fn add_column_increases_both_field_counts() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("ALTER TABLE t ADD COLUMN score FLOAT");

    let info = db.db.describe_table("t").unwrap();
    assert_eq!(info.schema.logical_num_fields(), 2);
    assert_eq!(info.schema.physical_num_fields(), 2);
}

#[test]
fn add_duplicate_column_is_an_error() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");

    let err = db.run("ALTER TABLE t ADD COLUMN name INT").unwrap_err();
    assert!(
        matches!(err, EngineError::DuplicateColumn { .. }),
        "expected DuplicateColumn for duplicate column name, got {err:?}"
    );
}

#[test]
fn insert_after_add_column_requires_new_column_value() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("ALTER TABLE t ADD COLUMN age INT");

    // Three logical columns → VALUES must supply three items.
    let result = db.run_ok("INSERT INTO t VALUES (1, 'alice', 30)");
    assert!(matches!(result, StatementResult::Inserted { rows: 1, .. }));

    // Two values is now too few.
    let err = db.run("INSERT INTO t VALUES (2, 'bob')").unwrap_err();
    assert!(
        matches!(err, EngineError::WrongColumnCount { .. }),
        "expected WrongColumnCount, got {err:?}"
    );
}

#[test]
fn drop_column_removes_it_from_logical_schema() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING, age INT)");

    let result = db.run_ok("ALTER TABLE t DROP COLUMN name");
    assert!(
        matches!(result, StatementResult::ColumnDropped { ref table, ref column_name }
            if table == "t" && column_name == "name"),
        "unexpected result: {result:?}"
    );

    // Logical schema excludes the dropped column.
    assert_eq!(logical_names(&db, "t"), ["id", "age"]);
}

#[test]
fn drop_column_keeps_physical_slot_but_reduces_logical_count() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING, age INT)");
    db.run_ok("ALTER TABLE t DROP COLUMN name");

    let info = db.db.describe_table("t").unwrap();
    // Physical layout still has three slots (old rows on disk have three values).
    assert_eq!(info.schema.physical_num_fields(), 3);
    // Only two columns are visible to SQL.
    assert_eq!(info.schema.logical_num_fields(), 2);
}

#[test]
fn insert_after_drop_column_requires_logical_arity_only() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING, age INT)");
    db.run_ok("ALTER TABLE t DROP COLUMN name");

    // Two logical columns remain → VALUES needs exactly two items.
    let result = db.run_ok("INSERT INTO t VALUES (1, 30)");
    assert!(matches!(result, StatementResult::Inserted { rows: 1, .. }));
}

#[test]
fn insert_too_many_values_after_drop_column_errors() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING, age INT)");
    db.run_ok("ALTER TABLE t DROP COLUMN name");

    // Still supplying three values (as if name were not dropped) must fail.
    let err = db.run("INSERT INTO t VALUES (1, 'alice', 30)").unwrap_err();
    assert!(
        matches!(err, EngineError::WrongColumnCount { .. }),
        "expected WrongColumnCount, got {err:?}"
    );
}

#[test]
fn drop_column_if_exists_on_missing_column_is_noop() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");

    // Without IF EXISTS this would be an error, but with it it is silently ignored.
    let result = db.run_ok("ALTER TABLE t DROP COLUMN IF EXISTS ghost");
    assert!(
        matches!(result, StatementResult::NoOp { .. }),
        "expected NoOp, got {result:?}"
    );
}

#[test]
fn drop_missing_column_without_if_exists_errors() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");

    let err = db.run("ALTER TABLE t DROP COLUMN ghost").unwrap_err();
    // The binder resolves the column against the schema before calling the catalog,
    // so it surfaces UnknownColumn rather than catalog's ColumnNotFound.
    assert!(
        matches!(
            err,
            EngineError::UnknownColumn { .. } | EngineError::Catalog(_)
        ),
        "expected an error for missing column, got {err:?}"
    );
}

#[test]
fn add_then_drop_same_column_restores_logical_schema() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("ALTER TABLE t ADD COLUMN age INT");
    db.run_ok("ALTER TABLE t DROP COLUMN age");

    // Back to two logical columns even though physical has three.
    let info = db.db.describe_table("t").unwrap();
    assert_eq!(info.schema.logical_num_fields(), 2);
    assert_eq!(info.schema.physical_num_fields(), 3);
    assert_eq!(logical_names(&db, "t"), ["id", "name"]);
}

#[test]
fn rename_column_updates_schema_name() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");

    let result = db.run_ok("ALTER TABLE t RENAME COLUMN name TO full_name");
    assert!(
        matches!(result, StatementResult::ColumnRenamed {
            ref table, ref old_name, ref new_name
        } if table == "t" && old_name == "name" && new_name == "full_name"),
        "unexpected result: {result:?}"
    );

    assert_eq!(logical_names(&db, "t"), ["id", "full_name"]);
}

#[test]
fn rename_column_old_name_no_longer_in_schema() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("ALTER TABLE t RENAME COLUMN name TO full_name");

    let info = db.db.describe_table("t").unwrap();
    assert!(
        info.schema.field_by_name("name").is_none(),
        "old column name should not be present after rename"
    );
    assert!(info.schema.field_by_name("full_name").is_some());
}

#[test]
fn rename_table_makes_new_name_accessible() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, name STRING)");

    let result = db.run_ok("ALTER TABLE users RENAME TO accounts");
    assert!(
        matches!(result, StatementResult::TableRenamed {
            ref old_name, ref new_name
        } if old_name == "users" && new_name == "accounts"),
        "unexpected result: {result:?}"
    );

    // New name is visible in the catalog.
    assert_eq!(logical_names(&db, "accounts"), ["id", "name"]);
}

#[test]
fn rename_table_old_name_no_longer_accessible() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT)");
    db.run_ok("ALTER TABLE users RENAME TO accounts");

    let err = db
        .db
        .describe_table("users")
        .expect_err("old name should be gone");
    assert!(
        matches!(
            err,
            EngineError::Catalog(CatalogError::TableNotFound { .. })
        ),
        "expected TableNotFound, got {err:?}"
    );
}

#[test]
fn alter_missing_table_if_exists_is_noop() {
    let db = TestDb::new();

    let result = db.run_ok("ALTER TABLE IF EXISTS ghost ADD COLUMN x INT");
    assert!(
        matches!(result, StatementResult::NoOp { .. }),
        "expected NoOp, got {result:?}"
    );
}

#[test]
fn alter_missing_table_without_if_exists_errors() {
    let db = TestDb::new();

    let err = db.run("ALTER TABLE ghost ADD COLUMN x INT").unwrap_err();
    assert!(
        matches!(err, EngineError::TableNotFound(_) | EngineError::Catalog(_)),
        "expected TableNotFound for missing table, got {err:?}"
    );
}

#[test]
fn insert_before_and_after_drop_column_rows_coexist() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, tag STRING, score INT)");

    // Row written with all three columns.
    db.run_ok("INSERT INTO t VALUES (1, 'alpha', 100)");

    // Drop the middle column.
    db.run_ok("ALTER TABLE t DROP COLUMN tag");

    // New row written with only the two logical columns.
    db.run_ok("INSERT INTO t VALUES (2, 200)");

    // Both rows must be present in the heap.
    let rows = db.scan_all("t");
    assert_eq!(
        rows.len(),
        2,
        "expected 2 rows after insert-drop-insert sequence"
    );
}

#[test]
fn select_star_after_drop_column_excludes_dropped_slot() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING, age INT)");
    db.run_ok("INSERT INTO t VALUES (1, 'alice', 30)");
    db.run_ok("ALTER TABLE t DROP COLUMN name");

    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { schema, rows, .. } = result else {
        panic!("expected Selected, got {result:?}");
    };

    // Output schema must only contain the two surviving columns.
    assert_eq!(
        schema.physical_num_fields(),
        2,
        "output schema must not carry the dropped slot"
    );
    let col_names: Vec<&str> = schema.fields().map(|f| f.name.as_str()).collect();
    assert_eq!(col_names, ["id", "age"]);

    // Each row must have exactly two values.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn select_star_after_add_then_drop_shows_only_live_columns() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("ALTER TABLE t ADD COLUMN age INT");
    db.run_ok("ALTER TABLE t DROP COLUMN name");
    db.run_ok("INSERT INTO t VALUES (1, 42)");

    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { schema, rows, .. } = result else {
        panic!("expected Selected, got {result:?}");
    };

    let col_names: Vec<&str> = schema.fields().map(|f| f.name.as_str()).collect();
    assert_eq!(col_names, ["id", "age"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn select_star_with_no_dropped_columns_is_unchanged() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'alice')");

    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { schema, rows, .. } = result else {
        panic!("expected Selected, got {result:?}");
    };

    assert_eq!(schema.physical_num_fields(), 2);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn multiple_add_columns_accumulate_in_order() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("ALTER TABLE t ADD COLUMN a INT");
    db.run_ok("ALTER TABLE t ADD COLUMN b STRING");
    db.run_ok("ALTER TABLE t ADD COLUMN c FLOAT");

    assert_eq!(logical_names(&db, "t"), ["id", "a", "b", "c"]);

    let info = db.db.describe_table("t").unwrap();
    assert_eq!(info.schema.logical_num_fields(), 4);
    assert_eq!(info.schema.physical_num_fields(), 4);
}

#[test]
fn add_column_with_default_old_rows_see_default_value() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'alice')");

    // Add a column with a literal default.
    db.run_ok("ALTER TABLE t ADD COLUMN score INT DEFAULT 42");

    // SELECT * must return the default for the pre-existing row.
    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { rows, .. } = result else {
        panic!("expected Selected");
    };

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].len(),
        3,
        "row should have three values after ADD COLUMN"
    );
    let score = rows[0].get(2).expect("index 2 must exist");
    assert_eq!(
        *score,
        Value::Int64(42),
        "old row must show the declared DEFAULT, got {score:?}"
    );
}

#[test]
fn add_column_without_default_old_rows_see_null() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("INSERT INTO t VALUES (1)");

    db.run_ok("ALTER TABLE t ADD COLUMN tag STRING");

    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { rows, .. } = result else {
        panic!("expected Selected");
    };

    assert_eq!(
        *rows[0].get(1).unwrap(),
        Value::Null,
        "no DEFAULT means NULL for old rows"
    );
}

#[test]
fn add_column_with_default_new_rows_use_supplied_value() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, name STRING)");
    db.run_ok("ALTER TABLE t ADD COLUMN score INT DEFAULT 99");

    // New row explicitly supplies 7; the default should not override it.
    db.run_ok("INSERT INTO t VALUES (1, 'bob', 7)");

    let result = db.run_ok("SELECT * FROM t");
    let StatementResult::Selected { rows, .. } = result else {
        panic!("expected Selected");
    };

    assert_eq!(
        *rows[0].get(2).unwrap(),
        Value::Int64(7),
        "supplied value must win over default"
    );
}

#[test]
fn add_column_default_survives_catalog_reconstruction() {
    // This checks that the default is persisted in the system catalog and
    // re-read correctly when the schema is reconstructed from ColumnRow records.
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT)");
    db.run_ok("ALTER TABLE t ADD COLUMN bonus INT DEFAULT 100");

    // Force schema reconstruction by reading the field directly.
    let info = db.db.describe_table("t").unwrap();
    let bonus_field = info
        .schema
        .field_by_name("bonus")
        .expect("bonus field must exist")
        .1;

    assert_eq!(
        bonus_field.missing_default_value,
        Some(Value::Int64(100)),
        "default must be stored in Field after catalog roundtrip"
    );
}
