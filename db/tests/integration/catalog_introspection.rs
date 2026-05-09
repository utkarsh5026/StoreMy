//! Integration tests for `Database::list_user_tables` and `describe_table` —
//! the public accessors backing the REPL's `\dt` and `\d <name>` commands.

use storemy::primitives::ColumnId;

use crate::common::TestDb;

#[test]
fn list_user_tables_is_empty_on_fresh_db() {
    let t = TestDb::new();
    let tables = t.db.list_user_tables().expect("list");
    assert!(
        tables.is_empty(),
        "expected no user tables, got: {tables:?}"
    );
}

#[test]
fn list_user_tables_returns_sorted_names() {
    let t = TestDb::new();
    t.run_ok("CREATE TABLE zebra (id INT NOT NULL, PRIMARY KEY (id));");
    t.run_ok("CREATE TABLE apple (id INT NOT NULL, name STRING, PRIMARY KEY (id));");
    t.run_ok("CREATE TABLE mango (id INT NOT NULL, PRIMARY KEY (id));");

    let names: Vec<String> =
        t.db.list_user_tables()
            .expect("list")
            .into_iter()
            .map(|info| info.name.to_string())
            .collect();

    assert_eq!(names, vec!["apple", "mango", "zebra"]);
}

#[test]
fn describe_table_returns_schema_and_primary_key() {
    let t = TestDb::new();
    t.run_ok("CREATE TABLE users (id INT NOT NULL, name STRING, age INT, PRIMARY KEY (id));");

    let info = t.db.describe_table("users").expect("describe");

    assert_eq!(info.name, "users");
    assert_eq!(info.schema.physical_num_fields(), 3);

    let fields: Vec<&str> = info.schema.fields().map(|f| f.name.as_str()).collect();
    assert_eq!(fields, vec!["id", "name", "age"]);

    let pk = info.primary_key.expect("PK should be set");
    assert_eq!(pk, vec![ColumnId::try_from(0usize).unwrap()]);
}

#[test]
fn describe_table_for_unknown_returns_error() {
    let t = TestDb::new();
    let err = t.db.describe_table("nonexistent").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("nonexistent") || msg.to_lowercase().contains("not found"),
        "unexpected error message: {msg}"
    );
}
