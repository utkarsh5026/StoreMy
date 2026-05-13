//! End-to-end tests for UNIQUE constraints.
//!
//! Covers two creation paths:
//!   - Inline in `CREATE TABLE` (column-level and table-level `UNIQUE`)
//!   - `ALTER TABLE ADD CONSTRAINT … UNIQUE`
//!
//! For each path we verify:
//!   1. The constraint appears in `TableInfo::unique_constraints` with the right columns.
//!   2. A backing B-tree index is created in the catalog and reachable by name.
//!   3. The backing index name follows the `{table}_{constraint}_idx` convention.
//!   4. Auto-generated constraint names follow `{table}_unique_{col}`.
//!   5. Dropping the constraint removes both the constraint row and the backing index.
//!   6. Trying to add a UNIQUE constraint on a table that already has duplicate data fails.

use storemy::{
    engine::{ConstraintViolation, EngineError, StatementResult},
    index::IndexKind,
    primitives::ColumnId,
};

use crate::common::TestDb;

fn col(id: usize) -> ColumnId {
    ColumnId::try_from(id).unwrap()
}

/// Returns the unique constraints registered on `table`.
fn unique_constraints(db: &TestDb, table: &str) -> Vec<storemy::catalog::UniqueConstraint> {
    let txn = db.txn_manager.begin().unwrap();
    let info = db.catalog.get_table_info(&txn, table).unwrap();
    txn.commit().unwrap();
    info.unique_constraints
}

/// Returns true when an index named `name` exists in the catalog's live index map.
fn index_exists(db: &TestDb, name: &str) -> bool {
    db.catalog.get_index_by_name(name).is_some()
}

#[test]
fn create_table_column_level_unique_registers_constraint() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, name STRING)");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].columns, vec![col(1)]);
}

#[test]
fn create_table_column_level_unique_auto_name_follows_convention() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, name STRING)");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints[0].name.as_str(), "users_unique_email");
}

#[test]
fn create_table_column_level_unique_creates_backing_index() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, name STRING)");

    let constraints = unique_constraints(&db, "users");
    assert!(
        constraints[0].backing_index_id.is_some(),
        "UNIQUE constraint must have a backing index id"
    );

    let expected_index_name = "users_users_unique_email_idx";
    assert!(
        index_exists(&db, expected_index_name),
        "backing index '{expected_index_name}' must be live in the catalog"
    );
}

#[test]
fn create_table_column_level_unique_backing_index_is_btree() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    let txn = db.txn_manager.begin().unwrap();
    let indexes = db.catalog.list_indexes(&txn).unwrap();
    txn.commit().unwrap();

    let user_indexes: Vec<_> = indexes
        .iter()
        .filter(|i| i.table_id == db.file_id_of("users"))
        .collect();
    assert_eq!(user_indexes.len(), 1);
    assert_eq!(user_indexes[0].kind, IndexKind::Btree);
}

#[test]
fn create_table_table_level_unique_registers_constraint() {
    let db = TestDb::new();
    db.run_ok(
        "CREATE TABLE orders (id INT, user_id INT, product_id INT, UNIQUE (user_id, product_id))",
    );

    let constraints = unique_constraints(&db, "orders");
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].columns, vec![col(1), col(2)]);
}

#[test]
fn create_table_table_level_named_unique_uses_given_name() {
    let db = TestDb::new();
    db.run_ok(
        "CREATE TABLE orders (id INT, ref_id INT, CONSTRAINT orders_ref_unique UNIQUE (ref_id))",
    );

    let constraints = unique_constraints(&db, "orders");
    assert_eq!(constraints[0].name.as_str(), "orders_ref_unique");
}

#[test]
fn create_table_multiple_unique_constraints_all_registered() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, username STRING UNIQUE)");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints.len(), 2);

    let names: Vec<_> = constraints.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"users_unique_email"));
    assert!(names.contains(&"users_unique_username"));
}

#[test]
fn create_table_multiple_unique_each_has_backing_index() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, username STRING UNIQUE)");

    let constraints = unique_constraints(&db, "users");
    for uc in &constraints {
        assert!(
            uc.backing_index_id.is_some(),
            "constraint '{}' must have a backing index",
            uc.name.as_str()
        );
    }

    assert!(index_exists(&db, "users_users_unique_email_idx"));
    assert!(index_exists(&db, "users_users_unique_username_idx"));
}

// ── ALTER TABLE ADD CONSTRAINT UNIQUE ────────────────────────────────────────

#[test]
fn alter_add_unique_returns_correct_result() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");

    let result = db.run_ok("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    assert!(
        matches!(
            &result,
            StatementResult::UniqueConstraintAdded { table, constraint, .. }
                if table == "users" && constraint == "users_email_key"
        ),
        "unexpected result: {result:?}"
    );
}

#[test]
fn alter_add_unique_result_contains_index_name() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");

    let result = db.run_ok("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    let StatementResult::UniqueConstraintAdded { index, .. } = result else {
        panic!("expected UniqueConstraintAdded, got {result:?}");
    };
    assert_eq!(index, "users_users_email_key_idx");
}

#[test]
fn alter_add_unique_constraint_visible_in_catalog() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");
    db.run_ok("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].name.as_str(), "users_email_key");
    assert_eq!(constraints[0].columns, vec![col(1)]);
}

#[test]
fn alter_add_unique_backing_index_live_in_catalog() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");
    db.run_ok("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    assert!(
        index_exists(&db, "users_users_email_key_idx"),
        "backing index must be live after ALTER TABLE ADD CONSTRAINT"
    );
}

#[test]
fn alter_add_unique_auto_name_follows_convention() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");
    db.run_ok("ALTER TABLE users ADD UNIQUE (email)");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].name.as_str(), "users_unique_email");
}

#[test]
fn alter_add_composite_unique_columns_in_declaration_order() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE orders (id INT, user_id INT, product_id INT)");
    db.run_ok("ALTER TABLE orders ADD CONSTRAINT orders_up_key UNIQUE (user_id, product_id)");

    let constraints = unique_constraints(&db, "orders");
    assert_eq!(constraints[0].columns, vec![col(1), col(2)]);
}

#[test]
fn alter_add_unique_on_table_with_duplicates_rejects() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("INSERT INTO users VALUES (2, 'a@example.com')");

    let result = db.run("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    assert!(
        result.is_err(),
        "adding UNIQUE on a column with duplicate values must fail"
    );
}

#[test]
fn drop_constraint_returns_correct_result() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    let result = db.run_ok("ALTER TABLE users DROP CONSTRAINT users_unique_email");

    assert!(
        matches!(
            &result,
            StatementResult::ConstraintDropped { table, constraint }
                if table == "users" && constraint == "users_unique_email"
        ),
        "unexpected result: {result:?}"
    );
}

#[test]
fn drop_constraint_removes_from_catalog() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("ALTER TABLE users DROP CONSTRAINT users_unique_email");

    let constraints = unique_constraints(&db, "users");
    assert!(
        constraints.is_empty(),
        "constraint must be gone from catalog after DROP CONSTRAINT"
    );
}

#[test]
fn drop_constraint_also_drops_backing_index() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    assert!(
        index_exists(&db, "users_users_unique_email_idx"),
        "index must exist before drop"
    );

    db.run_ok("ALTER TABLE users DROP CONSTRAINT users_unique_email");

    assert!(
        !index_exists(&db, "users_users_unique_email_idx"),
        "backing index must be removed when constraint is dropped"
    );
}

#[test]
fn drop_nonexistent_constraint_returns_error() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");

    let result = db.run("ALTER TABLE users DROP CONSTRAINT ghost");

    assert!(
        result.is_err(),
        "dropping a nonexistent constraint must return an error"
    );
}

#[test]
fn drop_one_constraint_leaves_others_intact() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, username STRING UNIQUE)");
    db.run_ok("ALTER TABLE users DROP CONSTRAINT users_unique_email");

    let constraints = unique_constraints(&db, "users");
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].name.as_str(), "users_unique_username");

    assert!(!index_exists(&db, "users_users_unique_email_idx"));
    assert!(index_exists(&db, "users_users_unique_username_idx"));
}

// ── UNIQUE enforcement on INSERT ──────────────────────────────────────────────

#[test]
fn insert_duplicate_into_unique_column_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");

    let result = db.run("INSERT INTO users VALUES (2, 'a@example.com')");

    assert!(
        matches!(
            result,
            Err(EngineError::Constraint(
                ConstraintViolation::UniqueViolation { .. }
            ))
        ),
        "expected UniqueViolation, got: {result:?}"
    );
}

#[test]
fn insert_unique_violation_names_the_constraint() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");

    let err = db
        .run("INSERT INTO users VALUES (2, 'a@example.com')")
        .unwrap_err();

    let EngineError::Constraint(ConstraintViolation::UniqueViolation { constraint }) = err else {
        panic!("expected UniqueViolation, got {err:?}");
    };
    assert_eq!(constraint, "users_unique_email");
}

#[test]
fn insert_distinct_values_into_unique_column_succeeds() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@example.com')");

    assert_eq!(db.scan_all("users").len(), 2);
}

#[test]
fn multi_row_insert_with_internal_duplicate_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    let result = db.run("INSERT INTO users VALUES (1, 'x@y.com'), (2, 'x@y.com')");

    assert!(
        matches!(
            result,
            Err(EngineError::Constraint(
                ConstraintViolation::UniqueViolation { .. }
            ))
        ),
        "expected UniqueViolation for duplicate within same INSERT, got: {result:?}"
    );
}

#[test]
fn unique_violation_on_multi_row_insert_stops_at_duplicate() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");

    // First row is unique, second is a duplicate — the statement errors.
    let result = db.run("INSERT INTO users VALUES (1, 'a@b.com'), (2, 'a@b.com')");
    assert!(
        matches!(
            result,
            Err(EngineError::Constraint(
                ConstraintViolation::UniqueViolation { .. }
            ))
        ),
        "expected UniqueViolation, got: {result:?}"
    );

    // NOTE: WAL undo (ARIES) is not yet implemented, so the first row written
    // before the violation was detected is not rolled back. The table ends up
    // with one row rather than zero. This test documents the current behavior;
    // it should be tightened to assert len == 0 once undo is wired up.
    assert_eq!(
        db.scan_all("users").len(),
        1,
        "currently the pre-violation row survives because WAL undo is not yet implemented"
    );
}

#[test]
fn unique_constraint_not_enforced_on_column_without_constraint() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, name STRING)");

    db.run_ok("INSERT INTO users VALUES (1, 'a@b.com', 'Alice')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@b.com', 'Alice')");

    assert_eq!(
        db.scan_all("users").len(),
        2,
        "duplicates on unconstrained column must be allowed"
    );
}

#[test]
fn unique_enforcement_works_after_alter_add_constraint() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)");

    let result = db.run("INSERT INTO users VALUES (2, 'a@example.com')");

    assert!(
        matches!(
            result,
            Err(EngineError::Constraint(
                ConstraintViolation::UniqueViolation { .. }
            ))
        ),
        "UNIQUE constraint added via ALTER TABLE must be enforced on subsequent INSERTs"
    );
}

// ── UNIQUE enforcement on UPDATE ──────────────────────────────────────────────

#[test]
fn update_duplicate_value_into_unique_column_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@example.com')");

    let result = db.run("UPDATE users SET email = 'a@example.com' WHERE id = 2");

    assert!(
        matches!(
            result,
            Err(EngineError::Constraint(
                ConstraintViolation::UniqueViolation { .. }
            ))
        ),
        "expected UniqueViolation when UPDATE introduces a duplicate, got: {result:?}"
    );
}

#[test]
fn update_unique_violation_names_the_constraint() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@example.com')");

    let err = db
        .run("UPDATE users SET email = 'a@example.com' WHERE id = 2")
        .unwrap_err();

    let EngineError::Constraint(ConstraintViolation::UniqueViolation { constraint }) = err else {
        panic!("expected UniqueViolation, got {err:?}");
    };
    assert_eq!(constraint, "users_unique_email");
}

#[test]
fn update_to_same_value_on_unique_column_is_allowed() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");

    // Updating a row to its own current value must not violate uniqueness.
    db.run_ok("UPDATE users SET email = 'a@example.com' WHERE id = 1");

    assert_eq!(db.scan_all("users").len(), 1);
}

#[test]
fn update_to_fresh_value_on_unique_column_is_allowed() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");

    db.run_ok("UPDATE users SET email = 'new@example.com' WHERE id = 1");

    assert_eq!(db.scan_all("users").len(), 1);
}

#[test]
fn update_non_unique_column_is_always_allowed() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE, name STRING)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com', 'Alice')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@example.com', 'Bob')");

    // Both rows updated to the same name — no constraint on that column.
    db.run_ok("UPDATE users SET name = 'Same'");

    assert_eq!(db.scan_all("users").len(), 2);
}

#[test]
fn update_does_not_touch_heap_when_unique_check_fails() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");
    db.run_ok("INSERT INTO users VALUES (2, 'b@example.com')");

    let _ = db.run("UPDATE users SET email = 'a@example.com' WHERE id = 2");

    // Row 2 must still have its original email — heap write must not have happened.
    let rows = db.scan_all("users");
    let emails: Vec<_> = rows.iter().map(|t| t.get(1).cloned().unwrap()).collect();
    assert!(
        emails.contains(&storemy::Value::String("b@example.com".into())),
        "row 2 must still have original email after rejected UPDATE"
    );
}

#[test]
fn drop_constraint_allows_previously_rejected_insert() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE users (id INT, email STRING UNIQUE)");
    db.run_ok("INSERT INTO users VALUES (1, 'a@example.com')");

    // Confirm it's blocked before drop.
    assert!(
        db.run("INSERT INTO users VALUES (2, 'a@example.com')")
            .is_err()
    );

    db.run_ok("ALTER TABLE users DROP CONSTRAINT users_unique_email");

    // After dropping the constraint the duplicate insert must succeed.
    db.run_ok("INSERT INTO users VALUES (2, 'a@example.com')");
    assert_eq!(db.scan_all("users").len(), 2);
}
