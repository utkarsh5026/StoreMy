//! End-to-end integration tests for SQL JOIN queries.
//!
//! These tests exercise the full pipeline: SQL text → parser → binder →
//! planner → executor → `StatementResult::Selected`. They are the only layer
//! that can catch bugs in column-offset arithmetic, algorithm selection
//! (`HashJoin` vs `NestedLoopJoin`), and NULL semantics across the join boundary.
//!
//! Individual executor correctness is covered by unit tests inside each
//! `execution/join/*.rs` module. The integration tests here focus on what those
//! unit tests *cannot* see: planner decisions, scope resolution, schema merging,
//! and projection on top of joined output.
//!
//! # Result ordering
//!
//! Heap scans have no guaranteed row order, so every multi-row result set is
//! sorted before assertion. [`sort_by_int`] sorts by an integer column index;
//! `NULL` sorts last via an `i64::MAX` sentinel.

use storemy::{Value, engine::StatementResult};

use crate::common::TestDb;

// ── shared helpers ────────────────────────────────────────────────────────────

/// Run a `SELECT` through the database and collect every row as a `Vec<Value>`.
///
/// Mirrors the helper in `engine/update.rs` unit tests so the pattern is
/// consistent across the codebase.
fn select_rows(db: &TestDb, sql: &str) -> Vec<Vec<Value>> {
    match db.run_ok(sql) {
        StatementResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|t| (0..t.len()).map(|i| t.get(i).unwrap().clone()).collect())
            .collect(),
        other => panic!("expected Selected, got {other:?}"),
    }
}

/// Sort rows by the `i64` value at column `col`. `NULL` sorts last.
fn sort_by_int(rows: &mut [Vec<Value>], col: usize) {
    rows.sort_by_key(|r| match r.get(col) {
        Some(Value::Int64(v)) => *v,
        _ => i64::MAX,
    });
}

/// A simple equi-join on matching integer keys. The planner should pick
/// `HashJoin`. Rows with no counterpart on the other side are excluded.
#[test]
fn inner_join_equi_matches_correct_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE employees (id INT, name STRING)");
    db.run_ok("CREATE TABLE departments (dept_id INT, dept STRING)");
    db.run_ok("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
    // HR (dept_id=5) has no employee counterpart — it should be absent from output
    db.run_ok("INSERT INTO departments VALUES (1, 'Eng'), (2, 'Mkt'), (5, 'HR')");

    let mut rows = select_rows(
        &db,
        "SELECT * FROM employees JOIN departments ON employees.id = departments.dept_id",
    );
    sort_by_int(&mut rows, 0);

    // Carol (id=3) has no department — excluded
    assert_eq!(rows.len(), 2);
    // Output columns: employees.id | employees.name | departments.dept_id | departments.dept
    assert_eq!(rows[0], vec![
        Value::Int64(1),
        Value::String("Alice".into()),
        Value::Int64(1),
        Value::String("Eng".into()),
    ]);
    assert_eq!(rows[1], vec![
        Value::Int64(2),
        Value::String("Bob".into()),
        Value::Int64(2),
        Value::String("Mkt".into()),
    ]);
}

/// Disjoint key sets — no pair shares a key, so the result must be empty.
#[test]
fn inner_join_disjoint_keys_returns_empty() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (2)");
    db.run_ok("INSERT INTO t2 VALUES (10), (20)");

    let rows = select_rows(&db, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    assert!(rows.is_empty(), "disjoint keys must produce no output");
}

/// Empty right table — `HashJoin` builds an empty hash table, so no left row
/// can ever find a match.
#[test]
fn inner_join_empty_right_returns_empty() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (2)");
    // t2 intentionally left empty

    let rows = select_rows(&db, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    assert!(rows.is_empty(), "empty right table must produce no output");
}

/// Empty left table — nothing drives the outer loop, so the result is empty
/// regardless of how many rows the right side has.
#[test]
fn inner_join_empty_left_returns_empty() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    // t1 intentionally left empty
    db.run_ok("INSERT INTO t2 VALUES (1), (2)");

    let rows = select_rows(&db, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    assert!(rows.is_empty(), "empty left table must produce no output");
}

/// One left key matches multiple right rows — the join must emit one output row
/// per matching pair (a mini Cartesian product within the key group).
#[test]
fn inner_join_one_left_key_matches_multiple_right_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE orders (order_id INT)");
    db.run_ok("CREATE TABLE items (order_id INT, price INT)");
    db.run_ok("INSERT INTO orders VALUES (1)");
    db.run_ok("INSERT INTO items VALUES (1, 100), (1, 200), (1, 300)");

    let mut rows = select_rows(
        &db,
        "SELECT * FROM orders JOIN items ON orders.order_id = items.order_id",
    );
    // Output columns: orders.order_id | items.order_id | items.price
    sort_by_int(&mut rows, 2);

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![
        Value::Int64(1),
        Value::Int64(1),
        Value::Int64(100)
    ]);
    assert_eq!(rows[1], vec![
        Value::Int64(1),
        Value::Int64(1),
        Value::Int64(200)
    ]);
    assert_eq!(rows[2], vec![
        Value::Int64(1),
        Value::Int64(1),
        Value::Int64(300)
    ]);
}

/// A non-equi predicate (`<`) cannot be extracted as a hash key, so the planner
/// must fall back to `NestedLoopJoin` and evaluate the full expression per pair.
#[test]
fn inner_join_non_equi_predicate_falls_back_to_nested_loop() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (x INT)");
    db.run_ok("CREATE TABLE t2 (y INT)");
    db.run_ok("INSERT INTO t1 VALUES (3)");
    // 1 and 2 are less than 3, so they don't match; 4 and 5 do
    db.run_ok("INSERT INTO t2 VALUES (1), (2), (4), (5)");

    let mut rows = select_rows(&db, "SELECT * FROM t1 JOIN t2 ON t1.x < t2.y");
    sort_by_int(&mut rows, 1);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::Int64(3), Value::Int64(4)]);
    assert_eq!(rows[1], vec![Value::Int64(3), Value::Int64(5)]);
}

/// `NULL` on either join key side must never match — SQL three-valued logic says
/// `NULL = anything` is `UNKNOWN`, which is treated as false. `NullsNeverMatch`
/// must hold for both left and right sides.
#[test]
fn inner_join_null_keys_never_produce_output() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (NULL)");
    db.run_ok("INSERT INTO t2 VALUES (1), (NULL)");

    let rows = select_rows(&db, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    // Only (1, 1) is a valid match; NULL = NULL is false
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Value::Int64(1), Value::Int64(1)]);
}

/// Compound `AND` predicate — `try_extract_equi` cannot decompose this, so the
/// planner falls back to `NestedLoopJoin` with the full expression. Both
/// conditions must hold for a row to appear in the output.
#[test]
fn inner_join_compound_and_predicate_applies_both_conditions() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT, threshold INT)");
    db.run_ok("CREATE TABLE t2 (id INT, value INT)");
    db.run_ok("INSERT INTO t1 VALUES (1, 50)");
    // Both t2 rows match the equi part (id=1), but only value=70 passes the
    // inequality (value > threshold = 50)
    db.run_ok("INSERT INTO t2 VALUES (1, 30), (1, 70)");

    let rows = select_rows(
        &db,
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t2.value > t1.threshold",
    );

    assert_eq!(rows.len(), 1);
    // Output: t1.id | t1.threshold | t2.id | t2.value
    assert_eq!(
        rows[0][3],
        Value::Int64(70),
        "only value=70 clears the threshold"
    );
}

/// Projection on top of a join — verifies that `SELECT col_list` narrows the
/// joined schema correctly and that column indices survive the project step.
#[test]
fn inner_join_with_explicit_column_projection() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT, name STRING)");
    db.run_ok("CREATE TABLE t2 (tid INT, code STRING)");
    db.run_ok("INSERT INTO t1 VALUES (1, 'Alice')");
    db.run_ok("INSERT INTO t2 VALUES (1, 'ENG')");

    let rows = select_rows(
        &db,
        "SELECT t1.name, t2.code FROM t1 JOIN t2 ON t1.id = t2.tid",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![
        Value::String("Alice".into()),
        Value::String("ENG".into()),
    ]);
}

// ── LEFT JOIN ────────────────────────────────────────────────────────────────

/// Left rows with no matching right row must appear in the output with `NULL`
/// padding on every right-side column. Matched rows behave like INNER JOIN.
#[test]
fn left_join_unmatched_left_rows_get_null_right_side() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE employees (id INT, name STRING)");
    db.run_ok("CREATE TABLE departments (dept_id INT, dept STRING)");
    db.run_ok("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
    // Bob and Carol have no department
    db.run_ok("INSERT INTO departments VALUES (1, 'Eng')");

    let mut rows = select_rows(
        &db,
        "SELECT * FROM employees LEFT JOIN departments ON employees.id = departments.dept_id",
    );
    sort_by_int(&mut rows, 0);

    // All three employees appear
    assert_eq!(rows.len(), 3);
    // Alice — matched
    assert_eq!(rows[0], vec![
        Value::Int64(1),
        Value::String("Alice".into()),
        Value::Int64(1),
        Value::String("Eng".into()),
    ]);
    // Bob — no match, right columns are NULL
    assert_eq!(rows[1][0], Value::Int64(2));
    assert_eq!(rows[1][1], Value::String("Bob".into()));
    assert_eq!(rows[1][2], Value::Null, "unmatched dept_id must be NULL");
    assert_eq!(rows[1][3], Value::Null, "unmatched dept must be NULL");
    // Carol — no match
    assert_eq!(rows[2][0], Value::Int64(3));
    assert_eq!(rows[2][2], Value::Null);
    assert_eq!(rows[2][3], Value::Null);
}

/// With an empty right table the left outer join must still emit every left row,
/// all with `NULL` on the right side.
#[test]
fn left_join_empty_right_all_left_rows_preserved_with_nulls() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (2), (3)");
    // t2 intentionally left empty

    let mut rows = select_rows(&db, "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id");
    sort_by_int(&mut rows, 0);

    assert_eq!(rows.len(), 3);
    for (expected_id, row) in [1i64, 2, 3].into_iter().zip(rows.iter()) {
        assert_eq!(row[0], Value::Int64(expected_id));
        assert_eq!(
            row[1],
            Value::Null,
            "right id must be NULL when right is empty"
        );
    }
}

/// A left row whose join key is `NULL` can never match any right row (NULL
/// never equals anything). LEFT JOIN must still emit that row, with `NULL` on
/// the right side — it must not be silently dropped.
#[test]
fn left_join_null_key_on_left_never_matches_but_row_is_preserved() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (NULL)");
    db.run_ok("INSERT INTO t2 VALUES (1)");

    let mut rows = select_rows(&db, "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id");
    // NULLs in col 0 sort last via the i64::MAX sentinel
    sort_by_int(&mut rows, 0);

    assert_eq!(rows.len(), 2);
    // id=1 matches
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[0][1], Value::Int64(1));
    // NULL id on left — right side must also be NULL
    assert_eq!(rows[1][0], Value::Null);
    assert_eq!(rows[1][1], Value::Null);
}

// ── CROSS JOIN ───────────────────────────────────────────────────────────────

/// `CROSS JOIN` produces the full Cartesian product — every left row paired
/// with every right row, with no filtering. The planner routes this through
/// `BoundFrom::Cross` → `NestedLoopJoin` with a `true` predicate.
#[test]
fn cross_join_produces_full_cartesian_product() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (a INT)");
    db.run_ok("CREATE TABLE t2 (b INT)");
    db.run_ok("INSERT INTO t1 VALUES (1), (2)");
    db.run_ok("INSERT INTO t2 VALUES (10), (20), (30)");

    let rows = select_rows(&db, "SELECT * FROM t1 CROSS JOIN t2");

    assert_eq!(rows.len(), 6, "2 × 3 = 6 rows");
    let mut pairs: Vec<(i64, i64)> = rows
        .into_iter()
        .map(|r| {
            let a = match r[0] {
                Value::Int64(v) => v,
                ref v => panic!("expected Int64, got {v:?}"),
            };
            let b = match r[1] {
                Value::Int64(v) => v,
                ref v => panic!("expected Int64, got {v:?}"),
            };
            (a, b)
        })
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, [
        (1, 10),
        (1, 20),
        (1, 30),
        (2, 10),
        (2, 20),
        (2, 30)
    ]);
}

/// Chaining three tables tests that column-offset arithmetic stays correct as
/// the join schema grows. After `a JOIN b`, the merged schema has 3 columns;
/// the second join key from `b` must resolve to index 2 (not 1), and the
/// right-side key from `c` to index 0 relative to c's schema.
#[test]
fn three_table_chain_join_resolves_column_offsets_correctly() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE a (a_id INT)");
    db.run_ok("CREATE TABLE b (a_id INT, b_id INT)");
    db.run_ok("CREATE TABLE c (b_id INT)");
    db.run_ok("INSERT INTO a VALUES (1), (2)");
    db.run_ok("INSERT INTO b VALUES (1, 10), (2, 20)");
    db.run_ok("INSERT INTO c VALUES (10), (20)");

    let mut rows = select_rows(
        &db,
        "SELECT * FROM a JOIN b ON a.a_id = b.a_id JOIN c ON b.b_id = c.b_id",
    );
    sort_by_int(&mut rows, 0);

    // Output columns: a.a_id | b.a_id | b.b_id | c.b_id
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![
        Value::Int64(1),
        Value::Int64(1),
        Value::Int64(10),
        Value::Int64(10),
    ]);
    assert_eq!(rows[1], vec![
        Value::Int64(2),
        Value::Int64(2),
        Value::Int64(20),
        Value::Int64(20),
    ]);
}

// ── unsupported join kinds ────────────────────────────────────────────────────

/// `A RIGHT JOIN B` keeps every row from B, NULL-padding the A side when no
/// match exists. Internally the planner rewrites it as `B LEFT JOIN A` plus a
/// reorder projection, so this test verifies the observable result is correct.
#[test]
fn right_join_is_symmetric_with_left_join() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t1 (id INT)");
    db.run_ok("CREATE TABLE t2 (id INT)");
    db.run_ok("INSERT INTO t1 (id) VALUES (1), (2)");
    db.run_ok("INSERT INTO t2 (id) VALUES (2), (3)");

    // t1 RIGHT JOIN t2 keeps every t2 row.
    // t2 has id=2 (matches t1.id=2) and id=3 (no match → t1.id is NULL).
    let result = db
        .run("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
        .unwrap();

    let rows = match result {
        StatementResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    };
    assert_eq!(rows.len(), 2, "one matched row + one null-padded row");

    let null_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.get(0).unwrap().is_null())
        .collect();
    assert_eq!(null_rows.len(), 1, "t2.id=3 has no match in t1");
}
