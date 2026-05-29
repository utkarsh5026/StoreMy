//! End-to-end tests for SELECT expression evaluation across all scalar types.
//!
//! # Architecture
//!
//! Two harnesses, mirroring the JSON test suite:
//!
//! - **Expression cases** (`ExprCase`) — single-row table, `SELECT <expr> FROM t`, asserts column 0
//!   of the single returned row. Covers expression output for every operator / type combination.
//!
//! - **Filter cases** (`FilterCase`) — multi-row table, `SELECT id FROM t WHERE <predicate>`,
//!   asserts which row IDs survive. Exercises the same operators in the `WHERE` pipeline so bugs in
//!   predicate evaluation are caught separately from bugs in projection.
//!
//! # Type coverage
//!
//! | SQL keyword | Stored variant      | Notes                                     |
//! |-------------|---------------------|-------------------------------------------|
//! | `INT`       | `Fixed(Int32)`      | Comparisons widen via `numeric_eq`/`cmp`  |
//! | `BIGINT`    | `Fixed(Int64)`      | Integer literals are `Int64`; use for arith|
//! | `FLOAT`     | `Fixed(Float64)`    | Float literals are `Float64`              |
//! | `STRING`    | `Dyn(Varchar)`      | Used for LIKE / string comparisons        |
//!
//! Integer literals in SQL parse as `Int64`.  Arithmetic operators require
//! same-type operands, so `BIGINT` columns pair cleanly with integer literals
//! while `INT` columns work only for comparisons (which widen `Int32↔Int64`).
//!
//! # Adding a new operator or type
//!
//! 1. Add `fn <op>_cases() -> Vec<ExprCase>` and/or `fn <op>_filter_cases() -> Vec<FilterCase>` in
//!    the matching section.
//! 2. Add `#[test]` functions calling `run_expr_cases(...)` / `run_filter_cases(...)`.
//!
//! The harness creates a fresh `TestDb` per case — no shared state.

use storemy::{Value, engine::StatementResult, types::FixedValue};

use crate::common::TestDb;

/// Sentinel that tells the harness to omit the extra column (SQL NULL).
const NULL_VALUE: &str = "__NULL__";

/// One SELECT-expression assertion on a single-row table.
///
/// The harness creates `t (id INT, <columns>)`, inserts one row
/// `(1, <values>)` — or `(1)` when `values == NULL_VALUE` — runs
/// `SELECT <expr> FROM t`, and asserts that column 0 equals `expected`.
struct ExprCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// Extra column definitions, e.g. `"n BIGINT"` or `"a BIGINT, b BIGINT"`.
    columns: &'static str,
    /// VALUES for the extra columns, e.g. `"10"` or `"3, 7"`.
    /// Use `NULL_VALUE` to omit the column (inserts SQL NULL via column-list INSERT).
    values: &'static str,
    /// SQL expression passed to `SELECT`.
    expr: &'static str,
    /// Expected value of column 0 in the single returned row.
    expected: Value,
}

/// One WHERE-filter assertion over a multi-row table.
///
/// The harness creates `t (id INT, <columns>)`, inserts one row per entry in
/// `rows` with `id = i + 1`, then runs `SELECT id FROM t WHERE <predicate>`
/// and asserts the returned IDs (sorted) match `expected_ids`.
struct FilterCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// Extra column definitions.
    columns: &'static str,
    /// Per-row extra column values. Use `NULL_VALUE` for a NULL row.
    /// Row IDs are 1-indexed (`rows[0]` → id=1, `rows[1]` → id=2, …).
    rows: Vec<&'static str>,
    /// `WHERE` predicate (without the `WHERE` keyword).
    predicate: &'static str,
    /// Sorted list of row IDs expected to pass the predicate.
    expected_ids: Vec<i32>,
}

fn select_rows(db: &TestDb, sql: &str) -> Vec<Vec<Value>> {
    match db.run_ok(sql) {
        StatementResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|t| (0..t.len()).map(|i| t.get(i).unwrap().clone()).collect())
            .collect(),
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn run_expr_cases(cases: &[ExprCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok(&format!("CREATE TABLE t (id INT, {})", case.columns));
        if case.values == NULL_VALUE {
            db.run_ok("INSERT INTO t (id) VALUES (1)");
        } else {
            db.run_ok(&format!("INSERT INTO t VALUES (1, {})", case.values));
        }
        let rows = select_rows(&db, &format!("SELECT {} FROM t", case.expr));
        assert!(
            !rows.is_empty(),
            "case {:?}: SELECT {} FROM t returned no rows",
            case.name,
            case.expr,
        );
        assert_eq!(
            rows[0][0], case.expected,
            "case {:?}: SELECT {} FROM t  (columns={}, values={})",
            case.name, case.expr, case.columns, case.values,
        );
    }
}

fn run_filter_cases(cases: &[FilterCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok(&format!("CREATE TABLE t (id INT, {})", case.columns));
        for (i, row) in case.rows.iter().enumerate() {
            let id = i32::try_from(i).expect("test row index fits in i32") + 1;
            if *row == NULL_VALUE {
                db.run_ok(&format!("INSERT INTO t (id) VALUES ({id})"));
            } else {
                db.run_ok(&format!("INSERT INTO t VALUES ({id}, {row})"));
            }
        }
        let rows = select_rows(&db, &format!("SELECT id FROM t WHERE {}", case.predicate));
        let mut ids: Vec<i32> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Fixed(FixedValue::Int32(v)) => *v,
                other => panic!("expected Int32 id, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids, case.expected_ids,
            "filter case {:?}: WHERE {}",
            case.name, case.predicate,
        );
    }
}

fn arith_bigint_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "add — basic",
            columns: "n BIGINT",
            values: "10",
            expr: "n + 5",
            expected: Value::int64(15),
        },
        ExprCase {
            name: "sub — basic",
            columns: "n BIGINT",
            values: "10",
            expr: "n - 3",
            expected: Value::int64(7),
        },
        ExprCase {
            name: "mul — basic",
            columns: "n BIGINT",
            values: "7",
            expr: "n * 6",
            expected: Value::int64(42),
        },
        ExprCase {
            name: "div — exact",
            columns: "n BIGINT",
            values: "20",
            expr: "n / 4",
            expected: Value::int64(5),
        },
        ExprCase {
            name: "div — truncates toward zero",
            columns: "n BIGINT",
            values: "7",
            expr: "n / 2",
            expected: Value::int64(3),
        },
        ExprCase {
            name: "sub — negative result",
            columns: "n BIGINT",
            values: "3",
            expr: "n - 10",
            expected: Value::int64(-7),
        },
        ExprCase {
            name: "add — col + col",
            columns: "n BIGINT",
            values: "10",
            expr: "n + n",
            expected: Value::int64(20),
        },
        ExprCase {
            name: "mul — col * col",
            columns: "n BIGINT",
            values: "6",
            expr: "n * n",
            expected: Value::int64(36),
        },
        ExprCase {
            name: "add — two distinct BIGINT columns",
            columns: "a BIGINT, b BIGINT",
            values: "3, 7",
            expr: "a + b",
            expected: Value::int64(10),
        },
        ExprCase {
            name: "nested arithmetic — (a + b) * c",
            columns: "a BIGINT, b BIGINT",
            values: "2, 3",
            expr: "(a + b) * a",
            expected: Value::int64(10),
        },
        ExprCase {
            name: "null + value propagates to NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n + 1",
            expected: Value::Null,
        },
        ExprCase {
            name: "value + null propagates to NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "1 + n",
            expected: Value::Null,
        },
        ExprCase {
            name: "null * value propagates to NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n * 2",
            expected: Value::Null,
        },
    ]
}

fn arith_bigint_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "filter on computed expression > threshold",
            columns: "n BIGINT",
            rows: vec!["2", "5", "8"],
            predicate: "n * 2 > 9",
            expected_ids: vec![2, 3],
        },
        FilterCase {
            name: "filter: n + 5 = target",
            columns: "n BIGINT",
            rows: vec!["5", "10", "15"],
            predicate: "n + 5 = 15",
            expected_ids: vec![2],
        },
        FilterCase {
            name: "filter: n - 3 < 5  (n < 8: rows with n=1 and n=7 qualify)",
            columns: "n BIGINT",
            rows: vec!["1", "7", "10"],
            predicate: "n - 3 < 5",
            expected_ids: vec![1, 2],
        },
    ]
}

#[test]
fn arith_bigint_expr() {
    run_expr_cases(&arith_bigint_cases());
}

#[test]
fn arith_bigint_filter() {
    run_filter_cases(&arith_bigint_filter_cases());
}

// ── arithmetic — FLOAT ────────────────────────────────────────────────────────

fn arith_float_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "add",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f + 1.5",
            expected: Value::float64(4.0),
        },
        ExprCase {
            name: "sub",
            columns: "f FLOAT",
            values: "5.0",
            expr: "f - 1.5",
            expected: Value::float64(3.5),
        },
        ExprCase {
            name: "mul",
            columns: "f FLOAT",
            values: "2.0",
            expr: "f * 3.5",
            expected: Value::float64(7.0),
        },
        ExprCase {
            name: "div",
            columns: "f FLOAT",
            values: "7.0",
            expr: "f / 2.0",
            expected: Value::float64(3.5),
        },
        ExprCase {
            name: "two float columns",
            columns: "a FLOAT, b FLOAT",
            values: "1.5, 2.5",
            expr: "a + b",
            expected: Value::float64(4.0),
        },
        ExprCase {
            name: "null propagates",
            columns: "f FLOAT",
            values: NULL_VALUE,
            expr: "f + 1.0",
            expected: Value::Null,
        },
    ]
}

fn arith_float_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "float multiplication in predicate",
            columns: "f FLOAT",
            rows: vec!["1.0", "2.5", "4.0"],
            predicate: "f * 2.0 > 5.0",
            expected_ids: vec![3],
        },
        FilterCase {
            name: "float division in predicate",
            columns: "f FLOAT",
            rows: vec!["3.0", "6.0", "9.0"],
            predicate: "f / 3.0 = 2.0",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn arith_float_expr() {
    run_expr_cases(&arith_float_cases());
}

#[test]
fn arith_float_filter() {
    run_filter_cases(&arith_float_filter_cases());
}

// ── comparison — BIGINT ───────────────────────────────────────────────────────
//
// All six SQL comparison operators: =, <>, <, <=, >, >=.
// NULL on either side propagates to NULL (three-valued logic).
#[allow(clippy::too_many_lines)]
fn cmp_bigint_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "= match",
            columns: "n BIGINT",
            values: "5",
            expr: "n = 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "= no match",
            columns: "n BIGINT",
            values: "5",
            expr: "n = 6",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "<> match",
            columns: "n BIGINT",
            values: "5",
            expr: "n <> 6",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<> no match",
            columns: "n BIGINT",
            values: "5",
            expr: "n <> 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "< true",
            columns: "n BIGINT",
            values: "3",
            expr: "n < 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "< false (equal)",
            columns: "n BIGINT",
            values: "5",
            expr: "n < 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "< false (greater)",
            columns: "n BIGINT",
            values: "7",
            expr: "n < 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "<= at boundary",
            columns: "n BIGINT",
            values: "5",
            expr: "n <= 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<= below boundary",
            columns: "n BIGINT",
            values: "4",
            expr: "n <= 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<= above boundary",
            columns: "n BIGINT",
            values: "6",
            expr: "n <= 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "> true",
            columns: "n BIGINT",
            values: "7",
            expr: "n > 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "> false (equal)",
            columns: "n BIGINT",
            values: "5",
            expr: "n > 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: ">= at boundary",
            columns: "n BIGINT",
            values: "5",
            expr: "n >= 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: ">= above boundary",
            columns: "n BIGINT",
            values: "6",
            expr: "n >= 5",
            expected: Value::bool(true),
        },
        // column vs column
        ExprCase {
            name: "col < col (true)",
            columns: "a BIGINT, b BIGINT",
            values: "3, 7",
            expr: "a < b",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "col = col (true)",
            columns: "a BIGINT, b BIGINT",
            values: "5, 5",
            expr: "a = b",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "col > col (false)",
            columns: "a BIGINT, b BIGINT",
            values: "3, 7",
            expr: "a > b",
            expected: Value::bool(false),
        },
        // null propagation
        ExprCase {
            name: "null = value → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n = 5",
            expected: Value::Null,
        },
        ExprCase {
            name: "null < value → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n < 5",
            expected: Value::Null,
        },
        ExprCase {
            name: "null <> value → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n <> 5",
            expected: Value::Null,
        },
        ExprCase {
            name: "null >= value → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n >= 5",
            expected: Value::Null,
        },
    ]
}

fn cmp_bigint_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "> filter",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "15"],
            predicate: "n > 7",
            expected_ids: vec![3, 4],
        },
        FilterCase {
            name: "= filter",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10"],
            predicate: "n = 5",
            expected_ids: vec![2],
        },
        FilterCase {
            name: "<= filter",
            columns: "n BIGINT",
            rows: vec!["3", "7", "12"],
            predicate: "n <= 7",
            expected_ids: vec![1, 2],
        },
        FilterCase {
            name: "<> filter",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3"],
            predicate: "n <> 2",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "null row excluded from comparison filter",
            columns: "n BIGINT",
            rows: vec!["1", NULL_VALUE, "10"],
            predicate: "n > 0",
            expected_ids: vec![1, 3],
        },
    ]
}

#[test]
fn cmp_bigint_expr() {
    run_expr_cases(&cmp_bigint_cases());
}

#[test]
fn cmp_bigint_filter() {
    run_filter_cases(&cmp_bigint_filter_cases());
}

// ── comparison — INT (Int32 vs Int64 widening) ────────────────────────────────
//
// INT columns store Int32. Integer literals are Int64.  The comparison
// operators use `numeric_eq` / `numeric_cmp` which widen Int32↔Int64, so
// `n = 5` works correctly.  Arithmetic does NOT widen — use BIGINT for that.

fn cmp_int_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "= match (Int32 vs Int64 literal)",
            columns: "n INT",
            values: "5",
            expr: "n = 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "= no match",
            columns: "n INT",
            values: "5",
            expr: "n = 6",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "< true",
            columns: "n INT",
            values: "3",
            expr: "n < 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "> true",
            columns: "n INT",
            values: "8",
            expr: "n > 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<= boundary",
            columns: "n INT",
            values: "5",
            expr: "n <= 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: ">= boundary",
            columns: "n INT",
            values: "5",
            expr: "n >= 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "null propagates",
            columns: "n INT",
            values: NULL_VALUE,
            expr: "n = 5",
            expected: Value::Null,
        },
    ]
}

fn cmp_int_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "> filter on INT column (Int32 vs Int64 widening)",
            columns: "n INT",
            rows: vec!["2", "5", "8"],
            predicate: "n > 4",
            expected_ids: vec![2, 3],
        },
        FilterCase {
            name: "= filter on INT column",
            columns: "n INT",
            rows: vec!["10", "20", "30"],
            predicate: "n = 20",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn cmp_int_expr() {
    run_expr_cases(&cmp_int_cases());
}

#[test]
fn cmp_int_filter() {
    run_filter_cases(&cmp_int_filter_cases());
}

// ── comparison — FLOAT ────────────────────────────────────────────────────────

fn cmp_float_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "= match",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f = 2.5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "= no match",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f = 3.0",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "< true",
            columns: "f FLOAT",
            values: "1.5",
            expr: "f < 2.5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "< false (equal)",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f < 2.5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "> true",
            columns: "f FLOAT",
            values: "3.5",
            expr: "f > 2.5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<= boundary",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f <= 2.5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: ">= boundary",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f >= 2.5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "col < col",
            columns: "a FLOAT, b FLOAT",
            values: "1.5, 2.5",
            expr: "a < b",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "null propagates",
            columns: "f FLOAT",
            values: NULL_VALUE,
            expr: "f = 2.5",
            expected: Value::Null,
        },
    ]
}

fn cmp_float_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "> float filter",
            columns: "f FLOAT",
            rows: vec!["1.0", "2.5", "4.0"],
            predicate: "f > 2.0",
            expected_ids: vec![2, 3],
        },
        FilterCase {
            name: "<= float filter",
            columns: "f FLOAT",
            rows: vec!["1.0", "2.5", "3.0"],
            predicate: "f <= 2.5",
            expected_ids: vec![1, 2],
        },
    ]
}

#[test]
fn cmp_float_expr() {
    run_expr_cases(&cmp_float_cases());
}

#[test]
fn cmp_float_filter() {
    run_filter_cases(&cmp_float_filter_cases());
}

// ── comparison — STRING ───────────────────────────────────────────────────────
//
// String comparison is lexicographic (byte order). LIKE handles pattern
// matching — see the LIKE section below.

fn cmp_string_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "= match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s = 'alice'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "= no match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s = 'bob'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "<> match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s <> 'bob'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "< lexicographic (true)",
            columns: "s STRING",
            values: "'alice'",
            expr: "s < 'bob'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "< lexicographic (false — greater)",
            columns: "s STRING",
            values: "'bob'",
            expr: "s < 'alice'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "> lexicographic (true)",
            columns: "s STRING",
            values: "'bob'",
            expr: "s > 'alice'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "<= equal strings",
            columns: "s STRING",
            values: "'cat'",
            expr: "s <= 'cat'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "col = col (same value)",
            columns: "a STRING, b STRING",
            values: "'same', 'same'",
            expr: "a = b",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "col < col (different values)",
            columns: "a STRING, b STRING",
            values: "'apple', 'mango'",
            expr: "a < b",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "null propagates",
            columns: "s STRING",
            values: NULL_VALUE,
            expr: "s = 'alice'",
            expected: Value::Null,
        },
    ]
}

fn cmp_string_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "= filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'charlie'"],
            predicate: "s = 'bob'",
            expected_ids: vec![2],
        },
        FilterCase {
            name: "< lexicographic filter",
            columns: "s STRING",
            rows: vec!["'apple'", "'mango'", "'zebra'"],
            predicate: "s < 'mango'",
            expected_ids: vec![1],
        },
        FilterCase {
            name: ">= lexicographic filter",
            columns: "s STRING",
            rows: vec!["'apple'", "'mango'", "'zebra'"],
            predicate: "s >= 'mango'",
            expected_ids: vec![2, 3],
        },
        FilterCase {
            name: "null row excluded",
            columns: "s STRING",
            rows: vec!["'alice'", NULL_VALUE, "'charlie'"],
            predicate: "s = 'alice'",
            expected_ids: vec![1],
        },
    ]
}

#[test]
fn cmp_string_expr() {
    run_expr_cases(&cmp_string_cases());
}

#[test]
fn cmp_string_filter() {
    run_filter_cases(&cmp_string_filter_cases());
}

// ── IS NULL / IS NOT NULL ─────────────────────────────────────────────────────
//
// `IS NULL` never propagates NULL — it always returns a Bool.
// Literals are never NULL, even `0` or `''`.

fn is_null_cases() -> Vec<ExprCase> {
    vec![
        // BIGINT column
        ExprCase {
            name: "BIGINT null IS NULL → true",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n IS NULL",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "BIGINT value IS NULL → false",
            columns: "n BIGINT",
            values: "5",
            expr: "n IS NULL",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "BIGINT null IS NOT NULL → false",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n IS NOT NULL",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "BIGINT value IS NOT NULL → true",
            columns: "n BIGINT",
            values: "5",
            expr: "n IS NOT NULL",
            expected: Value::bool(true),
        },
        // INT column
        ExprCase {
            name: "INT null IS NULL → true",
            columns: "n INT",
            values: NULL_VALUE,
            expr: "n IS NULL",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "INT value IS NOT NULL → true",
            columns: "n INT",
            values: "0",
            expr: "n IS NOT NULL",
            expected: Value::bool(true),
        },
        // FLOAT column
        ExprCase {
            name: "FLOAT null IS NULL → true",
            columns: "f FLOAT",
            values: NULL_VALUE,
            expr: "f IS NULL",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "FLOAT value IS NULL → false",
            columns: "f FLOAT",
            values: "3.14",
            expr: "f IS NULL",
            expected: Value::bool(false),
        },
        // STRING column
        ExprCase {
            name: "STRING null IS NULL → true",
            columns: "s STRING",
            values: NULL_VALUE,
            expr: "s IS NULL",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "STRING empty string IS NULL → false",
            columns: "s STRING",
            values: "''",
            expr: "s IS NULL",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "STRING value IS NOT NULL → true",
            columns: "s STRING",
            values: "'hello'",
            expr: "s IS NOT NULL",
            expected: Value::bool(true),
        },
        // Literals are never NULL
        ExprCase {
            name: "literal 0 IS NULL → false",
            columns: "n BIGINT",
            values: "1",
            expr: "0 IS NULL",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "literal '' IS NULL → false",
            columns: "n BIGINT",
            values: "1",
            expr: "'' IS NULL",
            expected: Value::bool(false),
        },
    ]
}

fn is_null_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "IS NULL keeps only null rows",
            columns: "n BIGINT",
            rows: vec!["10", NULL_VALUE, "30", NULL_VALUE],
            predicate: "n IS NULL",
            expected_ids: vec![2, 4],
        },
        FilterCase {
            name: "IS NOT NULL excludes null rows",
            columns: "n BIGINT",
            rows: vec!["10", NULL_VALUE, "30"],
            predicate: "n IS NOT NULL",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "STRING IS NULL filter",
            columns: "s STRING",
            rows: vec!["'alice'", NULL_VALUE, "'bob'"],
            predicate: "s IS NULL",
            expected_ids: vec![2],
        },
        FilterCase {
            name: "IS NOT NULL on mixed FLOAT column",
            columns: "f FLOAT",
            rows: vec!["1.0", NULL_VALUE, NULL_VALUE, "4.0"],
            predicate: "f IS NOT NULL",
            expected_ids: vec![1, 4],
        },
    ]
}

#[test]
fn is_null_expr() {
    run_expr_cases(&is_null_cases());
}

#[test]
fn is_null_filter() {
    run_filter_cases(&is_null_filter_cases());
}

// ── IN / NOT IN ───────────────────────────────────────────────────────────────
//
// `v IN (...)` uses strict PartialEq on Value — no type widening.
// BIGINT columns are used here so column values (Int64) and list literals
// (Int64) have matching types; the same strict equality also applies to STRING.
//
// NULL behaviour:
// - NULL IN (...)  → NULL  (null propagation, not false)
// - v IN (..., NULL, ...)  → NULL if v not found in non-null items

fn in_bigint_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "value in list → true",
            columns: "n BIGINT",
            values: "2",
            expr: "n IN (1, 2, 3)",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "value not in list → false",
            columns: "n BIGINT",
            values: "5",
            expr: "n IN (1, 2, 3)",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT IN — miss → true",
            columns: "n BIGINT",
            values: "5",
            expr: "n NOT IN (1, 2, 3)",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NOT IN — hit → false",
            columns: "n BIGINT",
            values: "2",
            expr: "n NOT IN (1, 2, 3)",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "null IN list → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n IN (1, 2, 3)",
            expected: Value::Null,
        },
        ExprCase {
            name: "null NOT IN list → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n NOT IN (1, 2, 3)",
            expected: Value::Null,
        },
        ExprCase {
            name: "single-element list — match",
            columns: "n BIGINT",
            values: "42",
            expr: "n IN (42)",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "single-element list — no match",
            columns: "n BIGINT",
            values: "99",
            expr: "n IN (42)",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "first element matches",
            columns: "n BIGINT",
            values: "1",
            expr: "n IN (1, 2, 3)",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "last element matches",
            columns: "n BIGINT",
            values: "3",
            expr: "n IN (1, 2, 3)",
            expected: Value::bool(true),
        },
    ]
}

fn in_string_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "string in list → true",
            columns: "s STRING",
            values: "'bob'",
            expr: "s IN ('alice', 'bob', 'charlie')",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "string not in list → false",
            columns: "s STRING",
            values: "'dave'",
            expr: "s IN ('alice', 'bob', 'charlie')",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT IN string — miss → true",
            columns: "s STRING",
            values: "'dave'",
            expr: "s NOT IN ('alice', 'bob')",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NOT IN string — hit → false",
            columns: "s STRING",
            values: "'alice'",
            expr: "s NOT IN ('alice', 'bob')",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "null string IN list → NULL",
            columns: "s STRING",
            values: NULL_VALUE,
            expr: "s IN ('alice', 'bob')",
            expected: Value::Null,
        },
    ]
}

fn in_bigint_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "IN keeps matching rows",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3", "4", "5"],
            predicate: "n IN (2, 4)",
            expected_ids: vec![2, 4],
        },
        FilterCase {
            name: "NOT IN excludes matching rows",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3", "4", "5"],
            predicate: "n NOT IN (2, 4)",
            expected_ids: vec![1, 3, 5],
        },
        FilterCase {
            name: "null rows produce NULL from IN — excluded from WHERE",
            columns: "n BIGINT",
            rows: vec!["1", NULL_VALUE, "3"],
            predicate: "n IN (1, 2, 3)",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "large IN list",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3", "4", "5", "6", "7"],
            predicate: "n IN (1, 3, 5, 7)",
            expected_ids: vec![1, 3, 5, 7],
        },
    ]
}

fn in_string_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "string IN filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'charlie'", "'dave'"],
            predicate: "s IN ('alice', 'charlie')",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "string NOT IN filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'charlie'"],
            predicate: "s NOT IN ('bob')",
            expected_ids: vec![1, 3],
        },
    ]
}

#[test]
fn in_bigint_expr() {
    run_expr_cases(&in_bigint_cases());
}

#[test]
fn in_string_expr() {
    run_expr_cases(&in_string_cases());
}

#[test]
fn in_bigint_filter() {
    run_filter_cases(&in_bigint_filter_cases());
}

#[test]
fn in_string_filter() {
    run_filter_cases(&in_string_filter_cases());
}

// ── BETWEEN / NOT BETWEEN ─────────────────────────────────────────────────────
//
// `v BETWEEN lo AND hi` is inclusive on both ends: equivalent to `lo <= v AND v <= hi`.
// Ordering uses the same `PartialOrd` as the comparison operators.
// NULL on any operand (v, lo, or hi) → NULL.

fn between_bigint_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "in range",
            columns: "n BIGINT",
            values: "10",
            expr: "n BETWEEN 5 AND 15",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "at lower bound (inclusive)",
            columns: "n BIGINT",
            values: "5",
            expr: "n BETWEEN 5 AND 15",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "at upper bound (inclusive)",
            columns: "n BIGINT",
            values: "15",
            expr: "n BETWEEN 5 AND 15",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "below range",
            columns: "n BIGINT",
            values: "4",
            expr: "n BETWEEN 5 AND 15",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "above range",
            columns: "n BIGINT",
            values: "16",
            expr: "n BETWEEN 5 AND 15",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT BETWEEN — in range → false",
            columns: "n BIGINT",
            values: "10",
            expr: "n NOT BETWEEN 5 AND 15",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT BETWEEN — out of range → true",
            columns: "n BIGINT",
            values: "20",
            expr: "n NOT BETWEEN 5 AND 15",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NOT BETWEEN — at boundary → false",
            columns: "n BIGINT",
            values: "5",
            expr: "n NOT BETWEEN 5 AND 15",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "null → NULL",
            columns: "n BIGINT",
            values: NULL_VALUE,
            expr: "n BETWEEN 5 AND 15",
            expected: Value::Null,
        },
        ExprCase {
            name: "point range (lo = hi) — match",
            columns: "n BIGINT",
            values: "7",
            expr: "n BETWEEN 7 AND 7",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "point range (lo = hi) — no match",
            columns: "n BIGINT",
            values: "6",
            expr: "n BETWEEN 7 AND 7",
            expected: Value::bool(false),
        },
    ]
}

fn between_float_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "in range",
            columns: "f FLOAT",
            values: "2.5",
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "at lower bound",
            columns: "f FLOAT",
            values: "1.0",
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "at upper bound",
            columns: "f FLOAT",
            values: "3.0",
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "below range",
            columns: "f FLOAT",
            values: "0.5",
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "above range",
            columns: "f FLOAT",
            values: "3.1",
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "null → NULL",
            columns: "f FLOAT",
            values: NULL_VALUE,
            expr: "f BETWEEN 1.0 AND 3.0",
            expected: Value::Null,
        },
    ]
}

fn between_string_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "string in lexicographic range",
            columns: "s STRING",
            values: "'fox'",
            expr: "s BETWEEN 'apple' AND 'mango'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "string at lower bound",
            columns: "s STRING",
            values: "'apple'",
            expr: "s BETWEEN 'apple' AND 'mango'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "string at upper bound",
            columns: "s STRING",
            values: "'mango'",
            expr: "s BETWEEN 'apple' AND 'mango'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "string below range",
            columns: "s STRING",
            values: "'ant'",
            expr: "s BETWEEN 'apple' AND 'mango'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "string above range",
            columns: "s STRING",
            values: "'zebra'",
            expr: "s BETWEEN 'apple' AND 'mango'",
            expected: Value::bool(false),
        },
    ]
}

fn between_bigint_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "BETWEEN keeps in-range rows",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "15", "20"],
            predicate: "n BETWEEN 5 AND 15",
            expected_ids: vec![2, 3, 4],
        },
        FilterCase {
            name: "NOT BETWEEN excludes in-range rows",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "15", "20"],
            predicate: "n NOT BETWEEN 5 AND 15",
            expected_ids: vec![1, 5],
        },
        FilterCase {
            name: "null row excluded from BETWEEN filter",
            columns: "n BIGINT",
            rows: vec!["1", NULL_VALUE, "10"],
            predicate: "n BETWEEN 1 AND 10",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "point range boundary filter",
            columns: "n BIGINT",
            rows: vec!["4", "5", "6"],
            predicate: "n BETWEEN 5 AND 5",
            expected_ids: vec![2],
        },
    ]
}

fn between_float_filter_cases() -> Vec<FilterCase> {
    vec![FilterCase {
        name: "float BETWEEN filter",
        columns: "f FLOAT",
        rows: vec!["0.5", "1.5", "2.5", "3.5"],
        predicate: "f BETWEEN 1.0 AND 3.0",
        expected_ids: vec![2, 3],
    }]
}

fn between_string_filter_cases() -> Vec<FilterCase> {
    vec![FilterCase {
        name: "string BETWEEN lexicographic filter",
        columns: "s STRING",
        rows: vec!["'ant'", "'cat'", "'fox'", "'zebra'"],
        predicate: "s BETWEEN 'cat' AND 'mango'",
        expected_ids: vec![2, 3],
    }]
}

#[test]
fn between_bigint_expr() {
    run_expr_cases(&between_bigint_cases());
}

#[test]
fn between_float_expr() {
    run_expr_cases(&between_float_cases());
}

#[test]
fn between_string_expr() {
    run_expr_cases(&between_string_cases());
}

#[test]
fn between_bigint_filter() {
    run_filter_cases(&between_bigint_filter_cases());
}

#[test]
fn between_float_filter() {
    run_filter_cases(&between_float_filter_cases());
}

#[test]
fn between_string_filter() {
    run_filter_cases(&between_string_filter_cases());
}

// ── LIKE / NOT LIKE ───────────────────────────────────────────────────────────
//
// Wildcards:
//   `%`  — matches any sequence of characters (including empty)
//   `_`  — matches exactly one character
//
// LIKE is case-sensitive in this implementation.
// NULL on either operand propagates to NULL.
#[allow(clippy::too_many_lines)]
fn like_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "exact match — equal strings",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'alice'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "exact match — different strings",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'bob'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "empty string matches empty pattern",
            columns: "s STRING",
            values: "''",
            expr: "s LIKE ''",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "non-empty does not match empty pattern",
            columns: "s STRING",
            values: "'a'",
            expr: "s LIKE ''",
            expected: Value::bool(false),
        },
        // prefix %
        ExprCase {
            name: "% prefix — matches",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'ali%'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "% prefix — no match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'bob%'",
            expected: Value::bool(false),
        },
        // suffix %
        ExprCase {
            name: "% suffix — matches",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE '%ice'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "% suffix — no match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE '%xyz'",
            expected: Value::bool(false),
        },
        // substring %
        ExprCase {
            name: "% contains — matches",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE '%lic%'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "% contains — no match",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE '%xyz%'",
            expected: Value::bool(false),
        },
        // % alone
        ExprCase {
            name: "% alone matches any non-empty",
            columns: "s STRING",
            values: "'anything'",
            expr: "s LIKE '%'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "% alone matches empty string",
            columns: "s STRING",
            values: "''",
            expr: "s LIKE '%'",
            expected: Value::bool(true),
        },
        // leading and trailing %
        ExprCase {
            name: "%value% — full surroundsearch",
            columns: "s STRING",
            values: "'foobar'",
            expr: "s LIKE '%oob%'",
            expected: Value::bool(true),
        },
        // _ single character
        ExprCase {
            name: "_ matches one char",
            columns: "s STRING",
            values: "'a'",
            expr: "s LIKE '_'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "_ does not match empty",
            columns: "s STRING",
            values: "''",
            expr: "s LIKE '_'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "_ does not match two chars",
            columns: "s STRING",
            values: "'ab'",
            expr: "s LIKE '_'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "_ in middle matches",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'a_ice'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "three _ matches exactly three chars",
            columns: "s STRING",
            values: "'abc'",
            expr: "s LIKE '___'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "three _ does not match two chars",
            columns: "s STRING",
            values: "'ab'",
            expr: "s LIKE '___'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "a_% — starts with a, at least two chars",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'a_%'",
            expected: Value::bool(true),
        },
        // NOT LIKE
        ExprCase {
            name: "NOT LIKE — no match → true",
            columns: "s STRING",
            values: "'alice'",
            expr: "s NOT LIKE 'bob%'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NOT LIKE — match → false",
            columns: "s STRING",
            values: "'alice'",
            expr: "s NOT LIKE 'ali%'",
            expected: Value::bool(false),
        },
        // case sensitivity
        ExprCase {
            name: "LIKE is case-sensitive (uppercase input)",
            columns: "s STRING",
            values: "'Alice'",
            expr: "s LIKE 'alice'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "LIKE is case-sensitive (uppercase pattern)",
            columns: "s STRING",
            values: "'alice'",
            expr: "s LIKE 'Alice'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "LIKE matches exactly with correct case",
            columns: "s STRING",
            values: "'Alice'",
            expr: "s LIKE 'Alice'",
            expected: Value::bool(true),
        },
        // null propagation
        ExprCase {
            name: "null LIKE pattern → NULL",
            columns: "s STRING",
            values: NULL_VALUE,
            expr: "s LIKE 'ali%'",
            expected: Value::Null,
        },
        ExprCase {
            name: "null NOT LIKE pattern → NULL",
            columns: "s STRING",
            values: NULL_VALUE,
            expr: "s NOT LIKE 'ali%'",
            expected: Value::Null,
        },
    ]
}

fn like_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "prefix LIKE filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'alfred'", "'charlie'"],
            predicate: "s LIKE 'al%'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "suffix LIKE filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'ice'"],
            predicate: "s LIKE '%ice'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "NOT LIKE filter",
            columns: "s STRING",
            rows: vec!["'alice'", "'bob'", "'alfred'"],
            predicate: "s NOT LIKE 'al%'",
            expected_ids: vec![2],
        },
        FilterCase {
            name: "null row excluded from LIKE filter",
            columns: "s STRING",
            rows: vec!["'alice'", NULL_VALUE, "'alfred'"],
            predicate: "s LIKE 'al%'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "_ wildcard — exactly two chars after 'a'",
            columns: "s STRING",
            rows: vec!["'a'", "'ab'", "'abc'", "'abcd'"],
            predicate: "s LIKE 'a__'",
            expected_ids: vec![3],
        },
        FilterCase {
            name: "% matches any row",
            columns: "s STRING",
            rows: vec!["'a'", "'longer string'", "''"],
            predicate: "s LIKE '%'",
            expected_ids: vec![1, 2, 3],
        },
    ]
}

#[test]
fn like_expr() {
    run_expr_cases(&like_cases());
}

#[test]
fn like_filter() {
    run_filter_cases(&like_filter_cases());
}

// ── logical operators — AND, OR, NOT ─────────────────────────────────────────
//
// AND and OR require Bool operands — passing a non-Bool is a TypeError.
// NOT also requires Bool.
// Three-valued logic: NULL AND true → NULL, NULL OR true → true is NOT
// modeled here — both sides are evaluated eagerly, and then the null guard
// fires on either NULL before the operator is applied.

fn logical_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "true AND true",
            columns: "n BIGINT",
            values: "1",
            expr: "n = 1 AND n > 0",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "true AND false",
            columns: "n BIGINT",
            values: "1",
            expr: "n = 1 AND n > 5",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "false AND true",
            columns: "n BIGINT",
            values: "1",
            expr: "n > 5 AND n = 1",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "false AND false",
            columns: "n BIGINT",
            values: "1",
            expr: "n > 5 AND n > 10",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "true OR false",
            columns: "n BIGINT",
            values: "1",
            expr: "n = 1 OR n > 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "false OR true",
            columns: "n BIGINT",
            values: "1",
            expr: "n > 5 OR n = 1",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "true OR true",
            columns: "n BIGINT",
            values: "1",
            expr: "n = 1 OR n >= 1",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "false OR false",
            columns: "n BIGINT",
            values: "1",
            expr: "n > 5 OR n > 10",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT true → false",
            columns: "n BIGINT",
            values: "1",
            expr: "NOT n = 1",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "NOT false → true",
            columns: "n BIGINT",
            values: "1",
            expr: "NOT n = 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NOT NOT true → true",
            columns: "n BIGINT",
            values: "1",
            expr: "NOT NOT n = 1",
            expected: Value::bool(true),
        },
        // compound: three-way AND
        ExprCase {
            name: "three-way AND all true",
            columns: "n BIGINT",
            values: "5",
            expr: "n > 1 AND n < 10 AND n = 5",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "three-way AND one false",
            columns: "n BIGINT",
            values: "5",
            expr: "n > 1 AND n < 10 AND n = 6",
            expected: Value::bool(false),
        },
    ]
}

fn logical_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "AND — compound predicate",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "15"],
            predicate: "n > 3 AND n < 12",
            expected_ids: vec![2, 3],
        },
        FilterCase {
            name: "OR — union of two conditions",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "15"],
            predicate: "n = 1 OR n = 15",
            expected_ids: vec![1, 4],
        },
        FilterCase {
            name: "NOT — negated equality",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10"],
            predicate: "NOT n = 5",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "AND + OR — precedence check",
            columns: "n BIGINT",
            rows: vec!["1", "2", "5", "10"],
            predicate: "n = 1 OR n = 2 AND n > 1",
            // AND binds tighter: `n = 1 OR (n = 2 AND n > 1)` → ids 1, 2
            expected_ids: vec![1, 2],
        },
        FilterCase {
            name: "compound: (n > 5 OR n = 1) AND n < 15",
            columns: "n BIGINT",
            rows: vec!["1", "5", "10", "20"],
            predicate: "(n > 5 OR n = 1) AND n < 15",
            expected_ids: vec![1, 3],
        },
    ]
}

#[test]
fn logical_expr() {
    run_expr_cases(&logical_cases());
}

#[test]
fn logical_filter() {
    run_filter_cases(&logical_filter_cases());
}

// ── CASE expressions ──────────────────────────────────────────────────────────
//
// Two forms:
// - Searched CASE: `CASE WHEN <cond> THEN <val> … [ELSE <val>] END`
// - Simple CASE:   `CASE <expr> WHEN <val> THEN <result> … [ELSE <val>] END`
//
// Branches are evaluated in order; the first true WHEN wins.
// When no branch matches and there is no ELSE, the result is NULL.

fn case_cases() -> Vec<ExprCase> {
    vec![
        // Searched CASE
        ExprCase {
            name: "searched CASE — first branch",
            columns: "n BIGINT",
            values: "1",
            expr: "CASE WHEN n = 1 THEN 100 WHEN n = 2 THEN 200 ELSE 0 END",
            expected: Value::int64(100),
        },
        ExprCase {
            name: "searched CASE — second branch",
            columns: "n BIGINT",
            values: "2",
            expr: "CASE WHEN n = 1 THEN 100 WHEN n = 2 THEN 200 ELSE 0 END",
            expected: Value::int64(200),
        },
        ExprCase {
            name: "searched CASE — ELSE branch",
            columns: "n BIGINT",
            values: "99",
            expr: "CASE WHEN n = 1 THEN 100 WHEN n = 2 THEN 200 ELSE 0 END",
            expected: Value::int64(0),
        },
        ExprCase {
            name: "searched CASE — no match and no ELSE → NULL",
            columns: "n BIGINT",
            values: "99",
            expr: "CASE WHEN n = 1 THEN 100 END",
            expected: Value::Null,
        },
        // CASE returning a string
        ExprCase {
            name: "CASE — string result (small)",
            columns: "n BIGINT",
            values: "2",
            expr: "CASE WHEN n < 5 THEN 'small' ELSE 'large' END",
            expected: Value::varchar("small".to_string()),
        },
        ExprCase {
            name: "CASE — string result (large)",
            columns: "n BIGINT",
            values: "10",
            expr: "CASE WHEN n < 5 THEN 'small' ELSE 'large' END",
            expected: Value::varchar("large".to_string()),
        },
        // CASE in arithmetic context
        ExprCase {
            name: "CASE result used in arithmetic",
            columns: "n BIGINT",
            values: "3",
            expr: "CASE WHEN n > 0 THEN n ELSE 0 END + 10",
            expected: Value::int64(13),
        },
        // CASE with range check
        ExprCase {
            name: "CASE — range classification low",
            columns: "n BIGINT",
            values: "5",
            expr: "CASE WHEN n < 10 THEN 'low' WHEN n < 100 THEN 'mid' ELSE 'high' END",
            expected: Value::varchar("low".to_string()),
        },
        ExprCase {
            name: "CASE — range classification mid",
            columns: "n BIGINT",
            values: "50",
            expr: "CASE WHEN n < 10 THEN 'low' WHEN n < 100 THEN 'mid' ELSE 'high' END",
            expected: Value::varchar("mid".to_string()),
        },
        ExprCase {
            name: "CASE — range classification high",
            columns: "n BIGINT",
            values: "500",
            expr: "CASE WHEN n < 10 THEN 'low' WHEN n < 100 THEN 'mid' ELSE 'high' END",
            expected: Value::varchar("high".to_string()),
        },
    ]
}

fn case_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "CASE in WHERE — keeps matching rows",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3", "4", "5"],
            predicate: "CASE WHEN n <= 3 THEN 1 ELSE 0 END = 1",
            expected_ids: vec![1, 2, 3],
        },
        FilterCase {
            name: "CASE used as computed key — equality filter",
            columns: "n BIGINT",
            rows: vec!["5", "50", "500"],
            predicate: "CASE WHEN n < 10 THEN 'low' WHEN n < 100 THEN 'mid' ELSE 'high' END = 'mid'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn case_expr() {
    run_expr_cases(&case_cases());
}

#[test]
fn case_filter() {
    run_filter_cases(&case_filter_cases());
}

#[test]
fn compound_predicate_between_and_not_null() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 5)");
    db.run_ok("INSERT INTO t VALUES (2, 10)");
    db.run_ok("INSERT INTO t (id) VALUES (3)"); // n = NULL
    db.run_ok("INSERT INTO t VALUES (4, 20)");

    let rows = select_rows(
        &db,
        "SELECT id FROM t WHERE n IS NOT NULL AND n BETWEEN 5 AND 15",
    );
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Fixed(FixedValue::Int32(v)) => *v,
            other => panic!("expected Int32, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn compound_predicate_in_and_like() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, category STRING, name STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'fruit', 'apple')");
    db.run_ok("INSERT INTO t VALUES (2, 'fruit', 'banana')");
    db.run_ok("INSERT INTO t VALUES (3, 'veg', 'carrot')");
    db.run_ok("INSERT INTO t VALUES (4, 'fruit', 'avocado')");

    // fruit category AND name starts with 'a'
    let rows = select_rows(
        &db,
        "SELECT id FROM t WHERE category IN ('fruit') AND name LIKE 'a%'",
    );
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Fixed(FixedValue::Int32(v)) => *v,
            other => panic!("expected Int32, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 4]);
}

#[test]
fn compound_predicate_arithmetic_and_comparison() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, price BIGINT, qty BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 10, 3)"); // total = 30
    db.run_ok("INSERT INTO t VALUES (2, 5,  10)"); // total = 50
    db.run_ok("INSERT INTO t VALUES (3, 20, 1)"); // total = 20
    db.run_ok("INSERT INTO t VALUES (4, 7,  6)"); // total = 42

    let rows = select_rows(&db, "SELECT id FROM t WHERE price * qty > 35");
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Fixed(FixedValue::Int32(v)) => *v,
            other => panic!("expected Int32, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn select_expression_in_projection_with_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 3)");
    db.run_ok("INSERT INTO t VALUES (2, 7)");
    db.run_ok("INSERT INTO t VALUES (3, 12)");

    // project n * 2 while also filtering — expression in both SELECT and WHERE
    let rows = select_rows(&db, "SELECT n * 2 FROM t WHERE n > 5");
    let mut values: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Fixed(FixedValue::Int64(v)) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![14, 24]);
}

#[test]
fn null_propagation_chain() {
    // NULL flows through multiple operations without crashing.
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, n BIGINT)");
    db.run_ok("INSERT INTO t (id) VALUES (1)"); // n = NULL

    // NULL in arithmetic → NULL
    let rows = select_rows(&db, "SELECT n + 10 FROM t");
    assert_eq!(rows[0][0], Value::Null, "NULL + 10 should be NULL");

    // NULL in comparison → NULL (excluded from WHERE)
    let rows = select_rows(&db, "SELECT id FROM t WHERE n > 0");
    assert!(rows.is_empty(), "NULL > 0 should yield no rows");

    // IS NULL on the chain result
    let rows = select_rows(&db, "SELECT (n + 10) IS NULL FROM t");
    assert_eq!(
        rows[0][0],
        Value::bool(true),
        "(NULL + 10) IS NULL should be true"
    );
}
