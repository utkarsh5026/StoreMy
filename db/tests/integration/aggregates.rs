//! End-to-end tests for GROUP BY and aggregate functions.
//!
//! # Architecture
//!
//! Two harnesses, parallel to the `select_expressions` suite:
//!
//! - **Ungrouped cases** (`UngroupedCase`) — multi-row table, no `GROUP BY`. `SELECT <agg_exprs>
//!   FROM t` always produces exactly one output row. Each case asserts all column values in that
//!   row.
//!
//! - **Group cases** (`GroupCase`) — multi-row table, with `GROUP BY`. `SELECT <select> FROM t
//!   GROUP BY <group_by>` produces one row per group. The harness sorts both actual and expected
//!   rows before comparing so that the non-deterministic group ordering does not cause false
//!   failures.
//!
//! # Aggregate function coverage
//!
//! | SQL function   | Output type       | Notes                                    |
//! |----------------|-------------------|------------------------------------------|
//! | `COUNT(*)`     | `Int64`           | Counts every row, including NULL rows    |
//! | `COUNT(col)`   | `Int64`           | Skips NULL; returns 0 when all are NULL  |
//! | `SUM(col)`     | `Int64` / `Float64` | Skips NULL; NULL when all are NULL     |
//! | `AVG(col)`     | `Float64`         | Skips NULL; NULL when all are NULL       |
//! | `MIN(col)`     | same as input     | Skips NULL; NULL when all are NULL       |
//! | `MAX(col)`     | same as input     | Skips NULL; NULL when all are NULL       |
//!
//! # Limitations (not tested here — engine returns `Unsupported`)
//!
//! - `HAVING` — not yet implemented.
//! - `ORDER BY` combined with `GROUP BY` / aggregates — not yet implemented.
//!
//! # Adding a new aggregate or type
//!
//! 1. Add `fn <agg>_cases() -> Vec<UngroupedCase>` and/or `fn <agg>_group_cases() ->
//!    Vec<GroupCase>` in the matching section.
//! 2. Add `#[test]` functions calling `run_ungrouped_cases` / `run_group_cases`.

use storemy::{Value, engine::StatementResult};

use crate::common::TestDb;

const NULL_ROW: &str = "__NULL__";

/// One ungrouped-aggregate assertion.
///
/// The harness creates `t (id INT, <columns>)`, inserts one row per `rows`
/// entry (or a NULL row when the entry is `NULL_ROW`), then runs
/// `SELECT <select> FROM t` and asserts that the single output row equals
/// `expected` column-by-column.
struct UngroupedCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// Extra column definitions, e.g. `"n BIGINT"`.
    columns: &'static str,
    /// Per-row extra column values. Use `NULL_ROW` to omit extra columns
    /// (inserts SQL NULL for those columns).
    rows: Vec<&'static str>,
    /// `SELECT` list passed verbatim, e.g. `"SUM(n), COUNT(*)"`.
    select: &'static str,
    /// Expected values in the output row, one per `SELECT`-list item.
    expected: Vec<Value>,
}

/// One GROUP BY aggregate assertion.
///
/// The harness creates `t (id INT, <columns>)`, inserts one row per `rows`
/// entry, then runs `SELECT <select> FROM t GROUP BY <group_by>`.
/// Both the actual and expected rows are sorted (by their `Debug`
/// representation) before comparison so that non-deterministic group
/// ordering does not break tests.
struct GroupCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// Extra column definitions, e.g. `"cat STRING, n BIGINT"`.
    columns: &'static str,
    /// Per-row extra column values (same `NULL_ROW` sentinel as above).
    rows: Vec<&'static str>,
    /// Full `SELECT` list, e.g. `"cat, SUM(n)"`.
    select: &'static str,
    /// `GROUP BY` clause (without the keyword), e.g. `"cat"`.
    group_by: &'static str,
    /// Expected output rows.  Each inner `Vec` is one row, columns in the
    /// same order as `select`.  The outer `Vec` will be sorted before
    /// comparison.
    expected_rows: Vec<Vec<Value>>,
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

fn insert_rows(db: &TestDb, _columns: &str, rows: &[&str]) {
    for (i, row) in rows.iter().enumerate() {
        let id = i32::try_from(i).expect("row index fits i32") + 1;
        if *row == NULL_ROW {
            db.run_ok(&format!("INSERT INTO t (id) VALUES ({id})"));
        } else {
            db.run_ok(&format!("INSERT INTO t VALUES ({id}, {row})"));
        }
    }
}

fn run_ungrouped_cases(cases: &[UngroupedCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok(&format!("CREATE TABLE t (id INT, {})", case.columns));
        insert_rows(&db, case.columns, &case.rows);

        let rows = select_rows(&db, &format!("SELECT {} FROM t", case.select));
        assert_eq!(
            rows.len(),
            1,
            "case {:?}: expected exactly one output row from ungrouped aggregate",
            case.name,
        );
        assert_eq!(
            rows[0].len(),
            case.expected.len(),
            "case {:?}: column count mismatch",
            case.name,
        );
        for (col, (actual, expected)) in rows[0].iter().zip(case.expected.iter()).enumerate() {
            assert_eq!(
                actual, expected,
                "case {:?}: column {col} — SELECT {} FROM t  (rows={:?})",
                case.name, case.select, case.rows,
            );
        }
    }
}

fn sort_rows(rows: &mut [Vec<Value>]) {
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
}

fn run_group_cases(cases: &[GroupCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok(&format!("CREATE TABLE t (id INT, {})", case.columns));
        insert_rows(&db, case.columns, &case.rows);

        let sql = format!("SELECT {} FROM t GROUP BY {}", case.select, case.group_by);
        let mut actual = select_rows(&db, &sql);
        let mut expected = case.expected_rows.clone();

        sort_rows(&mut actual);
        sort_rows(&mut expected);

        assert_eq!(
            actual, expected,
            "case {:?}: {}  (rows={:?})",
            case.name, sql, case.rows,
        );
    }
}

fn count_star_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "three rows",
            columns: "n BIGINT",
            rows: vec!["10", "20", "30"],
            select: "COUNT(*)",
            expected: vec![Value::int64(3)],
        },
        UngroupedCase {
            name: "single row",
            columns: "n BIGINT",
            rows: vec!["1"],
            select: "COUNT(*)",
            expected: vec![Value::int64(1)],
        },
        UngroupedCase {
            name: "counts NULL rows — NULL rows still count",
            columns: "n BIGINT",
            rows: vec!["1", NULL_ROW, "3"],
            select: "COUNT(*)",
            expected: vec![Value::int64(3)],
        },
        UngroupedCase {
            name: "all NULL rows still counted",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "COUNT(*)",
            expected: vec![Value::int64(2)],
        },
    ]
}

#[test]
fn count_star_ungrouped() {
    run_ungrouped_cases(&count_star_ungrouped_cases());
}

fn count_col_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "no NULLs",
            columns: "n BIGINT",
            rows: vec!["10", "20", "30"],
            select: "COUNT(n)",
            expected: vec![Value::int64(3)],
        },
        UngroupedCase {
            name: "one NULL skipped",
            columns: "n BIGINT",
            rows: vec!["10", NULL_ROW, "30"],
            select: "COUNT(n)",
            expected: vec![Value::int64(2)],
        },
        UngroupedCase {
            name: "all NULL → 0",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "COUNT(n)",
            expected: vec![Value::int64(0)],
        },
        UngroupedCase {
            name: "single non-null",
            columns: "n BIGINT",
            rows: vec!["42"],
            select: "COUNT(n)",
            expected: vec![Value::int64(1)],
        },
    ]
}

#[test]
fn count_col_ungrouped() {
    run_ungrouped_cases(&count_col_ungrouped_cases());
}

fn sum_bigint_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "basic sum",
            columns: "n BIGINT",
            rows: vec!["1", "2", "3"],
            select: "SUM(n)",
            expected: vec![Value::int64(6)],
        },
        UngroupedCase {
            name: "single value",
            columns: "n BIGINT",
            rows: vec!["42"],
            select: "SUM(n)",
            expected: vec![Value::int64(42)],
        },
        UngroupedCase {
            name: "NULL rows skipped",
            columns: "n BIGINT",
            rows: vec!["10", NULL_ROW, "20"],
            select: "SUM(n)",
            expected: vec![Value::int64(30)],
        },
        UngroupedCase {
            name: "all NULL → NULL",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "SUM(n)",
            expected: vec![Value::Null],
        },
    ]
}

fn sum_float_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "float sum",
            columns: "f FLOAT",
            rows: vec!["1.5", "2.5", "3.0"],
            select: "SUM(f)",
            expected: vec![Value::float64(7.0)],
        },
        UngroupedCase {
            name: "float NULL skipped",
            columns: "f FLOAT",
            rows: vec!["1.0", NULL_ROW, "2.0"],
            select: "SUM(f)",
            expected: vec![Value::float64(3.0)],
        },
        UngroupedCase {
            name: "float all NULL → NULL",
            columns: "f FLOAT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "SUM(f)",
            expected: vec![Value::Null],
        },
    ]
}

#[test]
fn sum_bigint_ungrouped() {
    run_ungrouped_cases(&sum_bigint_ungrouped_cases());
}

#[test]
fn sum_float_ungrouped() {
    run_ungrouped_cases(&sum_float_ungrouped_cases());
}

fn avg_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "integer column avg",
            columns: "n BIGINT",
            rows: vec!["2", "4", "6"],
            select: "AVG(n)",
            expected: vec![Value::float64(4.0)],
        },
        UngroupedCase {
            name: "float column avg",
            columns: "f FLOAT",
            rows: vec!["1.0", "3.0"],
            select: "AVG(f)",
            expected: vec![Value::float64(2.0)],
        },
        UngroupedCase {
            name: "NULL skipped in avg",
            columns: "n BIGINT",
            rows: vec!["10", NULL_ROW, "20"],
            select: "AVG(n)",
            expected: vec![Value::float64(15.0)],
        },
        UngroupedCase {
            name: "all NULL → NULL",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "AVG(n)",
            expected: vec![Value::Null],
        },
        UngroupedCase {
            name: "single value avg",
            columns: "n BIGINT",
            rows: vec!["7"],
            select: "AVG(n)",
            expected: vec![Value::float64(7.0)],
        },
    ]
}

#[test]
fn avg_ungrouped() {
    run_ungrouped_cases(&avg_ungrouped_cases());
}

fn min_max_bigint_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "MIN basic",
            columns: "n BIGINT",
            rows: vec!["5", "2", "8"],
            select: "MIN(n)",
            expected: vec![Value::int64(2)],
        },
        UngroupedCase {
            name: "MAX basic",
            columns: "n BIGINT",
            rows: vec!["5", "2", "8"],
            select: "MAX(n)",
            expected: vec![Value::int64(8)],
        },
        UngroupedCase {
            name: "MIN and MAX together",
            columns: "n BIGINT",
            rows: vec!["3", "1", "7", "5"],
            select: "MIN(n), MAX(n)",
            expected: vec![Value::int64(1), Value::int64(7)],
        },
        UngroupedCase {
            name: "MIN single value",
            columns: "n BIGINT",
            rows: vec!["42"],
            select: "MIN(n)",
            expected: vec![Value::int64(42)],
        },
        UngroupedCase {
            name: "MIN skips NULL",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, "3", NULL_ROW, "7"],
            select: "MIN(n)",
            expected: vec![Value::int64(3)],
        },
        UngroupedCase {
            name: "MAX skips NULL",
            columns: "n BIGINT",
            rows: vec!["3", NULL_ROW, "7", NULL_ROW],
            select: "MAX(n)",
            expected: vec![Value::int64(7)],
        },
        UngroupedCase {
            name: "MIN all NULL → NULL",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "MIN(n)",
            expected: vec![Value::Null],
        },
        UngroupedCase {
            name: "MAX all NULL → NULL",
            columns: "n BIGINT",
            rows: vec![NULL_ROW, NULL_ROW],
            select: "MAX(n)",
            expected: vec![Value::Null],
        },
    ]
}

fn min_max_float_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "MIN float",
            columns: "f FLOAT",
            rows: vec!["3.5", "1.0", "2.5"],
            select: "MIN(f)",
            expected: vec![Value::float64(1.0)],
        },
        UngroupedCase {
            name: "MAX float",
            columns: "f FLOAT",
            rows: vec!["3.5", "1.0", "2.5"],
            select: "MAX(f)",
            expected: vec![Value::float64(3.5)],
        },
    ]
}

fn min_max_string_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "MIN string — lexicographic",
            columns: "s STRING",
            rows: vec!["'mango'", "'apple'", "'zebra'"],
            select: "MIN(s)",
            expected: vec![Value::varchar("apple".into())],
        },
        UngroupedCase {
            name: "MAX string — lexicographic",
            columns: "s STRING",
            rows: vec!["'mango'", "'apple'", "'zebra'"],
            select: "MAX(s)",
            expected: vec![Value::varchar("zebra".into())],
        },
    ]
}

#[test]
fn min_max_bigint_ungrouped() {
    run_ungrouped_cases(&min_max_bigint_ungrouped_cases());
}

#[test]
fn min_max_float_ungrouped() {
    run_ungrouped_cases(&min_max_float_ungrouped_cases());
}

#[test]
fn min_max_string_ungrouped() {
    run_ungrouped_cases(&min_max_string_ungrouped_cases());
}

fn multi_agg_ungrouped_cases() -> Vec<UngroupedCase> {
    vec![
        UngroupedCase {
            name: "COUNT(*), SUM, AVG, MIN, MAX all together",
            columns: "n BIGINT",
            rows: vec!["2", "4", "6", "8"],
            select: "COUNT(*), SUM(n), AVG(n), MIN(n), MAX(n)",
            expected: vec![
                Value::int64(4),
                Value::int64(20),
                Value::float64(5.0),
                Value::int64(2),
                Value::int64(8),
            ],
        },
        UngroupedCase {
            name: "COUNT(*) and COUNT(col) differ when NULLs present",
            columns: "n BIGINT",
            rows: vec!["1", NULL_ROW, "3"],
            select: "COUNT(*), COUNT(n)",
            expected: vec![Value::int64(3), Value::int64(2)],
        },
    ]
}

#[test]
fn multi_agg_ungrouped() {
    run_ungrouped_cases(&multi_agg_ungrouped_cases());
}

fn count_star_group_cases() -> Vec<GroupCase> {
    vec![
        GroupCase {
            name: "two groups, equal size",
            columns: "cat STRING",
            rows: vec!["'a'", "'b'", "'a'", "'b'"],
            select: "cat, COUNT(*)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(2)], vec![
                Value::varchar("b".into()),
                Value::int64(2),
            ]],
        },
        GroupCase {
            name: "three groups, unequal size",
            columns: "cat STRING",
            rows: vec!["'x'", "'y'", "'x'", "'x'", "'y'", "'z'"],
            select: "cat, COUNT(*)",
            group_by: "cat",
            expected_rows: vec![
                vec![Value::varchar("x".into()), Value::int64(3)],
                vec![Value::varchar("y".into()), Value::int64(2)],
                vec![Value::varchar("z".into()), Value::int64(1)],
            ],
        },
        GroupCase {
            name: "single group",
            columns: "cat STRING",
            rows: vec!["'only'", "'only'"],
            select: "cat, COUNT(*)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("only".into()), Value::int64(2)]],
        },
        GroupCase {
            name: "integer group key",
            columns: "g BIGINT",
            rows: vec!["1", "2", "1", "3", "2"],
            select: "g, COUNT(*)",
            group_by: "g",
            expected_rows: vec![
                vec![Value::int64(1), Value::int64(2)],
                vec![Value::int64(2), Value::int64(2)],
                vec![Value::int64(3), Value::int64(1)],
            ],
        },
    ]
}

#[test]
fn count_star_group() {
    run_group_cases(&count_star_group_cases());
}

fn sum_group_cases() -> Vec<GroupCase> {
    vec![
        GroupCase {
            name: "SUM per category",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 10", "'b', 20", "'a', 30", "'b', 5"],
            select: "cat, SUM(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(40)], vec![
                Value::varchar("b".into()),
                Value::int64(25),
            ]],
        },
        GroupCase {
            name: "SUM skips NULLs within a group",
            columns: "cat STRING, n BIGINT",
            // second row: cat='a', n=NULL (SQL NULL literal)
            rows: vec!["'a', 10", "'a', NULL", "'a', 20"],
            select: "cat, SUM(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(30)]],
        },
        GroupCase {
            name: "SUM with integer group key",
            columns: "g BIGINT, v BIGINT",
            rows: vec!["1, 5", "2, 10", "1, 3"],
            select: "g, SUM(v)",
            group_by: "g",
            expected_rows: vec![vec![Value::int64(1), Value::int64(8)], vec![
                Value::int64(2),
                Value::int64(10),
            ]],
        },
    ]
}

#[test]
fn sum_group() {
    run_group_cases(&sum_group_cases());
}

fn avg_group_cases() -> Vec<GroupCase> {
    vec![
        GroupCase {
            name: "AVG per group",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 2", "'b', 6", "'a', 4", "'b', 10"],
            select: "cat, AVG(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::float64(3.0)], vec![
                Value::varchar("b".into()),
                Value::float64(8.0),
            ]],
        },
        GroupCase {
            name: "AVG of floats per group",
            columns: "cat STRING, f FLOAT",
            rows: vec!["'x', 1.0", "'x', 3.0", "'y', 2.0"],
            select: "cat, AVG(f)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("x".into()), Value::float64(2.0)], vec![
                Value::varchar("y".into()),
                Value::float64(2.0),
            ]],
        },
    ]
}

#[test]
fn avg_group() {
    run_group_cases(&avg_group_cases());
}

// ── GROUP BY — MIN / MAX ──────────────────────────────────────────────────────

fn min_max_group_cases() -> Vec<GroupCase> {
    vec![
        GroupCase {
            name: "MIN per group",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 5", "'b', 3", "'a', 2", "'b', 9"],
            select: "cat, MIN(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(2)], vec![
                Value::varchar("b".into()),
                Value::int64(3),
            ]],
        },
        GroupCase {
            name: "MAX per group",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 5", "'b', 3", "'a', 2", "'b', 9"],
            select: "cat, MAX(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(5)], vec![
                Value::varchar("b".into()),
                Value::int64(9),
            ]],
        },
        GroupCase {
            name: "MIN and MAX together per group",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 7", "'a', 1", "'a', 4", "'b', 10", "'b', 6"],
            select: "cat, MIN(n), MAX(n)",
            group_by: "cat",
            expected_rows: vec![
                vec![Value::varchar("a".into()), Value::int64(1), Value::int64(7)],
                vec![
                    Value::varchar("b".into()),
                    Value::int64(6),
                    Value::int64(10),
                ],
            ],
        },
        GroupCase {
            name: "MIN string per group — lexicographic",
            columns: "g BIGINT, s STRING",
            rows: vec!["1, 'banana'", "1, 'apple'", "2, 'zebra'", "2, 'mango'"],
            select: "g, MIN(s)",
            group_by: "g",
            expected_rows: vec![vec![Value::int64(1), Value::varchar("apple".into())], vec![
                Value::int64(2),
                Value::varchar("mango".into()),
            ]],
        },
    ]
}

#[test]
fn min_max_group() {
    run_group_cases(&min_max_group_cases());
}

// ── GROUP BY — COUNT(col) ─────────────────────────────────────────────────────

fn count_col_group_cases() -> Vec<GroupCase> {
    vec![
        GroupCase {
            name: "COUNT(col) per group, no NULLs",
            columns: "cat STRING, n BIGINT",
            rows: vec!["'a', 1", "'a', 2", "'b', 3"],
            select: "cat, COUNT(n)",
            group_by: "cat",
            expected_rows: vec![vec![Value::varchar("a".into()), Value::int64(2)], vec![
                Value::varchar("b".into()),
                Value::int64(1),
            ]],
        },
        GroupCase {
            name: "COUNT(*) and COUNT(col) differ when NULLs in one group",
            columns: "cat STRING, n BIGINT",
            // group 'a': two rows (second has n=NULL), group 'b': one row
            rows: vec!["'a', 10", "'a', NULL", "'b', 5"],
            select: "cat, COUNT(*), COUNT(n)",
            group_by: "cat",
            expected_rows: vec![
                vec![Value::varchar("a".into()), Value::int64(2), Value::int64(1)],
                vec![Value::varchar("b".into()), Value::int64(1), Value::int64(1)],
            ],
        },
    ]
}

#[test]
fn count_col_group() {
    run_group_cases(&count_col_group_cases());
}

// ── GROUP BY — with WHERE filter ──────────────────────────────────────────────
//
// WHERE runs before aggregation; filtered rows never enter any group.
// The harness structs above do not carry a WHERE clause, so these cases are
// written as standalone tests.

#[test]
fn group_with_where_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, cat STRING, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 'a', 10)");
    db.run_ok("INSERT INTO t VALUES (2, 'a', 20)");
    db.run_ok("INSERT INTO t VALUES (3, 'b', 5)");
    db.run_ok("INSERT INTO t VALUES (4, 'b', 50)");

    // WHERE keeps only rows with n <= 20; 'b' row with n=50 is excluded
    let mut actual =
        match db.run_ok("SELECT cat, COUNT(*), SUM(n) FROM t WHERE n <= 20 GROUP BY cat") {
            StatementResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|t| {
                    (0..t.len())
                        .map(|i| t.get(i).unwrap().clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            other => panic!("expected Selected, got {other:?}"),
        };
    sort_rows(&mut actual);

    let mut expected = vec![
        vec![
            Value::varchar("a".into()),
            Value::int64(2),
            Value::int64(30),
        ],
        vec![Value::varchar("b".into()), Value::int64(1), Value::int64(5)],
    ];
    sort_rows(&mut expected);

    assert_eq!(actual, expected);
}

#[test]
fn group_by_two_columns() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, region STRING, product STRING, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 'east', 'widget', 5)");
    db.run_ok("INSERT INTO t VALUES (2, 'east', 'gadget', 3)");
    db.run_ok("INSERT INTO t VALUES (3, 'west', 'widget', 7)");
    db.run_ok("INSERT INTO t VALUES (4, 'east', 'widget', 2)");
    db.run_ok("INSERT INTO t VALUES (5, 'west', 'gadget', 4)");

    let mut actual =
        match db.run_ok("SELECT region, product, SUM(n) FROM t GROUP BY region, product") {
            StatementResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|t| {
                    (0..t.len())
                        .map(|i| t.get(i).unwrap().clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            other => panic!("expected Selected, got {other:?}"),
        };
    sort_rows(&mut actual);

    let mut expected = vec![
        vec![
            Value::varchar("east".into()),
            Value::varchar("gadget".into()),
            Value::int64(3),
        ],
        vec![
            Value::varchar("east".into()),
            Value::varchar("widget".into()),
            Value::int64(7),
        ],
        vec![
            Value::varchar("west".into()),
            Value::varchar("gadget".into()),
            Value::int64(4),
        ],
        vec![
            Value::varchar("west".into()),
            Value::varchar("widget".into()),
            Value::int64(7),
        ],
    ];
    sort_rows(&mut expected);

    assert_eq!(actual, expected);
}

// ── NULL handling — group key is NULL ─────────────────────────────────────────
//
// A NULL group key forms its own group (NULL != NULL in SQL but GROUP BY
// treats all NULLs as one group).

#[test]
fn null_group_key_forms_its_own_group() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, cat STRING)");
    db.run_ok("INSERT INTO t VALUES (1, 'a')");
    db.run_ok("INSERT INTO t VALUES (2, 'a')");
    db.run_ok("INSERT INTO t (id) VALUES (3)"); // cat = NULL
    db.run_ok("INSERT INTO t (id) VALUES (4)"); // cat = NULL

    let mut actual = match db.run_ok("SELECT cat, COUNT(*) FROM t GROUP BY cat") {
        StatementResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|t| {
                (0..t.len())
                    .map(|i| t.get(i).unwrap().clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        other => panic!("expected Selected, got {other:?}"),
    };
    sort_rows(&mut actual);

    let mut expected = vec![vec![Value::varchar("a".into()), Value::int64(2)], vec![
        Value::Null,
        Value::int64(2),
    ]];
    sort_rows(&mut expected);

    assert_eq!(actual, expected);
}

// ── unsupported features ──────────────────────────────────────────────────────
//
// Lock in the error shape so breakage is deliberate.

#[test]
fn having_is_not_yet_supported() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 10)");

    let result = db.run("SELECT n, COUNT(*) FROM t GROUP BY n HAVING COUNT(*) > 1");
    assert!(
        result.is_err(),
        "HAVING should return an error until it is implemented"
    );
}

#[test]
fn order_by_with_group_by_is_not_yet_supported() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, n BIGINT)");
    db.run_ok("INSERT INTO t VALUES (1, 10)");

    let result = db.run("SELECT n, COUNT(*) FROM t GROUP BY n ORDER BY n");
    assert!(
        result.is_err(),
        "ORDER BY combined with GROUP BY should return an error until it is implemented"
    );
}
