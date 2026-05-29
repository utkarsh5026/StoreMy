//! End-to-end tests for JSON column storage and operators.
//!
//! # Architecture
//!
//! Every operator is covered by two kinds of test:
//!
//! - **Expression cases** (`ExprCase`) — single-row table, check the evaluated expression value
//!   directly. Covers all JSON sub-value types (string, integer, float, bool true/false, JSON null
//!   → SQL NULL, nested object, array), plus missing-key → SQL NULL and NULL-column → SQL NULL
//!   (null propagation).
//!
//! - **Filter cases** (`FilterCase`) — multi-row table, check which row IDs survive a `WHERE`
//!   predicate. Covers the operator in a real query pipeline.
//!
//! # Adding a new operator
//!
//! 1. Implement the operator in `execution/eval/json.rs` and wire it up in
//!    `execution/eval/mod.rs::eval_binary`.
//! 2. Add `fn <op>_cases() -> Vec<ExprCase>` and `fn <op>_filter_cases() -> Vec<FilterCase>` in the
//!    matching section below.
//! 3. Add `#[test]` functions calling `run_expr_cases(...)` / `run_filter_cases(...)`.
//!
//! The harness creates a fresh isolated `TestDb` per case — no shared state.

use storemy::{
    Value,
    engine::StatementResult,
    types::{DynValue, FixedValue},
};

use crate::common::TestDb;

/// Sentinel: insert a NULL value for the `payload` column.
const NULL_DOC: &str = "__NULL__";

/// One expression-level assertion.
///
/// The harness creates `t(id INT, payload JSON)`, inserts `doc` as the JSON
/// column (or NULL when `doc == NULL_DOC`), runs `SELECT <expr> FROM t`, and
/// asserts that the value in column 0 equals `expected`.
struct ExprCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// JSON literal stored in `payload`, or `NULL_DOC` for a SQL NULL column.
    doc: &'static str,
    /// SQL expression to `SELECT` — may reference `payload`.
    expr: &'static str,
    /// Expected value of column 0 in the single returned row.
    expected: Value,
}

/// One WHERE-filter assertion over multiple rows.
///
/// The harness creates `t(id INT, payload JSON)`, inserts each `docs[i]` with
/// `id = i + 1`, runs `SELECT id FROM t WHERE <predicate>`, and asserts the
/// returned IDs (sorted) match `expected_ids`.
struct FilterCase {
    /// Human-readable label shown on assertion failure.
    name: &'static str,
    /// JSON doc per row. Use `NULL_DOC` for a row with a NULL payload. Row IDs
    /// are 1-indexed (`docs[0]` → id = 1, `docs[1]` → id = 2, …).
    docs: Vec<&'static str>,
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

/// Extract the raw JSON string from a `Value::Dyn(DynValue::Json(...))`.
fn as_json_str(v: &Value) -> &str {
    match v {
        Value::Dyn(DynValue::Json(s)) => s.as_str(),
        other => panic!("expected JSON value, got {other:?}"),
    }
}

fn run_expr_cases(cases: &[ExprCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok("CREATE TABLE t (id INT, payload JSON)");
        if case.doc == NULL_DOC {
            db.run_ok("INSERT INTO t (id) VALUES (1)");
        } else {
            db.run_ok(&format!("INSERT INTO t VALUES (1, '{}')", case.doc));
        }
        let rows = select_rows(&db, &format!("SELECT {} FROM t", case.expr));
        assert!(
            !rows.is_empty(),
            "case {:?}: SELECT {} FROM t produced no rows (doc={})",
            case.name,
            case.expr,
            case.doc,
        );
        assert_eq!(
            rows[0][0], case.expected,
            "case {:?}: SELECT {} FROM t\n  doc  = {}\n  got  = {:?}",
            case.name, case.expr, case.doc, rows[0][0],
        );
    }
}

fn run_filter_cases(cases: &[FilterCase]) {
    for case in cases {
        let db = TestDb::new();
        db.run_ok("CREATE TABLE t (id INT, payload JSON)");
        for (i, doc) in case.docs.iter().enumerate() {
            let id = i32::try_from(i).expect("test row index fits in i32") + 1;
            if *doc == NULL_DOC {
                db.run_ok(&format!("INSERT INTO t (id) VALUES ({id})"));
            } else {
                db.run_ok(&format!("INSERT INTO t VALUES ({id}, '{doc}')"));
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

/// Build a valid JSON object whose byte length exceeds 255 bytes (inline limit).
fn large_json(type_val: &str) -> String {
    let pad = "x".repeat(300);
    format!(r#"{{"type":"{type_val}","pad":"{pad}","meta":{{"level":1}}}}"#)
}

#[test]
fn storage_insert_and_scan() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click","x":10}')"#);

    let rows = db.scan_all("events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::int32(1)));
    assert!(as_json_str(rows[0].get(1).unwrap()).contains("click"));
}

#[test]
fn storage_invalid_json_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    assert!(
        db.run(r"INSERT INTO t VALUES (1, 'not valid json')")
            .is_err(),
        "inserting invalid JSON must fail"
    );
}

// ── `->` (extract as JSON) ────────────────────────────────────────────────────
//
// Returns the matched sub-value re-encoded as a JSON Value.
// A string "hello" stays "hello" (with quotes); scalars, arrays, objects
// are all valid JSON Values that can be chained with another `->`.

fn arrow_json_cases() -> Vec<ExprCase> {
    // serde_json (without preserve_order) uses BTreeMap → keys are sorted.
    vec![
        ExprCase {
            name: "string value",
            doc: r#"{"s":"hello"}"#,
            expr: "payload->'s'",
            expected: Value::json(r#""hello""#).unwrap(),
        },
        ExprCase {
            name: "integer value",
            doc: r#"{"n":42}"#,
            expr: "payload->'n'",
            expected: Value::json("42").unwrap(),
        },
        ExprCase {
            name: "float value",
            doc: r#"{"f":1.5}"#,
            expr: "payload->'f'",
            expected: Value::json("1.5").unwrap(),
        },
        ExprCase {
            name: "bool true",
            doc: r#"{"b":true}"#,
            expr: "payload->'b'",
            expected: Value::json("true").unwrap(),
        },
        ExprCase {
            name: "bool false",
            doc: r#"{"b":false}"#,
            expr: "payload->'b'",
            expected: Value::json("false").unwrap(),
        },
        // JSON null → the found value is serde_json::Value::Null, caught by the
        // early-return guard in eval_arrow → SQL NULL.
        ExprCase {
            name: "json null value → SQL NULL",
            doc: r#"{"k":null}"#,
            expr: "payload->'k'",
            expected: Value::Null,
        },
        ExprCase {
            name: "nested object",
            doc: r#"{"o":{"x":1}}"#,
            expr: "payload->'o'",
            expected: Value::json(r#"{"x":1}"#).unwrap(),
        },
        ExprCase {
            name: "array value",
            doc: r#"{"a":[1,2,3]}"#,
            expr: "payload->'a'",
            expected: Value::json("[1,2,3]").unwrap(),
        },
        ExprCase {
            name: "missing key → SQL NULL",
            doc: r#"{"a":1}"#,
            expr: "payload->'missing'",
            expected: Value::Null,
        },
        // NULL column → eval_binary short-circuits at the null guard → SQL NULL.
        ExprCase {
            name: "NULL column → SQL NULL (null propagation)",
            doc: NULL_DOC,
            expr: "payload->'k'",
            expected: Value::Null,
        },
    ]
}

fn arrow_json_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "filter by nested field",
            docs: vec![
                r#"{"user":{"role":"admin"}}"#,
                r#"{"user":{"role":"viewer"}}"#,
                r#"{"user":{"role":"admin"}}"#,
            ],
            predicate: r"payload->'user'->>'role' = 'admin'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "missing key does not match",
            docs: vec![r#"{"type":"click"}"#, r#"{"x":1}"#],
            predicate: "payload->>'type' = 'click'",
            expected_ids: vec![1],
        },
        FilterCase {
            name: "NULL column skipped in filter",
            docs: vec![NULL_DOC, r#"{"k":"v"}"#],
            predicate: "payload->>'k' = 'v'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn arrow_json_value_types() {
    run_expr_cases(&arrow_json_cases());
}

#[test]
fn arrow_json_filter() {
    run_filter_cases(&arrow_json_filter_cases());
}

fn arrow_text_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "string value — quotes stripped",
            doc: r#"{"s":"hello"}"#,
            expr: "payload->>'s'",
            expected: Value::varchar("hello".to_string()),
        },
        ExprCase {
            name: "integer value — rendered as digits",
            doc: r#"{"n":42}"#,
            expr: "payload->>'n'",
            expected: Value::varchar("42".to_string()),
        },
        ExprCase {
            name: "float value — rendered as decimal",
            doc: r#"{"f":1.5}"#,
            expr: "payload->>'f'",
            expected: Value::varchar("1.5".to_string()),
        },
        ExprCase {
            name: "bool true — rendered as text",
            doc: r#"{"b":true}"#,
            expr: "payload->>'b'",
            expected: Value::varchar("true".to_string()),
        },
        ExprCase {
            name: "bool false — rendered as text",
            doc: r#"{"b":false}"#,
            expr: "payload->>'b'",
            expected: Value::varchar("false".to_string()),
        },
        ExprCase {
            name: "json null value → SQL NULL",
            doc: r#"{"k":null}"#,
            expr: "payload->>'k'",
            expected: Value::Null,
        },
        ExprCase {
            name: "nested object — rendered as JSON text",
            doc: r#"{"o":{"x":1}}"#,
            expr: "payload->>'o'",
            expected: Value::varchar(r#"{"x":1}"#.to_string()),
        },
        ExprCase {
            name: "array — rendered as JSON text",
            doc: r#"{"a":[1,2,3]}"#,
            expr: "payload->>'a'",
            expected: Value::varchar("[1,2,3]".to_string()),
        },
        ExprCase {
            name: "missing key → SQL NULL",
            doc: r#"{"a":1}"#,
            expr: "payload->>'missing'",
            expected: Value::Null,
        },
        ExprCase {
            name: "NULL column → SQL NULL (null propagation)",
            doc: NULL_DOC,
            expr: "payload->>'k'",
            expected: Value::Null,
        },
        ExprCase {
            name: "chain -> then ->> extracts nested string",
            doc: r#"{"u":{"name":"alice"}}"#,
            expr: "payload->'u'->>'name'",
            expected: Value::varchar("alice".to_string()),
        },
        ExprCase {
            name: "two keys from same row",
            doc: r#"{"type":"click","x":10}"#,
            // SQLite-style: project two expressions; check only col 0 here.
            // The second key is tested separately in the multi-column test below.
            expr: "payload->>'type'",
            expected: Value::varchar("click".to_string()),
        },
    ]
}

fn arrow_text_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "filter by string value",
            docs: vec![
                r#"{"type":"click"}"#,
                r#"{"type":"hover"}"#,
                r#"{"type":"click"}"#,
            ],
            predicate: "payload->>'type' = 'click'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "filter by number-as-text",
            docs: vec![r#"{"score":100}"#, r#"{"score":200}"#, r#"{"score":100}"#],
            predicate: "payload->>'score' = '100'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "filter by bool-as-text",
            docs: vec![
                r#"{"active":true}"#,
                r#"{"active":false}"#,
                r#"{"active":true}"#,
            ],
            predicate: "payload->>'active' = 'true'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "NULL column skipped in filter",
            docs: vec![NULL_DOC, r#"{"k":"v"}"#],
            predicate: "payload->>'k' = 'v'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn arrow_text_value_types() {
    run_expr_cases(&arrow_text_cases());
}

#[test]
fn arrow_text_multi_key_same_row() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click","x":10}')"#);

    let rows = select_rows(&db, "SELECT payload->>'type', payload->>'x' FROM events");
    assert_eq!(rows[0][0].as_str().unwrap(), "click");
    assert_eq!(rows[0][1].as_str().unwrap(), "10");
}

#[test]
fn arrow_text_filter() {
    run_filter_cases(&arrow_text_filter_cases());
}

// ── `?` (key exists) ─────────────────────────────────────────────────────────
//
// Object: true iff `key` is a top-level field name.
// Array: true iff any element is a string equal to `key`.
// Other JSON types: always false.
// NULL operand: SQL NULL (null propagation via eval_binary guard).

fn key_exists_cases() -> Vec<ExprCase> {
    vec![
        // ── object checks ──
        ExprCase {
            name: "existing top-level key → true",
            doc: r#"{"type":"click","x":1}"#,
            expr: "payload ? 'type'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "another existing key → true",
            doc: r#"{"type":"click","x":1}"#,
            expr: "payload ? 'x'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "missing key → false",
            doc: r#"{"type":"click"}"#,
            expr: "payload ? 'missing'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "empty object → false",
            doc: r"{}",
            expr: "payload ? 'k'",
            expected: Value::bool(false),
        },
        // nested keys are NOT visible at the top level
        ExprCase {
            name: "nested key not visible at top level → false",
            doc: r#"{"outer":{"inner":1}}"#,
            expr: "payload ? 'inner'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "outer key of nested object → true",
            doc: r#"{"outer":{"inner":1}}"#,
            expr: "payload ? 'outer'",
            expected: Value::bool(true),
        },
        // ── array checks ──
        ExprCase {
            name: "string element present in array → true",
            doc: r#"["rust","sql","json"]"#,
            expr: "payload ? 'sql'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "string element absent from array → false",
            doc: r#"["a","b","c"]"#,
            expr: "payload ? 'z'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "empty array → false",
            doc: r"[]",
            expr: "payload ? 'a'",
            expected: Value::bool(false),
        },
        // integer elements never match a string key (? is string-only)
        ExprCase {
            name: "integer elements in array do not match string key → false",
            doc: r"[1,2,3]",
            expr: "payload ? '1'",
            expected: Value::bool(false),
        },
        // ── null propagation ──
        ExprCase {
            name: "NULL column → SQL NULL",
            doc: NULL_DOC,
            expr: "payload ? 'k'",
            expected: Value::Null,
        },
    ]
}

fn key_exists_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "rows with key survive filter",
            docs: vec![r#"{"type":"click"}"#, r#"{"x":1}"#, r#"{"type":"hover"}"#],
            predicate: "payload ? 'type'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "array element membership filter",
            docs: vec![
                r#"["rust","go"]"#,
                r#"["python","java"]"#,
                r#"["rust","sql"]"#,
            ],
            predicate: "payload ? 'rust'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "NULL column rows not matched",
            docs: vec![NULL_DOC, r#"{"k":1}"#],
            predicate: "payload ? 'k'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn key_exists_value_types() {
    run_expr_cases(&key_exists_cases());
}

#[test]
fn key_exists_filter() {
    run_filter_cases(&key_exists_filter_cases());
}

// ── `@>` (contains) ──────────────────────────────────────────────────────────
//
// `l @> r` → true iff every key/value pair in r exists in l (recursively).
// Array variant: every element of r must appear somewhere in l.
// Scalar: equality.
// NULL either side → SQL NULL.
#[allow(clippy::too_many_lines)]
fn contains_cases() -> Vec<ExprCase> {
    vec![
        // ── object subset checks ──
        ExprCase {
            name: "object contains single-key subset → true",
            doc: r#"{"type":"click","x":10,"y":20}"#,
            expr: r#"payload @> '{"type":"click"}'"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "object contains multi-key subset → true",
            doc: r#"{"type":"click","x":10,"y":20}"#,
            expr: r#"payload @> '{"type":"click","x":10}'"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "object contains itself → true",
            doc: r#"{"type":"click"}"#,
            expr: r#"payload @> '{"type":"click"}'"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "empty object always contained → true",
            doc: r#"{"type":"click","x":10}"#,
            expr: r"payload @> '{}'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "needle has missing key → false",
            doc: r#"{"type":"click"}"#,
            expr: r#"payload @> '{"missing":"val"}'"#,
            expected: Value::bool(false),
        },
        ExprCase {
            name: "needle has wrong value → false",
            doc: r#"{"type":"click"}"#,
            expr: r#"payload @> '{"type":"hover"}'"#,
            expected: Value::bool(false),
        },
        ExprCase {
            name: "needle is bigger than haystack → false",
            doc: r#"{"a":1}"#,
            expr: r#"payload @> '{"a":1,"b":2}'"#,
            expected: Value::bool(false),
        },
        ExprCase {
            name: "nested object subset → true",
            doc: r#"{"user":{"name":"alice","age":30}}"#,
            expr: r#"payload @> '{"user":{"name":"alice"}}'"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "nested object wrong leaf value → false",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: r#"payload @> '{"user":{"name":"bob"}}'"#,
            expected: Value::bool(false),
        },
        ExprCase {
            name: "needle deeper than haystack → false",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: r#"payload @> '{"user":{"name":"alice","age":30}}'"#,
            expected: Value::bool(false),
        },
        // ── array checks ──
        ExprCase {
            name: "array contains element subset → true",
            doc: r"[1,2,3,4]",
            expr: r"payload @> '[2,4]'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "array missing one element → false",
            doc: r"[1,2,3]",
            expr: r"payload @> '[2,5]'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "empty array always contained → true",
            doc: r"[1,2,3]",
            expr: r"payload @> '[]'",
            expected: Value::bool(true),
        },
        // ── scalar checks ──
        ExprCase {
            name: "integer scalar equality → true",
            doc: r"42",
            expr: r"payload @> '42'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "integer scalar inequality → false",
            doc: r"42",
            expr: r"payload @> '43'",
            expected: Value::bool(false),
        },
        ExprCase {
            name: "string scalar equality → true",
            doc: r#""hello""#,
            expr: r#"payload @> '"hello"'"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "array contains scalar element → true",
            doc: r"[1,2,3]",
            expr: r"payload @> '2'",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "array does not contain scalar → false",
            doc: r"[1,2,3]",
            expr: r"payload @> '5'",
            expected: Value::bool(false),
        },
        // ── null propagation ──
        ExprCase {
            name: "NULL column → SQL NULL",
            doc: NULL_DOC,
            expr: r"payload @> '{}'",
            expected: Value::Null,
        },
    ]
}

fn contains_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "filter by object subset",
            docs: vec![
                r#"{"type":"click","x":1}"#,
                r#"{"type":"hover"}"#,
                r#"{"type":"click","x":2}"#,
            ],
            predicate: r#"payload @> '{"type":"click"}'"#,
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "filter by array subset",
            docs: vec![r"[1,2,3]", r"[4,5,6]", r"[1,2,7]"],
            predicate: r"payload @> '[1,2]'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "NULL column skipped in filter",
            docs: vec![NULL_DOC, r#"{"k":1}"#],
            predicate: r"payload @> '{}'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn contains_value_types() {
    run_expr_cases(&contains_cases());
}

#[test]
fn contains_filter() {
    run_filter_cases(&contains_filter_cases());
}

// ── `<@` (contained by) ──────────────────────────────────────────────────────
//
// `a <@ b` ≡ `b @> a`.
// eval_binary dispatches ContainedBy as eval_contains(r, l) — args are swapped.

fn contained_by_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "small doc contained by larger one → true",
            doc: r#"{"type":"click","x":10}"#,
            expr: r#"'{"type":"click"}' <@ payload"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "doc not contained by smaller one → false",
            doc: r#"{"type":"click"}"#,
            expr: r#"'{"type":"click","x":10}' <@ payload"#,
            expected: Value::bool(false),
        },
        ExprCase {
            name: "empty object contained by any object → true",
            doc: r#"{"type":"click"}"#,
            expr: r"'{}' <@ payload",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "array subset contained by larger array → true",
            doc: r"[1,2,3,4]",
            expr: r"'[2,4]' <@ payload",
            expected: Value::bool(true),
        },
        ExprCase {
            name: "array not a subset → false",
            doc: r"[1,2,3]",
            expr: r"'[2,5]' <@ payload",
            expected: Value::bool(false),
        },
        // <@ is the mathematical inverse of @>
        ExprCase {
            name: "symmetry: <@ equals flipped @>",
            doc: r#"{"type":"click","x":10}"#,
            // Both sides of the = compare the same logical predicate.
            // We just check <@ directly here; the symmetry test is separate.
            expr: r#"'{"type":"click"}' <@ payload"#,
            expected: Value::bool(true),
        },
        ExprCase {
            name: "NULL column → SQL NULL",
            doc: NULL_DOC,
            expr: r"'{}' <@ payload",
            expected: Value::Null,
        },
    ]
}

fn contained_by_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "literal contained by column value",
            docs: vec![r#"{"type":"click","x":1}"#, r#"{"type":"hover"}"#],
            predicate: r#"'{"type":"click"}' <@ payload"#,
            expected_ids: vec![1],
        },
        FilterCase {
            name: "NULL column skipped",
            docs: vec![NULL_DOC, r#"{"k":1}"#],
            predicate: r"'{}' <@ payload",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn contained_by_value_types() {
    run_expr_cases(&contained_by_cases());
}

#[test]
fn contained_by_filter() {
    run_filter_cases(&contained_by_filter_cases());
}

#[test]
fn contained_by_is_inverse_of_contains() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"type":"click","x":10}')"#);

    let contains = select_rows(&db, r#"SELECT payload @> '{"type":"click"}' FROM t"#);
    let contained_by = select_rows(&db, r#"SELECT '{"type":"click"}' <@ payload FROM t"#);
    assert_eq!(
        contains[0][0], contained_by[0][0],
        "<@ must be the inverse of @>"
    );
}

fn hash_arrow_json_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "single key as json",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: "payload #> '{user}'",
            expected: Value::json(r#"{"name":"alice"}"#).unwrap(),
        },
        ExprCase {
            name: "two-level path as json",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: "payload #> '{user,name}'",
            expected: Value::json(r#""alice""#).unwrap(),
        },
        ExprCase {
            name: "array index via integer segment",
            doc: r#"{"items":["a","b","c"]}"#,
            expr: "payload #> '{items,1}'",
            expected: Value::json(r#""b""#).unwrap(),
        },
        ExprCase {
            name: "missing intermediate key returns null",
            doc: r#"{"a":1}"#,
            expr: "payload #> '{missing,nested}'",
            expected: Value::Null,
        },
        ExprCase {
            name: "missing leaf returns null",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: "payload #> '{user,age}'",
            expected: Value::Null,
        },
        ExprCase {
            name: "empty path returns null",
            doc: r#"{"a":1}"#,
            expr: "payload #> '{}'",
            expected: Value::Null,
        },
        ExprCase {
            name: "NULL column returns null",
            doc: NULL_DOC,
            expr: "payload #> '{user}'",
            expected: Value::Null,
        },
    ]
}

fn hash_arrow_text_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "string leaf strips quotes",
            doc: r#"{"user":{"name":"alice"}}"#,
            expr: "payload #>> '{user,name}'",
            expected: Value::varchar("alice".to_string()),
        },
        ExprCase {
            name: "integer leaf rendered as digits",
            doc: r#"{"meta":{"count":42}}"#,
            expr: "payload #>> '{meta,count}'",
            expected: Value::varchar("42".to_string()),
        },
        ExprCase {
            name: "bool leaf rendered as text",
            doc: r#"{"flags":{"active":true}}"#,
            expr: "payload #>> '{flags,active}'",
            expected: Value::varchar("true".to_string()),
        },
        ExprCase {
            name: "three levels deep",
            doc: r#"{"a":{"b":{"c":"deep"}}}"#,
            expr: "payload #>> '{a,b,c}'",
            expected: Value::varchar("deep".to_string()),
        },
        ExprCase {
            name: "missing path returns null",
            doc: r#"{"a":1}"#,
            expr: "payload #>> '{a,b}'",
            expected: Value::Null,
        },
        ExprCase {
            name: "NULL column returns null",
            doc: NULL_DOC,
            expr: "payload #>> '{user,name}'",
            expected: Value::Null,
        },
    ]
}

fn hash_arrow_filter_cases() -> Vec<FilterCase> {
    vec![
        FilterCase {
            name: "filter by nested field via #>>",
            docs: vec![
                r#"{"user":{"role":"admin"}}"#,
                r#"{"user":{"role":"viewer"}}"#,
                r#"{"user":{"role":"admin"}}"#,
            ],
            predicate: "payload #>> '{user,role}' = 'admin'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "filter by deeply nested number-as-text",
            docs: vec![
                r#"{"meta":{"score":100}}"#,
                r#"{"meta":{"score":200}}"#,
                r#"{"meta":{"score":100}}"#,
            ],
            predicate: "payload #>> '{meta,score}' = '100'",
            expected_ids: vec![1, 3],
        },
        FilterCase {
            name: "missing path does not match",
            docs: vec![r#"{"a":{"b":1}}"#, r#"{"x":1}"#],
            predicate: "payload #>> '{a,b}' = '1'",
            expected_ids: vec![1],
        },
        FilterCase {
            name: "NULL column skipped in filter",
            docs: vec![NULL_DOC, r#"{"user":{"name":"alice"}}"#],
            predicate: "payload #>> '{user,name}' = 'alice'",
            expected_ids: vec![2],
        },
    ]
}

#[test]
fn hash_arrow_json_value_types() {
    run_expr_cases(&hash_arrow_json_cases());
}

#[test]
fn hash_arrow_text_value_types() {
    run_expr_cases(&hash_arrow_text_cases());
}

#[test]
fn hash_arrow_filter() {
    run_filter_cases(&hash_arrow_filter_cases());
}

// ── overflow (JSON > 255 bytes) ───────────────────────────────────────────────

#[test]
fn overflow_round_trip() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    let doc = large_json("click");
    assert!(doc.len() > 255, "test requires doc > 255 bytes");

    db.run_ok(&format!("INSERT INTO t VALUES (1, '{doc}')"));

    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 1);
    let stored = as_json_str(rows[0].get(1).unwrap());
    assert_eq!(
        stored, doc,
        "full JSON must survive the overflow round-trip"
    );
}

#[test]
fn overflow_arrow_text_extracts_short_key() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        large_json("hover")
    ));

    let rows = select_rows(&db, "SELECT payload->>'type' FROM t");
    assert_eq!(rows[0][0].as_str().unwrap(), "hover");
}

#[test]
fn overflow_arrow_json_extracts_nested_object() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        large_json("scroll")
    ));

    let rows = select_rows(&db, "SELECT payload->'meta' FROM t");
    assert!(
        as_json_str(&rows[0][0]).contains("level"),
        "nested object must be extractable from overflow doc"
    );
}

#[test]
fn overflow_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        large_json("click")
    ));
    db.run_ok(&format!(
        "INSERT INTO t VALUES (2, '{}')",
        large_json("hover")
    ));

    let rows = select_rows(&db, "SELECT id FROM t WHERE payload->>'type' = 'click'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::int32(1));
}

#[test]
fn overflow_mixed_inline_and_overflow_queryable() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"type":"small"}')"#);
    db.run_ok(&format!(
        "INSERT INTO t VALUES (2, '{}')",
        large_json("big")
    ));

    let rows = select_rows(&db, "SELECT id, payload->>'type' FROM t");
    assert_eq!(rows.len(), 2);

    let mut pairs: Vec<(i32, String)> = rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Fixed(FixedValue::Int32(v)) => *v,
                other => panic!("expected Int32, got {other:?}"),
            };
            let ty = r[1].as_str().unwrap().to_string();
            (id, ty)
        })
        .collect();
    pairs.sort_by_key(|(id, _)| *id);

    assert_eq!(pairs[0], (1, "small".to_string()));
    assert_eq!(pairs[1], (2, "big".to_string()));
}

// ── DML integration ───────────────────────────────────────────────────────────

#[test]
fn dml_delete_where_json_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO events VALUES
        (1, '{"type":"click"}'),
        (2, '{"type":"hover"}'),
        (3, '{"type":"click"}')"#,
    );

    let result = db.run_ok(r"DELETE FROM events WHERE payload->>'type' = 'click'");
    let StatementResult::Deleted { rows, .. } = result else {
        panic!("expected Deleted");
    };
    assert_eq!(rows, 2);
    assert_eq!(db.scan_all("events").len(), 1);
}

#[test]
fn dml_update_where_json_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO events VALUES
        (1, '{"type":"click"}'),
        (2, '{"type":"hover"}')"#,
    );

    db.run_ok(r"UPDATE events SET id = 99 WHERE payload->>'type' = 'hover'");

    let rows = select_rows(
        &db,
        "SELECT id FROM events WHERE payload->>'type' = 'hover'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::int32(99));
}

#[test]
fn dml_delete_with_contains_filter() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO t VALUES
        (1, '{"type":"click","x":1}'),
        (2, '{"type":"hover"}'),
        (3, '{"type":"click","x":2}')"#,
    );

    let result = db.run_ok(r#"DELETE FROM t WHERE payload @> '{"type":"click"}'"#);
    let StatementResult::Deleted { rows, .. } = result else {
        panic!("expected Deleted");
    };
    assert_eq!(rows, 2);
    assert_eq!(db.scan_all("t").len(), 1);
}

#[test]
fn dml_null_column_in_where_does_not_crash() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    db.run_ok("INSERT INTO t (id) VALUES (1)");
    db.run_ok(r#"INSERT INTO t VALUES (2, '{"k":"v"}')"#);

    let rows = select_rows(&db, "SELECT id FROM t WHERE data->>'k' = 'v'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::int32(2));
}
