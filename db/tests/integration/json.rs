//! End-to-end tests for JSON column storage and the `->` / `->>` path operators.
//!
//! Covers the full pipeline: SQL text → parser → binder → planner → executor.
//! Each test stands alone with its own `TestDb`.

use storemy::{Value, engine::StatementResult, types::DynValue};

use crate::common::TestDb;

fn select_rows(db: &TestDb, sql: &str) -> Vec<Vec<Value>> {
    match db.run_ok(sql) {
        StatementResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|t| (0..t.len()).map(|i| t.get(i).unwrap().clone()).collect())
            .collect(),
        other => panic!("expected Selected, got {other:?}"),
    }
}

/// Extracts the raw JSON string from a `Value::Dyn(DynValue::Json(...))`.
fn as_json(v: &Value) -> &str {
    match v {
        Value::Dyn(DynValue::Json(s)) => s.as_str(),
        other => panic!("expected JSON value, got {other:?}"),
    }
}

// ── INSERT / round-trip ───────────────────────────────────────────────────────

#[test]
fn insert_and_scan_json_column() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click","x":10}')"#);

    let rows = db.scan_all("events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::int32(1)));
    let json_val = rows[0].get(1).unwrap();
    assert!(
        as_json(json_val).contains("click"),
        "stored JSON should contain the original content"
    );
}

#[test]
fn invalid_json_insert_is_rejected() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    assert!(
        db.run(r"INSERT INTO t VALUES (1, 'not valid json')")
            .is_err(),
        "inserting invalid JSON should fail"
    );
}

// ── SELECT with `->` projection ───────────────────────────────────────────────

#[test]
fn select_arrow_extracts_object_key_as_json() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click"}')"#);

    let rows = select_rows(&db, "SELECT payload->'type' FROM events");
    assert_eq!(rows.len(), 1);
    // -> re-encodes the matched sub-value as JSON, so a string keeps its quotes
    assert_eq!(as_json(&rows[0][0]), r#""click""#);
}

#[test]
fn select_arrow_text_extracts_string_without_quotes() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click"}')"#);

    let rows = select_rows(&db, "SELECT payload->>'type' FROM events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_str().unwrap(), "click");
}

#[test]
fn select_arrow_text_on_number_returns_digits_as_text() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE logs (id INT, data JSON)");
    db.run_ok(r#"INSERT INTO logs VALUES (1, '{"count":42}')"#);

    let rows = select_rows(&db, "SELECT data->>'count' FROM logs");
    assert_eq!(rows[0][0].as_str().unwrap(), "42");
}

#[test]
fn select_arrow_missing_key_returns_null() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"a":1}')"#);

    let rows = select_rows(&db, "SELECT data->'missing' FROM t");
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn select_arrow_chained_nested_object() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"user":{"name":"alice"}}')"#);

    let rows = select_rows(&db, "SELECT payload->'user'->>'name' FROM t");
    assert_eq!(rows[0][0].as_str().unwrap(), "alice");
}

#[test]
fn select_arrow_multiple_keys_same_row() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO events VALUES (1, '{"type":"click","x":10}')"#);

    let rows = select_rows(&db, "SELECT payload->>'type', payload->>'x' FROM events");
    assert_eq!(rows[0][0].as_str().unwrap(), "click");
    assert_eq!(rows[0][1].as_str().unwrap(), "10");
}

// ── WHERE filtering with `->` / `->>` ────────────────────────────────────────

#[test]
fn where_arrow_text_filters_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO events VALUES
        (1, '{"type":"click"}'),
        (2, '{"type":"hover"}'),
        (3, '{"type":"click"}')"#,
    );

    let rows = select_rows(
        &db,
        "SELECT id FROM events WHERE payload->>'type' = 'click'",
    );
    assert_eq!(rows.len(), 2, "only the two click events should match");
}

#[test]
fn delete_where_arrow_text_removes_matching_rows() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE events (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO events VALUES
        (1, '{"type":"click"}'),
        (2, '{"type":"hover"}'),
        (3, '{"type":"click"}')"#,
    );

    let result = db.run_ok(r"DELETE FROM events WHERE payload->>'type' = 'click'");
    let storemy::engine::StatementResult::Deleted { rows, .. } = result else {
        panic!("expected Deleted");
    };
    assert_eq!(rows, 2);
    assert_eq!(db.scan_all("events").len(), 1);
}

#[test]
fn update_where_arrow_text_updates_matching_rows() {
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
fn where_arrow_with_null_json_column_does_not_crash() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    // Insert a NULL for the JSON column via DEFAULT NULL behavior
    db.run_ok("INSERT INTO t (id) VALUES (1)");
    db.run_ok(r#"INSERT INTO t VALUES (2, '{"k":"v"}')"#);

    // WHERE on a NULL JSON column should propagate NULL (not match, not crash)
    let rows = select_rows(&db, "SELECT id FROM t WHERE data->>'k' = 'v'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::int32(2));
}

#[test]
fn question_key_exists_in_object_returns_true() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"type":"click","x":1}')"#);

    let rows = select_rows(&db, "SELECT payload ? 'type' FROM t");
    assert_eq!(rows[0][0], Value::bool(true));
}

#[test]
fn question_missing_key_returns_false() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"type":"click"}')"#);

    let rows = select_rows(&db, "SELECT payload ? 'missing' FROM t");
    assert_eq!(rows[0][0], Value::bool(false));
}

#[test]
fn question_filters_rows_in_where_clause() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(
        r#"INSERT INTO t VALUES
        (1, '{"type":"click"}'),
        (2, '{"x":1}'),
        (3, '{"type":"hover"}')"#,
    );

    let rows = select_rows(&db, "SELECT id FROM t WHERE payload ? 'type'");
    assert_eq!(rows.len(), 2);
}

#[test]
fn question_on_string_array_element_returns_true() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, tags JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '["rust","sql","json"]')"#);

    let rows = select_rows(&db, "SELECT tags ? 'sql' FROM t");
    assert_eq!(rows[0][0], Value::bool(true));
}

#[test]
fn question_nested_key_not_visible_at_top_level() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"outer":{"inner":1}}')"#);

    let rows = select_rows(&db, "SELECT payload ? 'inner' FROM t");
    assert_eq!(rows[0][0], Value::bool(false));
}

#[test]
fn question_null_json_column_propagates_null() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, data JSON)");
    db.run_ok("INSERT INTO t (id) VALUES (1)");

    let rows = select_rows(&db, "SELECT id FROM t WHERE data ? 'k'");
    assert_eq!(rows.len(), 0, "NULL ? key should not match");
}

// ── Overflow (JSON > 255 bytes) ───────────────────────────────────────────────

/// Build a valid JSON object whose byte length exceeds `TEXT_MAX_INLINE_SIZE`
/// (255). The `"type"` key stays short so extraction tests remain readable.
fn large_json(type_val: &str) -> String {
    // padding pushes total length well past 255
    let padding = "x".repeat(300);
    format!(r#"{{"type":"{type_val}","padding":"{padding}","nested":{{"level":1}}}}"#)
}

#[test]
fn overflow_json_round_trips_full_content() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    let doc = large_json("click");
    assert!(
        doc.len() > 255,
        "test requires doc > 255 bytes to trigger overflow"
    );

    db.run_ok(&format!("INSERT INTO t VALUES (1, '{doc}')"));

    // scan_all resolves the overflow pointer — we should get the full string back
    let rows = db.scan_all("t");
    assert_eq!(rows.len(), 1);
    let stored = as_json(rows[0].get(1).unwrap());
    assert_eq!(
        stored, doc,
        "full JSON must survive the overflow round-trip"
    );
}

#[test]
fn overflow_json_arrow_text_extracts_short_key() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        large_json("hover")
    ));

    // Even though the doc overflows, ->> must resolve the pointer first
    let rows = select_rows(&db, "SELECT payload->>'type' FROM t");
    assert_eq!(rows[0][0].as_str().unwrap(), "hover");
}

#[test]
fn overflow_json_arrow_returns_json_value() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    db.run_ok(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        large_json("scroll")
    ));

    let rows = select_rows(&db, "SELECT payload->'nested' FROM t");
    // -> re-encodes the sub-value as JSON
    let nested = as_json(&rows[0][0]);
    assert!(
        nested.contains("level"),
        "nested object should be extractable from overflow doc"
    );
}

#[test]
fn overflow_json_where_filter_matches() {
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
fn overflow_json_delete_where_filter() {
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

    let result = db.run_ok("DELETE FROM t WHERE payload->>'type' = 'click'");
    let storemy::engine::StatementResult::Deleted { rows, .. } = result else {
        panic!("expected Deleted");
    };
    assert_eq!(rows, 1);
    assert_eq!(db.scan_all("t").len(), 1);
}

#[test]
fn mixed_inline_and_overflow_rows_both_queryable() {
    let db = TestDb::new();
    db.run_ok("CREATE TABLE t (id INT, payload JSON)");
    // inline (< 255 bytes)
    db.run_ok(r#"INSERT INTO t VALUES (1, '{"type":"small"}')"#);
    // overflow (> 255 bytes)
    db.run_ok(&format!(
        "INSERT INTO t VALUES (2, '{}')",
        large_json("big")
    ));

    let rows = select_rows(&db, "SELECT id, payload->>'type' FROM t");
    assert_eq!(rows.len(), 2);

    // collect (id, type) pairs regardless of scan order
    let mut pairs: Vec<(i32, &str)> = rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Fixed(storemy::types::FixedValue::Int32(v)) => *v,
                other => panic!("expected Int32, got {other:?}"),
            };
            let ty = r[1].as_str().unwrap();
            (id, ty)
        })
        .collect();
    pairs.sort_by_key(|(id, _)| *id);

    assert_eq!(pairs[0], (1, "small"));
    assert_eq!(pairs[1], (2, "big"));
}
