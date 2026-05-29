//! Evaluation of PostgreSQL-style JSON operators: `->`, `->>`, `#>`, `#>>`, and `?`.
//!
//! Three operators are covered:
//!
//! | Operator | Meaning |
//! |----------|---------|
//! | `doc -> key`       | Extract a sub-value and return it as a JSON string. |
//! | `doc ->> key`      | Extract a sub-value and return it as plain text (strips quotes from strings). |
//! | `doc #> '{a,b}'`   | Walk a multi-level path and return the result as JSON. |
//! | `doc #>> '{a,b}'`  | Walk a multi-level path and return the result as plain text. |
//! | `doc ? key`        | Return `true` if `key` exists at the top level of the JSON object, or as a string element in a JSON array. |
//!
//! All entry points receive already-evaluated [`Value`]s. NULL short-circuiting
//! is handled by the caller (`eval_binary` in `mod.rs`) before any of these
//! are invoked, so none of the functions here need to deal with `NULL` on either
//! operand.
//!
//! Internally, every function parses the left-hand [`Value`] with `serde_json`
//! on each call — there is no cached parse tree. This keeps the `Value` type
//! simple at the cost of re-parsing on repeated accesses.

use super::ExecutionError;
use crate::{
    Value,
    types::{DynValue, FixedValue},
};

/// Controls whether [`eval_arrow`] returns a JSON value (`->`) or plain text (`->>`).
///
/// The two arrow operators share identical lookup logic; only the return
/// encoding differs, so the mode is factored out here rather than duplicated
/// across two functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JsonArrowMode {
    /// `->` — re-encode the matched sub-value as a JSON string.
    ///
    /// A string field `"hello"` becomes the JSON value `"hello"` (with quotes).
    AsJson,
    /// `->>` — strip JSON quoting and return plain text.
    ///
    /// A string field `"hello"` becomes the plain text `hello` (without quotes).
    /// Non-string scalars (numbers, booleans) are formatted via their `Display` form.
    AsText,
}

/// Evaluates one step of a JSON arrow extraction — `->` or `->>`.
///
/// `l` must hold a JSON document (`Value::Dyn(DynValue::Json(...))`).
/// `r` is one path segment: a text key for object field lookup, or an integer
/// index for zero-based array access.
///
/// Because the parser builds a **left-associative** tree, a chain like
/// `payload->'user'->>'name'` becomes two separate calls: first `->` on
/// `payload` with key `'user'`, then `->>` on the result with key `'name'`.
///
/// ## Return value
///
/// - If the key is missing or the index is out of range, returns `Value::Null`.
/// - [`JsonArrowMode::AsJson`]: re-encodes the matched sub-value as a `Json` [`Value`], so the
///   result can be chained into another `->` call.
/// - [`JsonArrowMode::AsText`]: returns a `Varchar` [`Value`]. Strings lose their surrounding
///   quotes; numbers and booleans use their `Display` form.
///
/// # Errors
///
/// Returns [`ExecutionError::TypeError`] when:
/// - `l` is not a `Json` value.
/// - `r` is not a text or integer value.
/// - The stored JSON string is malformed.
/// - Re-encoding the sub-value fails (only possible if `serde_json` produces output it cannot then
///   round-trip, which should not happen in practice).
///
/// # Examples
///
/// ```ignore
/// let doc = Value::json(r#"{"user": {"name": "alice"}}"#).unwrap();
/// let key = Value::varchar("user".into());
///
/// // -> returns the sub-object as a JSON value
/// let sub = eval_arrow(&doc, &key, JsonArrowMode::AsJson).unwrap();
/// // sub is Value::Json(r#"{"name":"alice"}"#)
///
/// // ->> on a string field strips the quotes
/// let doc2 = Value::json(r#"{"name": "alice"}"#).unwrap();
/// let name = eval_arrow(&doc2, &Value::varchar("name".into()), JsonArrowMode::AsText).unwrap();
/// // name is Value::Varchar("alice")
/// ```
pub(super) fn eval_arrow(
    l: &Value,
    r: &Value,
    mode: JsonArrowMode,
) -> Result<Value, ExecutionError> {
    let key: serde_json::Value = match r {
        Value::Dyn(DynValue::Text(s) | DynValue::Varchar(s)) => {
            serde_json::Value::String(s.clone())
        }
        Value::Fixed(FixedValue::Int32(n)) => serde_json::Value::from(*n),
        Value::Fixed(FixedValue::Int64(n)) => serde_json::Value::from(*n),
        Value::Fixed(FixedValue::Uint32(n)) => serde_json::Value::from(*n),
        Value::Fixed(FixedValue::Uint64(n)) => serde_json::Value::from(*n),
        other => {
            return Err(ExecutionError::TypeError(format!(
                "JSON path key must be text or integer, got {other}"
            )));
        }
    };

    let parsed = parse_json(l)?;

    let found = lookup(&parsed, &key).unwrap_or(serde_json::Value::Null);
    if found.is_null() {
        return Ok(Value::Null);
    }

    match mode {
        JsonArrowMode::AsJson => {
            let s = serde_json::to_string(&found).map_err(|e| {
                ExecutionError::TypeError(format!("failed to encode JSON sub-value: {e}"))
            })?;
            Value::json(&s)
                .map_err(|e| ExecutionError::TypeError(format!("invalid JSON sub-value: {e}")))
        }
        JsonArrowMode::AsText => {
            let text = match found {
                serde_json::Value::String(s) => s,
                serde_json::Value::Null => {
                    unreachable!("null is caught by the early return above")
                }
                other => other.to_string(),
            };
            Ok(Value::varchar(text))
        }
    }
}

/// Evaluates one step of a JSON path extraction — `#>` or `#>>`.
///
/// Unlike [`eval_arrow`] which takes a single key, these operators accept a
/// `PostgreSQL` text-array path on the right-hand side: `payload #> '{user,name}'`.
///
/// ## Path format
///
/// `r` must hold a `Varchar` or `Text` value whose content is a `PostgreSQL`
/// text-array literal — curly-brace delimited, comma-separated:
///
/// ```text
/// '{key1,key2,...}'      →  walk key1, then key2, …
/// '{0,items,name}'       →  integer segments → array index; string segments → object field
/// ```
///
/// Segments that parse as non-negative integers are treated as zero-based array
/// indices. All other segments are treated as object field names.
///
/// ## Return value
///
/// - Intermediate `null` / missing-key / out-of-bounds → `Value::Null`.
/// - [`JsonArrowMode::AsJson`]: re-encodes the final sub-value as a `Json` [`Value`].
/// - [`JsonArrowMode::AsText`]: returns a `Varchar` [`Value`] (strings lose quotes, scalars use
///   `Display`).
///
/// # Errors
///
/// Returns [`ExecutionError::TypeError`] when:
/// - `l` is not a `Json` value.
/// - `r` is not a text value holding a well-formed `{…}` path.
/// - The stored JSON string is malformed.
pub(super) fn eval_path_arrow(
    l: &Value,
    r: &Value,
    mode: JsonArrowMode,
) -> Result<Value, ExecutionError> {
    let raw = match r {
        Value::Dyn(DynValue::Text(s) | DynValue::Varchar(s)) => s.as_str(),
        other => {
            return Err(ExecutionError::TypeError(format!(
                "#> path must be a text array literal like '{{key,subkey}}', got {other}"
            )));
        }
    };
    let segments = parse_path(raw);
    match mode {
        JsonArrowMode::AsJson => eval_path_json(l, &segments),
        JsonArrowMode::AsText => eval_path_text(l, &segments),
    }
}

/// Evaluates `doc #> segments` — walks a pre-parsed path and returns the result as JSON.
///
/// Takes `&[String]` directly so callers that resolved the path at bind time pay no
/// per-row parsing cost.  The dynamic fallback ([`eval_path_arrow`]) parses first,
/// then delegates here.
pub(super) fn eval_path_json(l: &Value, segments: &[String]) -> Result<Value, ExecutionError> {
    if segments.is_empty() {
        return Ok(Value::Null);
    }
    let current = parse_json(l)?;
    let Some(found) = walk_path(current, segments) else {
        return Ok(Value::Null);
    };
    let s = serde_json::to_string(&found)
        .map_err(|e| ExecutionError::TypeError(format!("failed to encode JSON sub-value: {e}")))?;
    Value::json(&s).map_err(|e| ExecutionError::TypeError(format!("invalid JSON sub-value: {e}")))
}

/// Evaluates `doc #>> segments` — walks a pre-parsed path and returns the result as text.
///
/// See [`eval_path_json`] for the pre-parsed design rationale.
pub(super) fn eval_path_text(l: &Value, segments: &[String]) -> Result<Value, ExecutionError> {
    if segments.is_empty() {
        return Ok(Value::Null);
    }
    let current = parse_json(l)?;
    let Some(found) = walk_path(current, segments) else {
        return Ok(Value::Null);
    };
    let text = match found {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    Ok(Value::varchar(text))
}

/// Parses a `PostgreSQL` text-array path literal into path segments.
///
/// Strips optional `{` / `}` delimiters and splits on commas.
/// An empty path (`{}`) returns an empty `Vec`.
pub(super) fn parse_path(raw: &str) -> Vec<String> {
    let inner = raw.trim();
    let inner = inner.strip_prefix('{').unwrap_or(inner);
    let inner = inner.strip_suffix('}').unwrap_or(inner);
    if inner.is_empty() {
        return Vec::new();
    }
    inner.split(',').map(|s| s.trim().to_owned()).collect()
}

/// Walks `segments` through a parsed JSON document.
///
/// Returns `None` if any step is missing or resolves to JSON null.
fn walk_path(mut current: serde_json::Value, segments: &[String]) -> Option<serde_json::Value> {
    for segment in segments {
        let key = match segment.parse::<u64>() {
            Ok(n) => serde_json::Value::from(n),
            Err(_) => serde_json::Value::String(segment.clone()),
        };
        match lookup(&current, &key) {
            None | Some(serde_json::Value::Null) => return None,
            Some(found) => current = found,
        }
    }
    Some(current)
}

/// Evaluates `doc ? key` — checks whether `key` exists in the JSON document `l`.
///
/// The semantics mirror `PostgreSQL`:
/// - When `l` is a JSON **object**, returns `true` if `key` is a top-level field name.
/// - When `l` is a JSON **array**, returns `true` if any element equals the string `key`.
/// - For any other JSON type (number, boolean, string, null), returns `false`.
///
/// `?` is defined over **string keys only**. To test integer membership in a
/// JSON array, use `@>` (containment) instead.
///
/// # Errors
///
/// Returns [`ExecutionError::TypeError`] when:
/// - `l` is not a `Json` value.
/// - `l` holds a malformed JSON string.
/// - `r` is not a text value.
///
/// # Examples
///
/// ```ignore
/// let doc = Value::json(r#"{"type": "click", "x": 1}"#).unwrap();
///
/// // top-level key exists
/// let exists = eval_key_exists(&doc, &Value::varchar("type".into())).unwrap();
/// assert_eq!(exists, Value::bool(true));
///
/// // nested keys are not visible at the top level
/// let nested = Value::json(r#"{"outer": {"inner": 1}}"#).unwrap();
/// let missing = eval_key_exists(&nested, &Value::varchar("inner".into())).unwrap();
/// assert_eq!(missing, Value::bool(false));
/// ```
pub(super) fn eval_key_exists(l: &Value, r: &Value) -> Result<Value, ExecutionError> {
    let key = match r {
        Value::Dyn(DynValue::Text(s) | DynValue::Varchar(s)) => s.as_str(),
        other => {
            return Err(ExecutionError::TypeError(format!(
                "? requires a text key, got {other} — use @> for non-string membership checks"
            )));
        }
    };

    let parsed = parse_json(l)?;
    let exists = match &parsed {
        serde_json::Value::Object(map) => map.contains_key(key),
        serde_json::Value::Array(arr) => arr.iter().any(|v| v.as_str() == Some(key)),
        _ => false,
    };
    Ok(Value::bool(exists))
}

/// Evaluates `l @> r` — returns `true` if `l` contains `r`.
///
/// Both operands must be JSON values. Containment semantics follow `PostgreSQL`:
///
/// - **Object @> Object**: every key/value pair in `r` must exist in `l`, with values checked
///   recursively.
/// - **Array @> Array**: every element of `r` must appear somewhere in `l` (recursive element
///   matching, order-independent).
/// - **Array @> scalar**: the scalar must appear somewhere in the array.
/// - **Scalar @> scalar**: the two scalars must be equal.
///
/// # Errors
///
/// Returns [`ExecutionError::TypeError`] when `l` is not JSON or either
/// operand holds a malformed JSON string.
pub(super) fn eval_contains(l: &Value, r: &Value) -> Result<Value, ExecutionError> {
    let haystack = coerce_to_json_doc(l)?;
    let needle = coerce_to_json_doc(r)?;
    Ok(Value::bool(contains(&haystack, &needle)))
}

/// Recursive containment check used by [`eval_contains`].
///
/// Mirrors `PostgreSQL` `@>` semantics:
///
/// - **Object @> Object**: every key/value pair in `needle` must exist in `haystack` with a
///   recursively contained value.
/// - **Array @> Array**: every element of `needle` must match at least one element of `haystack`
///   (order-independent, recursive).
/// - **Array @> scalar**: the scalar must appear somewhere in the array.
/// - **Scalar @> scalar**: the two scalars must be equal.
fn contains(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Object};
    match (haystack, needle) {
        (Object(h), Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).is_some_and(|hv| contains(hv, nv))),
        (Array(h), Array(n)) => n.iter().all(|nv| h.iter().any(|hv| contains(hv, nv))),
        (Array(h), nv) => h.iter().any(|hv| contains(hv, nv)),
        (h, n) => h == n,
    }
}

/// Parses a `Json` [`Value`] into a `serde_json::Value`.
///
/// Requires `v` to be a `Json` variant — strings, numbers, and other value
/// types are rejected so callers get a clear error when the wrong column type
/// is used on the left-hand side of a JSON operator.
fn parse_json(v: &Value) -> Result<serde_json::Value, ExecutionError> {
    match v {
        Value::Dyn(DynValue::Json(_)) => v
            .to_serde_json()
            .map_err(|e| ExecutionError::TypeError(format!("invalid JSON document: {e}"))),
        other => Err(ExecutionError::TypeError(format!(
            "JSON operator requires a JSON operand, got {other}"
        ))),
    }
}

/// Converts any [`Value`] to a `serde_json::Value`, treating `Varchar` and
/// `Text` as JSON text to be parsed rather than as plain string scalars.
///
/// Used by [`eval_contains`] so that SQL string literals like `'{"type":"click"}'`
/// are implicitly coerced to JSON documents — matching `PostgreSQL`'s behaviour
/// where `@>` accepts a `text` literal on either side.
fn coerce_to_json_doc(v: &Value) -> Result<serde_json::Value, ExecutionError> {
    match v {
        Value::Dyn(DynValue::Varchar(s) | DynValue::Text(s)) => serde_json::from_str(s.as_str())
            .map_err(|e| ExecutionError::TypeError(format!("@> operand is not valid JSON: {e}"))),
        _ => v
            .to_serde_json()
            .map_err(|e| ExecutionError::TypeError(format!("invalid JSON in @> operand: {e}"))),
    }
}

/// Returns the value at `key` inside `doc`, or `None` if not found.
///
/// String keys do object field lookup; integer keys do zero-based array indexing.
/// Any other key type (which `serde_json` can represent but our callers never
/// produce) returns `None`.
fn lookup(doc: &serde_json::Value, key: &serde_json::Value) -> Option<serde_json::Value> {
    match key {
        serde_json::Value::String(s) => doc.get(s).cloned(),
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|i| doc.get(usize::try_from(i).ok()?))
            .cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    fn json(s: &str) -> Value {
        Value::json(s).expect("valid JSON")
    }

    fn key_exists(doc: &str, key: &str) -> Value {
        eval_key_exists(&json(doc), &Value::varchar(key.into())).expect("eval failed")
    }

    #[test]
    fn existing_top_level_key_returns_true() {
        assert_eq!(
            key_exists(r#"{"type":"click","x":1}"#, "type"),
            Value::bool(true)
        );
    }

    #[test]
    fn missing_key_returns_false() {
        assert_eq!(
            key_exists(r#"{"type":"click"}"#, "missing"),
            Value::bool(false)
        );
    }

    #[test]
    fn nested_key_is_not_visible_at_top_level() {
        // `?` only checks top-level keys; "inner" lives inside "outer"
        assert_eq!(
            key_exists(r#"{"outer":{"inner":1}}"#, "inner"),
            Value::bool(false)
        );
        assert_eq!(
            key_exists(r#"{"outer":{"inner":1}}"#, "outer"),
            Value::bool(true)
        );
    }

    #[test]
    fn empty_object_returns_false() {
        assert_eq!(key_exists(r"{}", "anything"), Value::bool(false));
    }

    // ── array element lookup ──────────────────────────────────────────────────

    #[test]
    fn existing_string_element_in_array_returns_true() {
        assert_eq!(key_exists(r#"["a","b","c"]"#, "b"), Value::bool(true));
    }

    #[test]
    fn missing_string_element_in_array_returns_false() {
        assert_eq!(key_exists(r#"["a","b"]"#, "z"), Value::bool(false));
    }

    #[test]
    fn empty_array_returns_false() {
        assert_eq!(key_exists(r"[]", "a"), Value::bool(false));
    }

    #[test]
    fn array_of_non_strings_returns_false() {
        // integer elements can never match a string key
        assert_eq!(key_exists(r"[1,2,3]", "1"), Value::bool(false));
    }

    #[test]
    fn non_string_rhs_errors() {
        // ? is string-only — passing an integer is a type error, not a NULL result.
        // For integer array membership use @> instead.
        let err = eval_key_exists(&json(r#"{"a":1}"#), &Value::int64(1)).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn non_json_lhs_errors() {
        let err = eval_key_exists(
            &Value::varchar("not json".into()),
            &Value::varchar("k".into()),
        )
        .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    fn contains_check(l: &str, r: &str) -> bool {
        match eval_contains(&json(l), &json(r)).expect("eval failed") {
            Value::Fixed(crate::types::FixedValue::Bool(b)) => b,
            other => panic!("expected bool, got {other:?}"),
        }
    }

    #[test]
    fn object_contains_subset_of_keys() {
        assert!(contains_check(r#"{"a":1,"b":2,"c":3}"#, r#"{"a":1}"#));
        assert!(contains_check(r#"{"a":1,"b":2}"#, r#"{"a":1,"b":2}"#));
    }

    #[test]
    fn object_missing_key_not_contained() {
        assert!(!contains_check(r#"{"a":1}"#, r#"{"a":1,"b":2}"#));
    }

    #[test]
    fn object_wrong_value_not_contained() {
        assert!(!contains_check(r#"{"a":1}"#, r#"{"a":2}"#));
    }

    #[test]
    fn object_contains_nested_subset() {
        assert!(contains_check(
            r#"{"user":{"name":"alice","age":30}}"#,
            r#"{"user":{"name":"alice"}}"#
        ));
        assert!(!contains_check(
            r#"{"user":{"name":"alice"}}"#,
            r#"{"user":{"name":"alice","age":30}}"#
        ));
    }

    #[test]
    fn array_contains_subset_of_elements() {
        assert!(contains_check(r"[1,2,3,4]", r"[2,4]"));
        assert!(!contains_check(r"[1,2,3]", r"[2,5]"));
    }

    #[test]
    fn array_contains_scalar() {
        assert!(contains_check(r"[1,2,3]", "2"));
        assert!(!contains_check(r"[1,2,3]", "5"));
    }

    #[test]
    fn scalar_contains_equal_scalar() {
        assert!(contains_check("42", "42"));
        assert!(!contains_check("42", "43"));
        assert!(contains_check(r#""hello""#, r#""hello""#));
        assert!(!contains_check(r#""hello""#, r#""world""#));
    }

    #[test]
    fn non_json_lhs_errors_for_contains() {
        let err = eval_contains(&Value::varchar("not json".into()), &json("1")).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    // ── #> / #>> (path extraction) ────────────────────────────────────────────

    fn path_json(doc: &str, path: &str) -> Value {
        eval_path_arrow(
            &json(doc),
            &Value::varchar(path.into()),
            JsonArrowMode::AsJson,
        )
        .expect("eval failed")
    }

    fn path_text(doc: &str, path: &str) -> Value {
        eval_path_arrow(
            &json(doc),
            &Value::varchar(path.into()),
            JsonArrowMode::AsText,
        )
        .expect("eval failed")
    }

    #[test]
    fn path_arrow_single_key_as_json() {
        assert_eq!(
            path_json(r#"{"user":{"name":"alice"}}"#, "{user}"),
            json(r#"{"name":"alice"}"#)
        );
    }

    #[test]
    fn path_arrow_two_levels_as_json() {
        assert_eq!(
            path_json(r#"{"user":{"name":"alice"}}"#, "{user,name}"),
            json(r#""alice""#)
        );
    }

    #[test]
    fn path_arrow_text_strips_string_quotes() {
        assert_eq!(
            path_text(r#"{"user":{"name":"alice"}}"#, "{user,name}"),
            Value::varchar("alice".into())
        );
    }

    #[test]
    fn path_arrow_integer_index_into_array() {
        assert_eq!(
            path_json(r#"{"items":["a","b","c"]}"#, "{items,1}"),
            json(r#""b""#)
        );
    }

    #[test]
    fn path_arrow_missing_intermediate_key_returns_null() {
        assert_eq!(path_json(r#"{"a":1}"#, "{missing,nested}"), Value::Null);
    }

    #[test]
    fn path_arrow_missing_leaf_returns_null() {
        assert_eq!(
            path_json(r#"{"user":{"name":"alice"}}"#, "{user,age}"),
            Value::Null
        );
    }

    #[test]
    fn path_arrow_empty_path_returns_null() {
        assert_eq!(path_json(r#"{"a":1}"#, "{}"), Value::Null);
    }

    #[test]
    fn path_arrow_non_text_rhs_errors() {
        let err = eval_path_arrow(&json(r#"{"a":1}"#), &Value::int64(1), JsonArrowMode::AsJson)
            .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn path_arrow_three_levels_deep() {
        assert_eq!(
            path_text(r#"{"a":{"b":{"c":"deep"}}}"#, "{a,b,c}"),
            Value::varchar("deep".into())
        );
    }

    #[test]
    fn parse_path_strips_braces() {
        assert_eq!(parse_path("{user,name}"), vec!["user", "name"]);
    }

    #[test]
    fn parse_path_bare_no_braces() {
        assert_eq!(parse_path("a,b,c"), vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_path_single_segment() {
        assert_eq!(parse_path("{user}"), vec!["user"]);
    }

    #[test]
    fn parse_path_empty_returns_empty() {
        assert!(parse_path("{}").is_empty());
        assert!(parse_path("").is_empty());
    }

    #[test]
    fn parse_path_trims_whitespace_around_commas() {
        assert_eq!(parse_path("{ a , b , c }"), vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_path_integer_segment_preserved_as_string() {
        // parse_path just returns strings; the int→index conversion happens in walk_path
        assert_eq!(parse_path("{items,0}"), vec!["items", "0"]);
    }

    #[test]
    fn eval_path_json_single_key() {
        let segs = vec!["user".to_owned()];
        assert_eq!(
            eval_path_json(&json(r#"{"user":{"name":"alice"}}"#), &segs).unwrap(),
            json(r#"{"name":"alice"}"#)
        );
    }

    #[test]
    fn eval_path_json_two_levels() {
        let segs = vec!["user".to_owned(), "name".to_owned()];
        assert_eq!(
            eval_path_json(&json(r#"{"user":{"name":"alice"}}"#), &segs).unwrap(),
            json(r#""alice""#)
        );
    }

    #[test]
    fn eval_path_json_integer_segment_indexes_array() {
        let segs = vec!["items".to_owned(), "1".to_owned()];
        assert_eq!(
            eval_path_json(&json(r#"{"items":["a","b","c"]}"#), &segs).unwrap(),
            json(r#""b""#)
        );
    }

    #[test]
    fn eval_path_json_missing_key_returns_null() {
        let segs = vec!["missing".to_owned()];
        assert_eq!(
            eval_path_json(&json(r#"{"a":1}"#), &segs).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn eval_path_json_missing_intermediate_returns_null() {
        let segs = vec!["a".to_owned(), "b".to_owned()];
        assert_eq!(
            eval_path_json(&json(r#"{"x":1}"#), &segs).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn eval_path_json_empty_segments_returns_null() {
        assert_eq!(
            eval_path_json(&json(r#"{"a":1}"#), &[]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn eval_path_json_non_json_lhs_errors() {
        let segs = vec!["k".to_owned()];
        let err = eval_path_json(&Value::varchar("not json".into()), &segs).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    // ── eval_path_text (pre-parsed segments) ─────────────────────────────────

    #[test]
    fn eval_path_text_string_strips_quotes() {
        let segs = vec!["user".to_owned(), "name".to_owned()];
        assert_eq!(
            eval_path_text(&json(r#"{"user":{"name":"alice"}}"#), &segs).unwrap(),
            Value::varchar("alice".into())
        );
    }

    #[test]
    fn eval_path_text_integer_rendered_as_digits() {
        let segs = vec!["meta".to_owned(), "count".to_owned()];
        assert_eq!(
            eval_path_text(&json(r#"{"meta":{"count":42}}"#), &segs).unwrap(),
            Value::varchar("42".into())
        );
    }

    #[test]
    fn eval_path_text_bool_rendered_as_text() {
        let segs = vec!["flags".to_owned(), "active".to_owned()];
        assert_eq!(
            eval_path_text(&json(r#"{"flags":{"active":true}}"#), &segs).unwrap(),
            Value::varchar("true".into())
        );
    }

    #[test]
    fn eval_path_text_missing_returns_null() {
        let segs = vec!["a".to_owned(), "b".to_owned()];
        assert_eq!(
            eval_path_text(&json(r#"{"a":1}"#), &segs).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn eval_path_text_empty_segments_returns_null() {
        assert_eq!(
            eval_path_text(&json(r#"{"a":1}"#), &[]).unwrap(),
            Value::Null
        );
    }
}
