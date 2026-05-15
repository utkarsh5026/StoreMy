//! Scalar expression evaluator — maps a parsed [`Expr`] over a single tuple row.
//!
//! The entry point is [`eval_expr`], which walks an [`Expr`] tree and returns
//! the resulting [`Value`]. It is the counterpart to
//! [`crate::execution::expression::BooleanExpression::eval`]: where that function returns `bool`
//! and is purpose-built for filter predicates, this one returns a full [`Value`]
//! and is suitable anywhere a scalar result is needed — `SET` assignment
//! right-hand sides, `CHECK` constraint bodies, computed default values, and so on.
//!
//! # What it handles
//!
//! - **Literals** — constants already baked into the AST; returned as-is.
//! - **Column references** — resolved by name through the schema, then fetched from the tuple by
//!   physical index.
//! - **Binary operators** — comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`) and logical connectives
//!   (`AND`, `OR`) evaluated over recursively evaluated operands.
//! - **Unary `NOT`** — logical negation of a `Bool` operand.
//! - **`IS NULL` / `IS NOT NULL`** — always yields a definite [`Value::Bool`]; never propagates
//!   `NULL` from the tested expression.
//! - **`IN` / `NOT IN`** — membership over a parenthesized list; uses SQL three-valued logic (a
//!   NULL needle or NULL in the list can yield [`Value::Null`]).
//!
//! # What it does NOT handle
//!
//! Aggregate expressions (`COUNT(*)`, `SUM(col)`, etc.) require many rows and
//! cannot be reduced to a single value from one tuple. Calling `eval_expr` on
//! an `Agg` or `CountStar` node returns [`ExecutionError::TypeError`].
//!
//! # NULL semantics
//!
//! `NULL` propagates through comparisons: if either operand is `NULL`, the
//! comparison returns `Value::Null` rather than `Value::Bool(false)`. This
//! matches SQL three-valued logic and lets callers decide what a null result
//! means in context (e.g. a `CHECK` constraint passes on `NULL`; a `WHERE`
//! filter treats it as non-matching). Logical `AND` and `OR` propagate `NULL`
//! too, matching the SQL standard.
//!
//! This is intentionally different from [`crate::execution::expression::BooleanExpression::eval`],
//! which collapses `NULL` to `false` for simplicity in the filter/join operators.

use std::cmp::Ordering;

use super::ExecutionError;
use crate::{
    Value,
    parser::statements::{BinOp, CaseBranch, Expr, UnOp},
    tuple::{Tuple, TupleSchema},
};

/// Evaluates a scalar [`Expr`] against a single tuple row.
///
/// Walks the expression tree recursively. Column references are resolved by
/// name through `schema` and fetched from `tuple` by physical index. All
/// other forms are evaluated in-place.
///
/// # Errors
///
/// - [`ExecutionError::TypeError`] if a column name is not found in `schema`.
/// - [`ExecutionError::TypeError`] if a column index is out of bounds in `tuple`.
/// - [`ExecutionError::TypeError`] if an aggregate expression (`Agg`, `CountStar`) is encountered —
///   those require more than one row.
/// - [`ExecutionError::TypeError`] if a logical or `NOT` operand is not `Bool`.
pub fn eval_expr(
    expr: &Expr,
    tuple: &Tuple,
    schema: &TupleSchema,
) -> Result<Value, ExecutionError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column(col_ref) => {
            // `field_by_name` looks up the column in the schema by its string name
            // and returns `(ColumnId, &Field)`. `ColumnId` is a new type over `u32`,
            // so `usize::from(col_id)` gives us the physical slot index in the tuple.
            let (col_id, _field) =
                schema.field_by_name(col_ref.name.as_str()).ok_or_else(|| {
                    ExecutionError::TypeError(format!("unknown column '{}'", col_ref.name))
                })?;
            let idx = usize::from(col_id);
            tuple.get(idx).cloned().ok_or_else(|| {
                ExecutionError::TypeError(format!("column index {idx} out of bounds"))
            })
        }

        Expr::BinaryOp { lhs, op, rhs } => {
            let l = eval_expr(lhs, tuple, schema)?;
            let r = eval_expr(rhs, tuple, schema)?;
            eval_binary(*op, &l, &r)
        }

        Expr::UnaryOp { op, operand } => {
            let v = eval_expr(operand, tuple, schema)?;
            eval_unary(*op, &v)
        }

        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, tuple, schema)?;
            Ok(Value::Bool(if *negated {
                !v.is_null()
            } else {
                v.is_null()
            }))
        }

        Expr::In {
            expr,
            list,
            negated,
        } => eval_in(expr, tuple, schema, list, *negated),

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => eval_between(expr, tuple, schema, low, high, *negated),

        Expr::Case {
            operand,
            branches,
            else_result,
        } => eval_case(
            operand.as_deref(),
            tuple,
            schema,
            branches,
            else_result.as_deref(),
        ),

        Expr::Like {
            expr,
            pattern,
            negated,
        } => eval_like(expr, tuple, schema, pattern, *negated),

        // Aggregates operate over many rows and cannot produce a single value
        // from one tuple. The planner should never route them here.
        Expr::Agg { .. } | Expr::CountStar => Err(ExecutionError::TypeError(
            "aggregate expressions cannot be evaluated as scalar expressions".to_string(),
        )),
    }
}

/// Applies a binary operator to two already-evaluated [`Value`]s.
///
/// **NULL propagation**: any comparison with a `NULL` operand returns
/// `Value::Null`. Logical `AND`/`OR` do the same for now (no short-circuit
/// three-valued logic — an `AND` with one `NULL` side returns `NULL` regardless
/// of the other side).
///
/// Returns [`ExecutionError::TypeError`] for `AND`/`OR` when a non-null
/// operand is not `Bool`.
fn eval_binary(op: BinOp, l: &Value, r: &Value) -> Result<Value, ExecutionError> {
    // NULL propagates through all operators.
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }

    match op {
        BinOp::And => {
            // Both operands must be Bool; NULL is already handled above.
            let lb = as_bool(l, "AND")?;
            let rb = as_bool(r, "AND")?;
            Ok(Value::Bool(lb && rb))
        }
        BinOp::Or => {
            let lb = as_bool(l, "OR")?;
            let rb = as_bool(r, "OR")?;
            Ok(Value::Bool(lb || rb))
        }
        // Comparison operators: use PartialOrd / PartialEq, return Bool.
        BinOp::Eq => Ok(Value::Bool(l == r)),
        BinOp::NotEq => Ok(Value::Bool(l != r)),
        BinOp::Lt => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_lt)),
        BinOp::LtEq => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_le)),
        BinOp::Gt => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_gt)),
        BinOp::GtEq => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_ge)),
    }
}

/// Applies a unary operator to an already-evaluated [`Value`].
fn eval_unary(op: UnOp, v: &Value) -> Result<Value, ExecutionError> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnOp::Not => {
            let b = as_bool(v, "NOT")?;
            Ok(Value::Bool(!b))
        }
    }
}

/// Evaluates `expr [NOT] IN (list…)` for a single row.
///
/// SQL treats `expr IN (v1, v2, …)` as `expr = v1 OR expr = v2 OR …` with the same
/// `NULL` rules as `=`. Called from [`Expr::In`] in [`eval_expr`].
///
/// # NULL semantics
///
/// Unlike [`Expr::IsNull`], `IN` can return [`Value::Null`] (unknown):
///
/// - **NULL needle** (`NULL IN (1, 2)`) → `Value::Null`
/// - **Definite match** on a non-null list element → `Value::Bool(!negated)` (`IN` → true, `NOT IN`
///   → false)
/// - **No match, but a NULL in the list** (`2 IN (1, NULL, 3)`) → `Value::Null` — membership cannot
///   be proved or disproved
/// - **No match, no NULL in the list** → `Value::Bool(negated)` (`IN` → false, `NOT IN` → true)
fn eval_in(
    expr: &Expr,
    tuple: &Tuple,
    schema: &TupleSchema,
    list: &[Expr],
    negated: bool,
) -> Result<Value, ExecutionError> {
    let v = eval_expr(expr, tuple, schema)?;
    let values = list
        .iter()
        .map(|e| eval_expr(e, tuple, schema))
        .collect::<Result<Vec<Value>, ExecutionError>>()?;

    if v.is_null() {
        return Ok(Value::Null);
    }

    // We need to check for NULLs in the list, because NULL in the list makes the result NULL.
    // otherwise we would return a definite value.
    let mut saw_null = false;
    for value in values {
        if value.is_null() {
            saw_null = true;
            continue;
        }
        if value == v {
            return Ok(Value::Bool(!negated));
        }
    }

    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(negated))
    }
}

/// Evaluates `expr [NOT] BETWEEN low AND high` for a single row.
///
/// Semantically equivalent to `(low <= expr) AND (expr <= high)` with inclusive
/// bounds on both ends. `NOT BETWEEN` negates the boolean result. Called from
/// [`Expr::Between`] in [`eval_expr`].
///
/// # NULL and incomparable values
///
/// If `expr`, `low`, or `high` evaluates to [`Value::Null`], the result is
/// [`Value::Null`] (unknown), not `false`. If any pair is incomparable under
/// [`PartialOrd`] (cross-type, e.g. `Int64` vs `String`), the result is also
/// [`Value::Null`], consistent with comparison operators via [`cmp_to_bool`].
fn eval_between(
    expr: &Expr,
    tuple: &Tuple,
    schema: &TupleSchema,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<Value, ExecutionError> {
    let v = eval_expr(expr, tuple, schema)?;
    let lo = eval_expr(low, tuple, schema)?;
    let hi = eval_expr(high, tuple, schema)?;

    if v.is_null() || lo.is_null() || hi.is_null() {
        return Ok(Value::Null);
    }

    match (lo.partial_cmp(&v), v.partial_cmp(&hi)) {
        (Some(lo_ord), Some(hi_ord)) => {
            let in_range = lo_ord.is_le() && hi_ord.is_le();
            Ok(Value::Bool(if negated { !in_range } else { in_range }))
        }
        _ => Ok(Value::Null),
    }
}

/// Evaluates `expr [NOT] LIKE pattern` for a single row.
///
/// The pattern follows SQL wildcard rules:
/// - `%` matches any sequence of characters, including the empty string.
/// - `_` matches exactly one character.
/// - All other characters match themselves literally.
///
/// # NULL semantics
///
/// If either `expr` or `pattern` evaluates to [`Value::Null`], the result is
/// [`Value::Null`] — LIKE propagates NULL just like a comparison operator.
fn eval_like(
    expr: &Expr,
    tuple: &Tuple,
    schema: &TupleSchema,
    pattern: &Expr,
    negated: bool,
) -> Result<Value, ExecutionError> {
    let text = eval_expr(expr, tuple, schema)?;
    let pat = eval_expr(pattern, tuple, schema)?;

    if text.is_null() || pat.is_null() {
        return Ok(Value::Null);
    }

    let Value::String(text_str) = text else {
        return Err(ExecutionError::TypeError(
            "LIKE requires a String left-hand side".to_string(),
        ));
    };
    let Value::String(pat_str) = pat else {
        return Err(ExecutionError::TypeError(
            "LIKE requires a String pattern".to_string(),
        ));
    };

    let matched = like_matches(&text_str, &pat_str);
    Ok(Value::Bool(if negated { !matched } else { matched }))
}

/// Returns `true` when `text` matches the SQL LIKE `pattern`.
///
/// Implemented with recursive backtracking over the pattern characters.
/// `%` in the pattern tries every possible split of the remaining text;
/// `_` consumes exactly one character; any other character must match exactly.
fn like_matches(text: &str, pattern: &str) -> bool {
    let mut p_chars = pattern.chars();
    let Some(p_head) = p_chars.next() else {
        return text.is_empty(); // pattern exhausted: must also exhaust text
    };
    let p_tail = p_chars.as_str();

    match p_head {
        '%' => {
            // '%' can match zero characters (skip it) or consume one character
            // of text and stay at '%' (greedy attempt via tail-call skip).
            // Try matching zero chars consumed by '%':
            if like_matches(text, p_tail) {
                return true;
            }
            // Try consuming one character of text and keeping '%' in pattern:
            let mut t_chars = text.chars();
            while t_chars.next().is_some() {
                if like_matches(t_chars.as_str(), p_tail) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // '_' must consume exactly one character.
            let mut t_chars = text.chars();
            match t_chars.next() {
                None => false,
                Some(_) => like_matches(t_chars.as_str(), p_tail),
            }
        }
        literal => {
            // Literal character: text must start with this character.
            let mut t_chars = text.chars();
            match t_chars.next() {
                Some(tc) if tc == literal => like_matches(t_chars.as_str(), p_tail),
                _ => false,
            }
        }
    }
}

/// Evaluates `CASE … END` for a single row.
///
/// Branches are tried in source order; the first match wins and its `THEN`
/// expression is evaluated and returned. Called from [`Expr::Case`] in
/// [`eval_expr`].
///
/// # Searched vs simple form
///
/// - **Searched** (`operand` is `None`): `CASE WHEN cond1 THEN … WHEN cond2 THEN …`. Each `when`
///   must evaluate to [`Value::Bool(true)`]; `false`, [`Value::Null`], and non-boolean values do
///   not match.
/// - **Simple** (`operand` is `Some(expr)`): `CASE expr WHEN val1 THEN …`. Each `when` is compared
///   to the evaluated operand with `==`. A branch matches only when both sides are non-null and
///   equal; a [`Value::Null`] operand never matches (including `WHEN NULL`), and a null `when`
///   never matches a non-null operand.
///
/// # No match
///
/// If no branch matches, evaluates `else_result` when present; otherwise returns
/// [`Value::Null`].
fn eval_case(
    operand: Option<&Expr>,
    tuple: &Tuple,
    schema: &TupleSchema,
    branches: &[CaseBranch],
    else_result: Option<&Expr>,
) -> Result<Value, ExecutionError> {
    let base = operand.map(|e| eval_expr(e, tuple, schema)).transpose()?;

    for CaseBranch { when, then } in branches {
        let condition_holds = match &base {
            None => {
                let v = eval_expr(when, tuple, schema)?;
                matches!(v, Value::Bool(true))
            }
            Some(base_val) => {
                let v = eval_expr(when, tuple, schema)?;
                !base_val.is_null() && !v.is_null() && *base_val == v
            }
        };

        if condition_holds {
            return eval_expr(then, tuple, schema);
        }
    }

    match else_result {
        Some(e) => eval_expr(e, tuple, schema),
        None => Ok(Value::Null),
    }
}

/// Converts an `Option<Ordering>` from `PartialOrd::partial_cmp` to a
/// `Value::Bool` using a predicate on the ordering, or `Value::Null` when
/// the values are incomparable (e.g. `NaN`, or cross-type comparison).
fn cmp_to_bool(ord: Option<Ordering>, pred: fn(Ordering) -> bool) -> Value {
    match ord {
        Some(o) => Value::Bool(pred(o)),
        // Incomparable (NaN or mismatched types) maps to NULL — no error.
        None => Value::Null,
    }
}

/// Extracts the inner `bool` from a `Value::Bool`, returning a
/// `TypeError` that names the operator context (`op_name`) on failure.
fn as_bool(v: &Value, op_name: &str) -> Result<bool, ExecutionError> {
    v.as_bool().ok_or_else(|| {
        ExecutionError::TypeError(format!("{op_name} requires Bool operands, got {v}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Type, Value,
        parser::statements::{BinOp, CaseBranch, Expr, UnOp},
        primitives::NonEmptyString,
        tuple::{Field, Tuple, TupleSchema},
    };

    fn schema(fields: &[(&str, Type)]) -> TupleSchema {
        TupleSchema::new(
            fields
                .iter()
                .map(|(name, ty)| Field::new_non_empty(NonEmptyString::new(*name).unwrap(), *ty))
                .collect(),
        )
    }

    fn tuple(values: Vec<Value>) -> Tuple {
        Tuple::new(values)
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.into())
    }

    fn lit(v: Value) -> Expr {
        Expr::Literal(v)
    }

    fn binop(lhs: Expr, op: BinOp, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    fn is_null(inner: Expr, negated: bool) -> Expr {
        Expr::IsNull {
            expr: Box::new(inner),
            negated,
        }
    }

    fn in_list(inner: Expr, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::In {
            expr: Box::new(inner),
            list,
            negated,
        }
    }

    fn case(operand: Option<Expr>, branches: Vec<(Expr, Expr)>, else_result: Option<Expr>) -> Expr {
        Expr::Case {
            operand: operand.map(Box::new),
            branches: branches
                .into_iter()
                .map(|(when, then)| CaseBranch { when, then })
                .collect(),
            else_result: else_result.map(Box::new),
        }
    }

    fn between(inner: Expr, low: Expr, high: Expr, negated: bool) -> Expr {
        Expr::Between {
            expr: Box::new(inner),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        }
    }

    fn eval(expr: &Expr, t: &Tuple, s: &TupleSchema) -> Value {
        eval_expr(expr, t, s).expect("eval failed")
    }

    #[test]
    fn literal_returns_its_value() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(0)]);
        assert_eq!(eval(&lit(Value::Int64(42)), &t, &s), Value::Int64(42));
        assert_eq!(eval(&lit(Value::Null), &t, &s), Value::Null);
        assert_eq!(
            eval(&lit(Value::String("hi".into())), &t, &s),
            Value::String("hi".into())
        );
    }

    #[test]
    fn column_resolves_by_name() {
        let s = schema(&[("id", Type::Int64), ("name", Type::String)]);
        let t = tuple(vec![Value::Int64(7), Value::String("alice".into())]);
        assert_eq!(eval(&col("id"), &t, &s), Value::Int64(7));
        assert_eq!(eval(&col("name"), &t, &s), Value::String("alice".into()));
    }

    #[test]
    fn column_unknown_name_errors() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let err = eval_expr(&col("nope"), &t, &s).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn eq_returns_bool() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        assert_eq!(
            eval(&binop(col("x"), BinOp::Eq, lit(Value::Int64(5))), &t, &s),
            Value::Bool(true)
        );
        assert_eq!(
            eval(&binop(col("x"), BinOp::Eq, lit(Value::Int64(9))), &t, &s),
            Value::Bool(false)
        );
    }

    #[test]
    fn lt_gt_comparisons() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(25)]);
        assert_eq!(
            eval(&binop(col("age"), BinOp::Lt, lit(Value::Int64(30))), &t, &s),
            Value::Bool(true)
        );
        assert_eq!(
            eval(&binop(col("age"), BinOp::Gt, lit(Value::Int64(30))), &t, &s),
            Value::Bool(false)
        );
        assert_eq!(
            eval(
                &binop(col("age"), BinOp::GtEq, lit(Value::Int64(25))),
                &t,
                &s
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn null_comparison_yields_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        assert_eq!(
            eval(&binop(col("x"), BinOp::Eq, lit(Value::Int64(1))), &t, &s),
            Value::Null
        );
    }

    #[test]
    fn and_or_over_bools() {
        let s = schema(&[("a", Type::Bool), ("b", Type::Bool)]);
        let t = tuple(vec![Value::Bool(true), Value::Bool(false)]);
        assert_eq!(
            eval(&binop(col("a"), BinOp::And, col("b")), &t, &s),
            Value::Bool(false)
        );
        assert_eq!(
            eval(&binop(col("a"), BinOp::Or, col("b")), &t, &s),
            Value::Bool(true)
        );
    }

    #[test]
    fn and_with_null_yields_null() {
        let s = schema(&[("a", Type::Bool)]);
        let t = tuple(vec![Value::Bool(true)]);
        let expr = binop(col("a"), BinOp::And, lit(Value::Null));
        assert_eq!(eval(&expr, &t, &s), Value::Null);
    }

    // ── unary NOT ────────────────────────────────────────────────────────────

    #[test]
    fn not_flips_bool() {
        let s = schema(&[("active", Type::Bool)]);
        let t = tuple(vec![Value::Bool(true)]);
        let expr = Expr::UnaryOp {
            op: UnOp::Not,
            operand: Box::new(col("active")),
        };
        assert_eq!(eval(&expr, &t, &s), Value::Bool(false));
    }

    #[test]
    fn not_null_yields_null() {
        let s = schema(&[("active", Type::Bool)]);
        let t = tuple(vec![Value::Null]);
        let expr = Expr::UnaryOp {
            op: UnOp::Not,
            operand: Box::new(col("active")),
        };
        assert_eq!(eval(&expr, &t, &s), Value::Null);
    }

    #[test]
    fn not_non_bool_errors() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let expr = Expr::UnaryOp {
            op: UnOp::Not,
            operand: Box::new(col("x")),
        };
        assert!(matches!(
            eval_expr(&expr, &t, &s),
            Err(ExecutionError::TypeError(_))
        ));
    }

    #[test]
    fn aggregate_expr_returns_error() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let err = eval_expr(&Expr::CountStar, &t, &s).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn is_null_on_null_column() {
        let s = schema(&[("email", Type::String)]);
        let t = tuple(vec![Value::Null]);
        assert_eq!(
            eval(&is_null(col("email"), false), &t, &s),
            Value::Bool(true)
        );
    }

    #[test]
    fn is_not_null_on_null_column() {
        let s = schema(&[("email", Type::String)]);
        let t = tuple(vec![Value::Null]);
        assert_eq!(
            eval(&is_null(col("email"), true), &t, &s),
            Value::Bool(false)
        );
    }

    #[test]
    fn is_null_on_non_null_column() {
        let s = schema(&[("email", Type::String)]);
        let t = tuple(vec![Value::String("a@b.c".into())]);
        assert_eq!(
            eval(&is_null(col("email"), false), &t, &s),
            Value::Bool(false)
        );
    }

    #[test]
    fn is_not_null_on_non_null_column() {
        let s = schema(&[("email", Type::String)]);
        let t = tuple(vec![Value::String("a@b.c".into())]);
        assert_eq!(
            eval(&is_null(col("email"), true), &t, &s),
            Value::Bool(true)
        );
    }

    #[test]
    fn literal_null_is_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(0)]);
        assert_eq!(
            eval(&is_null(lit(Value::Null), false), &t, &s),
            Value::Bool(true)
        );
    }

    #[test]
    fn is_null_differs_from_eq_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        assert_eq!(
            eval(&binop(col("x"), BinOp::Eq, lit(Value::Null)), &t, &s),
            Value::Null
        );
        assert_eq!(eval(&is_null(col("x"), false), &t, &s), Value::Bool(true));
    }

    #[test]
    fn in_match() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(2)]);
        let expr = in_list(
            col("id"),
            vec![
                lit(Value::Int64(1)),
                lit(Value::Int64(2)),
                lit(Value::Int64(3)),
            ],
            false,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Bool(true));
    }

    #[test]
    fn in_no_match() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        let expr = in_list(
            col("id"),
            vec![
                lit(Value::Int64(1)),
                lit(Value::Int64(2)),
                lit(Value::Int64(3)),
            ],
            false,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Bool(false));
    }

    #[test]
    fn not_in_match() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let expr = in_list(
            col("id"),
            vec![lit(Value::Int64(1)), lit(Value::Int64(2))],
            true,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Bool(false));
    }

    #[test]
    fn not_in_no_match() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        let expr = in_list(
            col("id"),
            vec![lit(Value::Int64(1)), lit(Value::Int64(2))],
            true,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Bool(true));
    }

    #[test]
    fn in_null_needle() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        let expr = in_list(
            col("id"),
            vec![lit(Value::Int64(1)), lit(Value::Int64(2))],
            false,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Null);
    }

    #[test]
    fn in_null_in_list() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(2)]);
        let expr = in_list(
            col("id"),
            vec![lit(Value::Int64(1)), lit(Value::Null), lit(Value::Int64(3))],
            false,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Null);
    }

    #[test]
    fn not_in_null_in_list() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(2)]);
        let expr = in_list(
            col("id"),
            vec![lit(Value::Int64(1)), lit(Value::Null), lit(Value::Int64(3))],
            true,
        );
        assert_eq!(eval(&expr, &t, &s), Value::Null);
    }

    #[test]
    fn in_single_element() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(42)]);
        let expr = in_list(col("id"), vec![lit(Value::Int64(42))], false);
        assert_eq!(eval(&expr, &t, &s), Value::Bool(true));
    }

    #[test]
    fn in_empty_list() {
        let s = schema(&[("id", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        assert_eq!(
            eval(&in_list(col("id"), vec![], false), &t, &s),
            Value::Bool(false)
        );
        assert_eq!(
            eval(&in_list(col("id"), vec![], true), &t, &s),
            Value::Bool(true)
        );
    }

    #[test]
    fn nested_and_of_two_comparisons() {
        // (age > 18) AND (active = true)
        let s = schema(&[("age", Type::Int64), ("active", Type::Bool)]);
        let t = tuple(vec![Value::Int64(25), Value::Bool(true)]);
        let expr = binop(
            binop(col("age"), BinOp::Gt, lit(Value::Int64(18))),
            BinOp::And,
            binop(col("active"), BinOp::Eq, lit(Value::Bool(true))),
        );
        assert_eq!(eval(&expr, &t, &s), Value::Bool(true));
    }

    #[test]
    fn between_value_in_range() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(25)]);
        let e = between(
            col("age"),
            lit(Value::Int64(18)),
            lit(Value::Int64(65)),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn between_value_below_range() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(10)]);
        let e = between(
            col("age"),
            lit(Value::Int64(18)),
            lit(Value::Int64(65)),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(false));
    }

    #[test]
    fn between_value_above_range() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(100)]);
        let e = between(
            col("age"),
            lit(Value::Int64(18)),
            lit(Value::Int64(65)),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(false));
    }

    #[test]
    fn between_lower_bound_inclusive() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(10)]);
        let e = between(
            col("x"),
            lit(Value::Int64(10)),
            lit(Value::Int64(20)),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn between_upper_bound_inclusive() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(20)]);
        let e = between(
            col("x"),
            lit(Value::Int64(10)),
            lit(Value::Int64(20)),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn not_between_value_in_range() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(25)]);
        let e = between(
            col("age"),
            lit(Value::Int64(18)),
            lit(Value::Int64(65)),
            true,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(false));
    }

    #[test]
    fn not_between_value_outside_range() {
        let s = schema(&[("age", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        let e = between(
            col("age"),
            lit(Value::Int64(18)),
            lit(Value::Int64(65)),
            true,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn between_null_expr_yields_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        let e = between(col("x"), lit(Value::Int64(1)), lit(Value::Int64(10)), false);
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn between_null_low_yields_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        let e = between(col("x"), lit(Value::Null), lit(Value::Int64(10)), false);
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn between_null_high_yields_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(5)]);
        let e = between(col("x"), lit(Value::Int64(1)), lit(Value::Null), false);
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn between_string_values() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("m".into())]);
        let e = between(
            col("name"),
            lit(Value::String("a".into())),
            lit(Value::String("z".into())),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    // ── CASE WHEN ────────────────────────────────────────────────────────────

    #[test]
    fn case_searched_first_branch_matches() {
        // CASE WHEN x = 1 THEN 10 WHEN x = 2 THEN 20 END  — x is 1
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let e = case(
            None,
            vec![
                (
                    binop(col("x"), BinOp::Eq, lit(Value::Int64(1))),
                    lit(Value::Int64(10)),
                ),
                (
                    binop(col("x"), BinOp::Eq, lit(Value::Int64(2))),
                    lit(Value::Int64(20)),
                ),
            ],
            None,
        );
        assert_eq!(eval(&e, &t, &s), Value::Int64(10));
    }

    #[test]
    fn case_searched_second_branch_matches() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(2)]);
        let e = case(
            None,
            vec![
                (
                    binop(col("x"), BinOp::Eq, lit(Value::Int64(1))),
                    lit(Value::Int64(10)),
                ),
                (
                    binop(col("x"), BinOp::Eq, lit(Value::Int64(2))),
                    lit(Value::Int64(20)),
                ),
            ],
            None,
        );
        assert_eq!(eval(&e, &t, &s), Value::Int64(20));
    }

    #[test]
    fn case_searched_no_match_no_else_yields_null() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(99)]);
        let e = case(
            None,
            vec![(
                binop(col("x"), BinOp::Eq, lit(Value::Int64(1))),
                lit(Value::Int64(10)),
            )],
            None,
        );
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn case_searched_no_match_returns_else() {
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Int64(99)]);
        let e = case(
            None,
            vec![(
                binop(col("x"), BinOp::Eq, lit(Value::Int64(1))),
                lit(Value::Int64(10)),
            )],
            Some(lit(Value::Int64(0))),
        );
        assert_eq!(eval(&e, &t, &s), Value::Int64(0));
    }

    #[test]
    fn case_simple_operand_matches_first_branch() {
        // CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' END  — status is 1
        let s = schema(&[("status", Type::Int64)]);
        let t = tuple(vec![Value::Int64(1)]);
        let e = case(
            Some(col("status")),
            vec![
                (lit(Value::Int64(1)), lit(Value::String("active".into()))),
                (lit(Value::Int64(2)), lit(Value::String("inactive".into()))),
            ],
            None,
        );
        assert_eq!(eval(&e, &t, &s), Value::String("active".into()));
    }

    #[test]
    fn case_simple_no_match_returns_else() {
        let s = schema(&[("status", Type::Int64)]);
        let t = tuple(vec![Value::Int64(99)]);
        let e = case(
            Some(col("status")),
            vec![(lit(Value::Int64(1)), lit(Value::String("active".into())))],
            Some(lit(Value::String("unknown".into()))),
        );
        assert_eq!(eval(&e, &t, &s), Value::String("unknown".into()));
    }

    #[test]
    fn case_simple_null_operand_skips_all_branches() {
        // NULL operand never equals a non-null WHEN value — falls through to ELSE/NULL
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        let e = case(
            Some(col("x")),
            vec![(lit(Value::Int64(1)), lit(Value::Int64(10)))],
            Some(lit(Value::Int64(0))),
        );
        assert_eq!(eval(&e, &t, &s), Value::Int64(0));
    }

    fn like_expr(inner: Expr, pattern: Expr, negated: bool) -> Expr {
        Expr::Like {
            expr: Box::new(inner),
            pattern: Box::new(pattern),
            negated,
        }
    }

    #[test]
    fn like_prefix_wildcard_matches() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("Alice".into())]);
        let e = like_expr(col("name"), lit(Value::String("Ali%".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn like_prefix_wildcard_no_match() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("Bob".into())]);
        let e = like_expr(col("name"), lit(Value::String("Ali%".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(false));
    }

    #[test]
    fn like_percent_matches_empty() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String(String::new())]);
        let e = like_expr(col("name"), lit(Value::String("%".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn like_underscore_matches_one_char() {
        let s = schema(&[("code", Type::String)]);
        let t = tuple(vec![Value::String("A1".into())]);
        let e = like_expr(col("code"), lit(Value::String("__".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn like_underscore_wrong_length() {
        let s = schema(&[("code", Type::String)]);
        let t = tuple(vec![Value::String("A".into())]);
        let e = like_expr(col("code"), lit(Value::String("__".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(false));
    }

    #[test]
    fn like_exact_literal_match() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("hello".into())]);
        let e = like_expr(col("name"), lit(Value::String("hello".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn like_suffix_wildcard() {
        let s = schema(&[("email", Type::String)]);
        let t = tuple(vec![Value::String("user@example.com".into())]);
        let e = like_expr(
            col("email"),
            lit(Value::String("%@example.com".into())),
            false,
        );
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn not_like_negates() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("Alice".into())]);
        let e = like_expr(col("name"), lit(Value::String("B%".into())), true);
        assert_eq!(eval(&e, &t, &s), Value::Bool(true));
    }

    #[test]
    fn like_null_text_yields_null() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::Null]);
        let e = like_expr(col("name"), lit(Value::String("A%".into())), false);
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn like_null_pattern_yields_null() {
        let s = schema(&[("name", Type::String)]);
        let t = tuple(vec![Value::String("Alice".into())]);
        let e = like_expr(col("name"), lit(Value::Null), false);
        assert_eq!(eval(&e, &t, &s), Value::Null);
    }

    #[test]
    fn case_simple_null_when_does_not_match() {
        // A NULL in the WHEN list should not match even a NULL operand
        let s = schema(&[("x", Type::Int64)]);
        let t = tuple(vec![Value::Null]);
        let e = case(
            Some(col("x")),
            vec![(lit(Value::Null), lit(Value::Int64(10)))],
            Some(lit(Value::Int64(0))),
        );
        assert_eq!(eval(&e, &t, &s), Value::Int64(0));
    }
}
