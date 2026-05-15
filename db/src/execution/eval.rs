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
    parser::statements::{BinOp, Expr, UnOp},
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
        parser::statements::{BinOp, Expr, UnOp},
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

    // ── logical connectives ──────────────────────────────────────────────────

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
}
