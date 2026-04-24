//! Scalar and boolean expressions evaluated over a single tuple.
//!
//! These are the primitive building blocks that filter-like operators (`WHERE`
//! clauses, join residuals, `HAVING` clauses) use to decide whether a tuple
//! qualifies. They are intentionally separate from the physical operators in
//! [`super::unary`] because the same expression type is consumed in multiple
//! places (filters, joins, the binder) and will grow to include arithmetic,
//! functions, and CASE expressions.
//!
//! # Shape
//!
//! - [`Operand`] — one side of a comparison: either a column reference (resolved by index) or a
//!   literal constant.
//! - [`BooleanExpression`] — a tree of `And` / `Or` / `Not` nodes over
//!   [`Leaf`](BooleanExpression::Leaf) comparisons. Evaluates to `bool` for any given tuple.
//!
//! # NULL semantics
//!
//! `NULL` on either side of a `Leaf` comparison short-circuits to `false`. This
//! mirrors how SQL treats `NULL` in a `WHERE` clause (rows with `NULL` keys
//! are filtered out) but does not model full SQL three-valued logic.

use std::cmp::Ordering;

use super::ExecutionError;
use crate::{primitives::Predicate, tuple::Tuple, types::Value};

/// One side of a comparison in a [`BooleanExpression::Leaf`].
///
/// An operand is either a column reference (resolved by index into the tuple
/// being evaluated) or a literal constant hard-coded into the plan. This is
/// what lets a single `Leaf` shape express both filter predicates
/// (`col <op> literal`) and join residuals (`col <op> col`).
#[derive(Debug, Clone)]
pub enum Operand {
    Column(usize),
    Literal(Value),
}

impl Operand {
    /// Resolves this operand to a `Value` reference inside `tuple`.
    ///
    /// - `Column(i)` reads `tuple.get(i)`.
    /// - `Literal(v)` hands back the stored value.
    ///
    /// The output lifetime is tied to whichever of `self` or `tuple` lives
    /// shorter, so no cloning is needed to compare operands.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if a `Column` index is out of
    /// bounds for `tuple`.
    fn resolve<'a>(&'a self, tuple: &'a Tuple) -> Result<&'a Value, ExecutionError> {
        match self {
            Self::Column(idx) => tuple
                .get(*idx)
                .ok_or_else(|| ExecutionError::TypeError(format!("column {idx} out of bounds"))),
            Self::Literal(v) => Ok(v),
        }
    }
}

/// A boolean expression evaluated over a single tuple.
///
/// Compound nodes (`And` / `Or` / `Not`) recurse; `Leaf` compares two
/// [`Operand`]s with a [`Predicate`] operator. `NULL` on either side of a
/// comparison yields `false` — SQL three-valued logic is not modeled here.
#[derive(Debug, Clone)]
pub enum BooleanExpression {
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    Or(Box<BooleanExpression>, Box<BooleanExpression>),
    Not(Box<BooleanExpression>),
    Leaf {
        left: Operand,
        op: Predicate,
        right: Operand,
    },
}

impl BooleanExpression {
    /// Builds a `col <op> literal` leaf — the common filter shape.
    pub fn col_op_lit(col: usize, op: Predicate, lit: Value) -> Self {
        Self::Leaf {
            left: Operand::Column(col),
            op,
            right: Operand::Literal(lit),
        }
    }

    /// Builds a `col_l <op> col_r` leaf — used by join residual predicates
    /// evaluated over the concatenated `left ⋈ right` tuple.
    pub fn col_op_col(left_col: usize, op: Predicate, right_col: usize) -> Self {
        Self::Leaf {
            left: Operand::Column(left_col),
            op,
            right: Operand::Column(right_col),
        }
    }

    /// Evaluates the boolean expression against a tuple.
    ///
    /// Returns `Ok(true)` if the expression is satisfied, `Ok(false)` if not.
    /// `NULL` on either side of a `Leaf` comparison short-circuits to
    /// `Ok(false)`. `LIKE` against non-string operands returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any referenced column index
    /// is out of bounds for `tuple`.
    pub fn eval(&self, tuple: &Tuple) -> Result<bool, ExecutionError> {
        match self {
            Self::Leaf { left, op, right } => {
                let l = left.resolve(tuple)?;
                let r = right.resolve(tuple)?;

                if matches!(l, Value::Null) || matches!(r, Value::Null) {
                    return Ok(false);
                }

                Ok(match op {
                    Predicate::Equals => l == r,
                    Predicate::NotEqual | Predicate::NotEqualBracket => l != r,
                    Predicate::LessThan => l.partial_cmp(r).is_some_and(Ordering::is_lt),
                    Predicate::LessThanOrEqual => l.partial_cmp(r).is_some_and(Ordering::is_le),
                    Predicate::GreaterThan => l.partial_cmp(r).is_some_and(Ordering::is_gt),
                    Predicate::GreaterThanOrEqual => l.partial_cmp(r).is_some_and(Ordering::is_ge),
                    Predicate::Like => match (l, r) {
                        (Value::String(s), Value::String(p)) => Self::like_match(s, p),
                        _ => false,
                    },
                })
            }
            Self::And(l, r) => Ok(l.eval(tuple)? && r.eval(tuple)?),
            Self::Or(l, r) => Ok(l.eval(tuple)? || r.eval(tuple)?),
            Self::Not(expr) => Ok(!expr.eval(tuple)?),
        }
    }

    /// Checks whether `s` matches the SQL `LIKE` pattern in `pattern`.
    ///
    /// Supports two wildcards:
    /// - `%` — matches any sequence of zero or more characters.
    /// - `_` — matches exactly one character.
    ///
    /// Uses a DP approach over the pattern to handle overlapping `%` spans
    /// correctly and in O(|s| × |p|) time.
    fn like_match(s: &str, p: &str) -> bool {
        let s: Vec<char> = s.chars().collect();
        let p: Vec<char> = p.chars().collect();
        let n = s.len();
        let m = p.len();
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[0][0] = true;
        for j in 1..=m {
            if p[j - 1] == '%' {
                dp[0][j] = dp[0][j - 1];
            }
        }
        for i in 1..=n {
            for j in 1..=m {
                let pc = p[j - 1];
                dp[i][j] = if pc == '%' {
                    dp[i][j - 1] || dp[i - 1][j]
                } else if pc == '_' {
                    dp[i - 1][j - 1]
                } else {
                    dp[i - 1][j - 1] && s[i - 1] == pc
                };
            }
        }
        dp[n][m]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval_like(s: &str, pattern: &str) -> bool {
        let expr =
            BooleanExpression::col_op_lit(0, Predicate::Like, Value::String(pattern.to_string()));
        let tuple = Tuple::new(vec![Value::String(s.to_string())]);
        expr.eval(&tuple).unwrap()
    }

    #[test]
    fn test_boolean_expression_eval_leaf_equals() {
        let e = BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(7));
        assert!(
            e.eval(&Tuple::new(vec![Value::Int32(7), Value::Bool(true)]))
                .unwrap()
        );
        assert!(
            !e.eval(&Tuple::new(vec![Value::Int32(8), Value::Bool(true)]))
                .unwrap()
        );
    }

    #[test]
    fn test_boolean_expression_eval_leaf_ordered_preds() {
        let tup = Tuple::new(vec![Value::Int32(5), Value::Bool(false)]);

        let lt = BooleanExpression::col_op_lit(0, Predicate::LessThan, Value::Int32(10));
        assert!(lt.eval(&tup).unwrap());

        let le = BooleanExpression::col_op_lit(0, Predicate::LessThanOrEqual, Value::Int32(5));
        assert!(le.eval(&tup).unwrap());

        let gt = BooleanExpression::col_op_lit(0, Predicate::GreaterThan, Value::Int32(1));
        assert!(gt.eval(&tup).unwrap());

        let ge = BooleanExpression::col_op_lit(0, Predicate::GreaterThanOrEqual, Value::Int32(5));
        assert!(ge.eval(&tup).unwrap());
    }

    #[test]
    fn test_boolean_expression_eval_leaf_not_equal_and_bracket() {
        let tup = Tuple::new(vec![Value::Int32(1), Value::Bool(true)]);
        let ne = BooleanExpression::col_op_lit(0, Predicate::NotEqual, Value::Int32(2));
        let neb = BooleanExpression::col_op_lit(0, Predicate::NotEqualBracket, Value::Int32(2));
        assert!(ne.eval(&tup).unwrap());
        assert!(neb.eval(&tup).unwrap());
        assert!(
            !ne.eval(&Tuple::new(vec![Value::Int32(2), Value::Bool(true)]))
                .unwrap()
        );
    }

    #[test]
    fn test_boolean_expression_eval_and_or_not() {
        let t = Tuple::new(vec![Value::Bool(true), Value::Bool(false)]);

        let a = BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Bool(true));
        let b = BooleanExpression::col_op_lit(1, Predicate::Equals, Value::Bool(false));

        assert!(
            BooleanExpression::And(Box::new(a.clone()), Box::new(b.clone()))
                .eval(&t)
                .unwrap()
        );
        assert!(
            !BooleanExpression::And(Box::new(a.clone()), Box::new(b.clone()))
                .eval(&Tuple::new(vec![Value::Bool(true), Value::Bool(true)]))
                .unwrap()
        );

        assert!(
            BooleanExpression::Or(Box::new(b.clone()), Box::new(a.clone()))
                .eval(&t)
                .unwrap()
        );
        assert!(
            BooleanExpression::Or(Box::new(a.clone()), Box::new(b.clone()))
                .eval(&Tuple::new(vec![Value::Bool(false), Value::Bool(false)]))
                .unwrap()
        );

        assert!(
            !BooleanExpression::Not(Box::new(a.clone()))
                .eval(&t)
                .unwrap()
        );

        let col0_false = BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Bool(false));
        assert!(
            BooleanExpression::Not(Box::new(col0_false))
                .eval(&t)
                .unwrap()
        );
    }

    #[test]
    fn test_boolean_expression_eval_leaf_column_out_of_bounds_errors() {
        let e = BooleanExpression::col_op_lit(99, Predicate::Equals, Value::Int32(0));
        let err = e.eval(&Tuple::new(vec![Value::Int32(0)])).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn test_boolean_expression_eval_leaf_like_non_string_returns_false() {
        let e = BooleanExpression::col_op_lit(0, Predicate::Like, Value::String("%".to_string()));
        assert!(!e.eval(&Tuple::new(vec![Value::Int32(1)])).unwrap());
    }

    #[test]
    fn test_boolean_expression_eval_ordered_pred_incomparable_returns_false() {
        let cross = BooleanExpression::col_op_lit(0, Predicate::LessThan, Value::Int64(100));
        assert!(!cross.eval(&Tuple::new(vec![Value::Int32(1)])).unwrap());

        let nan_cmp = BooleanExpression::col_op_lit(0, Predicate::LessThan, Value::Float64(0.0));
        assert!(
            !nan_cmp
                .eval(&Tuple::new(vec![Value::Float64(f64::NAN)]))
                .unwrap()
        );
    }

    #[test]
    fn test_boolean_expression_col_op_col_compares_two_columns() {
        // `col0 = col1` — the shape a join residual would use against a concatenated tuple.
        let e = BooleanExpression::col_op_col(0, Predicate::Equals, 1);
        assert!(
            e.eval(&Tuple::new(vec![Value::Int32(5), Value::Int32(5)]))
                .unwrap()
        );
        assert!(
            !e.eval(&Tuple::new(vec![Value::Int32(5), Value::Int32(6)]))
                .unwrap()
        );
    }

    #[test]
    fn test_boolean_expression_leaf_null_short_circuits_to_false() {
        let e = BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(1));
        assert!(!e.eval(&Tuple::new(vec![Value::Null])).unwrap());
    }

    // --- happy path: LIKE via eval ---

    #[test]
    fn test_boolean_expression_eval_like_exact_and_percent() {
        assert!(eval_like("hello", "hello"));
        assert!(eval_like("hello", "h%o"));
        assert!(eval_like("hello", "%ello"));
        assert!(eval_like("hello", "hel%"));
        assert!(eval_like("hello", "%ell%"));
        assert!(!eval_like("hello", "h%xo"));
    }

    #[test]
    fn test_boolean_expression_eval_like_underscore() {
        assert!(eval_like("a", "_"));
        assert!(eval_like("ab", "__"));
        assert!(eval_like("axb", "a_b"));
        assert!(!eval_like("ab", "___"));
        assert!(!eval_like("", "_"));
    }

    #[test]
    fn test_boolean_expression_eval_like_empty_string_patterns() {
        assert!(eval_like("", ""));
        assert!(eval_like("", "%"));
        assert!(eval_like("", "%%"));
        assert!(!eval_like("", "_"));
    }

    #[test]
    fn test_boolean_expression_eval_like_consecutive_percent() {
        assert!(eval_like("abc", "a%%bc"));
        assert!(eval_like("x", "%_%"));
    }

    #[test]
    fn test_boolean_expression_eval_like_pattern_longer_than_string() {
        assert!(!eval_like("hi", "hello"));
        assert!(!eval_like("a", "__"));
    }

    #[test]
    fn test_boolean_expression_eval_like_unicode_scalar() {
        assert!(eval_like("café", "caf_"));
        assert!(eval_like("café", "%é"));
        assert!(!eval_like("caf", "caf_"));
    }
}
