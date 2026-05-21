//! Physical join executors.
//!
//! All executors in this module implement the same logical operator: produce an output tuple
//! `left.concat(right)` for every pair \((l, r)\) where the [`JoinPredicate`] evaluates to `true`.
//! The implementations differ in *when* they materialize input, how they search for matches, and
//! which predicates they support.
//!
//! ## Implemented executors
//!
//! - [`NestedLoopJoin`]
//!   - **Algorithm**: buffer the entire right input once, then for each left tuple evaluate the
//!     predicate against every buffered right tuple.
//!   - **Predicates**: supports all operators implemented by [`JoinPredicate::evaluate`] (equality
//!     and ordering predicates; `LIKE` currently always returns `false`).
//!   - **Memory**: \(O(|R|)\) to materialize the right side.
//!   - **Output order**: left-input order; within a left tuple, matches follow the right input
//!     order from materialization time.
//!
//! - [`HashJoin`]
//!   - **Algorithm**: build a hash table on the right join key once, then probe it with each left
//!     tuple.
//!   - **Predicates**: **equality only** (`predicate.op == Predicate::Equals`). The constructor
//!     asserts this precondition.
//!   - **Memory**: \(O(|R|)\) for the hash table (buckets store cloned right tuples).
//!   - **Output order**: left-input order; within a left tuple, matches follow the bucket order
//!     (right scan order).
//!
//! - [`SortMergeJoin`]
//!   - **Algorithm**: drain *both* inputs, sort each side by its join key, then merge the two
//!     sorted streams and emit the Cartesian product for equal-key runs.
//!   - **Predicates**: **equality only** (`predicate.op == Predicate::Equals`). The constructor
//!     asserts this precondition.
//!   - **Memory**: \(O(|L| + |R|)\) because both sides are materialized for sorting.
//!   - **Output order**: ascending join key order (as defined by `Value::partial_cmp`).
//!
//! ## NULL semantics and key validation
//!
//! - **Join keys with `NULL` never match**. [`JoinPredicate::evaluate`] returns `Ok(false)` if
//!   either key is [`Value::Null`].
//! - Executors also **skip tuples whose join key is `NULL` or missing** while
//!   materializing/building their internal state:
//!   - [`NestedLoopJoin`] filters the *right* side during materialization.
//!   - [`HashJoin`] filters the right side during hash build, and skips left tuples with
//!     `NULL`/missing keys.
//!   - [`SortMergeJoin`] filters both sides while draining inputs prior to sorting.
//!
//! ## Rewind behavior
//!
//! All executors implement [`Executor::rewind`], but the amount of retained state differs:
//! - [`NestedLoopJoin`] keeps the materialized right buffer and rewinds only the left input.
//! - [`HashJoin`] keeps the built hash table and rewinds only the left input.
//! - [`SortMergeJoin`] drops the sorted buffers and rewinds **both** children so it can drain/sort
//!   again on the next iteration.

mod cross;
mod hash;
mod nested_loop;
mod sort_merge;

use std::cmp::Ordering;

pub use cross::CrossJoin;
pub use hash::HashJoin;
pub use nested_loop::NestedLoopJoin;
pub use sort_merge::SortMergeJoin;

use super::{ExecutionError, Executor};
use crate::{
    Value,
    execution::PlanNode,
    primitives::{ColumnId, Predicate},
    tuple::{Tuple, TupleSchema},
};

/// Specifies how non-matching rows are handled in a join.
///
/// - [`Inner`](JoinType::Inner): only rows that satisfy the join condition appear in the output.
///   This is the SQL default (`JOIN` or `INNER JOIN`).
/// - [`LeftOuter`](JoinType::LeftOuter): every left row appears at least once. When a left row has
///   no matching right row the right columns are filled with `NULL`.  SQL: `LEFT JOIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    FullOuter,
}

/// A join predicate is a condition that must be satisfied by two tuples in order to be joined.
/// It is used to determine which tuples from the left and right relations should be joined.
#[derive(Debug, Clone, Copy)]
pub struct JoinPredicate {
    pub left_col: ColumnId,
    pub right_col: ColumnId,
    pub op: Predicate,
}

impl JoinPredicate {
    /// Creates a new join predicate comparing `left_col` to `right_col` with `op`.
    pub fn new(left_col: ColumnId, right_col: ColumnId, op: Predicate) -> Self {
        Self {
            left_col,
            right_col,
            op,
        }
    }

    /// Evaluates the join predicate against two tuples.
    ///
    /// Returns `Ok(true)` if the predicate is satisfied, `Ok(false)` if not,
    /// or `Err(ExecutionError)` if there is an error.
    ///
    /// `NULL` values never match: if either side is [`Value::Null`], the result is `Ok(false)`.
    /// The [`Predicate::Like`] operator is currently unsupported and always returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// Returns an [`ExecutionError::TypeError`] if the
    /// column index is out of bounds for the tuple.
    pub fn evaluate(&self, left: &Tuple, right: &Tuple) -> Result<bool, ExecutionError> {
        let l = get_value(left, self.left_col, true)?;
        let r = get_value(right, self.right_col, false)?;

        if matches!(l, Value::Null) || matches!(r, Value::Null) {
            return Ok(false);
        }

        Ok(match self.op {
            Predicate::Equals => l == r,
            Predicate::NotEqual | Predicate::NotEqualBracket => l != r,
            Predicate::LessThan => l.partial_cmp(r).is_some_and(Ordering::is_lt),
            Predicate::LessThanOrEqual => l.partial_cmp(r).is_some_and(Ordering::is_le),
            Predicate::GreaterThan => l.partial_cmp(r).is_some_and(Ordering::is_gt),
            Predicate::GreaterThanOrEqual => l.partial_cmp(r).is_some_and(Ordering::is_ge),
            Predicate::Like => false,
        })
    }
}

/// Returns a tuple of `n` `NULL` values, used to pad the right side when a left row has no match
/// in a `LEFT OUTER JOIN`.
#[inline]
pub(super) fn null_right_tuple(n: usize) -> Tuple {
    Tuple::new(vec![Value::Null; n])
}

/// Fetches a value by column id with a side-specific error message.
///
/// This helper returns a reference into `tuple`, so the output lifetime is tied to the input
/// borrow of `tuple` (via `<'a>`).
#[inline]
pub(super) fn get_value(
    tuple: &Tuple,
    col: ColumnId,
    is_left: bool,
) -> Result<&Value, ExecutionError> {
    tuple.value_at(col).map_err(|_| {
        ExecutionError::TypeError(format!(
            "{} col {col} out of bounds",
            if is_left { "left" } else { "right" }
        ))
    })
}

/// Internal container for a pair of child inputs, their merged output schema, and the join type.
///
/// Used by every join executor as the common "two children + output schema + join type" bundle.
/// The join-specific matching logic (`JoinPredicate`, `Expr` residual,
/// etc.) lives on each executor itself.
///
/// Not exposed outside the execution module.
#[derive(Debug)]
pub(super) struct JoinInputs<'a> {
    pub(super) left: PlanNode<'a>,
    pub(super) right: PlanNode<'a>,
    pub(super) schema: TupleSchema,
    /// Number of columns in the left child's output.
    pub(super) left_width: usize,
    /// Number of columns in the right child's output.
    pub(super) right_width: usize,
    pub(super) join_type: JoinType,
}

impl<'a> JoinInputs<'a> {
    /// Constructs a new `JoinInputs` by merging the two children's schemas.
    ///
    /// `join_type` defaults to [`JoinType::Inner`]; call `with_join_type` on the
    /// enclosing executor to override it.
    pub(super) fn new(left: PlanNode<'a>, right: PlanNode<'a>) -> Self {
        let left_width = left.schema().physical_num_fields();
        let right_width = right.schema().physical_num_fields();
        let schema = left.schema().merge(right.schema());
        Self {
            left,
            right,
            schema,
            left_width,
            right_width,
            join_type: JoinType::Inner,
        }
    }
}

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests {
    use super::test_utils::*;

    fn tup(a: i32, b: i32) -> Tuple {
        Tuple::new(vec![Value::Int32(a), Value::Int32(b)])
    }

    fn tup_null_a(b: i32) -> Tuple {
        Tuple::new(vec![Value::Null, Value::Int32(b)])
    }

    #[test]
    fn test_predicate_equals_match() {
        let p = eq_pred(0, 0);
        assert!(p.evaluate(&tup(1, 9), &tup(1, 8)).unwrap());
        assert!(!p.evaluate(&tup(1, 9), &tup(2, 8)).unwrap());
    }

    #[test]
    fn test_predicate_not_equal_variants() {
        for op in [Predicate::NotEqual, Predicate::NotEqualBracket] {
            let p = JoinPredicate::new(col(0), col(0), op);
            assert!(p.evaluate(&tup(1, 0), &tup(2, 0)).unwrap());
            assert!(!p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap());
        }
    }

    #[test]
    fn test_predicate_ordering_ops() {
        let t1 = tup(1, 0);
        let t2 = tup(2, 0);
        let mk = |op| JoinPredicate::new(col(0), col(0), op);
        assert!(mk(Predicate::LessThan).evaluate(&t1, &t2).unwrap());
        assert!(!mk(Predicate::LessThan).evaluate(&t2, &t1).unwrap());
        assert!(mk(Predicate::LessThanOrEqual).evaluate(&t1, &t1).unwrap());
        assert!(mk(Predicate::GreaterThan).evaluate(&t2, &t1).unwrap());
        assert!(
            mk(Predicate::GreaterThanOrEqual)
                .evaluate(&t1, &t1)
                .unwrap()
        );
    }

    #[test]
    fn test_predicate_null_never_matches() {
        let p = eq_pred(0, 0);
        let l = tup_null_a(0);
        let r = tup(1, 0);
        assert!(!p.evaluate(&l, &r).unwrap());
        assert!(!p.evaluate(&r, &l).unwrap());
        assert!(!p.evaluate(&l, &l).unwrap());
    }

    #[test]
    fn test_predicate_like_always_false() {
        let p = JoinPredicate::new(col(0), col(0), Predicate::Like);
        assert!(!p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap());
    }

    #[test]
    fn test_predicate_out_of_bounds_left() {
        let p = eq_pred(5, 0);
        let err = p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(ref m) if m.contains("left")));
    }

    #[test]
    fn test_predicate_out_of_bounds_right() {
        let p = eq_pred(0, 5);
        let err = p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(ref m) if m.contains("right")));
    }
}
