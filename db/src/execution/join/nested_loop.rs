use std::collections::VecDeque;

use fallible_iterator::FallibleIterator;

use super::{JoinInputs, JoinType, null_right_tuple};
use crate::{
    execution::{ExecutionError, Executor, PlanNode, ResolvedExpr, eval_resolved_bool},
    tuple::{Tuple, TupleSchema},
};

/// Joins two inputs by pairing every left tuple with every right tuple.
///
/// The right input is materialized once. For each left tuple, the executor concatenates
/// `left.concat(right)` with every buffered right tuple and evaluates `predicate` over
/// the combined tuple, emitting pairs that satisfy it.
///
/// Because the predicate is a general [`ResolvedExpr`] evaluated via [`eval_resolved_bool`], NLJ
/// supports arbitrary boolean combinations over both sides (e.g. `l.a = r.x AND l.b < r.y`).
///
/// # SQL shape
///
/// Nested-loop join is the most general join executor here (it can handle non-equality
/// predicates). Example:
///
/// ```sql
/// SELECT *
/// FROM left_table  AS l
/// JOIN right_table AS r
///   ON l.a = r.x AND l.b < r.y;
/// ```
#[derive(Debug)]
pub struct NestedLoopJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    predicate: ResolvedExpr,
    join_type: JoinType,
    right_buf: Option<Vec<Tuple>>,
    row_matches: VecDeque<Tuple>,
}

impl<'a> NestedLoopJoin<'a> {
    /// Creates a nested-loop join executor for `left ⋈ right` using `predicate`.
    ///
    /// `predicate` must have all column references pre-resolved to
    /// [`crate::primitives::ColumnId`]s. It is evaluated over the concatenated
    /// `left.concat(right)` tuple.
    ///
    /// The right input is read and buffered on the first call to [`FallibleIterator::next`].
    #[tracing::instrument(skip_all, fields(op = "nlj"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: ResolvedExpr) -> Self {
        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            predicate,
            join_type: JoinType::Inner,
            right_buf: None,
            row_matches: VecDeque::new(),
        }
    }

    /// Sets the join type. Use [`JoinType::LeftOuter`] to get `LEFT JOIN` semantics.
    #[must_use]
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Returns the number of columns in the left child — i.e. the offset at which
    /// right-side columns start in the concatenated output tuple.
    #[inline]
    pub fn left_width(&self) -> usize {
        self.inputs.left_width
    }

    /// Reads and buffers every right-side tuple into `self.right_buf`.
    ///
    /// Unlike the previous equi-key version, nothing is filtered here — the predicate is
    /// now a general expression that decides matches on its own, including NULL semantics.
    ///
    /// # Errors
    ///
    /// Returns an [`ExecutionError`] if retrieving any tuple from the right input fails.
    fn materialize_right(&mut self) -> Result<(), ExecutionError> {
        if self.right_buf.is_some() {
            return Ok(());
        }
        let mut buf = Vec::new();
        while let Some(tuple) = self.inputs.right.next()? {
            buf.push(tuple);
        }
        tracing::debug!(tuples = buf.len(), "nlj: right side buffered");
        self.right_buf = Some(buf);
        Ok(())
    }
}

/// Produces joined tuples by pairing each left tuple with every right tuple satisfying the
/// predicate over the concatenated `left ⋈ right` tuple.
///
/// Note:
/// - Right input is only scanned and materialized once per executor.
/// - Output order is: all qualifying (left, right) pairs for each left row, in left input order.
///   The inner (right) order matches the right input order at materialization time.
/// - If `row_matches` is non-empty when `next` is called, it emits these join results before
///   advancing the left input.
impl FallibleIterator for NestedLoopJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_right()?;
        let right_width = self.inputs.right_width;

        loop {
            if let Some(tuple) = self.row_matches.pop_front() {
                return Ok(Some(tuple));
            }

            let Some(l) = self.inputs.left.next()? else {
                return Ok(None);
            };

            for right in self.right_buf.as_deref().unwrap() {
                let joined = l.concat(right);
                if eval_resolved_bool(&self.predicate, &joined)? {
                    self.row_matches.push_back(joined);
                }
            }

            // LEFT OUTER: if this left row matched nothing, emit it with a NULL-padded right side.
            if self.join_type == JoinType::LeftOuter && self.row_matches.is_empty() {
                self.row_matches
                    .push_back(l.concat(&null_right_tuple(right_width)));
            }
        }
    }
}

impl Executor for NestedLoopJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.row_matches.clear();
        self.inputs.left.rewind()
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_utils::*, NestedLoopJoin};

    #[test]
    fn test_nlj_basic_equi_join() {
        let left = build_heap(101, &[tup(1, 10), tup(2, 20), tup(3, 30)]);
        let right = build_heap_xy(102, &[tup(1, 100), tup(2, 200), tup(4, 400)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        let mut pairs: Vec<(i32, i32)> = out.iter().map(|t| (int(t, 0), int(t, 2))).collect();
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(1, 1), (2, 2)]);
    }

    #[test]
    fn test_nlj_empty_left() {
        let left = build_heap(103, &[]);
        let right = build_heap_xy(104, &[tup(1, 100)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_nlj_empty_right() {
        let left = build_heap(105, &[tup(1, 10)]);
        let right = build_heap_xy(106, &[]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_nlj_duplicate_keys_cartesian() {
        let left = build_heap(107, &[tup(1, 10), tup(1, 11)]);
        let right = build_heap_xy(108, &[tup(1, 100), tup(1, 101)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(drain(&mut j).len(), 4);
    }

    #[test]
    fn test_nlj_skips_right_null_keys() {
        let left = build_heap(109, &[tup(1, 10)]);
        let right = build_heap_xy(110, &[tup_null_a(100), tup(1, 200)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(drain(&mut j).len(), 1);
    }

    #[test]
    fn test_nlj_less_than_predicate() {
        let left = build_heap(111, &[tup(1, 0), tup(5, 0)]);
        let right = build_heap_xy(112, &[tup(3, 0), tup(10, 0)]);
        let p = nlj_col_expr(0, BinOp::Lt, 2);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), p);
        assert_eq!(drain(&mut j).len(), 3);
    }

    #[test]
    fn test_nlj_rewind_replays_output() {
        let left = build_heap(113, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap_xy(114, &[tup(1, 100), tup(2, 200)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }

    #[test]
    fn test_nlj_schema_merged() {
        let left = build_heap(115, &[]);
        let right = build_heap_xy(116, &[]);
        let j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(j.schema().physical_num_fields(), 4);
    }

    #[test]
    fn test_nlj_left_outer_unmatched_row_gets_nulls() {
        let left = build_heap(220, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap_xy(221, &[tup(1, 100)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2())
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        let unmatched = out.iter().find(|t| int(t, 0) == 9).unwrap();
        assert!(matches!(unmatched.get(2), Some(Value::Null)));
    }

    #[test]
    fn test_nlj_left_outer_all_matched_same_as_inner() {
        let left = build_heap(222, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap_xy(223, &[tup(1, 100), tup(2, 200)]);
        let pred = nlj_eq_0_0_w2();
        let mut inner = NestedLoopJoin::new(scan(&left), scan(&right), pred.clone());
        let mut outer = NestedLoopJoin::new(scan(&left), scan(&right), pred)
            .with_join_type(JoinType::LeftOuter);
        assert_eq!(drain(&mut inner).len(), drain(&mut outer).len());
    }

    #[test]
    fn test_nlj_left_outer_empty_right() {
        let left = build_heap(224, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap_xy(225, &[]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2())
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|t| matches!(t.get(2), Some(Value::Null))));
    }

    #[test]
    fn test_nlj_compound_and_predicate() {
        let left = build_heap(151, &[tup(1, 10), tup(1, 50), tup(2, 10)]);
        let right = build_heap_xy(152, &[tup(1, 20), tup(2, 5)]);
        let pred = ResolvedExpr::BinaryOp {
            lhs: Box::new(nlj_col_expr(0, BinOp::Eq, 2)),
            op: BinOp::And,
            rhs: Box::new(nlj_col_expr(1, BinOp::Lt, 3)),
        };
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), pred);
        assert_eq!(drain(&mut j).len(), 1);
    }
}
