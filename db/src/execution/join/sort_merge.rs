use std::{
    cmp::Ordering,
    collections::VecDeque,
    ops::{Index, IndexMut},
};

use fallible_iterator::FallibleIterator;

use super::{JoinInputs, JoinPredicate, JoinType, get_value, null_right_tuple};
use crate::{
    Value,
    execution::{ExecutionError, Executor, PlanNode, ResolvedExpr},
    primitives::Predicate,
    tuple::{Tuple, TupleSchema},
};

/// Drains all tuples from the given plan node into `buf`, skipping any rows with a NULL or missing
/// value at column `idx`.
///
/// This helper consumes the entire output from the given `PlanNode`, filtering out tuples whose
/// join key value (at the specified column index) is either `NULL` or absent. Only valid (non-NULL,
/// present) tuples are pushed into the provided buffer.
///
/// # Errors
///
/// Returns an [`ExecutionError`] if retrieving the next tuple from the node fails.
fn drain_tuples(
    node: &mut PlanNode,
    buf: &mut Vec<Tuple>,
    idx: usize,
) -> Result<(), ExecutionError> {
    while let Some(t) = node.next()? {
        if matches!(t.get(idx), Some(Value::Null) | None) {
            continue;
        }
        buf.push(t);
    }
    Ok(())
}

/// Drains all tuples from `node` into `buf` without any NULL filtering.
///
/// Used for the left side of a `LEFT OUTER JOIN` in sort-merge: every left row must appear
/// in the output regardless of its join-key value, so NULL-key rows must not be dropped here.
fn drain_all_tuples(node: &mut PlanNode, buf: &mut Vec<Tuple>) -> Result<(), ExecutionError> {
    while let Some(t) = node.next()? {
        buf.push(t);
    }
    Ok(())
}

#[derive(Debug)]
struct TupleCursor(Vec<Tuple>, usize);

impl TupleCursor {
    pub fn new() -> Self {
        Self(Vec::new(), 0)
    }

    pub fn forward(&mut self) {
        self.1 += 1;
    }

    pub fn exhausted(&self) -> bool {
        self.1 >= self.0.len()
    }

    pub fn current(&self) -> &Tuple {
        self.0.get(self.1).expect("current_idx set with pending")
    }

    pub fn current_idx(&self) -> usize {
        self.1
    }
}

impl Index<usize> for TupleCursor {
    type Output = Tuple;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for TupleCursor {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

/// Joins two inputs by sorting them on the join key and then merging.
///
/// Requires an equality predicate as the merge key. An optional `residual`
/// An `Expr` residual evaluated over `left.concat(right)` can further restrict
/// matches after the key match succeeds.
///
/// # SQL shape
///
/// Sort-merge join is also for equi-joins (an `=` key). It's a good fit when inputs are
/// already ordered on the join key or can be efficiently sorted:
///
/// ```sql
/// SELECT *
/// FROM left_table  AS l
/// JOIN right_table AS r
///   ON l.a = r.x;
/// ```
#[derive(Debug)]
pub struct SortMergeJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    predicate: JoinPredicate,
    join_type: JoinType,
    residual: Option<ResolvedExpr>,
    l_sorted: TupleCursor,
    r_sorted: TupleCursor,
    pending: VecDeque<Tuple>,
    sorted: bool,
    rows_produced: usize,
}

impl<'a> SortMergeJoin<'a> {
    /// Creates a new sort-merge join executor for `left ⋈ right` using the provided predicate.
    ///
    /// # Panics
    ///
    /// Panics if the join predicate is not [`Predicate::Equals`].
    #[tracing::instrument(skip_all, fields(op = "smj"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        assert_eq!(
            predicate.op,
            Predicate::Equals,
            "SortMergeJoin requires an equality predicate, got {}",
            predicate.op
        );
        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            predicate,
            join_type: JoinType::Inner,
            residual: None,
            l_sorted: TupleCursor::new(),
            r_sorted: TupleCursor::new(),
            pending: VecDeque::new(),
            sorted: false,
            rows_produced: 0,
        }
    }

    /// Sets the join type. Use [`JoinType::LeftOuter`] to get `LEFT JOIN` semantics.
    #[must_use]
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Attaches an optional residual predicate to this sort-merge join.
    ///
    /// The residual is evaluated over the concatenated `left ⋈ right` tuple *after* the
    /// equal-key match, so right-side column references must be offset by
    /// [`left_width`](Self::left_width).
    #[must_use]
    pub fn with_residual(mut self, residual: ResolvedExpr) -> Self {
        self.residual = Some(residual);
        self
    }

    /// Returns the left child's column width — the offset at which right columns
    /// start in the concatenated output tuple.
    #[inline]
    pub fn left_width(&self) -> usize {
        self.inputs.left_width
    }

    /// Drains both left and right child executors into in-memory vectors
    /// and sorts each side by its join key column.
    fn sort_inputs(&mut self) -> Result<(), ExecutionError> {
        if self.sorted {
            return Ok(());
        }

        let left_idx = usize::from(self.predicate.left_col);
        let right_idx = usize::from(self.predicate.right_col);

        // For LEFT OUTER JOIN we must preserve all left rows (including NULL-key ones) because
        // every left row must appear in the output. NULL keys sort first, so during the merge
        // they will advance the left cursor immediately and emit NULL-padded output rows.
        if self.join_type == JoinType::LeftOuter {
            drain_all_tuples(&mut self.inputs.left, &mut self.l_sorted.0)?;
        } else {
            drain_tuples(&mut self.inputs.left, &mut self.l_sorted.0, left_idx)?;
        }
        drain_tuples(&mut self.inputs.right, &mut self.r_sorted.0, right_idx)?;
        tracing::debug!(
            left = self.l_sorted.0.len(),
            right = self.r_sorted.0.len(),
            "smj: inputs drained, sorting"
        );

        Self::sort_by_column(&mut self.l_sorted.0, left_idx);
        Self::sort_by_column(&mut self.r_sorted.0, right_idx);

        self.sorted = true;
        Ok(())
    }

    /// Sorts the provided slice of tuples in place by the value in column `col`.
    ///
    /// Tuples whose join key is missing (no such column) will be grouped at the end.
    ///
    /// # Arguments
    ///
    /// * `tuples` - The mutable slice of tuples to sort.
    /// * `col`    - The column index to use as the sort key.
    fn sort_by_column(tuples: &mut [Tuple], col: usize) {
        tuples.sort_by(|a, b| match (a.get(col), b.get(col)) {
            (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(Ordering::Equal),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (None, None) => Ordering::Equal,
        });
    }

    /// Finds groups of tuples on both the left and right with the same join key value,
    /// and concatenates each pair to form join result tuples, which are buffered in `self.pending`.
    ///
    /// This method advances both cursors past all the rows with the current matching key.
    ///
    /// # Errors
    ///
    /// Returns an [`ExecutionError`] if any join key value lookup fails,
    /// or if the join key values are of incomparable types.
    fn collect_equals(&mut self) -> Result<(), ExecutionError> {
        let curr = self.get_key(true)?.clone();
        let right_width = self.inputs.right_width;

        let right_start = self.r_sorted.current_idx();
        while !self.r_sorted.exhausted() && self.get_key(false)? == &curr {
            self.r_sorted.forward();
        }
        let right_end = self.r_sorted.current_idx();

        while !self.l_sorted.exhausted() && self.get_key(true)? == &curr {
            let l_curr = self.l_sorted.current().clone();
            let mut row_matched = false;
            for i in right_start..right_end {
                let r = &self.r_sorted[i];
                let joined = l_curr.concat(r);

                let keep = match &self.residual {
                    None => true,
                    Some(expr) => expr.eval_bool(&joined)?,
                };
                if keep {
                    row_matched = true;
                    self.pending.push_back(joined);
                }
            }
            // LEFT OUTER: if the residual rejected every right candidate for this left row,
            // emit it with a NULL-padded right side rather than dropping it entirely.
            if self.join_type == JoinType::LeftOuter && !row_matched {
                self.pending
                    .push_back(l_curr.concat(&null_right_tuple(right_width)));
            }
            self.l_sorted.forward();
        }

        Ok(())
    }

    /// Retrieves the join key value for the current tuple on the specified side (left or right).
    ///
    /// # Arguments
    ///
    /// * `is_left` - If `true`, fetches the value from the left input using the left join key
    ///   column. If `false`, fetches the value from the right input using the right join key
    ///   column.
    ///
    /// # Returns
    ///
    /// Returns a reference to the join key [`Value`] for the current tuple on the chosen side,
    /// or an [`ExecutionError`] if the column index is out of bounds.
    fn get_key(&self, is_left: bool) -> Result<&Value, ExecutionError> {
        let (t, col) = if is_left {
            (&self.l_sorted, self.predicate.left_col)
        } else {
            (&self.r_sorted, self.predicate.right_col)
        };
        get_value(t.current(), col, is_left)
    }
}

/// Produces joined tuples in key order after sorting both inputs.
///
/// This implementation drains and sorts both children on the first call to `next`, then advances
/// the two cursors until it finds equal key runs to emit (via the internal pending queue).
impl FallibleIterator for SortMergeJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.sort_inputs()?;
        let right_width = self.inputs.right_width;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                self.rows_produced += 1;
                return Ok(Some(tuple));
            }

            if self.l_sorted.exhausted() {
                tracing::debug!(rows_produced = self.rows_produced, "smj: merge complete");
                return Ok(None);
            }

            if self.r_sorted.exhausted() {
                // For LEFT OUTER: flush all remaining left rows as NULL-padded into pending,
                // then loop back to drain them. For INNER: we're done.
                if self.join_type == JoinType::LeftOuter {
                    while !self.l_sorted.exhausted() {
                        let l = self.l_sorted.current().clone();
                        self.pending
                            .push_back(l.concat(&null_right_tuple(right_width)));
                        self.l_sorted.forward();
                    }
                    continue;
                }
                tracing::debug!(rows_produced = self.rows_produced, "smj: merge complete");
                return Ok(None);
            }

            let lk = self.get_key(true)?;
            let rk = self.get_key(false)?;

            match lk.partial_cmp(rk) {
                Some(Ordering::Less) => {
                    // Left key is smaller — this left row has no matching right row.
                    // For LEFT OUTER, emit it with a NULL-padded right side.
                    if self.join_type == JoinType::LeftOuter {
                        let l = self.l_sorted.current().clone();
                        self.pending
                            .push_back(l.concat(&null_right_tuple(right_width)));
                    }
                    self.l_sorted.forward();
                }
                Some(Ordering::Greater) => self.r_sorted.forward(),
                Some(Ordering::Equal) => self.collect_equals()?,
                None => return Err(ExecutionError::TypeError("incomparable join keys".into())),
            }
        }
    }
}

impl Executor for SortMergeJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.l_sorted = TupleCursor::new();
        self.r_sorted = TupleCursor::new();
        self.sorted = false;
        self.rows_produced = 0;
        self.inputs.left.rewind()?;
        self.inputs.right.rewind()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_utils::*, SortMergeJoin};

    #[test]
    fn test_smj_basic_equi_join() {
        let left = build_heap(129, &[tup(3, 30), tup(1, 10), tup(2, 20)]);
        let right = build_heap(130, &[tup(2, 200), tup(1, 100), tup(4, 400)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        let keys: Vec<i32> = out.iter().map(|t| int(t, 0)).collect();
        assert_eq!(keys, vec![1, 2]);
    }

    #[test]
    fn test_smj_cartesian_on_duplicate_runs() {
        let left = build_heap(131, &[tup(1, 10), tup(1, 11)]);
        let right = build_heap(132, &[tup(1, 100), tup(1, 101), tup(1, 102)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        assert_eq!(out.len(), 6, "2 left × 3 right = 6 pairs");
        let mut seen: Vec<(i32, i32)> = out.iter().map(|t| (int(t, 1), int(t, 3))).collect();
        seen.sort_unstable();
        let mut expected = vec![
            (10, 100),
            (10, 101),
            (10, 102),
            (11, 100),
            (11, 101),
            (11, 102),
        ];
        expected.sort_unstable();
        assert_eq!(seen, expected);
    }

    #[test]
    fn test_smj_equal_run_to_end_of_right() {
        let left = build_heap(133, &[tup(5, 0), tup(5, 1)]);
        let right = build_heap(134, &[tup(5, 100), tup(5, 101)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 4);
    }

    #[test]
    fn test_smj_disjoint_keys() {
        let left = build_heap(135, &[tup(1, 0), tup(2, 0)]);
        let right = build_heap(136, &[tup(3, 0), tup(4, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_smj_skips_null_keys() {
        let left = build_heap(137, &[tup_null_a(0), tup(1, 1)]);
        let right = build_heap(138, &[tup_null_a(0), tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 1);
    }

    #[test]
    fn test_smj_both_empty() {
        let left = build_heap(139, &[]);
        let right = build_heap(140, &[]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_smj_interleaved_keys() {
        let left = build_heap(141, &[tup(1, 0), tup(3, 0), tup(5, 0)]);
        let right = build_heap(142, &[tup(2, 0), tup(3, 0), tup(4, 0), tup(5, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        let keys: Vec<i32> = out.iter().map(|t| int(t, 0)).collect();
        assert_eq!(keys, vec![3, 5]);
    }

    #[test]
    fn test_smj_rewind_replays() {
        let left = build_heap(143, &[tup(1, 0), tup(2, 0)]);
        let right = build_heap(144, &[tup(1, 0), tup(2, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }

    #[test]
    #[should_panic(expected = "SortMergeJoin requires an equality predicate")]
    fn test_smj_new_panics_on_non_equality() {
        let left = build_heap(145, &[]);
        let right = build_heap(146, &[]);
        let p = JoinPredicate::new(col(0), col(0), Predicate::GreaterThan);
        let _ = SortMergeJoin::new(scan(&left), scan(&right), p);
    }

    #[test]
    fn test_smj_residual_filters_after_key_match() {
        let left = build_heap(149, &[tup(1, 10), tup(1, 50)]);
        let right = build_heap_xy(150, &[tup(1, 20), tup(1, 60)]);
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j =
            SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0)).with_residual(residual);
        assert_eq!(drain(&mut j).len(), 3);
    }

    #[test]
    fn test_smj_left_outer_unmatched_row_gets_nulls() {
        let left = build_heap(240, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap(241, &[tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        let unmatched = out.iter().find(|t| int(t, 0) == 9).unwrap();
        assert!(matches!(unmatched.get(2), Some(Value::Null)));
    }

    #[test]
    fn test_smj_left_outer_null_key_left_row_preserved() {
        let left = build_heap(242, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(243, &[tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        assert_eq!(drain(&mut j).len(), 2);
    }

    #[test]
    fn test_smj_left_outer_empty_right() {
        let left = build_heap(244, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(245, &[]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|t| matches!(t.get(2), Some(Value::Null))));
    }

    #[test]
    fn test_smj_left_outer_right_exhausted_mid_merge() {
        let left = build_heap(246, &[tup(1, 10), tup(5, 50), tup(9, 90)]);
        let right = build_heap(247, &[tup(1, 100), tup(5, 500)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        assert_eq!(drain(&mut j).len(), 3);
    }

    #[test]
    fn test_smj_left_outer_residual_rejects_all_emits_null() {
        let left = build_heap(248, &[tup(1, 999)]);
        let right = build_heap_xy(249, &[tup(1, 1)]);
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter)
            .with_residual(residual);
        let out = drain(&mut j);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].get(2), Some(Value::Null)));
    }

    #[test]
    fn test_smj_left_outer_rewind() {
        let left = build_heap(250, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap(251, &[tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }
}
