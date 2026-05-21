use std::{cmp::Ordering, collections::VecDeque};

use fallible_iterator::FallibleIterator;

use super::{JoinInputs, JoinPredicate, JoinType, get_value, null_right_tuple};
use crate::{
    Value,
    execution::{ExecutionError, Executor, PlanNode, ResolvedExpr, TupleCursor},
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
        self.inputs.join_type = join_type;
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

        // LeftOuter and FullOuter must preserve NULL-key left rows — every left row must appear
        // in the output regardless of its join key.
        if matches!(
            self.inputs.join_type,
            JoinType::LeftOuter | JoinType::FullOuter
        ) {
            drain_all_tuples(&mut self.inputs.left, &mut self.l_sorted.tuples)?;
        } else {
            drain_tuples(&mut self.inputs.left, &mut self.l_sorted.tuples, left_idx)?;
        }
        // FullOuter must also preserve NULL-key right rows.
        if self.inputs.join_type == JoinType::FullOuter {
            drain_all_tuples(&mut self.inputs.right, &mut self.r_sorted.tuples)?;
        } else {
            drain_tuples(&mut self.inputs.right, &mut self.r_sorted.tuples, right_idx)?;
        }
        tracing::debug!(
            left = self.l_sorted.tuples.len(),
            right = self.r_sorted.tuples.len(),
            "smj: inputs drained, sorting"
        );

        Self::sort_by_column(&mut self.l_sorted.tuples, left_idx);
        Self::sort_by_column(&mut self.r_sorted.tuples, right_idx);

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
                let joined = l_curr.concat(&self.r_sorted[i]);
                let keep = match &self.residual {
                    None => true,
                    Some(expr) => expr.eval_bool(&joined)?,
                };
                if keep {
                    row_matched = true;
                    self.pending.push_back(joined);
                }
            }
            // LeftOuter/FullOuter: if the residual rejected every right candidate for this left
            // row, emit it with a NULL-padded right side rather than dropping it entirely.
            if matches!(
                self.inputs.join_type,
                JoinType::LeftOuter | JoinType::FullOuter
            ) && !row_matched
            {
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

    /// Retrieves the current join key values for both the left and right inputs.
    ///
    /// This function fetches the value from the left tuple using the left join key column,
    /// and the value from the right tuple using the right join key column. These are typically
    /// used to compare and advance the cursors in a sort-merge join implementation.
    ///
    /// # Returns
    ///
    /// On success, returns a tuple of references to the left and right join key [`Value`]s,
    /// respectively. Returns an [`ExecutionError`] if either key column is out of bounds.
    fn current_join_key_values(&self) -> Result<(&Value, &Value), ExecutionError> {
        let lk = get_value(self.l_sorted.current(), self.predicate.left_col, true)?;
        let rk = get_value(self.r_sorted.current(), self.predicate.right_col, false)?;
        Ok((lk, rk))
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
        let left_width = self.inputs.left_width;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                self.rows_produced += 1;
                return Ok(Some(tuple));
            }

            if self.l_sorted.exhausted() {
                if self.inputs.join_type != JoinType::FullOuter {
                    tracing::debug!(rows_produced = self.rows_produced, "smj: merge complete");
                    return Ok(None);
                }

                while !self.r_sorted.exhausted() {
                    let r = self.r_sorted.current().clone();
                    self.pending
                        .push_back(null_right_tuple(left_width).concat(&r));
                    self.r_sorted.forward();
                }
                // If right was already exhausted, pending is empty and we're done.
                if self.pending.is_empty() {
                    tracing::debug!(rows_produced = self.rows_produced, "smj: merge complete");
                    return Ok(None);
                }
                continue;
            }

            if self.r_sorted.exhausted() {
                // For LEFT OUTER / FULL OUTER: flush all remaining left rows as NULL-padded into
                // pending, then loop back to drain them. For INNER: we're done.
                if matches!(
                    self.inputs.join_type,
                    JoinType::LeftOuter | JoinType::FullOuter
                ) {
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

            let (lk, rk) = self.current_join_key_values()?;

            match lk.partial_cmp(rk) {
                Some(Ordering::Less) => {
                    // Left key is smaller — this left row has no matching right row.
                    if matches!(
                        self.inputs.join_type,
                        JoinType::LeftOuter | JoinType::FullOuter
                    ) {
                        let l = self.l_sorted.current().clone();
                        self.pending
                            .push_back(l.concat(&null_right_tuple(right_width)));
                    }
                    self.l_sorted.forward();
                }
                Some(Ordering::Greater) => {
                    // Right key is smaller — this right row has no matching left row.
                    if self.inputs.join_type == JoinType::FullOuter {
                        let r = self.r_sorted.current().clone();
                        self.pending
                            .push_back(null_right_tuple(left_width).concat(&r));
                    }
                    self.r_sorted.forward();
                }
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

    // ── FULL OUTER JOIN ───────────────────────────────────────────────────────

    // Exercises the Greater branch (unmatched right row) and the Less branch
    // (unmatched left row) in the same run.
    //
    // left:  [(1,10), (3,30)]   right: [(2,200), (3,300)]
    // sorted merge:
    //   L=1 < R=2 → emit (1,10,NULL,NULL)
    //   L=3 > R=2 → emit (NULL,NULL,2,200)
    //   L=3 = R=3 → emit (3,30,3,300)
    #[test]
    fn test_smj_full_outer_unmatched_on_both_sides() {
        let left = build_heap(260, &[tup(1, 10), tup(3, 30)]);
        let right = build_heap(261, &[tup(2, 200), tup(3, 300)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 3);

        let is_int = |t: &&Tuple, col: usize, val: i32| matches!(t.get(col), Some(Value::Int32(v)) if *v == val);

        let matched = out.iter().find(|t| is_int(t, 0, 3)).unwrap();
        assert_eq!(int(matched, 2), 3);

        let left_only = out.iter().find(|t| is_int(t, 0, 1)).unwrap();
        assert!(matches!(left_only.get(2), Some(Value::Null)));

        let right_only = out.iter().find(|t| is_int(t, 2, 2)).unwrap();
        assert!(matches!(right_only.get(0), Some(Value::Null)));
    }

    // Exercises the l_sorted.exhausted() → drain remaining right branch.
    #[test]
    fn test_smj_full_outer_empty_left() {
        let left = build_heap(262, &[]);
        let right = build_heap(263, &[tup(1, 100), tup(2, 200)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2, "both right rows must appear with null left");
        assert!(out.iter().all(|t| matches!(t.get(0), Some(Value::Null))));
    }

    // Exercises the r_sorted.exhausted() → flush remaining left branch.
    #[test]
    fn test_smj_full_outer_empty_right() {
        let left = build_heap(264, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(265, &[]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2, "both left rows must appear with null right");
        assert!(out.iter().all(|t| matches!(t.get(2), Some(Value::Null))));
    }

    // All keys disjoint: every row from both sides must appear, none matched.
    // Exercises the Greater branch (drains all right rows) then the
    // r_sorted.exhausted() flush (drains remaining left rows).
    #[test]
    fn test_smj_full_outer_disjoint_keys() {
        let left = build_heap(266, &[tup(1, 0), tup(2, 0)]);
        let right = build_heap(267, &[tup(3, 0), tup(4, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 4);
        let left_only = out
            .iter()
            .filter(|t| matches!(t.get(2), Some(Value::Null)))
            .count();
        let right_only = out
            .iter()
            .filter(|t| matches!(t.get(0), Some(Value::Null)))
            .count();
        assert_eq!(left_only, 2);
        assert_eq!(right_only, 2);
    }

    // Exercises sort_inputs NULL preservation for the left side.
    #[test]
    fn test_smj_full_outer_null_key_left_row_preserved() {
        let left = build_heap(268, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(269, &[tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2, "null-key left row + matched row");
        // null-key left row: both left key and right key are null
        let null_key_row = out
            .iter()
            .find(|t| {
                matches!(t.get(0), Some(Value::Null)) && matches!(t.get(2), Some(Value::Null))
            })
            .unwrap();
        assert_eq!(int(null_key_row, 1), 10);
    }

    // Exercises sort_inputs NULL preservation for the right side.
    #[test]
    fn test_smj_full_outer_null_key_right_row_preserved() {
        let left = build_heap(270, &[tup(1, 10)]);
        let right = build_heap_xy(271, &[tup_null_a(99), tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2, "matched row + null-key right row");
        // null-key right row: left columns are null, right.y = 99
        let null_key_row = out
            .iter()
            .find(|t| {
                matches!(t.get(0), Some(Value::Null)) && matches!(t.get(2), Some(Value::Null))
            })
            .unwrap();
        assert_eq!(int(null_key_row, 3), 99);
    }

    #[test]
    fn test_smj_full_outer_rewind() {
        let left = build_heap(272, &[tup(1, 10), tup(3, 30)]);
        let right = build_heap(273, &[tup(2, 200), tup(3, 300)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter);
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 3);
        assert_eq!(first, second);
    }

    // Exercises collect_equals residual-rejection with FullOuter: keys match but
    // residual rejects all candidates → left row must still appear with null right.
    #[test]
    fn test_smj_full_outer_residual_rejects_all_emits_null_right() {
        let left = build_heap(274, &[tup(1, 999)]);
        let right = build_heap_xy(275, &[tup(1, 1)]);
        let residual = nlj_col_expr(1, BinOp::Lt, 3); // 999 < 1 is false
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::FullOuter)
            .with_residual(residual);
        let out = drain(&mut j);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].get(2), Some(Value::Null)));
        assert_eq!(int(&out[0], 0), 1);
    }
}
