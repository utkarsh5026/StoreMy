//! Equality hash-join executor.
//!
//! [`HashJoin`] builds a hash table on the right child's join key, then probes it once per left
//! tuple. Use it when the join condition includes an equality between keys (e.g. `l.a = r.x`).
//!
//! An optional [`HashJoin::with_residual`] expression can filter matches after the key probe —
//! for example `l.a = r.x AND l.b < r.y`, where equality is the hash key and the comparison is
//! the residual.
//!
//! Only [`Predicate::Equals`] is supported as the hash key; other operators must use
//! [`super::NestedLoopJoin`] or [`super::SortMergeJoin`].

use std::collections::{HashMap, VecDeque};

use fallible_iterator::FallibleIterator;

use super::{JoinInputs, JoinPredicate, JoinType, null_right_tuple};
use crate::{
    Value,
    execution::{ExecutionError, Executor, PlanNode, ResolvedExpr, eval_resolved_bool},
    primitives::Predicate,
    tuple::{Tuple, TupleSchema},
};

/// Joins two inputs by hashing the right side on an equality join key.
///
/// The right input is scanned once to populate `hash_table`. Each left tuple looks up its key in
/// the table; every right tuple in the matching bucket is a candidate output row (subject to an
/// optional residual predicate).
///
/// # SQL shape
///
/// ```sql
/// SELECT *
/// FROM left_table  AS l
/// JOIN right_table AS r
///   ON l.a = r.x;
/// ```
///
/// Compound conditions split key vs residual:
///
/// ```sql
/// -- l.a = r.x  → hash key via [`JoinPredicate`]
/// -- l.b < r.y  → [`HashJoin::with_residual`]
/// SELECT * FROM l JOIN r ON l.a = r.x AND l.b < r.y;
/// ```
///
/// # Memory and output order
///
/// - **Memory**: \(O(|R|)\) for the hash table (each bucket stores cloned right tuples).
/// - **Output order**: left-input order; within a left row, matches follow bucket order (right scan
///   order at build time).
///
/// # NULL keys
///
/// Right tuples whose join key is [`Value::Null`] or missing are dropped during hash build. Left
/// tuples with a `NULL` or missing key never probe a bucket; for [`JoinType::LeftOuter`] they
/// still produce one output row with a `NULL`-padded right side.
#[derive(Debug)]
pub struct HashJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    predicate: JoinPredicate,
    join_type: JoinType,
    residual: Option<ResolvedExpr>,
    hash_table: Option<HashMap<Value, Vec<Tuple>>>,
    bucket_matches: VecDeque<Tuple>,
    left_tuple: Option<Tuple>,
    /// Whether the current left row has emitted at least one joined tuple.
    ///
    /// Used for [`JoinType::LeftOuter`] when every hash-key match is rejected by the residual:
    /// the left row must still appear once with a `NULL`-padded right side.
    left_matched: bool,
}

impl<'a> HashJoin<'a> {
    /// Creates a hash join for `left ⋈ right` keyed on `predicate`.
    ///
    /// `predicate` names the left and right key columns and must use
    /// [`Predicate::Equals`]. The right child is not read until the first
    /// [`FallibleIterator::next`] call.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let pred = JoinPredicate::new(left_col, right_col, Predicate::Equals);
    /// let mut join = HashJoin::new(left_plan, right_plan, pred);
    /// while let Some(tuple) = join.next()? {
    ///     // ...
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `predicate.op` is not [`Predicate::Equals`].
    #[tracing::instrument(skip_all, fields(op = "hash_join"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        assert_eq!(
            predicate.op,
            Predicate::Equals,
            "HashJoin requires an equality predicate, got {}",
            predicate.op
        );

        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            predicate,
            join_type: JoinType::Inner,
            residual: None,
            hash_table: None,
            bucket_matches: VecDeque::new(),
            left_tuple: None,
            left_matched: false,
        }
    }

    /// Sets how unmatched left rows are handled.
    ///
    /// Use [`JoinType::LeftOuter`] for SQL `LEFT JOIN`: every left row appears at least once,
    /// with `NULL` right columns when there is no key match (or no residual match).
    #[must_use]
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Adds a filter evaluated on each key-matched `left.concat(right)` pair.
    ///
    /// The residual runs only after a successful hash probe. Column references in the expression
    /// must use the concatenated layout: right-side columns are offset by
    /// [`left_width`](Self::left_width).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // ON l.a = r.x AND l.b < r.y
    /// HashJoin::new(left, right, eq_predicate)
    ///     .with_residual(/* l.b < r.y as ResolvedExpr */);
    /// ```
    #[must_use]
    pub fn with_residual(mut self, residual: ResolvedExpr) -> Self {
        self.residual = Some(residual);
        self
    }

    /// Returns the number of columns in the left child.
    ///
    /// Right-side fields start at this index in concatenated output tuples — important when
    /// building a [`with_residual`](Self::with_residual) expression.
    #[inline]
    pub fn left_width(&self) -> usize {
        self.inputs.left_width
    }

    /// Scans the right child and inserts each tuple into `hash_table` keyed by the join column.
    ///
    /// Tuples whose key is [`Value::Null`] or missing are skipped. Runs at most once per
    /// executor.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError`] if reading any tuple from the right input fails.
    fn build_hash_table(&mut self) -> Result<(), ExecutionError> {
        if self.hash_table.is_some() {
            return Ok(());
        }

        let right_idx = usize::from(self.predicate.right_col);
        let mut null_keys = 0usize;
        let mut entries = 0usize;
        let mut table: HashMap<Value, Vec<Tuple>> = HashMap::new();

        while let Some(t) = self.inputs.right.next()? {
            match t.get(right_idx) {
                Some(Value::Null) | None => {
                    null_keys += 1;
                }
                Some(v) => {
                    let key = v.clone();
                    table.entry(key).or_default().push(t);
                    entries += 1;
                }
            }
        }
        tracing::debug!(entries, null_keys, "hash_join: table built");
        self.hash_table = Some(table);
        Ok(())
    }

    /// Emits the next joined tuple from `bucket_matches` for the current left row.
    ///
    /// Pops right-side candidates enqueued by [`begin_left_probe`], concatenates each with
    /// `left_tuple`, and applies the optional residual. Returns the first candidate that passes;
    /// rejected candidates are dropped and the next candidate is tried.
    ///
    /// Sets [`HashJoin::left_matched`] to `true` when a tuple is returned, so
    /// [`JoinType::LeftOuter`] logic in [`FallibleIterator::next`] knows not to emit a
    /// `NULL`-padded row for this left key.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError`] if evaluating the residual expression fails.
    ///
    /// # Panics
    ///
    /// Panics if `bucket_matches` is non-empty but `left_tuple` is `None` (internal invariant
    /// violation — [`begin_left_probe`] must set `left_tuple` before enqueueing matches).
    fn drain_bucket_matches(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(r) = self.bucket_matches.pop_front() {
            let l = self
                .left_tuple
                .as_ref()
                .expect("current_left set with pending");
            let joined = l.concat(&r);
            let keep = match &self.residual {
                None => true,
                Some(expr) => eval_resolved_bool(expr, &joined)?,
            };
            if keep {
                self.left_matched = true;
                return Ok(Some(joined));
            }
        }
        Ok(None)
    }

    /// Starts probing one left row: hash lookup and loading `bucket_matches`.
    ///
    /// Returns `Some(tuple)` when a [`JoinType::LeftOuter`] row must be emitted immediately
    /// (NULL/missing key or no bucket match). Returns `None` when the caller should keep
    /// looping — either the row was skipped (inner join) or matches were enqueued for
    /// [`drain_bucket_matches`].
    fn begin_left_probe(&mut self, l: Tuple, left_idx: usize, right_width: usize) -> Option<Tuple> {
        let key = match l.get(left_idx) {
            Some(Value::Null) | None => {
                if self.join_type == JoinType::LeftOuter {
                    return Some(l.concat(&null_right_tuple(right_width)));
                }
                return None;
            }
            Some(v) => v.clone(),
        };

        match self.hash_table.as_ref().unwrap().get(&key) {
            Some(bucket) if !bucket.is_empty() => {
                tracing::trace!(matches = bucket.len(), "hash_join: probe hit");
                self.bucket_matches.extend(bucket.iter().cloned());
                self.left_matched = false;
                self.left_tuple = Some(l);
            }
            _ => {
                if self.join_type == JoinType::LeftOuter {
                    return Some(l.concat(&null_right_tuple(right_width)));
                }
            }
        }
        None
    }
}

/// Produces joined tuples by probing the built hash table for each left key.
///
/// On the first `next` call the right side is hashed. For each left tuple:
///
/// 1. Look up the key in `hash_table` and enqueue matching right tuples in `pending`.
/// 2. Pop candidates, apply the optional residual, and return those that pass.
/// 3. For [`JoinType::LeftOuter`], if no row was emitted for this left tuple, return
///    `left.concat(null_right)`.
///
/// If `pending` is non-empty when `next` is called, those rows are returned before advancing
/// the left input.
///
/// # Errors
///
/// Returns [`ExecutionError`] if building the hash table, reading the left input, or evaluating
/// the residual expression fails.
impl FallibleIterator for HashJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.build_hash_table()?;
        let left_idx = usize::from(self.predicate.left_col);
        let right_width = self.inputs.right_width;

        loop {
            if let Some(tuple) = self.drain_bucket_matches()? {
                return Ok(Some(tuple));
            }

            // pending is exhausted. For LEFT OUTER: if this left row produced no output
            // (all candidates were rejected by the residual), emit a NULL-padded row.
            if self.join_type == JoinType::LeftOuter
                && let Some(l) = self.left_tuple.take()
                && !self.left_matched
            {
                return Ok(Some(l.concat(&null_right_tuple(right_width))));
            }

            // when the left input is exhausted, we return None.
            let Some(l) = self.inputs.left.next()? else {
                self.left_tuple = None;
                return Ok(None);
            };

            if let Some(tuple) = self.begin_left_probe(l, left_idx, right_width) {
                return Ok(Some(tuple));
            }
        }
    }
}

impl Executor for HashJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    /// Clears probe state and rewinds only the left child.
    ///
    /// The built hash table is kept, so a second scan does not re-read the right input.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError`] if rewinding the left child fails.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.bucket_matches.clear();
        self.left_tuple = None;
        self.left_matched = false;
        self.inputs.left.rewind()
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_utils::*, HashJoin};

    #[test]
    fn test_hash_join_basic_match() {
        let left = build_heap(117, &[tup(1, 10), tup(2, 20), tup(3, 30)]);
        let right = build_heap(118, &[tup(2, 200), tup(3, 300), tup(9, 999)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        let mut keys: Vec<i32> = out.iter().map(|t| int(t, 0)).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![2, 3]);
    }

    #[test]
    fn test_hash_join_multiple_right_per_key() {
        let left = build_heap(119, &[tup(1, 10)]);
        let right = build_heap(120, &[tup(1, 100), tup(1, 101), tup(1, 102)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 3);
    }

    #[test]
    fn test_hash_join_skips_null_keys() {
        let left = build_heap(121, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(122, &[tup_null_a(100), tup(1, 101)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 1);
    }

    #[test]
    fn test_hash_join_empty_right() {
        let left = build_heap(123, &[tup(1, 10)]);
        let right = build_heap(124, &[]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_hash_join_rewind_replays() {
        let left = build_heap(125, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(126, &[tup(1, 100), tup(2, 200)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }

    #[test]
    #[should_panic(expected = "HashJoin requires an equality predicate")]
    fn test_hash_join_new_panics_on_non_equality() {
        let left = build_heap(127, &[]);
        let right = build_heap(128, &[]);
        let p = JoinPredicate::new(col(0), col(0), Predicate::LessThan);
        let _ = HashJoin::new(scan(&left), scan(&right), p);
    }

    #[test]
    fn test_hash_join_residual_filters_after_key_match() {
        let left = build_heap(147, &[tup(1, 10), tup(1, 50)]);
        let right = build_heap_xy(148, &[tup(1, 20), tup(1, 60)]);
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0)).with_residual(residual);
        assert_eq!(drain(&mut j).len(), 3);
    }

    #[test]
    fn test_hash_left_outer_unmatched_row_gets_nulls() {
        let left = build_heap(230, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap(231, &[tup(1, 100)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        let unmatched = out.iter().find(|t| int(t, 0) == 9).unwrap();
        assert!(matches!(unmatched.get(2), Some(Value::Null)));
    }

    #[test]
    fn test_hash_left_outer_null_key_left_row_preserved() {
        let left = build_heap(232, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(233, &[tup(1, 100)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        assert_eq!(drain(&mut j).len(), 2);
    }

    #[test]
    fn test_hash_left_outer_empty_right() {
        let left = build_heap(234, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(235, &[]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|t| matches!(t.get(2), Some(Value::Null))));
    }

    #[test]
    fn test_hash_left_outer_residual_rejects_all_emits_null() {
        let left = build_heap(236, &[tup(1, 999)]);
        let right = build_heap_xy(237, &[tup(1, 1)]);
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter)
            .with_residual(residual);
        let out = drain(&mut j);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].get(2), Some(Value::Null)));
    }

    #[test]
    fn test_hash_left_outer_rewind() {
        let left = build_heap(238, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap(239, &[tup(1, 100)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }
}
