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

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    ops::{Index, IndexMut},
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
}

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor};
use crate::{
    Value,
    execution::{PlanNode, ResolvedExpr, eval_resolved_bool},
    primitives::{ColumnId, Predicate},
    tuple::{Tuple, TupleSchema},
};

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
fn null_right_tuple(n: usize) -> Tuple {
    Tuple::new(vec![Value::Null; n])
}

/// Fetches a value by column id with a side-specific error message.
///
/// This helper returns a reference into `tuple`, so the output lifetime is tied to the input
/// borrow of `tuple` (via `<'a>`).
#[inline]
fn get_value(tuple: &Tuple, col: ColumnId, is_left: bool) -> Result<&Value, ExecutionError> {
    tuple.value_at(col).map_err(|_| {
        ExecutionError::TypeError(format!(
            "{} col {col} out of bounds",
            if is_left { "left" } else { "right" }
        ))
    })
}

/// Internal container for a pair of child inputs and their merged output schema.
///
/// Used by every join executor as the common "two children + output schema" bundle.
/// The join-specific matching logic (`JoinPredicate`, `Expr` residual,
/// etc.) lives on each executor itself.
///
/// Not exposed outside the execution module.
#[derive(Debug)]
struct JoinInputs<'a> {
    left: PlanNode<'a>,
    right: PlanNode<'a>,
    schema: TupleSchema,
    /// Number of columns in the left child's output.
    left_width: usize,
    /// Number of columns in the right child's output.
    right_width: usize,
}

impl<'a> JoinInputs<'a> {
    /// Constructs a new `JoinInputs` by merging the two children's schemas.
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>) -> Self {
        let left_width = left.schema().physical_num_fields();
        let right_width = right.schema().physical_num_fields();
        let schema = left.schema().merge(right.schema());
        Self {
            left,
            right,
            schema,
            left_width,
            right_width,
        }
    }
}

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
    right_buf: Vec<Tuple>,
    materialized: bool,
    pending: VecDeque<Tuple>,
}

impl<'a> NestedLoopJoin<'a> {
    /// Creates a nested-loop join executor for `left ⋈ right` using `predicate`.
    ///
    /// `predicate` must have all column references pre-resolved to [`ColumnId`]s.
    /// It is evaluated over the concatenated `left.concat(right)` tuple.
    ///
    /// The right input is read and buffered on the first call to [`FallibleIterator::next`].
    #[tracing::instrument(skip_all, fields(op = "nlj"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: ResolvedExpr) -> Self {
        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            predicate,
            join_type: JoinType::Inner,
            right_buf: Vec::new(),
            materialized: false,
            pending: VecDeque::new(),
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
        if self.materialized {
            return Ok(());
        }
        let right = &mut self.inputs.right;
        while let Some(tuple) = right.next()? {
            self.right_buf.push(tuple);
        }
        tracing::debug!(tuples = self.right_buf.len(), "nlj: right side buffered");
        self.materialized = true;
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
/// - If `pending` is non-empty when `next` is called, it emits these join results before advancing
///   the left input.
impl FallibleIterator for NestedLoopJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_right()?;
        let right_width = self.inputs.right_width;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                return Ok(Some(tuple));
            }

            let Some(l) = self.inputs.left.next()? else {
                return Ok(None);
            };

            for right in &self.right_buf {
                let joined = l.concat(right);
                if eval_resolved_bool(&self.predicate, &joined)? {
                    self.pending.push_back(joined);
                }
            }

            // LEFT OUTER: if this left row matched nothing, emit it with a NULL-padded right side.
            if self.join_type == JoinType::LeftOuter && self.pending.is_empty() {
                self.pending
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
        self.pending.clear();
        self.inputs.left.rewind()
    }
}

/// Produces the Cartesian product of two inputs — every left row paired with every right row,
/// with no predicate filtering.
///
/// The right input is materialized once on the first call to `next`. Each subsequent left
/// tuple fans out to `|right|` output rows unconditionally.
///
/// # SQL shape
///
/// ```sql
/// SELECT * FROM left_table CROSS JOIN right_table;
/// ```
#[derive(Debug)]
pub struct CrossJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    right_buf: Vec<Tuple>,
    materialized: bool,
    pending: VecDeque<Tuple>,
}

impl<'a> CrossJoin<'a> {
    /// Creates a cross-join executor for `left × right`.
    #[tracing::instrument(skip_all, fields(op = "cross_join"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>) -> Self {
        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            right_buf: Vec::new(),
            materialized: false,
            pending: VecDeque::new(),
        }
    }

    fn materialize_right(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }
        let right = &mut self.inputs.right;
        while let Some(tuple) = right.next()? {
            self.right_buf.push(tuple);
        }
        tracing::debug!(
            tuples = self.right_buf.len(),
            "cross_join: right side buffered"
        );
        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for CrossJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_right()?;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                return Ok(Some(tuple));
            }

            let Some(l) = self.inputs.left.next()? else {
                return Ok(None);
            };

            for right in &self.right_buf {
                self.pending.push_back(l.concat(right));
            }
        }
    }
}

impl Executor for CrossJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.inputs.left.rewind()
    }
}

#[derive(Debug)]
/// Joins two inputs by building a hash table on the right input.
///
/// Requires an equality predicate as the hash key. An optional `residual`
/// An `Expr` residual evaluated over `left.concat(right)` can further restrict
/// matches after the key probe — useful for compound join conditions like
/// `l.a = r.x AND l.b < r.y`, where `l.a = r.x` is the hash key and `l.b < r.y`
/// is the residual.
pub struct HashJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    predicate: JoinPredicate,
    join_type: JoinType,
    residual: Option<ResolvedExpr>,
    hash_table: HashMap<Value, Vec<Tuple>>,
    pending: VecDeque<Tuple>,
    materialized: bool,
    left_tuple: Option<Tuple>,
    /// Tracks whether the current left row has produced at least one output tuple.
    /// Used by `LEFT OUTER JOIN` to decide whether to emit a NULL-padded row.
    left_matched: bool,
}

impl<'a> HashJoin<'a> {
    /// Creates a hash join executor for `left ⋈ right` using `predicate` as the
    /// equality key.
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
            hash_table: HashMap::new(),
            pending: VecDeque::new(),
            materialized: false,
            left_tuple: None,
            left_matched: false,
        }
    }

    /// Sets the join type. Use [`JoinType::LeftOuter`] to get `LEFT JOIN` semantics.
    #[must_use]
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Attaches an optional residual predicate to this hash join.
    ///
    /// The residual is evaluated over the concatenated `left ⋈ right` tuple *after* the
    /// hash-key match succeeds, so right-side column references must be offset by
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

    /// Materializes the hash table for the right input using the join key.
    ///
    /// Tuples where the join key is `NULL` or missing are skipped (won't match).
    fn build_hash_table(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        let right_idx = usize::from(self.predicate.right_col);
        let mut null_keys: usize = 0;
        while let Some(t) = self.inputs.right.next()? {
            match t.get(right_idx) {
                Some(Value::Null) | None => {
                    null_keys += 1;
                }
                Some(v) => {
                    let key = v.clone();
                    self.hash_table.entry(key).or_default().push(t);
                }
            }
        }
        let entries: usize = self.hash_table.values().map(Vec::len).sum();
        tracing::debug!(entries, null_keys, "hash_join: table built");
        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for HashJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.build_hash_table()?;
        let left_idx = usize::from(self.predicate.left_col);
        let right_width = self.inputs.right_width;

        loop {
            // Drain pending right-side candidates for the current left tuple.
            while let Some(r) = self.pending.pop_front() {
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

            // pending is exhausted. For LEFT OUTER: if this left row produced no output
            // (all candidates were rejected by the residual), emit a NULL-padded row.
            if self.join_type == JoinType::LeftOuter
                && let Some(l) = self.left_tuple.take()
                && !self.left_matched
            {
                return Ok(Some(l.concat(&null_right_tuple(right_width))));
            }

            let Some(l) = self.inputs.left.next()? else {
                self.left_tuple = None;
                return Ok(None);
            };

            let key = match l.get(left_idx) {
                Some(Value::Null) | None => {
                    // NULL key can never match. LEFT OUTER must still emit the left row.
                    if self.join_type == JoinType::LeftOuter {
                        return Ok(Some(l.concat(&null_right_tuple(right_width))));
                    }
                    continue;
                }
                Some(v) => v.clone(),
            };

            match self.hash_table.get(&key) {
                Some(bucket) if !bucket.is_empty() => {
                    tracing::trace!(matches = bucket.len(), "hash_join: probe hit");
                    self.pending.extend(bucket.iter().cloned());
                    self.left_matched = false;
                    self.left_tuple = Some(l);
                }
                _ => {
                    // No match in hash table. LEFT OUTER emits a NULL-padded row.
                    if self.join_type == JoinType::LeftOuter {
                        return Ok(Some(l.concat(&null_right_tuple(right_width))));
                    }
                }
            }
        }
    }
}

impl Executor for HashJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.left_tuple = None;
        self.left_matched = false;
        self.inputs.left.rewind()
    }
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

#[derive(Debug)]
/// Joins two inputs by sorting them on the join key and then merging.
///
/// Requires an equality predicate as the merge key. An optional `residual`
/// An `Expr` residual evaluated over `left.concat(right)` can further restrict
/// matches after the key match succeeds.
///
/// # SQL shape
///
/// Sort-merge join is also for equi-joins (an `=` key). It’s a good fit when inputs are
/// already ordered on the join key or can be efficiently sorted:
///
/// ```sql
/// SELECT *
/// FROM left_table  AS l
/// JOIN right_table AS r
///   ON l.a = r.x;
/// ```
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
        let curr = self.get_value(true)?.clone();
        let right_width = self.inputs.right_width;

        let right_start = self.r_sorted.current_idx();
        while !self.r_sorted.exhausted() && self.get_value(false)? == &curr {
            self.r_sorted.forward();
        }
        let right_end = self.r_sorted.current_idx();

        while !self.l_sorted.exhausted() && self.get_value(true)? == &curr {
            let l_curr = self.l_sorted.current().clone();
            let mut row_matched = false;
            for i in right_start..right_end {
                let r = &self.r_sorted[i];
                let joined = l_curr.concat(r);

                let keep = match &self.residual {
                    None => true,
                    Some(expr) => eval_resolved_bool(expr, &joined)?,
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
    fn get_value(&self, is_left: bool) -> Result<&Value, ExecutionError> {
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

            let lk = self.get_value(true)?;
            let rk = self.get_value(false)?;

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
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        execution::{ResolvedExpr, scan::SeqScan},
        heap::file::HeapFile,
        parser::statements::BinOp,
        primitives::ColumnId,
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn schema_ab() -> TupleSchema {
        TupleSchema::new(vec![field("a", Type::Int32), field("b", Type::Int32)])
    }

    // Right-side schema for NLJ tests — distinct names avoid ambiguity in the merged schema.
    fn schema_xy() -> TupleSchema {
        TupleSchema::new(vec![field("x", Type::Int32), field("y", Type::Int32)])
    }

    fn tup(a: i32, b: i32) -> Tuple {
        Tuple::new(vec![Value::Int32(a), Value::Int32(b)])
    }

    fn tup_null_a(b: i32) -> Tuple {
        Tuple::new(vec![Value::Null, Value::Int32(b)])
    }

    struct HeapHarness {
        heap: HeapFile,
        #[allow(dead_code)]
        wal: Arc<Wal>,
        _dir: tempfile::TempDir,
        txn: TransactionId,
    }

    fn build_heap(id: u64, tuples: &[Tuple]) -> HeapHarness {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join(format!("w{id}.wal")), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let file_id = FileId::new(id);
        let path = dir.path().join(format!("h{id}.db"));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len((4 * crate::storage::PAGE_SIZE) as u64)
            .unwrap();
        drop(file);
        store.register_file(file_id, &path).unwrap();

        let heap = HeapFile::new(
            file_id,
            schema_ab(),
            Arc::clone(&store),
            0,
            Arc::clone(&wal),
        );

        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        for t in tuples {
            heap.insert_tuple(txn, t).unwrap();
        }
        HeapHarness {
            heap,
            wal,
            _dir: dir,
            txn,
        }
    }

    // Heap with schema_xy() — used for NLJ right-side to avoid name clashes with schema_ab().
    fn build_heap_xy(id: u64, tuples: &[Tuple]) -> HeapHarness {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join(format!("w{id}.wal")), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));
        let file_id = FileId::new(id);
        let path = dir.path().join(format!("h{id}.db"));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len((4 * crate::storage::PAGE_SIZE) as u64)
            .unwrap();
        drop(file);
        store.register_file(file_id, &path).unwrap();
        let heap = HeapFile::new(
            file_id,
            schema_xy(),
            Arc::clone(&store),
            0,
            Arc::clone(&wal),
        );
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        for t in tuples {
            heap.insert_tuple(txn, t).unwrap();
        }
        HeapHarness {
            heap,
            wal,
            _dir: dir,
            txn,
        }
    }

    fn scan(h: &HeapHarness) -> PlanNode<'_> {
        PlanNode::SeqScan(SeqScan::new(&h.heap, h.txn))
    }

    fn col(n: u32) -> ColumnId {
        ColumnId::new(n).unwrap()
    }

    fn eq_pred(l: u32, r: u32) -> JoinPredicate {
        JoinPredicate::new(col(l), col(r), Predicate::Equals)
    }

    // Build a ResolvedExpr for NLJ over the concatenated (schema_ab ⋈ schema_xy) tuple.
    // Left columns: a=0, b=1. Right columns in concat output: x=2, y=3.
    fn nlj_col_expr(left_id: u32, op: BinOp, right_id: u32) -> ResolvedExpr {
        ResolvedExpr::BinaryOp {
            lhs: Box::new(ResolvedExpr::Column(ColumnId::new(left_id).unwrap())),
            op,
            rhs: Box::new(ResolvedExpr::Column(ColumnId::new(right_id).unwrap())),
        }
    }

    // Most NLJ tests: left.a = right.x (a=0, x=2 in the concat output).
    fn nlj_eq_0_0_w2() -> ResolvedExpr {
        nlj_col_expr(0, BinOp::Eq, 2)
    }

    fn drain<I: FallibleIterator<Item = Tuple, Error = ExecutionError>>(
        iter: &mut I,
    ) -> Vec<Tuple> {
        let mut out = Vec::new();
        while let Some(t) = iter.next().unwrap() {
            out.push(t);
        }
        out
    }

    fn int(t: &Tuple, i: usize) -> i32 {
        match t.get(i) {
            Some(Value::Int32(v)) => *v,
            other => panic!("expected Int32 at {i}, got {other:?}"),
        }
    }

    // happy path: equality
    #[test]
    fn test_predicate_equals_match() {
        let p = eq_pred(0, 0);
        assert!(p.evaluate(&tup(1, 9), &tup(1, 8)).unwrap());
        assert!(!p.evaluate(&tup(1, 9), &tup(2, 8)).unwrap());
    }

    // both NotEqual variants behave the same
    #[test]
    fn test_predicate_not_equal_variants() {
        for op in [Predicate::NotEqual, Predicate::NotEqualBracket] {
            let p = JoinPredicate::new(col(0), col(0), op);
            assert!(p.evaluate(&tup(1, 0), &tup(2, 0)).unwrap());
            assert!(!p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap());
        }
    }

    // ordered predicates
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

    // NULL on either side short-circuits to false
    #[test]
    fn test_predicate_null_never_matches() {
        let p = eq_pred(0, 0);
        let l = tup_null_a(0);
        let r = tup(1, 0);
        assert!(!p.evaluate(&l, &r).unwrap());
        assert!(!p.evaluate(&r, &l).unwrap());
        assert!(!p.evaluate(&l, &l).unwrap());
    }

    // Like is unimplemented and always returns false
    #[test]
    fn test_predicate_like_always_false() {
        let p = JoinPredicate::new(col(0), col(0), Predicate::Like);
        assert!(!p.evaluate(&tup(1, 0), &tup(1, 0)).unwrap());
    }

    // error paths on column out of bounds
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

    // ===== NestedLoopJoin =====

    // happy path: equi-join keeps only matching key pairs
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

    // empty left short-circuits
    #[test]
    fn test_nlj_empty_left() {
        let left = build_heap(103, &[]);
        let right = build_heap_xy(104, &[tup(1, 100)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert!(j.next().unwrap().is_none());
    }

    // empty right materializes to zero rows, nothing matches
    #[test]
    fn test_nlj_empty_right() {
        let left = build_heap(105, &[tup(1, 10)]);
        let right = build_heap_xy(106, &[]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert!(j.next().unwrap().is_none());
    }

    // duplicate keys yield the cartesian product across the pair
    #[test]
    fn test_nlj_duplicate_keys_cartesian() {
        let left = build_heap(107, &[tup(1, 10), tup(1, 11)]);
        let right = build_heap_xy(108, &[tup(1, 100), tup(1, 101)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(drain(&mut j).len(), 4);
    }

    // rows whose join key is NULL never match
    #[test]
    fn test_nlj_skips_right_null_keys() {
        let left = build_heap(109, &[tup(1, 10)]);
        let right = build_heap_xy(110, &[tup_null_a(100), tup(1, 200)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(drain(&mut j).len(), 1);
    }

    // inequality predicate goes through the generic evaluate path
    #[test]
    fn test_nlj_less_than_predicate() {
        let left = build_heap(111, &[tup(1, 0), tup(5, 0)]);
        let right = build_heap_xy(112, &[tup(3, 0), tup(10, 0)]);
        // left.a < right.x  (a=0, x=2 in concat)
        let p = nlj_col_expr(0, BinOp::Lt, 2);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), p);
        // (1,3), (1,10), (5,10)
        assert_eq!(drain(&mut j).len(), 3);
    }

    // rewind replays the same output
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

    // schema is the concatenation of the two inputs
    #[test]
    fn test_nlj_schema_merged() {
        let left = build_heap(115, &[]);
        let right = build_heap_xy(116, &[]);
        let j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2());
        assert_eq!(j.schema().physical_num_fields(), 4);
    }

    // ===== HashJoin =====

    // happy path: probe hits the right-side buckets
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

    // one left row fans out to every right row with the same key
    #[test]
    fn test_hash_join_multiple_right_per_key() {
        let left = build_heap(119, &[tup(1, 10)]);
        let right = build_heap(120, &[tup(1, 100), tup(1, 101), tup(1, 102)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 3);
    }

    // NULL keys are dropped from both the build and the probe
    #[test]
    fn test_hash_join_skips_null_keys() {
        let left = build_heap(121, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(122, &[tup_null_a(100), tup(1, 101)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 1);
    }

    // empty right table -> empty hash table -> no output
    #[test]
    fn test_hash_join_empty_right() {
        let left = build_heap(123, &[tup(1, 10)]);
        let right = build_heap(124, &[]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    // rewind clears pending + left state and replays
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

    // constructor enforces the equality-only precondition
    #[test]
    #[should_panic(expected = "HashJoin requires an equality predicate")]
    fn test_hash_join_new_panics_on_non_equality() {
        let left = build_heap(127, &[]);
        let right = build_heap(128, &[]);
        let p = JoinPredicate::new(col(0), col(0), Predicate::LessThan);
        let _ = HashJoin::new(scan(&left), scan(&right), p);
    }

    // residual narrows matches after the hash-key probe: key match AND left.b < right.y
    #[test]
    fn test_hash_join_residual_filters_after_key_match() {
        let left = build_heap(147, &[tup(1, 10), tup(1, 50)]);
        let right = build_heap_xy(148, &[tup(1, 20), tup(1, 60)]);

        // key = a=x (col0=col0); residual = left.b < right.y
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0)).with_residual(residual);

        let out = drain(&mut j);
        // Pairs that match key=1: (10,20), (10,60), (50,20), (50,60).
        // Residual keeps those where left.b < right.b: (10,20), (10,60), (50,60) — 3 rows.
        assert_eq!(out.len(), 3);
    }

    // happy path: outputs are emitted in ascending key order
    #[test]
    fn test_smj_basic_equi_join() {
        let left = build_heap(129, &[tup(3, 30), tup(1, 10), tup(2, 20)]);
        let right = build_heap(130, &[tup(2, 200), tup(1, 100), tup(4, 400)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        let keys: Vec<i32> = out.iter().map(|t| int(t, 0)).collect();
        assert_eq!(keys, vec![1, 2]);
    }

    // cartesian product over equal runs on both sides (collect_equals correctness)
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

    // equal run that extends to end of right input must not panic on exhausted cursor
    #[test]
    fn test_smj_equal_run_to_end_of_right() {
        let left = build_heap(133, &[tup(5, 0), tup(5, 1)]);
        let right = build_heap(134, &[tup(5, 100), tup(5, 101)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 4);
    }

    // disjoint key ranges produce no output
    #[test]
    fn test_smj_disjoint_keys() {
        let left = build_heap(135, &[tup(1, 0), tup(2, 0)]);
        let right = build_heap(136, &[tup(3, 0), tup(4, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    // NULL keys are skipped during drain
    #[test]
    fn test_smj_skips_null_keys() {
        let left = build_heap(137, &[tup_null_a(0), tup(1, 1)]);
        let right = build_heap(138, &[tup_null_a(0), tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 1);
    }

    // both empty
    #[test]
    fn test_smj_both_empty() {
        let left = build_heap(139, &[]);
        let right = build_heap(140, &[]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    // interleaved keys advance the correct cursor
    #[test]
    fn test_smj_interleaved_keys() {
        let left = build_heap(141, &[tup(1, 0), tup(3, 0), tup(5, 0)]);
        let right = build_heap(142, &[tup(2, 0), tup(3, 0), tup(4, 0), tup(5, 0)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        let out = drain(&mut j);
        let keys: Vec<i32> = out.iter().map(|t| int(t, 0)).collect();
        assert_eq!(keys, vec![3, 5]);
    }

    // rewind re-sorts and replays
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

    // constructor enforces equality-only precondition
    #[test]
    #[should_panic(expected = "SortMergeJoin requires an equality predicate")]
    fn test_smj_new_panics_on_non_equality() {
        let left = build_heap(145, &[]);
        let right = build_heap(146, &[]);
        let p = JoinPredicate::new(col(0), col(0), Predicate::GreaterThan);
        let _ = SortMergeJoin::new(scan(&left), scan(&right), p);
    }

    // residual narrows matches after equal-key merge: key match AND left.b < right.y
    #[test]
    fn test_smj_residual_filters_after_key_match() {
        let left = build_heap(149, &[tup(1, 10), tup(1, 50)]);
        let right = build_heap_xy(150, &[tup(1, 20), tup(1, 60)]);

        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j =
            SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0)).with_residual(residual);

        // Same selectivity as the HashJoin case: 3 rows.
        assert_eq!(drain(&mut j).len(), 3);
    }

    // ===== CrossJoin =====

    // every left row pairs with every right row
    #[test]
    fn test_cross_join_cartesian_product() {
        let left = build_heap(200, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(201, &[tup(3, 30), tup(4, 40), tup(5, 50)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert_eq!(drain(&mut j).len(), 6, "2 left × 3 right = 6 rows");
    }

    // empty right → nothing
    #[test]
    fn test_cross_join_empty_right() {
        let left = build_heap(202, &[tup(1, 10)]);
        let right = build_heap(203, &[]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert!(j.next().unwrap().is_none());
    }

    // empty left → nothing
    #[test]
    fn test_cross_join_empty_left() {
        let left = build_heap(204, &[]);
        let right = build_heap(205, &[tup(1, 10)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert!(j.next().unwrap().is_none());
    }

    // rewind replays the full Cartesian product
    #[test]
    fn test_cross_join_rewind() {
        let left = build_heap(206, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(207, &[tup(3, 30), tup(4, 40)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        let first = drain(&mut j).len();
        j.rewind().unwrap();
        let second = drain(&mut j).len();
        assert_eq!(first, 4);
        assert_eq!(first, second);
    }

    // schema is the concatenation of both sides (2+2 = 4 columns)
    #[test]
    fn test_cross_join_schema() {
        let left = build_heap(208, &[]);
        let right = build_heap(209, &[]);
        let j = CrossJoin::new(scan(&left), scan(&right));
        assert_eq!(j.schema().physical_num_fields(), 4);
    }

    // NULL values in rows do not affect cross join (no predicate, no filtering)
    #[test]
    fn test_cross_join_null_rows_included() {
        let left = build_heap(210, &[tup_null_a(1), tup(2, 3)]);
        let right = build_heap(211, &[tup(10, 11)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        // Both left rows (including the NULL-a one) must appear.
        assert_eq!(drain(&mut j).len(), 2);
    }

    // ===== NLJ LEFT OUTER JOIN =====

    // left rows with no matching right get NULL-padded right columns
    #[test]
    fn test_nlj_left_outer_unmatched_row_gets_nulls() {
        let left = build_heap(220, &[tup(1, 10), tup(9, 90)]);
        let right = build_heap_xy(221, &[tup(1, 100)]);
        // left.a = right.x; row (9,90) has no match
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), nlj_eq_0_0_w2())
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        assert_eq!(out.len(), 2);
        // The unmatched row has NULL at index 2 (first right column).
        let unmatched = out.iter().find(|t| int(t, 0) == 9).unwrap();
        assert!(matches!(unmatched.get(2), Some(Value::Null)));
    }

    // when all right rows match, LEFT OUTER behaves identically to INNER
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

    // empty right → all left rows appear with NULLs on right
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

    // ===== HashJoin LEFT OUTER JOIN =====

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

    // NULL-key left row must appear with NULL right side (not silently dropped)
    #[test]
    fn test_hash_left_outer_null_key_left_row_preserved() {
        let left = build_heap(232, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(233, &[tup(1, 100)]);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        // two left rows → two output rows
        assert_eq!(drain(&mut j).len(), 2);
    }

    // empty right → all left rows preserved with NULLs
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

    // residual rejects all hash-key matches → still emit NULL-padded row for that left row
    #[test]
    fn test_hash_left_outer_residual_rejects_all_emits_null() {
        // left.a matches right.a (key=1), but residual left.b < right.b rejects everything.
        let left = build_heap(236, &[tup(1, 999)]);
        let right = build_heap_xy(237, &[tup(1, 1)]);
        // key: a=x (col0=col0); residual: left.b < right.y (col1 < col3)
        let residual = nlj_col_expr(1, BinOp::Lt, 3);
        let mut j = HashJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter)
            .with_residual(residual);
        let out = drain(&mut j);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].get(2), Some(Value::Null)));
    }

    // rewind after LEFT OUTER works correctly
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

    // ===== SortMergeJoin LEFT OUTER JOIN =====

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

    // NULL-key left row must appear even though it can never match
    #[test]
    fn test_smj_left_outer_null_key_left_row_preserved() {
        let left = build_heap(242, &[tup_null_a(10), tup(1, 11)]);
        let right = build_heap(243, &[tup(1, 100)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        assert_eq!(drain(&mut j).len(), 2);
    }

    // empty right → every left row emitted with NULLs
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

    // right side exhausted mid-merge: remaining left rows get NULL-padded
    #[test]
    fn test_smj_left_outer_right_exhausted_mid_merge() {
        let left = build_heap(246, &[tup(1, 10), tup(5, 50), tup(9, 90)]);
        let right = build_heap(247, &[tup(1, 100), tup(5, 500)]);
        let mut j = SortMergeJoin::new(scan(&left), scan(&right), eq_pred(0, 0))
            .with_join_type(JoinType::LeftOuter);
        let out = drain(&mut j);
        // (1,1) match, (5,5) match, (9,_) unmatched → 3 rows total
        assert_eq!(out.len(), 3);
    }

    // residual rejects all matches for a key group → NULL-padded row emitted
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

    // rewind re-sorts and replays LEFT OUTER output correctly
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

    // NLJ supports compound boolean conditions.
    // Equi-join on left.a = right.x AND left.b < right.y.
    #[test]
    fn test_nlj_compound_and_predicate() {
        let left = build_heap(151, &[tup(1, 10), tup(1, 50), tup(2, 10)]);
        let right = build_heap_xy(152, &[tup(1, 20), tup(2, 5)]);

        // left.a = right.x AND left.b < right.y  (a=0, b=1, x=2, y=3 in concat)
        let pred = ResolvedExpr::BinaryOp {
            lhs: Box::new(nlj_col_expr(0, BinOp::Eq, 2)),
            op: BinOp::And,
            rhs: Box::new(nlj_col_expr(1, BinOp::Lt, 3)),
        };

        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), pred);
        let out = drain(&mut j);
        // Candidates with matching a=x:
        //   (1,10)·(1,20) → 10<20 ✓
        //   (1,50)·(1,20) → 50<20 ✗
        //   (2,10)·(2,5)  → 10<5  ✗
        // → 1 row.
        assert_eq!(out.len(), 1);
    }
}
