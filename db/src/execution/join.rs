//! Join executors for combining tuples from two inputs.
//!
//! This module contains multiple physical join implementations that all produce the same logical
//! output: concatenated tuples from a left and a right input when a [`JoinPredicate`] matches.
//! Different executors trade memory, startup cost, and per-row work.

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    ops::{Index, IndexMut},
};

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor};
use crate::{
    Value,
    execution::PlanNode,
    primitives::{ColumnId, Predicate},
    tuple::{Tuple, TupleSchema},
};

/// A join predicate is a condition that must be satisfied by two tuples in order to be joined.
/// It is used to determine which tuples from the left and right relations should be joined.
#[derive(Debug, Clone, Copy)]
pub struct JoinPredicate {
    /// Column on the left input used as the join key.
    pub left_col: ColumnId,
    /// Column on the right input used as the join key.
    pub right_col: ColumnId,
    /// Comparison operator applied to the two key values.
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
    /// Returns an [`ExecutionError::TypeError`](crate::execution::ExecutionError::TypeError) if the
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

/// Fetches a value by column id with a side-specific error message.
///
/// This helper returns a reference into `tuple`, so the output lifetime is tied to the input
/// borrow of `tuple` (via `<'a>`).
#[inline]
fn get_value(tuple: &Tuple, col: ColumnId, is_left: bool) -> Result<&Value, ExecutionError> {
    tuple.get(usize::from(col)).ok_or_else(|| {
        ExecutionError::TypeError(format!(
            "{} col {col} out of bounds",
            if is_left { "left" } else { "right" }
        ))
    })
}

/// Internal container for the left and right input plan nodes, output schema, and join predicate.
///
/// Used by join executor implementations to encapsulate the two child nodes and predicate,
/// and to provide the merged output schema for the join result.
///
/// Not exposed outside the execution module.
#[derive(Debug)]
struct JoinBox<'a> {
    left: PlanNode<'a>,
    right: PlanNode<'a>,
    schema: TupleSchema,
    predicate: JoinPredicate,
}

impl<'a> JoinBox<'a> {
    /// Constructs a new `JoinBox` for two input plan nodes and a join predicate.
    ///
    /// Combines the left and right schemas into a composite output schema.
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        let schema = left.schema().merge(right.schema());
        Self {
            left,
            right,
            schema,
            predicate,
        }
    }

    /// Returns the column index (as `usize`) of the join key on the left input.
    ///
    /// This index is derived from the `left_col` field in the `JoinPredicate`.
    #[inline]
    fn left_idx(&self) -> usize {
        usize::from(self.predicate.left_col)
    }

    /// Returns the column index (as `usize`) of the join key on the right input.
    ///
    /// This index is derived from the `right_col` field in the `JoinPredicate`.
    #[inline]
    fn right_idx(&self) -> usize {
        usize::from(self.predicate.right_col)
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

#[derive(Debug)]
/// Joins two inputs by comparing every left tuple with every right tuple.
///
/// The right input is materialized once (skipping rows with `NULL` join keys), then the executor
/// scans the left input and evaluates the join predicate against each buffered right tuple.
pub struct NestedLoopJoin<'a> {
    join_box: Box<JoinBox<'a>>,
    right_buf: Vec<Tuple>,
    materialized: bool,
    pending: VecDeque<Tuple>,
}

impl<'a> NestedLoopJoin<'a> {
    /// Creates a nested-loop join executor for `left ⋈ right` using `predicate`.
    ///
    /// The right input is read and buffered on the first call to [`FallibleIterator::next`].
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        Self {
            join_box: Box::new(JoinBox::new(left, right, predicate)),
            right_buf: Vec::new(),
            materialized: false,
            pending: VecDeque::new(),
        }
    }

    /// Reads and buffers all right-side tuples with non-NULL join key values into `self.right_buf`.
    ///
    /// This method materializes the entire right input exactly once, filtering out any rows where
    /// the join key (at `right_col`) is either missing or explicitly `NULL`. Each valid tuple
    /// is pushed into the in-memory right buffer for repeated probing during the nested-loop
    /// join.
    ///
    /// # Errors
    ///
    /// Returns an [`ExecutionError`] if retrieving any tuple from the right input fails.
    fn materialize_right(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        let right_idx = usize::from(self.join_box.predicate.right_col);
        let right = &mut self.join_box.right;
        while let Some(tuple) = right.next()? {
            if matches!(tuple.get(right_idx), Some(Value::Null) | None) {
                continue;
            }
            self.right_buf.push(tuple);
        }
        self.materialized = true;
        Ok(())
    }
}

/// Produces joined tuples by pairing each left tuple with every matching right tuple.
///
/// On the first call to `next`, this implementation materializes (buffers) all right-side tuples
/// with non-NULL join keys. Then, for each tuple from the left input, it compares the tuple against
/// every tuple in this right buffer using the join predicate. Any left/right pair that satisfies
/// the predicate is concatenated and buffered in `pending`, and then returned from the iterator.
///
/// Note:
/// - Right input is only scanned and materialized once per executor.
/// - Output order is: all qualifying (left, right) pairs for each left row, in left input order.
///   The inner (right) order matches the right input order at materialization time.
/// - If `pending` is non-empty when `next` is called, it emits these join results before advancing
///   the left input.
/// - Skips right-side tuples where the join key is `NULL`. This is ensured during materialization.
///
/// See [`Executor`] for rewind semantics and `materialize_right` for buffering behavior.
impl FallibleIterator for NestedLoopJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances the join and returns the next joined tuple, if available.
    ///
    /// - Materializes (buffers) the right input on the first call.
    /// - Buffers all output tuples for the current left tuple before returning the first one.
    /// - Returns `Ok(None)` when the left input is exhausted.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_right()?;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                return Ok(Some(tuple));
            }

            let Some(l) = self.join_box.left.next()? else {
                return Ok(None);
            };

            for right in &self.right_buf {
                if !self.join_box.predicate.evaluate(&l, right)? {
                    continue;
                }
                self.pending.push_back(l.concat(right));
            }
        }
    }
}

impl Executor for NestedLoopJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.join_box.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.join_box.left.rewind()
    }
}

#[derive(Debug)]
/// Joins two inputs by building a hash table on the right input.
///
/// This join only supports equality predicates. The right input is read once and grouped by its
/// join key (skipping rows with `NULL` keys). Each left tuple probes the hash table to find
/// matching right tuples.
pub struct HashJoin<'a> {
    join_box: Box<JoinBox<'a>>,
    hash_table: HashMap<Value, Vec<Tuple>>,
    pending: VecDeque<Tuple>,
    materialized: bool,
    left_tuple: Option<Tuple>,
}

impl<'a> HashJoin<'a> {
    /// Creates a hash join executor for `left ⋈ right` using `predicate`.
    ///
    /// # Panics
    ///
    /// Panics if `predicate.op` is not [`Predicate::Equals`].
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        assert_eq!(
            predicate.op,
            Predicate::Equals,
            "HashJoin requires an equality predicate, got {}",
            predicate.op
        );
        Self {
            join_box: Box::new(JoinBox::new(left, right, predicate)),
            hash_table: HashMap::new(),
            pending: VecDeque::new(),
            materialized: false,
            left_tuple: None,
        }
    }

    /// Materializes the hash table for the right input using the join key.
    ///
    /// This method reads all remaining tuples from the right child (if not already done)
    /// and groups them into a hash table, keyed by the value of the right join column.
    ///
    /// Tuples where the join key is `NULL` or missing are skipped (won't match).
    /// Only non-null, present join key values are included in the hash table.
    ///
    /// - The hash table is only built once, subsequent calls are a no-op.
    /// - If the materialization is already complete, returns immediately.
    ///
    /// # Errors
    /// Returns an error if advancing the right child fails.
    fn build_hash_table(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        let right_idx = usize::from(self.join_box.predicate.right_col);
        while let Some(t) = self.join_box.right.next()? {
            match t.get(right_idx) {
                Some(Value::Null) | None => {}
                Some(v) => {
                    let key = v.clone();
                    self.hash_table.entry(key).or_default().push(t);
                }
            }
        }
        self.materialized = true;
        Ok(())
    }
}

/// Produces the next joined tuple from the hash join.
///
/// On the first call, the right input is fully materialized into an in-memory hash table
/// keyed by the right join column (rows with `NULL`/missing keys are skipped).
///
/// Each subsequent call probes using the next left tuple:
/// - Left tuples with `NULL`/missing join keys are skipped.
/// - If the left key exists in the hash table, this buffers the matching right tuples and returns
///   `left.concat(right)` for each match.
/// - If there is no bucket for the left key, it advances to the next left tuple.
///
/// Output is streamed: at most one joined tuple is returned per call. For each left tuple,
/// matches are returned in the right-side scan/bucket order.
///
/// # Errors
/// Returns an error if either child executor returns an error while advancing.
impl FallibleIterator for HashJoin<'_> {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.build_hash_table()?;
        let left_idx = usize::from(self.join_box.predicate.left_col);

        loop {
            if let Some(r) = self.pending.pop_front() {
                let l = self
                    .left_tuple
                    .as_ref()
                    .expect("current_left set with pending");
                return Ok(Some(l.concat(&r)));
            }

            let Some(l) = self.join_box.left.next()? else {
                self.left_tuple = None;
                return Ok(None);
            };

            let key = match l.get(left_idx) {
                Some(Value::Null) | None => continue,
                Some(v) => v.clone(),
            };

            if let Some(bucket) = self.hash_table.get(&key) {
                if bucket.is_empty() {
                    continue;
                }
                self.pending.extend(bucket.iter().cloned());
                self.left_tuple = Some(l);
            }
        }
    }
}

impl Executor for HashJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.join_box.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.left_tuple = None;
        self.join_box.left.rewind()
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
/// This join only supports equality predicates. Both inputs are drained, sorted by their join
/// keys (skipping rows with `NULL` keys), then scanned in lockstep to find equal key groups.
pub struct SortMergeJoin<'a> {
    join_box: Box<JoinBox<'a>>,
    l_sorted: TupleCursor,
    r_sorted: TupleCursor,
    pending: VecDeque<Tuple>,
    sorted: bool,
}

impl<'a> SortMergeJoin<'a> {
    /// Creates a new sort-merge join executor for `left ⋈ right` using the provided predicate.
    ///
    /// Both input plan nodes will be fully drained and sorted on the join key before producing any
    /// output. Only equality predicates are supported for sort-merge joins in this
    /// implementation.
    ///
    /// # Panics
    ///
    /// Panics if the join predicate is not [`Predicate::Equals`].
    ///
    /// # Arguments
    ///
    /// * `left` - The left child plan node (input relation).
    /// * `right` - The right child plan node (input relation).
    /// * `predicate` - The join predicate describing the join columns and operator.
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>, predicate: JoinPredicate) -> Self {
        assert_eq!(
            predicate.op,
            Predicate::Equals,
            "SortMergeJoin requires an equality predicate, got {}",
            predicate.op
        );
        Self {
            join_box: Box::new(JoinBox::new(left, right, predicate)),
            l_sorted: TupleCursor::new(),
            r_sorted: TupleCursor::new(),
            pending: VecDeque::new(),
            sorted: false,
        }
    }

    /// Drains both left and right child executors into in-memory vectors
    /// and sorts each side by its join key column.
    ///
    /// This transformation is performed only once on first access.
    ///
    /// # Errors
    ///
    /// Returns an [`ExecutionError`] if draining tuples or retrieving join key values fails.
    fn sort_inputs(&mut self) -> Result<(), ExecutionError> {
        if self.sorted {
            return Ok(());
        }

        let left_idx = self.join_box.left_idx();
        let right_idx = self.join_box.right_idx();

        drain_tuples(&mut self.join_box.left, &mut self.l_sorted.0, left_idx)?;
        drain_tuples(&mut self.join_box.right, &mut self.r_sorted.0, right_idx)?;

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
        let l_idx = self.join_box.predicate.left_col;
        let r_idx = self.join_box.predicate.right_col;

        let curr = get_value(self.l_sorted.current(), l_idx, true)?.clone();

        // Advance the right cursor to collect all right-side tuples with this join key.
        let right_start = self.r_sorted.current_idx();
        while !self.r_sorted.exhausted()
            && get_value(self.r_sorted.current(), r_idx, false)? == &curr
        {
            self.r_sorted.forward();
        }
        let right_end = self.r_sorted.current_idx();

        // For each left tuple in this group, concatenate it with every right tuple from the right
        // group.
        while !self.l_sorted.exhausted()
            && get_value(self.l_sorted.current(), l_idx, true)? == &curr
        {
            let l_curr = self.l_sorted.current().clone();
            for i in right_start..right_end {
                let r = &self.r_sorted[i];
                self.pending.push_back(l_curr.concat(r));
            }
            self.l_sorted.forward();
        }

        Ok(())
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
        let l_idx = self.join_box.predicate.left_col;
        let r_idx = self.join_box.predicate.right_col;

        loop {
            if let Some(tuple) = self.pending.pop_front() {
                return Ok(Some(tuple));
            }

            if self.l_sorted.exhausted() || self.r_sorted.exhausted() {
                return Ok(None);
            }

            let lk = get_value(self.l_sorted.current(), l_idx, true)?;
            let rk = get_value(self.r_sorted.current(), r_idx, false)?;

            match lk.partial_cmp(rk) {
                Some(Ordering::Less) => self.l_sorted.forward(),
                Some(Ordering::Greater) => self.r_sorted.forward(),
                Some(Ordering::Equal) => self.collect_equals()?,
                None => return Err(ExecutionError::TypeError("incomparable join keys".into())),
            }
        }
    }
}

impl Executor for SortMergeJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.join_box.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.l_sorted = TupleCursor::new();
        self.r_sorted = TupleCursor::new();
        self.sorted = false;
        self.join_box.left.rewind()?;
        self.join_box.right.rewind()?;
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
        execution::scan::SeqScan,
        heap::file::HeapFile,
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    fn schema_ab() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("a", Type::Int32),
            Field::new("b", Type::Int32),
        ])
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

    fn scan(h: &HeapHarness) -> PlanNode<'_> {
        PlanNode::SeqScan(SeqScan::new(&h.heap, h.txn))
    }

    fn col(n: u32) -> ColumnId {
        ColumnId::new(n).unwrap()
    }

    fn eq_pred(l: u32, r: u32) -> JoinPredicate {
        JoinPredicate::new(col(l), col(r), Predicate::Equals)
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

    // ===== JoinPredicate::evaluate =====

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
        let right = build_heap(102, &[tup(1, 100), tup(2, 200), tup(4, 400)]);

        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
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
        let right = build_heap(104, &[tup(1, 100)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    // empty right materializes to zero rows, nothing matches
    #[test]
    fn test_nlj_empty_right() {
        let left = build_heap(105, &[tup(1, 10)]);
        let right = build_heap(106, &[]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert!(j.next().unwrap().is_none());
    }

    // duplicate keys yield the cartesian product across the pair
    #[test]
    fn test_nlj_duplicate_keys_cartesian() {
        let left = build_heap(107, &[tup(1, 10), tup(1, 11)]);
        let right = build_heap(108, &[tup(1, 100), tup(1, 101)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 4);
    }

    // rows whose join key is NULL never match and are filtered at materialization
    #[test]
    fn test_nlj_skips_right_null_keys() {
        let left = build_heap(109, &[tup(1, 10)]);
        let right = build_heap(110, &[tup_null_a(100), tup(1, 200)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(drain(&mut j).len(), 1);
    }

    // inequality predicate goes through the generic evaluate path
    #[test]
    fn test_nlj_less_than_predicate() {
        let left = build_heap(111, &[tup(1, 0), tup(5, 0)]);
        let right = build_heap(112, &[tup(3, 0), tup(10, 0)]);
        let p = JoinPredicate::new(col(0), col(0), Predicate::LessThan);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), p);
        // (1,3), (1,10), (5,10)
        assert_eq!(drain(&mut j).len(), 3);
    }

    // rewind replays the same output
    #[test]
    fn test_nlj_rewind_replays_output() {
        let left = build_heap(113, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(114, &[tup(1, 100), tup(2, 200)]);
        let mut j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
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
        let right = build_heap(116, &[]);
        let j = NestedLoopJoin::new(scan(&left), scan(&right), eq_pred(0, 0));
        assert_eq!(j.schema().num_fields(), 4);
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

    // ===== SortMergeJoin =====

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
}
