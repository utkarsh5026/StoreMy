//! Set-oriented relational operators: `UNION`, `INTERSECT`, `EXCEPT`, and `DISTINCT`.
//!
//! Each operator is a streaming [`Executor`] that wraps one or two child [`PlanNode`]s and
//! produces tuples according to standard SQL set semantics:
//!
//! - [`Union`] — concatenates two result sets, optionally deduplicating.
//! - [`Intersect`] — keeps only tuples that appear in both inputs.
//! - [`Except`] — keeps only tuples from the left input that do not appear in the right.
//! - [`Distinct`] — removes duplicate tuples from a single input.
//!
//! Deduplication is done by hashing whole [`Tuple`] values, so `Tuple` must implement
//! `Hash` and `Eq`. Operators that need to deduplicate against the right-hand side
//! (`Intersect`, `Except`) materialize the entire right input into a [`HashSet`] before
//! streaming the left input.

use std::collections::HashSet;

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor, PlanNode};
use crate::tuple::{Tuple, TupleSchema};

/// Streams the `UNION` of two child executors.
///
/// When `all` is `false` (i.e. `UNION` without `ALL`), duplicate tuples are suppressed by
/// keeping a [`HashSet`] of every tuple emitted so far. When `all` is `true` (`UNION ALL`),
/// tuples are forwarded without any deduplication check.
///
/// The left child is exhausted first; once it signals end-of-stream the operator switches
/// to the right child.
#[derive(Debug)]
pub struct Union<'a> {
    left: Box<PlanNode<'a>>,
    right: Box<PlanNode<'a>>,
    /// `true` once the left child has returned `None`.
    left_done: bool,
    /// When `true`, emit every tuple without deduplication (`UNION ALL`).
    all: bool,
    /// Tracks tuples already emitted; only populated when `all` is `false`.
    seen: HashSet<Tuple>,
}

impl<'a> Union<'a> {
    /// Creates a new `Union` operator over `left` and `right`.
    ///
    /// Set `all` to `true` for `UNION ALL` (no deduplication) or `false` for `UNION`
    /// (duplicates suppressed).
    #[tracing::instrument(skip_all, fields(op = "union", all))]
    pub fn new(left: Box<PlanNode<'a>>, right: Box<PlanNode<'a>>, all: bool) -> Self {
        Self {
            left,
            right,
            left_done: false,
            all,
            seen: HashSet::new(),
        }
    }
}

impl FallibleIterator for Union<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances to the next tuple in the union.
    ///
    /// Returns `Ok(Some(tuple))` for each qualifying tuple, `Ok(None)` when both children
    /// are exhausted, or `Err` if either child returns an error.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by the left or right child executor.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        loop {
            let tuple = if self.left_done {
                match self.right.next()? {
                    Some(t) => t,
                    None => return Ok(None),
                }
            } else if let Some(t) = self.left.next()? {
                t
            } else {
                self.left_done = true;
                continue;
            };

            if self.all || self.seen.insert(tuple.clone()) {
                return Ok(Some(tuple));
            }
        }
    }
}

impl Executor for Union<'_> {
    fn schema(&self) -> &TupleSchema {
        self.left.schema()
    }

    /// Resets the operator so it can be iterated again from the start.
    ///
    /// Clears the deduplication set and rewinds both child executors.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by either child's `rewind`.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.left_done = false;
        self.seen.clear();
        self.left.rewind()?;
        self.right.rewind()
    }
}

/// Shared implementation for `INTERSECT` and `EXCEPT`.
///
/// On the first call to `next`, materializes every tuple from the right child into
/// `right_set`. Subsequent calls stream the left child and either keep (`exclude = false`)
/// or discard (`exclude = true`) tuples that are present in `right_set`.
#[derive(Debug)]
struct MembershipFilter<'a> {
    left: Box<PlanNode<'a>>,
    right: Box<PlanNode<'a>>,
    /// All tuples from the right child, populated lazily on first call to `next`.
    right_set: HashSet<Tuple>,
    /// `true` once the right child has been fully consumed into `right_set`.
    materialized: bool,
    /// When `true`, exclude tuples found in `right_set` (`EXCEPT`).
    /// When `false`, include only tuples found in `right_set` (`INTERSECT`).
    exclude: bool,
}

impl<'a> MembershipFilter<'a> {
    fn new(left: Box<PlanNode<'a>>, right: Box<PlanNode<'a>>, exclude: bool) -> Self {
        Self {
            left,
            right,
            right_set: HashSet::new(),
            materialized: false,
            exclude,
        }
    }

    /// Consumes the entire right child into `right_set` if not already done.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by the right child executor.
    fn materialize_right(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }
        while let Some(tuple) = self.right.next()? {
            self.right_set.insert(tuple);
        }
        tracing::debug!(
            set_size = self.right_set.len(),
            "membership_filter: right side materialized"
        );
        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for MembershipFilter<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances to the next qualifying tuple.
    ///
    /// Materializes the right child on the first call, then streams the left child,
    /// returning tuples whose membership in `right_set` satisfies `exclude`.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] from either child executor.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_right()?;
        while let Some(tuple) = self.left.next()? {
            if self.right_set.contains(&tuple) != self.exclude {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for MembershipFilter<'_> {
    fn schema(&self) -> &TupleSchema {
        self.left.schema()
    }

    /// Resets the filter so it can be iterated again from the start.
    ///
    /// Clears the materialized right-hand set and rewinds both child executors.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by either child's `rewind`.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.right_set.clear();
        self.materialized = false;
        self.left.rewind()?;
        self.right.rewind()
    }
}

/// Streams tuples that appear in both the left and right child (`INTERSECT`).
///
/// Wraps `MembershipFilter` with `exclude = false`. The right child is materialized in
/// full before streaming begins; the left child is then streamed and only tuples present
/// in the right set are returned.
#[derive(Debug)]
pub struct Intersect<'a>(MembershipFilter<'a>);

impl<'a> Intersect<'a> {
    /// Creates a new `Intersect` operator over `left` and `right`.
    #[tracing::instrument(skip_all, fields(op = "intersect"))]
    pub fn new(left: Box<PlanNode<'a>>, right: Box<PlanNode<'a>>) -> Self {
        Self(MembershipFilter::new(left, right, false))
    }
}

impl FallibleIterator for Intersect<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances to the next tuple present in both inputs.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] from either child executor.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.0.next()
    }
}

impl Executor for Intersect<'_> {
    fn schema(&self) -> &TupleSchema {
        self.0.schema()
    }

    /// Resets the operator so it can be iterated again from the start.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by either child's `rewind`.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.0.rewind()
    }
}

/// Streams tuples from the left child that do not appear in the right child (`EXCEPT`).
///
/// Wraps `MembershipFilter` with `exclude = true`. The right child is materialized in
/// full before streaming begins; the left child is then streamed and only tuples absent
/// from the right set are returned.
#[derive(Debug)]
pub struct Except<'a>(MembershipFilter<'a>);

impl<'a> Except<'a> {
    /// Creates a new `Except` operator over `left` and `right`.
    #[tracing::instrument(skip_all, fields(op = "except"))]
    pub fn new(left: Box<PlanNode<'a>>, right: Box<PlanNode<'a>>) -> Self {
        Self(MembershipFilter::new(left, right, true))
    }
}

impl FallibleIterator for Except<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances to the next tuple present in the left input but absent from the right.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] from either child executor.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.0.next()
    }
}

impl Executor for Except<'_> {
    fn schema(&self) -> &TupleSchema {
        self.0.schema()
    }

    /// Resets the operator so it can be iterated again from the start.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by either child's `rewind`.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.0.rewind()
    }
}

/// Removes duplicate tuples from a single child executor (`SELECT DISTINCT`).
///
/// Maintains a [`HashSet`] of every tuple emitted so far and skips any tuple that has
/// already been seen. Memory use is proportional to the number of distinct tuples in the
/// child's output.
#[derive(Debug)]
pub struct Distinct<'a> {
    child: Box<PlanNode<'a>>,
    seen: HashSet<Tuple>,
    input_count: usize,
}

impl<'a> Distinct<'a> {
    /// Creates a new `Distinct` operator wrapping `child`.
    #[tracing::instrument(skip_all, fields(op = "distinct"))]
    pub fn new(child: Box<PlanNode<'a>>) -> Self {
        Self {
            child,
            seen: HashSet::new(),
            input_count: 0,
        }
    }
}

impl FallibleIterator for Distinct<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances to the next tuple not yet returned by this operator.
    ///
    /// Skips over duplicates until a previously-unseen tuple is found, then returns it.
    /// Returns `Ok(None)` when the child is exhausted.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by the child executor.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(tuple) = self.child.next()? {
            self.input_count += 1;
            if self.seen.insert(tuple.clone()) {
                return Ok(Some(tuple));
            }
        }
        tracing::debug!(
            unique = self.seen.len(),
            input = self.input_count,
            "distinct: exhausted"
        );
        Ok(None)
    }
}

impl Executor for Distinct<'_> {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    /// Resets the operator so it can be iterated again from the start.
    ///
    /// Clears the set of seen tuples and rewinds the child executor.
    ///
    /// # Errors
    ///
    /// Propagates any [`ExecutionError`] returned by the child's `rewind`.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.seen.clear();
        self.input_count = 0;
        self.child.rewind()
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

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn schema_ab() -> TupleSchema {
        TupleSchema::new(vec![field("a", Type::Int32), field("b", Type::Int32)])
    }

    fn tup(a: i32, b: i32) -> Tuple {
        Tuple::new(vec![Value::Int32(a), Value::Int32(b)])
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

    fn scan(h: &HeapHarness) -> Box<PlanNode<'_>> {
        Box::new(PlanNode::SeqScan(SeqScan::new(&h.heap, h.txn)))
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

    fn sorted_pairs(tuples: &[Tuple]) -> Vec<(i32, i32)> {
        let mut v: Vec<(i32, i32)> = tuples
            .iter()
            .map(|t| match (t.get(0), t.get(1)) {
                (Some(Value::Int32(a)), Some(Value::Int32(b))) => (*a, *b),
                other => panic!("unexpected tuple contents: {other:?}"),
            })
            .collect();
        v.sort_unstable();
        v
    }

    // UNION suppresses duplicates across both sides
    #[test]
    fn test_union_dedup() {
        let left = build_heap(201, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(202, &[tup(2, 2), tup(3, 3)]);
        let mut u = Union::new(scan(&left), scan(&right), false);
        let out = drain(&mut u);
        assert_eq!(sorted_pairs(&out), vec![(1, 1), (2, 2), (3, 3)]);
    }

    // UNION ALL preserves every duplicate
    #[test]
    fn test_union_all_preserves_duplicates() {
        let left = build_heap(203, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(204, &[tup(2, 2), tup(3, 3)]);
        let mut u = Union::new(scan(&left), scan(&right), true);
        let out = drain(&mut u);
        assert_eq!(sorted_pairs(&out), vec![(1, 1), (2, 2), (2, 2), (3, 3)]);
    }

    // internal duplicates within one side also get deduped
    #[test]
    fn test_union_dedup_within_single_side() {
        let left = build_heap(205, &[tup(1, 1), tup(1, 1), tup(2, 2)]);
        let right = build_heap(206, &[]);
        let mut u = Union::new(scan(&left), scan(&right), false);
        let out = drain(&mut u);
        assert_eq!(sorted_pairs(&out), vec![(1, 1), (2, 2)]);
    }

    // empty inputs on either side
    #[test]
    fn test_union_empty_left() {
        let left = build_heap(207, &[]);
        let right = build_heap(208, &[tup(1, 1)]);
        let mut u = Union::new(scan(&left), scan(&right), false);
        assert_eq!(sorted_pairs(&drain(&mut u)), vec![(1, 1)]);
    }

    #[test]
    fn test_union_both_empty() {
        let left = build_heap(209, &[]);
        let right = build_heap(210, &[]);
        let mut u = Union::new(scan(&left), scan(&right), false);
        assert!(u.next().unwrap().is_none());
    }

    // rewind clears `seen` so dedup state is reset between passes
    #[test]
    fn test_union_rewind_replays() {
        let left = build_heap(211, &[tup(1, 1)]);
        let right = build_heap(212, &[tup(1, 1), tup(2, 2)]);
        let mut u = Union::new(scan(&left), scan(&right), false);
        let first = drain(&mut u).len();
        u.rewind().unwrap();
        let second = drain(&mut u).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }

    // schema is the left child's schema
    #[test]
    fn test_union_schema_from_left() {
        let left = build_heap(213, &[]);
        let right = build_heap(214, &[]);
        let u = Union::new(scan(&left), scan(&right), false);
        assert_eq!(u.schema().physical_num_fields(), 2);
    }

    // ===== Intersect =====

    // tuples in both sides are returned; tuples in only one are dropped
    #[test]
    fn test_intersect_basic() {
        let left = build_heap(215, &[tup(1, 1), tup(2, 2), tup(3, 3)]);
        let right = build_heap(216, &[tup(2, 2), tup(3, 3), tup(4, 4)]);
        let mut i = Intersect::new(scan(&left), scan(&right));
        assert_eq!(sorted_pairs(&drain(&mut i)), vec![(2, 2), (3, 3)]);
    }

    // no overlap -> empty
    #[test]
    fn test_intersect_disjoint() {
        let left = build_heap(217, &[tup(1, 1)]);
        let right = build_heap(218, &[tup(2, 2)]);
        let mut i = Intersect::new(scan(&left), scan(&right));
        assert!(i.next().unwrap().is_none());
    }

    // empty right -> empty result even if left is non-empty
    #[test]
    fn test_intersect_empty_right() {
        let left = build_heap(219, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(220, &[]);
        let mut i = Intersect::new(scan(&left), scan(&right));
        assert!(i.next().unwrap().is_none());
    }

    // Intersect doesn't dedup the left stream: duplicate left rows both pass through
    // when present on the right. (This matches bag-semantics for the left side.)
    #[test]
    fn test_intersect_keeps_left_duplicates() {
        let left = build_heap(221, &[tup(1, 1), tup(1, 1), tup(2, 2)]);
        let right = build_heap(222, &[tup(1, 1)]);
        let mut i = Intersect::new(scan(&left), scan(&right));
        let out = drain(&mut i);
        assert_eq!(sorted_pairs(&out), vec![(1, 1), (1, 1)]);
    }

    // rewind clears materialized set and re-reads both sides
    #[test]
    fn test_intersect_rewind_replays() {
        let left = build_heap(223, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(224, &[tup(2, 2)]);
        let mut i = Intersect::new(scan(&left), scan(&right));
        let first = drain(&mut i).len();
        i.rewind().unwrap();
        let second = drain(&mut i).len();
        assert_eq!(first, 1);
        assert_eq!(first, second);
    }

    // ===== Except =====

    // keeps left-only tuples
    #[test]
    fn test_except_basic() {
        let left = build_heap(225, &[tup(1, 1), tup(2, 2), tup(3, 3)]);
        let right = build_heap(226, &[tup(2, 2)]);
        let mut e = Except::new(scan(&left), scan(&right));
        assert_eq!(sorted_pairs(&drain(&mut e)), vec![(1, 1), (3, 3)]);
    }

    // every left tuple present in right -> empty
    #[test]
    fn test_except_all_filtered() {
        let left = build_heap(227, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(228, &[tup(1, 1), tup(2, 2)]);
        let mut e = Except::new(scan(&left), scan(&right));
        assert!(e.next().unwrap().is_none());
    }

    // empty right passes every left tuple through unchanged
    #[test]
    fn test_except_empty_right() {
        let left = build_heap(229, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(230, &[]);
        let mut e = Except::new(scan(&left), scan(&right));
        assert_eq!(sorted_pairs(&drain(&mut e)), vec![(1, 1), (2, 2)]);
    }

    // empty left -> empty
    #[test]
    fn test_except_empty_left() {
        let left = build_heap(231, &[]);
        let right = build_heap(232, &[tup(1, 1)]);
        let mut e = Except::new(scan(&left), scan(&right));
        assert!(e.next().unwrap().is_none());
    }

    // Except also keeps left-side duplicates (bag semantics on the left)
    #[test]
    fn test_except_keeps_left_duplicates() {
        let left = build_heap(233, &[tup(1, 1), tup(1, 1), tup(2, 2)]);
        let right = build_heap(234, &[tup(2, 2)]);
        let mut e = Except::new(scan(&left), scan(&right));
        assert_eq!(sorted_pairs(&drain(&mut e)), vec![(1, 1), (1, 1)]);
    }

    // rewind replays
    #[test]
    fn test_except_rewind_replays() {
        let left = build_heap(235, &[tup(1, 1), tup(2, 2)]);
        let right = build_heap(236, &[tup(2, 2)]);
        let mut e = Except::new(scan(&left), scan(&right));
        let first = drain(&mut e).len();
        e.rewind().unwrap();
        let second = drain(&mut e).len();
        assert_eq!(first, 1);
        assert_eq!(first, second);
    }

    // dedups consecutive and non-consecutive duplicates
    #[test]
    fn test_distinct_removes_duplicates() {
        let child = build_heap(237, &[
            tup(1, 1),
            tup(2, 2),
            tup(1, 1),
            tup(2, 2),
            tup(3, 3),
        ]);
        let mut d = Distinct::new(scan(&child));
        assert_eq!(sorted_pairs(&drain(&mut d)), vec![(1, 1), (2, 2), (3, 3)]);
    }

    // all-same input collapses to one
    #[test]
    fn test_distinct_all_same() {
        let child = build_heap(238, &[tup(7, 7), tup(7, 7), tup(7, 7)]);
        let mut d = Distinct::new(scan(&child));
        let out = drain(&mut d);
        assert_eq!(out.len(), 1);
    }

    // all-distinct input passes through unchanged
    #[test]
    fn test_distinct_all_unique() {
        let child = build_heap(239, &[tup(1, 1), tup(2, 2), tup(3, 3)]);
        let mut d = Distinct::new(scan(&child));
        assert_eq!(sorted_pairs(&drain(&mut d)), vec![(1, 1), (2, 2), (3, 3)]);
    }

    // empty input -> empty output
    #[test]
    fn test_distinct_empty() {
        let child = build_heap(240, &[]);
        let mut d = Distinct::new(scan(&child));
        assert!(d.next().unwrap().is_none());
    }

    // rewind clears `seen` so the same output is produced again
    #[test]
    fn test_distinct_rewind_replays() {
        let child = build_heap(241, &[tup(1, 1), tup(1, 1), tup(2, 2)]);
        let mut d = Distinct::new(scan(&child));
        let first = drain(&mut d).len();
        d.rewind().unwrap();
        let second = drain(&mut d).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }
}
