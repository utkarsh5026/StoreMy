//! Cartesian product (`CROSS JOIN`) executor.
//!
//! [`CrossJoin`] pairs every tuple from the left child with every tuple from the right child.
//! There is no join predicate — unlike [`super::NestedLoopJoin`], nothing is filtered after
//! concatenation.
//!
//! The right input is fully materialized on the first [`FallibleIterator::next`] call, so memory
//! use is \(O(|R|)\) for the right side plus whatever the left iterator holds. Output size is
//! \(|L| \times |R|\).

use std::collections::VecDeque;

use fallible_iterator::FallibleIterator;

use super::JoinInputs;
use crate::{
    execution::{ExecutionError, Executor, PlanNode},
    tuple::{Tuple, TupleSchema},
};

/// Produces the Cartesian product of two inputs — every left row paired with every right row,
/// with no predicate filtering.
///
/// The right input is materialized once on the first call to [`FallibleIterator::next`]. Each
/// subsequent left tuple fans out to `|right|` output rows unconditionally.
///
/// # SQL shape
///
/// ```sql
/// SELECT * FROM left_table CROSS JOIN right_table;
/// ```
///
/// # Memory and output order
///
/// - **Memory**: \(O(|R|)\) to buffer the right side for the lifetime of the executor.
/// - **Output order**: left-input order; within each left row, right tuples appear in the order
///   they were read during materialization.
#[derive(Debug)]
pub struct CrossJoin<'a> {
    inputs: Box<JoinInputs<'a>>,
    right_buf: Option<Vec<Tuple>>,
    pending: VecDeque<Tuple>,
}

impl<'a> CrossJoin<'a> {
    /// Creates a cross-join executor for `left × right`.
    ///
    /// The output schema is the concatenation of both children (see [`Executor::schema`]).
    /// The right child is not read until the first [`FallibleIterator::next`] call.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut join = CrossJoin::new(left_plan, right_plan);
    /// while let Some(tuple) = join.next()? {
    ///     // each tuple is left.concat(right)
    /// }
    /// ```
    #[tracing::instrument(skip_all, fields(op = "cross_join"))]
    pub fn new(left: PlanNode<'a>, right: PlanNode<'a>) -> Self {
        Self {
            inputs: Box::new(JoinInputs::new(left, right)),
            right_buf: None,
            pending: VecDeque::new(),
        }
    }

    /// Reads and buffers every tuple from the right child into [`CrossJoin::right_buf`].
    ///
    /// Called at most once per executor; later calls return immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError`] if pulling any tuple from the right input fails.
    fn materialize_right(&mut self) -> Result<(), ExecutionError> {
        if self.right_buf.is_some() {
            return Ok(());
        }
        let mut buf = Vec::new();
        while let Some(tuple) = self.inputs.right.next()? {
            buf.push(tuple);
        }
        tracing::debug!(tuples = buf.len(), "cross_join: right side buffered");
        self.right_buf = Some(buf);
        Ok(())
    }
}

/// Emits `left.concat(right)` for every pair in the Cartesian product.
///
/// The right side is materialized on the first `next` call. For each left tuple, all buffered
/// right tuples are enqueued into `pending` and returned one at a time. If `pending` already
/// holds rows when `next` is called, those are returned before advancing the left input.
///
/// An empty right buffer yields no output rows (even if the left side has tuples). An exhausted
/// left input yields [`None`].
///
/// # Errors
///
/// Returns [`ExecutionError`] if materializing the right side or advancing the left input fails.
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

            for right in self.right_buf.as_deref().unwrap() {
                self.pending.push_back(l.concat(right));
            }
        }
    }
}

impl Executor for CrossJoin<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.inputs.schema
    }

    /// Clears buffered output and rewinds only the left child.
    ///
    /// The materialized right buffer is kept, so a second scan does not re-read the right input.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError`] if rewinding the left child fails.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.pending.clear();
        self.inputs.left.rewind()
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_utils::*, CrossJoin};

    #[test]
    fn test_cross_join_cartesian_product() {
        let left = build_heap(200, &[tup(1, 10), tup(2, 20)]);
        let right = build_heap(201, &[tup(3, 30), tup(4, 40), tup(5, 50)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert_eq!(drain(&mut j).len(), 6, "2 left × 3 right = 6 rows");
    }

    #[test]
    fn test_cross_join_empty_right() {
        let left = build_heap(202, &[tup(1, 10)]);
        let right = build_heap(203, &[]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert!(j.next().unwrap().is_none());
    }

    #[test]
    fn test_cross_join_empty_left() {
        let left = build_heap(204, &[]);
        let right = build_heap(205, &[tup(1, 10)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert!(j.next().unwrap().is_none());
    }

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

    #[test]
    fn test_cross_join_schema() {
        let left = build_heap(208, &[]);
        let right = build_heap(209, &[]);
        let j = CrossJoin::new(scan(&left), scan(&right));
        assert_eq!(j.schema().physical_num_fields(), 4);
    }

    #[test]
    fn test_cross_join_null_rows_included() {
        let left = build_heap(210, &[tup_null_a(1), tup(2, 3)]);
        let right = build_heap(211, &[tup(10, 11)]);
        let mut j = CrossJoin::new(scan(&left), scan(&right));
        assert_eq!(drain(&mut j).len(), 2);
    }
}
