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

use crate::tuple::{Tuple, TupleSchema};

use super::{ExecutionError, Executor, PlanNode};

/// Streams the `UNION` of two child executors.
///
/// When `all` is `false` (i.e. `UNION` without `ALL`), duplicate tuples are suppressed by
/// keeping a [`HashSet`] of every tuple emitted so far. When `all` is `true` (`UNION ALL`),
/// tuples are forwarded without any deduplication check.
///
/// The left child is exhausted first; once it signals end-of-stream the operator switches
/// to the right child.
#[derive(Debug)]
pub struct Union {
    left: Box<PlanNode>,
    right: Box<PlanNode>,
    /// `true` once the left child has returned `None`.
    left_done: bool,
    /// When `true`, emit every tuple without deduplication (`UNION ALL`).
    all: bool,
    /// Tracks tuples already emitted; only populated when `all` is `false`.
    seen: HashSet<Tuple>,
}

impl Union {
    /// Creates a new `Union` operator over `left` and `right`.
    ///
    /// Set `all` to `true` for `UNION ALL` (no deduplication) or `false` for `UNION`
    /// (duplicates suppressed).
    pub fn new(left: Box<PlanNode>, right: Box<PlanNode>, all: bool) -> Self {
        Self {
            left,
            right,
            left_done: false,
            all,
            seen: HashSet::new(),
        }
    }
}

impl FallibleIterator for Union {
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

impl Executor for Union {
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
struct MembershipFilter {
    left: Box<PlanNode>,
    right: Box<PlanNode>,
    /// All tuples from the right child, populated lazily on first call to `next`.
    right_set: HashSet<Tuple>,
    /// `true` once the right child has been fully consumed into `right_set`.
    materialized: bool,
    /// When `true`, exclude tuples found in `right_set` (`EXCEPT`).
    /// When `false`, include only tuples found in `right_set` (`INTERSECT`).
    exclude: bool,
}

impl MembershipFilter {
    fn new(left: Box<PlanNode>, right: Box<PlanNode>, exclude: bool) -> Self {
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
        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for MembershipFilter {
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

impl Executor for MembershipFilter {
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
/// Wraps [`MembershipFilter`] with `exclude = false`. The right child is materialized in
/// full before streaming begins; the left child is then streamed and only tuples present
/// in the right set are returned.
#[derive(Debug)]
pub struct Intersect(MembershipFilter);

impl Intersect {
    /// Creates a new `Intersect` operator over `left` and `right`.
    pub fn new(left: Box<PlanNode>, right: Box<PlanNode>) -> Self {
        Self(MembershipFilter::new(left, right, false))
    }
}

impl FallibleIterator for Intersect {
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

impl Executor for Intersect {
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
/// Wraps [`MembershipFilter`] with `exclude = true`. The right child is materialized in
/// full before streaming begins; the left child is then streamed and only tuples absent
/// from the right set are returned.
#[derive(Debug)]
pub struct Except(MembershipFilter);

impl Except {
    /// Creates a new `Except` operator over `left` and `right`.
    pub fn new(left: Box<PlanNode>, right: Box<PlanNode>) -> Self {
        Self(MembershipFilter::new(left, right, true))
    }
}

impl FallibleIterator for Except {
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

impl Executor for Except {
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
pub struct Distinct {
    child: Box<PlanNode>,
    seen: HashSet<Tuple>,
}

impl Distinct {
    /// Creates a new `Distinct` operator wrapping `child`.
    pub fn new(child: Box<PlanNode>) -> Self {
        Self {
            child,
            seen: HashSet::new(),
        }
    }
}

impl FallibleIterator for Distinct {
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
            if self.seen.insert(tuple.clone()) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for Distinct {
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
        self.child.rewind()
    }
}
