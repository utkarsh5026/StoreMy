//! Query execution engine.
//!
//! This module implements the volcano/iterator model for query execution.
//! Every operator in a query plan implements [`Executor`], which extends
//! [`FallibleIterator`] to produce tuples one at a time.
//!
//! ## Plan tree
//!
//! A query is represented as a tree of [`PlanNode`]s. Leaf nodes read from
//! storage (e.g. [`PlanNode::SeqScan`]); internal nodes transform or combine
//! tuples from their children (e.g. [`PlanNode::Filter`], [`PlanNode::HashJoin`]).
//!
//! ## Lifecycle
//!
//! Operators follow Rust's RAII model — resources are allocated in `new()` and
//! released when the operator is dropped. There is no explicit `open()`/`close()`.
//! Operators that need one-time setup (e.g. building a hash table) do so lazily
//! on the first [`FallibleIterator::next`] call.
//!
//! ## Modules
//!
//! - [`scan`]      — sequential and index scans
//! - [`filter`]    — predicate filtering
//! - [`project`]   — column projection
//! - [`sort`]      — in-memory sort
//! - [`limit`]     — row-count limiting
//! - [`join`]      — nested-loop, hash, and sort-merge joins
//! - [`setops`]    — union, intersect, except, distinct
//! - [`aggregate`] — grouping and aggregation

pub mod aggregate;
pub mod join;
pub mod scan;
pub mod setops;
pub mod unary;

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use crate::tuple::{Tuple, TupleSchema};

/// Errors produced by the execution engine.
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// An operator does not support rewinding (e.g. hash join).
    #[error("this operator does not support rewind")]
    RewindNotSupported,

    /// A type mismatch or invalid value was encountered during execution.
    #[error("type error: {0}")]
    TypeError(String),

    /// An I/O error from the storage layer.
    #[error("storage error: {0}")]
    Storage(String),
}

/// The core trait every execution operator must implement.
///
/// `Executor` extends [`FallibleIterator`] — so every operator is a fallible
/// iterator over [`Tuple`]s and gets `.map()`, `.filter()`, `.take()`,
/// `.collect()` etc. for free.
///
/// The two additional methods are:
/// - [`schema`](Self::schema) — the output schema of this operator
/// - [`rewind`](Self::rewind) — reset to the start (only some operators support this)
pub trait Executor: FallibleIterator<Item = Tuple, Error = ExecutionError> {
    /// Returns the output schema of this operator.
    fn schema(&self) -> &TupleSchema;

    /// Resets the operator to its initial state so it can be iterated again.
    ///
    /// Only sequential scans and simple wrappers support this.
    /// The default returns [`ExecutionError::RewindNotSupported`].
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        Err(ExecutionError::RewindNotSupported)
    }
}

/// A node in the physical query plan.
///
/// `PlanNode` is the single type that flows through the planner, optimizer,
/// and executor. Every variant wraps a concrete operator struct defined in one
/// of the sub-modules.
///
/// Because `PlanNode` implements [`Executor`] (and therefore [`FallibleIterator`]),
/// operator structs can hold their children as `Box<PlanNode>` and call
/// `.next()` / `.rewind()` directly.
#[derive(Debug)]
pub enum PlanNode {
    // ── Scans ────────────────────────────────────────────────────────────
    SeqScan(scan::SeqScan),
    IndexScan(scan::IndexScan),

    // ── Unary operators ──────────────────────────────────────────────────
    Filter(unary::Filter),
    Project(unary::Project),
    Sort(unary::Sort),
    Limit(unary::Limit),

    // ── Joins ────────────────────────────────────────────────────────────
    NestedLoopJoin(join::NestedLoopJoin),
    HashJoin(join::HashJoin),
    SortMergeJoin(join::SortMergeJoin),

    // ── Set operations ───────────────────────────────────────────────────
    Union(setops::Union),
    Intersect(setops::Intersect),
    Except(setops::Except),
    Distinct(setops::Distinct),

    Aggregate(aggregate::Aggregate),
}

macro_rules! dispatch {
    ($self:expr, $method:ident($($arg:expr),*)) => {
        match $self {
            PlanNode::SeqScan(op)        => op.$method($($arg),*),
            PlanNode::IndexScan(op)      => op.$method($($arg),*),
            PlanNode::Filter(op)         => op.$method($($arg),*),
            PlanNode::Project(op)        => op.$method($($arg),*),
            PlanNode::Sort(op)           => op.$method($($arg),*),
            PlanNode::Limit(op)          => op.$method($($arg),*),
            PlanNode::NestedLoopJoin(op) => op.$method($($arg),*),
            PlanNode::HashJoin(op)       => op.$method($($arg),*),
            PlanNode::SortMergeJoin(op)  => op.$method($($arg),*),
            PlanNode::Union(op)          => op.$method($($arg),*),
            PlanNode::Intersect(op)      => op.$method($($arg),*),
            PlanNode::Except(op)         => op.$method($($arg),*),
            PlanNode::Distinct(op)       => op.$method($($arg),*),
            PlanNode::Aggregate(op)      => op.$method($($arg),*),
        }
    };
}

impl FallibleIterator for PlanNode {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        dispatch!(self, next())
    }
}

impl Executor for PlanNode {
    fn schema(&self) -> &TupleSchema {
        dispatch!(self, schema())
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        dispatch!(self, rewind())
    }
}
