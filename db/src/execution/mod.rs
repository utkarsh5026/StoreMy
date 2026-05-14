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
//! - [`scan`]       — sequential and index scans
//! - [`unary`]      — `Filter`, `Project`, `Sort`, `Limit`
//! - [`join`]       — nested-loop, hash, and sort-merge joins
//! - [`setops`]     — union, intersect, except, distinct
//! - [`aggregate`]  — grouping and aggregation
//! - [`expression`] — `BooleanExpression`, the predicate primitive shared by filter and join
//!   operators

pub mod aggregate;
pub mod eval;
pub mod expression;
pub mod join;
pub mod scan;
pub mod setops;
pub mod unary;

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use crate::{
    TransactionId,
    execution::{expression::BooleanExpression, setops::Distinct},
    heap::file::HeapFile,
    index::AnyIndex,
    primitives::ColumnId,
    tuple::{Tuple, TupleSchema},
};

/// Errors produced by the execution engine.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("this operator does not support rewind")]
    RewindNotSupported,

    #[error("type error: {0}")]
    TypeError(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("index error: {0}")]
    Index(String),
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
pub enum PlanNode<'a> {
    SeqScan(scan::SeqScan<'a>),
    IndexScan(scan::IndexScan<'a>),

    Filter(unary::Filter<'a>),
    Project(unary::Project<'a>),
    Sort(unary::Sort<'a>),
    Limit(unary::Limit<'a>),

    NestedLoopJoin(join::NestedLoopJoin<'a>),
    HashJoin(join::HashJoin<'a>),
    SortMergeJoin(join::SortMergeJoin<'a>),

    Union(setops::Union<'a>),
    Intersect(setops::Intersect<'a>),
    Except(setops::Except<'a>),
    Distinct(setops::Distinct<'a>),

    Aggregate(aggregate::Aggregate<'a>),
}

impl<'a> PlanNode<'a> {
    pub fn seq_scan(file: &'a HeapFile, txn: TransactionId) -> Self {
        Self::SeqScan(scan::SeqScan::new(file, txn))
    }

    pub fn index_scan(
        heap: &'a HeapFile,
        index: &'a AnyIndex,
        txn: TransactionId,
        spec: scan::IndexScanSpec,
    ) -> Self {
        Self::IndexScan(scan::IndexScan::new(heap, index, txn, spec))
    }

    pub fn filter(child: Self, predicate: BooleanExpression) -> Self {
        Self::Filter(unary::Filter::new(Box::new(child), predicate))
    }

    pub fn distinct(child: Self) -> Self {
        Self::Distinct(Distinct::new(Box::new(child)))
    }

    pub fn sort(child: Self, keys: Vec<unary::SortKey>) -> Self {
        Self::Sort(unary::Sort::new(keys, Box::new(child)))
    }

    pub fn limit(child: Self, limit: u64, offset: u64) -> Self {
        Self::Limit(unary::Limit::new(Box::new(child), limit, offset))
    }

    pub fn project(child: Self, items: Vec<unary::ProjectItem>) -> Result<Self, ExecutionError> {
        Ok(Self::Project(unary::Project::with_items(
            Box::new(child),
            items,
        )?))
    }

    pub fn nested_loop_join(left: Self, right: Self, predicate: BooleanExpression) -> Self {
        Self::NestedLoopJoin(join::NestedLoopJoin::new(left, right, predicate))
    }

    pub fn hash_join(left: Self, right: Self, predicate: join::JoinPredicate) -> Self {
        Self::HashJoin(join::HashJoin::new(left, right, predicate))
    }

    pub fn aggregate(
        child: Self,
        group_by: &[ColumnId],
        agg_exprs: Vec<aggregate::AggregateExpr>,
    ) -> Result<Self, ExecutionError> {
        Ok(Self::Aggregate(aggregate::Aggregate::new(
            child, group_by, agg_exprs,
        )?))
    }
}

/// Helper macro to delegate method calls to the correct operator in a `PlanNode`.
///
/// This macro matches on the concrete variant of `PlanNode` and calls the specified
/// method (`$method`) with the given arguments (`$($arg),*`). It is used to uniformly
/// dispatch trait method implementations (such as `next`, `rewind`, or `schema`) to
/// the correct wrapped operator, so that `PlanNode` can transparently forward calls
/// without manual match boilerplate everywhere.
///
/// # Example
///
/// ```ignore
/// impl FallibleIterator for PlanNode<'_> {
///     fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
///         dispatch!(self, next())
///     }
/// }
/// ```
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

/// Implements the [`FallibleIterator`] trait for [`PlanNode`].
///
/// This allows `PlanNode` to be used as a fallible iterator over [`Tuple`]s
/// (with [`ExecutionError`] as the error type), abstracting over all physical
/// operator variants. The actual implementation for each operator is delegated
/// using the `dispatch!` macro, which forwards the method call to the
/// concrete child operator wrapped by this `PlanNode`.
impl FallibleIterator for PlanNode<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances the iterator and returns the next tuple produced by this
    /// plan node, or `None` when iteration is finished. Any execution error
    /// encountered during operator execution is returned as an error.
    ///
    /// The call is dispatched to the concrete operator implementation.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        dispatch!(self, next())
    }
}

/// Implements the [`Executor`] trait for [`PlanNode`].
///
/// This enables every `PlanNode` to expose its output schema and to be
/// rewound ("reset" for re-execution) in a uniform way by forwarding
/// calls to the appropriate child operator via the `dispatch!` macro.
impl Executor for PlanNode<'_> {
    /// Returns a reference to the [`TupleSchema`] describing the output
    /// columns produced by this node. Calls the `schema` method of the
    /// inner operator.
    fn schema(&self) -> &TupleSchema {
        dispatch!(self, schema())
    }

    /// Resets the plan node so that iteration may start again from the
    /// beginning. The actual rewinding logic is implemented by each
    /// concrete operator.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        dispatch!(self, rewind())
    }
}
