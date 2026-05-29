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
//! - [`eval`]       — `ColumnLookup` trait, `ResolvedExpr` type, and `ResolvedExpr::eval` — column
//!   names resolved once via `ResolvedExpr::resolve`, evaluated with plain index lookups. JSON
//!   operator evaluation lives in [`eval::json`].

pub mod aggregate;
pub mod eval;
pub mod join;
pub mod scan;
pub mod setops;
pub mod unary;

pub use eval::{ColumnLookup, ResolvedCaseBranch, ResolvedExpr};
use fallible_iterator::FallibleIterator;
use thiserror::Error;

use crate::{
    TransactionId,
    execution::setops::Distinct,
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

/// A buffer of materialized tuples with a forwarding cursor.
///
/// Used by any operator that must fully buffer its input before emitting output
/// — [`Aggregate`](aggregate::Aggregate) for group finalization, and both sides
/// of [`SortMergeJoin`](join::SortMergeJoin) for sort-then-merge.
///
/// The cursor advances with [`forward`](Self::forward) and resets to the start
/// with [`reset`](Self::reset), so the same buffer can be replayed without
/// re-materializing.
#[derive(Debug, Default)]
pub struct TupleCursor {
    pub(crate) tuples: Vec<Tuple>,
    cursor: usize,
}

impl TupleCursor {
    /// Creates an empty cursor positioned before any tuple.
    pub fn new() -> Self {
        Self {
            tuples: Vec::new(),
            cursor: 0,
        }
    }

    /// Creates a cursor pre-loaded with `tuples`, positioned at the first element.
    pub fn from_vec(tuples: Vec<Tuple>) -> Self {
        Self { tuples, cursor: 0 }
    }

    /// Advances the cursor by one position.
    pub fn forward(&mut self) {
        self.cursor += 1;
    }

    /// Returns `true` when the cursor has moved past the last tuple.
    pub fn exhausted(&self) -> bool {
        self.cursor >= self.tuples.len()
    }

    /// Returns a reference to the tuple at the current cursor position.
    ///
    /// # Panics
    ///
    /// Panics if called when [`exhausted`](Self::exhausted) is `true`.
    pub fn current(&self) -> &Tuple {
        self.tuples
            .get(self.cursor)
            .expect("cursor is not exhausted")
    }

    /// Returns the raw cursor index (0-based position of the current tuple).
    pub fn current_idx(&self) -> usize {
        self.cursor
    }

    /// Resets the cursor to the beginning without clearing the buffered tuples.
    pub fn reset(&mut self) {
        self.cursor = 0;
    }
}

impl std::ops::Index<usize> for TupleCursor {
    type Output = Tuple;

    fn index(&self, index: usize) -> &Self::Output {
        &self.tuples[index]
    }
}

impl std::ops::IndexMut<usize> for TupleCursor {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.tuples[index]
    }
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

    /// Returns the number of physical fields in the output schema of this operator.
    ///
    /// This accounts for all underlying representation fields that may be used for
    /// disk or memory layout purposes, which may differ from the logical column count.
    fn physical_num_fields(&self) -> usize {
        self.schema().physical_num_fields()
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

    CrossJoin(join::CrossJoin<'a>),
    NestedLoopJoin(join::NestedLoopJoin<'a>),
    HashJoin(join::HashJoin<'a>),
    SortMergeJoin(join::SortMergeJoin<'a>),

    Union(setops::Union<'a>),
    Intersect(setops::Intersect<'a>),
    Except(setops::Except<'a>),
    Distinct(setops::Distinct<'a>),

    Aggregate(aggregate::Aggregate<'a>),
}

/// Selects the physical join algorithm and carries its algorithm-specific predicate.
///
/// Pass this to [`PlanNode::join`] together with a [`join::JoinType`] to build any join node
/// without needing a separate factory function per algorithm × join-type combination.
pub enum JoinAlgo {
    /// Cartesian product — no predicate. [`join::JoinType`] is ignored.
    Cross,
    /// Nested-loop join evaluated against an arbitrary boolean expression.
    NestedLoop(ResolvedExpr),
    /// Hash join keyed on a single equality column pair.
    Hash(join::JoinPredicate),
    /// Sort-merge join keyed on a single equality column pair.
    SortMerge(join::JoinPredicate),
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

    pub fn filter(child: Self, predicate: ResolvedExpr) -> Self {
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

    /// Builds a join node for `left ⋈ right`.
    ///
    /// `algo` picks the physical algorithm and carries its predicate; `join_type` controls
    /// how unmatched rows are handled. [`JoinAlgo::Cross`] ignores `join_type`.
    pub fn join(left: Self, right: Self, algo: JoinAlgo, join_type: join::JoinType) -> Self {
        match algo {
            JoinAlgo::Cross => Self::CrossJoin(join::CrossJoin::new(left, right)),
            JoinAlgo::NestedLoop(expr) => Self::NestedLoopJoin(
                join::NestedLoopJoin::new(left, right, expr).with_join_type(join_type),
            ),
            JoinAlgo::Hash(pred) => {
                Self::HashJoin(join::HashJoin::new(left, right, pred).with_join_type(join_type))
            }
            JoinAlgo::SortMerge(pred) => Self::SortMergeJoin(
                join::SortMergeJoin::new(left, right, pred).with_join_type(join_type),
            ),
        }
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
            PlanNode::CrossJoin(op)      => op.$method($($arg),*),
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
