//! Single-child (unary) execution operators.
//!
//! Each operator in this module wraps one child [`PlanNode`] and transforms
//! the stream of tuples it produces without combining multiple inputs.
//!
//! | Operator | SQL equivalent              |
//! |----------|-----------------------------|
//! | [`Filter`]  | `WHERE <predicate>`      |
//! | [`Project`] | `SELECT col1, col2, …`   |
//! | [`Sort`]    | `ORDER BY col [ASC|DESC]` |
//! | [`Limit`]   | `LIMIT n OFFSET m`        |
//!
//! All operators implement [`Executor`] (and therefore [`FallibleIterator`]),
//! so they can be composed freely into a plan tree via `Box<PlanNode>`.

use std::cmp::Ordering;

use fallible_iterator::FallibleIterator;

use crate::{
    execution::PlanNode,
    primitives::{self, Predicate},
    tuple::{Tuple, TupleSchema},
    types::Value,
};

use super::{ExecutionError, Executor};

/// Keeps only the tuples from its child that satisfy a single column predicate.
///
/// Corresponds to a SQL `WHERE` clause of the form `col <op> value`, where
/// `op` is one of the comparisons in [`Predicate`]. Multi-column or compound
/// predicates are expressed by chaining multiple `Filter` nodes.
///
/// The output schema is identical to the child's — no columns are added or
/// removed by filtering.
#[derive(Debug)]
pub struct Filter {
    child: Box<PlanNode>,
    col_id: primitives::ColumnId,
    op: Predicate,
    operand: Value,
}

impl Filter {
    /// Returns `true` if `tuple` satisfies the filter predicate.
    ///
    /// Returns `false` when the column index is out of bounds for the tuple,
    /// or when a `LIKE` pattern is applied to non-string values.
    fn eval(&self, tuple: &Tuple) -> bool {
        let col_index = usize::from(self.col_id);
        let Some(val) = tuple.get(col_index) else {
            return false;
        };
        match self.op {
            Predicate::Equals => val == &self.operand,
            Predicate::NotEqual | Predicate::NotEqualBracket => val != &self.operand,
            Predicate::LessThan => val.partial_cmp(&self.operand).is_some_and(Ordering::is_lt),
            Predicate::LessThanOrEqual => {
                val.partial_cmp(&self.operand).is_some_and(Ordering::is_le)
            }
            Predicate::GreaterThan => val.partial_cmp(&self.operand).is_some_and(Ordering::is_gt),
            Predicate::GreaterThanOrEqual => {
                val.partial_cmp(&self.operand).is_some_and(Ordering::is_ge)
            }
            Predicate::Like => match (val, &self.operand) {
                (Value::String(s), Value::String(pattern)) => Self::like_match(s, pattern),
                _ => false,
            },
        }
    }

    /// Checks whether `s` matches the SQL `LIKE` pattern in `pattern`.
    ///
    /// Supports two wildcards:
    /// - `%` — matches any sequence of zero or more characters.
    /// - `_` — matches exactly one character.
    ///
    /// Uses a DP approach over the pattern to handle overlapping `%` spans
    /// correctly and in O(|s| × |p|) time.
    fn like_match(s: &str, pattern: &str) -> bool {
        let s = s.as_bytes();
        let p = pattern.as_bytes();
        let mut dp = vec![false; p.len() + 1];
        dp[0] = true;
        for (j, &pc) in p.iter().enumerate() {
            if pc == b'%' {
                dp[j + 1] = dp[j];
            }
        }
        for &sc in s {
            let mut prev = dp[0];
            dp[0] = false;
            for (j, &pc) in p.iter().enumerate() {
                let next = match pc {
                    b'%' => dp[j + 1] || dp[j],
                    b'_' => prev,
                    c => prev && c == sc,
                };
                prev = dp[j + 1];
                dp[j + 1] = next;
            }
        }
        *dp.last().unwrap_or(&false)
    }
}

impl Filter {
    /// Creates a new `Filter` operator.
    ///
    /// - `child` — the upstream operator to filter.
    /// - `col_id` — the column to test against `operand`.
    /// - `op` — the comparison predicate.
    /// - `operand` — the right-hand value of the comparison.
    pub fn new(
        child: Box<PlanNode>,
        col_id: primitives::ColumnId,
        op: Predicate,
        operand: Value,
    ) -> Self {
        Self { child, col_id, op, operand }
    }
}

impl FallibleIterator for Filter {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(tuple) = self.child.next()? {
            if self.eval(&tuple) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for Filter {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

/// Picks a subset of columns from each tuple produced by its child.
///
/// Corresponds to the column list in a SQL `SELECT`, e.g.
/// `SELECT id, name FROM …` projects two columns out of however many the
/// child exposes.
///
/// The output schema contains only the projected fields, in the order given
/// by `col_ids` at construction time.
#[derive(Debug)]
pub struct Project {
    child: Box<PlanNode>,
    col_indices: Vec<usize>,
    output_schema: TupleSchema,
}

impl Project {
    /// Creates a new `Project` operator that selects `col_ids` from each tuple.
    ///
    /// Column indices are validated against the child's schema immediately —
    /// any out-of-bounds `ColumnId` causes an error here rather than silently
    /// dropping values during iteration.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any `ColumnId` in `col_ids`
    /// refers to a column index that does not exist in the child's schema.
    pub fn new(
        child: Box<PlanNode>,
        col_ids: &[primitives::ColumnId],
    ) -> Result<Self, ExecutionError> {
        let col_indices = col_ids
            .iter()
            .map(|&c| usize::from(c))
            .collect::<Vec<usize>>();

        let output_schema = child
            .schema()
            .project(&col_indices)
            .map_err(|e| ExecutionError::TypeError(e.to_string()))?;
        Ok(Self {
            child,
            col_indices,
            output_schema,
        })
    }
}

impl FallibleIterator for Project {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        match self.child.next()? {
            Some(tuple) => Ok(Some(tuple.project(&self.col_indices))),
            None => Ok(None),
        }
    }
}

impl Executor for Project {
    fn schema(&self) -> &TupleSchema {
        &self.output_schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

/// Sorts all tuples from its child by a single column, either ascending or descending.
///
/// Corresponds to `ORDER BY col [ASC | DESC]` in SQL. Because sorting requires
/// seeing the full input before producing any output, `Sort` is a **blocking**
/// operator: on the first call to `next` it drains the child completely into
/// memory, sorts the collected tuples, and then yields them one by one.
///
/// The output schema is identical to the child's.
#[derive(Debug)]
pub struct Sort {
    ascending: bool,
    col_id: primitives::ColumnId,
    child: Box<PlanNode>,
    sorted: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl Sort {
    /// Creates a new `Sort` operator.
    ///
    /// - `ascending` — pass `true` for `ASC`, `false` for `DESC`.
    /// - `col_id` — the column to sort by.
    /// - `child` — the upstream operator to drain.
    pub fn new(ascending: bool, col_id: primitives::ColumnId, child: Box<PlanNode>) -> Self {
        Self {
            ascending,
            col_id,
            child,
            sorted: Vec::new(),
            cursor: 0,
            materialized: false,
        }
    }

    /// Drains the child into `self.sorted` and sorts it, but only on the first call.
    ///
    /// Subsequent calls return immediately because `self.materialized` is `true`.
    ///
    /// `NULL` values sort before all non-null values. Incomparable non-null values
    /// (which should not arise for well-typed data) are treated as equal.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's `next`.
    fn materialize_tuples(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        while let Some(tuple) = self.child.next()? {
            self.sorted.push(tuple);
        }

        let col_index = usize::from(self.col_id);
        self.sorted.sort_by(|a, b| {
            let ord = match (a.get(col_index), b.get(col_index)) {
                (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            if self.ascending { ord } else { ord.reverse() }
        });

        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for Sort {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_tuples()?;
        if self.cursor >= self.sorted.len() {
            return Ok(None);
        }
        let tuple = self.sorted[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(tuple))
    }
}

impl Executor for Sort {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.cursor = 0;
        Ok(())
    }
}

/// Restricts the number of tuples produced by its child, with an optional starting offset.
///
/// Corresponds to `LIMIT n OFFSET m` in SQL. The first `offset` tuples from the
/// child are consumed and discarded on the initial `next` call; after that, at
/// most `limit` tuples are returned before the operator signals end-of-stream.
///
/// The output schema is identical to the child's.
#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct Limit {
    limit: u64,
    offset: u64,
    count: u64,
    child: Box<PlanNode>,
    initialized: bool,
}

impl Limit {
    /// Creates a new `Limit` operator.
    ///
    /// - `child` — the upstream operator to restrict.
    /// - `limit` — maximum number of tuples to return.
    /// - `offset` — number of leading tuples to skip before counting starts.
    pub fn new(child: Box<PlanNode>, limit: u64, offset: u64) -> Self {
        Self { child, limit, offset, count: 0, initialized: false }
    }

    /// Skips the first `self.offset` tuples from the child, but only once.
    ///
    /// If the child is exhausted before the full offset is consumed, the skip
    /// stops early and the operator will immediately return `None` on the next
    /// `next` call.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's `next`.
    fn skip_offset(&mut self) -> Result<(), ExecutionError> {
        if self.initialized {
            return Ok(());
        }
        for _ in 0..self.offset {
            let Some(_) = self.child.next()? else {
                break;
            };
        }
        self.initialized = true;
        Ok(())
    }
}

impl FallibleIterator for Limit {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.skip_offset()?;
        if self.count >= self.limit {
            return Ok(None);
        }

        let ch = self.child.next()?;
        match ch {
            Some(tup) => {
                self.count += 1;
                Ok(Some(tup))
            }
            None => Ok(None),
        }
    }
}

impl Executor for Limit {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.count = 0;
        self.initialized = false;
        self.child.rewind()
    }
}
