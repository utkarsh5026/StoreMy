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

use super::{ExecutionError, Executor};
use crate::{
    execution::PlanNode,
    primitives::{self, Predicate},
    tuple::{Tuple, TupleSchema},
    types::Value,
};

/// A boolean expression that can be evaluated against a tuple.
/// The expression is evaluated using the `eval` method.
#[derive(Debug, Clone)]
pub enum BooleanExpression {
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    Or(Box<BooleanExpression>, Box<BooleanExpression>),
    Not(Box<BooleanExpression>),
    Leaf {
        col_index: usize,
        op: Predicate,
        operand: Value,
    },
}

impl BooleanExpression {
    /// Evaluates the boolean expression against a tuple.
    ///
    /// Returns `true` if the expression is satisfied, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns `false` if the column index is out of bounds for the tuple,
    /// or when a `LIKE` pattern is applied to non-string values.
    pub fn eval(&self, tuple: &Tuple) -> bool {
        match self {
            Self::Leaf {
                col_index,
                op,
                operand,
            } => {
                let Some(val) = tuple.get(*col_index) else {
                    return false;
                };

                match op {
                    Predicate::Equals => val == operand,
                    Predicate::NotEqual | Predicate::NotEqualBracket => val != operand,
                    Predicate::LessThan => val.partial_cmp(operand).is_some_and(Ordering::is_lt),
                    Predicate::LessThanOrEqual => {
                        val.partial_cmp(operand).is_some_and(Ordering::is_le)
                    }
                    Predicate::GreaterThan => val.partial_cmp(operand).is_some_and(Ordering::is_gt),
                    Predicate::GreaterThanOrEqual => {
                        val.partial_cmp(operand).is_some_and(Ordering::is_ge)
                    }
                    Predicate::Like => match (val, operand) {
                        (Value::String(s), Value::String(p)) => Self::like_match(s, p),
                        _ => false,
                    },
                }
            }
            Self::And(left, right) => left.eval(tuple) && right.eval(tuple),
            Self::Or(left, right) => left.eval(tuple) || right.eval(tuple),
            Self::Not(expr) => !expr.eval(tuple),
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
    fn like_match(s: &str, p: &str) -> bool {
        let s: Vec<char> = s.chars().collect();
        let p: Vec<char> = p.chars().collect();
        let n = s.len();
        let m = p.len();
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[0][0] = true;
        for j in 1..=m {
            if p[j - 1] == '%' {
                dp[0][j] = dp[0][j - 1];
            }
        }
        for i in 1..=n {
            for j in 1..=m {
                let pc = p[j - 1];
                dp[i][j] = if pc == '%' {
                    dp[i][j - 1] || dp[i - 1][j]
                } else if pc == '_' {
                    dp[i - 1][j - 1]
                } else {
                    dp[i - 1][j - 1] && s[i - 1] == pc
                };
            }
        }
        dp[n][m]
    }
}

/// Keeps only the tuples from its child that satisfy a single column predicate.
///
/// Corresponds to a SQL `WHERE` clause of the form `col <op> value`, where
/// `op` is one of the comparisons in [`Predicate`]. Multi-column or compound
/// predicates are expressed by chaining multiple `Filter` nodes.
///
/// The output schema is identical to the child's — no columns are added or
/// removed by filtering.
#[derive(Debug)]
pub struct Filter<'a> {
    child: Box<PlanNode<'a>>,
    predicate: BooleanExpression,
}

impl<'a> Filter<'a> {
    /// Creates a new `Filter` operator.
    ///
    /// - `child` — the upstream operator to filter.
    /// - `predicate` — the boolean expression to evaluate.
    pub fn new(child: Box<PlanNode<'a>>, predicate: BooleanExpression) -> Self {
        Self { child, predicate }
    }
}

impl FallibleIterator for Filter<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(tuple) = self.child.next()? {
            if self.predicate.eval(&tuple) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for Filter<'_> {
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
pub struct Project<'a> {
    child: Box<PlanNode<'a>>,
    col_indices: Vec<usize>,
    output_schema: TupleSchema,
}

impl<'a> Project<'a> {
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
        child: Box<PlanNode<'a>>,
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

impl FallibleIterator for Project<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        match self.child.next()? {
            Some(tuple) => Ok(Some(tuple.project(&self.col_indices))),
            None => Ok(None),
        }
    }
}

impl Executor for Project<'_> {
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
pub struct Sort<'a> {
    ascending: bool,
    col_id: primitives::ColumnId,
    child: Box<PlanNode<'a>>,
    sorted: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl<'a> Sort<'a> {
    /// Creates a new `Sort` operator.
    ///
    /// - `ascending` — pass `true` for `ASC`, `false` for `DESC`.
    /// - `col_id` — the column to sort by.
    /// - `child` — the upstream operator to drain.
    pub fn new(ascending: bool, col_id: primitives::ColumnId, child: Box<PlanNode<'a>>) -> Self {
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

impl FallibleIterator for Sort<'_> {
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

impl Executor for Sort<'_> {
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
pub struct Limit<'a> {
    limit: u64,
    offset: u64,
    count: u64,
    child: Box<PlanNode<'a>>,
    initialized: bool,
}

impl<'a> Limit<'a> {
    /// Creates a new `Limit` operator.
    ///
    /// - `child` — the upstream operator to restrict.
    /// - `limit` — maximum number of tuples to return.
    /// - `offset` — number of leading tuples to skip before counting starts.
    pub fn new(child: Box<PlanNode<'a>>, limit: u64, offset: u64) -> Self {
        Self {
            child,
            limit,
            offset,
            count: 0,
            initialized: false,
        }
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

impl FallibleIterator for Limit<'_> {
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

impl Executor for Limit<'_> {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.count = 0;
        self.initialized = false;
        self.child.rewind()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fallible_iterator::FallibleIterator;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        execution::{PlanNode, scan::SeqScan},
        heap::file::HeapFile,
        primitives::ColumnId,
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    // --- test helpers (SeqScan + heap; mirrors execution/scan.rs) ---

    fn scan_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("flag", Type::Bool),
        ])
    }

    fn make_scan_tuple(id: i32, flag: bool) -> Tuple {
        Tuple::new(vec![Value::Int32(id), Value::Bool(flag)])
    }

    fn begin_txn(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    fn make_registered_heap_file(existing_pages: u32) -> (HeapFile, Arc<Wal>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let file_id = FileId::new(1);
        let path = dir.path().join("heap.db");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let needed = (existing_pages as usize).max(4) * crate::storage::PAGE_SIZE;
        file.set_len(needed as u64).unwrap();
        drop(file);

        store.register_file(file_id, &path).unwrap();

        let heap = HeapFile::new(
            file_id,
            scan_schema(),
            Arc::clone(&store),
            existing_pages,
            Arc::clone(&wal),
        );
        (heap, wal, dir)
    }

    fn eval_like(s: &str, pattern: &str) -> bool {
        let expr = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::Like,
            operand: Value::String(pattern.to_string()),
        };
        let tuple = Tuple::new(vec![Value::String(s.to_string())]);
        expr.eval(&tuple)
    }

    #[test]
    fn test_boolean_expression_eval_leaf_equals() {
        let e = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::Equals,
            operand: Value::Int32(7),
        };
        assert!(e.eval(&Tuple::new(vec![Value::Int32(7), Value::Bool(true)])));
        assert!(!e.eval(&Tuple::new(vec![Value::Int32(8), Value::Bool(true)])));
    }

    #[test]
    fn test_boolean_expression_eval_leaf_ordered_preds() {
        let tup = Tuple::new(vec![Value::Int32(5), Value::Bool(false)]);

        let lt = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::LessThan,
            operand: Value::Int32(10),
        };
        assert!(lt.eval(&tup));

        let le = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::LessThanOrEqual,
            operand: Value::Int32(5),
        };
        assert!(le.eval(&tup));

        let gt = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::GreaterThan,
            operand: Value::Int32(1),
        };
        assert!(gt.eval(&tup));

        let ge = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::GreaterThanOrEqual,
            operand: Value::Int32(5),
        };
        assert!(ge.eval(&tup));
    }

    #[test]
    fn test_boolean_expression_eval_leaf_not_equal_and_bracket() {
        let tup = Tuple::new(vec![Value::Int32(1), Value::Bool(true)]);
        let ne = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::NotEqual,
            operand: Value::Int32(2),
        };
        let neb = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::NotEqualBracket,
            operand: Value::Int32(2),
        };
        assert!(ne.eval(&tup));
        assert!(neb.eval(&tup));
        assert!(!ne.eval(&Tuple::new(vec![Value::Int32(2), Value::Bool(true)])));
    }

    #[test]
    fn test_boolean_expression_eval_and_or_not() {
        let t = Tuple::new(vec![Value::Bool(true), Value::Bool(false)]);

        let a = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::Equals,
            operand: Value::Bool(true),
        };
        let b = BooleanExpression::Leaf {
            col_index: 1,
            op: Predicate::Equals,
            operand: Value::Bool(false),
        };

        assert!(BooleanExpression::And(Box::new(a.clone()), Box::new(b.clone())).eval(&t));
        assert!(
            !BooleanExpression::And(Box::new(a.clone()), Box::new(b.clone()))
                .eval(&Tuple::new(vec![Value::Bool(true), Value::Bool(true)]))
        );

        assert!(BooleanExpression::Or(Box::new(b.clone()), Box::new(a.clone())).eval(&t));
        assert!(
            BooleanExpression::Or(Box::new(a.clone()), Box::new(b.clone()))
                .eval(&Tuple::new(vec![Value::Bool(false), Value::Bool(false)]))
        );

        assert!(!BooleanExpression::Not(Box::new(a.clone())).eval(&t));

        let col0_false = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::Equals,
            operand: Value::Bool(false),
        };
        assert!(BooleanExpression::Not(Box::new(col0_false)).eval(&t));
    }

    // --- edge cases: BooleanExpression::eval ---

    #[test]
    fn test_boolean_expression_eval_leaf_column_out_of_bounds_returns_false() {
        let e = BooleanExpression::Leaf {
            col_index: 99,
            op: Predicate::Equals,
            operand: Value::Int32(0),
        };
        assert!(!e.eval(&Tuple::new(vec![Value::Int32(0)])));
    }

    #[test]
    fn test_boolean_expression_eval_leaf_like_non_string_returns_false() {
        let e = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::Like,
            operand: Value::String("%".to_string()),
        };
        assert!(!e.eval(&Tuple::new(vec![Value::Int32(1)])));
    }

    #[test]
    fn test_boolean_expression_eval_ordered_pred_incomparable_returns_false() {
        let cross = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::LessThan,
            operand: Value::Int64(100),
        };
        assert!(!cross.eval(&Tuple::new(vec![Value::Int32(1)])));

        let nan_cmp = BooleanExpression::Leaf {
            col_index: 0,
            op: Predicate::LessThan,
            operand: Value::Float64(0.0),
        };
        assert!(!nan_cmp.eval(&Tuple::new(vec![Value::Float64(f64::NAN)])));
    }

    // --- happy path: LIKE via eval ---

    #[test]
    fn test_boolean_expression_eval_like_exact_and_percent() {
        assert!(eval_like("hello", "hello"));
        assert!(eval_like("hello", "h%o"));
        assert!(eval_like("hello", "%ello"));
        assert!(eval_like("hello", "hel%"));
        assert!(eval_like("hello", "%ell%"));
        assert!(!eval_like("hello", "h%xo"));
    }

    #[test]
    fn test_boolean_expression_eval_like_underscore() {
        assert!(eval_like("a", "_"));
        assert!(eval_like("ab", "__"));
        assert!(eval_like("axb", "a_b"));
        assert!(!eval_like("ab", "___"));
        assert!(!eval_like("", "_"));
    }

    #[test]
    fn test_boolean_expression_eval_like_empty_string_patterns() {
        assert!(eval_like("", ""));
        assert!(eval_like("", "%"));
        assert!(eval_like("", "%%"));
        assert!(!eval_like("", "_"));
    }

    #[test]
    fn test_boolean_expression_eval_like_consecutive_percent() {
        assert!(eval_like("abc", "a%%bc"));
        assert!(eval_like("x", "%_%"));
    }

    #[test]
    fn test_boolean_expression_eval_like_pattern_longer_than_string() {
        assert!(!eval_like("hi", "hello"));
        assert!(!eval_like("a", "__"));
    }

    #[test]
    fn test_boolean_expression_eval_like_unicode_scalar() {
        assert!(eval_like("café", "caf_"));
        assert!(eval_like("café", "%é"));
        assert!(!eval_like("caf", "caf_"));
    }

    // --- happy path: operators + SeqScan ---

    #[test]
    fn test_filter_next_yields_matching_tuples_only() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(2, false)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(3, true)).unwrap();

        let pred = BooleanExpression::Leaf {
            col_index: 1,
            op: Predicate::Equals,
            operand: Value::Bool(true),
        };
        let mut filter = Filter::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), pred);

        let mut out = Vec::new();
        while let Some(t) = filter.next().unwrap() {
            out.push(t);
        }
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], make_scan_tuple(1, true));
        assert_eq!(out[1], make_scan_tuple(3, true));
    }

    #[test]
    fn test_project_new_reorders_columns() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(42, false)).unwrap();

        let mut proj = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(1u32).unwrap(),
            ColumnId::try_from(0u32).unwrap(),
        ])
        .unwrap();

        assert_eq!(proj.schema().field(0).unwrap().name, "flag");
        assert_eq!(proj.schema().field(1).unwrap().name, "id");

        let row = proj.next().unwrap().unwrap();
        assert_eq!(row, Tuple::new(vec![Value::Bool(false), Value::Int32(42)]));
    }

    #[test]
    fn test_sort_next_ascending_and_descending() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in [3_i32, 1, 2] {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let col = ColumnId::try_from(0u32).unwrap();

        {
            let mut asc = Sort::new(
                true,
                col,
                Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            );
            let mut asc_out = Vec::new();
            while let Some(t) = asc.next().unwrap() {
                asc_out.push(t);
            }
            assert_eq!(
                asc_out
                    .into_iter()
                    .map(|t| t.get(0).unwrap().clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
            );
        }

        // Same txn: a second read txn can block on page locks held until the first
        // scan's resources are fully released; one txn avoids lock handoff races.
        let mut desc = Sort::new(
            false,
            col,
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut desc_out = Vec::new();
        while let Some(t) = desc.next().unwrap() {
            desc_out.push(t);
        }
        assert_eq!(
            desc_out
                .into_iter()
                .map(|t| t.get(0).unwrap().clone())
                .collect::<Vec<_>>(),
            vec![Value::Int32(3), Value::Int32(2), Value::Int32(1)]
        );
    }

    #[test]
    fn test_sort_null_sorts_before_non_null() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &Tuple::new(vec![Value::Int32(10), Value::Bool(true)]))
            .unwrap();
        heap.insert_tuple(txn, &Tuple::new(vec![Value::Null, Value::Bool(false)]))
            .unwrap();

        let mut sort = Sort::new(
            true,
            ColumnId::try_from(0u32).unwrap(),
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let first = sort.next().unwrap().unwrap();
        assert_eq!(first.get(0), Some(&Value::Null));
        let second = sort.next().unwrap().unwrap();
        assert_eq!(second.get(0), Some(&Value::Int32(10)));
    }

    #[test]
    fn test_limit_next_offset_and_limit_window() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in 1_i32..=3 {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 1, 1);
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
    }

    // --- edge cases: operators ---

    #[test]
    fn test_filter_next_empty_child_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let mut filter = Filter::new(
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            BooleanExpression::Leaf {
                col_index: 0,
                op: Predicate::Equals,
                operand: Value::Int32(0),
            },
        );
        assert_eq!(filter.next().unwrap(), None);
    }

    #[test]
    fn test_project_new_invalid_column_id_returns_type_error() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let err = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(99u32).unwrap(),
        ])
        .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn test_sort_next_empty_child_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let mut sort = Sort::new(
            true,
            ColumnId::try_from(0u32).unwrap(),
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        assert_eq!(sort.next().unwrap(), None);
    }

    #[test]
    fn test_limit_next_limit_zero_returns_none_after_skip() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 0, 0);
        assert_eq!(lim.next().unwrap(), None);
    }

    #[test]
    fn test_limit_next_offset_past_end_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 5, 10);
        assert_eq!(lim.next().unwrap(), None);
    }

    // --- rewind / invariants ---

    #[test]
    fn test_filter_rewind_delegates_to_child() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(7, true)).unwrap();

        let mut filter = Filter::new(
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            BooleanExpression::Leaf {
                col_index: 0,
                op: Predicate::Equals,
                operand: Value::Int32(7),
            },
        );
        assert_eq!(filter.next().unwrap(), Some(make_scan_tuple(7, true)));
        filter.rewind().unwrap();
        assert_eq!(filter.next().unwrap(), Some(make_scan_tuple(7, true)));
    }

    #[test]
    fn test_project_rewind_delegates_to_child() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, false)).unwrap();

        let mut proj = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(0u32).unwrap(),
        ])
        .unwrap();
        assert_eq!(
            proj.next().unwrap(),
            Some(Tuple::new(vec![Value::Int32(1)]))
        );
        proj.rewind().unwrap();
        assert_eq!(
            proj.next().unwrap(),
            Some(Tuple::new(vec![Value::Int32(1)]))
        );
    }

    #[test]
    fn test_sort_rewind_replays_buffer() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in [2_i32, 3, 1] {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut sort = Sort::new(
            true,
            ColumnId::try_from(0u32).unwrap(),
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut first_pass = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            first_pass.push(t);
        }
        sort.rewind().unwrap();
        let mut second_pass = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            second_pass.push(t);
        }
        assert_eq!(first_pass, second_pass);
    }

    #[test]
    fn test_limit_rewind_resets_window() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in 1_i32..=3 {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 1, 1);
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
        lim.rewind().unwrap();
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
    }
}
