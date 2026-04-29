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

use super::{ExecutionError, Executor, expression::BooleanExpression};
use crate::{
    execution::PlanNode,
    primitives,
    tuple::{Field, Tuple, TupleSchema},
    types::{Type, Value},
};

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
            if self.predicate.eval(&tuple)? {
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

/// One output column produced by [`Project`].
///
/// Either picks a column out of the child tuple by index, or emits a
/// constant value that is repeated on every output row.
#[derive(Debug, Clone)]
pub enum ProjectItem {
    /// Pick column `idx` from the child tuple.
    Column(usize),
    /// Emit `value` on every row, labeled `name` in the output schema.
    Literal { value: Value, name: String },
}

impl ProjectItem {
    /// Convenience constructor matching [`Project::new`]'s old signature.
    pub fn column(col_id: primitives::ColumnId) -> Self {
        Self::Column(usize::from(col_id))
    }

    /// Builds a literal output column. `NULL` literals default to
    /// [`Type::String`] since SQL `NULL` carries no inherent type.
    pub fn literal(value: Value, name: impl Into<String>) -> Self {
        Self::Literal {
            value,
            name: name.into(),
        }
    }
}

/// Picks a subset of columns from each tuple produced by its child, optionally
/// interleaving constant SQL literals.
///
/// Corresponds to the column list in a SQL `SELECT`. For every input row the
/// operator builds an output row by walking its [`ProjectItem`]s in order:
/// `Column(i)` copies the i-th value out of the child tuple; `Literal { .. }`
/// clones the constant.
///
/// The output schema is computed once at construction time — column items copy
/// the field from the child schema, literal items synthesize a [`Field`] from
/// the literal's runtime type.
#[derive(Debug)]
pub struct Project<'a> {
    child: Box<PlanNode<'a>>,
    items: Vec<ProjectItem>,
    output_schema: TupleSchema,
}

impl<'a> Project<'a> {
    /// Creates a `Project` from a list of column indices — every output
    /// column is taken straight from the child tuple.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any `ColumnId` refers to an
    /// index that does not exist in the child's schema.
    pub fn new(
        child: Box<PlanNode<'a>>,
        col_ids: &[primitives::ColumnId],
    ) -> Result<Self, ExecutionError> {
        let items: Vec<ProjectItem> = col_ids.iter().copied().map(ProjectItem::column).collect();
        Self::with_items(child, items)
    }

    /// Creates a `Project` from a mixed list of column picks and literals.
    ///
    /// Use this when the SQL projection contains constant expressions like
    /// `SELECT 1, 'guest' AS role, name FROM users`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any [`ProjectItem::Column`]
    /// refers to an out-of-range index in the child's schema.
    pub fn with_items(
        child: Box<PlanNode<'a>>,
        items: Vec<ProjectItem>,
    ) -> Result<Self, ExecutionError> {
        let child_schema = child.schema();
        let fields = items
            .iter()
            .map(|item| match item {
                ProjectItem::Column(idx) => {
                    let field = child_schema.field(*idx).ok_or_else(|| {
                        ExecutionError::TypeError(format!(
                            "Project column index {idx} out of range"
                        ))
                    })?;
                    Ok(field.clone())
                }
                ProjectItem::Literal { value, name } => {
                    let ty = value.get_type().unwrap_or(Type::String);
                    let mut field = Field::new(name.clone(), ty);
                    if !value.is_null() {
                        field = field.not_null();
                    }
                    Ok(field)
                }
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(Self {
            child,
            items,
            output_schema: TupleSchema::new(fields),
        })
    }
}

impl FallibleIterator for Project<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        let Some(input) = self.child.next()? else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(self.items.len());
        for item in &self.items {
            match item {
                ProjectItem::Column(idx) => match input.get(*idx) {
                    Some(v) => out.push(v.clone()),
                    None => {
                        return Err(ExecutionError::TypeError(format!(
                            "Project: column index {idx} missing from input tuple"
                        )));
                    }
                },
                ProjectItem::Literal { value, .. } => out.push(value.clone()),
            }
        }
        Ok(Some(Tuple::new(out)))
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

/// One column of an `ORDER BY` clause: which column, and which direction.
///
/// Multiple `SortKey`s are evaluated lexicographically — the first key
/// decides the ordering, and later keys are only consulted to break ties.
#[derive(Debug, Clone, Copy)]
pub struct SortKey {
    pub col_id: primitives::ColumnId,
    pub ascending: bool,
}

impl SortKey {
    pub fn asc(col_id: primitives::ColumnId) -> Self {
        Self {
            col_id,
            ascending: true,
        }
    }

    pub fn desc(col_id: primitives::ColumnId) -> Self {
        Self {
            col_id,
            ascending: false,
        }
    }
}

/// Sorts all tuples from its child by an ordered list of keys.
///
/// Corresponds to `ORDER BY c1 [ASC|DESC], c2 [ASC|DESC], …` in SQL.
/// Because sorting requires seeing the full input before producing any
/// output, `Sort` is a **blocking** operator: on the first call to `next`
/// it drains the child completely into memory, sorts the collected
/// tuples, and then yields them one by one.
///
/// The output schema is identical to the child's.
#[derive(Debug)]
pub struct Sort<'a> {
    keys: Vec<SortKey>,
    child: Box<PlanNode<'a>>,
    sorted: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl<'a> Sort<'a> {
    /// Creates a new `Sort` operator.
    ///
    /// - `keys` — the ordered list of `(column, direction)` pairs. The first key is the primary
    ///   sort key; subsequent keys break ties.
    /// - `child` — the upstream operator to drain.
    pub fn new(keys: Vec<SortKey>, child: Box<PlanNode<'a>>) -> Self {
        Self {
            keys,
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
    /// `NULL` values sort before all non-null values for each key. Each key's
    /// `ascending` flag only flips the contribution of that key — it does not
    /// reverse the whole comparator. Incomparable non-null values (which should
    /// not arise for well-typed data) are treated as equal for that key.
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

        let keys = self.keys.clone();
        self.sorted.sort_by(|a, b| {
            for key in &keys {
                let ord = Self::compare_by_sort_key(*key, a, b);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        self.materialized = true;
        Ok(())
    }

    /// Compares two tuples `a` and `b` using the specified `SortKey`.
    ///
    /// This function is used to determine the row order in the `Sort` operator.
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key specifying the column and direction by which to compare the tuples.
    /// * `a` - The first tuple to compare.
    /// * `b` - The second tuple to compare.
    ///
    /// # Returns
    ///
    /// - [`Ordering::Less`] if `a` should come before `b` according to the sort key.
    /// - [`Ordering::Greater`] if `a` should come after `b`.
    /// - [`Ordering::Equal`] if the tuples are equal on this sort key.
    ///
    /// # Details
    ///
    /// - The value at the column specified by `key.col_id` is compared between `a` and `b`.
    /// - `NULL` values (represented as `None`) sort before all non-null values for each key.
    /// - If both values are non-null but not comparable (e.g., type mismatch), they are considered
    ///   equal.
    /// - The `ascending` flag on the key determines whether the natural ordering is used (`true`)
    ///   or reversed (`false`—i.e., descending).
    fn compare_by_sort_key(key: SortKey, a: &Tuple, b: &Tuple) -> Ordering {
        let idx = usize::from(key.col_id);
        let ord = match (a.get(idx), b.get(idx)) {
            (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(Ordering::Equal),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if key.ascending { ord } else { ord.reverse() }
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
        primitives::{ColumnId, Predicate},
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

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

    #[test]
    fn test_filter_next_yields_matching_tuples_only() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(2, false)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(3, true)).unwrap();

        let pred = BooleanExpression::col_op_lit(1, Predicate::Equals, Value::Bool(true));
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
                vec![SortKey::asc(col)],
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
            vec![SortKey::desc(col)],
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
    fn test_sort_multi_key_breaks_ties_with_secondary() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        // Same id (1), different flag — secondary key decides the order.
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(1, false)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(2, true)).unwrap();

        // ORDER BY id ASC, flag DESC  =>  (1,true), (1,false), (2,true)
        let mut sort = Sort::new(
            vec![
                SortKey::asc(ColumnId::try_from(0u32).unwrap()),
                SortKey::desc(ColumnId::try_from(1u32).unwrap()),
            ],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut out = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            out.push(t);
        }
        assert_eq!(out, vec![
            make_scan_tuple(1, true),
            make_scan_tuple(1, false),
            make_scan_tuple(2, true),
        ]);
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
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
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
            BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(0)),
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
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
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
            BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(7)),
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
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
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
