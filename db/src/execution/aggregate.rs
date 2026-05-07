//! Grouping and aggregation operator.
//!
//! This module provides the [`Aggregate`] operator, which corresponds to a SQL
//! `GROUP BY` clause with one or more aggregate functions in the `SELECT` list.
//!
//! ## How it works
//!
//! `Aggregate` is a **blocking** operator — it must consume the entire input
//! before it can produce any output. Execution is split into two phases:
//!
//! 1. **Accumulate** — drain the child, grouping rows by their `GROUP BY` column values. For each
//!    group, maintain one `Accumulator` per aggregate spec. This happens lazily on the first call
//!    to [`FallibleIterator::next`].
//!
//! 2. **Emit** — iterate over the finalized groups, yielding one [`Tuple`] per group. The tuple
//!    layout is: GROUP BY column values first, then one value per aggregate spec, in declaration
//!    order.
//!
//! A query with no `GROUP BY` is treated as a single group with an empty key,
//! so `SELECT COUNT(*) FROM t` always produces exactly one output row.
//!
//! ## NULL handling
//!
//! `COUNT(*)` counts every row. All other aggregates (`SUM`, `MIN`, `MAX`,
//! `AVG`, `COUNT(col)`) skip `NULL` values in the input. If every input value
//! for a group is `NULL`, the aggregate result is `NULL` (except `COUNT`, which
//! returns `0`).

use std::{cmp::Ordering, collections::HashMap};

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor};
use crate::{
    execution::PlanNode,
    primitives::{ColumnId, NonEmptyString},
    tuple::{Field, Tuple, TupleSchema},
    types::{Type, Value},
};

/// A SQL aggregate function.
///
/// Each variant corresponds to an aggregate you'd write in a `SELECT` list:
///
/// ```sql
/// SELECT COUNT(*)     FROM orders;     -- CountStar
/// SELECT COUNT(email) FROM users;      -- CountCol (skips NULL emails)
/// SELECT SUM(amount)  FROM payments;   -- Sum
/// SELECT MIN(price)   FROM products;   -- Min
/// SELECT MAX(price)   FROM products;   -- Max
/// SELECT AVG(score)   FROM results;    -- Avg
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunc {
    CountStar,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
}

/// Computes the SQL output type of an aggregate function for the given input column type.
///
/// This is used to infer the output [`Type`] of each aggregate in the `SELECT` list,
/// which determines the schema of each output row from the `Aggregate` operator.
///
/// - `COUNT(*)` and `COUNT(col)` always produce a [`Type::Int64`] result, regardless of input.
/// - `SUM` returns `Float64` when summing over a `Float64` input, or `Int64` otherwise (even for
///   integer types smaller than 64 bits).
/// - `AVG` always returns a `Float64`, matching typical SQL semantics.
/// - `MIN` and `MAX` produce the same type as the input column they operate on.
impl From<(&AggregateFunc, Type)> for Type {
    fn from((func, input_type): (&AggregateFunc, Type)) -> Self {
        match func {
            AggregateFunc::CountStar | AggregateFunc::CountCol => Type::Int64,
            AggregateFunc::Avg => Type::Float64,
            AggregateFunc::Sum => match input_type {
                Type::Float64 => Type::Float64,
                _ => Type::Int64,
            },
            AggregateFunc::Min | AggregateFunc::Max => input_type,
        }
    }
}

/// Binds an [`AggregateFunc`] to the column it operates on and names the output.
///
/// Think of one `AggregateExpr` as one aggregated item in the `SELECT` list.
/// For the query
///
/// ```sql
/// SELECT SUM(amount) AS total, COUNT(*) AS n
/// FROM payments
/// GROUP BY user_id;
/// ```
///
/// you would build two specs — one per aggregated output column — where
/// `col_id` points at the column in the child's schema:
///
/// ```ignore
/// // assuming `amount` is column index 2 in the child's schema
/// AggregateExpr { func: AggregateFunc::Sum,       col_id: col(2), output_name: "total".into() };
/// AggregateExpr { func: AggregateFunc::CountStar, col_id: col(0), output_name: "n".into()    };
/// ```
///
/// For `COUNT(*)` the `col_id` is ignored; conventionally pass column `0`.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub col_id: ColumnId,
    pub output_name: NonEmptyString,
}

/// Running state for one aggregate function over one group.
///
/// Created fresh for each new group key, updated row by row, then finalized
/// into a single [`Value`] once the child is exhausted.
#[derive(Debug)]
enum Accumulator {
    CountStar(u64),
    CountCol(u64),
    Sum(Option<Value>),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: u64 },
}

impl Accumulator {
    /// Creates an empty running state for `func`.
    ///
    /// The accumulator starts at the identity value for the aggregate:
    /// - `COUNT(*)` and `COUNT(col)` start at `0`
    /// - `SUM`/`MIN`/`MAX` start as "no value seen yet"
    /// - `AVG` starts at `(sum = 0.0, count = 0)`
    fn new(func: &AggregateFunc) -> Self {
        match func {
            AggregateFunc::CountStar => Self::CountStar(0),
            AggregateFunc::CountCol => Self::CountCol(0),
            AggregateFunc::Sum => Self::Sum(None),
            AggregateFunc::Min => Self::Min(None),
            AggregateFunc::Max => Self::Max(None),
            AggregateFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    /// Feeds one input value into the accumulator.
    ///
    /// `NULL` values are skipped by all accumulators except `CountStar`
    /// (corresponding to `COUNT(*)`), which increments for every input row.
    ///
    /// For `AVG`, only values that can be converted into `f64` contribute to the
    /// running `(sum, count)`.
    fn update(&mut self, val: &Value) {
        match self {
            Self::CountStar(n) => *n += 1,
            _ if val.is_null() => {}

            Self::CountCol(n) => *n += 1,

            Self::Sum(acc) => {
                *acc = Some(match acc.take() {
                    None => val.clone(),
                    Some(running) => running + val,
                });
            }

            Self::Min(acc) => {
                *acc = Some(match acc.take() {
                    None => val.clone(),
                    Some(current) => {
                        if val.partial_cmp(&current).is_some_and(Ordering::is_lt) {
                            val.clone()
                        } else {
                            current
                        }
                    }
                });
            }

            Self::Max(acc) => {
                *acc = Some(match acc.take() {
                    None => val.clone(),
                    Some(current) => {
                        if val.partial_cmp(&current).is_some_and(Ordering::is_gt) {
                            val.clone()
                        } else {
                            current
                        }
                    }
                });
            }

            Self::Avg { sum, count } => {
                if let Ok(f) = f64::try_from(val) {
                    *sum += f;
                    *count += 1;
                }
            }
        }
    }

    /// Consumes the accumulator and produces the final aggregate result.
    ///
    /// For `SUM`/`MIN`/`MAX`, this returns `NULL` if no non-`NULL` input value
    /// was seen. For `AVG`, this returns `NULL` when `count == 0`.
    fn finalize(self) -> Value {
        match self {
            Self::CountStar(n) | Self::CountCol(n) => Value::Int64(n.cast_signed()),
            Self::Sum(v) | Self::Min(v) | Self::Max(v) => v.unwrap_or(Value::Null),
            Self::Avg { sum, count } => {
                if count > 0 {
                    #[allow(clippy::cast_precision_loss)]
                    Value::Float64(sum / count as f64)
                } else {
                    Value::Null
                }
            }
        }
    }
}

/// Groups input tuples and computes aggregate functions over each group.
///
/// Corresponds to `SELECT <agg1>, <agg2>, … FROM … GROUP BY col1, col2, …`
///
/// # SQL → operator mapping
///
/// A query like
///
/// ```sql
/// SELECT region, product, SUM(amount), COUNT(*)
/// FROM   sales
/// GROUP BY region, product;
/// ```
///
/// maps to (roughly):
///
/// ```ignore
/// Aggregate::new(
///     /* child        = */ seq_scan_over_sales,
///     /* group_by_ids = */ &[col_region, col_product],
///     /* agg_exprs    = */ vec![
///         AggregateExpr { func: Sum,       col_id: col_amount, output_name: "sum_amount".into() },
///         AggregateExpr { func: CountStar, col_id: col(0),     output_name: "n".into()          },
///     ],
/// )?;
/// ```
///
/// The output tuple layout mirrors the SQL: GROUP BY columns first, in the
/// order they were passed, then one column per [`AggregateExpr`] in declaration
/// order — so here it's `(region, product, sum_amount, n)`.
///
/// An ungrouped query like `SELECT COUNT(*) FROM t` is just the same operator
/// with an empty `group_by_ids` slice, producing a single-row result.
///
/// See the [module documentation](self) for a description of the two execution
/// phases and NULL handling rules.
#[derive(Debug)]
pub struct Aggregate<'a> {
    child: Box<PlanNode<'a>>,
    group_by_cols: Vec<usize>,
    agg_exprs: Vec<AggregateExpr>,
    output_schema: TupleSchema,
    groups: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl<'a> Aggregate<'a> {
    /// Creates a new `Aggregate` operator.
    ///
    /// - `child` — the upstream operator to drain.
    /// - `group_by_ids` — column IDs (from the child's schema) to group by. Pass an empty slice for
    ///   ungrouped aggregation (`SELECT COUNT(*) FROM t`).
    /// - `agg_exprs` — the aggregate functions to compute, in the order they should appear in the
    ///   output tuple after the GROUP BY columns.
    ///
    /// # Example
    ///
    /// The query
    ///
    /// ```sql
    /// SELECT dept_id, AVG(salary)
    /// FROM   employees
    /// GROUP BY dept_id;
    /// ```
    ///
    /// maps to:
    ///
    /// ```ignore
    /// Aggregate::new(
    ///     employees_scan,
    ///     &[dept_id_col],
    ///     vec![AggregateExpr {
    ///         func: AggregateFunc::Avg,
    ///         col_id: salary_col,
    ///         output_name: "avg_salary".into(),
    ///     }],
    /// )?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any column ID in `group_by_ids`
    /// or any `col_id` in `agg_exprs` is out of bounds for the child's schema.
    pub fn new(
        child: PlanNode<'a>,
        group_by_ids: &[ColumnId],
        agg_exprs: Vec<AggregateExpr>,
    ) -> Result<Self, ExecutionError> {
        let child_schema = child.schema();

        let group_by_cols = Self::resolve_group_by_indices(group_by_ids, child_schema)?;
        let output_fields = Self::build_output_fields(&group_by_cols, child_schema, &agg_exprs)?;

        Ok(Self {
            child: Box::new(child),
            group_by_cols,
            agg_exprs,
            output_schema: TupleSchema::new(output_fields),
            groups: Vec::new(),
            cursor: 0,
            materialized: false,
        })
    }

    /// Resolves bound `GROUP BY` column IDs into concrete child-schema indices.
    ///
    /// In SQL terms, this is the step that turns planner/binder output
    /// (`GROUP BY` references) into positional offsets used during execution.
    ///
    /// # SQL examples
    ///
    /// Assume child schema `users(id, name, age)` with resolved indices
    /// `id -> 0`, `name -> 1`, `age -> 2`.
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) FROM users GROUP BY age;
    /// --   group_by_ids = [col(age=2)] -> group_by_cols = [2]
    ///
    /// -- SELECT id, name, COUNT(*) FROM users GROUP BY id, name;
    /// --   group_by_ids = [col(id=0), col(name=1)] -> group_by_cols = [0, 1]
    ///
    /// -- SELECT COUNT(*) FROM users;
    /// --   group_by_ids = [] -> group_by_cols = []
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] when a `GROUP BY` column id points
    /// outside the child schema (for example, SQL references a column position
    /// that is not present in the bound input).
    fn resolve_group_by_indices(
        group_by_ids: &[ColumnId],
        child_schema: &TupleSchema,
    ) -> Result<Vec<usize>, ExecutionError> {
        let group_by_cols = group_by_ids
            .iter()
            .map(|&c| {
                let idx = usize::from(c);
                child_schema.field_or_err(idx).map(|_| idx).map_err(|_| {
                    ExecutionError::TypeError(format!(
                        "GROUP BY column index {idx} is out of bounds"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(group_by_cols)
    }

    /// Builds output fields for `GROUP BY + aggregate` projection shape.
    ///
    /// The resulting fields are:
    /// - all `GROUP BY` columns (in order), followed by
    /// - one field per aggregate spec, using the inferred output type for the aggregate function.
    ///
    /// # SQL examples
    ///
    /// Assume child schema `users(id, name, age)` with resolved indices
    /// `id -> 0`, `name -> 1`, `age -> 2`.
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) AS n FROM users GROUP BY age;
    /// --   output fields: [age, n]
    ///
    /// -- SELECT name, SUM(age) AS total_age FROM users GROUP BY name;
    /// --   output fields: [name, total_age]
    ///
    /// -- SELECT COUNT(*) AS n FROM users;
    /// --   output fields: [n]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any aggregate input column in
    /// `agg_exprs` is out of bounds for `child_schema` (for example, a bound
    /// `SUM(col)` refers to a nonexistent input column index).
    fn build_output_fields(
        group_by_cols: &[usize],
        child_schema: &TupleSchema,
        agg_exprs: &[AggregateExpr],
    ) -> Result<Vec<Field>, ExecutionError> {
        let mut output_fields = child_schema
            .project_fields(group_by_cols)
            .expect("group_by_cols already validated by resolve_group_by_indices");

        for AggregateExpr {
            func,
            col_id,
            output_name,
        } in agg_exprs
        {
            let col_idx = usize::from(*col_id);
            let input_type = child_schema
                .field_or_err(col_idx)
                .map(|f| f.field_type)
                .map_err(|_| {
                    ExecutionError::TypeError(format!(
                        "aggregate column index {col_idx} is out of bounds"
                    ))
                })?;

            let output_type = Type::from((func, input_type));
            output_fields.push(Field::new_non_empty(output_name.clone(), output_type));
        }

        Ok(output_fields)
    }

    /// Drains the child once and materializes final `GROUP BY` result tuples.
    ///
    /// SQL-wise, this is the blocking transition from:
    /// - running group state (`key -> accumulators`) to
    /// - emitted rows (`GROUP BY columns + finalized aggregate values`).
    ///
    /// On subsequent calls it returns immediately because `self.materialized`
    /// is already `true` (same one-time materialization pattern as
    /// [`Sort`](super::unary::Sort)).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) FROM users GROUP BY age;
    /// --   materialize() builds one output tuple per distinct age:
    /// --   (age, count_star)
    ///
    /// -- SELECT COUNT(*) FROM users;
    /// --   materialize() builds exactly one tuple:
    /// --   (count_star)
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates child iteration failures from `self.child.next()` as
    /// [`ExecutionError`] (for example, storage/scan errors while draining input).
    fn materialize(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        let map = self.build_group_accumulators()?;
        self.groups = map
            .into_iter()
            .map(|(mut key, accums)| {
                key.extend(accums.into_iter().map(Accumulator::finalize));
                Tuple::new(key)
            })
            .collect();

        self.materialized = true;
        Ok(())
    }

    /// Builds in-memory group state: `group_key -> accumulators`.
    ///
    /// Each input tuple contributes to exactly one group key. If the key is
    /// first seen, initializes one [`Accumulator`] per aggregate spec, then
    /// updates those accumulators with this tuple's aggregate input values.
    ///
    /// # SQL examples
    ///
    /// Assume input schema `users(id, name, age)`.
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) FROM users GROUP BY age;
    /// --   key = [age], state per key = [CountStar]
    ///
    /// -- SELECT name, SUM(age), COUNT(age) FROM users GROUP BY name;
    /// --   key = [name], state per key = [Sum(age), CountCol(age)]
    ///
    /// -- SELECT COUNT(*) FROM users;
    /// --   key = [] (single global group), state = [CountStar]
    /// ```
    ///
    /// `NULL` handling follows [`Accumulator::update`]:
    /// - `COUNT(*)` increments for every row
    /// - other aggregates skip `NULL` inputs
    ///
    /// # Errors
    ///
    /// Propagates child iteration failures from `self.child.next()` as
    /// [`ExecutionError`].
    fn build_group_accumulators(
        &mut self,
    ) -> Result<HashMap<Vec<Value>, Vec<Accumulator>>, ExecutionError> {
        let mut map: HashMap<Vec<Value>, Vec<Accumulator>> = HashMap::new();
        loop {
            let Some(tuple) = self.child.next()? else {
                break;
            };

            let key = self
                .group_by_cols
                .iter()
                .map(|&i| ColumnId::try_from(i).unwrap())
                .map(|c| tuple.value_at_or_null(c))
                .cloned()
                .collect();

            let accums = map.entry(key).or_insert_with(|| {
                self.agg_exprs
                    .iter()
                    .map(|s| Accumulator::new(&s.func))
                    .collect()
            });

            for (accum, expr) in accums.iter_mut().zip(self.agg_exprs.iter()) {
                let val = tuple.value_at_or_null(expr.col_id);
                accum.update(val);
            }
        }
        Ok(map)
    }
}

impl FallibleIterator for Aggregate<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize()?;
        if self.cursor >= self.groups.len() {
            return Ok(None);
        }
        let tuple = self.groups[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(tuple))
    }
}

impl Executor for Aggregate<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.output_schema
    }

    /// Resets the cursor so the finalized groups can be iterated again.
    ///
    /// The child does **not** need to be rewound — the groups are already
    /// materialized in memory.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.cursor = 0;
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

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    // Schema: (group: Int32, val: Int32 NULLABLE)
    fn schema_gv() -> TupleSchema {
        TupleSchema::new(vec![field("group", Type::Int32), field("val", Type::Int32)])
    }

    fn row(g: i32, v: Option<i32>) -> Tuple {
        Tuple::new(vec![Value::Int32(g), v.map_or(Value::Null, Value::Int32)])
    }

    struct HeapHarness {
        heap: HeapFile,
        #[allow(dead_code)]
        wal: Arc<Wal>,
        _dir: tempfile::TempDir,
        txn: TransactionId,
    }

    fn build_heap(id: u64, schema: TupleSchema, tuples: &[Tuple]) -> HeapHarness {
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

        let heap = HeapFile::new(file_id, schema, Arc::clone(&store), 0, Arc::clone(&wal));

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

    fn spec(func: AggregateFunc, col_id: u32, name: &str) -> AggregateExpr {
        AggregateExpr {
            func,
            col_id: col(col_id),
            output_name: NonEmptyString::new(name).unwrap(),
        }
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

    fn i32_at(t: &Tuple, i: usize) -> i32 {
        match t.get(i) {
            Some(Value::Int32(v)) => *v,
            other => panic!("expected Int32 at {i}, got {other:?}"),
        }
    }

    fn i64_at(t: &Tuple, i: usize) -> i64 {
        match t.get(i) {
            Some(Value::Int64(v)) => *v,
            other => panic!("expected Int64 at {i}, got {other:?}"),
        }
    }

    #[test]
    fn test_count_col_skips_nulls() {
        let harness = build_heap(301, schema_gv(), &[
            row(1, Some(10)),
            row(1, None),
            row(1, Some(30)),
            row(1, None),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![spec(
            AggregateFunc::CountCol,
            1,
            "c",
        )])
        .unwrap();
        let out = drain(&mut agg);
        assert_eq!(out.len(), 1);
        assert_eq!(i64_at(&out[0], 0), 2, "COUNT(col) must ignore NULLs");
    }

    // And COUNT(*) still counts everything
    #[test]
    fn test_count_star_counts_all_rows_including_nulls() {
        let harness = build_heap(302, schema_gv(), &[
            row(1, Some(10)),
            row(1, None),
            row(1, None),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![spec(
            AggregateFunc::CountStar,
            0,
            "n",
        )])
        .unwrap();
        let out = drain(&mut agg);
        assert_eq!(i64_at(&out[0], 0), 3);
    }

    // Both in one query: proves the two variants diverge
    #[test]
    fn test_count_star_and_count_col_differ_on_null() {
        let harness = build_heap(303, schema_gv(), &[
            row(1, Some(10)),
            row(1, None),
            row(1, Some(30)),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![
            spec(AggregateFunc::CountStar, 0, "n_all"),
            spec(AggregateFunc::CountCol, 1, "n_vals"),
        ])
        .unwrap();
        let out = drain(&mut agg);
        assert_eq!(out.len(), 1);
        assert_eq!(i64_at(&out[0], 0), 3, "COUNT(*) counts every row");
        assert_eq!(i64_at(&out[0], 1), 2, "COUNT(val) skips the NULL");
    }

    // Grouped: each group's COUNT(col) independently skips its own NULLs
    #[test]
    fn test_count_col_per_group_skips_nulls() {
        let harness = build_heap(304, schema_gv(), &[
            row(1, Some(10)),
            row(1, None),
            row(2, None),
            row(2, None),
            row(2, Some(20)),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[col(0)], vec![spec(
            AggregateFunc::CountCol,
            1,
            "c",
        )])
        .unwrap();
        let mut out = drain(&mut agg);
        out.sort_by_key(|t| i32_at(t, 0));
        assert_eq!(out.len(), 2);
        assert_eq!((i32_at(&out[0], 0), i64_at(&out[0], 1)), (1, 1));
        assert_eq!((i32_at(&out[1], 0), i64_at(&out[1], 1)), (2, 1));
    }

    // All NULLs in the column -> COUNT(col) = 0 (not NULL, unlike SUM/AVG)
    #[test]
    fn test_count_col_all_null_returns_zero() {
        let harness = build_heap(305, schema_gv(), &[
            row(1, None),
            row(1, None),
            row(1, None),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![spec(
            AggregateFunc::CountCol,
            1,
            "c",
        )])
        .unwrap();
        let out = drain(&mut agg);
        assert_eq!(i64_at(&out[0], 0), 0);
    }

    // ===== Coverage for neighbouring behaviour while we're here =====

    // Empty input still emits one row for ungrouped aggregation... actually no,
    // current impl produces zero groups when input is empty even without GROUP BY.
    // Lock that behaviour down so a future change is deliberate.
    #[test]
    fn test_ungrouped_empty_input_produces_no_rows() {
        let harness = build_heap(306, schema_gv(), &[]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![spec(
            AggregateFunc::CountStar,
            0,
            "n",
        )])
        .unwrap();
        assert!(agg.next().unwrap().is_none());
    }

    // SUM skips NULLs (non-regression; this path already worked)
    #[test]
    fn test_sum_skips_nulls() {
        let harness = build_heap(307, schema_gv(), &[
            row(1, Some(10)),
            row(1, None),
            row(1, Some(20)),
        ]);
        let mut agg =
            Aggregate::new(scan(&harness), &[], vec![spec(AggregateFunc::Sum, 1, "s")]).unwrap();
        let out = drain(&mut agg);
        assert_eq!(out.len(), 1);
        // Note: Value::Add widens Int32+Int32 -> Int64, so multi-row SUM returns Int64
        // even though the input column is Int32. See aggregate-analysis bug #2.
        assert!(matches!(out[0].get(0), Some(Value::Int64(30))));
    }

    // AVG of all-null column -> Null (not 0/0)
    #[test]
    fn test_avg_all_null_returns_null() {
        let harness = build_heap(308, schema_gv(), &[row(1, None), row(1, None)]);
        let mut agg =
            Aggregate::new(scan(&harness), &[], vec![spec(AggregateFunc::Avg, 1, "a")]).unwrap();
        let out = drain(&mut agg);
        assert_eq!(out[0].get(0), Some(&Value::Null));
    }

    // AVG over integers
    #[test]
    fn test_avg_basic() {
        let harness = build_heap(309, schema_gv(), &[
            row(1, Some(2)),
            row(1, Some(4)),
            row(1, Some(6)),
        ]);
        let mut agg =
            Aggregate::new(scan(&harness), &[], vec![spec(AggregateFunc::Avg, 1, "a")]).unwrap();
        let out = drain(&mut agg);
        match out[0].get(0) {
            Some(Value::Float64(f)) => assert!((f - 4.0).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    // MIN / MAX ignore NULLs
    #[test]
    fn test_min_max_skip_nulls() {
        let harness = build_heap(310, schema_gv(), &[
            row(1, None),
            row(1, Some(5)),
            row(1, Some(2)),
            row(1, None),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[], vec![
            spec(AggregateFunc::Min, 1, "mn"),
            spec(AggregateFunc::Max, 1, "mx"),
        ])
        .unwrap();
        let out = drain(&mut agg);
        assert!(matches!(out[0].get(0), Some(Value::Int32(2))));
        assert!(matches!(out[0].get(1), Some(Value::Int32(5))));
    }

    // MIN of all-null column -> Null
    #[test]
    fn test_min_all_null_returns_null() {
        let harness = build_heap(311, schema_gv(), &[row(1, None), row(1, None)]);
        let mut agg =
            Aggregate::new(scan(&harness), &[], vec![spec(AggregateFunc::Min, 1, "mn")]).unwrap();
        let out = drain(&mut agg);
        assert_eq!(out[0].get(0), Some(&Value::Null));
    }

    // Grouping: output has group key(s) followed by aggregates, in order
    #[test]
    fn test_group_by_layout_key_then_aggregates() {
        let harness = build_heap(312, schema_gv(), &[
            row(1, Some(10)),
            row(2, Some(20)),
            row(1, Some(30)),
        ]);
        let mut agg = Aggregate::new(scan(&harness), &[col(0)], vec![
            spec(AggregateFunc::Sum, 1, "s"),
            spec(AggregateFunc::CountStar, 0, "n"),
        ])
        .unwrap();
        let mut out = drain(&mut agg);
        out.sort_by_key(|t| i32_at(t, 0));
        assert_eq!(out.len(), 2);
        // layout: (group, sum, count)
        assert_eq!(i32_at(&out[0], 0), 1);
        // group 1 has two rows: Int32+Int32 widens to Int64 via Value::Add
        assert!(matches!(out[0].get(1), Some(Value::Int64(40))));
        assert_eq!(i64_at(&out[0], 2), 2);
        assert_eq!(i32_at(&out[1], 0), 2);
        // group 2 has one row: Sum just clones the single value, stays Int32
        assert!(matches!(out[1].get(1), Some(Value::Int32(20))));
        assert_eq!(i64_at(&out[1], 2), 1);
    }

    // Rewind replays finalized groups without re-reading the child
    #[test]
    fn test_rewind_replays_groups() {
        let harness = build_heap(313, schema_gv(), &[row(1, Some(10)), row(2, Some(20))]);
        let mut agg = Aggregate::new(scan(&harness), &[col(0)], vec![spec(
            AggregateFunc::CountStar,
            0,
            "n",
        )])
        .unwrap();
        let first = drain(&mut agg).len();
        agg.rewind().unwrap();
        let second = drain(&mut agg).len();
        assert_eq!(first, 2);
        assert_eq!(first, second);
    }

    // Constructor surfaces out-of-bounds GROUP BY column
    #[test]
    fn test_new_rejects_bad_group_by_column() {
        let harness = build_heap(314, schema_gv(), &[]);
        let err = Aggregate::new(scan(&harness), &[col(99)], vec![spec(
            AggregateFunc::CountStar,
            0,
            "n",
        )])
        .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(ref m) if m.contains("GROUP BY")));
    }

    // Constructor surfaces out-of-bounds aggregate input column
    #[test]
    fn test_new_rejects_bad_agg_column() {
        let harness = build_heap(315, schema_gv(), &[]);
        let err = Aggregate::new(scan(&harness), &[], vec![spec(AggregateFunc::Sum, 99, "s")])
            .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(ref m) if m.contains("aggregate column")));
    }

    // Output schema: correct field count and count/avg promotion
    #[test]
    fn test_schema_types_for_count_and_avg() {
        let harness = build_heap(316, schema_gv(), &[]);
        let agg = Aggregate::new(scan(&harness), &[col(0)], vec![
            spec(AggregateFunc::CountStar, 0, "n"),
            spec(AggregateFunc::Avg, 1, "a"),
        ])
        .unwrap();
        let s = agg.schema();
        assert_eq!(s.physical_num_fields(), 3);
        assert_eq!(s.field(0).unwrap().field_type, Type::Int32); // group col unchanged
        assert_eq!(s.field(1).unwrap().field_type, Type::Int64); // COUNT -> Int64
        assert_eq!(s.field(2).unwrap().field_type, Type::Float64); // AVG -> Float64
    }
}
