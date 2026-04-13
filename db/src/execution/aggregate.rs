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
//! 1. **Accumulate** — drain the child, grouping rows by their `GROUP BY`
//!    column values. For each group, maintain one [`Accumulator`] per aggregate
//!    spec. This happens lazily on the first call to [`FallibleIterator::next`].
//!
//! 2. **Emit** — iterate over the finalized groups, yielding one [`Tuple`] per
//!    group. The tuple layout is: GROUP BY column values first, then one value
//!    per aggregate spec, in declaration order.
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

use std::collections::HashMap;

use fallible_iterator::FallibleIterator;

use crate::{
    execution::PlanNode,
    primitives::ColumnId,
    tuple::{Field, Tuple, TupleSchema},
    types::{Type, Value},
};

use super::{ExecutionError, Executor};

/// A SQL aggregate function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunc {
    CountStar,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
}

/// Binds an [`AggregateFunc`] to the column it operates on and names the output.
///
/// For `COUNT(*)` the `col_id` is ignored; conventionally pass column `0`.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub func: AggregateFunc,
    pub col_id: ColumnId,
    pub output_name: String,
}

/// Running state for one aggregate function over one group.
///
/// Created fresh for each new group key, updated row by row, then finalized
/// into a single [`Value`] once the child is exhausted.
#[derive(Debug)]
enum Accumulator {
    CountStar(u64),
    Sum(Option<Value>),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: u64 },
}

impl Accumulator {
    fn new(func: &AggregateFunc) -> Self {
        match func {
            AggregateFunc::CountStar | AggregateFunc::CountCol => Self::CountStar(0),
            AggregateFunc::Sum => Self::Sum(None),
            AggregateFunc::Min => Self::Min(None),
            AggregateFunc::Max => Self::Max(None),
            AggregateFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    /// Feeds one input value into the accumulator.
    ///
    /// `NULL` values are skipped by all accumulators except `CountStar` (which
    /// corresponds to `COUNT(*)` and always increments).
    fn update(&mut self, val: &Value) {
        match self {
            Self::CountStar(n) => *n += 1,
            _ if val.is_null() => {}

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
                        if val
                            .partial_cmp(&current)
                            .is_some_and(std::cmp::Ordering::is_lt)
                        {
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
                        if val
                            .partial_cmp(&current)
                            .is_some_and(std::cmp::Ordering::is_gt)
                        {
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

    /// Consumes the accumulator and produces the final output [`Value`].
    fn finalize(self) -> Value {
        match self {
            Self::CountStar(n) => Value::Int64(n.cast_signed()),
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
/// See the [module documentation](self) for a description of the two execution
/// phases and NULL handling rules.
#[derive(Debug)]
pub struct Aggregate {
    child: Box<PlanNode>,
    group_by_cols: Vec<usize>,
    agg_specs: Vec<AggregateSpec>,
    output_schema: TupleSchema,
    groups: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl Aggregate {
    /// Creates a new `Aggregate` operator.
    ///
    /// - `child` — the upstream operator to drain.
    /// - `group_by_ids` — column IDs (from the child's schema) to group by.
    ///   Pass an empty slice for ungrouped aggregation (`SELECT COUNT(*) FROM t`).
    /// - `agg_specs` — the aggregate functions to compute, in the order they
    ///   should appear in the output tuple after the GROUP BY columns.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any column ID in `group_by_ids`
    /// or any `col_id` in `agg_specs` is out of bounds for the child's schema.
    pub fn new(
        child: Box<PlanNode>,
        group_by_ids: &[ColumnId],
        agg_specs: Vec<AggregateSpec>,
    ) -> Result<Self, ExecutionError> {
        let child_schema = child.schema();

        let group_by_cols = group_by_ids
            .iter()
            .map(|&c| {
                let idx = usize::from(c);
                if child_schema.field(idx).is_none() {
                    Err(ExecutionError::TypeError(format!(
                        "GROUP BY column index {idx} is out of bounds"
                    )))
                } else {
                    Ok(idx)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut output_fields = group_by_cols
            .iter()
            .map(|&i| child_schema.field(i).unwrap().clone())
            .collect::<Vec<_>>();

        for spec in &agg_specs {
            let col_idx = usize::from(spec.col_id);
            let input_type = child_schema
                .field(col_idx)
                .map(|f| f.field_type)
                .ok_or_else(|| {
                    ExecutionError::TypeError(format!(
                        "aggregate column index {col_idx} is out of bounds"
                    ))
                })?;

            let output_type = match &spec.func {
                AggregateFunc::CountStar | AggregateFunc::CountCol => Type::Int64,
                AggregateFunc::Avg => Type::Float64,
                AggregateFunc::Sum => match input_type {
                    Type::Float64 => Type::Float64,
                    _ => Type::Int64,
                },
                AggregateFunc::Min | AggregateFunc::Max => input_type,
            };
            output_fields.push(Field::new(spec.output_name.clone(), output_type));
        }

        Ok(Self {
            child,
            group_by_cols,
            agg_specs,
            output_schema: TupleSchema::new(output_fields),
            groups: Vec::new(),
            cursor: 0,
            materialized: false,
        })
    }

    /// Drains the child and materializes the aggregate groups, but only once.
    ///
    /// On subsequent calls returns immediately because `self.materialized` is
    /// already `true` — identical to the pattern used by [`Sort`](super::unary::Sort).
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's `next`.
    fn materialize(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        let mut map: HashMap<Vec<Value>, Vec<Accumulator>> = HashMap::new();

        while let Some(tuple) = self.child.next()? {
            let key = self
                .group_by_cols
                .iter()
                .map(|&i| tuple.get(i).cloned().unwrap_or(Value::Null))
                .collect();

            let accums = map.entry(key).or_insert_with(|| {
                self.agg_specs
                    .iter()
                    .map(|s| Accumulator::new(&s.func))
                    .collect()
            });

            for (accum, spec) in accums.iter_mut().zip(self.agg_specs.iter()) {
                let val = tuple.get(usize::from(spec.col_id)).unwrap_or(&Value::Null);
                accum.update(val);
            }
        }

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
}

impl FallibleIterator for Aggregate {
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

impl Executor for Aggregate {
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
