//! SQL `SELECT` binding, planning, and execution.
//!
//! This module covers the complete pipeline for SQL `SELECT` statements:
//! name resolution against the catalog (binding), physical operator selection
//! (planning), and row collection (execution).
//!
//! # Binding shape
//!
//! - [`BoundFrom`] — the bound `FROM` tree, including base table scans and joined inputs.
//! - [`BoundSelectItem`] — one executable item from the `SELECT` list: column, literal, or
//!   aggregate.
//! - [`BoundProjection`] — one `SELECT` item plus its output alias.
//! - [`BoundSelectList`] — either `SELECT *` or an ordered list of [`BoundProjection`] values.
//! - [`BoundSelect`] — the full resolved query block with every column reference mapped to a
//!   numeric position.
//!
//! # Execution shape
//!
//! - [`Engine::exec_select`] — statement entry point for `SELECT ... FROM ...`.
//! - `build_plan` — lowers a [`BoundSelect`] into scan/filter/sort/project/aggregate/limit nodes.
//! - `build_project` — maps non-aggregate `SELECT` list entries to `ProjectItem`s.
//! - `build_aggregate` — maps `GROUP BY` and aggregate `SELECT` items to `Aggregate` plus a
//!   rewiring `Project`.
//!
//! # How it works
//!
//! Binding walks the `FROM` clause first and builds a [`Scope`] in lockstep.
//! Each table gets a column offset in the concatenated row. Later clauses
//! (`WHERE`, `SELECT`, `GROUP BY`, `ORDER BY`) ask the scope to resolve SQL
//! names into stable column indices. Planning then follows SQL clause order:
//! `FROM` builds the base scan, `WHERE` filters rows, non-aggregate `ORDER BY`
//! sorts while the full schema is still available, projection narrows the
//! tuple, `DISTINCT` removes duplicates, and `LIMIT/OFFSET` trims the final
//! stream. Aggregating queries use a blocking `Aggregate` operator followed by
//! a small `Project` that restores the user-written `SELECT` order.
//!
//! # NULL semantics
//!
//! `WHERE` NULL behavior is delegated to `BooleanExpression`: comparisons with
//! `NULL` evaluate to `false` rather than full SQL three-valued logic.
//! Aggregate NULL behavior is delegated to `Aggregate`: `COUNT(*)` counts
//! every row, while column aggregates skip `NULL` inputs.

use std::sync::Arc;

use fallible_iterator::FallibleIterator;

use super::scope::{BoundTable, ColumnResolver, Scope};
use crate::{
    FileId, TransactionId, Value,
    catalog::{TableInfo, manager::Catalog},
    engine::{Engine, EngineError, StatementResult},
    execution::{
        Executor, JoinAlgo, PlanNode, ResolvedExpr,
        aggregate::{AggregateExpr, AggregateFunc},
        join::{JoinPredicate, JoinType},
        unary::{ProjectItem, SortKey},
    },
    heap::file::HeapFile,
    parser::statements::{
        BinOp, ColumnRef, Expr, JoinKind, LimitClause, OrderBy, OrderDirection, SelectColumns,
        SelectItem, SelectStatement, TableRef, TableWithJoins,
    },
    primitives::{ColumnId, NonEmptyString, Predicate},
    transaction::ActiveTransaction,
    tuple::TupleSchema,
};

/// Bound `FROM` input for one `SELECT` query block.
///
/// A `BoundFrom` is the planner-facing shape of SQL `FROM`: every table name
/// has been checked against the catalog, every table schema is attached, and
/// join predicates have already been lowered to [`BooleanExpression`] values
/// over the joined row layout.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT * FROM users;
/// ```
///
/// ```ignore
/// BoundFrom::Table {
///     table: TableRef { name: "users".into(), alias: None },
///     file_id,
///     schema: users_schema,
///     column_offset: 0,
/// }
/// ```
///
/// ```sql
/// -- SELECT * FROM users u JOIN orders o ON u.id = o.user_id;
/// ```
///
/// ```ignore
/// BoundFrom::Join {
///     kind: JoinKind::Inner,
///     left: Box::new(users_from),
///     right: Box::new(orders_from),
///     on: BooleanExpression::col_op_col(0, Predicate::Equals, 4),
///     schema: users_schema.merge(&orders_schema),
/// }
/// ```
///
/// The output tuple layout is left input columns first, then right input
/// columns for joins: `users.id`, `users.name`, `users.age`, `orders.id`,
/// `orders.user_id`, `orders.total`.
#[derive(Debug)]
enum BoundFrom {
    /// A base table reference from `FROM users` or `FROM users u`.
    Table {
        table: TableRef,
        file_id: FileId,
        schema: Arc<TupleSchema>,
    },
    /// A SQL `JOIN ... ON ...` input with its join predicate (pre-resolved).
    Join {
        kind: JoinKind,
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        on: ResolvedExpr,
        schema: Arc<TupleSchema>,
    },
    /// `FROM a, b` or `CROSS JOIN` — no predicate; emits the cartesian product.
    Cross {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        schema: Arc<TupleSchema>,
    },
}

impl BoundFrom {
    fn table(table_info: &TableInfo, table: &TableRef) -> Self {
        Self::Table {
            table: table.clone(),
            file_id: table_info.file_id,
            schema: Arc::clone(&table_info.schema),
        }
    }

    fn schema(&self) -> &TupleSchema {
        match self {
            Self::Cross { schema, .. } | Self::Table { schema, .. } | Self::Join { schema, .. } => {
                schema
            }
        }
    }

    fn join(kind: JoinKind, left: BoundFrom, right: BoundFrom, on: ResolvedExpr) -> Self {
        let schema = Arc::new(left.schema().merge(right.schema()));
        Self::Join {
            kind,
            left: Box::new(left),
            right: Box::new(right),
            on,
            schema,
        }
    }

    fn cross(left: BoundFrom, right: BoundFrom) -> Self {
        let schema = Arc::new(left.schema().merge(right.schema()));
        Self::Cross {
            left: Box::new(left),
            right: Box::new(right),
            schema,
        }
    }

    fn root_table_name(&self) -> &str {
        match self {
            Self::Table { table, .. } => &table.name,
            Self::Join { left, .. } | Self::Cross { left, .. } => left.root_table_name(),
        }
    }

    fn table_count(&self) -> usize {
        match self {
            Self::Table { .. } => 1,
            Self::Join { left, right, .. } | Self::Cross { left, right, .. } => {
                left.table_count() + right.table_count()
            }
        }
    }
}

/// One entry in a bound SQL `SELECT` list.
///
/// `expr` is the pre-resolved expression (all column names replaced by [`ColumnId`]s).
/// `alias` is the user-supplied `AS name`; when `None` the planner derives a default.
#[derive(Debug)]
struct SelectProjection {
    expr: ResolvedExpr,
    alias: Option<NonEmptyString>,
}

/// The bound SQL `SELECT` list.
///
/// `Star` corresponds to `SELECT *` — no `Project` node is built.
/// `Items` is an explicit list in the user's order.
#[derive(Debug)]
enum BoundSelectList {
    Star,
    Items(Vec<SelectProjection>),
}

/// A resolved SQL `SELECT` query block.
///
/// Every name has been resolved to an index; every literal has been coerced
/// to the column's type. The executor is free to assume this is internally
/// consistent.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT * FROM users WHERE age >= 18;
/// ```
///
/// ```ignore
/// BoundSelect {
///     from: BoundFrom::Table { .. },
///     select_list: BoundSelectList::Star,
///     filter: Some(BooleanExpression::col_op_lit(
///         2,
///         Predicate::GreaterThanOrEqual,
///         Value::int64(18),
///     )),
///     group_by: vec![],
///     having: None,
///     distinct: false,
///     order_by: vec![],
///     limit: None,
/// }
/// ```
struct BoundSelect {
    from: BoundFrom,
    select_list: BoundSelectList,
    filter: Option<ResolvedExpr>,
    group_by: Vec<ColumnId>,
    having: Option<ResolvedExpr>,
    distinct: bool,
    order_by: Vec<(ColumnId, OrderDirection)>,
    limit: Option<LimitClause>,
}

impl BoundSelect {
    /// Binds a parsed SQL `SELECT` into column-indexed query metadata.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for `SELECT` without `FROM`, for
    /// comma-separated multi-table `FROM` entries, or for aggregate arguments
    /// that are not a single column.
    ///
    /// Returns [`EngineError::Catalog`] when a `FROM` table is not in the catalog.
    ///
    /// Returns [`EngineError::UnknownColumn`] or [`EngineError::AmbiguousColumn`]
    /// when a SQL name cannot be resolved through the scope.
    pub fn bind(
        mut stmt: SelectStatement,
        catalog: &Catalog,
        txn: &ActiveTransaction,
    ) -> Result<Self, EngineError> {
        let [from_item] = stmt.from.as_slice() else {
            return Err(EngineError::Unsupported(if stmt.from.is_empty() {
                "no FROM clause".into()
            } else {
                "multi-table FROM not yet supported".into()
            }));
        };
        tracing::debug!(table = %from_item.table.name, joins = from_item.joins.len(), "binding SELECT");
        let from_item = stmt.from.remove(0);

        let (from, scope) = Self::resolve_from(from_item, catalog, txn)?;

        let select_list = match stmt.columns {
            SelectColumns::All => Ok(BoundSelectList::Star),
            SelectColumns::Exprs(items) => items
                .into_iter()
                .map(|it| Self::bind_select_item(&scope, it))
                .collect::<Result<Vec<_>, _>>()
                .map(BoundSelectList::Items),
        }?;

        let order_by = stmt
            .order_by
            .into_iter()
            .map(|OrderBy(col, dir)| Self::resolve_scope_col(&scope, &col).map(|id| (id, dir)))
            .collect::<Result<Vec<_>, _>>()?;

        let group_by = stmt
            .group_by
            .into_iter()
            .map(|col| Self::resolve_scope_col(&scope, &col))
            .collect::<Result<Vec<_>, _>>()?;

        let filter = stmt
            .where_clause
            .map(|expr| ResolvedExpr::resolve(expr, &scope))
            .transpose()?;

        let having = stmt
            .having
            .map(|expr| ResolvedExpr::resolve(expr, &scope))
            .transpose()?;

        Ok(Self {
            from,
            select_list,
            filter,
            group_by,
            having,
            distinct: stmt.distinct,
            order_by,
            limit: stmt.limit,
        })
    }

    /// Resolves one SQL `FROM` entry into a [`BoundFrom`] tree and name scope.
    ///
    /// Each table's column offset reflects its position in the merged output
    /// row. A join's right table is pushed into the scope before its `ON`
    /// clause is bound, which makes both sides visible to the predicate binder.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Catalog`] when a `FROM` or joined table is not
    /// present in the catalog. Returns predicate-binding errors from the `ON`
    /// clause, including [`EngineError::UnknownColumn`] and
    /// [`EngineError::AmbiguousColumn`].
    fn resolve_from(
        from: TableWithJoins,
        catalog: &Catalog,
        txn: &ActiveTransaction,
    ) -> Result<(BoundFrom, Scope), EngineError> {
        let TableWithJoins { table, joins } = from;

        let info = catalog.get_table_info(txn, &table.name)?;
        let mut scope = Scope::empty();
        scope.push(BoundTable::new(
            info.name.to_string(),
            table.alias.as_ref().map(ToString::to_string),
            Arc::clone(&info.schema),
            0,
        ));
        let mut left = BoundFrom::table(&info, &table);

        for j in joins {
            let right_offset = left.schema().physical_num_fields();
            let right_info = catalog.get_table_info(txn, &j.table.name)?;
            let right_table = BoundTable {
                name: right_info.name.to_string(),
                alias: j.table.alias.as_ref().map(ToString::to_string),
                schema: Arc::clone(&right_info.schema),
                column_offset: right_offset,
            };
            scope.push(right_table);

            let right = BoundFrom::table(&right_info, &j.table);
            left = if j.kind == JoinKind::Cross {
                BoundFrom::cross(left, right)
            } else {
                let on = j.on.expect("non-cross join must have ON clause");
                let resolved_on = ResolvedExpr::resolve(on, &scope)?;
                BoundFrom::join(j.kind, left, right, resolved_on)
            };
        }

        Ok((left, scope))
    }

    /// Binds one SQL projection expression into a [`SelectProjection`].
    ///
    /// Column references are resolved to [`ColumnId`]s via `resolve_scope_col` so
    /// that [`EngineError::UnknownColumn`] / [`EngineError::AmbiguousColumn`] are
    /// surfaced at bind time.  Aggregate arguments are resolved the same way.
    /// Complex expressions (`BinaryOp`, `LIKE`, etc.) are rejected until the
    /// planner gains support for scalar expressions in projections.
    fn bind_select_item(scope: &Scope, item: SelectItem) -> Result<SelectProjection, EngineError> {
        let SelectItem { expr, alias } = item;
        // Aggregate args are validated here: the Aggregate operator only handles a single
        // resolved column, so reject anything more complex at bind time.
        // Simple column references and aggregate args go through resolve_scope_col so that
        // UnknownColumn / AmbiguousColumn engine errors are surfaced with the right type.
        // Everything else (BinaryOp, UnaryOp, etc.) goes through ResolvedExpr::resolve.
        let resolved = match expr {
            Expr::Column(col) => ResolvedExpr::Column(Self::resolve_scope_col(scope, &col)?),
            Expr::Agg { func, arg } => {
                let resolved_arg = ResolvedExpr::resolve(*arg, scope)?;
                ResolvedExpr::Agg {
                    func,
                    arg: Box::new(resolved_arg),
                }
            }
            other => ResolvedExpr::resolve(other, scope)?,
        };
        Ok(SelectProjection {
            expr: resolved,
            alias,
        })
    }

    /// Resolves one SQL column reference into a planner-facing [`ColumnId`].
    ///
    /// # Errors
    ///
    /// Propagates [`EngineError::UnknownColumn`] and [`EngineError::AmbiguousColumn`]
    /// from the scope resolver. Returns [`EngineError::Unsupported`] if the
    /// resolved `usize` index is too large for [`ColumnId`].
    #[inline]
    fn resolve_scope_col(scope: &Scope, col: &ColumnRef) -> Result<ColumnId, EngineError> {
        let (idx, ..) = scope.resolve(col)?;
        ColumnId::try_from(idx)
            .map_err(|_| EngineError::Unsupported("column index out of bounds".to_string()))
    }
}

impl Engine<'_> {
    /// Projects away dropped (logical) columns from a plan node's output.
    ///
    /// Dropped columns remain in the *physical* schema so that existing on-disk
    /// tuples (written before a DROP COLUMN) still decode into the same slot
    /// layout. For SQL output, though, we must hide those slots — `SELECT *`
    /// should only return live columns.
    ///
    /// When the schema's physical and logical widths match, this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TypeError`] if the underlying `Project` operator
    /// rejects the requested projection.
    fn project_out_dropped_columns(node: PlanNode<'_>) -> Result<PlanNode<'_>, EngineError> {
        let schema = node.schema();
        if schema.physical_num_fields() == schema.logical_num_fields() {
            return Ok(node);
        }

        let items = schema
            .physical_iter()
            .enumerate()
            .filter(|(_, f)| !f.is_dropped)
            .map(|(phys_i, _)| {
                ColumnId::try_from(phys_i)
                    .map(|col_id| ProjectItem {
                        expr: ResolvedExpr::Column(col_id),
                        alias: None,
                    })
                    .map_err(|_| EngineError::Unsupported("column index out of bounds".to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PlanNode::project(node, items)?)
    }

    /// Executes a bound SQL `SELECT` and returns the selected rows.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TableNotFound`] when a bound base table no longer
    /// has a heap in the catalog.
    ///
    /// Returns [`EngineError::Unsupported`] for currently unwired SQL shapes:
    /// `JOIN` execution, `HAVING`, `SELECT *` with aggregates or `GROUP BY`,
    /// `ORDER BY` combined with aggregates or `GROUP BY`, and bare projected
    /// columns that are not listed in `GROUP BY`.
    ///
    /// Returns [`EngineError::TypeError`] when an executor reports a type or
    /// tuple-shape error while building or draining the plan.
    pub(super) fn exec_select(
        txn: &ActiveTransaction,
        catalog: &Catalog,
        select_stmt: SelectStatement,
    ) -> Result<StatementResult, EngineError> {
        let bound = BoundSelect::bind(select_stmt, catalog, txn)?;

        let root_table_name = bound.from.root_table_name().to_owned();
        let mut heap_files = Vec::with_capacity(bound.from.table_count());
        Self::collect_heap_files(&bound.from, catalog, &mut heap_files)?;
        let mut plan = Self::build_plan(bound, &heap_files, txn.transaction_id())?;
        let schema = plan.schema().to_owned();

        let mut rows = Vec::new();
        while let Some(t) = plan.next()? {
            rows.push(t);
        }
        drop(plan); // releases the &HeapFile borrows before `heaps` goes out of scope

        Ok(StatementResult::Selected {
            table: root_table_name.to_string(),
            schema,
            rows,
        })
    }

    /// Collects heap files for every base table named by a SQL `FROM` tree.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TableNotFound`] if a bound table's [`FileId`] has
    /// no heap in the catalog by execution time.
    fn collect_heap_files(
        from: &BoundFrom,
        catalog: &Catalog,
        heap_files: &mut Vec<(FileId, Arc<HeapFile>)>,
    ) -> Result<(), EngineError> {
        match from {
            BoundFrom::Table { file_id, table, .. } => {
                let heap_file = catalog
                    .get_heap(*file_id)
                    .ok_or_else(|| EngineError::TableNotFound(table.name.as_str().to_owned()))?;
                heap_files.push((*file_id, heap_file));
            }
            BoundFrom::Join { left, right, .. } => {
                Self::collect_heap_files(left, catalog, heap_files)?;
                Self::collect_heap_files(right, catalog, heap_files)?;
            }
            BoundFrom::Cross { left, right, .. } => {
                Self::collect_heap_files(left, catalog, heap_files)?;
                Self::collect_heap_files(right, catalog, heap_files)?;
            }
        }
        Ok(())
    }

    /// Lowers one bound SQL `SELECT` block into a physical executor tree.
    ///
    /// For non-aggregating queries, `ORDER BY` is planned before `Project` so
    /// keys can still refer to columns that are not selected. For aggregating
    /// queries, `Aggregate` emits `[GROUP BY columns..., aggregate results...]`
    /// and a following `Project` restores the user-written `SELECT` order.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for currently unwired SQL shapes.
    /// Returns [`EngineError::TypeError`] when the underlying operators reject
    /// the bound tuple shape.
    fn build_plan(
        bound: BoundSelect,
        heaps: &[(FileId, Arc<HeapFile>)],
        txn: TransactionId,
    ) -> Result<PlanNode<'_>, EngineError> {
        let BoundSelect {
            from,
            select_list,
            filter,
            group_by,
            having,
            distinct,
            order_by,
            limit,
        } = bound;

        let aggregating = !group_by.is_empty() || Self::has_aggregate(&select_list);
        tracing::debug!(
            root_table = %from.root_table_name(),
            aggregating,
            order_by = order_by.len(),
            "building plan"
        );
        let mut node = Self::build_from(from, heaps, txn)?;

        if let Some(pred) = filter {
            node = PlanNode::filter(node, pred);
        }

        // ORDER BY and HAVING are still bound against the FROM scope, so their
        // column ids would be wrong above an Aggregate. Reject until the binder
        // rebinds those clauses against the post-aggregate schema.
        if aggregating && !order_by.is_empty() {
            return Err(EngineError::Unsupported(
                "ORDER BY combined with GROUP BY / aggregates is not yet supported".into(),
            ));
        }
        if having.is_some() {
            return Err(EngineError::Unsupported("HAVING not yet supported".into()));
        }

        // ORDER BY must run BEFORE Project: the binder resolves order keys
        // against the FROM scope, and Project may narrow the schema and drop
        // the columns those keys refer to. Sorting first keeps every column
        // in scope while the comparison happens.
        if !aggregating && !order_by.is_empty() {
            let keys = order_by
                .iter()
                .map(|(col, direction)| SortKey {
                    col_id: *col,
                    ascending: matches!(direction, OrderDirection::Asc),
                })
                .collect();
            node = PlanNode::sort(node, keys);
        }

        if aggregating {
            node = Self::build_aggregate(node, &select_list, &group_by)?;
        } else if let BoundSelectList::Items(items) = select_list {
            node = Self::build_project(node, items)?;
        } else {
            node = Self::project_out_dropped_columns(node)?;
        }

        if distinct {
            node = PlanNode::distinct(node);
        }

        if let Some(LimitClause { limit, offset }) = limit {
            node = PlanNode::limit(node, limit.unwrap_or(u64::MAX), offset);
        }

        Ok(node)
    }

    /// Builds the SQL `FROM` input for a `SELECT` plan.
    ///
    /// A base table lowers to a sequential scan over that table's heap for the
    /// current transaction. Join and cross-product bound forms return
    /// [`EngineError::Unsupported`] until join execution is wired through.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::collect_heap_files`] did not preload a heap for a
    /// `BoundFrom::Table`; that would mean the planner's preload invariant was
    /// broken, not that the SQL query is invalid.
    fn build_from(
        from: BoundFrom,
        heaps: &[(FileId, Arc<HeapFile>)],
        txn: TransactionId,
    ) -> Result<PlanNode<'_>, EngineError> {
        match from {
            BoundFrom::Table { file_id, .. } => {
                let heap = heaps
                    .iter()
                    .find_map(|(id, h)| (*id == file_id).then_some(h))
                    .expect("collect_heaps preloaded every BoundFrom::Table");
                Ok(PlanNode::seq_scan(heap.as_ref(), txn))
            }
            BoundFrom::Join {
                kind,
                left,
                right,
                on,
                ..
            } => {
                let left = Self::build_from(*left, heaps, txn)?;
                let right = Self::build_from(*right, heaps, txn)?;

                match kind {
                    JoinKind::Inner | JoinKind::Left => {
                        let join_type = if kind == JoinKind::Inner {
                            JoinType::Inner
                        } else {
                            JoinType::LeftOuter
                        };
                        let left_width = left.physical_num_fields();
                        Ok(Self::build_join(left, right, &on, left_width, join_type))
                    }
                    JoinKind::Right => {
                        // RIGHT JOIN A ON A.x = B.y  ≡  B LEFT JOIN A ON B.y = A.x.
                        //
                        // Step 1: swap the executor inputs so the LeftOuter path runs.
                        // Step 2: remap column indices in the ON expression to match
                        //         the new physical layout (old-right is now left, etc.).
                        // Step 3: add a reorder projection to restore the binder's
                        //         expected column order (old-left columns first). All
                        //         column references in SELECT / WHERE / ORDER BY were
                        //         resolved against the SQL-order layout, so the tuple
                        //         that reaches those operators must match it.
                        let old_left_width = left.physical_num_fields();
                        let old_right_width = right.physical_num_fields();
                        let adjusted_on =
                            Self::swap_join_column_indices(on, old_left_width, old_right_width);

                        // After the swap the executor emits: (old-right | old-left).
                        // We want:                           (old-left  | old-right).
                        // Build a projection that picks columns in that order.
                        let reorder_items: Vec<ProjectItem> = (old_right_width
                            ..old_right_width + old_left_width)
                            .chain(0..old_right_width)
                            .map(|i| ProjectItem {
                                expr: ResolvedExpr::Column(
                                    ColumnId::try_from(i).expect("reorder index fits in ColumnId"),
                                ),
                                alias: None,
                            })
                            .collect();

                        let joined = Self::build_join(
                            right,
                            left,
                            &adjusted_on,
                            old_right_width,
                            JoinType::LeftOuter,
                        );
                        Ok(PlanNode::project(joined, reorder_items)?)
                    }
                    JoinKind::Full => {
                        // For Full Outer, unmatched rows on *both* sides must be emitted.
                        //
                        // SortMergeJoin handles this naturally: the merge cursor emits an
                        // unmatched left row when left-key < right-key and an unmatched right
                        // row when right-key < left-key, so no extra bookkeeping is needed.
                        //
                        // HashJoin cannot do this cleanly (it would need a second pass over
                        // the hash table to find unmatched right rows), so we skip the
                        // build_join dispatcher (which might pick HashJoin) and dispatch
                        // directly: SortMerge for equi-predicates, NestedLoop otherwise.
                        let left_width = left.schema().physical_num_fields();
                        let algo = if let Some(pred) = Self::try_extract_equi(&on, left_width) {
                            tracing::debug!(algorithm = "sort_merge_join", join_type = ?JoinType::FullOuter, "join selected");
                            JoinAlgo::SortMerge(pred)
                        } else {
                            tracing::debug!(algorithm = "nested_loop_join", join_type = ?JoinType::FullOuter, "join selected");
                            JoinAlgo::NestedLoop(on)
                        };
                        Ok(PlanNode::join(left, right, algo, JoinType::FullOuter))
                    }
                    JoinKind::Cross => Err(EngineError::Unsupported(format!(
                        "{kind} is not yet supported in the planner"
                    ))),
                }
            }
            BoundFrom::Cross { left, right, .. } => {
                let left = Self::build_from(*left, heaps, txn)?;
                let right = Self::build_from(*right, heaps, txn)?;
                Ok(PlanNode::join(
                    left,
                    right,
                    JoinAlgo::Cross,
                    JoinType::Inner,
                ))
            }
        }
    }

    /// Picks `HashJoin` when the `ON` clause is a simple column-equality between
    /// the two sides; falls back to `NestedLoopJoin` for everything else. The
    /// `join_type` decides whether the chosen operator runs as an inner join or
    /// a left-outer join.
    fn build_join<'a>(
        left: PlanNode<'a>,
        right: PlanNode<'a>,
        on: &ResolvedExpr,
        left_width: usize,
        join_type: JoinType,
    ) -> PlanNode<'a> {
        if let Some(pred) = Self::try_extract_equi(on, left_width) {
            tracing::debug!(algorithm = "hash_join", ?join_type, "join selected");
            PlanNode::join(left, right, JoinAlgo::Hash(pred), join_type)
        } else {
            tracing::debug!(algorithm = "nested_loop_join", ?join_type, "join selected");
            PlanNode::join(left, right, JoinAlgo::NestedLoop(on.clone()), join_type)
        }
    }

    /// Rewrites column indices in `expr` after swapping join inputs for RIGHT JOIN.
    ///
    /// The binder assigns indices based on the SQL-order layout:
    ///   old left  → `0..left_width`
    ///   old right → `left_width..left_width+right_width`
    ///
    /// After swapping, the new physical layout is:
    ///   new left  (old right) → `0..right_width`
    ///   new right (old left)  → `right_width..right_width+left_width`
    ///
    /// So every column index must be remapped accordingly.
    fn swap_join_column_indices(
        expr: ResolvedExpr,
        left_width: usize,
        right_width: usize,
    ) -> ResolvedExpr {
        expr.map_columns(|id| {
            let i = usize::from(id);
            let new_i = if i < left_width {
                right_width + i
            } else {
                i - left_width
            };
            ColumnId::try_from(new_i).expect("remapped column index fits in ColumnId")
        })
    }

    /// Tries to extract an equi-join key from a [`ResolvedExpr`].
    ///
    /// Returns `Some(JoinPredicate)` only when `expr` is `col = col` where the two
    /// columns sit on opposite sides of the join (one index < `left_width`, the other
    /// ≥ `left_width`). The right-side index in the returned predicate is relative to
    /// the right input (global index minus `left_width`).
    ///
    /// Because columns are already resolved to [`ColumnId`]s, no schema lookup is needed.
    fn try_extract_equi(expr: &ResolvedExpr, left_width: usize) -> Option<JoinPredicate> {
        let ResolvedExpr::BinaryOp {
            lhs,
            op: BinOp::Eq,
            rhs,
        } = expr
        else {
            return None;
        };
        let (ResolvedExpr::Column(lc), ResolvedExpr::Column(rc)) = (lhs.as_ref(), rhs.as_ref())
        else {
            return None;
        };

        let l = usize::from(*lc);
        let r = usize::from(*rc);

        let (lc_idx, rc_idx) = if l < left_width && r >= left_width {
            (l, r - left_width)
        } else if r < left_width && l >= left_width {
            (r, l - left_width)
        } else {
            return None;
        };

        Some(JoinPredicate::new(
            ColumnId::try_from(lc_idx).ok()?,
            ColumnId::try_from(rc_idx).ok()?,
            Predicate::Equals,
        ))
    }

    /// Detects whether a SQL `SELECT` list requires aggregate planning.
    fn has_aggregate(list: &BoundSelectList) -> bool {
        let BoundSelectList::Items(items) = list else {
            return false;
        };
        items
            .iter()
            .any(|p| matches!(p.expr, ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar))
    }

    /// Builds the non-aggregate SQL `SELECT` list as a `Project` operator.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if an aggregate expression reaches
    /// this non-aggregating path.
    ///
    /// Returns [`EngineError::TypeError`] if `Project` rejects a column index or
    /// output schema shape.
    fn build_project(
        child: PlanNode<'_>,
        projections: Vec<SelectProjection>,
    ) -> Result<PlanNode<'_>, EngineError> {
        let project_items = projections
            .into_iter()
            .map(|SelectProjection { expr, alias }| match expr {
                ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar => {
                    Err(EngineError::Unsupported(
                        "aggregate projections require the Aggregate operator".into(),
                    ))
                }
                expr => Ok(ProjectItem { expr, alias }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PlanNode::project(child, project_items)?)
    }

    /// Wires up SQL `GROUP BY` and aggregate `SELECT` items.
    ///
    /// The physical shape produced is:
    ///
    /// ```ignore
    /// Project { rewires SELECT-list order over Aggregate's output }
    ///   └── Aggregate { group_by, agg_exprs }
    ///         └── child (already filtered)
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for `SELECT *` combined with
    /// aggregates or `GROUP BY`, and for bare projected columns not listed in
    /// `GROUP BY`.
    ///
    /// Returns [`EngineError::TypeError`] if the `Aggregate` or rewiring
    /// `Project` operator rejects the bound tuple shape.
    fn build_aggregate<'a>(
        child: PlanNode<'a>,
        select_list: &BoundSelectList,
        group_by_cols: &[ColumnId],
    ) -> Result<PlanNode<'a>, EngineError> {
        tracing::debug!(group_by_cols = group_by_cols.len(), "building aggregate");
        let projections = match select_list {
            BoundSelectList::Items(items) => items,
            BoundSelectList::Star => {
                return Err(EngineError::Unsupported(
                    "SELECT * with GROUP BY / aggregates is not supported".into(),
                ));
            }
        };

        Self::reject_ungrouped_columns(&child, projections, group_by_cols)?;
        let agg_exprs = projections
            .iter()
            .filter_map(|p| Self::projection_to_agg_expr(p, child.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        let agg_node = PlanNode::aggregate(child, group_by_cols, agg_exprs)?;

        let project_items =
            Self::build_aggregate_rewiring_projection_items(projections, group_by_cols)?;

        Ok(PlanNode::project(agg_node, project_items)?)
    }

    /// Ensures that all non-aggregate columns in the projections appear in the GROUP BY clause.
    ///
    /// This function checks each projection in the SELECT list. If a projection references a column
    /// directly (i.e., not inside an aggregate function or expression), it ensures that this column
    /// is present in the `group_by_cols` list. If a column is found that is not included in the
    /// GROUP BY, and also not inside an aggregate, this function returns an error.
    ///
    /// # Arguments
    ///
    /// * `child` - The `PlanNode` representing the child of the current query operator, used for
    ///   schema access and error reporting.
    /// * `projections` - The list of selected projections (columns or expressions) in the query's
    ///   SELECT clause.
    /// * `group_by_cols` - List of `ColumnId` that appear in the GROUP BY clause.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if a projected column is not included in GROUP BY and
    /// is not part of an aggregate expression.
    fn reject_ungrouped_columns(
        child: &PlanNode<'_>,
        projections: &[SelectProjection],
        group_by_cols: &[ColumnId],
    ) -> Result<(), EngineError> {
        let invalid_col = projections
            .iter()
            .find_map(|SelectProjection { expr, .. }| {
                if let ResolvedExpr::Column(c) = expr
                    && !group_by_cols.contains(c)
                {
                    return Some(*c);
                }
                None
            });

        if let Some(col_id) = invalid_col {
            let col_name = child.schema().col_name(col_id).unwrap_or("<unknown>");
            return Err(EngineError::Unsupported(format!(
                "column '{col_name}' (index {col_id}) must appear in GROUP BY or be used in an aggregate",
            )));
        }

        Ok(())
    }

    /// Builds the `ProjectItem`s that restore SQL `SELECT` order above `Aggregate`.
    ///
    /// The child `Aggregate` always outputs group keys first, then aggregate
    /// results. This helper maps each bound projection to its post-aggregate
    /// column position or to a broadcast literal.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if a post-aggregate output position
    /// cannot fit in [`ColumnId`].
    fn build_aggregate_rewiring_projection_items(
        projections: &[SelectProjection],
        group_by_cols: &[ColumnId],
    ) -> Result<Vec<ProjectItem>, EngineError> {
        let mut agg_index = 0usize;
        projections
            .iter()
            .enumerate()
            .map(|(i, projection)| {
                Ok(match &projection.expr {
                    ResolvedExpr::Column(c) => {
                        let pos = group_by_cols
                            .iter()
                            .position(|g| g == c)
                            .expect("GROUP BY membership validated above");
                        ProjectItem::new(
                            ResolvedExpr::Column(Self::col_id(pos)?),
                            projection.alias.clone(),
                        )
                    }
                    ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar => {
                        let pos = group_by_cols.len() + agg_index;
                        agg_index += 1;
                        ProjectItem::new(
                            ResolvedExpr::Column(Self::col_id(pos)?),
                            projection.alias.clone(),
                        )
                    }
                    ResolvedExpr::Literal(v) => {
                        let name = match projection.alias.clone() {
                            Some(alias) => alias,
                            None => format!("?column?{}", i + 1).try_into().map_err(|e| {
                                EngineError::TypeError(format!(
                                    "invalid synthesized literal column name: {e}"
                                ))
                            })?,
                        };
                        ProjectItem::new(ResolvedExpr::Literal(v.clone()), Some(name))
                    }
                    _ => {
                        return Err(EngineError::Unsupported(
                            "complex expressions in SELECT projections are not yet supported"
                                .into(),
                        ));
                    }
                })
            })
            .collect::<Result<Vec<_>, EngineError>>()
    }

    /// Converts a [`SelectProjection`] with an aggregate expression into an [`AggregateExpr`].
    ///
    /// Returns `None` for non-aggregate projections (they are skipped when building the
    /// `Aggregate` operator). Returns `Some(Err)` when the aggregate argument is not a
    /// single resolved column.
    fn projection_to_agg_expr(
        proj: &SelectProjection,
        schema: &TupleSchema,
    ) -> Option<Result<AggregateExpr, EngineError>> {
        match &proj.expr {
            ResolvedExpr::CountStar => {
                let output_name = proj
                    .alias
                    .clone()
                    .unwrap_or_else(|| "COUNT(*)".try_into().unwrap());
                let aggr = AggregateExpr::new(
                    AggregateFunc::CountStar,
                    ResolvedExpr::Literal(Value::Null),
                    output_name,
                );
                Some(Ok(aggr))
            }
            ResolvedExpr::Agg { func, arg } => {
                let arg_display = match arg.as_ref() {
                    ResolvedExpr::Column(id) => schema.col_name(*id).unwrap_or("?").to_string(),
                    _ => "expr".to_string(),
                };
                let default_name: NonEmptyString = format!("{func}({arg_display})")
                    .try_into()
                    .unwrap_or_else(|_| "agg".try_into().unwrap());
                let output_name = proj.alias.clone().unwrap_or(default_name);
                let aggr =
                    AggregateExpr::new(AggregateFunc::from(*func), (**arg).clone(), output_name);
                Some(Ok(aggr))
            }
            _ => None,
        }
    }

    /// Converts a post-operator SQL column position into a [`ColumnId`].
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if `idx` cannot fit in the
    /// [`ColumnId`] type.
    #[inline]
    fn col_id(idx: usize) -> Result<ColumnId, EngineError> {
        ColumnId::try_from(idx)
            .map_err(|_| EngineError::Unsupported(format!("column index out of bounds: {idx}")))
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end and unit tests for the SELECT binder, planner, and executor.
    //!
    //! The execution tests drive the full pipeline — parse → bind → plan →
    //! execute — through `Engine::execute_statement`. The binding tests call
    //! `BoundSelect::bind` directly to validate name resolution in isolation.

    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::{BoundFrom, BoundSelect, BoundSelectList};
    use crate::{
        Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        execution::ResolvedExpr,
        parser::{
            Parser,
            statements::{
                AggFunc, BinOp, ColumnRef, Expr, Join, JoinKind, LimitClause, OrderBy,
                OrderDirection, SelectColumns, SelectItem, SelectStatement, TableRef,
                TableWithJoins,
            },
        },
        primitives::{NonEmptyString, Predicate},
        transaction::TransactionManager,
        tuple::{Field, Tuple, TupleSchema},
        types::FixedValue,
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).unwrap());
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, dir).unwrap();
        let txn_mgr = Arc::new(TransactionManager::new(wal, bp, dir.join("wal.log")));
        (catalog, txn_mgr)
    }

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

    fn parse(sql: &str) -> crate::parser::statements::Statement {
        Parser::new(sql).parse().expect("parse")
    }

    fn run(engine: &Engine<'_>, sql: &str) -> Result<StatementResult, EngineError> {
        engine.execute_statement(parse(sql))
    }

    fn run_ok(engine: &Engine<'_>, sql: &str) -> StatementResult {
        run(engine, sql).expect("execute")
    }

    /// Drives a `SELECT` end-to-end and unwraps to `(schema, rows)`. Panics
    /// if the result is anything other than [`StatementResult::Selected`].
    fn run_select(engine: &Engine<'_>, sql: &str) -> (TupleSchema, Vec<Tuple>) {
        match run_ok(engine, sql) {
            StatementResult::Selected { schema, rows, .. } => (schema, rows),
            other => panic!("expected Selected, got {other:?}"),
        }
    }

    fn field_names(schema: &TupleSchema) -> Vec<String> {
        schema
            .logical_iter()
            .map(|f| f.name.as_str().to_owned())
            .collect()
    }

    /// Creates `users(id Int64 NN, name String, age Int64 NN)` and inserts
    /// three rows: `(1, 'alice', 30)`, `(2, 'bob', 25)`, `(3, 'cara', 30)`.
    fn seed_users(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let (catalog, txn_mgr) = make_infra(dir);
        {
            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_table(
                    &txn,
                    "users",
                    TupleSchema::new(vec![
                        field("id", Type::Int64).not_null(),
                        field("name", Type::String),
                        field("age", Type::Int64).not_null(),
                    ]),
                    vec![],
                )
                .unwrap();
            txn.commit().unwrap();
        }
        let engine = Engine::new(&catalog, &txn_mgr);
        run_ok(
            &engine,
            "INSERT INTO users (id, name, age) VALUES \
             (1, 'alice', 30), (2, 'bob', 25), (3, 'cara', 30)",
        );
        (catalog, txn_mgr)
    }

    /// Same shape as [`seed_users`] but inserts no rows.
    fn seed_empty_users(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let (catalog, txn_mgr) = make_infra(dir);
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String),
                    field("age", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();
        (catalog, txn_mgr)
    }

    /// Sorts rows lexicographically by their `Value`s.
    fn sorted_rows(mut rows: Vec<Tuple>) -> Vec<Vec<Value>> {
        let mut out: Vec<Vec<Value>> = rows
            .drain(..)
            .map(|t| (0..t.len()).map(|i| t.get(i).unwrap().clone()).collect())
            .collect();
        out.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    fn vals(row: &Tuple) -> Vec<Value> {
        (0..row.len())
            .map(|i| row.get(i).unwrap().clone())
            .collect()
    }

    fn i64_at(v: &Value) -> i64 {
        match v {
            Value::Fixed(FixedValue::Int64(n)) => *n,
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    // ──────────────────────── execution tests ────────────────────────────────

    #[test]
    fn select_star_returns_full_schema_and_all_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT * FROM users");
        assert_eq!(field_names(&schema), vec!["id", "name", "age"]);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn select_columns_in_user_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT name, id FROM users");
        assert_eq!(field_names(&schema), vec!["name", "id"]);
        assert_eq!(rows[0].len(), 2);
    }

    #[test]
    fn column_alias_renames_field_in_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id AS user_id, name FROM users");
        assert_eq!(field_names(&schema), vec!["user_id", "name"]);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn literal_with_alias_broadcasts_per_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT 'guest' AS role, name FROM users");
        assert_eq!(field_names(&schema), vec!["role", "name"]);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.get(0).unwrap(), &Value::varchar("guest".into()));
        }
    }

    #[test]
    fn literal_without_alias_uses_default_name() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, _) = run_select(&engine, "SELECT 1, name FROM users");
        let names = field_names(&schema);
        assert_eq!(names[0], "?column?1");
        assert_eq!(names[1], "name");
    }

    #[test]
    fn where_filters_rows_keeping_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id, name FROM users WHERE age > 25");
        assert_eq!(field_names(&schema), vec!["id", "name"]);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_no_match_returns_empty_with_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id FROM users WHERE age > 999");
        assert_eq!(field_names(&schema), vec!["id"]);
        assert!(rows.is_empty());
    }

    #[test]
    fn order_by_single_key_ascending_is_stable() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY age");
        let ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        assert_eq!(ids, vec![2, 1, 3]);
    }

    #[test]
    fn order_by_desc_flips_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY id DESC");
        let ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        assert_eq!(ids, vec![3, 2, 1]);
    }

    #[test]
    fn order_by_two_keys_uses_secondary_for_ties() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY age, id DESC");
        let ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        assert_eq!(ids, vec![2, 3, 1]);
    }

    #[test]
    fn limit_caps_row_count() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn limit_with_offset_returns_sliding_window() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn distinct_collapses_duplicates_after_projection() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT DISTINCT age FROM users");
        let mut ages: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        ages.sort_unstable();
        assert_eq!(ages, vec![25, 30]);
    }

    #[test]
    fn select_on_empty_table_returns_no_rows_with_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_empty_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id, name FROM users");
        assert_eq!(field_names(&schema), vec!["id", "name"]);
        assert!(rows.is_empty());
    }

    /// **Known deviation:** the `Aggregate` operator does not yet synthesize
    /// the empty-group row, so we get zero rows back. This test documents the
    /// current behavior so a future fix flips the assertion.
    #[test]
    fn count_star_on_empty_table_emits_schema_but_no_row_today() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_empty_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT COUNT(*) FROM users");
        assert_eq!(field_names(&schema), vec!["COUNT(*)"]);
        // TODO(SQL-correctness): SQL says this should be vec![one row of 0].
        assert_eq!(rows.len(), 0, "empty-input ungrouped aggregate gap");
    }

    #[test]
    fn count_star_returns_total_row_count() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT COUNT(*) AS n FROM users");
        assert_eq!(field_names(&schema), vec!["n"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(3));
    }

    #[test]
    fn sum_aggregate_reduces_to_single_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT SUM(age) FROM users");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(85));
    }

    #[test]
    fn group_by_age_counts_per_group() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT age, COUNT(*) FROM users GROUP BY age");
        assert_eq!(field_names(&schema), vec!["age", "COUNT(*)"]);

        let got = sorted_rows(rows);
        assert_eq!(got, vec![vec![Value::int64(25), Value::int64(1)], vec![
            Value::int64(30),
            Value::int64(2)
        ],]);
    }

    #[test]
    fn select_order_is_rewired_above_aggregate() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT COUNT(*), age FROM users GROUP BY age");
        assert_eq!(field_names(&schema), vec!["COUNT(*)", "age"]);

        let got = sorted_rows(rows);
        assert_eq!(got, vec![vec![Value::int64(1), Value::int64(25)], vec![
            Value::int64(2),
            Value::int64(30)
        ],]);
    }

    #[test]
    fn aliases_on_group_key_and_aggregate_propagate() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, _) = run_select(
            &engine,
            "SELECT age AS years, COUNT(*) AS n FROM users GROUP BY age",
        );
        assert_eq!(field_names(&schema), vec!["years", "n"]);
    }

    #[test]
    fn literal_interleaved_with_aggregate_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(
            &engine,
            "SELECT 'group:' AS label, age, COUNT(*) AS n FROM users GROUP BY age",
        );
        assert_eq!(field_names(&schema), vec!["label", "age", "n"]);
        for row in &rows {
            assert_eq!(row.get(0).unwrap(), &Value::varchar("group:".into()));
        }
    }

    // ─────────────── error paths (execution) ──────────────────────────────

    fn expect_unsupported(r: Result<StatementResult, EngineError>) {
        match r {
            Err(EngineError::Unsupported(_)) => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn bare_column_not_in_group_by_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        expect_unsupported(run(
            &engine,
            "SELECT name, COUNT(*) FROM users GROUP BY age",
        ));
    }

    #[test]
    fn star_with_group_by_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        expect_unsupported(run(&engine, "SELECT * FROM users GROUP BY age"));
    }

    #[test]
    fn order_by_with_aggregates_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        expect_unsupported(run(
            &engine,
            "SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age",
        ));
    }

    #[test]
    fn having_clause_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        expect_unsupported(run(
            &engine,
            "SELECT age, COUNT(*) FROM users GROUP BY age HAVING age > 0",
        ));
    }

    #[test]
    fn where_project_limit_compose() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT name FROM users WHERE age = 30 LIMIT 1");
        assert_eq!(field_names(&schema), vec!["name"]);
        assert_eq!(rows.len(), 1);
        let name = vals(&rows[0]).remove(0);
        assert!(matches!(name.as_str(), Some(s) if s == "alice" || s == "cara"));
    }

    #[test]
    fn where_then_count_star_counts_filtered_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT COUNT(*) FROM users WHERE age = 30");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    // ──────────────────────── aggregate function tests ───────────────────────

    /// Seeds `users` with one NULL name: `(1,'alice',30)`, `(2,NULL,25)`, `(3,'cara',30)`.
    fn seed_users_with_null_name(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let (catalog, txn_mgr) = make_infra(dir);
        {
            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_table(
                    &txn,
                    "users",
                    TupleSchema::new(vec![
                        field("id", Type::Int64).not_null(),
                        field("name", Type::String),
                        field("age", Type::Int64).not_null(),
                    ]),
                    vec![],
                )
                .unwrap();
            txn.commit().unwrap();
        }
        let engine = Engine::new(&catalog, &txn_mgr);
        run_ok(
            &engine,
            "INSERT INTO users (id, name, age) VALUES \
             (1, 'alice', 30), (2, NULL, 25), (3, 'cara', 30)",
        );
        (catalog, txn_mgr)
    }

    // expr.rs tests the parser → AST shape.
    // resolve.rs tests resolve/eval at the unit level.
    // These tests verify the operators work end-to-end through the engine:
    // SQL string → parser → binder → planner → executor → rows.

    #[test]
    fn where_is_null_filters_to_null_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_with_null_name(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name IS NULL");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn where_is_not_null_excludes_null_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_with_null_name(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name IS NOT NULL");
        assert_eq!(rows.len(), 2);
        let mut ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn where_in_list_keeps_matching_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE id IN (1, 3)");
        assert_eq!(rows.len(), 2);
        let mut ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn where_not_in_list_excludes_matching_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE id NOT IN (1, 3)");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn where_between_inclusive_bounds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // age=25 is within [25, 28]; age=30 is not
        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE age BETWEEN 25 AND 28");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn where_not_between_excludes_range() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // age NOT BETWEEN 26 AND 40 → only bob (age=25) passes
        let (_, rows) = run_select(
            &engine,
            "SELECT id FROM users WHERE age NOT BETWEEN 26 AND 40",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn where_like_prefix_matches_name() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // 'a%' matches 'alice' only
        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name LIKE 'a%'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(1));
    }

    #[test]
    fn where_not_like_excludes_matching_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // NOT LIKE 'a%' → bob and cara (2 rows)
        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name NOT LIKE 'a%'");
        assert_eq!(rows.len(), 2);
        let mut ids: Vec<i64> = rows.iter().map(|r| i64_at(r.get(0).unwrap())).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn where_like_underscore_wildcard_matches_one_char() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // '_ob' matches 'bob': _ consumes 'b', then 'ob' matches literally
        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name LIKE '_ob'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(2));
    }

    #[test]
    fn where_like_on_null_column_skips_null_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_with_null_name(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // row 2 has name=NULL; LIKE on NULL → NULL → ResolvedExpr::eval_bool returns false
        let (_, rows) = run_select(&engine, "SELECT id FROM users WHERE name LIKE '%'");
        assert_eq!(rows.len(), 2, "NULL name row must not be returned by LIKE");
    }

    #[test]
    fn avg_returns_float64() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT AVG(age) FROM users");
        assert_eq!(field_names(&schema), vec!["AVG(age)"]);
        assert_eq!(rows.len(), 1);
        match rows[0].get(0).unwrap() {
            Value::Fixed(FixedValue::Float64(f)) => assert!((f - (85.0 / 3.0)).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn min_returns_minimum_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT MIN(age) FROM users");
        assert_eq!(field_names(&schema), vec!["MIN(age)"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(25));
    }

    #[test]
    fn max_returns_maximum_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT MAX(age) FROM users");
        assert_eq!(field_names(&schema), vec!["MAX(age)"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::int64(30));
    }

    #[test]
    fn count_col_skips_null_values() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_with_null_name(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, count_col_rows) = run_select(&engine, "SELECT COUNT(name) FROM users");
        let (_, count_star_rows) = run_select(&engine, "SELECT COUNT(*) FROM users");

        assert_eq!(count_col_rows[0].get(0).unwrap(), &Value::int64(2));
        assert_eq!(count_star_rows[0].get(0).unwrap(), &Value::int64(3));
    }

    #[test]
    fn multiple_aggregates_in_single_query() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(
            &engine,
            "SELECT SUM(age) AS s, COUNT(*) AS n, AVG(age) AS a, MIN(age) AS mn, MAX(age) AS mx \
             FROM users",
        );
        assert_eq!(field_names(&schema), vec!["s", "n", "a", "mn", "mx"]);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get(0).unwrap(), &Value::int64(85));
        assert_eq!(row.get(1).unwrap(), &Value::int64(3));
        match row.get(2).unwrap() {
            Value::Fixed(FixedValue::Float64(f)) => assert!((f - (85.0 / 3.0)).abs() < 1e-9),
            other => panic!("expected Float64 for AVG, got {other:?}"),
        }
        assert_eq!(row.get(3).unwrap(), &Value::int64(25));
        assert_eq!(row.get(4).unwrap(), &Value::int64(30));
    }

    #[test]
    fn min_max_avg_on_empty_table_return_null() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_empty_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(
            &engine,
            "SELECT MIN(age) AS mn, MAX(age) AS mx, AVG(age) AS a FROM users",
        );
        // Ungrouped aggregates on an empty table produce no rows today (same gap as COUNT(*)).
        assert_eq!(rows.len(), 0, "empty-input ungrouped aggregate gap");
    }

    #[test]
    fn group_by_with_min_max_per_group() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // age=30 group: id values are 1 and 3 → min=1, max=3
        // age=25 group: id value is 2 → min=2, max=2
        let (schema, rows) = run_select(
            &engine,
            "SELECT age, MIN(id) AS mn, MAX(id) AS mx FROM users GROUP BY age",
        );
        assert_eq!(field_names(&schema), vec!["age", "mn", "mx"]);

        let got = sorted_rows(rows);
        assert_eq!(got, vec![
            vec![Value::int64(25), Value::int64(2), Value::int64(2)],
            vec![Value::int64(30), Value::int64(1), Value::int64(3)],
        ]);
    }

    // ──────────────────────── join execution tests ───────────────────────────

    /// Seeds both `users` and `orders` for join execution tests.
    ///
    /// users: `(1,'alice',30)`, `(2,'bob',25)`, `(3,'cara',30)`
    /// orders: `(1, user_id=1, total=100)`, `(2, user_id=1, total=200)`, `(3, user_id=2,
    /// total=150)`
    ///
    /// alice has 2 orders, bob has 1 order, cara has none — so LEFT JOIN will
    /// null-pad cara's row and INNER JOIN will exclude her entirely.
    fn seed_users_and_orders(dir: &Path) -> (Catalog, Arc<TransactionManager>) {
        let (catalog, txn_mgr) = make_infra(dir);
        {
            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_table(
                    &txn,
                    "users",
                    TupleSchema::new(vec![
                        field("id", Type::Int64).not_null(),
                        field("name", Type::String),
                        field("age", Type::Int64).not_null(),
                    ]),
                    vec![],
                )
                .unwrap();
            catalog
                .create_table(
                    &txn,
                    "orders",
                    TupleSchema::new(vec![
                        field("id", Type::Int64).not_null(),
                        field("user_id", Type::Int64).not_null(),
                        field("total", Type::Int64).not_null(),
                    ]),
                    vec![],
                )
                .unwrap();
            txn.commit().unwrap();
        }
        let engine = Engine::new(&catalog, &txn_mgr);
        run_ok(
            &engine,
            "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'cara', 30)",
        );
        run_ok(
            &engine,
            "INSERT INTO orders (id, user_id, total) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)",
        );
        (catalog, txn_mgr)
    }

    #[test]
    fn inner_join_returns_only_matched_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // The merged tuple is: users.id, users.name, users.age, orders.id, orders.user_id,
        // orders.total alice (id=1) has 2 orders → 2 rows; bob (id=2) has 1 order → 1 row;
        // cara has none → excluded.
        let (schema, rows) = run_select(
            &engine,
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        );
        assert_eq!(schema.logical_num_fields(), 6);
        assert_eq!(rows.len(), 3);

        // cara's users.id (3) must not appear in any row's first column.
        let user_ids: Vec<&Value> = rows.iter().map(|r| r.get(0).unwrap()).collect();
        assert!(
            !user_ids.contains(&&Value::int64(3)),
            "cara should be excluded by inner join"
        );
    }

    #[test]
    fn inner_join_qualified_select_projects_correct_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // Qualified column references in SELECT must resolve to the right offsets.
        // users.id is at physical index 0; orders.total is at physical index 5.
        let (schema, rows) = run_select(
            &engine,
            "SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        );
        assert_eq!(field_names(&schema), vec!["id", "total"]);
        assert_eq!(rows.len(), 3);

        // totals for alice=100,200 and bob=150; sort to get a stable order
        let got = sorted_rows(rows);
        assert_eq!(got, vec![
            vec![Value::int64(1), Value::int64(100)],
            vec![Value::int64(1), Value::int64(200)],
            vec![Value::int64(2), Value::int64(150)],
        ]);
    }

    #[test]
    fn left_join_null_pads_unmatched_left_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // LEFT JOIN keeps every left row. cara (id=3) has no matching order,
        // so the orders columns (positions 3-5 in the merged tuple) are NULL.
        let (schema, rows) = run_select(
            &engine,
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
        );
        assert_eq!(schema.logical_num_fields(), 6);
        assert_eq!(rows.len(), 4, "alice×2 + bob×1 + cara×1 (null-padded)");

        let null_padded: Vec<&Tuple> = rows
            .iter()
            .filter(|r| r.get(3).unwrap().is_null())
            .collect();
        assert_eq!(
            null_padded.len(),
            1,
            "exactly one row should be null-padded"
        );
        assert_eq!(
            null_padded[0].get(0).unwrap(),
            &Value::int64(3),
            "the null-padded row must be cara (users.id = 3)"
        );
        // All three orders columns are NULL for that row.
        assert!(null_padded[0].get(4).unwrap().is_null());
        assert!(null_padded[0].get(5).unwrap().is_null());
    }

    #[test]
    fn cross_join_produces_cartesian_product() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // 3 users × 3 orders = 9 rows, no predicate.
        let (schema, rows) = run_select(&engine, "SELECT * FROM users CROSS JOIN orders");
        assert_eq!(schema.logical_num_fields(), 6);
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn right_join_column_order_matches_sql_written_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // The planner internally swaps inputs (orders LEFT JOIN users) to reuse
        // the LeftOuter path, then adds a reorder projection to restore the
        // SQL-written column layout: (users.id, users.name, users.age, orders.id,
        // orders.user_id, orders.total).
        //
        // If the reorder projection is missing or wrong, columns bleed across the
        // boundary — e.g. users.name at index 1 would instead show orders.user_id.
        // This test pins every column of one specific row to catch that.
        let (schema, rows) = run_select(
            &engine,
            "SELECT users.id, users.name, orders.total \
             FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        );
        assert_eq!(field_names(&schema), vec!["id", "name", "total"]);
        assert_eq!(rows.len(), 3, "all 3 orders have a matching user");

        // Sort by orders.total so the assertion order is deterministic.
        let mut got = rows
            .iter()
            .map(|r| {
                (
                    r.get(0).unwrap().clone(), // users.id
                    r.get(1).unwrap().clone(), // users.name
                    r.get(2).unwrap().clone(), // orders.total
                )
            })
            .collect::<Vec<_>>();
        got.sort_by_key(|(_, _, t)| i64_at(t));

        // Seed: alice(id=1) → total=100, bob(id=2) → total=150, alice(id=1) → total=200.
        // Sorted ascending by total: 100, 150, 200.
        assert_eq!(got, vec![
            (
                Value::int64(1),
                Value::varchar("alice".into()),
                Value::int64(100)
            ),
            (
                Value::int64(2),
                Value::varchar("bob".into()),
                Value::int64(150)
            ),
            (
                Value::int64(1),
                Value::varchar("alice".into()),
                Value::int64(200)
            ),
        ]);
    }

    #[test]
    fn right_join_null_pads_unmatched_right_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users_and_orders(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        // Add an order whose user_id=999 matches nobody in the users table.
        run_ok(
            &engine,
            "INSERT INTO orders (id, user_id, total) VALUES (99, 999, 500)",
        );

        let (_, rows) = run_select(
            &engine,
            "SELECT users.id, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        );
        // 3 matched orders + 1 unmatched order = 4 rows total
        assert_eq!(rows.len(), 4);

        // Exactly one row must have NULL in users.id (the unmatched order).
        let null_rows: Vec<&Tuple> = rows
            .iter()
            .filter(|r| r.get(0).unwrap().is_null())
            .collect();
        assert_eq!(null_rows.len(), 1, "one right row has no matching user");
        assert_eq!(
            null_rows[0].get(1).unwrap(),
            &Value::int64(500),
            "orders.total=500 for the unmatched order"
        );
    }

    // ──────────────────────── binder unit tests ──────────────────────────────
    // These call BoundSelect::bind directly to validate name resolution in
    // isolation, without going through the full Engine pipeline.

    /// `users(id Uint64 NN, name String, age Int64 NN)` — indices 0, 1, 2.
    fn users_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("name", Type::String),
            field("age", Type::Int64).not_null(),
        ])
    }

    /// `orders(id Uint64 NN, user_id Uint64 NN, total Int64 NN)`.
    /// Once joined onto `users`, its columns sit at indices 3, 4, 5.
    fn orders_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("user_id", Type::Uint64).not_null(),
            field("total", Type::Int64).not_null(),
        ])
    }

    fn create_table_direct(
        catalog: &Catalog,
        txn_mgr: &Arc<TransactionManager>,
        name: &str,
        schema: TupleSchema,
    ) {
        let txn = txn_mgr.begin().unwrap();
        catalog.create_table(&txn, name, schema, vec![]).unwrap();
        txn.commit().unwrap();
    }

    fn table_ref(name: &str, alias: Option<&str>) -> TableRef {
        TableRef {
            name: NonEmptyString::new(name).unwrap(),
            alias: alias.map(|a| NonEmptyString::new(a).unwrap()),
        }
    }

    fn just(name: &str) -> TableWithJoins {
        TableWithJoins {
            table: table_ref(name, None),
            joins: vec![],
        }
    }

    fn aliased(name: &str, alias: &str) -> TableWithJoins {
        TableWithJoins {
            table: table_ref(name, Some(alias)),
            joins: vec![],
        }
    }

    fn with_join(mut base: TableWithJoins, j: Join) -> TableWithJoins {
        base.joins.push(j);
        base
    }

    fn inner_join(name: &str, alias: Option<&str>, on: Expr) -> Join {
        Join {
            kind: JoinKind::Inner,
            table: table_ref(name, alias),
            on: Some(on),
        }
    }

    fn select_stmt(columns: SelectColumns, from: Vec<TableWithJoins>) -> SelectStatement {
        SelectStatement {
            distinct: false,
            columns,
            from,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        }
    }

    fn star_from(t: TableWithJoins) -> SelectStatement {
        select_stmt(SelectColumns::All, vec![t])
    }

    fn exprs(items: Vec<SelectItem>) -> SelectColumns {
        SelectColumns::Exprs(items)
    }

    fn item(expr: Expr, alias: Option<&str>) -> SelectItem {
        SelectItem {
            expr,
            alias: alias.map(|a| NonEmptyString::new(a).unwrap()),
        }
    }

    fn col_expr(name: &str) -> Expr {
        Expr::Column(ColumnRef::from(name))
    }

    fn pred(c: &str, op: Predicate, v: Value) -> Expr {
        let bin_op = match op {
            Predicate::Equals => BinOp::Eq,
            Predicate::NotEqual | Predicate::NotEqualBracket => BinOp::NotEq,
            Predicate::LessThan => BinOp::Lt,
            Predicate::LessThanOrEqual => BinOp::LtEq,
            Predicate::GreaterThan => BinOp::Gt,
            Predicate::GreaterThanOrEqual => BinOp::GtEq,
            Predicate::Like => panic!("test helper `pred`: LIKE is not an Expr::BinaryOp"),
        };
        Expr::BinaryOp {
            lhs: Box::new(Expr::Column(ColumnRef::from(c))),
            op: bin_op,
            rhs: Box::new(Expr::Literal(v)),
        }
    }

    fn expect_err<T, E>(r: Result<T, E>) -> E {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        }
    }

    fn bind_with_users<F>(build: F) -> Result<BoundSelect, EngineError>
    where
        F: FnOnce() -> SelectStatement,
    {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        create_table_direct(&catalog, &txn_mgr, "users", users_schema());

        let txn = txn_mgr.begin().unwrap();
        let res = BoundSelect::bind(build(), &catalog, &txn);
        txn.commit().unwrap();
        res
    }

    fn bind_with_users_and_orders<F>(build: F) -> Result<BoundSelect, EngineError>
    where
        F: FnOnce() -> SelectStatement,
    {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        create_table_direct(&catalog, &txn_mgr, "users", users_schema());
        create_table_direct(&catalog, &txn_mgr, "orders", orders_schema());

        let txn = txn_mgr.begin().unwrap();
        let res = BoundSelect::bind(build(), &catalog, &txn);
        txn.commit().unwrap();
        res
    }

    #[test]
    fn bind_select_star_single_table() {
        let bound = bind_with_users(|| star_from(just("users"))).unwrap();
        assert!(matches!(bound.select_list, BoundSelectList::Star));
        assert!(matches!(bound.from, BoundFrom::Table { .. }));
        assert!(bound.filter.is_none());
        assert!(bound.group_by.is_empty());
        assert!(bound.order_by.is_empty());
    }

    #[test]
    fn bind_select_no_from_errors() {
        let err = expect_err(bind_with_users(|| select_stmt(SelectColumns::All, vec![])));
        assert!(matches!(err, EngineError::Unsupported(_)));
    }

    #[test]
    fn bind_select_multi_table_from_errors() {
        let err = expect_err(bind_with_users_and_orders(|| {
            select_stmt(SelectColumns::All, vec![just("users"), just("orders")])
        }));
        assert!(matches!(err, EngineError::Unsupported(_)));
    }

    #[test]
    fn bind_projection_columns_by_index() {
        let bound = bind_with_users(|| {
            select_stmt(
                exprs(vec![
                    item(col_expr("id"), None),
                    item(col_expr("name"), None),
                ]),
                vec![just("users")],
            )
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!("expected Items");
        };
        assert_eq!(list.len(), 2);
        assert!(matches!(list[0].expr, ResolvedExpr::Column(c) if u32::from(c) == 0));
        assert!(matches!(list[1].expr, ResolvedExpr::Column(c) if u32::from(c) == 1));
        assert!(list[0].alias.is_none());
    }

    #[test]
    fn bind_projection_alias_preserved() {
        let bound = bind_with_users(|| {
            select_stmt(exprs(vec![item(col_expr("id"), Some("user_id"))]), vec![
                just("users"),
            ])
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert_eq!(list[0].alias.as_deref(), Some("user_id"));
        assert!(matches!(list[0].expr, ResolvedExpr::Column(_)));
    }

    #[test]
    fn bind_projection_literals() {
        let bound = bind_with_users(|| {
            select_stmt(
                exprs(vec![
                    item(Expr::Literal(Value::int64(1)), None),
                    item(Expr::Literal(Value::varchar("hi".into())), Some("greet")),
                    item(Expr::Literal(Value::Null), None),
                ]),
                vec![just("users")],
            )
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert!(matches!(
            &list[0].expr,
            ResolvedExpr::Literal(v) if *v == Value::int64(1)
        ));
        assert!(matches!(
            &list[1].expr,
            ResolvedExpr::Literal(v) if v.as_str() == Some("hi")
        ));
        assert_eq!(list[1].alias.as_deref(), Some("greet"));
        assert!(matches!(list[2].expr, ResolvedExpr::Literal(Value::Null)));
    }

    #[test]
    fn bind_projection_count_star_default_name() {
        let bound = bind_with_users(|| {
            select_stmt(exprs(vec![item(Expr::CountStar, None)]), vec![just(
                "users",
            )])
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert!(matches!(list[0].expr, ResolvedExpr::CountStar));
        assert!(list[0].alias.is_none());
    }

    #[test]
    fn bind_projection_count_star_with_alias() {
        let bound = bind_with_users(|| {
            select_stmt(exprs(vec![item(Expr::CountStar, Some("n"))]), vec![just(
                "users",
            )])
        })
        .unwrap();
        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert!(matches!(list[0].expr, ResolvedExpr::CountStar));
        assert_eq!(list[0].alias.as_deref(), Some("n"));
    }

    #[test]
    fn bind_projection_aggregate_resolves_col_id() {
        let bound = bind_with_users(|| {
            select_stmt(
                exprs(vec![item(
                    Expr::Agg {
                        func: AggFunc::Sum,
                        arg: Box::new(col_expr("age")),
                    },
                    None,
                )]),
                vec![just("users")],
            )
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        let ResolvedExpr::Agg { func, arg } = &list[0].expr else {
            panic!("expected Agg");
        };
        assert_eq!(*func, AggFunc::Sum);
        assert!(matches!(arg.as_ref(), ResolvedExpr::Column(c) if u32::from(*c) == 2));
        assert!(list[0].alias.is_none());
    }

    #[test]
    fn bind_projection_aggregate_literal_arg_is_now_supported() {
        // SUM(1) used to be rejected; arbitrary expressions in aggregate args are now valid.
        let bound = bind_with_users(|| {
            select_stmt(
                exprs(vec![item(
                    Expr::Agg {
                        func: AggFunc::Sum,
                        arg: Box::new(Expr::Literal(Value::int64(1))),
                    },
                    None,
                )]),
                vec![just("users")],
            )
        });
        assert!(bound.is_ok());
    }

    #[test]
    fn bind_projection_mixed_list_preserves_order() {
        let bound = bind_with_users(|| {
            select_stmt(
                exprs(vec![
                    item(col_expr("id"), None),
                    item(Expr::CountStar, None),
                    item(col_expr("name"), None),
                ]),
                vec![just("users")],
            )
        })
        .unwrap();
        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert!(matches!(list[0].expr, ResolvedExpr::Column(_)));
        assert!(matches!(list[1].expr, ResolvedExpr::CountStar));
        assert!(matches!(list[2].expr, ResolvedExpr::Column(_)));
    }

    #[test]
    fn bind_projection_unknown_column_errors() {
        let err = expect_err(bind_with_users(|| {
            select_stmt(exprs(vec![item(col_expr("nope"), None)]), vec![just(
                "users",
            )])
        }));
        assert!(matches!(err, EngineError::UnknownColumn { ref column, .. } if column == "nope"));
    }

    #[test]
    fn bind_join_on_clause_sees_both_sides() {
        let bound = bind_with_users_and_orders(|| {
            star_from(with_join(
                aliased("users", "u"),
                inner_join(
                    "orders",
                    Some("o"),
                    pred("u.id", Predicate::Equals, Value::uint64(0)),
                ),
            ))
        })
        .unwrap();
        assert!(matches!(bound.from, BoundFrom::Join { .. }));
    }

    #[test]
    fn bind_join_ambiguous_column_in_where_is_rejected() {
        // `id` exists in both `users` and `orders` — with eager resolution this
        // is caught at bind time rather than silently resolved at eval time.
        let mut stmt = star_from(with_join(
            just("users"),
            inner_join(
                "orders",
                None,
                pred("user_id", Predicate::Equals, Value::uint64(0)),
            ),
        ));
        stmt.where_clause = Some(pred("id", Predicate::Equals, Value::uint64(0)));
        assert!(bind_with_users_and_orders(|| stmt).is_err());
    }

    #[test]
    fn bind_join_qualified_columns_resolve_to_correct_offsets() {
        let bound = bind_with_users_and_orders(|| {
            select_stmt(
                exprs(vec![
                    item(col_expr("users.id"), None),
                    item(col_expr("orders.id"), None),
                ]),
                vec![with_join(
                    just("users"),
                    inner_join(
                        "orders",
                        None,
                        pred("user_id", Predicate::Equals, Value::uint64(0)),
                    ),
                )],
            )
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        let ResolvedExpr::Column(left_id) = list[0].expr else {
            panic!();
        };
        let ResolvedExpr::Column(right_id) = list[1].expr else {
            panic!();
        };
        assert_eq!(u32::from(left_id), 0);
        assert_eq!(u32::from(right_id), 3);
    }

    #[test]
    fn bind_unknown_qualifier_in_where_is_rejected() {
        // With eager resolution, an unknown qualifier in WHERE is caught at bind time.
        let bound = bind_with_users(|| {
            let mut s = star_from(just("users"));
            s.where_clause = Some(pred("x.id", Predicate::Equals, Value::uint64(0)));
            s
        });
        assert!(bound.is_err());
    }

    #[test]
    fn bind_group_order_limit_distinct_pass_through() {
        let bound = bind_with_users(|| {
            let mut s = star_from(just("users"));
            s.distinct = true;
            s.group_by = vec![ColumnRef::from("age")];
            s.order_by = vec![OrderBy(ColumnRef::from("name"), OrderDirection::Desc)];
            s.limit = Some(LimitClause {
                limit: Some(10),
                offset: 5,
            });
            s
        })
        .unwrap();

        assert!(bound.distinct);
        assert_eq!(bound.group_by.len(), 1);
        assert_eq!(u32::from(bound.group_by[0]), 2);
        assert_eq!(bound.order_by.len(), 1);
        assert_eq!(u32::from(bound.order_by[0].0), 1);
        assert_eq!(bound.order_by[0].1, OrderDirection::Desc);
        let lim = bound.limit.unwrap();
        assert_eq!(lim.limit, Some(10));
        assert_eq!(lim.offset, 5);
    }

    #[test]
    fn bind_having_resolves() {
        let bound = bind_with_users(|| {
            let mut s = star_from(just("users"));
            s.having = Some(pred("age", Predicate::GreaterThan, Value::int64(0)));
            s
        })
        .unwrap();
        assert!(bound.having.is_some());
    }
}
