//! SQL `SELECT` execution planning for the engine.
//!
//! This module covers the engine-side bridge from a parsed SQL `SELECT` statement
//! to a physical executor tree. The binder has already resolved names against the
//! catalog, so this layer mainly chooses operators for `FROM`, `WHERE`, `SELECT`,
//! `GROUP BY`, `ORDER BY`, `DISTINCT`, and `LIMIT/OFFSET`, then drains the plan
//! into a [`StatementResult::Selected`] result.
//!
//! # Shape
//!
//! - [`Engine::exec_select`] — statement entry point for `SELECT ... FROM ...`.
//! - `collect_heap_files` — gathers the heap files named by the bound `FROM` tree.
//! - `build_plan` — lowers a [`BoundSelect`] into scan/filter/sort/project/aggregate/limit nodes.
//! - `build_project` — maps non-aggregate `SELECT` list entries to `ProjectItem`s.
//! - `build_aggregate` — maps `GROUP BY` and aggregate `SELECT` items to `Aggregate` plus a
//!   rewiring `Project`.
//!
//! # How it works
//!
//! Planning follows SQL clause order where it matters for tuple shape:
//! `FROM` builds the base scan, `WHERE` filters rows, non-aggregate `ORDER BY`
//! sorts while the full `FROM` schema is still available, projection narrows the
//! tuple, `DISTINCT` removes duplicate projected rows, and `LIMIT/OFFSET` trims
//! the final stream. Aggregating queries use a blocking `Aggregate` operator
//! followed by a small `Project` that restores the user-written `SELECT` order.
//!
//! # NULL semantics
//!
//! `WHERE` NULL behavior is delegated to `BooleanExpression`: comparisons with
//! `NULL` evaluate to `false` rather than full SQL three-valued logic. Aggregate
//! NULL behavior is delegated to `Aggregate`: `COUNT(*)` counts every row, while
//! column aggregates skip `NULL` inputs.

use std::sync::Arc;

use fallible_iterator::FallibleIterator;

use crate::{
    FileId, TransactionId,
    binder::{
        Bound, BoundSelect,
        query::{BoundFrom, BoundProjection, BoundSelectItem, BoundSelectList},
    },
    catalog::manager::Catalog,
    engine::{Engine, EngineError, StatementResult},
    execution::{
        Executor, PlanNode,
        unary::{ProjectItem, SortKey},
    },
    heap::file::HeapFile,
    parser::statements::{OrderDirection, Statement},
    primitives::ColumnId,
};

impl Engine<'_> {
    /// Executes a bound SQL `SELECT` and returns the selected rows.
    ///
    /// The parser has already produced a [`Statement::Select`] and the binder
    /// turns it into a [`BoundSelect`] with column names resolved to numeric
    /// positions. The output tuple layout is the final SQL result layout:
    /// `SELECT *` keeps the `FROM` schema, explicit projections follow the
    /// user-written `SELECT` list, and aggregating queries use the projection
    /// order after the aggregate rewiring step.
    ///
    /// # SQL examples
    ///
    /// Given `users(id, name, age)` with binder-resolved indices
    /// `id → 0`, `name → 1`, `age → 2`:
    ///
    /// ```sql
    /// -- SELECT * FROM users;
    /// ```
    ///
    /// ```ignore
    /// BoundSelect {
    ///     from: BoundFrom::Table { table: users_ref, file_id, schema: users_schema, column_offset: 0 },
    ///     select_list: BoundSelectList::Star,
    ///     filter: None,
    ///     group_by: vec![],
    ///     having: None,
    ///     distinct: false,
    ///     order_by: vec![],
    ///     limit: None,
    /// }
    /// ```
    ///
    /// ```sql
    /// -- SELECT name FROM users WHERE age >= 18 ORDER BY name LIMIT 10;
    /// ```
    ///
    /// ```ignore
    /// BoundSelect {
    ///     from: BoundFrom::Table { table: users_ref, file_id, schema: users_schema, column_offset: 0 },
    ///     select_list: BoundSelectList::Items(vec![
    ///         BoundProjection {
    ///             item: BoundSelectItem::Column(ColumnId::try_from(1).unwrap()),
    ///             alias: None,
    ///         },
    ///     ]),
    ///     filter: Some(BooleanExpression::col_op_lit(
    ///         2,
    ///         Predicate::GreaterThanOrEqual,
    ///         Value::Int64(18),
    ///     )),
    ///     group_by: vec![],
    ///     having: None,
    ///     distinct: false,
    ///     order_by: vec![(ColumnId::try_from(1).unwrap(), OrderDirection::Asc)],
    ///     limit: Some(LimitClause { limit: Some(10), offset: 0 }),
    /// }
    /// ```
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) AS n FROM users GROUP BY age;
    /// ```
    ///
    /// ```ignore
    /// BoundSelect {
    ///     from: BoundFrom::Table {
    ///         table: users_ref,
    ///         file_id,
    ///         schema: users_schema,
    ///         column_offset: 0,
    ///     },
    ///     select_list: BoundSelectList::Items(vec![
    ///         BoundProjection {
    ///             item: BoundSelectItem::Column(ColumnId::try_from(2).unwrap()),
    ///             alias: None,
    ///         },
    ///         BoundProjection {
    ///             item: BoundSelectItem::Aggregate(AggregateExpr {
    ///                 func: AggregateFunc::CountStar,
    ///                 col_id: ColumnId::default(),
    ///                 output_name: "n".into(),
    ///             }),
    ///             alias: Some("n".into()),
    ///         },
    ///     ]),
    ///     group_by: vec![ColumnId::try_from(2).unwrap()],
    ///     filter: None,
    ///     having: None,
    ///     distinct: false,
    ///     order_by: vec![],
    ///     limit: None,
    /// }
    /// ```
    ///
    /// # SQL → operator mapping
    ///
    /// ```sql
    /// -- SELECT DISTINCT name
    /// -- FROM users
    /// -- WHERE age >= 18
    /// -- ORDER BY name
    /// -- LIMIT 10 OFFSET 5;
    /// ```
    ///
    /// ```ignore
    /// let mut plan = PlanNode::seq_scan(users_heap, txn);
    /// plan = PlanNode::filter(plan, BooleanExpression::col_op_lit(
    ///     2,
    ///     Predicate::GreaterThanOrEqual,
    ///     Value::Int64(18),
    /// ));
    /// plan = PlanNode::sort(plan, vec![SortKey {
    ///     col_id: ColumnId::try_from(1).unwrap(),
    ///     ascending: true,
    /// }]);
    /// plan = PlanNode::project(plan, vec![
    ///     ProjectItem::column(ColumnId::try_from(1).unwrap(), None),
    /// ])?;
    /// plan = PlanNode::distinct(plan);
    /// plan = PlanNode::limit(plan, 10, 5);
    /// ```
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
    pub(super) fn exec_select(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::Select(bound) = bound else {
                unreachable!("dispatcher routed Select here")
            };

            let root_table_name = bound.from.root_table_name();
            let mut heap_files = Vec::with_capacity(bound.from.table_count());
            Self::collect_heap_files(&bound.from, catalog, &mut heap_files)?;
            let mut plan = Self::build_plan(&bound, &heap_files, txn.transaction_id())?;
            let schema = plan.schema().to_owned();

            let mut rows = Vec::new();
            while let Some(t) = plan
                .next()
                .map_err(|e| EngineError::type_error(e.to_string()))?
            {
                rows.push(t);
            }
            drop(plan); // releases the &HeapFile borrows before `heaps` goes out of scope

            Ok(StatementResult::Selected {
                table: root_table_name.to_string(),
                schema,
                rows,
            })
        })
    }

    /// Collects heap files for every base table named by a SQL `FROM` tree.
    ///
    /// `FROM users` contributes one `(file_id, heap)` pair. `FROM users JOIN
    /// orders ...` and cross products are walked recursively so the later
    /// planner can look up each `BoundFrom::Table` by [`FileId`] without
    /// reaching back into the catalog.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT * FROM users;
    /// --   collect_heap_files(BoundFrom::Table { file_id: users_file_id, .. }, catalog, heaps)
    ///
    /// -- SELECT * FROM users JOIN orders ON users.id = orders.user_id;
    /// --   collect_heap_files(BoundFrom::Join { left: users, right: orders, .. }, catalog, heaps)
    ///
    /// -- SELECT * FROM users CROSS JOIN orders;
    /// --   collect_heap_files(BoundFrom::Cross { left: users, right: orders, .. }, catalog, heaps)
    /// ```
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
                    .ok_or_else(|| EngineError::TableNotFound(table.name.clone()))?;
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
    /// This is where clause-level SQL shape becomes operator shape. For
    /// non-aggregating queries, `ORDER BY` is planned before `Project` so keys
    /// can still refer to columns that are not selected. For aggregating
    /// queries, `Aggregate` emits `[GROUP BY columns..., aggregate results...]`
    /// and a following `Project` restores the user-written `SELECT` order.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT name FROM users WHERE age = 30;
    /// ```
    ///
    /// ```ignore
    /// Project([Column(1)])
    ///   Filter(BooleanExpression::col_op_lit(2, Predicate::Equals, Value::Int64(30)))
    ///     SeqScan(users)
    /// ```
    ///
    /// ```sql
    /// -- SELECT name FROM users ORDER BY age DESC;
    /// ```
    ///
    /// ```ignore
    /// Project([Column(1)])
    ///   Sort([SortKey { col_id: ColumnId::try_from(2).unwrap(), ascending: false }])
    ///     SeqScan(users)
    /// ```
    ///
    /// ```sql
    /// -- SELECT COUNT(*), age FROM users GROUP BY age;
    /// ```
    ///
    /// ```ignore
    /// Project([Column(1), Column(0)])
    ///   Aggregate(group_by = [ColumnId::try_from(2).unwrap()], agg_exprs = [COUNT(*)])
    ///     SeqScan(users)
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for currently unwired SQL shapes:
    /// `JOIN`/cross execution, `HAVING`, aggregate queries with `ORDER BY`,
    /// `SELECT *` with aggregation, aggregate expressions on the non-aggregate
    /// projection path, and projected bare columns missing from `GROUP BY`.
    ///
    /// Returns [`EngineError::TypeError`] when the underlying `Project`,
    /// `Sort`, or `Aggregate` operator rejects the bound tuple shape.
    fn build_plan<'a>(
        bound: &BoundSelect,
        heaps: &'a [(FileId, Arc<HeapFile>)],
        txn: TransactionId,
    ) -> Result<PlanNode<'a>, EngineError> {
        let mut node = Self::build_from(&bound.from, heaps, txn)?;

        if let Some(pred) = &bound.filter {
            node = PlanNode::filter(node, pred.clone());
        }

        let aggregating = !bound.group_by.is_empty() || Self::has_aggregate(&bound.select_list);

        // ORDER BY and HAVING are still bound against the FROM scope by the
        // binder, so their column ids would be wrong above an Aggregate.
        // Reject them with a clear message until the binder rebinds those
        // clauses against the post-aggregate schema.
        if aggregating && !bound.order_by.is_empty() {
            return Err(EngineError::Unsupported(
                "ORDER BY combined with GROUP BY / aggregates is not yet supported".into(),
            ));
        }
        if bound.having.is_some() {
            return Err(EngineError::Unsupported("HAVING not yet supported".into()));
        }

        // ORDER BY must run BEFORE Project: the binder resolves order keys
        // against the FROM scope, and Project may narrow the schema and drop
        // the columns those keys refer to. Sorting first keeps every column
        // in scope while the comparison happens; the subsequent Project
        // narrows the already-ordered rows.
        if !aggregating && !bound.order_by.is_empty() {
            let keys = bound
                .order_by
                .iter()
                .map(|(col, direction)| SortKey {
                    col_id: *col,
                    ascending: matches!(direction, OrderDirection::Asc),
                })
                .collect();
            node = PlanNode::sort(node, keys);
        }

        if aggregating {
            node = Self::build_aggregate(node, &bound.select_list, &bound.group_by)?;
        } else if let BoundSelectList::Items(items) = &bound.select_list {
            node = Self::build_project(node, items)?;
        }

        if bound.distinct {
            node = PlanNode::distinct(node);
        }

        if let Some(limit) = &bound.limit {
            node = PlanNode::limit(node, limit.limit.unwrap_or(u64::MAX), limit.offset);
        }

        Ok(node)
    }

    /// Builds the SQL `FROM` input for a `SELECT` plan.
    ///
    /// A base table lowers to a sequential scan over that table's heap for the
    /// current transaction. Join and cross-product bound forms are recognized
    /// here, but execution is not wired through this planner yet.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- FROM users
    /// --   PlanNode::seq_scan(users_heap, txn)
    ///
    /// -- FROM users JOIN orders ON users.id = orders.user_id
    /// --   currently returns EngineError::Unsupported
    ///
    /// -- FROM users CROSS JOIN orders
    /// --   currently returns EngineError::Unsupported
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] for `JOIN` or cross-product inputs
    /// because this planner currently only executes single-table `FROM`.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::collect_heap_files`] did not preload a heap for a
    /// `BoundFrom::Table`; that would mean the planner's preload invariant was
    /// broken, not that the SQL query is invalid.
    fn build_from<'a>(
        from: &BoundFrom,
        heaps: &'a [(FileId, Arc<HeapFile>)],
        txn: TransactionId,
    ) -> Result<PlanNode<'a>, EngineError> {
        match from {
            BoundFrom::Table { file_id, .. } => {
                let heap = heaps
                    .iter()
                    .find_map(|(id, h)| (id == file_id).then_some(h))
                    .expect("collect_heaps preloaded every BoundFrom::Table");
                Ok(PlanNode::seq_scan(heap.as_ref(), txn))
            }
            BoundFrom::Join { .. } | BoundFrom::Cross { .. } => Err(EngineError::Unsupported(
                "JOIN execution not yet wired through the planner".into(),
            )),
        }
    }

    /// Detects whether a SQL `SELECT` list requires aggregate planning.
    ///
    /// `SELECT COUNT(*) FROM users` and `SELECT age, SUM(age) FROM users GROUP
    /// BY age` return `true`; `SELECT * FROM users` and `SELECT age FROM users`
    /// return `false`. `SELECT *` has no explicit items, so it is never
    /// aggregate-bearing by itself.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT * FROM users;
    /// --   has_aggregate(&BoundSelectList::Star) == false
    ///
    /// -- SELECT name FROM users;
    /// --   has_aggregate(&BoundSelectList::Items([Column(1)])) == false
    ///
    /// -- SELECT COUNT(*) FROM users;
    /// --   has_aggregate(&BoundSelectList::Items([Aggregate(CountStar)])) == true
    /// ```
    fn has_aggregate(list: &BoundSelectList) -> bool {
        let BoundSelectList::Items(items) = list else {
            return false;
        };
        items
            .iter()
            .any(|p| matches!(p.item, BoundSelectItem::Aggregate(_)))
    }

    /// Builds the non-aggregate SQL `SELECT` list as a `Project` operator.
    ///
    /// Column projections copy values out of the child tuple. Literal
    /// projections broadcast the same value on every output row. The output
    /// tuple layout exactly follows the SQL-written `SELECT` item order.
    ///
    /// # SQL examples
    ///
    /// Given `users(id, name, age)` with `id → 0`, `name → 1`, `age → 2`:
    ///
    /// ```sql
    /// -- SELECT name, id FROM users;
    /// --   build_project(scan, [Column(1), Column(0)])
    /// ```
    ///
    /// ```ignore
    /// PlanNode::project(scan, vec![
    ///     ProjectItem::column(ColumnId::try_from(1).unwrap(), None),
    ///     ProjectItem::column(ColumnId::try_from(0).unwrap(), None),
    /// ])?
    /// ```
    ///
    /// ```sql
    /// -- SELECT 'guest' AS role, name FROM users;
    /// --   build_project(scan, [Literal("guest"), Column(1)])
    /// ```
    ///
    /// ```ignore
    /// PlanNode::project(scan, vec![
    ///     ProjectItem::literal(Value::String("guest".into()), "role"),
    ///     ProjectItem::column(ColumnId::try_from(1).unwrap(), None),
    /// ])?
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if an aggregate expression reaches
    /// this non-aggregating path.
    ///
    /// Returns [`EngineError::TypeError`] if `Project` rejects a column index or
    /// output schema shape.
    fn build_project<'a>(
        child: PlanNode<'a>,
        projections: &[BoundProjection],
    ) -> Result<PlanNode<'a>, EngineError> {
        let project_items = projections
            .iter()
            .enumerate()
            .map(|(i, proj)| match &proj.item {
                BoundSelectItem::Column(c) => Ok(ProjectItem::column(*c, proj.alias.clone())),
                BoundSelectItem::Literal(v) => {
                    let name = if let Some(name) = &proj.alias {
                        name.clone()
                    } else {
                        format!("?column?{}", i + 1).try_into().map_err(|e| {
                            EngineError::type_error(format!(
                                "invalid synthesized literal column name: {e}"
                            ))
                        })?
                    };
                    Ok(ProjectItem::literal(v.clone(), name))
                }
                BoundSelectItem::Aggregate(_) => Err(EngineError::Unsupported(
                    "aggregate projections require the Aggregate operator".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        PlanNode::project(child, project_items).map_err(|e| EngineError::type_error(e.to_string()))
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
    /// `Aggregate` emits tuples laid out as `[group_by_cols..., agg_results...]`
    /// in declaration order, but the user's `SELECT` list can interleave
    /// columns, aggregates, and literal constants in any order. The `Project`
    /// above the `Aggregate` rewires positions so the final output matches the
    /// SQL-written order.
    ///
    /// # SQL examples
    ///
    /// Given `users(id, name, age)` with `age → 2`:
    ///
    /// ```sql
    /// -- SELECT COUNT(*) FROM users;
    /// --   build_aggregate(scan, [Aggregate(CountStar)], [])
    /// ```
    ///
    /// ```ignore
    /// Project([Column(0)])
    ///   Aggregate(group_by = [], agg_exprs = [COUNT(*)])
    ///     scan
    /// ```
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) AS n FROM users GROUP BY age;
    /// --   build_aggregate(scan, [Column(2), Aggregate(CountStar)], [ColumnId::try_from(2).unwrap()])
    /// ```
    ///
    /// ```ignore
    /// Project([Column(0), Column(1)])
    ///   Aggregate(group_by = [ColumnId::try_from(2).unwrap()], agg_exprs = [COUNT(*)])
    ///     scan
    /// ```
    ///
    /// ```sql
    /// -- SELECT COUNT(*), age FROM users GROUP BY age;
    /// --   build_aggregate(scan, [Aggregate(CountStar), Column(2)], [ColumnId::try_from(2).unwrap()])
    /// ```
    ///
    /// ```ignore
    /// Project([Column(1), Column(0)])
    ///   Aggregate(group_by = [ColumnId::try_from(2).unwrap()], agg_exprs = [COUNT(*)])
    ///     scan
    /// ```
    ///
    /// Bare column references are validated to appear in `GROUP BY` —
    /// `SELECT name, COUNT(*) FROM t GROUP BY age` is rejected because
    /// `name` is neither aggregated nor a grouping key.
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
        let projections = match select_list {
            BoundSelectList::Items(items) => items,
            BoundSelectList::Star => {
                return Err(EngineError::Unsupported(
                    "SELECT * with GROUP BY / aggregates is not supported".into(),
                ));
            }
        };

        let agg_node = Self::create_aggregate_plan(child, projections, group_by_cols)?;

        let project_items =
            Self::build_aggregate_rewiring_projection_items(projections, group_by_cols)?;

        PlanNode::project(agg_node, project_items)
            .map_err(|e| EngineError::type_error(e.to_string()))
    }

    /// Builds the `ProjectItem`s that restore SQL `SELECT` order above `Aggregate`.
    ///
    /// The child `Aggregate` always outputs group keys first, then aggregate
    /// results. SQL can request those values in any order, so this helper maps
    /// each bound projection to its post-aggregate column position or to a
    /// broadcast literal.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT age, COUNT(*) FROM users GROUP BY age;
    /// --   [ProjectItem::column(ColumnId::try_from(0).unwrap(), None), ProjectItem::column(ColumnId::try_from(1).unwrap(), None)]
    ///
    /// -- SELECT COUNT(*) AS n, age FROM users GROUP BY age;
    /// --   [ProjectItem::column(ColumnId::try_from(1).unwrap(), Some("n".into())), ProjectItem::column(ColumnId::try_from(0).unwrap(), None)]
    ///
    /// -- SELECT 'group' AS label, age, COUNT(*) FROM users GROUP BY age;
    /// --   [ProjectItem::literal(Value::String("group".into()), "label"), ProjectItem::column(ColumnId::try_from(0).unwrap(), None), ProjectItem::column(ColumnId::try_from(1).unwrap(), None)]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] if a post-aggregate output position
    /// cannot fit in [`ColumnId`].
    fn build_aggregate_rewiring_projection_items(
        projections: &[BoundProjection],
        group_by_cols: &[ColumnId],
    ) -> Result<Vec<ProjectItem>, EngineError> {
        let mut agg_index = 0usize;
        projections
            .iter()
            .enumerate()
            .map(|(i, projection)| {
                Ok(match &projection.item {
                    BoundSelectItem::Column(c) => {
                        let pos = group_by_cols
                            .iter()
                            .position(|g| g == c)
                            .expect("GROUP BY membership validated above");
                        ProjectItem::column(Self::col_id(pos)?, projection.alias.clone())
                    }
                    BoundSelectItem::Aggregate(_) => {
                        let pos = group_by_cols.len() + agg_index;
                        agg_index += 1;
                        ProjectItem::column(Self::col_id(pos)?, projection.alias.clone())
                    }
                    BoundSelectItem::Literal(v) => {
                        let name = match projection.alias.clone() {
                            Some(alias) => alias,
                            None => format!("?column?{}", i + 1).try_into().map_err(|e| {
                                EngineError::type_error(format!(
                                    "invalid synthesized literal column name: {e}"
                                ))
                            })?,
                        };
                        ProjectItem::literal(v.clone(), name)
                    }
                })
            })
            .collect::<Result<Vec<_>, EngineError>>()
    }

    /// Builds the physical `Aggregate` node for SQL grouping and aggregate calls.
    ///
    /// Aggregate expressions are pulled from the `SELECT` list in declaration
    /// order because that is the order of the aggregate result columns after
    /// the group keys. Bare columns are checked against `GROUP BY` before the
    /// operator is built.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT COUNT(*) FROM users;
    /// --   PlanNode::aggregate(scan, &[], vec![AggregateExpr { func: AggregateFunc::CountStar, .. }])?
    ///
    /// -- SELECT age, SUM(age) FROM users GROUP BY age;
    /// --   PlanNode::aggregate(scan, &[ColumnId::try_from(2).unwrap()], vec![AggregateExpr { func: AggregateFunc::Sum, col_id: ColumnId::try_from(2).unwrap(), .. }])?
    ///
    /// -- SELECT name, COUNT(*) FROM users GROUP BY age;
    /// --   EngineError::Unsupported("column 'name' ... must appear in GROUP BY ...")
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsupported`] when a projected bare column is not
    /// present in the SQL `GROUP BY` list.
    ///
    /// Returns [`EngineError::TypeError`] if the underlying `Aggregate` operator
    /// rejects a group key or aggregate input column.
    fn create_aggregate_plan<'a>(
        child: PlanNode<'a>,
        projections: &[BoundProjection],
        group_by_cols: &[ColumnId],
    ) -> Result<PlanNode<'a>, EngineError> {
        let agg_exprs = projections
            .iter()
            .filter_map(|p| match &p.item {
                BoundSelectItem::Aggregate(a) => Some(a.clone()),
                _ => None,
            })
            .collect();

        let invalid_col = projections.iter().find_map(|p| match &p.item {
            BoundSelectItem::Column(c) if !group_by_cols.contains(c) => Some(*c),
            _ => None,
        });

        if let Some(col_id) = invalid_col {
            let col_name = child.schema().col_name(col_id).unwrap_or("<unknown>");
            return Err(EngineError::Unsupported(format!(
                "column '{col_name}' (index {col_id}) must appear in GROUP BY or be used in an aggregate",
            )));
        }

        Ok(PlanNode::aggregate(child, group_by_cols, agg_exprs)?)
    }

    /// Converts a post-operator SQL column position into a [`ColumnId`].
    ///
    /// Used when planner-created `ProjectItem`s refer to columns in an
    /// intermediate tuple, such as the `Aggregate` output layout
    /// `[GROUP BY columns..., aggregate results...]`.
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
    //! End-to-end tests for the SELECT planner / executor.
    //!
    //! Every test drives the full pipeline — parse → bind → plan → execute —
    //! through `Engine::execute_statement`. That keeps the tests honest:
    //! a planner change that breaks a downstream operator is caught here,
    //! not deferred to integration tests.
    //!
    //! Each test seeds a fresh `users` table (sometimes with `orders`) in
    //! a temp dir, runs SQL, and asserts on either the returned schema or
    //! the row contents. Aggregation tests sort the rows before asserting
    //! because the `Aggregate` operator is hash-based and emits groups in
    //! arbitrary order.
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        parser::Parser,
        primitives::NonEmptyString,
        transaction::TransactionManager,
        tuple::{Field, Tuple, TupleSchema},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).unwrap());
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).unwrap();
        let txn_mgr = TransactionManager::new(wal, bp);
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
    /// `cara` and `alice` share `age = 30` so we can exercise tie-breakers.
    fn seed_users(dir: &Path) -> (Catalog, TransactionManager) {
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
                    None,
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

    /// Same shape as [`seed_users`] but inserts no rows — used to exercise
    /// empty-input behavior.
    fn seed_empty_users(dir: &Path) -> (Catalog, TransactionManager) {
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
                None,
            )
            .unwrap();
        txn.commit().unwrap();
        (catalog, txn_mgr)
    }

    /// Sorts rows lexicographically by their `Value`s — used to make
    /// hash-aggregate output deterministic before asserting on it.
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

    // ──────────────────────── projection / schema ────────────────────────

    /// `SELECT *` preserves the table's full schema (names + types) and
    /// returns every row.
    #[test]
    fn select_star_returns_full_schema_and_all_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT * FROM users");
        assert_eq!(field_names(&schema), vec!["id", "name", "age"]);
        assert_eq!(rows.len(), 3);
    }

    /// Explicit projection narrows the schema to the requested columns
    /// in the user-written order.
    #[test]
    fn select_columns_in_user_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT name, id FROM users");
        assert_eq!(field_names(&schema), vec!["name", "id"]);
        assert_eq!(rows[0].len(), 2);
    }

    /// `SELECT col AS alias` renames the field in the output schema —
    /// row values are unchanged.
    #[test]
    fn column_alias_renames_field_in_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id AS user_id, name FROM users");
        assert_eq!(field_names(&schema), vec!["user_id", "name"]);
        assert_eq!(rows.len(), 3);
    }

    /// Literal projections appear once per input row (broadcast) and use
    /// the SQL alias when one is given.
    #[test]
    fn literal_with_alias_broadcasts_per_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT 'guest' AS role, name FROM users");
        assert_eq!(field_names(&schema), vec!["role", "name"]);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.get(0).unwrap(), &Value::String("guest".into()));
        }
    }

    /// A literal without an alias gets a synthesized `?column?N` name —
    /// matches Postgres convention.
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

    // ──────────────────────────── WHERE ─────────────────────────────────

    /// `WHERE` keeps only matching rows; schema is unchanged from the
    /// child scan.
    #[test]
    fn where_filters_rows_keeping_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id, name FROM users WHERE age > 25");
        assert_eq!(field_names(&schema), vec!["id", "name"]);
        assert_eq!(rows.len(), 2); // alice (30) + cara (30)
    }

    /// Predicate that matches no rows yields an empty result with the
    /// projected schema still present.
    #[test]
    fn where_no_match_returns_empty_with_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id FROM users WHERE age > 999");
        assert_eq!(field_names(&schema), vec!["id"]);
        assert!(rows.is_empty());
    }

    // ─────────────────────────── ORDER BY ───────────────────────────────

    /// Single-key ascending sort. Stable: ties between alice (1, 30) and
    /// cara (3, 30) preserve insertion order.
    #[test]
    fn order_by_single_key_ascending_is_stable() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY age");
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Int64(v) => *v,
                other => panic!("expected Int64, got {other:?}"),
            })
            .collect();
        // bob(25), alice(30), cara(30) — stable order preserves insertion.
        assert_eq!(ids, vec![2, 1, 3]);
    }

    /// `DESC` flips the order; ties still preserved by stable sort.
    #[test]
    fn order_by_desc_flips_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY id DESC");
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Int64(v) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ids, vec![3, 2, 1]);
    }

    /// Multi-key sort: primary key drives, secondary breaks ties.
    /// `ORDER BY age, id DESC` → bob(25), cara(30), alice(30).
    #[test]
    fn order_by_two_keys_uses_secondary_for_ties() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY age, id DESC");
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Int64(v) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ids, vec![2, 3, 1]);
    }

    // ─────────────────────────── LIMIT / OFFSET ─────────────────────────

    /// `LIMIT n` caps the number of rows returned.
    #[test]
    fn limit_caps_row_count() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    /// `OFFSET m` skips the first m rows; combined with LIMIT it produces
    /// a sliding window. With ORDER BY the window is deterministic.
    #[test]
    fn limit_with_offset_returns_sliding_window() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(2));
    }

    // ─────────────────────────── DISTINCT ───────────────────────────────

    /// `DISTINCT` removes exact-duplicate rows after projection.
    /// `users.age` has values {30, 25, 30}, so DISTINCT yields {25, 30}.
    #[test]
    fn distinct_collapses_duplicates_after_projection() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT DISTINCT age FROM users");
        let mut ages: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Int64(v) => *v,
                _ => unreachable!(),
            })
            .collect();
        ages.sort_unstable();
        assert_eq!(ages, vec![25, 30]);
    }

    /// Selecting from an empty table returns zero rows but a populated schema.
    #[test]
    fn select_on_empty_table_returns_no_rows_with_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_empty_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT id, name FROM users");
        assert_eq!(field_names(&schema), vec!["id", "name"]);
        assert!(rows.is_empty());
    }

    /// `COUNT(*)` over an empty table.
    ///
    /// SQL semantics: an *ungrouped* aggregate over zero rows still returns
    /// exactly one row (`COUNT = 0`, `SUM = NULL`, etc.). The schema is
    /// always emitted regardless.
    ///
    /// **Known deviation:** today the [`Aggregate`] operator does not
    /// synthesize that empty-group row, so we get zero rows back. The
    /// schema is still correct. This test documents the current behavior
    /// so the future fix in `Aggregate::materialize` flips the assertion
    /// rather than introducing a new test.
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

    /// `COUNT(*)` over a populated table returns total row count in one row.
    #[test]
    fn count_star_returns_total_row_count() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT COUNT(*) AS n FROM users");
        assert_eq!(field_names(&schema), vec!["n"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(3));
    }

    /// `SUM(col)` reduces over the input and returns one row.
    #[test]
    fn sum_aggregate_reduces_to_single_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT SUM(age) FROM users");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(85)); // 30+25+30
    }

    /// `GROUP BY col, COUNT(*)` produces one row per distinct value with
    /// the group key first, then aggregates — matches Aggregate's output
    /// layout.
    #[test]
    fn group_by_age_counts_per_group() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT age, COUNT(*) FROM users GROUP BY age");
        assert_eq!(field_names(&schema), vec!["age", "COUNT(*)"]);

        // Aggregate is hash-based — sort before asserting.
        let got = sorted_rows(rows);
        assert_eq!(got, vec![
            vec![Value::Int64(25), Value::Int64(1)], // bob
            vec![Value::Int64(30), Value::Int64(2)], // alice + cara
        ]);
    }

    /// `SELECT COUNT(*), age GROUP BY age` — aggregate written *before*
    /// the group key in the SELECT list. The rewiring `Project` must
    /// flip the column order from Aggregate's `(age, count)` layout
    /// back to the user's `(count, age)` order.
    #[test]
    fn select_order_is_rewired_above_aggregate() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT COUNT(*), age FROM users GROUP BY age");
        assert_eq!(field_names(&schema), vec!["COUNT(*)", "age"]);

        // After sorting by the COUNT column, the (1, 25) row comes first
        // and (2, 30) second.
        let got = sorted_rows(rows);
        assert_eq!(got, vec![vec![Value::Int64(1), Value::Int64(25)], vec![
            Value::Int64(2),
            Value::Int64(30)
        ],]);
    }

    /// `SELECT col AS alias, COUNT(*) AS n GROUP BY col` — both kinds
    /// of alias propagate to the output schema.
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

    /// Mixing a literal with a group-by column and an aggregate: the
    /// literal broadcasts; the rewiring Project handles the index
    /// shuffle correctly.
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
            assert_eq!(row.get(0).unwrap(), &Value::String("group:".into()));
        }
    }

    // ─────────────────────────── error paths ────────────────────────────

    fn expect_unsupported(r: Result<StatementResult, EngineError>) {
        match r {
            Err(EngineError::Unsupported(_)) => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// Bare column reference that isn't in `GROUP BY` is rejected — SQL's
    /// "must appear in GROUP BY or aggregate" rule.
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

    /// `SELECT *` combined with `GROUP BY` is rejected — there is no
    /// sensible expansion across all FROM columns when grouping.
    #[test]
    fn star_with_group_by_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        expect_unsupported(run(&engine, "SELECT * FROM users GROUP BY age"));
    }

    /// `ORDER BY` together with `GROUP BY`/aggregates currently rejected:
    /// the binder still resolves order keys against the FROM scope, so
    /// indices would be wrong above the Aggregate.
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

    /// `HAVING` is rejected for the same reason as `ORDER BY`-after-aggregate.
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

    // JOIN is not exercised here: the parser doesn't yet accept qualified
    // column references (`u.id`, `o.user_id`), so JOIN SQL can't even reach
    // the planner. The planner's `BoundFrom::Join` → Unsupported branch is
    // covered by the binder's join tests at the bound-AST level, and will
    // be tested end-to-end once the parser supports qualified refs.

    // ─────────────────────── interaction tests ──────────────────────────

    /// `WHERE` + projection + `LIMIT` compose: filter narrows rows,
    /// project narrows columns, limit caps count.
    #[test]
    fn where_project_limit_compose() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT name FROM users WHERE age = 30 LIMIT 1");
        assert_eq!(field_names(&schema), vec!["name"]);
        assert_eq!(rows.len(), 1);
        // Either alice or cara — first matching row in heap order.
        let name = vals(&rows[0]).remove(0);
        assert!(matches!(&name, Value::String(s) if s == "alice" || s == "cara"));
    }

    /// `WHERE` runs *before* the aggregate input — `COUNT(*)` over a
    /// filtered set returns the matching row count.
    #[test]
    fn where_then_count_star_counts_filtered_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT COUNT(*) FROM users WHERE age = 30");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(2));
    }
}
