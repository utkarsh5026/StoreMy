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
        Executor, PlanNode, ResolvedExpr,
        aggregate::{AggregateExpr, AggregateFunc},
        join::JoinPredicate,
        resolve_expr,
        unary::{ProjectItem, SortKey},
    },
    heap::file::HeapFile,
    parser::statements::{
        AggFunc, BinOp, ColumnRef, Expr, JoinKind, LimitClause, OrderBy, OrderDirection,
        SelectColumns, SelectItem, SelectStatement, Statement, TableRef, TableWithJoins,
    },
    primitives::{ColumnId, NonEmptyString, Predicate},
    transaction::Transaction,
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
        schema: TupleSchema,
    },
    /// A SQL `JOIN ... ON ...` input with its join predicate (pre-resolved).
    Join {
        kind: JoinKind,
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        on: ResolvedExpr,
        schema: TupleSchema,
    },
    /// `FROM a, b` or `CROSS JOIN` — no predicate; emits the cartesian product.
    Cross {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        schema: TupleSchema,
    },
}

impl BoundFrom {
    fn table(table_info: TableInfo, table: TableRef) -> Self {
        Self::Table {
            table,
            file_id: table_info.file_id,
            schema: table_info.schema,
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
        let schema = left.schema().merge(right.schema());
        Self::Join {
            kind,
            left: Box::new(left),
            right: Box::new(right),
            on,
            schema,
        }
    }

    fn cross(left: BoundFrom, right: BoundFrom) -> Self {
        let schema = left.schema().merge(right.schema());
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
///         Value::Int64(18),
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
        stmt: SelectStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, EngineError> {
        if stmt.from.is_empty() {
            return Err(EngineError::Unsupported("no FROM clause".to_string()));
        }
        if stmt.from.len() != 1 {
            return Err(EngineError::Unsupported("multi-table FROM".to_string()));
        }

        let root = stmt.from.first().unwrap();
        tracing::debug!(table = %root.table.name, joins = root.joins.len(), "binding SELECT");

        let (from, scope) = Self::resolve_from(stmt.from.first().unwrap().clone(), catalog, txn)?;

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
            .map(|expr| {
                resolve_expr(&scope, expr).map_err(|e| EngineError::TypeError(e.to_string()))
            })
            .transpose()?;

        let having = stmt
            .having
            .map(|expr| {
                resolve_expr(&scope, expr).map_err(|e| EngineError::TypeError(e.to_string()))
            })
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
        txn: &Transaction<'_>,
    ) -> Result<(BoundFrom, Scope), EngineError> {
        let TableWithJoins { table, joins } = from;

        let info = catalog.get_table_info(txn, &table.name)?;
        let mut scope = Scope::empty();
        scope.push(BoundTable::new(
            info.name.to_string(),
            table.alias.as_ref().map(ToString::to_string),
            info.schema.clone(),
            0,
        ));
        let mut left = BoundFrom::table(info, table);

        for j in joins {
            let right_offset = left.schema().physical_num_fields();
            let right_info = catalog.get_table_info(txn, &j.table.name)?;
            let right_table = BoundTable {
                name: right_info.name.to_string(),
                alias: j.table.alias.as_ref().map(ToString::to_string),
                schema: right_info.schema.clone(),
                column_offset: right_offset,
            };
            scope.push(right_table);

            let right = BoundFrom::table(right_info, j.table);
            left = if j.kind == JoinKind::Cross {
                BoundFrom::cross(left, right)
            } else {
                let on = j.on.expect("non-cross join must have ON clause");
                let resolved_on =
                    resolve_expr(&scope, on).map_err(|e| EngineError::TypeError(e.to_string()))?;
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
        let resolved = match expr {
            Expr::Column(col) => ResolvedExpr::Column(Self::resolve_scope_col(scope, &col)?),
            Expr::Literal(v) => ResolvedExpr::Literal(v),
            Expr::CountStar => ResolvedExpr::CountStar,
            Expr::Agg { func, arg } => Self::bind_agg_to_resolved(scope, func, *arg)?,
            Expr::BinaryOp { .. }
            | Expr::In { .. }
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Like { .. }
            | Expr::UnaryOp { .. }
            | Expr::IsNull { .. } => {
                return Err(EngineError::Unsupported(
                    "binary/unary/is null expressions in SELECT projections are not yet supported"
                        .into(),
                ));
            }
        };
        Ok(SelectProjection {
            expr: resolved,
            alias,
        })
    }

    /// Resolves a non-`COUNT(*)` aggregate argument and returns the `ResolvedExpr::Agg` node.
    fn bind_agg_to_resolved(
        scope: &Scope,
        func: AggFunc,
        arg: Expr,
    ) -> Result<ResolvedExpr, EngineError> {
        let Expr::Column(col) = arg else {
            return Err(EngineError::Unsupported(
                "aggregates currently support only a single column argument".to_string(),
            ));
        };
        let col_id = Self::resolve_scope_col(scope, &col)?;
        Ok(ResolvedExpr::Agg {
            func,
            arg: Box::new(ResolvedExpr::Column(col_id)),
        })
    }

    /// Resolves SQL `ORDER BY` columns into sort keys over the bound `FROM` row.
    #[inline]
    /// Resolves one SQL column reference into a planner-facing [`ColumnId`].
    ///
    /// # Errors
    ///
    /// Propagates [`EngineError::UnknownColumn`] and [`EngineError::AmbiguousColumn`]
    /// from the scope resolver. Returns [`EngineError::Unsupported`] if the
    /// resolved `usize` index is too large for [`ColumnId`].
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

        let items: Vec<ProjectItem> = schema
            .physical_iter()
            .enumerate()
            .filter(|(_, f)| !f.is_dropped)
            .map(|(phys_i, _)| ProjectItem::Column {
                idx: phys_i,
                alias: None,
            })
            .collect();

        PlanNode::project(node, items).map_err(|e| EngineError::TypeError(e.to_string()))
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
    pub(super) fn exec_select(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        let Statement::Select(select_stmt) = statement else {
            unreachable!("exec_select called with non-Select statement");
        };
        self.with_txn(|txn| {
            let bound = BoundSelect::bind(select_stmt, self.catalog, txn)?;

            let root_table_name = bound.from.root_table_name();
            let mut heap_files = Vec::with_capacity(bound.from.table_count());
            Self::collect_heap_files(&bound.from, self.catalog, &mut heap_files)?;
            let mut plan = Self::build_plan(&bound, &heap_files, txn.transaction_id())?;
            let schema = plan.schema().to_owned();

            let mut rows = Vec::new();
            while let Some(t) = plan
                .next()
                .map_err(|e| EngineError::TypeError(e.to_string()))?
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
    fn build_plan<'a>(
        bound: &BoundSelect,
        heaps: &'a [(FileId, Arc<HeapFile>)],
        txn: TransactionId,
    ) -> Result<PlanNode<'a>, EngineError> {
        let aggregating = !bound.group_by.is_empty() || Self::has_aggregate(&bound.select_list);
        tracing::debug!(
            root_table = %bound.from.root_table_name(),
            aggregating,
            order_by = bound.order_by.len(),
            "building plan"
        );
        let mut node = Self::build_from(&bound.from, heaps, txn)?;

        if let Some(pred) = &bound.filter {
            node = PlanNode::filter(node, pred.clone());
        }

        // ORDER BY and HAVING are still bound against the FROM scope, so their
        // column ids would be wrong above an Aggregate. Reject until the binder
        // rebinds those clauses against the post-aggregate schema.
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
        // in scope while the comparison happens.
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
        } else {
            node = Self::project_out_dropped_columns(node)?;
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
    /// current transaction. Join and cross-product bound forms return
    /// [`EngineError::Unsupported`] until join execution is wired through.
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
            BoundFrom::Join {
                kind,
                left,
                right,
                on,
                ..
            } => {
                if !matches!(kind, JoinKind::Inner) {
                    return Err(EngineError::Unsupported(format!(
                        "{kind} is not yet supported; only INNER JOIN is implemented"
                    )));
                }
                let left_node = Self::build_from(left, heaps, txn)?;
                let right_node = Self::build_from(right, heaps, txn)?;
                let left_width = left_node.schema().physical_num_fields();
                Ok(Self::build_join(left_node, right_node, on, left_width))
            }
            BoundFrom::Cross { left, right, .. } => {
                let left_node = Self::build_from(left, heaps, txn)?;
                let right_node = Self::build_from(right, heaps, txn)?;
                Ok(PlanNode::nested_loop_join(
                    left_node,
                    right_node,
                    ResolvedExpr::Literal(Value::Bool(true)),
                ))
            }
        }
    }

    /// Picks `HashJoin` when the `ON` clause is a simple column-equality between
    /// the two sides; falls back to `NestedLoopJoin` for everything else.
    fn build_join<'a>(
        left: PlanNode<'a>,
        right: PlanNode<'a>,
        on: &ResolvedExpr,
        left_width: usize,
    ) -> PlanNode<'a> {
        if let Some(pred) = Self::try_extract_equi(on, left_width) {
            tracing::debug!(algorithm = "hash_join", "join selected");
            PlanNode::hash_join(left, right, pred)
        } else {
            tracing::debug!(algorithm = "nested_loop_join", "join selected");
            PlanNode::nested_loop_join(left, right, on.clone())
        }
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
    fn build_project<'a>(
        child: PlanNode<'a>,
        projections: &[SelectProjection],
    ) -> Result<PlanNode<'a>, EngineError> {
        let project_items = projections
            .iter()
            .enumerate()
            .map(|(i, proj)| match &proj.expr {
                ResolvedExpr::Column(c) => Ok(ProjectItem::column(*c, proj.alias.clone())),
                ResolvedExpr::Literal(v) => {
                    let name = if let Some(name) = &proj.alias {
                        name.clone()
                    } else {
                        format!("?column?{}", i + 1).try_into().map_err(|e| {
                            EngineError::TypeError(format!(
                                "invalid synthesized literal column name: {e}"
                            ))
                        })?
                    };
                    Ok(ProjectItem::literal(v.clone(), name))
                }
                ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar => {
                    Err(EngineError::Unsupported(
                        "aggregate projections require the Aggregate operator".into(),
                    ))
                }
                _ => Err(EngineError::Unsupported(
                    "complex expressions in SELECT projections are not yet supported".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        PlanNode::project(child, project_items).map_err(|e| EngineError::TypeError(e.to_string()))
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

        let agg_node = Self::create_aggregate_plan(child, projections, group_by_cols)?;

        let project_items =
            Self::build_aggregate_rewiring_projection_items(projections, group_by_cols)?;

        PlanNode::project(agg_node, project_items)
            .map_err(|e| EngineError::TypeError(e.to_string()))
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
                        ProjectItem::column(Self::col_id(pos)?, projection.alias.clone())
                    }
                    ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar => {
                        let pos = group_by_cols.len() + agg_index;
                        agg_index += 1;
                        ProjectItem::column(Self::col_id(pos)?, projection.alias.clone())
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
                        ProjectItem::literal(v.clone(), name)
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

    /// Builds the physical `Aggregate` node for SQL grouping and aggregate calls.
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
        projections: &[SelectProjection],
        group_by_cols: &[ColumnId],
    ) -> Result<PlanNode<'a>, EngineError> {
        let invalid_col = projections.iter().find_map(|p| {
            if let ResolvedExpr::Column(c) = p.expr
                && !group_by_cols.contains(&c)
            {
                return Some(c);
            }
            None
        });

        if let Some(col_id) = invalid_col {
            let col_name = child.schema().col_name(col_id).unwrap_or("<unknown>");
            return Err(EngineError::Unsupported(format!(
                "column '{col_name}' (index {col_id}) must appear in GROUP BY or be used in an aggregate",
            )));
        }

        let schema = child.schema().clone();
        let agg_exprs = projections
            .iter()
            .filter_map(|p| Self::projection_to_agg_expr(p, &schema))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PlanNode::aggregate(child, group_by_cols, agg_exprs)?)
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
                Some(Ok(AggregateExpr {
                    func: AggregateFunc::CountStar,
                    col_id: ColumnId::default(),
                    output_name,
                }))
            }
            ResolvedExpr::Agg { func, arg } => {
                let col_id = match arg.as_ref() {
                    ResolvedExpr::Column(id) => *id,
                    _ => {
                        return Some(Err(EngineError::Unsupported(
                            "aggregates currently support only a single column argument".into(),
                        )));
                    }
                };
                let agg_func = match func {
                    AggFunc::Count => AggregateFunc::CountCol,
                    AggFunc::Sum => AggregateFunc::Sum,
                    AggFunc::Avg => AggregateFunc::Avg,
                    AggFunc::Min => AggregateFunc::Min,
                    AggFunc::Max => AggregateFunc::Max,
                };
                let default_name: NonEmptyString = {
                    let col_name = schema.col_name(col_id).unwrap_or("?");
                    format!("{func}({col_name})")
                        .try_into()
                        .unwrap_or_else(|_| "agg".try_into().unwrap())
                };
                Some(Ok(AggregateExpr {
                    func: agg_func,
                    col_id,
                    output_name: proj.alias.clone().unwrap_or(default_name),
                }))
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
        wal::writer::Wal,
    };

    // ─────────────────────── shared infrastructure ───────────────────────────

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
            assert_eq!(row.get(0).unwrap(), &Value::String("guest".into()));
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
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Int64(v) => *v,
                other => panic!("expected Int64, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec![2, 1, 3]);
    }

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
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(2));
    }

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
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(3));
    }

    #[test]
    fn sum_aggregate_reduces_to_single_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT SUM(age) FROM users");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(85));
    }

    #[test]
    fn group_by_age_counts_per_group() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (schema, rows) = run_select(&engine, "SELECT age, COUNT(*) FROM users GROUP BY age");
        assert_eq!(field_names(&schema), vec!["age", "COUNT(*)"]);

        let got = sorted_rows(rows);
        assert_eq!(got, vec![vec![Value::Int64(25), Value::Int64(1)], vec![
            Value::Int64(30),
            Value::Int64(2)
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
        assert_eq!(got, vec![vec![Value::Int64(1), Value::Int64(25)], vec![
            Value::Int64(2),
            Value::Int64(30)
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
            assert_eq!(row.get(0).unwrap(), &Value::String("group:".into()));
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
        assert!(matches!(&name, Value::String(s) if s == "alice" || s == "cara"));
    }

    #[test]
    fn where_then_count_star_counts_filtered_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = seed_users(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        let (_, rows) = run_select(&engine, "SELECT COUNT(*) FROM users WHERE age = 30");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &Value::Int64(2));
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
        txn_mgr: &TransactionManager,
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
                    item(Expr::Literal(Value::Int64(1)), None),
                    item(Expr::Literal(Value::String("hi".into())), Some("greet")),
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
            list[0].expr,
            ResolvedExpr::Literal(Value::Int64(1))
        ));
        assert!(matches!(&list[1].expr, ResolvedExpr::Literal(Value::String(s)) if s == "hi"));
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
    fn bind_projection_aggregate_non_column_arg_unsupported() {
        let err = expect_err(bind_with_users(|| {
            select_stmt(
                exprs(vec![item(
                    Expr::Agg {
                        func: AggFunc::Sum,
                        arg: Box::new(Expr::Literal(Value::Int64(1))),
                    },
                    None,
                )]),
                vec![just("users")],
            )
        }));
        assert!(matches!(err, EngineError::Unsupported(_)));
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
                    pred("u.id", Predicate::Equals, Value::Uint64(0)),
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
                pred("user_id", Predicate::Equals, Value::Uint64(0)),
            ),
        ));
        stmt.where_clause = Some(pred("id", Predicate::Equals, Value::Uint64(0)));
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
                        pred("user_id", Predicate::Equals, Value::Uint64(0)),
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
            s.where_clause = Some(pred("x.id", Predicate::Equals, Value::Uint64(0)));
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
            s.having = Some(pred("age", Predicate::GreaterThan, Value::Int64(0)));
            s
        })
        .unwrap();
        assert!(bound.having.is_some());
    }
}
