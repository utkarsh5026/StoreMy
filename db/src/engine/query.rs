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
        aggregate::{Aggregate, AggregateExpr},
        unary::{ProjectItem, SortKey},
    },
    heap::file::HeapFile,
    parser::statements::{OrderDirection, Statement},
    primitives::ColumnId,
};

impl Engine<'_> {
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

    /// Walks the bound `FROM` tree and appends every base table's heap as `(file_id, heap)`.
    ///
    /// Join and cross-product nodes are traversed recursively; only [`BoundFrom::Table`]
    /// nodes contribute entries. Used so [`Self::build_plan`] can look up heaps by
    /// [`FileId`] without touching the catalog again. Missing heaps become
    /// [`EngineError::TableNotFound`].
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

        if aggregating {
            node = Self::build_aggregate(node, bound)?;
        } else if let BoundSelectList::Items(items) = &bound.select_list {
            node = Self::build_project(node, items)?;
        }

        if bound.distinct {
            node = PlanNode::distinct(node);
        }

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

        if let Some(limit) = &bound.limit {
            node = PlanNode::limit(node, limit.limit.unwrap_or(u64::MAX), limit.offset);
        }

        Ok(node)
    }

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

    fn has_aggregate(list: &BoundSelectList) -> bool {
        let BoundSelectList::Items(items) = list else {
            return false;
        };
        items
            .iter()
            .any(|p| matches!(p.item, BoundSelectItem::Aggregate(_)))
    }

    /// Builds a `Project` node from the bound `SELECT` items in the
    /// non-aggregating case. Aggregates are rejected here because they
    /// only make sense above an [`Aggregate`] operator — see
    /// [`Self::build_aggregate`].
    fn build_project<'a>(
        child: PlanNode<'a>,
        items: &[BoundProjection],
    ) -> Result<PlanNode<'a>, EngineError> {
        let project_items = items
            .iter()
            .enumerate()
            .map(|(i, projection)| match &projection.item {
                BoundSelectItem::Column(c) => Ok(ProjectItem::column(*c)),
                BoundSelectItem::Literal(v) => {
                    let name = projection
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("?column?{}", i + 1));
                    Ok(ProjectItem::literal(v.clone(), name))
                }
                BoundSelectItem::Aggregate(_) => Err(EngineError::Unsupported(
                    "aggregate projections require the Aggregate operator".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        PlanNode::project(child, project_items).map_err(|e| EngineError::type_error(e.to_string()))
    }

    /// Wires up `GROUP BY` + aggregate execution.
    ///
    /// The shape produced is:
    ///
    /// ```text
    /// Project { rewires SELECT-list order over Aggregate's output }
    ///   └── Aggregate { group_by, agg_exprs }
    ///         └── child (already filtered)
    /// ```
    ///
    /// `Aggregate` emits tuples laid out as `[group_by_cols..., agg_results...]`
    /// in declaration order, but the user's `SELECT` list can interleave
    /// columns, aggregates, and literal constants in any order. The
    /// `Project` above the `Aggregate` rewires positions so the final
    /// output matches the SQL-written order.
    ///
    /// Bare column references are validated to appear in `GROUP BY` —
    /// `SELECT name, COUNT(*) FROM t GROUP BY age` is rejected because
    /// `name` is neither aggregated nor a grouping key.
    fn build_aggregate<'a>(
        child: PlanNode<'a>,
        bound: &BoundSelect,
    ) -> Result<PlanNode<'a>, EngineError> {
        let items = match &bound.select_list {
            BoundSelectList::Items(items) => items,
            BoundSelectList::Star => {
                return Err(EngineError::Unsupported(
                    "SELECT * with GROUP BY / aggregates is not supported".into(),
                ));
            }
        };

        // Pull aggregates out of the SELECT list in declaration order —
        // their position here is also their offset inside the Aggregate
        // operator's output (after the group-by columns).
        let agg_exprs: Vec<AggregateExpr> = items
            .iter()
            .filter_map(|p| match &p.item {
                BoundSelectItem::Aggregate(a) => Some(a.clone()),
                _ => None,
            })
            .collect();

        // Reject bare columns that aren't in GROUP BY. This is the SQL
        // "column must appear in GROUP BY or be used in an aggregate" rule.
        for projection in items {
            if let BoundSelectItem::Column(c) = &projection.item
                && !bound.group_by.contains(c)
            {
                return Err(EngineError::Unsupported(format!(
                    "column index {} must appear in GROUP BY or be used in an aggregate",
                    u32::from(*c),
                )));
            }
        }

        let agg_node = PlanNode::Aggregate(
            Aggregate::new(child, &bound.group_by, agg_exprs)
                .map_err(|e| EngineError::type_error(e.to_string()))?,
        );

        // Build the rewiring projection. `agg_index` tracks which aggregate
        // we are placing as we walk the SELECT list left-to-right; aggregates
        // sit at offsets `[group_by.len() .. group_by.len() + agg_count)`
        // in the Aggregate operator's output schema.
        let mut agg_index = 0usize;
        let mut project_items = Vec::with_capacity(items.len());
        for (i, projection) in items.iter().enumerate() {
            match &projection.item {
                BoundSelectItem::Column(c) => {
                    let pos = bound
                        .group_by
                        .iter()
                        .position(|g| g == c)
                        .expect("GROUP BY membership validated above");
                    project_items.push(ProjectItem::column(Self::col_id(pos)?));
                }
                BoundSelectItem::Aggregate(_) => {
                    let pos = bound.group_by.len() + agg_index;
                    agg_index += 1;
                    project_items.push(ProjectItem::column(Self::col_id(pos)?));
                }
                BoundSelectItem::Literal(v) => {
                    let name = projection
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("?column?{}", i + 1));
                    project_items.push(ProjectItem::literal(v.clone(), name));
                }
            }
        }

        PlanNode::project(agg_node, project_items)
            .map_err(|e| EngineError::type_error(e.to_string()))
    }

    /// Converts a `usize` index to a `ColumnId`, ensuring it fits within the valid range.
    ///
    /// Returns an error if the index is out of bounds for a column reference.
    ///
    /// # Arguments
    ///
    /// * `idx` - The positional index of the column to convert.
    ///
    /// # Errors
    ///
    /// Returns `EngineError::Unsupported` if the index cannot be converted
    /// to a valid `ColumnId` (e.g., index is out of bounds for u32).
    #[inline]
    fn col_id(idx: usize) -> Result<ColumnId, EngineError> {
        ColumnId::try_from(idx)
            .map_err(|_| EngineError::Unsupported(format!("column index out of bounds: {idx}")))
    }
}
