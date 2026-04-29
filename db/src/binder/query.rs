use crate::{
    FileId, Value,
    binder::{
        BindError,
        scope::{BoundTable, ColumnResolver, Scope},
    },
    catalog::manager::{Catalog, TableInfo},
    execution::{
        aggregate::{AggregateExpr, AggregateFunc},
        expression::BooleanExpression,
    },
    parser::statements::{
        AggFunc, ColumnRef, Expr, JoinKind, LimitClause, OrderBy, OrderDirection, SelectColumns,
        SelectItem, SelectStatement, TableRef, TableWithJoins, WhereCondition,
    },
    primitives::ColumnId,
    transaction::Transaction,
    tuple::TupleSchema,
};

#[derive(Debug)]
pub enum BoundFrom {
    Table {
        table: TableRef,
        file_id: FileId,
        schema: TupleSchema,
        column_offset: usize,
    },
    Join {
        kind: JoinKind,
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        on: BooleanExpression,
        schema: TupleSchema,
    },
    Cross {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        schema: TupleSchema,
    },
}

impl BoundFrom {
    fn table(table_info: TableInfo, table: TableRef, offset: usize) -> Self {
        Self::Table {
            table,
            file_id: table_info.file_id,
            schema: table_info.schema,
            column_offset: offset,
        }
    }

    fn schema(&self) -> &TupleSchema {
        match self {
            Self::Cross { schema, .. } | Self::Table { schema, .. } | Self::Join { schema, .. } => {
                schema
            }
        }
    }

    fn join(kind: JoinKind, left: BoundFrom, right: BoundFrom, on: BooleanExpression) -> Self {
        let schema = left.schema().merge(right.schema());
        Self::Join {
            kind,
            left: Box::new(left),
            right: Box::new(right),
            on,
            schema,
        }
    }
}

/// One bound entry in a `SELECT` projection list.
///
/// Mirrors the executable subset of [`Expr`] — only the shapes the engine
/// can actually evaluate today. Future binary expressions (e.g. `age + 1`)
/// will grow this enum, not the parser side.
#[derive(Debug)]
pub enum BoundSelectItem {
    /// A direct column reference, resolved to a global index in the
    /// FROM-clause output schema.
    Column(ColumnId),
    /// A literal constant (`SELECT 1`, `SELECT 'hello'`, `SELECT NULL`).
    Literal(Value),
    /// An aggregate (`SUM(x)`, `COUNT(*)`, …). The contained
    /// [`AggregateExpr`] is what the executor's Aggregate operator
    /// consumes; the planner is responsible for splitting these out below
    /// a Project node when one is needed.
    Aggregate(AggregateExpr),
}

/// One projection item plus its optional output alias.
///
/// `alias` is the user-supplied `AS name` (or the implicit `expr name`
/// form). When `None`, the executor / planner falls back to a default
/// name derived from the item.
#[derive(Debug)]
pub struct BoundProjection {
    pub item: BoundSelectItem,
    pub alias: Option<String>,
}

/// The bound SELECT projection list.
///
/// `Star` corresponds to `SELECT *` and means "produce the FROM-clause
/// schema unchanged" — no Project node is built. `Items` is an explicit
/// list in the user's order.
#[derive(Debug)]
pub enum BoundSelectList {
    Star,
    Items(Vec<BoundProjection>),
}

/// A resolved single-table SELECT.
///
/// Every name has been resolved to an index; every literal has been coerced
/// to the column's type. The executor is free to assume this is internally
/// consistent — any error it encounters is a bug, not bad input.
pub struct BoundSelect {
    pub from: BoundFrom,

    /// The SELECT projection — `Star` for `SELECT *`, `Items` otherwise.
    /// Aggregates live inside this list (as [`BoundSelectItem::Aggregate`])
    /// rather than in a side field, so order and aliases are preserved.
    pub select_list: BoundSelectList,

    pub filter: Option<BooleanExpression>,

    /// Column indices (into the FROM schema) to group by.
    pub group_by: Vec<ColumnId>,
    /// Predicate evaluated against the aggregate's output schema.
    pub having: Option<BooleanExpression>,

    pub distinct: bool,
    /// `(col_index_in_final_schema, direction)`, in priority order.
    pub order_by: Vec<(ColumnId, OrderDirection)>,
    pub limit: Option<LimitClause>,
}

impl BoundSelect {
    pub(in crate::binder) fn bind(
        stmt: SelectStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        if stmt.from.is_empty() {
            return Err(BindError::Unsupported("no FROM clause".to_string()));
        }

        if stmt.from.len() != 1 {
            return Err(BindError::Unsupported("multi-table FROM".to_string()));
        }

        let (from, scope) = Self::resolve_from(stmt.from.first().unwrap().clone(), catalog, txn)?;
        let select_list = Self::resolve_select_list(&scope, stmt.columns)?;
        let order_by = Self::resolve_order_by(&scope, stmt.order_by)?;
        let group_by = Self::resolve_group_by(&scope, stmt.group_by)?;
        let filter = Self::resolve_filter(&scope, stmt.where_clause.as_ref())?;
        let having = Self::resolve_filter(&scope, stmt.having.as_ref())?;

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

    /// Walks a `FROM` entry, building the executable [`BoundFrom`] tree
    /// and the [`Scope`] used to resolve names within it in lockstep.
    ///
    /// Each table's column offset reflects its position in the merged
    /// output row, so a `ColumnRef` resolved through the returned scope
    /// gives the correct global index. The `ON` predicate of every join
    /// is bound against the scope as it stands *after* the right-hand
    /// table has been pushed, so both sides are visible.
    fn resolve_from(
        from: TableWithJoins,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<(BoundFrom, Scope), BindError> {
        let TableWithJoins { table, joins } = from;

        let info = catalog.get_table_info(txn, &table.name)?;
        let mut scope = Scope::empty();
        scope.push(BoundTable::new(
            info.name.clone(),
            table.alias.clone(),
            info.schema.clone(),
            0,
        ));
        let mut left = BoundFrom::table(info, table, 0);

        for j in joins {
            let right_offset = left.schema().num_fields();
            let right_info = catalog.get_table_info(txn, &j.table.name)?;
            let right_table = BoundTable {
                name: right_info.name.clone(),
                alias: j.table.alias.clone(),
                schema: right_info.schema.clone(),
                column_offset: right_offset,
            };
            scope.push(right_table);

            let right = BoundFrom::table(right_info, j.table, right_offset);
            left = BoundFrom::join(j.kind, left, right, scope.bind_where(&j.on)?);
        }

        Ok((left, scope))
    }

    /// Binds the parsed projection list into a [`BoundSelectList`].
    ///
    /// `SELECT *` becomes `Star` (no rewrite); explicit items are bound
    /// one by one via [`Self::bind_select_item`], preserving order and
    /// aliases.
    fn resolve_select_list(
        scope: &Scope,
        columns: SelectColumns,
    ) -> Result<BoundSelectList, BindError> {
        match columns {
            SelectColumns::All => Ok(BoundSelectList::Star),
            SelectColumns::Exprs(items) => items
                .into_iter()
                .map(|it| Self::bind_select_item(scope, it))
                .collect::<Result<Vec<_>, _>>()
                .map(BoundSelectList::Items),
        }
    }

    /// Binds one [`SelectItem`] into a [`BoundProjection`].
    ///
    /// Only the shapes the executor can evaluate today are accepted:
    /// plain columns, literals, single-column aggregates, and `COUNT(*)`.
    /// Nested expressions like `SUM(a + 1)` parse fine but are rejected
    /// here with [`BindError::Unsupported`] until binary expressions land.
    fn bind_select_item(scope: &Scope, item: SelectItem) -> Result<BoundProjection, BindError> {
        let SelectItem { expr, alias } = item;
        let bound = match expr {
            Expr::Column(col) => BoundSelectItem::Column(Self::resolve_scope_col(scope, &col)?),
            Expr::Literal(v) => BoundSelectItem::Literal(v),
            Expr::CountStar => BoundSelectItem::Aggregate(AggregateExpr {
                func: AggregateFunc::CountStar,
                col_id: ColumnId::default(),
                output_name: alias.clone().unwrap_or_else(|| "COUNT(*)".to_string()),
            }),
            Expr::Agg(func, arg) => Self::bind_agg(scope, &func, *arg, alias.as_deref())?,
        };
        Ok(BoundProjection { item: bound, alias })
    }

    /// Binds a non-`COUNT(*)` aggregate `f(arg)`.
    ///
    /// The argument must currently be a single column reference; richer
    /// expressions are reported as `Unsupported`. The synthesized
    /// `output_name` follows the user alias when present, otherwise
    /// `FUNC(col)` to match how most databases label such columns.
    fn bind_agg(
        scope: &Scope,
        func: &AggFunc,
        arg: Expr,
        alias: Option<&str>,
    ) -> Result<BoundSelectItem, BindError> {
        let Expr::Column(col) = arg else {
            return Err(BindError::Unsupported(
                "aggregates currently support only a single column argument".to_string(),
            ));
        };

        let default_name = format!("{func}({})", col.name);
        let col_id = Self::resolve_scope_col(scope, &col)?;
        let agg_func = match func {
            AggFunc::Count => AggregateFunc::CountCol,
            AggFunc::Sum => AggregateFunc::Sum,
            AggFunc::Avg => AggregateFunc::Avg,
            AggFunc::Min => AggregateFunc::Min,
            AggFunc::Max => AggregateFunc::Max,
        };
        Ok(BoundSelectItem::Aggregate(AggregateExpr {
            func: agg_func,
            col_id,
            output_name: alias.map_or(default_name, str::to_string),
        }))
    }

    fn resolve_order_by(
        scope: &Scope,
        order_by: Vec<OrderBy>,
    ) -> Result<Vec<(ColumnId, OrderDirection)>, BindError> {
        order_by
            .into_iter()
            .map(|order| {
                let col_id = Self::resolve_scope_col(scope, &order.0)?;
                Ok((col_id, order.1))
            })
            .collect::<Result<Vec<_>, BindError>>()
    }

    fn resolve_group_by(
        scope: &Scope,
        group_by: Vec<ColumnRef>,
    ) -> Result<Vec<ColumnId>, BindError> {
        group_by
            .into_iter()
            .map(|col| Self::resolve_scope_col(scope, &col))
            .collect()
    }

    #[inline]
    fn resolve_scope_col(scope: &Scope, col: &ColumnRef) -> Result<ColumnId, BindError> {
        let (idx, ..) = scope.resolve(col)?;
        ColumnId::try_from(idx)
            .map_err(|_| BindError::Unsupported("column index out of bounds".to_string()))
    }

    #[inline]
    fn resolve_filter(
        scope: &Scope,
        filter: Option<&WhereCondition>,
    ) -> Result<Option<BooleanExpression>, BindError> {
        filter.map(|w| scope.bind_where(w)).transpose()
    }
}
