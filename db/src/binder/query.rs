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
        SelectItem, SelectStatement, TableRef, TableWithJoins,
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
        let filter = scope.resolve_where(stmt.where_clause.as_ref())?;
        let having = scope.resolve_where(stmt.having.as_ref())?;

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
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Type,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        parser::statements::{Join, JoinKind, WhereCondition},
        primitives::Predicate,
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn make_catalog_and_txn_mgr(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    /// `users(id Uint64 NN, name String, age Int64 NN)` — three columns at
    /// indices 0, 1, 2. Used as the canonical "left side" in join tests.
    fn users_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Uint64).not_null(),
            Field::new("name", Type::String),
            Field::new("age", Type::Int64).not_null(),
        ])
    }

    /// `orders(id Uint64 NN, user_id Uint64 NN, total Int64 NN)` — once
    /// joined onto `users`, its columns sit at indices 3, 4, 5. Note the
    /// shared column name `id`: the binder must require qualification
    /// when both tables are in scope.
    fn orders_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Uint64).not_null(),
            Field::new("user_id", Type::Uint64).not_null(),
            Field::new("total", Type::Int64).not_null(),
        ])
    }

    fn create_table(
        catalog: &Catalog,
        txn_mgr: &TransactionManager,
        name: &str,
        schema: TupleSchema,
    ) {
        let txn = txn_mgr.begin().unwrap();
        catalog.create_table(&txn, name, schema, None).unwrap();
        txn.commit().unwrap();
    }

    fn table_ref(name: &str, alias: Option<&str>) -> TableRef {
        TableRef {
            name: name.into(),
            alias: alias.map(str::to_string),
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

    fn inner_join(name: &str, alias: Option<&str>, on: WhereCondition) -> Join {
        Join {
            kind: JoinKind::Inner,
            table: table_ref(name, alias),
            on,
        }
    }

    fn select(columns: SelectColumns, from: Vec<TableWithJoins>) -> SelectStatement {
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
        select(SelectColumns::All, vec![t])
    }

    fn items(items: Vec<SelectItem>) -> SelectColumns {
        SelectColumns::Exprs(items)
    }

    fn item(expr: Expr, alias: Option<&str>) -> SelectItem {
        SelectItem {
            expr,
            alias: alias.map(str::to_string),
        }
    }

    fn col_expr(name: &str) -> Expr {
        Expr::Column(ColumnRef::from(name))
    }

    fn pred(c: &str, op: Predicate, v: Value) -> WhereCondition {
        WhereCondition::predicate(ColumnRef::from(c), op, v)
    }

    fn expect_err<T, E>(r: Result<T, E>) -> E {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        }
    }

    fn bind_with_users<F>(build: F) -> Result<BoundSelect, BindError>
    where
        F: FnOnce() -> SelectStatement,
    {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_table(&catalog, &txn_mgr, "users", users_schema());

        let txn = txn_mgr.begin().unwrap();
        let res = BoundSelect::bind(build(), &catalog, &txn);
        txn.commit().unwrap();
        res
    }

    fn bind_with_users_and_orders<F>(build: F) -> Result<BoundSelect, BindError>
    where
        F: FnOnce() -> SelectStatement,
    {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_table(&catalog, &txn_mgr, "users", users_schema());
        create_table(&catalog, &txn_mgr, "orders", orders_schema());

        let txn = txn_mgr.begin().unwrap();
        let res = BoundSelect::bind(build(), &catalog, &txn);
        txn.commit().unwrap();
        res
    }

    // SELECT * FROM users → Star projection, single-table FROM, no filter.
    #[test]
    fn test_bind_select_star_single_table() {
        let bound = bind_with_users(|| star_from(just("users"))).unwrap();
        assert!(matches!(bound.select_list, BoundSelectList::Star));
        assert!(matches!(bound.from, BoundFrom::Table { .. }));
        assert!(bound.filter.is_none());
        assert!(bound.group_by.is_empty());
        assert!(bound.order_by.is_empty());
    }

    // Empty FROM list is rejected with Unsupported (binder doesn't yet
    // support `SELECT 1` style queries with no source).
    #[test]
    fn test_bind_select_no_from_errors() {
        let err = expect_err(bind_with_users(|| select(SelectColumns::All, vec![])));
        assert!(matches!(err, BindError::Unsupported(_)));
    }

    // Comma-separated FROM is parsed but not yet supported by the binder.
    #[test]
    fn test_bind_select_multi_table_from_errors() {
        let err = expect_err(bind_with_users_and_orders(|| {
            select(SelectColumns::All, vec![just("users"), just("orders")])
        }));
        assert!(matches!(err, BindError::Unsupported(_)));
    }

    // SELECT id, name → two Column items at the right indices.
    #[test]
    fn test_bind_projection_columns_by_index() {
        let bound = bind_with_users(|| {
            select(
                items(vec![
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
        assert!(matches!(list[0].item, BoundSelectItem::Column(c) if u32::from(c) == 0));
        assert!(matches!(list[1].item, BoundSelectItem::Column(c) if u32::from(c) == 1));
        assert!(list[0].alias.is_none());
    }

    // SELECT id AS user_id → Column + alias preserved on BoundProjection.
    #[test]
    fn test_bind_projection_alias_preserved() {
        let bound = bind_with_users(|| {
            select(items(vec![item(col_expr("id"), Some("user_id"))]), vec![
                just("users"),
            ])
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        assert_eq!(list[0].alias.as_deref(), Some("user_id"));
        assert!(matches!(list[0].item, BoundSelectItem::Column(_)));
    }

    // SELECT 1, 'hi' AS greet, NULL → three Literal items, parser-supplied
    // values carried through verbatim.
    #[test]
    fn test_bind_projection_literals() {
        let bound = bind_with_users(|| {
            select(
                items(vec![
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
            list[0].item,
            BoundSelectItem::Literal(Value::Int64(1))
        ));
        assert!(
            matches!(list[1].item, BoundSelectItem::Literal(Value::String(ref s)) if s == "hi")
        );
        assert_eq!(list[1].alias.as_deref(), Some("greet"));
        assert!(matches!(
            list[2].item,
            BoundSelectItem::Literal(Value::Null)
        ));
    }

    // SELECT COUNT(*) → Aggregate(CountStar) with synthesized output_name.
    #[test]
    fn test_bind_projection_count_star_default_name() {
        let bound = bind_with_users(|| {
            select(items(vec![item(Expr::CountStar, None)]), vec![just(
                "users",
            )])
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        let BoundSelectItem::Aggregate(agg) = &list[0].item else {
            panic!("expected aggregate");
        };
        assert_eq!(agg.func, AggregateFunc::CountStar);
        assert_eq!(agg.output_name, "COUNT(*)");
        assert!(list[0].alias.is_none());
    }

    // SELECT COUNT(*) AS n → output_name + alias both equal "n".
    #[test]
    fn test_bind_projection_count_star_with_alias() {
        let bound = bind_with_users(|| {
            select(items(vec![item(Expr::CountStar, Some("n"))]), vec![just(
                "users",
            )])
        })
        .unwrap();
        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        let BoundSelectItem::Aggregate(agg) = &list[0].item else {
            panic!();
        };
        assert_eq!(agg.output_name, "n");
        assert_eq!(list[0].alias.as_deref(), Some("n"));
    }

    // SELECT SUM(age) → AggregateFunc::Sum, col_id resolved to age's index,
    // synthesized output_name "SUM(age)".
    #[test]
    fn test_bind_projection_aggregate_resolves_col_id() {
        let bound = bind_with_users(|| {
            select(
                items(vec![item(Expr::agg(AggFunc::Sum, col_expr("age")), None)]),
                vec![just("users")],
            )
        })
        .unwrap();

        let BoundSelectList::Items(list) = &bound.select_list else {
            panic!();
        };
        let BoundSelectItem::Aggregate(agg) = &list[0].item else {
            panic!();
        };
        assert_eq!(agg.func, AggregateFunc::Sum);
        assert_eq!(u32::from(agg.col_id), 2); // age is the 3rd column
        assert_eq!(agg.output_name, "SUM(age)");
    }

    // Aggregate with a non-column argument (a literal here) is rejected
    // with Unsupported. This is the guardrail against speculatively
    // recursing into Expr.
    #[test]
    fn test_bind_projection_aggregate_non_column_arg_unsupported() {
        let err = expect_err(bind_with_users(|| {
            select(
                items(vec![item(
                    Expr::agg(AggFunc::Sum, Expr::Literal(Value::Int64(1))),
                    None,
                )]),
                vec![just("users")],
            )
        }));
        assert!(matches!(err, BindError::Unsupported(_)));
    }

    // SELECT id, COUNT(*), name → mixed list keeps user-written order
    // (Column / Aggregate / Column). The old projection+aggregates split
    // couldn't represent this without a separate interleaving array.
    #[test]
    fn test_bind_projection_mixed_list_preserves_order() {
        let bound = bind_with_users(|| {
            select(
                items(vec![
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
        assert!(matches!(list[0].item, BoundSelectItem::Column(_)));
        assert!(matches!(list[1].item, BoundSelectItem::Aggregate(_)));
        assert!(matches!(list[2].item, BoundSelectItem::Column(_)));
    }

    // Unknown column in projection → UnknownColumn error.
    #[test]
    fn test_bind_projection_unknown_column_errors() {
        let err = expect_err(bind_with_users(|| {
            select(items(vec![item(col_expr("nope"), None)]), vec![just(
                "users",
            )])
        }));
        assert!(matches!(err, BindError::UnknownColumn { ref column, .. } if column == "nope"));
    }

    // JOIN's ON clause must resolve names from BOTH sides. This is the
    // case the old code broke (it bound ON against the right side only).
    #[test]
    fn test_bind_join_on_clause_sees_both_sides() {
        let bound = bind_with_users_and_orders(|| {
            star_from(with_join(
                aliased("users", "u"),
                inner_join(
                    "orders",
                    Some("o"),
                    pred("u.id", Predicate::Equals, Value::Uint64(0)), // refers to LEFT side
                ),
            ))
        })
        .unwrap();
        // Reaching here = ON resolved against the left side even though
        // only the right was just pushed.
        assert!(matches!(bound.from, BoundFrom::Join { .. }));
    }

    // After a join, an unqualified `id` is ambiguous (both users and
    // orders have an `id`). The binder must report AmbiguousColumn.
    #[test]
    fn test_bind_join_unqualified_shared_column_is_ambiguous() {
        let mut stmt = star_from(with_join(
            just("users"),
            inner_join(
                "orders",
                None,
                // user_id is unique to orders, so ON itself binds fine.
                pred("user_id", Predicate::Equals, Value::Uint64(0)),
            ),
        ));
        stmt.where_clause = Some(pred("id", Predicate::Equals, Value::Uint64(0)));
        let err = expect_err(bind_with_users_and_orders(|| stmt));
        assert!(matches!(err, BindError::AmbiguousColumn { ref column } if column == "id"));
    }

    // After a join, qualified `users.id` resolves to the left table
    // (offset 0); `orders.id` resolves to the right side at the merged
    // offset (3, since users has 3 fields).
    #[test]
    fn test_bind_join_qualified_columns_resolve_to_correct_offsets() {
        let bound = bind_with_users_and_orders(|| {
            select(
                items(vec![
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
        let BoundSelectItem::Column(left_id) = list[0].item else {
            panic!();
        };
        let BoundSelectItem::Column(right_id) = list[1].item else {
            panic!();
        };
        assert_eq!(u32::from(left_id), 0);
        assert_eq!(u32::from(right_id), 3);
    }

    // Unknown alias in a qualified ref → UnknownColumn against the bogus
    // qualifier, not the underlying table name.
    #[test]
    fn test_bind_unknown_qualifier_errors() {
        let err = expect_err(bind_with_users(|| {
            let mut s = star_from(just("users"));
            s.where_clause = Some(pred("x.id", Predicate::Equals, Value::Uint64(0)));
            s
        }));
        assert!(matches!(err, BindError::UnknownColumn { ref table, .. } if table == "x"));
    }

    // GROUP BY age → resolved column index. ORDER BY name DESC → resolved
    // index + direction preserved. LIMIT and DISTINCT pass through.
    #[test]
    fn test_bind_group_order_limit_distinct_pass_through() {
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
        assert_eq!(u32::from(bound.group_by[0]), 2); // age
        assert_eq!(bound.order_by.len(), 1);
        assert_eq!(u32::from(bound.order_by[0].0), 1); // name
        assert_eq!(bound.order_by[0].1, OrderDirection::Desc);
        let lim = bound.limit.unwrap();
        assert_eq!(lim.limit, Some(10));
        assert_eq!(lim.offset, 5);
    }

    // HAVING binds against scope (same mechanism as WHERE).
    #[test]
    fn test_bind_having_resolves() {
        let bound = bind_with_users(|| {
            let mut s = star_from(just("users"));
            s.having = Some(pred("age", Predicate::GreaterThan, Value::Int64(0)));
            s
        })
        .unwrap();
        assert!(bound.having.is_some());
    }
}
