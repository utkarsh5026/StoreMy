//! Bound `SELECT` queries: names resolved, clauses lowered, and columns indexed.
//!
//! This module covers the SQL surface from `SELECT ... FROM ...` through joins,
//! `WHERE`, projection items, aggregates, `GROUP BY`, `HAVING`, `DISTINCT`,
//! `ORDER BY`, and `LIMIT`. The parser still owns raw syntax; this binder turns
//! that syntax into a shape the planner can consume without looking names up
//! again.
//!
//! # Shape
//!
//! - [`BoundFrom`] — the bound `FROM` tree, including base table scans and joined inputs.
//! - [`BoundSelectItem`] — one executable item from the `SELECT` list: column, literal, or
//!   aggregate.
//! - [`BoundProjection`] — one `SELECT` item plus the output alias SQL gave it.
//! - [`BoundSelectList`] — either `SELECT *` or an ordered list of [`BoundProjection`] values.
//! - [`BoundSelect`] — the full query block, with every column reference resolved to a
//!   [`ColumnId`].
//!
//! # How it works
//!
//! Binding walks the `FROM` clause first and builds a `Scope` in
//! lockstep. Each table gets a column offset in the concatenated row. Later clauses (`JOIN ... ON`,
//! `WHERE`, `SELECT`, `GROUP BY`, `HAVING`, and `ORDER BY`) ask that scope to resolve SQL names
//! like `age`, `users.id`, or `o.total` into stable column indices.
//!
//! Examples use two schemas throughout:
//!
//! ```sql
//! -- users(id, name, age)       resolves to id → 0, name → 1, age → 2
//! -- orders(id, user_id, total) resolves to id → 3, user_id → 4, total → 5
//! -- when joined as: users JOIN orders ...
//! ```
//!
//! # NULL semantics
//!
//! The binder only coerces literal values against column types inside predicates. `NULL` may bind
//! only where the referenced column is nullable; the final truth-table behavior belongs to
//! [`BooleanExpression`], where a `NULL` comparison evaluates to `false` rather than full SQL
//! three-valued logic.

use crate::{
    FileId, Value,
    binder::{
        BindError,
        scope::{BoundTable, ColumnResolver, Scope},
    },
    catalog::{TableInfo, manager::Catalog},
    execution::{
        aggregate::{AggregateExpr, AggregateFunc},
        expression::BooleanExpression,
    },
    parser::statements::{
        AggFunc, ColumnRef, Expr, JoinKind, LimitClause, OrderBy, OrderDirection, SelectColumns,
        SelectItem, SelectStatement, TableRef, TableWithJoins,
    },
    primitives::{ColumnId, NonEmptyString},
    transaction::Transaction,
    tuple::TupleSchema,
};

/// Bound `FROM` input for one `SELECT` query block.
///
/// A `BoundFrom` is the planner-facing shape of SQL `FROM`: every table name has
/// been checked against the catalog, every table schema is attached, and join
/// predicates have already been lowered to [`BooleanExpression`] values over the
/// joined row layout.
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
/// The output tuple layout is left input columns first, then right input columns
/// for joins: `users.id`, `users.name`, `users.age`, `orders.id`,
/// `orders.user_id`, `orders.total`.
#[derive(Debug)]
pub enum BoundFrom {
    /// A base table reference from `FROM users` or `FROM users u`.
    Table {
        table: TableRef,
        file_id: FileId,
        schema: TupleSchema,
        column_offset: usize,
    },
    /// A SQL `JOIN ... ON ...` input with its join predicate already bound.
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

    /// Returns the name of the root table in this `FROM` clause input.
    ///
    /// - If this is a simple table reference (`FROM users`), returns the table name.
    /// - If this is a join or cross product (`FROM users JOIN posts`), recursively finds the
    ///   leftmost table's name. This matches the binding model, which keeps left-deep join trees.
    ///
    /// # Panics
    ///
    /// This method assumes the join tree is left-deep (every join has a "left"), in accordance
    /// with how the binder produces `BoundFrom` shapes.
    pub fn root_table_name(&self) -> &str {
        match self {
            Self::Table { table, .. } => &table.name,
            Self::Join { left, .. } | Self::Cross { left, .. } => left.root_table_name(),
        }
    }

    /// Returns the number of base tables present in this `FROM` clause tree.
    ///
    /// - For a simple table reference (`FROM users`), returns 1.
    /// - For joins or cross products, recursively sums the number of base tables in each input.
    ///
    /// This is useful for planner logic that may need to enumerate all tables referenced in a
    /// query, such as in join planning or determining table sources.
    pub fn table_count(&self) -> usize {
        match self {
            Self::Table { .. } => 1,
            Self::Join { left, right, .. } | Self::Cross { left, right, .. } => {
                left.table_count() + right.table_count()
            }
        }
    }
}

/// One bound expression from a SQL `SELECT` projection list.
///
/// Mirrors the executable subset of [`Expr`]: plain columns, literal constants,
/// `COUNT(*)`, and single-column aggregates. Future SQL expressions like
/// `age + 1` will grow this enum after the executor has a matching expression
/// shape.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT age FROM users;
/// ```
///
/// ```ignore
/// BoundSelectItem::Column(ColumnId::try_from(2).unwrap())
/// ```
///
/// ```sql
/// -- SELECT 'guest' FROM users;
/// ```
///
/// ```ignore
/// BoundSelectItem::Literal(Value::String("guest".into()))
/// ```
///
/// ```sql
/// -- SELECT COUNT(*) FROM users;
/// ```
///
/// ```ignore
/// BoundSelectItem::Aggregate(AggregateExpr {
///     func: AggregateFunc::CountStar,
///     col_id: ColumnId::default(),
///     output_name: "COUNT(*)".into(),
/// })
/// ```
///
/// ```sql
/// -- SELECT SUM(age) AS total_age FROM users;
/// ```
///
/// ```ignore
/// BoundSelectItem::Aggregate(AggregateExpr {
///     func: AggregateFunc::Sum,
///     col_id: ColumnId::try_from(2).unwrap(),
///     output_name: "total_age".into(),
/// })
/// ```
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

/// One SQL projection entry plus its optional output alias.
///
/// `alias` is the user-supplied `AS name` (or the implicit `expr name`
/// form). When `None`, the executor / planner falls back to a default
/// name derived from the item.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT age FROM users;
/// ```
///
/// ```ignore
/// BoundProjection {
///     item: BoundSelectItem::Column(ColumnId::try_from(2).unwrap()),
///     alias: None,
/// }
/// ```
///
/// ```sql
/// -- SELECT age AS years FROM users;
/// ```
///
/// ```ignore
/// BoundProjection {
///     item: BoundSelectItem::Column(ColumnId::try_from(2).unwrap()),
///     alias: Some("years".into()),
/// }
/// ```
///
/// ```sql
/// -- SELECT COUNT(*) AS n FROM users;
/// ```
///
/// ```ignore
/// BoundProjection {
///     item: BoundSelectItem::Aggregate(AggregateExpr {
///         func: AggregateFunc::CountStar,
///         col_id: ColumnId::default(),
///         output_name: "n".into(),
///     }),
///     alias: Some("n".into()),
/// }
/// ```
#[derive(Debug)]
pub struct BoundProjection {
    /// Bound SQL expression for this output column.
    pub item: BoundSelectItem,
    /// SQL output name from `AS alias`, if one was written.
    pub alias: Option<NonEmptyString>,
}

/// The bound SQL `SELECT` list.
///
/// `Star` corresponds to `SELECT *` and means "produce the FROM-clause
/// schema unchanged" — no Project node is built. `Items` is an explicit
/// list in the user's order.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT * FROM users;
/// ```
///
/// ```ignore
/// BoundSelectList::Star
/// ```
///
/// ```sql
/// -- SELECT id, name FROM users;
/// ```
///
/// ```ignore
/// BoundSelectList::Items(vec![
///     BoundProjection {
///         item: BoundSelectItem::Column(ColumnId::try_from(0).unwrap()),
///         alias: None,
///     },
///     BoundProjection {
///         item: BoundSelectItem::Column(ColumnId::try_from(1).unwrap()),
///         alias: None,
///     },
/// ])
/// ```
///
/// ```sql
/// -- SELECT id, COUNT(*), name FROM users;
/// ```
///
/// ```ignore
/// BoundSelectList::Items(vec![
///     BoundProjection { item: BoundSelectItem::Column(ColumnId::try_from(0).unwrap()), alias: None },
///     BoundProjection {
///         item: BoundSelectItem::Aggregate(AggregateExpr {
///             func: AggregateFunc::CountStar,
///             col_id: ColumnId::default(),
///             output_name: "COUNT(*)".into(),
///         }),
///         alias: None,
///     },
///     BoundProjection { item: BoundSelectItem::Column(ColumnId::try_from(1).unwrap()), alias: None },
/// ])
/// ```
///
/// The output tuple layout follows the SQL order for `Items`; for `Star`, it is
/// exactly the `FROM` schema layout.
#[derive(Debug)]
pub enum BoundSelectList {
    /// SQL `SELECT *`, preserving the entire `FROM` output row.
    Star,
    /// SQL `SELECT expr [, expr ...]`, preserving user-written order.
    Items(Vec<BoundProjection>),
}

/// A resolved SQL `SELECT` query block.
///
/// Every name has been resolved to an index; every literal has been coerced
/// to the column's type. The executor is free to assume this is internally
/// consistent — any error it encounters is a bug, not bad input.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT * FROM users WHERE age >= 18;
/// ```
///
/// ```ignore
/// BoundSelect {
///     from: BoundFrom::Table {
///         table: TableRef { name: "users".into(), alias: None },
///         file_id,
///         schema: users_schema,
///         column_offset: 0,
///     },
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
///
/// ```sql
/// -- SELECT age, COUNT(*) AS n FROM users GROUP BY age ORDER BY age DESC LIMIT 10;
/// ```
///
/// ```ignore
/// BoundSelect {
///     from: BoundFrom::Table {
///         table: TableRef { name: "users".into(), alias: None },
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
///     filter: None,
///     group_by: vec![ColumnId::try_from(2).unwrap()],
///     having: None,
///     distinct: false,
///     order_by: vec![(ColumnId::try_from(2).unwrap(), OrderDirection::Desc)],
///     limit: Some(LimitClause { limit: Some(10), offset: 0 }),
/// }
/// ```
///
/// # SQL → operator mapping
///
/// ```sql
/// -- SELECT u.name, o.total
/// -- FROM users u JOIN orders o ON u.id = o.user_id
/// -- WHERE o.total > 100
/// -- ORDER BY o.total DESC;
/// ```
///
/// ```ignore
/// BoundSelect::bind(select_ast, catalog, txn)?
/// // from        = BoundFrom::Join { on: <u.id = o.user_id>, schema: users ⋈ orders, .. }
/// // select_list = Items([Column(1), Column(5)])
/// // filter      = Some(BooleanExpression::col_op_lit(5, Predicate::GreaterThan, Value::Int64(100)))
/// // order_by    = [(ColumnId::try_from(5).unwrap(), OrderDirection::Desc)]
/// ```
pub struct BoundSelect {
    /// SQL `FROM` and `JOIN` tree.
    pub from: BoundFrom,

    /// The SELECT projection — `Star` for `SELECT *`, `Items` otherwise.
    /// Aggregates live inside this list (as [`BoundSelectItem::Aggregate`])
    /// rather than in a side field, so order and aliases are preserved.
    pub select_list: BoundSelectList,

    /// SQL `WHERE` predicate, if present.
    pub filter: Option<BooleanExpression>,

    /// Column indices (into the FROM schema) to group by.
    pub group_by: Vec<ColumnId>,
    /// SQL `HAVING` predicate, currently resolved against the `FROM` scope.
    pub having: Option<BooleanExpression>,

    /// SQL `SELECT DISTINCT`.
    pub distinct: bool,
    /// `(col_index_in_final_schema, direction)`, in priority order.
    pub order_by: Vec<(ColumnId, OrderDirection)>,
    /// SQL `LIMIT [n] [OFFSET m]`, if present.
    pub limit: Option<LimitClause>,
}

impl BoundSelect {
    /// Binds a parsed SQL `SELECT` into column-indexed query metadata.
    ///
    /// ```sql
    /// -- SELECT * FROM users;
    /// --   BoundSelect::bind(ast, catalog, txn)
    ///
    /// -- SELECT name FROM users WHERE age >= 18;
    /// --   BoundSelect::bind(ast, catalog, txn)
    ///
    /// -- SELECT SUM(age) AS total_age FROM users GROUP BY name;
    /// --   BoundSelect::bind(ast, catalog, txn)
    ///
    /// -- SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;
    /// --   BoundSelect::bind(ast, catalog, txn)
    /// ```
    ///
    /// The result preserves SQL clause order as data: `FROM` first for row
    /// layout, then projection, filters, grouping, ordering, and limit metadata.
    ///
    /// # Errors
    ///
    /// Returns [`BindError::Unsupported`] for `SELECT` without `FROM`, for
    /// comma-separated multi-table `FROM` entries, or for aggregate arguments
    /// that are not a single column.
    ///
    /// Returns [`BindError::UnknownTable`] or [`BindError::UnknownColumn`] when
    /// a SQL name cannot be resolved through the catalog/scope.
    ///
    /// Returns [`BindError::AmbiguousColumn`] when an unqualified column name
    /// appears in more than one joined table, such as `id` in
    /// `users JOIN orders`.
    ///
    /// Returns [`BindError::TypeMismatch`] or [`BindError::NullViolation`] when
    /// a predicate literal cannot be coerced to the referenced column.
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

    /// Resolves one SQL `FROM` entry into a [`BoundFrom`] tree and name scope.
    ///
    /// Each table's column offset reflects its position in the merged output
    /// row, so later clauses resolve names directly to executor column indices.
    /// A join's right table is pushed into the scope before its `ON` clause is
    /// bound, which makes both sides visible to the predicate binder.
    ///
    /// ```sql
    /// -- FROM users
    /// --   (BoundFrom::Table { column_offset: 0, .. }, Scope[users.id → 0, users.name → 1, users.age → 2])
    ///
    /// -- FROM users u JOIN orders o ON u.id = o.user_id
    /// --   BoundFrom::Join {
    /// --       left:  BoundFrom::Table { column_offset: 0, .. },
    /// --       right: BoundFrom::Table { column_offset: 3, .. },
    /// --       on:    BooleanExpression::col_op_col(0, Predicate::Equals, 4),
    /// --       ..
    /// --   }
    ///
    /// -- FROM users u JOIN orders o ON o.total > 100
    /// --   BoundFrom::Join {
    /// --       on: BooleanExpression::col_op_lit(5, Predicate::GreaterThan, Value::Int64(100)),
    /// --       ..
    /// --   }
    /// ```
    ///
    /// The joined output tuple layout is left columns followed by right columns:
    /// `users.id → 0`, `users.name → 1`, `users.age → 2`, `orders.id → 3`,
    /// `orders.user_id → 4`, `orders.total → 5`.
    ///
    /// # Errors
    ///
    /// Returns [`BindError::UnknownTable`] when a `FROM` or joined table is not
    /// present in the catalog. Returns predicate-binding errors from the `ON`
    /// clause, including [`BindError::UnknownColumn`],
    /// [`BindError::AmbiguousColumn`], [`BindError::TypeMismatch`], and
    /// [`BindError::NullViolation`].
    fn resolve_from(
        from: TableWithJoins,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<(BoundFrom, Scope), BindError> {
        let TableWithJoins { table, joins } = from;

        let info = catalog.get_table_info(txn, &table.name)?;
        let mut scope = Scope::empty();
        scope.push(BoundTable::new(
            info.name.to_string(),
            table.alias.as_ref().map(ToString::to_string),
            info.schema.clone(),
            0,
        ));
        let mut left = BoundFrom::table(info, table, 0);

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

            let right = BoundFrom::table(right_info, j.table, right_offset);
            left = BoundFrom::join(j.kind, left, right, scope.bind_where(&j.on)?);
        }

        Ok((left, scope))
    }

    /// Binds the SQL `SELECT` list into a [`BoundSelectList`].
    ///
    /// `SELECT *` stays as [`BoundSelectList::Star`], meaning the planner can
    /// pass through the full `FROM` schema. Explicit projection items are bound
    /// left to right, preserving SQL output order and aliases.
    ///
    /// ```sql
    /// -- SELECT *
    /// --   BoundSelectList::Star
    ///
    /// -- SELECT id, name
    /// --   BoundSelectList::Items(vec![
    /// --       BoundProjection { item: BoundSelectItem::Column(ColumnId::try_from(0).unwrap()), alias: None },
    /// --       BoundProjection { item: BoundSelectItem::Column(ColumnId::try_from(1).unwrap()), alias: None },
    /// --   ])
    ///
    /// -- SELECT name AS label, COUNT(*) AS n
    /// --   BoundSelectList::Items(vec![
    /// --       BoundProjection { item: BoundSelectItem::Column(ColumnId::try_from(1).unwrap()), alias: Some("label".into()) },
    /// --       BoundProjection { item: BoundSelectItem::Aggregate(count_star), alias: Some("n".into()) },
    /// --   ])
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates the errors from [`Self::bind_select_item`], including unknown
    /// or ambiguous column references and unsupported expression shapes.
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

    /// Binds one SQL projection expression into a [`BoundProjection`].
    ///
    /// Only the projection shapes the executor can evaluate today are accepted:
    /// plain columns, literals, single-column aggregates, and `COUNT(*)`.
    /// Nested expressions like `SUM(age + 1)` parse fine but are rejected here
    /// until binary expressions land.
    ///
    /// ```sql
    /// -- SELECT age
    /// --   BoundProjection {
    /// --       item: BoundSelectItem::Column(ColumnId::try_from(2).unwrap()),
    /// --       alias: None,
    /// --   }
    ///
    /// -- SELECT 'guest' AS role
    /// --   BoundProjection {
    /// --       item: BoundSelectItem::Literal(Value::String("guest".into())),
    /// --       alias: Some("role".into()),
    /// --   }
    ///
    /// -- SELECT COUNT(*) AS n
    /// --   BoundProjection {
    /// --       item: BoundSelectItem::Aggregate(AggregateExpr {
    /// --           func: AggregateFunc::CountStar,
    /// --           col_id: ColumnId::default(),
    /// --           output_name: "n".into(),
    /// --       }),
    /// --       alias: Some("n".into()),
    /// --   }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`BindError::UnknownColumn`] or [`BindError::AmbiguousColumn`]
    /// for column projections that cannot be resolved uniquely. Propagates
    /// [`BindError::Unsupported`] from [`Self::bind_agg`] for aggregate
    /// arguments that are not plain columns.
    fn bind_select_item(scope: &Scope, item: SelectItem) -> Result<BoundProjection, BindError> {
        let SelectItem { expr, alias } = item;
        let bound = match expr {
            Expr::Column(col) => {
                let col = Self::resolve_scope_col(scope, &col)?;
                BoundSelectItem::Column(col)
            }
            Expr::Literal(v) => BoundSelectItem::Literal(v),
            Expr::CountStar => BoundSelectItem::Aggregate(AggregateExpr {
                func: AggregateFunc::CountStar,
                col_id: ColumnId::default(),
                output_name: alias
                    .clone()
                    .unwrap_or_else(|| "COUNT(*)".try_into().unwrap()),
            }),
            Expr::Agg(func, arg) => Self::bind_agg(scope, &func, *arg, alias.as_ref())?,
            Expr::BinaryOp { .. } | Expr::UnaryOp { .. } => {
                return Err(BindError::Unsupported(
                    "binary/unary expressions in SELECT projections are not yet supported".into(),
                ));
            }
        };
        Ok(BoundProjection { item: bound, alias })
    }

    /// Binds a non-`COUNT(*)` SQL aggregate into an [`AggregateExpr`].
    ///
    /// The aggregate argument must currently be a single column reference. The
    /// output name follows the user alias when present; otherwise it is
    /// synthesized as `FUNC(col)` to match how most databases label aggregate
    /// outputs.
    ///
    /// ```sql
    /// -- SELECT COUNT(age) FROM users
    /// --   BoundSelectItem::Aggregate(AggregateExpr {
    /// --       func: AggregateFunc::CountCol,
    /// --       col_id: ColumnId::try_from(2).unwrap(),
    /// --       output_name: "COUNT(age)".into(),
    /// --   })
    ///
    /// -- SELECT SUM(age) AS total_age FROM users
    /// --   BoundSelectItem::Aggregate(AggregateExpr {
    /// --       func: AggregateFunc::Sum,
    /// --       col_id: ColumnId::try_from(2).unwrap(),
    /// --       output_name: "total_age".into(),
    /// --   })
    ///
    /// -- SELECT AVG(orders.total) FROM users JOIN orders ON users.id = orders.user_id
    /// --   BoundSelectItem::Aggregate(AggregateExpr {
    /// --       func: AggregateFunc::Avg,
    /// --       col_id: ColumnId::try_from(5).unwrap(),
    /// --       output_name: "AVG(total)".into(),
    /// --   })
    /// ```
    ///
    /// For grouped queries, the aggregate operator emits group key columns
    /// first, followed by one column per aggregate in projection order.
    ///
    /// # Errors
    ///
    /// Returns [`BindError::Unsupported`] when the aggregate argument is not a
    /// single column reference, such as `SUM(1)` or `SUM(age + 1)`. Returns
    /// [`BindError::UnknownColumn`] or [`BindError::AmbiguousColumn`] when the
    /// aggregate column cannot be resolved uniquely.
    fn bind_agg(
        scope: &Scope,
        func: &AggFunc,
        arg: Expr,
        alias: Option<&NonEmptyString>,
    ) -> Result<BoundSelectItem, BindError> {
        let Expr::Column(col) = arg else {
            return Err(BindError::Unsupported(
                "aggregates currently support only a single column argument".to_string(),
            ));
        };

        let default_name: NonEmptyString = format!("{func}({})", col.name)
            .try_into()
            .map_err(|e| BindError::Unsupported(format!("invalid aggregate output name: {e}")))?;
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
            output_name: alias.cloned().unwrap_or(default_name),
        }))
    }

    /// Resolves SQL `ORDER BY` columns into sort keys over the bound `FROM` row.
    ///
    /// ```sql
    /// -- ORDER BY age
    /// --   vec![(ColumnId::try_from(2).unwrap(), OrderDirection::Asc)]
    ///
    /// -- ORDER BY users.id DESC, name ASC
    /// --   vec![
    /// --       (ColumnId::try_from(0).unwrap(), OrderDirection::Desc),
    /// --       (ColumnId::try_from(1).unwrap(), OrderDirection::Asc),
    /// --   ]
    ///
    /// -- ORDER BY orders.total DESC  -- after users JOIN orders
    /// --   vec![(ColumnId::try_from(5).unwrap(), OrderDirection::Desc)]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`BindError::UnknownColumn`] when an `ORDER BY` name is not in
    /// scope, [`BindError::AmbiguousColumn`] when an unqualified name matches
    /// multiple joined tables, or [`BindError::Unsupported`] if the resolved
    /// index cannot fit in a [`ColumnId`].
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

    /// Resolves SQL `GROUP BY` columns into grouping keys over the bound `FROM` row.
    ///
    /// ```sql
    /// -- GROUP BY age
    /// --   vec![ColumnId::try_from(2).unwrap()]
    ///
    /// -- GROUP BY users.id, orders.total  -- after users JOIN orders
    /// --   vec![
    /// --       ColumnId::try_from(0).unwrap(),
    /// --       ColumnId::try_from(5).unwrap(),
    /// --   ]
    ///
    /// -- SELECT age, COUNT(*) FROM users GROUP BY age
    /// --   group_by = vec![ColumnId::try_from(2).unwrap()]
    /// ```
    ///
    /// The aggregate operator later emits group key columns first, in this
    /// vector order, then aggregate outputs in `SELECT` declaration order.
    ///
    /// # Errors
    ///
    /// Returns [`BindError::UnknownColumn`] when a grouping column is not in
    /// scope, [`BindError::AmbiguousColumn`] when an unqualified grouping name
    /// appears in more than one joined table, or [`BindError::Unsupported`] if
    /// the resolved index cannot fit in a [`ColumnId`].
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
    /// Resolves one SQL column reference into a planner-facing [`ColumnId`].
    ///
    /// ```sql
    /// -- SELECT name FROM users
    /// --   resolve_scope_col(scope, &ColumnRef::from("name"))?
    /// --   // ColumnId::try_from(1).unwrap()
    ///
    /// -- SELECT orders.total FROM users JOIN orders ON users.id = orders.user_id
    /// --   resolve_scope_col(scope, &ColumnRef::from("orders.total"))?
    /// --   // ColumnId::try_from(5).unwrap()
    ///
    /// -- SELECT id FROM users JOIN orders ON users.id = orders.user_id
    /// --   // error: `id` is ambiguous because both tables expose it
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates [`BindError::UnknownColumn`] and [`BindError::AmbiguousColumn`]
    /// from the scope resolver. Returns [`BindError::Unsupported`] if the
    /// resolved `usize` index is too large for [`ColumnId`].
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
        parser::statements::{BinOp, ColumnRef, Expr, Join, JoinKind},
        primitives::{NonEmptyString, Predicate},
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

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    /// `users(id Uint64 NN, name String, age Int64 NN)` — three columns at
    /// indices 0, 1, 2. Used as the canonical "left side" in join tests.
    fn users_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("name", Type::String),
            field("age", Type::Int64).not_null(),
        ])
    }

    /// `orders(id Uint64 NN, user_id Uint64 NN, total Int64 NN)` — once
    /// joined onto `users`, its columns sit at indices 3, 4, 5. Note the
    /// shared column name `id`: the binder must require qualification
    /// when both tables are in scope.
    fn orders_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("user_id", Type::Uint64).not_null(),
            field("total", Type::Int64).not_null(),
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
            alias: alias.map(|a| {
                NonEmptyString::new(a).expect("test helper `item` requires a valid non-empty alias")
            }),
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
        Expr::binary(Expr::Column(ColumnRef::from(c)), bin_op, Expr::Literal(v))
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
