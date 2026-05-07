use crate::{
    FileId, Value,
    binder::BindError,
    catalog::manager::TableInfo,
    execution::expression::BooleanExpression,
    parser::statements::{ColumnRef, WhereCondition},
    tuple::{Field, TupleSchema},
};

/// One table participating in a multi-table resolution environment.
///
/// Carries everything name resolution needs: the table's own schema, its
/// alias (if any), and the column offset where this table's fields begin
/// in the joined output row. The qualifier label used for `t.col` lookups
/// is the alias when present, otherwise the table name.
pub(super) struct BoundTable {
    pub name: String,
    pub alias: Option<String>,
    pub schema: TupleSchema,
    pub column_offset: usize,
}

impl BoundTable {
    pub fn new(
        name: String,
        alias: Option<String>,
        schema: TupleSchema,
        column_offset: usize,
    ) -> Self {
        Self {
            name,
            alias,
            schema,
            column_offset,
        }
    }

    fn qualifier_label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Shared name-resolution behavior for both [`Scope`] (multi-table) and
/// [`SingleTableScope`] (DML).
///
/// Implementors only need to say how to resolve a single [`ColumnRef`]
/// against whatever tables are in scope. The default [`bind_where`]
/// walks the predicate tree and uses [`resolve`] at every leaf, so all
/// the recursion / value-coercion logic lives in one place.
///
/// [`bind_where`]: ColumnResolver::bind_where
/// [`resolve`]: ColumnResolver::resolve
pub(super) trait ColumnResolver {
    /// Resolves a `ColumnRef` against the in-scope tables.
    ///
    /// Returns `(global_column_index, field, owning_table_name)` so the
    /// caller can build a column-vs-literal predicate and report errors
    /// against the correct table.
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), BindError>;

    /// Binds a parsed [`WhereCondition`] tree into a [`BooleanExpression`].
    ///
    /// Default implementation recursively binds `And`/`Or` and uses
    /// [`Self::resolve`] + [`bind_value_for`] at each leaf — so a
    /// single-table and multi-table scope share this code unchanged.
    fn bind_where(&self, w: &WhereCondition) -> Result<BooleanExpression, BindError> {
        match w {
            WhereCondition::Predicate { field, op, value } => {
                let (idx, fld, table) = self.resolve(field)?;
                let operand = bind_value_for(value, fld, table)?;
                Ok(BooleanExpression::col_op_lit(idx, *op, operand))
            }
            WhereCondition::And(l, r) => Ok(BooleanExpression::And(
                Box::new(self.bind_where(l)?),
                Box::new(self.bind_where(r)?),
            )),
            WhereCondition::Or(l, r) => Ok(BooleanExpression::Or(
                Box::new(self.bind_where(l)?),
                Box::new(self.bind_where(r)?),
            )),
        }
    }

    /// Resolves an optional [`WhereCondition`] into an optional [`BooleanExpression`].
    ///
    /// If the input is `None`, returns `Ok(None)`. If the input is `Some(condition)`,
    /// attempts to bind it using [`Self::bind_where`] and returns `Ok(Some(expr))`
    /// on success, or an error if binding fails.
    ///
    /// This is intended for binding the `WHERE` or `HAVING` clauses, which may or may not be
    /// present.
    fn resolve_where(
        &self,
        w: Option<&WhereCondition>,
    ) -> Result<Option<BooleanExpression>, BindError> {
        w.map(|w| self.bind_where(w)).transpose()
    }
}

/// Multi-table name-resolution environment used while binding `SELECT`.
///
/// Built up incrementally as the binder walks the `FROM` clause: each
/// table (or join right-hand side) is `push`ed in, after which predicates
/// and expressions can be resolved against the accumulated set. Used by
/// every place in `SELECT` binding that needs to turn a `ColumnRef` into
/// a global column index — `ON`, `WHERE`, projection list, `GROUP BY`,
/// `ORDER BY`.
pub(super) struct Scope {
    tables: Vec<BoundTable>,
}

impl Scope {
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn push(&mut self, table: BoundTable) {
        self.tables.push(table);
    }
}

impl ColumnResolver for Scope {
    /// Resolves a [`ColumnRef`] against the accumulated multi-table scope.
    ///
    /// Qualifiers narrow resolution to tables whose qualifier label matches
    /// (`alias` when present, otherwise the table name). Without a qualifier,
    /// the column must exist in exactly one in-scope table.
    ///
    /// Returns `(global_column_index, field, owning_table_name)` where:
    /// - `global_column_index` is the offset of the owning table plus the local column index (used
    ///   by the executor),
    /// - `field` borrows from the owning table's schema,
    /// - `owning_table_name` is the owning table's `name.as_str()`.
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), BindError> {
        let candidates: Vec<&BoundTable> = match &col.qualifier {
            Some(q) => self
                .tables
                .iter()
                .filter(|t| t.qualifier_label() == q)
                .collect(),
            None => self.tables.iter().collect(),
        };

        if let Some(q) = &col.qualifier {
            if candidates.is_empty() {
                return Err(BindError::unknown_column(q.clone(), col.name.clone()));
            }
            if candidates.len() > 1 {
                return Err(BindError::ambiguous_column(q.clone()));
            }
        }

        let matches: Vec<(usize, &Field, &str)> = candidates
            .iter()
            .filter_map(|t| {
                t.schema.field_by_name(&col.name).map(|(local, fld)| {
                    (t.column_offset + usize::from(local), fld, t.name.as_str())
                })
            })
            .collect();

        match matches.len() {
            0 => {
                let table = col
                    .qualifier
                    .clone()
                    .or_else(|| self.tables.first().map(|t| t.name.clone()))
                    .unwrap_or_default();

                Err(BindError::unknown_column(table, col.name.clone()))
            }
            1 => Ok(matches[0]),
            _ => Err(BindError::AmbiguousColumn {
                column: col.name.clone(),
            }),
        }
    }
}

/// Single-table convenience used by DML (`INSERT`/`UPDATE`/`DELETE`).
///
/// DML's write target is exactly one table, so the qualifier
/// disambiguation a multi-table [`Scope`] needs is dead weight here.
/// This type exposes flat field access (`name`, `file_id`, `schema`)
/// for the binder and a small `bind_where` API. When DML grows
/// `UPDATE … FROM` / subquery support, it will keep this as the
/// *target* and gain a separate [`Scope`] field for resolution.
pub(super) struct SingleTableScope {
    pub name: String,
    pub alias: Option<String>,
    pub file_id: FileId,
    pub schema: TupleSchema,
}

impl SingleTableScope {
    pub fn from_info(info: TableInfo, alias: Option<String>) -> Self {
        Self {
            name: info.name,
            alias,
            file_id: info.file_id,
            schema: info.schema,
        }
    }

    fn qualifier_label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

impl ColumnResolver for SingleTableScope {
    /// Resolves a [`ColumnRef`] against this single DML target table.
    ///
    /// Since DML operates on exactly one table, column resolution is
    /// unambiguous:
    /// - With a qualifier (`t.col`), it must match this table's qualifier label (alias if present,
    ///   otherwise the table name).
    /// - Without a qualifier (`col`), the column must exist in this table's schema.
    ///
    /// Returns `(local_column_index, field, owning_table_name)`, where:
    /// - `field` borrows from `self.schema`,
    /// - `owning_table_name` is `self.name.as_str()`.
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), BindError> {
        let ColumnRef {
            name: col_name,
            qualifier,
        } = col;
        if let Some(q) = qualifier
            && q != self.qualifier_label()
        {
            return Err(BindError::unknown_column(q.clone(), col_name.clone()));
        }

        let (local, fld) = self
            .schema
            .field_by_name(col_name)
            .ok_or_else(|| BindError::unknown_column(self.name.clone(), col_name.clone()))?;

        Ok((usize::from(local), fld, self.name.as_str()))
    }
}

/// Coerces a literal to a column's declared type, honoring nullability.
///
/// Shared between [`Scope`] and [`SingleTableScope`]: the rule is the
/// same in both — `NULL` is allowed iff the column is nullable, and
/// every other value must be coercible to the declared type.
pub(super) fn bind_value_for(
    value: &Value,
    field: &Field,
    table: &str,
) -> Result<Value, BindError> {
    if matches!(value, Value::Null) {
        if !field.nullable {
            return Err(BindError::NullViolation {
                table: table.into(),
                column: field.name.to_string(),
            });
        }
        return Ok(Value::Null);
    }
    Value::try_from((value, field.field_type)).map_err(|e| BindError::TypeMismatch {
        column: field.name.to_string(),
        expected: field.field_type.to_string(),
        got: e.to_string(),
    })
}
