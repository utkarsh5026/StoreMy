use super::{ConstraintViolation, EngineError};
use crate::{
    FileId, Value,
    catalog::TableInfo,
    execution::expression::BooleanExpression,
    parser::statements::{BinOp, ColumnRef, Expr, UnOp},
    primitives::{NonEmptyString, Predicate},
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
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError>;

    /// Binds a parsed boolean [`Expr`] tree into a [`BooleanExpression`].
    ///
    /// Default implementation recursively binds `And`/`Or` and uses
    /// [`Self::resolve`] + [`fit_value_to_field`] at each leaf — so a
    /// single-table and multi-table scope share this code unchanged.
    fn bind_where(&self, w: &Expr) -> Result<BooleanExpression, EngineError> {
        match w {
            Expr::BinaryOp { lhs, op, rhs } => match op {
                BinOp::And => Ok(BooleanExpression::And(
                    Box::new(self.bind_where(lhs)?),
                    Box::new(self.bind_where(rhs)?),
                )),
                BinOp::Or => Ok(BooleanExpression::Or(
                    Box::new(self.bind_where(lhs)?),
                    Box::new(self.bind_where(rhs)?),
                )),
                BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
                    self.bind_comparison(lhs, *op, rhs)
                }
            },
            Expr::UnaryOp {
                op: UnOp::Not,
                operand,
            } => Ok(BooleanExpression::Not(Box::new(self.bind_where(operand)?))),
            other => Err(EngineError::Unsupported(format!(
                "unsupported boolean expression in WHERE/HAVING/ON: {other}"
            ))),
        }
    }

    /// Resolves an optional boolean [`Expr`] into an optional [`BooleanExpression`].
    fn resolve_where(&self, w: Option<&Expr>) -> Result<Option<BooleanExpression>, EngineError> {
        w.map(|w| self.bind_where(w)).transpose()
    }

    fn bind_comparison(
        &self,
        lhs: &Expr,
        op: BinOp,
        rhs: &Expr,
    ) -> Result<BooleanExpression, EngineError> {
        let pred = binop_to_predicate(op).ok_or_else(|| {
            EngineError::Unsupported(format!("unsupported comparison operator in WHERE: {op:?}"))
        })?;

        match (lhs, rhs) {
            (Expr::Column(lc), Expr::Literal(rv)) => {
                let (l_idx, l_fld, l_table) = self.resolve(lc)?;
                let lit = fit_value_to_field(rv, l_fld, l_table)?;
                Ok(BooleanExpression::col_op_lit(l_idx, pred, lit))
            }
            (Expr::Literal(lv), Expr::Column(rc)) => {
                let (r_idx, r_fld, r_table) = self.resolve(rc)?;
                let lit = fit_value_to_field(lv, r_fld, r_table)?;
                Ok(BooleanExpression::col_op_lit(
                    r_idx,
                    flip_predicate(pred),
                    lit,
                ))
            }
            (Expr::Column(lc), Expr::Column(rc)) => {
                let (l_idx, _l_fld, _l_table) = self.resolve(lc)?;
                let (r_idx, _r_fld, _r_table) = self.resolve(rc)?;
                Ok(BooleanExpression::col_op_col(l_idx, pred, r_idx))
            }
            _ => Err(EngineError::Unsupported(format!(
                "unsupported comparison operands in WHERE/HAVING/ON: {lhs} {op} {rhs}"
            ))),
        }
    }
}

fn binop_to_predicate(op: BinOp) -> Option<Predicate> {
    Some(match op {
        BinOp::Eq => Predicate::Equals,
        BinOp::NotEq => Predicate::NotEqualBracket,
        BinOp::Lt => Predicate::LessThan,
        BinOp::LtEq => Predicate::LessThanOrEqual,
        BinOp::Gt => Predicate::GreaterThan,
        BinOp::GtEq => Predicate::GreaterThanOrEqual,
        BinOp::And | BinOp::Or => return None,
    })
}

fn flip_predicate(p: Predicate) -> Predicate {
    match p {
        Predicate::LessThan => Predicate::GreaterThan,
        Predicate::GreaterThan => Predicate::LessThan,
        Predicate::LessThanOrEqual => Predicate::GreaterThanOrEqual,
        Predicate::GreaterThanOrEqual => Predicate::LessThanOrEqual,
        other => other,
    }
}

/// Multi-table name-resolution environment used while binding `SELECT`.
///
/// Built up incrementally as the binder walks the `FROM` clause: each
/// table (or join right-hand side) is `push`ed in, after which predicates
/// and expressions can be resolved against the accumulated set.
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
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError> {
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
                return Err(EngineError::UnknownColumn {
                    table: q.to_string(),
                    column: col.name.to_string(),
                });
            }
            if candidates.len() > 1 {
                return Err(EngineError::AmbiguousColumn {
                    column: q.to_string(),
                });
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
                    .as_ref()
                    .map(ToString::to_string)
                    .or_else(|| self.tables.first().map(|t| t.name.clone()))
                    .unwrap_or_default();

                Err(EngineError::UnknownColumn {
                    table,
                    column: col.name.to_string(),
                })
            }
            1 => Ok(matches[0]),
            _ => Err(EngineError::AmbiguousColumn {
                column: col.name.to_string(),
            }),
        }
    }
}

/// Single-table convenience used by DML (`INSERT`/`UPDATE`/`DELETE`).
///
/// DML's write target is exactly one table, so the qualifier
/// disambiguation a multi-table [`Scope`] needs is dead weight here.
pub(super) struct SingleTableScope {
    pub name: NonEmptyString,
    pub alias: Option<NonEmptyString>,
    pub file_id: FileId,
    pub schema: TupleSchema,
}

impl SingleTableScope {
    pub fn from_info(info: TableInfo, alias: Option<NonEmptyString>) -> Self {
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
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError> {
        let ColumnRef {
            name: col_name,
            qualifier,
        } = col;
        if let Some(q) = qualifier
            && q != self.qualifier_label()
        {
            return Err(EngineError::UnknownColumn {
                table: q.to_string(),
                column: col_name.to_string(),
            });
        }

        let (local, fld) =
            self.schema
                .field_by_name(col_name)
                .ok_or_else(|| EngineError::UnknownColumn {
                    table: self.name.to_string(),
                    column: col_name.to_string(),
                })?;

        Ok((usize::from(local), fld, self.name.as_str()))
    }
}

/// Coerces a literal to a column's declared type, honoring nullability.
pub(super) fn fit_value_to_field(
    value: &Value,
    field: &Field,
    table: &str,
) -> Result<Value, EngineError> {
    if matches!(value, Value::Null) {
        if !field.nullable {
            return Err(ConstraintViolation::NullViolation {
                table: table.into(),
                column: field.name.to_string(),
            }
            .into());
        }
        return Ok(Value::Null);
    }
    Value::try_from((value, field.field_type)).map_err(|e| EngineError::TypeMismatch {
        column: field.name.to_string(),
        expected: field.field_type.to_string(),
        got: e.to_string(),
    })
}
