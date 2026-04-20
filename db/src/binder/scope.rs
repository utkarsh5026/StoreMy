use crate::{
    FileId, Value,
    binder::BindError,
    catalog::manager::TableInfo,
    execution::unary::BooleanExpression,
    parser::statements::{ColumnRef, WhereCondition},
    tuple::{Field, TupleSchema},
};

/// A bound single-table context. DML operates on one of these; SELECT will
/// eventually have a `Scope` made of a `Vec<TableScope>` (one per FROM entry).
pub(super) struct TableScope {
    pub name: String, // actual table name
    pub alias: Option<String>,
    pub file_id: FileId,
    pub schema: TupleSchema,
}

impl TableScope {
    pub fn from_info(info: TableInfo, alias: Option<String>) -> Self {
        Self {
            name: info.name,
            alias,
            file_id: info.file_id,
            schema: info.schema,
        }
    }

    /// Returns `true` if `q` refers to this table (by alias if set, else by name).
    fn qualifier_matches(&self, q: &str) -> bool {
        match &self.alias {
            Some(a) => a == q,
            None => self.name == q,
        }
    }

    /// Resolve a `ColumnRef` (possibly qualified) to a column index in this table.
    pub fn resolve(&self, col: &ColumnRef) -> Result<usize, BindError> {
        if let Some(q) = &col.qualifier {
            if !self.qualifier_matches(q) {
                return Err(BindError::UnknownTable(q.clone()));
            }
        }
        self.schema
            .field_by_name(&col.name)
            .map(|(i, _)| i)
            .ok_or_else(|| BindError::UnknownColumn {
                table: self.alias.clone().unwrap_or_else(|| self.name.clone()),
                column: col.name.clone(),
            })
    }

    pub fn bind_predicate(
        w: &WhereCondition,
        scope: &Self,
    ) -> Result<BooleanExpression, BindError> {
        match w {
            WhereCondition::Predicate { field, op, value } => {
                let idx = scope.resolve(field)?;
                let column = scope.schema.field(idx).expect("resolved idx in range");
                let operand = Self::bind_value_for(value, column, &scope.name)?;
                Ok(BooleanExpression::Leaf {
                    col_index: idx,
                    op: *op,
                    operand,
                })
            }
            WhereCondition::And(l, r) => Ok(BooleanExpression::And(
                Box::new(Self::bind_predicate(l, scope)?),
                Box::new(Self::bind_predicate(r, scope)?),
            )),
            WhereCondition::Or(l, r) => Ok(BooleanExpression::Or(
                Box::new(Self::bind_predicate(l, scope)?),
                Box::new(Self::bind_predicate(r, scope)?),
            )),
        }
    }

    pub(super) fn bind_value_for(
        value: &Value,
        field: &Field,
        table: &str,
    ) -> Result<Value, BindError> {
        if matches!(value, Value::Null) {
            if !field.nullable {
                return Err(BindError::NullViolation {
                    table: table.into(),
                    column: field.name.clone(),
                });
            }
            return Ok(Value::Null);
        }
        Value::try_from((value, field.field_type)).map_err(|e| BindError::TypeMismatch {
            column: field.name.clone(),
            expected: field.field_type.to_string(),
            got: e.to_string(),
        })
    }
}
