use std::collections::HashSet;

use crate::{
    Value,
    binder::{BindError, BoundDelete, BoundInsert, expr::BoundExpr, scope::TableScope},
    catalog::manager::Catalog,
    parser::statements::{DeleteStatement, InsertStatement},
    transaction::Transaction,
    tuple::{Field, TupleSchema},
};

pub(in crate::binder) fn bind_delete(
    stmt: DeleteStatement,
    catalog: &Catalog,
    txn: &Transaction<'_>,
) -> Result<BoundDelete, BindError> {
    let DeleteStatement {
        table_name,
        alias,
        where_clause,
    } = stmt;

    let table_info = catalog.get_table_info(txn, table_name.as_str())?;
    let table_scope = TableScope::from_info(table_info.clone(), alias);
    let predicate = where_clause
        .map(|w| TableScope::bind_predicate(&w, &table_scope))
        .transpose()?;

    Ok(BoundDelete {
        name: table_name,
        file_id: table_scope.file_id,
        filter: predicate,
    })
}

pub(in crate::binder) fn bind_insert(
    stmt: InsertStatement,
    catalog: &Catalog,
    txn: &Transaction<'_>,
) -> Result<BoundInsert, BindError> {
    let info = catalog.get_table_info(txn, &stmt.table_name)?;
    let schema = info.schema.clone();
    let table_name = info.name.clone();

    let projection = build_projection(stmt.columns.as_deref(), &schema, &table_name)?;

    let rows = stmt
        .values
        .into_iter()
        .map(|row| bind_row(&row, &schema, &projection, &table_name))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(BoundInsert {
        name: table_name,
        file_id: info.file_id,
        schema,
        rows,
    })
}

/// Permutation `p` such that `p[i]` is the index in the SQL VALUES row
/// that supplies the value for schema field `i`.
///
/// When `cols` is `None`, VALUES is already in schema order and `p` is
/// the identity. When `cols` is `Some`, every schema field must be named
/// exactly once; any unknown, duplicate, extra, or missing column is an
/// error.
fn build_projection(
    cols: Option<&[String]>,
    schema: &TupleSchema,
    table: &str,
) -> Result<Vec<usize>, BindError> {
    let n = schema.num_fields();

    let Some(cols) = cols else {
        return Ok((0..n).collect());
    };

    let mut seen: HashSet<&str> = HashSet::with_capacity(cols.len());
    for c in cols {
        if schema.field_by_name(c).is_none() {
            return Err(BindError::UnknownColumn {
                table: table.into(),
                column: c.clone(),
            });
        }
        if !seen.insert(c.as_str()) {
            return Err(BindError::DuplicateInsertColumn {
                table: table.into(),
                column: c.clone(),
            });
        }
    }

    if seen.len() != n {
        return Err(BindError::WrongColumnCount {
            table: table.into(),
            expected: n,
            got: seen.len(),
        });
    }

    let mut perm = vec![0usize; n];
    for (i, slot) in perm.iter_mut().enumerate() {
        let fname = &schema.field(i).expect("schema index in range").name;
        *slot = cols
            .iter()
            .position(|c| c == fname)
            .expect("insert column list covers every schema field");
    }
    Ok(perm)
}

fn bind_row(
    row: &[Value],
    schema: &TupleSchema,
    projection: &[usize],
    table: &str,
) -> Result<Vec<BoundExpr>, BindError> {
    let n = schema.num_fields();
    if row.len() != n {
        return Err(BindError::WrongColumnCount {
            table: table.into(),
            expected: n,
            got: row.len(),
        });
    }

    (0..n)
        .map(|i| {
            let field = schema.field(i).expect("schema index in range");
            let value = &row[projection[i]];
            bind_literal_for_column(value, field, table)
        })
        .collect()
}

fn bind_literal_for_column(
    value: &Value,
    field: &Field,
    table: &str,
) -> Result<BoundExpr, BindError> {
    if matches!(value, &Value::Null) {
        if !field.nullable {
            return Err(BindError::NullViolation {
                table: table.into(),
                column: field.name.clone(),
            });
        }
        return Ok(BoundExpr::Literal(Value::Null));
    }

    let coerced =
        Value::try_from((value, field.field_type)).map_err(|e| BindError::TypeMismatch {
            column: field.name.clone(),
            expected: field.field_type.to_string(),
            got: e.to_string(),
        })?;
    Ok(BoundExpr::Literal(coerced))
}
