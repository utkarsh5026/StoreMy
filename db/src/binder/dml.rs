//! Binds DML statements (`INSERT`, `DELETE`, and `UPDATE`) into executable plans.
//!
//! This module turns parsed DML statements into "bound" structures that:
//! - resolve the target table via the catalog,
//! - apply table aliases and name resolution for predicates,
//! - coerce literal values to the table's column types,
//! - and reorder `VALUES` rows to match the table schema when a column list is provided.
use std::collections::HashSet;

use crate::{
    FileId, Value,
    binder::{
        BindError,
        expr::BoundExpr,
        scope::{ColumnResolver, SingleTableScope, bind_value_for},
    },
    catalog::manager::Catalog,
    execution::expression::BooleanExpression,
    parser::statements::{
        Assignment, DeleteStatement, InsertSource, InsertStatement, UpdateStatement,
    },
    primitives::ColumnId,
    transaction::Transaction,
    tuple::{Field, TupleSchema},
};

/// A bound `DELETE` statement ready for planning and execution.
pub struct BoundDelete {
    /// Target table name as it appears in the catalog.
    pub name: String,
    /// Storage identifier for the target table.
    pub file_id: FileId,
    /// Optional predicate used to filter which rows are deleted.
    pub filter: Option<BooleanExpression>,
}

/// A bound `UPDATE` statement ready for planning and execution.
pub struct BoundUpdate {
    /// Target table name as it appears in the catalog.
    pub name: String,
    /// Storage identifier for the target table.
    pub file_id: FileId,
    /// Table schema used to interpret assignments and predicates.
    pub schema: TupleSchema,
    /// Column assignments expressed as `(schema_index, value)` pairs.
    pub assignments: Vec<(usize, Value)>,
    /// Optional predicate used to filter which rows are updated.
    pub filter: Option<BooleanExpression>,
}

/// A bound `INSERT` statement ready for planning and execution.
pub struct BoundInsert {
    /// Target table name as it appears in the catalog.
    pub name: String,
    /// Storage identifier for the target table.
    pub file_id: FileId,
    /// Table schema that the inserted rows must match.
    pub schema: TupleSchema,
    /// One row per `VALUES (...)` tuple, already reordered into schema order.
    /// `rows[r][c]` is the expression for column `c` of row `r`.
    pub rows: Vec<Vec<BoundExpr>>,
}

/// Binds a parsed `DELETE` statement against the catalog and current transaction.
///
/// # Errors
///
/// Returns an error if the target table does not exist, or if the `WHERE` clause
/// contains an invalid reference or type mismatch while binding.
impl BoundDelete {
    pub(in crate::binder) fn bind(
        stmt: DeleteStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let DeleteStatement {
            table_name,
            alias,
            where_clause,
        } = stmt;

        let table_info = catalog.get_table_info(txn, table_name.as_str())?;
        let file_id = table_info.file_id;
        let table_scope = SingleTableScope::from_info(table_info, alias);
        let predicate = where_clause
            .map(|w| table_scope.bind_where(&w))
            .transpose()?;

        Ok(Self {
            name: table_name,
            file_id,
            filter: predicate,
        })
    }
}

/// Binds a parsed `UPDATE` statement against the catalog and current transaction.
///
/// Resolves the target table, type-checks each `SET col = value` assignment
/// against the schema, and binds the optional `WHERE` predicate inside the
/// table's scope (so qualified column references like `t.col` and `alias.col`
/// resolve correctly).
///
/// # Errors
///
/// Returns an error if the target table does not exist, an assignment names
/// an unknown column, the same column is assigned twice in one statement, a
/// `NULL` is assigned to a non-nullable column, a value cannot be coerced to
/// the column type, or the `WHERE` clause fails to bind.
impl BoundUpdate {
    pub(in crate::binder) fn bind(
        stmt: UpdateStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let UpdateStatement {
            table_name,
            alias,
            assignments,
            where_clause,
        } = stmt;

        let info = catalog.get_table_info(txn, &table_name)?;
        let scope = SingleTableScope::from_info(info, alias);

        let mut seen: HashSet<ColumnId> = HashSet::with_capacity(assignments.len());
        let mut bound_assignments = Vec::with_capacity(assignments.len());

        for Assignment { column, value } in assignments {
            let (col_id, field) =
                scope
                    .schema
                    .field_by_name(&column)
                    .ok_or_else(|| BindError::UnknownColumn {
                        table: scope.name.clone(),
                        column: column.clone(),
                    })?;
            if !seen.insert(col_id) {
                return Err(BindError::DuplicateColumn(column));
            }
            let coerced = bind_value_for(&value, field, &scope.name)?;
            bound_assignments.push((usize::from(col_id), coerced));
        }

        let filter = where_clause.map(|w| scope.bind_where(&w)).transpose()?;

        Ok(Self {
            name: scope.name,
            file_id: scope.file_id,
            schema: scope.schema,
            assignments: bound_assignments,
            filter,
        })
    }
}

/// Binds a parsed `INSERT ... VALUES ...` statement against the catalog and schema.
///
/// This resolves the target table, checks that the provided column list (if any)
/// names each schema field exactly once, reorders each `VALUES` row into schema
/// order, and coerces literals to the column types.
///
/// # Errors
///
/// Returns an error if the table or columns are unknown, if the column list is
/// invalid (duplicate/missing/extra), if a row has the wrong arity, if a `NULL`
/// is provided for a non-nullable column, or if a value cannot be coerced to the
/// expected column type.
impl BoundInsert {
    pub(in crate::binder) fn bind(
        stmt: InsertStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let info = catalog.get_table_info(txn, &stmt.table_name)?;
        let schema = info.schema.clone();
        let table_name = info.name.clone();

        let projection = build_projection(stmt.columns.as_deref(), &schema, &table_name)?;

        let values = match stmt.source {
            InsertSource::Values(rows) => rows,
            InsertSource::DefaultValues => {
                return Err(BindError::Unsupported(
                    "INSERT … DEFAULT VALUES is not yet supported by the binder".into(),
                ));
            }
            InsertSource::Select(_) => {
                return Err(BindError::Unsupported(
                    "INSERT … SELECT is not yet supported by the binder".into(),
                ));
            }
        };

        let rows = values
            .into_iter()
            .map(|row| bind_row(&row, &schema, &projection, &table_name))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            name: table_name,
            file_id: info.file_id,
            schema,
            rows,
        })
    }
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
        if !schema.contains(c) {
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

/// Binds a single `VALUES (...)` row into schema order.
///
/// # Errors
///
/// Returns an error if the row arity does not match the schema, if a non-nullable
/// column is set to `NULL`, or if a literal cannot be coerced to the column type.
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

/// Binds a literal for a specific table column, applying nullability and coercions.
///
/// # Errors
///
/// Returns an error if `value` is `NULL` for a non-nullable column, or if the
/// value cannot be coerced to the column's type.
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

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Type,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        execution::expression::BooleanExpression,
        parser::statements::{
            Assignment, ColumnRef, DeleteStatement, InsertSource, InsertStatement, UpdateStatement,
            WhereCondition,
        },
        primitives::Predicate,
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    // ── fixture helpers ───────────────────────────────────────────────────

    fn make_catalog_and_txn_mgr(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    /// (id: Uint64 NOT NULL, name: String nullable, age: Int64 NOT NULL)
    fn three_col_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Uint64).not_null(),
            Field::new("name", Type::String),
            Field::new("age", Type::Int64).not_null(),
        ])
    }

    /// Creates a `users` table with `three_col_schema` and commits.
    fn create_users(catalog: &Catalog, txn_mgr: &TransactionManager) {
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", three_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();
    }

    fn assignment(col: &str, value: Value) -> Assignment {
        Assignment {
            column: col.to_string(),
            value,
        }
    }

    fn predicate(col: &str, op: Predicate, value: Value) -> WhereCondition {
        WhereCondition::predicate(ColumnRef::from(col), op, value)
    }

    /// Convenience: extract the error from a `Result<T, E>` without requiring `T: Debug`.
    fn expect_err<T, E>(r: Result<T, E>) -> E {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // bind_delete
    // ──────────────────────────────────────────────────────────────────────

    // --- happy path ---

    // DELETE without WHERE binds to None filter and the catalog file_id.
    #[test]
    fn test_bind_delete_no_where_binds_with_no_filter() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "users").unwrap();
        let stmt = DeleteStatement {
            table_name: "users".into(),
            alias: None,
            where_clause: None,
        };
        let bound = BoundDelete::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert_eq!(bound.name, "users");
        assert_eq!(bound.file_id, info.file_id);
        assert!(bound.filter.is_none());
    }

    // DELETE with WHERE binds the predicate against the table's schema.
    #[test]
    fn test_bind_delete_with_where_binds_predicate() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = DeleteStatement {
            table_name: "users".into(),
            alias: None,
            where_clause: Some(predicate("age", Predicate::Equals, Value::Int64(30))),
        };
        let bound = BoundDelete::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert!(matches!(bound.filter, Some(BooleanExpression::Leaf { .. })));
    }

    // --- error paths ---

    // Missing target table surfaces as a Catalog error wrapped in BindError.
    #[test]
    fn test_bind_delete_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let stmt = DeleteStatement {
            table_name: "ghost".into(),
            alias: None,
            where_clause: None,
        };
        let err = expect_err(BoundDelete::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::Catalog(_)), "got {err:?}");
    }

    // WHERE referencing a missing column errors as UnknownColumn.
    #[test]
    fn test_bind_delete_where_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = DeleteStatement {
            table_name: "users".into(),
            alias: None,
            where_clause: Some(predicate("nope", Predicate::Equals, Value::Int64(1))),
        };
        let err = expect_err(BoundDelete::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::UnknownColumn { ref column, .. } if column == "nope"));
    }

    // ──────────────────────────────────────────────────────────────────────
    // bind_insert
    // ──────────────────────────────────────────────────────────────────────

    // --- happy path ---

    // No column list → identity projection; rows already in schema order.
    #[test]
    fn test_bind_insert_no_column_list_identity_projection() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![
                Value::Int64(1),
                Value::String("alice".into()),
                Value::Int64(30),
            ]]),
        };
        let bound = BoundInsert::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert_eq!(bound.name, "users");
        assert_eq!(bound.rows.len(), 1);
        assert_eq!(bound.rows[0].len(), 3);
        // Coerced: Int64(1) → Uint64(1) for the id column.
        assert!(matches!(
            bound.rows[0][0],
            BoundExpr::Literal(Value::Uint64(1))
        ));
        assert!(matches!(
            bound.rows[0][1],
            BoundExpr::Literal(Value::String(ref s)) if s == "alice"
        ));
        assert!(matches!(
            bound.rows[0][2],
            BoundExpr::Literal(Value::Int64(30))
        ));
    }

    // Column list reorders VALUES into schema order: schema is (id, name, age),
    // user provides (age, id, name) — bound row[0] should be id, row[1] name, row[2] age.
    #[test]
    fn test_bind_insert_column_list_reorders_to_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: Some(vec!["age".into(), "id".into(), "name".into()]),
            source: InsertSource::Values(vec![vec![
                Value::Int64(99),
                Value::Int64(7),
                Value::String("bob".into()),
            ]]),
        };
        let bound = BoundInsert::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert!(matches!(
            bound.rows[0][0],
            BoundExpr::Literal(Value::Uint64(7))
        ));
        assert!(matches!(
            bound.rows[0][1],
            BoundExpr::Literal(Value::String(ref s)) if s == "bob"
        ));
        assert!(matches!(
            bound.rows[0][2],
            BoundExpr::Literal(Value::Int64(99))
        ));
    }

    // NULL into a nullable column is allowed and produces a Literal(Null).
    #[test]
    fn test_bind_insert_null_into_nullable_column_ok() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![
                Value::Int64(1),
                Value::Null, // name is nullable
                Value::Int64(30),
            ]]),
        };
        let bound = BoundInsert::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert!(matches!(bound.rows[0][1], BoundExpr::Literal(Value::Null)));
    }

    // --- error paths ---

    // Missing target table errors via Catalog.
    #[test]
    fn test_bind_insert_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "ghost".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![Value::Int64(1)]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::Catalog(_)));
    }

    // Column list naming an unknown column errors as UnknownColumn.
    #[test]
    fn test_bind_insert_column_list_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: Some(vec!["id".into(), "nope".into(), "age".into()]),
            source: InsertSource::Values(vec![vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
            ]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::UnknownColumn { ref column, .. } if column == "nope"));
    }

    // Duplicate column names in the column list error as DuplicateInsertColumn.
    #[test]
    fn test_bind_insert_duplicate_column_in_column_list_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: Some(vec!["id".into(), "id".into(), "age".into()]),
            source: InsertSource::Values(vec![vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
            ]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateInsertColumn { ref column, .. } if column == "id")
        );
    }

    // Column list shorter than schema → WrongColumnCount.
    #[test]
    fn test_bind_insert_column_list_missing_columns_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: Some(vec!["id".into(), "name".into()]),
            source: InsertSource::Values(vec![vec![Value::Int64(1), Value::String("a".into())]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::WrongColumnCount {
            expected: 3,
            got: 2,
            ..
        }));
    }

    // Row arity not matching schema → WrongColumnCount.
    #[test]
    fn test_bind_insert_row_arity_mismatch_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![Value::Int64(1), Value::String("a".into())]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::WrongColumnCount {
            expected: 3,
            got: 2,
            ..
        }));
    }

    // NULL into a NOT NULL column → NullViolation.
    #[test]
    fn test_bind_insert_null_into_not_null_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![
                Value::Null, // id is NOT NULL
                Value::String("a".into()),
                Value::Int64(1),
            ]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::NullViolation { ref column, .. } if column == "id"));
    }

    // String literal into an Int64 column → TypeMismatch.
    #[test]
    fn test_bind_insert_type_mismatch_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = InsertStatement {
            table_name: "users".into(),
            columns: None,
            source: InsertSource::Values(vec![vec![
                Value::Int64(1),
                Value::String("a".into()),
                Value::String("not a number".into()), // age expects Int64
            ]]),
        };
        let err = expect_err(BoundInsert::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::TypeMismatch { ref column, .. } if column == "age"));
    }

    // ──────────────────────────────────────────────────────────────────────
    // bind_update
    // ──────────────────────────────────────────────────────────────────────

    // --- happy path ---

    // Single assignment with no WHERE binds to (idx, coerced_value) and no filter.
    #[test]
    fn test_bind_update_single_assignment_no_where() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("age", Value::Int64(42))],
            where_clause: None,
        };
        let bound = BoundUpdate::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert_eq!(bound.name, "users");
        assert_eq!(bound.assignments.len(), 1);
        assert_eq!(bound.assignments[0].0, 2); // age is index 2
        assert!(matches!(bound.assignments[0].1, Value::Int64(42)));
        assert!(bound.filter.is_none());
    }

    // Multi-column assignments preserve order and resolve correct indices.
    #[test]
    fn test_bind_update_multiple_assignments_resolve_indices() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![
                assignment("name", Value::String("zed".into())),
                assignment("age", Value::Int64(1)),
            ],
            where_clause: None,
        };
        let bound = BoundUpdate::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert_eq!(bound.assignments[0].0, 1); // name
        assert_eq!(bound.assignments[1].0, 2); // age
    }

    // WHERE qualified by alias resolves through the bound TableScope.
    #[test]
    fn test_bind_update_where_with_alias_qualifier() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: Some("u".into()),
            assignments: vec![assignment("age", Value::Int64(1))],
            where_clause: Some(predicate("u.id", Predicate::Equals, Value::Int64(7))),
        };
        let bound = BoundUpdate::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert!(bound.filter.is_some());
    }

    // NULL into a nullable column is fine.
    #[test]
    fn test_bind_update_null_into_nullable_column_ok() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("name", Value::Null)],
            where_clause: None,
        };
        let bound = BoundUpdate::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        assert!(matches!(bound.assignments[0].1, Value::Null));
    }

    // --- error paths ---

    // Missing target table errors via Catalog.
    #[test]
    fn test_bind_update_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "ghost".into(),
            alias: None,
            assignments: vec![assignment("age", Value::Int64(1))],
            where_clause: None,
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::Catalog(_)));
    }

    // Assignment naming an unknown column errors as UnknownColumn.
    #[test]
    fn test_bind_update_unknown_column_in_assignment_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("nope", Value::Int64(1))],
            where_clause: None,
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::UnknownColumn { ref column, .. } if column == "nope"));
    }

    // Same column assigned twice in one statement → DuplicateColumn.
    #[test]
    fn test_bind_update_duplicate_column_assignment_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![
                assignment("age", Value::Int64(1)),
                assignment("age", Value::Int64(2)),
            ],
            where_clause: None,
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::DuplicateColumn(ref n) if n == "age"));
    }

    // NULL into a NOT NULL column → NullViolation.
    #[test]
    fn test_bind_update_null_into_not_null_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("age", Value::Null)],
            where_clause: None,
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::NullViolation { ref column, .. } if column == "age"));
    }

    // Type mismatch on the right-hand side surfaces as TypeMismatch.
    #[test]
    fn test_bind_update_type_mismatch_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("age", Value::String("not a number".into()))],
            where_clause: None,
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::TypeMismatch { ref column, .. } if column == "age"));
    }

    // WHERE referencing a missing column errors as UnknownColumn.
    #[test]
    fn test_bind_update_where_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        create_users(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let stmt = UpdateStatement {
            table_name: "users".into(),
            alias: None,
            assignments: vec![assignment("age", Value::Int64(1))],
            where_clause: Some(predicate("nope", Predicate::Equals, Value::Int64(0))),
        };
        let err = expect_err(BoundUpdate::bind(stmt, &catalog, &txn));
        txn.commit().unwrap();

        assert!(matches!(err, BindError::UnknownColumn { ref column, .. } if column == "nope"));
    }
}
