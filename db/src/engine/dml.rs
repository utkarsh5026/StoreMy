//! Runs row-changing SQL (`INSERT`, `DELETE`, `UPDATE`) on behalf of the storage engine.
//!
//! Each operation lives in its own submodule (`insert`, `delete`, `update`), exposing a single
//! `execute` entry point. Helpers shared between operations (`collect_matching`,
//! `resolve_where_clause`) sit at the file root so all three submodules can reach them via
//! `super::`.
//!
//! Parsed statements are turned into heap operations inside a short transaction callback.
//! Column names are matched to the table schema, optional `WHERE` clauses become
//! [`BooleanExpression`](crate::execution::unary::BooleanExpression) trees, and matching rows
//! are read or updated through the catalog's [`HeapFile`](crate::heap::file::HeapFile).

use fallible_iterator::FallibleIterator;

use crate::{
    TransactionId, Value,
    catalog::manager::Catalog,
    engine::EngineError,
    execution::unary::BooleanExpression,
    heap::file::HeapFile,
    parser::statements::{Assignment, WhereCondition},
    primitives::RecordId,
    tuple::{Tuple, TupleSchema},
};

pub(super) mod insert {
    use std::collections::HashSet;

    use super::{Catalog, EngineError, Tuple};
    use crate::{
        Value,
        catalog::manager::TableInfo,
        engine::StatementResult,
        parser::statements::InsertStatement,
        transaction::{Transaction, TransactionManager},
        tuple::TupleSchema,
    };

    /// Inserts one or more rows into a table and reports how many were written.
    ///
    /// If the statement lists columns, every table column must appear exactly once (order
    /// independent of the physical schema). If no column list is given, each supplied tuple
    /// must list values in schema order. Values are converted to [`Tuple`] rows and appended
    /// via [`HeapFile::bulk_insert`](crate::heap::file::HeapFile::bulk_insert).
    ///
    /// The work runs inside [`super::super::with_txn`], which starts and commits a transaction
    /// around the closure.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] when the table or columns are unknown, the number of columns
    /// does not match the table, the heap cannot be opened, or insertion fails.
    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: InsertStatement,
    ) -> Result<StatementResult, EngineError> {
        let InsertStatement {
            table_name,
            columns,
            values,
            ..
        } = statement;

        let f = |txn: &Transaction<'_>| {
            let TableInfo {
                schema, file_id, ..
            } = catalog.get_table_info(txn, &table_name)?;

            let tuples = values_to_schema_projection(columns.as_deref(), &schema, &table_name)
                .and_then(|p| materialize_insert_tuples(values, &schema, &table_name, &p))?;

            let heap = catalog.get_table_heap(file_id)?;
            let record_ids = heap.bulk_insert(txn.transaction_id(), tuples)?;
            Ok(StatementResult::inserted(table_name, record_ids.len()))
        };

        crate::engine::with_txn(f, txn_manager)
    }

    /// Builds the index slice used by [`Tuple::project`](crate::tuple::Tuple::project) when
    /// materializing INSERT rows.
    ///
    /// Parsed `VALUES` tuples follow the order of columns in the SQL text. On disk, rows follow
    /// [`TupleSchema`](crate::tuple::TupleSchema) declaration order. This helper returns a
    /// permutation `p` such that the `i`-th field of `input.project(&p)` is the value for schema
    /// field `i`.
    ///
    /// When `cols` is `None`, literals are already in schema order, so `p` is the identity
    /// `[0, 1, …, n - 1]`. When `cols` is `Some`, every field in `schema` must appear exactly
    /// once (names may be listed in any order); `p[i]` is the index within the value list of
    /// the column matching `schema.field(i).name`.
    ///
    /// # Errors
    ///
    /// - [`EngineError::column_not_found`](crate::engine::EngineError::column_not_found) — a
    ///   listed name is not a column of `schema`.
    /// - [`EngineError::column_already_exists`](crate::engine::EngineError::column_already_exists) —
    ///   the same column name appears twice in `cols`.
    /// - [`EngineError::wrong_column_count`](crate::engine::EngineError::wrong_column_count) —
    ///   `cols` does not name every field exactly once (too few or too many entries after uniqueness).
    pub(super) fn values_to_schema_projection(
        cols: Option<&[String]>,
        schema: &TupleSchema,
        table_name: &str,
    ) -> Result<Vec<usize>, EngineError> {
        let n = schema.num_fields();

        let projection = if let Some(cols) = cols {
            let mut seen: HashSet<&str> = HashSet::with_capacity(cols.len());
            for col in cols {
                schema
                    .field_by_name(col)
                    .ok_or_else(|| EngineError::column_not_found(table_name, col.as_str()))?;

                if !seen.insert(col) {
                    return Err(EngineError::column_already_exists(table_name, col.as_str()));
                }
            }

            if seen.len() != n {
                return Err(EngineError::wrong_column_count(table_name, n, seen.len()));
            }

            let mut perm = vec![0usize; n];
            for (i, slot) in perm.iter_mut().enumerate() {
                let fname = &schema.field(i).expect("schema index in range").name;
                *slot = cols
                    .iter()
                    .position(|c| c == fname)
                    .expect("insert column list covers every schema field");
            }
            perm
        } else {
            (0..n).collect()
        };

        Ok(projection)
    }

    /// Turns parsed `INSERT … VALUES` rows into [`Tuple`] values ready for the heap.
    ///
    /// Each inner `Vec<Value>` is one row of literals in SQL column order (matching the parser).
    /// Every row must have `schema.num_fields()` entries.
    ///
    /// `projection` is typically built by [`values_to_schema_projection`]: for each schema
    /// field index `i`, `projection[i]` picks which value in the row to read, then non-null
    /// cells are coerced to `schema.field(i).field_type` using the same `TryFrom<(&Value, Type)>`
    /// implementation as `WHERE` literals (`crate::types`), producing a [`Tuple`] in schema /
    /// heap field order.
    ///
    /// # Errors
    ///
    /// - [`EngineError::wrong_column_count`](crate::engine::EngineError::wrong_column_count) —
    ///   a `VALUES` row has the wrong number of values.
    /// - [`EngineError::type_error`](crate::engine::EngineError::type_error) — a non-null
    ///   literal cannot be coerced to the column type.
    /// - [`EngineError::null_violation`](crate::engine::EngineError::null_violation) — `NULL`
    ///   for a column that is not nullable.
    pub(super) fn materialize_insert_tuples(
        values: Vec<Vec<Value>>,
        schema: &TupleSchema,
        table_name: &str,
        projection: &[usize],
    ) -> Result<Vec<Tuple>, EngineError> {
        let expected = schema.num_fields();
        values
            .into_iter()
            .map(|row| {
                if row.len() != expected {
                    return Err(EngineError::wrong_column_count(
                        table_name,
                        expected,
                        row.len(),
                    ));
                }

                let mut slot = Vec::with_capacity(expected);
                for i in 0..expected {
                    let field = schema.field(i).expect("schema index in range");
                    let cell = &row[projection[i]];

                    if matches!(cell, Value::Null) {
                        if !field.nullable {
                            return Err(EngineError::null_violation(
                                table_name,
                                field.name.as_str(),
                            ));
                        }
                        slot.push(Value::Null);
                        continue;
                    }

                    let coerced = Value::try_from((cell, field.field_type))
                        .map_err(|e| EngineError::type_error(e.to_string()))?;
                    slot.push(coerced);
                }
                Ok(Tuple::new(slot))
            })
            .collect::<Result<Vec<Tuple>, EngineError>>()
    }
}

pub(super) mod delete {
    use super::{Catalog, EngineError, RecordId, collect_matching, resolve_where_clause};
    use crate::{
        engine::StatementResult,
        parser::statements::DeleteStatement,
        transaction::{Transaction, TransactionManager},
    };

    /// Deletes every row in a table that satisfies an optional `WHERE` clause.
    ///
    /// When `where_clause` is `None`, the whole table is scanned and every tuple is removed.
    /// When a predicate is present, only rows for which the expression evaluates to true are
    /// deleted.
    ///
    /// The work runs inside [`super::super::with_txn`].
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] when the table is missing, the heap cannot be read, `WHERE`
    /// resolution fails, or a per-row delete fails.
    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: DeleteStatement,
    ) -> Result<StatementResult, EngineError> {
        let table_name = statement.table_name;

        let f = |txn: &Transaction<'_>| -> Result<StatementResult, EngineError> {
            let table_info = catalog.get_table_info(txn, &table_name)?;
            let heap = catalog.get_table_heap(table_info.file_id)?;

            let predicate = statement
                .where_clause
                .map(|w| resolve_where_clause(&w, &table_info.schema))
                .transpose()?;

            let txn_id = txn.transaction_id();
            let rids = collect_matching(&heap, txn_id, predicate.as_ref())?
                .into_iter()
                .map(|(rid, _)| rid)
                .collect::<Vec<RecordId>>();

            let deleted = rids.len();
            for rid in rids {
                heap.delete_tuple(txn_id, rid)?;
            }

            Ok(StatementResult::deleted(table_name, deleted))
        };

        crate::engine::with_txn(f, txn_manager)
    }
}

pub(super) mod update {
    use std::collections::HashSet;

    use super::{
        Assignment, Catalog, EngineError, TupleSchema, Value, collect_matching,
        resolve_where_clause,
    };
    use crate::{
        catalog::manager::TableInfo,
        engine::StatementResult,
        parser::statements::UpdateStatement,
        transaction::{Transaction, TransactionManager},
    };

    /// Applies column assignments to all rows matching an optional `WHERE` clause.
    ///
    /// Assignments are resolved to field indexes in the table schema; each matching row is
    /// read, updated in memory, and written back with
    /// [`HeapFile::update_tuple`](crate::heap::file::HeapFile::update_tuple).
    ///
    /// The work runs inside [`super::super::with_txn`].
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] when the table or assignment column names are unknown, a new
    /// value does not fit the column type, `WHERE` resolution fails, or an update fails at
    /// the heap layer.
    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: UpdateStatement,
    ) -> Result<StatementResult, EngineError> {
        let UpdateStatement {
            table_name,
            assignments,
            where_clause,
            ..
        } = statement;

        let f = |txn: &Transaction<'_>| -> Result<StatementResult, EngineError> {
            let TableInfo {
                schema, file_id, ..
            } = catalog.get_table_info(txn, &table_name)?;
            let heap_file = catalog.get_table_heap(file_id)?;

            let resolved_assignments =
                resolve_assignments(assignments.as_slice(), &schema, &table_name)?;

            let predicate = where_clause
                .map(|w| resolve_where_clause(&w, &schema))
                .transpose()?;

            let txn_id = txn.transaction_id();
            let rows = collect_matching(&heap_file, txn_id, predicate.as_ref())?;
            let updated = rows.len();

            for (rid, mut tuple) in rows {
                for (idx, value) in &resolved_assignments {
                    tuple
                        .set_field(*idx, value.clone(), &schema)
                        .map_err(|e| EngineError::type_error(e.to_string()))?;
                }
                heap_file.update_tuple(txn_id, rid, &tuple)?;
            }

            Ok(StatementResult::updated(table_name, updated))
        };

        crate::engine::with_txn(f, txn_manager)
    }

    /// Turns `SET` assignments into `(field_index, value)` pairs using `schema`.
    ///
    /// Output order matches the input assignment order. Duplicate column names are not merged;
    /// callers receive one entry per assignment.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::column_not_found`] if a column name is not in `schema`. The
    /// `table_name` is only used for that error message.
    pub(super) fn resolve_assignments(
        assignments: &[Assignment],
        schema: &TupleSchema,
        table_name: &str,
    ) -> Result<Vec<(usize, Value)>, EngineError> {
        let mut field_mapping = Vec::with_capacity(assignments.len());
        let mut seen: HashSet<&str> = HashSet::with_capacity(assignments.len());
        for Assignment { column, value } in assignments {
            if !seen.insert(column) {
                return Err(EngineError::column_already_exists(table_name, column));
            }

            let (idx, _) = schema
                .field_by_name(column)
                .ok_or_else(|| EngineError::column_not_found(table_name, column))?;
            field_mapping.push((idx, value.clone()));
        }
        Ok(field_mapping)
    }
}

/// Collects every heap row visible to `transaction_id`, optionally filtered by `predicate`.
///
/// Rows with no predicate are included. When `predicate` is `Some`, a row is kept only if the
/// expression evaluates to true for that row's tuple.
///
/// # Errors
///
/// Returns [`EngineError`] if the heap scan cannot advance.
fn collect_matching(
    heap: &HeapFile,
    transaction_id: TransactionId,
    predicate: Option<&BooleanExpression>,
) -> Result<Vec<(RecordId, Tuple)>, EngineError> {
    let mut scan = heap.scan(transaction_id)?;
    let mut out = Vec::new();
    while let Some((rid, tuple)) = FallibleIterator::next(&mut scan)? {
        if predicate.is_none_or(|p| p.eval(&tuple)) {
            out.push((rid, tuple));
        }
    }
    Ok(out)
}

/// Builds a [`BooleanExpression`] from a parsed [`WhereCondition`].
///
/// Leaf predicates resolve the column name to an index, coerce the literal to the column's
/// [`crate::types::Type`], and store the comparison operator. `AND` and `OR` nodes recurse.
///
/// For unknown columns in a leaf, the error's table name is the literal `"?"` (the AST node does
/// not carry the table name).
///
/// # Errors
///
/// Returns [`EngineError`] when a column name does not exist in `schema`, or when a literal
/// cannot be converted to the column's type.
fn resolve_where_clause(
    w: &WhereCondition,
    schema: &TupleSchema,
) -> Result<BooleanExpression, EngineError> {
    match w {
        WhereCondition::Predicate { field, op, value } => {
            let (idx, field_def) = schema
                .field_by_name(field)
                .ok_or_else(|| EngineError::column_not_found("?", field.clone()))?;
            let operand = Value::try_from((value, field_def.field_type))
                .map_err(|e| EngineError::type_error(e.to_string()))?;
            Ok(BooleanExpression::Leaf {
                col_index: idx,
                op: *op,
                operand,
            })
        }

        WhereCondition::And(left, right) => {
            let left = resolve_where_clause(left, schema)?;
            let right = resolve_where_clause(right, schema)?;
            Ok(BooleanExpression::And(Box::new(left), Box::new(right)))
        }

        WhereCondition::Or(left, right) => {
            let left = resolve_where_clause(left, schema)?;
            let right = resolve_where_clause(right, schema)?;
            Ok(BooleanExpression::Or(Box::new(left), Box::new(right)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::update::resolve_assignments;
    use super::*;
    use crate::primitives::Predicate;
    use crate::tuple::Field;
    use crate::types::Type;

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("name", Type::String),
        ])
    }

    fn leaf(field: &str, value: Value) -> WhereCondition {
        WhereCondition::Predicate {
            field: field.into(),
            op: Predicate::Equals,
            value,
        }
    }

    // --- resolve_assignments: happy path ---

    // maps each column name to its schema index, preserving assignment order
    #[test]
    fn test_resolve_assignments_maps_columns_to_indices() {
        let s = schema();
        let asgs = vec![
            Assignment {
                column: "name".into(),
                value: Value::String("x".into()),
            },
            Assignment {
                column: "id".into(),
                value: Value::Int64(1),
            },
        ];
        let out = resolve_assignments(&asgs, &s, "t").unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, 1);
        assert_eq!(out[1].0, 0);
    }

    // empty assignment list yields empty mapping (no error)
    #[test]
    fn test_resolve_assignments_empty() {
        let s = schema();
        let out = resolve_assignments(&[], &s, "t").unwrap();
        assert!(out.is_empty());
    }

    // duplicate column names are rejected (matches INSERT behavior)
    #[test]
    fn test_resolve_assignments_duplicate_column_rejected() {
        let s = schema();
        let asgs = vec![
            Assignment {
                column: "id".into(),
                value: Value::Int64(1),
            },
            Assignment {
                column: "id".into(),
                value: Value::Int64(2),
            },
        ];
        match resolve_assignments(&asgs, &s, "t").unwrap_err() {
            EngineError::DuplicateInsertColumn { table, column } => {
                assert_eq!(table, "t");
                assert_eq!(column, "id");
            }
            other => panic!("expected DuplicateInsertColumn, got {other:?}"),
        }
    }

    // --- resolve_assignments: error paths ---

    // unknown column surfaces ColumnNotFound carrying the table name
    #[test]
    fn test_resolve_assignments_unknown_column() {
        let s = schema();
        let asgs = vec![Assignment {
            column: "ghost".into(),
            value: Value::Int64(1),
        }];
        let err = resolve_assignments(&asgs, &s, "t").unwrap_err();
        match err {
            EngineError::ColumnNotFound { table, column } => {
                assert_eq!(table, "t");
                assert_eq!(column, "ghost");
            }
            other => panic!("expected ColumnNotFound, got {other:?}"),
        }
    }

    // a leaf predicate resolves to BooleanExpression::Leaf with the right index/op
    #[test]
    fn test_resolve_where_predicate_leaf() {
        let s = schema();
        let w = leaf("id", Value::Int64(5));
        match resolve_where_clause(&w, &s).unwrap() {
            BooleanExpression::Leaf {
                col_index,
                op,
                operand,
            } => {
                assert_eq!(col_index, 0);
                assert_eq!(op, Predicate::Equals);
                assert!(matches!(operand, Value::Int32(5)));
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    // And recurses into both sides
    #[test]
    fn test_resolve_where_and_recurses() {
        let s = schema();
        let w = WhereCondition::And(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        assert!(matches!(
            resolve_where_clause(&w, &s).unwrap(),
            BooleanExpression::And(_, _)
        ));
    }

    // Or recurses into both sides
    #[test]
    fn test_resolve_where_or_recurses() {
        let s = schema();
        let w = WhereCondition::Or(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        assert!(matches!(
            resolve_where_clause(&w, &s).unwrap(),
            BooleanExpression::Or(_, _)
        ));
    }

    // nested And inside Or fully resolves (no early return)
    #[test]
    fn test_resolve_where_nested() {
        let s = schema();
        let inner = WhereCondition::And(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        let outer = WhereCondition::Or(Box::new(inner), Box::new(leaf("id", Value::Int64(2))));
        assert!(matches!(
            resolve_where_clause(&outer, &s).unwrap(),
            BooleanExpression::Or(_, _)
        ));
    }

    // --- resolve_where_clause: error paths ---

    // unknown field in a leaf predicate -> ColumnNotFound (table is the "?" placeholder)
    #[test]
    fn test_resolve_where_unknown_field() {
        let s = schema();
        let w = leaf("ghost", Value::Int64(1));
        match resolve_where_clause(&w, &s).unwrap_err() {
            EngineError::ColumnNotFound { table, column } => {
                assert_eq!(table, "?");
                assert_eq!(column, "ghost");
            }
            other => panic!("expected ColumnNotFound, got {other:?}"),
        }
    }

    // type-incompatible literal -> TypeError from Value::try_from
    #[test]
    fn test_resolve_where_type_mismatch() {
        let s = schema();
        let w = leaf("id", Value::String("not a number".into()));
        let err = resolve_where_clause(&w, &s).unwrap_err();
        assert!(matches!(err, EngineError::TypeError(_)));
    }

    // failure in the LEFT branch of And short-circuits before evaluating RIGHT
    #[test]
    fn test_resolve_where_and_left_error_short_circuits() {
        let s = schema();
        let bad = leaf("ghost", Value::Int64(1));
        let ok = leaf("id", Value::Int64(2));
        assert!(
            resolve_where_clause(&WhereCondition::And(Box::new(bad), Box::new(ok)), &s).is_err()
        );
    }

    // failure in the RIGHT branch of Or still surfaces an error
    #[test]
    fn test_resolve_where_or_right_error_propagates() {
        let s = schema();
        let ok = leaf("id", Value::Int64(1));
        let bad = leaf("ghost", Value::Int64(2));
        assert!(
            resolve_where_clause(&WhereCondition::Or(Box::new(ok), Box::new(bad)), &s).is_err()
        );
    }

    // --- INSERT helpers ---
    mod insert_helpers {
        use super::super::insert::{materialize_insert_tuples, values_to_schema_projection};
        use super::*;

        fn nn_schema() -> TupleSchema {
            // (id Int32 NOT NULL, name String) — same field order as `schema()` but id is non-null
            TupleSchema::new(vec![
                Field::new("id", Type::Int32).not_null(),
                Field::new("name", Type::String),
            ])
        }

        // --- values_to_schema_projection ---

        // None columns -> identity projection [0..n]
        #[test]
        fn test_projection_identity_when_no_cols() {
            let s = schema();
            let p = values_to_schema_projection(None, &s, "t").unwrap();
            assert_eq!(p, vec![0, 1]);
        }

        // Reordered cols -> permutation matching schema order
        #[test]
        fn test_projection_reordered_cols() {
            let s = schema(); // schema is [id, name]
            let cols = vec!["name".to_string(), "id".to_string()];
            let p = values_to_schema_projection(Some(&cols), &s, "t").unwrap();
            // schema[0] = "id" -> input position 1; schema[1] = "name" -> input position 0
            assert_eq!(p, vec![1, 0]);
        }

        // Unknown column name -> ColumnNotFound
        #[test]
        fn test_projection_unknown_column() {
            let s = schema();
            let cols = vec!["id".to_string(), "ghost".to_string()];
            match values_to_schema_projection(Some(&cols), &s, "t").unwrap_err() {
                EngineError::ColumnNotFound { table, column } => {
                    assert_eq!(table, "t");
                    assert_eq!(column, "ghost");
                }
                other => panic!("expected ColumnNotFound, got {other:?}"),
            }
        }

        // Duplicate column name -> DuplicateInsertColumn
        #[test]
        fn test_projection_duplicate_column() {
            let s = schema();
            let cols = vec!["id".to_string(), "id".to_string()];
            match values_to_schema_projection(Some(&cols), &s, "t").unwrap_err() {
                EngineError::DuplicateInsertColumn { table, column } => {
                    assert_eq!(table, "t");
                    assert_eq!(column, "id");
                }
                other => panic!("expected DuplicateInsertColumn, got {other:?}"),
            }
        }

        // Too few columns named -> WrongColumnCount
        #[test]
        fn test_projection_missing_columns() {
            let s = schema();
            let cols = vec!["id".to_string()];
            match values_to_schema_projection(Some(&cols), &s, "t").unwrap_err() {
                EngineError::WrongColumnCount {
                    table,
                    expected,
                    got,
                } => {
                    assert_eq!(table, "t");
                    assert_eq!(expected, 2);
                    assert_eq!(got, 1);
                }
                other => panic!("expected WrongColumnCount, got {other:?}"),
            }
        }

        // --- materialize_insert_tuples ---

        // Happy path with reordered projection produces tuples in schema order
        #[test]
        fn test_materialize_reorders_via_projection() {
            let s = schema();
            let projection = vec![1, 0]; // schema [id, name] from input [name, id]
            let rows = vec![vec![Value::String("alice".into()), Value::Int64(7)]];
            let out = materialize_insert_tuples(rows, &s, "t", &projection).unwrap();
            assert_eq!(out.len(), 1);
            assert!(matches!(out[0].get(0), Some(Value::Int32(7))));
            assert!(matches!(out[0].get(1), Some(Value::String(s)) if s == "alice"));
        }

        // Row with too few values -> WrongColumnCount, no panic
        #[test]
        fn test_materialize_row_too_few_values() {
            let s = schema();
            let projection = vec![0, 1];
            let rows = vec![vec![Value::Int64(1)]];
            match materialize_insert_tuples(rows, &s, "t", &projection).unwrap_err() {
                EngineError::WrongColumnCount {
                    expected, got, ..
                } => {
                    assert_eq!(expected, 2);
                    assert_eq!(got, 1);
                }
                other => panic!("expected WrongColumnCount, got {other:?}"),
            }
        }

        // Row with too many values -> WrongColumnCount
        #[test]
        fn test_materialize_row_too_many_values() {
            let s = schema();
            let projection = vec![0, 1];
            let rows = vec![vec![
                Value::Int64(1),
                Value::String("x".into()),
                Value::Int64(99),
            ]];
            match materialize_insert_tuples(rows, &s, "t", &projection).unwrap_err() {
                EngineError::WrongColumnCount {
                    expected, got, ..
                } => {
                    assert_eq!(expected, 2);
                    assert_eq!(got, 3);
                }
                other => panic!("expected WrongColumnCount, got {other:?}"),
            }
        }

        // NULL into a nullable column is accepted and stored as Value::Null
        #[test]
        fn test_materialize_null_into_nullable_ok() {
            let s = schema(); // both fields nullable by default
            let projection = vec![0, 1];
            let rows = vec![vec![Value::Int64(1), Value::Null]];
            let out = materialize_insert_tuples(rows, &s, "t", &projection).unwrap();
            assert!(matches!(out[0].get(1), Some(Value::Null)));
        }

        // NULL into a NOT NULL column -> NullViolation
        #[test]
        fn test_materialize_null_into_not_null_rejected() {
            let s = nn_schema(); // id is NOT NULL
            let projection = vec![0, 1];
            let rows = vec![vec![Value::Null, Value::String("x".into())]];
            match materialize_insert_tuples(rows, &s, "t", &projection).unwrap_err() {
                EngineError::NullViolation { table, column } => {
                    assert_eq!(table, "t");
                    assert_eq!(column, "id");
                }
                other => panic!("expected NullViolation, got {other:?}"),
            }
        }

        // Int64 -> Int32 in range coerces successfully
        #[test]
        fn test_materialize_int_coercion_succeeds() {
            let s = schema();
            let projection = vec![0, 1];
            let rows = vec![vec![Value::Int64(42), Value::String("a".into())]];
            let out = materialize_insert_tuples(rows, &s, "t", &projection).unwrap();
            assert!(matches!(out[0].get(0), Some(Value::Int32(42))));
        }

        // String into Int32 column -> TypeError
        #[test]
        fn test_materialize_type_mismatch_rejected() {
            let s = schema();
            let projection = vec![0, 1];
            let rows = vec![vec![Value::String("not a number".into()), Value::Null]];
            assert!(matches!(
                materialize_insert_tuples(rows, &s, "t", &projection).unwrap_err(),
                EngineError::TypeError(_)
            ));
        }

        // Int64 outside Int32 range -> TypeError from coercion failure
        #[test]
        fn test_materialize_int_out_of_range_rejected() {
            let s = schema();
            let projection = vec![0, 1];
            let rows = vec![vec![
                Value::Int64(i64::from(i32::MAX) + 1),
                Value::String("x".into()),
            ]];
            assert!(matches!(
                materialize_insert_tuples(rows, &s, "t", &projection).unwrap_err(),
                EngineError::TypeError(_)
            ));
        }
    }
}
