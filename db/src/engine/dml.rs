//! Runs row-changing SQL (`INSERT`, `DELETE`, `UPDATE`) on behalf of the storage engine.
//!
//! Each operation lives in its own submodule (`insert`, `delete`, `update`), exposing a single
//! `execute` entry point. Helpers shared across statement kinds
//! ([`collect_matching`](super::collect_matching),
//! [`resolve_where_clause`](super::resolve_where_clause)) live in the parent [`engine`](super)
//! module so both DML and query paths can reuse them.
//!
//! Parsed statements are turned into heap operations inside a short transaction callback.
//! Column names are matched to the table schema, optional `WHERE` clauses become
//! [`BooleanExpression`](crate::execution::unary::BooleanExpression) trees, and matching rows
//! are read or updated through the catalog's [`HeapFile`](crate::heap::file::HeapFile).

use crate::{
    Value,
    catalog::manager::Catalog,
    engine::EngineError,
    parser::statements::Assignment,
    tuple::{Tuple, TupleSchema},
};

pub(super) mod insert {
    use super::{Catalog, EngineError, Tuple};
    use crate::{
        binder::{Bound, BoundInsert},
        engine::StatementResult,
        parser::statements::Statement,
        transaction::{Transaction, TransactionManager},
    };

    /// Runs a pre-bound INSERT: evaluates each row's expressions into a
    /// [`Tuple`] and bulk-inserts into the backing heap. All column resolution,
    /// value coercion, and NOT NULL checks happened in the binder.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] when binding fails, the heap cannot be opened,
    /// or insertion fails at the storage layer.
    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: Statement,
    ) -> Result<StatementResult, EngineError> {
        let f = |catalog: &Catalog,
                 bound: Bound,
                 txn: &Transaction<'_>|
         -> Result<StatementResult, EngineError> {
            match bound {
                Bound::Insert(b) => execute_insert(catalog, b, txn),
                _ => unreachable!("binder returned non-Insert variant for Insert input"),
            }
        };

        crate::engine::bind_and_execute(catalog, txn_manager, statement, f)
    }

    fn execute_insert(
        catalog: &Catalog,
        bound: BoundInsert,
        txn: &Transaction<'_>,
    ) -> Result<StatementResult, EngineError> {
        let BoundInsert {
            name,
            file_id,
            rows,
            ..
        } = bound;

        let heap = catalog.get_table_heap(file_id)?;
        let tuples: Vec<Tuple> = rows
            .into_iter()
            .map(|row| Tuple::new(row.into_iter().map(|e| e.eval()).collect()))
            .collect();

        let record_ids = heap.bulk_insert(txn.transaction_id(), tuples)?;
        Ok(StatementResult::inserted(name, record_ids.len()))
    }
}

pub(super) mod delete {
    use super::{Catalog, EngineError};
    use crate::{
        binder::{Bound, BoundDelete},
        engine::{StatementResult, collect_matching},
        parser::statements::Statement,
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
        statement: Statement,
    ) -> Result<StatementResult, EngineError> {
        let f = |catalog: &Catalog,
                 bound: Bound,
                 txn: &Transaction<'_>|
         -> Result<StatementResult, EngineError> {
            match bound {
                Bound::Delete(b) => execute_delete(catalog, b, txn),
                _ => unreachable!("binder returned non-Delete variant for Delete input"),
            }
        };

        crate::engine::bind_and_execute(catalog, txn_manager, statement, f)
    }

    fn execute_delete(
        catalog: &Catalog,
        bound: BoundDelete,
        txn: &Transaction<'_>,
    ) -> Result<StatementResult, EngineError> {
        let BoundDelete {
            name,
            file_id,
            filter,
        } = bound;
        let tid = txn.transaction_id();
        let heap = catalog.get_table_heap(file_id)?;
        let predicate = filter.as_ref();

        let deleted = collect_matching(&heap, tid, predicate).and_then(|rids| {
            let deleted = rids.len();
            for (rid, _) in &rids {
                heap.delete_tuple(tid, *rid)?;
            }
            Ok(StatementResult::deleted(name, deleted))
        })?;
        Ok(deleted)
    }
}

pub(super) mod update {
    use std::collections::HashSet;

    use super::{Assignment, Catalog, EngineError, TupleSchema, Value};
    use crate::{
        catalog::manager::TableInfo,
        engine::{StatementResult, collect_matching, resolve_where_clause},
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

#[cfg(test)]
mod tests {
    use super::{update::resolve_assignments, *};
    use crate::{tuple::Field, types::Type};

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("name", Type::String),
        ])
    }

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
}
