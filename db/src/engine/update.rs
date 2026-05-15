//! Execution of `UPDATE` statements.
//!
//! The entry point is [`Engine::exec_update`], which resolves the target table,
//! validates and coerces the `SET` assignments, and then applies changes row by
//! row with full constraint enforcement.
//!
//! ## Update pipeline
//!
//! 1. **Bind** — each `SET col = expr` is resolved against the schema: the column must exist, must
//!    not appear twice in the same statement, and the right-hand side must be a literal value
//!    (expression assignment is not yet supported).  The literal is coerced to the column's
//!    declared type.
//!
//! 2. **Scan** — the optional `WHERE` predicate is bound and evaluated against a full heap scan.
//!    All matching rows are collected before any row is mutated (see *Halloween problem* note
//!    below).
//!
//! 3. **Constraint prep** — the engine determines which indexes and FK constraints are actually
//!    relevant to the columns being changed, so that rows whose index keys or FK columns are
//!    untouched pay no extra cost.
//!
//! 4. **Per-row update** — for every matched row:
//!    - The new tuple is built by applying all assignments in-place.
//!    - UNIQUE constraints are checked: if the new index key already exists under a *different*
//!      row, the statement is rejected.
//!    - Outbound FK constraints are checked: the new child value must still have a matching parent
//!      row.
//!    - Inbound FK referential actions are enforced: child rows that reference the *old* parent key
//!      are handled via `CASCADE`, `SET NULL`, or `RESTRICT` depending on the constraint
//!      definition.
//!    - The tuple is written back to the heap.
//!    - Affected index entries are rotated: the old key is deleted and the new key is inserted.
//!      Unchanged keys are skipped.
//!
//! ## Halloween problem
//!
//! The full scan-then-act split (collect all matching rows first, then update
//! them) prevents the Halloween problem: without it, a row updated to a value
//! that still satisfies the `WHERE` predicate could be visited — and updated —
//! again in the same pass, leading to incorrect results or infinite loops on
//! index-driven scans.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::fk::{ParentFkCheck, RowChange};
use crate::{
    FileId, IndexId, TransactionId,
    catalog::{LiveIndex, manager::Catalog},
    engine::{
        ConstraintViolation, Engine, EngineError, StatementResult,
        fk::InboundParentFkCheck,
        scope::{ColumnResolver, SingleTableScope},
    },
    execution::eval,
    parser::statements::{Assignment, Expr, UpdateStatement},
    primitives::ColumnId,
    transaction::Transaction,
    tuple::{Tuple, TupleSchema},
};

impl Engine<'_> {
    /// Executes an `UPDATE` statement end-to-end.
    ///
    /// # Steps
    ///
    /// 1. **Bind** — resolve the table, validate each `SET` assignment (column exists, no
    ///    duplicates, literal values only), and coerce every value to the column's declared type.
    /// 2. **Scan** — apply the optional `WHERE` filter and collect all matching rows from the heap
    ///    file.
    /// 3. **Constraint prep** — determine which indexes and FK constraints are relevant to the
    ///    columns being changed.  Irrelevant constraints are skipped entirely so no unnecessary
    ///    catalog reads or index probes occur.
    /// 4. **Per-row update** — for every matching row:
    ///    - Apply the assignments to an in-memory tuple.
    ///    - Enforce UNIQUE constraints (index key must not already exist under a different row).
    ///    - Enforce outbound FK constraints (child values must still reference a valid parent row).
    ///    - Execute referential actions for inbound FKs (CASCADE / SET NULL on child rows that
    ///      reference the old parent key).
    ///    - Write the updated tuple back to the heap.
    ///    - Sync every affected index (`delete old key → insert new key`).
    ///
    /// # Errors
    ///
    /// Returns `EngineError::Constraint` for UNIQUE or FK violations.
    /// Propagates catalog and I/O errors from lower layers.
    pub(super) fn exec_update(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        stmt: UpdateStatement,
    ) -> Result<StatementResult, EngineError> {
        let UpdateStatement {
            table_name,
            alias,
            assignments,
            where_clause,
        } = stmt;

        let info = catalog.get_table_info(txn, table_name.as_str())?;
        tracing::debug!(table = %table_name, "exec update");
        let ai_col = info.auto_increment_column;
        let check_constraints = info.check_constraints.clone();
        let scope = SingleTableScope::from_info(info, alias);
        let heap_file = catalog.get_table_heap(scope.file_id)?;

        let filter = where_clause.map(|w| scope.bind_where(&w)).transpose()?;
        let rows = Self::collect_matching_rows(&heap_file, txn.transaction_id(), filter.as_ref())?;

        let assignments = Self::bind_assignments(&scope, assignments, ai_col)?;
        let affected_indexes =
            Self::get_affected_indices(assignments.as_slice(), catalog, scope.file_id);
        let unique_checks = if affected_indexes.is_empty() {
            HashMap::new()
        } else {
            Self::build_unique_check_map(catalog, txn, scope.file_id)?
        };

        let assignment_cols = assignments
            .iter()
            .map(|(c, _)| *c)
            .collect::<HashSet<ColumnId>>();

        let (outbound_fk_checks, inbound_checks) =
            Self::prepare_fk_checks_for_update(catalog, txn, scope.file_id, &assignment_cols)?;

        let txn_id = txn.transaction_id();
        let rows_matched = rows.len();
        let mut updated = 0;
        for (rid, old_tuple) in rows {
            let new_tuple = Self::build_updated_tuple(
                &old_tuple,
                &assignments,
                &scope.schema,
                scope.name.as_str(),
            )?;
            Self::check_tuple_constraints(
                &new_tuple,
                &scope.schema,
                &check_constraints,
                scope.name.as_str(),
            )?;

            let change = RowChange {
                before: &old_tuple,
                after: &new_tuple,
            };
            Self::check_unique_for_update(&affected_indexes, &unique_checks, txn_id, change)?;
            Self::check_outbound_fks_for_update(&outbound_fk_checks, change, txn_id)?;
            Self::enforce_referential_actions_on_update(&inbound_checks, change, txn_id)?;

            heap_file.update_tuple(txn_id, rid, &new_tuple)?;
            Self::sync_indexes_after_fk_action(&affected_indexes, txn_id, rid, change)?;
            updated += 1;
        }

        tracing::debug!(
            table = %scope.name,
            rows_matched,
            rows_updated = updated,
            "update complete"
        );
        Ok(StatementResult::updated(scope.name.as_str(), updated))
    }

    /// Returns only the indexes that must be maintained for this UPDATE.
    ///
    /// An index is considered "affected" when at least one of its key columns
    /// appears in the assignment set.  Indexes whose columns are all untouched
    /// are excluded so the per-row loop never performs unnecessary key compares
    /// or index writes.
    fn get_affected_indices(
        assignments: &[(ColumnId, Expr)],
        catalog: &Catalog,
        file_id: FileId,
    ) -> Vec<Arc<LiveIndex>> {
        let touched_cols = assignments
            .iter()
            .map(|(c, _)| *c)
            .collect::<HashSet<ColumnId>>();

        // We find the indexes that cover at least one column being updated —
        // indexes on untouched columns don't need to be maintained.
        catalog
            .indexes_for(file_id)
            .into_iter()
            .filter(|live| {
                live.table_columns
                    .iter()
                    .any(|col| touched_cols.contains(col))
            })
            .collect()
    }

    /// Validates a raw `SET` assignment list into `(ColumnId, Expr)` pairs.
    ///
    /// Walks each `SET col = expr` item and:
    /// - Resolves `col` against the table schema, returning an error if it does not exist.
    /// - Rejects the same column appearing twice (`SET a = 1, a = 2` is illegal).
    /// - Rejects updates to auto-increment columns.
    ///
    /// The RHS expression is kept unevaluated — type coercion and evaluation happen
    /// per-row at execution time against the pre-mutation tuple via [`eval_expr`].
    ///
    /// # Errors
    ///
    /// - [`EngineError::UnknownColumn`] if a column name is not found in the schema.
    /// - [`EngineError::DuplicateColumn`] if a column is targeted more than once.
    /// - [`EngineError::UpdateAutoIncrementColumn`] if an AI column is targeted.
    fn bind_assignments(
        scope: &SingleTableScope,
        assignments: Vec<Assignment>,
        ai_col: Option<ColumnId>,
    ) -> Result<Vec<(ColumnId, Expr)>, EngineError> {
        let mut seen: HashSet<ColumnId> = HashSet::with_capacity(assignments.len());
        let table_name = scope.name.as_str();

        assignments
            .into_iter()
            .map(|ass| {
                let column = ass.column.into_inner();
                let (col_id, _field) = Self::require_column(&scope.schema, table_name, &column)?;

                if ai_col == Some(col_id) {
                    return Err(EngineError::UpdateAutoIncrementColumn {
                        table: table_name.to_owned(),
                        column,
                    });
                }

                if seen.contains(&col_id) {
                    return Err(EngineError::DuplicateColumn {
                        table: table_name.to_string(),
                        column,
                    });
                }

                seen.insert(col_id);
                Ok((col_id, ass.value))
            })
            .collect()
    }

    /// Evaluates every SET expression against `old_tuple` and returns a new
    /// tuple with all assignments applied.
    ///
    /// All RHS expressions are evaluated first (against the pre-mutation row),
    /// then every value is written into the clone. This two-step approach is
    /// what makes `SET a = b, b = a` a correct swap — both reads see the old
    /// values regardless of the order the assignments appear in the statement.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TypeError`] if expression evaluation fails or the computed value cannot be
    ///   coerced to the column's declared type.
    fn build_updated_tuple(
        old_tuple: &Tuple,
        assignments: &[(ColumnId, Expr)],
        schema: &TupleSchema,
        table_name: &str,
    ) -> Result<Tuple, EngineError> {
        let computed = assignments
            .iter()
            .map(|(col_id, expr)| {
                let raw = eval::eval_expr(expr, old_tuple, schema)
                    .map_err(|e| EngineError::TypeError(e.to_string()))?;

                let field = schema
                    .field_of(*col_id)
                    .map_err(|e| EngineError::TypeError(e.to_string()))?;

                Self::fit_value_to_field(&raw, field, table_name)
            })
            .collect::<Result<Vec<_>, EngineError>>()?;

        let mut new_tuple = old_tuple.clone();
        for ((col_id, _), value) in assignments.iter().zip(&computed) {
            new_tuple
                .set_field(usize::from(*col_id), value.clone(), schema)
                .map_err(|e| EngineError::TypeError(e.to_string()))?;
        }
        Ok(new_tuple)
    }

    /// Builds the outbound and inbound FK check descriptors for this UPDATE,
    /// keeping only constraints that involve at least one assigned column.
    ///
    /// **Outbound** (child → parent): constraints declared on `file_id`'s table.
    /// Only those whose local FK columns overlap `assignment_cols` are kept —
    /// if none of the FK columns changed, the parent reference cannot be broken.
    ///
    /// **Inbound** (parent ← child): constraints where `file_id`'s table is the
    /// referenced parent.  Only those whose referenced columns overlap
    /// `assignment_cols` are kept — if the referenced key didn't change, no
    /// child row can be affected.
    ///
    /// Filtering here avoids spurious catalog/heap work in the per-row loop.
    fn prepare_fk_checks_for_update(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
        assignment_cols: &HashSet<ColumnId>,
    ) -> Result<(Vec<ParentFkCheck>, Vec<InboundParentFkCheck>), EngineError> {
        let contains_assignment_col = |col: ColumnId| assignment_cols.contains(&col);

        // Outbound FK checks: constraints declared on `file_id`'s table.
        // Only those whose local FK columns overlap `assignment_cols` are kept —
        // if none of the FK columns changed, the parent reference cannot be broken.
        let outbound = Self::prepare_outbound_ref_checks(catalog, txn, file_id)?
            .into_iter()
            .filter(|fk| {
                fk.local_ref_pairs
                    .iter()
                    .any(|&(local_col, _)| contains_assignment_col(local_col))
            })
            .collect::<Vec<_>>();

        // Inbound FK checks: constraints where `file_id`'s table is the
        // referenced parent. Only those whose referenced columns overlap
        // `assignment_cols` are kept — if the referenced key didn't change, no
        // child row can be affected.
        let inbound = Self::prepare_inbound_ref_checks(catalog, txn, file_id)?
            .into_iter()
            .filter(|fk| {
                fk.col_pairs
                    .iter()
                    .any(|&(_, ref_col)| contains_assignment_col(ref_col))
            })
            .collect();

        Ok((outbound, inbound))
    }

    /// Builds a `{ IndexId → constraint_name }` map for every UNIQUE constraint
    /// backed by one of the affected indexes.
    ///
    /// Only indexes that were already identified as "affected" (i.e. their key
    /// columns overlap the assignment set) are relevant.  When `affected_indexes`
    /// is empty this returns immediately with an empty map, avoiding an unnecessary
    /// catalog round-trip.
    ///
    /// The map is used in [`Self::check_unique_for_update`] to produce a
    /// human-readable constraint name in the error message.
    fn build_unique_check_map(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<HashMap<IndexId, String>, EngineError> {
        let table = catalog.get_table_info_by_id(txn, file_id)?;
        Ok(table
            .unique_constraints
            .into_iter()
            .filter_map(|uc| {
                uc.backing_index_id
                    .map(|id| (id, uc.name.as_str().to_owned()))
            })
            .collect())
    }

    /// Checks that the updated tuple does not violate any UNIQUE constraint.
    ///
    /// Iterates over `affected_indexes` and, for each one that backs a UNIQUE
    /// constraint, projects both the old and new index keys.  If the key
    /// changed *and* the new key already exists in the index, the update would
    /// create a duplicate — so an error is returned.
    ///
    /// The old-key comparison is the self-assign short-circuit: if
    /// `SET email = 'same@email.com'` produces the same key, the current row
    /// already occupies that slot legitimately and no conflict exists.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Constraint`] → [`ConstraintViolation::UniqueViolation`]
    /// with the constraint name when a duplicate key is detected.
    fn check_unique_for_update(
        affected_indexes: &[Arc<LiveIndex>],
        unique_checks: &HashMap<IndexId, String>,
        txn_id: TransactionId,
        change: RowChange<'_>,
    ) -> Result<(), EngineError> {
        for live in affected_indexes {
            let Some(constraint) = unique_checks.get(&live.index_id) else {
                continue;
            };
            let old_key = live.create_index_key(change.before)?;
            let new_key = live.create_index_key(change.after)?;
            if old_key != new_key && !live.access.search(txn_id, &new_key)?.is_empty() {
                return Err(ConstraintViolation::UniqueViolation {
                    constraint: constraint.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Checks that updating a child row does not break any outbound FK constraint.
    ///
    /// "Outbound" means `file_id`'s table holds the foreign key and references a
    /// parent table.  For each such FK:
    ///
    /// - If none of the FK's local columns changed value (`old == new`), the parent reference is
    ///   unchanged — skip it.
    /// - Otherwise, verify that the new FK value still has a matching row in the parent table via
    ///   [`Self::referenced_row_exists`].
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Constraint`] → [`ConstraintViolation::ForeignKeyViolation`]
    /// if the new child value references a parent row that does not exist.
    fn check_outbound_fks_for_update(
        outbound_checks: &[ParentFkCheck],
        change: RowChange<'_>,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for fk in outbound_checks {
            let cols_changed = fk.local_ref_pairs.iter().any(|&(local_col, _)| {
                change.before.get(usize::from(local_col))
                    != change.after.get(usize::from(local_col))
            });
            if cols_changed && !Self::referenced_row_exists(fk, change.after, tid)? {
                return Err(ConstraintViolation::ForeignKeyViolation {
                    constraint: fk.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::Engine,
        index::{CompositeKey, IndexKind},
        parser::Parser,
        primitives::{ColumnId, NonEmptyString},
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).unwrap());
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).unwrap();
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    fn col_id(i: usize) -> ColumnId {
        ColumnId::try_from(i).unwrap()
    }

    fn run(engine: &Engine<'_>, sql: &str) {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt).expect("execute");
    }

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

    #[test]
    fn update_changes_index_entry_when_indexed_column_changes() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_email_idx",
                "users",
                table_file_id,
                &[col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email) VALUES (1, 'old@b.com');",
        );
        run(
            &engine,
            "UPDATE users SET email = 'new@b.com' WHERE id = 1;",
        );

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let old_key = CompositeKey::single(Value::String("old@b.com".to_string()));
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &old_key)
            .unwrap();
        assert!(miss.is_empty(), "old key must no longer resolve");

        let new_key = CompositeKey::single(Value::String("new@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &new_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "new key must resolve to the updated row");
    }

    #[test]
    fn update_unrelated_column_keeps_index_consistent() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_email_idx",
                "users",
                table_file_id,
                &[col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com');",
        );
        run(&engine, "UPDATE users SET id = 99 WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "indexed column was untouched — entry must remain"
        );
    }

    #[test]
    fn update_on_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![
                    field("x", Type::Int64).not_null(),
                    field("y", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x, y) VALUES (1, 10), (2, 20);");
        run(&engine, "UPDATE noidx SET y = 999 WHERE x = 1;");
    }

    #[test]
    fn update_partial_composite_index_rotates_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("a", Type::Int64).not_null(),
                    field("b", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "t_ab_idx",
                "t",
                table_file_id,
                &[col_id(0), col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (a, b) VALUES (10, 20);");
        run(&engine, "UPDATE t SET b = 99 WHERE a = 10;");

        let live = catalog.get_index_by_name("t_ab_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let old_key = CompositeKey::new(vec![Value::Int64(10), Value::Int64(20)]);
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &old_key)
            .unwrap();
        assert!(miss.is_empty(), "old composite key must no longer resolve");

        let new_key = CompositeKey::new(vec![Value::Int64(10), Value::Int64(99)]);
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &new_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "rotated key must resolve to the row");
    }

    #[test]
    fn update_self_assign_indexed_column_keeps_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_email_idx",
                "users",
                table_file_id,
                &[col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com');",
        );
        run(&engine, "UPDATE users SET email = 'a@b.com' WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "self-assign must leave the index entry untouched and resolvable"
        );
    }

    #[test]
    fn update_only_touches_indexes_whose_columns_changed() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_email_idx",
                "users",
                table_file_id,
                &[col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_name_idx",
                "users",
                table_file_id,
                &[col_id(2)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email, name) VALUES (1, 'a@b.com', 'Ada');",
        );
        run(&engine, "UPDATE users SET name = 'Grace' WHERE id = 1;");

        let probe_txn = txn_mgr.begin().unwrap();

        let email_idx = catalog.get_index_by_name("users_email_idx").unwrap();
        let email_hits = email_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("a@b.com".to_string())),
            )
            .unwrap();
        assert_eq!(
            email_hits.len(),
            1,
            "email index entry must remain — its column wasn't touched"
        );

        let name_idx = catalog.get_index_by_name("users_name_idx").unwrap();
        let old_name_miss = name_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("Ada".to_string())),
            )
            .unwrap();
        assert!(old_name_miss.is_empty(), "old name key must miss");
        let new_name_hits = name_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("Grace".to_string())),
            )
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(new_name_hits.len(), 1, "new name key must hit");
    }

    /// Runs a SELECT and returns every row as a flat `Vec<Vec<Value>>`.
    fn select_rows(engine: &Engine<'_>, sql: &str) -> Vec<Vec<Value>> {
        use crate::engine::StatementResult;
        let stmt = Parser::new(sql).parse().expect("parse");
        match engine.execute_statement(stmt).expect("execute") {
            StatementResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|t| (0..t.len()).map(|i| t.get(i).unwrap().clone()).collect())
                .collect(),
            other => panic!("expected Selected, got {other:?}"),
        }
    }

    fn make_pair_table(dir: &Path) -> (Catalog, TransactionManager) {
        let (catalog, txn_mgr) = make_infra(dir);
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("a", Type::Int64).not_null(),
                    field("b", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();
        (catalog, txn_mgr)
    }

    #[test]
    fn update_set_column_reference_copies_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_pair_table(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO t (a, b) VALUES (5, 99);");
        run(&engine, "UPDATE t SET a = b;");

        let rows = select_rows(&engine, "SELECT a, b FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![Value::Int64(99), Value::Int64(99)],
            "a should take b's original value"
        );
    }

    #[test]
    fn update_swap_reads_pre_mutation_values() {
        // SET a = b, b = a must read both columns before any write happens.
        // With single-phase apply this would set both columns to old_b.
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_pair_table(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO t (a, b) VALUES (10, 20);");
        run(&engine, "UPDATE t SET a = b, b = a;");

        let rows = select_rows(&engine, "SELECT a, b FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![Value::Int64(20), Value::Int64(10)],
            "columns must be swapped using pre-mutation values"
        );
    }

    #[test]
    fn update_set_column_ref_applies_per_row() {
        // Each row evaluates the expression against its own pre-mutation values.
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_pair_table(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "INSERT INTO t (a, b) VALUES (1, 100), (2, 200), (3, 300);",
        );
        run(&engine, "UPDATE t SET a = b;");

        let mut rows = select_rows(&engine, "SELECT a, b FROM t");
        rows.sort_by_key(|r| match r[1] {
            Value::Int64(v) => v,
            _ => 0,
        });
        assert_eq!(rows[0], vec![Value::Int64(100), Value::Int64(100)]);
        assert_eq!(rows[1], vec![Value::Int64(200), Value::Int64(200)]);
        assert_eq!(rows[2], vec![Value::Int64(300), Value::Int64(300)]);
    }

    #[test]
    fn update_literal_still_works_after_refactor() {
        // Regression: plain literal SET must still work correctly.
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_pair_table(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO t (a, b) VALUES (1, 2);");
        run(&engine, "UPDATE t SET a = 42;");

        let rows = select_rows(&engine, "SELECT a, b FROM t");
        assert_eq!(rows[0], vec![Value::Int64(42), Value::Int64(2)]);
    }

    #[test]
    fn update_column_ref_with_where_only_touches_matching_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_pair_table(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO t (a, b) VALUES (1, 10), (2, 20);");
        run(&engine, "UPDATE t SET a = b WHERE a = 1;");

        let mut rows = select_rows(&engine, "SELECT a, b FROM t");
        rows.sort_by_key(|r| match r[1] {
            Value::Int64(v) => v,
            _ => 0,
        });
        assert_eq!(
            rows[0],
            vec![Value::Int64(10), Value::Int64(10)],
            "matched row: a takes b's value"
        );
        assert_eq!(
            rows[1],
            vec![Value::Int64(2), Value::Int64(20)],
            "unmatched row: unchanged"
        );
    }
}
