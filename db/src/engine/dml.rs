//! Row-changing SQL (`INSERT`, `DELETE`, `UPDATE`).
//!
//! This module is the SQL → storage bridge for DML statements. The binder produces a
//! fully-resolved `Bound*` shape (column indices, coerced values, optional `WHERE` predicate),
//! and these executor entry points apply the requested changes inside a short transaction
//! callback, maintaining any registered secondary indexes.
//!
//! ## Shape
//!
//! - [`Engine::exec_insert`] — `INSERT INTO … VALUES …`
//! - [`Engine::exec_delete`] — `DELETE FROM … [WHERE …]`
//! - [`Engine::exec_update`] — `UPDATE … SET … [WHERE …]`
//!
//! ## How it works
//!
//! - **INSERT** evaluates each row's value expressions into a [`Tuple`], bulk-inserts into the
//!   table heap, then inserts `(key, rid)` pairs into every registered index for the table.
//! - **DELETE** scans for qualifying rows (optional `WHERE` predicate), deletes the heap tuples,
//!   then deletes the corresponding keys from every index.
//! - **UPDATE** scans for qualifying rows, applies the assignment list to each tuple, writes the
//!   updated tuple back to the heap, then maintains indexes whose covered columns were touched.
//!
//! ## NULL semantics
//!
//! Predicates in `WHERE` are evaluated by
//! [`BooleanExpression`](crate::execution::expression::BooleanExpression): any `NULL` in a
//! comparison short-circuits to `false`, so rows with `NULL` keys do not qualify.

use std::collections::HashSet;

use crate::{
    binder::{Bound, BoundDelete, BoundExpr, BoundInsert, BoundUpdate},
    engine::{Engine, EngineError, StatementResult, collect_matching},
    parser::statements::{Statement, UpdateStatement},
    tuple::Tuple,
};

impl Engine<'_> {
    /// Executes `INSERT INTO <table> …` by inserting tuples into the heap and updating indexes.
    ///
    /// The binder has already resolved the target table and coerced values to the table schema.
    /// This executor step evaluates each row's expressions into a [`Tuple`], bulk-inserts those
    /// tuples into the backing heap, and (if any indexes exist) inserts one `(key, rid)` entry per
    /// index per inserted row.
    ///
    /// # SQL examples
    ///
    /// Assume a fixed schema throughout:
    ///
    /// ```sql
    /// -- users(id, email) where the binder resolves: id → 0, email → 1
    /// ```
    ///
    /// ```sql
    /// -- 1. Single-row insert
    /// --
    /// --   INSERT INTO users VALUES (1, 'a@x');
    /// --
    /// --   BoundInsert {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       rows: [
    /// --           [Expr::Literal(1), Expr::Literal("a@x")],
    /// --       ],
    /// --       ..
    /// --   }
    /// ```
    ///
    /// ```sql
    /// -- 2. Multi-row insert
    /// --
    /// --   INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x');
    /// --
    /// --   BoundInsert { rows: [ ..two rows.. ], .. }
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates heap insertion and index maintenance failures (I/O errors, type errors while
    /// forming index keys, or access-method insert failures).
    pub(super) fn exec_insert(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::Insert(BoundInsert {
                name,
                file_id,
                rows,
                ..
            }) = bound
            else {
                unreachable!("binder returned non-Insert variant for Insert input");
            };

            let heap = catalog.get_table_heap(file_id)?;
            let tuples = rows
                .iter()
                .map(|row| row.iter().map(BoundExpr::eval).collect())
                .map(Tuple::new)
                .collect::<Vec<_>>();

            let indexes = catalog.indexes_for(file_id);
            let tid = txn.transaction_id();
            if indexes.is_empty() {
                let rids = heap.bulk_insert(tid, tuples)?;
                return Ok(StatementResult::inserted(name, rids.len()));
            }

            let rids = heap.bulk_insert(tid, tuples.iter().cloned())?;
            rids.iter()
                .zip(tuples.iter())
                .try_for_each(|(rid, tuple)| {
                    indexes
                        .iter()
                        .try_for_each(|index| index.insert(tid, tuple, *rid))
                })?;
            Ok(StatementResult::inserted(name, rids.len()))
        })
    }

    /// Executes `DELETE FROM <table> [WHERE <predicate>]` and updates indexes.
    ///
    /// The binder turns the optional `WHERE` into a
    /// [`BooleanExpression`](crate::execution::expression::BooleanExpression) tree evaluated over
    /// each table tuple. This method collects all matching `(rid, tuple)` pairs, deletes the heap
    /// tuples, then deletes the corresponding keys from every registered index.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- 1. Unconditional delete (delete all rows)
    /// --
    /// --   DELETE FROM users;
    /// --
    /// --   BoundDelete { name: "users", file_id: <resolved>, filter: None }
    /// ```
    ///
    /// ```sql
    /// -- 2. Conditional delete
    /// --
    /// --   DELETE FROM users WHERE id = 1;
    /// --
    /// --   BoundDelete {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       filter: Some(BooleanExpression::Leaf { Column(0), Equals, Literal(1) }),
    /// --   }
    /// ```
    ///
    /// # NULL handling
    ///
    /// If the predicate evaluates to `false` due to `NULL` short-circuiting, the row is not
    /// deleted (matching `WHERE` behavior).
    pub(super) fn exec_delete(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::Delete(BoundDelete {
                name,
                file_id,
                filter,
            }) = bound
            else {
                unreachable!("binder returned non-Delete variant for Delete input");
            };

            let tid = txn.transaction_id();
            let heap = catalog.get_table_heap(file_id)?;
            let predicate = filter.as_ref();
            let rows = collect_matching(&heap, tid, predicate)?;
            let deleted = rows.len();

            let indexes = catalog.indexes_for(file_id);
            for (rid, tuple) in &rows {
                heap.delete_tuple(tid, *rid)?;
                for index in &indexes {
                    index.delete(tid, tuple, *rid)?;
                }
            }

            Ok(StatementResult::deleted(name, deleted))
        })
    }

    /// Executes `UPDATE <table> SET … [WHERE <predicate>]` and maintains affected indexes.
    ///
    /// The binder resolves each assignment's target column to an index in the table schema and
    /// evaluates the assignment expressions against the *old* row to produce new values. This
    /// executor step:
    ///
    /// - Collects all rows matching the optional `WHERE` predicate.
    /// - Applies the assignment list to each tuple and writes it back to the heap.
    /// - Updates indexes whose covered columns intersect the assignment set.
    ///
    /// Index maintenance is pruned in two ways:
    ///
    /// - **Statement-level pruning:** if none of an index's `table_columns` appear in the
    ///   assignments, the index is skipped for every row.
    /// - **Row-level pruning:** even for affected indexes, if the projected key is unchanged for a
    ///   particular row, the delete+insert pair is skipped.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- 1. Unconditional update
    /// --
    /// --   UPDATE users SET email = 'x@y';
    /// --
    /// --   BoundUpdate {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       assignments: [(1, Value::String("x@y"))],
    /// --       filter: None,
    /// --       ..
    /// --   }
    /// ```
    ///
    /// ```sql
    /// -- 2. Conditional update
    /// --
    /// --   UPDATE users SET email = 'x@y' WHERE id = 1;
    /// --
    /// --   BoundUpdate { assignments: [(1, ..)], filter: Some(BooleanExpression::Leaf { .. }), .. }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a type error if applying an assignment violates the table schema (e.g. wrong type
    /// for the target column). Propagates heap update and index maintenance failures.
    pub(super) fn exec_update(
        &self,
        statement: UpdateStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::Update(statement), |catalog, bound, txn| {
            let Bound::Update(BoundUpdate {
                name,
                file_id,
                schema,
                assignments,
                filter,
            }) = bound
            else {
                unreachable!("binder returned non-Update variant for Update input");
            };

            let heap_file = catalog.get_table_heap(file_id)?;
            let txn_id = txn.transaction_id();
            let rows = collect_matching(&heap_file, txn_id, filter.as_ref())?;
            let updated = rows.len();

            // Statement-level pruning: an index only needs maintenance if at
            // least one of its covered columns appears in the assignments. We
            // compute this once here so the per-row loop skips uninvolved
            // indexes entirely (no clone, no key projection, no I/O).
            let touched: HashSet<usize> = assignments.iter().map(|(c, _)| *c).collect();
            let affected_indexes = catalog
                .indexes_for(file_id)
                .into_iter()
                .filter(|live| {
                    live.table_columns
                        .iter()
                        .any(|c| touched.contains(&usize::from(*c)))
                })
                .collect::<Vec<_>>();
            // The old tuple is only needed for affected indexes' old-key
            // computation; if none are affected we skip the clone entirely.
            let needs_old = !affected_indexes.is_empty();

            for (rid, mut tuple) in rows {
                let old_tuple = needs_old.then(|| tuple.clone());
                for (idx, value) in &assignments {
                    tuple
                        .set_field(*idx, value.clone(), &schema)
                        .map_err(|e| EngineError::type_error(e.to_string()))?;
                }
                heap_file.update_tuple(txn_id, rid, &tuple)?;

                if let Some(old) = &old_tuple {
                    // Per-row pruning: even when an index is affected at the
                    // statement level, this particular row's projected key
                    // might be unchanged (e.g. SET email = LOWER(email) when
                    // already lowercase). Skip the delete+insert pair in that
                    // case so the index stays at rest.
                    for live in &affected_indexes {
                        let old_key = live.create_index_key(old)?;
                        let new_key = live.create_index_key(&tuple)?;
                        if old_key != new_key {
                            live.access.delete(txn_id, &old_key, rid)?;
                            live.access.insert(txn_id, &new_key, rid)?;
                        }
                    }
                }
            }

            Ok(StatementResult::updated(name, updated))
        })
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
        primitives::ColumnId,
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

    // After INSERT, every registered index on the table contains the new
    // (key, rid) pair: index search by key returns exactly the record id
    // the heap insert produced.
    #[test]
    fn insert_updates_single_column_hash_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        // users(id INT64, email STRING).
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                ]),
                None,
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
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com'), (2, 'c@d.com');",
        );

        // After commit, both rows must be visible from the index.
        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "exactly one rid for unique email");

        // Sanity: the rid the index returned actually points into the user
        // table's heap (file_id matches).
        assert_eq!(hits[0].file_id, table_file_id);
    }

    // Composite indexes pull values from declaration order — search must
    // return hits keyed on (col_b, col_a), not (col_a, col_b).
    #[test]
    fn insert_updates_composite_hash_index_in_declaration_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    Field::new("a", Type::Int64).not_null(),
                    Field::new("b", Type::Int64).not_null(),
                ]),
                None,
            )
            .unwrap();
        // Index on (b, a) — declaration order matters.
        catalog
            .create_index(
                &txn,
                "t_ba_idx",
                "t",
                table_file_id,
                &[col_id(1), col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (a, b) VALUES (10, 20);");

        let live = catalog.get_index_by_name("t_ba_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        // Correct order (b, a) = (20, 10) → hit.
        let key_ok = CompositeKey::new(vec![Value::Int64(20), Value::Int64(10)]);
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key_ok)
            .unwrap();
        assert_eq!(hits.len(), 1);

        // Swapped (a, b) = (10, 20) → no hit.
        let key_swapped = CompositeKey::new(vec![Value::Int64(10), Value::Int64(20)]);
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &key_swapped)
            .unwrap();
        probe_txn.commit().unwrap();
        assert!(miss.is_empty(), "swapped key must not match");
    }

    // INSERT into a table with no indexes still works — the no-index path
    // skips the projection loop entirely.
    #[test]
    fn insert_into_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![Field::new("x", Type::Int64).not_null()]),
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
        // No assertion needed beyond "this didn't panic or error" — the
        // smoke test is that the no-index branch runs.
    }

    // After DELETE, the deleted rows' index entries are gone; surviving
    // rows still resolve via the index.
    #[test]
    fn delete_removes_index_entries_for_deleted_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                ]),
                None,
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
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com'), (2, 'c@d.com');",
        );
        run(&engine, "DELETE FROM users WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        // Deleted row's key — must miss.
        let deleted_key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &deleted_key)
            .unwrap();
        assert!(
            miss.is_empty(),
            "index entry for deleted row should be gone, got {miss:?}"
        );

        // Surviving row's key — still hits.
        let live_key = CompositeKey::single(Value::String("c@d.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &live_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "surviving row should still be in the index");
    }

    // Bulk delete — all rows wiped, all corresponding index entries gone.
    #[test]
    fn delete_all_clears_every_index_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![Field::new("k", Type::Int64).not_null()]),
                None,
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "t_k_idx",
                "t",
                table_file_id,
                &[col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (k) VALUES (10), (20), (30);");
        run(&engine, "DELETE FROM t;");

        let live = catalog.get_index_by_name("t_k_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        for k in [10i64, 20, 30] {
            let key = CompositeKey::single(Value::Int64(k));
            let hits = live
                .access
                .search(probe_txn.transaction_id(), &key)
                .unwrap();
            assert!(hits.is_empty(), "index entry for k={k} should be gone");
        }
        probe_txn.commit().unwrap();
    }

    // After UPDATE on an indexed column, the old key must not resolve and
    // the new key must — i.e. the index entry has actually moved.
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
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                ]),
                None,
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

    // UPDATE that only touches a non-indexed column still re-issues delete +
    // insert against every index (v1 isn't optimized to skip), but the
    // resulting index state must be correct: the indexed key still hits.
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
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                ]),
                None,
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
        // Update only `id`; email is untouched.
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

    // UPDATE on a table without indexes uses the lean path: no clone, no
    // fan-out. Smoke test that it still applies assignments correctly.
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
                    Field::new("x", Type::Int64).not_null(),
                    Field::new("y", Type::Int64).not_null(),
                ]),
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x, y) VALUES (1, 10), (2, 20);");
        run(&engine, "UPDATE noidx SET y = 999 WHERE x = 1;");
    }

    // Updating only one column of a composite (a, b) index must still
    // rotate the index entry — the new key has the changed component, the
    // old key must miss.
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
                    Field::new("a", Type::Int64).not_null(),
                    Field::new("b", Type::Int64).not_null(),
                ]),
                None,
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

    // Update assigning the same value to an indexed column — projected key
    // is unchanged. The result must still resolve via the indexed key
    // (the per-row equality skip is invisible from outside).
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
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                ]),
                None,
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
        // Assigning the same literal — projected key on `email` doesn't change.
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

    // Two indexes, one touched and one not, on the same table — UPDATE
    // touching only the second index's columns must:
    // - leave the first index's entries intact (unchanged keys still hit), AND
    // - rotate the second index's entries correctly.
    // This is the integration test for statement-level pruning.
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
                    Field::new("id", Type::Int64).not_null(),
                    Field::new("email", Type::String).not_null(),
                    Field::new("name", Type::String).not_null(),
                ]),
                None,
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
        // Update name only — email index must remain at rest.
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

    // DELETE on a table without indexes — pure heap path, no fan-out.
    #[test]
    fn delete_on_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![Field::new("x", Type::Int64).not_null()]),
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
        run(&engine, "DELETE FROM noidx WHERE x = 2;");
    }
}
