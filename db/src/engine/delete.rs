//! Execution of `DELETE` statements.
//!
//! The entry point is [`Engine::exec_delete`], which resolves the target table,
//! optionally binds a `WHERE` predicate, and delegates row removal to
//! [`Engine::delete_rows_and_indexes`].
//!
//! ## Deletion order
//!
//! All rows that match the predicate are collected first (full heap scan), then
//! each matched row is processed in three steps:
//!
//! 1. **Inbound FK actions** — child tables that reference this table are consulted. Depending on
//!    the declared referential action the engine will error (`RESTRICT`), nullify the child column
//!    (`SET NULL`), or recursively delete the child row (`CASCADE`).
//! 2. **Heap deletion** — the tuple is marked deleted in the heap file.
//! 3. **Index cleanup** — every index entry that pointed at the deleted row is removed.
//!
//! The scan-then-act split means the predicate is evaluated against the
//! snapshot visible at the start of the statement, which avoids Halloween-
//! problem style anomalies where a row could be re-matched after being
//! partially modified by a cascaded action.

use crate::{
    FileId,
    catalog::manager::Catalog,
    engine::{Engine, EngineError, StatementResult},
    parser::statements::{DeleteStatement, Expr},
    transaction::Transaction,
};

impl Engine<'_> {
    /// Execute a `DELETE` statement and return the number of rows removed.
    ///
    /// When no `WHERE` clause is present every row in the table is deleted.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableNotFound`] if `stmt.table_name` does not exist in the catalog.
    /// - Constraint and storage errors propagated from [`Self::delete_rows_and_indexes`].
    pub(super) fn exec_delete(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        stmt: DeleteStatement,
    ) -> Result<StatementResult, EngineError> {
        let DeleteStatement {
            table_name,
            where_clause,
            ..
        } = stmt;

        let table_info = catalog.get_table_info(txn, table_name.as_str())?;
        tracing::debug!(table = %table_name, "exec delete");
        let file_id = table_info.file_id;

        let deleted = Self::delete_rows_and_indexes(catalog, txn, file_id, where_clause.as_ref())?;
        tracing::debug!(table = %table_name, rows_deleted = deleted, "delete complete");
        Ok(StatementResult::deleted(
            table_name.as_str().to_string(),
            deleted,
        ))
    }

    /// Delete every heap row matching `predicate` and clean up indexes and child references.
    ///
    /// Scans the heap for `file_id` and collects every row that satisfies
    /// `predicate` (or all rows when `predicate` is `None`). Then, for each
    /// matched row:
    ///
    /// 1. **Inbound FK actions** — calls [`Self::enforce_referential_actions_on_delete`] for every
    ///    child table that has a foreign key pointing at this table. The behaviors depends on the
    ///    child's declared referential action:
    ///    - `RESTRICT` / `NO ACTION` — returns a [`ConstraintViolation`] error.
    ///    - `CASCADE` — recursively deletes matching rows in the child table.
    ///    - `SET NULL` — nullifies the child's FK column(s) for matching rows.
    /// 2. **Heap deletion** — marks the tuple deleted via [`HeapFile::delete_tuple`].
    /// 3. **Index cleanup** — removes the deleted row's key from every index registered for
    ///    `file_id`.
    ///
    /// Returns the count of rows removed from *this* table only; rows removed
    /// by cascading deletes in child tables are not counted.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableNotFound`] or storage errors if the heap or an index cannot be
    ///   accessed.
    /// - [`EngineError::ConstraintViolation`] wrapping [`ConstraintViolation::ForeignKeyViolation`]
    ///   when a child table uses `RESTRICT` and a matching child row exists.
    pub(super) fn delete_rows_and_indexes(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
        predicate: Option<&Expr>,
    ) -> Result<usize, EngineError> {
        let tid = txn.transaction_id();
        let heap = catalog.get_table_heap(file_id)?;
        let schema = heap.schema().clone();
        let rows = Self::collect_matching_rows(&heap, tid, predicate, &schema)?;
        let inbound_checks = Self::prepare_inbound_ref_checks(catalog, txn, file_id)?;
        let indexes = catalog.indexes_for(file_id);

        let mut deleted = 0;
        for (rid, tuple) in rows {
            Self::enforce_referential_actions_on_delete(
                catalog,
                txn,
                &inbound_checks,
                &tuple,
                tid,
            )?;
            heap.delete_tuple(tid, rid)?;
            for index in &indexes {
                index.delete(tid, &tuple, rid)?;
            }
            deleted += 1;
        }
        Ok(deleted)
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
    fn delete_removes_index_entries_for_deleted_rows() {
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
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com'), (2, 'c@d.com');",
        );
        run(&engine, "DELETE FROM users WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let deleted_key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &deleted_key)
            .unwrap();
        assert!(
            miss.is_empty(),
            "index entry for deleted row should be gone, got {miss:?}"
        );

        let live_key = CompositeKey::single(Value::String("c@d.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &live_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "surviving row's index entry must remain");
    }

    #[test]
    fn delete_all_clears_every_index_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![field("k", Type::Int64).not_null()]),
                vec![],
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

    #[test]
    fn delete_on_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![field("x", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
        run(&engine, "DELETE FROM noidx WHERE x = 2;");
    }
}
