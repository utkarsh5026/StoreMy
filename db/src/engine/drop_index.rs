use crate::{
    catalog::manager::Catalog,
    engine::{Engine, EngineError, StatementResult},
    parser::statements::DropIndexStatement,
    transaction::Transaction,
};

impl Engine<'_> {
    /// Executes a `DROP INDEX` statement after resolving index ownership.
    ///
    /// Looks up the index by name, confirms it belongs to the `ON <table>`
    /// clause, then removes it from the catalog. For `DROP INDEX IF EXISTS`,
    /// a missing or mismatched index is silently treated as a no-op.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- DROP INDEX users_age_idx ON users;
    /// --   StatementResult::IndexDropped { name: "users_age_idx" }
    ///
    /// -- DROP INDEX IF EXISTS ghost_idx ON users;
    /// --   StatementResult::IndexDropped { name: "ghost_idx" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::UnknownIndex`] when the index does not exist
    /// (without `IF EXISTS`), or exists but belongs to a different table.
    ///
    /// Returns [`EngineError::UnknownTable`] when the `ON <table>` clause
    /// names a table that is not in the catalog.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot remove the
    /// index metadata.
    pub(super) fn exec_drop_index(
        txn: &Transaction<'_>,
        catalog: &Catalog,
        stmt: DropIndexStatement,
    ) -> Result<StatementResult, EngineError> {
        let index_name = stmt.index_name.into_inner();
        tracing::debug!(index = %index_name, "drop index");

        if let Some(index) = catalog.get_index_by_name(&index_name) {
            let Some(table_name) = stmt.table_name.as_ref() else {
                return Err(EngineError::UnknownIndex(index_name));
            };

            let table = Self::check_table(catalog, txn, table_name.as_str(), false)?
                .expect("if_exists=false should never yield None");

            if catalog.index_belongs_to_table(table.file_id, &index) {
                catalog.drop_index(txn, &index_name)?;
                return Ok(StatementResult::index_dropped(index_name));
            }

            if stmt.if_exists {
                return Ok(StatementResult::index_dropped(index_name));
            }
            return Err(EngineError::UnknownIndex(index_name));
        }

        if stmt.if_exists {
            return Ok(StatementResult::index_dropped(index_name));
        }
        Err(EngineError::UnknownIndex(index_name))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        Type,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        index::IndexKind,
        parser::statements::{CreateIndexStatement, DropIndexStatement, Statement},
        primitives::NonEmptyString,
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Arc<Wal>, Arc<PageStore>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        (wal, bp)
    }

    fn make_catalog_and_txn(dir: &Path) -> (Catalog, TransactionManager) {
        let (wal, bp) = make_infra(dir);
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

    fn make_users_table(catalog: &Catalog, txn_mgr: &TransactionManager) {
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                    field("age", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();
    }

    fn create_index_stmt(
        index_name: &str,
        table_name: &str,
        columns: &[&str],
        kind: IndexKind,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: NonEmptyString::new(index_name).unwrap(),
            table_name: NonEmptyString::new(table_name).unwrap(),
            columns: columns
                .iter()
                .map(|c| NonEmptyString::new(*c).unwrap())
                .collect(),
            if_not_exists: false,
            index_type: kind,
        }
    }

    fn drop_index_stmt(
        index_name: &str,
        table_name: Option<&str>,
        if_exists: bool,
    ) -> DropIndexStatement {
        DropIndexStatement {
            index_name: NonEmptyString::new(index_name).unwrap(),
            table_name: table_name.map(|t| NonEmptyString::new(t).unwrap()),
            if_exists,
        }
    }

    #[test]
    fn test_drop_index_existing_returns_index_dropped() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                IndexKind::Hash,
            )))
            .unwrap();

        let result = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "users_email_idx",
                Some("users"),
                false,
            )))
            .unwrap();

        match result {
            StatementResult::IndexDropped { name } => assert_eq!(name, "users_email_idx"),
            other => panic!("expected IndexDropped, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_index_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "ghost_idx",
                Some("users"),
                false,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownIndex(_)),
            "expected UnknownIndex, got: {err:?}"
        );
    }

    #[test]
    fn test_drop_index_if_exists_on_missing_returns_index_dropped() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "ghost_idx",
                Some("users"),
                true,
            )))
            .unwrap();

        match result {
            StatementResult::IndexDropped { name } => assert_eq!(name, "ghost_idx"),
            other => panic!("expected IndexDropped, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_index_wrong_table_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "orders",
                TupleSchema::new(vec![field("id", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "orders_id_idx",
                "orders",
                &["id"],
                IndexKind::Hash,
            )))
            .unwrap();

        let err = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "orders_id_idx",
                Some("users"),
                false,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownIndex(_)),
            "expected UnknownIndex, got: {err:?}"
        );
    }

    #[test]
    fn test_drop_index_wrong_table_with_if_exists_returns_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "orders",
                TupleSchema::new(vec![field("id", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "orders_id_idx",
                "orders",
                &["id"],
                IndexKind::Hash,
            )))
            .unwrap();

        let result = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "orders_id_idx",
                Some("users"),
                true,
            )))
            .unwrap();

        match result {
            StatementResult::IndexDropped { name } => assert_eq!(name, "orders_id_idx"),
            other => panic!("expected IndexDropped, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_index_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                IndexKind::Hash,
            )))
            .unwrap();

        let err = engine
            .execute_statement(Statement::DropIndex(drop_index_stmt(
                "users_email_idx",
                Some("ghost_table"),
                false,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableNotFound(_)),
            "expected TableNotFound, got: {err:?}"
        );
    }
}
