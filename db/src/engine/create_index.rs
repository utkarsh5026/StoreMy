use crate::{
    catalog::manager::Catalog,
    engine::{Engine, EngineError, StatementResult},
    parser::statements::CreateIndexStatement,
    transaction::Transaction,
};

impl Engine<'_> {
    /// Executes a `CREATE INDEX` statement after binding table and column names.
    ///
    /// For a fixed schema `users(id, name, age)` with resolved indices
    /// `id -> 0`, `name -> 1`, `age -> 2`, binding turns the SQL column list into
    /// ordered [`crate::primitives::ColumnId`] values. Execution stores the index
    /// metadata in the catalog; the index rows themselves are owned by the index
    /// subsystem.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- CREATE INDEX users_age_idx ON users(age) USING HASH;
    /// -- ```sql
    ///
    /// -- ```ignore
    /// --   StatementResult::IndexCreated { name: "users_age_idx", table: "users", already_exists: false }
    /// -- ```
    ///
    /// -- ```sql
    /// -- CREATE INDEX IF NOT EXISTS users_age_idx ON users(age) USING HASH;
    /// -- ```sql
    ///
    /// -- ```ignore
    /// --   StatementResult::IndexCreated { name: "users_age_idx", table: "", already_exists: true }
    /// -- ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TableNotFound`] when the target table is unknown.
    /// Returns [`EngineError::IndexAlreadyExists`] when the index name already exists
    /// without `IF NOT EXISTS`. Returns [`EngineError::UnknownColumn`] when an indexed
    /// column is unknown, or [`EngineError::DuplicateColumn`] when the same column
    /// appears more than once in the index column list.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot persist the index
    /// metadata.
    pub(super) fn exec_create_index(
        txn: &Transaction<'_>,
        catalog: &Catalog,
        stmt: CreateIndexStatement,
    ) -> Result<StatementResult, EngineError> {
        let table = Self::check_table(catalog, txn, &stmt.table_name, false)?
            .expect("if_exists=false should never yield None");

        let index_name = stmt.index_name.into_inner();

        if catalog.get_index_by_name(&index_name).is_some() {
            if stmt.if_not_exists {
                return Ok(StatementResult::index_created(index_name, "", true));
            }
            return Err(EngineError::IndexAlreadyExists(index_name));
        }

        let column_indices =
            Self::resolve_column_ids(&table.schema, table.name.as_str(), stmt.columns.iter())?;

        catalog.create_index(
            txn,
            &index_name,
            &table.name,
            table.file_id,
            &column_indices,
            stmt.index_type,
        )?;

        Ok(StatementResult::index_created(
            index_name, table.name, false,
        ))
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
        parser::statements::{CreateIndexStatement, Statement},
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
        if_not_exists: bool,
        kind: IndexKind,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: NonEmptyString::new(index_name).unwrap(),
            table_name: NonEmptyString::new(table_name).unwrap(),
            columns: columns
                .iter()
                .map(|c| NonEmptyString::new(*c).unwrap())
                .collect(),
            if_not_exists,
            index_type: kind,
        }
    }

    #[test]
    fn test_create_index_fresh_returns_index_created() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                false,
                IndexKind::Hash,
            )))
            .unwrap();

        match result {
            StatementResult::IndexCreated {
                name,
                table,
                already_exists,
            } => {
                assert_eq!(name, "users_email_idx");
                assert_eq!(table, "users");
                assert!(!already_exists);
            }
            other => panic!("expected IndexCreated, got: {other:?}"),
        }
    }

    #[test]
    fn test_create_index_composite_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_age_idx",
                "users",
                &["email", "age"],
                false,
                IndexKind::Btree,
            )))
            .unwrap();

        match result {
            StatementResult::IndexCreated {
                name,
                already_exists,
                ..
            } => {
                assert_eq!(name, "users_email_age_idx");
                assert!(!already_exists);
            }
            other => panic!("expected IndexCreated, got: {other:?}"),
        }
    }

    #[test]
    fn test_create_index_if_not_exists_on_existing_returns_already_exists() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                false,
                IndexKind::Hash,
            )))
            .unwrap();

        let result = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                true,
                IndexKind::Hash,
            )))
            .unwrap();

        match result {
            StatementResult::IndexCreated { already_exists, .. } => {
                assert!(already_exists);
            }
            other => panic!("expected IndexCreated, got: {other:?}"),
        }
    }

    #[test]
    fn test_create_index_duplicate_without_if_not_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                false,
                IndexKind::Hash,
            )))
            .unwrap();

        let err = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_email_idx",
                "users",
                &["email"],
                false,
                IndexKind::Hash,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::IndexAlreadyExists(_)),
            "expected IndexAlreadyExists, got: {err:?}"
        );
    }

    #[test]
    fn test_create_index_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "ghost_idx",
                "ghost",
                &["id"],
                false,
                IndexKind::Hash,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableNotFound(_)),
            "expected TableNotFound, got: {err:?}"
        );
    }

    #[test]
    fn test_create_index_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_nope_idx",
                "users",
                &["nope"],
                false,
                IndexKind::Hash,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownColumn { .. }),
            "expected UnknownColumn, got: {err:?}"
        );
    }

    #[test]
    fn test_create_index_duplicate_column_in_list_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::CreateIndex(create_index_stmt(
                "users_dup_idx",
                "users",
                &["email", "email"],
                false,
                IndexKind::Hash,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::DuplicateColumn { .. }),
            "expected DuplicateColumn, got: {err:?}"
        );
    }
}
