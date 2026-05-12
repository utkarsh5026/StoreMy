use crate::{
    catalog::{TableInfo, manager::Catalog},
    engine::{Engine, EngineError, StatementResult},
    parser::statements::DropStatement,
    transaction::Transaction,
};

impl Engine<'_> {
    /// Executes a `DROP TABLE` statement after binding the target table.
    ///
    /// Binding resolves `DROP TABLE users` to the catalog's canonical table name
    /// and heap [`crate::FileId`]. Execution performs the catalog drop, or returns
    /// the SQL no-op result for `DROP TABLE IF EXISTS` when the table is missing.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- DROP TABLE users;
    /// --   StatementResult::TableDropped { name: "users" }
    ///
    /// -- DROP TABLE IF EXISTS ghost;
    /// --   StatementResult::TableDropped { name: "ghost" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Bind`] when the table does not exist and the SQL
    /// did not include `IF EXISTS`.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot remove the table
    /// metadata or associated storage.
    pub(super) fn exec_drop_table(
        txn: &Transaction<'_>,
        catalog: &Catalog,
        statement: DropStatement,
    ) -> Result<StatementResult, EngineError> {
        let table_name = statement.table_name.into_inner();
        let result = match Self::check_table(catalog, txn, &table_name, statement.if_exists)? {
            Some(TableInfo { name, .. }) => {
                catalog.drop_table(txn, &name)?;
                StatementResult::table_dropped(name)
            }
            None => StatementResult::table_dropped(table_name),
        };

        Ok(result)
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
        parser::statements::{
            ColumnDef, CreateTableStatement, DropStatement, Statement, Uniqueness,
        },
        primitives::NonEmptyString,
        transaction::TransactionManager,
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

    fn col(name: &str, col_type: Type, nullable: bool, primary_key: bool) -> ColumnDef {
        ColumnDef {
            name: NonEmptyString::new(name).unwrap(),
            col_type,
            nullable,
            primary_key,
            auto_increment: false,
            default: None,
            unique: Uniqueness::NotUnique,
            check: None,
            references: None,
        }
    }

    fn statement_create(
        table: &str,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        primary_key: Option<&str>,
    ) -> CreateTableStatement {
        CreateTableStatement {
            table_name: table.try_into().unwrap(),
            if_not_exists,
            columns,
            primary_key: primary_key
                .into_iter()
                .map(|s| s.try_into().unwrap())
                .collect::<Vec<NonEmptyString>>(),
            constraints: vec![],
        }
    }

    #[test]
    fn test_drop_table_existing_returns_table_dropped() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_create = statement_create(
            "t1",
            false,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(stmt_create))
            .unwrap();

        let stmt_drop = DropStatement {
            table_name: "t1".try_into().unwrap(),
            if_exists: false,
        };
        let result = engine
            .execute_statement(Statement::Drop(stmt_drop))
            .unwrap();

        match result {
            StatementResult::TableDropped { name } => assert_eq!(name, "t1"),
            other => panic!("expected TableDropped, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_table_missing_without_if_exists_returns_err() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_drop = DropStatement {
            table_name: "ghost".try_into().unwrap(),
            if_exists: false,
        };
        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::Drop(stmt_drop))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownTable(_)),
            "expected UnknownTable, got: {err:?}"
        );
    }

    #[test]
    fn test_drop_table_missing_with_if_exists_returns_ok() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_drop = DropStatement {
            table_name: "ghost".try_into().unwrap(),
            if_exists: true,
        };
        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::Drop(stmt_drop))
            .unwrap();

        match result {
            StatementResult::TableDropped { name } => assert_eq!(name, "ghost"),
            other => panic!("expected TableDropped, got: {other:?}"),
        }
    }
}
