use crate::{
    catalog::manager::Catalog,
    engine::{Engine, EngineError, ShownIndex, StatementResult},
    parser::statements::ShowIndexesStatement,
    transaction::Transaction,
};

impl Engine<'_> {
    /// Executes `SHOW INDEXES [FROM table]` by reading catalog index metadata.
    ///
    /// With no `FROM` clause every index in the catalog is listed. With a
    /// `FROM <table>` clause the table is resolved and only its indexes are
    /// returned. The catalog stores index columns as ids; this function maps
    /// them back to the SQL column names in declaration order.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- SHOW INDEXES;
    /// ```
    ///
    /// ```ignore
    /// --   StatementResult::IndexesShown { scope: None, rows: [...all indexes...] }
    /// ```
    ///
    /// ```sql
    /// -- SHOW INDEXES FROM users;
    /// ```
    ///
    /// ```ignore
    /// --   StatementResult::IndexesShown { scope: Some("users"), rows: [ShownIndex { name: "users_age_idx", table: "users", ... }] }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::UnknownTable`] when `FROM <table>` names a table
    /// that is not in the catalog.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot list index
    /// metadata.
    pub(super) fn exec_show_indexes(
        txn: &Transaction<'_>,
        catalog: &Catalog,
        stmt: ShowIndexesStatement,
    ) -> Result<StatementResult, EngineError> {
        let (scope, infos) = match stmt.0 {
            None => (None, catalog.list_indexes(txn)?),
            Some(name) => {
                let table = Self::check_table(catalog, txn, name.as_str(), false)?
                    .expect("if_exists=false should never yield None");
                (
                    Some(table.name.as_str().to_string()),
                    catalog.list_indexes_for(txn, table.file_id)?,
                )
            }
        };

        let rows = infos
            .into_iter()
            .map(|i| ShownIndex {
                name: i.name,
                table: catalog
                    .table_name_by_id(i.table_id)
                    .unwrap_or_else(|| format!("<file {}>", i.table_id.0)),
                columns: i.columns,
                kind: i.kind,
            })
            .collect();

        Ok(StatementResult::indexes_shown(scope, rows))
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
        parser::statements::{ShowIndexesStatement, Statement},
        primitives::{ColumnId, NonEmptyString},
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

    fn make_users_with_email_index(catalog: &Catalog, txn_mgr: &TransactionManager) {
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
                &[ColumnId::try_from(1usize).unwrap()],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();
    }

    // SHOW INDEXES with no FROM lists every index across every table,
    // sorted by name. Each row carries the index name, resolved table name,
    // column list in declaration order, and access kind.
    #[test]
    fn test_show_indexes_lists_all_with_resolved_table_names() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_with_email_index(&catalog, &txn_mgr);

        // Second table with a composite index to exercise multi-column rendering.
        let txn = txn_mgr.begin().unwrap();
        let t_id = catalog
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
                "t_ba_idx",
                "t",
                t_id,
                &[
                    ColumnId::try_from(1usize).unwrap(),
                    ColumnId::try_from(0usize).unwrap(),
                ],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::ShowIndexes(ShowIndexesStatement(None)))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert!(scope.is_none());
        assert_eq!(rows.len(), 2);

        // Sorted by name: "t_ba_idx" < "users_email_idx".
        assert_eq!(rows[0].name, "t_ba_idx");
        assert_eq!(rows[0].table, "t");
        assert_eq!(rows[0].columns, vec!["b".to_string(), "a".to_string()]);
        assert_eq!(rows[0].kind, IndexKind::Hash);

        assert_eq!(rows[1].name, "users_email_idx");
        assert_eq!(rows[1].table, "users");
        assert_eq!(rows[1].columns, vec!["email".to_string()]);
    }

    // SHOW INDEXES FROM <table> filters to that table's indexes, populates
    // `scope` with the table name, and ignores indexes on other tables.
    #[test]
    fn test_show_indexes_from_table_filters_to_that_table() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_with_email_index(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let other_id = catalog
            .create_table(
                &txn,
                "other",
                TupleSchema::new(vec![field("k", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "other_k_idx",
                "other",
                other_id,
                &[ColumnId::try_from(0usize).unwrap()],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::ShowIndexes(ShowIndexesStatement(Some(
                "users".try_into().unwrap(),
            ))))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert_eq!(scope.as_deref(), Some("users"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "users_email_idx");
        assert_eq!(rows[0].table, "users");
    }

    // SHOW INDEXES on an empty catalog returns an empty list, not an error.
    #[test]
    fn test_show_indexes_empty_catalog_returns_no_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::ShowIndexes(ShowIndexesStatement(None)))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert!(scope.is_none());
        assert!(rows.is_empty());
    }

    // SHOW INDEXES FROM <table> on a table with no indexes returns an empty
    // list with scope still set to the table name.
    #[test]
    fn test_show_indexes_from_table_with_no_indexes_returns_empty() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "lonely",
                TupleSchema::new(vec![field("x", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::ShowIndexes(ShowIndexesStatement(Some(
                "lonely".try_into().unwrap(),
            ))))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert_eq!(scope.as_deref(), Some("lonely"));
        assert!(rows.is_empty());
    }

    // SHOW INDEXES FROM <missing-table> errors as UnknownTable.
    #[test]
    fn test_show_indexes_from_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::ShowIndexes(ShowIndexesStatement(Some(
                "ghost".try_into().unwrap(),
            ))))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableNotFound(ref n) if n == "ghost"),
            "expected TableNotFound, got: {err:?}"
        );
    }
}
