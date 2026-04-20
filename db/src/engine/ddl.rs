//! DDL execution helpers for the SQL engine.
//!
//! This module turns parsed DDL statements (currently `CREATE TABLE` and
//! `DROP TABLE`) into catalog operations wrapped in a transaction via
//! [`super::with_txn`].
//!
//! The functions here are intentionally small and delegate most work to the
//! catalog. They are responsible for:
//! - Implementing `IF NOT EXISTS` / `IF EXISTS` behavior.
//! - Translating a parsed column list into a [`TupleSchema`].
//! - Resolving a primary-key declaration into column indices when possible.

pub(super) mod create_table {
    use crate::{
        binder::{Bound, BoundCreateTable},
        catalog::manager::Catalog,
        engine::{EngineError, StatementResult, bind_and_execute},
        parser::statements::Statement,
        transaction::{Transaction, TransactionManager},
    };

    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: Statement,
    ) -> Result<StatementResult, EngineError> {
        bind_and_execute(
            catalog,
            txn_manager,
            statement,
            |catalog, bound, txn| match bound {
                Bound::CreateTable(b) => execute_create_table(catalog, b, txn),
                _ => unreachable!("binder returned non-CreateTable variant for CreateTable input"),
            },
        )
    }

    fn execute_create_table(
        catalog: &Catalog,
        bound: BoundCreateTable,
        txn: &Transaction<'_>,
    ) -> Result<StatementResult, EngineError> {
        match bound {
            BoundCreateTable::AlreadyExists { name, file_id } => {
                Ok(StatementResult::table_created(name, file_id, true))
            }
            BoundCreateTable::New {
                name,
                schema,
                primary_key,
            } => {
                let file_id = catalog.create_table(txn, &name, schema, primary_key)?;
                Ok(StatementResult::table_created(name, file_id, false))
            }
        }
    }
}

pub(super) mod drop_table {
    use crate::{
        binder::{Bound, BoundDrop},
        catalog::manager::Catalog,
        engine::{EngineError, StatementResult, bind_and_execute},
        parser::statements::{DropStatement, Statement},
        transaction::{Transaction, TransactionManager},
    };

    pub fn execute(
        catalog: &Catalog,
        txn_manager: &TransactionManager,
        statement: DropStatement,
    ) -> Result<StatementResult, EngineError> {
        bind_and_execute(
            catalog,
            txn_manager,
            Statement::Drop(statement),
            |catalog, bound, txn| match bound {
                Bound::Drop(b) => execute_drop_table(catalog, b, txn),
                _ => unreachable!("binder returned non-Drop variant for Drop input"),
            },
        )
    }

    fn execute_drop_table(
        catalog: &Catalog,
        bound: BoundDrop,
        txn: &Transaction<'_>,
    ) -> Result<StatementResult, EngineError> {
        match bound {
            BoundDrop::NoOp { name } => Ok(StatementResult::table_dropped(name)),
            BoundDrop::Drop { name, .. } => {
                catalog.drop_table(txn, &name)?;
                Ok(StatementResult::table_dropped(name))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Type,
        binder::BindError,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{EngineError, StatementResult},
        parser::statements::{ColumnDef, CreateTableStatement, DropStatement},
        transaction::TransactionManager,
        tuple::TupleSchema,
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
            name: name.to_string(),
            col_type,
            nullable,
            primary_key,
            auto_increment: false,
            default: None,
        }
    }

    fn statement_create(
        table: &str,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        primary_key: Option<&str>,
    ) -> CreateTableStatement {
        CreateTableStatement {
            table_name: table.to_string(),
            if_not_exists,
            columns,
            primary_key: primary_key.map(std::string::ToString::to_string),
        }
    }

    // --- happy path ---
    // create_table returns TableCreated with already_exists=false for fresh table.
    #[test]
    fn test_create_table_fresh_returns_table_created() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt = statement_create(
            "users",
            false,
            vec![
                col("id", Type::Uint64, false, false),
                col("name", Type::String, true, false),
            ],
            None,
        );

        let result = create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap();

        match result {
            StatementResult::TableCreated {
                name,
                file_id: _,
                already_exists,
            } => {
                assert_eq!(name, "users");
                assert!(!already_exists);
            }
            other => panic!("expected TableCreated, got: {other:?}"),
        }

        assert!(dir.path().join("users.dat").exists());
    }

    // drop_table returns TableDropped after dropping an existing table.
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
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt_create),
        )
        .unwrap();

        let stmt_drop = DropStatement {
            table_name: "t1".to_string(),
            if_exists: false,
        };
        let result = drop_table::execute(&catalog, &txn_mgr, stmt_drop).unwrap();

        match result {
            StatementResult::TableDropped { name } => assert_eq!(name, "t1"),
            other => panic!("expected TableDropped, got: {other:?}"),
        }
    }

    // --- edge cases ---
    // IF NOT EXISTS on existing table returns already_exists=true and same file_id.
    #[test]
    fn test_create_table_if_not_exists_existing_returns_already_exists_true_and_same_file_id() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_create = statement_create(
            "dup",
            false,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt_create),
        )
        .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let existing_info = catalog.get_table_info(&txn, "dup").unwrap();
        txn.commit().unwrap();

        let stmt_if_not_exists = statement_create(
            "dup",
            true,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        let result = create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt_if_not_exists),
        )
        .unwrap();

        match result {
            StatementResult::TableCreated {
                name,
                file_id,
                already_exists,
            } => {
                assert_eq!(name, "dup");
                assert!(already_exists);
                assert_eq!(file_id, existing_info.file_id);
            }
            other => panic!("expected TableCreated, got: {other:?}"),
        }
    }

    // Inline PRIMARY KEY on a column should be used when present.
    #[test]
    fn test_create_table_inline_primary_key_sets_primary_key_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt = statement_create(
            "pk_inline",
            false,
            vec![
                col("id", Type::Uint64, true, true),
                col("name", Type::String, true, false),
            ],
            None,
        );
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_inline").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![0]));
    }

    // If both inline PK and table-level PK exist, inline wins (due to Option::or).
    #[test]
    fn test_create_table_inline_primary_key_takes_precedence_over_table_level_primary_key() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt = statement_create(
            "pk_precedence",
            false,
            vec![
                col("id", Type::Uint64, false, true),
                col("name", Type::String, false, false),
            ],
            Some("name"),
        );
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_precedence").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![0]));
    }

    // Table-level primary key should be used when no inline PK exists.
    #[test]
    fn test_create_table_table_level_primary_key_used_when_no_inline_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt = statement_create(
            "pk_table_level",
            false,
            vec![
                col("id", Type::Uint64, false, false),
                col("k", Type::Int64, false, false),
            ],
            Some("k"),
        );
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_table_level").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![1]));
    }

    // If the primary key name doesn't exist in schema, the binder rejects it.
    #[test]
    fn test_create_table_primary_key_name_missing_creates_table_without_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt = statement_create(
            "pk_missing",
            false,
            vec![col("id", Type::Uint64, false, false)],
            Some("nope"),
        );
        let err = create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap_err();

        assert!(
            matches!(err, EngineError::Bind(BindError::PrimaryKeyNotInColumns(_))),
            "expected Bind(PrimaryKeyNotInColumns), got: {err:?}"
        );
    }

    // --- error paths ---
    // Creating a duplicate table without IF NOT EXISTS should error.
    #[test]
    fn test_create_table_duplicate_without_if_not_exists_returns_err() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt1 = statement_create(
            "dup_err",
            false,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt1),
        )
        .unwrap();

        let stmt2 = statement_create(
            "dup_err",
            false,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        let err = create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt2),
        )
        .unwrap_err();

        assert!(
            matches!(err, EngineError::Bind(BindError::TableAlreadyExists(_))),
            "expected Bind(TableAlreadyExists), got: {err:?}"
        );
    }

    // Dropping a missing table without IF EXISTS should error.
    #[test]
    fn test_drop_table_missing_without_if_exists_returns_err() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_drop = DropStatement {
            table_name: "ghost".to_string(),
            if_exists: false,
        };
        let err = drop_table::execute(&catalog, &txn_mgr, stmt_drop).unwrap_err();

        assert!(
            matches!(err, EngineError::Bind(BindError::UnknownTable(_))),
            "expected Bind(UnknownTable), got: {err:?}"
        );
    }

    // IF EXISTS should suppress the missing-table error and return Ok(TableDropped).
    #[test]
    fn test_drop_table_missing_with_if_exists_returns_ok() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let stmt_drop = DropStatement {
            table_name: "ghost".to_string(),
            if_exists: true,
        };
        let result = drop_table::execute(&catalog, &txn_mgr, stmt_drop).unwrap();

        match result {
            StatementResult::TableDropped { name } => assert_eq!(name, "ghost"),
            other => panic!("expected TableDropped, got: {other:?}"),
        }
    }

    // --- property / invariant tests ---
    // create_table should store schema consistent with TupleSchema::from(ColumnDef list).
    #[test]
    fn test_create_table_schema_matches_tuple_schema_from_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let cols = vec![
            col("id", Type::Uint64, false, false),
            col("name", Type::String, true, false),
        ];
        let expected_schema = TupleSchema::from(cols.iter().collect::<Vec<&ColumnDef>>());

        let stmt = CreateTableStatement {
            table_name: "schema_check".to_string(),
            if_not_exists: false,
            columns: cols,
            primary_key: None,
        };
        create_table::execute(
            &catalog,
            &txn_mgr,
            crate::parser::statements::Statement::CreateTable(stmt),
        )
        .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "schema_check").unwrap();
        txn.commit().unwrap();

        let expected_names: Vec<_> = expected_schema.fields().map(|f| f.name.as_str()).collect();
        let actual_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(actual_names, expected_names);
    }
}
