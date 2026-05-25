use crate::{
    FileId,
    catalog::{CachedCheckConstraint, CatalogError, ConstraintDef, TableInfo, manager::Catalog},
    engine::{Engine, EngineError, StatementResult},
    index::IndexKind,
    parser::statements::{AlterAction, AlterTableStatement, TableConstraint},
    transaction::ActiveTransaction,
    tuple::TupleSchema,
};

impl Engine<'_> {
    /// Executes an `ALTER TABLE` statement after resolving names against the catalog.
    ///
    /// All binding is done inline: column names are resolved, the target table is
    /// looked up, and constraint columns are validated before any catalog mutation.
    /// When `IF EXISTS` is set and the table is missing the whole statement becomes
    /// a silent no-op.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- ALTER TABLE users RENAME TO accounts;
    /// --   StatementResult::TableRenamed { old_name: "users", new_name: "accounts" }
    ///
    /// -- ALTER TABLE users ADD COLUMN age INT64;
    /// --   StatementResult::ColumnAdded { table: "users", column_name: "age" }
    ///
    /// -- ALTER TABLE IF EXISTS ghost ADD COLUMN x INT64;
    /// --   StatementResult::NoOp { statement: "ALTER TABLE IF EXISTS ghost" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::UnknownTable`] when the table is missing without `IF EXISTS`.
    ///
    /// Returns [`EngineError::UnknownColumn`] when a referenced column does not exist.
    ///
    /// Returns [`EngineError::DuplicateColumn`] when `ADD COLUMN` or `RENAME COLUMN TO`
    /// targets a name already present in the schema.
    ///
    /// Returns [`EngineError::TableAlreadyExists`] when `RENAME TO` targets a name
    /// that is already registered.
    ///
    /// Returns [`EngineError::PrimaryKeyAlreadyExists`] when `ADD PRIMARY KEY` is used
    /// on a table that already has a primary key.
    ///
    /// Returns [`EngineError::Catalog`] on catalog write failures.
    #[allow(clippy::too_many_lines)]
    pub(super) fn exec_alter_table(
        txn: &ActiveTransaction<'_>,
        catalog: &Catalog,
        stmt: AlterTableStatement,
    ) -> Result<StatementResult, EngineError> {
        let table_name = stmt.table_name.into_inner();

        let Some(table_info) = Self::check_table(catalog, txn, &table_name, stmt.if_exists)? else {
            return Ok(StatementResult::NoOp {
                statement: format!("ALTER TABLE IF EXISTS {table_name}"),
            });
        };

        let TableInfo {
            file_id,
            ref schema,
            ..
        } = table_info;

        let operation = match &stmt.action {
            AlterAction::RenameTable { .. } => "rename_table",
            AlterAction::RenameColumn { .. } => "rename_column",
            AlterAction::AddColumn(_) => "add_column",
            AlterAction::DropColumn { .. } => "drop_column",
            AlterAction::SetDefault { .. } => "set_default",
            AlterAction::DropDefault { .. } => "drop_default",
            AlterAction::DropNotNull { .. } => "drop_not_null",
            AlterAction::AddPrimaryKey { .. } => "add_primary_key",
            AlterAction::DropPrimaryKey => "drop_primary_key",
            AlterAction::DropConstraint { .. } => "drop_constraint",
            AlterAction::AddConstraint { .. } => "add_constraint",
            AlterAction::ValidateConstraint { .. } => "validate_constraint",
        };
        tracing::debug!(table = %table_name, operation, "alter table");

        match stmt.action {
            // ALTER TABLE users RENAME TO accounts;
            AlterAction::RenameTable { to } => {
                let old_name = table_name;
                let new_name = to.into_inner();

                if catalog.table_exists(&new_name) {
                    return Err(EngineError::TableAlreadyExists(new_name));
                }

                catalog.rename_table(txn, &old_name, &new_name)?;
                Ok(StatementResult::TableRenamed { old_name, new_name })
            }

            // ALTER TABLE users RENAME COLUMN name TO full_name;
            AlterAction::RenameColumn { from, to } => {
                let old_name = from.into_inner();
                let new_name = to.into_inner();

                Self::require_column(schema, &table_name, old_name.as_str())?;
                Self::assure_no_duplicate_col(schema, &table_name, new_name.as_str())?;

                catalog.rename_column(txn, file_id, &old_name, &new_name)?;
                Ok(StatementResult::ColumnRenamed {
                    table: table_name,
                    old_name,
                    new_name,
                })
            }

            // ALTER TABLE users ADD COLUMN email TEXT NOT NULL;
            AlterAction::AddColumn(col_def) => {
                let column_name = col_def.name.as_str().to_owned();
                Self::assure_no_duplicate_col(schema, &table_name, &column_name)?;
                catalog.add_column(txn, file_id, col_def)?;
                Ok(StatementResult::ColumnAdded {
                    table: table_name,
                    column_name,
                })
            }

            // ALTER TABLE users DROP COLUMN age;
            // ALTER TABLE users DROP COLUMN IF EXISTS age;
            AlterAction::DropColumn { name, if_exists } => {
                let column_name = name.into_inner();

                if schema.field_by_name(&column_name).is_none() {
                    if if_exists {
                        Ok(StatementResult::NoOp {
                            statement: format!(
                                "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}"
                            ),
                        })
                    } else {
                        Err(EngineError::UnknownColumn {
                            table: table_name,
                            column: column_name,
                        })
                    }
                } else {
                    catalog.drop_column(txn, file_id, &column_name)?;
                    Ok(StatementResult::ColumnDropped {
                        table: table_name,
                        column_name,
                    })
                }
            }

            // ALTER TABLE users ALTER COLUMN name SET DEFAULT 'anon';
            AlterAction::SetDefault { column, value } => {
                let column_name = column.into_inner();

                Self::require_column(schema, &table_name, &column_name)?;
                catalog.set_column_default(txn, file_id, &column_name, value)?;
                Ok(StatementResult::ColumnDefaultSet {
                    table: table_name,
                    column: column_name,
                })
            }

            // ALTER TABLE users ALTER COLUMN name DROP DEFAULT;
            AlterAction::DropDefault { column } => {
                let column_name = column.into_inner();
                Self::require_column(schema, &table_name, &column_name)?;
                catalog.drop_column_default(txn, file_id, &column_name)?;
                Ok(StatementResult::ColumnDefaultDropped {
                    table: table_name,
                    column: column_name,
                })
            }

            // ALTER TABLE users ALTER COLUMN name DROP NOT NULL;
            AlterAction::DropNotNull { column } => {
                let column_name = column.into_inner();
                Self::require_column(schema, &table_name, &column_name)?;
                catalog.drop_column_not_null(txn, file_id, &column_name)?;
                Ok(StatementResult::ColumnNotNullDropped {
                    table: table_name,
                    column: column_name,
                })
            }

            // ALTER TABLE users ADD PRIMARY KEY (id);
            AlterAction::AddPrimaryKey { columns } => {
                if table_info.primary_key.is_some() {
                    return Err(EngineError::PrimaryKeyAlreadyExists(table_name));
                }

                let column_ids = Self::resolve_column_ids(schema, &table_name, columns.iter())?;
                catalog.set_primary_key(txn, file_id, column_ids)?;
                Ok(StatementResult::PrimaryKeySet { table: table_name })
            }

            // ALTER TABLE users DROP PRIMARY KEY;
            AlterAction::DropPrimaryKey => {
                catalog.drop_primary_key(txn, file_id)?;
                Ok(StatementResult::PrimaryKeyDropped { table: table_name })
            }

            // ALTER TABLE users DROP CONSTRAINT fk_orders;
            // ALTER TABLE users DROP CONSTRAINT IF EXISTS fk_orders;
            AlterAction::DropConstraint { name, if_exists } => {
                let n = name.as_str();
                match catalog.drop_constraint(txn, file_id, n) {
                    Ok(()) => Ok(StatementResult::constraint_dropped(table_name, n)),
                    Err(CatalogError::ConstraintNotFound { .. }) if if_exists => {
                        Ok(StatementResult::NoOp {
                            statement: format!(
                                "ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {n}"
                            ),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }

            // ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email);
            // ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users
            // (id);
            AlterAction::AddConstraint {
                name,
                constraint,
                not_valid,
            } => {
                let constraint_name =
                    Self::resolve_constraint_name(name.as_ref(), &table_name, &constraint);
                match constraint {
                    TableConstraint::Unique { columns } => {
                        let column_ids =
                            Self::resolve_column_ids(schema, &table_name, columns.iter())?;
                        let index_name = format!("{table_name}_{constraint_name}_idx");

                        let index_id = catalog.create_index(
                            txn,
                            &index_name,
                            &table_name,
                            file_id,
                            &column_ids,
                            IndexKind::Btree,
                        )?;
                        catalog.add_constraint(txn, file_id, ConstraintDef::Unique {
                            name: constraint_name.clone(),
                            columns: column_ids,
                            backing_index_id: Some(index_id),
                        })?;
                        Self::populate_index_from_heap(catalog, txn, file_id, &index_name)?;

                        Ok(StatementResult::unique_constraint_added(
                            table_name,
                            constraint_name,
                            index_name,
                        ))
                    }

                    TableConstraint::Check { expr } => {
                        if not_valid {
                            catalog.add_constraint(txn, file_id, ConstraintDef::Check {
                                name: constraint_name.clone(),
                                expr: expr.to_string(),
                                validated: false,
                            })?;
                        } else {
                            let cached =
                                CachedCheckConstraint::new(constraint_name.clone(), expr, true);
                            let resolved = Self::resolve_check_constraints(
                                schema,
                                std::slice::from_ref(&cached),
                            )?;

                            let heap = catalog.get_table_heap(file_id)?;
                            let tid = txn.transaction_id();
                            let mut scan = heap.scan(tid)?;
                            while let Some((_rid, tuple)) =
                                fallible_iterator::FallibleIterator::next(&mut scan)?
                            {
                                Self::check_tuple_constraints(&tuple, &resolved, &table_name)?;
                            }

                            catalog.add_constraint(txn, file_id, ConstraintDef::Check {
                                name: constraint_name.clone(),
                                expr: cached.expr.to_string(),
                                validated: true,
                            })?;
                        }

                        Ok(StatementResult::check_constraint_added(
                            table_name,
                            constraint_name,
                            not_valid,
                        ))
                    }

                    TableConstraint::ForeignKey { .. } => Err(EngineError::Unsupported(
                        "ALTER TABLE ADD CONSTRAINT: only UNIQUE and CHECK are supported"
                            .to_string(),
                    )),
                }
            }

            AlterAction::ValidateConstraint { name } => {
                let constraint_name = name.as_str();

                let cached = table_info.get_check_constraint(constraint_name)?;

                let resolved = Self::resolve_check_constraints(schema, &[cached])?;
                let heap = catalog.get_table_heap(file_id)?;
                let mut scan = heap.scan(txn.transaction_id())?;
                while let Some((_rid, tuple)) =
                    fallible_iterator::FallibleIterator::next(&mut scan)?
                {
                    Self::check_tuple_constraints(&tuple, &resolved, &table_name)?;
                }

                catalog.mark_constraint_validated(txn, file_id, constraint_name)?;
                Ok(StatementResult::constraint_validated(
                    table_name,
                    constraint_name,
                ))
            }
        }
    }

    /// Rejects `ALTER TABLE` column operations that would introduce a duplicate name.
    ///
    /// Used before `ADD COLUMN` and `RENAME COLUMN ... TO ...` to ensure the target
    /// name is not already present in the table's logical schema.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::DuplicateColumn`] when `column_name` already exists on
    /// `table_name`.
    fn assure_no_duplicate_col(
        schema: &TupleSchema,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), EngineError> {
        if schema.field_by_name(column_name).is_some() {
            return Err(EngineError::DuplicateColumn {
                table: table_name.to_string(),
                column: column_name.to_string(),
            });
        }
        Ok(())
    }

    /// Backfills a newly created UNIQUE backing index from every live heap row.
    ///
    /// After `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE`, the engine creates an empty
    /// btree index and then calls this to scan the table once. Each tuple is keyed and
    /// inserted; if the index already holds that key, the scan stops — two existing rows
    /// with the same key means the constraint cannot be added.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::IndexNameNotFound`] if `index_name` is not in the catalog.
    /// - [`CatalogError::InvalidCatalogRow`] when existing data contains duplicate keys.
    /// - Other [`CatalogError`] variants from heap or index access during the scan.
    fn populate_index_from_heap(
        catalog: &Catalog,
        txn: &ActiveTransaction<'_>,
        table_file_id: FileId,
        index_name: &str,
    ) -> Result<(), CatalogError> {
        use fallible_iterator::FallibleIterator;

        let heap = catalog.get_table_heap(table_file_id)?;
        let live = catalog
            .get_index_by_name(index_name)
            .ok_or_else(|| CatalogError::IndexNameNotFound(index_name.to_string()))?;
        let tid = txn.transaction_id();
        let mut scan = heap.scan(tid)?;
        while let Some((rid, tuple)) = FallibleIterator::next(&mut scan)? {
            let key = live.create_index_key(&tuple)?;
            let hits = live.access.search(tid, &key)?;
            if !hits.is_empty() {
                return Err(CatalogError::invalid_catalog_row(format!(
                    "UNIQUE constraint violated while building index '{index_name}': duplicate key in existing table data"
                )));
            }
            live.insert(tid, &tuple, rid)?;
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
        engine::{Engine, EngineError, StatementResult},
        parser::{
            Parser,
            statements::{
                AlterAction, AlterTableStatement, ColumnDef, Expr, Statement, TableConstraint,
                Uniqueness,
            },
        },
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
                    field("name", Type::String).not_null(),
                    field("age", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();
    }

    fn alter_stmt(table_name: &str, if_exists: bool, action: AlterAction) -> AlterTableStatement {
        AlterTableStatement {
            table_name: NonEmptyString::new(table_name).unwrap(),
            if_exists,
            action,
        }
    }

    fn col_def(name: &str, col_type: Type) -> ColumnDef {
        ColumnDef {
            name: NonEmptyString::new(name).unwrap(),
            col_type,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            default: None,
            unique: Uniqueness::NotUnique,
            check: None,
            references: None,
        }
    }

    // IF EXISTS on a missing table is a silent no-op.
    #[test]
    fn test_alter_table_if_exists_on_missing_table_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "ghost",
                true,
                AlterAction::DropPrimaryKey,
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::NoOp { .. }),
            "expected NoOp, got: {result:?}"
        );
    }

    // Missing table without IF EXISTS is an error.
    #[test]
    fn test_alter_table_missing_table_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "ghost",
                false,
                AlterAction::DropPrimaryKey,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableNotFound(_)),
            "expected TableNotFound, got: {err:?}"
        );
    }

    // RENAME TO changes the table's catalog name.
    #[test]
    fn test_alter_table_rename_table_renames_it() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::RenameTable {
                    to: "accounts".try_into().unwrap(),
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::TableRenamed { ref old_name, ref new_name } if old_name == "users" && new_name == "accounts"),
            "expected TableRenamed, got: {result:?}"
        );
    }

    // RENAME TO a name already in the catalog errors.
    #[test]
    fn test_alter_table_rename_table_to_existing_name_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "accounts",
                TupleSchema::new(vec![field("id", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::RenameTable {
                    to: "accounts".try_into().unwrap(),
                },
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableAlreadyExists(_)),
            "expected TableAlreadyExists, got: {err:?}"
        );
    }

    // ADD COLUMN appends a new column to the schema.
    #[test]
    fn test_alter_table_add_column_adds_it() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::AddColumn(col_def("email", Type::String)),
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::ColumnAdded { ref table, ref column_name } if table == "users" && column_name == "email"),
            "expected ColumnAdded, got: {result:?}"
        );
    }

    // ADD COLUMN with a name already in the schema errors.
    #[test]
    fn test_alter_table_add_column_duplicate_name_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::AddColumn(col_def("name", Type::String)),
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::DuplicateColumn { .. }),
            "expected DuplicateColumn, got: {err:?}"
        );
    }

    // DROP COLUMN removes an existing column.
    #[test]
    fn test_alter_table_drop_column_drops_it() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::DropColumn {
                    name: "age".try_into().unwrap(),
                    if_exists: false,
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::ColumnDropped { ref column_name, .. } if column_name == "age"),
            "expected ColumnDropped, got: {result:?}"
        );
    }

    // DROP COLUMN IF EXISTS on a missing column is a no-op.
    #[test]
    fn test_alter_table_drop_column_if_exists_on_missing_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::DropColumn {
                    name: "ghost".try_into().unwrap(),
                    if_exists: true,
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::NoOp { .. }),
            "expected NoOp, got: {result:?}"
        );
    }

    // DROP COLUMN on a missing column without IF EXISTS errors.
    #[test]
    fn test_alter_table_drop_column_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::DropColumn {
                    name: "ghost".try_into().unwrap(),
                    if_exists: false,
                },
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownColumn { .. }),
            "expected UnknownColumn, got: {err:?}"
        );
    }

    // RENAME COLUMN renames an existing column.
    #[test]
    fn test_alter_table_rename_column_renames_it() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::RenameColumn {
                    from: "name".try_into().unwrap(),
                    to: "full_name".try_into().unwrap(),
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::ColumnRenamed { ref old_name, ref new_name, .. } if old_name == "name" && new_name == "full_name"),
            "expected ColumnRenamed, got: {result:?}"
        );
    }

    // RENAME COLUMN from a missing column errors.
    #[test]
    fn test_alter_table_rename_column_unknown_source_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::RenameColumn {
                    from: "ghost".try_into().unwrap(),
                    to: "x".try_into().unwrap(),
                },
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownColumn { .. }),
            "expected UnknownColumn, got: {err:?}"
        );
    }

    // ALTER COLUMN SET DEFAULT stores the default value.
    #[test]
    fn test_alter_table_set_default_stores_value() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::SetDefault {
                    column: "name".try_into().unwrap(),
                    value: Value::String("anon".to_string()),
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::ColumnDefaultSet { .. }),
            "expected ColumnDefaultSet, got: {result:?}"
        );
    }

    // ADD PRIMARY KEY sets the PK on a table that has none.
    #[test]
    fn test_alter_table_add_primary_key_sets_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::AddPrimaryKey {
                    columns: vec!["id".try_into().unwrap()],
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::PrimaryKeySet { .. }),
            "expected PrimaryKeySet, got: {result:?}"
        );
    }

    // ADD PRIMARY KEY when one already exists errors.
    #[test]
    fn test_alter_table_add_primary_key_when_already_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "pk_table",
                TupleSchema::new(vec![field("id", Type::Int64).not_null()]),
                vec![crate::catalog::ConstraintDef::PrimaryKey {
                    columns: vec![crate::primitives::ColumnId::try_from(0usize).unwrap()],
                }],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "pk_table",
                false,
                AlterAction::AddPrimaryKey {
                    columns: vec!["id".try_into().unwrap()],
                },
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::PrimaryKeyAlreadyExists(_)),
            "expected PrimaryKeyAlreadyExists, got: {err:?}"
        );
    }

    // DROP CONSTRAINT removes a named constraint.
    #[test]
    fn test_alter_table_drop_constraint_if_exists_on_missing_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::AlterTable(alter_stmt(
                "users",
                false,
                AlterAction::DropConstraint {
                    name: "ghost_constraint".try_into().unwrap(),
                    if_exists: true,
                },
            )))
            .unwrap();

        assert!(
            matches!(result, StatementResult::NoOp { .. }),
            "expected NoOp, got: {result:?}"
        );
    }

    /// Parse and execute a SQL statement, panicking on parse or engine error.
    fn run(engine: &Engine<'_>, sql: &str) {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt).expect("execute");
    }

    /// Parse and execute a SQL statement, returning the engine result.
    fn try_run(engine: &Engine<'_>, sql: &str) -> Result<StatementResult, EngineError> {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt)
    }

    /// Build an `AlterTableStatement` for `ADD CONSTRAINT … CHECK`.
    fn add_check_stmt(
        table: &str,
        constraint_name: &str,
        expr: Expr,
        not_valid: bool,
    ) -> Statement {
        Statement::AlterTable(AlterTableStatement {
            table_name: NonEmptyString::new(table).unwrap(),
            if_exists: false,
            action: AlterAction::AddConstraint {
                name: Some(NonEmptyString::new(constraint_name).unwrap()),
                constraint: TableConstraint::Check { expr },
                not_valid,
            },
        })
    }

    /// Build an `AlterTableStatement` for `VALIDATE CONSTRAINT`.
    fn validate_stmt(table: &str, constraint_name: &str) -> Statement {
        Statement::AlterTable(AlterTableStatement {
            table_name: NonEmptyString::new(table).unwrap(),
            if_exists: false,
            action: AlterAction::ValidateConstraint {
                name: NonEmptyString::new(constraint_name).unwrap(),
            },
        })
    }

    // ── ADD CONSTRAINT CHECK — strict path ────────────────────────────────────

    // A violating row must block the constraint from being added.
    #[test]
    fn test_add_check_constraint_blocks_when_existing_row_violates() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        // Insert a row with age = 5 before the constraint exists.
        run(&engine, "INSERT INTO users VALUES (1, 'alice', 5)");

        // The constraint requires age > 10 — row with age=5 must be rejected.
        let expr = Parser::parse_expr("age > 10").unwrap();
        let err = engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, false))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Constraint(_)),
            "expected ConstraintViolation, got: {err:?}"
        );

        // The constraint must not have been stored in the catalog.
        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "users").unwrap();
        txn.commit().unwrap();
        assert!(
            info.check_constraints.is_empty(),
            "constraint must not be persisted after a violation"
        );
    }

    // A clean table: constraint is added; a subsequent violating INSERT must fail.
    #[test]
    fn test_add_check_constraint_succeeds_on_clean_table_and_enforces_inserts() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users VALUES (1, 'alice', 25)");

        // Constraint: age > 10. Existing row (25) passes; inserting age=5 must fail.
        let expr = Parser::parse_expr("age > 10").unwrap();
        let result = engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, false))
            .unwrap();

        assert!(
            matches!(result, StatementResult::CheckConstraintAdded {
                not_valid: false,
                ..
            }),
            "expected CheckConstraintAdded (validated), got: {result:?}"
        );

        // Violating insert must now be rejected.
        let err = try_run(&engine, "INSERT INTO users VALUES (2, 'bob', 5)").unwrap_err();
        assert!(
            matches!(err, EngineError::Constraint(_)),
            "expected ConstraintViolation on subsequent bad insert, got: {err:?}"
        );
    }

    // Empty table: always succeeds regardless of the expression.
    #[test]
    fn test_add_check_constraint_succeeds_on_empty_table() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        let expr = Parser::parse_expr("age > 10").unwrap();
        let result = engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, false))
            .unwrap();

        assert!(
            matches!(result, StatementResult::CheckConstraintAdded {
                not_valid: false,
                ..
            }),
            "expected CheckConstraintAdded, got: {result:?}"
        );
    }

    // ── ADD CONSTRAINT CHECK NOT VALID ─────────────────────────────────────────

    // NOT VALID with a violating row: succeeds (no scan), stored as unverified.
    #[test]
    fn test_add_check_not_valid_skips_scan_and_stores_unvalidated() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        // Row with age=5 violates `age > 10` — must NOT block the NOT VALID constraint.
        run(&engine, "INSERT INTO users VALUES (1, 'alice', 5)");

        let expr = Parser::parse_expr("age > 10").unwrap();
        let result = engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, true))
            .unwrap();

        assert!(
            matches!(result, StatementResult::CheckConstraintAdded {
                not_valid: true,
                ..
            }),
            "expected CheckConstraintAdded (not_valid=true), got: {result:?}"
        );

        // The catalog must hold the constraint as unvalidated.
        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "users").unwrap();
        txn.commit().unwrap();
        let chk = info
            .check_constraints
            .iter()
            .find(|c| c.name == "chk_age")
            .expect("constraint must exist in catalog");
        assert!(!chk.validated, "constraint must be stored as not validated");
    }

    // NOT VALID constraint still enforces new writes after being added.
    #[test]
    fn test_add_check_not_valid_still_enforces_new_inserts() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        let expr = Parser::parse_expr("age > 10").unwrap();
        engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, true))
            .unwrap();

        // age=5 violates `age > 10` — must still be rejected even with NOT VALID.
        let err = try_run(&engine, "INSERT INTO users VALUES (1, 'alice', 5)").unwrap_err();
        assert!(
            matches!(err, EngineError::Constraint(_)),
            "NOT VALID constraint must still reject new bad inserts, got: {err:?}"
        );
    }

    // ── VALIDATE CONSTRAINT ────────────────────────────────────────────────────

    // After fixing the violating row, VALIDATE CONSTRAINT succeeds and marks it validated.
    #[test]
    fn test_validate_constraint_succeeds_when_all_rows_pass() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        // age=30 satisfies `age > 10` — VALIDATE must succeed.
        run(&engine, "INSERT INTO users VALUES (1, 'alice', 30)");
        let expr = Parser::parse_expr("age > 10").unwrap();
        engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, true))
            .unwrap();

        // Validate: all rows pass.
        let result = engine
            .execute_statement(validate_stmt("users", "chk_age"))
            .unwrap();

        assert!(
            matches!(result, StatementResult::ConstraintValidated { .. }),
            "expected ConstraintValidated, got: {result:?}"
        );

        // The catalog must now show validated = true.
        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "users").unwrap();
        txn.commit().unwrap();
        let chk = info
            .check_constraints
            .iter()
            .find(|c| c.name == "chk_age")
            .expect("constraint must still exist");
        assert!(
            chk.validated,
            "constraint must be marked validated after VALIDATE CONSTRAINT"
        );
    }

    // VALIDATE CONSTRAINT fails when a violating row still exists.
    #[test]
    fn test_validate_constraint_fails_when_row_violates() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_table(&catalog, &txn_mgr);
        let engine = Engine::new(&catalog, &txn_mgr);

        // age=5 violates `age > 10`.
        run(&engine, "INSERT INTO users VALUES (1, 'alice', 5)");
        let expr = Parser::parse_expr("age > 10").unwrap();
        engine
            .execute_statement(add_check_stmt("users", "chk_age", expr, true))
            .unwrap();

        let err = engine
            .execute_statement(validate_stmt("users", "chk_age"))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Constraint(_)),
            "expected ConstraintViolation from VALIDATE CONSTRAINT, got: {err:?}"
        );

        // The constraint must remain unvalidated after a failed VALIDATE.
        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "users").unwrap();
        txn.commit().unwrap();
        let chk = info
            .check_constraints
            .iter()
            .find(|c| c.name == "chk_age")
            .expect("constraint must still exist");
        assert!(
            !chk.validated,
            "constraint must stay unvalidated after a failed VALIDATE"
        );
    }
}
