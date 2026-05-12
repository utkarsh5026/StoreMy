use crate::{
    FileId,
    catalog::{CatalogError, ConstraintDef, TableInfo, manager::Catalog},
    engine::{Engine, EngineError, StatementResult},
    index::IndexKind,
    parser::statements::{AlterAction, AlterTableStatement, TableConstraint},
    transaction::Transaction,
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
        txn: &Transaction<'_>,
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
            file_id, schema, ..
        } = table_info;

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

                Self::require_column(&schema, &table_name, old_name.as_str())?;
                Self::assure_no_duplicate_col(&schema, &table_name, new_name.as_str())?;

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
                Self::assure_no_duplicate_col(&schema, &table_name, &column_name)?;
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

                Self::require_column(&schema, &table_name, &column_name)?;
                catalog.set_column_default(txn, file_id, &column_name, value)?;
                Ok(StatementResult::ColumnDefaultSet {
                    table: table_name,
                    column: column_name,
                })
            }

            // ALTER TABLE users ALTER COLUMN name DROP DEFAULT;
            AlterAction::DropDefault { column } => {
                let column_name = column.into_inner();
                Self::require_column(&schema, &table_name, &column_name)?;
                catalog.drop_column_default(txn, file_id, &column_name)?;
                Ok(StatementResult::ColumnDefaultDropped {
                    table: table_name,
                    column: column_name,
                })
            }

            // ALTER TABLE users ALTER COLUMN name DROP NOT NULL;
            AlterAction::DropNotNull { column } => {
                let column_name = column.into_inner();
                Self::require_column(&schema, &table_name, &column_name)?;
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

                let column_ids = Self::resolve_column_ids(&schema, &table_name, columns.iter())?;
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
            // ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id);
            AlterAction::AddConstraint { name, constraint } => match &constraint {
                TableConstraint::Unique { columns } => {
                    let column_ids =
                        Self::resolve_column_ids(&schema, &table_name, columns.iter())?;
                    let constraint_name =
                        Self::resolve_constraint_name(name.as_ref(), &table_name, &constraint);
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
                _ => Err(EngineError::Unsupported(
                    "ALTER TABLE ADD CONSTRAINT: only UNIQUE is supported for execution"
                        .to_string(),
                )),
            },
        }
    }

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

    /// Inserts one secondary-index entry per live heap row for `index_name`.
    ///
    /// Called after [`Catalog::create_index`] when enforcing UNIQUE on existing
    /// data. Duplicate keys are rejected before insert.
    fn populate_index_from_heap(
        catalog: &Catalog,
        txn: &Transaction<'_>,
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
        parser::statements::{AlterAction, AlterTableStatement, ColumnDef, Statement, Uniqueness},
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
            matches!(err, EngineError::UnknownTable(_)),
            "expected UnknownTable, got: {err:?}"
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
}
