use crate::{
    catalog::{ConstraintDef, manager::Catalog, systable::FkAction},
    engine::{Engine, EngineError, StatementResult},
    index::IndexKind,
    parser::statements::{ColumnDef, CreateTableStatement, Uniqueness},
    primitives::{ColumnId, NonEmptyString},
    transaction::Transaction,
    tuple::TupleSchema,
};

impl Engine<'_> {
    /// Executes a `CREATE TABLE` statement after binding table names and schema.
    ///
    /// Given a table like `users(id, name, age)`, binding has already converted
    /// the SQL column list into the physical [`TupleSchema`] and resolved any
    /// primary key names to [`crate::primitives::ColumnId`] values. Execution
    /// either creates the catalog entry or reports the successful no-op required
    /// by `IF NOT EXISTS`.
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- CREATE TABLE users(id BIGINT, name TEXT, age BIGINT);
    /// -- ```sql
    ///
    /// -- ```ignore
    /// --   StatementResult::TableCreated {
    /// --       name: "users",
    /// --       file_id: <new heap file>,
    /// --       already_exists: false,
    /// --   }
    /// -- ```
    ///
    /// -- ```sql
    /// -- CREATE TABLE IF NOT EXISTS users(id BIGINT);
    /// -- ```sql
    ///
    /// -- ```ignore
    /// --   StatementResult::TableCreated {
    /// --       name: "users",
    /// --       file_id: <resolved users heap file>,
    /// --       already_exists: true,
    /// --   }
    /// -- ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TableAlreadyExists`] when the table already exists
    /// without `IF NOT EXISTS`. Returns [`EngineError::UnknownColumn`] when a
    /// primary key name is not in the column list.
    ///
    /// Returns [`EngineError::Catalog`] when creating the table's catalog entry
    /// or heap file fails.
    pub(super) fn exec_create_table(
        stmt: CreateTableStatement,
        txn: &Transaction<'_>,
        catalog: &Catalog,
    ) -> Result<StatementResult, EngineError> {
        if catalog.table_exists(&stmt.table_name) {
            return Self::handle_table_already_exists(stmt, txn, catalog);
        }

        let ai_count = stmt.columns.iter().filter(|c| c.auto_increment).count();
        if ai_count > 1 {
            return Err(EngineError::MultipleAutoIncrementColumns {
                table: stmt.table_name.as_str().to_owned(),
                count: ai_count,
            });
        }

        let CreateTableStatement {
            table_name,
            columns,
            primary_key: table_pk,
            constraints: table_constraints,
            ..
        } = stmt;

        let schema = TupleSchema::from(columns.as_slice());

        Self::resolve_column_ids(
            &schema,
            table_name.as_str(),
            columns.iter().map(|c| &c.name),
        )?;

        let mut defs: Vec<ConstraintDef> =
            Vec::with_capacity(1 + columns.len() + table_constraints.len());

        if let Some(pk) = Self::resolve_primary_key(
            columns.as_slice(),
            table_pk.as_slice(),
            &schema,
            table_name.as_str(),
        )? {
            defs.push(pk);
        }

        Self::resolve_column_constraints(
            columns.as_slice(),
            &schema,
            table_name.as_str(),
            catalog,
            txn,
            &mut defs,
        )?;

        for tc in table_constraints {
            defs.push(Self::resolve_table_constraint(
                catalog,
                txn,
                &tc,
                &schema,
                table_name.as_str(),
            )?);
        }

        // UNIQUE constraints need a backing B-tree index, but we need the table's
        // file_id to create indexes — so split them out and add them after the table exists.
        let (unique_defs, other_defs): (Vec<_>, Vec<_>) = defs
            .into_iter()
            .partition(|d| matches!(d, ConstraintDef::Unique { .. }));

        let file_id = catalog.create_table(txn, table_name.as_str(), schema, other_defs)?;

        // If one column declared AUTO_INCREMENT, persist its column id so the
        // counter survives restarts and is visible to the insert path.
        if let Some(ai_col_id) = columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.auto_increment)
            .and_then(|(i, _)| ColumnId::try_from(i).ok())
        {
            catalog.register_auto_increment(txn, file_id, ai_col_id)?;
        }

        for def in unique_defs {
            let ConstraintDef::Unique { name, columns, .. } = def else {
                unreachable!()
            };
            let index_name = format!("{}_{}_idx", table_name.as_str(), name);
            let index_id = catalog.create_index(
                txn,
                &index_name,
                table_name.as_str(),
                file_id,
                &columns,
                IndexKind::Btree,
            )?;
            catalog.add_constraint(txn, file_id, ConstraintDef::Unique {
                name,
                columns,
                backing_index_id: Some(index_id),
            })?;
        }

        Ok(StatementResult::table_created(
            table_name.as_str().to_owned(),
            file_id,
            false,
        ))
    }

    /// Handles `CREATE TABLE` when the name is already registered in the catalog.
    ///
    /// Without `IF NOT EXISTS`, this is a hard error. With `IF NOT EXISTS`, returns
    /// [`StatementResult::table_created`] with `already_exists: true` and the existing table's
    /// [`crate::FileId`], after reloading table metadata from the catalog.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableAlreadyExists`] — same name and not `IF NOT EXISTS`.
    /// - [`EngineError::Catalog`] — propagated from [`Engine::check_table`] if the catalog cannot
    ///   read table metadata (should be rare right after [`Catalog::table_exists`] reported true).
    fn handle_table_already_exists(
        statement: CreateTableStatement,
        txn: &Transaction<'_>,
        catalog: &Catalog,
    ) -> Result<StatementResult, EngineError> {
        let table_name = statement.table_name.into_inner();
        if !statement.if_not_exists {
            return Err(EngineError::TableAlreadyExists(table_name));
        }

        let table = Self::check_table(catalog, txn, &table_name, true)?
            .expect("if_exists=true should never yield None");
        Ok(StatementResult::table_created(
            table_name,
            table.file_id,
            true,
        ))
    }

    /// Builds a [`ConstraintDef::PrimaryKey`] from the parsed `CREATE TABLE` shape.
    ///
    /// Column order in the resulting [`ConstraintDef`] matches the order names appear in the
    /// winning source: any column with `ColumnDef::primary_key` (inline `PRIMARY KEY`), else
    /// the table-level `PRIMARY KEY (col, ...)` list. If both are present, **inline declarations
    /// take precedence** and the table-level list is ignored.
    ///
    /// Each name is resolved against `schema` (which mirrors `columns` in definition order).
    ///
    /// Returns [`None`] when no primary key is declared.
    ///
    /// # Errors
    ///
    /// - [`EngineError::UnknownColumn`] — a declared PK column name is not in `schema`.
    fn resolve_primary_key(
        columns: &[ColumnDef],
        table_pk: &[NonEmptyString],
        schema: &TupleSchema,
        table_name: &str,
    ) -> Result<Option<ConstraintDef>, EngineError> {
        let pk_names: Vec<&str> = {
            let inline: Vec<&str> = columns
                .iter()
                .filter_map(|c| c.primary_key.then_some(c.name.as_str()))
                .collect();
            if inline.is_empty() {
                table_pk.iter().map(NonEmptyString::as_str).collect()
            } else {
                inline
            }
        };

        if pk_names.is_empty() {
            return Ok(None);
        }

        let pk_cols = pk_names
            .into_iter()
            .map(|n| Self::require_column(schema, table_name, n).map(|(id, _)| id))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(ConstraintDef::PrimaryKey { columns: pk_cols }))
    }

    /// Collects catalog constraints declared **on individual columns** in `CREATE TABLE`.
    ///
    /// For each [`ColumnDef`]:
    ///
    /// - `UNIQUE` → [`ConstraintDef::Unique`] with a generated name `{table}_unique_{col}` and
    ///   `backing_index_id: None` (indexes may be created separately).
    /// - `REFERENCES ...` → [`ConstraintDef::ForeignKey`] for the single referenced column, with a
    ///   generated name `{table}_fk_{col}` and referential actions copied from the parse tree.
    ///
    /// Table-level `UNIQUE (...)`, `FOREIGN KEY (...)`, and `CHECK` constraints are handled by
    /// [`Engine::resolve_table_constraint`], not here.
    ///
    /// # Errors
    ///
    /// - [`EngineError::UnknownColumn`] — local or referenced column name missing from the relevant
    ///   schema.
    /// - [`EngineError::UnknownTable`] — referenced table does not exist.
    /// - Other catalog errors from [`Engine::check_table`] surface as [`EngineError::Catalog`].
    fn resolve_column_constraints(
        columns: &[ColumnDef],
        schema: &TupleSchema,
        table_name: &str,
        catalog: &Catalog,
        txn: &Transaction<'_>,
        defs: &mut Vec<ConstraintDef>,
    ) -> Result<(), EngineError> {
        for col in columns {
            if col.unique == Uniqueness::Unique {
                let (col_id, _) = Self::require_column(schema, table_name, col.name.as_str())?;
                defs.push(ConstraintDef::Unique {
                    name: format!("{table_name}_unique_{}", col.name.as_str()),
                    columns: vec![col_id],
                    backing_index_id: None,
                });
            }

            if let Some(reference) = &col.references {
                let (local_id, _) = Self::require_column(schema, table_name, col.name.as_str())?;

                let ref_info = Self::check_table(catalog, txn, reference.table.as_str(), false)?
                    .expect("if_exists=false never yields None");
                let (ref_id, _) = Self::require_column(
                    &ref_info.schema,
                    ref_info.name.as_str(),
                    reference.column.as_str(),
                )?;

                defs.push(ConstraintDef::ForeignKey {
                    name: format!("{table_name}_fk_{}", col.name.as_str()),
                    local_columns: vec![local_id],
                    ref_table_id: ref_info.file_id,
                    ref_columns: vec![ref_id],
                    on_delete: reference.on_delete.map(FkAction::from),
                    on_update: reference.on_update.map(FkAction::from),
                });
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
        Type,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        parser::statements::{ColumnDef, CreateTableStatement, Statement, Uniqueness},
        primitives::{ColumnId, NonEmptyString},
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

    fn col_id(idx: usize) -> ColumnId {
        ColumnId::try_from(idx).unwrap()
    }

    // --- happy path ---
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

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .execute_statement(Statement::CreateTable(stmt))
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

    // --- edge cases ---
    #[test]
    fn test_create_table_if_not_exists_existing_returns_already_exists_true_and_same_file_id() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(statement_create(
                "dup",
                false,
                vec![col("id", Type::Uint64, false, false)],
                None,
            )))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let existing_info = catalog.get_table_info(&txn, "dup").unwrap();
        txn.commit().unwrap();

        let result = engine
            .execute_statement(Statement::CreateTable(statement_create(
                "dup",
                true,
                vec![col("id", Type::Uint64, false, false)],
                None,
            )))
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

    #[test]
    fn test_create_table_inline_primary_key_sets_primary_key_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(statement_create(
                "pk_inline",
                false,
                vec![
                    col("id", Type::Uint64, true, true),
                    col("name", Type::String, true, false),
                ],
                None,
            )))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_inline").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(0)]));
    }

    // Inline PK wins over table-level PK when both are present.
    #[test]
    fn test_create_table_inline_primary_key_takes_precedence_over_table_level_primary_key() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(statement_create(
                "pk_precedence",
                false,
                vec![
                    col("id", Type::Uint64, false, true),
                    col("name", Type::String, false, false),
                ],
                Some("name"),
            )))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_precedence").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(0)]));
    }

    #[test]
    fn test_create_table_table_level_primary_key_used_when_no_inline_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(statement_create(
                "pk_table_level",
                false,
                vec![
                    col("id", Type::Uint64, false, false),
                    col("k", Type::Int64, false, false),
                ],
                Some("k"),
            )))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_table_level").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(1)]));
    }

    // The new engine path (no binder) returns UnknownColumn, not BindError::PrimaryKeyNotInColumns.
    #[test]
    fn test_create_table_primary_key_name_missing_returns_unknown_column() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::CreateTable(statement_create(
                "pk_missing",
                false,
                vec![col("id", Type::Uint64, false, false)],
                Some("nope"),
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownColumn { .. }),
            "expected UnknownColumn for missing PK column, got: {err:?}"
        );
    }

    // --- error paths ---
    // The new engine path returns TableAlreadyExists directly (no binder).
    #[test]
    fn test_create_table_duplicate_without_if_not_exists_returns_err() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(statement_create(
                "dup_err",
                false,
                vec![col("id", Type::Uint64, false, false)],
                None,
            )))
            .unwrap();

        let err = engine
            .execute_statement(Statement::CreateTable(statement_create(
                "dup_err",
                false,
                vec![col("id", Type::Uint64, false, false)],
                None,
            )))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::TableAlreadyExists(_)),
            "expected TableAlreadyExists, got: {err:?}"
        );
    }

    // --- auto_increment validation ---
    #[test]
    fn test_create_table_multiple_auto_increment_columns_returns_err() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let ai_col = |name: &str| ColumnDef {
            name: NonEmptyString::new(name).unwrap(),
            col_type: Type::Int64,
            nullable: false,
            primary_key: false,
            auto_increment: true,
            default: None,
            unique: Uniqueness::NotUnique,
            check: None,
            references: None,
        };

        let stmt = CreateTableStatement {
            table_name: "bad_ai".try_into().unwrap(),
            if_not_exists: false,
            columns: vec![ai_col("id"), ai_col("seq")],
            primary_key: vec![],
            constraints: vec![],
        };

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .execute_statement(Statement::CreateTable(stmt))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::MultipleAutoIncrementColumns {
                count: 2,
                ..
            }),
            "expected MultipleAutoIncrementColumns, got: {err:?}"
        );
    }

    // --- property / invariant tests ---
    #[test]
    fn test_create_table_schema_matches_tuple_schema_from_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let cols = vec![
            col("id", Type::Uint64, false, false),
            col("name", Type::String, true, false),
        ];
        let expected_schema = TupleSchema::from(cols.as_slice());

        let stmt = CreateTableStatement {
            table_name: "schema_check".try_into().unwrap(),
            if_not_exists: false,
            columns: cols,
            primary_key: vec![],
            constraints: vec![],
        };
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .execute_statement(Statement::CreateTable(stmt))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "schema_check").unwrap();
        txn.commit().unwrap();

        let expected_names: Vec<_> = expected_schema.fields().map(|f| f.name.as_str()).collect();
        let actual_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(actual_names, expected_names);
    }
}
