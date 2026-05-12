//! DDL statement execution for the SQL engine.
//!
//! This module covers the schema-facing SQL surface: `CREATE TABLE`, `DROP TABLE`,
//! `CREATE INDEX`, `DROP INDEX`, and `SHOW INDEXES`. Parsing keeps the user's
//! syntax, binding resolves names into catalog objects, and these helpers perform
//! the final catalog mutation or metadata read inside one transaction.
//!
//! # Shape
//!
//! - [`Engine::exec_create_table`] — executes `CREATE TABLE`, including `IF NOT EXISTS`, by
//!   creating a table catalog entry and heap file.
//! - [`Engine::exec_drop_table`] — executes `DROP TABLE`, including `IF EXISTS`, by removing the
//!   resolved table from the catalog.
//! - [`Engine::exec_create_index`] — executes `CREATE INDEX`, including `IF NOT EXISTS`, by
//!   creating an index entry for resolved table columns.
//! - [`Engine::exec_drop_index`] — executes `DROP INDEX`, including `IF EXISTS`, by removing the
//!   resolved index entry.
//! - [`Engine::exec_show_indexes`] — executes `SHOW INDEXES [FROM table]` by reading catalog index
//!   metadata and shaping it for the statement result.
//!
//! # How it works
//!
//! Each helper calls [`Engine::bind_and_execute`]. The binder produces a compact
//! [`Bound`] variant such as [`BoundCreateTable::New`] or
//! [`BoundDropIndex::NoOp`]; the executor then either applies that bound shape to
//! the catalog or returns the SQL no-op result requested by `IF EXISTS` /
//! `IF NOT EXISTS`.
//!
//! # NULL semantics
//!
//! DDL execution does not evaluate row expressions. `NULL` appears only as
//! column nullability recorded in the `CREATE TABLE` schema; `SHOW INDEXES`
//! returns catalog strings and index kinds, not row values.
//!
//! UNIQUE-with-index orchestration (empty B-tree, catalog rows, then heap
//! backfill) lives in this module so the catalog stays metadata-oriented.

use crate::{
    FileId,
    binder::{
        Bound, BoundAlterAction, BoundAlterTable, BoundConstraintBody, BoundCreateIndex,
        BoundCreateTable, BoundDropIndex, BoundShowIndexes,
    },
    catalog::{CatalogError, ConstraintDef, manager::Catalog},
    engine::{Engine, EngineError, ShownIndex, StatementResult},
    index::IndexKind,
    parser::statements::{
        AlterTableStatement, CreateIndexStatement, DropIndexStatement, ShowIndexesStatement,
        Statement,
    },
    primitives::ColumnId,
    transaction::Transaction,
};

/// Inserts one secondary-index entry per live heap row for `index_name`.
///
/// Used after [`Catalog::create_index`] when enforcing UNIQUE on existing data.
/// Duplicate keys are rejected before insert (B-trees may otherwise hold the
/// same key under different [`RecordId`](crate::primitives::RecordId)s).
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

fn add_unique_constraint_with_btree_backfill(
    catalog: &Catalog,
    txn: &Transaction<'_>,
    table_file_id: FileId,
    table_name: &str,
    constraint_name: &str,
    index_name: &str,
    columns: &[ColumnId],
) -> Result<(), CatalogError> {
    let index_id = catalog.create_index(
        txn,
        index_name,
        table_name,
        table_file_id,
        columns,
        IndexKind::Btree,
    )?;
    catalog.add_constraint(txn, table_file_id, ConstraintDef::Unique {
        name: constraint_name.to_owned(),
        columns: columns.to_vec(),
        backing_index_id: Some(index_id),
    })?;
    populate_index_from_heap(catalog, txn, table_file_id, index_name)?;
    Ok(())
}

impl Engine<'_> {
    /// Executes a `CREATE TABLE` statement after binding table names and schema.
    ///
    /// Given a table like `users(id, name, age)`, binding has already converted
    /// the SQL column list into the physical [`TupleSchema`] and resolved any
    /// primary key names to [`crate::primitives::ColumnId`] values. Execution
    /// either creates the catalog entry or reports the successful no-op required
    /// by `IF NOT EXISTS`.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- CREATE TABLE users(id BIGINT PRIMARY KEY, name TEXT, age BIGINT);
    /// ```
    ///
    /// ```ignore
    /// BoundCreateTable::New {
    ///     name: "users".into(),
    ///     schema: TupleSchema::new(vec![/* id -> 0, name -> 1, age -> 2 */]),
    ///     primary_key: Some(vec![ColumnId::try_from(0usize).unwrap()]),
    /// }
    /// ```
    ///
    /// ```sql
    /// -- CREATE TABLE IF NOT EXISTS users(id BIGINT);
    /// ```
    ///
    /// ```ignore
    /// BoundCreateTable::AlreadyExists {
    ///     name: "users".into(),
    ///     file_id: <resolved users heap file>,
    /// }
    /// ```
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- CREATE TABLE users(id BIGINT, name TEXT, age BIGINT);
    /// --   StatementResult::TableCreated {
    /// --       name: "users",
    /// --       file_id: <new heap file>,
    /// --       already_exists: false,
    /// --   }
    ///
    /// -- CREATE TABLE IF NOT EXISTS users(id BIGINT);
    /// --   StatementResult::TableCreated {
    /// --       name: "users",
    /// --       file_id: <existing heap file>,
    /// --       already_exists: true,
    /// --   }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Bind`] when SQL-level validation fails, such as a
    /// duplicate table name without `IF NOT EXISTS`, duplicate column names, or a
    /// primary key name that is not present in the column list.
    ///
    /// Returns [`EngineError::Catalog`] when creating the table's catalog entry
    /// or heap file fails.
    pub(super) fn exec_create_table(
        &self,
        statement: Statement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::CreateTable(b) = bound else {
                unreachable!("binder returned non-CreateTable variant for CreateTable input");
            };

            match b {
                BoundCreateTable::AlreadyExists { name, file_id } => {
                    Ok(StatementResult::table_created(name, file_id, true))
                }
                BoundCreateTable::New {
                    name,
                    schema,
                    primary_key,
                    constraints,
                } => {
                    let file_id = catalog.create_table(txn, &name, schema, primary_key)?;
                    for c in &constraints {
                        match &c.body {
                            BoundConstraintBody::Unique { columns } => {
                                let constraint_name: String = match c.name.as_ref() {
                                    Some(n) => n.as_str().to_owned(),
                                    None => catalog
                                        .autoname_unique_constraint_for(txn, file_id, columns)?,
                                };
                                let index_name = format!("{name}_{constraint_name}_idx");
                                add_unique_constraint_with_btree_backfill(
                                    catalog,
                                    txn,
                                    file_id,
                                    &name,
                                    constraint_name.as_str(),
                                    index_name.as_str(),
                                    columns.as_slice(),
                                )?;
                            }
                            BoundConstraintBody::ForeignKey {
                                local_columns,
                                ref_table_id,
                                ref_columns,
                                on_delete,
                                on_update,
                            } => {
                                let constraint_name: String = if let Some(n) = c.name.as_ref() {
                                    n.as_str().to_owned()
                                } else {
                                    let tbl = catalog.get_table_info_by_id(txn, file_id)?;
                                    let col_names: Vec<&str> = local_columns
                                        .iter()
                                        .map(|&id| {
                                            tbl.schema.field(usize::from(id)).unwrap().name.as_str()
                                        })
                                        .collect();
                                    format!("{}_{}_fkey", tbl.name.as_str(), col_names.join("_"))
                                };
                                catalog.add_constraint(
                                    txn,
                                    file_id,
                                    ConstraintDef::ForeignKey {
                                        name: constraint_name,
                                        local_columns: local_columns.clone(),
                                        ref_table_id: *ref_table_id,
                                        ref_columns: ref_columns.clone(),
                                        on_delete: *on_delete,
                                        on_update: *on_update,
                                    },
                                )?;
                            }
                            BoundConstraintBody::Check { .. } => {}
                        }
                    }
                    Ok(StatementResult::table_created(name, file_id, false))
                }
            }
        })
    }

    /// Executes a `CREATE INDEX` statement after binding table and column names.
    ///
    /// For a fixed schema `users(id, name, age)` with resolved indices
    /// `id -> 0`, `name -> 1`, `age -> 2`, binding turns the SQL column list into
    /// ordered [`crate::primitives::ColumnId`] values. Execution stores the index
    /// metadata in the catalog; the index rows themselves are owned by the index
    /// subsystem.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- CREATE INDEX users_age_idx ON users(age) USING HASH;
    /// ```
    ///
    /// ```ignore
    /// BoundCreateIndex::New {
    ///     index_name: "users_age_idx".into(),
    ///     table_name: "users".into(),
    ///     table_file_id: <resolved users heap file>,
    ///     column_indices: vec![ColumnId::try_from(2usize).unwrap()],
    ///     kind: IndexKind::Hash,
    /// }
    /// ```
    ///
    /// ```sql
    /// -- CREATE INDEX users_name_age_idx ON users(name, age) USING HASH;
    /// ```
    ///
    /// ```ignore
    /// BoundCreateIndex::New {
    ///     index_name: "users_name_age_idx".into(),
    ///     table_name: "users".into(),
    ///     column_indices: vec![
    ///         ColumnId::try_from(1usize).unwrap(),
    ///         ColumnId::try_from(2usize).unwrap(),
    ///     ],
    ///     kind: IndexKind::Hash,
    ///     ..
    /// }
    /// ```
    ///
    /// ```sql
    /// -- CREATE INDEX IF NOT EXISTS users_age_idx ON users(age) USING HASH;
    /// ```
    ///
    /// ```ignore
    /// BoundCreateIndex::AlreadyExists {
    ///     index_name: "users_age_idx".into(),
    /// }
    /// ```
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- CREATE INDEX users_age_idx ON users(age) USING HASH;
    /// --   StatementResult::IndexCreated {
    /// --       name: "users_age_idx",
    /// --       table: "users",
    /// --       already_exists: false,
    /// --   }
    ///
    /// -- CREATE INDEX IF NOT EXISTS users_age_idx ON users(age) USING HASH;
    /// --   StatementResult::IndexCreated {
    /// --       name: "users_age_idx",
    /// --       table: "",
    /// --       already_exists: true,
    /// --   }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Bind`] when the target table is unknown, the index
    /// name already exists without `IF NOT EXISTS`, an indexed column is unknown,
    /// or the same column appears more than once in the index column list.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot persist the index
    /// metadata.
    pub(super) fn exec_create_index(
        &self,
        statement: CreateIndexStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::CreateIndex(statement), |catalog, bound, txn| {
            let Bound::CreateIndex(b) = bound else {
                unreachable!("binder returned non-CreateIndex variant for CreateIndex input");
            };

            match b {
                BoundCreateIndex::AlreadyExists { index_name } => {
                    Ok(StatementResult::index_created(index_name, "", true))
                }
                BoundCreateIndex::New {
                    index_name,
                    table_name,
                    table_file_id,
                    column_indices,
                    kind,
                } => {
                    catalog.create_index(
                        txn,
                        &index_name,
                        &table_name,
                        table_file_id,
                        &column_indices,
                        kind,
                    )?;
                    Ok(StatementResult::index_created(
                        index_name, table_name, false,
                    ))
                }
            }
        })
    }

    /// Executes a `DROP INDEX` statement after binding index ownership.
    ///
    /// Binding checks both the index name and the `ON <table>` clause. Execution
    /// drops the catalog index entry when the bound shape is concrete, or returns
    /// the SQL no-op result for `DROP INDEX IF EXISTS`.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- DROP INDEX users_age_idx ON users;
    /// ```
    ///
    /// ```ignore
    /// BoundDropIndex::Drop {
    ///     index_name: "users_age_idx".into(),
    /// }
    /// ```
    ///
    /// ```sql
    /// -- DROP INDEX IF EXISTS ghost_idx ON users;
    /// ```
    ///
    /// ```ignore
    /// BoundDropIndex::NoOp {
    ///     index_name: "ghost_idx".into(),
    /// }
    /// ```
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
    /// Returns [`EngineError::Bind`] when the table is unknown, the index is
    /// unknown without `IF EXISTS`, or the named index belongs to a different
    /// table.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot remove the index
    /// metadata.
    pub(super) fn exec_drop_index(
        &self,
        statement: DropIndexStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::DropIndex(statement), |catalog, bound, txn| {
            let Bound::DropIndex(b) = bound else {
                unreachable!("binder returned non-DropIndex variant for DropIndex input");
            };

            match b {
                BoundDropIndex::NoOp { index_name } => {
                    Ok(StatementResult::index_dropped(index_name))
                }
                BoundDropIndex::Drop { index_name } => {
                    catalog.drop_index(txn, &index_name)?;
                    Ok(StatementResult::index_dropped(index_name))
                }
            }
        })
    }

    /// Executes `SHOW INDEXES [FROM table]` by reading catalog index metadata.
    ///
    /// For a fixed schema `users(id, name, age)` with resolved indices
    /// `id -> 0`, `name -> 1`, `age -> 2`, the catalog stores index columns as
    /// ids but this result reports the SQL column names in index declaration
    /// order. With no `FROM` clause, every index in the catalog is listed.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SHOW INDEXES;
    /// ```
    ///
    /// ```ignore
    /// BoundShowIndexes::AllTables
    /// ```
    ///
    /// ```sql
    /// -- SHOW INDEXES FROM users;
    /// ```
    ///
    /// ```ignore
    /// BoundShowIndexes::OneTable {
    ///     name: "users".into(),
    ///     file_id: <resolved users heap file>,
    /// }
    /// ```
    ///
    /// # SQL -> result mapping
    ///
    /// ```sql
    /// -- SHOW INDEXES FROM users;
    /// --   StatementResult::IndexesShown {
    /// --       scope: Some("users"),
    /// --       rows: vec![
    /// --           ShownIndex {
    /// --               name: "users_age_idx",
    /// --               table: "users",
    /// --               columns: vec!["age"],
    /// --               kind: IndexKind::Hash,
    /// --           },
    /// --       ],
    /// --   }
    /// ```
    ///
    /// The output rows are laid out as user-facing index metadata: index name,
    /// table name, indexed column names in declaration order, and index kind.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Bind`] when `SHOW INDEXES FROM <table>` names an
    /// unknown table.
    ///
    /// Returns [`EngineError::Catalog`] when the catalog cannot list index
    /// metadata for the requested scope.
    pub(super) fn exec_show_indexes(
        &self,
        statement: ShowIndexesStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::ShowIndexes(statement), |catalog, bound, txn| {
            let Bound::ShowIndexes(b) = bound else {
                unreachable!("binder returned non-ShowIndexes variant for ShowIndexes input");
            };

            let (scope, infos) = match b {
                BoundShowIndexes::AllTables => (None, catalog.list_indexes(txn)?),
                BoundShowIndexes::OneTable { name, file_id } => {
                    (Some(name), catalog.list_indexes_for(txn, file_id)?)
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
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn exec_alter_table(
        &self,
        statement: AlterTableStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::AlterTable(statement), |catalog, bound, txn| {
            let Bound::AlterTable(b) = bound else {
                unreachable!("binder returned non-AlterTable variant for AlterTable input");
            };

            match b {
                BoundAlterTable::NoOp { table_name } => Ok(StatementResult::NoOp {
                    statement: format!("ALTER TABLE IF EXISTS {table_name}"),
                }),
                BoundAlterTable::Action {
                    table_name,
                    file_id,
                    action,
                } => match action {
                    BoundAlterAction::RenameTable { old_name, new_name } => {
                        catalog.rename_table(txn, &old_name, &new_name)?;
                        Ok(StatementResult::TableRenamed { old_name, new_name })
                    }
                    BoundAlterAction::RenameColumn { old_name, new_name } => {
                        catalog.rename_column(txn, file_id, &old_name, &new_name)?;
                        Ok(StatementResult::ColumnRenamed {
                            table: table_name,
                            old_name,
                            new_name,
                        })
                    }
                    BoundAlterAction::AddColumn { column } => {
                        let column_name = column.name.clone();
                        catalog.add_column(txn, file_id, column)?;
                        Ok(StatementResult::ColumnAdded {
                            table: table_name,
                            column_name: column_name.as_str().to_owned(),
                        })
                    }
                    BoundAlterAction::DropColumn { column_name, .. } => {
                        catalog.drop_column(txn, file_id, &column_name)?;
                        Ok(StatementResult::ColumnDropped {
                            table: table_name,
                            column_name,
                        })
                    }
                    BoundAlterAction::SetDefault { column, value } => {
                        catalog.set_column_default(txn, file_id, &column, value)?;
                        Ok(StatementResult::ColumnDefaultSet {
                            table: table_name,
                            column,
                        })
                    }
                    BoundAlterAction::DropDefault { column } => {
                        catalog.drop_column_default(txn, file_id, &column)?;
                        Ok(StatementResult::ColumnDefaultDropped {
                            table: table_name,
                            column,
                        })
                    }
                    BoundAlterAction::DropNotNull { column } => {
                        catalog.drop_column_not_null(txn, file_id, &column)?;
                        Ok(StatementResult::ColumnNotNullDropped {
                            table: table_name,
                            column,
                        })
                    }
                    BoundAlterAction::AddPrimaryKey { column_ids } => {
                        catalog.set_primary_key(txn, file_id, column_ids)?;
                        Ok(StatementResult::PrimaryKeySet { table: table_name })
                    }
                    BoundAlterAction::DropPrimaryKey => {
                        catalog.drop_primary_key(txn, file_id)?;
                        Ok(StatementResult::PrimaryKeyDropped { table: table_name })
                    }
                    BoundAlterAction::AddConstraint { constraint } => match &constraint.body {
                        BoundConstraintBody::Unique { columns } => {
                            let constraint_name: String = match constraint.name.as_ref() {
                                Some(n) => n.as_str().to_owned(),
                                None => {
                                    catalog.autoname_unique_constraint_for(txn, file_id, columns)?
                                }
                            };
                            let index_name = format!("{table_name}_{constraint_name}_idx");
                            add_unique_constraint_with_btree_backfill(
                                catalog,
                                txn,
                                file_id,
                                &table_name,
                                constraint_name.as_str(),
                                index_name.as_str(),
                                columns.as_slice(),
                            )?;
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
                    BoundAlterAction::DropConstraint { name, if_exists } => {
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
                },
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        Type,
        binder::BindError,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{Engine, EngineError, StatementResult},
        parser::statements::{ColumnDef, CreateTableStatement, Uniqueness},
        primitives::{ColumnId, NonEmptyString},
        transaction::TransactionManager,
        tuple::TupleSchema,
        wal::writer::Wal,
    };

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

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

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
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
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(
                stmt_create,
            ))
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
        let result = engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(
                stmt_if_not_exists,
            ))
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
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_inline").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(0)]));
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
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_precedence").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(0)]));
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
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "pk_table_level").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![col_id(1)]));
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
        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
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
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt1))
            .unwrap();

        let stmt2 = statement_create(
            "dup_err",
            false,
            vec![col("id", Type::Uint64, false, false)],
            None,
        );
        let err = engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt2))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Bind(BindError::TableAlreadyExists(_))),
            "expected Bind(TableAlreadyExists), got: {err:?}"
        );
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
            table_name: "schema_check".try_into().unwrap(),
            if_not_exists: false,
            columns: cols,
            primary_key: vec![],
            constraints: vec![],
        };
        let engine = Engine::new(&catalog, &txn_mgr);
        engine
            .exec_create_table(crate::parser::statements::Statement::CreateTable(stmt))
            .unwrap();

        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, "schema_check").unwrap();
        txn.commit().unwrap();

        let expected_names: Vec<_> = expected_schema.fields().map(|f| f.name.as_str()).collect();
        let actual_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(actual_names, expected_names);
    }

    use crate::{index::IndexKind, parser::statements::ShowIndexesStatement, tuple::Field};

    fn make_users_with_email_index(catalog: &Catalog, txn_mgr: &TransactionManager) {
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                crate::tuple::TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
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
                &[ColumnId::try_from(1usize).unwrap()],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();
    }

    // SHOW INDEXES with no FROM lists every index across every table,
    // sorted by name. Each row carries its index name, the resolved table
    // name, the column list in declaration order, and the access kind.
    #[test]
    fn test_show_indexes_lists_all_with_resolved_table_names() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        make_users_with_email_index(&catalog, &txn_mgr);

        // Add a second table with a composite index so the test exercises
        // both single- and multi-column rendering.
        let txn = txn_mgr.begin().unwrap();
        let t_id = catalog
            .create_table(
                &txn,
                "t",
                crate::tuple::TupleSchema::new(vec![
                    field("a", Type::Int64).not_null(),
                    field("b", Type::Int64).not_null(),
                ]),
                None,
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
            .exec_show_indexes(ShowIndexesStatement(None))
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

        // Second table with its own index — must not appear in the output.
        let txn = txn_mgr.begin().unwrap();
        let other_id = catalog
            .create_table(
                &txn,
                "other",
                TupleSchema::new(vec![field("k", Type::Int64).not_null()]),
                None,
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
            .exec_show_indexes(ShowIndexesStatement(Some("users".try_into().unwrap())))
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
            .exec_show_indexes(ShowIndexesStatement(None))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert!(scope.is_none());
        assert!(rows.is_empty());
    }

    // SHOW INDEXES FROM <table> on a table with no indexes returns an empty
    // list with the scope still set to the table name.
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
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let result = engine
            .exec_show_indexes(ShowIndexesStatement(Some("lonely".try_into().unwrap())))
            .unwrap();

        let StatementResult::IndexesShown { scope, rows } = result else {
            panic!("expected IndexesShown, got: {result:?}");
        };
        assert_eq!(scope.as_deref(), Some("lonely"));
        assert!(rows.is_empty());
    }

    // SHOW INDEXES FROM <missing-table> errors via the binder's UnknownTable.
    #[test]
    fn test_show_indexes_from_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = engine
            .exec_show_indexes(ShowIndexesStatement(Some("ghost".try_into().unwrap())))
            .unwrap_err();

        assert!(
            matches!(err, EngineError::Bind(BindError::UnknownTable(ref n)) if n == "ghost"),
            "expected Bind(UnknownTable), got: {err:?}"
        );
    }
}
