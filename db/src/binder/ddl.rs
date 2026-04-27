//! Binding for schema-changing statements (`CREATE TABLE`, `DROP TABLE`).
//!
//! This module sits at the "SQL surface → bound plan" seam for DDL. It resolves
//! object names against the catalog, performs static checks that must succeed
//! before the executor mutates catalog state, and produces compact bound shapes
//! that DDL execution can carry forward without repeating lookups.
//!
//! ## Shape
//!
//! - [`BoundCreateTable`] — bound outcome of `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS`.
//! - [`BoundDrop`] — bound outcome of `DROP TABLE` / `DROP TABLE IF EXISTS`.
//!
//! ## How it works
//!
//! Binding is a single-phase catalog + AST check:
//!
//! - `DROP TABLE` performs catalog resolution and either returns a concrete `FileId` to drop or a
//!   no-op under `IF EXISTS`.
//! - `CREATE TABLE` checks name collisions, validates the column list, builds a [`TupleSchema`] in
//!   declaration order, and resolves the (single-column) primary key.
//!
//! ## NULL semantics
//!
//! DDL binding does not evaluate expressions; `NULL` behavior here is limited to
//! column nullability as recorded in the produced [`TupleSchema`].
use std::collections::HashSet;

use crate::{
    FileId,
    binder::BindError,
    catalog::{
        CatalogError,
        manager::{Catalog, TableInfo},
    },
    index::IndexKind,
    parser::statements::{
        ColumnDef, CreateIndexStatement, CreateTableStatement, DropIndexStatement, DropStatement,
    },
    transaction::Transaction,
    tuple::TupleSchema,
};

/// Outcome of binding a `DROP TABLE` statement.
///
/// Think of this as the DDL analogue of a planned operator: it answers “what
/// does `DROP TABLE …` mean in terms of catalog objects?” and encodes whether
/// the executor should do work.
///
/// ## SQL examples
///
/// ```sql
/// -- 1. Plain DROP TABLE resolves to a concrete heap file
/// --
/// --     DROP TABLE users;
/// --
/// --     BoundDrop::Drop { name: "users".into(), file_id: <resolved> }
/// --
/// -- 2. IF EXISTS turns a missing table into a successful no-op
/// --
/// --     DROP TABLE IF EXISTS ghost;
/// --
/// --     BoundDrop::NoOp { name: "ghost".into() }
/// ```
pub enum BoundDrop {
    Drop { name: String, file_id: FileId },
    NoOp { name: String },
}

/// Outcome of binding a `CREATE TABLE` statement.
///
/// Binding either produces a fully-resolved "create this new table" shape
/// (including the physical row layout) or a successful no-op under
/// `IF NOT EXISTS` that still reports which existing object matched.
///
/// ## SQL examples
///
/// The examples below assume a simple type system where `BIGINT` lowers to
/// `Type::Int64` and `TEXT` lowers to `Type::String`.
///
/// ```sql
/// -- 1. A new table binds to its physical row schema (declaration order)
/// --
/// --     CREATE TABLE users(id BIGINT PRIMARY KEY, name TEXT, age BIGINT);
/// --
/// --     BoundCreateTable::New {
/// --         name: "users".into(),
/// --         schema: TupleSchema::new(vec![
/// --             Field::new("id",   Type::Int64).not_null(),
/// --             Field::new("name", Type::String),
/// --             Field::new("age",  Type::Int64),
/// --         ]),
/// --         primary_key: Some(vec![0]),   // `id` resolved to index 0
/// --     }
/// --
/// -- 2. IF NOT EXISTS binds as a successful no-op when the name is taken
/// --
/// --     CREATE TABLE IF NOT EXISTS users(id BIGINT);
/// --
/// --     BoundCreateTable::AlreadyExists { name: "users".into(), file_id: <resolved> }
/// --
/// -- 3. A table-level PRIMARY KEY clause resolves by name to a column index
/// --
/// --     CREATE TABLE t(a BIGINT, b BIGINT, PRIMARY KEY (b));
/// --
/// --     BoundCreateTable::New { primary_key: Some(vec![1]), .. }
/// ```
///
/// ## Output layout
///
/// `schema` is the on-disk tuple layout for rows of the created table: columns
/// appear in `CREATE TABLE` declaration order.
pub enum BoundCreateTable {
    New {
        name: String,
        schema: TupleSchema,
        primary_key: Option<Vec<usize>>,
    },
    AlreadyExists {
        name: String,
        file_id: FileId,
    },
}

/// Resolves a parsed [`DropStatement`] against the catalog.
///
/// Looks up the target table with [`Catalog::get_table_info`], which reads
/// through `txn` so the result matches whatever catalog pages this transaction
/// already sees.
///
/// # Resolution
///
/// - **Table present** — returns [`BoundDrop::Drop`] with the catalog’s canonical table `name`,
///   [`FileId`], and any other metadata already folded into the lookup. The executor uses `file_id`
///   so it does not repeat the resolution step.
///
/// - **Table absent and `IF EXISTS`** — returns [`BoundDrop::NoOp`] using the statement’s
///   `table_name` for diagnostics only; no catalog mutation is scheduled.
///
/// - **Table absent without `IF EXISTS`** — [`BindError::UnknownTable`].
///
/// - **Any other [`CatalogError`]** — forwarded as [`BindError::Catalog`].
///
/// # Errors
///
/// - [`BindError::UnknownTable`] — missing table when `stmt.if_exists` is false.
/// - [`BindError::Catalog`] — I/O or consistency errors from the catalog layer.
impl BoundDrop {
    pub(in crate::binder) fn bind(
        stmt: &DropStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        match check_table(catalog, txn, &stmt.table_name) {
            Ok(TableInfo { name, file_id, .. }) => Ok(Self::Drop { name, file_id }),
            Err(BindError::UnknownTable(_)) if stmt.if_exists => Ok(Self::NoOp {
                name: stmt.table_name.clone(),
            }),
            Err(e) => Err(e),
        }
    }
}

impl BoundCreateTable {
    pub(in crate::binder) fn bind(
        stmt: &CreateTableStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let CreateTableStatement {
            table_name,
            columns,
            primary_key,
            if_not_exists,
        } = stmt;

        if catalog.table_exists(table_name) {
            return Self::handle_table_exists(catalog, txn, table_name, *if_not_exists);
        }

        let mut seen = HashSet::with_capacity(columns.len());
        columns
            .iter()
            .try_for_each(|col| -> Result<(), BindError> {
                if seen.insert(col.name.as_str()) {
                    Ok(())
                } else {
                    Err(BindError::duplicate_column(col.name.as_str()))
                }
            })?;

        let schema = TupleSchema::from(columns.iter().collect::<Vec<&ColumnDef>>());
        let primary_key = Self::resolve_primary_key(columns.as_slice(), primary_key, &schema)?;

        Ok(Self::New {
            name: table_name.clone(),
            schema,
            primary_key,
        })
    }

    fn handle_table_exists(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        table_name: &str,
        if_not_exists: bool,
    ) -> Result<Self, BindError> {
        if if_not_exists {
            let info = catalog.get_table_info(txn, table_name)?;
            return Ok(Self::AlreadyExists {
                name: table_name.to_string(),
                file_id: info.file_id,
            });
        }
        Err(BindError::table_already_exists(table_name))
    }

    fn resolve_primary_key(
        columns: &[ColumnDef],
        table_pk: &[String],
        schema: &TupleSchema,
    ) -> Result<Option<Vec<usize>>, BindError> {
        let pk_names = {
            let inline_pk_names = columns
                .iter()
                .filter_map(|c| c.primary_key.then_some(c.name.as_str()))
                .collect::<Vec<_>>();

            let table_pk_names = table_pk.iter().map(String::as_str).collect::<Vec<_>>();

            if inline_pk_names.is_empty() {
                table_pk_names
            } else {
                inline_pk_names
            }
        };

        let primary_key = if pk_names.is_empty() {
            None
        } else {
            let mut key_indices = Vec::with_capacity(pk_names.len());
            for name in pk_names {
                match schema.field_by_name(name) {
                    Some((idx, _)) => key_indices.push(idx),
                    None => return Err(BindError::PrimaryKeyNotInColumns(name.to_string())),
                }
            }
            Some(key_indices)
        };

        Ok(primary_key)
    }
}

#[derive(Debug)]
pub enum BoundCreateIndex {
    New {
        index_name: String,
        table_name: String,
        table_file_id: FileId,
        column_indices: Vec<usize>,
        kind: IndexKind,
    },
    AlreadyExists {
        index_name: String,
    },
}

impl BoundCreateIndex {
    pub(in crate::binder) fn bind(
        stmt: CreateIndexStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let CreateIndexStatement {
            index_name,
            table_name,
            columns,
            index_type,
            if_not_exists,
        } = stmt;

        let TableInfo {
            name,
            file_id,
            schema,
            ..
        } = check_table(catalog, txn, table_name)?;
        if catalog.get_index_by_name(&index_name).is_some() {
            if if_not_exists {
                return Ok(Self::AlreadyExists { index_name });
            }
            return Err(BindError::IndexAlreadyExists(index_name));
        }

        let column_indices = Self::resolve_column_indices(columns.as_slice(), &schema, &name)?;
        Ok(Self::New {
            index_name,
            table_name: name,
            table_file_id: file_id,
            column_indices,
            kind: index_type,
        })
    }

    fn resolve_column_indices(
        columns: &[String],
        table_schema: &TupleSchema,
        table_name: &str,
    ) -> Result<Vec<usize>, BindError> {
        let n = columns.len();

        let mut seen = HashSet::with_capacity(n);
        let mut column_indices = Vec::with_capacity(n);

        for col in columns {
            if !seen.insert(col) {
                return Err(BindError::duplicate_column(col.as_str()));
            }
            let Some((idx, _)) = table_schema.field_by_name(col.as_str()) else {
                return Err(BindError::UnknownColumn {
                    table: table_name.to_string(),
                    column: (*col).to_string(),
                });
            };
            column_indices.push(idx);
        }

        Ok(column_indices)
    }
}

/// Outcome of binding a `DROP INDEX` statement.
///
/// Mirrors [`BoundDrop`] for indexes: either a concrete drop targeting a
/// resolved index name, or a successful no-op under `IF EXISTS`.
#[derive(Debug)]
pub enum BoundDropIndex {
    Drop { index_name: String },
    NoOp { index_name: String },
}

impl BoundDropIndex {
    pub(in crate::binder) fn bind(
        stmt: DropIndexStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let DropIndexStatement {
            index_name,
            table_name,
            if_exists,
        } = stmt;

        if let Some(index) = catalog.get_index_by_name(&index_name) {
            let TableInfo { file_id, .. } = check_table(catalog, txn, &table_name)?;
            if catalog.index_belongs_to_table(file_id, &index) {
                return Ok(Self::Drop { index_name });
            }

            if if_exists {
                return Ok(Self::NoOp { index_name });
            }
            return Err(BindError::UnknownIndex(index_name));
        }

        if if_exists {
            return Ok(Self::NoOp { index_name });
        }
        Err(BindError::UnknownIndex(index_name))
    }
}

/// Looks up a table by name in the catalog using the provided transaction context.
///
/// # Arguments
///
/// - `catalog`: Reference to the [`Catalog`] used for looking up table information.
/// - `txn`: Active [`Transaction`] context for catalog access (ensures correct snapshot/isolation).
/// - `table_name`: Name of the table to look up. Accepts anything convertible to a string
///   reference.
///
/// # Returns
///
/// - `Ok(TableInfo)`: If the table exists and metadata was fetched successfully.
/// - `Err(BindError)`: If the table does not exist (error variant will be
///   `BindError::UnknownTable`) or if any other catalog error occurs.
///
/// # Errors
///
/// - Returns `BindError::UnknownTable` if the table is not found.
/// - Propagates other catalog errors wrapped in `BindError`.
fn check_table(
    catalog: &Catalog,
    txn: &Transaction<'_>,
    table_name: impl AsRef<str>,
) -> Result<TableInfo, BindError> {
    Ok(match catalog.get_table_info(txn, table_name.as_ref()) {
        Ok(i) => i,
        Err(CatalogError::TableNotFound { table_name }) => {
            return Err(BindError::unknown_table(table_name));
        }
        Err(other) => return Err(other.into()),
    })
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        PAGE_SIZE, TransactionId, Type,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        index::hash::HashIndex,
        parser::statements::{ColumnDef, CreateTableStatement, DropStatement},
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn make_catalog_and_txn_mgr(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    /// Variant of [`make_catalog_and_txn_mgr`] that also exposes the underlying
    /// `PageStore` and `Wal` so index tests can build and register a real
    /// `HashIndex` against the catalog.
    fn make_full_infra(dir: &Path) -> (Catalog, TransactionManager, Arc<PageStore>, Arc<Wal>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(Arc::clone(&wal), Arc::clone(&bp));
        (catalog, txn_mgr, bp, wal)
    }

    /// Builds a small live `HashIndex` and registers it under `index_name` for
    /// `table_file_id`. The index itself is never queried by these tests — the
    /// binder only consults the registry — but the catalog API requires a real
    /// `AnyIndex`, so we stand one up over a freshly-created data file.
    fn register_test_index(
        catalog: &Catalog,
        bp: &Arc<PageStore>,
        wal: &Wal,
        dir: &Path,
        table_file_id: FileId,
        index_name: &str,
        idx_file_id: FileId,
    ) {
        const NUM_BUCKETS: u32 = 4;
        let path = dir.join(format!("idx_{}.dat", idx_file_id.0));
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(NUM_BUCKETS) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);
        bp.register_file(idx_file_id, &path).unwrap();

        let hash = HashIndex::new(
            idx_file_id,
            vec![Type::Int64],
            NUM_BUCKETS,
            Arc::clone(bp),
            NUM_BUCKETS,
        );
        let init_txn = TransactionId::new(idx_file_id.0);
        wal.log_begin(init_txn).unwrap();
        hash.init(init_txn).unwrap();
        bp.release_all(init_txn);

        catalog
            .register_index(index_name, table_file_id, hash.into())
            .unwrap();
    }

    fn create_index_stmt(
        index_name: &str,
        table_name: &str,
        columns: &[&str],
        kind: IndexKind,
        if_not_exists: bool,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
            columns: columns
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            index_type: kind,
            if_not_exists,
        }
    }

    fn drop_index_stmt(
        index_name: &str,
        table_name: &str,
        if_exists: bool,
    ) -> crate::parser::statements::DropIndexStatement {
        crate::parser::statements::DropIndexStatement {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            if_exists,
        }
    }

    fn col(name: &str, ty: Type, pk: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            col_type: ty,
            nullable: true,
            primary_key: pk,
            auto_increment: false,
            default: None,
        }
    }

    fn create_stmt(
        name: &str,
        cols: Vec<ColumnDef>,
        if_not_exists: bool,
        table_pk: Option<&str>,
    ) -> CreateTableStatement {
        CreateTableStatement {
            table_name: name.to_string(),
            if_not_exists,
            columns: cols,
            primary_key: table_pk.into_iter().map(str::to_string).collect(),
        }
    }

    fn drop_stmt(name: &str, if_exists: bool) -> DropStatement {
        DropStatement {
            table_name: name.to_string(),
            if_exists,
        }
    }

    fn two_col_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Uint64).not_null(),
            Field::new("name", Type::String).not_null(),
        ])
    }

    // Existing table is resolved to Drop carrying the canonical name + file_id.
    #[test]
    fn test_bind_drop_existing_table_returns_drop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let bound = BoundDrop::bind(&drop_stmt("users", false), &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundDrop::Drop { name, file_id: fid } => {
                assert_eq!(name, "users");
                assert_eq!(fid, file_id);
            }
            BoundDrop::NoOp { .. } => panic!("expected Drop, got NoOp"),
        }
    }

    // ── error / edge: bind_drop ───────────────────────────────────────────

    // IF EXISTS on a missing table is a NoOp, not an error.
    #[test]
    fn test_bind_drop_missing_with_if_exists_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let bound = BoundDrop::bind(&drop_stmt("ghost", true), &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundDrop::NoOp { name } => assert_eq!(name, "ghost"),
            BoundDrop::Drop { .. } => panic!("expected NoOp, got Drop"),
        }
    }

    // Missing table without IF EXISTS surfaces as BindError::UnknownTable.
    #[test]
    fn test_bind_drop_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let err = match BoundDrop::bind(&drop_stmt("ghost", false), &catalog, &txn) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // ── happy path: bind_create_table ─────────────────────────────────────

    // New table with no PK produces BoundCreateTable::New with primary_key = None.
    #[test]
    fn test_bind_create_table_new_no_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            None,
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New {
                name,
                primary_key,
                schema,
            } => {
                assert_eq!(name, "t");
                assert!(primary_key.is_none());
                assert_eq!(schema.num_fields(), 2);
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Inline `primary_key: true` on a column resolves to that column's index.
    #[test]
    fn test_bind_create_table_inline_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![
                col("a", Type::Int64, false),
                col("b", Type::Int64, true),
                col("c", Type::Int64, false),
            ],
            false,
            None,
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![1]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Table-level PK clause resolves to the named column's index.
    #[test]
    fn test_bind_create_table_table_level_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            Some("b"),
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![1]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // When both inline and table-level PK are present, inline wins (find().or() order).
    #[test]
    fn test_bind_create_table_inline_pk_wins_over_table_level() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, true), col("b", Type::Int64, false)],
            false,
            Some("b"),
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![0]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // ── edge cases: bind_create_table ─────────────────────────────────────

    // IF NOT EXISTS on an existing table returns AlreadyExists with that table's file_id.
    #[test]
    fn test_bind_create_table_if_not_exists_on_existing() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], true, None);
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateTable::AlreadyExists { name, file_id: fid } => {
                assert_eq!(name, "t");
                assert_eq!(fid, file_id);
            }
            BoundCreateTable::New { .. } => panic!("expected AlreadyExists"),
        }
    }

    // Empty column list still binds: ddl.rs itself doesn't enforce non-empty columns.
    #[test]
    fn test_bind_create_table_empty_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt("t", vec![], false, None);
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New {
                primary_key,
                schema,
                ..
            } => {
                assert!(primary_key.is_none());
                assert_eq!(schema.num_fields(), 0);
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // ── error paths: bind_create_table ────────────────────────────────────

    // Existing table without IF NOT EXISTS errors as TableAlreadyExists.
    #[test]
    fn test_bind_create_table_existing_without_if_not_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], false, None);
        let err = match BoundCreateTable::bind(&stmt, &catalog, &txn2) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::TableAlreadyExists(ref n) if n == "t"),
            "unexpected error: {err:?}"
        );
    }

    // Duplicate column names error before PK resolution runs.
    #[test]
    fn test_bind_create_table_duplicate_columns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("a", Type::Int64, false)],
            false,
            None,
        );
        let err = match BoundCreateTable::bind(&stmt, &catalog, &txn) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "a"),
            "unexpected error: {err:?}"
        );
    }

    // Duplicate-column check fires before PK-not-found, even if PK is also bogus.
    #[test]
    fn test_bind_create_table_duplicate_columns_takes_precedence_over_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("a", Type::Int64, false)],
            false,
            Some("missing"),
        );
        let err = match BoundCreateTable::bind(&stmt, &catalog, &txn) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn.commit().unwrap();

        assert!(matches!(err, BindError::DuplicateColumn(_)));
    }

    // Table-level PK referencing a missing column errors as PrimaryKeyNotInColumns.
    #[test]
    fn test_bind_create_table_pk_not_in_columns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            Some("c"),
        );
        let err = match BoundCreateTable::bind(&stmt, &catalog, &txn) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::PrimaryKeyNotInColumns(ref n) if n == "c"),
            "unexpected error: {err:?}"
        );
    }

    // Column-name lookup is case-sensitive: PK "A" with column "a" should fail.
    #[test]
    fn test_bind_create_table_pk_lookup_is_case_sensitive() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], false, Some("A"));
        let err = match BoundCreateTable::bind(&stmt, &catalog, &txn) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        txn.commit().unwrap();

        assert!(matches!(err, BindError::PrimaryKeyNotInColumns(_)));
    }

    // ── happy path: bind_create_index ─────────────────────────────────────

    // Resolves a single column name to its index in the table's schema and
    // preserves the access-method kind from the AST.
    #[test]
    fn test_bind_create_index_new_single_column() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_name_idx", "users", &["name"], IndexKind::Hash, false);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::New {
                index_name,
                table_name,
                table_file_id: tfid,
                column_indices,
                kind,
            } => {
                assert_eq!(index_name, "users_name_idx");
                assert_eq!(table_name, "users");
                assert_eq!(tfid, table_file_id);
                assert_eq!(column_indices, vec![1]);
                assert_eq!(kind, IndexKind::Hash);
            }
            BoundCreateIndex::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Composite indexes: each column resolves to its own offset, in declaration order.
    #[test]
    fn test_bind_create_index_composite_preserves_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let schema = TupleSchema::new(vec![
            Field::new("a", Type::Int64).not_null(),
            Field::new("b", Type::Int64).not_null(),
            Field::new("c", Type::Int64).not_null(),
        ]);
        catalog.create_table(&txn, "t", schema, None).unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        // Order in the AST is (c, a) → indices (2, 0). Order is meaningful: it
        // determines the composite-key layout the index will use.
        let stmt = create_index_stmt("t_ca_idx", "t", &["c", "a"], IndexKind::Btree, false);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::New {
                column_indices,
                kind,
                ..
            } => {
                assert_eq!(column_indices, vec![2, 0]);
                assert_eq!(kind, IndexKind::Btree);
            }
            BoundCreateIndex::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // ── error / edge: bind_create_index ───────────────────────────────────

    // Unknown column inside the column list surfaces as BindError::UnknownColumn,
    // tagged with the table name the binder resolved.
    #[test]
    fn test_bind_create_index_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("ix", "users", &["ghost"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(
                err,
                BindError::UnknownColumn { ref table, ref column }
                    if table == "users" && column == "ghost"
            ),
            "unexpected error: {err:?}"
        );
    }

    // A column listed twice in CREATE INDEX (...) is a static error before any
    // schema lookup — this catches user typos like `(a, a)`.
    #[test]
    fn test_bind_create_index_duplicate_columns_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("ix", "users", &["id", "id"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "id"),
            "unexpected error: {err:?}"
        );
    }

    // CREATE INDEX against a missing table is reshaped from CatalogError::TableNotFound
    // into BindError::UnknownTable so the binder layer's vocabulary stays clean.
    #[test]
    fn test_bind_create_index_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_index_stmt("ix", "ghost", &["x"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn).unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // An index name already in the registry collides — without IF NOT EXISTS this is
    // a hard error.
    #[test]
    fn test_bind_create_index_already_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1000),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_id_idx", "users", &["id"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::IndexAlreadyExists(ref n) if n == "users_id_idx"),
            "unexpected error: {err:?}"
        );
    }

    // With IF NOT EXISTS, a name collision binds as AlreadyExists (a no-op).
    #[test]
    fn test_bind_create_index_if_not_exists_on_existing() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1001),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_id_idx", "users", &["id"], IndexKind::Hash, true);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::AlreadyExists { index_name } => {
                assert_eq!(index_name, "users_id_idx");
            }
            BoundCreateIndex::New { .. } => panic!("expected AlreadyExists"),
        }
    }

    // A registered index name resolves to BoundDropIndex::Drop carrying that name.
    #[test]
    fn test_bind_drop_index_existing_returns_drop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1002),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let bound = BoundDropIndex::bind(
            drop_index_stmt("users_id_idx", "users", false),
            &catalog,
            &txn2,
        )
        .expect("drop should bind");
        txn2.commit().unwrap();

        match bound {
            BoundDropIndex::Drop { index_name } => assert_eq!(index_name, "users_id_idx"),
            BoundDropIndex::NoOp { .. } => panic!("expected Drop, got NoOp"),
        }
    }

    // DROP INDEX IF EXISTS on a missing index is a successful no-op.
    #[test]
    fn test_bind_drop_index_missing_with_if_exists_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let bound =
            BoundDropIndex::bind(drop_index_stmt("ghost_idx", "users", true), &catalog, &txn)
                .expect("if-exists drop should bind");
        txn.commit().unwrap();

        match bound {
            BoundDropIndex::NoOp { index_name } => assert_eq!(index_name, "ghost_idx"),
            BoundDropIndex::Drop { .. } => panic!("expected NoOp, got Drop"),
        }
    }

    // Without IF EXISTS, a missing index is a hard BindError::UnknownIndex.
    #[test]
    fn test_bind_drop_index_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let err =
            BoundDropIndex::bind(drop_index_stmt("ghost_idx", "users", false), &catalog, &txn)
                .unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownIndex(ref n) if n == "ghost_idx"),
            "unexpected error: {err:?}"
        );
    }

    // Existing index + wrong table qualifier is treated as not found for that table.
    #[test]
    fn test_bind_drop_index_existing_wrong_table_errors_without_if_exists() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let users_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        catalog
            .create_table(&txn, "orders", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            users_file_id,
            "users_id_idx",
            FileId::new(1003),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let err = BoundDropIndex::bind(
            drop_index_stmt("users_id_idx", "orders", false),
            &catalog,
            &txn2,
        )
        .unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownIndex(ref n) if n == "users_id_idx"),
            "unexpected error: {err:?}"
        );
    }
}
