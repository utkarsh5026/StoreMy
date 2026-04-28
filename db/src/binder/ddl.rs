//! Binding for schema-changing SQL (`CREATE` / `DROP` / `SHOW` on tables and indexes).
//!
//! Sits at the SQL → bound-plan seam for DDL: resolve object names against the catalog inside a
//! [`Transaction`](crate::transaction::Transaction), enforce static rules (duplicate columns,
//! unknown PK columns, index ownership), and hand the executor compact
//! [`Bound*`](crate::binder::Bound) values so it does not repeat lookups.
//!
//! ## Shape
//!
//! - [`BoundDrop`] — `DROP TABLE` / `DROP TABLE IF EXISTS`
//! - [`BoundCreateTable`] — `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS`
//! - [`BoundCreateIndex`] — `CREATE INDEX` / `CREATE INDEX IF NOT EXISTS`
//! - [`BoundDropIndex`] — `DROP INDEX` / `DROP INDEX IF EXISTS`
//! - [`BoundShowIndexes`] — `SHOW INDEXES` / `SHOW INDEXES FROM <table>`
//!
//! ## How it works
//!
//! Single-phase binding (no iterator phases like a physical operator):
//!
//! - **Tables** — validate the column list, build a [`TupleSchema`] in declaration order, resolve
//!   primary key column names to [`ColumnId`](crate::primitives::ColumnId)s (or surface
//!   `AlreadyExists` under `IF NOT EXISTS`).
//! - **Indexes** — resolve the base table and each indexed column name to schema positions; reject
//!   duplicate names in the index column list; check index name collisions against the catalog.
//! - **Drop index** — resolve the index by name, then confirm it belongs to the named table (or
//!   treat as no-op / error per `IF EXISTS`).
//! - **Show indexes** — either list all indexes, or resolve `FROM <table>` to a
//!   [`FileId`](crate::FileId) for filtered listing.
//!
//! ## NULL semantics
//!
//! Does not evaluate scalar expressions. `NULL` only appears indirectly: each
//! [`ColumnDef`](crate::parser::statements::ColumnDef) contributes nullability to the built
//! [`TupleSchema`]; binding does not insert default values or run `CHECK` constraints.
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
        ShowIndexesStatement,
    },
    primitives::ColumnId,
    transaction::Transaction,
    tuple::TupleSchema,
};

/// Bound outcome of `DROP TABLE` / `DROP TABLE IF EXISTS`.
///
/// Either resolves the table to a concrete [`FileId`] for the executor to remove, or records a
/// successful no-op when `IF EXISTS` is set and the name is unknown.
///
/// # SQL examples
///
/// ```sql
/// -- DROP TABLE users;
/// --
/// --   BoundDrop::Drop { name: "users", file_id: <heap FileId> }
/// ```
///
/// ```sql
/// -- DROP TABLE IF EXISTS ghost;
/// --
/// --   When `ghost` is missing:
/// --   BoundDrop::NoOp { name: "ghost" }
/// ```
pub enum BoundDrop {
    Drop { name: String, file_id: FileId },
    NoOp { name: String },
}

impl BoundDrop {
    /// Binds a parsed [`DropStatement`](crate::parser::statements::DropStatement) against the
    /// catalog.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- DROP TABLE users
    /// --   BoundDrop::bind(&stmt, catalog, txn)
    /// --     -> Ok(BoundDrop::Drop { .. })
    ///
    /// -- DROP TABLE IF EXISTS users
    /// --   same `Drop` when the table exists
    ///
    /// -- DROP TABLE IF EXISTS missing
    /// --   -> Ok(BoundDrop::NoOp { name: "missing" }) when catalog has no such table
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable) — table name not in
    ///   the catalog and `IF EXISTS` is false.
    /// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — other catalog read failures
    ///   while resolving the table.
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

/// Bound outcome of `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS`.
///
/// On success either describes a **new** table (`schema` + optional composite primary key as
/// resolved [`ColumnId`](crate::primitives::ColumnId)s), or reports **already exists** under
/// `IF NOT EXISTS` so the executor can skip allocation.
///
/// # SQL examples
///
/// Fixed reference schema for the snippets below: `users(id, name, age)` with binder-style indices
/// `id → 0`, `name → 1`, `age → 2` after [`TupleSchema`] is built in column list order.
///
/// ```sql
/// -- CREATE TABLE users (
/// --   id INT64 NOT NULL PRIMARY KEY,
/// --   name STRING,
/// --   age INT64
/// -- );
/// --
/// --   BoundCreateTable::New {
/// --       name: "users",
/// --       schema: TupleSchema { fields in order id, name, age },
/// --       primary_key: Some(vec![ColumnId(0)]),
/// --   }
/// ```
///
/// ```sql
/// -- CREATE TABLE IF NOT EXISTS users (...);
/// --
/// --   When `users` already exists:
/// --   BoundCreateTable::AlreadyExists { name: "users", file_id: <heap FileId> }
/// ```
pub enum BoundCreateTable {
    New {
        name: String,
        schema: TupleSchema,
        primary_key: Option<Vec<ColumnId>>,
    },
    AlreadyExists {
        name: String,
        file_id: FileId,
    },
}

impl BoundCreateTable {
    /// Binds a parsed [`CreateTableStatement`](crate::parser::statements::CreateTableStatement).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- CREATE TABLE t (a INT64, b STRING, PRIMARY KEY (a))
    /// --   primary_key resolves to the schema index of `a` (here 0)
    ///
    /// -- CREATE TABLE t (id INT64 PRIMARY KEY, email STRING)
    /// --   inline `PRIMARY KEY` on `id` is equivalent to `PRIMARY KEY (id)` for binding
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::TableAlreadyExists`](crate::binder::BindError::TableAlreadyExists) — name
    ///   already taken and `IF NOT EXISTS` is false.
    /// - [`BindError::DuplicateColumn`](crate::binder::BindError::DuplicateColumn) — duplicate
    ///   column name in the `CREATE TABLE` column list.
    /// - [`BindError::PrimaryKeyNotInColumns`](crate::binder::BindError::PrimaryKeyNotInColumns) —
    ///   `PRIMARY KEY (...)` names a column not present in the table definition.
    /// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — catalog read failure on the
    ///   `IF NOT EXISTS` path when the table already exists.
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
            let TableInfo { name, file_id, .. } = catalog.get_table_info(txn, table_name)?;
            return Ok(Self::AlreadyExists { name, file_id });
        }
        Err(BindError::table_already_exists(table_name))
    }

    fn resolve_primary_key(
        columns: &[ColumnDef],
        table_pk: &[String],
        schema: &TupleSchema,
    ) -> Result<Option<Vec<ColumnId>>, BindError> {
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
            Some(
                pk_names
                    .into_iter()
                    .map(|name| {
                        let Some((idx, _)) = schema.field_by_name(name) else {
                            return Err(BindError::PrimaryKeyNotInColumns(name.to_string()));
                        };
                        Ok(idx)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        Ok(primary_key)
    }
}

/// Bound outcome of `CREATE INDEX` / `CREATE INDEX IF NOT EXISTS`.
///
/// Resolves the base table and each indexed column **name** to a zero-based
/// [`ColumnId`](crate::primitives::ColumnId) (schema position) in index declaration order. The
/// executor persists names and uses these indices when registering the live index.
///
/// # SQL examples
///
/// Same reference table `users(id, name, age)` with `id → 0`, `name → 1`, `age → 2`.
///
/// ```sql
/// -- CREATE INDEX users_name_idx ON users (name) USING HASH;
/// --
/// --   BoundCreateIndex::New {
/// --       index_name: "users_name_idx",
/// --       table_name: "users",
/// --       table_file_id: <users heap FileId>,
/// --       column_indices: vec![ColumnId(1)],
/// --       kind: IndexKind::Hash,
/// --   }
/// ```
///
/// ```sql
/// -- CREATE INDEX IF NOT EXISTS users_name_idx ON users (name) USING HASH;
/// --
/// --   When that index name is already registered:
/// --   BoundCreateIndex::AlreadyExists { index_name: "users_name_idx" }
/// ```
#[derive(Debug)]
pub enum BoundCreateIndex {
    New {
        index_name: String,
        table_name: String,
        table_file_id: FileId,
        column_indices: Vec<ColumnId>,
        kind: IndexKind,
    },
    AlreadyExists {
        index_name: String,
    },
}

impl BoundCreateIndex {
    /// Binds a parsed [`CreateIndexStatement`](crate::parser::statements::CreateIndexStatement).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- CREATE INDEX idx ON users (id, name) USING HASH;
    /// --   column_indices: vec![ColumnId(0), ColumnId(1)]  -- declaration order
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable) — `ON` table not
    ///   found.
    /// - [`BindError::UnknownColumn`](crate::binder::BindError::UnknownColumn) — a name in the
    ///   index column list is not a column of that table.
    /// - [`BindError::DuplicateColumn`](crate::binder::BindError::DuplicateColumn) — the same
    ///   column name appears twice in the index column list.
    /// - [`BindError::IndexAlreadyExists`](crate::binder::BindError::IndexAlreadyExists) — index
    ///   name is taken and `IF NOT EXISTS` is false.
    /// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — other catalog failures while
    ///   loading table metadata or checking the index registry.
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
    ) -> Result<Vec<ColumnId>, BindError> {
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

/// Bound outcome of `DROP INDEX <name> ON <table>` / `… IF EXISTS`.
///
/// After resolving the index by name, binding checks that it belongs to the named table. Either
/// returns a concrete drop for the executor, or a no-op when `IF EXISTS` allows ignoring a missing
/// or mismatched index.
///
/// # SQL examples
///
/// ```sql
/// -- DROP INDEX users_email_idx ON users;
/// --
/// --   BoundDropIndex::Drop { index_name: "users_email_idx" }
/// --   (only when that index exists on `users`)
/// ```
///
/// ```sql
/// -- DROP INDEX IF EXISTS ghost_idx ON users;
/// --
/// --   BoundDropIndex::NoOp { index_name: "ghost_idx" }
/// --   when the index is missing, or exists but not on `users`
/// ```
#[derive(Debug)]
pub enum BoundDropIndex {
    Drop { index_name: String },
    NoOp { index_name: String },
}

/// Bound outcome of `SHOW INDEXES` / `SHOW INDEXES FROM <table>`.
///
/// Without `FROM`, the executor lists every index in the catalog. With `FROM`, resolves the table
/// once to `(name, file_id)` so listing can filter index metadata by `table_id` without a second
/// name lookup.
///
/// # SQL examples
///
/// ```sql
/// -- SHOW INDEXES;
/// --
/// --   BoundShowIndexes::AllTables
/// ```
///
/// ```sql
/// -- SHOW INDEXES FROM users;
/// --
/// --   BoundShowIndexes::OneTable { name: "users", file_id: <heap FileId> }
/// ```
#[derive(Debug)]
pub enum BoundShowIndexes {
    AllTables,
    OneTable { name: String, file_id: FileId },
}

impl BoundShowIndexes {
    /// Binds a parsed [`ShowIndexesStatement`](crate::parser::statements::ShowIndexesStatement).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SHOW INDEXES FROM users
    /// --   BoundShowIndexes::bind(&stmt, catalog, txn)
    /// --     -> Ok(BoundShowIndexes::OneTable { .. })
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable) — `FROM` names a table
    ///   that is not in the catalog.
    /// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — other catalog read failures.
    pub(in crate::binder) fn bind(
        stmt: &ShowIndexesStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        match &stmt.0 {
            None => Ok(Self::AllTables),
            Some(name) => {
                let TableInfo { name, file_id, .. } = check_table(catalog, txn, name)?;
                Ok(Self::OneTable { name, file_id })
            }
        }
    }
}

impl BoundDropIndex {
    /// Binds a parsed [`DropIndexStatement`](crate::parser::statements::DropIndexStatement).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- DROP INDEX idx ON t
    /// --   BoundDropIndex::bind(stmt, catalog, txn)
    /// --     -> Ok(BoundDropIndex::Drop { index_name: "idx" })
    /// --   when `idx` exists and is registered on table `t`
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownIndex`](crate::binder::BindError::UnknownIndex) — index name not
    ///   found, or index exists but not on the given table, when `IF EXISTS` is false.
    /// - [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable) — `ON` table not found
    ///   when the index exists and must be checked for ownership.
    /// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — other catalog failures.
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

/// Shared catalog lookup for DDL that names a table (`ON users`, `FROM users`, `DROP TABLE users`).
///
/// Maps [`CatalogError::TableNotFound`](crate::catalog::CatalogError::TableNotFound) to
/// [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable); other catalog errors become
/// [`BindError::Catalog`](crate::binder::BindError::Catalog).
///
/// # Errors
///
/// - [`BindError::UnknownTable`](crate::binder::BindError::UnknownTable) — no such table.
/// - [`BindError::Catalog`](crate::binder::BindError::Catalog) — other catalog read failures.
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
            .register_index(index_name, table_file_id, hash.into(), vec![
                ColumnId::try_from(0usize).unwrap(),
            ])
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

    fn col_id(idx: usize) -> ColumnId {
        ColumnId::try_from(idx).unwrap()
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
                assert_eq!(primary_key, Some(vec![col_id(1)]));
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
                assert_eq!(primary_key, Some(vec![col_id(1)]));
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
                assert_eq!(primary_key, Some(vec![col_id(0)]));
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
                assert_eq!(column_indices, vec![col_id(1)]);
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
                assert_eq!(column_indices, vec![col_id(2), col_id(0)]);
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
