//! Table management operations for the system catalog.
//!
//! This module extends [`Catalog`] with methods for creating, looking up, and
//! dropping user tables. Each operation keeps the in-memory cache of
//! [`TableInfo`] consistent with the on-disk system tables (`pg_tables` and
//! `pg_columns` equivalents) so that callers always see a coherent view.

use std::sync::Arc;

use crate::{
    FileId,
    catalog::{
        CatalogError,
        manager::{Catalog, TableInfo},
        systable::{ColumnRow, SystemTable, TableRow},
    },
    heap::file::HeapFile,
    transaction::Transaction,
    tuple::TupleSchema,
};

impl Catalog {
    /// Returns metadata for a table by name, loading it from disk if not cached.
    ///
    /// Checks the in-memory table cache first. On a miss, reads the system
    /// tables inside `txn`, populates the cache, and returns the result.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::TableNotFound`] if no table with `name` exists
    /// in the system catalog. Propagates any I/O or deserialization errors from
    /// the system table scan.
    pub fn get_table_info(
        &self,
        txn: &Transaction<'_>,
        name: &str,
    ) -> Result<TableInfo, CatalogError> {
        if let Some(info) = self.user_tables.read().get(name).cloned() {
            return Ok(info);
        }
        let info = self.load_table_from_disk(txn, name)?;
        self.user_tables
            .write()
            .insert(name.to_string(), info.clone());
        Ok(info)
    }

    /// Returns true if a table with the given name exists in the catalog.
    ///
    /// This function does not perform any I/O or disk operations.
    pub fn table_exists(&self, name: &str) -> bool {
        self.user_tables.read().contains_key(name)
    }

    /// Returns the [`HeapFile`] for a table identified by its `file_id`.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::HeapNotFound`] if no heap has been registered
    /// for `file_id`. This usually means the table was never opened in the
    /// current process lifetime.
    pub fn get_table_heap(&self, file_id: FileId) -> Result<Arc<HeapFile>, CatalogError> {
        self.get_heap(file_id)
            .ok_or_else(|| CatalogError::heap_not_found(file_id))
    }

    /// Creates a new table with the given schema and registers it in the catalog.
    ///
    /// Allocates a fresh [`FileId`], creates the backing heap file on disk,
    /// writes rows to the `Tables` and `Columns` system tables inside `txn`,
    /// and inserts the table into the in-memory cache.
    ///
    /// `primary_key` is a list of column indices (zero-based) that form the
    /// primary key. Only single-column primary keys are supported; passing more
    /// than one index returns an error. Pass `None` or an empty `Vec` for tables
    /// without a primary key.
    ///
    /// Returns the [`FileId`] assigned to the new table.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::TableAlreadyExists`] if a table named `name` is already in the in-memory
    ///   cache.
    /// - [`CatalogError::InvalidCatalogRow`] if a primary key index is out of bounds, or if more
    ///   than one index is provided.
    /// - Propagates heap creation and system table insertion errors.
    pub fn create_table(
        &self,
        txn: &Transaction<'_>,
        name: &str,
        schema: TupleSchema,
        primary_key: Option<Vec<usize>>,
    ) -> Result<FileId, CatalogError> {
        if self.user_tables.read().contains_key(name) {
            return Err(CatalogError::table_already_exists(name));
        }

        let file_id = self.next_file_id();
        let file_path = self.file_name(name);

        let heap = Self::create_heap_file(
            file_id,
            &file_path,
            schema.clone(),
            self.buffer_pool(),
            self.wal(),
        )?;
        self.register_heap(file_id, heap);

        let primary_key_name = match &primary_key {
            None => None,
            Some(indices) if indices.is_empty() => None,
            Some(indices) if indices.len() == 1 => {
                let i = indices[0];
                let field = schema.field(i).ok_or_else(|| {
                    CatalogError::invalid_catalog_row(format!(
                        "primary key column index {i} out of bounds for schema"
                    ))
                })?;
                Some(field.name.clone())
            }
            Some(_) => {
                return Err(CatalogError::invalid_catalog_row(
                    "composite primary keys are not yet supported in the system catalog",
                ));
            }
        };

        self.insert_systable_tuple(
            txn,
            SystemTable::Tables,
            &TableRow::new(
                file_id,
                name.to_string(),
                file_path.clone(),
                primary_key_name,
            )?,
        )?;

        let columns = ColumnRow::from_schema(file_id, &schema)?;
        for column in columns {
            self.insert_systable_tuple(txn, SystemTable::Columns, &column)?;
        }

        let table_info = TableInfo::new(name, schema, file_id, file_path, primary_key);
        self.user_tables
            .write()
            .insert(table_info.name.clone(), table_info);
        Ok(file_id)
    }

    /// Reads table and column metadata from the system catalog for a table named `name`.
    ///
    /// Scans the `Tables` system table to find the matching [`TableRow`], then
    /// scans `Columns` and filters by `table_id`. Opens (or re-opens) the
    /// backing heap file and returns a fully populated [`TableInfo`].
    ///
    /// This is the slow path called by [`get_table_info`] on a cache miss.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::TableNotFound`] when no row in the `Tables`
    /// system table matches `name`. Propagates scan, deserialization, and heap
    /// open errors.
    fn load_table_from_disk(
        &self,
        txn: &Transaction<'_>,
        name: &str,
    ) -> Result<TableInfo, CatalogError> {
        let table_rows = self.scan_system_table::<TableRow>(txn, SystemTable::Tables)?;
        let row = table_rows
            .into_iter()
            .find(|r| r.table_name == name)
            .ok_or_else(|| CatalogError::table_not_found(name))?;

        let all_cols = self.scan_system_table::<ColumnRow>(txn, SystemTable::Columns)?;
        let cols = all_cols
            .into_iter()
            .filter(|c| c.table_id == row.table_id.0.cast_signed())
            .collect::<Vec<_>>();

        let pk = row.primary_key.as_ref().map(|pk_name| {
            cols.iter()
                .enumerate()
                .filter(|(_, c)| c.column_name == *pk_name)
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        });

        let schema = TupleSchema::from(cols);

        let heap = self.open_existing_heap(
            row.table_id,
            &row.file_path,
            &row.table_name,
            schema.clone(),
        )?;
        self.register_heap(row.table_id, heap);

        Ok(TableInfo::new(
            row.table_name,
            schema,
            row.table_id,
            row.file_path,
            pk,
        ))
    }

    /// Removes a table from the catalog, system tables, in-memory cache, and disk.
    ///
    /// Looks up the table by `name`, deletes its rows from the `Tables` and
    /// `Columns` system tables inside `txn`, removes the heap file from the
    /// filesystem, and evicts the table from the in-memory heap and table caches.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::TableNotFound`] if the table does not exist.
    /// Returns [`CatalogError::Io`] if the backing file cannot be removed.
    /// Propagates system table deletion errors.
    pub fn drop_table(&self, txn: &Transaction<'_>, name: &str) -> Result<(), CatalogError> {
        let table_info = self.get_table_info(txn, name)?;
        let table_id = table_info.file_id;
        let file_path = table_info.file_path;

        self.delete_systable_rows(txn, SystemTable::Tables, |t| {
            TableRow::try_from(t)
                .map(|r| r.table_id == table_id)
                .unwrap_or(false)
        })?;

        let table_id_signed = table_id.0.cast_signed();
        self.delete_systable_rows(txn, SystemTable::Columns, |t| {
            ColumnRow::try_from(t)
                .map(|r| r.table_id == table_id_signed)
                .unwrap_or(false)
        })?;

        std::fs::remove_file(file_path)?;

        self.open_heaps.write().remove(&table_id);
        self.user_tables.write().remove(name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, Type,
        buffer_pool::page_store::PageStore,
        catalog::CatalogError,
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

    fn two_col_schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Uint64).not_null(),
            Field::new("name", Type::String).not_null(),
        ])
    }

    fn one_col_schema() -> TupleSchema {
        TupleSchema::new(vec![Field::new("val", Type::Int64).not_null()])
    }

    #[test]
    fn test_get_table_info_cache_hit_returns_same_info() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.name, "users");
        assert_eq!(info.file_id, file_id);
    }

    // Cache miss: evict from in-memory cache and reload from disk.
    #[test]
    fn test_get_table_info_cache_miss_loads_from_disk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let file_id = catalog
            .create_table(&txn, "orders", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("orders");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "orders").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.name, "orders");
        assert_eq!(info.file_id, file_id);
    }

    // Cache miss repopulates the cache: a third call must hit the cache.
    #[test]
    fn test_get_table_info_cache_miss_repopulates_cache() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "items", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("items");

        let txn2 = txn_mgr.begin().unwrap();
        catalog.get_table_info(&txn2, "items").unwrap();
        txn2.commit().unwrap();

        assert!(
            catalog.user_tables.read().contains_key("items"),
            "cache should be repopulated after a miss"
        );
    }

    // ── get_table_info: error paths ───────────────────────────────────────

    // Asking for a table that was never created must return TableNotFound.
    #[test]
    fn test_get_table_info_nonexistent_table_returns_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.get_table_info(&txn, "ghost");
        assert!(
            matches!(result, Err(CatalogError::TableNotFound { .. })),
            "expected TableNotFound, got: {:?}",
            result.err()
        );
        txn.commit().unwrap();
    }

    // ── get_table_heap: happy path ────────────────────────────────────────

    // After create_table the heap is registered and retrievable.
    #[test]
    fn test_get_table_heap_after_create_returns_ok() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let heap = catalog.get_table_heap(file_id);
        assert!(
            heap.is_ok(),
            "expected heap to be registered after create_table"
        );
    }

    // ── get_table_heap: error paths ───────────────────────────────────────

    // Requesting a heap for an unregistered FileId must return HeapNotFound.
    #[test]
    fn test_get_table_heap_unknown_file_id_returns_heap_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, _) = make_catalog_and_txn(dir.path());

        let result = catalog.get_table_heap(FileId(9999));
        assert!(
            matches!(result, Err(CatalogError::HeapNotFound { .. })),
            "expected HeapNotFound, got: {:?}",
            result.err()
        );
    }

    // ── create_table: happy path ──────────────────────────────────────────

    // No primary key (None) must succeed and return a valid FileId.
    #[test]
    fn test_create_table_no_pk_none_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "t1", two_col_schema(), None);
        txn.commit().unwrap();

        assert!(result.is_ok(), "create_table with None pk must succeed");
    }

    // Empty primary key vec behaves the same as None.
    #[test]
    fn test_create_table_no_pk_empty_vec_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "t2", two_col_schema(), Some(vec![]));
        txn.commit().unwrap();

        assert!(
            result.is_ok(),
            "create_table with empty pk vec must succeed"
        );
    }

    // Single-column pk at index 0.
    #[test]
    fn test_create_table_single_pk_index_zero_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "t3", two_col_schema(), Some(vec![0]));
        txn.commit().unwrap();

        assert!(result.is_ok());
    }

    // Single-column pk at last valid index (boundary condition).
    #[test]
    fn test_create_table_single_pk_last_column_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "t4", two_col_schema(), Some(vec![1]));
        txn.commit().unwrap();

        assert!(result.is_ok());
    }

    // Primary key column name stored in TableInfo matches the field name.
    #[test]
    fn test_create_table_pk_name_stored_correctly() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "t5", two_col_schema(), Some(vec![0]))
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "t5").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.primary_key, Some(vec![0]));
    }

    // Schema stored in TableInfo must match what was passed to create_table.
    #[test]
    fn test_create_table_schema_preserved_in_table_info() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "t6", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "t6").unwrap();
        txn2.commit().unwrap();

        let stored_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(stored_names, &["id", "name"]);
    }

    // create_table must create the backing .dat file on disk.
    #[test]
    fn test_create_table_creates_file_on_disk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "things", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let path = dir.path().join("things.dat");
        assert!(
            path.exists(),
            "expected things.dat to exist after create_table"
        );
    }

    // Single-column schema with pk at index 0.
    #[test]
    fn test_create_table_single_column_schema_with_pk_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "tiny", one_col_schema(), Some(vec![0]));
        txn.commit().unwrap();

        assert!(result.is_ok());
    }

    // ── create_table: error paths ─────────────────────────────────────────

    // Creating the same table twice must return TableAlreadyExists.
    #[test]
    fn test_create_table_duplicate_name_returns_already_exists() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "dup", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.create_table(&txn2, "dup", two_col_schema(), None);

        assert!(
            matches!(result, Err(CatalogError::TableAlreadyExists { .. })),
            "expected TableAlreadyExists, got: {result:?}"
        );
        txn2.commit().unwrap();
    }

    // Primary key index out of bounds must return InvalidCatalogRow.
    #[test]
    fn test_create_table_pk_index_oob_returns_invalid_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "bad", two_col_schema(), Some(vec![2]));

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for OOB pk index, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    // A composite (multi-column) primary key must return InvalidCatalogRow.
    #[test]
    fn test_create_table_composite_pk_returns_invalid_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.create_table(&txn, "multi", two_col_schema(), Some(vec![0, 1]));

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for composite pk, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    // After drop, the backing file must no longer exist on disk.
    #[test]
    fn test_drop_table_removes_file_from_disk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "gone", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let path = dir.path().join("gone.dat");
        assert!(path.exists(), "file should exist before drop");

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "gone").unwrap();
        txn2.commit().unwrap();

        assert!(!path.exists(), "file should be deleted after drop");
    }

    // After drop, the table must be evicted from the in-memory tables cache.
    #[test]
    fn test_drop_table_evicts_from_tables_cache() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "ephemeral", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "ephemeral").unwrap();
        txn2.commit().unwrap();

        assert!(
            !catalog.user_tables.read().contains_key("ephemeral"),
            "tables cache should not contain dropped table"
        );
    }

    // After drop, the heap must be evicted from the heaps registry.
    #[test]
    fn test_drop_table_evicts_from_heaps_cache() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let file_id = catalog
            .create_table(&txn, "temp", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "temp").unwrap();
        txn2.commit().unwrap();

        let result = catalog.get_table_heap(file_id);
        assert!(
            matches!(result, Err(CatalogError::HeapNotFound { .. })),
            "heap should be unregistered after drop"
        );
    }

    // After drop, get_table_info must return TableNotFound.
    #[test]
    fn test_drop_table_get_info_returns_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "vanish", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "vanish").unwrap();
        txn2.commit().unwrap();

        let txn3 = txn_mgr.begin().unwrap();
        let result = catalog.get_table_info(&txn3, "vanish");
        txn3.commit().unwrap();

        assert!(
            matches!(result, Err(CatalogError::TableNotFound { .. })),
            "expected TableNotFound after drop, got: {:?}",
            result.err()
        );
    }

    // Dropping a table that doesn't exist must return TableNotFound.
    #[test]
    fn test_drop_table_nonexistent_returns_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let result = catalog.drop_table(&txn, "nobody");

        assert!(
            matches!(result, Err(CatalogError::TableNotFound { .. })),
            "expected TableNotFound when dropping nonexistent table, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    // Schema field names and pk must survive a full create → evict → reload cycle.
    #[test]
    fn test_load_table_from_disk_schema_round_trips() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "rt", two_col_schema(), Some(vec![0]))
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("rt");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "rt").unwrap();
        txn2.commit().unwrap();

        let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(names, &["id", "name"]);
        assert_eq!(info.primary_key, Some(vec![0]));
    }

    // A table with no primary key reloads with primary_key == None.
    #[test]
    fn test_load_table_from_disk_no_pk_reloads_as_none() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "nopk", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("nopk");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "nopk").unwrap();
        txn2.commit().unwrap();

        assert!(
            info.primary_key.is_none() || info.primary_key == Some(vec![]),
            "expected no primary key on reload, got: {:?}",
            info.primary_key
        );
    }

    // ── invariants ────────────────────────────────────────────────────────

    // FileIds returned by consecutive create_table calls must be distinct.
    #[test]
    fn test_create_table_file_ids_are_unique() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let id1 = catalog
            .create_table(&txn, "a", two_col_schema(), None)
            .unwrap();
        let id2 = catalog
            .create_table(&txn, "b", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        assert_ne!(id1, id2, "each table must receive a unique FileId");
    }

    // Creating a table then dropping it frees the name for re-creation.
    #[test]
    fn test_create_drop_recreate_same_name_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();

        catalog
            .create_table(&txn, "phoenix", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "phoenix").unwrap();
        txn2.commit().unwrap();

        let txn3 = txn_mgr.begin().unwrap();
        let result = catalog.create_table(&txn3, "phoenix", two_col_schema(), None);
        txn3.commit().unwrap();

        assert!(
            result.is_ok(),
            "should be able to recreate a table after dropping it"
        );
    }
}
