//! Column management operations for the system catalog.
//!
//! This module extends [`Catalog`] with methods for mutating column metadata.
//! Currently covers renaming; future operations (ADD COLUMN, DROP COLUMN) will
//! live here as well.

use std::sync::Arc;

use crate::{
    FileId,
    catalog::{
        CatalogError,
        manager::{Catalog, TableInfo},
        systable::{ColumnRow, PrimaryKeyColumnRow},
    },
    parser::statements::ColumnDef,
    primitives::{ColumnId, NonEmptyString},
    transaction::Transaction,
    tuple::TupleSchema,
};

impl Catalog {
    /// Renames a column for the given table in the catalog.
    ///
    /// This updates the `Columns` system table row for `(table_id, old_name)` to
    /// use `new_name`, preserving the column's type, position, and nullability.
    ///
    /// Note that this does **not** rewrite table data on disk. Any other catalog
    /// state that references columns by name (if present) must be updated by
    /// the caller as part of the same transaction.
    ///
    /// This function does not currently enforce uniqueness of `new_name` within
    /// `table_id`. Callers should ensure no existing column already has
    /// `new_name` to avoid creating duplicate column names in the catalog.
    ///
    /// # Errors
    ///
    /// - Returns [`CatalogError::ColumnNotFound`] if `(table_id, old_name)` does not exist.
    /// - Returns an error if the new column metadata fails validation or the system table update
    ///   fails.
    pub fn rename_column(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), CatalogError> {
        let table = self.get_table_info_by_id(txn, table_id)?;
        let column = self.get_table_col(txn, &table.name, table_id, old_name)?;

        self.delete_column(txn, table_id, old_name)?;

        let new_column = ColumnRow {
            table_id,
            column_name: NonEmptyString::new(new_name)
                .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
            column_type: column.column_type,
            position: column.position,
            nullable: column.nullable,
            is_dropped: false,
            missing_default_value: None,
        };
        self.insert_systable_tuple(txn, &new_column)?;
        self.refresh_table_schema(txn, table)
    }

    /// Drops a non-primary-key column from a table in the catalog.
    ///
    /// The operation:
    /// 1. Resolves the target table and column metadata.
    /// 2. Verifies the column is not part of the primary key.
    /// 3. Deletes the column row from the `Columns` system table.
    /// 4. Rebuilds the table schema from remaining catalog rows and updates the in-memory
    ///    `user_tables` cache entry for that table.
    ///
    /// This updates catalog metadata only. Existing heap tuples are not
    /// rewritten here; higher layers are responsible for coordinating any data
    /// migration semantics around column drops.
    ///
    /// # Errors
    ///
    /// - Returns [`CatalogError::TableNotFound`] if `table_id` does not resolve to a table.
    /// - Returns [`CatalogError::ColumnNotFound`] if `column_name` is not found in `table_id`.
    /// - Returns [`CatalogError::CannotAlterPrimaryKeyColumn`] if `column_name` is part of the
    ///   table's primary key.
    /// - Propagates system-table scan and write errors.
    pub fn drop_column(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        column_name: &str,
    ) -> Result<(), CatalogError> {
        let table = self.get_table_info_by_id(txn, table_id)?;
        let col = self.get_table_col(txn, &table.name, table_id, column_name)?;

        let pk_cols = self.scan_system_table_where::<PrimaryKeyColumnRow, _>(txn, |r| {
            r.table_id == table_id && r.column_id == col.position
        })?;

        if !pk_cols.is_empty() {
            return Err(CatalogError::cannot_alter_primary_key_column(
                &table.name,
                column_name,
            ));
        }

        self.delete_column(txn, table_id, column_name)?;
        let tombstone = ColumnRow {
            is_dropped: true,
            ..col
        };
        self.insert_systable_tuple(txn, &tombstone)?;
        self.refresh_table_schema(txn, table)
    }

    /// Adds a column to an existing table in the catalog.
    ///
    /// This updates the `Columns` system table by inserting a new [`ColumnRow`]
    /// with a position equal to the current schema's field count, then rebuilds
    /// the table's [`TupleSchema`] from the system catalog and refreshes the
    /// in-memory `user_tables` cache entry.
    ///
    /// This updates catalog metadata only. Existing heap tuples are not
    /// rewritten here; higher layers are responsible for coordinating any data
    /// backfill / default-value semantics for previously-stored rows.
    ///
    /// # Errors
    ///
    /// - Returns [`CatalogError::TableNotFound`] if `table_id` does not resolve to a table.
    /// - Returns [`CatalogError::ColumnAlreadyExists`] if a column with the same name is already
    ///   present in the catalog for this table.
    /// - Returns [`CatalogError::InvalidCatalogRow`] if the computed column position does not fit
    ///   in the on-disk [`ColumnId`] representation.
    /// - Propagates validation errors from [`ColumnRow::new`] and system-table read/write errors.
    pub fn add_column(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        column: ColumnDef,
    ) -> Result<(), CatalogError> {
        let table = self.get_table_info_by_id(txn, table_id)?;
        if self
            .get_table_col(txn, &table.name, table_id, &column.name)
            .is_ok()
        {
            return Err(CatalogError::column_already_exists(table.name, column.name));
        }

        let all_cols =
            self.scan_system_table_where::<ColumnRow, _>(txn, |r| r.table_id == table_id)?;
        let next_pos = all_cols
            .iter()
            .map(|r| u32::from(r.position))
            .max()
            .map_or(0, |m| m + 1);
        let position = ColumnId::try_from(next_pos)
            .map_err(|_| CatalogError::invalid_catalog_row("column position out of range"))?;

        let new_col = ColumnRow {
            table_id,
            column_name: column.name,
            column_type: column.col_type,
            position,
            nullable: column.nullable,
            is_dropped: false,
            missing_default_value: None,
        };
        self.insert_systable_tuple(txn, &new_col)?;
        self.refresh_table_schema(txn, table)
    }

    /// Deletes a column definition from the system catalog's columns table.
    ///
    /// Removes all entries in the system `ColumnRow` table for the column named `column_name`
    /// in the table identified by `table_id`. This does not update any in-memory cache or
    /// table schema; the caller is responsible for evicting/reloading the in-memory metadata.
    ///
    /// # Arguments
    ///
    /// * `txn` - The active catalog transaction.
    /// * `table_id` - The ID of the table whose column should be deleted.
    /// * `column_name` - The name of the column to delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying system table row deletion fails.
    fn delete_column(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        column_name: &str,
    ) -> Result<(), CatalogError> {
        self.delete_systable_rows::<ColumnRow, _>(txn, |r| {
            r.table_id == table_id && r.column_name.as_str() == column_name
        })?;
        Ok(())
    }

    /// Looks up a column definition by name and table in the catalog.
    ///
    /// Searches the system catalog for a column with the given `column_name` in the
    /// table identified by `table_id`. Returns the corresponding [`ColumnRow`] if found;
    /// otherwise, returns [`CatalogError::column_not_found`].
    ///
    /// # Arguments
    ///
    /// * `txn` - Active catalog transaction.
    /// * `table_name` - The table's name (only used for error messages).
    /// * `table_id` - The table's unique file identifier.
    /// * `column_name` - The name of the column to look up.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::column_not_found`] if the column does not exist in the catalog.
    fn get_table_col(
        &self,
        txn: &Transaction<'_>,
        table_name: &str,
        table_id: FileId,
        column_name: &str,
    ) -> Result<ColumnRow, CatalogError> {
        let col = self
            .scan_system_table_where::<ColumnRow, _>(txn, |r| {
                r.table_id == table_id && r.column_name == column_name && !r.is_dropped
            })?
            .pop()
            .ok_or_else(|| CatalogError::column_not_found(table_name, column_name))?;

        Ok(col)
    }

    /// Rebuilds a table's [`TupleSchema`] from the system catalog and writes
    /// the updated [`TableInfo`] back into the in-memory cache.
    ///
    /// Call this after any operation that mutates `CATALOG_COLUMNS` for
    /// `table.file_id` so that the cache stays consistent without requiring a
    /// full evict-and-reload cycle.
    fn refresh_table_schema(
        &self,
        txn: &Transaction<'_>,
        mut table: TableInfo,
    ) -> Result<(), CatalogError> {
        let cols =
            self.scan_system_table_where::<ColumnRow, _>(txn, |r| r.table_id == table.file_id)?;
        table.schema = TupleSchema::from(cols);

        // Re-register the heap with the new schema so that INSERT/scan sees
        // the updated physical layout.  We rebuild rather than mutating
        // in-place because HeapFile.schema is not behind a lock.
        let file_id = table.file_id;
        if let Some(old_heap) = self.get_heap(file_id) {
            let new_heap = crate::heap::file::HeapFile::new(
                file_id,
                table.schema.clone(),
                Arc::clone(&self.buffer_pool),
                old_heap.page_count(),
                Arc::clone(&self.wal),
            );
            self.register_heap(file_id, new_heap);
        }

        let mut cache = self.user_tables.write();
        cache.remove(&table.name);
        cache.insert(table.name.clone(), table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Type,
        buffer_pool::page_store::PageStore,
        catalog::CatalogError,
        primitives::ColumnId,
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
            Field::new("id", Type::Uint64).unwrap().not_null(),
            Field::new("name", Type::String).unwrap().not_null(),
        ])
    }

    fn col(id: usize) -> ColumnId {
        ColumnId::try_from(id).unwrap()
    }

    // Happy path: the new column name is visible after evicting the cache and
    // reloading (rename_column only updates the system table, not the cache).
    #[test]
    fn test_rename_column_new_name_visible_on_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .rename_column(&txn2, file_id, "name", "label")
            .unwrap();
        txn2.commit().unwrap();

        // Evict so get_table_info re-reads from the system tables.
        catalog.user_tables.write().remove("t");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "t").unwrap();
        txn3.commit().unwrap();

        let col_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert!(
            col_names.contains(&"label"),
            "new column name must appear after reload, got: {col_names:?}"
        );
    }

    // The old column name must not appear in the schema after a rename + reload.
    #[test]
    fn test_rename_column_old_name_absent_on_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .rename_column(&txn2, file_id, "name", "label")
            .unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("t");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "t").unwrap();
        txn3.commit().unwrap();

        let col_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert!(
            !col_names.contains(&"name"),
            "old column name must be gone after rename, got: {col_names:?}"
        );
    }

    // Attempting to rename a column that does not exist must return ColumnNotFound.
    #[test]
    fn test_rename_column_nonexistent_returns_column_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.rename_column(&txn2, file_id, "ghost", "specter");
        txn2.commit().unwrap();

        assert!(
            matches!(result, Err(CatalogError::ColumnNotFound { .. })),
            "expected ColumnNotFound for a nonexistent column, got: {result:?}"
        );
    }

    // The renamed column must preserve its type, position, and nullability.
    #[test]
    fn test_rename_column_preserves_type_position_and_nullability() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        // two_col_schema: col 0 = "id" Uint64 not-null, col 1 = "name" String not-null
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.rename_column(&txn2, file_id, "id", "pk").unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("t");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "t").unwrap();
        txn3.commit().unwrap();

        let pk_field = info
            .schema
            .fields()
            .find(|f| f.name == "pk")
            .expect("renamed column 'pk' not found in schema");

        assert_eq!(pk_field.field_type, Type::Uint64, "type must be preserved");
        assert!(!pk_field.nullable, "nullability must be preserved");
    }

    // ── drop_column ───────────────────────────────────────────────────────

    // Dropping a PK column must be rejected.
    #[test]
    fn test_drop_column_primary_key_returns_cannot_alter_primary_key() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), Some(vec![col(0)]))
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.drop_column(&txn2, file_id, "id");
        txn2.commit().unwrap();

        assert!(
            matches!(
                result,
                Err(CatalogError::CannotAlterPrimaryKeyColumn { .. })
            ),
            "expected CannotAlterPrimaryKeyColumn, got: {result:?}"
        );
    }

    // Happy path: dropped column is absent from the in-memory schema immediately.
    #[test]
    fn test_drop_column_absent_from_cache_immediately() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_column(&txn2, file_id, "name").unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        let logical_names: Vec<_> = info
            .schema
            .logical_iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(
            !logical_names.contains(&"name"),
            "dropped column must not appear in logical schema, got: {logical_names:?}"
        );
        assert_eq!(
            info.schema.physical_num_fields(),
            2,
            "physical slot count must be preserved after drop"
        );
    }

    // The surviving column must still be present after the drop.
    #[test]
    fn test_drop_column_sibling_column_survives() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_column(&txn2, file_id, "name").unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        let col_names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert!(
            col_names.contains(&"id"),
            "surviving column must remain in schema, got: {col_names:?}"
        );
    }

    // Schema change must persist across a cache evict + reload cycle.
    #[test]
    fn test_drop_column_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_column(&txn2, file_id, "name").unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("t");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "t").unwrap();
        txn3.commit().unwrap();

        let logical_names: Vec<_> = info
            .schema
            .logical_iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(
            logical_names,
            &["id"],
            "only surviving column must be logical after reload, got: {logical_names:?}"
        );
        assert_eq!(
            info.schema.physical_num_fields(),
            2,
            "physical slot must be preserved across cache evict/reload"
        );
    }

    // Dropping a column that does not exist must return ColumnNotFound.
    #[test]
    fn test_drop_column_nonexistent_returns_column_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.drop_column(&txn2, file_id, "ghost");
        txn2.commit().unwrap();

        assert!(
            matches!(result, Err(CatalogError::ColumnNotFound { .. })),
            "expected ColumnNotFound for a nonexistent column, got: {result:?}"
        );
    }

    // Dropping a non-PK column on a table that has a PK must succeed.
    #[test]
    fn test_drop_column_non_pk_on_table_with_pk_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        // "id" is the PK; "name" is a plain column — dropping "name" must be allowed.
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), Some(vec![col(0)]))
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.drop_column(&txn2, file_id, "name");
        txn2.commit().unwrap();

        assert!(
            result.is_ok(),
            "dropping a non-PK column on a table with a PK must succeed, got: {result:?}"
        );
    }

    fn col_def(name: &str, ty: Type) -> crate::parser::statements::ColumnDef {
        crate::parser::statements::ColumnDef {
            name: NonEmptyString::new(name)
                .expect("test helper `col_def` requires a valid non-empty column name"),
            col_type: ty,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            default: None,
        }
    }

    // ── add_column ────────────────────────────────────────────────────────

    // Adding a new column appends it to the in-memory schema immediately.
    #[test]
    fn test_add_column_appears_in_schema_immediately() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("age", Type::Int64))
            .unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert!(
            names.contains(&"age"),
            "new column must appear in schema immediately, got: {names:?}"
        );
    }

    // The new column must be appended after the existing columns.
    #[test]
    fn test_add_column_appended_at_end() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("age", Type::Int64))
            .unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        // two_col_schema gives (id, name); new column must come after both.
        let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names.last().copied(),
            Some("age"),
            "new column must be last, got: {names:?}"
        );
    }

    // Existing columns must still be present and in the same positions.
    #[test]
    fn test_add_column_existing_columns_unaffected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("extra", Type::Int64))
            .unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(
            &names[..2],
            &["id", "name"],
            "existing columns must remain at their positions, got: {names:?}"
        );
    }

    // The schema change must survive a cache evict + reload from disk.
    #[test]
    fn test_add_column_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("score", Type::Int64))
            .unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("t");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "t").unwrap();
        txn3.commit().unwrap();

        let names: Vec<_> = info.schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            &["id", "name", "score"],
            "schema must persist across reload, got: {names:?}"
        );
    }

    // Adding two columns in sequence must produce schema length 4.
    #[test]
    fn test_add_column_twice_grows_schema() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("a", Type::Int64))
            .unwrap();
        catalog
            .add_column(&txn2, file_id, col_def("b", Type::Int64))
            .unwrap();
        let info = catalog.get_table_info(&txn2, "t").unwrap();
        txn2.commit().unwrap();

        assert_eq!(
            info.schema.physical_num_fields(),
            4,
            "schema must have 4 fields after two adds, got: {}",
            info.schema.physical_num_fields()
        );
    }

    // Adding a column with a name that already exists must fail.
    #[test]
    fn test_add_column_duplicate_name_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let result = catalog.add_column(&txn2, file_id, col_def("name", Type::Int64));
        txn2.commit().unwrap();

        assert!(
            result.is_err(),
            "adding a column whose name already exists must error, got: {result:?}"
        );
    }

    // Dropping a column from a composite PK must be rejected for every PK member.
    #[test]
    fn test_drop_column_composite_pk_member_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), Some(vec![col(0), col(1)]))
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let r1 = catalog.drop_column(&txn2, file_id, "id");
        let r2 = catalog.drop_column(&txn2, file_id, "name");
        txn2.commit().unwrap();

        assert!(
            matches!(r1, Err(CatalogError::CannotAlterPrimaryKeyColumn { .. })),
            "first PK column must be rejected, got: {r1:?}"
        );
        assert!(
            matches!(r2, Err(CatalogError::CannotAlterPrimaryKeyColumn { .. })),
            "second PK column must be rejected, got: {r2:?}"
        );
    }
}
