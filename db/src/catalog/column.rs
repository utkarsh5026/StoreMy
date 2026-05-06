//! Column management operations for the system catalog.
//!
//! This module extends [`Catalog`] with methods for mutating column metadata.
//! Currently covers renaming; future operations (ADD COLUMN, DROP COLUMN) will
//! live here as well.

use crate::{
    FileId,
    catalog::{CatalogError, manager::Catalog, systable::ColumnRow},
    transaction::Transaction,
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
        let column = self
            .scan_system_table_where::<ColumnRow, _>(txn, |r| {
                r.table_id == table_id && r.column_name == old_name
            })?
            .pop()
            .ok_or_else(|| {
                let table_name = self
                    .table_name_by_id(table_id)
                    .unwrap_or_else(|| table_id.to_string());
                CatalogError::column_not_found(table_name, old_name)
            })?;

        self.delete_systable_rows::<ColumnRow, _>(txn, |r| {
            r.table_id == table_id && r.column_name == old_name
        })?;

        let new_column = ColumnRow::new(
            table_id,
            new_name.to_string(),
            column.column_type,
            column.position,
            column.nullable,
        )?;
        self.insert_systable_tuple(txn, &new_column)?;
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
}
