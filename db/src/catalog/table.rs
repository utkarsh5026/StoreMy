use std::sync::Arc;

use crate::FileId;
use crate::catalog::CatalogError;
use crate::catalog::manager::{Catalog, TableInfo};
use crate::catalog::systable::{ColumnRow, SystemTable, TableRow};
use crate::heap::file::HeapFile;
use crate::transaction::Transaction;
use crate::tuple::TupleSchema;

impl Catalog {
    pub fn get_table_info(
        &self,
        txn: &Transaction<'_>,
        name: &str,
    ) -> Result<TableInfo, CatalogError> {
        if let Some(info) = self.tables.read().get(name).cloned() {
            return Ok(info);
        }
        let info = self.load_table_from_disk(txn, name)?;
        self.tables.write().insert(name.to_string(), info.clone());
        Ok(info)
    }

    pub fn get_table_heap(&self, file_id: FileId) -> Result<Arc<HeapFile>, CatalogError> {
        self.get_heap(file_id)
            .ok_or_else(|| CatalogError::heap_not_found(file_id))
    }

    pub fn create_table(
        &self,
        txn: &Transaction<'_>,
        name: &str,
        schema: TupleSchema,
        primary_key: Option<Vec<usize>>,
    ) -> Result<FileId, CatalogError> {
        if self.tables.read().contains_key(name) {
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

        let table_info = TableInfo::new(name.to_string(), schema, file_id, file_path, primary_key);
        self.tables
            .write()
            .insert(table_info.name.clone(), table_info);
        Ok(file_id)
    }

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

        self.heaps.write().remove(&table_id);
        self.tables.write().remove(name);

        Ok(())
    }
}
