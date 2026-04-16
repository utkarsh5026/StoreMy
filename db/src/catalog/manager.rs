use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
};

use parking_lot::RwLock;

use crate::{
    FileId, PAGE_SIZE, TransactionId,
    buffer_pool::page_store::PageStore,
    catalog::{CatalogError, systable::SystemTable},
    heap::file::HeapFile,
    tuple::TupleSchema,
    wal::writer::Wal,
};

pub struct TableInfo {
    pub name: String,
    pub schema: TupleSchema,
    pub file_id: FileId,
    pub file_path: PathBuf,
    pub primary_key: Option<Vec<usize>>,
}

impl TableInfo {
    fn new(
        name: String,
        schema: TupleSchema,
        file_id: FileId,
        file_path: PathBuf,
        primary_key: Option<Vec<usize>>,
    ) -> Self {
        Self {
            name,
            schema,
            file_id,
            file_path,
            primary_key,
        }
    }
}

#[derive(Default)]
struct SystemHeaps {
    tables: Option<HeapFile>,
    columns: Option<HeapFile>,
    indexes: Option<HeapFile>,
}

impl SystemHeaps {
    fn insert(&mut self, table: SystemTable, file: HeapFile) {
        match table {
            SystemTable::Tables => self.tables = Some(file),
            SystemTable::Columns => self.columns = Some(file),
            SystemTable::Indexes => self.indexes = Some(file),
        }
    }

    fn get(&self, table: SystemTable) -> Option<&HeapFile> {
        match table {
            SystemTable::Tables => self.tables.as_ref(),
            SystemTable::Columns => self.columns.as_ref(),
            SystemTable::Indexes => self.indexes.as_ref(),
        }
    }
}

pub struct Catalog {
    wal: Arc<Wal>,
    buffer_pool: Arc<PageStore>,
    data_dir: PathBuf,
    next_file_id: AtomicU64,
    tables: RwLock<HashMap<String, TableInfo>>,
    system: SystemHeaps,
}

impl Catalog {
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn initialize(
        buffer_pool: &Arc<PageStore>,
        wal: &Arc<Wal>,
        data_dir: &Path,
    ) -> Result<Self, CatalogError> {
        let mut tables = HashMap::new();
        let mut system = SystemHeaps::default();

        for table in SystemTable::ALL {
            let schema = table.schema();
            let file_path = data_dir.join(table.file_name());

            let file_id = table.file_id();
            let pages = Self::create_file_if_not_exists(&file_path)?;

            buffer_pool
                .register_file(file_id, &file_path)
                .map_err(|_| CatalogError::Io(std::io::Error::other("failed to register file")))?;

            let file = HeapFile::new(
                file_id,
                schema.clone(),
                buffer_pool.clone(),
                pages,
                wal.clone(),
            );

            Self::verify_system_heap(*table, &file)?;

            let table_info = TableInfo::new(
                table.table_name().to_string(),
                schema,
                file_id,
                file_path.clone(),
                None,
            );

            tables.insert(table_info.name.clone(), table_info);
            system.insert(*table, file);
        }

        Ok(Self {
            wal: wal.clone(),
            buffer_pool: buffer_pool.clone(),
            data_dir: data_dir.to_path_buf(),
            next_file_id: AtomicU64::new(100),
            tables: RwLock::new(tables),
            system,
        })
    }

    /// Scans every tuple in `heap` and validates it against `table`'s schema and
    /// semantic constraints.
    ///
    /// Called during [`Catalog::initialize`] on each system table file. A
    /// newly-created (empty) file passes immediately. For an existing file, any
    /// tuple that fails structural or semantic validation causes startup to abort
    /// with [`CatalogError::Corruption`] or [`CatalogError::InvalidCatalogRow`].
    fn verify_system_heap(table: SystemTable, heap: &HeapFile) -> Result<(), CatalogError> {
        let scan = heap
            .scan(TransactionId::new(1))
            .map_err(|e| CatalogError::corruption(table.table_name(), e.to_string()))?;

        for tuple in scan {
            table.validate_row(&tuple)?;
        }

        Ok(())
    }

    fn create_file_if_not_exists(path: &Path) -> Result<u32, CatalogError> {
        if !path.exists() {
            File::create(path)?;
            return Ok(0);
        }
        let bytes = std::fs::metadata(path)?.len();
        let page_size = PAGE_SIZE as u64;
        let pages = bytes.div_ceil(page_size);
        let pages = u32::try_from(pages).map_err(|_| CatalogError::FileTooLarge)?;
        Ok(pages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{
        buffer_pool::page_store::PageStore,
        catalog::{CatalogError, systable::SystemTable},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Arc<Wal>, Arc<PageStore>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("failed to create WAL"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        (wal, bp)
    }

    // Fresh directory with no prior state must succeed.
    #[test]
    fn test_initialize_fresh_dir_succeeds() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        assert!(Catalog::initialize(&bp, &wal, dir.path()).is_ok());
    }

    // After initialization all three catalog data files must exist on disk.
    #[test]
    fn test_initialize_creates_all_catalog_files() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        Catalog::initialize(&bp, &wal, dir.path()).unwrap();

        for table in SystemTable::ALL {
            let path = dir.path().join(table.file_name());
            assert!(path.exists(), "expected {path:?} to exist after initialize");
        }
    }

    // data_dir() must return the exact path that was passed to initialize.
    #[test]
    fn test_initialize_data_dir_matches_input() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        assert_eq!(catalog.data_dir(), dir.path());
    }

    // Re-initializing from the same directory with fresh infrastructure must succeed.
    // Exercises create_file_if_not_exists on existing files (non-zero page count
    // branch) and verify_system_heap on a valid but empty heap.
    #[test]
    fn test_initialize_reopen_existing_dir_succeeds() {
        let dir = tempdir().unwrap();

        let (wal1, bp1) = make_infra(dir.path());
        Catalog::initialize(&bp1, &wal1, dir.path()).unwrap();

        // Second pass: fresh WAL file and PageStore, same data directory.
        let wal2 = Arc::new(Wal::new(&dir.path().join("wal2.log"), 0).unwrap());
        let bp2 = Arc::new(PageStore::new(64, wal2.clone()));
        assert!(
            Catalog::initialize(&bp2, &wal2, dir.path()).is_ok(),
            "re-opening an existing catalog must succeed"
        );
    }

    // data_dir() on a reopened catalog must still match the directory.
    #[test]
    fn test_initialize_reopen_data_dir_matches() {
        let dir = tempdir().unwrap();
        let (wal1, bp1) = make_infra(dir.path());
        Catalog::initialize(&bp1, &wal1, dir.path()).unwrap();

        let wal2 = Arc::new(Wal::new(&dir.path().join("wal2.log"), 0).unwrap());
        let bp2 = Arc::new(PageStore::new(64, wal2.clone()));
        let catalog = Catalog::initialize(&bp2, &wal2, dir.path()).unwrap();
        assert_eq!(catalog.data_dir(), dir.path());
    }

    // A data directory path that does not exist must return CatalogError::Io.
    #[test]
    fn test_initialize_nonexistent_dir_returns_io_error() {
        let dir = tempdir().unwrap();
        let ghost = dir.path().join("no_such_subdir");
        let (wal, bp) = make_infra(dir.path());

        match Catalog::initialize(&bp, &wal, &ghost) {
            Ok(_) => panic!("expected an error for a nonexistent directory, got Ok"),
            Err(CatalogError::Io(_)) => {}
            Err(e) => panic!("expected CatalogError::Io, got: {e}"),
        }
    }
}
