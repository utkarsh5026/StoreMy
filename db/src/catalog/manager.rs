//! Core catalog types and infrastructure for managing tables.
//!
//! This module defines [`Catalog`], the central registry that maps table names
//! to their metadata and open heap files. It owns the three system tables
//! (`Tables`, `Columns`, `Indexes`) and exposes the low-level helpers used by
//! the higher-level operations in the sibling `table` and `systable` modules.

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use parking_lot::RwLock;

use crate::{
    FileId, PAGE_SIZE, TransactionId,
    buffer_pool::page_store::PageStore,
    catalog::{
        CatalogError,
        index::LiveIndex,
        systable::{CatalogRow, SystemTable},
    },
    heap::file::HeapFile,
    primitives::{ColumnId, IndexId},
    transaction::Transaction,
    tuple::{Tuple, TupleSchema},
    wal::writer::Wal,
};

/// Metadata for a single user table held in the catalog's in-memory cache.
#[derive(Clone, Debug)]
pub struct TableInfo {
    /// Logical name of the table (matches the key in the `tables` map).
    pub name: String,
    /// Column layout used to encode and decode tuples in this table's heap.
    pub schema: TupleSchema,
    /// Stable numeric identifier for the backing heap file.
    pub file_id: FileId,
    /// Absolute path to the `.dat` file on disk.
    pub file_path: PathBuf,
    /// Zero-based column identifiers of the primary key, in declaration
    /// order, if one was declared.
    ///
    /// `None` means no primary key. The `Vec` holds one entry per PK column,
    /// so composite keys appear as `Some(vec![…, …])`.
    pub primary_key: Option<Vec<ColumnId>>,
}

impl TableInfo {
    /// Constructs a [`TableInfo`] from its component parts.
    pub(super) fn new(
        name: impl Into<String>,
        schema: TupleSchema,
        file_id: FileId,
        file_path: PathBuf,
        primary_key: Option<Vec<ColumnId>>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            file_id,
            file_path,
            primary_key,
        }
    }
}

/// Holds the three open system-table heap files used by the catalog.
///
/// Stored directly (not behind `Arc`) because only [`Catalog`] ever needs them
/// and they are protected by the catalog's own locking discipline.
#[derive(Default)]
pub(super) struct SystemHeaps {
    tables: Option<HeapFile>,
    columns: Option<HeapFile>,
    indexes: Option<HeapFile>,
    primary_key_columns: Option<HeapFile>,
}

impl SystemHeaps {
    /// Registers `file` as the heap for `table`, replacing any prior entry.
    pub(super) fn insert(&mut self, table: SystemTable, file: HeapFile) {
        match table {
            SystemTable::Tables => self.tables = Some(file),
            SystemTable::Columns => self.columns = Some(file),
            SystemTable::Indexes => self.indexes = Some(file),
            SystemTable::PrimaryKeyColumns => self.primary_key_columns = Some(file),
        }
    }

    /// Returns a reference to the heap for `table`, or `None` if not yet registered.
    pub(super) fn get(&self, table: SystemTable) -> Option<&HeapFile> {
        match table {
            SystemTable::Tables => self.tables.as_ref(),
            SystemTable::Columns => self.columns.as_ref(),
            SystemTable::Indexes => self.indexes.as_ref(),
            SystemTable::PrimaryKeyColumns => self.primary_key_columns.as_ref(),
        }
    }
}

/// The system catalog: the single source of truth for table metadata and open heap files.
///
/// `Catalog` is `Send + Sync` — all mutable state is guarded by [`RwLock`] or
/// [`AtomicU64`]. Callers hold a shared `&Catalog` reference and pass a
/// [`Transaction`] into each write operation so that changes are durable and
/// recoverable via the WAL.
pub struct Catalog {
    pub(super) wal: Arc<Wal>,
    pub(super) buffer_pool: Arc<PageStore>,
    data_dir: PathBuf,
    next_file_id: AtomicU64,
    next_index_id: AtomicI64,
    /// Cache of user-table metadata, keyed by table name.
    pub(super) user_tables: RwLock<HashMap<String, TableInfo>>,
    pub(super) open_heaps: RwLock<HashMap<FileId, Arc<HeapFile>>>,
    /// Live secondary indexes registered against each table, keyed by the
    /// table's heap [`FileId`]. Mirrors the role of [`open_heaps`] — pure
    /// in-memory cache, populated by `register_index` (today) and by
    /// replaying `SystemTable::Indexes` rows at database open (next).
    ///
    /// The DML hot path reads this via [`indexes_for`] and clones the
    /// `Arc`s out, so callers don't hold the lock across index operations.
    pub(super) open_indexes: RwLock<HashMap<FileId, Vec<Arc<LiveIndex>>>>,
    /// Index name → live instance. Used by DDL (`DROP INDEX`) and by the
    /// planner when resolving a referenced index name. Stays in sync with
    /// `open_indexes` because `register_index` writes both atomically.
    pub(super) indexes_by_name: RwLock<HashMap<String, Arc<LiveIndex>>>,
    pub(super) system: SystemHeaps,
}

impl Catalog {
    /// Opens or creates the catalog at `data_dir`, initializing all system tables.
    ///
    /// For each of the three system tables (`Tables`, `Columns`, `Indexes`),
    /// this function creates the heap file if it does not exist, or opens it if
    /// it does. Every existing tuple is then validated against the expected
    /// schema so that a corrupt catalog is detected at startup rather than at
    /// query time.
    ///
    /// The first user-assigned [`FileId`] starts at `100`, leaving the range
    /// `0..100` reserved for system tables.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Io`] if any heap file cannot be created or read.
    /// Returns [`CatalogError::Corruption`] or [`CatalogError::InvalidCatalogRow`]
    /// if an existing system table contains malformed tuples.
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

            let file =
                Self::create_heap_file(file_id, &file_path, schema.clone(), buffer_pool, wal)?;

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

        let catalog = Self {
            wal: wal.clone(),
            buffer_pool: buffer_pool.clone(),
            data_dir: data_dir.to_path_buf(),
            next_file_id: AtomicU64::new(100),
            next_index_id: AtomicI64::new(1),
            user_tables: RwLock::new(tables),
            open_heaps: RwLock::new(HashMap::new()),
            open_indexes: RwLock::new(HashMap::new()),
            indexes_by_name: RwLock::new(HashMap::new()),
            system,
        };
        catalog.replay_user_objects()?;
        Ok(catalog)
    }

    /// Creates a heap file at `file_path` and registers it with the buffer pool.
    ///
    /// If the file does not yet exist it is created on disk. Page count is
    /// derived from the file size for existing files, and assumed to be `1`
    /// for brand-new files (the buffer pool materializes page 0 on first
    /// access). Used by both `initialize` (system tables) and `create_table`
    /// (user tables).
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Io`] if the file cannot be created or its
    /// metadata cannot be read. Returns [`CatalogError::FileTooLarge`] if the
    /// file size overflows `u32` pages. Propagates buffer-pool registration
    /// failures as [`CatalogError::Io`].
    pub(super) fn create_heap_file(
        file_id: FileId,
        file_path: &Path,
        schema: TupleSchema,
        buffer_pool: &Arc<PageStore>,
        wal: &Arc<Wal>,
    ) -> Result<HeapFile, CatalogError> {
        let fresh = !file_path.exists();
        if fresh {
            File::create(file_path)?;
        }

        buffer_pool
            .register_file(file_id, file_path)
            .map_err(|_| CatalogError::Io(std::io::Error::other("failed to register file")))?;

        // A freshly created file is empty but the buffer pool will materialize
        // page 0 on first access, so we start with 1 page. For existing files
        // we derive the page count from the file size.
        let pages = if fresh {
            1
        } else {
            let bytes = std::fs::metadata(file_path)?.len();
            let p = u32::try_from(bytes.div_ceil(PAGE_SIZE as u64))
                .map_err(|_| CatalogError::FileTooLarge)?;
            p.max(1)
        };

        Ok(HeapFile::new(
            file_id,
            schema,
            buffer_pool.clone(),
            pages,
            wal.clone(),
        ))
    }

    /// Opens an existing heap file at `file_path` and registers it with the buffer pool.
    ///
    /// Unlike [`create_heap_file`], a missing file is treated as catalog
    /// corruption rather than a normal "first open" situation.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if the file does not exist.
    /// Returns [`CatalogError::Io`] if metadata cannot be read or the file
    /// cannot be registered. Returns [`CatalogError::FileTooLarge`] if the
    /// file size overflows `u32` pages.
    pub(super) fn open_existing_heap(
        &self,
        file_id: FileId,
        file_path: &Path,
        table_name: &str,
        schema: TupleSchema,
    ) -> Result<HeapFile, CatalogError> {
        if !file_path.exists() {
            return Err(CatalogError::corruption(
                table_name,
                format!("data file missing: {}", file_path.display()),
            ));
        }

        let bytes = std::fs::metadata(file_path)?.len();
        let pages = u32::try_from(bytes.div_ceil(PAGE_SIZE as u64))
            .map_err(|_| CatalogError::FileTooLarge)?;

        if self.get_heap(file_id).is_none() {
            self.buffer_pool
                .register_file(file_id, file_path)
                .map_err(|_| CatalogError::Io(std::io::Error::other("failed to register file")))?;
        }

        Ok(HeapFile::new(
            file_id,
            schema,
            self.buffer_pool.clone(),
            pages,
            self.wal.clone(),
        ))
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

        for (_, tuple) in scan {
            table.validate_row(&tuple)?;
        }

        Ok(())
    }

    /// Scans every tuple in `T`'s system table and deserializes them into `Vec<T>`.
    ///
    /// The system table is recovered from `T::TABLE` (see [`CatalogRow`]), so
    /// callers only have to name the row type. The scan uses `txn`'s
    /// visibility so only committed tuples visible to the transaction are
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if the system table heap is not
    /// open. Propagates heap scan errors and any deserialization error from
    /// `T::try_from`.
    pub(super) fn scan_system_table<T: CatalogRow>(
        &self,
        txn: &Transaction<'_>,
    ) -> Result<Vec<T>, CatalogError> {
        self.scan_system_table_with_tid(txn.transaction_id())
    }

    /// Same as [`Self::scan_system_table`], but takes a raw [`TransactionId`].
    ///
    /// Used during catalog bootstrap (`initialize`) where no
    /// `TransactionManager` exists yet — system-table contents are committed
    /// data, so any `TransactionId` later than every prior commit sees
    /// everything.
    pub(super) fn scan_system_table_with_tid<T: CatalogRow>(
        &self,
        tid: TransactionId,
    ) -> Result<Vec<T>, CatalogError> {
        let heap = self.get_system_heap(T::TABLE)?;
        let scan = heap.scan(tid).map_err(CatalogError::from)?;
        scan.map(|(_, tuple)| T::try_from(&tuple)).collect()
    }

    /// Scans `T`'s system table, deserializes each tuple into `T`, and returns
    /// only rows for which `keep` returns `true`.
    ///
    /// This is a convenience wrapper around [`scan_system_table`] that avoids
    /// "scan everything then filter" boilerplate at call sites.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if the system table heap is not
    /// open. Propagates heap scan errors and any deserialization error from
    /// `T::try_from`.
    pub(super) fn scan_system_table_where<T, F>(
        &self,
        txn: &Transaction<'_>,
        keep: F,
    ) -> Result<Vec<T>, CatalogError>
    where
        T: CatalogRow,
        F: Fn(&T) -> bool,
    {
        let heap = self.get_system_heap(T::TABLE)?;
        let mut scan = heap
            .scan(txn.transaction_id())
            .map_err(CatalogError::from)?;

        let mut out = Vec::new();
        while let Some((_, tuple)) = fallible_iterator::FallibleIterator::next(&mut scan)? {
            let row = T::try_from(&tuple)?;
            if keep(&row) {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// Encodes `row` and inserts it into its system table under `txn`.
    ///
    /// The destination table is `T::TABLE`. The `for<'a> &'a T: Into<Tuple>`
    /// bound is satisfied by every row type that has a `From<&Row> for Tuple`
    /// impl in [`systable`].
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if the system table heap is not
    /// open. Propagates heap insertion errors as [`CatalogError`].
    pub(super) fn insert_systable_tuple<T>(
        &self,
        txn: &Transaction<'_>,
        row: &T,
    ) -> Result<(), CatalogError>
    where
        T: CatalogRow,
        for<'a> &'a T: Into<Tuple>,
    {
        let heap = self.get_system_heap(T::TABLE)?;
        let tuple: Tuple = row.into();
        heap.insert_tuple(txn.transaction_id(), &tuple)?;
        Ok(())
    }

    /// Deletes every tuple in `T`'s system table whose decoded row satisfies
    /// `predicate`.
    ///
    /// Attempts to decode each tuple into `T` via `TryFrom<&Tuple>`. Tuples
    /// that fail to decode are treated as non-matching (and are not deleted).
    ///
    /// The deletion is recorded in `txn` and will be rolled back if the
    /// transaction aborts.
    pub(super) fn delete_systable_rows<T, F>(
        &self,
        txn: &Transaction<'_>,
        predicate: F,
    ) -> Result<(), CatalogError>
    where
        T: CatalogRow,
        F: Fn(&T) -> bool,
    {
        let heap = self.get_system_heap(T::TABLE)?;
        let tid = txn.transaction_id();
        heap.delete_if(tid, |t| {
            T::try_from(t).map(|r| predicate(&r)).unwrap_or(false)
        })
        .map(|_| ())
        .map_err(CatalogError::from)
    }

    /// Returns the open [`HeapFile`] for `table`, or `None` if not registered.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if the system table heap is not
    /// open.
    #[inline]
    fn get_system_heap(&self, table: SystemTable) -> Result<&HeapFile, CatalogError> {
        self.system
            .get(table)
            .ok_or_else(|| CatalogError::corruption(table.table_name(), "system table not found"))
    }

    /// Returns the directory where all catalog and table files are stored.
    #[inline]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns a reference to the shared buffer pool.
    pub(super) fn buffer_pool(&self) -> &Arc<PageStore> {
        &self.buffer_pool
    }

    /// Returns a reference to the shared write-ahead log.
    pub(super) fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }

    /// Atomically allocates and returns the next unique [`FileId`].
    ///
    /// Uses relaxed ordering — callers must not assume any happens-before
    /// relationship with other threads beyond the uniqueness guarantee.
    pub(super) fn next_file_id(&self) -> FileId {
        FileId(self.next_file_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Atomically allocates the next unique [`IndexId`].
    pub(super) fn next_index_id(&self) -> IndexId {
        IndexId(self.next_index_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Bumps `next_file_id` to at least `floor + 1`.
    ///
    /// Used during catalog replay so the counter resumes past every
    /// [`FileId`] already on disk. Idempotent: if the counter is already
    /// past `floor + 1`, this is a no-op.
    pub(super) fn bump_next_file_id_past(&self, floor: u64) {
        self.next_file_id.fetch_max(floor + 1, Ordering::Relaxed);
    }

    /// Bumps `next_index_id` to at least `floor + 1`. Counterpart of
    /// [`Self::bump_next_file_id_past`] for indexes.
    pub(super) fn bump_next_index_id_past(&self, floor: i64) {
        self.next_index_id.fetch_max(floor + 1, Ordering::Relaxed);
    }

    /// Canonical path for an index file: `<data_dir>/idx_<name>.dat`.
    #[inline]
    pub(super) fn index_file_path(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!("idx_{name}.dat"))
    }

    /// Returns the open [`HeapFile`] for `file_id`, or `None` if not registered.
    pub fn get_heap(&self, file_id: FileId) -> Option<Arc<HeapFile>> {
        self.open_heaps.read().get(&file_id).cloned()
    }

    /// Wraps `heap` in an `Arc` and inserts it into the heap registry under `file_id`.
    pub(super) fn register_heap(&self, file_id: FileId, heap: HeapFile) {
        self.open_heaps.write().insert(file_id, Arc::new(heap));
    }

    /// Returns the canonical path for a table file: `<data_dir>/<name>.dat`.
    #[inline]
    pub(super) fn file_name(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!("{name}.dat"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Value,
        buffer_pool::page_store::PageStore,
        catalog::{
            CatalogError,
            systable::{ColumnRow, SystemTable, TableRow},
        },
        transaction::TransactionManager,
        tuple::Tuple,
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

    // create_heap_file must create the file on disk when it does not exist.
    #[test]
    fn test_create_heap_file_creates_file() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let path = dir.path().join("test_table.dat");
        let schema = SystemTable::Tables.schema();

        assert!(!path.exists());
        let heap = Catalog::create_heap_file(FileId(100), &path, schema, &bp, &wal);
        assert!(heap.is_ok());
        assert!(path.exists());
    }

    // create_heap_file must succeed when the file already exists.
    #[test]
    fn test_create_heap_file_existing_file_ok() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let path = dir.path().join("test_table.dat");
        let schema = SystemTable::Tables.schema();

        File::create(&path).unwrap();
        assert!(path.exists());
        let heap = Catalog::create_heap_file(FileId(100), &path, schema, &bp, &wal);
        assert!(heap.is_ok());
    }

    // create_heap_file must return Io error when the parent directory doesn't exist.
    #[test]
    fn test_create_heap_file_bad_parent_returns_io_error() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let path = dir.path().join("no_such_dir").join("table.dat");
        let schema = SystemTable::Tables.schema();

        let result = Catalog::create_heap_file(FileId(100), &path, schema, &bp, &wal);
        assert!(result.is_err(), "expected Io error for bad parent dir");
        match result.err().unwrap() {
            CatalogError::Io(_) => {}
            e => panic!("expected CatalogError::Io, got: {e}"),
        }
    }

    // open_existing_heap must return Corruption when the file is missing.
    #[test]
    fn test_open_existing_heap_missing_file_yields_corruption() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();

        let ghost = dir.path().join("ghost.dat");
        let schema = SystemTable::Tables.schema();

        let result = catalog.open_existing_heap(FileId(100), &ghost, "ghost_table", schema);
        assert!(result.is_err(), "expected Corruption for missing file");
        match result.err().unwrap() {
            CatalogError::Corruption { .. } => {}
            e => panic!("expected CatalogError::Corruption, got: {e}"),
        }
    }

    // open_existing_heap must succeed when the file exists on disk.
    #[test]
    fn test_open_existing_heap_valid_file_succeeds() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();

        let path = dir.path().join("users.dat");
        File::create(&path).unwrap();
        let schema = SystemTable::Tables.schema();

        let heap = catalog.open_existing_heap(FileId(100), &path, "users", schema);
        assert!(heap.is_ok());
    }

    fn make_catalog_and_txn_mgr(dir: &Path) -> (Catalog, TransactionManager) {
        let (wal, bp) = make_infra(dir);
        let catalog = Catalog::initialize(&bp, &wal, dir).unwrap();
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    // Scanning an empty system table must return an empty vec.
    #[test]
    fn test_scan_system_table_empty_returns_empty_vec() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let rows: Vec<TableRow> = catalog.scan_system_table(&txn).unwrap();
        assert!(rows.is_empty());

        txn.commit().unwrap();
    }

    // After inserting a tuple, scan_system_table must return it deserialized.
    #[test]
    fn test_scan_system_table_returns_inserted_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let heap = catalog.system.get(SystemTable::Tables).unwrap();
        let tuple = Tuple::new(vec![
            Value::Uint64(42),
            Value::String("users".into()),
            Value::String("/data/users.dat".into()),
        ]);

        let insert_txn = txn_mgr.begin().unwrap();
        heap.insert_tuple(insert_txn.transaction_id(), &tuple)
            .unwrap();
        insert_txn.commit().unwrap();

        let txn = txn_mgr.begin().unwrap();
        let rows: Vec<TableRow> = catalog.scan_system_table(&txn).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table_name, "users");
        assert_eq!(rows[0].table_id, FileId(42));

        txn.commit().unwrap();
    }

    // scan_system_table must deserialize multiple rows correctly.
    #[test]
    fn test_scan_system_table_multiple_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let heap = catalog.system.get(SystemTable::Columns).unwrap();
        let tuples = vec![
            Tuple::new(vec![
                Value::Uint64(1),
                Value::String("id".into()),
                Value::Uint32(3), // Type::Int64
                Value::Uint32(0),
                Value::Bool(false),
                Value::Bool(false), // is_dropped
                Value::Null,        // missing_default_value
            ]),
            Tuple::new(vec![
                Value::Uint64(1),
                Value::String("name".into()),
                Value::Uint32(5), // Type::String
                Value::Uint32(1),
                Value::Bool(true),
                Value::Bool(false), // is_dropped
                Value::Null,        // missing_default_value
            ]),
        ];

        let insert_txn = txn_mgr.begin().unwrap();
        for t in &tuples {
            heap.insert_tuple(insert_txn.transaction_id(), t).unwrap();
        }
        insert_txn.commit().unwrap();

        let txn = txn_mgr.begin().unwrap();
        let rows: Vec<ColumnRow> = catalog.scan_system_table(&txn).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].column_name, "id");
        assert_eq!(rows[1].column_name, "name");
        assert!(rows[1].nullable);

        txn.commit().unwrap();
    }

    // scan_system_table must propagate deserialization errors.
    #[test]
    fn test_scan_system_table_bad_tuple_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());

        let heap = catalog.system.get(SystemTable::Columns).unwrap();
        // Invalid: column_type 999 has no Type variant
        let bad_tuple = Tuple::new(vec![
            Value::Uint64(1),
            Value::String("col".into()),
            Value::Uint32(999), // no such Type variant
            Value::Uint32(0),
            Value::Bool(false),
            Value::Bool(false),
            Value::Null,
        ]);

        let insert_txn = txn_mgr.begin().unwrap();
        heap.insert_tuple(insert_txn.transaction_id(), &bad_tuple)
            .unwrap();
        insert_txn.commit().unwrap();

        let txn = txn_mgr.begin().unwrap();
        let result: Result<Vec<ColumnRow>, _> = catalog.scan_system_table(&txn);
        assert!(result.is_err());

        // txn auto-aborts on drop
    }
}
