use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use crate::{
    buffer_pool::{double_write::DoubleWriteBuffer, page_store::PageStore},
    catalog::{CatalogError, manager::Catalog},
    database::Database,
    recovery::Aries,
    transaction::TransactionManager,
    wal::{WalError, writer::Wal},
};

const WAL_FILE_NAME: &str = "wal.log";
const MASTER_RECORD_FILE_NAME: &str = "master.rec";
const DOUBLE_WRITE_FILE_NAME: &str = "double_write.bin";
/// Number of double-write buffer slots.  Each slot is one page (4 KiB) plus a
/// 512-byte header.  128 slots ≈ 576 KiB — negligible overhead.
const DWB_SLOTS: usize = 128;
pub const BUFFER_POOL_PAGES: usize = 1028;
pub const WORKER_THREADS: usize = 4;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("database '{0}' already exists")]
    AlreadyExists(String),
    #[error("database '{0}' not found")]
    NotFound(String),
    #[error(
        "invalid database name '{0}': must start with a letter, \
         contain only letters/digits/underscores, and be ≤ 63 characters"
    )]
    InvalidName(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("double-write buffer error: {0}")]
    Dwb(#[from] crate::buffer_pool::double_write::DwbError),
    #[error("crash recovery failed: {0}")]
    Recovery(#[from] crate::recovery::RecoveryError),
}

/// Manages multiple named databases, each stored in its own subdirectory of `root`.
///
/// Each call to `create` spins up a full Database instance (WAL, buffer pool,
/// catalog, transaction manager) in `root/<name>/`. Existing databases are
/// discovered on `initialize` by scanning subdirectory names.
pub struct DatabaseRegistry {
    root: PathBuf,
    dbs: RwLock<HashMap<String, Arc<Database>>>,
}

impl DatabaseRegistry {
    /// Scans `root` for valid database subdirectories and loads each one.
    ///
    /// Directories whose names fail the naming rules are silently skipped.
    /// Directories that fail to boot log a warning and are also skipped so a
    /// single corrupted database doesn't prevent the server from starting.
    pub fn initialize(root: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;

        let mut dbs = HashMap::new();
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !is_valid_name(&name) {
                continue;
            }
            match boot_database(&entry.path()) {
                Ok(db) => {
                    dbs.insert(name, Arc::new(db));
                }
                Err(e) => {
                    tracing::warn!(db = name, error = %e, "skipping database directory — failed to boot");
                }
            }
        }

        Ok(Self {
            root,
            dbs: RwLock::new(dbs),
        })
    }

    /// Returns a sorted list of all known database names.
    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.dbs.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns the handle for `name`, or `None` if no such database exists.
    pub fn get(&self, name: &str) -> Option<Arc<Database>> {
        self.dbs.read().unwrap().get(name).cloned()
    }

    /// Creates a new database at `root/<name>/` and registers it.
    ///
    /// Returns `RegistryError::AlreadyExists` if a database with that name is
    /// already registered, and `RegistryError::InvalidName` if the name breaks
    /// the naming rules.
    pub fn create(&self, name: &str) -> Result<(), RegistryError> {
        if !is_valid_name(name) {
            return Err(RegistryError::InvalidName(name.to_owned()));
        }

        // Take the write lock for the entire create operation so two concurrent
        // requests for the same name can't both pass the existence check.
        let mut dbs = self.dbs.write().unwrap();
        if dbs.contains_key(name) {
            return Err(RegistryError::AlreadyExists(name.to_owned()));
        }

        let db_dir = self.root.join(name);
        std::fs::create_dir_all(&db_dir)?;

        let db = boot_database(&db_dir)?;
        dbs.insert(name.to_owned(), Arc::new(db));
        Ok(())
    }
}

/// `name` must start with an ASCII letter, contain only ASCII
/// alphanumerics/underscores, and be at most 63 characters long.
fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Boots a full database stack at `dir`.
///
/// Startup order: WAL → double-write buffer (torn-page recovery) → buffer pool
/// → ARIES crash recovery → checkpoint thread → catalog → transaction manager.
///
/// This is the single canonical place that knows which filenames and
/// configuration constants belong to a database directory.  Both
/// [`DatabaseRegistry`] and the single-database CLI (`main.rs`) call this
/// function so neither can drift out of sync with the other.
pub fn boot_database(dir: &Path) -> Result<Database, RegistryError> {
    let wal = Arc::new(Wal::new(&dir.join(WAL_FILE_NAME), 0)?);

    let mut dwb = DoubleWriteBuffer::open(dir.join(DOUBLE_WRITE_FILE_NAME), DWB_SLOTS)?;
    dwb.recover_torn_pages()?;
    tracing::info!(db_dir = %dir.display(), "double-write buffer recovery complete");

    let mut buffer_pool = PageStore::new(BUFFER_POOL_PAGES, wal.clone());
    buffer_pool.set_double_write_buffer(dwb);
    let buffer_pool = Arc::new(buffer_pool);

    let aries = Arc::new(Aries::new(
        dir.join(WAL_FILE_NAME),
        dir.join(MASTER_RECORD_FILE_NAME),
    ));
    let recovery = aries.recover(&buffer_pool, &wal)?;
    tracing::info!(
        db_dir        = %dir.display(),
        losers_undone = recovery.att.len(),
        dirty_pages   = recovery.dpt.len(),
        redo_lsn      = ?recovery.redo_lsn,
        "ARIES recovery complete"
    );

    let aries_bg = Arc::clone(&aries);
    let wal_bg = Arc::clone(&wal);
    thread::Builder::new()
        .name(format!("storemy-checkpoint-{}", dir.display()))
        .spawn(move || aries_bg.checkpoint_loop(&wal_bg, CHECKPOINT_INTERVAL))
        .expect("failed to spawn checkpoint thread");

    let catalog = Catalog::initialize(&buffer_pool, &wal, dir)?;
    let txn_mgr = TransactionManager::new(wal, buffer_pool);
    Ok(Database::new(
        Arc::new(catalog),
        Arc::new(txn_mgr),
        WORKER_THREADS,
    ))
}
