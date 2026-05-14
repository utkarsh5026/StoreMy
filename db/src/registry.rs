use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use crate::{
    buffer_pool::page_store::PageStore,
    catalog::{CatalogError, manager::Catalog},
    database::Database,
    transaction::TransactionManager,
    wal::writer::{Wal, WalError},
};

const WAL_FILE_NAME: &str = "wal.log";
const BUFFER_POOL_PAGES: usize = 1028;
const WORKER_THREADS: usize = 4;

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

/// Boots a full database stack at `dir` — WAL → buffer pool → catalog → txn manager → DB.
fn boot_database(dir: &Path) -> Result<Database, RegistryError> {
    let wal = Arc::new(Wal::new(&dir.join(WAL_FILE_NAME), 0)?);
    let buffer_pool = Arc::new(PageStore::new(BUFFER_POOL_PAGES, wal.clone()));
    let catalog = Catalog::initialize(&buffer_pool, &wal, dir)?;
    let txn_mgr = TransactionManager::new(wal, buffer_pool);
    Ok(Database::new(
        Arc::new(catalog),
        Arc::new(txn_mgr),
        WORKER_THREADS,
    ))
}
