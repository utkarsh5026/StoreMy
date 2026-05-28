//! Runs SQL statements through a small worker-backed database facade.
//!
//! This module provides [`Database`], which accepts SQL text, schedules work on
//! background threads, and exposes helper methods for read-only catalog queries.
//! It is the high-level entry point used by callers that want to execute parsed
//! SQL without managing parser and execution wiring directly.

use std::{
    path::Path,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use parking_lot::Mutex;

use crate::{
    PAGE_SIZE,
    buffer_pool::{double_write::DoubleWriteBuffer, page_store::PageStore},
    catalog::{CatalogError, TableInfo, manager::Catalog},
    engine::{Engine, EngineError, StatementResult},
    parser::Parser,
    recovery::{Aries, RecoveryError},
    transaction::TransactionManager,
    wal::{WalError, writer::Wal},
};

const WAL_FILE_NAME: &str = "wal.log";
const MASTER_RECORD_FILE_NAME: &str = "master.rec";
const DOUBLE_WRITE_FILE_NAME: &str = "double_write.bin";

/// Error returned when opening (booting) a database directory.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseOpenError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("double-write buffer error: {0}")]
    Dwb(#[from] crate::buffer_pool::double_write::DwbError),

    #[error("crash recovery failed: {0}")]
    Recovery(#[from] RecoveryError),
}

/// Tunable parameters for booting a [`Database`].
///
/// All fields have sensible defaults via [`Default`]; use struct update syntax to
/// override only what you need:
///
/// ```rust
/// # use storemy::database::DatabaseConfig;
/// let config = DatabaseConfig {
///     buffer_pool_pages: 4096,
///     ..DatabaseConfig::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Number of pages held in the buffer pool.
    pub buffer_pool_pages: usize,
    /// Number of SQL worker threads.
    pub worker_threads: usize,
    /// How often the checkpoint thread fires.
    pub checkpoint_interval: Duration,
    /// Number of double-write buffer slots (each slot ≈ one page + 512 B header).
    pub dwb_slots: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            buffer_pool_pages: 1028,
            worker_threads: 4,
            checkpoint_interval: Duration::from_secs(30),
            dwb_slots: 128,
        }
    }
}

/// Alias for the result type returned by SQL execution.
pub type QueryResult = Result<StatementResult, EngineError>;

struct Job {
    sql: String,
    reply: mpsc::Sender<QueryResult>,
}

struct DatabaseInner {
    catalog: Arc<Catalog>,
    txn_manager: Arc<crate::transaction::TransactionManager>,
    queue_tx: mpsc::Sender<Job>,
}

/// Thread-pool backed database coordinator.
///
/// This is a small façade that accepts SQL strings, schedules them onto a fixed
/// pool of worker threads, and returns results through a receiver.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    /// Opens (or creates on first boot) a database at `dir` using the given `config`.
    ///
    /// Startup order: WAL → double-write buffer (torn-page recovery) → buffer pool
    /// → ARIES crash recovery → checkpoint thread → catalog → transaction manager.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseOpenError`] if any subsystem fails to initialise.
    pub fn open(dir: &Path, config: &DatabaseConfig) -> Result<Self, DatabaseOpenError> {
        let &DatabaseConfig {
            buffer_pool_pages,
            worker_threads,
            checkpoint_interval,
            dwb_slots,
        } = config;

        let wal = Arc::new(Wal::new(&dir.join(WAL_FILE_NAME), 0)?);

        let mut dwb = DoubleWriteBuffer::open(dir.join(DOUBLE_WRITE_FILE_NAME), dwb_slots)?;
        dwb.recover_torn_pages()?;
        tracing::info!(db_dir = %dir.display(), "double-write buffer recovery complete");

        let mut buffer_pool = PageStore::new(buffer_pool_pages, wal.clone());
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
            .spawn(move || aries_bg.checkpoint_loop(&wal_bg, checkpoint_interval))
            .expect("failed to spawn checkpoint thread");

        let catalog = Catalog::initialize(&buffer_pool, dir)?;
        let txn_mgr = Arc::new(TransactionManager::new(
            wal,
            buffer_pool,
            dir.join(WAL_FILE_NAME),
        ));
        Ok(Self::new(Arc::new(catalog), txn_mgr, worker_threads))
    }

    /// Creates a new `Database` with `workers` background worker threads.
    ///
    /// # Panics
    ///
    /// Panics if a worker thread cannot be spawned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use storemy::{catalog::manager::Catalog, database::Database, transaction::TransactionManager};
    ///
    /// # fn example(catalog: Arc<Catalog>, txn_manager: Arc<TransactionManager>) {
    /// let db = Database::new(catalog, txn_manager, 4);
    /// let _ = db;
    /// # }
    /// ```
    pub fn new(
        catalog: Arc<Catalog>,
        txn_manager: Arc<crate::transaction::TransactionManager>,
        workers: usize,
    ) -> Self {
        let (queue_tx, queue_rx) = mpsc::channel::<Job>();
        let queue_rx = Arc::new(Mutex::new(queue_rx));

        let inner = Arc::new(DatabaseInner {
            catalog,
            txn_manager,
            queue_tx,
        });

        for i in 0..workers {
            let queue_rx = Arc::clone(&queue_rx);
            let inner = Arc::clone(&inner);

            thread::Builder::new()
                .name(format!("storemy-db-worker-{i}"))
                .spawn(move || {
                    loop {
                        let Ok(job) = queue_rx.lock().recv() else {
                            break; // channel closed => shutdown
                        };

                        let out = Self::execute_sql(&inner.catalog, &inner.txn_manager, &job.sql);
                        let _ = job.reply.send(out);
                    }
                })
                .expect("failed to spawn database worker thread");
        }

        Self { inner }
    }

    /// Executes a single SQL statement on the worker pool.
    ///
    /// Returns a receiver that yields exactly one `QueryResult`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use storemy::catalog::manager::Catalog;
    /// # use storemy::database::Database;
    /// # use storemy::transaction::TransactionManager;
    /// # fn example(catalog: Arc<Catalog>, txn_manager: Arc<TransactionManager>) {
    /// let db = Database::new(catalog, txn_manager, 2);
    /// let rx = db.execute("SELECT * FROM users");
    /// let result = rx.recv().expect("worker should send one result");
    /// let _ = result;
    /// # }
    /// ```
    pub fn execute(&self, sql: impl Into<String>) -> mpsc::Receiver<QueryResult> {
        let (reply_tx, reply_rx) = mpsc::channel::<QueryResult>();
        let job = Job {
            sql: sql.into(),
            reply: reply_tx,
        };

        let _ = self.inner.queue_tx.send(job);
        reply_rx
    }

    /// Returns metadata for every user table, sorted by name.
    ///
    /// Runs synchronously on the calling thread — this is read-only catalog
    /// I/O (no executor work), so it doesn't go through the worker pool.
    ///
    /// # Errors
    ///
    /// Returns an error if starting a transaction fails, reading table metadata
    /// from the catalog fails, or committing the transaction fails.
    pub fn list_user_tables(&self) -> Result<Vec<TableInfo>, EngineError> {
        let txn = self.inner.txn_manager.begin()?;
        let result = self.inner.catalog.list_tables(&txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// Returns metadata for a single user table by name, or
    /// [`EngineError::Catalog`] if no such table exists.
    ///
    /// # Errors
    ///
    /// Returns an error if starting a transaction fails, looking up the table
    /// fails, or committing the transaction fails.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo, EngineError> {
        let txn = self.inner.txn_manager.begin()?;
        let result = self.inner.catalog.get_table_info(&txn, name)?;
        txn.commit()?;
        Ok(result)
    }

    /// Reads every heap page of `name` and returns the table's metadata
    /// together with each page's raw bytes.
    ///
    /// Used by the visualization endpoint. Each page is read under a shared
    /// lock that is dropped as soon as the bytes are copied; the surrounding
    /// transaction is read-only.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableNotFound`] if `name` is not a registered table or its heap file is not
    ///   open in the catalog.
    /// - [`EngineError::Transaction`] / [`EngineError::Storage`] on lock or I/O failure.
    pub fn read_heap_pages(
        &self,
        name: &str,
    ) -> Result<(TableInfo, Vec<[u8; PAGE_SIZE]>), EngineError> {
        let txn = self.inner.txn_manager.begin()?;
        let info = self.inner.catalog.get_table_info(&txn, name)?;
        let heap = self
            .inner
            .catalog
            .get_heap(info.file_id)
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;

        let n = heap.page_count();
        let mut pages: Vec<[u8; PAGE_SIZE]> = Vec::with_capacity(n as usize);
        for page_no in 0..n {
            let bytes = heap.read_raw_page(page_no, txn.transaction_id())?;
            pages.push(bytes);
        }
        txn.commit()?;
        Ok((info, pages))
    }

    /// Parses and runs `sql` on a worker thread.
    ///
    /// Semicolon-separated statements are executed in order; execution stops at
    /// the first error. On success, returns the result of the last statement.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Parse`] for empty or invalid SQL, or any error
    /// produced while executing a statement.
    fn execute_sql(
        catalog: &Catalog,
        txn_manager: &Arc<TransactionManager>,
        sql: &str,
    ) -> QueryResult {
        let stmts = Parser::new(sql)
            .parse_all()
            .map_err(|e| EngineError::Parse(e.to_string()))?;

        if stmts.is_empty() {
            return Err(EngineError::Parse("empty input".to_string()));
        }

        let engine = Engine::new(catalog, txn_manager);
        let mut last = Err(EngineError::Parse("empty input".to_string()));
        for stmt in stmts {
            last = engine.execute_statement(stmt);
            if last.is_err() {
                break;
            }
        }
        last
    }
}
