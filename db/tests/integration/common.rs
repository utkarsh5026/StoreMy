//! Shared fixtures for the integration test suite.
//!
//! Each test gets its own [`tempfile::TempDir`] so it owns an isolated WAL,
//! buffer pool, catalog, and data directory — no cross-test interference.

use std::sync::Arc;

use fallible_iterator::FallibleIterator;
use storemy::{
    FileId, TransactionId,
    buffer_pool::page_store::PageStore,
    catalog::manager::Catalog,
    database::{Database, QueryResult},
    engine::StatementResult,
    transaction::TransactionManager,
    tuple::Tuple,
    wal::writer::Wal,
};
use tempfile::TempDir;

/// A fully wired database plus the [`TempDir`] backing its files.
///
/// The `TempDir` field is named so it stays alive for the duration of the test
/// (otherwise it would be dropped immediately and the data dir wiped).
pub struct TestDb {
    pub db: Database,
    pub catalog: Arc<Catalog>,
    pub txn_manager: Arc<TransactionManager>,
    pub _dir: TempDir,
}

impl TestDb {
    /// Builds a fresh database in a new temp directory.
    pub fn new() -> Self {
        Self::with_workers(2)
    }

    pub fn with_workers(workers: usize) -> Self {
        let dir = tempfile::tempdir().expect("create tempdir");
        let wal = Arc::new(Wal::new(&dir.path().join("wal.log"), 0).expect("create wal"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog =
            Arc::new(Catalog::initialize(&bp, &wal, dir.path()).expect("initialize catalog"));
        let txn_manager = Arc::new(TransactionManager::new(wal, bp));
        let db = Database::new(catalog.clone(), txn_manager.clone(), workers);

        Self {
            db,
            catalog,
            txn_manager,
            _dir: dir,
        }
    }

    /// Submit `sql` to the worker pool and block until the result is back.
    pub fn run(&self, sql: &str) -> QueryResult {
        self.db
            .execute(sql.to_string())
            .recv()
            .expect("worker did not reply")
    }

    /// Same as [`Self::run`] but unwraps the result; convenient for happy paths.
    pub fn run_ok(&self, sql: &str) -> StatementResult {
        self.run(sql)
            .unwrap_or_else(|e| panic!("statement failed: {sql}\n  -> {e}"))
    }

    /// Look up a table's [`FileId`] in its own short transaction.
    pub fn file_id_of(&self, table: &str) -> FileId {
        let txn = self.txn_manager.begin().expect("begin txn");
        let info = self
            .catalog
            .get_table_info(&txn, table)
            .unwrap_or_else(|e| panic!("table '{table}' not in catalog: {e}"));
        let id = info.file_id;
        txn.commit().expect("commit txn");
        id
    }

    /// Read every live tuple from a table by scanning its heap directly.
    ///
    /// Used to verify INSERT/UPDATE/DELETE effects until SELECT is wired up
    /// in the engine.
    pub fn scan_all(&self, table: &str) -> Vec<Tuple> {
        let file_id = self.file_id_of(table);
        let heap = self
            .catalog
            .get_table_heap(file_id)
            .expect("open heap file");

        let txn = self.txn_manager.begin().expect("begin txn");
        let txn_id: TransactionId = txn.transaction_id();
        let mut scan = heap.scan(txn_id).expect("start scan");

        let mut out = Vec::new();
        while let Some((_rid, tuple)) = FallibleIterator::next(&mut scan).expect("scan step") {
            out.push(tuple);
        }
        txn.commit().expect("commit txn");
        out
    }
}
