//! Runs SQL statements through a small worker-backed database facade.
//!
//! This module provides [`Database`], which accepts SQL text, schedules work on
//! background threads, and exposes helper methods for read-only catalog queries.
//! It is the high-level entry point used by callers that want to execute parsed
//! SQL without managing parser and execution wiring directly.

use std::{
    sync::{Arc, mpsc},
    thread,
};

use parking_lot::Mutex;

use crate::{
    catalog::manager::{Catalog, TableInfo},
    engine::{self, EngineError, StatementResult},
    parser::Parser,
};

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
    /// use db::{catalog::manager::Catalog, database::Database, transaction::TransactionManager};
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

                        let out = execute_sql(&inner.catalog, &inner.txn_manager, &job.sql);
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
    /// # use db::catalog::manager::Catalog;
    /// # use db::database::Database;
    /// # use db::transaction::TransactionManager;
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
}

fn execute_sql(
    catalog: &Catalog,
    txn_manager: &crate::transaction::TransactionManager,
    sql: &str,
) -> QueryResult {
    let mut parser = Parser::new(sql);
    let statement = parser
        .parse()
        .map_err(|e| EngineError::Parse(e.to_string()))?;

    engine::execute_statement(catalog, statement, txn_manager)
}
