use std::{
    sync::{Arc, mpsc},
    thread,
};

use parking_lot::Mutex;

use crate::{
    catalog::manager::Catalog,
    engine::{self, EngineError, StatementResult},
    parser::Parser,
};

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
    pub fn execute(&self, sql: impl Into<String>) -> mpsc::Receiver<QueryResult> {
        let (reply_tx, reply_rx) = mpsc::channel::<QueryResult>();
        let job = Job {
            sql: sql.into(),
            reply: reply_tx,
        };

        let _ = self.inner.queue_tx.send(job);
        reply_rx
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
