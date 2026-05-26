//! Transaction management for the database engine.
//!
//! This module provides the core building blocks for ACID-style transactions:
//! a [`TransactionManager`] that hands out [`Transaction`] handles, and a
//! [`CompletedTransaction`] record that captures the outcome after commit or
//! abort.
//!
//! Each transaction is assigned a unique, monotonically increasing
//! [`TransactionId`] and is recorded in the write-ahead log (WAL) so that
//! incomplete transactions can be recovered or rolled back after a crash.
//! Page locks acquired during the transaction are released automatically when
//! the transaction finishes.
//!
//! # Lifecycle
//!
//! ```text
//! TransactionManager::begin()
//!     → Transaction (state: Active)
//!         → Transaction::commit() → CompletedTransaction (state: Committed)
//!         → Transaction::abort()  → CompletedTransaction (state: Aborted)
//!         → drop() while Active   → implicit abort
//! ```

use std::{
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time,
};

use thiserror::Error;

use crate::{
    buffer_pool::page_store::{PageStore, PageStoreError},
    primitives::{Lsn, NonEmptyString, TransactionId},
    wal::{WalError, writer::Wal},
};

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),

    #[error("store: {0}")]
    Store(#[from] PageStoreError),

    #[error("transaction {0} is not active")]
    NotActive(TransactionId),

    #[error("transaction {0} already committed or aborted")]
    AlreadyFinished(TransactionId),

    #[error("savepoint '{0}' does not exist")]
    SavepointNotFound(String),
}

pub struct Active;
pub struct Committed;
pub struct Aborted;

/// Central coordinator for starting and finishing transactions.
///
/// `TransactionManager` owns the WAL writer and the page store, both shared
/// via [`Arc`]. It issues sequentially numbered transaction IDs and writes the
/// corresponding WAL records when a transaction begins, commits, or aborts.
///
/// Typically one `TransactionManager` exists per database instance and is
/// shared across threads.
pub struct TransactionManager {
    next_txn_id: AtomicU64,
    wal: Arc<Wal>,
    store: Arc<PageStore>,
}

impl TransactionManager {
    /// Creates a new `TransactionManager` backed by the given WAL and page store.
    pub fn new(wal: Arc<Wal>, store: Arc<PageStore>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            wal,
            store,
        }
    }

    /// Starts a new transaction and writes a BEGIN record to the WAL.
    ///
    /// The returned [`Transaction`] holds a shared reference to this manager via
    /// [`Arc`]. The transaction starts in the [`TransactionState::Active`] state
    /// and will abort automatically if dropped without an explicit commit or
    /// abort call.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the BEGIN record cannot be written.
    pub fn begin(self: &Arc<Self>) -> Result<ActiveTransaction, TransactionError> {
        let id = self.next_txn_id.fetch_add(1, Ordering::AcqRel);
        let txn_id = TransactionId::new(id);
        self.wal.log_begin(txn_id)?;
        tracing::debug!(txn_id = %txn_id, "txn begin");
        Ok(Transaction::new(Arc::clone(self), txn_id))
    }

    /// Writes a COMMIT record to the WAL and releases all page locks held by `id`.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the COMMIT record cannot be written.
    fn commit(&self, id: TransactionId) -> Result<(), TransactionError> {
        self.wal.log_commit(id)?;
        self.store.release_all(id);
        tracing::debug!(txn_id = %id, "txn commit");
        Ok(())
    }

    /// Writes an ABORT record to the WAL and releases all page locks held by `id`.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the ABORT record cannot be written.
    fn abort(&self, id: TransactionId) -> Result<(), TransactionError> {
        self.wal.log_abort(id)?;
        self.store.release_all(id);
        tracing::warn!(txn_id = %id, "txn abort");
        Ok(())
    }

    /// Writes a `Savepoint` WAL record for `id` and returns the tail LSN
    /// captured immediately before that record was appended.
    fn savepoint(&self, id: TransactionId, name: &str) -> Result<Lsn, TransactionError> {
        let marker_lsn = self.wal.current_lsn();
        self.wal.log_savepoint(id, name)?;
        Ok(marker_lsn)
    }
}

/// The current lifecycle state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionState::Active => write!(f, "ACTIVE"),
            TransactionState::Committed => write!(f, "COMMITTED"),
            TransactionState::Aborted => write!(f, "ABORTED"),
        }
    }
}

/// A named WAL position recorded inside an active transaction.
///
/// Created by `SAVEPOINT <name>` and stored in the owning [`Transaction`].
/// The `lsn` field marks the point in the WAL that a subsequent
/// `ROLLBACK TO SAVEPOINT` must undo back to.
pub struct Savepoint {
    /// The user-visible name (`SAVEPOINT s1` → `"s1"`).
    pub name: NonEmptyString,
    /// WAL byte offset at the moment this savepoint was created.
    /// All log records with LSN > this value belong to work done after
    /// the savepoint and will be undone by `ROLLBACK TO SAVEPOINT`.
    pub lsn: Lsn,
}

/// An in-progress transaction handle.
///
/// A `Transaction` is obtained from [`TransactionManager::begin`] and
/// represents a single active unit of work. It holds a shared [`Arc`] to its
/// manager so the handle is not tied to a borrow of the caller's stack frame.
///
/// Dropping a `Transaction` while it is still [`TransactionState::Active`]
/// automatically triggers an abort so that no locks are left dangling.
pub struct Transaction<S> {
    _state: PhantomData<S>,
    manager: Arc<TransactionManager>,
    id: TransactionId,
    start_time: time::Instant,
    needs_abort: bool,
    savepoints: Vec<Savepoint>,
}

pub type ActiveTransaction = Transaction<Active>;

/// Whether the session is in autocommit mode or inside an explicit transaction.
pub enum TxnContext {
    /// Every statement gets its own auto-committed transaction.
    Autocommit,

    /// User issued BEGIN; an explicit transaction is open.
    Explicit(ActiveTransaction),

    /// A statement inside the explicit transaction failed.
    /// Only ROLLBACK is accepted until the user cleans up.
    Aborted(ActiveTransaction),
}

/// Per-connection session state, including transaction mode.
pub struct Session {
    pub ctx: TxnContext,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            ctx: TxnContext::Autocommit,
        }
    }
}

impl Transaction<Active> {
    /// Creates a new `Transaction` in the [`TransactionState::Active`] state.
    ///
    /// Callers should prefer [`TransactionManager::begin`], which allocates the
    /// ID and writes the WAL record before constructing the handle.
    pub fn new(manager: Arc<TransactionManager>, id: TransactionId) -> Self {
        Self {
            _state: PhantomData,
            manager,
            id,
            start_time: time::Instant::now(),
            needs_abort: true,
            savepoints: Vec::new(),
        }
    }

    /// Commits the transaction and returns a [`CompletedTransaction`] record.
    ///
    /// Writes a COMMIT record to the WAL and releases all page locks held by
    /// this transaction. The `Transaction` is consumed so it cannot be used
    /// after this call.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the COMMIT record cannot be written.
    /// In that case the transaction is effectively lost and the caller should
    /// treat the operation as failed.
    pub fn commit(mut self) -> Result<CompletedTransaction, TransactionError> {
        self.manager.commit(self.id)?;
        self.needs_abort = false;
        Ok(CompletedTransaction {
            id: self.id,
            state: TransactionState::Committed,
            start_time: self.start_time,
            end_time: time::Instant::now(),
        })
    }

    /// Aborts the transaction and returns a [`CompletedTransaction`] record.
    ///
    /// Writes an ABORT record to the WAL and releases all page locks held by
    /// this transaction. The `Transaction` is consumed so it cannot be used
    /// after this call.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the ABORT record cannot be written.
    pub fn abort(self) -> Result<CompletedTransaction, TransactionError> {
        self.manager.abort(self.id)?;
        Ok(CompletedTransaction {
            id: self.id,
            state: TransactionState::Aborted,
            start_time: self.start_time,
            end_time: time::Instant::now(),
        })
    }

    /// Returns the unique transaction identifier assigned to this transaction.
    ///
    /// This ID uniquely identifies the transaction within the database system,
    /// and can be used for logging, tracking, and isolation purposes.
    pub fn transaction_id(&self) -> TransactionId {
        self.id
    }

    /// Records `SAVEPOINT <name>` for this transaction.
    ///
    /// Captures the current WAL tail LSN, appends a savepoint log record, and
    /// pushes the marker onto the in-memory stack. Redefining an existing name
    /// drops any nested savepoints created after it. A later
    /// `ROLLBACK TO SAVEPOINT` will undo all log records with LSN strictly
    /// greater than the captured value.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the savepoint record cannot be written.
    pub fn savepoint(&mut self, name: NonEmptyString) -> Result<(), TransactionError> {
        let lsn = self.manager.savepoint(self.id, name.as_str())?;
        if let Some(pos) = self.savepoints.iter().rposition(|s| s.name == name) {
            self.savepoints.truncate(pos);
        }
        self.savepoints.push(Savepoint { name, lsn });
        Ok(())
    }

    /// Discards a savepoint without rolling back (`RELEASE SAVEPOINT`).
    ///
    /// Removes the named entry and every savepoint pushed after it. Work logged
    /// since the savepoint is kept.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::SavepointNotFound`] when no savepoint with
    /// `name` exists on this transaction.
    pub fn release_savepoint(&mut self, name: &str) -> Result<(), TransactionError> {
        let pos = self.find_savepoint(name)?;
        self.savepoints.truncate(pos);
        Ok(())
    }

    /// Prepares `ROLLBACK TO SAVEPOINT` by locating the marker and trimming the stack.
    ///
    /// Returns the WAL LSN recorded when `name` was created. The caller is
    /// responsible for undoing all log records with LSN greater than this value,
    /// then leaving the stack truncated to include `name` and any savepoints
    /// created before it.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::SavepointNotFound`] when no savepoint with
    /// `name` exists on this transaction.
    pub fn truncate_to_savepoint(&mut self, name: &str) -> Result<Lsn, TransactionError> {
        let pos = self.find_savepoint(name)?;
        let lsn = self.savepoints[pos].lsn;
        self.savepoints.truncate(pos + 1);
        Ok(lsn)
    }

    /// Locates `name` on this transaction's savepoint stack.
    ///
    /// Shared by [`Self::release_savepoint`] and [`Self::truncate_to_savepoint`].
    /// The stack may contain the same name more than once if it was redefined;
    /// SQL resolves names to the **latest** definition, so this scans from the
    /// back ([`Iterator::rposition`]) and returns that index.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::SavepointNotFound`] if `name` is not on the stack.
    fn find_savepoint(&self, name: &str) -> Result<usize, TransactionError> {
        self.savepoints
            .iter()
            .rposition(|s| s.name == name)
            .ok_or_else(|| TransactionError::SavepointNotFound(name.to_owned()))
    }
}

impl<S> Drop for Transaction<S> {
    /// Aborts the transaction if it is still active when dropped.
    ///
    /// This is a safety net for early returns and panics; prefer calling
    /// [`Transaction::abort`] explicitly so that WAL errors are not silently
    /// swallowed.
    fn drop(&mut self) {
        if self.needs_abort {
            let _ = self.manager.abort(self.id);
        }
    }
}

/// An immutable record of a transaction that has finished.
///
/// Returned by [`Transaction::commit`] and [`Transaction::abort`]. Contains
/// the final state, the assigned ID, and the start/end timestamps so callers
/// can measure how long the transaction ran.
pub struct CompletedTransaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub start_time: time::Instant,
    pub end_time: time::Instant,
}

impl CompletedTransaction {
    /// Returns the elapsed time between when the transaction started and when it finished.
    pub fn duration(&self) -> time::Duration {
        self.end_time.duration_since(self.start_time)
    }
}

impl fmt::Display for CompletedTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "txn[{}] status={} duration={:.3}s",
            self.id,
            self.state,
            self.duration().as_secs_f64()
        )
    }
}
