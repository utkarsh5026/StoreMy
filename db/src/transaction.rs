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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fmt, time};

use thiserror::Error;

use crate::buffer_pool::page_store::{PageStore, PageStoreError};
use crate::primitives::TransactionId;
use crate::wal::writer::{Wal, WalError};

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
}

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
    /// The returned [`Transaction`] borrows `self`, so the manager must outlive
    /// the transaction. The transaction starts in the [`TransactionState::Active`]
    /// state and will abort automatically if dropped without an explicit commit
    /// or abort call.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the BEGIN record cannot be written.
    pub fn begin(&self) -> Result<Transaction<'_>, TransactionError> {
        let id = self.next_txn_id.fetch_add(1, Ordering::AcqRel);
        let txn_id = TransactionId::new(id);
        self.wal.log_begin(txn_id)?;
        Ok(Transaction::new(self, txn_id))
    }

    /// Writes a COMMIT record to the WAL and releases all page locks held by `id`.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Wal`] if the COMMIT record cannot be written.
    fn commit(&self, id: TransactionId) -> Result<(), TransactionError> {
        self.wal.log_commit(id)?;
        self.store.release_all(id);
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
        Ok(())
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

/// An in-progress transaction handle.
///
/// A `Transaction` is obtained from [`TransactionManager::begin`] and
/// represents a single active unit of work. It holds a shared reference to its
/// manager for the duration of its lifetime, so the manager must outlive it.
///
/// Dropping a `Transaction` while it is still [`TransactionState::Active`]
/// automatically triggers an abort so that no locks are left dangling.
pub struct Transaction<'a> {
    state: TransactionState,
    manager: &'a TransactionManager,
    id: TransactionId,
    start_time: time::Instant,
}

impl<'a> Transaction<'a> {
    /// Creates a new `Transaction` in the [`TransactionState::Active`] state.
    ///
    /// Callers should prefer [`TransactionManager::begin`], which allocates the
    /// ID and writes the WAL record before constructing the handle.
    pub fn new(manager: &'a TransactionManager, id: TransactionId) -> Self {
        Self {
            state: TransactionState::Active,
            manager,
            id,
            start_time: time::Instant::now(),
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
    pub fn commit(self) -> Result<CompletedTransaction, TransactionError> {
        self.manager.commit(self.id)?;
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
}

impl Drop for Transaction<'_> {
    /// Aborts the transaction if it is still active when dropped.
    ///
    /// This is a safety net for early returns and panics; prefer calling
    /// [`Transaction::abort`] explicitly so that WAL errors are not silently
    /// swallowed.
    fn drop(&mut self) {
        if self.state == TransactionState::Active {
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
