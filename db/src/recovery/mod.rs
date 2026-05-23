//! Crash recovery using the ARIES algorithm.
//!
//! Recovery runs three passes over the WAL on every database startup:
//!
//! 1. **Analysis** — scans forward from the last checkpoint to rebuild the Active Transactions
//!    Table (ATT) and Dirty Page Table (DPT).
//! 2. **Redo** — replays every logged change from `redo_lsn` forward, restoring the exact at-crash
//!    state of every data page.
//! 3. **Undo** — rolls back every loser transaction (those left in the ATT after Analysis), writing
//!    Compensation Log Records (CLRs) so that a crash during undo is itself recoverable.
//!
//! The public entry point is [`Aries::recover`].

mod analysis;
mod redo;
mod undo;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use thiserror::Error;

use crate::{
    buffer_pool::page_store::PageStore,
    primitives::{Lsn, PageId, TransactionId},
    wal::{WalError, log::TxnStatus, reader::WalReader, writer::Wal},
};

/// Top-level error for any failure during crash recovery.
#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("analysis failed: {0}")]
    Analysis(#[from] analysis::AnalysisError),

    #[error("redo failed: {0}")]
    Redo(#[from] redo::RedoError),

    #[error("undo failed: {0}")]
    Undo(#[from] undo::UndoError),
}

/// One row in the Active Transactions Table.
///
/// The ATT tracks every transaction that was running (or aborting) when the
/// database crashed.  After Analysis, every entry that remains in the ATT is a
/// *loser* — a transaction whose changes must be rolled back by the Undo pass.
#[derive(Debug, Clone)]
pub struct AttEntry {
    /// Whether the transaction was progressing normally or had already started
    /// rolling back at the time the checkpoint snapshot was taken.
    pub status: TxnStatus,

    /// LSN of the most recent log record written by this transaction.
    ///
    /// Advanced every time Analysis sees any record (Update, Insert, Delete,
    /// CLR, Abort, …) from this transaction.
    pub last_lsn: Lsn,

    /// LSN of the **next** record that the Undo pass should process for this
    /// transaction.
    ///
    /// For a normal Update/Insert/Delete record this equals `last_lsn`.
    /// After a CLR is seen, it jumps to `clr.undo_next_lsn` — skipping past
    /// the record the CLR already compensated.  This is how Undo resumes
    /// correctly after a crash-during-undo: Analysis rebuilds `undo_next_lsn`
    /// from the CLRs already in the log so Undo never re-undoes work it
    /// already did.
    pub undo_next_lsn: Lsn,
}

impl AttEntry {
    /// Creates a fresh entry for a transaction whose `Begin` record is at `lsn`.
    pub fn new_running(lsn: Lsn) -> Self {
        Self {
            status: TxnStatus::Running,
            last_lsn: lsn,
            undo_next_lsn: lsn,
        }
    }

    /// Updates the `last_lsn` and `undo_next_lsn` fields for this ATT entry.
    ///
    /// # Arguments
    ///
    /// * `last_lsn` - The newest log sequence number written by this transaction.
    /// * `undo_next_lsn` - The log sequence number to process next during undo, which may skip some
    ///   records (e.g., after a CLR) to avoid re-undoing.
    pub(super) fn update_lsn(&mut self, last_lsn: Lsn, undo_next_lsn: Lsn) {
        self.last_lsn = last_lsn;
        self.undo_next_lsn = undo_next_lsn;
    }
}

/// One row in the Dirty Page Table.
///
/// The DPT tracks every page that *might* have changes that were not yet
/// flushed to disk at crash time.  The `rec_lsn` field is the **earliest** LSN
/// that dirtied the page since it was last known to be clean.
///
/// Key property: any log record with `lsn < rec_lsn` for this page is
/// guaranteed to be reflected in the on-disk copy already.  The Redo pass
/// exploits this to skip I/O for records it knows are already applied.
#[derive(Debug, Clone)]
pub struct DptEntry {
    /// Earliest LSN that dirtied this page since it was last clean on disk.
    ///
    /// This is a *lower bound* on what might be missing from the data file.
    /// It is set once (the first time a data record touches the page during
    /// the Analysis scan) and never overwritten — later records touching the
    /// same page push `last_lsn` in the ATT but leave `rec_lsn` alone.
    pub rec_lsn: Lsn,
}

/// Everything the Analysis pass produces, consumed by Redo and Undo.
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Active Transactions Table after the forward scan.
    ///
    /// Keys are the transaction IDs of *losers* — transactions that were still
    /// running (or aborting) at crash time and need to be undone.
    pub att: HashMap<TransactionId, AttEntry>,

    /// Dirty Page Table after the forward scan.
    ///
    /// Keys are page IDs of pages that *might* have unflushed changes.
    pub dpt: HashMap<PageId, DptEntry>,

    /// Where the Redo pass must begin scanning.
    ///
    /// Equal to `min(dpt.values().rec_lsn)`, or `Lsn(0)` if the DPT is empty.
    /// Records before this LSN are guaranteed to be on disk already.
    pub redo_lsn: Lsn,
}

impl AnalysisResult {
    /// Computes `redo_lsn` from the current DPT contents.
    ///
    /// Called once after the Analysis scan loop finishes.
    pub fn compute_redo_lsn(dpt: &HashMap<PageId, DptEntry>) -> Lsn {
        dpt.values().map(|e| e.rec_lsn).min().unwrap_or(Lsn(0))
    }
}

/// ARIES crash-recovery manager.
///
/// Holds the paths needed to open the WAL and master record on startup.
/// Each recovery pass is implemented as a method in its own submodule
/// (`analysis`, `redo`, `undo`) following the same pattern as [`Engine`].
///
/// Call [`Aries::recover`] once at database startup before accepting any
/// new transactions.
///
/// [`Engine`]: crate::engine::Engine
pub struct Aries {
    /// Path to the WAL file.
    wal_path: PathBuf,
    /// Path to the master record file.
    ///
    /// The master record is a tiny file (8 bytes) that stores the LSN of the
    /// last completed `CheckpointEnd`.  Analysis reads it to know where to
    /// start scanning.  Written atomically via write-then-rename at the end of
    /// each checkpoint.  If the file does not exist the database has never been
    /// checkpointed and Analysis scans from LSN 0.
    master_path: PathBuf,
}

impl Aries {
    /// Creates a new recovery manager for the given WAL and master record paths.
    pub fn new(wal_path: PathBuf, master_path: PathBuf) -> Self {
        Self {
            wal_path,
            master_path,
        }
    }

    /// Runs all three ARIES passes and returns when the database is consistent.
    ///
    /// Call this once at startup, before the transaction manager begins
    /// accepting work.  `buffer_pool` must already be created; Redo uses it to
    /// fetch and rewrite pages.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError`] if the WAL cannot be opened or any pass fails.
    #[tracing::instrument(name = "aries_recover", skip(self, buffer_pool, wal))]
    pub fn recover(
        &self,
        buffer_pool: &Arc<PageStore>,
        wal: &Arc<Wal>,
    ) -> Result<AnalysisResult, RecoveryError> {
        let checkpoint_lsn = self.read_master()?;
        let mut reader = WalReader::open(&self.wal_path)?;
        let result = Self::run_analysis(&mut reader, checkpoint_lsn)?;
        Self::run_redo(&mut reader, &result, buffer_pool)?;

        // Snapshot before Undo consumes the ATT — callers (e.g. main.rs) log
        // loser counts and we'd lose the information once it drains to empty.
        let snapshot = result.clone();
        Self::run_undo(&mut reader, wal, buffer_pool, result)?;
        Ok(snapshot)
    }

    /// Reads the LSN of the last completed checkpoint from the master record file.
    ///
    /// Returns `None` if the file does not exist (fresh database, no checkpoint
    /// yet).  Returns `Some(lsn)` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::Wal`] on an I/O error other than "not found".
    fn read_master(&self) -> Result<Option<Lsn>, RecoveryError> {
        match std::fs::read(&self.master_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(RecoveryError::Wal(WalError::Io(e))),
            Ok(bytes) => {
                if bytes.len() < 8 {
                    return Ok(None);
                }
                let raw = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                Ok(Some(Lsn(raw)))
            }
        }
    }
}
