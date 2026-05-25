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

use std::{collections::HashMap, fs, io, path::PathBuf, sync::Arc, time::Duration};

use thiserror::Error;

use crate::{
    buffer_pool::page_store::PageStore,
    primitives::{Lsn, PageId, TransactionId},
    recovery::{analysis::Analysis, redo::Redo, undo::Undo},
    wal::{WalError, log::TxnStatus, reader::WalReader, writer::Wal},
};

/// File extension used for the master record's write-then-rename staging file.
const MASTER_RECORD_TMP_EXT: &str = "tmp";

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
        let result = Analysis::default().run(&mut reader, checkpoint_lsn)?;
        Redo::new(&result, buffer_pool).run(&mut reader)?;

        // Snapshot before Undo consumes the ATT — callers (e.g. main.rs) log
        // loser counts and we'd lose the information once it drains to empty.
        let snapshot = result.clone();
        Undo::new(wal, buffer_pool, result).run(&mut reader)?;
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
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

    /// Runs fuzzy checkpoints on a fixed interval until the thread is killed.
    ///
    /// Intended to be spawned on a background thread (e.g. via
    /// [`std::thread::spawn`]).  Each iteration sleeps for `interval`, then calls
    /// [`Self::take_checkpoint`].  Failures are logged and retried on the next
    /// tick — the loop never exits on error.
    pub fn checkpoint_loop(&self, wal: &Arc<Wal>, interval: Duration) {
        loop {
            std::thread::sleep(interval);
            match self.take_checkpoint(wal) {
                Ok(lsn) => tracing::debug!(?lsn, "fuzzy checkpoint completed"),
                Err(e) => tracing::error!("checkpoint failed, will retry next interval: {e}"),
            }
        }
    }

    /// Performs one full fuzzy checkpoint and persists its end LSN to the master record.
    ///
    /// This is the coordinator step that ties together WAL checkpointing and the
    /// on-disk master record:
    ///
    /// 1. [`Wal::checkpoint`] writes `CheckpointBegin` / `CheckpointEnd` (with ATT and DPT
    ///    snapshots) and forces them to disk.
    /// 2. The returned `CheckpointEnd` LSN is written to the master record file via
    ///    write-then-rename (`.tmp` → final path) so the update is atomic.
    ///
    /// On the next startup, the master record supplies this LSN to Analysis so
    /// the forward scan can begin from the latest completed checkpoint rather than
    /// LSN 0.  If the process crashes after step 1 but before step 2 completes, the
    /// checkpoint records remain in the WAL but are ignored until a later
    /// checkpoint updates the master record.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::Wal`] if checkpointing or master-record I/O fails.
    #[tracing::instrument(name = "aries_checkpoint", skip(self, wal), err)]
    pub fn take_checkpoint(&self, wal: &Arc<Wal>) -> Result<Lsn, RecoveryError> {
        let end_lsn = wal.checkpoint().map_err(RecoveryError::Wal)?;
        let bytes = u64::from(end_lsn).to_le_bytes();
        let tmp = self.master_path.with_extension(MASTER_RECORD_TMP_EXT);
        fs::write(&tmp, bytes).map_err(|e| RecoveryError::Wal(WalError::Io(e)))?;
        fs::rename(&tmp, &self.master_path).map_err(|e| RecoveryError::Wal(WalError::Io(e)))?;
        Ok(end_lsn)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{analysis::Analysis, *};
    use crate::{
        primitives::{FileId, PageId, PageNumber},
        wal::{reader::WalReader, writer::Wal},
    };

    const NO_BUF: usize = 0;

    fn tid(n: u64) -> TransactionId {
        TransactionId::new(n)
    }

    fn page(n: u32) -> PageId {
        PageId::new(FileId::new(1), PageNumber::new(n))
    }

    /// Creates a WAL and an `Aries` instance sharing the same temp directory.
    fn setup(dir: &tempfile::TempDir) -> (Arc<Wal>, Aries) {
        let wal_path = dir.path().join("wal");
        let master_path = dir.path().join("master");
        let wal = Arc::new(Wal::new(&wal_path, NO_BUF).unwrap());
        let aries = Aries::new(wal_path, master_path);
        (wal, aries)
    }

    /// Reads the raw LSN from the master record file, bypassing `read_master`.
    /// Used to verify the file contents independently of the code under test.
    fn read_master_file(dir: &tempfile::TempDir) -> Option<Lsn> {
        let bytes = std::fs::read(dir.path().join("master")).ok()?;
        if bytes.len() < 8 {
            return None;
        }
        Some(Lsn(u64::from_le_bytes(bytes[..8].try_into().unwrap())))
    }

    // ── master record ────────────────────────────────────────────────────────

    #[test]
    fn take_checkpoint_creates_master_record_file() {
        let dir = tempdir().unwrap();
        let (wal, aries) = setup(&dir);

        aries.take_checkpoint(&wal).unwrap();

        assert!(
            dir.path().join("master").exists(),
            "master record file must be created after the first checkpoint"
        );
    }

    #[test]
    fn master_record_contains_the_returned_lsn() {
        // The 8 bytes written to disk must decode to exactly the LSN that
        // take_checkpoint returned — this is what Analysis reads on the next boot.
        let dir = tempdir().unwrap();
        let (wal, aries) = setup(&dir);

        let end_lsn = aries.take_checkpoint(&wal).unwrap();

        let stored = read_master_file(&dir).expect("master record must exist");
        assert_eq!(stored, end_lsn);
    }

    #[test]
    fn second_checkpoint_overwrites_master_with_newer_lsn() {
        // After two checkpoints the master must point at the most recent one,
        // not the first. Recovery would be unnecessarily long otherwise.
        let dir = tempdir().unwrap();
        let (wal, aries) = setup(&dir);

        let first = aries.take_checkpoint(&wal).unwrap();
        let second = aries.take_checkpoint(&wal).unwrap();

        assert!(second > first, "second checkpoint LSN must be greater");
        let stored = read_master_file(&dir).unwrap();
        assert_eq!(
            stored, second,
            "master must point at the most recent checkpoint"
        );
    }

    #[test]
    fn read_master_returns_none_when_file_absent() {
        // A brand-new database has no master record; Analysis must scan from LSN 0.
        let dir = tempdir().unwrap();
        let (_, aries) = setup(&dir);

        let result = aries.read_master().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn read_master_returns_lsn_after_checkpoint() {
        let dir = tempdir().unwrap();
        let (wal, aries) = setup(&dir);

        let end_lsn = aries.take_checkpoint(&wal).unwrap();

        let read_back = aries.read_master().unwrap();
        assert_eq!(read_back, Some(end_lsn));
    }

    // ── analysis seeding ─────────────────────────────────────────────────────

    #[test]
    fn analysis_seeds_from_checkpoint_and_catches_post_checkpoint_records() {
        // Scenario:
        //   T1: Begin → Insert(page1) → Commit     ← winner, before checkpoint
        //   T2: Begin → Insert(page2)               ← loser,  before checkpoint
        //   [checkpoint]
        //   T3: Begin → Insert(page3)               ← loser,  after  checkpoint
        //
        // Analysis seeded from the checkpoint must:
        //   - exclude T1 (winner — committed before checkpoint)
        //   - include T2 (loser  — was in the ATT snapshot)
        //   - include T3 (loser  — picked up by the forward scan from CheckpointBegin)
        //   - have all three pages in the DPT
        let dir = tempdir().unwrap();
        let (wal, aries) = setup(&dir);

        wal.log_begin(tid(1)).unwrap();
        wal.log_insert(tid(1), page(1), vec![], vec![1]).unwrap();
        wal.log_commit(tid(1)).unwrap();

        wal.log_begin(tid(2)).unwrap();
        wal.log_insert(tid(2), page(2), vec![], vec![2]).unwrap();

        let checkpoint_end_lsn = aries.take_checkpoint(&wal).unwrap();

        wal.log_begin(tid(3)).unwrap();
        wal.log_insert(tid(3), page(3), vec![], vec![3]).unwrap();

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let result = Analysis::default()
            .run(&mut reader, Some(checkpoint_end_lsn))
            .unwrap();

        assert!(
            !result.att.contains_key(&tid(1)),
            "T1 committed — must not be a loser"
        );
        assert!(
            result.att.contains_key(&tid(2)),
            "T2 never committed — must be a loser"
        );
        assert!(
            result.att.contains_key(&tid(3)),
            "T3 never committed — must be a loser"
        );

        assert!(result.dpt.contains_key(&page(1)), "page1 must be in DPT");
        assert!(result.dpt.contains_key(&page(2)), "page2 must be in DPT");
        assert!(result.dpt.contains_key(&page(3)), "page3 must be in DPT");
    }

    #[test]
    fn analysis_without_checkpoint_scans_from_lsn_zero() {
        // When no checkpoint exists (None passed), Analysis must still correctly
        // identify all losers by scanning the whole log from LSN 0.
        let dir = tempdir().unwrap();
        let (wal, _aries) = setup(&dir);

        wal.log_begin(tid(1)).unwrap();
        wal.log_insert(tid(1), page(1), vec![], vec![1]).unwrap();
        // no commit — T1 is a loser

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert!(
            result.att.contains_key(&tid(1)),
            "T1 must be identified as a loser"
        );
        assert!(result.dpt.contains_key(&page(1)));
    }
}
