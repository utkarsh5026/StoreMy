//! Write-Ahead Log (WAL) writer.
//!
//! This module provides [`Wal`], the component responsible for durably recording
//! every database change before it touches any heap page. The WAL is the primary
//! mechanism for crash recovery: on restart the log can be replayed to bring the
//! database back to a consistent state.
//!
//! # Design overview
//!
//! Records are appended sequentially to a single file. Each record is assigned a
//! monotonically increasing [`Lsn`] (Log Sequence Number) that equals the byte
//! offset at which the record begins. This means LSNs double as file positions,
//! making random-access replay straightforward.
//!
//! Writes are first accumulated in an in-memory buffer. The buffer is flushed to
//! disk either explicitly (via [`Wal::force`]) or by the background [`Wal::flush_loop`]
//! thread. Records larger than the buffer bypass it and are written directly.
//!
//! # Thread safety
//!
//! All mutable state is guarded by a [`Mutex`]. The flush condvar coordinates
//! between callers that need durability guarantees (e.g. [`Wal::log_commit`]) and
//! the background flush thread.

use std::{collections::HashMap, fs, io::Seek, os::unix::fs::FileExt, path::Path};

use parking_lot::{Condvar, Mutex};

use crate::{
    codec::Encode,
    primitives::{Lsn, PageId, TransactionId},
    storage::open_persistent_file,
    wal::{
        WalError,
        log::{LogRecord, LogRecordBody, TxnStatus},
    },
};

/// Per-transaction bookkeeping kept in memory while a transaction is active.
struct TxnInfo {
    /// LSN of the first record written for this transaction (the `Begin` record).
    #[allow(dead_code)]
    first: Lsn,
    /// LSN of the most recent record written for this transaction.
    last: Lsn,
    /// LSN of the next record to process during undo (set when an `Abort` is logged).
    undo_next: Lsn,
}

/// Mutable WAL state, protected as a unit by a single [`Mutex`].
struct WalState {
    /// Active (not yet committed or fully undone) transactions, keyed by ID.
    active_txns: HashMap<TransactionId, TxnInfo>,
    /// Pages modified by buffered or in-flight records, mapped to the LSN of
    /// the *first* write to that page. The buffer pool uses this to determine
    /// the oldest LSN a page must be flushed before a checkpoint.
    dirty_pages: HashMap<PageId, Lsn>,
    /// In-memory write buffer. Records are staged here before being written to
    /// the file in batches.
    buffer: Vec<u8>,
    /// Number of valid bytes currently in `buffer`.
    buf_offset: usize,
    /// LSN that will be assigned to the *next* record written.
    current_at: Lsn,
    /// LSN up to which data has been durably written (and synced) to the file.
    flushed_till: Lsn,
}

impl WalState {
    /// Returns `true` when the write buffer holds no unflushed data.
    pub(super) const fn is_empty(&self) -> bool {
        self.buf_offset == 0
    }

    /// Returns the total capacity of the write buffer in bytes.
    pub(super) const fn len(&self) -> usize {
        self.buffer.len()
    }
}

/// Append-only Write-Ahead Log backed by a single file.
///
/// `Wal` serializes log records for every database mutation and ensures they
/// reach durable storage before the corresponding heap pages are considered
/// committed. It tracks active transactions and the dirty-page table needed by
/// the buffer pool for checkpointing.
///
/// Construct a `Wal` with [`Wal::new`], then spawn a background thread running
/// [`Wal::flush_loop`] to handle asynchronous flushing. Operations that require
/// durability (like [`Wal::log_commit`]) will block until the background thread
/// has flushed the relevant records.
pub struct Wal {
    /// The underlying WAL file. Accessed via `write_at` for positioned I/O so
    /// the file cursor does not need to be kept in sync with concurrent writes.
    file: fs::File,
    /// All mutable state, serialized under a single lock.
    state: Mutex<WalState>,
    /// Notifies the flush loop that new data is available, and notifies waiters
    /// (e.g. callers of [`Wal::force`]) that data has been flushed.
    flush_cond: Condvar,
}

impl Wal {
    /// Opens (or creates) the WAL file at `path` and returns a ready-to-use `Wal`.
    ///
    /// The file is opened in read/write mode without truncation so that an
    /// existing log is preserved for crash recovery. The initial LSN is set to
    /// the current end of the file, so new records are appended after any
    /// previously written data.
    ///
    /// `buf_size` controls the in-memory write buffer size in bytes. A value of
    /// `0` disables buffering — every record is written directly to the file,
    /// which is useful in tests to avoid needing a background flush thread.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the file cannot be opened or its length
    /// cannot be determined.
    pub fn new(path: &Path, buf_size: usize) -> Result<Self, WalError> {
        let mut file = open_persistent_file(path)?;

        let end = file.seek(std::io::SeekFrom::End(0))?;
        let start_lsn = Lsn(end);

        Ok(Self {
            file,
            state: Mutex::new(WalState {
                active_txns: HashMap::new(),
                dirty_pages: HashMap::new(),
                buffer: vec![0u8; buf_size],
                buf_offset: 0,
                current_at: start_lsn,
                flushed_till: start_lsn,
            }),
            flush_cond: Condvar::new(),
        })
    }

    /// Logs a `Begin` record for `tid` and registers it as an active transaction.
    ///
    /// This must be called before any data-modification records (`log_insert`,
    /// `log_update`, `log_delete`) for the given transaction ID.
    ///
    /// Returns the LSN assigned to the `Begin` record.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] or [`WalError::Codec`] if writing the record fails.
    pub fn log_begin(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let mut state = self.state.lock();
        let lsn = Self::write_record(
            &self.file,
            &mut state,
            tid,
            Lsn::INVALID,
            LogRecordBody::Begin,
        )?;
        state.active_txns.insert(tid, TxnInfo {
            first: lsn,
            last: lsn,
            undo_next: Lsn::INVALID,
        });
        Ok(lsn)
    }

    /// Logs a `Commit` record for `tid` and waits until it is durably flushed.
    ///
    /// After this call returns successfully the transaction is no longer tracked
    /// as active. The caller is guaranteed that the commit record has reached
    /// stable storage.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::UnknownTransaction`] if `tid` was never begun or has
    /// already been committed. Returns [`WalError::Io`] or [`WalError::Codec`]
    /// on write failures.
    pub fn log_commit(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let lsn = {
            let mut state = self.state.lock();
            let prev = state
                .active_txns
                .get(&tid)
                .ok_or(WalError::UnknownTransaction(tid))?
                .last;
            let lsn = Self::write_record(&self.file, &mut state, tid, prev, LogRecordBody::Commit)?;
            state.active_txns.get_mut(&tid).unwrap().last = lsn;
            lsn
        };
        self.force(lsn)?;

        self.state.lock().active_txns.remove(&tid);
        Ok(lsn)
    }

    /// Logs an `Abort` record for `tid` and sets up its undo chain.
    ///
    /// Unlike `log_commit`, the transaction is **not** removed from the active
    /// set after this call — it remains registered so that the recovery manager
    /// can walk the undo chain via `undo_next` and roll back its changes.
    ///
    /// Returns the LSN of the `Abort` record.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::UnknownTransaction`] if `tid` is not active.
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    pub fn log_abort(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let mut state = self.state.lock();
        let prev = state
            .active_txns
            .get(&tid)
            .ok_or(WalError::UnknownTransaction(tid))?
            .last;
        let lsn = Self::write_record(&self.file, &mut state, tid, prev, LogRecordBody::Abort)?;
        let info = state.active_txns.get_mut(&tid).unwrap();
        info.last = lsn;
        info.undo_next = lsn;
        Ok(lsn)
    }

    /// Logs an `Update` record describing an in-place change to a page.
    ///
    /// `before` is the page image before the change; `after` is the image after.
    /// Both images are needed so that the record can be used for both redo and
    /// undo during recovery.
    ///
    /// The page is added to the dirty-page table if it is not already present.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::UnknownTransaction`] if `tid` is not active.
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    pub fn log_update(
        &self,
        tid: TransactionId,
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Update {
            page_id,
            before,
            after,
        })
    }

    /// Logs an `Insert` record for a new tuple written to `page_id`.
    ///
    /// `before` is the full page image *before* the insert (used to undo the
    /// operation during recovery).  `after` is the full page image *after* the
    /// insert (used to redo it).
    ///
    /// The page is added to the dirty-page table if it is not already present.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::UnknownTransaction`] if `tid` is not active.
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    pub fn log_insert(
        &self,
        tid: TransactionId,
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Insert {
            page_id,
            before,
            after,
        })
    }

    /// Logs a `Delete` record for a tuple removed from `page_id`.
    ///
    /// `before` is the full page image *before* the delete (used to undo the
    /// operation).  `after` is the full page image *after* the delete (used to
    /// redo it during recovery).
    ///
    /// The page is added to the dirty-page table if it is not already present.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::UnknownTransaction`] if `tid` is not active.
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    pub fn log_delete(
        &self,
        tid: TransactionId,
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Delete {
            page_id,
            before,
            after,
        })
    }

    /// Appends a Compensation Log Record (CLR) describing one undo step.
    ///
    /// Called exclusively by the ARIES Undo pass. Unlike [`Wal::log_update`], the
    /// caller supplies `prev_lsn` and `undo_next_lsn` explicitly because:
    ///
    /// - `prev_lsn` — Undo owns the loser's chain state (in the ATT inherited from Analysis);
    ///   losers are not in `active_txns` during recovery.
    /// - `undo_next_lsn` — set to the `prev_lsn` of the record this CLR compensates, so a future
    ///   recovery skips past work already done.
    ///
    /// `after` is the page image *after* the before-image has been restored
    /// (i.e. what the page looks like with the undone change removed). It is
    /// used by Redo on a subsequent recovery to reapply the compensation
    /// idempotently via the `page_lsn` check.
    ///
    /// Also marks `page_id` dirty in the WAL's dirty-page table so that the
    /// next checkpoint reflects the compensation.
    ///
    /// Returns the LSN assigned to the CLR.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    /// Does **not** error if `tid` is not in `active_txns` — losers written
    /// during recovery legitimately bypass that map.
    pub fn log_clr(
        &self,
        tid: TransactionId,
        prev_lsn: Lsn,
        page_id: PageId,
        after: Vec<u8>,
        undo_next_lsn: Lsn,
    ) -> Result<Lsn, WalError> {
        let mut state = self.state.lock();
        let lsn = Self::write_record(&self.file, &mut state, tid, prev_lsn, LogRecordBody::Clr {
            page_id,
            after,
            undo_next_lsn,
        })?;
        state.dirty_pages.entry(page_id).or_insert(lsn);
        Ok(lsn)
    }

    /// Appends an `End` record marking a transaction as fully terminated.
    ///
    /// Written by:
    /// - The Undo pass after a loser's entire chain has been compensated.
    /// - (Future) Normal-abort completion once rollback finishes.
    ///
    /// `prev_lsn` is the transaction's most recent record LSN (the last CLR
    /// for undone losers, or the abort record for cleanly-aborted txns). The
    /// caller supplies it because the recovery path has no entry in
    /// `active_txns`.
    ///
    /// Returns the LSN assigned to the `End` record. The record is **not**
    /// forced to disk by this call — Undo is idempotent, so a crash before
    /// flush simply means the next recovery re-undoes (harmlessly).
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] or [`WalError::Codec`] on write failures.
    pub fn log_end(&self, tid: TransactionId, prev_lsn: Lsn) -> Result<Lsn, WalError> {
        let mut state = self.state.lock();
        let lsn = Self::write_record(&self.file, &mut state, tid, prev_lsn, LogRecordBody::End)?;
        state.active_txns.remove(&tid);
        Ok(lsn)
    }

    /// Blocks until all records up to (and including) `target` have been flushed
    /// to stable storage.
    ///
    /// If the WAL is already flushed past `target` the call returns immediately.
    /// Otherwise it wakes the flush loop and waits on the condvar until
    /// `flushed_till >= target`.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if a flush triggered by this call fails.
    pub fn force(&self, target: Lsn) -> Result<(), WalError> {
        let mut state = self.state.lock();
        if state.flushed_till >= target {
            return Ok(());
        }

        self.flush_cond.notify_all();
        while state.flushed_till < target {
            self.flush_cond.wait(&mut state);
        }
        Ok(())
    }

    /// Flushes any buffered records and closes the WAL.
    ///
    /// Consumes `self`, so no further writes are possible after this call.
    /// Intended for clean shutdown when no background flush thread is running,
    /// or to guarantee all buffered data is on disk before process exit.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the final flush fails.
    pub fn close(self) -> Result<(), WalError> {
        let mut state = self.state.lock();
        Self::flush_state(&self.file, &mut state)?;
        Ok(())
    }

    /// Writes a data-modification record and updates transaction and dirty-page state.
    ///
    /// Shared implementation used by [`log_insert`], [`log_update`], and [`log_delete`].
    /// Looks up the previous LSN for `tid`, delegates to [`write_record`], then
    /// records the page as dirty using the LSN of the *first* write to that page.
    fn log_data_op(
        &self,
        tid: TransactionId,
        page_id: PageId,
        body: LogRecordBody,
    ) -> Result<Lsn, WalError> {
        let mut state = self.state.lock();
        let prev = state
            .active_txns
            .get(&tid)
            .ok_or(WalError::UnknownTransaction(tid))?
            .last;
        let lsn = Self::write_record(&self.file, &mut state, tid, prev, body)?;
        state.active_txns.get_mut(&tid).unwrap().last = lsn;
        state.dirty_pages.entry(page_id).or_insert(lsn);
        Ok(lsn)
    }

    /// Encodes one log record and appends it to the buffer or the file directly.
    ///
    /// If the encoded record exceeds the buffer capacity the buffer is flushed
    /// first and the record is written directly via `write_at`. Otherwise the
    /// record is copied into the buffer (flushing first if there is not enough
    /// room). In both cases `current_at` is advanced by the record's byte length.
    ///
    /// Returns the LSN assigned to the record, which equals `state.current_at`
    /// at the time of the call.
    fn write_record(
        file: &fs::File,
        state: &mut WalState,
        tid: TransactionId,
        prev_lsn: Lsn,
        body: LogRecordBody,
    ) -> Result<Lsn, WalError> {
        let assigned_lsn = state.current_at;

        let rec = LogRecord::new(assigned_lsn, prev_lsn, tid, body)?;
        let mut data = Vec::with_capacity(4096);
        rec.encode(&mut data)?;

        if data.len() > state.len() {
            Self::flush_state(file, state)?;
            file.write_at(&data, state.flushed_till.into())?;
            state.flushed_till = Lsn::from(u64::from(assigned_lsn) + data.len() as u64);
        } else {
            if state.buf_offset + data.len() > state.len() {
                Self::flush_state(file, state)?;
            }
            state.buffer[state.buf_offset..state.buf_offset + data.len()].copy_from_slice(&data);
            state.buf_offset += data.len();
        }

        state.current_at = Lsn::from(u64::from(assigned_lsn) + data.len() as u64);
        Ok(assigned_lsn)
    }

    /// Writes the buffered bytes to the file at `flushed_till` and resets the buffer.
    ///
    /// Does nothing if the buffer is empty. After a successful write, `flushed_till`
    /// is advanced to `current_at` and `buf_offset` is reset to zero.
    fn flush_state(file: &fs::File, state: &mut WalState) -> Result<(), WalError> {
        if state.is_empty() {
            return Ok(());
        }

        file.write_at(&state.buffer[..state.buf_offset], state.flushed_till.into())?;
        state.flushed_till = state.current_at;
        state.buf_offset = 0;
        Ok(())
    }

    /// Runs the background flush loop — intended to be executed on a dedicated thread.
    ///
    /// The loop waits until the buffer is non-empty, snapshots the buffered bytes,
    /// releases the lock, writes and syncs the data to the file, then re-acquires
    /// the lock to update `flushed_till` and compact any bytes written concurrently
    /// while the lock was released.
    ///
    /// After each flush all waiters blocked in [`Wal::force`] are notified via the
    /// condvar.
    ///
    /// This method runs forever and should be spawned as a background thread:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use std::path::Path;
    /// # use storemy::wal::writer::Wal;
    /// let wal = Arc::new(Wal::new(Path::new("wal.log"), 65536).unwrap());
    /// let wal_bg = Arc::clone(&wal);
    /// std::thread::spawn(move || wal_bg.flush_loop());
    /// ```
    pub fn flush_loop(&self) {
        loop {
            let mut state = self.state.lock();
            while state.is_empty() {
                self.flush_cond.wait(&mut state);
            }

            let (buff_offset, at) = (state.buf_offset, state.flushed_till);
            let data = state.buffer[..buff_offset].to_vec();
            drop(state);

            self.file.write_at(&data, u64::from(at)).unwrap();
            self.file.sync_all().unwrap();

            let mut state = self.state.lock();
            state.flushed_till = Lsn::from(u64::from(at) + buff_offset as u64);
            let remaining = state.buf_offset - buff_offset;

            state
                .buffer
                .copy_within(buff_offset..buff_offset + remaining, 0);
            state.buf_offset = remaining;
            self.flush_cond.notify_all();
        }
    }

    /// Performs a fuzzy checkpoint and returns the LSN of the `CheckpointEnd` record.
    ///
    /// A fuzzy checkpoint does **not** pause transactions or flush dirty pages.
    /// Instead it snapshots the current ATT and DPT in memory, writes that snapshot
    /// to the log, and forces both records to disk.  The database continues accepting
    /// writes throughout.
    ///
    /// # What gets written
    ///
    /// Two records are appended:
    ///
    /// 1. `CheckpointBegin` — marks the start of the snapshot window.
    /// 2. `CheckpointEnd`   — carries the ATT and DPT snapshots.  Its `prev_lsn` points at the
    ///    `CheckpointBegin` so that Analysis can find the start of the window and replay any
    ///    records written between the two markers.
    ///
    /// # Snapshot semantics
    ///
    /// The ATT and DPT are snapped **under the same lock hold** as the
    /// `CheckpointBegin` write, giving a view consistent with that exact moment.
    /// All active transactions are recorded as [`TxnStatus::Running`]; if any of
    /// them had already called [`Wal::log_abort`] the forward scan that Analysis
    /// performs from `CheckpointBegin` will encounter their `Abort` records and
    /// correct the status.
    ///
    /// # Caller responsibility
    ///
    /// This method only writes and flushes the WAL records.  The returned
    /// `end_lsn` must be persisted to the master record file (via
    /// `Aries::take_checkpoint`) so that the next startup knows where to begin
    /// the Analysis scan.  If the process crashes before the master record is
    /// updated, the checkpoint is silently ignored on recovery and Analysis falls
    /// back to the previous checkpoint (or LSN 0).
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] or [`WalError::Codec`] if writing or flushing
    /// either record fails.
    pub fn checkpoint(&self) -> Result<Lsn, WalError> {
        let (begin_lsn, att_snapshot, dpt_snapshot) = {
            let mut state = self.state.lock();
            let begin_lsn = Self::write_record(
                &self.file,
                &mut state,
                TransactionId::INVALID,
                Lsn::INVALID,
                LogRecordBody::CheckpointBegin,
            )?;

            let att_snapshot = state
                .active_txns
                .iter()
                .map(|(tid, info)| (*tid, info.last, TxnStatus::Running))
                .collect::<Vec<_>>();

            let dpt_snapshot = state
                .dirty_pages
                .iter()
                .map(|(page_id, lsn)| (*page_id, *lsn))
                .collect::<Vec<_>>();

            (begin_lsn, att_snapshot, dpt_snapshot)
        };

        let end_lsn = {
            let mut state = self.state.lock();
            Self::write_record(
                &self.file,
                &mut state,
                TransactionId::INVALID,
                begin_lsn,
                LogRecordBody::CheckpointEnd {
                    att_snapshot,
                    dpt_snapshot,
                },
            )?
        };

        self.force(end_lsn)?;
        Ok(end_lsn)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fallible_iterator::FallibleIterator;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        primitives::{FileId, PageNumber},
        wal::{log::LogRecordType, reader::WalReader},
    };

    /// `buf_size=0` forces every record through the direct-write path,
    /// advancing `flushed_till` immediately. `force()` returns without waiting,
    /// so no background `flush_loop` thread is needed.
    const NO_BUF: usize = 0;

    fn make_wal(buf_size: usize) -> (Wal, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Wal::new(&dir.path().join("test.wal"), buf_size).unwrap();
        (wal, dir)
    }

    fn page(file: u64, p: u32) -> PageId {
        PageId::new(FileId::new(file), PageNumber::new(p))
    }

    // Access private state from tests in the same module.
    fn active_txns(wal: &Wal) -> Vec<TransactionId> {
        wal.state.lock().active_txns.keys().copied().collect()
    }

    fn dirty_pages(wal: &Wal) -> HashMap<PageId, Lsn> {
        wal.state.lock().dirty_pages.clone()
    }

    fn txn_last_lsn(wal: &Wal, tid: TransactionId) -> Lsn {
        wal.state.lock().active_txns[&tid].last
    }

    fn txn_undo_next(wal: &Wal, tid: TransactionId) -> Lsn {
        wal.state.lock().active_txns[&tid].undo_next
    }

    #[test]
    fn lsns_are_monotonically_increasing() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 1);

        let lsn1 = wal.log_begin(tid).unwrap();
        let lsn2 = wal.log_insert(tid, p, vec![], vec![1, 2, 3]).unwrap();
        let lsn3 = wal.log_commit(tid).unwrap();

        assert!(lsn1 < lsn2, "begin < insert");
        assert!(lsn2 < lsn3, "insert < commit");
    }

    #[test]
    fn each_record_advances_lsn() {
        let (wal, _dir) = make_wal(NO_BUF);
        let t1 = TransactionId::new(1);
        let t2 = TransactionId::new(2);

        let a = wal.log_begin(t1).unwrap();
        let b = wal.log_begin(t2).unwrap();
        assert!(a < b);
    }

    #[test]
    fn begin_registers_transaction() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        assert!(active_txns(&wal).contains(&tid));
    }

    #[test]
    fn commit_removes_transaction() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        wal.log_commit(tid).unwrap();
        assert!(!active_txns(&wal).contains(&tid));
    }

    #[test]
    fn abort_keeps_transaction_for_undo_processing() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        wal.log_abort(tid).unwrap();
        assert!(active_txns(&wal).contains(&tid));
    }

    #[test]
    fn abort_sets_undo_next_lsn() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        let abort_lsn = wal.log_abort(tid).unwrap();
        assert_eq!(txn_undo_next(&wal, tid), abort_lsn);
    }

    #[test]
    fn last_lsn_advances_with_each_record() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 1);

        wal.log_begin(tid).unwrap();
        let after_insert = wal.log_insert(tid, p, vec![], vec![1]).unwrap();
        assert_eq!(txn_last_lsn(&wal, tid), after_insert);

        let after_update = wal.log_update(tid, p, vec![1], vec![2]).unwrap();
        assert_eq!(txn_last_lsn(&wal, tid), after_update);
    }

    #[test]
    fn insert_marks_page_dirty() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 1);
        wal.log_begin(tid).unwrap();
        let lsn = wal.log_insert(tid, p, vec![], vec![1, 2, 3]).unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&lsn));
    }

    #[test]
    fn update_marks_page_dirty() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 2);
        wal.log_begin(tid).unwrap();
        let lsn = wal.log_update(tid, p, vec![0], vec![1]).unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&lsn));
    }

    #[test]
    fn delete_marks_page_dirty() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 3);
        wal.log_begin(tid).unwrap();
        let lsn = wal.log_delete(tid, p, vec![9, 8, 7], vec![]).unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&lsn));
    }

    #[test]
    fn dirty_page_lsn_is_first_writer_not_latest() {
        // dirty_pages uses or_insert — subsequent writes to the same page
        // do not overwrite the LSN. The buffer pool needs the earliest LSN
        // that requires this page to be flushed before a checkpoint.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 1);
        wal.log_begin(tid).unwrap();
        let first = wal.log_insert(tid, p, vec![], vec![1]).unwrap();
        let _second = wal.log_update(tid, p, vec![1], vec![2]).unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&first));
    }

    #[test]
    fn multiple_pages_tracked_independently() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p1 = page(1, 1);
        let p2 = page(1, 2);
        wal.log_begin(tid).unwrap();
        let lsn1 = wal.log_insert(tid, p1, vec![], vec![1]).unwrap();
        let lsn2 = wal.log_insert(tid, p2, vec![], vec![2]).unwrap();
        let dirty = dirty_pages(&wal);
        assert_eq!(dirty.get(&p1), Some(&lsn1));
        assert_eq!(dirty.get(&p2), Some(&lsn2));
    }

    #[test]
    fn commit_unknown_transaction_errors() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(99);
        let err = wal.log_commit(tid).unwrap_err();
        assert!(matches!(err, WalError::UnknownTransaction(t) if t == tid));
    }

    #[test]
    fn abort_unknown_transaction_errors() {
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(99);
        let err = wal.log_abort(tid).unwrap_err();
        assert!(matches!(err, WalError::UnknownTransaction(t) if t == tid));
    }

    #[test]
    fn insert_unknown_transaction_errors() {
        let (wal, _dir) = make_wal(NO_BUF);
        let err = wal
            .log_insert(TransactionId::new(99), page(1, 1), vec![], vec![])
            .unwrap_err();
        assert!(matches!(err, WalError::UnknownTransaction(_)));
    }

    #[test]
    fn update_unknown_transaction_errors() {
        let (wal, _dir) = make_wal(NO_BUF);
        let err = wal
            .log_update(TransactionId::new(99), page(1, 1), vec![], vec![])
            .unwrap_err();
        assert!(matches!(err, WalError::UnknownTransaction(_)));
    }

    #[test]
    fn delete_unknown_transaction_errors() {
        let (wal, _dir) = make_wal(NO_BUF);
        let err = wal
            .log_delete(TransactionId::new(99), page(1, 1), vec![], vec![])
            .unwrap_err();
        assert!(matches!(err, WalError::UnknownTransaction(_)));
    }

    #[test]
    fn multiple_transactions_interleaved() {
        let (wal, _dir) = make_wal(NO_BUF);
        let t1 = TransactionId::new(1);
        let t2 = TransactionId::new(2);
        let p = page(1, 1);

        wal.log_begin(t1).unwrap();
        wal.log_begin(t2).unwrap();
        assert_eq!(active_txns(&wal).len(), 2);

        wal.log_insert(t1, p, vec![], vec![1]).unwrap();
        wal.log_update(t2, p, vec![1], vec![2]).unwrap();

        wal.log_commit(t1).unwrap();
        assert!(!active_txns(&wal).contains(&t1));
        assert!(active_txns(&wal).contains(&t2));

        wal.log_commit(t2).unwrap();
        assert!(active_txns(&wal).is_empty());
    }

    #[test]
    fn force_already_flushed_returns_immediately() {
        // With NO_BUF every record is written directly, so flushed_till is
        // already past commit_lsn when force() is called the second time.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let lsn = wal.log_begin(tid).unwrap();
        wal.log_commit(tid).unwrap();
        assert!(wal.force(lsn).is_ok());
    }

    #[test]
    fn record_larger_than_buffer_writes_directly() {
        // buf_size=16: every record (header alone = 41 bytes) exceeds the buffer
        // and must take the direct write_at path instead of being buffered.
        let (wal, _dir) = make_wal(16);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        assert!(
            wal.log_insert(tid, page(1, 1), vec![], vec![0u8; 512])
                .is_ok()
        );
    }

    #[test]
    fn flush_loop_unblocks_force_waiters() {
        // With a large buffer, log_commit calls force() which waits on the
        // condvar. The background flush_loop must flush and notify to unblock it.
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("wal"), 4096).unwrap());

        let wal_bg = Arc::clone(&wal);
        std::thread::spawn(move || wal_bg.flush_loop());

        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        assert!(wal.log_commit(tid).is_ok());
    }

    #[test]
    fn close_flushes_remaining_buffer() {
        // With a large buffer, begin record sits in memory unflushed.
        // close() must flush it synchronously without needing flush_loop.
        let (wal, _dir) = make_wal(4096);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        assert!(wal.close().is_ok());
    }

    // ── log_clr ──────────────────────────────────────────────────────────────

    #[test]
    fn clr_marks_page_dirty() {
        // The Undo pass writes a CLR then restores the before-image to the page;
        // the WAL must record that page as dirty so the next checkpoint sees it.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 5);
        let lsn = wal
            .log_clr(tid, Lsn::INVALID, p, vec![9, 8, 7], Lsn::INVALID)
            .unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&lsn));
    }

    #[test]
    fn clr_does_not_require_active_transaction() {
        // During recovery, losers are NOT in active_txns — log_clr must succeed
        // anyway because it bypasses the active-txn lookup entirely.
        let (wal, _dir) = make_wal(NO_BUF);
        let loser = TransactionId::new(42);
        let p = page(2, 1);
        assert!(
            wal.log_clr(loser, Lsn::INVALID, p, vec![], Lsn::INVALID)
                .is_ok()
        );
    }

    #[test]
    fn clr_lsn_is_after_compensated_record() {
        // The CLR is written *after* the original operation record, so its LSN
        // must be strictly greater.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 1);
        wal.log_begin(tid).unwrap();
        let insert_lsn = wal.log_insert(tid, p, vec![], vec![1, 2]).unwrap();
        let clr_lsn = wal
            .log_clr(tid, insert_lsn, p, vec![], Lsn::INVALID)
            .unwrap();
        assert!(clr_lsn > insert_lsn);
    }

    #[test]
    fn clr_does_not_overwrite_earlier_dirty_entry() {
        // dirty_pages records the *first* LSN that dirtied a page.  A CLR for
        // the same page must not overwrite that entry.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        let p = page(1, 9);
        wal.log_begin(tid).unwrap();
        let first = wal.log_insert(tid, p, vec![], vec![1]).unwrap();
        wal.log_clr(tid, first, p, vec![], Lsn::INVALID).unwrap();
        assert_eq!(dirty_pages(&wal).get(&p), Some(&first));
    }

    #[test]
    fn end_removes_active_transaction() {
        // Normal abort path: Begin → data ops → Abort → (undo) → End.
        // After log_end the transaction must leave active_txns.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        let abort_lsn = wal.log_abort(tid).unwrap();
        wal.log_end(tid, abort_lsn).unwrap();
        assert!(!active_txns(&wal).contains(&tid));
    }

    #[test]
    fn end_tolerates_absent_transaction() {
        // Recovery path: losers were never re-inserted into active_txns, so
        // log_end must tolerate the absence without returning an error.
        let (wal, _dir) = make_wal(NO_BUF);
        let loser = TransactionId::new(99);
        assert!(wal.log_end(loser, Lsn::INVALID).is_ok());
    }

    #[test]
    fn end_lsn_is_after_prev_lsn() {
        // The End record is written after the final CLR (or Abort), so its LSN
        // must be strictly greater than the prev_lsn handed in.
        let (wal, _dir) = make_wal(NO_BUF);
        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        let abort_lsn = wal.log_abort(tid).unwrap();
        let end_lsn = wal.log_end(tid, abort_lsn).unwrap();
        assert!(end_lsn > abort_lsn);
    }

    /// Opens the WAL file at `path` and collects every readable record into a Vec.
    fn read_all_records(path: &std::path::Path) -> Vec<LogRecord> {
        let mut reader = WalReader::open(path).unwrap();
        let mut out = Vec::new();
        while let Some(r) = reader.next().unwrap() {
            out.push(r);
        }
        out
    }

    fn flushed_till(wal: &Wal) -> Lsn {
        wal.state.lock().flushed_till
    }

    #[test]
    fn checkpoint_writes_begin_then_end_record() {
        // A checkpoint must produce exactly two records in order: Begin then End.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        wal.checkpoint().unwrap();

        let records = read_all_records(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].header.record_type,
            LogRecordType::CheckpointBegin
        );
        assert_eq!(records[1].header.record_type, LogRecordType::CheckpointEnd);
    }

    #[test]
    fn checkpoint_end_prev_lsn_points_at_begin() {
        // Analysis uses CheckpointEnd.prev_lsn to find CheckpointBegin and
        // resume the forward scan from that point. If this link is wrong,
        // records written during the checkpoint window will be missed.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        wal.checkpoint().unwrap();

        let records = read_all_records(&path);
        let begin_lsn = records[0].header.lsn;
        let end_prev_lsn = records[1].header.prev_lsn;
        assert_eq!(
            end_prev_lsn, begin_lsn,
            "CheckpointEnd.prev_lsn must equal CheckpointBegin.lsn"
        );
    }

    #[test]
    fn checkpoint_records_use_invalid_tid() {
        // Checkpoint records do not belong to any user transaction.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        wal.checkpoint().unwrap();

        for r in read_all_records(&path) {
            assert_eq!(
                r.header.tid,
                TransactionId::INVALID,
                "{:?} must carry INVALID tid",
                r.header.record_type
            );
        }
    }

    #[test]
    fn checkpoint_captures_active_txn_in_att_snapshot() {
        // An uncommitted transaction must appear in the ATT snapshot with its
        // correct last_lsn and TxnStatus::Running.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        let tid = TransactionId::new(1);
        let p = page(1, 1);
        wal.log_begin(tid).unwrap();
        let insert_lsn = wal.log_insert(tid, p, vec![], vec![1]).unwrap();

        wal.checkpoint().unwrap();

        let records = read_all_records(&path);
        let end = records
            .iter()
            .find(|r| r.header.record_type == LogRecordType::CheckpointEnd)
            .unwrap();

        match &end.body {
            LogRecordBody::CheckpointEnd { att_snapshot, .. } => {
                assert_eq!(att_snapshot.len(), 1);
                let (snap_tid, snap_lsn, snap_status) = att_snapshot[0];
                assert_eq!(snap_tid, tid);
                assert_eq!(snap_lsn, insert_lsn);
                assert_eq!(snap_status, TxnStatus::Running);
            }
            _ => panic!("expected CheckpointEnd body"),
        }
    }

    #[test]
    fn checkpoint_captures_dirty_page_in_dpt_snapshot() {
        // An insert marks a page dirty with the LSN of that insert.
        // The DPT snapshot must carry that (page_id, rec_lsn) pair.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        let tid = TransactionId::new(1);
        let p = page(1, 7);
        wal.log_begin(tid).unwrap();
        let insert_lsn = wal.log_insert(tid, p, vec![], vec![1]).unwrap();

        wal.checkpoint().unwrap();

        let records = read_all_records(&path);
        let end = records
            .iter()
            .find(|r| r.header.record_type == LogRecordType::CheckpointEnd)
            .unwrap();

        match &end.body {
            LogRecordBody::CheckpointEnd { dpt_snapshot, .. } => {
                assert_eq!(dpt_snapshot.len(), 1);
                assert_eq!(dpt_snapshot[0], (p, insert_lsn));
            }
            _ => panic!("expected CheckpointEnd body"),
        }
    }

    #[test]
    fn committed_txn_absent_from_att_snapshot() {
        // A committed transaction leaves active_txns, so it must not appear
        // in the ATT snapshot — it is a winner and needs no undo.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        let tid = TransactionId::new(1);
        wal.log_begin(tid).unwrap();
        wal.log_insert(tid, page(1, 1), vec![], vec![1]).unwrap();
        wal.log_commit(tid).unwrap();

        wal.checkpoint().unwrap();

        let records = read_all_records(&path);
        let end = records
            .iter()
            .find(|r| r.header.record_type == LogRecordType::CheckpointEnd)
            .unwrap();

        match &end.body {
            LogRecordBody::CheckpointEnd { att_snapshot, .. } => {
                assert!(
                    att_snapshot.is_empty(),
                    "committed txn must not appear in ATT snapshot"
                );
            }
            _ => panic!("expected CheckpointEnd body"),
        }
    }

    #[test]
    fn second_checkpoint_lsn_greater_than_first() {
        // Each checkpoint appends two records, so the second end_lsn must be
        // strictly greater than the first.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        let first = wal.checkpoint().unwrap();
        let second = wal.checkpoint().unwrap();
        assert!(second > first);
    }

    #[test]
    fn checkpoint_is_durable_after_return() {
        // With NO_BUF every write goes direct, so flushed_till must reach
        // end_lsn by the time checkpoint() returns — force() is a no-op but
        // the invariant must hold regardless.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::new(&path, NO_BUF).unwrap();

        let end_lsn = wal.checkpoint().unwrap();
        assert!(flushed_till(&wal) >= end_lsn);
    }
}
