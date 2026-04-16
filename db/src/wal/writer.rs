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

use std::collections::HashMap;
use std::fs;
use std::io::Seek;
use std::os::unix::fs::FileExt;
use std::path::Path;
use parking_lot::{Condvar, Mutex};
use std::time::SystemTime;

use crate::codec::{CodecError, Encode};
use crate::primitives::{Lsn, PageId, TransactionId};
use crate::storage::Page;
use crate::wal::log::{LogRecord, LogRecordBody};

use thiserror::Error;

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// An I/O failure while reading or writing the WAL file.
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A failure while encoding a log record into bytes.
    #[error("WAL codec error: {0}")]
    Codec(#[from] CodecError),

    /// An operation was attempted on a transaction ID that has no active entry.
    ///
    /// This typically means the transaction was never begun, or it was already
    /// committed or removed.
    #[error("unknown transaction: {0}")]
    UnknownTransaction(TransactionId),

    /// A page-level WAL operation required a before-image but the page
    /// returned `None` from [`Page::before_image`].
    #[error("missing before-image for page {0:?}")]
    MissingBeforeImage(PageId),
}

/// Per-transaction bookkeeping kept in memory while a transaction is active.
struct TxnInfo {
    /// LSN of the first record written for this transaction (the `Begin` record).
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
    /// All mutable state, serialised under a single lock.
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
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

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
        state.active_txns.insert(
            tid,
            TxnInfo {
                first: lsn,
                last: lsn,
                undo_next: Lsn::INVALID,
            },
        );
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
        self.log_data_op(
            tid,
            page_id,
            LogRecordBody::Update {
                page_id,
                before,
                after,
            },
        )
    }

    /// Logs an `Insert` record for a new tuple written to `page_id`.
    ///
    /// `after` is the encoded tuple that was inserted.
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
        after: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Insert { page_id, after })
    }

    /// Logs a `Delete` record for a tuple removed from `page_id`.
    ///
    /// `before` is the encoded tuple that was deleted, needed to undo the
    /// operation during recovery.
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
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Delete { page_id, before })
    }

    /// Logs an `Insert` record by reading the after-image directly from `page`.
    ///
    /// Equivalent to calling [`Self::log_insert`] with `page.page_data()`.
    pub fn log_page_insert(
        &self,
        tid: TransactionId,
        page_id: PageId,
        page: &impl Page,
    ) -> Result<Lsn, WalError> {
        self.log_insert(tid, page_id, page.page_data().to_vec())
    }

    /// Logs a `Delete` record by reading the before-image directly from `page`.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::MissingBeforeImage`] if the page has no before-image.
    pub fn log_page_delete(
        &self,
        tid: TransactionId,
        page_id: PageId,
        page: &impl Page,
    ) -> Result<Lsn, WalError> {
        let before = page
            .before_image()
            .ok_or(WalError::MissingBeforeImage(page_id))?
            .to_vec();
        self.log_delete(tid, page_id, before)
    }

    /// Logs an `Update` record by reading both images directly from `page`.
    ///
    /// `before_image()` provides the undo image and `page_data()` the redo image.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::MissingBeforeImage`] if the page has no before-image.
    pub fn log_page_update(
        &self,
        tid: TransactionId,
        page_id: PageId,
        page: &impl Page,
    ) -> Result<Lsn, WalError> {
        let before = page
            .before_image()
            .ok_or(WalError::MissingBeforeImage(page_id))?
            .to_vec();
        self.log_update(tid, page_id, before, page.page_data().to_vec())
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

        let rec = LogRecord::new(assigned_lsn, prev_lsn, tid, SystemTime::now(), body)?;
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
    /// # use db::wal::writer::Wal;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{FileId, PageNumber};
    use std::sync::Arc;
    use tempfile::tempdir;

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
        wal.state
            .lock()
            .active_txns
            .keys()
            .copied()
            .collect()
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
        let lsn2 = wal.log_insert(tid, p, vec![1, 2, 3]).unwrap();
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
        let after_insert = wal.log_insert(tid, p, vec![1]).unwrap();
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
        let lsn = wal.log_insert(tid, p, vec![1, 2, 3]).unwrap();
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
        let lsn = wal.log_delete(tid, p, vec![9, 8, 7]).unwrap();
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
        let first = wal.log_insert(tid, p, vec![1]).unwrap();
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
        let lsn1 = wal.log_insert(tid, p1, vec![1]).unwrap();
        let lsn2 = wal.log_insert(tid, p2, vec![2]).unwrap();
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
            .log_insert(TransactionId::new(99), page(1, 1), vec![])
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
            .log_delete(TransactionId::new(99), page(1, 1), vec![])
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

        wal.log_insert(t1, p, vec![1]).unwrap();
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
        assert!(wal.log_insert(tid, page(1, 1), vec![0u8; 512]).is_ok());
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
}
