use std::collections::HashMap;
use std::fs;
use std::io::Seek;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::time::SystemTime;

use crate::codec::{CodecError, Encode};
use crate::primitives::{Lsn, PageId, TransactionId};
use crate::wal::log::{LogRecord, LogRecordBody};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("unknown transaction: {0}")]
    UnknownTransaction(TransactionId),
}

struct TxnInfo {
    first: Lsn,
    last: Lsn,
    undo_next: Lsn,
}

struct WalState {
    active_txns: HashMap<TransactionId, TxnInfo>,
    dirty_pages: HashMap<PageId, Lsn>,
    buffer: Vec<u8>,
    buf_offset: usize,
    current_at: Lsn,
    flushed_till: Lsn,
}

impl WalState {
    pub(super) const fn is_empty(&self) -> bool {
        self.buf_offset == 0
    }

    pub(super) const fn len(&self) -> usize {
        self.buffer.len()
    }
}

pub struct Wal {
    file: fs::File,
    state: Mutex<WalState>,
    flush_cond: Condvar,
}

impl Wal {
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

    pub fn log_begin(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let mut state = self.state.lock().unwrap();
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

    pub fn log_commit(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let lsn = {
            let mut state = self.state.lock().unwrap();
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

        self.state.lock().unwrap().active_txns.remove(&tid);
        Ok(lsn)
    }

    pub fn log_abort(&self, tid: TransactionId) -> Result<Lsn, WalError> {
        let mut state = self.state.lock().unwrap();
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

    pub fn log_insert(
        &self,
        tid: TransactionId,
        page_id: PageId,
        after: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Insert { page_id, after })
    }

    pub fn log_delete(
        &self,
        tid: TransactionId,
        page_id: PageId,
        before: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        self.log_data_op(tid, page_id, LogRecordBody::Delete { page_id, before })
    }

    pub fn force(&self, target: Lsn) -> Result<(), WalError> {
        let mut state = self.state.lock().unwrap();
        if state.flushed_till >= target {
            return Ok(());
        }

        self.flush_cond.notify_all();
        while state.flushed_till < target {
            state = self.flush_cond.wait(state).unwrap();
        }
        Ok(())
    }

    pub fn close(self) -> Result<(), WalError> {
        let mut state = self.state.lock().unwrap();
        Self::flush_state(&self.file, &mut state)?;
        Ok(())
    }

    fn log_data_op(
        &self,
        tid: TransactionId,
        page_id: PageId,
        body: LogRecordBody,
    ) -> Result<Lsn, WalError> {
        let mut state = self.state.lock().unwrap();
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

    fn flush_state(file: &fs::File, state: &mut WalState) -> Result<(), WalError> {
        if state.is_empty() {
            return Ok(());
        }

        file.write_at(&state.buffer[..state.buf_offset], state.flushed_till.into())?;
        state.flushed_till = state.current_at;
        state.buf_offset = 0;
        Ok(())
    }

    pub fn flush_loop(&self) {
        loop {
            let mut state = self.state.lock().unwrap();
            while state.is_empty() {
                state = self.flush_cond.wait(state).unwrap();
            }

            let (buff_offset, at) = (state.buf_offset, state.flushed_till);
            let data = state.buffer[..buff_offset].to_vec();
            drop(state);

            self.file.write_at(&data, u64::from(at)).unwrap();
            self.file.sync_all().unwrap();

            let mut state = self.state.lock().unwrap();
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
            .unwrap()
            .active_txns
            .keys()
            .copied()
            .collect()
    }

    fn dirty_pages(wal: &Wal) -> HashMap<PageId, Lsn> {
        wal.state.lock().unwrap().dirty_pages.clone()
    }

    fn txn_last_lsn(wal: &Wal, tid: TransactionId) -> Lsn {
        wal.state.lock().unwrap().active_txns[&tid].last
    }

    fn txn_undo_next(wal: &Wal, tid: TransactionId) -> Lsn {
        wal.state.lock().unwrap().active_txns[&tid].undo_next
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
