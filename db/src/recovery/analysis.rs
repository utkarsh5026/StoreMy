//! ARIES Analysis pass: rebuild the ATT and DPT from the WAL.
//!
//! After a crash, the database does not know which transactions were still in
//! flight or which pages might hold unflushed changes. Analysis answers that by
//! scanning the write-ahead log forward and maintaining two tables:
//!
//! - **ATT** (Active Transactions Table) — one row per *loser* transaction that still needs undo
//!   (running or aborting at crash time).
//! - **DPT** (Dirty Page Table) — one row per page that might need redo, with the earliest LSN that
//!   dirtied it (`rec_lsn`).
//!
//! When a fuzzy checkpoint exists, the scan can start from the checkpoint's
//! `CheckpointEnd` snapshot instead of LSN 0. The master record points at that
//! record; [`Analysis::run`] loads the snapshot, then continues forward
//! from the matching `CheckpointBegin` so records written during the checkpoint
//! window are not missed.

use std::collections::HashMap;

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use super::{AnalysisResult, AttEntry, DptEntry};
use crate::{
    primitives::{Lsn, PageId, TransactionId},
    wal::{
        WalError,
        log::{LogRecord, LogRecordBody, LogRecordHeader, TxnStatus},
        reader::WalReader,
    },
};

/// Failure modes specific to the Analysis pass.
#[derive(Debug, Error)]
pub enum AnalysisError {
    /// WAL I/O or decode failed while scanning or seeking.
    ///
    /// Torn tails at the end of the log are not reported here — [`WalReader`]
    /// treats them as clean EOF.
    #[error("WAL error during analysis: {0}")]
    Wal(#[from] WalError),

    /// The checkpoint LSN from the master record could not be read as a whole
    /// `CheckpointEnd` record.
    ///
    /// This should not happen if the master record is written only after the
    /// checkpoint is forced to disk. Surfaced explicitly instead of silently
    /// restarting from LSN 0.
    #[error("checkpoint record at LSN {0} is torn or missing")]
    TornCheckpoint(Lsn),
}

#[derive(Debug, Default)]
pub(in crate::recovery) struct Analysis {
    active_transactions: HashMap<TransactionId, AttEntry>,
    dirty_pages: HashMap<PageId, DptEntry>,
}

impl Analysis {
    /// Runs the Analysis pass over `reader` and returns the rebuilt tables.
    ///
    /// If `checkpoint_lsn` is `None`, scanning starts at LSN 0 (no prior
    /// checkpoint). If it is `Some`, the ATT and DPT are seeded from the
    /// `CheckpointEnd` at that LSN, then the scan continues from its
    /// `prev_lsn` (the paired `CheckpointBegin`).
    ///
    /// The returned [`AnalysisResult`] includes `redo_lsn`, the minimum
    /// `rec_lsn` across the DPT — the starting point for the Redo pass.
    ///
    /// # Errors
    ///
    /// Returns [`AnalysisError::Wal`] on seek/read/decode failures, or
    /// [`AnalysisError::TornCheckpoint`] when the checkpoint record at
    /// `checkpoint_lsn` cannot be read intact.
    #[tracing::instrument(
        name = "aries_analysis",
        skip(reader),
        fields(checkpoint_lsn = ?checkpoint_lsn)
    )]
    pub(in crate::recovery) fn run(
        mut self,
        reader: &mut WalReader,
        checkpoint_lsn: Option<Lsn>,
    ) -> Result<AnalysisResult, AnalysisError> {
        let scan_from_lsn = match checkpoint_lsn {
            None => Lsn::INVALID,
            Some(ckpt_lsn) => self.load_checkpoint_snapshot(reader, ckpt_lsn)?,
        };
        let mut records_scanned = 0usize;

        reader.seek_to(scan_from_lsn).map_err(AnalysisError::Wal)?;
        while let Some(record) = reader.next().map_err(AnalysisError::Wal)? {
            records_scanned += 1;
            self.process_record(record);
        }
        let redo_lsn = AnalysisResult::compute_redo_lsn(&self.dirty_pages);
        tracing::debug!(
            scan_from_lsn = ?scan_from_lsn,
            records_scanned,
            att_size = self.active_transactions.len(),
            dpt_size = self.dirty_pages.len(),
            redo_lsn = ?redo_lsn,
            "analysis pass complete"
        );
        Ok(AnalysisResult {
            att: self.active_transactions,
            dpt: self.dirty_pages,
            redo_lsn,
        })
    }

    /// Seeds `att` and `dpt` from a `CheckpointEnd` and returns where to resume scanning.
    ///
    /// Reads the record at `ckpt_lsn`. On success, copies `att_snapshot` and
    /// `dpt_snapshot` into the tables and returns `prev_lsn` from that record
    /// (the LSN of the matching `CheckpointBegin`). If the record is missing,
    /// torn, or not a `CheckpointEnd`, returns `Ok(Lsn::INVALID)` so analysis restarts
    /// from the beginning of the log (except when `read_at` fails outright, which
    /// yields [`AnalysisError::TornCheckpoint`]).
    ///
    /// # Errors
    ///
    /// Returns [`AnalysisError::TornCheckpoint`] when `read_at(ckpt_lsn)` fails.
    fn load_checkpoint_snapshot(
        &mut self,
        reader: &mut WalReader,
        ckpt_lsn: Lsn,
    ) -> Result<Lsn, AnalysisError> {
        let checkpoint_end = reader
            .read_at(ckpt_lsn)
            .map_err(|_| AnalysisError::TornCheckpoint(ckpt_lsn))?;

        // The master record must point at a CheckpointEnd.  Any other record type
        // means the master record is stale or the WAL is corrupt.
        let LogRecordBody::CheckpointEnd {
            att_snapshot,
            dpt_snapshot,
        } = checkpoint_end.body
        else {
            return Ok(Lsn::INVALID);
        };

        for (tid, last_lsn, status) in att_snapshot {
            self.active_transactions.insert(tid, AttEntry {
                status,
                last_lsn,
                undo_next_lsn: last_lsn,
            });
        }

        for (page_id, rec_lsn) in dpt_snapshot {
            self.dirty_pages.insert(page_id, DptEntry { rec_lsn });
        }

        // prev_lsn on the CheckpointEnd points at the matching CheckpointBegin.
        // Scan must start there so we don't miss records written during the window.
        Ok(checkpoint_end.header.prev_lsn)
    }

    /// Ensures there is an entry for the given transaction (`tid`) in the
    /// Active Transaction Table (ATT). If the entry does not exist, it is
    /// created with a running status and the given `lsn` set as its LSN fields.
    ///
    /// Returns a mutable reference to the corresponding `AttEntry`, allowing
    /// the caller to update the transaction's analysis state.
    ///
    /// # Arguments
    ///
    /// * `tid` - The transaction ID to ensure in the ATT.
    /// * `lsn` - The log sequence number associated with this operation.
    fn ensure_att_entry(&mut self, tid: TransactionId, lsn: Lsn) -> &mut AttEntry {
        self.active_transactions
            .entry(tid)
            .or_insert_with(|| AttEntry::new_running(lsn))
    }

    /// Ensures there is an entry for the given page in the Dirty Page Table (DPT).
    ///
    /// If the page is not yet tracked, inserts a row with `rec_lsn` — the earliest
    /// LSN that dirtied this page during the scan. If the page is already present,
    /// the existing `rec_lsn` is left unchanged (later records may advance the
    /// txn's ATT pointers but never move `rec_lsn` forward).
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page to ensure in the DPT.
    /// * `rec_lsn` - The LSN to record when the page is first seen as dirty.
    fn ensure_dpt_entry(&mut self, page_id: PageId, rec_lsn: Lsn) -> &mut DptEntry {
        self.dirty_pages
            .entry(page_id)
            .or_insert_with(|| DptEntry { rec_lsn })
    }

    /// Updates the ATT and DPT for one log record.
    ///
    /// Each [`LogRecordBody`] variant has a fixed effect: `Begin` adds a running
    /// txn, `Commit`/`End` remove winners or finished rollbacks, data records and
    /// CLRs advance LSN pointers and mark pages dirty, and checkpoint markers are
    /// no-ops during the forward scan (snapshots are handled in
    /// [`load_checkpoint_snapshot`]).
    fn process_record(&mut self, record: LogRecord) {
        let LogRecord {
            header: LogRecordHeader {
                lsn, tid, prev_lsn, ..
            },
            body,
        } = record;

        match body {
            // A new transaction started: we add it to the ATT as Running.
            // Both LSN fields start at the Begin record — that's where Undo would
            // begin walking backward if this txn turns out to be a loser.
            LogRecordBody::Begin => {
                self.ensure_att_entry(tid, lsn);
            }

            // Commit: winner — durable on the log, nothing to undo.
            // End: Undo finished rolling back a loser — drop it so it isn't undone again.
            LogRecordBody::Commit | LogRecordBody::End => {
                self.active_transactions.remove(&tid);
            }

            // An aborted transaction is still a loser — its changes may have
            // leaked onto data pages and must be rolled back by Undo.
            // Mark it Aborting and record the current position so Undo knows where
            // to start.
            LogRecordBody::Abort => {
                let entry = self.ensure_att_entry(tid, lsn);
                entry.status = TxnStatus::Aborting;
                entry.update_lsn(lsn, prev_lsn);
            }

            // Update / Insert / Delete all have the same ATT+DPT effect:
            // advance the txn's LSN pointers and note the page as dirty.
            LogRecordBody::Update { page_id, .. }
            | LogRecordBody::Insert { page_id, .. }
            | LogRecordBody::Delete { page_id, .. } => {
                self.ensure_att_entry(tid, lsn).update_lsn(lsn, lsn);
                self.ensure_dpt_entry(page_id, lsn);
            }

            // A CLR was written by a previous (crashed) Undo pass.
            // It advances the txn's last_lsn like any other record, but
            // undo_next_lsn jumps to clr.undo_next_lsn — the record before the
            // one this CLR already compensated.  This is what lets a new Undo pass
            // resume exactly where the previous one left off.
            LogRecordBody::Clr {
                page_id,
                undo_next_lsn: clr_undo_next,
                ..
            } => {
                self.ensure_att_entry(tid, lsn)
                    .update_lsn(lsn, clr_undo_next);
                self.ensure_dpt_entry(page_id, lsn);
            }

            // Savepoint: advances last_lsn only; no page changes.
            LogRecordBody::Savepoint { .. } => {
                let entry = self.ensure_att_entry(tid, lsn);
                let undo_next = entry.undo_next_lsn;
                entry.update_lsn(lsn, undo_next);
            }

            // Checkpoint markers: already accounted for via the master record and
            // load_checkpoint_snapshot.  Seeing them during the scan is normal
            // (they sit between CheckpointBegin and CheckpointEnd in the log);
            // just ignore them.
            LogRecordBody::CheckpointBegin | LogRecordBody::CheckpointEnd { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        codec::Encode,
        primitives::{FileId, PageNumber},
        wal::log::{LogRecord, LogRecordBody, TxnStatus},
    };

    fn tid(n: u64) -> TransactionId {
        TransactionId::new(n)
    }

    fn page(n: u32) -> PageId {
        PageId::new(FileId::new(1), PageNumber::new(n))
    }

    /// Write a slice of records to a temp file and return it.
    fn write_log(records: &[LogRecord]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for r in records {
            r.encode(f.as_file_mut()).unwrap();
        }
        f.as_file().sync_all().unwrap();
        f
    }

    /// When every transaction commits, ATT must be empty after Analysis.
    /// A committed txn's pages still appear in the DPT (they may not have been
    /// flushed), and `redo_lsn` reflects the earliest of those.
    #[test]
    fn all_committed_att_is_empty() {
        // LSNs are byte offsets; Begin is 41 bytes (header only), Insert body
        // adds page_id (8) + two length-prefixed 4-byte images (4+4 + 4+4) =
        // 24 bytes → total body 24, record 65 bytes. Commit is 41 bytes.
        // We don't assert exact LSN values here — just structural results.
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(1), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(1), LogRecordBody::Insert {
                page_id: page(5),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            LogRecord::new(Lsn(110), Lsn(41), tid(1), LogRecordBody::Commit).unwrap(),
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert!(result.att.is_empty(), "no losers expected");
        assert!(result.dpt.contains_key(&page(5)), "page 5 should be in DPT");
    }

    /// A txn that began and wrote a record but never committed is a loser.
    #[test]
    fn uncommitted_txn_is_a_loser() {
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(2), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(2), LogRecordBody::Update {
                page_id: page(7),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            // no Commit — crash here
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert!(result.att.contains_key(&tid(2)), "T2 must be a loser");
        let entry = &result.att[&tid(2)];
        assert_eq!(entry.last_lsn, Lsn(41));
        assert_eq!(entry.undo_next_lsn, Lsn(41));
        assert!(result.dpt.contains_key(&page(7)));
        assert_eq!(result.redo_lsn, Lsn(41));
    }

    /// An Abort record marks the txn Aborting but keeps it in the ATT —
    /// its changes still need rolling back.
    #[test]
    fn aborted_txn_stays_in_att_as_aborting() {
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(3), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(3), LogRecordBody::Insert {
                page_id: page(9),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            LogRecord::new(Lsn(110), Lsn(41), tid(3), LogRecordBody::Abort).unwrap(),
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert!(
            result.att.contains_key(&tid(3)),
            "aborted txn is still a loser"
        );
        assert_eq!(result.att[&tid(3)].status, TxnStatus::Aborting);
    }

    /// When a CLR is in the log (from a previous crashed Undo pass), Analysis
    /// must set `undo_next_lsn` to the CLR'`undo_next_lsn`sn — NOT to the CLR's
    /// own LSN.  This lets the new Undo pass skip the record already compensated.
    #[test]
    fn clr_advances_undo_next_lsn() {
        // T4: Begin@0, Insert@41, (crash, undo starts), CLR@110 compensates
        // the Insert (undo_next points at Begin@0), crash again.
        // Analysis should see T4 with undo_next_lsn = Lsn(0) (the Begin),
        // not Lsn(110) (the CLR itself).
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(4), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(4), LogRecordBody::Insert {
                page_id: page(3),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            // CLR written by a previous Undo run; compensates the Insert at
            // LSN 41, so undo_next skips back to its prev_lsn = Lsn(0).
            LogRecord::new(Lsn(110), Lsn(41), tid(4), LogRecordBody::Clr {
                page_id: page(3),
                after: vec![0u8; 4],
                undo_next_lsn: Lsn(0), // ← points at Begin
            })
            .unwrap(),
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        let entry = result.att.get(&tid(4)).expect("T4 must still be a loser");
        assert_eq!(entry.last_lsn, Lsn(110), "last_lsn advances to the CLR");
        assert_eq!(
            entry.undo_next_lsn,
            Lsn(0),
            "undo_next_lsn jumps to Begin, not to the CLR"
        );
    }

    /// If the Undo pass completed a rollback and wrote an End record before the
    /// crash, Analysis must not treat that txn as a loser.
    #[test]
    fn end_record_removes_txn_from_att() {
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(5), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(5), LogRecordBody::Insert {
                page_id: page(2),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            // CLR written during a previous Undo run.
            LogRecord::new(Lsn(110), Lsn(41), tid(5), LogRecordBody::Clr {
                page_id: page(2),
                after: vec![0u8; 4],
                undo_next_lsn: Lsn(0),
            })
            .unwrap(),
            // End: the previous Undo run finished before the crash.
            LogRecord::new(Lsn(159), Lsn(110), tid(5), LogRecordBody::End).unwrap(),
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert!(
            !result.att.contains_key(&tid(5)),
            "T5 ended cleanly — must not be a loser"
        );
    }

    #[test]
    fn redo_lsn_is_minimum_rec_lsn() {
        // Two uncommitted txns touching different pages.
        // Page 5 first dirtied at LSN 41, page 7 first dirtied at LSN 110.
        // redo_lsn must be 41.
        let records = vec![
            LogRecord::new(Lsn(0), Lsn::INVALID, tid(6), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(41), Lsn(0), tid(6), LogRecordBody::Insert {
                page_id: page(5),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            LogRecord::new(Lsn(110), Lsn::INVALID, tid(7), LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(151), Lsn(110), tid(7), LogRecordBody::Update {
                page_id: page(7),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
        ];

        let f = write_log(&records);
        let mut reader = WalReader::open(f.path()).unwrap();
        let result = Analysis::default().run(&mut reader, None).unwrap();

        assert_eq!(result.redo_lsn, Lsn(41));
    }
}
