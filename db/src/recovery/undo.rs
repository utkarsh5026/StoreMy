//! ARIES Undo pass: roll back every loser transaction.
//!
//! The Undo pass is driven by a **max-heap** of `(Lsn, TransactionId)` rather
//! than a linear backward scan over the log. At every step we pop the
//! globally-latest unprocessed loser record across all losers, restore its
//! before-image to the page, emit a Compensation Log Record (CLR), and push
//! the next link in that loser's `prev_lsn` chain (or, if the chain is
//! exhausted, write an `End` record and drop the loser from the ATT).
//!
//! The max-heap matters because two losers can interleave on the same page.
//! Undoing them out of WAL order would restore one loser's before-image on
//! top of another loser's still-live change and corrupt the page. Reverse-LSN
//! order is the only safe undo order.
//!
//! # Crash-during-undo correctness
//!
//! Every CLR carries `undo_next_lsn = compensated_record.prev_lsn`. On a
//! crash mid-undo, Analysis re-reads the CLRs already in the log and advances
//! each loser's `undo_next_lsn` past the records that were already
//! compensated; Redo re-applies the CLRs idempotently via the `page_lsn`
//! check; Undo then resumes from exactly the right spot. CLRs themselves are
//! never undone — the match arm for `Clr` only follows `undo_next_lsn`.

use std::{
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

use thiserror::Error;

use super::{AnalysisResult, Aries, AttEntry};
use crate::{
    PAGE_SIZE,
    buffer_pool::page_store::{PageStore, PageStoreError},
    primitives::{Lsn, PageId, TransactionId},
    wal::{WalError, log::LogRecordBody, reader::WalReader, writer::Wal},
};

/// Failure modes specific to the Undo pass.
#[derive(Debug, Error)]
pub enum UndoError {
    /// WAL seek, read, decode, or CLR/End append failed.
    #[error("WAL error during undo: {0}")]
    Wal(#[from] WalError),

    /// Buffer pool I/O failed for a page that must be read or written.
    ///
    /// [`PageStoreError::FileNotRegistered`] is not mapped here — those pages
    /// are skipped with a warning, matching the Redo pass's contract.
    #[error("page store error during undo: {0}")]
    Page(#[from] PageStoreError),

    /// A logged before-image is not exactly [`PAGE_SIZE`] bytes.
    ///
    /// [`PAGE_SIZE`]: crate::PAGE_SIZE
    #[error(
        "before-image size mismatch for {page_id:?}: \
         expected {expected_size} bytes, got {got_size}"
    )]
    ImageSizeMismatch {
        page_id: PageId,
        expected_size: usize,
        got_size: usize,
    },

    /// A loser's `undo_next_lsn` pointed at a record that is not a data
    /// record, `Clr`, or `Begin`.  This means the log is corrupt or the
    /// Analysis pass produced an invalid ATT.
    #[error("loser {tid:?} pointed at LSN {lsn:?} which decoded as a non-undoable record")]
    UnexpectedRecord { tid: TransactionId, lsn: Lsn },
}

#[derive(Default)]
struct UndoStats {
    clrs_followed: usize,
    records_undone: usize,
    clrs_written: usize,
    losers_finalized: usize,
    skipped_unregistered: usize,
}

impl Aries {
    /// Runs the Undo pass: rolls back every loser transaction left in the ATT.
    ///
    /// Consumes `analysis` because the ATT is fully drained as losers finalize
    /// and nothing downstream needs it.
    ///
    /// # Errors
    ///
    /// Returns [`UndoError::Wal`] on log read/write failures,
    /// [`UndoError::Page`] on buffer-pool errors other than unregistered files,
    /// [`UndoError::ImageSizeMismatch`] when a before-image length is wrong,
    /// and [`UndoError::UnexpectedRecord`] when the loser's chain points at a
    /// record type that should never appear there.
    #[tracing::instrument(name = "aries_undo", skip(reader, wal, buffer_pool, analysis))]
    pub(super) fn run_undo(
        reader: &mut WalReader,
        wal: &Arc<Wal>,
        buffer_pool: &Arc<PageStore>,
        analysis: AnalysisResult,
    ) -> Result<(), UndoError> {
        let AnalysisResult { mut att, .. } = analysis;
        let losers_started = att.len();
        let mut stats = UndoStats::default();

        // Max-heap of (Lsn, TransactionId). Tuples order by LSN first; the
        // tid never matters as a tiebreaker because LSNs are unique.
        let mut heap: BinaryHeap<(Lsn, TransactionId)> = att
            .iter()
            .filter(|(_, e)| e.undo_next_lsn != Lsn::INVALID)
            .map(|(tid, e)| (e.undo_next_lsn, *tid))
            .collect();

        while let Some((lsn, tid)) = heap.pop() {
            let record = reader.read_at(lsn)?;
            match record.body {
                LogRecordBody::Clr { undo_next_lsn, .. } => {
                    stats.clrs_followed += 1;
                    // CLRs are never undone — they ARE the undo. Just follow
                    // the pointer to the next record in the chain.
                    Self::advance_or_end(tid, undo_next_lsn, &mut att, wal, &mut heap, &mut stats)?;
                }
                LogRecordBody::Update {
                    page_id, before, ..
                }
                | LogRecordBody::Insert {
                    page_id, before, ..
                }
                | LogRecordBody::Delete {
                    page_id, before, ..
                } => {
                    Self::undo_data_record(
                        tid,
                        page_id,
                        &before,
                        record.header.prev_lsn,
                        &mut att,
                        wal,
                        buffer_pool,
                        &mut heap,
                        &mut stats,
                    )?;
                }
                LogRecordBody::Begin => {
                    Self::finalize_loser(tid, &mut att, wal, &mut stats)?;
                }
                _ => return Err(UndoError::UnexpectedRecord { tid, lsn }),
            }
        }

        tracing::debug!(
            losers_started,
            clrs_followed = stats.clrs_followed,
            records_undone = stats.records_undone,
            clrs_written = stats.clrs_written,
            losers_finalized = stats.losers_finalized,
            skipped_unregistered = stats.skipped_unregistered,
            "undo pass complete"
        );
        Ok(())
    }

    /// Undoes one data record: restores the before-image to the page, emits a
    /// CLR, and either pushes the next chain link onto the heap or finalizes
    /// the loser if the chain is exhausted.
    ///
    /// The CLR's `undo_next_lsn` is set to `rec_prev_lsn` — the LSN of the
    /// record that came *before* the one we just undid in this loser's chain.
    /// This is the field that makes crash-during-undo recoverable: on the
    /// next recovery, Analysis sees the CLR, advances `undo_next_lsn`, and
    /// Undo resumes past the record we already compensated.
    ///
    /// `before` must be exactly [`PAGE_SIZE`] bytes; otherwise the log record
    /// is corrupt and we return [`UndoError::ImageSizeMismatch`].
    ///
    /// [`PAGE_SIZE`]: crate::PAGE_SIZE
    #[allow(clippy::too_many_arguments)]
    fn undo_data_record(
        tid: TransactionId,
        page_id: PageId,
        before: &[u8],
        rec_prev_lsn: Lsn,
        att: &mut HashMap<TransactionId, AttEntry>,
        wal: &Arc<Wal>,
        buffer_pool: &Arc<PageStore>,
        heap: &mut BinaryHeap<(Lsn, TransactionId)>,
        stats: &mut UndoStats,
    ) -> Result<(), UndoError> {
        stats.records_undone += 1;
        let before_array: [u8; PAGE_SIZE] =
            before
                .try_into()
                .map_err(|_| UndoError::ImageSizeMismatch {
                    page_id,
                    expected_size: PAGE_SIZE,
                    got_size: before.len(),
                })?;

        // The page may belong to a file the catalog hasn't registered yet
        // (same convention as Redo). If so we can't write the page, but we
        // still need to advance the chain so the loser eventually finalizes.
        let guard = match buffer_pool.fetch_for_recovery(page_id) {
            Ok(g) => g,
            Err(PageStoreError::FileNotRegistered(fid)) => {
                stats.skipped_unregistered += 1;
                tracing::warn!(
                    file_id = ?fid,
                    ?page_id,
                    "file not registered during undo — chain advanced without page write"
                );
                return Self::advance_or_end(tid, rec_prev_lsn, att, wal, heap, stats);
            }
            Err(e) => return Err(UndoError::Page(e)),
        };

        // The CLR's prev_lsn continues the loser's per-txn chain from its
        // current tail (which may be the original record OR a previous CLR
        // we just wrote for an earlier undo step).
        let clr_prev = att.get(&tid).expect("loser missing from ATT").last_lsn;
        let clr_lsn = wal.log_clr(tid, clr_prev, page_id, before.to_vec(), rec_prev_lsn)?;
        stats.clrs_written += 1;

        // Stamping page_lsn = clr_lsn is what makes Redo idempotent on
        // re-run: the page_lsn >= record_lsn check will skip this CLR.
        guard.write(&before_array, clr_lsn);
        att.get_mut(&tid).expect("loser missing from ATT").last_lsn = clr_lsn;

        Self::advance_or_end(tid, rec_prev_lsn, att, wal, heap, stats)
    }

    /// Advances the loser's undo chain one step, or finalizes it if the chain
    /// is exhausted (`next_lsn == Lsn::INVALID`).
    ///
    /// Used both after undoing a data record (where `next_lsn = rec.prev_lsn`)
    /// and when following a CLR (where `next_lsn = clr.undo_next_lsn`).
    fn advance_or_end(
        tid: TransactionId,
        next_lsn: Lsn,
        att: &mut HashMap<TransactionId, AttEntry>,
        wal: &Arc<Wal>,
        heap: &mut BinaryHeap<(Lsn, TransactionId)>,
        stats: &mut UndoStats,
    ) -> Result<(), UndoError> {
        if next_lsn == Lsn::INVALID {
            Self::finalize_loser(tid, att, wal, stats)
        } else {
            att.get_mut(&tid)
                .expect("loser missing from ATT")
                .undo_next_lsn = next_lsn;
            heap.push((next_lsn, tid));
            Ok(())
        }
    }

    /// Writes an `End` record for `tid` and removes it from the ATT.
    ///
    /// Called when a loser's chain is fully undone (we reached either a
    /// `Begin` record or `Lsn::INVALID` via the chain pointers). The `End`
    /// record is the durable signal that this loser is done; on re-crash
    /// Analysis will see it and not put `tid` back in the ATT.
    fn finalize_loser(
        tid: TransactionId,
        att: &mut HashMap<TransactionId, AttEntry>,
        wal: &Arc<Wal>,
        stats: &mut UndoStats,
    ) -> Result<(), UndoError> {
        stats.losers_finalized += 1;
        let prev = att.get(&tid).expect("loser missing from ATT").last_lsn;
        wal.log_end(tid, prev)?;
        att.remove(&tid);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use fallible_iterator::FallibleIterator;
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::{
        PAGE_SIZE,
        buffer_pool::page_store::PageStore,
        primitives::{FileId, Lsn, PageId, PageNumber, TransactionId},
        recovery::{AnalysisResult, AttEntry},
        wal::{
            log::{LogRecordType, TxnStatus},
            reader::WalReader,
            writer::Wal,
        },
    };

    fn tid(n: u64) -> TransactionId {
        TransactionId::new(n)
    }

    fn pid(file: u64, page: u32) -> PageId {
        PageId::new(FileId::new(file), PageNumber::new(page))
    }

    /// Returns a [`PAGE_SIZE`] image with every byte set to `tag`.
    fn page_image(tag: u8) -> Vec<u8> {
        vec![tag; PAGE_SIZE]
    }

    /// Creates a WAL (`buf_size=0`, direct writes) and `PageStore` backed by a
    /// single data file pre-extended to `num_pages` pages.  Both share one
    /// temp directory whose path is returned so tests can open a `WalReader` on
    /// the same WAL file.
    fn make_env(num_pages: u32) -> (Arc<Wal>, Arc<PageStore>, TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let data_path = dir.path().join("data.db");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&data_path)
            .unwrap();
        f.set_len(num_pages as u64 * PAGE_SIZE as u64).unwrap();
        store.register_file(FileId::new(1), &data_path).unwrap();

        (wal, store, dir)
    }

    /// Writes `bytes` into `page_id`'s buffer-pool frame, stamped at `lsn`.
    fn seed_page(store: &PageStore, page_id: PageId, bytes: &[u8], lsn: Lsn) {
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(&bytes[..PAGE_SIZE]);
        let guard = store.fetch_for_recovery(page_id).unwrap();
        guard.write(&data, lsn);
    }

    /// Returns the raw bytes of `page_id` from the buffer pool.
    fn read_page(store: &PageStore, page_id: PageId) -> [u8; PAGE_SIZE] {
        store.fetch_for_recovery(page_id).unwrap().read()
    }

    /// Builds the minimal `AnalysisResult` that Undo needs: just the ATT.
    /// DPT and `redo_lsn` are unused by the Undo pass.
    fn analysis(losers: Vec<(TransactionId, AttEntry)>) -> AnalysisResult {
        AnalysisResult {
            att: losers.into_iter().collect(),
            dpt: HashMap::new(),
            redo_lsn: Lsn(0),
        }
    }

    // ── happy path ────────────────────────────────────────────────────────────

    /// An empty ATT means no losers — Undo must return Ok without doing anything.
    #[test]
    fn test_run_undo_empty_att_is_noop() {
        let (wal, store, dir) = make_env(0);
        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let result = Aries::run_undo(&mut reader, &wal, &store, analysis(vec![]));
        assert!(
            result.is_ok(),
            "empty ATT must not error: {:?}",
            result.err()
        );
    }

    /// Single loser with one Insert: the before-image must be written back to
    /// the page, proving that `undo_data_record` and `finalize_loser` both ran.
    #[test]
    fn test_run_undo_single_insert_restores_before_image() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0xAA);
        let after = page_image(0xBB);

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, page_id, before.clone(), after.clone())
            .unwrap();
        seed_page(&store, page_id, &after, insert);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: insert,
                undo_next_lsn: insert,
            })]),
        )
        .unwrap();

        assert_eq!(
            read_page(&store, page_id)[0],
            0xAA,
            "before-image must be restored after undoing Insert"
        );
    }

    /// Single loser with one Update: before-image restored (same path as Insert).
    #[test]
    fn test_run_undo_update_restores_before_image() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0x11);
        let after = page_image(0x22);

        let _begin = wal.log_begin(t1).unwrap();
        let update = wal
            .log_update(t1, page_id, before.clone(), after.clone())
            .unwrap();
        seed_page(&store, page_id, &after, update);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: update,
                undo_next_lsn: update,
            })]),
        )
        .unwrap();

        assert_eq!(
            read_page(&store, page_id)[0],
            0x11,
            "before-image must be restored after undoing Update"
        );
    }

    /// Single loser with one Delete: before-image restored.
    #[test]
    fn test_run_undo_delete_restores_before_image() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0xCC);
        let after = page_image(0x00);

        let _begin = wal.log_begin(t1).unwrap();
        let delete = wal
            .log_delete(t1, page_id, before.clone(), after.clone())
            .unwrap();
        seed_page(&store, page_id, &after, delete);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: delete,
                undo_next_lsn: delete,
            })]),
        )
        .unwrap();

        assert_eq!(
            read_page(&store, page_id)[0],
            0xCC,
            "before-image must be restored after undoing Delete"
        );
    }

    /// The entire before-image (not just byte 0) must be copied onto the page.
    /// Uses a pattern with a distinctive non-zero interior byte to catch a
    /// partial-write regression.
    #[test]
    fn test_run_undo_full_before_image_is_written_byte_for_byte() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);

        let mut before = [0x5A_u8; PAGE_SIZE];
        before[PAGE_SIZE / 2] = 0xFF; // distinctive interior byte
        let after = [0xA5_u8; PAGE_SIZE];

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, page_id, before.to_vec(), after.to_vec())
            .unwrap();
        seed_page(&store, page_id, &after, insert);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: insert,
                undo_next_lsn: insert,
            })]),
        )
        .unwrap();

        assert_eq!(
            read_page(&store, page_id),
            before,
            "the entire before-image must be on the page after undo"
        );
    }

    /// A loser with three data ops on two pages must have all three undone.
    /// The max-heap guarantees reverse-LSN order: op3 first, then op2, then op1.
    #[test]
    fn test_run_undo_multiple_ops_all_undone_in_reverse_order() {
        let (wal, store, dir) = make_env(4);
        let t1 = tid(1);
        let p0 = pid(1, 0);
        let p1 = pid(1, 1);

        let _begin = wal.log_begin(t1).unwrap();
        // op1: Insert on p0
        let _op1 = wal
            .log_insert(t1, p0, page_image(0xA0), page_image(0xA1))
            .unwrap();
        // op2: Update on p1
        let op2 = wal
            .log_update(t1, p1, page_image(0xB0), page_image(0xB1))
            .unwrap();
        // op3: Update on p0 — its before-image must be the state after op1
        let op3 = wal
            .log_update(t1, p0, page_image(0xA1), page_image(0xA2))
            .unwrap();

        seed_page(&store, p0, &page_image(0xA2), op3);
        seed_page(&store, p1, &page_image(0xB1), op2);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: op3,
                undo_next_lsn: op3,
            })]),
        )
        .unwrap();

        // p0: op3 undone (→ 0xA1), then op1 undone (→ 0xA0).
        assert_eq!(
            read_page(&store, p0)[0],
            0xA0,
            "p0 must be at T1's initial before-image"
        );
        // p1: op2 undone (→ 0xB0).
        assert_eq!(
            read_page(&store, p1)[0],
            0xB0,
            "p1 must be at T1's before-image"
        );
    }

    /// Two independent losers on separate pages — both must be fully rolled back.
    #[test]
    fn test_run_undo_two_losers_on_separate_pages_both_restored() {
        let (wal, store, dir) = make_env(4);
        let t1 = tid(1);
        let t2 = tid(2);
        let p0 = pid(1, 0);
        let p1 = pid(1, 1);

        let _b1 = wal.log_begin(t1).unwrap();
        let i1 = wal
            .log_insert(t1, p0, page_image(0xAA), page_image(0xBB))
            .unwrap();
        let _b2 = wal.log_begin(t2).unwrap();
        let i2 = wal
            .log_insert(t2, p1, page_image(0xCC), page_image(0xDD))
            .unwrap();

        seed_page(&store, p0, &page_image(0xBB), i1);
        seed_page(&store, p1, &page_image(0xDD), i2);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![
                (t1, AttEntry {
                    status: TxnStatus::Running,
                    last_lsn: i1,
                    undo_next_lsn: i1,
                }),
                (t2, AttEntry {
                    status: TxnStatus::Running,
                    last_lsn: i2,
                    undo_next_lsn: i2,
                }),
            ]),
        )
        .unwrap();

        assert_eq!(read_page(&store, p0)[0], 0xAA, "T1's page must be restored");
        assert_eq!(read_page(&store, p1)[0], 0xCC, "T2's page must be restored");
    }

    /// Two losers both wrote to the same page. The max-heap must undo them in
    /// globally-reverse LSN order (T2's Insert first, then T1's Insert) so the
    /// page ends up at T1's before-image — not at an intermediate state.
    #[test]
    fn test_run_undo_two_losers_same_page_undone_in_reverse_lsn_order() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let t2 = tid(2);
        let page_id = pid(1, 0);

        // T1 wrote first; T2 wrote on top of T1.
        let _b1 = wal.log_begin(t1).unwrap();
        let i1 = wal
            .log_insert(t1, page_id, page_image(0x10), page_image(0x20))
            .unwrap();
        let _b2 = wal.log_begin(t2).unwrap();
        // T2's before-image is T1's after-image; after T2 the page
        let i2 = wal
            .log_insert(t2, page_id, page_image(0x20), page_image(0x30))
            .unwrap();

        // Page is at T2's after-state at the start of Undo.
        seed_page(&store, page_id, &page_image(0x30), i2);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![
                (t1, AttEntry {
                    status: TxnStatus::Running,
                    last_lsn: i1,
                    undo_next_lsn: i1,
                }),
                (t2, AttEntry {
                    status: TxnStatus::Running,
                    last_lsn: i2,
                    undo_next_lsn: i2,
                }),
            ]),
        )
        .unwrap();

        // T2 undone first (0x30 → 0x20), then T1 (0x20 → 0x10).
        assert_eq!(
            read_page(&store, page_id)[0],
            0x10,
            "page must reach T1's before-image after both losers are undone in LSN order"
        );
    }

    /// Undo must append a CLR and an End record to the WAL for each loser.
    /// Verified by re-scanning the file after the pass completes.
    #[test]
    fn test_run_undo_writes_clr_and_end_records_to_wal() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0xAA);
        let after = page_image(0xBB);

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, page_id, before.clone(), after.clone())
            .unwrap();
        seed_page(&store, page_id, &after, insert);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: insert,
                undo_next_lsn: insert,
            })]),
        )
        .unwrap();

        // Re-scan the WAL file and collect record types in order.
        let mut reader2 = WalReader::open(&dir.path().join("wal")).unwrap();
        let mut types = Vec::new();
        while let Some(rec) = reader2.next().unwrap() {
            types.push(rec.header.record_type);
        }

        // Expected sequence: Begin, Insert, CLR (undo of Insert), End (loser finalized).
        assert_eq!(types.len(), 4, "expected 4 records but got {types:?}");
        assert!(
            matches!(types[2], LogRecordType::Clr),
            "third record must be a CLR, got {:?}",
            types[2]
        );
        assert!(
            matches!(types[3], LogRecordType::End),
            "fourth record must be an End, got {:?}",
            types[3]
        );
    }

    // ── CLR handling ──────────────────────────────────────────────────────────

    /// If `undo_next_lsn` in the ATT points directly at a CLR (which Analysis
    /// would set after seeing a CLR in the log), Undo must follow the CLR's
    /// `undo_next_lsn` and NOT write the before-image again.
    ///
    /// We seed the page with a sentinel (0xFF) that is different from both the
    /// CLR's `after` field (0xAA) and the Insert's `after` field (0xBB).
    /// If the CLR arm incorrectly tries to "undo the CLR," it would write 0xAA
    /// to the page; the 0xFF sentinel lets us catch that.
    #[test]
    fn test_run_undo_clr_in_chain_is_followed_not_re_undone() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0xAA);
        let after = page_image(0xBB);

        let begin_lsn = wal.log_begin(t1).unwrap();
        let insert_lsn = wal
            .log_insert(t1, page_id, before.clone(), after.clone())
            .unwrap();

        // A CLR was written during a prior partial-undo pass.
        // Its undo_next_lsn = begin_lsn (points past the Insert to Begin).
        // `after` = before-image of the Insert (the page state after undo was applied).
        let clr_lsn = wal
            .log_clr(t1, insert_lsn, page_id, before.clone(), begin_lsn)
            .unwrap();

        // Seed with a sentinel that is neither 0xAA (CLR.after) nor 0xBB (Insert.after).
        // If the CLR arm wrongly writes to the page we'll see 0xAA, not 0xFF.
        seed_page(&store, page_id, &page_image(0xFF), clr_lsn);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: clr_lsn,
                undo_next_lsn: clr_lsn, // ATT points AT the CLR itself
            })]),
        )
        .unwrap();

        assert_eq!(
            read_page(&store, page_id)[0],
            0xFF,
            "CLR in the undo chain must be followed (not re-applied); page must stay at sentinel"
        );
    }

    /// A loser whose `undo_next_lsn == Lsn::INVALID` is filtered out of the
    /// heap entirely: no records are read, no page is written, no End is emitted.
    /// This covers the `filter(|e| e.undo_next_lsn != INVALID)` branch in
    /// `run_undo`.
    #[test]
    fn test_run_undo_loser_with_invalid_undo_next_is_skipped() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);
        let before = page_image(0xAA);
        let after = page_image(0xBB);

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, page_id, before.clone(), after.clone())
            .unwrap();
        let clr = wal
            .log_clr(t1, insert, page_id, before.clone(), Lsn::INVALID)
            .unwrap();

        // Seed with 0xAA — if Undo wrongly processes the loser it would write
        // the before-image again (still 0xAA), making the test vacuous; but the
        // real check is that run_undo returns Ok and doesn't panic.
        seed_page(&store, page_id, &before, clr);

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let result = Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: clr,
                undo_next_lsn: Lsn::INVALID, // fully compensated — nothing to undo
            })]),
        );
        assert!(
            result.is_ok(),
            "loser with INVALID undo_next_lsn must not cause an error: {:?}",
            result.err()
        );

        // WAL must NOT have grown with a new End record (the loser was skipped).
        let mut reader2 = WalReader::open(&dir.path().join("wal")).unwrap();
        let count = {
            let mut n = 0usize;
            while reader2.next().unwrap().is_some() {
                n += 1;
            }
            n
        };
        assert_eq!(
            count, 3,
            "no End record should be appended for a skipped loser"
        );
    }

    /// A before-image that is not exactly `PAGE_SIZE` bytes must surface as
    /// `UndoError::ImageSizeMismatch` — the WAL record is corrupt.
    #[test]
    fn test_run_undo_wrong_before_image_size_returns_image_size_mismatch() {
        let (wal, store, dir) = make_env(2);
        let t1 = tid(1);
        let page_id = pid(1, 0);

        let bad_before = vec![0xAA_u8; 16]; // intentionally too short
        let after = page_image(0xBB);

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, page_id, bad_before.clone(), after.clone())
            .unwrap();

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let err = Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: insert,
                undo_next_lsn: insert,
            })]),
        )
        .unwrap_err();

        assert!(
            matches!(err, UndoError::ImageSizeMismatch {
                got_size: 16,
                expected_size: PAGE_SIZE,
                ..
            }),
            "expected ImageSizeMismatch(16), got {err:?}"
        );
    }

    /// A record type that cannot appear in a loser's undo chain (here: Commit)
    /// must return `UndoError::UnexpectedRecord`.
    #[test]
    fn test_run_undo_unexpected_record_type_returns_error() {
        let (wal, store, dir) = make_env(0);
        let t1 = tid(1);

        let _begin = wal.log_begin(t1).unwrap();
        let commit = wal.log_commit(t1).unwrap(); // Commit is unexpected in an undo chain

        // Manually craft an ATT entry whose undo_next_lsn points at the Commit.
        // This simulates a corrupt ATT or an as-yet unimplemented abort path.
        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let err = Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: commit,
                undo_next_lsn: commit,
            })]),
        )
        .unwrap_err();

        assert!(
            matches!(err, UndoError::UnexpectedRecord { .. }),
            "Commit in an undo chain must surface as UnexpectedRecord, got {err:?}"
        );
    }

    /// If a page's file is not registered (catalog hasn't opened it yet),
    /// `undo_data_record` must skip the page write, advance the loser's chain,
    /// and eventually finalize the loser — returning `Ok(())`.
    #[test]
    fn test_run_undo_unregistered_file_skips_page_write_and_finalizes() {
        let (wal, store, dir) = make_env(0); // FileId(1) has 0 pages; FileId(99) not registered
        let t1 = tid(1);
        let unknown = pid(99, 0); // FileId(99) never registered
        let before = page_image(0xAA);
        let after = page_image(0xBB);

        let _begin = wal.log_begin(t1).unwrap();
        let insert = wal
            .log_insert(t1, unknown, before.clone(), after.clone())
            .unwrap();

        let mut reader = WalReader::open(&dir.path().join("wal")).unwrap();
        let result = Aries::run_undo(
            &mut reader,
            &wal,
            &store,
            analysis(vec![(t1, AttEntry {
                status: TxnStatus::Running,
                last_lsn: insert,
                undo_next_lsn: insert,
            })]),
        );

        assert!(
            result.is_ok(),
            "unregistered file must not bubble up as an error: {:?}",
            result.err()
        );

        // The loser must still have been finalized: a CLR (chain advanced) and
        // an End record should have been appended.
        let mut reader2 = WalReader::open(&dir.path().join("wal")).unwrap();
        let types: Vec<_> = {
            let mut v = Vec::new();
            while let Some(rec) = reader2.next().unwrap() {
                v.push(rec.header.record_type);
            }
            v
        };
        assert!(
            types.contains(&LogRecordType::End),
            "loser with unregistered file must still be finalized with an End record; got {types:?}"
        );
    }
}
