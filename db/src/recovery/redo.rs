//! ARIES Redo pass: replay missing page changes from the WAL.
//!
//! Redo scans the log forward from [`AnalysisResult::redo_lsn`] and, for each
//! data record (Update, Insert, Delete, CLR), decides whether the logged
//! after-image still needs to be applied to the page in the buffer pool. The
//! decision uses ARIES's four-check rule implemented in [`maybe_redo`]:
//!
//! 1. Page must appear in the DPT from Analysis.
//! 2. Record LSN must be at or after that page's `rec_lsn`.
//! 3. Page's on-disk `page_lsn` must be less than the record's LSN.
//! 4. If all pass, copy the after-image into the frame and stamp `page_lsn`.
//!
//! Redo is idempotent: re-running after a partial apply skips records already
//! reflected on the page. Pages whose heap file is not registered yet are
//! skipped with a warning (catalog runs after recovery today).

use std::sync::Arc;

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use super::{AnalysisResult, Aries};
use crate::{
    PAGE_SIZE,
    buffer_pool::page_store::{PageStore, PageStoreError},
    primitives::{Lsn, PageId},
    wal::{WalError, log::LogRecordBody, reader::WalReader},
};

/// Failure modes specific to the Redo pass.
#[derive(Debug, Error)]
pub enum RedoError {
    /// WAL seek, read, or decode failed while scanning from `redo_lsn`.
    #[error("WAL error during redo: {0}")]
    Wal(#[from] WalError),

    /// Buffer pool I/O failed for a page that must be read or written.
    ///
    /// [`PageStoreError::FileNotRegistered`] is not mapped here — those pages
    /// are skipped with a warning because heap files are registered after recovery.
    #[error("page store error during redo: {0}")]
    Page(#[from] PageStoreError),

    /// A logged after-image is not exactly [`PAGE_SIZE`] bytes.
    ///
    /// After-images are full-page snapshots taken right after the mutation.
    /// Any other length means the WAL record is corrupt.
    #[error(
        "after-image size mismatch for {page_id:?}: \
         expected {expected_size} bytes, got {got_size}"
    )]
    ImageSizeMismatch {
        page_id: PageId,
        expected_size: usize,
        got_size: usize,
    },
}

#[derive(Default)]
struct RedoStats {
    records_examined: usize,
    pages_applied: usize,
    skipped_not_in_dpt: usize,
    skipped_before_rec_lsn: usize,
    skipped_already_on_disk: usize,
    skipped_unregistered: usize,
}

impl Aries {
    /// Runs the Redo pass: replay WAL records from `analysis.redo_lsn` forward.
    ///
    /// Only records with an after-image (Update, Insert, Delete, CLR) are
    /// candidates. Lifecycle and checkpoint records are ignored. Each candidate
    /// is handed to [`maybe_redo`] for the four-check decision.
    ///
    /// # Errors
    ///
    /// Returns [`RedoError::Wal`] on seek/read failures,
    /// [`RedoError::Page`] on buffer-pool errors other than unregistered files,
    /// and [`RedoError::ImageSizeMismatch`] when an after-image length is wrong.
    #[tracing::instrument(
        name = "aries_redo",
        skip(reader, analysis, buffer_pool),
        fields(redo_lsn = ?analysis.redo_lsn)
    )]
    pub(super) fn run_redo(
        reader: &mut WalReader,
        analysis: &AnalysisResult,
        buffer_pool: &Arc<PageStore>,
    ) -> Result<(), RedoError> {
        reader.seek_to(analysis.redo_lsn).map_err(RedoError::Wal)?;

        let mut stats = RedoStats::default();
        while let Some(record) = reader.next().map_err(RedoError::Wal)? {
            let lsn = record.header.lsn;

            // Only data-mutation records carry an after-image that can be
            // replayed onto a page.  Lifecycle records (Begin, Commit, Abort,
            // End) and checkpoint markers have no page content to redo.
            match record.body {
                LogRecordBody::Update { page_id, after, .. }
                | LogRecordBody::Insert { page_id, after, .. }
                | LogRecordBody::Delete { page_id, after, .. }
                | LogRecordBody::Clr { page_id, after, .. } => {
                    stats.records_examined += 1;
                    Self::maybe_redo(lsn, page_id, &after, analysis, buffer_pool, &mut stats)?;
                }

                _ => {}
            }
        }

        tracing::debug!(
            records_examined = stats.records_examined,
            pages_applied = stats.pages_applied,
            skipped_not_in_dpt = stats.skipped_not_in_dpt,
            skipped_before_rec_lsn = stats.skipped_before_rec_lsn,
            skipped_already_on_disk = stats.skipped_already_on_disk,
            skipped_unregistered = stats.skipped_unregistered,
            "redo pass complete"
        );
        Ok(())
    }

    /// Applies one log record's after-image if the four-check rule says it is needed.
    ///
    /// Returns `Ok(())` when the record is skipped (already on disk, not in DPT,
    /// or file not registered) as well as when the after-image is written.
    ///
    /// # Errors
    ///
    /// Returns [`RedoError::Page`] for buffer-pool failures other than
    /// [`PageStoreError::FileNotRegistered`], and [`RedoError::ImageSizeMismatch`]
    /// when `after` is not [`PAGE_SIZE`] bytes.
    ///
    /// # Panics
    ///
    /// Panics if a fetched page buffer is shorter than 8 bytes (should never
    /// happen for a valid frame).
    fn maybe_redo(
        lsn: Lsn,
        page_id: PageId,
        after: &[u8],
        analysis: &AnalysisResult,
        buffer_pool: &Arc<PageStore>,
        stats: &mut RedoStats,
    ) -> Result<(), RedoError> {
        let AnalysisResult {
            dpt: dirty_pages, ..
        } = analysis;
        // Is page_id in the DPT?
        //
        // The DPT only contains pages that were dirty at (or after) the last
        // checkpoint.  If a page is absent, it was clean throughout — the
        // on-disk copy is guaranteed to include this change. We Skip immediately,
        // no I/O required.
        let Some(dpt_entry) = dirty_pages.get(&page_id) else {
            stats.skipped_not_in_dpt += 1;
            return Ok(());
        };

        // Is dpt[page_id].rec_lsn > lsn?
        //
        // rec_lsn is the earliest LSN that dirtied this page since the last
        // checkpoint.  Any log record with lsn < rec_lsn was already flushed
        // to disk by the buffer pool before the crash — skip it.
        if dpt_entry.rec_lsn > lsn {
            stats.skipped_before_rec_lsn += 1;
            return Ok(());
        }

        let guard = match buffer_pool.fetch_for_recovery(page_id) {
            Ok(g) => g,
            Err(PageStoreError::FileNotRegistered(fid)) => {
                stats.skipped_unregistered += 1;
                tracing::warn!(
                    file_id = ?fid,
                    ?page_id,
                    ?lsn,
                    "file not registered during redo — page skipped; \
                     register data files before recovery to apply this change"
                );
                return Ok(());
            }
            Err(e) => return Err(RedoError::Page(e)),
        };

        let page_bytes = guard.read();
        let page_lsn = Lsn(u64::from_le_bytes(
            page_bytes[..8]
                .try_into()
                .expect("page buffer is always at least 8 bytes"),
        ));

        if page_lsn >= lsn {
            stats.skipped_already_on_disk += 1;
            return Ok(());
        }

        let page_data: [u8; PAGE_SIZE] =
            after.try_into().map_err(|_| RedoError::ImageSizeMismatch {
                page_id,
                expected_size: PAGE_SIZE,
                got_size: after.len(),
            })?;

        guard.write(&page_data, lsn);
        stats.pages_applied += 1;
        tracing::trace!(?page_id, ?lsn, "redo applied after-image");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, os::unix::fs::FileExt, path::Path, time::SystemTime};

    use tempfile::{NamedTempFile, TempDir, tempdir};

    use super::*;
    use crate::{
        codec::Encode,
        primitives::{FileId, PageNumber, TransactionId},
        recovery::{AttEntry, DptEntry},
        wal::{log::LogRecord, writer::Wal},
    };

    // ── helpers ─────────────────────────────────────────────────────────────

    fn tid(n: u64) -> TransactionId {
        TransactionId::new(n)
    }

    fn pid(file: u64, page: u32) -> PageId {
        PageId::new(FileId::new(file), PageNumber::new(page))
    }

    fn ts() -> SystemTime {
        SystemTime::UNIX_EPOCH
    }

    /// Builds a `PAGE_SIZE`-byte page image whose first 8 bytes encode `page_lsn`
    /// (so the page parses back to the expected LSN through `from_le_bytes`) and
    /// whose remaining bytes are filled with `tag` for easy identity checks.
    fn page_image(page_lsn: Lsn, tag: u8) -> Vec<u8> {
        let mut v = vec![tag; PAGE_SIZE];
        v[..8].copy_from_slice(&page_lsn.0.to_le_bytes());
        v
    }

    /// Writes records to a fresh temp WAL file in order, assigning each
    /// record's LSN to its byte offset in the file (the ARIES convention used
    /// by `WalReader::seek_to`).  Returns the temp file plus the LSN actually
    /// stamped onto each record's header.
    ///
    /// Computing offsets at write-time saves us from hard-coding record sizes
    /// (which change with the page size and image lengths).
    fn write_log(bodies: Vec<(TransactionId, LogRecordBody)>) -> (NamedTempFile, Vec<Lsn>) {
        let mut f = NamedTempFile::new().unwrap();
        let mut lsns = Vec::with_capacity(bodies.len());
        let mut prev = Lsn::INVALID;
        for (t, body) in bodies {
            let offset = f.as_file().metadata().unwrap().len();
            let lsn = Lsn(offset);
            let rec = LogRecord::new(lsn, prev, t, ts(), body).unwrap();
            rec.encode(f.as_file_mut()).unwrap();
            lsns.push(lsn);
            prev = lsn;
        }
        f.as_file().sync_all().unwrap();
        (f, lsns)
    }

    /// Creates a `PageStore` backed by a dummy WAL and registers a single data
    /// file pre-extended to `num_pages` pages of zeros.  The WAL is never
    /// exercised by Redo (Redo only writes the buffer pool frame; it does not
    /// flush or force), so a no-op `Wal` is fine here.
    fn make_store_with_file(
        cap: usize,
        file_id: FileId,
        num_pages: u32,
    ) -> (Arc<PageStore>, TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("dummy.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(cap, wal));

        let data_path = dir.path().join("data.db");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&data_path)
            .unwrap();
        f.set_len(u64::from(num_pages) * PAGE_SIZE as u64).unwrap();
        store.register_file(file_id, &data_path).unwrap();
        (store, dir, data_path)
    }

    /// Hand-builds an `AnalysisResult` carrying only the DPT — Redo never
    /// touches the ATT, so we leave it empty.  `redo_lsn` is derived the same
    /// way the real Analysis pass derives it: the minimum `rec_lsn`.
    fn analysis_with(dpt_entries: &[(PageId, Lsn)]) -> AnalysisResult {
        let dpt: HashMap<PageId, DptEntry> = dpt_entries
            .iter()
            .map(|(p, l)| (*p, DptEntry { rec_lsn: *l }))
            .collect();
        let redo_lsn = AnalysisResult::compute_redo_lsn(&dpt);
        AnalysisResult {
            att: HashMap::<TransactionId, AttEntry>::new(),
            dpt,
            redo_lsn,
        }
    }

    /// Overwrites page `page_no` on disk with `bytes`.  Lets a test seed an
    /// already-advanced `page_lsn` (first 8 bytes) to exercise the third check
    /// in the four-check rule.
    fn write_disk_page(path: &Path, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_at(bytes, u64::from(page_no) * PAGE_SIZE as u64)
            .unwrap();
        f.sync_all().unwrap();
    }

    /// Reads `page_id` through the buffer pool.  After Redo runs, the pool
    /// holds whatever Redo applied (or, for untouched pages, fresh zeros read
    /// from disk on this fetch).  We deliberately verify via the pool rather
    /// than the disk file because the test's dummy `Wal` is never flushed —
    /// `flush_all` would block forever waiting for `force(last_lsn)` to
    /// advance.
    fn page_bytes(store: &PageStore, page_id: PageId) -> [u8; PAGE_SIZE] {
        let g = store.fetch_for_recovery(page_id).unwrap();
        g.read()
    }

    /// Decodes the `page_lsn` (first 8 bytes) from a fetched page.
    fn page_lsn_of(bytes: &[u8; PAGE_SIZE]) -> Lsn {
        Lsn(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
    }

    /// A `Begin` record placed first in the log so subsequent data records land
    /// at LSN > 0.  Required because the on-disk `page_lsn` of a fresh page is
    /// 0; if a data record's LSN were also 0, Redo's check 3 (`page_lsn >= lsn`)
    /// would trip and the record would be skipped.  Real ARIES doesn't worry
    /// about this because the first record's LSN is almost never 0 in practice
    /// (the WAL is initialized with a header), but tests need to be explicit.
    fn sentinel_begin() -> (TransactionId, LogRecordBody) {
        (tid(99), LogRecordBody::Begin)
    }

    // ── the four-check decision tree ────────────────────────────────────────

    /// Happy path: page is in the DPT (check 1 ✓), `rec_lsn ≤ record_lsn`
    /// (check 2 ✓), on-disk `page_lsn = 0 < record_lsn` (check 3 ✓).
    /// Redo must copy the after-image into the frame.
    #[test]
    fn applies_after_image_when_all_checks_pass() {
        let target = pid(1, 0);

        // Sentinel Begin pushes the real record past LSN 0 so check 3 actually
        // distinguishes "applied" from "not applied".
        let (wal, lsns) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: page_image(Lsn(0), 0xAA),
            }),
        ]);
        let record_lsn = lsns[1];

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(target, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        let bytes = page_bytes(&store, target);
        assert_eq!(bytes[8], 0xAA, "after-image bytes must land in the frame");
        assert!(record_lsn.0 > 0, "sanity: record lsn is non-zero");
    }

    /// Check 1 fails: the page is *not* in the DPT.  A clean page (one that
    /// was flushed before crash) needs no redo, and Redo must skip it without
    /// writing the after-image.
    #[test]
    fn skips_when_page_not_in_dpt() {
        let clean = pid(1, 0);
        let (wal, _) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: clean,
                before: vec![],
                after: page_image(Lsn(0), 0xCC),
            }),
        ]);

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        // DPT is empty — no page is "possibly dirty".
        let analysis = analysis_with(&[]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        // Page is still the all-zero original (Redo never even fetched it, but
        // this fetch loads it fresh from disk).  If Redo had applied the
        // after-image, we'd see 0xCC in byte 8.
        assert_eq!(page_bytes(&store, clean), [0u8; PAGE_SIZE]);
    }

    /// Check 2 fails: `dpt.rec_lsn > record_lsn`.  Any record older than the
    /// earliest unflushed change for this page is guaranteed to already be on
    /// disk (the buffer pool would have flushed it earlier).  Redo must skip.
    #[test]
    fn skips_when_rec_lsn_above_record_lsn() {
        let target = pid(1, 0);
        let (wal, lsns) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: page_image(Lsn(0), 0xDD),
            }),
        ]);
        let record_lsn = lsns[1];

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        // DPT says this page was first dirtied *after* record_lsn — so the
        // record predates the DPT and is by definition already on disk.
        // Force `redo_lsn = 0` so the scan still encounters the record
        // (otherwise WalReader would seek past it and check 2 wouldn't fire).
        let mut dpt = HashMap::new();
        dpt.insert(target, DptEntry {
            rec_lsn: Lsn(record_lsn.0 + 1),
        });
        let analysis = AnalysisResult {
            att: HashMap::new(),
            dpt,
            redo_lsn: Lsn(0),
        };

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        assert_eq!(page_bytes(&store, target), [0u8; PAGE_SIZE]);
    }

    /// Check 3 fails: on-disk `page_lsn >= record_lsn`.  The mutation is
    /// already reflected on disk — Redo must not overwrite the (possibly
    /// newer) page.  We seed disk with a page whose first 8 bytes encode a
    /// large LSN.
    #[test]
    fn skips_when_page_lsn_at_or_above_record_lsn() {
        let target = pid(1, 0);
        let (wal, lsns) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: page_image(Lsn(0), 0xEE),
            }),
        ]);
        let record_lsn = lsns[1];

        let (store, _dir, path) = make_store_with_file(4, FileId::new(1), 2);
        // Seed page on disk with page_lsn strictly above record_lsn and a
        // distinctive filler byte that should survive.
        let mut seeded = [0u8; PAGE_SIZE];
        let v = page_image(Lsn(record_lsn.0 + 100), 0x77);
        seeded.copy_from_slice(&v);
        write_disk_page(&path, 0, &seeded);

        let analysis = analysis_with(&[(target, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        // Redo loaded the page (for the check), but must NOT have overwritten
        // its bytes with the 0xEE after-image.
        let bytes = page_bytes(&store, target);
        assert_eq!(bytes[8], 0x77, "page contents must NOT be overwritten");
        assert_eq!(
            page_lsn_of(&bytes).0,
            record_lsn.0 + 100,
            "page_lsn must remain at the (higher) seeded value"
        );
    }

    // ── record-type filtering ───────────────────────────────────────────────

    /// Only `Update`, `Insert`, `Delete`, and `Clr` carry an after-image.  Other
    /// record types (`Begin`, `Commit`, `Abort`, `End`, the checkpoint markers)
    /// must be silently ignored — not fall into an error path.
    #[test]
    fn ignores_non_data_records() {
        let target = pid(1, 0);
        let (wal, _) = write_log(vec![
            (tid(1), LogRecordBody::Begin),
            (tid(1), LogRecordBody::Commit),
            (tid(2), LogRecordBody::Abort),
            (tid(2), LogRecordBody::End),
            (tid(3), LogRecordBody::CheckpointBegin),
            (tid(3), LogRecordBody::CheckpointEnd {
                att_snapshot: vec![],
                dpt_snapshot: vec![],
            }),
        ]);

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(target, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        // Page is fresh zeros — none of the records carried page bytes.
        assert_eq!(page_bytes(&store, target), [0u8; PAGE_SIZE]);
    }

    /// CLRs are redo-only records written by Undo.  They must be redone just
    /// like a regular data mutation — that's how a crash-during-undo recovers
    /// the partial rollback work it already did before the crash.
    #[test]
    fn applies_clr_after_image() {
        let target = pid(1, 0);
        let (wal, _) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Clr {
                page_id: target,
                after: page_image(Lsn(0), 0xBB),
                undo_next_lsn: Lsn::INVALID,
            }),
        ]);

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(target, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        let bytes = page_bytes(&store, target);
        assert_eq!(bytes[8], 0xBB, "CLR after-image must be applied");
    }

    /// If a page belongs to a file we have not registered yet (today, the
    /// catalog runs *after* recovery), Redo logs a warning and skips the
    /// record instead of erroring.  This keeps recovery usable while file
    /// registration is bootstrapped.
    #[test]
    fn skips_unregistered_file_with_no_error() {
        let unknown = pid(99, 0); // FileId(99) never registered
        let (wal, _) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: unknown,
                before: vec![],
                after: page_image(Lsn(0), 0xFF),
            }),
        ]);

        // Register a different file so the store is otherwise healthy.
        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(unknown, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        let result = Aries::run_redo(&mut reader, &analysis, &store);
        assert!(
            result.is_ok(),
            "unregistered file must NOT bubble up as an error: {:?}",
            result.err()
        );
    }

    /// If an after-image is not exactly `PAGE_SIZE` bytes the WAL record is
    /// corrupt — Redo must surface `ImageSizeMismatch` rather than panic on
    /// the `try_into` conversion.
    #[test]
    fn returns_image_size_mismatch_for_wrong_size() {
        let target = pid(1, 0);
        let (wal, _) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: vec![0u8; 16], // too small
            }),
        ]);

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(target, Lsn(0))]);

        let mut reader = WalReader::open(wal.path()).unwrap();
        let err = Aries::run_redo(&mut reader, &analysis, &store).unwrap_err();
        assert!(
            matches!(err, RedoError::ImageSizeMismatch {
                expected_size: PAGE_SIZE,
                got_size: 16,
                ..
            }),
            "expected ImageSizeMismatch, got {err:?}"
        );
    }

    /// Redo is idempotent: running it twice in a row over the same log must
    /// leave the buffer pool in the same state.  This is the property that
    /// makes ARIES safe against a crash *during* recovery.
    ///
    /// For check 3 to trip on the second run, the after-image's first 8 bytes
    /// must equal the record's LSN (this is what a real producer does: stamp
    /// `page_lsn` after the WAL write, then capture the page).  We achieve that
    /// with a two-pass write: the first pass discovers the LSN, the second
    /// rebuilds the log with that LSN baked into the after-image.
    #[test]
    fn redo_is_idempotent() {
        let target = pid(1, 0);

        let (_, lsns) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: page_image(Lsn(0), 0xA5), // placeholder
            }),
        ]);
        let insert_lsn = lsns[1];

        // Pass 2: bake insert_lsn into the after-image so the page that lands
        // in the frame carries `page_lsn = insert_lsn`.
        let (wal, _) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: target,
                before: vec![],
                after: page_image(insert_lsn, 0xA5),
            }),
        ]);

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);
        let analysis = analysis_with(&[(target, Lsn(0))]);

        // First run: applies the after-image.  Frame bytes now start with
        // insert_lsn (encoded into the after-image we just wrote).
        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();
        let after_first = page_bytes(&store, target);
        assert_eq!(after_first[8], 0xA5, "first run applied the after-image");
        assert_eq!(page_lsn_of(&after_first), insert_lsn);

        // Second run: check 3 trips because page_lsn (= insert_lsn) is no
        // longer less than the record's LSN, so the frame is left alone.
        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();
        let after_second = page_bytes(&store, target);
        assert_eq!(
            after_first, after_second,
            "redo must be idempotent across runs"
        );
    }

    /// Redo must seek to `analysis.redo_lsn` before iterating: records *before*
    /// that LSN are guaranteed to be on disk and must never be visited.  We
    /// place two records in the log and set `redo_lsn` to the second one's
    /// LSN — only that record should be applied.
    #[test]
    fn starts_scan_at_redo_lsn() {
        let first = pid(1, 0);
        let second = pid(1, 1);

        let (wal, lsns) = write_log(vec![
            sentinel_begin(),
            (tid(1), LogRecordBody::Insert {
                page_id: first,
                before: vec![],
                after: page_image(Lsn(0), 0xEE),
            }),
            (tid(1), LogRecordBody::Insert {
                page_id: second,
                before: vec![],
                after: page_image(Lsn(0), 0xBB),
            }),
        ]);
        let second_lsn = lsns[2];

        let (store, _dir, _path) = make_store_with_file(4, FileId::new(1), 2);

        // Put BOTH pages in the DPT so check 1 wouldn't filter anything, but
        // set `redo_lsn` = LSN of the second record.  Only the second record
        // should be visited.
        let mut dpt = HashMap::new();
        dpt.insert(first, DptEntry { rec_lsn: Lsn(0) });
        dpt.insert(second, DptEntry { rec_lsn: Lsn(0) });
        let analysis = AnalysisResult {
            att: HashMap::new(),
            dpt,
            redo_lsn: second_lsn,
        };

        let mut reader = WalReader::open(wal.path()).unwrap();
        Aries::run_redo(&mut reader, &analysis, &store).unwrap();

        // Page 0 was never visited — buffer pool fetches it fresh from disk.
        assert_eq!(
            page_bytes(&store, first),
            [0u8; PAGE_SIZE],
            "record before redo_lsn must not be applied"
        );
        // Page 1 must show the 0xBB after-image.
        assert_eq!(page_bytes(&store, second)[8], 0xBB);
    }
}
