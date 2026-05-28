//! Property-based crash-kill tests for ARIES recovery.
//!
//! These tests verify three core guarantees of the `StoreMy` recovery engine
//! across every possible crash point in the WAL:
//!
//! 1. **No error**: `Aries::recover` returns `Ok` regardless of where the WAL was truncated — torn
//!    tails are normal, not errors.
//!
//! 2. **Winner invariant**: transactions that committed before the crash (their Commit record
//!    survived the truncation) must never appear in the ATT after recovery. Appearing in the ATT
//!    would cause spurious undo of durable work.
//!
//! 3. **Idempotency**: a second recovery run on the same (post-recovery) WAL must find zero losers.
//!    The first run writes CLRs and End records; the second run's Analysis sees those and removes
//!    all losers from the ATT.
//!
//! Each test uses `proptest` to generate hundreds of (workload, `crash_point`)
//! pairs automatically. When a failure is found, proptest shrinks it to the
//! smallest input that still fails — making bugs easy to debug.
//!
//! # How the crash simulation works
//!
//! After writing a workload to a WAL file, the test truncates the file at a
//! random byte offset via `File::set_len`. This models a power loss that left
//! only `truncate_at` bytes on disk. The WAL writer used for the workload is
//! dropped before truncation; a fresh WAL writer (opened on the truncated file)
//! is passed to `Aries::recover` so CLRs and End records are appended at the
//! correct position.
//!
//! # Why a fresh `PageStore` with no registered files
//!
//! The Redo and Undo passes skip pages whose file is not registered in the
//! buffer pool (both log a warning and continue). This lets us run full
//! `Aries::recover` without setting up heap files, focusing the test on the
//! WAL-layer correctness of Analysis, Redo, and Undo.

use std::{fs, sync::Arc};

use fallible_iterator::FallibleIterator;
use proptest::prelude::*;
use storemy::{
    PAGE_SIZE,
    buffer_pool::page_store::PageStore,
    primitives::{FileId, PageId, PageNumber, TransactionId},
    recovery::Aries,
    storage::Page,
    wal::{PageLogOp, log::LogRecordBody, reader::WalReader, writer::Wal},
};
use tempfile::tempdir;

fn txn(n: u64) -> TransactionId {
    TransactionId::new(n)
}

fn page_for(n: u64) -> PageId {
    // Keep page numbers small (0..32) so multiple transactions share pages
    // and exercise the interleaving paths in Redo/Undo.
    PageId::new(
        FileId::new(1),
        PageNumber::new(u32::try_from(n % 32).expect("page index in 0..32")),
    )
}

/// A full-page image of exactly `PAGE_SIZE` bytes, every byte set to `tag`.
///
/// Both before- and after-images must be `PAGE_SIZE` bytes or the Undo pass
/// returns `ImageSizeMismatch`. Using a distinctive tag per transaction lets
/// us tell images apart when debugging a failure.
fn image(tag: u8) -> Vec<u8> {
    vec![tag; PAGE_SIZE]
}

struct TestPage {
    before: Vec<u8>,
    after: Vec<u8>,
    page_lsn: storemy::Lsn,
}

impl Page for TestPage {
    fn page_data(&self) -> [u8; PAGE_SIZE] {
        let mut page = [0; PAGE_SIZE];
        page[..self.after.len()].copy_from_slice(&self.after);
        page
    }

    fn before_image(&self) -> Option<[u8; PAGE_SIZE]> {
        let mut page = [0; PAGE_SIZE];
        page[..self.before.len()].copy_from_slice(&self.before);
        Some(page)
    }

    fn set_before_image(&mut self) {
        self.before = self.after.clone();
    }

    fn page_lsn(&self) -> storemy::Lsn {
        self.page_lsn
    }

    fn set_page_lsn(&mut self, lsn: storemy::Lsn) {
        self.page_lsn = lsn;
    }
}

/// Writes a deterministic workload to `wal` and returns every
/// `(TransactionId, commit_lsn)` pair for transactions that committed.
///
/// Workload shape (driven by `seed` and `n_txns`):
/// - Creates transactions with IDs `1 ..= n_txns`.
/// - Each does one `Insert` on a page derived from its ID.
/// - A transaction commits when `(txn_index + seed) % 3 != 0`; the rest leave their changes
///   uncommitted (they become losers after a crash).
///
/// Using `buf_size = 0` (no internal buffer) means every record is written
/// directly to the file — no background flush thread is needed, and the file
/// position (`flushed_till`) advances after each write.
fn run_workload(wal: &Wal, n_txns: usize, seed: u64) -> Vec<(TransactionId, u64)> {
    let mut committed = Vec::new();
    for i in 1..=n_txns {
        let t = txn(i as u64);
        let p = page_for(i as u64);

        wal.log_begin(t).unwrap();
        // before = tag i (original page state); after = tag i+100 (post-insert state)
        let tag = u8::try_from(i).expect("txn index fits in u8");
        let after_tag = u8::try_from(i + 100).expect("txn tag offset fits in u8");
        wal.log_page_operation(
            t,
            p,
            &mut TestPage {
                before: image(tag),
                after: image(after_tag),
                page_lsn: storemy::Lsn::INVALID,
            },
            PageLogOp::Insert,
        )
        .unwrap();

        if (i as u64 + seed) % 3 != 0 {
            let commit_lsn = wal.log_commit(t).unwrap();
            committed.push((t, commit_lsn.0));
        }
    }
    committed
}

/// Scans the WAL file at `path` (forward, stopping at the first torn record)
/// and returns the `TransactionId` of every transaction that has a complete
/// `Commit` record in the file.
///
/// This is the ground truth for "which transactions are winners in the
/// truncated WAL" — we use it to drive the assertion in `committed_txns_not_in_att`.
fn surviving_winners(wal_path: &std::path::Path) -> Vec<TransactionId> {
    let mut reader = WalReader::open(wal_path).unwrap();
    let mut winners = Vec::new();
    while let Some(rec) = reader.next().unwrap() {
        if matches!(rec.body, LogRecordBody::Commit) {
            winners.push(rec.header.tid);
        }
    }
    winners
}

proptest! {
    // 300 cases gives good coverage while keeping CI fast.
    // Each case writes a workload, truncates the WAL, and runs recovery.
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// **Property 1 — Recovery never errors at any crash point.**
    ///
    /// `Aries::recover` must return `Ok(…)` regardless of where the WAL was
    /// truncated. A torn tail is the normal result of a power loss — it is NOT
    /// an I/O error. Any `Err` here is a genuine recovery bug.
    ///
    /// `crash_byte` ranges over 0 (empty WAL) to 65 535 (well past the largest
    /// possible workload), so every record-boundary and mid-record position is
    /// exercised across proptest runs.
    #[test]
    fn recovery_never_errors(
        seed       in 0u64..8u64,
        n_txns     in 1usize..5usize,
        crash_byte in 0usize..65_536usize,
    ) {
        let dir = tempdir().unwrap();
        let wal_path    = dir.path().join("wal");
        let master_path = dir.path().join("master");

        // Phase 1 — write workload (WAL writer is dropped before truncation so
        // the OS flushes its kernel buffers before we resize the file).
        {
            let wal = Arc::new(Wal::new(&wal_path, 0).unwrap());
            run_workload(&wal, n_txns, seed);
        }

        // Phase 2 — truncate the WAL at `crash_byte`, clamped to the file length.
        let wal_len = fs::metadata(&wal_path)
            .map(|m| usize::try_from(m.len()).expect("test WAL fits in usize"))
            .unwrap_or(0);
        let truncate_at = crash_byte.min(wal_len);
        {
            // Scoped block: the write-mode handle is dropped (and the file
            // descriptor released) before the read-mode WalReader opens it.
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.set_len(truncate_at as u64).unwrap();
        }

        // Phase 3 — open a fresh WAL writer on the truncated file (its
        // `start_lsn` equals `truncate_at`, so CLRs are appended there) and
        // run full ARIES recovery.
        let recovery_wal = Arc::new(Wal::new(&wal_path, 0).unwrap());
        let recovery_bp  = Arc::new(PageStore::new(16, recovery_wal.clone()));
        let aries        = Aries::new(wal_path, master_path);

        prop_assert!(
            aries.recover(&recovery_bp, &recovery_wal).is_ok(),
            "Aries::recover must not error: \
             crash_byte={crash_byte} truncate_at={truncate_at} wal_len={wal_len}"
        );
    }

    /// **Property 2 — Committed transactions are never in the ATT after recovery.**
    ///
    /// A `TransactionId` whose `Commit` record survived the truncation is a
    /// *winner*. ARIES must never put a winner in the ATT (the Dirty Transactions
    /// Table returned by Analysis). If it does, the Undo pass would roll back
    /// durable, committed work — a correctness violation.
    ///
    /// The test determines surviving winners by re-reading the truncated WAL
    /// before invoking recovery, then checks the ATT snapshot returned by
    /// `Aries::recover`.
    #[test]
    fn committed_txns_not_in_att(
        seed       in 0u64..8u64,
        n_txns     in 1usize..5usize,
        crash_byte in 0usize..65_536usize,
    ) {
        let dir = tempdir().unwrap();
        let wal_path    = dir.path().join("wal");
        let master_path = dir.path().join("master");

        {
            let wal = Arc::new(Wal::new(&wal_path, 0).unwrap());
            run_workload(&wal, n_txns, seed);
        }

        let wal_len = fs::metadata(&wal_path)
            .map(|m| usize::try_from(m.len()).expect("test WAL fits in usize"))
            .unwrap_or(0);
        let truncate_at = crash_byte.min(wal_len);
        {
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.set_len(truncate_at as u64).unwrap();
        }

        // Ground truth: which Commit records actually survived the truncation?
        let winners = surviving_winners(&wal_path);

        let recovery_wal = Arc::new(Wal::new(&wal_path, 0).unwrap());
        let recovery_bp  = Arc::new(PageStore::new(16, recovery_wal.clone()));
        let aries        = Aries::new(wal_path, master_path);

        // If recovery itself errors, that is caught by `recovery_never_errors`.
        let Ok(snapshot) = aries.recover(&recovery_bp, &recovery_wal) else {
            return Ok(());
        };

        // The ATT snapshot shows what Analysis found BEFORE Undo ran.
        // No winning TID should ever appear there.
        for winner in &winners {
            prop_assert!(
                !snapshot.att.contains_key(winner),
                "winner tid {:?} found in ATT after recovery — \
                 ARIES invariant violated (crash_byte={crash_byte} \
                 truncate_at={truncate_at})",
                winner
            );
        }
    }

    /// **Property 3 — Recovery is idempotent: a second run always finds zero losers.**
    ///
    /// The first `Aries::recover` writes `Clr` and `End` records to the WAL for
    /// every loser it undoes. On the second run, Analysis sees those `End`
    /// records and does not add those transactions back to the ATT. The second
    /// Undo pass therefore has nothing to do.
    ///
    /// If the second run's ATT is non-empty, either:
    /// - Analysis failed to process an `End` record correctly, or
    /// - The Undo pass from the first run did not write `End` records for all
    ///   losers it processed (or wrote them at the wrong position).
    #[test]
    fn recovery_is_idempotent(
        seed       in 0u64..8u64,
        n_txns     in 1usize..5usize,
        crash_byte in 0usize..65_536usize,
    ) {
        let dir = tempdir().unwrap();
        let wal_path    = dir.path().join("wal");
        let master_path = dir.path().join("master");

        {
            let wal = Arc::new(Wal::new(&wal_path, 0).unwrap());
            run_workload(&wal, n_txns, seed);
        }

        let wal_len = fs::metadata(&wal_path)
            .map(|m| usize::try_from(m.len()).expect("test WAL fits in usize"))
            .unwrap_or(0);
        let truncate_at = crash_byte.min(wal_len);
        {
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.set_len(truncate_at as u64).unwrap();
        }


        {
            let wal1 = Arc::new(Wal::new(&wal_path, 0).unwrap());
            let bp1  = Arc::new(PageStore::new(16, wal1.clone()));
            let a1   = Aries::new(wal_path.clone(), master_path.clone());
            if a1.recover(&bp1, &wal1).is_err() {
                // First recovery errored — covered by `recovery_never_errors`.
                return Ok(());
            }
        }

        // Second recovery — the WAL now includes End records from the first run.
        // Analysis must see them and not re-add those transactions to the ATT.
        let wal2 = Arc::new(Wal::new(&wal_path, 0).unwrap());
        let bp2  = Arc::new(PageStore::new(16, wal2.clone()));
        let a2   = Aries::new(wal_path, master_path);

        let Ok(snapshot2) = a2.recover(&bp2, &wal2) else {
            return Ok(());
        };

        prop_assert!(
            snapshot2.att.is_empty(),
            "second recovery must have an empty ATT; found losers: {:?}",
            snapshot2.att.keys().collect::<Vec<_>>()
        );
    }
}
