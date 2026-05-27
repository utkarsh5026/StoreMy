//! Hash index access method.
//!
//! `HashIndex` implements the `IndexKind::Hash` access method using **static
//! (closed) hashing**: the number of buckets is fixed at `CREATE INDEX` time
//! and never grows.  Each bucket maps to exactly one head page in the index
//! file — bucket *N* lives at page *N*.  When a bucket page fills beyond what
//! a single [`crate::storage::PAGE_SIZE`]-byte frame can hold, a fresh overflow
//! page is appended at the end of the file and linked via the `overflow` pointer
//! in `HashBucketHeader` (defined in the private `bucket` module), forming a
//! singly-linked
//! chain.
//!
//! ## Lookup model
//!
//! All three operations (`insert`, `delete`, `search`) hash the composite key
//! with SipHash-1-3 and then walk the bucket's page chain until they find — or
//! exhaust — the target entry.  Range queries have no bucket shortcut and scan
//! every bucket; they exist only to satisfy the [`Index`] trait, not as a
//! performance feature of this access method.
//!
//! ## Overflow-chain safety
//!
//! The chain-walk is guarded by `MAX_OVERFLOW_HOPS` (1 000 pages) to turn a
//! file-corruption cycle into a bounded [`IndexError::CorruptIndex`] rather
//! than an infinite loop.
//!
//! Bucket/page layout and bucket-local helpers live in the private `bucket`
//! module.

mod bucket;

use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use siphasher::sip::SipHasher13;

use self::bucket::{BucketNumber, HashBucket, HashBucketHeader};
use crate::{
    FileId, Lsn, PageNumber, TransactionId, Type,
    buffer_pool::{
        LockRequest,
        page_store::{PageGuard, PageStore},
    },
    index::{CompositeKey, Index, IndexEntry, IndexError, IndexKind, PageKind},
    primitives::{PageId, RecordId},
    storage::TypedPage,
};

const MAX_OVERFLOW_HOPS: usize = 1000; // cycle-detection bound, same as Go

/// The hash-index access method. One instance per `CREATE INDEX … USING HASH`
/// declaration in the catalog.
///
/// All mutable state that changes after construction — the page count as
/// overflow pages are appended — is tracked with an [`AtomicU32`] so that
/// `HashIndex` can be shared across threads behind an `Arc` without a mutex on
/// the hot path.
pub struct HashIndex {
    /// Identity of the index's backing file in the buffer pool.
    file_id: FileId,
    /// Expected SQL type for each key column, in declaration order.
    key_types: Vec<Type>,
    /// Total number of head-bucket pages; fixed for the lifetime of the index.
    num_buckets: u32,
    /// Shared buffer pool used to read and write pages.
    store: Arc<PageStore>,
    /// Total pages allocated in the file, including head pages and any overflow
    /// pages grown via [`Self::spill_to_overflow`].
    num_pages: AtomicU32,
}

impl HashIndex {
    /// Opens an existing hash-index file (or starts a fresh one when paired
    /// with [`Self::init`]).
    ///
    /// `existing_pages` must be at least `num_buckets` — every head page must
    /// already exist in the file before any reads can succeed.  For a brand-new
    /// index, pass `num_buckets` here and then call [`Self::init`] to stamp the
    /// empty bucket headers.
    ///
    /// # Panics
    ///
    /// Panics when `key_types` is empty, `num_buckets` is zero, or
    /// `existing_pages < num_buckets`.
    pub fn new(
        file_id: FileId,
        key_types: Vec<Type>,
        num_buckets: u32,
        store: Arc<PageStore>,
        existing_pages: u32,
    ) -> Self {
        assert!(!key_types.is_empty(), "index must have at least one column");
        assert!(num_buckets > 0);
        assert!(
            existing_pages >= num_buckets,
            "existing_pages must cover at least the {num_buckets} head pages"
        );
        Self {
            file_id,
            key_types,
            num_buckets,
            store,
            num_pages: AtomicU32::new(existing_pages),
        }
    }

    /// Stamps every head page (`0..num_buckets`) with an empty bucket header.
    ///
    /// Must be called exactly once on a freshly created index file before any
    /// reads.  Calling it a second time overwrites existing entries and is not
    /// safe in production.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if any page cannot be fetched from the buffer
    /// pool or cannot be encoded and written back.
    pub fn init(&self, txn: TransactionId) -> Result<(), IndexError> {
        for i in 0..self.num_buckets {
            let pid = self.pid(PageNumber::new(i));
            let g = self.store.fetch_page(LockRequest::exclusive(txn, pid))?;
            let mut bucket = HashBucket::new_bucket(BucketNumber(i));
            Self::write_bucket(&g, &mut bucket, Lsn::INVALID)?;
        }
        Ok(())
    }

    /// Claims a fresh page number at the end of the file.
    fn allocate_page(&self) -> PageNumber {
        PageNumber::from(self.num_pages.fetch_add(1, Ordering::AcqRel))
    }

    /// Maps a [`CompositeKey`] to a bucket using SipHash-2-4 with a fixed key.
    fn hash_to_bucket(&self, key: &CompositeKey) -> BucketNumber {
        let mut h = SipHasher13::new_with_keys(0, 0);
        key.hash(&mut h);
        let bucket_u64 = h.finish() % u64::from(self.num_buckets);
        let Ok(bucket) = u32::try_from(bucket_u64) else {
            unreachable!("bucket index is bounded by num_buckets (u32)");
        };
        BucketNumber(bucket)
    }

    /// Constructs the fully qualified [`PageId`] for a page in this index file.
    fn pid(&self, p: PageNumber) -> PageId {
        PageId {
            file_id: self.file_id,
            page_no: p,
        }
    }

    /// Fetches and decodes a hash bucket page from the buffer pool.
    fn read_bucket(
        &self,
        txn: TransactionId,
        pn: PageNumber,
        exclusive: bool,
    ) -> Result<(PageGuard<'_>, HashBucket), IndexError> {
        let g = self.read_page(txn, pn, exclusive, &self.store)?;
        let bytes = g.read();
        let page = TypedPage::<HashBucketHeader, Vec<IndexEntry>>::from_page_bytes(&bytes)?;
        if page.kind != PageKind::HashBucket {
            return Err(IndexError::CorruptIndex(
                "expected HashBucket page, found another kind",
            ));
        }
        Ok((g, page))
    }

    /// Encodes `b` into the frame held by `g`, stamping it with `lsn`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if encoding the bucket body fails.
    fn write_bucket(g: &PageGuard<'_>, b: &mut HashBucket, lsn: Lsn) -> Result<(), IndexError> {
        b.page_lsn = lsn;
        let bytes = b.to_page_bytes()?;
        g.write(&bytes, lsn);
        Ok(())
    }

    /// Appends a new overflow page to the chain and inserts `entry` into it.
    ///
    /// `tail_guard` / `tail` are the current chain tail — this method writes the
    /// new overflow page first, then updates `tail.header.overflow` to point at
    /// it so that both writes are visible before this function returns.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if the buffer pool cannot allocate the new page or
    /// either write fails.
    fn spill_to_overflow(
        &self,
        txn: TransactionId,
        tail_guard: &PageGuard<'_>,
        tail: &mut HashBucket,
        bucket: BucketNumber,
        entry: IndexEntry,
    ) -> Result<(), IndexError> {
        let new_pn = self.allocate_page();

        let mut overflow = HashBucket::new_bucket(bucket);
        overflow.push(entry);

        let new_guard = self
            .store
            .fetch_page(LockRequest::exclusive(txn, self.pid(new_pn)))?;
        Self::write_bucket(&new_guard, &mut overflow, Lsn::INVALID)?;
        self.num_pages.fetch_max(new_pn.0 + 1, Ordering::AcqRel);

        tail.header.overflow = Some(new_pn);
        Self::write_bucket(tail_guard, tail, Lsn::INVALID)
    }

    /// Returns `Err(CorruptIndex)` when the overflow chain looks like a cycle.
    ///
    /// A legitimate chain longer than [`MAX_OVERFLOW_HOPS`] pages is treated as
    /// corruption because it would imply a file with more than a gigabyte of
    /// overflow pages for a single bucket — far outside normal operation.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::CorruptIndex`] when `hops == MAX_OVERFLOW_HOPS`.
    fn ensure_hops_limit(hops: usize) -> Result<(), IndexError> {
        if hops == MAX_OVERFLOW_HOPS {
            return Err(IndexError::CorruptIndex("overflow chain too long"));
        }
        Ok(())
    }

    /// Walks the overflow chain starting at `head`, calling `f` on each page.
    ///
    /// Stops early when `f` returns [`std::ops::ControlFlow::Break`], allowing
    /// callers to short-circuit once they find what they need.  All pages are
    /// read with a shared lock; use the dedicated insert/delete paths for
    /// exclusive access.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if a page cannot be read or the chain exceeds
    /// [`MAX_OVERFLOW_HOPS`] (which signals a corruption cycle).
    fn traverse<F>(&self, txn: TransactionId, head: PageNumber, mut f: F) -> Result<(), IndexError>
    where
        F: FnMut(&HashBucket) -> std::ops::ControlFlow<()>,
    {
        let mut pn = Some(head);
        let mut hops = 0usize;
        while let Some(current) = pn {
            Self::ensure_hops_limit(hops)?;
            let (_g, b) = self.read_bucket(txn, current, false)?;
            if let std::ops::ControlFlow::Break(()) = f(&b) {
                return Ok(());
            }
            pn = b.header.overflow;
            hops += 1;
        }
        Ok(())
    }
}

impl Index for HashIndex {
    /// Inserts `(key, rid)` into the hash index.
    ///
    /// Hashes `key` to find the head bucket page, walks the overflow chain
    /// looking for space, and either appends in-place or spills to a new
    /// overflow page.
    ///
    /// # Errors
    ///
    /// - [`IndexError::DuplicateEntry`] — the exact `(key, rid)` pair already exists.
    /// - [`IndexError::KeyTypeMismatch`] / [`IndexError::KeyArityMismatch`] — `key` does not match
    ///   the declared column types or arity.
    /// - [`IndexError`] variants from the buffer pool or codec on I/O failure.
    fn insert(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        self.check_key_type(key)?;
        let bucket = self.hash_to_bucket(key);
        let entry = IndexEntry::new(key.clone(), rid);
        let head = PageNumber::from(bucket);

        let mut pn = head;
        loop {
            let (g, mut b) = self.read_bucket(txn, pn, true)?;
            if b.contains(&entry) {
                return Err(IndexError::DuplicateEntry);
            }

            if b.has_space_for(&entry) {
                b.push(entry);
                return Self::write_bucket(&g, &mut b, Lsn::INVALID);
            }

            match b.header.overflow {
                Some(next) => {
                    drop(g);
                    pn = next;
                }
                None => return self.spill_to_overflow(txn, &g, &mut b, bucket, entry),
            }
        }
    }

    /// Removes the `(key, rid)` entry from the hash index.
    ///
    /// Walks the bucket's overflow chain until the entry is found, then removes
    /// it from the page body and writes the page back.  The page is not
    /// compacted or merged with adjacent pages; empty overflow pages remain in
    /// the chain.
    ///
    /// # Errors
    ///
    /// - [`IndexError::NotFound`] — no matching entry exists anywhere in the chain.
    /// - [`IndexError::KeyTypeMismatch`] / [`IndexError::KeyArityMismatch`] — `key` does not match
    ///   the declared column types or arity.
    /// - [`IndexError`] variants from the buffer pool or codec on I/O failure.
    fn delete(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        self.check_key_type(key)?;
        let bucket = self.hash_to_bucket(key);
        let mut pn = Some(PageNumber::from(bucket));
        let mut hops = 0;

        while let Some(current) = pn {
            Self::ensure_hops_limit(hops)?;
            let (g, mut b) = self.read_bucket(txn, current, true)?;
            if let Some(pos) = b.find(key, &rid) {
                b.remove(pos);
                return Self::write_bucket(&g, &mut b, Lsn::INVALID);
            }
            pn = b.header.overflow;
            hops += 1;
        }
        Err(IndexError::NotFound)
    }

    /// Returns every [`RecordId`] whose key equals `key`.
    ///
    /// Hashes `key` to the correct bucket and scans its full overflow chain.
    /// Returns an empty `Vec` (not an error) when no match exists.
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] / [`IndexError::KeyArityMismatch`] — `key` does not match
    ///   the declared column types or arity.
    /// - [`IndexError`] variants from the buffer pool or codec on I/O failure.
    fn search(&self, txn: TransactionId, key: &CompositeKey) -> Result<Vec<RecordId>, IndexError> {
        self.check_key_type(key)?;
        let bucket = self.hash_to_bucket(key);
        let mut results = Vec::new();
        let pn = PageNumber::from(bucket);
        self.traverse(txn, pn, |b| {
            for e in &b.body {
                if e.key == *key {
                    results.push(e.rid);
                }
            }
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(results)
    }

    /// Returns every [`RecordId`] whose key falls in the inclusive range
    /// `[start, end]`.
    ///
    /// Hash indexes have no ordering, so this scans **all** buckets and their
    /// overflow chains.  This operation is `O(total_pages)` and is only present
    /// to satisfy the [`Index`] trait contract; prefer a B-Tree index for
    /// range-heavy workloads.
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] / [`IndexError::KeyArityMismatch`] — either bound does not
    ///   match the declared column types or arity.
    /// - [`IndexError`] variants from the buffer pool or codec on I/O failure.
    fn range_search(
        &self,
        txn: TransactionId,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        self.check_key_type(start)?;
        self.check_key_type(end)?;
        let mut results = Vec::new();
        for b in 0..self.num_buckets {
            let pn = PageNumber::from(b);
            self.traverse(txn, pn, |p| {
                for e in &p.body {
                    let lo = e.key.partial_cmp(start).unwrap();
                    let hi = e.key.partial_cmp(end).unwrap();
                    if lo.is_ge() && hi.is_le() {
                        results.push(e.rid);
                    }
                }
                std::ops::ControlFlow::Continue(())
            })?;
        }
        Ok(results)
    }

    /// Returns [`IndexKind::Hash`].
    fn kind(&self) -> IndexKind {
        IndexKind::Hash
    }

    /// Returns the expected SQL types for each key column, in declaration order.
    fn key_types(&self) -> &[Type] {
        &self.key_types
    }

    /// Returns the [`FileId`] of this index's backing file.
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{bucket::HashBucket, *};
    use crate::{
        FileId, TransactionId, Value,
        buffer_pool::page_store::PageStore,
        index::encode_index_page,
        primitives::{PageNumber, RecordId, SlotId},
        storage::{PAGE_SIZE, StorageError},
        wal::writer::Wal,
    };

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    /// Single-column key shorthand for tests that predate composite indexes.
    fn k(v: Value) -> CompositeKey {
        CompositeKey::single(v)
    }

    // ── HashIndex integration tests ──────────────────────────────────────────

    /// Bundle the test fixture so individual tests can stay short.
    /// `_dir` is held to keep the temp directory alive for the whole test.
    struct Fixture {
        index: HashIndex,
        wal: Arc<Wal>,
        _dir: TempDir,
    }

    fn make_index(num_buckets: u32, key_types: Vec<Type>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(64, Arc::clone(&wal)));

        let file_id = FileId::new(1);
        let path = dir.path().join("hash.db");

        // Create the file and pre-size it for the head pages so reads don't
        // run past EOF before we initialize them.
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(num_buckets) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);

        store.register_file(file_id, &path).unwrap();

        let index = HashIndex::new(
            file_id,
            key_types,
            num_buckets,
            Arc::clone(&store),
            num_buckets,
        );

        // Production-equivalent setup: write empty bucket headers into every
        // head page so subsequent reads can decode them.
        let init_txn = TransactionId::new(0);
        wal.log_begin(init_txn).unwrap();
        index.init(init_txn).unwrap();
        store.release_all(init_txn);

        Fixture {
            index,
            wal,
            _dir: dir,
        }
    }

    fn begin(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    #[test]
    fn insert_then_search_returns_rid() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 10, 3);
        fx.index.insert(txn, &k(Value::int32(42)), r).unwrap();

        let found = fx.index.search(txn, &k(Value::int32(42))).unwrap();
        assert_eq!(found, vec![r]);
    }

    #[test]
    fn search_missing_key_returns_empty() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let found = fx.index.search(txn, &k(Value::int32(999))).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn insert_duplicate_key_rid_pair_errors() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &k(Value::int32(1)), r).unwrap();

        let err = fx.index.insert(txn, &k(Value::int32(1)), r).unwrap_err();
        assert!(matches!(err, IndexError::DuplicateEntry));
    }

    #[test]
    fn same_key_different_rids_all_returned() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let rids = [rid(1, 0, 0), rid(1, 0, 1), rid(1, 1, 0)];
        for r in &rids {
            fx.index.insert(txn, &k(Value::int32(7)), *r).unwrap();
        }

        let mut found = fx.index.search(txn, &k(Value::int32(7))).unwrap();
        found.sort_by_key(|r| (r.page_no.0, r.slot_id.0));
        assert_eq!(found, rids);
    }

    #[test]
    fn delete_existing_entry_removes_it() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &k(Value::int32(5)), r).unwrap();
        fx.index.delete(txn, &k(Value::int32(5)), r).unwrap();
        assert!(
            fx.index
                .search(txn, &k(Value::int32(5)))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn delete_missing_returns_not_found() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .delete(txn, &k(Value::int32(123)), rid(1, 0, 0))
            .unwrap_err();
        assert!(matches!(err, IndexError::NotFound));
    }

    #[test]
    fn delete_one_rid_leaves_others_intact() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r1 = rid(1, 0, 0);
        let r2 = rid(1, 0, 1);
        fx.index.insert(txn, &k(Value::int32(9)), r1).unwrap();
        fx.index.insert(txn, &k(Value::int32(9)), r2).unwrap();

        fx.index.delete(txn, &k(Value::int32(9)), r1).unwrap();
        let remaining = fx.index.search(txn, &k(Value::int32(9))).unwrap();
        assert_eq!(remaining, vec![r2]);
    }

    #[test]
    fn key_type_mismatch_errors_on_insert() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .insert(txn, &k(Value::varchar("nope".into())), rid(1, 0, 0))
            .unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    #[test]
    fn key_type_mismatch_errors_on_search() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .search(txn, &k(Value::varchar("nope".into())))
            .unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    #[test]
    fn kind_and_key_type_accessors() {
        let fx = make_index(4, vec![Type::Int64]);
        assert_eq!(fx.index.kind(), IndexKind::Hash);
        assert_eq!(fx.index.key_types(), &[Type::Int64]);
    }

    #[test]
    fn init_writes_decodable_headers_into_every_head_page() {
        // make_index already calls init(); verify each head page now decodes
        // as a HashBucket with the right BucketNumber and an empty entry list.
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        for i in 0..4 {
            let (_g, b) = fx
                .index
                .read_bucket(txn, PageNumber::new(i), false)
                .unwrap();
            assert_eq!(b.header.bucket, BucketNumber(i));
            assert!(b.body.is_empty());
            assert!(b.header.overflow.is_none());
        }
    }

    #[test]
    fn read_bucket_rejects_wrong_kind_page() {
        // Stamp a head page with a non-HashBucket kind and verify read_bucket
        // returns CorruptIndex rather than silently decoding the body.
        // encode_index_page produces a valid CRC — TypedPage::from_page_bytes
        // passes the checksum, then the explicit kind check fires.
        let fx = make_index(2, vec![Type::Int32]);
        let init_txn = TransactionId::new(99);
        fx.wal.log_begin(init_txn).unwrap();
        let pid = fx.index.pid(PageNumber::new(0));
        let g = fx
            .index
            .store
            .fetch_page(LockRequest::exclusive(init_txn, pid))
            .unwrap();
        let bogus = encode_index_page(PageKind::BTreeLeaf, |_| Ok(())).unwrap();
        g.write(&bogus, Lsn::INVALID);
        drop(g);
        fx.index.store.release_all(init_txn);

        let txn = begin(&fx.wal, 1);
        match fx.index.read_bucket(txn, PageNumber::new(0), false) {
            Err(IndexError::CorruptIndex(_)) => {}
            other => panic!("expected CorruptIndex, got {:?}", other.err()),
        }
    }

    #[test]
    fn read_bucket_rejects_crc_corrupted_page() {
        // Verify the CRC error from TypedPage::from_page_bytes propagates as
        // IndexError::Storage. Without this, silently swallowing CRC failures
        // would go undetected.
        let fx = make_index(2, vec![Type::Int32]);
        let init_txn = TransactionId::new(99);
        fx.wal.log_begin(init_txn).unwrap();

        let pid = fx.index.pid(PageNumber::new(0));
        let g = fx
            .index
            .store
            .fetch_page(LockRequest::exclusive(init_txn, pid))
            .unwrap();
        let mut bytes = HashBucket::new_bucket(BucketNumber(0))
            .to_page_bytes()
            .unwrap();
        // Flip a payload bit so the CRC no longer matches.
        bytes[PAGE_SIZE - 1] ^= 0x01;
        g.write(&bytes, Lsn::INVALID);
        drop(g);
        fx.index.store.release_all(init_txn);

        let txn = begin(&fx.wal, 1);
        match fx.index.read_bucket(txn, PageNumber::new(0), false) {
            Err(IndexError::Storage(StorageError::ChecksumMismatch { .. })) => {}
            other => panic!("expected ChecksumMismatch, got {:?}", other.err()),
        }
    }

    #[test]
    fn many_inserts_into_one_bucket_force_overflow_chain() {
        // num_buckets = 1 funnels every insert into bucket 0.
        let fx = make_index(1, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let n = 300;
        for i in 0..n {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &k(Value::int32(i)), rid(1, 0, slot))
                .unwrap();
        }

        assert!(
            fx.index.num_pages.load(Ordering::Acquire) > 1,
            "expected at least one overflow page to be allocated"
        );

        // Spot-check that entries from both ends of the chain are findable.
        for i in [0, 100, 250, n - 1] {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            let found = fx.index.search(txn, &k(Value::int32(i))).unwrap();
            assert_eq!(found, vec![rid(1, 0, slot)], "missing key {i}");
        }
    }

    #[test]
    fn delete_walks_overflow_chain() {
        let fx = make_index(1, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        for i in 0..300 {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &k(Value::int32(i)), rid(1, 0, slot))
                .unwrap();
        }
        // A late key will live on an overflow page.
        fx.index
            .delete(txn, &k(Value::int32(290)), rid(1, 0, 290))
            .unwrap();
        assert!(
            fx.index
                .search(txn, &k(Value::int32(290)))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn range_search_returns_keys_in_inclusive_range() {
        let fx = make_index(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        for i in 0..20 {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &k(Value::int32(i)), rid(1, 0, slot))
                .unwrap();
        }
        let found = fx
            .index
            .range_search(txn, &k(Value::int32(5)), &k(Value::int32(10)))
            .unwrap();
        assert_eq!(found.len(), 6, "expected keys 5..=10 inclusive");
    }

    fn name_key(last: &str, first: &str) -> CompositeKey {
        CompositeKey::new(vec![
            Value::varchar(last.into()),
            Value::varchar(first.into()),
        ])
    }

    #[test]
    fn composite_key_two_columns_roundtrip() {
        let fx = make_index(4, vec![Type::String, Type::String]);
        let txn = begin(&fx.wal, 1);

        let smith_ada = name_key("Smith", "Ada");
        let smith_bob = name_key("Smith", "Bob");
        let r1 = rid(1, 0, 0);
        let r2 = rid(1, 0, 1);
        fx.index.insert(txn, &smith_ada, r1).unwrap();
        fx.index.insert(txn, &smith_bob, r2).unwrap();

        // Each composite key resolves only to its own rid — same first
        // component must not be enough to find the other.
        assert_eq!(fx.index.search(txn, &smith_ada).unwrap(), vec![r1]);
        assert_eq!(fx.index.search(txn, &smith_bob).unwrap(), vec![r2]);
    }

    #[test]
    fn composite_key_arity_mismatch_errors() {
        let fx = make_index(4, vec![Type::String, Type::String]);
        let txn = begin(&fx.wal, 1);

        let single = CompositeKey::single(Value::varchar("Smith".into()));
        let err = fx.index.search(txn, &single).unwrap_err();
        assert!(matches!(err, IndexError::KeyArityMismatch {
            expected: 2,
            got: 1
        }));
    }

    #[test]
    fn composite_key_per_position_type_mismatch_errors() {
        let fx = make_index(4, vec![Type::String, Type::Int32]);
        let txn = begin(&fx.wal, 1);

        // Right arity (2), wrong type at position 1: String where Int32 expected.
        let bad = CompositeKey::new(vec![
            Value::varchar("Smith".into()),
            Value::varchar("not-an-int".into()),
        ]);
        let err = fx.index.insert(txn, &bad, rid(1, 0, 0)).unwrap_err();
        match err {
            IndexError::KeyTypeMismatch {
                position,
                expected: Type::Int32,
                ..
            } => assert_eq!(position, 1),
            other => panic!("expected KeyTypeMismatch at position 1, got {other:?}"),
        }
    }

    #[test]
    fn composite_key_column_order_matters() {
        let ab = CompositeKey::new(vec![Value::varchar("a".into()), Value::varchar("b".into())]);
        let ba = CompositeKey::new(vec![Value::varchar("b".into()), Value::varchar("a".into())]);
        assert_ne!(ab, ba);

        // And end-to-end: insert (a,b), search (b,a), get nothing.
        let fx = make_index(4, vec![Type::String, Type::String]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &ab, rid(1, 0, 0)).unwrap();
        assert!(fx.index.search(txn, &ba).unwrap().is_empty());
    }
}
