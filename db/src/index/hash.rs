//! Hash index access method — storage backing for `CREATE INDEX … USING HASH`
//! and the planner's chosen path for equality predicates against the indexed
//! column.
//!
//! # Shape
//!
//! - [`HashIndex`] — the access method. Implements [`Index`], so the planner sees it as just
//!   another secondary index.
//! - [`HashBucket`] — one page's worth of entries. Pages are linked into a per-bucket chain via the
//!   `overflow` pointer.
//! - [`BucketNumber`] — `0..num_buckets`; bucket N lives at page N.
//! - [`IndexEntry`] (from [`crate::index`]) — the `(key, RecordId)` pair stored in each bucket.
//!
//! # SQL → access-method mapping
//!
//! Throughout this file the running schema is:
//!
//! ```sql
//! CREATE TABLE users (id INT, email TEXT, age INT);
//! CREATE INDEX users_email_idx ON users USING HASH (email);
//! ```
//!
//! Reads, writes, and deletes hitting `email` flow through this module:
//!
//! ```sql
//! -- accelerated by HashIndex::search
//! SELECT * FROM users WHERE email = 'a@b.com';
//!
//! -- HashIndex::insert called once per row inserted
//! INSERT INTO users VALUES (1, 'a@b.com', 30);
//!
//! -- after locating the matching tuple, executor calls HashIndex::delete
//! DELETE FROM users WHERE email = 'a@b.com';
//!
//! -- range queries fall back to a full bucket scan; planner should not
//! -- pick this index for them.
//! SELECT * FROM users WHERE email BETWEEN 'a' AND 'm';
//! ```
//!
//! # How it works
//!
//! Static hashing with separate chaining. Each key hashes to a fixed bucket
//! `0..num_buckets`; the bucket lives at page `bucket_no`. When a bucket
//! page fills, a fresh page is allocated at the end of the file and linked
//! via the head's `overflow` pointer, forming a chain. Lookups walk the
//! chain; inserts append to the first page with room, allocating a new
//! overflow page if every existing one is full.
//!
//! # On-disk layout (one bucket page, `PAGE_SIZE` bytes)
//!
//!   envelope: kind(1) = `PageKind::HashBucket` | crc32(4)
//!   body:     `bucket_num`(4) | n(4) | overflow(4) | n × `IndexEntry`
//!   trailing zero padding to `PAGE_SIZE`
//!
//! `overflow` is `NIL` when this is the chain tail. The envelope is shared
//! with the rest of the index family; see [`crate::storage`].
//!
//! # NULL semantics
//!
//! `Value::Null` is hashed like any other variant — it lands in some
//! deterministic bucket and matches itself on equality. SQL semantics
//! (`NULL = NULL` is unknown, not true) are enforced by the executor *before*
//! it consults the index, so this layer never sees a `NULL`-vs-`NULL`
//! lookup in well-formed plans.
//!
//! # Concurrency
//!
//! Inserts hold the chain tail's exclusive lock for the duration of any
//! spill, so two concurrent inserts on the same bucket serialize cleanly.
//! Searches release each page's pin before walking to the next; the lock
//! manager keeps the lock until commit.
//!
//! `HashIndex` and `HashBucket` live in this single file because the hash
//! implementation is small. Promote to a directory if/when it outgrows that.

use std::{
    hash::{Hash, Hasher},
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use siphasher::sip::SipHasher13;

use crate::{
    FileId, Lsn, PageNumber, TransactionId, Type, Value,
    buffer_pool::{
        LockRequest,
        page_store::{PageGuard, PageStore},
    },
    codec::{CodecError, Decode, Encode},
    index::{Index, IndexEntry, IndexError, IndexKind, check_key_type},
    primitives::{PageId, RecordId},
    storage::{ENVELOPE_HEADER_SIZE, PAGE_SIZE, PageKind, decode_index_page, encode_index_page},
};

const NIL: u32 = u32::MAX;

/// A bucket index in `0..num_buckets`.
///
/// Static hashing means bucket N lives at page N, so this also doubles as a
/// [`PageNumber`] via the `From<BucketNumber> for PageNumber` impl.
///
/// Not user-visible at the SQL layer — the bucket count is a property of
/// the index file format, decided when `CREATE INDEX … USING HASH` runs and
/// frozen thereafter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct BucketNumber(u32);

impl From<BucketNumber> for u32 {
    fn from(bucket: BucketNumber) -> Self {
        bucket.0
    }
}

impl From<u32> for BucketNumber {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<BucketNumber> for PageNumber {
    fn from(bucket: BucketNumber) -> Self {
        PageNumber::from(u32::from(bucket))
    }
}

/// One page's worth of `(key, RecordId)` entries plus a link to the next
/// page in the bucket's chain.
///
/// Internal layout type — never appears in a plan or in user-facing SQL.
/// Constructed by [`HashBucket::new`] for fresh overflow pages and by
/// [`Decode::decode`] when [`HashIndex::read_bucket`] reads a page off disk.
#[derive(Debug, Clone)]
pub struct HashBucket {
    bucket: BucketNumber,
    overflow: Option<PageNumber>,
    entries: Vec<IndexEntry>,
}

impl HashBucket {
    /// Bytes consumed before the first [`IndexEntry`] on a page: envelope
    /// (`kind` + `crc`) plus the bucket-specific `bucket_num + n + overflow`.
    ///
    /// Used by [`Self::has_space_for`] to compute remaining room on a page —
    /// effectively the answer to "how many entries fit in one bucket page
    /// before we need to spill into a new overflow page?"
    pub const HEADER_SIZE: usize = ENVELOPE_HEADER_SIZE + 4 + 4 + 4;

    /// Builds an empty in-memory bucket for the given bucket number.
    ///
    /// Two callers in this file:
    /// - [`HashIndex::init`] when stamping head pages on `CREATE INDEX`.
    /// - [`HashIndex::spill_to_overflow`] when the chain tail fills and a fresh overflow page is
    ///   allocated.
    pub fn new(bucket: BucketNumber) -> Self {
        Self {
            bucket,
            overflow: None,
            entries: Vec::new(),
        }
    }

    /// Bytes this bucket would occupy on disk if encoded right now
    /// (header + every `IndexEntry`).
    ///
    /// Drives [`Self::has_space_for`]; not exposed to the planner.
    pub fn used_bytes(&self) -> usize {
        Self::HEADER_SIZE
            + self
                .entries
                .iter()
                .map(IndexEntry::encoded_size)
                .sum::<usize>()
    }

    /// Bytes still available in this bucket page before it spills to overflow.
    #[inline]
    fn free_bytes(&self) -> usize {
        PAGE_SIZE.saturating_sub(self.used_bytes())
    }

    /// Whether `entry` will fit in this bucket page without spilling.
    ///
    /// Returns `true` when the bucket can absorb the next insert in-place,
    /// `false` when [`HashIndex::spill_to_overflow`] must allocate a fresh
    /// page and link it from the current tail.
    ///
    /// The size estimate is exact because [`IndexEntry::encoded_size`]
    /// mirrors [`IndexEntry::encode`] branch-for-branch.
    pub fn has_space_for(&self, entry: &IndexEntry) -> bool {
        entry.encoded_size() <= self.free_bytes()
    }

    fn contains(&self, entry: &IndexEntry) -> bool {
        self.entries.iter().any(|e| e == entry)
    }

    fn push(&mut self, entry: IndexEntry) {
        self.entries.push(entry);
    }

    fn find(&self, key: &Value, rid: &RecordId) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.key == *key && e.rid == *rid)
    }

    fn remove(&mut self, pos: usize) {
        self.entries.remove(pos);
    }
}

/// Body-only codec: the envelope (`kind`, `crc`) is handled by
/// [`encode_index_page`] / [`decode_index_page`] in `storage`. This impl
/// writes only the bucket-specific fields and the entry list.
impl Encode for HashBucket {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        w.write_u32::<LittleEndian>(u32::from(self.bucket))?;
        let entry_count =
            u32::try_from(self.entries.len()).map_err(|_| CodecError::NumericDoesNotFit {
                value: u64::try_from(self.entries.len()).unwrap_or(u64::MAX),
                target: "u32",
            })?;
        w.write_u32::<LittleEndian>(entry_count)?;

        let overflow = self.overflow.map_or(NIL, |p| p.0);
        w.write_u32::<LittleEndian>(overflow)?;
        for e in &self.entries {
            e.encode(w)?;
        }
        Ok(())
    }
}

/// Body-only codec: assumes the envelope header has already been
/// stripped by [`decode_index_page`] before the reader is handed off here.
impl Decode for HashBucket {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let bucket = BucketNumber(r.read_u32::<LittleEndian>()?);
        let n = r.read_u32::<LittleEndian>()?;
        let raw_overflow = r.read_u32::<LittleEndian>()?;
        let overflow = (raw_overflow != NIL).then_some(PageNumber(raw_overflow));

        let mut entries = Vec::with_capacity(n as usize);
        for _ in 0..n {
            entries.push(IndexEntry::decode(r)?);
        }
        Ok(Self {
            bucket,
            overflow,
            entries,
        })
    }
}

// HashIndex: hash index spanning multiple bucket pages.
//
// Knows its FileId and directory structure. All page access via PageStore.
//
// Operations:
//   - search(txn, key) -> Vec<(PageId, SlotId)>
//   - insert(txn, key, rid)
//   - delete(txn, key, rid)

const MAX_OVERFLOW_HOPS: usize = 1000; // cycle-detection bound, same as Go

/// The hash-index access method. One instance per `CREATE INDEX … USING HASH`
/// declaration in the catalog.
///
/// Implements [`Index`], so the executor doesn't need to know it's holding a
/// hash (vs. B-tree) when it dispatches inserts and lookups — see the trait
/// method docs below for the SQL fragments each one accelerates.
///
/// # SQL → operator mapping
///
/// ```sql
/// CREATE INDEX users_email_idx ON users USING HASH (email);
/// -- catalog opens the file, computes existing_pages, and constructs:
/// --   HashIndex::new(file_id, Type::String, /*num_buckets=*/ 64, store, existing_pages)
/// -- then on first creation only:
/// --   idx.init(txn)?;
/// ```
pub struct HashIndex {
    file_id: FileId,
    key_type: Type,
    num_buckets: u32,
    /// Maps bucket -> page number of the bucket's head page.
    /// In the Go version this is mutable; here we keep the static-hash
    /// convention that bucket i lives at page i. If you later add extendible
    /// hashing, replace this with a proper directory page.
    store: Arc<PageStore>,
    /// Total pages this index occupies in `file_id` — head pages
    /// (`0..num_buckets`) plus any overflow pages allocated since open.
    /// Allocation is `fetch_add`; reconciled with `fetch_max` after a write
    /// to keep the counter in sync regardless of allocation path.
    num_pages: AtomicU32,
}

impl HashIndex {
    /// Opens an existing hash-index file (or starts a fresh one when paired
    /// with [`Self::init`]).
    ///
    /// `num_buckets` is the static bucket count baked into the file format —
    /// changing it requires a rebuild. `existing_pages` is the page count
    /// read from disk metadata; pass `num_buckets` for a brand-new file
    /// (head pages only, no overflow yet).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // CREATE INDEX users_email_idx ON users USING HASH (email);
    /// let idx = HashIndex::new(file_id, Type::String, 64, store, 64);
    /// idx.init(txn)?;  // first time only
    ///
    /// // Re-opening an existing index where the file holds 80 pages
    /// // (64 head + 16 overflow allocated previously):
    /// let idx = HashIndex::new(file_id, Type::String, 64, store, 80);
    /// ```
    ///
    /// # Panics
    ///
    /// - `num_buckets == 0` — must declare at least one bucket.
    /// - `existing_pages < num_buckets` — head pages must always be present.
    pub fn new(
        file_id: FileId,
        key_type: Type,
        num_buckets: u32,
        store: Arc<PageStore>,
        existing_pages: u32,
    ) -> Self {
        assert!(num_buckets > 0);
        assert!(
            existing_pages >= num_buckets,
            "existing_pages must cover at least the {num_buckets} head pages"
        );
        Self {
            file_id,
            key_type,
            num_buckets,
            store,
            num_pages: AtomicU32::new(existing_pages),
        }
    }

    /// Stamps every head page (`0..num_buckets`) with an empty bucket header.
    ///
    /// Call this once when the index file is first created, before any
    /// insert or lookup. Without it, the buffer pool returns a zero-filled
    /// page whose envelope CRC is invalid, and [`Self::read_bucket`] would
    /// fail on first read.
    ///
    /// # SQL mapping
    ///
    /// ```sql
    /// CREATE INDEX users_email_idx ON users USING HASH (email);
    /// -- runs HashIndex::new + HashIndex::init in the same transaction
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IndexError::PageStore`] — fetching or locking a head page failed.
    /// - [`IndexError::Codec`] — encoding the empty bucket failed (effectively unreachable for an
    ///   empty bucket, but propagated for symmetry).
    ///
    /// # Warning
    ///
    /// Calling this on an already-populated file will clobber every head
    /// page. Catalog code must invoke it on creation only, never on re-open.
    pub fn init(&self, txn: TransactionId) -> Result<(), IndexError> {
        for i in 0..self.num_buckets {
            let pid = self.pid(PageNumber::new(i));
            let g = self.store.fetch_page(LockRequest::exclusive(txn, pid))?;
            let bucket = HashBucket::new(BucketNumber(i));
            Self::write_bucket(&g, &bucket, Lsn::INVALID)?;
        }
        Ok(())
    }

    /// Claims a fresh page number at the end of the file.
    ///
    /// `fetch_add` guarantees concurrent allocators get distinct numbers.
    /// The page exists logically the moment this returns; it becomes
    /// physical on first flush via `PageStore::flush_frame_to_file`.
    fn allocate_page(&self) -> PageNumber {
        PageNumber::from(self.num_pages.fetch_add(1, Ordering::AcqRel))
    }

    /// Maps `key` to a bucket using SipHash-2-4 with a fixed key.
    ///
    /// `SipHash` from the `siphasher` crate is stable across Rust versions,
    /// unlike `std::collections::hash_map::DefaultHasher` whose algorithm is
    /// implementation-defined. Stability matters here because the bucket
    /// number is baked into the on-disk layout: a key that hashed to bucket
    /// 7 yesterday must still hash to bucket 7 after a stdlib upgrade or
    /// the index returns nothing.
    ///
    /// The keys `(0, 0)` are arbitrary but fixed for the same reason. If
    /// you ever need to rotate the seed (e.g. to defend against adversarial
    /// key inputs), that's a file-format change requiring a rebuild.
    fn hash_to_bucket(&self, key: &Value) -> BucketNumber {
        let mut h = SipHasher13::new_with_keys(0, 0);
        key.hash(&mut h);
        let bucket_u64 = h.finish() % u64::from(self.num_buckets);
        let Ok(bucket) = u32::try_from(bucket_u64) else {
            unreachable!("bucket index is bounded by num_buckets (u32)");
        };
        BucketNumber(bucket)
    }

    fn pid(&self, p: PageNumber) -> PageId {
        PageId {
            file_id: self.file_id,
            page_no: p,
        }
    }

    fn read_bucket(
        &self,
        txn: TransactionId,
        pn: PageNumber,
        exclusive: bool,
    ) -> Result<(PageGuard<'_>, HashBucket), IndexError> {
        let pid = self.pid(pn);
        let req = if exclusive {
            LockRequest::exclusive(txn, pid)
        } else {
            LockRequest::shared(txn, pid)
        };
        let g = self.store.fetch_page(req)?;
        let page = g.read();
        let (kind, payload) = decode_index_page(&page)?;
        if kind != PageKind::HashBucket {
            return Err(IndexError::CorruptIndex(
                "expected HashBucket page, found another kind",
            ));
        }
        let mut reader: &[u8] = payload;
        let bucket = HashBucket::decode(&mut reader)?;
        Ok((g, bucket))
    }

    /// Wraps the bucket body in the index-page envelope and hands the full
    /// `PAGE_SIZE` buffer to the buffer pool. Trailing payload bytes stay
    /// zero per the envelope contract.
    fn write_bucket(g: &PageGuard<'_>, b: &HashBucket, lsn: Lsn) -> Result<(), IndexError> {
        let buf = encode_index_page(PageKind::HashBucket, |body| {
            let mut cursor = std::io::Cursor::new(body);
            b.encode(&mut cursor)
        })?;
        g.write(&buf, lsn);
        Ok(())
    }

    /// Allocates a fresh overflow page, writes `entry` into it, and links it
    /// from `tail`.
    ///
    /// Caller must hold `tail_guard` exclusively on the current chain tail.
    /// On return the new page is durably written, the counter is reconciled,
    /// and the tail's `overflow` field has been patched and re-flushed.
    ///
    /// Write order is deliberate: new page first, then the link that
    /// publishes it. A crash between the two leaks an unreferenced page
    /// (the entry is gone, but no chain points at garbage).
    fn spill_to_overflow(
        &self,
        txn: TransactionId,
        tail_guard: &PageGuard<'_>,
        tail: &mut HashBucket,
        bucket: BucketNumber,
        entry: IndexEntry,
    ) -> Result<(), IndexError> {
        let new_pn = self.allocate_page();

        let mut overflow = HashBucket::new(bucket);
        overflow.push(entry);

        let new_guard = self
            .store
            .fetch_page(LockRequest::exclusive(txn, self.pid(new_pn)))?;
        Self::write_bucket(&new_guard, &overflow, Lsn::INVALID)?;
        self.num_pages.fetch_max(new_pn.0 + 1, Ordering::AcqRel);

        tail.overflow = Some(new_pn);
        Self::write_bucket(tail_guard, tail, Lsn::INVALID)
    }

    fn ensure_hops_limit(hops: usize) -> Result<(), IndexError> {
        if hops == MAX_OVERFLOW_HOPS {
            return Err(IndexError::CorruptIndex("overflow chain too long"));
        }
        Ok(())
    }

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
            pn = b.overflow;
            hops += 1;
        }
        Ok(())
    }
}

impl Index for HashIndex {
    /// Adds a `(key, rid)` pair to the index.
    ///
    /// Called by the executor after a heap insert succeeds, once per
    /// secondary index registered on the table.
    ///
    /// # SQL mapping
    ///
    /// ```sql
    /// -- For each row inserted, the executor calls this once per index:
    /// INSERT INTO users VALUES (1, 'a@b.com', 30);
    /// -- → idx.insert(txn, &Value::String("a@b.com".into()), rid_of_new_row)
    /// ```
    ///
    /// Multiple rids per key are allowed (the index is non-unique by
    /// default); only the exact `(key, rid)` pair is treated as a duplicate.
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] — `key` doesn't match the declared index key type (e.g.
    ///   inserting an `INT` into an index on `TEXT`).
    /// - [`IndexError::DuplicateEntry`] — the exact `(key, rid)` pair is already present in this
    ///   bucket's chain.
    /// - [`IndexError::PageStore`] — fetching or locking a chain page failed.
    /// - [`IndexError::Codec`] — encoding the updated page failed.
    /// - [`IndexError::CorruptIndex`] — overflow chain exceeded its hop bound.
    fn insert(&self, txn: TransactionId, key: &Value, rid: RecordId) -> Result<(), IndexError> {
        check_key_type(self.key_type, key)?;
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
                return Self::write_bucket(&g, &b, Lsn::INVALID);
            }

            match b.overflow {
                Some(next) => {
                    drop(g);
                    pn = next;
                }
                None => return self.spill_to_overflow(txn, &g, &mut b, bucket, entry),
            }
        }
    }

    /// Removes the specific `(key, rid)` entry, if present.
    ///
    /// The executor first locates the heap row matching the predicate, then
    /// calls this with the resolved `rid` for each index registered on the
    /// table.
    ///
    /// # SQL mapping
    ///
    /// ```sql
    /// DELETE FROM users WHERE email = 'a@b.com';
    /// -- After locating the matching row at rid R:
    /// --   idx.delete(txn, &Value::String("a@b.com".into()), R)
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] — `key` type doesn't match the index.
    /// - [`IndexError::NotFound`] — no entry matches the `(key, rid)` pair.
    /// - [`IndexError::PageStore`] / [`IndexError::Codec`] — page I/O failure.
    /// - [`IndexError::CorruptIndex`] — overflow chain too long or wrong page kind.
    fn delete(&self, txn: TransactionId, key: &Value, rid: RecordId) -> Result<(), IndexError> {
        check_key_type(self.key_type, key)?;
        let bucket = self.hash_to_bucket(key);
        let mut pn = Some(PageNumber::from(bucket));
        let mut hops = 0;

        while let Some(current) = pn {
            Self::ensure_hops_limit(hops)?;
            let (g, mut b) = self.read_bucket(txn, current, true)?;
            if let Some(pos) = b.find(key, &rid) {
                b.remove(pos);
                return Self::write_bucket(&g, &b, Lsn::INVALID);
            }
            pn = b.overflow;
            hops += 1;
        }
        Err(IndexError::NotFound)
    }

    /// Returns every `RecordId` whose key equals `key`.
    ///
    /// # SQL mapping
    ///
    /// ```sql
    /// -- Equality predicate against the indexed column:
    /// SELECT * FROM users WHERE email = 'a@b.com';
    /// -- → idx.search(txn, &Value::String("a@b.com".into()))
    /// --   yields the rids; the executor then fetches each tuple from the heap.
    /// ```
    ///
    /// An empty `Vec` means "no matching rows" — that's not an error.
    /// Multiple results are possible because the index is non-unique by default.
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] — `key` doesn't match the declared key type.
    /// - [`IndexError::PageStore`] / [`IndexError::Codec`] — page I/O failure.
    /// - [`IndexError::CorruptIndex`] — overflow chain exceeded its hop bound or refers to a page
    ///   of the wrong kind.
    fn search(&self, txn: TransactionId, key: &Value) -> Result<Vec<RecordId>, IndexError> {
        check_key_type(self.key_type, key)?;
        let bucket = self.hash_to_bucket(key);
        let mut results = Vec::new();
        let pn = PageNumber::from(bucket);
        self.traverse(txn, pn, |b| {
            for e in &b.entries {
                if e.key == *key {
                    results.push(e.rid);
                }
            }
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(results)
    }

    /// Returns every `RecordId` whose key falls in `[start, end]` (inclusive).
    ///
    /// **Hash indexes don't accelerate range scans** — this implementation
    /// walks every bucket and every overflow page, comparing each key
    /// individually. The planner should prefer a B-tree index for `BETWEEN`,
    /// `<`, `>` predicates; this method exists only because the [`Index`]
    /// trait requires it.
    ///
    /// # SQL mapping
    ///
    /// ```sql
    /// -- Falls through to here only if no better index is available:
    /// SELECT * FROM users WHERE email BETWEEN 'a' AND 'm';
    /// -- → idx.range_search(
    /// --       txn,
    /// --       &Value::String("a".into()),
    /// --       &Value::String("m".into()),
    /// --   )
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IndexError::KeyTypeMismatch`] for either bound.
    /// - [`IndexError::PageStore`] / [`IndexError::Codec`] — page I/O failure.
    /// - [`IndexError::CorruptIndex`] for cycles in any chain.
    fn range_search(
        &self,
        txn: TransactionId,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<RecordId>, IndexError> {
        check_key_type(self.key_type, start)?;
        check_key_type(self.key_type, end)?;
        let mut results = Vec::new();
        for b in 0..self.num_buckets {
            let pn = PageNumber::from(b);
            self.traverse(txn, pn, |p| {
                for e in &p.entries {
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

    /// Always [`IndexKind::Hash`]. Used by the planner to decide which
    /// predicate shapes are supportable: equality goes here, ranges should
    /// fall back to a different access path.
    fn kind(&self) -> IndexKind {
        IndexKind::Hash
    }

    /// The declared SQL type of the indexed column — the `TEXT` in
    /// `CREATE INDEX … ON users (email)` where `email TEXT`. Used by
    /// [`check_key_type`] on every public entry point to reject
    /// type-mismatched keys before they touch a page.
    fn key_type(&self) -> Type {
        self.key_type
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        primitives::{PageNumber, RecordId, SlotId},
        storage::StorageError,
        wal::writer::Wal,
    };

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    #[test]
    fn header_size_matches_layout() {
        // envelope (5) + bucket_num(4) + n(4) + overflow(4) = 17
        assert_eq!(HashBucket::HEADER_SIZE, ENVELOPE_HEADER_SIZE + 12);
        assert_eq!(HashBucket::HEADER_SIZE, 17);
    }

    #[test]
    fn empty_bucket_uses_only_header() {
        let b = HashBucket::new(BucketNumber(0));
        assert_eq!(b.used_bytes(), HashBucket::HEADER_SIZE);
        assert_eq!(b.free_bytes(), PAGE_SIZE - HashBucket::HEADER_SIZE);
    }

    #[test]
    fn used_bytes_grows_with_each_entry() {
        let mut b = HashBucket::new(BucketNumber(0));
        let entry = IndexEntry::new(Value::Int32(7), rid(1, 0, 0));
        let before = b.used_bytes();
        b.push(entry.clone());
        assert_eq!(b.used_bytes(), before + entry.encoded_size());
    }

    #[test]
    fn has_space_for_returns_false_when_full() {
        let mut b = HashBucket::new(BucketNumber(0));
        let entry = IndexEntry::new(Value::Int32(0), rid(1, 0, 0));
        let per_entry = entry.encoded_size();

        // Pack the bucket until adding one more entry would overflow.
        while b.free_bytes() >= per_entry {
            b.push(entry.clone());
        }
        assert!(
            !b.has_space_for(&entry),
            "after filling, next entry should not fit"
        );
    }

    #[test]
    fn body_codec_roundtrip_empty() {
        let b = HashBucket::new(BucketNumber(42));
        let mut buf = Vec::new();
        b.encode(&mut buf).unwrap();

        let decoded = HashBucket::from_bytes(&buf).unwrap();
        assert_eq!(decoded.bucket, BucketNumber(42));
        assert_eq!(decoded.overflow, None);
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn body_codec_roundtrip_with_entries_and_overflow() {
        let mut b = HashBucket::new(BucketNumber(3));
        b.overflow = Some(PageNumber::new(99));
        b.push(IndexEntry::new(Value::Int32(1), rid(1, 0, 0)));
        b.push(IndexEntry::new(Value::String("hello".into()), rid(1, 2, 5)));

        let bytes = b.to_bytes().unwrap();
        let decoded = HashBucket::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.bucket, BucketNumber(3));
        assert_eq!(decoded.overflow, Some(PageNumber::new(99)));
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].key, Value::Int32(1));
        assert_eq!(decoded.entries[1].key, Value::String("hello".into()));
    }

    // The body codec no longer reads a kind byte — that's the envelope's job
    // (see `storage::envelope_tests`). The hash-specific check is "if the
    // envelope decodes a page of the wrong kind, read_bucket rejects it" —
    // covered by `read_bucket_rejects_wrong_kind_page` below.

    // ── HashIndex integration tests ──────────────────────────────────────────

    /// Bundle the test fixture so individual tests can stay short.
    /// `_dir` is held to keep the temp directory alive for the whole test.
    struct Fixture {
        index: HashIndex,
        wal: Arc<Wal>,
        _dir: TempDir,
    }

    fn make_index(num_buckets: u32, key_type: Type) -> Fixture {
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
            key_type,
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
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 10, 3);
        fx.index.insert(txn, &Value::Int32(42), r).unwrap();

        let found = fx.index.search(txn, &Value::Int32(42)).unwrap();
        assert_eq!(found, vec![r]);
    }

    #[test]
    fn search_missing_key_returns_empty() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let found = fx.index.search(txn, &Value::Int32(999)).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn insert_duplicate_key_rid_pair_errors() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &Value::Int32(1), r).unwrap();

        let err = fx.index.insert(txn, &Value::Int32(1), r).unwrap_err();
        assert!(matches!(err, IndexError::DuplicateEntry));
    }

    #[test]
    fn same_key_different_rids_all_returned() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let rids = [rid(1, 0, 0), rid(1, 0, 1), rid(1, 1, 0)];
        for r in &rids {
            fx.index.insert(txn, &Value::Int32(7), *r).unwrap();
        }

        let mut found = fx.index.search(txn, &Value::Int32(7)).unwrap();
        found.sort_by_key(|r| (r.page_no.0, r.slot_id.0));
        assert_eq!(found, rids);
    }

    #[test]
    fn delete_existing_entry_removes_it() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &Value::Int32(5), r).unwrap();
        fx.index.delete(txn, &Value::Int32(5), r).unwrap();
        assert!(fx.index.search(txn, &Value::Int32(5)).unwrap().is_empty());
    }

    #[test]
    fn delete_missing_returns_not_found() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .delete(txn, &Value::Int32(123), rid(1, 0, 0))
            .unwrap_err();
        assert!(matches!(err, IndexError::NotFound));
    }

    #[test]
    fn delete_one_rid_leaves_others_intact() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let r1 = rid(1, 0, 0);
        let r2 = rid(1, 0, 1);
        fx.index.insert(txn, &Value::Int32(9), r1).unwrap();
        fx.index.insert(txn, &Value::Int32(9), r2).unwrap();

        fx.index.delete(txn, &Value::Int32(9), r1).unwrap();
        let remaining = fx.index.search(txn, &Value::Int32(9)).unwrap();
        assert_eq!(remaining, vec![r2]);
    }

    #[test]
    fn key_type_mismatch_errors_on_insert() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .insert(txn, &Value::String("nope".into()), rid(1, 0, 0))
            .unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    #[test]
    fn key_type_mismatch_errors_on_search() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let err = fx
            .index
            .search(txn, &Value::String("nope".into()))
            .unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    #[test]
    fn kind_and_key_type_accessors() {
        let fx = make_index(4, Type::Int64);
        assert_eq!(fx.index.kind(), IndexKind::Hash);
        assert_eq!(fx.index.key_type(), Type::Int64);
    }

    #[test]
    fn init_writes_decodable_headers_into_every_head_page() {
        // make_index already calls init(); verify each head page now decodes
        // as a HashBucket with the right BucketNumber and an empty entry list.
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        for i in 0..4 {
            let (_g, b) = fx
                .index
                .read_bucket(txn, PageNumber::new(i), false)
                .unwrap();
            assert_eq!(b.bucket, BucketNumber(i));
            assert!(b.entries.is_empty());
            assert!(b.overflow.is_none());
        }
    }

    #[test]
    fn read_bucket_rejects_wrong_kind_page() {
        // Stamp a head page with a non-HashBucket envelope (e.g. BTreeLeaf)
        // and verify read_bucket returns CorruptIndex rather than silently
        // decoding the body.
        let fx = make_index(2, Type::Int32);
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
        // PageGuard doesn't implement Debug, so unwrap_err() won't compile —
        // pattern-match on the result instead.
        match fx.index.read_bucket(txn, PageNumber::new(0), false) {
            Err(IndexError::CorruptIndex(_)) => {}
            other => panic!("expected CorruptIndex, got {:?}", other.err()),
        }
    }

    #[test]
    fn read_bucket_rejects_crc_corrupted_page() {
        // Verify the envelope's CRC error propagates as IndexError::Storage.
        // Without this, swapping read_bucket to silently swallow CRC failures
        // would go undetected — the envelope tests can't catch that.
        let fx = make_index(2, Type::Int32);
        let init_txn = TransactionId::new(99);
        fx.wal.log_begin(init_txn).unwrap();

        let pid = fx.index.pid(PageNumber::new(0));
        let g = fx
            .index
            .store
            .fetch_page(LockRequest::exclusive(init_txn, pid))
            .unwrap();
        let mut bytes = encode_index_page(PageKind::HashBucket, |body| {
            let mut cur = std::io::Cursor::new(body);
            HashBucket::new(BucketNumber(0)).encode(&mut cur)
        })
        .unwrap();
        // Flip a payload bit so CRC no longer matches.
        bytes[ENVELOPE_HEADER_SIZE + 8] ^= 0x01;
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
        // num_buckets = 1 funnels every insert into bucket 0. Each Int32 entry
        // takes 5 + 14 = 19 bytes; with the 13-byte header a page holds about
        // (4096 - 13) / 19 ≈ 214 entries. Inserting 300 forces at least one
        // overflow page to be allocated.
        let fx = make_index(1, Type::Int32);
        let txn = begin(&fx.wal, 1);
        let n = 300;
        for i in 0..n {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &Value::Int32(i), rid(1, 0, slot))
                .unwrap();
        }

        assert!(
            fx.index.num_pages.load(Ordering::Acquire) > 1,
            "expected at least one overflow page to be allocated"
        );

        // Spot-check that entries from both ends of the chain are findable.
        for i in [0, 100, 250, n - 1] {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            let found = fx.index.search(txn, &Value::Int32(i)).unwrap();
            assert_eq!(found, vec![rid(1, 0, slot)], "missing key {i}");
        }
    }

    #[test]
    fn delete_walks_overflow_chain() {
        let fx = make_index(1, Type::Int32);
        let txn = begin(&fx.wal, 1);
        for i in 0..300 {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &Value::Int32(i), rid(1, 0, slot))
                .unwrap();
        }
        // A late key will live on an overflow page.
        fx.index
            .delete(txn, &Value::Int32(290), rid(1, 0, 290))
            .unwrap();
        assert!(fx.index.search(txn, &Value::Int32(290)).unwrap().is_empty());
    }

    #[test]
    fn range_search_returns_keys_in_inclusive_range() {
        let fx = make_index(4, Type::Int32);
        let txn = begin(&fx.wal, 1);
        for i in 0..20 {
            let slot = u16::try_from(i).expect("test key must fit into RID slot");
            fx.index
                .insert(txn, &Value::Int32(i), rid(1, 0, slot))
                .unwrap();
        }
        let found = fx
            .index
            .range_search(txn, &Value::Int32(5), &Value::Int32(10))
            .unwrap();
        assert_eq!(found.len(), 6, "expected keys 5..=10 inclusive");
    }
}
