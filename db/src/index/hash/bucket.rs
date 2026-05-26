//! Hash bucket page layout and helpers.
//!
//! This module holds the page-local representation used by `HashIndex`.
//! The index algorithm itself lives in `hash/mod.rs`.

use crate::{
    Lsn, PageNumber,
    codec::{CodecError, Decode, Encode, ReadLeExt, WriteLeExt},
    index::{CompositeKey, IndexEntry, PageKind},
    primitives::RecordId,
    storage::{PAGE_SIZE, TypedPage, UNIVERSAL_HEADER_SIZE},
};

/// A bucket index in `0..num_buckets`.
///
/// Static hashing means bucket N lives at page N, so this also doubles as a
/// [`PageNumber`] via the `From<BucketNumber> for PageNumber` impl.
///
/// Not user-visible at the SQL layer — the bucket count is a property of
/// the index file format, decided when `CREATE INDEX … USING HASH` runs and
/// frozen thereafter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct BucketNumber(pub(super) u32);

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

/// Hand-written codec because `BucketNumber` is a tuple struct (the derive only
/// supports named fields). Wire format is the inner `u32` little-endian — no
/// length prefix, no discriminant.
impl Encode for BucketNumber {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_le_u32(self.0)?;
        Ok(())
    }
}

impl Decode for BucketNumber {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_le_u32()?))
    }
}

/// Fixed per-page metadata for a hash bucket page.
///
/// Stored in the [`TypedPage`] `header` slot, immediately after the 13-byte
/// universal header. On-disk: `bucket_num(4) | overflow(4)` = 8 bytes.
///
/// - `bucket` — which logical bucket this page belongs to.
/// - `overflow` — next page in the overflow chain, or `None` at the tail.
#[derive(Debug, Clone, Encode, Decode)]
pub(super) struct HashBucketHeader {
    /// Logical bucket number this page stores entries for.
    pub bucket: BucketNumber,
    /// Next overflow page in the chain, or `None` for chain tail.
    pub overflow: Option<PageNumber>,
}

/// A single hash bucket page containing `(key, RecordId)` entries plus an
/// optional link to the next overflow page in the bucket chain.
pub(super) type HashBucket = TypedPage<HashBucketHeader, Vec<IndexEntry>>;

impl TypedPage<HashBucketHeader, Vec<IndexEntry>> {
    /// Bytes consumed before the first [`IndexEntry`] on a page.
    ///
    /// ```text
    /// UNIVERSAL_HEADER_SIZE (13) + bucket_num (4) + overflow (4) + entry_count (4) = 25
    /// ```
    ///
    /// Used by [`Self::has_space_for`] to compute remaining room on a page.
    pub(super) const HEADER_SIZE: usize = UNIVERSAL_HEADER_SIZE + 4 + 4 + 4;

    /// Builds an empty in-memory bucket for the given bucket number.
    ///
    /// Two callers in this file:
    /// - `HashIndex::init` when stamping head pages on `CREATE INDEX`.
    /// - `HashIndex::spill_to_overflow` when the chain tail fills and a fresh overflow page is
    ///   allocated.
    pub(super) fn new_bucket(bucket: BucketNumber) -> Self {
        TypedPage::new(
            PageKind::HashBucket,
            Lsn::INVALID,
            HashBucketHeader {
                bucket,
                overflow: None,
            },
            Vec::new(),
        )
    }

    /// Bytes this bucket would occupy on disk if encoded right now
    /// (header + every `IndexEntry`).
    ///
    /// Drives [`Self::has_space_for`]; not exposed to the planner.
    pub(super) fn used_bytes(&self) -> usize {
        Self::HEADER_SIZE
            + self
                .body
                .iter()
                .map(IndexEntry::encoded_size)
                .sum::<usize>()
    }

    /// Bytes still available in this bucket page before it spills to overflow.
    #[inline]
    pub(super) fn free_bytes(&self) -> usize {
        PAGE_SIZE.saturating_sub(self.used_bytes())
    }

    /// Whether `entry` will fit in this bucket page without spilling.
    ///
    /// Returns `true` when the bucket can absorb the next insert in-place,
    /// `false` when `HashIndex::spill_to_overflow` must allocate a fresh
    /// page and link it from the current tail.
    ///
    /// The size estimate is exact because [`IndexEntry::encoded_size`]
    /// mirrors [`IndexEntry::encode`] branch-for-branch.
    pub(super) fn has_space_for(&self, entry: &IndexEntry) -> bool {
        entry.encoded_size() <= self.free_bytes()
    }

    /// Returns `true` when an identical `(key, RecordId)` entry exists.
    pub(super) fn contains(&self, entry: &IndexEntry) -> bool {
        self.body.iter().any(|e| e == entry)
    }

    /// Appends one entry to this page body in insertion order.
    pub(super) fn push(&mut self, entry: IndexEntry) {
        self.body.push(entry);
    }

    /// Finds the first matching `(key, RecordId)` pair and returns its index.
    pub(super) fn find(&self, key: &CompositeKey, rid: &RecordId) -> Option<usize> {
        self.body
            .iter()
            .position(|e| e.key == *key && e.rid == *rid)
    }

    /// Removes the entry at `pos`, shifting later entries left by one slot.
    ///
    /// # Panics
    ///
    /// Panics if `pos >= self.body.len()`.
    pub(super) fn remove(&mut self, pos: usize) {
        self.body.remove(pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FileId, Value,
        index::CompositeKey,
        primitives::{RecordId, SlotId},
    };

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    /// Single-column key shorthand for tests that predate composite indexes.
    fn k(v: Value) -> CompositeKey {
        CompositeKey::single(v)
    }

    fn name_key(last: &str, first: &str) -> CompositeKey {
        CompositeKey::new(vec![
            Value::String(last.into()),
            Value::String(first.into()),
        ])
    }

    #[test]
    fn header_size_matches_layout() {
        // universal(13) + bucket_num(4) + overflow(4) + entry_count(4) = 25
        assert_eq!(HashBucket::HEADER_SIZE, UNIVERSAL_HEADER_SIZE + 12);
        assert_eq!(HashBucket::HEADER_SIZE, 25);
    }

    #[test]
    fn empty_bucket_uses_only_header() {
        let b = HashBucket::new_bucket(BucketNumber(0));
        assert_eq!(b.used_bytes(), HashBucket::HEADER_SIZE);
        assert_eq!(b.free_bytes(), PAGE_SIZE - HashBucket::HEADER_SIZE);
    }

    #[test]
    fn used_bytes_grows_with_each_entry() {
        let mut b = HashBucket::new_bucket(BucketNumber(0));
        let entry = IndexEntry::new(k(Value::Int32(7)), rid(1, 0, 0));
        let before = b.used_bytes();
        b.push(entry.clone());
        assert_eq!(b.used_bytes(), before + entry.encoded_size());
    }

    #[test]
    fn has_space_for_returns_false_when_full() {
        let mut b = HashBucket::new_bucket(BucketNumber(0));
        let entry = IndexEntry::new(k(Value::Int32(0)), rid(1, 0, 0));
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
    fn page_codec_roundtrip_empty() {
        let b = HashBucket::new_bucket(BucketNumber(42));
        let buf = b.to_bytes().unwrap();
        assert_eq!(buf.len(), PAGE_SIZE);

        let decoded = HashBucket::from_bytes(&buf).unwrap();
        assert_eq!(decoded.header.bucket, BucketNumber(42));
        assert_eq!(decoded.header.overflow, None);
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn page_codec_roundtrip_with_entries_and_overflow() {
        let mut b = HashBucket::new_bucket(BucketNumber(3));
        b.header.overflow = Some(PageNumber::new(99));
        b.push(IndexEntry::new(k(Value::Int32(1)), rid(1, 0, 0)));
        b.push(IndexEntry::new(
            k(Value::String("hello".into())),
            rid(1, 2, 5),
        ));

        let bytes = b.to_bytes().unwrap();
        assert_eq!(bytes.len(), PAGE_SIZE);
        let decoded = HashBucket::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.header.bucket, BucketNumber(3));
        assert_eq!(decoded.header.overflow, Some(PageNumber::new(99)));
        assert_eq!(decoded.body.len(), 2);
        assert_eq!(decoded.body[0].key, k(Value::Int32(1)));
        assert_eq!(decoded.body[1].key, k(Value::String("hello".into())));
    }

    #[test]
    fn composite_key_codec_roundtrip() {
        // Composite IndexEntry survives encode/decode unchanged. Catches
        // accidental drift between CompositeKey::encode and decode.
        let mut b = HashBucket::new_bucket(BucketNumber(0));
        b.push(IndexEntry::new(name_key("Smith", "Ada"), rid(1, 0, 0)));
        b.push(IndexEntry::new(
            CompositeKey::new(vec![Value::Int32(7), Value::String("x".into())]),
            rid(1, 0, 1),
        ));

        let bytes = b.to_bytes().unwrap();
        let decoded = HashBucket::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.body, b.body);
    }
}
