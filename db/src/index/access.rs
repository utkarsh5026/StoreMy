//! The shared contracts every secondary-index access method follows.
//!
//! In database terminology, an *access method* is the algorithm-plus-layout
//! that lets the executor find rows by some criterion (a B-tree, a hash
//! index, an R-tree, …). [`Index`] is the abstraction the executor and the
//! `IndexManager` talk to; each family ([`crate::index::btree`],
//! [`crate::index::hash`], …) provides its own impl.
//!
//! This file also holds the **on-disk envelope** every index page wears:
//! a [`PageKind`] tag plus a CRC, written by [`encode_index_page`] and
//! verified by [`decode_index_page`]. Heap pages predate this convention
//! and are *not* envelope pages — they live in their own format.

use crate::{
    FileId, PageNumber, TransactionId, Type,
    buffer_pool::{
        LockRequest,
        page_store::{PageGuard, PageStore},
    },
    codec::CodecError,
    index::{CompositeKey, IndexError, IndexKind},
    primitives::{PageId, RecordId},
    storage::{PAGE_SIZE, StorageError},
};

/// A trait representing a secondary index over a table.
///
/// The `Index` trait abstracts any auxiliary structure (hash, B-tree, etc.)
/// that maps from key values to a set of record identifiers (`RecordId`).
///
/// Methods should be concurrency-safe and respect transactional semantics;
/// implementations are responsible for ensuring thread safety and
/// consistency, hence the `Send + Sync` bounds.
pub trait Index: Send + Sync {
    /// Inserts a `(key, rid)` pair into the index.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID under which the insert occurs.
    /// * `key` - The value to be indexed (must match the index's `key_type`).
    /// * `rid` - The record identifier to associate with the key.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if:
    /// - The key type does not match the index's key type.
    /// - The exact `(key, rid)` pair already exists (for non-unique indexes).
    /// - Internal page or storage errors occur.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = CompositeKey::new(vec![Value::Int(7)]);
    /// index.insert(txn, &key, rid)?;
    /// ```
    fn insert(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError>;

    /// Removes the specific `(key, rid)` entry from the index, if present.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID under which the delete occurs.
    /// * `key` - The value whose record is to be removed.
    /// * `rid` - The record identifier to remove.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if:
    /// - The `(key, rid)` pair does not exist.
    /// - Storage or page-level errors occur.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = CompositeKey::new(vec![Value::Int(7)]);
    /// index.delete(txn, &key, rid)?;
    /// ```
    fn delete(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError>;

    /// Searches for all record IDs associated with a given key.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID performing the search.
    /// * `key` - The value to look up.
    ///
    /// # Returns
    ///
    /// A vector of `RecordId` matching the key, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if the key shape does not match the index
    /// definition or if the lookup cannot be completed due to storage errors.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = CompositeKey::new(vec![Value::Int(7)]);
    /// let matches = index.search(txn, &key)?;
    /// ```
    fn search(&self, txn: TransactionId, key: &CompositeKey) -> Result<Vec<RecordId>, IndexError>;

    /// Performs an inclusive range scan from `start` to `end`.
    ///
    /// Only supported on ordered (e.g., B-tree) indexes; unordered
    /// implementations (e.g., hash indexes) may return an error or a
    /// best-effort result containing entries between the bounds.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID making the request.
    /// * `start` - The lower bound (inclusive) of the key range.
    /// * `end` - The upper bound (inclusive) of the key range.
    ///
    /// # Returns
    ///
    /// All matching `RecordId` values, or an error.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if either bound has the wrong shape or type,
    /// if the index implementation does not support range scans, or if reading
    /// index pages fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let start = CompositeKey::new(vec![Value::Int(10)]);
    /// let end = CompositeKey::new(vec![Value::Int(20)]);
    /// let matches = index.range_search(txn, &start, &end)?;
    /// ```
    fn range_search(
        &self,
        txn: TransactionId,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError>;

    /// Returns the kind of this index (e.g., hash, B-tree).
    fn kind(&self) -> IndexKind;

    /// Returns the per-column declared types of the index, in column order.
    fn key_types(&self) -> &[Type];

    /// Returns the file ID of the index.
    fn file_id(&self) -> FileId;

    /// Checks whether a composite key matches this index's declared key layout.
    ///
    /// The check enforces both arity (same number of key components) and
    /// per-position type equality.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `IndexError::KeyArityMismatch` when the component count differs.
    /// - `IndexError::KeyTypeMismatch` when a component has the wrong type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// index.check_key_type(&key)?;
    /// ```
    fn check_key_type(&self, key: &CompositeKey) -> Result<(), IndexError> {
        let expected = self.key_types();
        if key.arity() != expected.len() {
            return Err(IndexError::KeyArityMismatch {
                expected: expected.len(),
                got: key.arity(),
            });
        }

        for (position, (t, v)) in expected.iter().zip(key.components()).enumerate() {
            match v.get_type() {
                Some(actual) if actual == *t => {}
                other => {
                    return Err(IndexError::KeyTypeMismatch {
                        position,
                        expected: *t,
                        got: other,
                    });
                }
            }
        }
        Ok(())
    }

    /// Fetches and pins an index page from the buffer pool.
    ///
    /// This is a small convenience shared by index implementations: it builds a [`PageId`] from
    /// `self.file_id()` and the provided [`PageNumber`], acquires either a shared or exclusive
    /// lock for `txn`, and returns the resulting pinned [`PageGuard`].
    ///
    /// The returned guard borrows `store` for `'a`, so the page remains pinned for at most as long
    /// as the caller keeps the guard alive.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError::PageStore`] if locking or fetching the page fails.
    fn read_page<'a>(
        &self,
        txn: TransactionId,
        p: PageNumber,
        exclusive: bool,
        store: &'a PageStore,
    ) -> Result<PageGuard<'a>, IndexError> {
        let page_id = PageId {
            file_id: self.file_id(),
            page_no: p,
        };

        let req = if exclusive {
            LockRequest::exclusive(txn, page_id)
        } else {
            LockRequest::shared(txn, page_id)
        };
        let guard = store.fetch_page(req)?;
        Ok(guard)
    }
}

// Every index page (hash bucket, B-tree node, …) shares a uniform 5-byte
// prelude so one set of helpers can stamp/verify the CRC and dispatch on
// kind. Heap pages are *not* envelope pages — they predate this convention
// and live in their own file with their own header.
//
// On-disk layout of a single envelope page (PAGE_SIZE bytes, zero-padded):
//
//   offset 0:    kind byte (see PageKind)
//   offset 1..5: CRC32 over the whole page (CRC slot read as zero)
//   offset 5..:  page-specific payload, zero-padded to PAGE_SIZE
//
// CRC32 is computed via crc32fast over the entire PAGE_SIZE buffer with the
// CRC slot itself treated as four zero bytes — that's the standard chicken-
// and-egg dodge.

/// Tag identifying which kind of index page this is.
///
/// Stored at byte 0 of every envelope page. Reserved ranges, by family:
///
///   `0xA0..=0xAF` — hash family
///   `0xB0..=0xBF` — B-tree family
///   (others reserved for future families)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PageKind {
    HashBucket = 0xA0,
    BTreeLeaf = 0xB0,
    BTreeInternal = 0xB1,
}

impl TryFrom<u8> for PageKind {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xA0 => Ok(Self::HashBucket),
            0xB0 => Ok(Self::BTreeLeaf),
            0xB1 => Ok(Self::BTreeInternal),
            other => Err(StorageError::UnknownPageKind(other)),
        }
    }
}

pub const ENVELOPE_HEADER_SIZE: usize = 5;

const KIND_OFFSET: usize = 0;
const CRC_OFFSET: usize = 1;

/// Compute CRC32 over `bytes`, treating the 4-byte CRC slot as zero.
fn compute_page_crc(bytes: &[u8; PAGE_SIZE]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes[..CRC_OFFSET]);
    h.update(&[0u8; 4]);
    h.update(&bytes[CRC_OFFSET + 4..]);
    h.finalize()
}

/// Encode an index page: stamps the kind byte, calls `payload` to fill the
/// body, then computes and stamps the CRC over the whole buffer.
///
/// The closure receives a mutable slice starting at [`ENVELOPE_HEADER_SIZE`]
/// and ending at [`PAGE_SIZE`]. The slice is zero-initialized; the closure
/// writes only the bytes it needs and the rest stays zero.
///
/// # Errors
///
/// Propagates any [`CodecError`] returned by the payload closure.
pub fn encode_index_page<F>(kind: PageKind, payload: F) -> Result<[u8; PAGE_SIZE], CodecError>
where
    F: FnOnce(&mut [u8]) -> Result<(), CodecError>,
{
    let mut buf = [0u8; PAGE_SIZE];
    buf[KIND_OFFSET] = kind as u8;
    payload(&mut buf[ENVELOPE_HEADER_SIZE..])?;
    let crc = compute_page_crc(&buf);
    buf[CRC_OFFSET..=4].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Verify and unpack an index page, returning the parsed kind tag and a
/// slice over the payload (everything past the envelope header).
///
/// # Errors
///
/// - [`StorageError::ChecksumMismatch`] if the stored CRC doesn't match what's recomputed over the
///   page.
/// - [`StorageError::UnknownPageKind`] if byte 0 isn't a recognized [`PageKind`] discriminant —
///   this catches uninitialized (all-zero) pages too, since their kind byte is `0x00`.
pub fn decode_index_page(bytes: &[u8; PAGE_SIZE]) -> Result<(PageKind, &[u8]), StorageError> {
    let stored = u32::from_le_bytes(bytes[CRC_OFFSET..=4].try_into().unwrap());
    let computed = compute_page_crc(bytes);
    if stored != computed {
        return Err(StorageError::ChecksumMismatch { stored, computed });
    }
    let kind = PageKind::try_from(bytes[KIND_OFFSET])?;
    Ok((kind, &bytes[ENVELOPE_HEADER_SIZE..]))
}

#[cfg(test)]
mod envelope_tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_kind_and_payload() {
        let buf = encode_index_page(PageKind::HashBucket, |body| {
            body[..4].copy_from_slice(&[1, 2, 3, 4]);
            Ok(())
        })
        .unwrap();

        let (kind, payload) = decode_index_page(&buf).unwrap();
        assert_eq!(kind, PageKind::HashBucket);
        assert_eq!(&payload[..4], &[1, 2, 3, 4]);
        // Bytes the closure didn't touch must be zero.
        assert!(payload[4..].iter().all(|&b| b == 0));
    }

    #[test]
    fn flipped_byte_in_payload_fails_checksum() {
        let mut buf = encode_index_page(PageKind::BTreeLeaf, |body| {
            body[0] = 0xAA;
            Ok(())
        })
        .unwrap();

        // Flip a payload bit that the encoder wrote.
        buf[ENVELOPE_HEADER_SIZE] ^= 0x01;

        match decode_index_page(&buf).unwrap_err() {
            StorageError::ChecksumMismatch { .. } => {}
            e => panic!("expected ChecksumMismatch, got {e:?}"),
        }
    }

    #[test]
    fn flipped_byte_in_kind_fails_checksum() {
        let mut buf = encode_index_page(PageKind::HashBucket, |_| Ok(())).unwrap();
        buf[0] = PageKind::BTreeLeaf as u8;
        match decode_index_page(&buf).unwrap_err() {
            StorageError::ChecksumMismatch { .. } => {}
            e => panic!("expected ChecksumMismatch, got {e:?}"),
        }
    }

    #[test]
    fn all_zero_page_is_rejected() {
        // An uninitialized page has a stored CRC of 0, but CRC32 of 4096
        // zero bytes is not zero — so the checksum check fires first.
        // Either rejection path is fine; what matters is that uninitialized
        // pages can't be confused for valid ones.
        let buf = [0u8; PAGE_SIZE];
        match decode_index_page(&buf).unwrap_err() {
            StorageError::ChecksumMismatch { .. } | StorageError::UnknownPageKind(0) => {}
            e => panic!("expected blank page rejection, got {e:?}"),
        }
    }

    #[test]
    fn unknown_kind_byte_is_rejected() {
        let mut buf = encode_index_page(PageKind::HashBucket, |_| Ok(())).unwrap();
        buf[0] = 0xFF;
        let crc = compute_page_crc(&buf);
        buf[CRC_OFFSET..=4].copy_from_slice(&crc.to_le_bytes());

        match decode_index_page(&buf).unwrap_err() {
            StorageError::UnknownPageKind(0xFF) => {}
            e => panic!("expected UnknownPageKind(0xFF), got {e:?}"),
        }
    }

    #[test]
    fn kind_try_from_round_trips() {
        for k in [
            PageKind::HashBucket,
            PageKind::BTreeLeaf,
            PageKind::BTreeInternal,
        ] {
            assert_eq!(PageKind::try_from(k as u8).unwrap(), k);
        }
    }
}
