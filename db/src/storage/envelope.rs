//! The shared 5-byte header every on-disk page begins with.
//!
//! `StoreMy` stores heap pages, B-tree nodes, and hash buckets in the same
//! fixed-size buffer ([`PAGE_SIZE`] bytes). Before any page-specific data,
//! every buffer starts with an **envelope**: a one-byte type tag ([`PageKind`])
//! followed by a four-byte CRC32 checksum. Keeping this layout in one place
//! means the double-write buffer, heap codec, and index codec can all stamp
//! and verify pages the same way.
//!
//! Layout:
//!
//! ```text
//! offset 0:    PageKind (u8)
//! offset 1..5: CRC32 over the whole page, with bytes 1..5 treated as zero
//! offset 5..:  page-specific payload
//! ```
//!
//! Heap pages write their own payload (`page_lsn`, slots, tuples) starting at
//! byte 5. Index pages use [`encode_index_page`](crate::index::access::encode_index_page)
//! / [`decode_index_page`](crate::index::access::decode_index_page) to wrap
//! their payloads behind the same envelope.

use super::PAGE_SIZE;
use crate::primitives::Lsn;

const CRC_OFFSET: usize = 1;

/// Number of bytes in the shared page envelope header.
///
/// The envelope is one [`PageKind`] byte plus a four-byte CRC32 field. All
/// page-specific fields start at this offset (byte 5).
pub const ENVELOPE_HEADER_SIZE: usize = 5;

/// Byte offset of the `page_lsn` field common to every page buffer.
///
/// All page types (heap, B-tree, hash) store an 8-byte little-endian LSN
/// immediately after the envelope header. The recovery (redo) pass uses this
/// offset to check whether a WAL record still needs to be replayed.
pub(crate) const PAGE_LSN_OFFSET: usize = ENVELOPE_HEADER_SIZE;

/// One-past-the-end byte offset of the `page_lsn` field.
///
/// Equivalent to `PAGE_LSN_OFFSET + size_of::<u64>()`.
pub(crate) const PAGE_LSN_END: usize = PAGE_LSN_OFFSET + std::mem::size_of::<u64>();

/// Reads the `page_lsn` from bytes `PAGE_LSN_OFFSET..PAGE_LSN_END` of a raw page buffer.
pub(crate) fn read_page_lsn(bytes: &[u8; PAGE_SIZE]) -> Lsn {
    Lsn(u64::from_le_bytes(
        bytes[PAGE_LSN_OFFSET..PAGE_LSN_END]
            .try_into()
            .expect("slice is exactly 8 bytes"),
    ))
}

/// Identifies what kind of page a buffer holds, stored at byte 0.
///
/// Recovery, the double-write buffer, and debug tools read this byte to tell
/// heap pages apart from index pages without decoding the payload. Discriminant
/// ranges are reserved by family:
///
/// - `0x10..=0x1F` — heap
/// - `0xA0..=0xAF` — hash index
/// - `0xB0..=0xBF` — B-tree index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PageKind {
    /// A heap file page holding table rows.
    Heap = 0x10,
    /// A heap overflow page holding tuple data that does not fit in a single
    /// heap page. Overflow pages are chained: each carries a pointer to the
    /// next overflow page in the sequence.
    HeapOverflow = 0x11,
    /// A hash-index bucket page.
    HashBucket = 0xA0,
    /// A B-tree leaf node page.
    BTreeLeaf = 0xB0,
    /// A B-tree internal (non-leaf) node page.
    BTreeInternal = 0xB1,
}

impl TryFrom<u8> for PageKind {
    type Error = super::StorageError;

    /// Parses a [`PageKind`] from the byte stored at offset 0 of a page buffer.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnknownPageKind`](super::StorageError::UnknownPageKind)
    /// when `value` is not one of the known discriminants (including `0x00` on
    /// an uninitialized page).
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::storage::PageKind;
    ///
    /// assert_eq!(PageKind::try_from(0x10).unwrap(), PageKind::Heap);
    /// assert!(PageKind::try_from(0xFF).is_err());
    /// ```
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x10 => Ok(Self::Heap),
            0x11 => Ok(Self::HeapOverflow),
            0xA0 => Ok(Self::HashBucket),
            0xB0 => Ok(Self::BTreeLeaf),
            0xB1 => Ok(Self::BTreeInternal),
            other => Err(super::StorageError::UnknownPageKind(other)),
        }
    }
}

/// Computes the CRC32 checksum for a full page buffer.
///
/// The four-byte CRC slot at bytes 1..5 is treated as zero during the hash.
/// That avoids the circular dependency of hashing a field that holds the hash
/// itself: write zeros, hash the rest, then store the result in those bytes.
pub(crate) fn compute_page_crc(bytes: &[u8; PAGE_SIZE]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes[..CRC_OFFSET]);
    h.update(&[0u8; 4]);
    h.update(&bytes[CRC_OFFSET + 4..]);
    h.finalize()
}

/// Writes the CRC32 of `page` into bytes 1..5.
///
/// Call this after every other byte in the buffer is final. The [`PageKind`]
/// byte at offset 0 must already be set.
pub(crate) fn stamp_page_crc(page: &mut [u8; PAGE_SIZE]) {
    let crc = compute_page_crc(page);
    page[CRC_OFFSET..=4].copy_from_slice(&crc.to_le_bytes());
}

/// Returns whether the stored CRC at bytes 1..5 matches the page content.
///
/// A freshly zeroed page (every byte is `0`) returns `false` because the stored
/// CRC is zero but the computed CRC over 4096 zero bytes is not. Callers that
/// treat an all-zero buffer as a valid blank page should check for that before
/// calling this function.
///
/// # Panics
///
/// Never panics: the input is always exactly [`PAGE_SIZE`] bytes, so slicing
/// the four-byte CRC field cannot fail.
pub(crate) fn page_crc_valid(bytes: &[u8; PAGE_SIZE]) -> bool {
    let stored = u32::from_le_bytes(bytes[CRC_OFFSET..=4].try_into().unwrap());
    stored == compute_page_crc(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageError;

    fn blank_page() -> [u8; PAGE_SIZE] {
        [0u8; PAGE_SIZE]
    }

    fn stamped_heap_page(payload_byte: u8) -> [u8; PAGE_SIZE] {
        let mut page = blank_page();
        page[0] = PageKind::Heap as u8;
        page[ENVELOPE_HEADER_SIZE] = payload_byte;
        stamp_page_crc(&mut page);
        page
    }

    // --- happy path: stamp and validate ---

    #[test]
    fn test_stamp_page_crc_then_page_crc_valid_succeeds() {
        let page = stamped_heap_page(0xAB);
        assert!(page_crc_valid(&page));
    }

    #[test]
    fn test_stamp_page_crc_writes_little_endian_crc() {
        let page = stamped_heap_page(0x01);
        let stored = u32::from_le_bytes(page[1..=4].try_into().unwrap());
        assert_eq!(stored, compute_page_crc(&page));
    }

    #[test]
    fn test_stamp_page_crc_is_idempotent() {
        let mut page = stamped_heap_page(0x42);
        let first: [u8; 4] = page[1..=4].try_into().unwrap();
        stamp_page_crc(&mut page);
        assert_eq!(page[1..=4], first);
    }

    // --- compute_page_crc behavior ---

    #[test]
    fn test_compute_page_crc_ignores_stored_crc_slot() {
        let a = stamped_heap_page(0x55);
        let mut b = a;
        b[1] ^= 0xFF;
        b[2] ^= 0xFF;
        b[3] ^= 0xFF;
        b[4] ^= 0xFF;
        assert_eq!(compute_page_crc(&a), compute_page_crc(&b));
    }

    #[test]
    fn test_compute_page_crc_changes_when_kind_changes() {
        let a = stamped_heap_page(0x00);
        let mut b = a;
        b[0] = PageKind::HashBucket as u8;
        assert_ne!(compute_page_crc(&a), compute_page_crc(&b));
    }

    #[test]
    fn test_compute_page_crc_changes_when_payload_changes() {
        let a = stamped_heap_page(0x00);
        let b = stamped_heap_page(0x01);
        assert_ne!(compute_page_crc(&a), compute_page_crc(&b));
    }

    #[test]
    fn test_compute_page_crc_matches_manual_algorithm() {
        let page = stamped_heap_page(0x77);
        let mut h = crc32fast::Hasher::new();
        h.update(&page[..1]);
        h.update(&[0u8; 4]);
        h.update(&page[5..]);
        assert_eq!(compute_page_crc(&page), h.finalize());
    }

    // --- edge cases ---

    #[test]
    fn test_page_crc_valid_rejects_all_zero_page() {
        assert!(!page_crc_valid(&blank_page()));
    }

    #[test]
    fn test_compute_page_crc_of_blank_page_is_nonzero() {
        assert_ne!(compute_page_crc(&blank_page()), 0);
    }

    #[test]
    fn test_envelope_header_size_is_five() {
        assert_eq!(ENVELOPE_HEADER_SIZE, 5);
    }

    // --- error paths: PageKind parsing ---

    #[test]
    fn test_page_kind_try_from_accepts_all_known_discriminants() {
        for kind in [
            PageKind::Heap,
            PageKind::HeapOverflow,
            PageKind::HashBucket,
            PageKind::BTreeLeaf,
            PageKind::BTreeInternal,
        ] {
            assert_eq!(PageKind::try_from(kind as u8).unwrap(), kind);
        }
    }

    #[test]
    fn test_page_kind_try_from_rejects_zero_byte() {
        match PageKind::try_from(0x00).unwrap_err() {
            StorageError::UnknownPageKind(0x00) => {}
            other => panic!("expected UnknownPageKind(0x00), got {other:?}"),
        }
    }

    #[test]
    fn test_page_kind_try_from_rejects_unknown_byte() {
        match PageKind::try_from(0xFF).unwrap_err() {
            StorageError::UnknownPageKind(0xFF) => {}
            other => panic!("expected UnknownPageKind(0xFF), got {other:?}"),
        }
    }

    #[test]
    fn test_page_kind_try_from_rejects_reserved_but_unassigned_bytes() {
        for byte in [0xA1, 0xB2] {
            match PageKind::try_from(byte).unwrap_err() {
                StorageError::UnknownPageKind(b) => assert_eq!(b, byte),
                other => panic!("expected UnknownPageKind({byte:#04x}), got {other:?}"),
            }
        }
    }

    // --- tamper / corruption detection ---

    #[test]
    fn test_page_crc_valid_rejects_flipped_payload_byte() {
        let mut page = stamped_heap_page(0xAA);
        page[ENVELOPE_HEADER_SIZE] ^= 0x01;
        assert!(!page_crc_valid(&page));
    }

    #[test]
    fn test_page_crc_valid_rejects_flipped_kind_without_restamp() {
        let mut page = stamped_heap_page(0x00);
        page[0] = PageKind::BTreeLeaf as u8;
        assert!(!page_crc_valid(&page));
    }

    #[test]
    fn test_restamping_after_kind_change_restores_validity() {
        let mut page = stamped_heap_page(0x00);
        page[0] = PageKind::BTreeLeaf as u8;
        assert!(!page_crc_valid(&page));
        stamp_page_crc(&mut page);
        assert!(page_crc_valid(&page));
    }

    #[test]
    fn test_page_kind_discriminants_match_repr_u8() {
        assert_eq!(PageKind::Heap as u8, 0x10);
        assert_eq!(PageKind::HeapOverflow as u8, 0x11);
        assert_eq!(PageKind::HashBucket as u8, 0xA0);
        assert_eq!(PageKind::BTreeLeaf as u8, 0xB0);
        assert_eq!(PageKind::BTreeInternal as u8, 0xB1);
    }
}
