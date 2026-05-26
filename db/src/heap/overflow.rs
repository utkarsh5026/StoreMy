//! Overflow pages for heap tuples that exceed the in-page size limit.
//!
//! When a tuple is too large to fit in the available space on a single heap
//! page, its data is spilled into one or more overflow pages. Each overflow
//! page carries a pointer to the next in the chain so arbitrarily large tuples
//! can be reconstructed by following the chain.
//!
//! ## On-disk layout
//!
//! ```text
//! offset  0       : PageKind::HeapOverflow (0x11)
//! offset  1..5    : CRC32 (u32 LE)
//! offset  5..13   : page_lsn (u64 LE)
//! offset 13..17   : next_page (u32 LE) — `None` encodes as NIL sentinel (`u32::MAX`)
//! offset 17..21   : payload_len (u32 LE)
//! offset 21..     : payload bytes (up to OVERFLOW_PAYLOAD_SIZE)
//! ```
//!
//! [`OverflowPage`] is a type alias for `TypedPage<OverflowHeader, OverflowBody>`,
//! so it inherits [`TypedPage::to_page_bytes`], [`TypedPage::from_page_bytes`],
//! and the [`Encode`] / [`Decode`] / [`crate::storage::Page`] trait impls for free.

use std::io::{Read, Write};

use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::{Lsn, PageNumber},
    storage::{PAGE_SIZE, PageKind, StorageError, TypedPage, UNIVERSAL_HEADER_SIZE},
};

/// Bytes consumed by the fixed header of an overflow page:
/// universal header (13) + `next_page` field (4) = 17.
pub const OVERFLOW_HDR_SIZE: usize = UNIVERSAL_HEADER_SIZE + PageNumber::SIZE;

/// Bytes available for the length prefix inside [`OverflowBody`].
const BODY_LEN_PREFIX: usize = size_of::<u32>();

/// Maximum number of payload bytes that fit in a single overflow page.
///
/// Computed as `PAGE_SIZE - OVERFLOW_HDR_SIZE - BODY_LEN_PREFIX`
/// = 4096 - 17 - 4 = 4075.
pub const OVERFLOW_PAYLOAD_SIZE: usize = PAGE_SIZE - OVERFLOW_HDR_SIZE - BODY_LEN_PREFIX;

/// Page-type-specific header for an overflow page.
///
/// Contains a single field: the page number of the next overflow page in the
/// chain. `None` marks the last (or only) overflow page for a given tuple.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct OverflowHeader {
    pub next_page: Option<PageNumber>,
}

impl Encode for OverflowHeader {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.next_page.encode(writer)
    }
}

impl Decode for OverflowHeader {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            next_page: Option::<PageNumber>::decode(reader)?,
        })
    }
}

/// Body of an overflow page: a length-prefixed raw byte payload.
///
/// On disk: `[payload_len: u32 LE][data: payload_len bytes]`.
/// The maximum `payload_len` is [`OVERFLOW_PAYLOAD_SIZE`] bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowBody {
    /// Raw payload bytes for this segment of the overflowed tuple.
    pub data: Vec<u8>,
}

impl Encode for OverflowBody {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.data.encode(writer)?;
        Ok(())
    }
}

impl Decode for OverflowBody {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let data = Vec::<u8>::decode(reader)?;
        Ok(Self { data })
    }
}

/// A single overflow page: a [`TypedPage`] whose header points to the next
/// page in the overflow chain and whose body holds raw tuple payload bytes.
///
/// All serialisation, CRC stamping, and `page_lsn` tracking are handled by
/// `TypedPage`. This alias inherits [`TypedPage::to_page_bytes`],
/// [`TypedPage::from_page_bytes`], and the [`Encode`] / [`Decode`] / [`crate::storage::Page`]
/// trait impls automatically.
pub type OverflowPage = TypedPage<OverflowHeader, OverflowBody>;

impl TypedPage<OverflowHeader, OverflowBody> {
    /// Creates a new overflow page holding `payload` bytes.
    ///
    /// `next_page` is the page number of the next overflow page in the chain,
    /// or `None` if this is the last (or only) segment.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::TupleTooLarge`] if `payload` exceeds
    /// [`OVERFLOW_PAYLOAD_SIZE`] bytes.
    pub fn overflow_new(
        next_page: Option<PageNumber>,
        payload: Vec<u8>,
    ) -> Result<Self, StorageError> {
        if payload.len() > OVERFLOW_PAYLOAD_SIZE {
            return Err(StorageError::TupleTooLarge {
                size: payload.len(),
                max: OVERFLOW_PAYLOAD_SIZE,
            });
        }
        Ok(TypedPage::new(
            PageKind::HeapOverflow,
            Lsn::INVALID,
            OverflowHeader { next_page },
            OverflowBody { data: payload },
        ))
    }

    /// Returns the page number of the next overflow page in the chain, or
    /// `None` if this is the last segment.
    pub fn next_page(&self) -> Option<PageNumber> {
        self.header.next_page
    }

    /// Returns `true` if there is no further overflow page after this one.
    pub fn is_last(&self) -> bool {
        self.header.next_page.is_none()
    }

    /// Returns a slice over the raw payload bytes stored in this page.
    pub fn payload(&self) -> &[u8] {
        &self.body.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page_crc_valid;

    fn make_page(next: Option<u32>, data: &[u8]) -> OverflowPage {
        OverflowPage::overflow_new(next.map(PageNumber::new), data.to_vec()).unwrap()
    }

    #[test]
    fn new_page_has_correct_kind_and_lsn() {
        let page = make_page(Some(0), b"hello");
        assert_eq!(page.kind, PageKind::HeapOverflow);
        assert_eq!(page.page_lsn, Lsn::INVALID);
    }

    #[test]
    fn payload_too_large_returns_error() {
        let big = vec![0u8; OVERFLOW_PAYLOAD_SIZE + 1];
        assert!(OverflowPage::overflow_new(None, big).is_err());
    }

    #[test]
    fn max_payload_fits() {
        let exact = vec![0xAAu8; OVERFLOW_PAYLOAD_SIZE];
        assert!(OverflowPage::overflow_new(None, exact).is_ok());
    }

    #[test]
    fn is_last_when_next_is_invalid() {
        let page = make_page(None, b"x");
        assert!(page.is_last());
    }

    #[test]
    fn is_not_last_when_next_is_valid() {
        let page = make_page(Some(7), b"x");
        assert!(!page.is_last());
        assert_eq!(page.next_page(), Some(PageNumber::new(7)));
    }

    #[test]
    fn roundtrip_preserves_all_fields() {
        let page = make_page(Some(42), b"overflow payload");
        let bytes = page.to_page_bytes().unwrap();

        let decoded = OverflowPage::from_page_bytes(&bytes).unwrap();
        assert_eq!(decoded.kind, PageKind::HeapOverflow);
        assert_eq!(decoded.header.next_page, Some(PageNumber::new(42)));
        assert_eq!(decoded.body.data, b"overflow payload");
    }

    #[test]
    fn encode_then_decode_via_traits() {
        let page = make_page(Some(99), b"trait roundtrip");

        let mut buf = Vec::new();
        page.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), PAGE_SIZE);

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: OverflowPage = OverflowPage::decode(&mut cursor).unwrap();
        assert_eq!(decoded.payload(), b"trait roundtrip");
    }

    #[test]
    fn to_page_bytes_stamps_valid_crc() {
        let bytes = make_page(Some(0), b"crc check").to_page_bytes().unwrap();
        assert!(page_crc_valid(&bytes));
    }

    #[test]
    fn overflow_hdr_size_is_17() {
        assert_eq!(OVERFLOW_HDR_SIZE, 17);
    }

    #[test]
    fn overflow_payload_size_is_correct() {
        assert_eq!(OVERFLOW_PAYLOAD_SIZE, PAGE_SIZE - 17 - 4);
        assert_eq!(OVERFLOW_PAYLOAD_SIZE, 4075);
    }
}
