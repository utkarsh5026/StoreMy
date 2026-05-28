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

use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use thiserror::Error;

use crate::{
    FileId, TransactionId,
    buffer_pool::page_store::{PageStore, PageStoreError},
    codec::{CodecError, Decode, Encode},
    primitives::{Lsn, PageDescriptor, PageId, PageNumber},
    storage::{PAGE_SIZE, PageKind, StorageError, TypedPage, UNIVERSAL_HEADER_SIZE},
    wal::WalError,
};

#[derive(Debug, Error)]
pub enum OverflowError {
    #[error("page store: {0}")]
    Store(#[from] PageStoreError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("wal: {0}")]
    Wal(#[from] WalError),
}

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

pub struct OverflowFile {
    file_id: FileId,
    buffer_pool: Arc<PageStore>,
    num_pages: AtomicU32,
}

impl OverflowFile {
    /// Creates a new `OverflowFile` instance.
    ///
    /// # Arguments
    ///
    /// * `file_id` - The identifier of the overflow file.
    /// * `buffer_pool` - A shared reference to the buffer pool for page storage.
    /// * `existing_pages` - The number of overflow pages that already exist in the file.
    ///
    /// # Returns
    ///
    /// An initialized `OverflowFile` with the provided file ID, buffer pool, and page count.
    pub fn new(file_id: FileId, buffer_pool: Arc<PageStore>, existing_pages: u32) -> Self {
        Self {
            file_id,
            buffer_pool,
            num_pages: AtomicU32::new(existing_pages),
        }
    }

    /// Reconstructs an overflowed tuple payload by following its page chain.
    ///
    /// `overflow_ptr` points at the first overflow page. Each page contributes
    /// its payload bytes and optionally names the next page in the chain. The
    /// returned vector is the concatenation of those chunks in logical tuple
    /// order.
    ///
    /// # Errors
    ///
    /// Returns [`OverflowError`] if any page cannot be read from the page store
    /// or decoded as an [`OverflowPage`].
    pub(in crate::heap) fn read_overflow(
        &self,
        txn: TransactionId,
        overflow_ptr: PageDescriptor,
    ) -> Result<Vec<u8>, OverflowError> {
        let mut result = Vec::new();
        let mut page_no = overflow_ptr.page_no;

        loop {
            let page_id = PageId::new(self.file_id, page_no);
            let (chunk, next) = self
                .buffer_pool
                .shared_read(
                    txn,
                    page_id,
                    |raw| -> Result<_, OverflowError> {
                        OverflowPage::from_page_bytes(&raw)
                            .map(Some)
                            .map_err(|e| StorageError::ParseError(e.to_string()).into())
                    },
                    |page| Ok((page.payload().to_vec(), page.next_page())),
                )?
                .expect("overflow page must be parseable; decode never returns None");

            result.extend_from_slice(&chunk);
            match next {
                None => break,
                Some(next_no) => page_no = next_no,
            }
        }

        Ok(result)
    }

    /// Stores `data` across one or more newly allocated overflow pages.
    ///
    /// The returned [`PageDescriptor`] points at the first page in the chain,
    /// which is the pointer stored by the owning tuple slot. Pages are created
    /// from the final chunk backward so each newly written page already knows
    /// the page number of its logical successor.
    ///
    /// # Errors
    ///
    /// Returns [`OverflowError`] if allocating or writing any overflow page
    /// fails.
    pub(in crate::heap) fn write_overflow(
        &self,
        txn: TransactionId,
        data: &[u8],
    ) -> Result<PageDescriptor, OverflowError> {
        let chunks: Vec<&[u8]> = data.chunks(OVERFLOW_PAYLOAD_SIZE).collect();
        let file_id = self.file_id;
        let mut next_page: Option<PageNumber> = None;
        let mut first_page_no = PageNumber::new(0);

        for chunk in chunks.iter().rev() {
            let page_no = self.num_pages.fetch_add(1, Ordering::AcqRel);
            let page_no = PageNumber::new(page_no);
            let page_id = PageId::new(file_id, page_no);

            self.buffer_pool.exclusive_create(txn, page_id, || {
                let payload = chunk.to_vec();
                OverflowPage::overflow_new(next_page, payload).map_err(OverflowError::from)
            })?;

            next_page = Some(page_no);
            first_page_no = page_no;
        }

        Ok(PageDescriptor::new(file_id, first_page_no))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::{FileId, storage::page_crc_valid, wal::writer::Wal};

    fn make_overflow_file(num_pages: u32) -> (OverflowFile, Arc<Wal>, tempfile::TempDir) {
        crate::test_utils::init_tracing();
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("wal.log"), 0).unwrap());
        let store = Arc::new(crate::buffer_pool::page_store::PageStore::new(
            64,
            Arc::clone(&wal),
        ));

        let file_id = FileId::new(1);
        let path = dir.path().join("overflow.dat");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(PAGE_SIZE as u64 * u64::from(num_pages.max(1)))
            .unwrap();
        store.register_file(file_id, &path).unwrap();

        let of = OverflowFile::new(file_id, store, 0);
        (of, wal, dir)
    }

    fn txn(n: u64) -> TransactionId {
        TransactionId::new(n)
    }

    #[test]
    fn write_and_read_single_chunk() {
        let (of, wal, _dir) = make_overflow_file(1);
        let t = txn(1);
        wal.log_begin(t).unwrap();
        let data = b"hello overflow".to_vec();
        let ptr = of.write_overflow(t, &data).unwrap();
        let got = of.read_overflow(t, ptr).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn write_and_read_exact_boundary() {
        // Exactly OVERFLOW_PAYLOAD_SIZE bytes must fit in one page with no spill.
        let (of, wal, _dir) = make_overflow_file(1);
        let t = txn(1);
        wal.log_begin(t).unwrap();
        let data = vec![0x5Au8; OVERFLOW_PAYLOAD_SIZE];
        let ptr = of.write_overflow(t, &data).unwrap();
        let got = of.read_overflow(t, ptr).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn write_and_read_one_byte_over_boundary() {
        // One extra byte forces a second overflow page.
        let (of, wal, _dir) = make_overflow_file(2);
        let t = txn(1);
        wal.log_begin(t).unwrap();
        let data = vec![0xBBu8; OVERFLOW_PAYLOAD_SIZE + 1];
        let ptr = of.write_overflow(t, &data).unwrap();
        let got = of.read_overflow(t, ptr).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn write_and_read_multi_chunk_order_preserved() {
        // Three full pages worth of data; verify the reconstruction order is correct.
        let (of, wal, _dir) = make_overflow_file(3);
        let t = txn(1);
        wal.log_begin(t).unwrap();
        let mut data = Vec::with_capacity(OVERFLOW_PAYLOAD_SIZE * 3);
        for i in 0..3u8 {
            data.extend(vec![i; OVERFLOW_PAYLOAD_SIZE]);
        }
        let ptr = of.write_overflow(t, &data).unwrap();
        let got = of.read_overflow(t, ptr).unwrap();
        assert_eq!(got, data);
    }

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
