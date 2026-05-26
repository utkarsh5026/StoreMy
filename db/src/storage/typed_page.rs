//! A generic typed page that enforces the universal on-disk header on every
//! page type in the database.
//!
//! Every page — heap, B-tree node, hash bucket, overflow — must begin with the
//! same fixed header so the buffer pool and the ARIES recovery pass can always
//! locate the `PageKind`, CRC32 checksum, and `page_lsn` at known byte offsets
//! regardless of what the rest of the page contains.
//!
//! `TypedPage<H, B>` encodes that invariant in the type system:
//!
//! ```text
//! offset 0       : PageKind (u8)
//! offset 1..5    : CRC32 (u32 LE)  — computed over the whole page, this slot zeroed
//! offset 5..13   : page_lsn (u64 LE)
//! offset 13..    : H bytes (via Encode/Decode), then B bytes (via Encode/Decode)
//! ```
//!
//! Both `H` and `B` are encoded by streaming into a [`Cursor`] over the
//! remaining page buffer. There is no need for special slice-based traits: the
//! CRC slot is handled by writing it last after the cursor is dropped, and
//! both `H` and `B` use the same [`Encode`] / [`Decode`] traits that every
//! other on-disk structure in `StoreMy` uses.
//!
//! The `before_image` field — an in-memory snapshot of the encoded page bytes
//! taken before the first write of a transaction — never reaches disk. It lets
//! the WAL writer emit a before-image for undo without re-reading from the file.

use std::io::Cursor;

use super::{
    PAGE_SIZE, Page, PageKind, StorageError, compute_page_crc,
    envelope::{PAGE_LSN_END, PAGE_LSN_OFFSET},
    page_crc_valid, stamp_page_crc,
};
use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::Lsn,
};

const KIND_OFFSET: usize = 0;

/// Byte size of the universal header present at the start of every page.
///
/// ```text
/// kind (1) + crc32 (4) + page_lsn (8) = 13 bytes
/// ```
///
/// Page-type-specific header fields ([`TypedPage::header`]) begin immediately
/// after this offset; the body ([`TypedPage::body`]) follows the header.
pub const UNIVERSAL_HEADER_SIZE: usize = PAGE_LSN_END; // 13

/// A page buffer structured around the universal on-disk header plus
/// page-type-specific header and body sections.
///
/// See the [module-level documentation](self) for the on-disk layout and the
/// role of each field.
#[derive(Debug)]
pub struct TypedPage<H, B> {
    /// Identifies what kind of page this buffer holds.
    pub kind: PageKind,

    /// LSN of the last WAL record applied to this page.
    ///
    /// `Lsn::INVALID` (0) means the page has never been modified by a
    /// transaction. The ARIES redo pass skips any WAL record whose LSN is
    /// ≤ this value, making redo idempotent.
    pub page_lsn: Lsn,

    /// Page-type-specific fixed metadata.
    ///
    /// Examples: `(num_slots, tuple_start)` for heap pages;
    /// `(bucket_num, count, overflow_ptr)` for hash buckets;
    /// `(parent, count, prev, next)` for B-tree leaf nodes.
    pub header: H,

    /// Variable-length page body.
    ///
    /// Examples: slot array + tuple data for heap; index entries for hash /
    /// B-tree; raw bytes for overflow pages.
    pub body: B,

    /// Snapshot of the encoded page bytes taken before the first write of the
    /// current transaction. `None` until [`Page::set_before_image`] is called.
    ///
    /// Never flushed to disk — used only by the WAL writer to produce
    /// before-image log records for undo.
    before_image: Option<[u8; PAGE_SIZE]>,
}

impl<H, B> TypedPage<H, B> {
    /// Creates a new `TypedPage` with no before-image set.
    ///
    /// Use this when constructing a brand-new page that has never been on disk
    /// (e.g. a freshly allocated page), rather than parsing raw bytes read from
    /// a file. Call [`TypedPage::from_page_bytes`] to reconstruct an existing
    /// page.
    pub fn new(kind: PageKind, page_lsn: Lsn, header: H, body: B) -> Self {
        Self {
            kind,
            page_lsn,
            header,
            body,
            before_image: None,
        }
    }
}

impl<H, B> TypedPage<H, B>
where
    H: Encode,
    B: Encode,
{
    /// Serialises the page into a `PAGE_SIZE`-byte array ready to be written
    /// to disk.
    ///
    /// # Encoding order
    ///
    /// 1. Write the `PageKind` byte at offset 0.
    /// 2. Leave bytes 1..5 as zero — the CRC slot is patched in at step 4.
    /// 3. Write `page_lsn` at bytes 5..13. Then open a cursor over the rest of the buffer and
    ///    stream `H` then `B` into it sequentially.
    /// 4. Drop the cursor and stamp the CRC — **must** be the last step, after every other byte in
    ///    the buffer is final.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `H` or `B` encoding fails.
    pub fn to_page_bytes(&self) -> Result<[u8; PAGE_SIZE], CodecError> {
        let mut buf = [0u8; PAGE_SIZE];

        buf[KIND_OFFSET] = self.kind as u8;
        buf[PAGE_LSN_OFFSET..UNIVERSAL_HEADER_SIZE].copy_from_slice(&self.page_lsn.0.to_le_bytes());
        {
            let mut cursor = Cursor::new(&mut buf[UNIVERSAL_HEADER_SIZE..]);
            self.header.encode(&mut cursor)?;
            self.body.encode(&mut cursor)?;
        }
        stamp_page_crc(&mut buf);

        Ok(buf)
    }
}

impl<H, B> TypedPage<H, B>
where
    H: Decode,
    B: Decode,
{
    /// Parses a `TypedPage` from a raw `PAGE_SIZE`-byte buffer read from disk.
    ///
    /// # Steps
    ///
    /// 1. Reject all-zero buffers — a blank page was never stamped; callers that need an empty page
    ///    should use [`TypedPage::new`] directly.
    /// 2. Verify the stored CRC at bytes 1..5 against the recomputed value.
    /// 3. Parse the `PageKind` byte and `page_lsn`.
    /// 4. Open a cursor at byte 13 and decode `H`, then `B` in sequence. The cursor advances
    ///    naturally through both; no manual byte counting is needed.
    ///
    /// The returned page's `before_image` is always `None`. Call
    /// [`Page::set_before_image`] before the first mutation within a
    /// transaction.
    ///
    /// # Errors
    ///
    /// - [`StorageError::UnknownPageKind`]`(0x00)` for an all-zero buffer.
    /// - [`StorageError::ChecksumMismatch`] if the stored CRC does not match.
    /// - [`StorageError::UnknownPageKind`] for an unrecognised kind byte.
    /// - [`StorageError::ParseError`] if `H` or `B` decoding fails.
    pub fn from_page_bytes(bytes: &[u8; PAGE_SIZE]) -> Result<Self, StorageError> {
        // Blank pages were never stamped — reject so callers use TypedPage::new.
        if bytes.iter().all(|&b| b == 0) {
            return Err(StorageError::UnknownPageKind(0x00));
        }

        if !page_crc_valid(bytes) {
            let stored = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            let computed = compute_page_crc(bytes);
            return Err(StorageError::ChecksumMismatch { stored, computed });
        }

        let kind = PageKind::try_from(bytes[KIND_OFFSET])?;

        let page_lsn = Lsn(u64::from_le_bytes(
            bytes[PAGE_LSN_OFFSET..UNIVERSAL_HEADER_SIZE]
                .try_into()
                .unwrap(), // exactly 8 bytes by construction
        ));

        let mut cursor = Cursor::new(&bytes[UNIVERSAL_HEADER_SIZE..]);
        let header = H::decode(&mut cursor).map_err(|e| StorageError::ParseError(e.to_string()))?;
        let body = B::decode(&mut cursor).map_err(|e| StorageError::ParseError(e.to_string()))?;

        Ok(Self {
            kind,
            page_lsn,
            header,
            body,
            before_image: None,
        })
    }
}

// ── Encode / Decode for TypedPage itself ─────────────────────────────────────

/// Converts the subset of [`StorageError`] variants that [`from_page_bytes`]
/// can return into a [`CodecError`], preserving as much information as
/// possible.
///
/// `UnknownPageKind` maps exactly to `UnknownDiscriminant` (both carry the
/// offending byte). Everything else becomes an `Io` error so the caller gets
/// a descriptive message without needing to know about `StorageError`.
fn storage_to_codec(e: StorageError) -> CodecError {
    match e {
        StorageError::UnknownPageKind(b) => CodecError::UnknownDiscriminant(b),
        other => CodecError::Io(std::io::Error::other(other.to_string())),
    }
}

/// Writes the page as exactly [`PAGE_SIZE`] bytes into `writer`.
///
/// The bytes are produced by [`TypedPage::to_page_bytes`], which stamps the
/// CRC and fills the universal header before writing. The output is always
/// exactly `PAGE_SIZE` bytes regardless of how much of the body slice is
/// actually used.
impl<H, B> Encode for TypedPage<H, B>
where
    H: Encode,
    B: Encode,
{
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let bytes = self.to_page_bytes()?;
        writer.write_all(&bytes)?;
        Ok(())
    }
}

/// Reads exactly [`PAGE_SIZE`] bytes from `reader` and reconstructs a
/// `TypedPage` via [`TypedPage::from_page_bytes`].
///
/// The CRC is verified and the kind byte is parsed as part of decoding.
/// Returns [`CodecError::UnknownDiscriminant`] for an unrecognised kind byte
/// and [`CodecError::Io`] for a checksum mismatch or a corrupt `H`/`B`
/// payload.
impl<H, B> Decode for TypedPage<H, B>
where
    H: Decode,
    B: Decode,
{
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; PAGE_SIZE];
        reader.read_exact(&mut buf)?;
        Self::from_page_bytes(&buf).map_err(storage_to_codec)
    }
}

impl<H, B> Page for TypedPage<H, B>
where
    H: Encode + Decode + Send + Sync,
    B: Encode + Decode + Send + Sync,
{
    fn page_data(&self) -> [u8; PAGE_SIZE] {
        self.to_page_bytes()
            .expect("TypedPage encoding must succeed for a valid page")
    }

    fn before_image(&self) -> Option<[u8; PAGE_SIZE]> {
        self.before_image
    }

    fn set_before_image(&mut self) {
        self.before_image = Some(self.page_data());
    }

    fn page_lsn(&self) -> Lsn {
        self.page_lsn
    }

    fn set_page_lsn(&mut self, lsn: Lsn) {
        self.page_lsn = lsn;
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;
    use crate::codec::CodecError;

    /// A tiny two-field header: a u32 tag and a u8 flags byte.
    #[derive(Debug, PartialEq, Eq)]
    struct TestHeader {
        tag: u32,
        flags: u8,
    }

    impl Encode for TestHeader {
        fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
            w.write_all(&self.tag.to_le_bytes())?;
            w.write_all(&[self.flags])?;
            Ok(())
        }
    }

    impl Decode for TestHeader {
        fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
            let mut tag = [0u8; 4];
            r.read_exact(&mut tag)?;
            let mut flags = [0u8; 1];
            r.read_exact(&mut flags)?;
            Ok(Self {
                tag: u32::from_le_bytes(tag),
                flags: flags[0],
            })
        }
    }

    /// A length-prefixed byte payload body — self-describing, no header
    /// context needed.
    #[derive(Debug, PartialEq, Eq)]
    struct TestBody {
        payload: Vec<u8>,
    }

    impl Encode for TestBody {
        fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
            let payload_len =
                u32::try_from(self.payload.len()).map_err(|_| CodecError::NumericDoesNotFit {
                    value: self.payload.len() as u64,
                    target: "u32",
                })?;
            w.write_all(&payload_len.to_le_bytes())?;
            w.write_all(&self.payload)?;
            Ok(())
        }
    }

    impl Decode for TestBody {
        fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
            let mut len_buf = [0u8; 4];
            r.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            r.read_exact(&mut payload)?;
            Ok(Self { payload })
        }
    }

    fn make_page(tag: u32, payload: &[u8]) -> TypedPage<TestHeader, TestBody> {
        TypedPage::new(
            PageKind::Heap,
            Lsn(42),
            TestHeader { tag, flags: 0xAB },
            TestBody {
                payload: payload.to_vec(),
            },
        )
    }

    #[test]
    fn roundtrip_preserves_all_fields() {
        let page = make_page(0xDEAD_BEEF, b"hello world");
        let bytes = page.to_page_bytes().unwrap();

        let decoded: TypedPage<TestHeader, TestBody> = TypedPage::from_page_bytes(&bytes).unwrap();

        assert_eq!(decoded.kind, PageKind::Heap);
        assert_eq!(decoded.page_lsn, Lsn(42));
        assert_eq!(decoded.header.tag, 0xDEAD_BEEF);
        assert_eq!(decoded.header.flags, 0xAB);
        assert_eq!(decoded.body.payload, b"hello world");
    }

    #[test]
    fn universal_header_bytes_are_correct() {
        let page = make_page(1, b"x");
        let bytes = page.to_page_bytes().unwrap();

        assert_eq!(bytes[0], PageKind::Heap as u8);
        let lsn = u64::from_le_bytes(
            bytes[PAGE_LSN_OFFSET..UNIVERSAL_HEADER_SIZE]
                .try_into()
                .unwrap(),
        );
        assert_eq!(lsn, 42);
    }

    #[test]
    fn h_and_b_are_written_contiguously_after_universal_header() {
        let page = make_page(0x0102_0304, b"body");
        let bytes = page.to_page_bytes().unwrap();

        // H is TestHeader: 4-byte tag + 1-byte flags = 5 bytes starting at 13.
        let tag = u32::from_le_bytes(bytes[13..17].try_into().unwrap());
        assert_eq!(tag, 0x0102_0304);
        assert_eq!(bytes[17], 0xAB);

        // B is TestBody: 4-byte length prefix + payload bytes, starting at 18.
        let len = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
        assert_eq!(len as usize, b"body".len());
        assert_eq!(&bytes[22..22 + len as usize], b"body");
    }

    #[test]
    fn to_page_bytes_stamps_valid_crc() {
        let bytes = make_page(1, b"data").to_page_bytes().unwrap();
        assert!(page_crc_valid(&bytes));
    }

    #[test]
    fn flipped_body_byte_fails_crc() {
        let mut bytes = make_page(2, b"abc").to_page_bytes().unwrap();
        bytes[PAGE_SIZE - 1] ^= 0xFF;
        assert!(TypedPage::<TestHeader, TestBody>::from_page_bytes(&bytes).is_err());
    }

    #[test]
    fn flipped_kind_byte_fails_crc() {
        let mut bytes = make_page(3, b"x").to_page_bytes().unwrap();
        bytes[0] = PageKind::HashBucket as u8;
        assert!(TypedPage::<TestHeader, TestBody>::from_page_bytes(&bytes).is_err());
    }

    #[test]
    fn blank_page_rejected_with_unknown_kind() {
        let blank = [0u8; PAGE_SIZE];
        match TypedPage::<TestHeader, TestBody>::from_page_bytes(&blank).unwrap_err() {
            StorageError::UnknownPageKind(0x00) => {}
            other => panic!("expected UnknownPageKind(0x00), got {other:?}"),
        }
    }

    #[test]
    fn before_image_is_none_on_construction() {
        assert!(make_page(1, b"z").before_image().is_none());
    }

    #[test]
    fn set_before_image_captures_current_state() {
        let mut page = make_page(7, b"snap");
        page.set_before_image();
        assert_eq!(page.before_image().unwrap(), page.to_page_bytes().unwrap());
    }

    #[test]
    fn snapshot_is_frozen_after_lsn_update() {
        let mut page = make_page(8, b"lsn");
        page.set_before_image(); // snapshot at Lsn(42)
        let snap_lsn = u64::from_le_bytes(
            page.before_image().unwrap()[PAGE_LSN_OFFSET..UNIVERSAL_HEADER_SIZE]
                .try_into()
                .unwrap(),
        );

        page.set_page_lsn(Lsn(999));
        assert_eq!(page.page_lsn(), Lsn(999));
        // Snapshot still shows the old LSN.
        assert_eq!(snap_lsn, 42);
    }

    #[test]
    fn encode_writes_exactly_page_size_bytes() {
        let page = make_page(1, b"hello");
        let mut buf = Vec::new();
        page.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), PAGE_SIZE);
    }

    #[test]
    fn encode_then_decode_roundtrip() {
        let page = make_page(0xCAFE_BABE, b"roundtrip");

        // Encode into a Vec, then decode back out of a Cursor over that Vec.
        let mut buf = Vec::new();
        page.encode(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded: TypedPage<TestHeader, TestBody> = TypedPage::decode(&mut cursor).unwrap();

        assert_eq!(decoded.kind, PageKind::Heap);
        assert_eq!(decoded.page_lsn, Lsn(42));
        assert_eq!(decoded.header.tag, 0xCAFE_BABE);
        assert_eq!(decoded.body.payload, b"roundtrip");
    }

    #[test]
    fn decode_multiple_pages_from_one_reader() {
        // Two pages written back-to-back — decode reads PAGE_SIZE each time
        // and leaves the cursor positioned correctly for the next.
        let p1 = make_page(1, b"first");
        let p2 = make_page(2, b"second");

        let mut buf = Vec::new();
        p1.encode(&mut buf).unwrap();
        p2.encode(&mut buf).unwrap();

        assert_eq!(buf.len(), PAGE_SIZE * 2);

        let mut cursor = Cursor::new(buf);
        let d1: TypedPage<TestHeader, TestBody> = TypedPage::decode(&mut cursor).unwrap();
        let d2: TypedPage<TestHeader, TestBody> = TypedPage::decode(&mut cursor).unwrap();

        assert_eq!(d1.header.tag, 1);
        assert_eq!(d1.body.payload, b"first");
        assert_eq!(d2.header.tag, 2);
        assert_eq!(d2.body.payload, b"second");
    }

    #[test]
    fn decode_unknown_kind_returns_unknown_discriminant() {
        // Write a page, corrupt the kind byte, re-stamp CRC so it passes
        // checksum but has an unrecognised kind.
        let mut bytes = make_page(1, b"x").to_page_bytes().unwrap();
        bytes[0] = 0xFF; // not a valid PageKind
        stamp_page_crc(&mut bytes); // fix CRC so it passes checksum check

        let mut cursor = Cursor::new(bytes.as_ref());
        match TypedPage::<TestHeader, TestBody>::decode(&mut cursor).unwrap_err() {
            CodecError::UnknownDiscriminant(0xFF) => {}
            other => panic!("expected UnknownDiscriminant(0xFF), got {other:?}"),
        }
    }
}
