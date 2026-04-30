//! Write-ahead log record types and encoding.
//!
//! This module defines the on-disk representation of WAL (Write-Ahead Log)
//! records.  Every database mutation is first written here before being applied
//! to data pages, which guarantees durability and enables crash recovery.
//!
//! A WAL record has two parts:
//!
//! 1. A fixed-size [`LogRecordHeader`] that contains the LSN, transaction ID, record type,
//!    timestamp, and a CRC32 checksum of the body.
//! 2. A variable-size [`LogRecordBody`] whose layout depends on the record type.
//!
//! [`LogRecord`] bundles both parts and provides [`Encode`]/[`Decode`] impls
//! for writing to and reading from an `io::Write`/`io::Read` stream.  Building
//! a complete record is done through [`LogRecord::new`], which automatically
//! fills in `body_len` and `checksum`.

use std::{
    io::{Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::{Lsn, PageId, TransactionId},
};

/// Identifies which operation a [`LogRecord`] represents.
///
/// The discriminant is stored as a single `u8` on disk (see [`TryFrom<u8>`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    /// Marks the beginning of a transaction.
    Begin = 0,
    /// Marks the successful end of a transaction (all changes are durable).
    Commit = 1,
    /// Marks an aborted transaction (all changes must be rolled back).
    Abort = 2,
    /// An in-place modification of an existing page slot.
    Update = 3,
    /// A new row was inserted into a page.
    Insert = 4,
    /// A row was deleted from a page.
    Delete = 5,
    /// Signals the start of a fuzzy checkpoint sweep.
    CheckpointBegin = 6,
    /// Signals the end of a fuzzy checkpoint sweep.
    CheckpointEnd = 7,
    /// Compensation Log Record — written during undo to record a compensating action.
    Clr = 8,
}

/// Converts a raw `u8` discriminant back into a [`LogRecordType`].
///
/// # Errors
///
/// Returns the original byte value as `Err` if it does not correspond to any
/// known variant.
impl TryFrom<u8> for LogRecordType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Begin),
            1 => Ok(Self::Commit),
            2 => Ok(Self::Abort),
            3 => Ok(Self::Update),
            4 => Ok(Self::Insert),
            5 => Ok(Self::Delete),
            6 => Ok(Self::CheckpointBegin),
            7 => Ok(Self::CheckpointEnd),
            8 => Ok(Self::Clr),
            other => Err(other),
        }
    }
}

/// Fixed-size prefix written before every log record body.
///
/// Because the header is fixed-size the WAL reader can always read exactly
/// [`LogRecordHeader::SIZE`] bytes, parse the header, and then read exactly
/// `body_len` more bytes for the body — no guessing, no scanning.
///
/// `checksum` covers only the body bytes. This avoids the circularity of
/// checksumming a field that is itself part of the checksum region.
pub struct LogRecordHeader {
    /// The log sequence number assigned to this record.
    pub lsn: Lsn,
    /// LSN of the previous record written by the same transaction, or
    /// [`Lsn::INVALID`] for the first record of a transaction.
    pub prev_lsn: Lsn,
    /// The transaction that produced this record.
    pub tid: TransactionId,
    /// Which operation this record represents.
    pub record_type: LogRecordType,
    /// Wall-clock time when this record was created, stored at second precision.
    pub timestamp: SystemTime,
    /// Number of bytes in the body that follows this header on disk.
    pub body_len: u32,
    /// CRC32 checksum computed over the encoded body bytes.
    pub checksum: u32,
}

impl LogRecordHeader {
    /// On-disk size of a serialized header, in bytes.
    ///
    /// Layout:
    /// ```text
    /// record_type : u8       =  1
    /// lsn         : u64 LE   =  8
    /// prev_lsn    : u64 LE   =  8
    /// tid         : u64 LE   =  8
    /// timestamp   : u64 LE   =  8
    /// body_len    : u32 LE   =  4
    /// checksum    : u32 LE   =  4
    ///                          ──
    ///                          41
    /// ```
    pub const SIZE: usize = 41;
}

/// Writes the header to `writer` in the canonical on-disk layout.
///
/// See [`LogRecordHeader::SIZE`] for the exact byte layout.
///
/// # Errors
///
/// Propagates any I/O error from `writer`.
impl Encode for LogRecordHeader {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u8(self.record_type as u8)?;
        self.lsn.encode(writer)?;
        self.prev_lsn.encode(writer)?;
        self.tid.encode(writer)?;
        let secs = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        writer.write_u64::<LittleEndian>(secs)?;
        writer.write_u32::<LittleEndian>(self.body_len)?;
        writer.write_u32::<LittleEndian>(self.checksum)?;
        Ok(())
    }
}

/// Reads a header from `reader`, expecting the canonical on-disk layout.
///
/// # Errors
///
/// Returns [`CodecError::UnknownDiscriminant`] if the `record_type` byte does
/// not match any [`LogRecordType`] variant.  Propagates any other I/O error.
impl Decode for LogRecordHeader {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let record_type =
            LogRecordType::try_from(reader.read_u8()?).map_err(CodecError::UnknownDiscriminant)?;
        let lsn = Lsn::decode(reader)?;
        let prev_lsn = Lsn::decode(reader)?;
        let tid = TransactionId::decode(reader)?;
        let secs = reader.read_u64::<LittleEndian>()?;
        let timestamp = UNIX_EPOCH + std::time::Duration::from_secs(secs);
        let body_len = reader.read_u32::<LittleEndian>()?;
        let checksum = reader.read_u32::<LittleEndian>()?;
        Ok(Self {
            lsn,
            prev_lsn,
            tid,
            record_type,
            timestamp,
            body_len,
            checksum,
        })
    }
}

/// The variable-length, type-specific payload of a log record.
///
/// Each variant holds exactly the fields that record type requires — no
/// `Option` fields that are only meaningful for some types.
pub enum LogRecordBody {
    /// Transaction begin — no payload.
    Begin,
    /// Transaction commit — no payload.
    Commit,
    /// Transaction abort — no payload.
    Abort,
    /// An existing page slot was overwritten.
    Update {
        /// The page that was modified.
        page_id: PageId,
        /// The page image before the update (used for undo).
        before: Vec<u8>,
        /// The page image after the update (used for redo).
        after: Vec<u8>,
    },
    /// A new row was inserted.
    Insert {
        /// The page that received the new row.
        page_id: PageId,
        /// The page image after the insert (used for redo).
        after: Vec<u8>,
    },
    /// A row was deleted.
    Delete {
        /// The page from which the row was removed.
        page_id: PageId,
        /// The page image before the delete (used for undo).
        before: Vec<u8>,
    },
    /// Compensation Log Record written during undo of an `Update` or `Insert`.
    Clr {
        /// The page that was compensated.
        page_id: PageId,
        /// The page image after applying the compensation (used for redo).
        after: Vec<u8>,
        /// LSN of the next record to undo for this transaction, skipping the
        /// record that this CLR compensates.
        undo_next_lsn: Lsn,
    },
    /// Fuzzy checkpoint start — no payload.
    CheckpointBegin,
    /// Fuzzy checkpoint end — no payload.
    CheckpointEnd,
}

impl LogRecordBody {
    /// Returns the [`LogRecordType`] discriminant that corresponds to this body variant.
    ///
    /// Used by [`LogRecord::new`] to fill in the header's `record_type` field
    /// without requiring the caller to specify it separately.
    fn record_type(&self) -> LogRecordType {
        match self {
            Self::Begin => LogRecordType::Begin,
            Self::Commit => LogRecordType::Commit,
            Self::Abort => LogRecordType::Abort,
            Self::Update { .. } => LogRecordType::Update,
            Self::Insert { .. } => LogRecordType::Insert,
            Self::Delete { .. } => LogRecordType::Delete,
            Self::Clr { .. } => LogRecordType::Clr,
            Self::CheckpointBegin => LogRecordType::CheckpointBegin,
            Self::CheckpointEnd => LogRecordType::CheckpointEnd,
        }
    }
}

/// Writes the body payload to `writer`.
///
/// Variants with no payload (`Begin`, `Commit`, `Abort`, `CheckpointBegin`,
/// `CheckpointEnd`) write zero bytes.  All other variants write a
/// [`PageId`] followed by one or two length-prefixed byte images.
///
/// # Errors
///
/// Propagates any I/O error from `writer`.
impl Encode for LogRecordBody {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            Self::Begin
            | Self::Commit
            | Self::Abort
            | Self::CheckpointBegin
            | Self::CheckpointEnd => {}

            Self::Update {
                page_id,
                before,
                after,
            } => {
                page_id.encode(writer)?;
                encode_image(writer, Some(before))?;
                encode_image(writer, Some(after))?;
            }

            Self::Insert { page_id, after } => {
                page_id.encode(writer)?;
                encode_image(writer, Some(after))?;
            }

            Self::Delete { page_id, before } => {
                page_id.encode(writer)?;
                encode_image(writer, Some(before))?;
            }

            Self::Clr {
                page_id,
                after,
                undo_next_lsn,
            } => {
                page_id.encode(writer)?;
                encode_image(writer, Some(after))?;
                undo_next_lsn.encode(writer)?;
            }
        }
        Ok(())
    }
}

impl LogRecordBody {
    /// Reads a body from `reader` using `record_type` to select the right layout.
    ///
    /// This is called by [`LogRecord`]'s [`Decode`] impl after the header has
    /// already been read, so `record_type` is known before any body bytes are
    /// consumed.
    ///
    /// # Errors
    ///
    /// Propagates any I/O error from `reader`.
    fn decode_for_type<R: Read>(
        record_type: LogRecordType,
        reader: &mut R,
    ) -> Result<Self, CodecError> {
        match record_type {
            LogRecordType::Begin => Ok(Self::Begin),
            LogRecordType::Commit => Ok(Self::Commit),
            LogRecordType::Abort => Ok(Self::Abort),
            LogRecordType::CheckpointBegin => Ok(Self::CheckpointBegin),
            LogRecordType::CheckpointEnd => Ok(Self::CheckpointEnd),

            LogRecordType::Update => Ok(Self::Update {
                page_id: PageId::decode(reader)?,
                before: decode_image(reader)?.unwrap_or_default(),
                after: decode_image(reader)?.unwrap_or_default(),
            }),

            LogRecordType::Insert => Ok(Self::Insert {
                page_id: PageId::decode(reader)?,
                after: decode_image(reader)?.unwrap_or_default(),
            }),

            LogRecordType::Delete => Ok(Self::Delete {
                page_id: PageId::decode(reader)?,
                before: decode_image(reader)?.unwrap_or_default(),
            }),

            LogRecordType::Clr => Ok(Self::Clr {
                page_id: PageId::decode(reader)?,
                after: decode_image(reader)?.unwrap_or_default(),
                undo_next_lsn: Lsn::decode(reader)?,
            }),
        }
    }
}

// ── LogRecord ────────────────────────────────────────────────────────────────

/// A complete WAL record: a fixed-size header followed by a variable-size body.
///
/// # On-disk layout
///
/// ```text
/// ┌─────────────────────────────────┐
/// │  LogRecordHeader  (41 bytes)    │  ← fixed, always present
/// ├─────────────────────────────────┤
/// │  body bytes  (header.body_len)  │  ← variable, type-specific
/// └─────────────────────────────────┘
/// ```
///
/// The header's `checksum` field is a CRC32 computed over the body bytes only.
/// Framing (writing the header then body, verifying the checksum) is the
/// responsibility of `WalWriter`/`WalReader`, not of this type's codec impls.
pub struct LogRecord {
    /// The fixed-size prefix describing this record.
    pub header: LogRecordHeader,
    /// The type-specific payload.
    pub body: LogRecordBody,
}

impl LogRecord {
    /// Builds a complete [`LogRecord`], filling in `body_len` and `checksum` automatically.
    ///
    /// The body is encoded into a temporary buffer so that its length and CRC32
    /// can be computed before the header is constructed.  The caller only needs
    /// to supply the LSN, previous LSN, transaction ID, timestamp, and body.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if encoding the body fails.
    pub fn new(
        lsn: Lsn,
        prev_lsn: Lsn,
        tid: TransactionId,
        timestamp: SystemTime,
        body: LogRecordBody,
    ) -> Result<Self, CodecError> {
        let body_bytes = {
            let mut buf = Vec::new();
            body.encode(&mut buf)?;
            buf
        };
        let header = LogRecordHeader {
            lsn,
            prev_lsn,
            tid,
            record_type: body.record_type(),
            timestamp,
            body_len: encoded_len_u32(body_bytes.len())?,
            checksum: crc32fast::hash(&body_bytes),
        };
        Ok(Self { header, body })
    }
}

/// Writes the full record (header then body) to `writer`.
///
/// # Errors
///
/// Propagates any I/O error from `writer`.
impl Encode for LogRecord {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.header.encode(writer)?;
        self.body.encode(writer)?;
        Ok(())
    }
}

/// Reads a full record (header then body) from `reader`.
///
/// # Errors
///
/// Returns [`CodecError::UnknownDiscriminant`] if the header's `record_type`
/// byte is unrecognized.  Propagates any other I/O error.
impl Decode for LogRecord {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let header = LogRecordHeader::decode(reader)?;
        let body = LogRecordBody::decode_for_type(header.record_type, reader)?;
        Ok(Self { header, body })
    }
}

/// WAL and framing use `u32` length prefixes; returns an error if `n` does not fit.
fn encoded_len_u32(n: usize) -> Result<u32, CodecError> {
    u32::try_from(n).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "encoded length exceeds u32::MAX",
        )
        .into()
    })
}

/// Writes a length-prefixed byte image to `writer`.
///
/// A `u32` little-endian length is written first, followed by the image bytes.
/// If `image` is `None`, a zero-length prefix is written and no payload bytes follow.
fn encode_image<W: Write>(writer: &mut W, image: Option<&[u8]>) -> Result<(), CodecError> {
    match image {
        Some(img) => {
            writer.write_u32::<LittleEndian>(encoded_len_u32(img.len())?)?;
            writer.write_all(img)?;
        }
        None => writer.write_u32::<LittleEndian>(0)?,
    }
    Ok(())
}

/// Reads a length-prefixed byte image from `reader`.
///
/// Returns `None` when the length prefix is zero (matching what [`encode_image`]
/// writes for `None`), or `Some(bytes)` otherwise.
fn decode_image<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, CodecError> {
    let len = reader.read_u32::<LittleEndian>()? as usize;
    if len == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{
        codec::{Decode, Encode},
        primitives::{FileId, PageNumber},
    };

    fn make_page_id() -> PageId {
        PageId::new(FileId::new(1), PageNumber::new(42))
    }

    fn roundtrip(record: &LogRecord) -> LogRecord {
        let mut buf = Vec::new();
        record.encode(&mut buf).expect("encode failed");
        LogRecord::decode(&mut Cursor::new(buf)).expect("decode failed")
    }

    #[test]
    fn record_type_roundtrips_all_variants() {
        let variants = [
            LogRecordType::Begin,
            LogRecordType::Commit,
            LogRecordType::Abort,
            LogRecordType::Update,
            LogRecordType::Insert,
            LogRecordType::Delete,
            LogRecordType::CheckpointBegin,
            LogRecordType::CheckpointEnd,
            LogRecordType::Clr,
        ];
        for v in variants {
            assert_eq!(LogRecordType::try_from(v as u8).unwrap(), v);
        }
    }

    #[test]
    fn record_type_rejects_unknown_discriminant() {
        assert_eq!(LogRecordType::try_from(9u8), Err(9u8));
        assert_eq!(LogRecordType::try_from(255u8), Err(255u8));
    }

    #[test]
    fn header_size_constant_matches_encoded_size() {
        let header = LogRecordHeader {
            lsn: Lsn(1),
            prev_lsn: Lsn::INVALID,
            tid: TransactionId::new(1),
            record_type: LogRecordType::Begin,
            timestamp: UNIX_EPOCH,
            body_len: 0,
            checksum: 0,
        };
        let mut buf = Vec::new();
        header.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), LogRecordHeader::SIZE);
    }

    #[test]
    fn header_roundtrip() {
        let header = LogRecordHeader {
            lsn: Lsn(42),
            prev_lsn: Lsn(10),
            tid: TransactionId::new(7),
            record_type: LogRecordType::Update,
            timestamp: UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
            body_len: 128,
            checksum: 0xDEAD_BEEF,
        };
        let mut buf = Vec::new();
        header.encode(&mut buf).unwrap();
        let decoded = LogRecordHeader::decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(decoded.lsn, header.lsn);
        assert_eq!(decoded.prev_lsn, header.prev_lsn);
        assert_eq!(decoded.tid, header.tid);
        assert_eq!(decoded.record_type, header.record_type);
        assert_eq!(decoded.body_len, header.body_len);
        assert_eq!(decoded.checksum, header.checksum);
    }

    #[test]
    fn begin_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(10),
            UNIX_EPOCH,
            LogRecordBody::Begin,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.lsn, Lsn(1));
        assert_eq!(decoded.header.record_type, LogRecordType::Begin);
        assert_eq!(decoded.header.tid, TransactionId::new(10));
        assert!(matches!(decoded.body, LogRecordBody::Begin));
    }

    #[test]
    fn commit_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(5),
            Lsn(2),
            TransactionId::new(3),
            UNIX_EPOCH,
            LogRecordBody::Commit,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.record_type, LogRecordType::Commit);
        assert!(matches!(decoded.body, LogRecordBody::Commit));
    }

    #[test]
    fn abort_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(7),
            Lsn(6),
            TransactionId::new(99),
            UNIX_EPOCH,
            LogRecordBody::Abort,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert!(matches!(decoded.body, LogRecordBody::Abort));
    }

    #[test]
    fn update_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(20),
            Lsn(15),
            TransactionId::new(5),
            UNIX_EPOCH,
            LogRecordBody::Update {
                page_id: make_page_id(),
                before: vec![0xDE, 0xAD],
                after: vec![0xBE, 0xEF, 0xFF],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.record_type, LogRecordType::Update);
        match decoded.body {
            LogRecordBody::Update {
                page_id,
                before,
                after,
            } => {
                assert_eq!(page_id, make_page_id());
                assert_eq!(before, vec![0xDE, 0xAD]);
                assert_eq!(after, vec![0xBE, 0xEF, 0xFF]);
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn insert_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(30),
            Lsn(25),
            TransactionId::new(7),
            UNIX_EPOCH,
            LogRecordBody::Insert {
                page_id: make_page_id(),
                after: vec![1, 2, 3, 4],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Insert { after, .. } => assert_eq!(after, vec![1, 2, 3, 4]),
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn delete_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(40),
            Lsn(35),
            TransactionId::new(8),
            UNIX_EPOCH,
            LogRecordBody::Delete {
                page_id: make_page_id(),
                before: vec![9, 8, 7],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Delete { before, .. } => assert_eq!(before, vec![9, 8, 7]),
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn clr_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(50),
            Lsn(45),
            TransactionId::new(12),
            UNIX_EPOCH,
            LogRecordBody::Clr {
                page_id: make_page_id(),
                after: vec![0xAA],
                undo_next_lsn: Lsn(10),
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Clr { undo_next_lsn, .. } => {
                assert_eq!(undo_next_lsn, Lsn(10));
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn checkpoint_begin_roundtrip() {
        let rec = LogRecord::new(
            Lsn(100),
            Lsn::INVALID,
            TransactionId::INVALID,
            UNIX_EPOCH,
            LogRecordBody::CheckpointBegin,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert!(matches!(decoded.body, LogRecordBody::CheckpointBegin));
    }

    #[test]
    fn checkpoint_end_roundtrip() {
        let rec = LogRecord::new(
            Lsn(101),
            Lsn(100),
            TransactionId::INVALID,
            UNIX_EPOCH,
            LogRecordBody::CheckpointEnd,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.prev_lsn, Lsn(100));
        assert!(matches!(decoded.body, LogRecordBody::CheckpointEnd));
    }

    #[test]
    fn new_sets_body_len_and_checksum() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
            UNIX_EPOCH,
            LogRecordBody::Insert {
                page_id: make_page_id(),
                after: vec![1, 2, 3],
            },
        )
        .unwrap();

        // body_len must match the actual encoded body size
        let mut body_buf = Vec::new();
        rec.body.encode(&mut body_buf).unwrap();
        assert_eq!(
            rec.header.body_len,
            u32::try_from(body_buf.len()).expect("test body fits in u32")
        );

        // checksum must match a freshly computed CRC over those bytes
        assert_eq!(rec.header.checksum, crc32fast::hash(&body_buf));
    }

    #[test]
    fn timestamp_preserved_at_second_precision() {
        let ts = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
            ts,
            LogRecordBody::Begin,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        let expected = ts.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let got = decoded
            .header
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(got, expected);
    }

    #[test]
    fn encode_decode_image_empty() {
        let mut buf = Vec::new();
        encode_image(&mut buf, None).unwrap();
        let result = decode_image(&mut Cursor::new(buf)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn encode_decode_image_nonempty() {
        let data = vec![1u8, 2, 3, 255];
        let mut buf = Vec::new();
        encode_image(&mut buf, Some(&data)).unwrap();
        let result = decode_image(&mut Cursor::new(buf)).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn decode_rejects_unknown_record_type() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
            UNIX_EPOCH,
            LogRecordBody::Begin,
        )
        .unwrap();
        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        buf[0] = 99; // corrupt record_type (first byte of header)
        let err = LogRecord::decode(&mut Cursor::new(buf));
        assert!(matches!(err, Err(CodecError::UnknownDiscriminant(99))));
    }

    #[test]
    fn decode_fails_on_truncated_input() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
            UNIX_EPOCH,
            LogRecordBody::Commit,
        )
        .unwrap();
        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        let truncated = &buf[..buf.len() / 2];
        let err = LogRecord::decode(&mut Cursor::new(truncated));
        assert!(err.is_err());
    }
}
