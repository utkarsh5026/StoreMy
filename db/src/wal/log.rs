use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec::{CodecError, Decode, Encode};
use crate::primitives::{Lsn, PageId, TransactionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    Begin = 0,
    Commit = 1,
    Abort = 2,
    Update = 3,
    Insert = 4,
    Delete = 5,
    CheckpointBegin = 6,
    CheckpointEnd = 7,
    Clr = 8,
}

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
/// `LogRecordHeader::SIZE` bytes, parse the header, and then read exactly
/// `body_len` more bytes for the body — no guessing, no scanning.
///
/// `checksum` covers only the body bytes. This avoids the circularity of
/// checksumming a field that is itself part of the checksum region.
pub struct LogRecordHeader {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub tid: TransactionId,
    pub record_type: LogRecordType,
    pub timestamp: SystemTime,
    pub body_len: u32,
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
    Begin,
    Commit,
    Abort,
    Update {
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    },
    Insert {
        page_id: PageId,
        after: Vec<u8>,
    },
    Delete {
        page_id: PageId,
        before: Vec<u8>,
    },
    Clr {
        page_id: PageId,
        after: Vec<u8>,
        undo_next_lsn: Lsn,
    },
    CheckpointBegin,
    CheckpointEnd,
}

impl LogRecordBody {
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
    /// Decode a body given an already-decoded `record_type` from the header.
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
    pub header: LogRecordHeader,
    pub body: LogRecordBody,
}

impl LogRecord {
    /// Construct a `LogRecord`, computing `body_len` and `checksum` automatically.
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
            body_len: body_bytes.len() as u32,
            checksum: crc32fast::hash(&body_bytes),
        };
        Ok(Self { header, body })
    }
}

impl Encode for LogRecord {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.header.encode(writer)?;
        self.body.encode(writer)?;
        Ok(())
    }
}

impl Decode for LogRecord {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let header = LogRecordHeader::decode(reader)?;
        let body = LogRecordBody::decode_for_type(header.record_type, reader)?;
        Ok(Self { header, body })
    }
}

fn encode_image<W: Write>(writer: &mut W, image: Option<&[u8]>) -> Result<(), CodecError> {
    match image {
        Some(img) => {
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32::<LittleEndian>(img.len() as u32)?;
            writer.write_all(img)?;
        }
        None => writer.write_u32::<LittleEndian>(0)?,
    }
    Ok(())
}

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
    use super::*;
    use crate::codec::{Decode, Encode};
    use crate::primitives::{FileId, PageNumber};
    use std::io::Cursor;

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
        assert_eq!(rec.header.body_len, body_buf.len() as u32);

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
