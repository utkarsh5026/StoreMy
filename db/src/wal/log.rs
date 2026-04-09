use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec::{CodecError, Decode, Encode};
use crate::primitives::{Lsn, PageId, TransactionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,

    Update,
    Insert,
    Delete,

    CheckpointBegin,
    CheckpointEnd,

    Clr,
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

pub struct LogRecord {
    pub lsn: Lsn,
    pub record_type: LogRecordType,
    pub tid: Option<TransactionId>,
    pub prev_lsn: Lsn,

    pub page_id: Option<PageId>,       // only for data records
    pub before_image: Option<Vec<u8>>, // UNDO image (Update/Delete)
    pub after_image: Option<Vec<u8>>,  // REDO image (Update/Insert)

    pub undo_next_lsn: Option<Lsn>, // only for CLR records
    pub timestamp: SystemTime,
}

impl Encode for LogRecord {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u8(self.record_type as u8)?;

        self.lsn.encode(writer)?;
        self.tid.unwrap_or(TransactionId::INVALID).encode(writer)?;
        self.prev_lsn.encode(writer)?;

        let secs = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        writer.write_u64::<LittleEndian>(secs)?;

        match self.page_id {
            Some(p) => {
                writer.write_u8(1)?;
                p.encode(writer)?;
            }
            None => writer.write_u8(0)?,
        }

        self.undo_next_lsn.unwrap_or(Lsn::INVALID).encode(writer)?;
        encode_image(writer, self.before_image.as_deref())?;
        encode_image(writer, self.after_image.as_deref())?;

        Ok(())
    }
}

impl Decode for LogRecord {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let record_type =
            LogRecordType::try_from(reader.read_u8()?).map_err(CodecError::UnknownDiscriminant)?;

        let lsn = Lsn::decode(reader)?;
        let raw_tid = TransactionId::decode(reader)?;
        let tid = raw_tid.is_valid().then_some(raw_tid);
        let prev_lsn = Lsn::decode(reader)?;

        let secs = reader.read_u64::<LittleEndian>()?;
        let timestamp = UNIX_EPOCH + std::time::Duration::from_secs(secs);

        let page_id = match reader.read_u8()? {
            1 => Some(PageId::decode(reader)?),
            _ => None,
        };

        let undo_lsn = Lsn::decode(reader)?;
        let undo_next_lsn = undo_lsn.is_valid().then_some(undo_lsn);

        let before_image = decode_image(reader)?;
        let after_image = decode_image(reader)?;

        Ok(LogRecord {
            lsn,
            record_type,
            tid,
            prev_lsn,
            page_id,
            before_image,
            after_image,
            undo_next_lsn,
            timestamp,
        })
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

    // ── LogRecordType ────────────────────────────────────────────────────────

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
            let byte = v as u8;
            assert_eq!(LogRecordType::try_from(byte).unwrap(), v);
        }
    }

    #[test]
    fn record_type_rejects_unknown_discriminant() {
        assert_eq!(LogRecordType::try_from(9u8), Err(9u8));
        assert_eq!(LogRecordType::try_from(255u8), Err(255u8));
    }

    // ── Begin / Commit / Abort (no page, no images) ──────────────────────────

    #[test]
    fn begin_record_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(1),
            record_type: LogRecordType::Begin,
            tid: Some(TransactionId::new(10)),
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.lsn, rec.lsn);
        assert_eq!(decoded.record_type, rec.record_type);
        assert_eq!(decoded.tid, rec.tid);
        assert_eq!(decoded.prev_lsn, rec.prev_lsn);
        assert!(decoded.page_id.is_none());
        assert!(decoded.before_image.is_none());
        assert!(decoded.after_image.is_none());
        assert!(decoded.undo_next_lsn.is_none());
    }

    #[test]
    fn commit_record_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(5),
            record_type: LogRecordType::Commit,
            tid: Some(TransactionId::new(3)),
            prev_lsn: Lsn::new(2),
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Commit);
        assert_eq!(decoded.tid, Some(TransactionId::new(3)));
        assert_eq!(decoded.prev_lsn, Lsn::new(2));
    }

    #[test]
    fn abort_record_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(7),
            record_type: LogRecordType::Abort,
            tid: Some(TransactionId::new(99)),
            prev_lsn: Lsn::new(6),
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Abort);
    }

    // ── Data records (Update / Insert / Delete) ───────────────────────────────

    #[test]
    fn update_record_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(20),
            record_type: LogRecordType::Update,
            tid: Some(TransactionId::new(5)),
            prev_lsn: Lsn::new(15),
            page_id: Some(make_page_id()),
            before_image: Some(vec![0xDE, 0xAD]),
            after_image: Some(vec![0xBE, 0xEF, 0xFF]),
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Update);
        assert_eq!(decoded.page_id, rec.page_id);
        assert_eq!(decoded.before_image, Some(vec![0xDE, 0xAD]));
        assert_eq!(decoded.after_image, Some(vec![0xBE, 0xEF, 0xFF]));
        assert!(decoded.undo_next_lsn.is_none());
    }

    #[test]
    fn insert_record_has_only_after_image() {
        let rec = LogRecord {
            lsn: Lsn::new(30),
            record_type: LogRecordType::Insert,
            tid: Some(TransactionId::new(7)),
            prev_lsn: Lsn::new(25),
            page_id: Some(make_page_id()),
            before_image: None,
            after_image: Some(vec![1, 2, 3, 4]),
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Insert);
        assert!(decoded.before_image.is_none());
        assert_eq!(decoded.after_image, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn delete_record_has_only_before_image() {
        let rec = LogRecord {
            lsn: Lsn::new(40),
            record_type: LogRecordType::Delete,
            tid: Some(TransactionId::new(8)),
            prev_lsn: Lsn::new(35),
            page_id: Some(make_page_id()),
            before_image: Some(vec![9, 8, 7]),
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Delete);
        assert_eq!(decoded.before_image, Some(vec![9, 8, 7]));
        assert!(decoded.after_image.is_none());
    }

    // ── CLR record ───────────────────────────────────────────────────────────

    #[test]
    fn clr_record_preserves_undo_next_lsn() {
        let rec = LogRecord {
            lsn: Lsn::new(50),
            record_type: LogRecordType::Clr,
            tid: Some(TransactionId::new(12)),
            prev_lsn: Lsn::new(45),
            page_id: Some(make_page_id()),
            before_image: None,
            after_image: Some(vec![0xAA]),
            undo_next_lsn: Some(Lsn::new(10)),
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::Clr);
        assert_eq!(decoded.undo_next_lsn, Some(Lsn::new(10)));
    }

    // ── Checkpoint records ───────────────────────────────────────────────────

    #[test]
    fn checkpoint_begin_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(100),
            record_type: LogRecordType::CheckpointBegin,
            tid: None,
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::CheckpointBegin);
        assert!(decoded.tid.is_none());
    }

    #[test]
    fn checkpoint_end_roundtrip() {
        let rec = LogRecord {
            lsn: Lsn::new(101),
            record_type: LogRecordType::CheckpointEnd,
            tid: None,
            prev_lsn: Lsn::new(100),
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.record_type, LogRecordType::CheckpointEnd);
        assert_eq!(decoded.prev_lsn, Lsn::new(100));
    }

    // ── Optional field sentinels ─────────────────────────────────────────────

    #[test]
    fn no_tid_decoded_as_none() {
        let rec = LogRecord {
            lsn: Lsn::new(200),
            record_type: LogRecordType::CheckpointBegin,
            tid: None,
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert!(decoded.tid.is_none());
    }

    #[test]
    fn invalid_undo_next_lsn_decoded_as_none() {
        let rec = LogRecord {
            lsn: Lsn::new(300),
            record_type: LogRecordType::Update,
            tid: Some(TransactionId::new(1)),
            prev_lsn: Lsn::new(1),
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let decoded = roundtrip(&rec);
        assert!(decoded.undo_next_lsn.is_none());
    }

    // ── Timestamp preservation ───────────────────────────────────────────────

    #[test]
    fn timestamp_preserved_at_second_precision() {
        let ts = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let rec = LogRecord {
            lsn: Lsn::new(1),
            record_type: LogRecordType::Begin,
            tid: Some(TransactionId::new(1)),
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: ts,
        };
        let decoded = roundtrip(&rec);
        // Timestamps are stored as whole seconds, so sub-second precision is lost.
        let expected_secs = ts.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let got_secs = decoded
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(got_secs, expected_secs);
    }

    // ── Image encoding helpers ───────────────────────────────────────────────

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
        // Build a valid Begin record then corrupt the first byte (record type).
        let rec = LogRecord {
            lsn: Lsn::new(1),
            record_type: LogRecordType::Begin,
            tid: Some(TransactionId::new(1)),
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        buf[0] = 99; // unknown discriminant
        let err = LogRecord::decode(&mut Cursor::new(buf));
        assert!(err.is_err());
    }

    #[test]
    fn decode_fails_on_truncated_input() {
        let rec = LogRecord {
            lsn: Lsn::new(1),
            record_type: LogRecordType::Commit,
            tid: Some(TransactionId::new(2)),
            prev_lsn: Lsn::INVALID,
            page_id: None,
            before_image: None,
            after_image: None,
            undo_next_lsn: None,
            timestamp: UNIX_EPOCH,
        };
        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        let truncated = &buf[..buf.len() / 2];
        let err = LogRecord::decode(&mut Cursor::new(truncated));
        assert!(err.is_err());
    }
}
