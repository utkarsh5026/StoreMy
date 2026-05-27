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
    /// Marks a transaction that has been fully rolled back.
    ///
    /// Written by the Undo pass after every record in a loser transaction has
    /// been compensated.  Analysis removes the transaction from the ATT when it
    /// sees this record, so a crash during undo does not re-undo already-done
    /// steps.
    End = 9,
    /// Marks a named rollback point within an active transaction.
    Savepoint = 10,
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
            9 => Ok(Self::End),
            10 => Ok(Self::Savepoint),
            other => Err(other),
        }
    }
}

/// Whether a transaction was running normally or had already started rolling
/// back at the time a checkpoint was taken.
///
/// Stored as a `u8` in the `CheckpointEnd` ATT snapshot.  Analysis uses this
/// to initialise each entry's status correctly without re-scanning earlier
/// records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnStatus {
    /// Transaction is progressing normally.
    Running = 0,
    /// Transaction has been aborted and its undo is in progress.
    Aborting = 1,
}

impl Encode for TxnStatus {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        (*self as u8).encode(w)
    }
}

impl Decode for TxnStatus {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        TxnStatus::try_from(u8::decode(r)?).map_err(CodecError::UnknownDiscriminant)
    }
}

impl TryFrom<u8> for TxnStatus {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Running),
            1 => Ok(Self::Aborting),
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
#[derive(Debug)]
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
        (self.record_type as u8).encode(writer)?;
        self.lsn.encode(writer)?;
        self.prev_lsn.encode(writer)?;
        self.tid.encode(writer)?;
        self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .encode(writer)?;
        self.body_len.encode(writer)?;
        self.checksum.encode(writer)?;
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
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            record_type: LogRecordType::try_from(u8::decode(r)?)
                .map_err(CodecError::UnknownDiscriminant)?,
            lsn: Lsn::decode(r)?,
            prev_lsn: Lsn::decode(r)?,
            tid: TransactionId::decode(r)?,
            timestamp: UNIX_EPOCH + std::time::Duration::from_secs(u64::decode(r)?),
            body_len: u32::decode(r)?,
            checksum: u32::decode(r)?,
        })
    }
}

/// The variable-length, type-specific payload of a log record.
///
/// Each variant holds exactly the fields that record type requires — no
/// `Option` fields that are only meaningful for some types.
#[derive(Debug)]
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
        /// The page image before the insert (used for undo).
        before: Vec<u8>,
        /// The page image after the insert (used for redo).
        after: Vec<u8>,
    },
    /// A row was deleted.
    Delete {
        /// The page from which the row was removed.
        page_id: PageId,
        /// The page image before the delete (used for undo).
        before: Vec<u8>,
        /// The page image after the delete (used for redo).
        after: Vec<u8>,
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
    /// Fuzzy checkpoint end — carries ATT and DPT snapshots.
    ///
    /// Analysis initialises its in-memory tables from these snapshots so it
    /// only needs to scan records written *after* the checkpoint, not from
    /// LSN 0.
    CheckpointEnd {
        /// Snapshot of the active-transaction table at checkpoint time.
        /// Each entry is `(tid, last_lsn, status)`.
        att_snapshot: Vec<(TransactionId, Lsn, TxnStatus)>,
        /// Snapshot of the dirty-page table at checkpoint time.
        /// Each entry is `(page_id, rec_lsn)` where `rec_lsn` is the oldest
        /// LSN that made the page dirty.
        dpt_snapshot: Vec<(PageId, Lsn)>,
    },
    /// Transaction fully rolled back — no payload.
    ///
    /// Written by the Undo pass after every record in a loser transaction has
    /// been compensated.  Mirrors [`LogRecordType::End`].
    End,
    /// Named savepoint within an active transaction (`SAVEPOINT <name>`).
    Savepoint { name: String },
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
            Self::CheckpointEnd { .. } => LogRecordType::CheckpointEnd,
            Self::End => LogRecordType::End,
            Self::Savepoint { .. } => LogRecordType::Savepoint,
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
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            Self::Begin | Self::Commit | Self::Abort | Self::CheckpointBegin | Self::End => {}

            Self::Savepoint { name } => {
                name.encode(w)?;
            }

            Self::Update {
                page_id,
                before,
                after,
            }
            | Self::Insert {
                page_id,
                before,
                after,
            }
            | Self::Delete {
                page_id,
                before,
                after,
            } => {
                page_id.encode(w)?;
                before.encode(w)?;
                after.encode(w)?;
            }

            Self::CheckpointEnd {
                att_snapshot,
                dpt_snapshot,
            } => {
                att_snapshot.encode(w)?;
                dpt_snapshot.encode(w)?;
            }

            Self::Clr {
                page_id,
                after,
                undo_next_lsn,
            } => {
                page_id.encode(w)?;
                after.encode(w)?;
                undo_next_lsn.encode(w)?;
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
    fn decode_for_type<R: Read>(record_type: LogRecordType, r: &mut R) -> Result<Self, CodecError> {
        Ok(match record_type {
            LogRecordType::Begin => Self::Begin,
            LogRecordType::Commit => Self::Commit,
            LogRecordType::Abort => Self::Abort,
            LogRecordType::End => Self::End,
            LogRecordType::Savepoint => Self::Savepoint {
                name: String::decode(r)?,
            },
            LogRecordType::CheckpointBegin => Self::CheckpointBegin,

            LogRecordType::Update | LogRecordType::Insert | LogRecordType::Delete => {
                let page_id = PageId::decode(r)?;
                let before = Vec::<u8>::decode(r)?;
                let after = Vec::<u8>::decode(r)?;
                match record_type {
                    LogRecordType::Update => Self::Update {
                        page_id,
                        before,
                        after,
                    },
                    LogRecordType::Insert => Self::Insert {
                        page_id,
                        before,
                        after,
                    },
                    LogRecordType::Delete => Self::Delete {
                        page_id,
                        before,
                        after,
                    },
                    _ => unreachable!(),
                }
            }

            LogRecordType::Clr => Self::Clr {
                page_id: PageId::decode(r)?,
                after: Vec::<u8>::decode(r)?,
                undo_next_lsn: Lsn::decode(r)?,
            },

            LogRecordType::CheckpointEnd => Self::CheckpointEnd {
                att_snapshot: Vec::<(TransactionId, Lsn, TxnStatus)>::decode(r)?,
                dpt_snapshot: Vec::<(PageId, Lsn)>::decode(r)?,
            },
        })
    }
}

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
#[derive(Debug)]
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
    /// can be computed before the header is constructed.  The caller supplies
    /// the LSN, previous LSN, transaction ID, and body; the timestamp is set to
    /// [`SystemTime::now`] at construction time.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if encoding the body fails.
    pub fn new(
        lsn: Lsn,
        prev_lsn: Lsn,
        tid: TransactionId,
        body: LogRecordBody,
    ) -> Result<Self, CodecError> {
        let body_bytes = {
            let mut buf = Vec::new();
            body.encode(&mut buf)?;
            buf
        };
        let body_len = u32::try_from(body_bytes.len())
            .map_err(|_| CodecError::numeric_does_not_fit(body_bytes.len(), "u32"))?;

        let header = LogRecordHeader {
            lsn,
            prev_lsn,
            tid,
            record_type: body.record_type(),
            timestamp: SystemTime::now(),
            body_len,
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

/// Reads a full record (header then body) from `r`.
///
/// # Errors
///
/// Returns [`CodecError::UnknownDiscriminant`] if the header's `record_type`
/// byte is unrecognized.  Propagates any other I/O error.
impl Decode for LogRecord {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let header = LogRecordHeader::decode(r)?;
        let body = LogRecordBody::decode_for_type(header.record_type, r)?;
        Ok(Self { header, body })
    }
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
            LogRecordType::End,
            LogRecordType::Savepoint,
        ];
        for v in variants {
            assert_eq!(LogRecordType::try_from(v as u8).unwrap(), v);
        }
    }

    #[test]
    fn record_type_rejects_unknown_discriminant() {
        assert_eq!(LogRecordType::try_from(11u8), Err(11u8));
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
        let rec =
            LogRecord::new(Lsn(5), Lsn(2), TransactionId::new(3), LogRecordBody::Commit).unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.record_type, LogRecordType::Commit);
        assert!(matches!(decoded.body, LogRecordBody::Commit));
    }

    #[test]
    fn abort_record_roundtrip() {
        let rec =
            LogRecord::new(Lsn(7), Lsn(6), TransactionId::new(99), LogRecordBody::Abort).unwrap();
        let decoded = roundtrip(&rec);
        assert!(matches!(decoded.body, LogRecordBody::Abort));
    }

    #[test]
    fn update_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(20),
            Lsn(15),
            TransactionId::new(5),
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
            LogRecordBody::Insert {
                page_id: make_page_id(),
                before: vec![0x00, 0x00],
                after: vec![1, 2, 3, 4],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Insert { before, after, .. } => {
                assert_eq!(before, vec![0x00, 0x00]);
                assert_eq!(after, vec![1, 2, 3, 4]);
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn delete_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(40),
            Lsn(35),
            TransactionId::new(8),
            LogRecordBody::Delete {
                page_id: make_page_id(),
                before: vec![9, 8, 7],
                after: vec![0x00, 0x00],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Delete { before, after, .. } => {
                assert_eq!(before, vec![9, 8, 7]);
                assert_eq!(after, vec![0x00, 0x00]);
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn clr_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(50),
            Lsn(45),
            TransactionId::new(12),
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
            LogRecordBody::CheckpointBegin,
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert!(matches!(decoded.body, LogRecordBody::CheckpointBegin));
    }

    #[test]
    fn checkpoint_end_empty_snapshots_roundtrip() {
        let rec = LogRecord::new(
            Lsn(101),
            Lsn(100),
            TransactionId::INVALID,
            LogRecordBody::CheckpointEnd {
                att_snapshot: vec![],
                dpt_snapshot: vec![],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.prev_lsn, Lsn(100));
        match decoded.body {
            LogRecordBody::CheckpointEnd {
                att_snapshot,
                dpt_snapshot,
            } => {
                assert!(att_snapshot.is_empty());
                assert!(dpt_snapshot.is_empty());
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn checkpoint_end_with_snapshots_roundtrip() {
        let tid1 = TransactionId::new(1);
        let tid2 = TransactionId::new(2);
        let page1 = make_page_id();

        let rec = LogRecord::new(
            Lsn(200),
            Lsn(199),
            TransactionId::INVALID,
            LogRecordBody::CheckpointEnd {
                att_snapshot: vec![
                    (tid1, Lsn(50), TxnStatus::Running),
                    (tid2, Lsn(80), TxnStatus::Aborting),
                ],
                dpt_snapshot: vec![(page1, Lsn(30))],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::CheckpointEnd {
                att_snapshot,
                dpt_snapshot,
            } => {
                assert_eq!(att_snapshot.len(), 2);
                assert_eq!(att_snapshot[0], (tid1, Lsn(50), TxnStatus::Running));
                assert_eq!(att_snapshot[1], (tid2, Lsn(80), TxnStatus::Aborting));
                assert_eq!(dpt_snapshot.len(), 1);
                assert_eq!(dpt_snapshot[0], (page1, Lsn(30)));
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn new_sets_body_len_and_checksum() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
            LogRecordBody::Insert {
                page_id: make_page_id(),
                before: vec![0xFF],
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
    fn decode_rejects_unknown_record_type() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
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
    fn end_record_roundtrip() {
        let rec =
            LogRecord::new(Lsn(99), Lsn(70), TransactionId::new(5), LogRecordBody::End).unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.record_type, LogRecordType::End);
        assert_eq!(decoded.header.lsn, Lsn(99));
        assert!(matches!(decoded.body, LogRecordBody::End));
        // End carries no payload so body_len must be zero.
        assert_eq!(decoded.header.body_len, 0);
    }

    #[test]
    fn savepoint_record_roundtrip() {
        let rec = LogRecord::new(
            Lsn(200),
            Lsn(150),
            TransactionId::new(7),
            LogRecordBody::Savepoint {
                name: "s1".to_owned(),
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        assert_eq!(decoded.header.record_type, LogRecordType::Savepoint);
        assert!(matches!(
            decoded.body,
            LogRecordBody::Savepoint { ref name } if name == "s1"
        ));
    }

    #[test]
    fn txn_status_try_from() {
        assert_eq!(TxnStatus::try_from(0u8), Ok(TxnStatus::Running));
        assert_eq!(TxnStatus::try_from(1u8), Ok(TxnStatus::Aborting));
        assert_eq!(TxnStatus::try_from(2u8), Err(2u8));
        assert_eq!(TxnStatus::try_from(255u8), Err(255u8));
    }

    // Verify that before/after in Insert are stored independently — corrupt one
    // and the other must survive the roundtrip intact.
    #[test]
    fn insert_before_and_after_are_independent() {
        let rec = LogRecord::new(
            Lsn(300),
            Lsn(250),
            TransactionId::new(11),
            LogRecordBody::Insert {
                page_id: make_page_id(),
                before: vec![0xAA, 0xBB],
                after: vec![0x11, 0x22, 0x33],
            },
        )
        .unwrap();
        let decoded = roundtrip(&rec);
        match decoded.body {
            LogRecordBody::Insert { before, after, .. } => {
                assert_eq!(before, vec![0xAA, 0xBB]);
                assert_eq!(after, vec![0x11, 0x22, 0x33]);
                assert_ne!(before, after);
            }
            _ => panic!("wrong body variant"),
        }
    }

    #[test]
    fn decode_fails_on_truncated_input() {
        let rec = LogRecord::new(
            Lsn(1),
            Lsn::INVALID,
            TransactionId::new(1),
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
