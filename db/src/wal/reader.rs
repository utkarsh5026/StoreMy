//! Forward iterator over a WAL file with torn-tail detection.
//!
//! [`WalReader`] reads log records produced by [`super::writer::Wal`]. It is
//! the single source used by the Analysis, Redo, and Undo passes during crash
//! recovery. Its contract is simple:
//!
//! - A complete, CRC-valid record → `Ok(Some(record))`
//! - A partial or corrupt record at the very end of the file → `Ok(None)`
//! - A genuine I/O error (hardware, permissions, …) → `Err(WalError::Io(…))`
//!
//! The `Ok(None)` case is called the *torn tail*: after a crash the OS may have
//! written only part of the last record. That is normal — the record was never
//! confirmed to the caller so it is simply discarded.

use std::{
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use fallible_iterator::FallibleIterator;

use super::{
    WalError,
    log::{LogRecord, LogRecordHeader},
};
use crate::{
    codec::{CodecError, Decode},
    primitives::Lsn,
};

/// Maximum allowed body size for a single log record (16 MiB).
///
/// A `body_len` larger than this almost certainly means the header bytes are
/// garbage (torn tail). Treating it as a torn record avoids allocating a
/// multi-gigabyte buffer before discovering the CRC doesn't match.
const MAX_RECORD_BODY: u32 = 16 * 1024 * 1024;

/// A read-only forward cursor over a WAL file.
///
/// Construct one with [`WalReader::open`], optionally jump to an LSN with
/// [`WalReader::seek_to`], then call [`WalReader::next`] in a loop.
pub struct WalReader {
    reader: BufReader<File>,
    pos: u64,
}

impl WalReader {
    /// Opens the WAL file at `path` and positions the cursor at byte 0.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self, WalError> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            pos: 0,
        })
    }

    /// Moves the cursor to `lsn`.
    ///
    /// In ARIES the LSN *is* the byte offset of the record in the log file, so
    /// this is a single seek call.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the seek fails.
    pub fn seek_to(&mut self, lsn: Lsn) -> Result<(), WalError> {
        self.reader.seek(SeekFrom::Start(lsn.0))?;
        self.pos = lsn.into();
        Ok(())
    }

    /// Reads and returns the log record at the given LSN.
    ///
    /// This seeks the WAL reader to the specified LSN and attempts to parse a
    /// single log record from that location. If the record is torn/truncated,
    /// returns [`WalError::TornRecord(lsn)`].
    ///
    /// # Arguments
    ///
    /// * `lsn` — The log sequence number (byte offset) where the record begins.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`WalError::Io`] if seeking or reading fails.
    /// - [`WalError::TornRecord(lsn)`] if the record at `lsn` is malformed or incomplete.
    pub fn read_at(&mut self, lsn: Lsn) -> Result<LogRecord, WalError> {
        self.seek_to(lsn)?;
        FallibleIterator::next(self)?.ok_or(WalError::TornRecord(lsn))
    }
}

impl FallibleIterator for WalReader {
    type Item = LogRecord;
    type Error = WalError;

    /// Reads and returns the next log record, advancing the cursor past it.
    ///
    /// Returns `Ok(None)` at the end of the valid log — including when a torn
    /// (partial or corrupt) record is encountered at the current position.
    /// This is the normal condition after a crash and is never an error.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] only for genuine filesystem errors.
    fn next(&mut self) -> Result<Option<LogRecord>, WalError> {
        let file_len = self.reader.get_ref().metadata()?.len();
        if self.pos + LogRecordHeader::SIZE as u64 > file_len {
            return Ok(None);
        }

        let mut hdr_buf = [0u8; LogRecordHeader::SIZE];
        self.reader.read_exact(&mut hdr_buf)?;

        // An unknown discriminant means the type byte is garbled — torn record.
        // Any other codec error (truncated field) is also a torn record since
        // the above code guaranteed enough bytes; only a bit-flip could cause it.
        let header = match LogRecordHeader::decode(&mut Cursor::new(&hdr_buf)) {
            Ok(h) => h,
            Err(CodecError::UnknownDiscriminant(_) | _) => return Ok(None),
        };

        if header.body_len > MAX_RECORD_BODY
            || self.pos + LogRecordHeader::SIZE as u64 + u64::from(header.body_len) > file_len
        {
            return Ok(None);
        }

        let mut body_buf = vec![0u8; header.body_len as usize];
        self.reader.read_exact(&mut body_buf)?;
        if crc32fast::hash(&body_buf) != header.checksum {
            return Ok(None);
        }

        let record_size = LogRecordHeader::SIZE + body_buf.len();
        let mut full = Vec::with_capacity(record_size);
        full.extend_from_slice(&hdr_buf);
        full.extend_from_slice(&body_buf);

        let record = LogRecord::decode(&mut Cursor::new(&full)).map_err(WalError::Codec)?;
        self.pos += record_size as u64;
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use fallible_iterator::FallibleIterator;
    use tempfile::NamedTempFile;

    use super::WalReader;
    use crate::{
        codec::Encode,
        primitives::{FileId, Lsn, PageId, PageNumber, TransactionId},
        wal::{
            WalError,
            log::{LogRecord, LogRecordBody},
        },
    };

    // Byte offsets where each record starts in the test file.
    const OFFSET_R1: u64 = 0;
    const OFFSET_R2: u64 = 41;
    const OFFSET_R3: u64 = 110;

    fn page_id() -> PageId {
        PageId::new(FileId::new(1), PageNumber::new(1))
    }

    /// Write records sequentially to a temp file and return it.
    fn write_records(records: &[LogRecord]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for r in records {
            r.encode(f.as_file_mut()).unwrap();
        }
        f.as_file().sync_all().unwrap();
        f
    }

    /// Three records whose stored LSN values do NOT match their byte offsets.
    fn make_records() -> Vec<LogRecord> {
        let tid = TransactionId::new(42);
        vec![
            LogRecord::new(Lsn(1000), Lsn::INVALID, tid, LogRecordBody::Begin).unwrap(),
            LogRecord::new(Lsn(2000), Lsn(1000), tid, LogRecordBody::Insert {
                page_id: page_id(),
                before: vec![0u8; 4],
                after: vec![1u8; 4],
            })
            .unwrap(),
            LogRecord::new(Lsn(3000), Lsn(2000), tid, LogRecordBody::Commit).unwrap(),
        ]
    }

    // ── happy path ──────────────────────────────────────────────────────────

    #[test]
    fn reads_all_records_in_order() {
        let records = make_records();
        let f = write_records(&records);
        let mut reader = WalReader::open(f.path()).unwrap();

        for expected in &records {
            let got = reader.next().unwrap().expect("expected a record");
            assert_eq!(got.header.lsn, expected.header.lsn);
            assert_eq!(got.header.record_type, expected.header.record_type);
            assert_eq!(got.header.tid, expected.header.tid);
        }
        // One more call should signal clean EOF.
        assert!(
            reader.next().unwrap().is_none(),
            "expected EOF after last record"
        );
    }

    #[test]
    fn seek_to_jumps_to_correct_record() {
        let records = make_records();
        let f = write_records(&records);
        let mut reader = WalReader::open(f.path()).unwrap();

        // Seek past the first record and confirm we read the second.
        reader.seek_to(Lsn(OFFSET_R2)).unwrap();
        let got = reader.next().unwrap().expect("expected Insert record");
        assert_eq!(got.header.lsn, records[1].header.lsn); // Lsn(2000)
        assert_eq!(got.header.record_type, records[1].header.record_type);
    }

    #[test]
    fn read_at_returns_correct_record() {
        let records = make_records();
        let f = write_records(&records);
        let mut reader = WalReader::open(f.path()).unwrap();

        // Jump directly to the Commit record.
        let got = reader.read_at(Lsn(OFFSET_R3)).unwrap();
        assert_eq!(got.header.lsn, records[2].header.lsn); // Lsn(3000)
        assert_eq!(got.header.record_type, records[2].header.record_type);
    }

    #[test]
    fn insert_body_survives_roundtrip() {
        let records = make_records();
        let f = write_records(&records);
        let mut reader = WalReader::open(f.path()).unwrap();

        reader.seek_to(Lsn(OFFSET_R2)).unwrap();
        let got = reader.next().unwrap().unwrap();

        match got.body {
            crate::wal::log::LogRecordBody::Insert { before, after, .. } => {
                assert_eq!(before, vec![0u8; 4]);
                assert_eq!(after, vec![1u8; 4]);
            }
            _ => panic!("expected Insert body"),
        }
    }

    // ── torn-tail cases ─────────────────────────────────────────────────────

    /// Only 10 bytes of record 3's header are present — caught by step 1.
    #[test]
    fn torn_header_returns_none() {
        let records = make_records();
        let f = write_records(&records);
        // Record 3 starts at byte 110; keep only 10 bytes of its header.
        f.as_file().set_len(OFFSET_R3 + 10).unwrap();

        let mut reader = WalReader::open(f.path()).unwrap();
        reader.next().unwrap(); // record 1 — complete
        reader.next().unwrap(); // record 2 — complete
        assert!(
            reader.next().unwrap().is_none(),
            "partial header must return Ok(None)"
        );
    }

    /// Reco's header is intact but its body is truncated — caught by step 5.
    #[test]
    fn torn_body_returns_none() {
        let records = make_records();
        let f = write_records(&records);
        // Truncate mid-body of the Insert record: keep header (41 B) + half
        // the body (14 B) = 55 B past the start of record 2.
        f.as_file().set_len(OFFSET_R2 + 41 + 14).unwrap();

        let mut reader = WalReader::open(f.path()).unwrap();
        reader.next().unwrap(); // record 1 — complete
        assert!(
            reader.next().unwrap().is_none(),
            "partial body must return Ok(None)"
        );
    }

    /// Flipping a byte in the Insert body causes a CRC mismatch — caught by step 7.
    #[test]
    fn crc_mismatch_returns_none() {
        let records = make_records();
        let f = write_records(&records);

        // First body byte of record 2 = OFFSET_R2 + header size = 41 + 41 = 82.
        f.as_file().seek(SeekFrom::Start(OFFSET_R2 + 41)).unwrap();
        f.as_file().write_all(&[0xFF]).unwrap();
        f.as_file().sync_all().unwrap();

        let mut reader = WalReader::open(f.path()).unwrap();
        reader.next().unwrap(); // record 1 is unaffected
        assert!(
            reader.next().unwrap().is_none(),
            "CRC mismatch must return Ok(None)"
        );
    }

    /// `read_at` converts Ok(None) into `WalError::TornRecord`.
    #[test]
    fn read_at_torn_lsn_returns_torn_record_error() {
        let records = make_records();
        let f = write_records(&records);
        // Keep only 10 bytes of record 3's header.
        f.as_file().set_len(OFFSET_R3 + 10).unwrap();

        let mut reader = WalReader::open(f.path()).unwrap();
        let err = reader.read_at(Lsn(OFFSET_R3)).unwrap_err();
        assert!(
            matches!(err, WalError::TornRecord(_)),
            "expected TornRecord, got {err:?}"
        );
    }

    /// Reading an empty file should immediately return Ok(None).
    #[test]
    fn empty_file_returns_none() {
        let f = NamedTempFile::new().unwrap();
        let mut reader = WalReader::open(f.path()).unwrap();
        assert!(reader.next().unwrap().is_none());
    }

    /// Multiple `seek_to` calls interleave correctly.
    #[test]
    fn multiple_seeks_are_independent() {
        let records = make_records();
        let f = write_records(&records);
        let mut reader = WalReader::open(f.path()).unwrap();

        // Read record 3, then seek back to record 1.
        let r3 = reader.read_at(Lsn(OFFSET_R3)).unwrap();
        assert_eq!(r3.header.lsn, records[2].header.lsn);

        let r1 = reader.read_at(Lsn(OFFSET_R1)).unwrap();
        assert_eq!(r1.header.lsn, records[0].header.lsn);
    }
}
