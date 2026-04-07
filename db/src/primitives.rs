//! Core primitive types for `StoreMy` database.
//!
//! This module defines the fundamental types used throughout the database:
//! - [`PageNumber`] - Page number within a file
//! - [`FileId`] - Unique identifier for a database file
//! - [`TransactionId`] - Unique identifier for a transaction
//! - [`Lsn`] - Log Sequence Number for WAL
//! - [`SlotId`] - Slot number within a page
//! - [`HashCode`] - Hash value for indexing

use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use byteorder::{ByteOrder, LittleEndian};

/// A page number within a database file.
///
/// Pages are numbered sequentially starting from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct PageNumber(pub u32);

impl PageNumber {
    pub const SIZE: usize = 4;

    #[inline]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    #[inline]
    pub const fn get(&self) -> u32 {
        self.0
    }

    pub fn serialize(&self, buf: &mut [u8]) {
        LittleEndian::write_u32(buf, self.0);
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self(LittleEndian::read_u32(buf))
    }

    #[inline]
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    #[inline]
    pub const fn offset(&self, page_size: usize) -> u64 {
        self.0 as u64 * page_size as u64
    }
}

impl fmt::Display for PageNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page({})", self.0)
    }
}

impl From<u32> for PageNumber {
    fn from(n: u32) -> Self {
        Self(n)
    }
}

/// A unique identifier for a database file (table or index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct FileId(pub u64);

impl FileId {
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn serialize(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.0);
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self(LittleEndian::read_u64(buf))
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "File({})", self.0)
    }
}

impl From<u64> for FileId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// A unique identifier for a database transaction.
///
/// Transaction IDs are monotonically increasing and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct TransactionId(pub u64);

impl TransactionId {
    pub const SIZE: usize = 8;

    /// Invalid transaction ID (used as sentinel).
    pub const INVALID: Self = Self(0);

    /// Creates a new `TransactionId`.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw transaction ID value.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.0
    }

    /// Returns true if this is a valid transaction ID.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != 0
    }

    /// Serializes the transaction ID to bytes.
    pub fn serialize(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.0);
    }

    /// Deserializes a transaction ID from bytes.
    pub fn deserialize(buf: &[u8]) -> Self {
        Self(LittleEndian::read_u64(buf))
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txn({})", self.0)
    }
}

impl From<u64> for TransactionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// A Log Sequence Number for write-ahead logging.
///
/// LSNs are monotonically increasing and identify log records uniquely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const SIZE: usize = 8;

    pub const INVALID: Self = Self(0);

    #[inline]
    pub const fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    #[inline]
    pub const fn get(&self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != 0
    }

    pub fn serialize(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.0);
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self(LittleEndian::read_u64(buf))
    }

    #[inline]
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LSN({})", self.0)
    }
}

impl From<u64> for Lsn {
    fn from(lsn: u64) -> Self {
        Self(lsn)
    }
}

/// A slot number within a page.
///
/// Slots identify tuple positions within a slotted page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SlotId(pub u16);

impl SlotId {
    pub const SIZE: usize = 2;

    /// Invalid slot ID.
    pub const INVALID: Self = Self(u16::MAX);

    #[inline]
    pub const fn new(slot: u16) -> Result<Self, &'static str> {
        if slot == u16::MAX {
            Err("invalid slot ID")
        } else {
            Ok(Self(slot))
        }
    }

    /// Returns true if this is a valid slot ID.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != u16::MAX
    }

    /// Serializes the slot ID to bytes.
    pub fn serialize(&self, buf: &mut [u8]) {
        LittleEndian::write_u16(buf, self.0);
    }

    /// Deserializes a slot ID from bytes.
    pub fn deserialize(buf: &[u8]) -> Self {
        Self(LittleEndian::read_u16(buf))
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot({})", self.0)
    }
}

impl TryFrom<u16> for SlotId {
    type Error = &'static str;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value == u16::MAX {
            Err("slot index out of bounds")
        } else {
            Ok(Self(value))
        }
    }
}

impl From<SlotId> for u16 {
    fn from(slot_id: SlotId) -> Self {
        slot_id.0
    }
}

impl From<SlotId> for usize {
    fn from(value: SlotId) -> Self {
        usize::from(value.0)
    }
}

impl TryFrom<usize> for SlotId {
    type Error = &'static str;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value)
            .map_err(|_| "slot index out of bounds")
            .and_then(SlotId::try_from)
    }
}

/// A hash code value used for hash indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HashCode(pub u64);

impl HashCode {
    /// Creates a new `HashCode`.
    #[inline]
    pub const fn new(hash: u64) -> Self {
        Self(hash)
    }

    /// Returns the raw hash value.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.0
    }

    /// Computes hash code for a byte slice.
    pub fn from_bytes(data: &[u8]) -> Self {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        Self(hasher.finish())
    }
}

impl fmt::Display for HashCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({:016x})", self.0)
    }
}

/// A record identifier that uniquely identifies a tuple in the database.
///
/// Combines file ID, page number, and slot ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub file_id: FileId,
    pub page_no: PageNumber,
    pub slot_id: SlotId,
}

impl RecordId {
    /// Creates a new `RecordId`.
    pub const fn new(file_id: FileId, page_no: PageNumber, slot_id: SlotId) -> Self {
        Self {
            file_id,
            page_no,
            slot_id,
        }
    }

    /// Serializes the record ID to bytes.
    pub fn serialize(&self, buf: &mut [u8]) {
        self.file_id.serialize(&mut buf[0..4]);
        self.page_no.serialize(&mut buf[4..8]);
        self.slot_id.serialize(&mut buf[8..10]);
    }

    /// Deserializes a record ID from bytes.
    pub fn deserialize(buf: &[u8]) -> Self {
        Self {
            file_id: FileId::deserialize(&buf[0..4]),
            page_no: PageNumber::deserialize(&buf[4..8]),
            slot_id: SlotId::deserialize(&buf[8..10]),
        }
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RID({}, {}, {})",
            self.file_id.0, self.page_no.0, self.slot_id.0
        )
    }
}

// illustrative
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    pub file_id: FileId,
    pub page_no: PageNumber,
}

impl PageId {
    pub fn new(file_id: FileId, page_no: PageNumber) -> Self {
        Self { file_id, page_no }
    }
}

/// Type alias for file paths.
pub type Filepath = PathBuf;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_number_serialization() {
        let pn = PageNumber::new(12345);
        let mut buf = [0u8; 4];
        pn.serialize(&mut buf);
        assert_eq!(PageNumber::deserialize(&buf), pn);
    }

    #[test]
    fn test_file_id_serialization() {
        let fid = FileId::new(42);
        let mut buf = [0u8; 4];
        fid.serialize(&mut buf);
        assert_eq!(FileId::deserialize(&buf), fid);
    }

    #[test]
    fn test_transaction_id_serialization() {
        let txn = TransactionId::new(999_999);
        let mut buf = [0u8; 8];
        txn.serialize(&mut buf);
        assert_eq!(TransactionId::deserialize(&buf), txn);
    }

    #[test]
    fn test_lsn_serialization() {
        let lsn = Lsn::new(0xDEAD_BEEF);
        let mut buf = [0u8; 8];
        lsn.serialize(&mut buf);
        assert_eq!(Lsn::deserialize(&buf), lsn);
    }

    #[test]
    fn test_page_number_offset() {
        let pn = PageNumber::new(10);
        assert_eq!(pn.offset(4096), 40960);
    }

    #[test]
    fn test_invalid_sentinels() {
        assert!(!TransactionId::INVALID.is_valid());
        assert!(!Lsn::INVALID.is_valid());
        assert!(!SlotId::INVALID.is_valid());
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(PageNumber::new(42).to_string(), "Page(42)");
        assert_eq!(FileId::new(1).to_string(), "File(1)");
        assert_eq!(TransactionId::new(100).to_string(), "Txn(100)");
        assert_eq!(Lsn::new(500).to_string(), "LSN(500)");
    }
}
