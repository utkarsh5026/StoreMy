//! Core primitive types for the database engine.
//!
//! These types name storage locations, transactions, log positions, and
//! simple SQL comparison operators. Most of them implement
//! [`Encode`](crate::codec::Encode) and [`Decode`](crate::codec::Decode) so
//! they can be stored in pages or log records with a fixed, little-endian
//! layout.
//!
//! Included definitions:
//!
//! - [`PageNumber`] — index of a page inside a file
//! - [`FileId`] — identifies a heap or index file
//! - [`TransactionId`] — identifies a transaction (monotonic, not reused)
//! - [`Lsn`] — log sequence number for the write-ahead log
//! - [`SlotId`] — tuple slot index on a slotted page
//! - [`ColumnId`] — column index in a table schema
//! - [`HashCode`] — hash value used for hash indexes
//! - [`RecordId`] — full tuple address (`file`, `page`, `slot`)
//! - [`PageId`] — page address without a slot (`file`, `page`)
//! - [`Predicate`] — comparison operators used in `WHERE` / join conditions
//! - [`Filepath`] — type alias for [`PathBuf`](std::path::PathBuf) in APIs

use std::{
    fmt,
    hash::{Hash, Hasher},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec::{CodecError, Decode, Encode};

/// Index of a page inside a database file.
///
/// Page numbers start at zero and increase by one for each successive page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct PageNumber(
    /// Zero-based page index.
    pub u32,
);

impl PageNumber {
    /// Size of the on-disk encoding in bytes (`u32` little-endian).
    pub const SIZE: usize = 4;

    /// Wraps a raw page index.
    #[inline]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    /// Returns the raw page index.
    #[inline]
    pub const fn get(&self) -> u32 {
        self.0
    }

    /// Returns the next page number.
    ///
    /// On `u32::MAX`, this wraps on overflow (same as `u32` addition).
    #[inline]
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    /// Byte offset of this page from the start of the file.
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

impl Encode for PageNumber {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u32::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for PageNumber {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u32::<LittleEndian>()?))
    }
}

/// Unique identifier for a database file (for example a table heap or index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct FileId(
    /// Opaque numeric file key.
    pub u64,
);

impl FileId {
    /// Wraps a raw file id.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
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

impl From<FileId> for u64 {
    fn from(id: FileId) -> Self {
        id.0
    }
}

impl From<&Path> for FileId {
    /// Derives a stable-ish id from a path using the standard library hasher.
    ///
    /// This is not a cryptographic hash; two different paths could collide.
    fn from(path: &Path) -> Self {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        Self(hasher.finish())
    }
}

impl Encode for FileId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u64::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for FileId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u64::<LittleEndian>()?))
    }
}

/// Catalog-assigned identifier for a secondary index.
///
/// Distinct from [`FileId`] (which identifies the index's *backing file*).
/// `IndexId` is the catalog's primary key for an index — stable across
/// renames, used to group multi-column index rows in `SystemTable::Indexes`,
/// and the on-disk encoding is signed `i64` (matching the system-table
/// column type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct IndexId(
    /// Opaque numeric index key.
    pub i64,
);

impl IndexId {
    /// Wraps a raw index id.
    #[inline]
    pub const fn new(id: i64) -> Self {
        Self(id)
    }
}

impl fmt::Display for IndexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({})", self.0)
    }
}

impl From<i64> for IndexId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl From<IndexId> for i64 {
    fn from(id: IndexId) -> Self {
        id.0
    }
}

/// Unique identifier for a database transaction.
///
/// IDs increase over time and are not reused after commit or abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct TransactionId(
    /// Raw transaction id; zero is reserved (see [`TransactionId::INVALID`]).
    pub u64,
);

impl TransactionId {
    /// Size of the on-disk encoding in bytes (`u64` little-endian).
    pub const SIZE: usize = 8;

    /// Sentinel meaning “no transaction” or “invalid”.
    pub const INVALID: Self = Self(0);

    /// Wraps a raw transaction id (callers may use [`TransactionId::INVALID`]).
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns `true` when this id is not [`TransactionId::INVALID`].
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != 0
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

impl Encode for TransactionId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u64::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for TransactionId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u64::<LittleEndian>()?))
    }
}

/// Log sequence number: a position in the write-ahead log.
///
/// Values increase over time and uniquely identify log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Lsn(
    /// Raw LSN value; zero is reserved (see [`Lsn::INVALID`]).
    pub u64,
);

impl Lsn {
    /// Sentinel meaning “no LSN” or “invalid”.
    pub const INVALID: Self = Self(0);
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

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

impl Encode for Lsn {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u64::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for Lsn {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u64::<LittleEndian>()?))
    }
}

/// Slot index for a tuple inside a slotted page.
///
/// [`SlotId::INVALID`] is reserved and must not name a real slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SlotId(
    /// Slot index; [`SlotId::INVALID`] uses `u16::MAX`.
    pub u16,
);

impl SlotId {
    /// Size of the on-disk encoding in bytes (`u16` little-endian).
    pub const SIZE: usize = 2;

    /// Sentinel meaning “no slot” or “invalid”.
    pub const INVALID: Self = Self(u16::MAX);

    /// Builds a slot id, rejecting the reserved value `u16::MAX`.
    ///
    /// # Errors
    ///
    /// Returns `Err("invalid slot ID")` when `slot == u16::MAX`.
    #[inline]
    pub const fn new(slot: u16) -> Result<Self, &'static str> {
        if slot == u16::MAX {
            Err("invalid slot ID")
        } else {
            Ok(Self(slot))
        }
    }

    /// Returns `true` when this is not [`SlotId::INVALID`].
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != u16::MAX
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot({})", self.0)
    }
}

impl TryFrom<u16> for SlotId {
    type Error = &'static str;

    /// # Errors
    ///
    /// Returns `Err("slot index out of bounds")` when `value == u16::MAX`.
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

    /// # Errors
    ///
    /// Returns an error when `value` does not fit in `u16` or equals `u16::MAX`.
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value)
            .map_err(|_| "slot index out of bounds")
            .and_then(SlotId::try_from)
    }
}

impl Encode for SlotId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u16::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for SlotId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u16::<LittleEndian>()?))
    }
}

/// Column index inside a table schema.
///
/// [`ColumnId::INVALID`] is reserved and must not name a real column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ColumnId(
    /// Zero-based column index; [`ColumnId::INVALID`] uses `u32::MAX`.
    pub u32,
);

impl ColumnId {
    /// Size of the on-disk encoding in bytes (`u32` little-endian).
    pub const SIZE: usize = 4;

    /// Sentinel meaning “no column” or “invalid”.
    pub const INVALID: Self = Self(u32::MAX);

    /// Builds a column id, rejecting the reserved value `u32::MAX`.
    ///
    /// # Errors
    ///
    /// Returns `Err("invalid column ID")` when `id == u32::MAX`.
    #[inline]
    pub const fn new(id: u32) -> Result<Self, &'static str> {
        if id == u32::MAX {
            Err("invalid column ID")
        } else {
            Ok(Self(id))
        }
    }

    /// Returns `true` when this is not [`ColumnId::INVALID`].
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != u32::MAX
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Col({})", self.0)
    }
}

impl TryFrom<u32> for ColumnId {
    type Error = &'static str;

    /// # Errors
    ///
    /// Returns `Err("column index out of bounds")` when `value == u32::MAX`.
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == u32::MAX {
            Err("column index out of bounds")
        } else {
            Ok(Self(value))
        }
    }
}

impl From<ColumnId> for u32 {
    fn from(col: ColumnId) -> Self {
        col.0
    }
}

impl From<ColumnId> for usize {
    fn from(col: ColumnId) -> Self {
        col.0 as usize
    }
}

impl TryFrom<usize> for ColumnId {
    type Error = &'static str;

    /// # Errors
    ///
    /// Returns an error when `value` does not fit in `u32` or equals `u32::MAX`.
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value)
            .map_err(|_| "column index out of bounds")
            .and_then(ColumnId::try_from)
    }
}

impl Encode for ColumnId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u32::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for ColumnId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u32::<LittleEndian>()?))
    }
}

/// 64-bit hash value used for hash-based indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HashCode(
    /// Raw hash bits (not guaranteed stable across Rust versions for [`HashCode::from_bytes`]).
    pub u64,
);

impl HashCode {
    /// Wraps a precomputed hash code.
    #[inline]
    pub const fn new(hash: u64) -> Self {
        Self(hash)
    }

    /// Returns the underlying `u64`.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.0
    }

    /// Hashes arbitrary bytes with the standard library’s default hasher.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::primitives::HashCode;
    /// let h = HashCode::from_bytes(b"hello");
    /// assert_eq!(h.get(), HashCode::from_bytes(b"hello").get());
    /// ```
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

impl Encode for HashCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u64::<LittleEndian>(self.0)?;
        Ok(())
    }
}

impl Decode for HashCode {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self(reader.read_u64::<LittleEndian>()?))
    }
}

/// Physical address of a single tuple: file, page, and slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    /// Heap or index file that owns the page.
    pub file_id: FileId,
    /// Page within that file.
    pub page_no: PageNumber,
    /// Tuple slot on the page.
    pub slot_id: SlotId,
}

impl RecordId {
    /// Builds a record id from its three parts.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::primitives::{FileId, PageNumber, RecordId, SlotId};
    /// let rid = RecordId::new(FileId::new(1), PageNumber::new(0), SlotId::new(3).unwrap());
    /// ```
    pub const fn new(file_id: FileId, page_no: PageNumber, slot_id: SlotId) -> Self {
        Self {
            file_id,
            page_no,
            slot_id,
        }
    }

    /// Encoded size of a `RecordId` on disk: `FileId(8) + PageNumber(4) + SlotId(2)`.
    pub const ENCODED_SIZE: usize = 8 + 4 + 2;
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

impl Encode for RecordId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.file_id.encode(writer)?;
        self.page_no.encode(writer)?;
        self.slot_id.encode(writer)?;
        Ok(())
    }
}

impl Decode for RecordId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            file_id: FileId::decode(reader)?,
            page_no: PageNumber::decode(reader)?,
            slot_id: SlotId::decode(reader)?,
        })
    }
}

// illustrative
/// Page location without a slot (file plus page number).
///
/// Use [`RecordId`] when you need the exact tuple; use `PageId` when the whole
/// page is the unit of work (for example buffer fixes or page-level I/O).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    /// File that contains the page.
    pub file_id: FileId,
    /// Page index inside that file.
    pub page_no: PageNumber,
}

impl PageId {
    /// Builds a page id from file and page number.
    pub fn new(file_id: FileId, page_no: PageNumber) -> Self {
        Self { file_id, page_no }
    }
}

impl Encode for PageId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.file_id.encode(writer)?;
        self.page_no.encode(writer)?;
        Ok(())
    }
}

impl Decode for PageId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            file_id: FileId::decode(reader)?,
            page_no: PageNumber::decode(reader)?,
        })
    }
}

/// SQL-style comparison operator (equality, ordering, pattern match).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// `=`
    Equals,
    /// `<`
    LessThan,
    /// `>`
    GreaterThan,
    /// `<=`
    LessThanOrEqual,
    /// `>=`
    GreaterThanOrEqual,
    /// `!=`
    NotEqual,
    /// `<>` (SQL not-equal)
    NotEqualBracket,
    /// `LIKE`
    Like,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Predicate::Equals => "=",
            Predicate::LessThan => "<",
            Predicate::GreaterThan => ">",
            Predicate::LessThanOrEqual => "<=",
            Predicate::GreaterThanOrEqual => ">=",
            Predicate::NotEqual => "!=",
            Predicate::NotEqualBracket => "<>",
            Predicate::Like => "LIKE",
        };
        write!(f, "{s}")
    }
}

impl TryFrom<&str> for Predicate {
    type Error = String;

    /// Parses a single SQL operator token (case-insensitive only for `"LIKE"` / `"like"`).
    ///
    /// # Errors
    ///
    /// Returns `Err` with a short message when `s` is not one of the supported spellings.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::primitives::Predicate;
    /// assert_eq!(
    ///     Predicate::try_from("<=").unwrap(),
    ///     Predicate::LessThanOrEqual
    /// );
    /// assert!(Predicate::try_from("??").is_err());
    /// ```
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "=" => Ok(Predicate::Equals),
            "<" => Ok(Predicate::LessThan),
            ">" => Ok(Predicate::GreaterThan),
            "<=" => Ok(Predicate::LessThanOrEqual),
            ">=" => Ok(Predicate::GreaterThanOrEqual),
            "!=" => Ok(Predicate::NotEqual),
            "<>" => Ok(Predicate::NotEqualBracket),
            "LIKE" | "like" => Ok(Predicate::Like),
            _ => Err(format!("unknown predicate operator: {s}")),
        }
    }
}

impl Encode for Predicate {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let discriminant: u8 = match self {
            Predicate::Equals => 0,
            Predicate::LessThan => 1,
            Predicate::GreaterThan => 2,
            Predicate::LessThanOrEqual => 3,
            Predicate::GreaterThanOrEqual => 4,
            Predicate::NotEqual => 5,
            Predicate::NotEqualBracket => 6,
            Predicate::Like => 7,
        };
        writer.write_u8(discriminant)?;
        Ok(())
    }
}

impl Decode for Predicate {
    /// # Errors
    ///
    /// Returns [`CodecError::UnknownDiscriminant`](crate::codec::CodecError::UnknownDiscriminant)
    /// when the stored discriminant is not in `0..=7`.
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        match reader.read_u8()? {
            0 => Ok(Predicate::Equals),
            1 => Ok(Predicate::LessThan),
            2 => Ok(Predicate::GreaterThan),
            3 => Ok(Predicate::LessThanOrEqual),
            4 => Ok(Predicate::GreaterThanOrEqual),
            5 => Ok(Predicate::NotEqual),
            6 => Ok(Predicate::NotEqualBracket),
            7 => Ok(Predicate::Like),
            other => Err(CodecError::UnknownDiscriminant(other)),
        }
    }
}

/// Convenience alias for owning file paths in engine APIs.
pub type Filepath = PathBuf;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Decode, Encode};

    #[test]
    fn test_page_number_serialization() {
        let pn = PageNumber::new(12345);
        assert_eq!(PageNumber::from_bytes(&pn.to_bytes().unwrap()).unwrap(), pn);
    }

    #[test]
    fn test_file_id_serialization() {
        let fid = FileId::new(42);
        assert_eq!(FileId::from_bytes(&fid.to_bytes().unwrap()).unwrap(), fid);
    }

    #[test]
    fn test_transaction_id_serialization() {
        let txn = TransactionId::new(999_999);
        assert_eq!(
            TransactionId::from_bytes(&txn.to_bytes().unwrap()).unwrap(),
            txn
        );
    }

    #[test]
    fn test_lsn_serialization() {
        let lsn = Lsn(0xDEAD_BEEF);
        assert_eq!(Lsn::from_bytes(&lsn.to_bytes().unwrap()).unwrap(), lsn);
    }

    #[test]
    fn test_page_number_offset() {
        let pn = PageNumber::new(10);
        assert_eq!(pn.offset(4096), 40960);
    }

    #[test]
    fn test_invalid_sentinels() {
        assert!(!TransactionId::INVALID.is_valid());
        assert!(!SlotId::INVALID.is_valid());
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(PageNumber::new(42).to_string(), "Page(42)");
        assert_eq!(FileId::new(1).to_string(), "File(1)");
        assert_eq!(TransactionId::new(100).to_string(), "Txn(100)");
        assert_eq!(Lsn(500).to_string(), "LSN(500)");
        assert_eq!(ColumnId::try_from(7u32).unwrap().to_string(), "Col(7)");
    }

    #[test]
    fn test_column_id_invalid_sentinel() {
        assert!(!ColumnId::INVALID.is_valid());
        assert!(ColumnId::try_from(u32::MAX).is_err());
        assert!(ColumnId::try_from(usize::MAX).is_err());
    }

    #[test]
    fn test_column_id_serialization() {
        let col = ColumnId::try_from(42u32).unwrap();
        assert_eq!(ColumnId::from_bytes(&col.to_bytes().unwrap()).unwrap(), col);
    }

    #[test]
    fn test_column_id_conversions() {
        let col = ColumnId::try_from(3u32).unwrap();
        assert_eq!(u32::from(col), 3u32);
        assert_eq!(usize::from(col), 3usize);
        assert_eq!(ColumnId::try_from(3usize).unwrap(), col);
    }
}
