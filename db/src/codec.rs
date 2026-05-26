//! Binary encoding and decoding traits for `StoreMy` database types.
//!
//! This module defines the [`Encode`] and [`Decode`] traits, which provide a
//! uniform interface for serializing and deserializing any database type to and
//! from a binary format. All on-disk structures — WAL records, pages, tuples —
//! implement these traits.
//!
//! ## Design
//!
//! Both traits are built around [`std::io::Write`] and [`std::io::Read`], so
//! they work with any destination or source: a [`Vec<u8>`], a [`std::fs::File`],
//! a [`std::io::BufWriter`], a network socket, or an in-memory
//! [`std::io::Cursor`]. The caller chooses; the implementor doesn't care.
//!
//! Convenience methods ([`Encode::to_bytes`], [`Decode::from_bytes`]) cover the
//! common case of working with raw byte slices without requiring the caller to
//! manage a writer or cursor manually.

use std::io::{self, Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
/// Re-export the derive macros so callers `use storemy::codec::{Encode, Decode};`
/// and pick up both the trait and its derive in one go. The derives generate
/// code that names `::storemy::codec::Encode` / `::storemy::codec::Decode`,
/// so this re-export site is also where the path the macros emit resolves.
pub use storemy_codec_derive::{Decode, Encode};
use thiserror::Error;

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Error)]
pub enum CodecError {
    /// An underlying I/O operation failed.
    ///
    /// This wraps any [`std::io::Error`] produced while reading from or writing
    /// to the underlying reader/writer. The `#[from]` attribute means `?` on
    /// any `io::Result` inside an `encode` or `decode` implementation
    /// automatically converts into this variant.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A byte that should identify a known variant was not recognized.
    ///
    /// Used when decoding enums: the stored discriminant byte did not match any
    /// known variant, which typically indicates file corruption or a version
    /// mismatch.
    #[error("Unknown discriminant: {0}")]
    UnknownDiscriminant(u8),

    /// A byte sequence that should be valid UTF-8 was not.
    ///
    /// Returned when decoding string fields whose stored bytes fail UTF-8
    /// validation.
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("numeric value {value} does not fit in {target}")]
    NumericDoesNotFit { value: u64, target: &'static str },
}

impl CodecError {
    /// Convenience method for constructing a [`CodecError::NumericDoesNotFit`] error.
    ///
    /// Use this when a numeric value cannot be safely represented in a target type,
    /// for example when a `u64` does not fit into a `u32`.
    ///
    /// # Arguments
    ///
    /// * `value` - The offending value which failed the conversion or exceeds the target's range.
    /// * `target` - The name or description of the target type (e.g., `"u32"`, `"i16"`).
    ///
    /// # Example
    ///
    /// ```
    /// use crate::codec::CodecError;
    /// let err = CodecError::numeric_does_not_fit(500, "u8");
    /// assert!(matches!(err, CodecError::NumericDoesNotFit {
    ///     value: 500,
    ///     target: "u8"
    /// }));
    /// ```
    pub fn numeric_does_not_fit(value: u64, target: &'static str) -> Self {
        Self::NumericDoesNotFit { value, target }
    }
}

/// Encodes a value into a binary format.
///
/// Implement this trait for any type that needs to be written to disk or sent
/// over the wire. Only [`encode`](Encode::encode) must be provided;
/// [`to_bytes`](Encode::to_bytes) is supplied automatically.
pub trait Encode {
    /// Writes the binary representation of `self` into `writer`.
    ///
    /// Implementors should write fields in a fixed, documented order so that
    /// [`Decode::decode`] can read them back in the same order.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if the underlying writer returns an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let record = LogRecord { ... };
    /// let mut buf = Vec::new();
    /// record.encode(&mut buf)?;
    /// ```
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError>;

    /// Encodes `self` into a freshly allocated `Vec<u8>`.
    ///
    /// This is a convenience wrapper around [`encode`](Encode::encode) for
    /// call sites that need an owned byte buffer rather than streaming into an
    /// existing writer.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if encoding fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let bytes = record.to_bytes()?;
    /// file.write_all(&bytes)?;
    /// ```
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::new();
        self.encode(&mut buf)?;
        Ok(buf)
    }
}

/// Decodes a value from a binary format.
///
/// Implement this trait for any type that needs to be read back from disk or
/// received over the wire. Only [`decode`](Decode::decode) must be provided;
/// [`from_bytes`](Decode::from_bytes) is supplied automatically.
pub trait Decode: Sized {
    /// Reads and reconstructs a value from `reader`.
    ///
    /// The reader's position advances by exactly the number of bytes consumed.
    /// When decoding multiple records sequentially (e.g. replaying a WAL file),
    /// the caller can pass the same reader across calls and each `decode` picks
    /// up where the previous one left off.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if the reader returns an error or reaches EOF
    /// before all fields are read, [`CodecError::UnknownDiscriminant`] for
    /// unrecognized enum variants, or [`CodecError::InvalidUtf8`] for malformed
    /// string data.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut cursor = std::io::Cursor::new(&file_bytes);
    /// while cursor.position() < file_bytes.len() as u64 {
    ///     let record = LogRecord::decode(&mut cursor)?;
    /// }
    /// ```
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError>;

    /// Decodes a value from a raw byte slice.
    ///
    /// This is a convenience wrapper around [`decode`](Decode::decode) that
    /// wraps `bytes` in a [`std::io::Cursor`] so the caller does not need to
    /// manage one manually.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if decoding fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let record = LogRecord::from_bytes(&raw_bytes)?;
    /// ```
    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::decode(&mut cursor)
    }
}

/// Convenience wrappers for little-endian reads from any [`io::Read`] source.
///
/// A blanket impl covers every `R: Read`, so `use crate::codec::ReadLeExt` is
/// all that is needed at call sites — no more turbofish on every byte-order
/// read.  The byteorder crate remains an implementation detail of this module.
pub trait ReadLeExt: Read {
    fn read_u8(&mut self) -> io::Result<u8>;
    fn read_le_u16(&mut self) -> io::Result<u16>;
    fn read_le_u32(&mut self) -> io::Result<u32>;
    fn read_le_u64(&mut self) -> io::Result<u64>;
    fn read_le_i32(&mut self) -> io::Result<i32>;
    fn read_le_i64(&mut self) -> io::Result<i64>;
    fn read_le_f64(&mut self) -> io::Result<f64>;
}

impl<R: Read> ReadLeExt for R {
    fn read_u8(&mut self) -> io::Result<u8> {
        ReadBytesExt::read_u8(self)
    }
    fn read_le_u16(&mut self) -> io::Result<u16> {
        ReadBytesExt::read_u16::<LittleEndian>(self)
    }
    fn read_le_u32(&mut self) -> io::Result<u32> {
        ReadBytesExt::read_u32::<LittleEndian>(self)
    }
    fn read_le_u64(&mut self) -> io::Result<u64> {
        ReadBytesExt::read_u64::<LittleEndian>(self)
    }
    fn read_le_i32(&mut self) -> io::Result<i32> {
        ReadBytesExt::read_i32::<LittleEndian>(self)
    }
    fn read_le_i64(&mut self) -> io::Result<i64> {
        ReadBytesExt::read_i64::<LittleEndian>(self)
    }
    fn read_le_f64(&mut self) -> io::Result<f64> {
        ReadBytesExt::read_f64::<LittleEndian>(self)
    }
}

/// Convenience wrappers for little-endian writes to any [`io::Write`] sink.
///
/// Symmetric counterpart to [`ReadLeExt`].
pub trait WriteLeExt: Write {
    fn write_u8(&mut self, v: u8) -> io::Result<()>;
    fn write_le_u16(&mut self, v: u16) -> io::Result<()>;
    fn write_le_u32(&mut self, v: u32) -> io::Result<()>;
    fn write_le_u64(&mut self, v: u64) -> io::Result<()>;
    fn write_le_i32(&mut self, v: i32) -> io::Result<()>;
    fn write_le_i64(&mut self, v: i64) -> io::Result<()>;
    fn write_le_f64(&mut self, v: f64) -> io::Result<()>;
}

impl<W: Write> WriteLeExt for W {
    fn write_u8(&mut self, v: u8) -> io::Result<()> {
        WriteBytesExt::write_u8(self, v)
    }
    fn write_le_u16(&mut self, v: u16) -> io::Result<()> {
        WriteBytesExt::write_u16::<LittleEndian>(self, v)
    }
    fn write_le_u32(&mut self, v: u32) -> io::Result<()> {
        WriteBytesExt::write_u32::<LittleEndian>(self, v)
    }
    fn write_le_u64(&mut self, v: u64) -> io::Result<()> {
        WriteBytesExt::write_u64::<LittleEndian>(self, v)
    }
    fn write_le_i32(&mut self, v: i32) -> io::Result<()> {
        WriteBytesExt::write_i32::<LittleEndian>(self, v)
    }
    fn write_le_i64(&mut self, v: i64) -> io::Result<()> {
        WriteBytesExt::write_i64::<LittleEndian>(self, v)
    }
    fn write_le_f64(&mut self, v: f64) -> io::Result<()> {
        WriteBytesExt::write_f64::<LittleEndian>(self, v)
    }
}

/// Length-prefixed list codec: `u32` count followed by each element in order.
///
/// One blanket impl serves every `Vec<T>` whose element type implements [`Encode`]:
/// `Vec<IndexEntry>`, `Vec<Type>`, `Vec<(CompositeKey, PageNumber)>`, etc. all use this.
/// Allowed by Rust's orphan rule because `Encode` is local to this crate even though
/// `Vec<T>` is not.
impl<T: Encode> Encode for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let n = u32::try_from(self.len()).map_err(|_| CodecError::NumericDoesNotFit {
            value: u64::try_from(self.len()).unwrap_or(u64::MAX),
            target: "u32",
        })?;
        writer.write_le_u32(n)?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let n = reader.read_le_u32()?;
        (0..n).map(|_| T::decode(reader)).collect()
    }
}
