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

use std::{
    io::{Read, Write},
    path::PathBuf,
};

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
    pub fn numeric_does_not_fit(value: usize, target: &'static str) -> Self {
        Self::NumericDoesNotFit {
            value: u64::try_from(value).unwrap_or(u64::MAX),
            target,
        }
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

impl Encode for u8 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(std::slice::from_ref(self))?;
        Ok(())
    }
}

impl Decode for u8 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        Ok(byte[0])
    }
}

impl Encode for i8 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_u8(writer, self.cast_unsigned())?;
        Ok(())
    }
}

impl Decode for i8 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_u8(reader)?.cast_signed())
    }
}

impl Encode for u16 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_u16::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for u16 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_u16::<LittleEndian>(reader)?)
    }
}

impl Encode for i16 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_i16::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for i16 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_i16::<LittleEndian>(reader)?)
    }
}

impl Encode for u32 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_u32::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for u32 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_u32::<LittleEndian>(reader)?)
    }
}

impl Encode for i32 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_i32::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for i32 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_i32::<LittleEndian>(reader)?)
    }
}

impl Encode for u64 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_u64::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for u64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_u64::<LittleEndian>(reader)?)
    }
}

impl Encode for i64 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_i64::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for i64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_i64::<LittleEndian>(reader)?)
    }
}

impl Encode for f64 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_f64::<LittleEndian>(writer, *self)?;
        Ok(())
    }
}

impl Decode for f64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_f64::<LittleEndian>(reader)?)
    }
}

impl Encode for String {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let len = u32::try_from(self.len())
            .map_err(|_| CodecError::numeric_does_not_fit(self.len(), "u32"))?;
        WriteBytesExt::write_u32::<LittleEndian>(writer, len)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl Decode for String {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = ReadBytesExt::read_u32::<LittleEndian>(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| CodecError::InvalidUtf8(e.utf8_error()))
    }
}

impl Encode for bool {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        WriteBytesExt::write_u8(writer, u8::from(*self))?;
        Ok(())
    }
}

impl Decode for bool {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(ReadBytesExt::read_u8(reader)? != 0)
    }
}

impl Encode for PathBuf {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let bytes = self.as_os_str().as_encoded_bytes();
        let len = u32::try_from(bytes.len())
            .map_err(|_| CodecError::numeric_does_not_fit(bytes.len(), "u32"))?;
        WriteBytesExt::write_u32::<LittleEndian>(writer, len)?;
        writer.write_all(bytes)?;
        Ok(())
    }
}

impl Decode for PathBuf {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(PathBuf::from(String::decode(reader)?))
    }
}

/// Generates `Encode` and `Decode` impls for tuple types.
///
/// Each invocation receives a list of `(index, TypeParam)` pairs — the index
/// becomes the tuple field accessor (`self.0`, `self.1`, …) and the type param
/// names the generic bound.  Both traits are emitted in a single call so the
/// arity list only needs to be written once.
///
/// ```ignore
/// impl_codec_tuple!((0, A), (1, B));           // (A, B)
/// impl_codec_tuple!((0, A), (1, B), (2, C));   // (A, B, C)
/// ```
macro_rules! impl_codec_tuple {
    ( $( ($idx:tt, $T:ident) ),+ ) => {
        impl<$( $T: Encode ),+> Encode for ($( $T, )+) {
            fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
                $( self.$idx.encode(writer)?; )+
                Ok(())
            }
        }

        impl<$( $T: Decode ),+> Decode for ($( $T, )+) {
            fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
                Ok(( $( $T::decode(reader)?, )+ ))
            }
        }
    };
}

impl_codec_tuple!((0, A), (1, B));
impl_codec_tuple!((0, A), (1, B), (2, C));
impl_codec_tuple!((0, A), (1, B), (2, C), (3, D));

impl<T: Encode> Encode for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let n = u32::try_from(self.len()).map_err(|_| CodecError::NumericDoesNotFit {
            value: u64::try_from(self.len()).unwrap_or(u64::MAX),
            target: "u32",
        })?;
        n.encode(writer)?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let n = u32::decode(reader)?;
        (0..n).map(|_| T::decode(reader)).collect()
    }
}

impl Encode for serde_json::Value {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let s = serde_json::to_string(self)
            .map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
        s.encode(writer)
    }
}

impl Decode for serde_json::Value {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let s = String::decode(reader)?;
        serde_json::from_str(&s)
            .map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
    }
}
