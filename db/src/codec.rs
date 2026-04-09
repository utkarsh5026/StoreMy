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

use std::io::{Read, Write};
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
