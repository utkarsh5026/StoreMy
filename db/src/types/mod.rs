//! Type system for `StoreMy` database.
//!
//! This module defines the supported data types and the [`Value`] enum
//! that represents runtime values in the database.
//!
//! There are two layers:
//!
//! - [`Type`] — a compile-time descriptor of what *kind* of value a column holds.
//! - [`Value`] — an owned runtime value that carries actual data, including [`Value::Null`].
//!
//! Both types support serialization using little-endian encoding (via the
//! `byteorder` crate). Each [`Value`] is written as its [`Type`] tag (4 bytes)
//! followed by the payload; strings use a 4-byte length prefix plus UTF-8 bytes,
//! capped at [`crate::STRING_MAX_SIZE`] for [`Type::String`].
//!
//! Implementation is split across this file ([`Type`]) and [`value`].

use std::{
    fmt,
    io::{Read, Write},
};

use thiserror::Error;

use crate::codec::{CodecError, Decode, Encode};

pub mod value;

pub use value::{ArithmeticError, DynValue, FixedValue, OverflowPointer, Value};

/// Errors related to the type system and value conversions.
#[derive(Error, Debug)]
pub enum TypeError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    Mismatch { expected: String, actual: String },

    #[error("Cannot convert {from} to {to}")]
    InvalidConversion { from: String, to: String },

    #[error("Null value not allowed for column {column}")]
    NullNotAllowed { column: String },

    #[error("Unsupported type: {message}")]
    UnsupportedType { message: String },
}

impl TypeError {
    pub(crate) fn invalid_conversion<F, T>(from: F, to: T) -> Self
    where
        F: fmt::Display,
        T: fmt::Display,
    {
        Self::InvalidConversion {
            from: from.to_string(),
            to: to.to_string(),
        }
    }
}

/// The data type of a column or expression.
///
/// `Type` is a lightweight, `Copy` descriptor used in schema definitions and
/// during serialization/deserialization to interpret raw bytes correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Type {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Float64,
    String,
    Bool,
    Text,
    Date,
    Time,
    Timestamp,
    Json,
}

/// Converts a `u32` to a [`Type`] by matching a numeric tag to the corresponding variant.
///
/// Returns a [`TypeError::UnsupportedType`] if the value does not correspond to any known tag.
impl TryFrom<u32> for Type {
    type Error = TypeError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Int32),
            1 => Ok(Self::Int64),
            2 => Ok(Self::Uint32),
            3 => Ok(Self::Uint64),
            4 => Ok(Self::Float64),
            5 => Ok(Self::String),
            6 => Ok(Self::Bool),
            7 => Ok(Self::Text),
            8 => Ok(Self::Date),
            9 => Ok(Self::Time),
            10 => Ok(Self::Timestamp),
            11 => Ok(Self::Json),
            _ => Err(TypeError::UnsupportedType {
                message: format!("Unsupported type: {value}"),
            }),
        }
    }
}

impl From<Type> for u32 {
    fn from(value: Type) -> Self {
        match value {
            Type::Int32 => 0,
            Type::Int64 => 1,
            Type::Uint32 => 2,
            Type::Uint64 => 3,
            Type::Float64 => 4,
            Type::String => 5,
            Type::Bool => 6,
            Type::Text => 7,
            Type::Date => 8,
            Type::Time => 9,
            Type::Timestamp => 10,
            Type::Json => 11,
        }
    }
}

impl Encode for Type {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        u32::from(*self).encode(w)
    }
}

/// Decodes a [`Type`] from a little-endian `u32` tag.
///
/// Validates that the tag matches a supported variant; otherwise, returns a [`CodecError`]
/// describing the issue.
///
/// # Errors
///
/// - [`CodecError::UnknownDiscriminant`] if the tag can fit in a `u8` but does not correspond to a
///   known [`Type`] variant.
/// - [`CodecError::NumericDoesNotFit`] if the tag does not fit in a `u8`.
/// - Any I/O error returned by the reader.
impl Decode for Type {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let tag = u32::decode(r)?;
        Type::try_from(tag).map_err(|_| match u8::try_from(tag) {
            Ok(tag_u8) => CodecError::UnknownDiscriminant(tag_u8),
            Err(_) => CodecError::numeric_does_not_fit(tag as usize, "u8"),
        })
    }
}

impl Type {
    /// Returns `true` if values of this type always occupy the same number of bytes.
    ///
    /// [`Type::String`], [`Type::Text`], and [`Type::Json`] are variable-length
    /// (length-prefixed UTF-8). Fixed-size columns can be accessed by direct
    /// offset arithmetic without scanning a length prefix.
    pub const fn is_fixed_size(&self) -> bool {
        !matches!(self, Type::String | Type::Text | Type::Json)
    }
}

impl TryFrom<&str> for Type {
    type Error = TypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "INT" | "INTEGER" | "INT32" => Ok(Self::Int32),
            "BIGINT" | "INT64" => Ok(Self::Int64),
            "UINT" | "UINT32" => Ok(Self::Uint32),
            "UBIGINT" | "UINT64" => Ok(Self::Uint64),
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Ok(Self::Float64),
            "VARCHAR" | "STRING" => Ok(Self::String),
            "TEXT" => Ok(Self::Text),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "JSON" => Ok(Self::Json),
            _ => Err(TypeError::UnsupportedType {
                message: format!("Unsupported type name: {value}"),
            }),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Type::Int32 => "INT",
            Type::Int64 => "BIGINT",
            Type::Uint32 => "INT UNSIGNED",
            Type::Uint64 => "BIGINT UNSIGNED",
            Type::Float64 => "DOUBLE",
            Type::String => "VARCHAR",
            Type::Text => "TEXT",
            Type::Bool => "BOOLEAN",
            Type::Date => "DATE",
            Type::Time => "TIME",
            Type::Timestamp => "TIMESTAMP",
            Type::Json => "JSON",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod type_fixed_size_tests {
    use super::Type;

    #[test]
    fn variable_length_types_are_not_fixed_size() {
        assert!(!Type::String.is_fixed_size());
        assert!(!Type::Text.is_fixed_size());
        assert!(!Type::Json.is_fixed_size());
    }

    #[test]
    fn scalar_types_are_fixed_size() {
        assert!(Type::Int32.is_fixed_size());
        assert!(Type::Int64.is_fixed_size());
        assert!(Type::Bool.is_fixed_size());
        assert!(Type::Timestamp.is_fixed_size());
    }
}
