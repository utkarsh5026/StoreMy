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
//! Both types support serialization to and from raw byte slices using little-endian
//! encoding (via the `byteorder` crate). Strings are stored as a 4-byte length prefix
//! followed by the UTF-8 bytes, capped at [`crate::STRING_MAX_SIZE`].

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use byteorder::{ByteOrder, LittleEndian};
use thiserror::Error;

use crate::STRING_MAX_SIZE;

/// Errors that can occur while serializing or deserializing a [`Value`].
#[derive(Error, Debug)]
pub enum SerializationError {
    /// The destination buffer is too small to hold the encoded value.
    #[error("Buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall {
        /// Number of bytes required.
        needed: usize,
        /// Number of bytes actually available.
        available: usize,
    },

    /// The byte slice could not be interpreted as the expected type.
    ///
    /// This covers cases like an invalid UTF-8 sequence inside a string field
    /// or a length prefix that claims more bytes than the buffer contains.
    #[error("Deserialization error: {message}")]
    DeserializationError {
        /// Human-readable description of what went wrong.
        message: String,
    },
}

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
}

impl Type {
    /// Returns the on-disk size of this type in bytes.
    ///
    /// For all fixed-width types this is the exact byte count. For [`Type::String`]
    /// it is the *maximum* possible size: a 4-byte length prefix plus
    /// [`crate::STRING_MAX_SIZE`] payload bytes. Use [`Value::serialized_size`] when
    /// you need the actual size of a specific string value.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::Type;
    ///
    /// assert_eq!(Type::Int32.size(), 4);
    /// assert_eq!(Type::Float64.size(), 8);
    /// assert_eq!(Type::Bool.size(), 1);
    /// ```
    pub const fn size(&self) -> usize {
        match self {
            Type::Int32 | Type::Uint32 => 4,
            Type::Int64 | Type::Uint64 | Type::Float64 => 8,
            Type::Bool => 1,
            Type::String => 4 + STRING_MAX_SIZE, // 4-byte length prefix
        }
    }

    /// Returns `true` if values of this type always occupy the same number of bytes.
    ///
    /// Every type except [`Type::String`] is fixed-size. Fixed-size columns can be
    /// accessed by direct offset arithmetic without scanning a length prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::Type;
    ///
    /// assert!(Type::Int64.is_fixed_size());
    /// assert!(!Type::String.is_fixed_size());
    /// ```
    pub const fn is_fixed_size(&self) -> bool {
        !matches!(self, Type::String)
    }
}

/// Parses a SQL-style type name (case-insensitive) into a [`Type`].
///
/// Accepted names per variant:
///
/// | Variant | Accepted strings |
/// |---------|-----------------|
/// | `Int32`   | `INT`, `INTEGER`, `INT32` |
/// | `Int64`   | `BIGINT`, `INT64` |
/// | `Uint32`  | `UINT`, `UINT32` |
/// | `Uint64`  | `UBIGINT`, `UINT64` |
/// | `Float64` | `FLOAT`, `DOUBLE`, `REAL`, `FLOAT64` |
/// | `String`  | `VARCHAR`, `TEXT`, `STRING` |
/// | `Bool`    | `BOOL`, `BOOLEAN` |
///
/// # Errors
///
/// Returns [`TypeError::UnsupportedType`] when `value` does not match any
/// of the accepted strings (unrecognized name).
///
/// # Examples
///
/// ```
/// use db::types::Type;
///
/// assert_eq!(Type::try_from("bigint").unwrap(), Type::Int64);
/// assert!(Type::try_from("uuid").is_err());
/// ```

impl TryFrom<&str> for Type {
    type Error = TypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "INT" | "INTEGER" | "INT32" => Ok(Type::Int32),
            "BIGINT" | "INT64" => Ok(Type::Int64),
            "UINT" | "UINT32" => Ok(Type::Uint32),
            "UBIGINT" | "UINT64" => Ok(Type::Uint64),
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Ok(Type::Float64),
            "VARCHAR" | "TEXT" | "STRING" => Ok(Type::String),
            "BOOL" | "BOOLEAN" => Ok(Type::Bool),
            _ => Err(TypeError::UnsupportedType {
                message: format!("Unsupported type name: {value}"),
            }),
        }
    }
}

/// Formats the type as a standard SQL type name.
///
/// The output matches the primary SQL keyword for each variant:
/// `INT`, `BIGINT`, `INT UNSIGNED`, `BIGINT UNSIGNED`, `DOUBLE`, `VARCHAR`, `BOOLEAN`.
impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Type::Int32 => "INT",
            Type::Int64 => "BIGINT",
            Type::Uint32 => "INT UNSIGNED",
            Type::Uint64 => "BIGINT UNSIGNED",
            Type::Float64 => "DOUBLE",
            Type::String => "VARCHAR",
            Type::Bool => "BOOLEAN",
        };
        write!(f, "{}", name)
    }
}

/// An owned runtime value stored in or retrieved from the database.
///
/// Each variant corresponds directly to a [`Type`] variant, plus [`Value::Null`]
/// which represents the absence of a value (SQL `NULL`).
///
/// `Value` implements [`PartialOrd`] with the convention that `NULL` sorts before
/// all other values, and that comparisons between different non-null types return
/// `None` (incomparable).
#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Float64(f64),
    String(String),
    Bool(bool),
    Null,
}

impl Value {
    /// Returns the [`Type`] of this value, or `None` if the value is `NULL`.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::{Type, Value};
    ///
    /// assert_eq!(Value::Int32(42).get_type(), Some(Type::Int32));
    /// assert_eq!(Value::Null.get_type(), None);
    /// ```
    pub fn get_type(&self) -> Option<Type> {
        match self {
            Value::Int32(_) => Some(Type::Int32),
            Value::Int64(_) => Some(Type::Int64),
            Value::Uint32(_) => Some(Type::Uint32),
            Value::Uint64(_) => Some(Type::Uint64),
            Value::Float64(_) => Some(Type::Float64),
            Value::String(_) => Some(Type::String),
            Value::Bool(_) => Some(Type::Bool),
            Value::Null => None,
        }
    }

    /// Returns `true` if this value is `NULL`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the number of bytes this value occupies when serialized.
    ///
    /// For strings, this is `4 + string.len()` (length prefix plus payload),
    /// which may be smaller than [`Type::String.size()`] if the string is
    /// shorter than the maximum. For `NULL`, this is `0`.
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Int32(_) | Value::Uint32(_) => 4,
            Value::Int64(_) | Value::Uint64(_) | Value::Float64(_) => 8,
            Value::Bool(_) => 1,
            Value::String(s) => 4 + s.len(), // 4-byte length prefix
            Value::Null => 0,
        }
    }

    /// Writes this value into `buf` using little-endian encoding.
    ///
    /// Returns the number of bytes written. The caller is responsible for
    /// ensuring `buf` is at least [`Value::serialized_size`] bytes long; if
    /// `buf` is too small the write will panic (via `byteorder`'s slice
    /// operations) or produce a truncated string.
    ///
    /// `NULL` writes zero bytes and returns `Ok(0)`.
    ///
    /// Strings are truncated to [`crate::STRING_MAX_SIZE`] bytes silently if
    /// they exceed the limit.
    ///
    /// # Errors
    ///
    /// Returns [`SerializationError::DeserializationError`] if the string
    /// length cannot be represented as a `u32` (i.e. it exceeds `u32::MAX`
    /// bytes before truncation — not possible in practice given
    /// `STRING_MAX_SIZE`, but checked for correctness).
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::Value;
    ///
    /// let mut buf = [0u8; 4];
    /// let written = Value::Int32(7).serialize(&mut buf).unwrap();
    /// assert_eq!(written, 4);
    /// assert_eq!(buf, [7, 0, 0, 0]); // little-endian
    /// ```
    pub fn serialize(&self, buf: &mut [u8]) -> Result<usize, SerializationError> {
        match self {
            Value::Int32(v) => {
                LittleEndian::write_i32(buf, *v);
                Ok(4)
            }
            Value::Int64(v) => {
                LittleEndian::write_i64(buf, *v);
                Ok(8)
            }
            Value::Uint32(v) => {
                LittleEndian::write_u32(buf, *v);
                Ok(4)
            }
            Value::Uint64(v) => {
                LittleEndian::write_u64(buf, *v);
                Ok(8)
            }
            Value::Float64(v) => {
                LittleEndian::write_f64(buf, *v);
                Ok(8)
            }
            Value::Bool(v) => {
                buf[0] = u8::from(*v);
                Ok(1)
            }
            Value::String(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len().min(STRING_MAX_SIZE);
                let len_u32 =
                    u32::try_from(len).map_err(|_| SerializationError::DeserializationError {
                        message: "String length exceeds u32::MAX".to_string(),
                    })?;
                LittleEndian::write_u32(buf, len_u32);
                buf[4..4 + len].copy_from_slice(&bytes[..len]);
                Ok(4 + len)
            }
            Value::Null => Ok(0),
        }
    }

    /// Checks that `buf` contains at least `needed` bytes.
    ///
    /// Returns an error if the buffer is too small.
    #[inline]
    fn ensure_buffer_size(buf: &[u8], needed: usize) -> Result<(), SerializationError> {
        if buf.len() < needed {
            return Err(SerializationError::BufferTooSmall {
                needed,
                available: buf.len(),
            });
        }
        Ok(())
    }

    /// Reads a value of the given [`Type`] from the front of `buf`.
    ///
    /// The bytes are interpreted using little-endian encoding, matching the
    /// format written by [`Value::serialize`]. For strings, the first 4 bytes
    /// are a little-endian `u32` length, followed by that many UTF-8 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SerializationError::BufferTooSmall`] when `buf` does not
    /// contain enough bytes for the requested type, or
    /// [`SerializationError::DeserializationError`] when a string payload
    /// contains invalid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::{Type, Value};
    ///
    /// let buf = [42u8, 0, 0, 0];
    /// let v = Value::deserialize(&buf, Type::Int32).unwrap();
    /// assert_eq!(v, Value::Int32(42));
    /// ```
    pub fn deserialize(buf: &[u8], typ: Type) -> Result<Self, SerializationError> {
        match typ {
            Type::Int32 => {
                Self::ensure_buffer_size(buf, 4)?;
                Ok(Value::Int32(LittleEndian::read_i32(buf)))
            }
            Type::Int64 => {
                Self::ensure_buffer_size(buf, 8)?;
                Ok(Value::Int64(LittleEndian::read_i64(buf)))
            }
            Type::Uint32 => {
                Self::ensure_buffer_size(buf, 4)?;
                Ok(Value::Uint32(LittleEndian::read_u32(buf)))
            }
            Type::Uint64 => {
                Self::ensure_buffer_size(buf, 8)?;
                Ok(Value::Uint64(LittleEndian::read_u64(buf)))
            }
            Type::Float64 => {
                Self::ensure_buffer_size(buf, 8)?;
                Ok(Value::Float64(LittleEndian::read_f64(buf)))
            }
            Type::Bool => {
                Self::ensure_buffer_size(buf, 1)?;
                Ok(Value::Bool(buf[0] != 0))
            }
            Type::String => {
                Self::ensure_buffer_size(buf, 4)?;
                let len = LittleEndian::read_u32(buf) as usize;
                Self::ensure_buffer_size(buf, 4 + len)?;
                let s = std::str::from_utf8(&buf[4..4 + len]).map_err(|e| {
                    SerializationError::DeserializationError {
                        message: format!("Invalid UTF-8: {e}"),
                    }
                })?;
                Ok(Value::String(s.to_string()))
            }
        }
    }

    /// Converts this value to an `i64` if it holds any integer type.
    ///
    /// Returns `None` for [`Value::Float64`], [`Value::String`], [`Value::Bool`],
    /// and [`Value::Null`]. For [`Value::Uint64`], returns `None` if the value
    /// exceeds `i64::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::Value;
    ///
    /// assert_eq!(Value::Int32(-1).as_i64(), Some(-1));
    /// assert_eq!(Value::Uint32(100).as_i64(), Some(100));
    /// assert_eq!(Value::Float64(1.5).as_i64(), None);
    /// ```
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Uint32(v) => Some(*v as i64),
            Value::Uint64(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Converts this value to an `f64` if it holds any numeric type.
    ///
    /// All integer variants are cast to `f64` (which may lose precision for
    /// very large 64-bit integers). Returns `None` for [`Value::String`],
    /// [`Value::Bool`], and [`Value::Null`].
    ///
    /// # Examples
    ///
    /// ```
    /// use db::types::Value;
    ///
    /// assert_eq!(Value::Int32(3).as_f64(), Some(3.0));
    /// assert_eq!(Value::Float64(2.5).as_f64(), Some(2.5));
    /// assert_eq!(Value::Bool(true).as_f64(), None);
    /// ```
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Uint32(v) => Some(*v as f64),
            Value::Uint64(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrows the inner string slice if this value is a [`Value::String`].
    ///
    /// Returns `None` for all other variants.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the inner `bool` if this value is a [`Value::Bool`].
    ///
    /// Returns `None` for all other variants.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

/// Compares two values for equality.
///
/// Two values are equal only when they are the same variant holding the same
/// data. Comparisons across different non-null variants always return `false`.
/// `NULL == NULL` returns `true` (unlike SQL semantics, which return `UNKNOWN`).
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Uint32(a), Value::Uint32(b)) => a == b,
            (Value::Uint64(a), Value::Uint64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Orders values within the same type, with `NULL` sorting before everything else.
///
/// Comparisons between different non-null types return `None` (incomparable).
/// `Float64` ordering follows [`f64::partial_cmp`], so `NaN` produces `None`.
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Uint32(a), Value::Uint32(b)) => a.partial_cmp(b),
            (Value::Uint64(a), Value::Uint64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less), // NULL sorts first
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None, // Different types are not comparable
        }
    }
}

/// Hashes a value consistently with its [`PartialEq`] implementation.
///
/// `Float64` is hashed by its bit pattern (`f64::to_bits`), so two `NaN`
/// values with the same bit pattern hash equal, even though `NaN != NaN`
/// under IEEE 754. The discriminant is always mixed in first so that, e.g.,
/// `Int32(1)` and `Int64(1)` produce different hashes.
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Int32(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::Uint32(v) => v.hash(state),
            Value::Uint64(v) => v.hash(state),
            Value::Float64(v) => v.to_bits().hash(state),
            Value::String(v) => v.hash(state),
            Value::Bool(v) => v.hash(state),
            Value::Null => {}
        }
    }
}

/// Formats the value in a SQL-like representation.
///
/// Strings are wrapped in single quotes (`'hello'`). `NULL` is printed as
/// the literal `NULL`. All numeric and boolean variants use their standard
/// Rust `Display` format.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Uint32(v) => write!(f, "{v}"),
            Value::Uint64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "'{v}'"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Wraps an `i32` as [`Value::Int32`].
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

/// Wraps an `i64` as [`Value::Int64`].
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

/// Wraps a `u32` as [`Value::Uint32`].
impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Uint32(v)
    }
}

/// Wraps a `u64` as [`Value::Uint64`].
impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Uint64(v)
    }
}

/// Wraps an `f64` as [`Value::Float64`].
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

/// Converts an owned `String` into [`Value::String`].
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

/// Converts a string slice into [`Value::String`] by cloning the contents.
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

/// Wraps a `bool` as [`Value::Bool`].
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

/// Converts an `Option<T>` into a `Value`, mapping `None` to [`Value::Null`].
///
/// This lets you write `Value::from(some_option)` for any `T: Into<Value>`.
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}
