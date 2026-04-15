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

/// Converts a `u32` to a [`Type`] by matching a numeric tag to the corresponding variant.
///
/// Returns a [`TypeError::UnsupportedType`] if the value does not correspond to any known tag.
impl TryFrom<u32> for Type {
    type Error = TypeError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Type::Int32),
            1 => Ok(Type::Int64),
            2 => Ok(Type::Uint32),
            3 => Ok(Type::Uint64),
            4 => Ok(Type::Float64),
            5 => Ok(Type::String),
            6 => Ok(Type::Bool),
            _ => Err(TypeError::UnsupportedType {
                message: format!("Unsupported type: {value}"),
            }),
        }
    }
}

/// Converts a [`Type`] to its corresponding `u32` tag.
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
        }
    }
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
        write!(f, "{name}")
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

macro_rules! impl_value_cmp {
    ($($variant:ident),* $(,)?) => {
        /// Compares two values for equality.
        ///
        /// Two values are equal only when they are the same variant holding the same
        /// data. Comparisons across different non-null variants always return `false`.
        /// `NULL == NULL` returns `true` (unlike SQL semantics, which return `UNKNOWN`).
        impl PartialEq for Value {
            fn eq(&self, other: &Self) -> bool {
                match (self, other) {
                    $(
                        (Value::$variant(a), Value::$variant(b)) => a == b,
                    )*
                    (Value::Null, Value::Null) => true,
                    _ => false,
                }
            }
        }

        /// Orders values within the same type, with `NULL` sorting before everything else.
        ///
        /// Comparisons between different non-null types return `None` (incomparable).
        /// `Float64` ordering follows [`f64::partial_cmp`], so `NaN` produces `None`.
        impl PartialOrd for Value {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                match (self, other) {
                    $((Value::$variant(a), Value::$variant(b)) => a.partial_cmp(b),)*
                    (Value::Null, Value::Null) => Some(Ordering::Equal),
                    (Value::Null, _)           => Some(Ordering::Less),
                    (_, Value::Null)           => Some(Ordering::Greater),
                    _ => None,
                }
            }
        }
    };
}

impl_value_cmp! { Int32, Int64, Uint32, Uint64, Float64, String, Bool }

impl Eq for Value {}

macro_rules! impl_value_hash_display {
    (
        hash_default { $( $hd_variant:ident ),* $(,)? }
        hash_custom { $( $hc_variant:ident => |$hc_v:ident, $hc_state:ident| $hc_body:expr ),* $(,)? }
        display_default { $( $dd_variant:ident ),* $(,)? }
        display_custom { $( $dc_variant:ident => |$dc_v:ident, $dc_f:ident| $dc_body:expr ),* $(,)? }
    ) => {
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
                    $(
                        Value::$hd_variant(v) => v.hash(state),
                    )*
                    $(
                        Value::$hc_variant($hc_v) => { let $hc_state = state; $hc_body }
                    ),*
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
                    $(
                        Value::$dd_variant(v) => write!(f, "{v}"),
                    )*
                    $(
                        Value::$dc_variant($dc_v) => { let $dc_f = f; $dc_body }
                    ),*
                    Value::Null => write!(f, "NULL"),
                }
            }
        }
    };
}

impl_value_hash_display! {
    hash_default { Int32, Int64, Uint32, Uint64, String, Bool }
    hash_custom {
        Float64 => |v, state| v.to_bits().hash(state),
    }
    display_default { Int32, Int64, Uint32, Uint64, Float64, Bool }
    display_custom {
        String => |v, f| write!(f, "'{v}'"),
    }
}

/// Implements `From<T>` for [`Value`] for a list of Rust types.
///
/// The macro expects each mapping as `<rust_type> => <ValueVariant>`.
macro_rules! impl_from_value {
    ($($rust_type:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$rust_type> for Value {
                #[inline]
                fn from(v: $rust_type) -> Self {
                    Value::$variant(v)
                }
            }
        )*
    };
}

impl_from_value! {
    i32   => Int32,
    i64   => Int64,
    u32   => Uint32,
    u64   => Uint64,
    f64   => Float64,
    String => String,
    bool  => Bool,
}

/// Adds two values, widening integers to `Int64` and floats to `Float64`.
///
/// Mixed integer/float combinations widen to `Float64`. Any unsupported
/// combination (e.g. adding a string to a number) returns [`Value::Null`].
///
/// Note: `NULL + anything` returns `NULL`. Callers that want SQL NULL-skip
/// behavior (like `SUM`) should check [`Value::is_null`] before calling `+`.
impl std::ops::Add<&Value> for Value {
    type Output = Value;

    fn add(self, rhs: &Value) -> Value {
        match (self, rhs) {
            (Value::Int32(x), Value::Int32(y)) => Value::Int64(i64::from(x) + i64::from(*y)),
            (Value::Int64(x), Value::Int32(y)) => Value::Int64(x + i64::from(*y)),
            (Value::Int32(x), Value::Int64(y)) => Value::Int64(i64::from(x) + y),
            (Value::Int64(x), Value::Int64(y)) => Value::Int64(x + y),
            (Value::Uint32(x), Value::Uint32(y)) => Value::Int64(i64::from(x) + i64::from(*y)),
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            (Value::Uint64(x), Value::Uint64(y)) => {
                Value::Int64(x.cast_signed() + (*y).cast_signed())
            }
            (Value::Float64(x), Value::Float64(y)) => Value::Float64(x + y),
            (Value::Float64(x), Value::Int32(y)) => Value::Float64(x + f64::from(*y)),
            #[allow(clippy::cast_precision_loss)]
            (Value::Float64(x), Value::Int64(y)) => Value::Float64(x + *y as f64),
            _ => Value::Null,
        }
    }
}

/// Converts a numeric [`Value`] to `f64`, returning `Err(())` for non-numeric or `NULL` values.
///
/// All integer variants are cast to `f64` (which may lose precision for very
/// large 64-bit integers). Strings, booleans, and `NULL` always return `Err(())`.
impl TryFrom<&Value> for f64 {
    type Error = ();

    fn try_from(val: &Value) -> Result<f64, ()> {
        match val {
            Value::Int32(v) => Ok(f64::from(*v)),
            Value::Uint32(v) => Ok(f64::from(*v)),
            Value::Float64(v) => Ok(*v),
            #[allow(clippy::cast_precision_loss)]
            Value::Int64(v) => Ok(*v as f64),
            #[allow(clippy::cast_precision_loss)]
            Value::Uint64(v) => Ok(*v as f64),
            _ => Err(()),
        }
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
