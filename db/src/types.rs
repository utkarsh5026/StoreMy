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

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    io::{Read, Write},
    mem::size_of,
    ops::{Add, Mul, Sub},
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use thiserror::Error;

use crate::{
    STRING_MAX_SIZE,
    codec::{CodecError, Decode, Encode},
    primitives::PageDescriptor,
};

/// Wire tag for [`Value::Null`]; not a [`Type`] variant.
const NULL_VALUE_TAG: u32 = u32::MAX;

/// Little-endian `u32` written before every [`Value`] payload ([`Type`] tag or [`NULL_VALUE_TAG`]).
const VALUE_TAG_SIZE: usize = size_of::<u32>();

/// u32 length prefix before inline UTF-8 bytes in [`DynValue::Varchar`] / [`DynValue::Text`].
const STRING_LENGTH_PREFIX_SIZE: usize = size_of::<u32>();

const BOOL_PAYLOAD_SIZE: usize = size_of::<u8>();
const I32_PAYLOAD_SIZE: usize = size_of::<i32>();
const I64_PAYLOAD_SIZE: usize = size_of::<i64>();

/// On-disk payload for [`DynValue::TextOverflow`]: sentinel u32, `total_len` u32, then
/// [`PageDescriptor`].
const TEXT_OVERFLOW_PAYLOAD_SIZE: usize =
    STRING_LENGTH_PREFIX_SIZE + STRING_LENGTH_PREFIX_SIZE + PageDescriptor::SIZE;

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

/// Errors from [`Value::checked_add`] and [`Value::checked_div`].
///
/// Subtraction and multiplication use [`Sub`] and [`Mul`]; those operators return
/// [`Value::Null`] on type mismatch, which is enough because neither can fail with
/// division-by-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticError {
    TypeMismatch,
    DivisionByZero,
}

impl TypeError {
    fn invalid_conversion<F, T>(from: F, to: T) -> Self
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
    /// [`Type::String`] and [`Type::Text`] are variable-length (length-prefixed UTF-8).
    /// Fixed-size columns can be accessed by direct offset arithmetic without
    /// scanning a length prefix.
    pub const fn is_fixed_size(&self) -> bool {
        !matches!(self, Type::String | Type::Text)
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
        };
        write!(f, "{name}")
    }
}

/// A fixed-width scalar — size is statically known.
#[derive(Debug, Clone, PartialEq)]
pub enum FixedValue {
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Float64(f64),
    Bool(bool),
    Date(i32),
    Time(i64),
    Timestamp(i64),
}

impl FixedValue {
    /// Returns the byte size of the value represented by this variant.
    ///
    /// - `Int32`, `Uint32`, `Date`: 4 bytes
    /// - `Int64`, `Uint64`, `Float64`, `Time`, `Timestamp`: 8 bytes
    /// - `Bool`: 1 byte
    pub const fn size(&self) -> usize {
        match self {
            Self::Int32(_) | Self::Uint32(_) | Self::Date(_) => I32_PAYLOAD_SIZE,
            Self::Int64(_)
            | Self::Uint64(_)
            | Self::Float64(_)
            | Self::Time(_)
            | Self::Timestamp(_) => I64_PAYLOAD_SIZE,
            Self::Bool(_) => BOOL_PAYLOAD_SIZE,
        }
    }

    fn get_type(&self) -> Type {
        match self {
            Self::Int32(_) => Type::Int32,
            Self::Int64(_) => Type::Int64,
            Self::Uint32(_) => Type::Uint32,
            Self::Uint64(_) => Type::Uint64,
            Self::Float64(_) => Type::Float64,
            Self::Bool(_) => Type::Bool,
            Self::Date(_) => Type::Date,
            Self::Time(_) => Type::Time,
            Self::Timestamp(_) => Type::Timestamp,
        }
    }

    /// Writes the little-endian scalar payload for this variant (no [`Type`] tag).
    ///
    /// Used by [`Encode for FixedValue`]. Tagged [`Value`] serialization writes the
    /// 4-byte type tag first, then calls this for the payload.
    ///
    /// Dispatches on [`Self::size`], then encodes the inner scalar with [`Encode`]:
    ///
    /// - 1 byte — `Bool`
    /// - 4 bytes — `Int32`, `Uint32`, `Date` (`i32` days since Unix epoch)
    /// - 8 bytes — `Int64`, `Uint64`, `Float64`, `Time`, `Timestamp` (`i64` micros since midnight
    ///   or Unix epoch)
    fn encode_payload<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self.size() {
            BOOL_PAYLOAD_SIZE => {
                let Self::Bool(v) = self else { unreachable!() };
                v.encode(w)
            }
            I32_PAYLOAD_SIZE => match self {
                Self::Int32(v) | Self::Date(v) => v.encode(w),
                Self::Uint32(v) => v.encode(w),
                _ => unreachable!(),
            },
            I64_PAYLOAD_SIZE => match self {
                Self::Int64(v) | Self::Time(v) | Self::Timestamp(v) => v.encode(w),
                Self::Uint64(v) => v.encode(w),
                Self::Float64(v) => v.encode(w),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn checked_add(&self, rhs: &Self) -> Result<Self, ArithmeticError> {
        match (self, rhs) {
            (Self::Int64(a), Self::Int64(b)) => Ok(Self::Int64(a.wrapping_add(*b))),
            (Self::Uint64(a), Self::Uint64(b)) => Ok(Self::Uint64(a.wrapping_add(*b))),
            (Self::Float64(a), Self::Float64(b)) => Ok(Self::Float64(a + b)),
            _ => Err(ArithmeticError::TypeMismatch),
        }
    }

    /// SQL expression `/` on two non-null operands of the same numeric kind.
    ///
    /// Integer division returns [`ArithmeticError::DivisionByZero`] when the
    /// divisor is zero; float division follows IEEE rules.
    pub fn checked_div(&self, rhs: &Self) -> Result<Self, ArithmeticError> {
        match (self, rhs) {
            (Self::Int64(a), Self::Int64(b)) => {
                if *b == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                Ok(Self::Int64(a / b))
            }
            (Self::Uint64(a), Self::Uint64(b)) => {
                if *b == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                Ok(Self::Uint64(a / b))
            }
            (Self::Float64(a), Self::Float64(b)) => Ok(Self::Float64(a / b)),
            _ => Err(ArithmeticError::TypeMismatch),
        }
    }

    /// Parses a SQL-style date literal into [`FixedValue::Date`].
    ///
    /// Accepts `YYYY-MM-DD` (leading/trailing whitespace is trimmed). The stored
    /// payload is an `i32` count of whole days since the Unix epoch (`1970-01-01`).
    ///
    /// Returns `None` if the string does not match the format or the day count
    /// does not fit in `i32`.
    pub fn parse_date(s: &str) -> Option<Self> {
        let d = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let days = i32::try_from(d.signed_duration_since(epoch).num_days()).ok()?;
        Some(Self::Date(days))
    }

    /// Parses a SQL-style time literal into [`FixedValue::Time`].
    ///
    /// Accepts `HH:MM:SS` and `HH:MM:SS.fff` (fractional seconds; whitespace
    /// trimmed). The stored payload is an `i64` count of microseconds since
    /// midnight.
    ///
    /// Returns `None` if the string does not match either format or the duration
    /// cannot be represented as microseconds.
    pub fn parse_time(s: &str) -> Option<Self> {
        let t = NaiveTime::parse_from_str(s.trim(), "%H:%M:%S")
            .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f"))
            .ok()?;
        let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
        Some(Self::Time(
            t.signed_duration_since(midnight).num_microseconds()?,
        ))
    }

    /// Parses a SQL-style timestamp literal into [`FixedValue::Timestamp`].
    ///
    /// Accepts (whitespace trimmed):
    ///
    /// - `YYYY-MM-DD HH:MM:SS`
    /// - `YYYY-MM-DDTHH:MM:SS`
    /// - `YYYY-MM-DD HH:MM:SS.fff` (fractional seconds with a space separator)
    ///
    /// The parsed naive datetime is interpreted as UTC. The stored payload is an
    /// `i64` count of microseconds since the Unix epoch.
    ///
    /// Returns `None` if the string does not match any accepted format.
    pub fn parse_timestamp(s: &str) -> Option<Self> {
        let s = s.trim();
        let f = |format: &'static str| NaiveDateTime::parse_from_str(s, format);

        let dt = f("%Y-%m-%d %H:%M:%S")
            .or_else(|_| f("%Y-%m-%dT%H:%M:%S"))
            .or_else(|_| f("%Y-%m-%d %H:%M:%S%.f"))
            .ok()?;
        Some(Self::Timestamp(dt.and_utc().timestamp_micros()))
    }

    /// Formats [`FixedValue::Date`] storage for [`fmt::Display`].
    ///
    /// Inverse of [`Self::parse_date`]: writes `YYYY-MM-DD` from the `i32` day
    /// count since the Unix epoch.
    fn write_date(days: i32, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is valid");
        let date = epoch + chrono::Duration::days(i64::from(days));
        write!(f, "{}", date.format("%Y-%m-%d"))
    }

    /// Formats [`FixedValue::Time`] storage for [`fmt::Display`].
    ///
    /// Inverse of [`Self::parse_time`]: writes `HH:MM:SS` when the payload is a
    /// whole number of seconds; otherwise `HH:MM:SS.fff` with trimmed fractional
    /// digits.
    fn write_time(micros: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid");
        let time = midnight + chrono::Duration::microseconds(micros);
        if micros % 1_000_000 == 0 {
            write!(f, "{}", time.format("%H:%M:%S"))
        } else {
            write!(f, "{}", time.format("%H:%M:%S%.f"))
        }
    }

    /// Formats [`FixedValue::Timestamp`] storage for [`fmt::Display`].
    ///
    /// Inverse of [`Self::parse_timestamp`]: writes `YYYY-MM-DD HH:MM:SS` in UTC,
    /// or `YYYY-MM-DD HH:MM:SS.fff` when sub-second precision is present. If the
    /// microsecond count is outside the range representable as a UTC datetime,
    /// falls back to printing the raw count.
    fn write_timestamp(micros: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(dt) = DateTime::from_timestamp_micros(micros) else {
            return write!(f, "{micros}");
        };
        let naive = dt.naive_utc();
        if micros % 1_000_000 == 0 {
            write!(f, "{}", naive.format("%Y-%m-%d %H:%M:%S"))
        } else {
            write!(f, "{}", naive.format("%Y-%m-%d %H:%M:%S%.f"))
        }
    }

    /// Parses `s` into a fixed-width [`FixedValue`] of kind `ty`.
    ///
    /// Numeric types use [`str::parse`]; [`Type::Bool`] accepts `true`/`false`/`1`/`0`;
    /// temporal types use [`Self::parse_date`], [`Self::parse_time`], and
    /// [`Self::parse_timestamp`].
    ///
    /// Returns `None` for [`Type::String`] and [`Type::Text`] — callers must build [`DynValue`]
    /// via [`Value::parse_as`].
    pub fn parse_as(s: &str, ty: Type) -> Option<Self> {
        match ty {
            Type::Int32 => s.parse().ok().map(Self::Int32),
            Type::Int64 => s.parse().ok().map(Self::Int64),
            Type::Uint32 => s.parse().ok().map(Self::Uint32),
            Type::Uint64 => s.parse().ok().map(Self::Uint64),
            Type::Float64 => s.parse().ok().map(Self::Float64),
            Type::Bool => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Some(Self::Bool(true)),
                "false" | "0" => Some(Self::Bool(false)),
                _ => None,
            },
            Type::Date => Self::parse_date(s),
            Type::Time => Self::parse_time(s),
            Type::Timestamp => Self::parse_timestamp(s),
            Type::String | Type::Text => None,
        }
    }
}

impl Hash for FixedValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self.size() {
            BOOL_PAYLOAD_SIZE => {
                let Self::Bool(v) = self else { unreachable!() };
                v.hash(state);
            }
            I32_PAYLOAD_SIZE => match self {
                Self::Int32(v) | Self::Date(v) => v.hash(state),
                Self::Uint32(v) => v.hash(state),
                _ => unreachable!(),
            },
            I64_PAYLOAD_SIZE => match self {
                Self::Int64(v) | Self::Time(v) | Self::Timestamp(v) => v.hash(state),
                Self::Uint64(v) => v.hash(state),
                Self::Float64(v) => v.to_bits().hash(state),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for FixedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Uint32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Uint64(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Date(v) => Self::write_date(*v, f),
            Self::Time(v) => Self::write_time(*v, f),
            Self::Timestamp(v) => Self::write_timestamp(*v, f),
        }
    }
}

impl PartialOrd for FixedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Uint32(a), Self::Uint32(b)) => a.partial_cmp(b),
            (Self::Uint64(a), Self::Uint64(b)) => a.partial_cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.partial_cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::Int32(a), Self::Int32(b)) | (Self::Date(a), Self::Date(b)) => a.partial_cmp(b),
            (Self::Int64(a), Self::Int64(b))
            | (Self::Time(a), Self::Time(b))
            | (Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Encode for FixedValue {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.encode_payload(w)
    }
}

/// A variable-width value — size depends on runtime content.
#[derive(Debug, Clone)]
pub enum DynValue {
    Varchar(String),
    Text(String),
    TextOverflow { total_len: u32, ptr: PageDescriptor },
}

impl DynValue {
    pub fn get_type(&self) -> Type {
        match self {
            Self::Varchar(_) => Type::String,
            Self::Text(_) | Self::TextOverflow { .. } => Type::Text,
        }
    }
}

impl PartialEq for DynValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Varchar(a), Self::Varchar(b)) | (Self::Text(a), Self::Text(b)) => a == b,
            // TextOverflow is a storage artifact — never compared in user-visible contexts.
            _ => false,
        }
    }
}

impl PartialOrd for DynValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Varchar(a), Self::Varchar(b)) | (Self::Text(a), Self::Text(b)) => {
                a.partial_cmp(b)
            }
            _ => None,
        }
    }
}

impl fmt::Display for DynValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Varchar(s) | Self::Text(s) => write!(f, "'{s}'"),
            Self::TextOverflow { .. } => write!(f, "<TEXT overflow>"),
        }
    }
}

impl Hash for DynValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Varchar(s) | Self::Text(s) => s.hash(state),
            Self::TextOverflow { .. } => (),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Fixed(FixedValue),
    Dyn(DynValue),
    Null,
}

impl Value {
    pub fn get_type(&self) -> Option<Type> {
        match self {
            Value::Fixed(v) => Some(v.get_type()),
            Value::Dyn(v) => Some(v.get_type()),
            Value::Null => None,
        }
    }

    /// Returns `true` if this value is `NULL`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Borrows the inner string slice if this value is [`DynValue::Varchar`] or [`DynValue::Text`].
    ///
    /// Returns `None` for all other variants.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Dyn(DynValue::Varchar(s) | DynValue::Text(s)) => Some(s),
            _ => None,
        }
    }

    /// Returns the inner `bool` if this value is [`FixedValue::Bool`].
    ///
    /// Returns `None` for all other variants.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Fixed(FixedValue::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    /// Number of bytes [`Encode`] will write for this value.
    ///
    /// Must stay in sync with [`Encode for Value`]: every variant here mirrors
    /// a branch there.
    ///
    /// - [`DynValue::Varchar`] applies the same [`STRING_MAX_SIZE`] truncation the encoder does;
    ///   the reported size is the post-truncation byte count.
    /// - [`DynValue::Text`] reports the **full** string length with no cap — callers are
    ///   responsible for routing oversize TEXT through the overflow path before calling [`Encode`].
    pub fn encoded_size(&self) -> usize {
        VALUE_TAG_SIZE
            + match self {
                Value::Null => 0,
                Value::Fixed(v) => v.size(),
                Value::Dyn(DynValue::Varchar(s)) => {
                    STRING_LENGTH_PREFIX_SIZE + s.len().min(STRING_MAX_SIZE)
                }
                Value::Dyn(DynValue::Text(s)) => STRING_LENGTH_PREFIX_SIZE + s.len(),
                Value::Dyn(DynValue::TextOverflow { .. }) => TEXT_OVERFLOW_PAYLOAD_SIZE,
            }
    }

    /// SQL expression `+` on two non-null operands of the same numeric kind.
    ///
    /// Integer pairs use wrapping arithmetic; [`FixedValue::Float64`] uses IEEE addition.
    /// The [`Add`] operator uses different (widening) rules for aggregate `SUM`.
    pub fn checked_add(&self, rhs: &Self) -> Result<Self, ArithmeticError> {
        match (self, rhs) {
            (Self::Fixed(a), Self::Fixed(b)) => Ok(Self::Fixed(a.checked_add(b)?)),
            _ => Err(ArithmeticError::TypeMismatch),
        }
    }

    /// SQL expression `/` on two non-null operands of the same numeric kind.
    ///
    /// Integer division returns [`ArithmeticError::DivisionByZero`] when the
    /// divisor is zero; float division follows IEEE rules.
    pub fn checked_div(&self, rhs: &Self) -> Result<Self, ArithmeticError> {
        match (self, rhs) {
            (Self::Fixed(a), Self::Fixed(b)) => Ok(Self::Fixed(a.checked_div(b)?)),
            _ => Err(ArithmeticError::TypeMismatch),
        }
    }

    /// Coerces this value to match a column's declared [`Type`].
    ///
    /// After parsing, runtime values often arrive in a generic form — every
    /// integer literal is built via [`Value::int64`], every quoted string via
    /// [`Value::varchar`] — while the target column may declare a narrower or
    /// distinct type. The engine uses this when binding literals to columns
    /// during `INSERT`, `UPDATE`, and similar statements.
    ///
    /// Supported conversions:
    ///
    /// - [`FixedValue::Int64`] → [`Type::Int32`], [`Type::Uint32`], or [`Type::Uint64`] when the
    ///   number fits in the target range.
    /// - [`DynValue::Varchar`] → [`Type::String`] or [`Type::Text`].
    /// - Any value whose [`Self::get_type`] already equals `target` → returned unchanged.
    ///
    /// All other `(value, target)` pairs return [`TypeError::InvalidConversion`].
    ///
    ///
    /// # Errors
    ///
    /// Returns [`TypeError::InvalidConversion`] when `self` cannot be represented
    /// as `target` (out-of-range integer, string into a numeric column, etc.).
    pub fn coerce_to(&self, target: Type) -> Result<Self, TypeError> {
        match (self, target) {
            (Self::Fixed(FixedValue::Int64(n)), Type::Int32) => i32::try_from(*n)
                .map(Self::int32)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Int32)),

            (Self::Fixed(FixedValue::Int64(n)), Type::Int64) => Ok(Self::int64(*n)),

            (Self::Fixed(FixedValue::Int64(n)), Type::Uint32) => u32::try_from(*n)
                .map(Self::uint32)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Uint32)),

            (Self::Fixed(FixedValue::Int64(n)), Type::Uint64) => u64::try_from(*n)
                .map(Self::uint64)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Uint64)),

            (Self::Dyn(DynValue::Varchar(s)), Type::String) => Ok(Self::varchar(s.clone())),
            (Self::Dyn(DynValue::Varchar(s) | DynValue::Text(s)), Type::Text) => {
                Ok(Self::text(s.clone()))
            }

            (Self::Dyn(DynValue::Varchar(s)), ty @ (Type::Date | Type::Time | Type::Timestamp)) => {
                FixedValue::parse_as(s, ty)
                    .map(Self::Fixed)
                    .ok_or_else(|| TypeError::invalid_conversion(s.as_str(), ty))
            }

            (v, ty) if v.get_type() == Some(ty) => Ok(v.clone()),
            (v, ty) => Err(TypeError::invalid_conversion(v, ty)),
        }
    }

    /// Parses a string representation into a [`Value`] of the requested [`Type`].
    ///
    /// The string is interpreted differently depending on `ty`:
    ///
    /// - **Fixed-width types** (`Int32`, `Bool`, `Date`, …) delegate to [`FixedValue::parse_as`].
    /// - **`String` / `Text`** accept the input as-is — the string is just wrapped, not
    ///   interpreted. There is no ambiguity for these two variants.
    ///
    /// Leading and trailing whitespace is trimmed before any conversion.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError::InvalidConversion`] when the string cannot be
    /// interpreted as `ty`.
    pub fn parse_as(s: &str, ty: Type) -> Result<Self, TypeError> {
        let s = s.trim();
        match ty {
            Type::String => Ok(Self::varchar(s.to_owned())),
            Type::Text => Ok(Self::text(s.to_owned())),
            _ => FixedValue::parse_as(s, ty)
                .map(Self::Fixed)
                .ok_or_else(|| TypeError::invalid_conversion(s, ty)),
        }
    }

    /// Builds a [`Type::Bool`] value.
    pub fn bool(b: bool) -> Self {
        Self::Fixed(FixedValue::Bool(b))
    }

    /// Builds a [`Type::Int32`] value.
    pub fn int32(n: i32) -> Self {
        Self::Fixed(FixedValue::Int32(n))
    }

    /// Builds a [`Type::Int64`] value.
    pub fn int64(n: i64) -> Self {
        Self::Fixed(FixedValue::Int64(n))
    }

    /// Builds a [`Type::Uint32`] value.
    pub fn uint32(n: u32) -> Self {
        Self::Fixed(FixedValue::Uint32(n))
    }

    /// Builds a [`Type::Uint64`] value.
    pub fn uint64(n: u64) -> Self {
        Self::Fixed(FixedValue::Uint64(n))
    }

    /// Builds a [`Type::Float64`] value.
    pub fn float64(n: f64) -> Self {
        Self::Fixed(FixedValue::Float64(n))
    }

    /// Builds a [`Type::String`] (`VARCHAR`) value.
    pub fn varchar(s: String) -> Self {
        Self::Dyn(DynValue::Varchar(s))
    }

    /// Builds a [`Type::Text`] value.
    pub fn text(s: String) -> Self {
        Self::Dyn(DynValue::Text(s))
    }

    /// Builds SQL `NULL`.
    pub fn null() -> Self {
        Self::Null
    }

    /// Builds a [`Type::Date`] value.
    pub fn date(d: i32) -> Self {
        Self::Fixed(FixedValue::Date(d))
    }

    /// Builds a [`Type::Time`] value.
    pub fn time(t: i64) -> Self {
        Self::Fixed(FixedValue::Time(t))
    }

    /// Builds a [`Type::Timestamp`] value.
    pub fn timestamp(t: i64) -> Self {
        Self::Fixed(FixedValue::Timestamp(t))
    }
}

/// Two values are equal only when they hold the same inner value.
/// `NULL == NULL` returns `true` (unlike SQL `UNKNOWN` semantics).
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Fixed(a), Self::Fixed(b)) => a == b,
            (Self::Dyn(a), Self::Dyn(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

/// `NULL` sorts before all non-null values; different non-null types are incomparable (`None`).
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Fixed(a), Self::Fixed(b)) => a.partial_cmp(b),
            (Self::Dyn(a), Self::Dyn(b)) => a.partial_cmp(b),
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl Eq for Value {}

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
            Self::Fixed(v) => v.hash(state),
            Self::Dyn(v) => v.hash(state),
            Self::Null => {}
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed(v) => v.fmt(f),
            Self::Dyn(v) => v.fmt(f),
            Self::Null => write!(f, "NULL"),
        }
    }
}

/// Implements `From<T>` for [`Value`] for a list of Rust types.
///
/// Each mapping is `<rust_type> => <constructor>`, where `constructor` is a
/// [`Value`] builder such as [`Value::int32`].
macro_rules! impl_from_value {
    ($($rust_type:ty => $ctor:ident),* $(,)?) => {
        $(
            impl From<$rust_type> for Value {
                #[inline]
                fn from(v: $rust_type) -> Self {
                    Value::$ctor(v)
                }
            }
        )*
    };
}

impl_from_value! {
    i32    => int32,
    i64    => int64,
    u32    => uint32,
    u64    => uint64,
    f64    => float64,
    String => varchar,
    bool   => bool,
}

/// Widening add for aggregate `SUM`. SQL expression `+` uses [`Value::checked_add`].
impl Add<&Value> for Value {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self {
        use FixedValue::{Float64, Int32, Int64, Uint32, Uint64};
        match (&self, rhs) {
            (Self::Fixed(Int32(x)), Self::Fixed(Int32(y))) => {
                Self::int64(i64::from(*x) + i64::from(*y))
            }
            (Self::Fixed(Int64(x)), Self::Fixed(Int32(y))) => Self::int64(*x + i64::from(*y)),
            (Self::Fixed(Int32(x)), Self::Fixed(Int64(y))) => Self::int64(i64::from(*x) + *y),
            (Self::Fixed(Int64(x)), Self::Fixed(Int64(y))) => Self::int64(*x + *y),
            (Self::Fixed(Uint32(x)), Self::Fixed(Uint32(y))) => {
                Self::int64(i64::from(*x) + i64::from(*y))
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            (Self::Fixed(Uint64(x)), Self::Fixed(Uint64(y))) => {
                Self::int64(x.cast_signed() + y.cast_signed())
            }
            (Self::Fixed(Float64(x)), Self::Fixed(Float64(y))) => Self::float64(*x + *y),
            (Self::Fixed(Float64(x)), Self::Fixed(Int32(y))) => Self::float64(*x + f64::from(*y)),
            #[allow(clippy::cast_precision_loss)]
            (Self::Fixed(Float64(x)), Self::Fixed(Int64(y))) => Self::float64(*x + *y as f64),
            _ => Self::Null,
        }
    }
}

impl Sub<&Value> for Value {
    type Output = Self;

    fn sub(self, rhs: &Self) -> Self {
        use FixedValue::{Float64, Int64, Uint64};
        match (&self, rhs) {
            (Self::Fixed(Int64(a)), Self::Fixed(Int64(b))) => Self::int64(a.wrapping_sub(*b)),
            (Self::Fixed(Uint64(a)), Self::Fixed(Uint64(b))) => Self::uint64(a.wrapping_sub(*b)),
            (Self::Fixed(Float64(a)), Self::Fixed(Float64(b))) => Self::float64(a - b),
            _ => Self::Null,
        }
    }
}

impl Mul<&Value> for Value {
    type Output = Self;

    fn mul(self, rhs: &Self) -> Self {
        use FixedValue::{Float64, Int32, Int64, Uint32, Uint64};
        match (&self, rhs) {
            (Self::Fixed(Int32(a)), Self::Fixed(Int32(b))) => {
                Self::int64(i64::from(*a) * i64::from(*b))
            }
            (Self::Fixed(Int64(a)), Self::Fixed(Int64(b))) => Self::int64(a.wrapping_mul(*b)),
            (Self::Fixed(Uint32(a)), Self::Fixed(Uint32(b))) => {
                Self::int64(i64::from(*a) * i64::from(*b))
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            (Self::Fixed(Uint64(a)), Self::Fixed(Uint64(b))) => {
                Self::int64(a.cast_signed() * b.cast_signed())
            }
            (Self::Fixed(Float64(a)), Self::Fixed(Float64(b))) => Self::float64(a * b),
            _ => Self::Null,
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
            Value::Fixed(FixedValue::Int32(v)) => Ok(f64::from(*v)),
            Value::Fixed(FixedValue::Uint32(v)) => Ok(f64::from(*v)),
            Value::Fixed(FixedValue::Float64(v)) => Ok(*v),
            #[allow(clippy::cast_precision_loss)]
            Value::Fixed(FixedValue::Int64(v)) => Ok(*v as f64),
            #[allow(clippy::cast_precision_loss)]
            Value::Fixed(FixedValue::Uint64(v)) => Ok(*v as f64),
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
            None => Self::Null,
        }
    }
}

impl Encode for DynValue {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            Self::Varchar(s) | Self::Text(s) => s.encode(w),
            Self::TextOverflow { total_len, ptr } => {
                u32::MAX.encode(w)?;
                total_len.encode(w)?;
                ptr.encode(w)
            }
        }
    }
}

impl Encode for Value {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let Some(ty) = self.get_type() else {
            NULL_VALUE_TAG.encode(w)?;
            return Ok(());
        };
        ty.encode(w)?;
        match self {
            Self::Fixed(v) => v.encode(w)?,
            Self::Dyn(v) => v.encode(w)?,
            Self::Null => unreachable!(),
        }
        Ok(())
    }
}

impl Decode for Value {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let tag = u32::decode(r)?;
        if tag == NULL_VALUE_TAG {
            return Ok(Value::Null);
        }

        let value_type = Type::try_from(tag).map_err(|_| match u8::try_from(tag) {
            Ok(tag_u8) => CodecError::UnknownDiscriminant(tag_u8),
            Err(_) => CodecError::numeric_does_not_fit(tag as usize, "u8"),
        })?;

        match value_type {
            Type::Int32 => Ok(Self::int32(i32::decode(r)?)),
            Type::Int64 => Ok(Self::int64(i64::decode(r)?)),
            Type::Uint32 => Ok(Self::uint32(u32::decode(r)?)),
            Type::Uint64 => Ok(Self::uint64(u64::decode(r)?)),
            Type::Float64 => Ok(Self::float64(f64::decode(r)?)),
            Type::Bool => Ok(Self::bool(u8::decode(r)? != 0)),
            Type::Date => Ok(Self::date(i32::decode(r)?)),
            Type::Time => Ok(Self::time(i64::decode(r)?)),
            Type::Timestamp => Ok(Self::timestamp(i64::decode(r)?)),
            Type::String => {
                let len = u32::decode(r)? as usize;
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
                Ok(Self::varchar(std::str::from_utf8(&buf)?.to_string()))
            }
            Type::Text => {
                let first = u32::decode(r)?;
                if first == u32::MAX {
                    let total_len = u32::decode(r)?;
                    let ptr = PageDescriptor::decode(r)?;
                    Ok(Self::Dyn(DynValue::TextOverflow { total_len, ptr }))
                } else {
                    // `first` is the byte length of the inline string.
                    // Do NOT call String::decode here — that would read a second length prefix.
                    let mut buf = vec![0u8; first as usize];
                    r.read_exact(&mut buf)?;
                    std::str::from_utf8(&buf)
                        .map(|s| Self::text(s.to_owned()))
                        .map_err(CodecError::InvalidUtf8)
                }
            }
        }
    }
}

#[cfg(test)]
mod fixed_value_temporal_tests {
    use std::cmp::Ordering;

    use super::FixedValue;

    #[test]
    fn time_partial_ord() {
        let early = FixedValue::parse_time("10:00:00").unwrap();
        let late = FixedValue::parse_time("16:00:00").unwrap();
        assert_eq!(early.partial_cmp(&late), Some(Ordering::Less));
    }

    #[test]
    fn int64_partial_ord() {
        assert_eq!(
            FixedValue::Int64(1).partial_cmp(&FixedValue::Int64(2)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn temporal_display_is_human_readable() {
        assert_eq!(
            FixedValue::parse_date("2024-01-15").unwrap().to_string(),
            "2024-01-15"
        );
        assert_eq!(
            FixedValue::parse_time("16:00:00").unwrap().to_string(),
            "16:00:00"
        );
        assert_eq!(
            FixedValue::parse_timestamp("2024-01-15 16:00:00")
                .unwrap()
                .to_string(),
            "2024-01-15 16:00:00"
        );
    }
}

#[cfg(test)]
mod coerce_to_tests {
    use super::{Type, TypeError, Value};

    #[test]
    fn int64_literal_to_int32() {
        let v = Value::int64(42);
        assert_eq!(v.coerce_to(Type::Int32).unwrap(), Value::int32(42));
    }

    #[test]
    fn int64_out_of_range_for_int32() {
        let v = Value::int64(i64::from(i32::MAX) + 1);
        let err = v.coerce_to(Type::Int32).unwrap_err();
        assert!(matches!(err, TypeError::InvalidConversion { .. }));
    }

    #[test]
    fn negative_int64_to_uint64_fails() {
        let v = Value::int64(-1);
        assert!(v.coerce_to(Type::Uint64).is_err());
    }

    #[test]
    fn string_to_varchar_column() {
        let v = Value::varchar("x".to_string());
        assert_eq!(
            v.coerce_to(Type::String).unwrap(),
            Value::varchar("x".to_string())
        );
    }

    #[test]
    fn string_to_text_column() {
        let v = Value::varchar("hello".to_string());
        assert_eq!(
            v.coerce_to(Type::Text).unwrap(),
            Value::text("hello".to_string())
        );
    }

    #[test]
    fn bool_same_type_unchanged() {
        let v = Value::bool(true);
        assert_eq!(v.coerce_to(Type::Bool).unwrap(), Value::bool(true));
    }

    #[test]
    fn string_literal_to_int_column_fails() {
        let v = Value::varchar("1".to_string());
        assert!(v.coerce_to(Type::Int32).is_err());
    }
}

#[cfg(test)]
mod value_codec_proptest {
    use proptest::prelude::*;

    use super::Value;
    use crate::codec::{Decode, Encode};

    proptest! {
        #[test]
        fn value_int32_roundtrip(v in any::<i32>()) {
            let val = Value::int32(v);
            let bytes = val.to_bytes().unwrap();
            prop_assert_eq!(Value::from_bytes(&bytes).unwrap(), val);
        }
    }
}
