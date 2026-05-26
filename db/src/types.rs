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
    ops::{Add, Mul, Sub},
};

use thiserror::Error;

use crate::{
    STRING_MAX_SIZE,
    codec::{CodecError, Decode, Encode, ReadLeExt, WriteLeExt},
};

/// Wire tag for [`Value::Null`]; not a [`Type`] variant.
const NULL_VALUE_TAG: u32 = u32::MAX;

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
        }
    }
}

/// Encodes a [`Type`] as a little-endian `u32` tag, matching the mapping defined in [`From<Type>
/// for u32`].
///
/// # Errors
///
/// Returns any underlying I/O error from the writer.
impl Encode for Type {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_le_u32(u32::from(*self))?;
        Ok(())
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
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = reader.read_le_u32()?;
        Type::try_from(tag).map_err(|_| match u8::try_from(tag) {
            Ok(tag_u8) => CodecError::UnknownDiscriminant(tag_u8),
            Err(_) => CodecError::NumericDoesNotFit {
                value: u64::from(tag),
                target: "u8",
            },
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
/// | `String`  | `VARCHAR`, `STRING` |
/// | `Text`    | `TEXT` |
/// | `Bool`    | `BOOL`, `BOOLEAN` |
///
/// # Errors
///
/// Returns [`TypeError::UnsupportedType`] when `value` does not match any
/// of the accepted strings (unrecognized name).
impl TryFrom<&str> for Type {
    type Error = TypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "INT" | "INTEGER" | "INT32" => Ok(Type::Int32),
            "BIGINT" | "INT64" => Ok(Type::Int64),
            "UINT" | "UINT32" => Ok(Type::Uint32),
            "UBIGINT" | "UINT64" => Ok(Type::Uint64),
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Ok(Type::Float64),
            "VARCHAR" | "STRING" => Ok(Type::String),
            "TEXT" => Ok(Type::Text),
            "BOOL" | "BOOLEAN" => Ok(Type::Bool),
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
    Text(String),
    Bool(bool),
    Null,
}

impl Value {
    /// Returns the [`Type`] of this value, or `None` if the value is `NULL`.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::types::{Type, Value};
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
            Value::Text(_) => Some(Type::Text),
            Value::Null => None,
        }
    }

    /// Returns `true` if this value is `NULL`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
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

    /// Number of bytes [`Encode`] will write for this value.
    ///
    /// Must stay in sync with [`Encode for Value`]: every variant here
    /// mirrors a branch there. The `String` arm applies the same
    /// [`STRING_MAX_SIZE`] truncation the encoder does.
    pub fn encoded_size(&self) -> usize {
        4 + match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int32(_) | Value::Uint32(_) => 4,
            Value::Int64(_) | Value::Uint64(_) | Value::Float64(_) => 8,
            Value::String(s) | Value::Text(s) => 4 + s.len().min(STRING_MAX_SIZE),
        }
    }

    /// SQL expression `+` on two non-null operands of the same numeric kind.
    ///
    /// Integer pairs use wrapping arithmetic; [`Value::Float64`] uses IEEE addition.
    /// The [`Add`] operator uses different (widening) rules for aggregate `SUM`.
    pub fn checked_add(&self, rhs: &Value) -> Result<Value, ArithmeticError> {
        match (self, rhs) {
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a.wrapping_add(*b))),
            (Value::Uint64(a), Value::Uint64(b)) => Ok(Value::Uint64(a.wrapping_add(*b))),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
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
}

impl TryFrom<(&Value, Type)> for Value {
    type Error = TypeError;

    fn try_from((v, target): (&Self, Type)) -> Result<Self, Self::Error> {
        match (v, target) {
            (Self::Int64(n), Type::Int32) => i32::try_from(*n)
                .map(Self::Int32)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Int32)),
            (Self::Int64(n), Type::Int64) => Ok(Self::Int64(*n)),
            (Self::Int64(n), Type::Uint32) => u32::try_from(*n)
                .map(Self::Uint32)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Uint32)),
            (Self::Int64(n), Type::Uint64) => u64::try_from(*n)
                .map(Self::Uint64)
                .map_err(|_| TypeError::invalid_conversion(*n, Type::Uint64)),
            (Self::String(s), Type::String) => Ok(Self::String(s.clone())),
            (Self::Text(s), Type::Text) => Ok(Self::Text(s.clone())),
            (v, ty) if v.get_type() == Some(ty) => Ok(v.clone()),
            (v, ty) => Err(TypeError::invalid_conversion(v, ty)),
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

impl_value_cmp! { Int32, Int64, Uint32, Uint64, Float64, String, Text, Bool }

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
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Uint32(v) => v.hash(state),
            Self::Uint64(v) => v.hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::String(v) | Self::Text(v) => v.hash(state),
            Self::Bool(v) => v.hash(state),
            Self::Null => {}
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Uint32(v) => write!(f, "{v}"),
            Self::Uint64(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::String(v) | Self::Text(v) => write!(f, "'{v}'"),
            Self::Null => write!(f, "NULL"),
        }
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

/// Widening add for aggregate `SUM`. SQL expression `+` uses [`Value::checked_add`].
impl Add<&Value> for Value {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self {
        match (&self, rhs) {
            (Self::Int32(x), Self::Int32(y)) => Self::Int64(i64::from(*x) + i64::from(*y)),
            (Self::Int64(x), Self::Int32(y)) => Self::Int64(*x + i64::from(*y)),
            (Self::Int32(x), Self::Int64(y)) => Self::Int64(i64::from(*x) + *y),
            (Self::Int64(x), Self::Int64(y)) => Self::Int64(*x + *y),
            (Self::Uint32(x), Self::Uint32(y)) => Self::Int64(i64::from(*x) + i64::from(*y)),
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            (Self::Uint64(x), Self::Uint64(y)) => Self::Int64(x.cast_signed() + y.cast_signed()),
            (Self::Float64(x), Self::Float64(y)) => Self::Float64(*x + *y),
            (Self::Float64(x), Self::Int32(y)) => Self::Float64(*x + f64::from(*y)),
            #[allow(clippy::cast_precision_loss)]
            (Self::Float64(x), Self::Int64(y)) => Self::Float64(*x + *y as f64),
            _ => Self::Null,
        }
    }
}

impl Sub<&Value> for Value {
    type Output = Self;

    fn sub(self, rhs: &Self) -> Self {
        match (&self, rhs) {
            (Self::Int64(a), Self::Int64(b)) => Self::Int64(a.wrapping_sub(*b)),
            (Self::Uint64(a), Self::Uint64(b)) => Self::Uint64(a.wrapping_sub(*b)),
            (Self::Float64(a), Self::Float64(b)) => Self::Float64(a - b),
            _ => Self::Null,
        }
    }
}

impl Mul<&Value> for Value {
    type Output = Self;

    fn mul(self, rhs: &Self) -> Self {
        match (&self, rhs) {
            (Self::Int64(a), Self::Int64(b)) => Self::Int64(a.wrapping_mul(*b)),
            (Self::Uint64(a), Self::Uint64(b)) => Self::Uint64(a.wrapping_mul(*b)),
            (Self::Float64(a), Self::Float64(b)) => Self::Float64(a * b),
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
            None => Self::Null,
        }
    }
}

impl Encode for Value {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let Some(value_type) = self.get_type() else {
            w.write_le_u32(NULL_VALUE_TAG)?;
            return Ok(());
        };

        value_type.encode(w)?;

        match self {
            Self::Int32(v) => w.write_le_i32(*v)?,
            Self::Int64(v) => w.write_le_i64(*v)?,
            Self::Uint32(v) => w.write_le_u32(*v)?,
            Self::Uint64(v) => w.write_le_u64(*v)?,
            Self::Float64(v) => w.write_le_f64(*v)?,
            Self::Bool(v) => w.write_u8(u8::from(*v))?,
            Self::String(s) | Self::Text(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len().min(STRING_MAX_SIZE);
                let len_u32 = u32::try_from(len)
                    .map_err(|_| CodecError::numeric_does_not_fit(len as u64, "u32"))?;
                w.write_le_u32(len_u32)?;
                w.write_all(&bytes[..len])?;
            }
            Self::Null => unreachable!(),
        }
        Ok(())
    }
}

impl Decode for Value {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let tag = r.read_le_u32()?;
        if tag == NULL_VALUE_TAG {
            return Ok(Value::Null);
        }

        let value_type = Type::try_from(tag).map_err(|_| match u8::try_from(tag) {
            Ok(tag_u8) => CodecError::UnknownDiscriminant(tag_u8),
            Err(_) => CodecError::numeric_does_not_fit(u64::from(tag), "u8"),
        })?;

        match value_type {
            Type::Int32 => Ok(Self::Int32(r.read_le_i32()?)),
            Type::Int64 => Ok(Self::Int64(r.read_le_i64()?)),
            Type::Uint32 => Ok(Self::Uint32(r.read_le_u32()?)),
            Type::Uint64 => Ok(Self::Uint64(r.read_le_u64()?)),
            Type::Float64 => Ok(Self::Float64(r.read_le_f64()?)),
            Type::Bool => Ok(Self::Bool(r.read_u8()? != 0)),
            Type::String | Type::Text => {
                let len = r.read_le_u32()? as usize;
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
                let s = std::str::from_utf8(&buf)?.to_string();

                if matches!(value_type, Type::String) {
                    Ok(Self::String(s))
                } else {
                    Ok(Self::Text(s))
                }
            }
        }
    }
}

#[cfg(test)]
mod coerce_to_type_tests {
    use super::{Type, TypeError, Value};

    #[test]
    fn int64_literal_to_int32() {
        let v = Value::Int64(42);
        assert_eq!(
            Value::try_from((&v, Type::Int32)).unwrap(),
            Value::Int32(42)
        );
    }

    #[test]
    fn int64_out_of_range_for_int32() {
        let v = Value::Int64(i64::from(i32::MAX) + 1);
        let err = Value::try_from((&v, Type::Int32)).unwrap_err();
        assert!(matches!(err, TypeError::InvalidConversion { .. }));
    }

    #[test]
    fn negative_int64_to_uint64_fails() {
        let v = Value::Int64(-1);
        assert!(Value::try_from((&v, Type::Uint64)).is_err());
    }

    #[test]
    fn string_to_varchar_column() {
        let v = Value::String("x".to_string());
        assert_eq!(
            Value::try_from((&v, Type::String)).unwrap(),
            Value::String("x".to_string())
        );
    }

    #[test]
    fn bool_same_type_unchanged() {
        let v = Value::Bool(true);
        assert_eq!(
            Value::try_from((&v, Type::Bool)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn string_literal_to_int_column_fails() {
        let v = Value::String("1".to_string());
        assert!(Value::try_from((&v, Type::Int32)).is_err());
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
            let val = Value::Int32(v);
            let bytes = val.to_bytes().unwrap();
            prop_assert_eq!(Value::from_bytes(&bytes).unwrap(), val);
        }
    }
}
