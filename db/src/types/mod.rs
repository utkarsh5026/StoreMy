//! Type system for StoreMy database.
//!
//! This module defines the supported data types and the [`Value`] enum
//! that represents runtime values in the database.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{Result, SerializationError};
use crate::STRING_MAX_SIZE;


/// Supported column types in StoreMy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Type {
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit unsigned integer
    Uint32,
    /// 64-bit unsigned integer
    Uint64,
    /// 64-bit floating point
    Float64,
    /// Variable-length string (max 255 bytes)
    String,
    /// Boolean value
    Bool,
}

impl Type {
    /// Returns the fixed size in bytes for this type.
    ///
    /// For String type, returns the maximum possible size (length prefix + max chars).
    pub const fn size(&self) -> usize {
        match self {
            Type::Int32 | Type::Uint32 => 4,
            Type::Int64 | Type::Uint64 | Type::Float64 => 8,
            Type::Bool => 1,
            Type::String => 4 + STRING_MAX_SIZE, // 4-byte length prefix
        }
    }

    /// Returns true if this type has a fixed size.
    pub const fn is_fixed_size(&self) -> bool {
        !matches!(self, Type::String)
    }

    /// Returns the SQL name for this type.
    pub const fn sql_name(&self) -> &'static str {
        match self {
            Type::Int32 => "INT",
            Type::Int64 => "BIGINT",
            Type::Uint32 => "INT UNSIGNED",
            Type::Uint64 => "BIGINT UNSIGNED",
            Type::Float64 => "DOUBLE",
            Type::String => "VARCHAR",
            Type::Bool => "BOOLEAN",
        }
    }

    /// Parses a type from SQL type name.
    pub fn from_sql_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "INT" | "INTEGER" | "INT32" => Some(Type::Int32),
            "BIGINT" | "INT64" => Some(Type::Int64),
            "UINT" | "UINT32" => Some(Type::Uint32),
            "UBIGINT" | "UINT64" => Some(Type::Uint64),
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Some(Type::Float64),
            "VARCHAR" | "TEXT" | "STRING" => Some(Type::String),
            "BOOL" | "BOOLEAN" => Some(Type::Bool),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sql_name())
    }
}


/// A runtime value in the database.
///
/// This enum represents all possible values that can be stored in or
/// retrieved from the database.
#[derive(Debug, Clone)]
pub enum Value {
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 32-bit unsigned integer
    Uint32(u32),
    /// 64-bit unsigned integer
    Uint64(u64),
    /// 64-bit floating point
    Float64(f64),
    /// String value
    String(String),
    /// Boolean value
    Bool(bool),
    /// NULL value
    Null,
}

impl Value {
    /// Returns the type of this value.
    ///
    /// Returns `None` for NULL values.
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

    /// Returns true if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the serialized size of this value in bytes.
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Int32(_) | Value::Uint32(_) => 4,
            Value::Int64(_) | Value::Uint64(_) | Value::Float64(_) => 8,
            Value::Bool(_) => 1,
            Value::String(s) => 4 + s.len(), // 4-byte length prefix
            Value::Null => 0,
        }
    }

    /// Serializes this value to bytes.
    ///
    /// Returns the number of bytes written.
    pub fn serialize(&self, buf: &mut [u8]) -> usize {
        match self {
            Value::Int32(v) => {
                LittleEndian::write_i32(buf, *v);
                4
            }
            Value::Int64(v) => {
                LittleEndian::write_i64(buf, *v);
                8
            }
            Value::Uint32(v) => {
                LittleEndian::write_u32(buf, *v);
                4
            }
            Value::Uint64(v) => {
                LittleEndian::write_u64(buf, *v);
                8
            }
            Value::Float64(v) => {
                LittleEndian::write_f64(buf, *v);
                8
            }
            Value::Bool(v) => {
                buf[0] = if *v { 1 } else { 0 };
                1
            }
            Value::String(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len().min(STRING_MAX_SIZE);
                LittleEndian::write_u32(buf, len as u32);
                buf[4..4 + len].copy_from_slice(&bytes[..len]);
                4 + len
            }
            Value::Null => 0,
        }
    }

    /// Checks if buffer has at least `needed` bytes.
    ///
    /// Returns an error if the buffer is too small.
    #[inline]
    fn ensure_buffer_size(buf: &[u8], needed: usize) -> Result<()> {
        if buf.len() < needed {
            return Err(SerializationError::BufferTooSmall {
                needed,
                available: buf.len(),
            }.into());
        }
        Ok(())
    }

    /// Deserializes a value from bytes.
    pub fn deserialize(buf: &[u8], typ: Type) -> Result<Self> {
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
                let s = std::str::from_utf8(&buf[4..4 + len])
                    .map_err(|e| SerializationError::DeserializationError {
                        message: format!("Invalid UTF-8: {}", e),
                    })?;
                Ok(Value::String(s.to_string()))
            }
        }
    }

    /// Attempts to convert this value to an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Uint32(v) => Some(*v as i64),
            Value::Uint64(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Attempts to convert this value to an f64.
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

    /// Attempts to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Attempts to get this value as a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

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

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Uint32(v) => write!(f, "{}", v),
            Value::Uint64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
        }
    }
}

// Convenient From implementations
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Uint32(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Uint64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_size() {
        assert_eq!(Type::Int32.size(), 4);
        assert_eq!(Type::Int64.size(), 8);
        assert_eq!(Type::Bool.size(), 1);
    }

    #[test]
    fn test_value_serialization_roundtrip() {
        let values = vec![
            (Value::Int32(42), Type::Int32),
            (Value::Int64(-1234567890), Type::Int64),
            (Value::Uint32(100), Type::Uint32),
            (Value::Float64(std::f64::consts::PI), Type::Float64),
            (Value::Bool(true), Type::Bool),
            (Value::String("hello".to_string()), Type::String),
        ];

        for (value, typ) in values {
            let mut buf = vec![0u8; 512];
            let written = value.serialize(&mut buf);
            let deserialized = Value::deserialize(&buf[..written], typ).unwrap();
            assert_eq!(value, deserialized);
        }
    }

    #[test]
    fn test_value_comparison() {
        assert!(Value::Int32(1) < Value::Int32(2));
        assert!(Value::String("a".to_string()) < Value::String("b".to_string()));
        assert!(Value::Null < Value::Int32(0));
    }

    #[test]
    fn test_value_from_conversions() {
        assert_eq!(Value::from(42i32), Value::Int32(42));
        assert_eq!(Value::from("test"), Value::String("test".to_string()));
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from(None::<i32>), Value::Null);
    }

    #[test]
    fn test_type_from_sql_name() {
        assert_eq!(Type::from_sql_name("INT"), Some(Type::Int32));
        assert_eq!(Type::from_sql_name("varchar"), Some(Type::String));
        assert_eq!(Type::from_sql_name("BOOLEAN"), Some(Type::Bool));
        assert_eq!(Type::from_sql_name("unknown"), None);
    }
}
