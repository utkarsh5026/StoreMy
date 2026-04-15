//! Typed field extraction from catalog tuples.
//!
//! The catalog stores its internal tables (schemas, columns, indexes) as plain
//! [`Tuple`]s. Reading those tuples back into typed Rust values requires
//! checking that each field index exists and holds the expected [`Value`]
//! variant. This module provides [`TupleReader`], a small cursor that wraps a
//! tuple and an index, and a set of [`TryFrom`] implementations that delegate
//! to it.
//!
//! # Usage pattern
//!
//! ```ignore
//! // Read field 0 as a String and field 1 as a u32:
//! let name  = TupleReader::read::<String>(&tuple, 0)?;
//! let arity = TupleReader::read::<u32>(&tuple, 1)?;
//! ```
//!
//! All conversions fail with [`CatalogError`] when the index is out of bounds
//! or the stored [`Value`] variant does not match the requested type.

use crate::catalog::CatalogError;
use crate::tuple::Tuple;
use crate::types::Value;

/// A cursor into a single field of a [`Tuple`].
///
/// `TupleReader` pairs a reference to a tuple with a zero-based field index.
/// It is [`Copy`] so that multiple extraction calls can share the same reader
/// without moving it — this is relied upon by the [`Option<T>`] impl, which
/// passes `t` both to [`extract`](TupleReader::extract) and to an inner
/// `T::try_from(t)` call inside the closure.
#[derive(Copy, Clone, Debug)]
pub(super) struct TupleReader<'a>(&'a Tuple, usize);

impl<'a> TupleReader<'a> {
    /// Creates a reader that points at field `index` of `tuple`.
    pub(super) fn new(tuple: &'a Tuple, index: usize) -> Self {
        Self(tuple, index)
    }

    /// Reads the field at the stored index, applies `extractor`, and returns
    /// the result.
    ///
    /// `expected` is a short human-readable type description used only in the
    /// error message when `extractor` returns `None`.
    ///
    /// # Errors
    ///
    /// - [`CatalogError`] with message `"field index out of bounds"` when the
    ///   index is past the last field in the tuple.
    /// - [`CatalogError`] with message `"field N: expected <expected>, got
    ///   <value>"` (1-indexed) when `extractor` returns `None`.
    pub(super) fn extract<T, E>(&self, expected: &str, extractor: E) -> Result<T, CatalogError>
    where
        E: FnOnce(&Value) -> Option<T>,
    {
        let TupleReader(tuple, index) = self;
        let v = tuple
            .get(*index)
            .ok_or_else(|| CatalogError::invalid_catalog_row("field index out of bounds"))?;

        extractor(v).ok_or_else(|| {
            CatalogError::invalid_catalog_row(format!(
                "field {}: expected {expected}, got {v:?}",
                self.1 + 1
            ))
        })
    }

    /// Reads field `i` of `tuple` as type `T`.
    ///
    /// Shorthand for constructing a [`TupleReader`] and calling `T::try_from`
    /// on it. `T` must implement `TryFrom<TupleReader<'a>, Error =
    /// CatalogError>`, which all scalar types in this module do.
    ///
    /// # Errors
    ///
    /// Propagates any [`CatalogError`] returned by `T::try_from`.
    pub(super) fn read<T>(tuple: &'a Tuple, i: usize) -> Result<T, CatalogError>
    where
        T: TryFrom<TupleReader<'a>, Error = CatalogError>,
    {
        T::try_from(TupleReader::new(tuple, i))
    }
}

/// Generates a `TryFrom<TupleReader<'a>>` impl for a scalar type.
///
/// Accepts a type, an `expected` string for error messages, and one or more
/// `pattern => expression` arms placed inside a `match v { … _ => None }`
/// block. A `_ => None` catch-all is appended automatically.
macro_rules! impl_from_reader {
    ($type:ty, $expected:literal $(, $pat:pat => $expr:expr)+) => {
        impl<'a> TryFrom<TupleReader<'a>> for $type {
            type Error = CatalogError;
            fn try_from(t: TupleReader<'a>) -> Result<Self, Self::Error> {
                t.extract($expected, |v| match v {
                    $($pat => $expr,)*
                    _ => None,
                })
            }
        }
    };
}

// `Value::Null` maps to an empty string so that optional catalog text fields
// stored as NULL can be read back as a plain `String` without a separate
// `Option` layer.
impl_from_reader!(String, "string or null",
    Value::Null       => Some(String::new()),
    Value::String(s)  => Some(s.clone())
);

// `Value::Int64` is accepted via `cast_unsigned` (bit-reinterpret, wrapping)
// to handle catalog rows written with a signed integer column.
// `Value::Uint32` is widened losslessly.
impl_from_reader!(u64, "uint64",
    Value::Uint64(n)  => Some(*n),
    Value::Int64(n)   => Some((*n).cast_unsigned()),
    Value::Uint32(n)  => Some(u64::from(*n))
);

impl_from_reader!(u32, "uint32",  Value::Uint32(n) => Some(*n));

// `Value::Int32` is widened losslessly to `i64`.
impl_from_reader!(i64, "i64",
    Value::Int32(n)   => Some(i64::from(*n)),
    Value::Int64(n)   => Some(*n)
);

impl_from_reader!(i32, "i32",   Value::Int32(n) => Some(*n));
impl_from_reader!(bool, "bool", Value::Bool(b)  => Some(*b));

/// Reads a nullable field as `Option<T>`.
///
/// `Value::Null` maps to `Ok(None)`. Any other value is forwarded to
/// `T::try_from`; success maps to `Ok(Some(value))`, while failure causes the
/// outer `extract` call to return `Err` (the inner error is not propagated
/// directly — `extract` produces its own `"expected null or not null"` message).
impl<'a, T> TryFrom<TupleReader<'a>> for Option<T>
where
    T: TryFrom<TupleReader<'a>, Error = CatalogError>,
{
    type Error = CatalogError;

    fn try_from(t: TupleReader<'a>) -> Result<Self, Self::Error> {
        t.extract("null or not null", |v| match v {
            Value::Null => Some(None),
            _ => T::try_from(t).ok().map(Some),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::Tuple;
    use crate::types::Value;

    fn tuple(values: Vec<Value>) -> Tuple {
        Tuple::new(values)
    }

    // --- extract: index out of bounds ---

    #[test]
    fn test_extract_index_oob_empty_tuple() {
        let t = tuple(vec![]);
        let reader = TupleReader::new(&t, 0);
        let err = reader.extract("string", |_| Some(())).unwrap_err();
        assert!(
            err.to_string().contains("field index out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_extract_index_oob_past_end() {
        let t = tuple(vec![Value::Int32(1)]);
        let reader = TupleReader::new(&t, 1); // only index 0 exists
        let err = reader.extract("i32", |_| Some(())).unwrap_err();
        assert!(err.to_string().contains("field index out of bounds"));
    }

    // --- extract: wrong type produces descriptive error ---

    #[test]
    fn test_extract_wrong_type_error_message_contains_field_number() {
        // field number in the message is 1-indexed (self.1 + 1)
        let t = tuple(vec![Value::Bool(true)]);
        let reader = TupleReader::new(&t, 0);
        let err = reader
            .extract("i32", |v| match v {
                Value::Int32(n) => Some(*n),
                _ => None,
            })
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("field 1"), "expected 'field 1' in: {msg}");
        assert!(msg.contains("i32"), "expected type hint in: {msg}");
    }

    #[test]
    fn test_extract_wrong_type_error_contains_field_number_at_index_2() {
        // index 2 → "field 3"
        let t = tuple(vec![Value::Int32(1), Value::Int32(2), Value::Bool(true)]);
        let reader = TupleReader::new(&t, 2);
        let err = reader
            .extract("i32", |v| match v {
                Value::Int32(n) => Some(*n),
                _ => None,
            })
            .unwrap_err();
        assert!(err.to_string().contains("field 3"));
    }

    #[test]
    fn test_string_from_string_value() {
        let t = tuple(vec![Value::String("hello".into())]);
        let s = TupleReader::read::<String>(&t, 0).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_string_from_null_yields_empty_string() {
        // Null → "" is intentional catalog behavior
        let t = tuple(vec![Value::Null]);
        let s = TupleReader::read::<String>(&t, 0).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn test_string_from_wrong_type_errors() {
        let t = tuple(vec![Value::Int32(42)]);
        assert!(TupleReader::read::<String>(&t, 0).is_err());
    }

    #[test]
    fn test_u64_from_uint64() {
        let t = tuple(vec![Value::Uint64(u64::MAX)]);
        assert_eq!(TupleReader::read::<u64>(&t, 0).unwrap(), u64::MAX);
    }

    #[test]
    fn test_u64_from_uint32_widens() {
        let t = tuple(vec![Value::Uint32(100)]);
        assert_eq!(TupleReader::read::<u64>(&t, 0).unwrap(), 100u64);
    }

    #[test]
    fn test_u64_from_int64_positive() {
        let t = tuple(vec![Value::Int64(42)]);
        assert_eq!(TupleReader::read::<u64>(&t, 0).unwrap(), 42u64);
    }

    #[test]
    fn test_u64_from_int64_negative_cast_unsigned() {
        // cast_unsigned() is a wrapping bit-reinterpret: -1i64 → u64::MAX
        let t = tuple(vec![Value::Int64(-1)]);
        assert_eq!(TupleReader::read::<u64>(&t, 0).unwrap(), u64::MAX);
    }

    #[test]
    fn test_u64_from_wrong_type_errors() {
        let t = tuple(vec![Value::Bool(true)]);
        assert!(TupleReader::read::<u64>(&t, 0).is_err());
    }

    #[test]
    fn test_u32_from_uint32() {
        let t = tuple(vec![Value::Uint32(u32::MAX)]);
        assert_eq!(TupleReader::read::<u32>(&t, 0).unwrap(), u32::MAX);
    }

    #[test]
    fn test_u32_from_wrong_type_errors() {
        // u32 only accepts Uint32; Uint64 is NOT accepted
        let t = tuple(vec![Value::Uint64(1)]);
        assert!(TupleReader::read::<u32>(&t, 0).is_err());
    }

    #[test]
    fn test_i64_from_int64() {
        let t = tuple(vec![Value::Int64(i64::MIN)]);
        assert_eq!(TupleReader::read::<i64>(&t, 0).unwrap(), i64::MIN);
    }

    #[test]
    fn test_i64_from_int32_widens() {
        let t = tuple(vec![Value::Int32(-7)]);
        assert_eq!(TupleReader::read::<i64>(&t, 0).unwrap(), -7i64);
    }

    #[test]
    fn test_i64_from_wrong_type_errors() {
        let t = tuple(vec![Value::Uint64(1)]);
        assert!(TupleReader::read::<i64>(&t, 0).is_err());
    }

    #[test]
    fn test_i32_from_int32() {
        let t = tuple(vec![Value::Int32(i32::MIN)]);
        assert_eq!(TupleReader::read::<i32>(&t, 0).unwrap(), i32::MIN);
    }

    #[test]
    fn test_i32_from_wrong_type_errors() {
        // i32 only accepts Int32; Int64 is NOT accepted
        let t = tuple(vec![Value::Int64(1)]);
        assert!(TupleReader::read::<i32>(&t, 0).is_err());
    }

    #[test]
    fn test_bool_from_bool_true() {
        let t = tuple(vec![Value::Bool(true)]);
        assert!(TupleReader::read::<bool>(&t, 0).unwrap());
    }

    #[test]
    fn test_bool_from_bool_false() {
        let t = tuple(vec![Value::Bool(false)]);
        assert!(!TupleReader::read::<bool>(&t, 0).unwrap());
    }

    #[test]
    fn test_bool_from_wrong_type_errors() {
        let t = tuple(vec![Value::Int32(1)]);
        assert!(TupleReader::read::<bool>(&t, 0).is_err());
    }

    #[test]
    fn test_option_string_from_null_yields_none() {
        let t = tuple(vec![Value::Null]);
        let v = TupleReader::read::<Option<String>>(&t, 0).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_option_i32_from_null_yields_none() {
        let t = tuple(vec![Value::Null]);
        let v = TupleReader::read::<Option<i32>>(&t, 0).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_option_i32_from_value_yields_some() {
        let t = tuple(vec![Value::Int32(99)]);
        let v = TupleReader::read::<Option<i32>>(&t, 0).unwrap();
        assert_eq!(v, Some(99));
    }

    #[test]
    fn test_option_bool_from_value_yields_some() {
        let t = tuple(vec![Value::Bool(false)]);
        let v = TupleReader::read::<Option<bool>>(&t, 0).unwrap();
        assert_eq!(v, Some(false));
    }

    #[test]
    fn test_option_i32_wrong_type_yields_err() {
        // Option<T> with a wrong non-null type propagates as Err,
        // because the inner T::try_from failure causes the extractor to return
        // None, which extract then converts to its own error.
        let t = tuple(vec![Value::Bool(true)]);
        assert!(TupleReader::read::<Option<i32>>(&t, 0).is_err());
    }

    #[test]
    fn test_read_selects_correct_index() {
        let t = tuple(vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)]);
        assert_eq!(TupleReader::read::<i32>(&t, 0).unwrap(), 10);
        assert_eq!(TupleReader::read::<i32>(&t, 1).unwrap(), 20);
        assert_eq!(TupleReader::read::<i32>(&t, 2).unwrap(), 30);
    }

    #[test]
    fn test_read_oob_index_errors() {
        let t = tuple(vec![Value::Int32(1)]);
        assert!(TupleReader::read::<i32>(&t, 5).is_err());
    }
}
