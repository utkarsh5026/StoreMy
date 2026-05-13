use std::{
    fmt,
    io::{Cursor, Read, Write},
};

use super::{Field, TupleError, TupleSchema};
use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::ColumnId,
    types::Value,
};

/// Writes the on-disk row bytes a heap page stores after `INSERT`.
///
/// Layout: a null bitmap, then each non-null value encoded via
/// [`Encode for Value`] (discriminant + payload). The schema itself is **not**
/// written - only the row bytes; the catalog is the single source of truth
/// for column names and types.
///
/// This is the serialization the [`crate::codec::Encode`] trait dispatches to;
/// [`Tuple::serialize`] is a thin wrapper over the same path that writes into
/// a byte slice.
impl Encode for (&TupleSchema, &Tuple) {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let (schema, tuple) = *self;
        let bitmap_size = schema.physical_num_fields().div_ceil(8);
        writer.write_all(&tuple.n_fields.to_le_bytes())?;
        let mut bitmap = vec![0u8; bitmap_size];
        for (i, value) in tuple.values.iter().enumerate() {
            if value.is_null() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        writer.write_all(&bitmap)?;
        for value in tuple.values.iter().filter(|v| !v.is_null()) {
            value.encode(writer)?;
        }
        Ok(())
    }
}

/// A single row - the unit every executor consumes and emits.
///
/// One tuple is one `INSERT ... VALUES (...)` row when written, or one row of a
/// `SELECT` result when read. Values are stored in the same order as the
/// columns in the corresponding [`TupleSchema`]; the schema is carried
/// alongside, not inline.
///
/// # SQL examples
///
/// Schema: `users(id, name, age)` resolved to indices `0, 1, 2`.
///
/// ```sql
/// -- INSERT INTO users VALUES (42, 'alice', 30);
/// --   Tuple::new(vec![
/// --       Value::Int32(42),
/// --       Value::String("alice".into()),
/// --       Value::Int32(30),
/// --   ])
///
/// -- SELECT age, id FROM users
/// --   tuple.project(&[2, 0])               -- (30, 42)
///
/// -- SELECT * FROM u CROSS JOIN o
/// --   user_tuple.concat(&order_tuple)      -- u columns followed by o columns
///
/// -- UPDATE users SET name = 'bob'
/// --   tuple.set_field(1, Value::String("bob".into()), &schema)
/// ```
///
/// Use [`TupleSchema::validate`] before persisting a tuple - `Tuple::new`
/// itself does no checking.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Tuple {
    pub(crate) n_fields: u16,
    pub(crate) values: Vec<Value>,
}

impl Tuple {
    #[inline]
    fn field_count_u16(len: usize) -> u16 {
        u16::try_from(len).expect("tuple field count exceeds u16::MAX")
    }

    /// Builds a tuple from the values produced by `INSERT ... VALUES (...)` or
    /// emitted by a child operator.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- INSERT INTO users VALUES (42, 'alice', 30);
    /// --   Tuple::new(vec![
    /// --       Value::Int32(42),
    /// --       Value::String("alice".into()),
    /// --       Value::Int32(30),
    /// --   ])
    /// ```
    ///
    /// Stores the values as-is - no validation. Pair with
    /// [`TupleSchema::validate`] when the row is about to be persisted.
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            n_fields: Self::field_count_u16(values.len()),
            values,
        }
    }

    /// Reads the value at column `index`, or `None` if out of bounds - the
    /// raw read used to evaluate `expression::Operand::Column(index)` against
    /// a row.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Retrieves the value at the column specified by [`ColumnId`], or `None` if
    /// the column index is out of bounds.
    ///
    /// # Arguments
    ///
    /// * `col` - The [`ColumnId`] representing the column to access.
    ///
    /// # Returns
    ///
    /// `Some(&Value)` if the column exists, or `None` if the column index is invalid.
    ///
    /// # Example
    ///
    /// ```
    /// let tuple = Tuple::new(vec![Value::Int32(1), Value::String("test".into())]);
    /// let col_id = ColumnId::from(1);
    /// assert_eq!(tuple.get_col(col_id), Some(&Value::String("test".into())));
    /// ```
    pub fn get_col(&self, col: ColumnId) -> Option<&Value> {
        self.values.get(usize::from(col))
    }

    /// Returns the number of values in this row - the arity. Should equal
    /// the matching schema's [`TupleSchema::physical_num_fields`] once validation
    /// passes.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if this row has no values - typically the empty group
    /// key used when a query has no `GROUP BY`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Reads the value referenced by `col`, returning
    /// [`TupleError::FieldIndexOutOfBounds`] rather than `None`.
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] when `col` does not address a value.
    pub fn value_at(&self, col: ColumnId) -> Result<&Value, TupleError> {
        let index = usize::from(col);
        self.values
            .get(index)
            .ok_or(TupleError::FieldIndexOutOfBounds { index })
    }

    /// Reads the value referenced by `col`, treating any out-of-range column
    /// reference as SQL `NULL` rather than an error.
    pub fn value_at_or_null(&self, col: ColumnId) -> &Value {
        self.values.get(usize::from(col)).unwrap_or(&Value::Null)
    }

    /// Pairs each [`Field`] in `schema` with the corresponding [`Value`] in
    /// this tuple.
    pub fn iter_with_schema<'a>(
        &'a self,
        schema: &'a TupleSchema,
    ) -> impl Iterator<Item = (&'a Field, &'a Value)> + 'a {
        schema.fields().zip(self.values.iter())
    }

    /// Mutable counterpart to [`Tuple::get`].
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Builds a new row containing only the values at `indices`.
    #[must_use]
    pub fn project(&self, indices: &[usize]) -> Self {
        let mut n_fields = 0u16;
        let values: Vec<Value> = indices
            .iter()
            .filter_map(|&i| {
                let value = self.values.get(i).cloned();
                if value.is_some() {
                    n_fields += 1;
                }
                value
            })
            .collect();
        Self { n_fields, values }
    }

    /// Iterates references to the row's values in column-declaration order.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Concatenates two rows - used to materialize the output of a JOIN.
    #[must_use]
    pub fn concat(&self, other: &Tuple) -> Self {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Self {
            n_fields: self.n_fields + other.n_fields,
            values,
        }
    }

    /// Writes the row's bytes into `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if `buf` is too small to hold the output.
    pub fn serialize(&self, schema: &TupleSchema, buf: &mut [u8]) -> Result<usize, CodecError> {
        let mut cursor = Cursor::new(buf);
        (schema, self).encode(&mut cursor)?;
        let pos = cursor.position();
        let written = usize::try_from(pos).map_err(|_| CodecError::NumericDoesNotFit {
            value: pos,
            target: "usize",
        })?;
        Ok(written)
    }

    /// Reconstructs a row from heap-page bytes - the inverse of
    /// [`Tuple::serialize`].
    ///
    /// # Errors
    ///
    /// Returns codec errors if the input is too short or corrupt.
    pub fn deserialize(schema: &TupleSchema, buf: &[u8]) -> Result<Self, CodecError> {
        use byteorder::ReadBytesExt;
        let mut reader = Cursor::new(buf);

        // Read the field count written by Encode, then size the bitmap from it.
        // This is the physical count — may be less than schema.num_fields() for
        // tuples written before a later ADD COLUMN.
        let n_fields = reader.read_u16::<byteorder::LittleEndian>()? as usize;
        let bitmap_size = n_fields.div_ceil(8);
        let mut bitmap = vec![0u8; bitmap_size];
        reader.read_exact(&mut bitmap)?;

        let mut values = Vec::with_capacity(n_fields);
        for i in 0..n_fields {
            let is_null = (bitmap[i / 8] & (1 << (i % 8))) != 0;
            if is_null {
                values.push(Value::Null);
            } else {
                values.push(Value::decode(&mut reader)?);
            }
        }

        // Pad columns added after this tuple was written with their missing value.
        for field in schema.fields().skip(n_fields) {
            values.push(field.missing_default_value.clone().unwrap_or(Value::Null));
        }

        Ok(Self {
            n_fields: Self::field_count_u16(values.len()),
            values,
        })
    }

    /// Overwrites the column at `index` with `value`, enforcing the same
    /// constraints as [`TupleSchema::validate`].
    ///
    /// # Errors
    ///
    /// - [`TupleError::FieldIndexOutOfBounds`] - invalid schema or row index
    /// - [`TupleError::NullNotAllowed`] - assigning `NULL` to a `NOT NULL` column
    /// - [`TupleError::TypeMismatch`] - value type differs from declared column type
    pub fn set_field(
        &mut self,
        index: usize,
        value: Value,
        schema: &TupleSchema,
    ) -> Result<(), TupleError> {
        let field = schema
            .field(index)
            .ok_or(TupleError::FieldIndexOutOfBounds { index })?;
        if index >= self.values.len() {
            return Err(TupleError::FieldIndexOutOfBounds { index });
        }

        if value.is_null() && !field.nullable {
            return Err(TupleError::NullNotAllowed {
                column: field.name.to_string(),
            });
        }

        if let Some(value_type) = value.get_type()
            && value_type != field.field_type
        {
            return Err(TupleError::TypeMismatch {
                column: field.name.to_string(),
                expected: field.field_type,
                actual: value_type,
            });
        }

        self.values[index] = value;
        Ok(())
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{value}")?;
        }
        write!(f, ")")
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

/// Consumes the tuple and iterates over its values by value.
impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// Iterates over references to the tuple's values without consuming it.
impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::{CodecError, Encode},
        types::{Type, Value},
    };

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn schema_id_name_age() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Int32).not_null(),
            field("name", Type::String),
            field("age", Type::Int32),
        ])
    }

    fn tuple_42_alice_30() -> Tuple {
        Tuple::new(vec![
            Value::Int32(42),
            Value::String("alice".into()),
            Value::Int32(30),
        ])
    }

    #[test]
    fn tuple_get_and_get_mut() {
        let mut tuple = tuple_42_alice_30();
        assert_eq!(tuple.get(0), Some(&Value::Int32(42)));
        assert!(tuple.get(99).is_none());

        *tuple.get_mut(0).unwrap() = Value::Int32(99);
        assert_eq!(tuple.get(0), Some(&Value::Int32(99)));
    }

    #[test]
    fn set_field_updates_compatible_value() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        tuple
            .set_field(1, Value::String("bob".into()), &schema)
            .unwrap();
        assert_eq!(tuple.get(1), Some(&Value::String("bob".into())));
        assert_eq!(tuple.get(0), Some(&Value::Int32(42)));
    }

    #[test]
    fn set_field_updates_int_in_place() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        tuple.set_field(2, Value::Int32(31), &schema).unwrap();
        assert_eq!(tuple.get(2), Some(&Value::Int32(31)));
    }

    #[test]
    fn set_field_index_out_of_bounds() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        let err = tuple.set_field(99, Value::Int32(1), &schema).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
    }

    #[test]
    fn set_field_type_mismatch() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        let err = tuple.set_field(0, Value::Int64(1), &schema).unwrap_err();
        assert!(matches!(
            err,
            TupleError::TypeMismatch {
                ref column,
                expected: Type::Int32,
                actual: Type::Int64
            } if column == "id"
        ));
    }

    #[test]
    fn set_field_null_into_nullable_column_ok() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        tuple.set_field(1, Value::Null, &schema).unwrap();
        assert_eq!(tuple.get(1), Some(&Value::Null));
    }

    #[test]
    fn set_field_null_into_not_null_column_rejected() {
        let schema = schema_id_name_age();
        let mut tuple = tuple_42_alice_30();
        let err = tuple.set_field(0, Value::Null, &schema).unwrap_err();
        assert!(matches!(
            err,
            TupleError::NullNotAllowed { column } if column == "id"
        ));
    }

    #[test]
    fn tuple_project_keeps_order() {
        let tuple = tuple_42_alice_30();
        let projected = tuple.project(&[2, 0]);
        assert_eq!(projected.get(0), Some(&Value::Int32(30)));
        assert_eq!(projected.get(1), Some(&Value::Int32(42)));
    }

    #[test]
    fn tuple_project_skips_oob_silently() {
        let tuple = tuple_42_alice_30();
        let projected = tuple.project(&[0, 999]);
        assert_eq!(projected.get(0), Some(&Value::Int32(42)));
        assert!(projected.get(1).is_none());
    }

    #[test]
    fn tuple_iter() {
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Bool(true)]);
        let vals: Vec<&Value> = tuple.iter().collect();
        assert_eq!(vals, [&Value::Int32(1), &Value::Bool(true)]);
    }

    #[test]
    fn tuple_concat() {
        let left = Tuple::new(vec![Value::Int32(1)]);
        let right = Tuple::new(vec![Value::Bool(false), Value::Int64(99)]);
        let joined = left.concat(&right);
        assert_eq!(joined.get(0), Some(&Value::Int32(1)));
        assert_eq!(joined.get(1), Some(&Value::Bool(false)));
        assert_eq!(joined.get(2), Some(&Value::Int64(99)));
    }

    #[test]
    fn tuple_from_vec() {
        let t: Tuple = vec![Value::Int32(7)].into();
        assert_eq!(t.get(0), Some(&Value::Int32(7)));
    }

    #[test]
    fn tuple_into_iter_consuming() {
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
        let vals: Vec<Value> = tuple.into_iter().collect();
        assert_eq!(vals, [Value::Int32(1), Value::Int32(2)]);
    }

    #[test]
    fn tuple_into_iter_ref() {
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
        let vals: Vec<&Value> = (&tuple).into_iter().collect();
        assert_eq!(vals, [&Value::Int32(1), &Value::Int32(2)]);
    }

    #[test]
    fn tuple_display() {
        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::String("hi".into()),
            Value::Null,
        ]);
        assert_eq!(tuple.to_string(), "(1, 'hi', NULL)");
    }

    fn roundtrip(schema: &TupleSchema, tuple: &Tuple) -> Tuple {
        let size = schema.serialized_size();
        let mut buf = vec![0u8; size];
        tuple.serialize(schema, &mut buf).unwrap();
        Tuple::deserialize(schema, &buf).unwrap()
    }

    #[test]
    fn encode_matches_serialize_then_deserialize() {
        let schema = schema_id_name_age();
        let tuple = tuple_42_alice_30();

        let mut enc = Vec::new();
        (&schema, &tuple).encode(&mut enc).unwrap();

        let mut buf = vec![0u8; schema.serialized_size()];
        let n = tuple.serialize(&schema, &mut buf).unwrap();
        assert_eq!(enc, &buf[..n]);

        assert_eq!(Tuple::deserialize(&schema, &enc).unwrap(), tuple);
        assert_eq!(Tuple::deserialize(&schema, &buf[..n]).unwrap(), tuple);
    }

    #[test]
    fn serialize_deserialize_all_fixed_types() {
        let schema = TupleSchema::new(vec![
            field("i32", Type::Int32),
            field("i64", Type::Int64),
            field("u32", Type::Uint32),
            field("u64", Type::Uint64),
            field("f64", Type::Float64),
            field("b", Type::Bool),
        ]);
        let tuple = Tuple::new(vec![
            Value::Int32(-1),
            Value::Int64(i64::MIN),
            Value::Uint32(u32::MAX),
            Value::Uint64(u64::MAX),
            Value::Float64(1.5),
            Value::Bool(true),
        ]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_deserialize_string() {
        let schema = TupleSchema::new(vec![field("s", Type::String)]);
        let tuple = Tuple::new(vec![Value::String("hello, world".into())]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_deserialize_with_nulls() {
        let schema = TupleSchema::new(vec![
            field("a", Type::Int32),
            field("b", Type::Int32),
            field("c", Type::Int32),
        ]);
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Null, Value::Int32(3)]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_deserialize_all_nulls() {
        let schema = TupleSchema::new(vec![field("x", Type::Bool), field("y", Type::Int64)]);
        let tuple = Tuple::new(vec![Value::Null, Value::Null]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_returns_bytes_written() {
        let schema = TupleSchema::new(vec![field("n", Type::Int32), field("b", Type::Bool)]);
        let tuple = Tuple::new(vec![Value::Int32(7), Value::Bool(true)]);
        let mut buf = vec![0u8; schema.serialized_size()];
        let written = tuple.serialize(&schema, &mut buf).unwrap();
        assert_eq!(written, 10);
    }

    #[test]
    fn serialize_buffer_too_small_returns_error() {
        let schema = TupleSchema::new(vec![field("a", Type::Int32)]);
        let tuple = Tuple::new(vec![Value::Int32(1)]);
        let mut buf: Vec<u8> = vec![];
        let err = tuple.serialize(&schema, &mut buf).unwrap_err();
        assert!(matches!(err, CodecError::Io(_)));
    }

    #[test]
    fn deserialize_buffer_too_small_returns_error() {
        let schema = TupleSchema::new(vec![field("a", Type::Int32)]);
        let err = Tuple::deserialize(&schema, &[]).unwrap_err();
        assert!(matches!(err, CodecError::Io(_)));
    }

    #[test]
    fn tuple_len_and_is_empty() {
        let t = tuple_42_alice_30();
        assert_eq!(t.len(), 3);
        assert!(!t.is_empty());

        let empty = Tuple::new(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn tuple_value_at_in_range() {
        let t = tuple_42_alice_30();
        let col = ColumnId::try_from(0u32).unwrap();
        assert_eq!(t.value_at(col).unwrap(), &Value::Int32(42));
    }

    #[test]
    fn tuple_value_at_out_of_range() {
        let t = tuple_42_alice_30();
        let col = ColumnId::try_from(99u32).unwrap();
        let err = t.value_at(col).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
    }

    #[test]
    fn tuple_value_at_or_null_in_range() {
        let t = tuple_42_alice_30();
        let col = ColumnId::try_from(0u32).unwrap();
        assert_eq!(t.value_at_or_null(col), &Value::Int32(42));
    }

    #[test]
    fn tuple_value_at_or_null_out_of_range() {
        let t = tuple_42_alice_30();
        let col = ColumnId::try_from(99u32).unwrap();
        assert_eq!(t.value_at_or_null(col), &Value::Null);
    }

    #[test]
    fn tuple_iter_with_schema_pairs() {
        let schema = schema_id_name_age();
        let tuple = tuple_42_alice_30();
        let pairs: Vec<(&str, &Value)> = tuple
            .iter_with_schema(&schema)
            .map(|(f, v)| (f.name.as_str(), v))
            .collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("id", &Value::Int32(42)));
        assert_eq!(pairs[1], ("name", &Value::String("alice".into())));
        assert_eq!(pairs[2], ("age", &Value::Int32(30)));
    }

    #[test]
    fn null_bitmap_encoding_correctness() {
        let fields: Vec<Field> = (0..8)
            .map(|i| field(format!("c{i}").as_str(), Type::Bool))
            .collect();
        let schema = TupleSchema::new(fields);

        let mut values: Vec<Value> = (0..8).map(|_| Value::Bool(false)).collect();
        values[0] = Value::Null;
        values[3] = Value::Null;
        values[7] = Value::Null;

        let tuple = Tuple::new(values);
        let mut buf = vec![0u8; schema.serialized_size()];
        tuple.serialize(&schema, &mut buf).unwrap();

        assert_eq!(buf[2], 0b1000_1001, "null bitmap byte mismatch");

        let restored = Tuple::deserialize(&schema, &buf).unwrap();
        assert_eq!(restored.get(0), Some(&Value::Null));
        assert_eq!(restored.get(3), Some(&Value::Null));
        assert_eq!(restored.get(7), Some(&Value::Null));
        assert_eq!(restored.get(1), Some(&Value::Bool(false)));
    }
}
