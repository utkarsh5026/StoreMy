//! Tuple schema and record representation.
//!
//! A *tuple* is a single row of values in a table. Each value corresponds to a
//! *field* (column), which carries a name, a [`Type`], and a nullability flag.
//! The [`TupleSchema`] groups the fields that describe the shape of every row
//! in a relation, while [`Tuple`] holds the actual [`Value`]s for one row.
//!
//! # Typical workflow
//!
//! 1. Define fields and collect them into a [`TupleSchema`].
//! 2. Build a [`Tuple`] from a `Vec<Value>` and call [`TupleSchema::validate`] to check nullability
//!    and type constraints before storing.
//! 3. Use [`Tuple::serialize`] / [`Tuple::deserialize`], or [`Encode`] on `(&TupleSchema, &Tuple)`
//!    and pass the same bytes to [`Tuple::deserialize`].
//!
//! # Module contents
//!
//! - [`Field`] — column definition with name, type, and nullability constraint
//! - [`TupleSchema`] — ordered list of fields that describes a relation's shape
//! - [`Tuple`] — a row of [`Value`]s
//! - [`TupleError`] — errors produced by schema validation and index operations

use std::{
    collections::HashMap,
    fmt,
    io::{Cursor, Read, Write},
};

use thiserror::Error;

use crate::{
    codec::{CodecError, Decode, Encode},
    types::{Type, Value},
};

/// Errors that can occur when working with tuples and schemas.
#[derive(Debug, Error)]
pub enum TupleError {
    /// A field index was beyond the last field in the schema or tuple.
    #[error("field index {index} is out of bounds")]
    FieldIndexOutOfBounds {
        /// The out-of-bounds index that was requested.
        index: usize,
    },

    /// A `NULL` value was supplied for a field declared `NOT NULL`.
    #[error("null value not allowed for column '{column}'")]
    NullNotAllowed {
        /// Name of the non-nullable column that received `NULL`.
        column: String,
    },

    /// The runtime type of a value does not match the field's declared type.
    #[error("type mismatch in column '{column}': expected {expected}, got {actual}")]
    TypeMismatch {
        /// Name of the offending column.
        column: String,
        /// The type declared in the schema.
        expected: Type,
        /// The type found in the tuple value.
        actual: Type,
    },

    /// The number of values in a tuple does not match the number of fields in
    /// the schema.
    #[error("tuple has {actual} fields, expected {expected}")]
    FieldCountMismatch {
        /// Number of fields the schema declares.
        expected: usize,
        /// Number of values the tuple actually contains.
        actual: usize,
    },
}

/// A field (column) definition in a tuple schema.
///
/// Each field has a name, a [`Type`] that every stored value must conform to,
/// and a flag indicating whether `NULL` is permitted. Fields default to
/// nullable; call [`Field::not_null`] to tighten the constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub field_type: Type,
    pub nullable: bool,
}

impl Field {
    /// Creates a new nullable field with the given name and type.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::{tuple::Field, types::Type};
    ///
    /// let f = Field::new("age", Type::Int32);
    /// assert_eq!(f.name, "age");
    /// assert!(f.nullable);
    /// ```
    pub fn new(name: impl Into<String>, field_type: Type) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable: true,
        }
    }

    /// Marks this field as `NOT NULL`, consuming and returning `self`.
    ///
    /// Intended for use in builder-style chains:
    ///
    /// ```
    /// use storemy::{tuple::Field, types::Type};
    ///
    /// let f = Field::new("id", Type::Int64).not_null();
    /// assert!(!f.nullable);
    /// ```
    #[must_use]
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.field_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

/// An ordered list of [`Field`]s that describes the shape of a relation.
///
/// `TupleSchema` keeps both a `Vec` for index-based access and a `HashMap` for
/// name-based lookup, so both operations are O(1).
#[derive(Debug, Clone, Default)]
pub struct TupleSchema {
    fields: Vec<Field>,
    field_indices: HashMap<String, usize>,
}

impl TupleSchema {
    /// Creates a new schema from a list of fields.
    ///
    /// Field names are expected to be unique; if duplicates exist, the last
    /// occurrence wins in the name-to-index map.
    pub fn new(fields: Vec<Field>) -> Self {
        let field_indices = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();

        Self {
            fields,
            field_indices,
        }
    }

    /// Returns the number of fields in the schema.
    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if the schema has no fields.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns the field at `index`, or `None` if the index is out of bounds.
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Returns the position and definition of the field named `name`, or
    /// `None` if no field with that name exists.
    ///
    /// The returned pair is `(index, &Field)`.
    pub fn field_by_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.field_indices.get(name).map(|&i| (i, &self.fields[i]))
    }

    /// Returns an iterator over all fields in declaration order.
    pub fn fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    /// Returns the maximum serialized byte size of a tuple with this schema.
    ///
    /// This is the sum of [`Type::size`] for every field. Note that
    /// [`Tuple::serialize`] writes an additional null bitmap on top of this.
    pub fn tuple_size(&self) -> usize {
        self.fields.iter().map(|f| f.field_type.size()).sum()
    }

    /// Returns the exact number of bytes [`Tuple::serialize`] writes for this schema
    /// in the worst case (all fields non-null).
    ///
    /// Layout: null bitmap (`ceil(n/8)` bytes) + one 1-byte discriminant per field
    /// + the raw payload bytes for every field type. Use this — not `tuple_size` — when ever sizing
    ///   a buffer for on-disk storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::{
    ///     tuple::{Field, TupleSchema},
    ///     types::Type,
    /// };
    ///
    /// let schema = TupleSchema::new(vec![
    ///     Field::new("id", Type::Int32), // 1 disc + 4 bytes
    ///     Field::new("ok", Type::Bool),  // 1 disc + 1 byte
    /// ]);
    /// // 2 fields → 1-byte bitmap + 2 discriminants + 5 payload = 8 total
    /// assert_eq!(schema.serialized_size(), 8);
    /// ```
    pub fn serialized_size(&self) -> usize {
        self.num_fields().div_ceil(8) + self.num_fields() + self.tuple_size()
    }

    /// Creates a new schema by appending all fields from `other` after the
    /// fields in `self`.
    ///
    /// Useful for representing the output schema of a JOIN.
    #[must_use]
    pub fn merge(&self, other: &TupleSchema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        Self::new(fields)
    }

    /// Creates a new schema containing only the fields at the given `indices`.
    ///
    /// Output fields appear in the order specified by `indices`.
    ///
    /// # Errors
    ///
    /// Returns [`TupleError::FieldIndexOutOfBounds`] if any index in `indices`
    /// is greater than or equal to [`num_fields`](Self::num_fields).
    pub fn project(&self, indices: &[usize]) -> Result<Self, TupleError> {
        let fields = indices
            .iter()
            .map(|&i| {
                self.fields
                    .get(i)
                    .cloned()
                    .ok_or(TupleError::FieldIndexOutOfBounds { index: i })
            })
            .collect::<Result<Vec<_>, TupleError>>()?;
        Ok(Self::new(fields))
    }

    /// Checks that `tuple` conforms to this schema.
    ///
    /// Verifies that:
    /// - the number of values equals the number of fields,
    /// - no `NOT NULL` field holds a `NULL` value, and
    /// - each non-null value's runtime type matches the field's declared type.
    ///
    /// # Errors
    ///
    /// Returns the first [`TupleError`] encountered:
    /// - [`TupleError::FieldCountMismatch`] — wrong number of values
    /// - [`TupleError::NullNotAllowed`] — `NULL` in a non-nullable column
    /// - [`TupleError::TypeMismatch`] — value type differs from the field's declared type
    pub fn validate(&self, tuple: &Tuple) -> Result<(), TupleError> {
        if tuple.values.len() != self.fields.len() {
            return Err(TupleError::FieldCountMismatch {
                expected: self.fields.len(),
                actual: tuple.values.len(),
            });
        }

        for (field, value) in self.fields.iter().zip(tuple.values.iter()) {
            if value.is_null() && !field.nullable {
                return Err(TupleError::NullNotAllowed {
                    column: field.name.clone(),
                });
            }

            if let Some(value_type) = value.get_type()
                && value_type != field.field_type
            {
                return Err(TupleError::TypeMismatch {
                    column: field.name.clone(),
                    expected: field.field_type,
                    actual: value_type,
                });
            }
        }

        Ok(())
    }

    pub fn field_indices(&self) -> &HashMap<String, usize> {
        &self.field_indices
    }
}

impl fmt::Display for TupleSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}")?;
        }
        write!(f, ")")
    }
}

/// Encodes the tuple using `schema` for layout: null bitmap, then each
/// non-null value encoded via [`Encode for Value`] (discriminant + payload).
/// The schema itself is **not** written — only the row bytes.
impl Encode for (&TupleSchema, &Tuple) {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let (schema, tuple) = *self;
        let bitmap_size = schema.num_fields().div_ceil(8);
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

/// A single row of [`Value`]s.
///
/// Values are stored in the same order as the fields in the corresponding
/// [`TupleSchema`]. Use [`TupleSchema::validate`] to check that a tuple is
/// well-formed before persisting it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Tuple {
    values: Vec<Value>,
}

impl Tuple {
    /// Creates a tuple from a pre-built list of values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Returns a reference to the value at `index`, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns a mutable reference to the value at `index`, or `None` if out
    /// of bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Creates a new tuple containing only the values at the given `indices`.
    ///
    /// Indices that are out of bounds are silently skipped. Output values
    /// appear in the order specified by `indices`.
    #[must_use]
    pub fn project(&self, indices: &[usize]) -> Self {
        let values = indices
            .iter()
            .filter_map(|&i| self.values.get(i).cloned())
            .collect();
        Self { values }
    }

    /// Returns an iterator over references to the values in this tuple, in
    /// field-declaration order.
    ///
    /// This is the borrowing counterpart to `IntoIterator for Tuple` and is
    /// what the compiler calls when you write `for v in &tuple`.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Creates a new tuple by appending all values from `other` after the
    /// values in `self`.
    ///
    /// Used to materialize the output rows of a JOIN.
    #[must_use]
    pub fn concat(&self, other: &Tuple) -> Self {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Self { values }
    }

    /// Serializes the tuple into `buf` using the layout described by `schema`.
    ///
    /// The on-disk format is:
    /// 1. A null bitmap — one bit per field, packed into `ceil(n / 8)` bytes. Bit `i` is set when
    ///    field `i` is `NULL`.
    /// 2. Each non-null value encoded via [`Encode for Value`]: a 1-byte discriminant followed by
    ///    the little-endian payload.
    ///
    /// Returns the total number of bytes written into `buf`.
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

    /// Deserializes a tuple from `buf` using the layout described by `schema`.
    ///
    /// Reads the null bitmap first, then decodes each non-null value via
    /// [`Decode for Value`] (reads discriminant + payload). The schema is only
    /// needed for the field count; the type comes from the encoded discriminant.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if `buf` is too short, or
    /// [`CodecError::UnknownDiscriminant`] / [`CodecError::InvalidUtf8`] on
    /// corrupt data.
    pub fn deserialize(schema: &TupleSchema, buf: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Cursor::new(buf);
        let bitmap_size = schema.num_fields().div_ceil(8);
        let mut bitmap = vec![0u8; bitmap_size];
        reader.read_exact(&mut bitmap)?;

        let mut values = Vec::with_capacity(schema.num_fields());
        for i in 0..schema.num_fields() {
            let is_null = (bitmap[i / 8] & (1 << (i % 8))) != 0;
            if is_null {
                values.push(Value::Null);
            } else {
                values.push(Value::decode(&mut reader)?);
            }
        }

        Ok(Self { values })
    }

    /// Sets the value of the field at the given `index` to `value`.
    ///
    /// Uses the same rules as [`TupleSchema::validate`]: `NULL` is allowed only when the field
    /// is nullable, and non-null values must match the field's declared [`Type`].
    ///
    /// # Errors
    ///
    /// - [`TupleError::FieldIndexOutOfBounds`] — `index` is not a valid field index for `schema`,
    ///   or is past the end of this tuple's values.
    /// - [`TupleError::NullNotAllowed`] — `value` is `NULL` but the field is `NOT NULL`.
    /// - [`TupleError::TypeMismatch`] — non-null `value` has a different runtime type than the
    ///   field.
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
                column: field.name.clone(),
            });
        }

        if let Some(value_type) = value.get_type()
            && value_type != field.field_type
        {
            return Err(TupleError::TypeMismatch {
                column: field.name.clone(),
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
///
/// This impl pairs with [`Tuple::iter`] and is what the compiler calls for `for v in &tuple`.
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

    fn schema_id_name_age() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32).not_null(),
            Field::new("name", Type::String),
            Field::new("age", Type::Int32),
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
    fn field_new_is_nullable() {
        let f = Field::new("score", Type::Float64);
        assert_eq!(f.name, "score");
        assert_eq!(f.field_type, Type::Float64);
        assert!(f.nullable);
    }

    #[test]
    fn field_not_null_clears_flag() {
        let f = Field::new("id", Type::Int64).not_null();
        assert!(!f.nullable);
    }

    #[test]
    fn field_display_nullable() {
        let f = Field::new("score", Type::Int32);
        assert_eq!(f.to_string(), "score INT");
    }

    #[test]
    fn field_display_not_null() {
        let f = Field::new("id", Type::Int64).not_null();
        assert_eq!(f.to_string(), "id BIGINT NOT NULL");
    }

    #[test]
    fn schema_num_fields_and_is_empty() {
        let empty = TupleSchema::new(vec![]);
        assert!(empty.is_empty());
        assert_eq!(empty.num_fields(), 0);

        let schema = schema_id_name_age();
        assert!(!schema.is_empty());
        assert_eq!(schema.num_fields(), 3);
    }

    #[test]
    fn schema_field_by_index() {
        let schema = schema_id_name_age();
        assert_eq!(schema.field(0).unwrap().name, "id");
        assert_eq!(schema.field(1).unwrap().name, "name");
        assert_eq!(schema.field(2).unwrap().name, "age");
        assert!(schema.field(3).is_none());
    }

    #[test]
    fn schema_field_by_name() {
        let schema = schema_id_name_age();

        let (idx, field) = schema.field_by_name("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(field.field_type, Type::String);

        assert!(schema.field_by_name("missing").is_none());
    }

    #[test]
    fn schema_fields_iterator_order() {
        let schema = schema_id_name_age();
        let names: Vec<&str> = schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "age"]);
    }

    #[test]
    fn schema_tuple_size() {
        // Int32 = 4, String = 4+255, Int32 = 4  → 267
        let schema = schema_id_name_age();
        assert_eq!(schema.tuple_size(), 4 + (4 + 255) + 4);
    }

    #[test]
    fn schema_serialized_size() {
        // 3 fields → 1-byte bitmap + 3 discriminants + 267 payload = 271
        let schema = schema_id_name_age();
        assert_eq!(schema.serialized_size(), 1 + 3 + schema.tuple_size());
    }

    #[test]
    fn schema_serialized_size_bitmap_grows_at_9_fields() {
        // 9 fields → 2-byte bitmap + 9 discriminants + 9 Bool bytes = 20
        let fields: Vec<Field> = (0..9)
            .map(|i| Field::new(format!("c{i}"), Type::Bool))
            .collect();
        let schema = TupleSchema::new(fields);
        assert_eq!(schema.serialized_size(), 2 + 9 + 9);
    }

    #[test]
    fn schema_merge() {
        let left = TupleSchema::new(vec![Field::new("a", Type::Int32)]);
        let right = TupleSchema::new(vec![Field::new("b", Type::Int64)]);
        let merged = left.merge(&right);

        assert_eq!(merged.num_fields(), 2);
        assert_eq!(merged.field(0).unwrap().name, "a");
        assert_eq!(merged.field(1).unwrap().name, "b");
    }

    #[test]
    fn schema_project_valid_indices() {
        let schema = schema_id_name_age();
        let projected = schema.project(&[2, 0]).unwrap();

        assert_eq!(projected.num_fields(), 2);
        assert_eq!(projected.field(0).unwrap().name, "age");
        assert_eq!(projected.field(1).unwrap().name, "id");
    }

    #[test]
    fn schema_project_out_of_bounds_returns_error() {
        let schema = schema_id_name_age();
        let err = schema.project(&[0, 99]).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
    }

    #[test]
    fn schema_display() {
        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int32).not_null(),
            Field::new("flag", Type::Bool),
        ]);
        assert_eq!(schema.to_string(), "(id INT NOT NULL, flag BOOLEAN)");
    }

    #[test]
    fn validate_ok() {
        let schema = schema_id_name_age();
        let tuple = tuple_42_alice_30();
        assert!(schema.validate(&tuple).is_ok());
    }

    #[test]
    fn validate_null_in_nullable_column_ok() {
        let schema = schema_id_name_age(); // name and age are nullable
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Null, Value::Null]);
        assert!(schema.validate(&tuple).is_ok());
    }

    #[test]
    fn validate_field_count_mismatch() {
        let schema = schema_id_name_age();
        let tuple = Tuple::new(vec![Value::Int32(1)]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(err, TupleError::FieldCountMismatch {
            expected: 3,
            actual: 1
        }));
    }

    #[test]
    fn validate_null_not_allowed() {
        let schema = schema_id_name_age(); // "id" is NOT NULL
        let tuple = Tuple::new(vec![Value::Null, Value::Null, Value::Null]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(err, TupleError::NullNotAllowed { column } if column == "id"));
    }

    #[test]
    fn validate_type_mismatch() {
        let schema = schema_id_name_age(); // id is Int32
        let tuple = Tuple::new(vec![
            Value::Bool(true), // wrong type
            Value::Null,
            Value::Null,
        ]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(
            err,
            TupleError::TypeMismatch { ref column, expected: Type::Int32, actual: Type::Bool }
            if column == "id"
        ));
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

        // Both paths must produce identical bytes.
        let mut enc = Vec::new();
        (&schema, &tuple).encode(&mut enc).unwrap();

        let mut buf = vec![0u8; schema.serialized_size()];
        let n = tuple.serialize(&schema, &mut buf).unwrap();
        assert_eq!(enc, &buf[..n]);

        // Round-trip via both entry points must recover the original tuple.
        assert_eq!(Tuple::deserialize(&schema, &enc).unwrap(), tuple);
        assert_eq!(Tuple::deserialize(&schema, &buf[..n]).unwrap(), tuple);
    }

    #[test]
    fn serialize_deserialize_all_fixed_types() {
        let schema = TupleSchema::new(vec![
            Field::new("i32", Type::Int32),
            Field::new("i64", Type::Int64),
            Field::new("u32", Type::Uint32),
            Field::new("u64", Type::Uint64),
            Field::new("f64", Type::Float64),
            Field::new("b", Type::Bool),
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
        let schema = TupleSchema::new(vec![Field::new("s", Type::String)]);
        let tuple = Tuple::new(vec![Value::String("hello, world".into())]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_deserialize_with_nulls() {
        let schema = TupleSchema::new(vec![
            Field::new("a", Type::Int32),
            Field::new("b", Type::Int32),
            Field::new("c", Type::Int32),
        ]);
        // middle value is NULL
        let tuple = Tuple::new(vec![Value::Int32(1), Value::Null, Value::Int32(3)]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_deserialize_all_nulls() {
        let schema = TupleSchema::new(vec![
            Field::new("x", Type::Bool),
            Field::new("y", Type::Int64),
        ]);
        let tuple = Tuple::new(vec![Value::Null, Value::Null]);
        assert_eq!(roundtrip(&schema, &tuple), tuple);
    }

    #[test]
    fn serialize_returns_bytes_written() {
        // Int32(7) + Bool(true) → 1-byte bitmap + (1 disc + 4) + (1 disc + 1) = 8 bytes
        let schema = TupleSchema::new(vec![
            Field::new("n", Type::Int32),
            Field::new("b", Type::Bool),
        ]);
        let tuple = Tuple::new(vec![Value::Int32(7), Value::Bool(true)]);
        let mut buf = vec![0u8; schema.serialized_size()];
        let written = tuple.serialize(&schema, &mut buf).unwrap();
        assert_eq!(written, 8);
    }

    #[test]
    fn serialize_buffer_too_small_returns_error() {
        let schema = TupleSchema::new(vec![Field::new("a", Type::Int32)]);
        let tuple = Tuple::new(vec![Value::Int32(1)]);
        let mut buf: Vec<u8> = vec![]; // no room even for the 1-byte bitmap
        let err = tuple.serialize(&schema, &mut buf).unwrap_err();
        assert!(matches!(err, CodecError::Io(_)));
    }

    #[test]
    fn deserialize_buffer_too_small_returns_error() {
        let schema = TupleSchema::new(vec![Field::new("a", Type::Int32)]);
        let err = Tuple::deserialize(&schema, &[]).unwrap_err();
        assert!(matches!(err, CodecError::Io(_)));
    }

    #[test]
    fn null_bitmap_encoding_correctness() {
        // 8 fields; set bits 0, 3, 7 as null → bitmap byte = 0b10001001 = 0x89
        let fields: Vec<Field> = (0..8)
            .map(|i| Field::new(format!("c{i}"), Type::Bool))
            .collect();
        let schema = TupleSchema::new(fields);

        let mut values: Vec<Value> = (0..8).map(|_| Value::Bool(false)).collect();
        values[0] = Value::Null;
        values[3] = Value::Null;
        values[7] = Value::Null;

        let tuple = Tuple::new(values);
        let mut buf = vec![0u8; schema.serialized_size()];
        tuple.serialize(&schema, &mut buf).unwrap();

        assert_eq!(buf[0], 0b1000_1001, "null bitmap byte mismatch");

        let restored = Tuple::deserialize(&schema, &buf).unwrap();
        assert_eq!(restored.get(0), Some(&Value::Null));
        assert_eq!(restored.get(3), Some(&Value::Null));
        assert_eq!(restored.get(7), Some(&Value::Null));
        assert_eq!(restored.get(1), Some(&Value::Bool(false)));
    }
}
