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
//! 2. Build a [`Tuple`] from a `Vec<Value>` and call [`TupleSchema::validate`]
//!    to check nullability and type constraints before storing.
//! 3. Use [`Tuple::serialize`] / [`Tuple::deserialize`] for on-disk I/O.
//!
//! # Module contents
//!
//! - [`Field`] — column definition with name, type, and nullability constraint
//! - [`TupleSchema`] — ordered list of fields that describes a relation's shape
//! - [`Tuple`] — a row of [`Value`]s
//! - [`TupleError`] — errors produced by schema validation and index operations

use std::collections::HashMap;
use std::fmt;

use crate::types::{SerializationError, Type, Value};

use thiserror::Error;

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
#[derive(Debug, Clone)]
pub struct Field {
    /// The column name, unique within a schema.
    pub name: String,
    /// The declared storage type for this column.
    pub field_type: Type,
    /// Whether this column accepts `NULL` values.
    pub nullable: bool,
}

impl Field {
    /// Creates a new nullable field with the given name and type.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::tuple::Field;
    /// use db::types::Type;
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
    /// use db::tuple::Field;
    /// use db::types::Type;
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

    /// Returns the exact number of bytes [`Tuple::serialize`] writes for this schema.
    ///
    /// This is [`tuple_size`](Self::tuple_size) plus a null bitmap: one bit per
    /// field, rounded up to the nearest byte. Use this — not `tuple_size` — whenever
    /// sizing a buffer for on-disk storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use db::tuple::{Field, TupleSchema};
    /// use db::types::Type;
    ///
    /// let schema = TupleSchema::new(vec![
    ///     Field::new("id", Type::Int32),   // 4 bytes
    ///     Field::new("ok", Type::Bool),    // 1 byte
    /// ]);
    /// // 2 fields → 1-byte bitmap; 4 + 1 field bytes = 6 total
    /// assert_eq!(schema.serialized_size(), 6);
    /// ```
    pub fn serialized_size(&self) -> usize {
        self.num_fields().div_ceil(8) + self.tuple_size()
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

/// A single row of [`Value`]s.
///
/// Values are stored in the same order as the fields in the corresponding
/// [`TupleSchema`]. Use [`TupleSchema::validate`] to check that a tuple is
/// well-formed before persisting it.
#[derive(Debug, Clone, PartialEq, Default)]
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
    /// 1. A null bitmap — one bit per field, packed into `ceil(n / 8)` bytes.
    ///    Bit `i` is set when field `i` is `NULL`.
    /// 2. The serialized bytes of each non-null value, in field order.
    ///
    /// Returns the total number of bytes written into `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`SerializationError::BufferTooSmall`] if `buf` is shorter than
    /// the null bitmap alone. Individual value serialization errors are
    /// propagated as-is.
    pub fn serialize(
        &self,
        schema: &TupleSchema,
        buf: &mut [u8],
    ) -> Result<usize, SerializationError> {
        let bitmap_size = schema.num_fields().div_ceil(8);
        if buf.len() < bitmap_size {
            return Err(SerializationError::BufferTooSmall {
                needed: bitmap_size,
                available: buf.len(),
            });
        }

        buf[..bitmap_size].fill(0);
        for (i, value) in self.values.iter().enumerate() {
            if value.is_null() {
                buf[i / 8] |= 1 << (i % 8);
            }
        }

        let mut offset = bitmap_size;
        for value in self.values.iter().filter(|v| !v.is_null()) {
            let written = value.serialize(&mut buf[offset..])?;
            offset += written;
        }

        Ok(offset)
    }

    /// Deserializes a tuple from `buf` using the layout described by `schema`.
    ///
    /// Reads the null bitmap first, then reconstructs each value in field
    /// order — inserting [`Value::Null`] for any field whose bitmap bit is set.
    ///
    /// # Errors
    ///
    /// Returns [`SerializationError::BufferTooSmall`] if `buf` is shorter than
    /// the null bitmap. Individual value deserialization errors are propagated
    /// as-is.
    pub fn deserialize(schema: &TupleSchema, buf: &[u8]) -> Result<Self, SerializationError> {
        let mut values = Vec::with_capacity(schema.num_fields());
        let bitmap_size = schema.num_fields().div_ceil(8);

        if buf.len() < bitmap_size {
            return Err(SerializationError::BufferTooSmall {
                needed: bitmap_size,
                available: buf.len(),
            });
        }

        let mut offset = bitmap_size;

        for (i, field) in schema.fields().enumerate() {
            let is_null = (buf[i / 8] & (1 << (i % 8))) != 0;

            if is_null {
                values.push(Value::Null);
            } else {
                let value = Value::deserialize(&buf[offset..], field.field_type)?;
                offset += value.serialized_size();
                values.push(value);
            }
        }

        Ok(Self { values })
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
