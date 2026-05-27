use std::{collections::HashMap, fmt, mem::size_of};

use super::{Tuple, TupleError};
use crate::{
    STRING_MAX_SIZE, Value,
    primitives::{ColumnId, NameError, NonEmptyString},
    types::Type,
};

/// One column declared in `CREATE TABLE` - name, type, and the `NOT NULL` flag.
///
/// # SQL examples
///
/// ```sql
/// -- CREATE TABLE users (
/// --   id   INT  NOT NULL,            -- Field::new("id",   Type::Int32).not_null()
/// --   name VARCHAR,                  -- Field::new("name", Type::String)
/// --   age  INT                       -- Field::new("age",  Type::Int32)
/// -- );
/// ```
///
/// Columns default to nullable; call [`Field::not_null`] to tighten the
/// constraint. The default matches SQL: a column is nullable unless declared
/// `NOT NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: NonEmptyString,
    pub field_type: Type,
    pub nullable: bool,
    pub is_dropped: bool,
    pub missing_default_value: Option<Value>,
}

impl Field {
    /// Builds a nullable column - the `CREATE TABLE` default.
    ///
    /// # Errors
    ///
    /// Returns [`NameError`] if `name` violates the [`NonEmptyString`]
    /// invariant (empty, NUL byte, or longer than `MAX_NAME_LEN`).
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- name VARCHAR
    /// --   Field::new("name", Type::String)
    /// ```
    pub fn new(name: impl Into<String>, field_type: Type) -> Result<Self, NameError> {
        Ok(Self {
            name: NonEmptyString::new(name)?,
            field_type,
            nullable: true,
            is_dropped: false,
            missing_default_value: None,
        })
    }

    pub fn new_non_empty(name: NonEmptyString, field_type: Type) -> Self {
        Self {
            name,
            field_type,
            nullable: true,
            is_dropped: false,
            missing_default_value: None,
        }
    }

    /// Marks this field (column) as `NOT NULL`, meaning it cannot store NULL values.
    ///
    /// The default for columns is nullable (`NULL`). Calling `not_null` changes
    /// the field such that it enforces non-nullability.
    ///
    /// # Returns
    ///
    /// Returns a new [`Field`] with `nullable` set to `false` (not null constraint).
    #[must_use]
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Sets the name of this field (column) to a new value.
    ///
    /// This mutates the field in place and returns a mutable reference to self, allowing for
    /// chained calls.
    ///
    /// # SQL examples
    ///
    /// Used by SQL projections that emit an alias with `AS`:
    ///
    /// ```sql
    /// -- SELECT column_name AS alias
    /// --   field.set_name("alias")
    /// ```
    /// # Errors
    ///
    /// Returns [`NameError`] if `name` violates the [`NonEmptyString`] invariant.
    pub fn set_name(&mut self, name: impl Into<String>) -> Result<&mut Self, NameError> {
        self.name = NonEmptyString::new(name)?;
        Ok(self)
    }

    /// Sets the default value used when this column is added after existing
    /// rows already exist.
    ///
    /// This is mainly used by schema evolution paths (`ALTER TABLE ... ADD COLUMN`)
    /// so old tuples can read a sensible value for the newly introduced field.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
    /// --   field.set_missing_default_value(Value::bool(true))
    /// ```
    #[must_use]
    pub fn set_missing_default_value(&mut self, value: Value) -> &mut Self {
        self.missing_default_value = Some(value);
        self
    }

    /// Marks whether this field is logically dropped while metadata is kept.
    ///
    /// A dropped field can remain in catalog/schema metadata for compatibility
    /// during migration or rewrite steps, even though it should no longer appear
    /// in normal query projections.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- ALTER TABLE users DROP COLUMN middle_name;
    /// --   field.set_is_dropped(true)
    /// ```
    #[must_use]
    pub fn set_is_dropped(&mut self, is_dropped: bool) -> &mut Self {
        self.is_dropped = is_dropped;
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

/// The ordered column list of a table or operator output - what `CREATE TABLE`
/// produces and what every executor advertises as its row layout.
///
/// `TupleSchema` keeps both a `Vec<Field>` for index-based access (the way the
/// executor walks columns) and a `HashMap<name, index>` for the binder's
/// name-to-index resolution, so both operations are O(1).
///
/// # SQL examples
///
/// ```sql
/// -- CREATE TABLE users (id INT NOT NULL, name VARCHAR, age INT);
/// --
/// --   TupleSchema::new(vec![
/// --       Field::new("id",   Type::Int32).not_null(),
/// --       Field::new("name", Type::String),
/// --       Field::new("age",  Type::Int32),
/// --   ])
///
/// -- SELECT * FROM users CROSS JOIN orders
/// --   left_schema.merge(&right_schema)    -- output schema of the join
///
/// -- INSERT INTO users VALUES (1, 'a', 30);
/// --   schema.validate(&tuple)             -- enforces NOT NULL and types
/// ```
#[derive(Debug, Clone, Default)]
pub struct TupleSchema {
    fields: Vec<Field>,
    field_indices: HashMap<NonEmptyString, usize>,
}

/// Fixed-width payload bytes for schema sizing; variable-length types return 0
/// ([`Type::Text`]) or a capped worst case ([`Type::String`]).
const fn fixed_type_payload_size(ty: Type) -> usize {
    match ty {
        Type::Int32 | Type::Uint32 => 4,
        Type::Int64 | Type::Uint64 | Type::Float64 | Type::Date | Type::Time | Type::Timestamp => 8,
        Type::Bool => 1,
        Type::String => STRING_LENGTH_PREFIX_SIZE + STRING_MAX_SIZE,
        Type::Text => 0,
    }
}

/// Little-endian `u16` field count at the start of every serialized tuple.
const TUPLE_FIELD_COUNT_SIZE: usize = size_of::<u16>();

/// One null bit per column in the bitmap that follows the field count.
const NULL_BITMAP_BITS_PER_BYTE: usize = 8;

/// Little-endian `u32` [`Type`] tag before each non-null [`Value`] payload.
const VALUE_TYPE_TAG_SIZE: usize = size_of::<u32>();

/// u32 length prefix before inline UTF-8 in [`Type::String`] worst-case sizing.
const STRING_LENGTH_PREFIX_SIZE: usize = size_of::<u32>();

const fn null_bitmap_size(num_fields: usize) -> usize {
    num_fields.div_ceil(NULL_BITMAP_BITS_PER_BYTE)
}

const fn worst_case_encoded_value_size(field_type: Type) -> usize {
    VALUE_TYPE_TAG_SIZE + fixed_type_payload_size(field_type)
}

impl TupleSchema {
    /// Builds a schema from a column list - typically the column list of a
    /// `CREATE TABLE` statement, after the binder has resolved each declared
    /// type.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- CREATE TABLE users (id INT NOT NULL, name VARCHAR, age INT);
    /// --
    /// --   TupleSchema::new(vec![
    /// --       Field::new("id",   Type::Int32).not_null(),
    /// --       Field::new("name", Type::String),
    /// --       Field::new("age",  Type::Int32),
    /// --   ])
    /// ```
    ///
    /// Column names are expected to be unique; if duplicates exist, the last
    /// occurrence wins in the name-to-index map. SQL itself rejects duplicate
    /// column names earlier, so this is a defensive fallback.
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

    /// Total number of columns in the physical layout, including logically
    /// dropped ones. This is the count the storage layer uses: null-bitmap
    /// width, tuple slot sizing, join offsets, and serialization all depend
    /// on this value because on-disk rows were written with this many slots.
    #[inline]
    pub fn physical_num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Number of columns visible to SQL — dropped columns excluded. This is
    /// the arity a user must satisfy in `INSERT … VALUES (…)` and the count
    /// returned to API callers or the REPL.
    #[inline]
    pub fn logical_num_fields(&self) -> usize {
        self.fields.iter().filter(|f| !f.is_dropped).count()
    }

    pub fn col_name(&self, col_id: ColumnId) -> Option<&str> {
        self.field(usize::from(col_id)).map(|f| f.name.as_str())
    }

    /// Returns the column at `index` in declaration order, or `None` if out
    /// of bounds.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- After binding `SELECT name FROM users`, the binder reads the
    /// -- resolved column index out of the schema:
    /// --   schema.field(1)   -- &Field { name: "name", field_type: String, .. }
    /// ```
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Resolves a column name to its index and definition - what the binder
    /// calls when turning a SQL identifier like `users.name` into the column
    /// reference an executor can use.
    ///
    /// Returns `(column_id, &Field)`, or `None` if no column with that name
    /// exists.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT name FROM users;
    /// --   schema.field_by_name("name")  -> Some((Col(1), &Field { name: "name", .. }))
    ///
    /// -- SELECT missing FROM users;
    /// --   schema.field_by_name("missing") -> None        -- binder reports an unresolved column
    /// ```
    pub fn field_by_name(&self, name: &str) -> Option<(ColumnId, &Field)> {
        self.field_indices.get(name).and_then(|&i| {
            let col_id = ColumnId::try_from(i).ok()?;
            Some((col_id, &self.fields[i]))
        })
    }

    /// Iterates columns in declaration order - the order `SELECT *` exposes
    /// them and the order rows are laid out in.
    pub fn fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    /// The exact number of bytes [`Tuple::serialize`] will write for `tuple`.
    ///
    /// Unlike [`TupleSchema::serialized_size`], which is a schema-level
    /// worst-case that assumes all columns are non-null and uses fixed maximum
    /// sizes, this method walks the actual values so it accounts for:
    ///
    /// - **Null compression** — null slots contribute nothing to the payload, only a bit in the
    ///   bitmap.
    /// - **TEXT columns** — [`Type::Text`] has no fixed maximum size, so the only accurate
    ///   measurement is the real string length.
    ///
    /// Use this in [`crate::heap`] code paths that need to know how large a
    /// slot to allocate before writing a tuple that contains TEXT columns.
    pub fn actual_serialized_size(&self, tuple: &Tuple) -> usize {
        let payload: usize = tuple
            .values
            .iter()
            .filter(|v| !v.is_null())
            .map(Value::encoded_size)
            .sum();
        TUPLE_FIELD_COUNT_SIZE + null_bitmap_size(self.physical_num_fields()) + payload
    }

    /// The exact number of bytes [`Tuple::serialize`] writes for this schema
    /// in the worst case (all columns non-null) - i.e. how big a heap-page
    /// slot must be to hold one row produced by `INSERT`.
    ///
    /// Layout: 2-byte field count + null bitmap (`ceil(n/8)` bytes) + one encoded
    /// [`Value`] per non-null column (4-byte [`Type`] tag + payload).
    pub fn serialized_size(&self) -> usize {
        TUPLE_FIELD_COUNT_SIZE
            + null_bitmap_size(self.physical_num_fields())
            + self
                .fields
                .iter()
                .map(|f| worst_case_encoded_value_size(f.field_type))
                .sum::<usize>()
    }

    /// Concatenates two schemas - the schema-level counterpart to
    /// [`Tuple::concat`]. Builds the output schema of any operator that places
    /// the right input's columns after the left input's, principally JOIN.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT * FROM users JOIN orders ON ...
    /// --
    /// -- users(id, name, age)  ⋈  orders(order_id, user_id, total)
    /// --   = (id, name, age, order_id, user_id, total)
    /// --
    /// --   users_schema.merge(&orders_schema)
    /// ```
    #[must_use]
    pub fn merge(&self, other: &TupleSchema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        Self::new(fields)
    }

    /// Checks that a row honours every column constraint - the gate `INSERT`
    /// runs before a tuple is written to a heap page.
    ///
    /// Verifies that:
    /// - the number of values equals the **physical** column count (including dropped slots, which
    ///   carry a placeholder `NULL`),
    /// - no live `NOT NULL` column holds a `NULL` value, and
    /// - each non-null live value's runtime type matches the column's declared type.
    ///
    /// Dropped columns (`field.is_dropped == true`) are skipped for the null
    /// and type checks: their slot is always filled with `NULL` or a stored
    /// default regardless of the original constraint.
    ///
    /// # Errors
    ///
    /// Returns the first [`TupleError`] encountered, in the order checks run:
    /// - [`TupleError::FieldCountMismatch`] - `VALUES` arity does not match the physical column
    ///   count
    /// - [`TupleError::NullNotAllowed`] - `NULL` written to a live `NOT NULL` column
    /// - [`TupleError::TypeMismatch`] - value type differs from a live column's declared type
    pub fn validate(&self, tuple: &Tuple) -> Result<(), TupleError> {
        if tuple.len() != self.fields.len() {
            return Err(TupleError::FieldCountMismatch {
                expected: self.fields.len(),
                actual: tuple.len(),
            });
        }

        for (field, value) in self.fields.iter().zip(tuple.iter()) {
            if field.is_dropped {
                continue;
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
        }

        Ok(())
    }

    /// Like [`TupleSchema::field`], but returns
    /// [`TupleError::FieldIndexOutOfBounds`] instead of `None`.
    ///
    /// The `?`-friendly counterpart for code paths that already return
    /// `Result<_, TupleError>` - an unresolved column reference becomes an
    /// early return instead of an `expect`/`unwrap`.
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] when `index >= self.physical_num_fields()` -
    /// usually means a `SELECT col` referenced something past the end of the
    /// child operator's output schema.
    pub fn field_or_err(&self, index: usize) -> Result<&Field, TupleError> {
        self.fields
            .get(index)
            .ok_or(TupleError::FieldIndexOutOfBounds { index })
    }

    /// Resolves a [`ColumnId`] (a typed column reference, as carried by
    /// [`crate::execution::aggregate::AggregateExpr`] and friends) to its
    /// [`Field`].
    ///
    /// Equivalent to [`TupleSchema::field_or_err`] with `usize::from(col)`,
    /// but the typed argument documents that the index is a bound column
    /// reference rather than a raw offset.
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] when `col` does not address a
    /// column in this schema.
    pub fn field_of(&self, col: ColumnId) -> Result<&Field, TupleError> {
        self.field_or_err(usize::from(col))
    }

    /// Selects a subset of columns by physical index, returning their
    /// [`Field`] definitions in the requested order.
    ///
    /// Useful for operators that build their output schema by concatenating a
    /// projection of the child with extra columns of their own.
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] for the first index in `indices`
    /// that is past the end of the schema - usually a sign the binder
    /// produced a `GROUP BY` index that does not match the child's width.
    pub fn project_fields(
        &self,
        indices: impl IntoIterator<Item = usize>,
    ) -> Result<Vec<Field>, TupleError> {
        indices
            .into_iter()
            .map(|i| {
                self.fields
                    .get(i)
                    .cloned()
                    .ok_or(TupleError::FieldIndexOutOfBounds { index: i })
            })
            .collect()
    }

    pub fn logical_iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter().filter(|f| !f.is_dropped)
    }

    pub fn physical_iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Type, Value};

    fn schema_id_name_age() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32).unwrap().not_null(),
            Field::new("name", Type::String).unwrap(),
            Field::new("age", Type::Int32).unwrap(),
        ])
    }

    fn tuple_42_alice_30() -> Tuple {
        Tuple::new(vec![
            Value::int32(42),
            Value::varchar("alice".into()),
            Value::int32(30),
        ])
    }

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    #[test]
    fn field_new_is_nullable() {
        let f = field("score", Type::Float64);
        assert_eq!(f.name, "score");
        assert_eq!(f.field_type, Type::Float64);
        assert!(f.nullable);
    }

    #[test]
    fn field_not_null_clears_flag() {
        let f = field("id", Type::Int64).not_null();
        assert!(!f.nullable);
    }

    #[test]
    fn field_display_nullable() {
        let f = field("score", Type::Int32);
        assert_eq!(f.to_string(), "score INT");
    }

    #[test]
    fn field_display_not_null() {
        let f = field("id", Type::Int64).not_null();
        assert_eq!(f.to_string(), "id BIGINT NOT NULL");
    }

    #[test]
    fn schema_physical_num_fields() {
        assert_eq!(TupleSchema::new(vec![]).physical_num_fields(), 0);
        assert_eq!(schema_id_name_age().physical_num_fields(), 3);
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
        assert_eq!(usize::from(idx), 1);
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
    fn schema_serialized_size() {
        let schema = schema_id_name_age();
        // field-count header + null bitmap + three encoded values (type tag + payload each)
        assert_eq!(
            schema.serialized_size(),
            TUPLE_FIELD_COUNT_SIZE
                + null_bitmap_size(3)
                + (8 + (VALUE_TYPE_TAG_SIZE + STRING_LENGTH_PREFIX_SIZE + STRING_MAX_SIZE) + 8)
        );
    }

    #[test]
    fn schema_serialized_size_bitmap_grows_at_9_fields() {
        let fields: Vec<Field> = (0..9)
            .map(|i| field(format!("c{i}").as_str(), Type::Bool))
            .collect();
        let schema = TupleSchema::new(fields);
        assert_eq!(
            schema.serialized_size(),
            TUPLE_FIELD_COUNT_SIZE
                + null_bitmap_size(9)
                + 9 * worst_case_encoded_value_size(Type::Bool)
        );
    }

    #[test]
    fn schema_merge() {
        let left = TupleSchema::new(vec![field("a", Type::Int32)]);
        let right = TupleSchema::new(vec![Field::new("b", Type::Int64).unwrap()]);
        let merged = left.merge(&right);
        assert_eq!(merged.physical_num_fields(), 2);
        assert_eq!(merged.field(0).unwrap().name, "a");
        assert_eq!(merged.field(1).unwrap().name, "b");
    }

    #[test]
    fn schema_display() {
        let schema = TupleSchema::new(vec![
            field("id", Type::Int32).not_null(),
            field("flag", Type::Bool),
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
        let schema = schema_id_name_age();
        let tuple = Tuple::new(vec![Value::int32(1), Value::Null, Value::Null]);
        assert!(schema.validate(&tuple).is_ok());
    }

    #[test]
    fn validate_field_count_mismatch() {
        let schema = schema_id_name_age();
        let tuple = Tuple::new(vec![Value::int32(1)]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(err, TupleError::FieldCountMismatch {
            expected: 3,
            actual: 1
        }));
    }

    #[test]
    fn validate_null_not_allowed() {
        let schema = schema_id_name_age();
        let tuple = Tuple::new(vec![Value::Null, Value::Null, Value::Null]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(err, TupleError::NullNotAllowed { column } if column == "id"));
    }

    #[test]
    fn validate_type_mismatch() {
        let schema = schema_id_name_age();
        let tuple = Tuple::new(vec![Value::bool(true), Value::Null, Value::Null]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(
            err,
            TupleError::TypeMismatch {
                ref column,
                expected: Type::Int32,
                actual: Type::Bool
            } if column == "id"
        ));
    }

    #[test]
    fn schema_field_or_err_in_range() {
        let schema = schema_id_name_age();
        assert_eq!(schema.field_or_err(0).unwrap().name, "id");
    }

    #[test]
    fn schema_field_or_err_out_of_range() {
        let schema = schema_id_name_age();
        let err = schema.field_or_err(99).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
    }

    #[test]
    fn schema_field_of_column_id() {
        let schema = schema_id_name_age();
        let col = ColumnId::try_from(1u32).unwrap();
        assert_eq!(schema.field_of(col).unwrap().name, "name");
    }

    #[test]
    fn schema_project_fields_keeps_order() {
        let schema = schema_id_name_age();
        let fields = schema.project_fields([2_usize, 0]).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "age");
        assert_eq!(fields[1].name, "id");
    }

    #[test]
    fn schema_project_fields_out_of_range() {
        let schema = schema_id_name_age();
        let err = schema.project_fields([0_usize, 99]).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
    }

    // Dropped columns are skipped entirely by validate — NULL is always
    // acceptable in their slot, even if the original constraint was NOT NULL.
    #[test]
    fn validate_skips_dropped_not_null_column() {
        // Schema: id NOT NULL, name NOT NULL (dropped), age nullable.
        let mut name_field = Field::new("name", Type::String).unwrap().not_null();
        let _ = name_field.set_is_dropped(true);

        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int32).unwrap().not_null(),
            name_field,
            Field::new("age", Type::Int32).unwrap(),
        ]);

        // Slot 1 is NULL (the dropped column's placeholder) — must not error.
        let tuple = Tuple::new(vec![Value::int32(1), Value::Null, Value::int32(30)]);
        assert!(schema.validate(&tuple).is_ok());
    }

    // Dropped columns are also skipped for the type check.
    #[test]
    fn validate_skips_type_check_for_dropped_column() {
        let mut dropped = Field::new("tag", Type::String).unwrap();
        let _ = dropped.set_is_dropped(true);

        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int32).unwrap().not_null(),
            dropped,
        ]);

        // Value::Bool in a STRING slot would normally be a TypeMismatch,
        // but the column is dropped so it must be ignored.
        let tuple = Tuple::new(vec![Value::int32(1), Value::bool(true)]);
        assert!(schema.validate(&tuple).is_ok());
    }

    // Live columns still have their constraints enforced after a drop.
    #[test]
    fn validate_still_enforces_live_not_null_after_drop() {
        let mut dropped = Field::new("tag", Type::String).unwrap();
        let _ = dropped.set_is_dropped(true);

        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int32).unwrap().not_null(),
            dropped,
        ]);

        // id is NOT NULL and live — writing NULL there must still fail.
        let tuple = Tuple::new(vec![Value::Null, Value::Null]);
        let err = schema.validate(&tuple).unwrap_err();
        assert!(matches!(err, TupleError::NullNotAllowed { column } if column == "id"));
    }
}
