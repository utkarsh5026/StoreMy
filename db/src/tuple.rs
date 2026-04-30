//! Row and schema — the data model the executor produces and consumes.
//!
//! Every operator in [`crate::execution`] takes [`Tuple`]s in and produces
//! [`Tuple`]s out, all interpreted relative to a [`TupleSchema`]. This module
//! defines those two shapes and the SQL-level constraints they encode:
//!
//! ```sql
//! CREATE TABLE users (
//!     id   INT     NOT NULL,
//!     name VARCHAR,
//!     age  INT
//! );
//!
//! -- After CREATE: the binder builds a TupleSchema with three Fields.
//! -- After INSERT: each row is a Tuple validated against that schema.
//! -- During SELECT: scan/filter/join operators pass Tuples through whose
//! --                layout is described by the operator's output schema.
//! ```
//!
//! # Shape
//!
//! - [`Field`] — one column from `CREATE TABLE`: name, [`Type`], and a `NOT NULL` flag.
//! - [`TupleSchema`] — the ordered column list of a table or the output schema of an operator.
//! - [`Tuple`] — one row of [`Value`]s, in column-declaration order.
//! - [`TupleError`] — errors that `INSERT`/`UPDATE` validation and column lookups produce.
//!
//! # SQL → API mapping
//!
//! | SQL                                       | API                                                                    |
//! |-------------------------------------------|------------------------------------------------------------------------|
//! | `CREATE TABLE t (a INT NOT NULL)`         | `TupleSchema::new(vec![Field::new("a", Int32).not_null()])`            |
//! | `INSERT INTO t VALUES (1)`                | `Tuple::new(vec![Value::Int32(1)])` + `schema.validate(&tuple)`        |
//! | `SELECT b, a FROM t`                      | `tuple.project(&[1, 0])` / `schema.project(&[1, 0])`                   |
//! | `SELECT * FROM l, r`                      | `left_tuple.concat(&right_tuple)` + `left_schema.merge(&right_schema)` |
//! | `UPDATE t SET col = v`                    | `tuple.set_field(idx, value, &schema)`                                 |
//! | row → page bytes                          | `tuple.serialize(&schema, buf)`                                        |
//! | page bytes → row                          | `Tuple::deserialize(&schema, buf)`                                     |
//!
//! # NULL semantics
//!
//! A column with `nullable == false` rejects [`Value::Null`] at validation
//! time — the SQL `NOT NULL` constraint is enforced by [`TupleSchema::validate`]
//! and [`Tuple::set_field`], not by the executor. Operators downstream are free
//! to assume a row already passed validation; `NULL` semantics inside
//! expressions are handled separately in [`crate::execution::expression`].
//!
//! # On-disk format
//!
//! [`Tuple::serialize`] / [`Tuple::deserialize`] convert a row to and from the
//! exact bytes a heap page stores; the layout is documented inline on those
//! methods. The schema itself is **not** part of the row bytes — the catalog
//! holds the schema and pages just hold packed rows.

use std::{
    collections::HashMap,
    fmt,
    io::{Cursor, Read, Write},
};

use thiserror::Error;

use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::ColumnId,
    types::{Type, Value},
};

/// Errors produced by row validation and column lookups.
///
/// Each variant maps to a specific SQL-level cause:
///
/// - `FieldIndexOutOfBounds` — the executor or binder asked for a column past the schema's width.
///   Usually means a `SELECT col` referenced something the resolver did not actually bind.
/// - `NullNotAllowed` — `NULL` written to a `NOT NULL` column (`INSERT` or `UPDATE`).
/// - `TypeMismatch` — value type differs from the column's declared type.
/// - `FieldCountMismatch` — `INSERT … VALUES (…)` arity does not match the table's column list.
#[derive(Debug, Error)]
pub enum TupleError {
    #[error("field index {index} is out of bounds")]
    FieldIndexOutOfBounds { index: usize },

    #[error("null value not allowed for column '{column}'")]
    NullNotAllowed { column: String },

    #[error("type mismatch in column '{column}': expected {expected}, got {actual}")]
    TypeMismatch {
        column: String,
        expected: Type,
        actual: Type,
    },

    #[error("tuple has {actual} fields, expected {expected}")]
    FieldCountMismatch { expected: usize, actual: usize },
}

/// One column declared in `CREATE TABLE` — name, type, and the `NOT NULL` flag.
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
    pub name: String,
    pub field_type: Type,
    pub nullable: bool,
}

impl Field {
    /// Builds a nullable column — the `CREATE TABLE` default.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- name VARCHAR
    /// --   Field::new("name", Type::String)
    ///
    /// -- age INT
    /// --   Field::new("age", Type::Int32)
    ///
    /// -- score DOUBLE
    /// --   Field::new("score", Type::Float64)
    /// ```
    pub fn new(name: impl Into<String>, field_type: Type) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable: true,
        }
    }

    /// Tightens this column to `NOT NULL`, consuming and returning `self`.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- id INT NOT NULL
    /// --   Field::new("id", Type::Int32).not_null()
    ///
    /// -- email VARCHAR NOT NULL
    /// --   Field::new("email", Type::String).not_null()
    /// ```
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
    ///
    /// # Examples
    /// ```
    /// let mut field = Field::new("old_name", Type::Int32);
    /// field.set_name("new_name");
    /// assert_eq!(field.name, "new_name");
    /// ```
    pub fn set_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = name.into();
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

/// The ordered column list of a table or operator output — what `CREATE TABLE`
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
/// -- SELECT age, id FROM users
/// --   schema.project(&[2, 0])             -- output schema of the projection
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
    field_indices: HashMap<String, usize>,
}

impl TupleSchema {
    /// Builds a schema from a column list — typically the column list of a
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

    /// Returns the number of columns in the schema — the arity of a row that
    /// matches it. Equivalent to the count of items in `INSERT … VALUES (…)`.
    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if the schema has no columns — `CREATE TABLE t ()` in
    /// shape. Mostly useful as the identity for [`TupleSchema::merge`].
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
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

    /// Resolves a column name to its index and definition — what the binder
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
    /// --   schema.field_by_name("name")  → Some((Col(1), &Field { name: "name", .. }))
    ///
    /// -- SELECT missing FROM users;
    /// --   schema.field_by_name("missing") → None        -- binder reports an unresolved column
    /// ```
    pub fn field_by_name(&self, name: &str) -> Option<(ColumnId, &Field)> {
        self.field_indices.get(name).and_then(|&i| {
            let col_id = ColumnId::try_from(i).ok()?;
            Some((col_id, &self.fields[i]))
        })
    }

    /// Iterates columns in declaration order — the order `SELECT *` exposes
    /// them and the order rows are laid out in.
    pub fn fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    /// Sum of [`Type::size`] for every column — the raw payload size of a
    /// fully non-null row, before the null bitmap and per-value discriminants
    /// that [`Tuple::serialize`] also writes.
    ///
    /// Use [`TupleSchema::serialized_size`] when sizing a heap-page slot;
    /// this method exists mainly to compose that calculation.
    pub fn tuple_size(&self) -> usize {
        self.fields.iter().map(|f| f.field_type.size()).sum()
    }

    /// The exact number of bytes [`Tuple::serialize`] writes for this schema
    /// in the worst case (all columns non-null) — i.e. how big a heap-page
    /// slot must be to hold one row produced by `INSERT`.
    ///
    /// Layout: null bitmap (`ceil(n/8)` bytes) + one 1-byte discriminant per
    /// column + raw payload bytes for every column type. Use this — not
    /// [`TupleSchema::tuple_size`] — when sizing an on-disk buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use storemy::{
    ///     tuple::{Field, TupleSchema},
    ///     types::Type,
    /// };
    ///
    /// // CREATE TABLE t (id INT, ok BOOLEAN);
    /// let schema = TupleSchema::new(vec![
    ///     Field::new("id", Type::Int32), // 1 disc + 4 bytes
    ///     Field::new("ok", Type::Bool),  // 1 disc + 1 byte
    /// ]);
    /// // 2 columns → 1-byte bitmap + 2 discriminants + 5 payload = 8 total
    /// assert_eq!(schema.serialized_size(), 8);
    /// ```
    pub fn serialized_size(&self) -> usize {
        self.num_fields().div_ceil(8) + self.num_fields() + self.tuple_size()
    }

    /// Concatenates two schemas — the schema-level counterpart to
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

    /// Builds the output schema of a `SELECT cols FROM …` projection.
    ///
    /// Output columns appear in the order specified by `indices`, so
    /// projecting reorders columns as well as drops them.
    ///
    /// # SQL examples
    ///
    /// Schema: `users(id, name, age)` resolved to indices `0, 1, 2`.
    ///
    /// ```sql
    /// -- SELECT age, id FROM users
    /// --   schema.project(&[2, 0])
    /// --     → TupleSchema(age INT, id INT NOT NULL)
    ///
    /// -- SELECT name FROM users
    /// --   schema.project(&[1])
    /// --     → TupleSchema(name VARCHAR)
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TupleError::FieldIndexOutOfBounds`] if any index points past
    /// the end of this schema — typically a sign the binder produced an
    /// index the schema does not actually carry.
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

    /// Checks that a row honours every column constraint — the gate `INSERT`
    /// runs before a tuple is written to a heap page.
    ///
    /// Verifies that:
    /// - the number of values equals the number of columns,
    /// - no `NOT NULL` column holds a `NULL` value, and
    /// - each non-null value's runtime type matches the column's declared type.
    ///
    /// # SQL examples
    ///
    /// Schema: `users(id INT NOT NULL, name VARCHAR, age INT)`.
    ///
    /// ```sql
    /// -- INSERT INTO users VALUES (1, 'alice', 30);   -- → Ok(())
    /// -- INSERT INTO users VALUES (1, NULL, NULL);    -- → Ok(())  (name, age are nullable)
    /// -- INSERT INTO users VALUES (NULL, 'a', 1);     -- → NullNotAllowed { column: "id" }
    /// -- INSERT INTO users VALUES (TRUE, 'a', 1);     -- → TypeMismatch  { column: "id",
    /// --                                              --                  expected: Int32,
    /// --                                              --                  actual:   Bool }
    /// -- INSERT INTO users VALUES (1);                -- → FieldCountMismatch { expected: 3, actual: 1 }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the first [`TupleError`] encountered, in the order checks run:
    /// - [`TupleError::FieldCountMismatch`] — `VALUES` arity does not match the column list
    /// - [`TupleError::NullNotAllowed`] — `NULL` written to a `NOT NULL` column
    /// - [`TupleError::TypeMismatch`] — value type differs from the declared column type
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

    /// Borrows the name → index map the binder uses to resolve qualified
    /// column references (e.g. `users.id`) to positional column indices.
    pub fn field_indices(&self) -> &HashMap<String, usize> {
        &self.field_indices
    }

    /// Like [`TupleSchema::field`], but returns
    /// [`TupleError::FieldIndexOutOfBounds`] instead of `None`.
    ///
    /// The `?`-friendly counterpart for code paths that already return
    /// `Result<_, TupleError>` — an unresolved column reference becomes an
    /// early return instead of an `expect`/`unwrap`.
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] when `index >= self.num_fields()` —
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

    /// Returns `true` if a column named `name` exists — what the binder uses
    /// to reject `SELECT missing FROM users` early, before any executor runs.
    ///
    /// Reads better than `schema.field_by_name(name).is_some()` when the
    /// caller only needs a yes/no answer.
    pub fn contains(&self, name: &str) -> bool {
        self.field_indices.contains_key(name)
    }

    /// Like [`TupleSchema::project`], but returns a bare `Vec<Field>` instead
    /// of wrapping it in a new schema. Useful for operators that build their
    /// output schema by concatenating a projection of the child with extra
    /// columns of their own.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT user_id, COUNT(*) AS n
    /// -- FROM events
    /// -- GROUP BY user_id;
    /// --
    /// --   The aggregate's output schema is
    /// --       child.project_fields(&[user_id_idx])      -- group-by columns
    /// --     ++ aggregate output columns (n BIGINT)
    /// ```
    ///
    /// # Errors
    ///
    /// [`TupleError::FieldIndexOutOfBounds`] for the first index in `indices`
    /// that is past the end of the schema — usually a sign the binder
    /// produced a `GROUP BY` index that does not match the child's width.
    pub fn project_fields(&self, indices: &[usize]) -> Result<Vec<Field>, TupleError> {
        indices
            .iter()
            .map(|&i| {
                self.fields
                    .get(i)
                    .cloned()
                    .ok_or(TupleError::FieldIndexOutOfBounds { index: i })
            })
            .collect()
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

/// Writes the on-disk row bytes a heap page stores after `INSERT`.
///
/// Layout: a null bitmap, then each non-null value encoded via
/// [`Encode for Value`] (discriminant + payload). The schema itself is **not**
/// written — only the row bytes; the catalog is the single source of truth
/// for column names and types.
///
/// This is the serialization the [`crate::codec::Encode`] trait dispatches to;
/// [`Tuple::serialize`] is a thin wrapper over the same path that writes into
/// a byte slice.
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

/// A single row — the unit every executor consumes and emits.
///
/// One tuple is one `INSERT … VALUES (…)` row when written, or one row of a
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
/// Use [`TupleSchema::validate`] before persisting a tuple — `Tuple::new`
/// itself does no checking.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Tuple {
    values: Vec<Value>,
}

impl Tuple {
    /// Builds a tuple from the values produced by `INSERT … VALUES (…)` or
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
    /// Stores the values as-is — no validation. Pair with
    /// [`TupleSchema::validate`] when the row is about to be persisted.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Reads the value at column `index`, or `None` if out of bounds — the
    /// raw read used to evaluate `expression::Operand::Column(index)` against
    /// a row.
    ///
    /// # SQL examples
    ///
    /// Given the row `(42, 'alice', 30)` for `users(id, name, age)`:
    ///
    /// ```sql
    /// -- WHERE age = 30  →  tuple.get(2)        → Some(&Value::Int32(30))
    /// --                    tuple.get(99)       → None
    /// ```
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns the number of values in this row — the arity. Should equal
    /// the matching schema's [`TupleSchema::num_fields`] once validation
    /// passes.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if this row has no values — typically the empty group
    /// key used when a query has no `GROUP BY`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Reads the value referenced by `col`, returning
    /// [`TupleError::FieldIndexOutOfBounds`] rather than `None`.
    ///
    /// # SQL examples
    ///
    /// Row `(42, 'alice', 30)` for `users(id, name, age)`:
    ///
    /// ```sql
    /// -- WHERE age = 30
    /// --   tuple.value_at(col(2)) → Ok(&Value::Int32(30))
    ///
    /// -- column reference past the end of the row
    /// --   tuple.value_at(col(99)) → Err(FieldIndexOutOfBounds { index: 99 })
    /// ```
    ///
    /// Prefer this over `tuple.get(usize::from(col)).ok_or(...)` at every
    /// call site — it produces the same error variant the rest of the schema
    /// API already uses.
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
    ///
    /// Use this where the SQL semantics treat a missing column reference as
    /// `NULL` — primarily aggregate group keys and accumulator inputs after
    /// projection. For `WHERE`/`JOIN` operands, prefer the strict
    /// [`Tuple::value_at`].
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT user_id, COUNT(*) FROM events GROUP BY user_id;
    /// --   COUNT(*) ignores its column reference; an out-of-bounds col_id
    /// --   safely resolves to NULL instead of erroring:
    /// --     tuple.value_at_or_null(col(0)) → &Value::Null  (when 0 is past the row)
    /// ```
    pub fn value_at_or_null(&self, col: ColumnId) -> &Value {
        self.values.get(usize::from(col)).unwrap_or(&Value::Null)
    }

    /// Pairs each [`Field`] in `schema` with the corresponding [`Value`] in
    /// this tuple — handy for printing a `SELECT` row with column names, or
    /// for type-check passes that walk the row alongside the schema.
    ///
    /// Stops at the shorter of the two — it does **not** check that arities
    /// match. Callers that need that guarantee should call
    /// [`TupleSchema::validate`] first.
    pub fn iter_with_schema<'a>(
        &'a self,
        schema: &'a TupleSchema,
    ) -> impl Iterator<Item = (&'a Field, &'a Value)> + 'a {
        schema.fields().zip(self.values.iter())
    }

    /// Mutable counterpart to [`Tuple::get`] — used by `UPDATE` callers that
    /// want to overwrite a column in place without re-validating. Prefer
    /// [`Tuple::set_field`] when you need the column's `NOT NULL` and type
    /// constraints enforced.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Builds a new row containing only the values at `indices` — the
    /// row-level counterpart to [`TupleSchema::project`] for a `SELECT cols`
    /// projection.
    ///
    /// Output values appear in the order specified by `indices`, so
    /// projecting reorders as well as drops columns. Out-of-bounds indices
    /// are silently skipped — this method does no validation; the caller is
    /// expected to have produced `indices` from a bound schema.
    ///
    /// # SQL examples
    ///
    /// Given the row `(42, 'alice', 30)` for `users(id, name, age)`:
    ///
    /// ```sql
    /// -- SELECT age, id FROM users
    /// --   tuple.project(&[2, 0]) → Tuple(30, 42)
    ///
    /// -- SELECT name FROM users
    /// --   tuple.project(&[1])    → Tuple('alice')
    /// ```
    #[must_use]
    pub fn project(&self, indices: &[usize]) -> Self {
        let values = indices
            .iter()
            .filter_map(|&i| self.values.get(i).cloned())
            .collect();
        Self { values }
    }

    /// Iterates references to the row's values in column-declaration order —
    /// the order `SELECT *` returns them.
    ///
    /// This is the borrowing counterpart to `IntoIterator for Tuple` and is
    /// what the compiler calls when you write `for v in &tuple`.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Concatenates two rows — used to materialize the output of a JOIN.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT * FROM users u JOIN orders o ON u.id = o.user_id;
    /// --
    /// -- With u_row = (42, 'alice', 30) and o_row = (1001, 42, 9.99):
    /// --   u_row.concat(&o_row) → Tuple(42, 'alice', 30, 1001, 42, 9.99)
    /// ```
    ///
    /// Pair with [`TupleSchema::merge`] for the matching output schema.
    #[must_use]
    pub fn concat(&self, other: &Tuple) -> Self {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Self { values }
    }

    /// Writes the row's bytes into `buf` — the format a heap page stores
    /// after `INSERT`.
    ///
    /// The on-disk format is:
    /// 1. A null bitmap — one bit per column, packed into `ceil(n / 8)` bytes. Bit `i` is set when
    ///    column `i` is `NULL`.
    /// 2. Each non-null value encoded via [`Encode for Value`]: a 1-byte discriminant followed by
    ///    the little-endian payload.
    ///
    /// The schema is consulted only for the column count; column names and
    /// types live in the catalog, not in the row bytes.
    ///
    /// Returns the total number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if `buf` is too small to hold the output —
    /// size the buffer with [`TupleSchema::serialized_size`].
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

    /// Reconstructs a row from heap-page bytes — the inverse of
    /// [`Tuple::serialize`] used by `SELECT` when reading rows back.
    ///
    /// Reads the null bitmap first, then decodes each non-null value via
    /// [`Decode for Value`] (reads discriminant + payload). The schema is only
    /// needed for the column count; the runtime type of each value comes from
    /// the encoded discriminant.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if `buf` is too short, or
    /// [`CodecError::UnknownDiscriminant`] / [`CodecError::InvalidUtf8`] on
    /// corrupt data — both indicate a page that was not written by this
    /// build (or a buggy page write).
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

    /// Overwrites the column at `index` with `value`, enforcing the same
    /// constraints as [`TupleSchema::validate`]. The row-level write that
    /// implements `UPDATE … SET col = value`.
    ///
    /// # SQL examples
    ///
    /// Schema `users(id INT NOT NULL, name VARCHAR, age INT)`, row `(42, 'alice', 30)`:
    ///
    /// ```sql
    /// -- UPDATE users SET name = 'bob' WHERE id = 42;
    /// --   tuple.set_field(1, Value::String("bob".into()), &schema)        → Ok(())
    ///
    /// -- UPDATE users SET age = NULL WHERE id = 42;
    /// --   tuple.set_field(2, Value::Null, &schema)                        → Ok(())
    ///
    /// -- UPDATE users SET id = NULL;                  -- id is NOT NULL
    /// --   tuple.set_field(0, Value::Null, &schema)                        → NullNotAllowed { column: "id" }
    ///
    /// -- UPDATE users SET id = 1::BIGINT;             -- type-mismatched literal
    /// --   tuple.set_field(0, Value::Int64(1), &schema)                    → TypeMismatch { column: "id", … }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`TupleError::FieldIndexOutOfBounds`] — `index` is not a valid column for `schema`, or is
    ///   past the end of this tuple's values.
    /// - [`TupleError::NullNotAllowed`] — assigning `NULL` to a `NOT NULL` column.
    /// - [`TupleError::TypeMismatch`] — value type differs from the declared column type.
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
    fn schema_contains() {
        let schema = schema_id_name_age();
        assert!(schema.contains("name"));
        assert!(!schema.contains("missing"));
    }

    #[test]
    fn schema_project_fields_keeps_order() {
        let schema = schema_id_name_age();
        let fields = schema.project_fields(&[2, 0]).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "age");
        assert_eq!(fields[1].name, "id");
    }

    #[test]
    fn schema_project_fields_out_of_range() {
        let schema = schema_id_name_age();
        let err = schema.project_fields(&[0, 99]).unwrap_err();
        assert!(matches!(err, TupleError::FieldIndexOutOfBounds {
            index: 99
        }));
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
