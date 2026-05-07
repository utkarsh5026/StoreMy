//! Single-input SQL operators: `WHERE`, `SELECT`, `ORDER BY`, and `LIMIT`.
//!
//! Each operator in this module wraps one child [`PlanNode`] and transforms the
//! stream of tuples it produces without combining it with another input. These
//! are the physical operators the binder/planner reaches for after it has
//! resolved SQL names like `users.age` into tuple positions like `2`.
//!
//! # Shape
//!
//! - [`Filter`] — applies a boolean expression for `WHERE <predicate>`.
//! - [`ProjectItem`] — describes one item in the `SELECT` list: either a resolved column or a
//!   literal constant.
//! - [`Project`] — builds the output row for `SELECT col, literal, ...`.
//! - [`SortKey`] — one resolved `ORDER BY` item and direction.
//! - [`Sort`] — performs a blocking in-memory `ORDER BY`.
//! - [`Limit`] — applies `LIMIT n OFFSET m` as a streaming row window.
//!
//! # How it works
//!
//! `Filter`, `Project`, and `Limit` are streaming operators: each call to
//! [`FallibleIterator::next`] pulls only as much as it needs from the child.
//! [`Sort`] is blocking: the first `next` call drains the whole child, sorts the
//! materialized rows, then emits the sorted buffer one row at a time.
//!
//! # NULL semantics
//!
//! `Filter` delegates to [`BooleanExpression`]: a `NULL` on either side of a
//! comparison makes that predicate `false`, rather than modeling full SQL
//! three-valued logic. `Project` copies `NULL` values unchanged and can emit
//! literal `NULL`s. `Sort` orders `NULL` before non-`NULL` in ascending order
//! and after non-`NULL` in descending order because the per-key comparison is
//! reversed. `Limit` is row-count based and does not inspect values.

use std::cmp::Ordering;

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor, expression::BooleanExpression};
use crate::{
    execution::PlanNode,
    primitives::{self, NonEmptyString},
    tuple::{Field, Tuple, TupleSchema},
    types::{Type, Value},
};

/// Applies a SQL `WHERE` predicate to rows from one child plan.
///
/// Each input tuple is tested with a [`BooleanExpression`]. Rows that evaluate
/// to `true` pass through unchanged; rows that evaluate to `false` are skipped.
/// The output tuple layout is exactly the child layout because filtering never
/// adds, removes, or reorders columns.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- SELECT * FROM users WHERE age >= 18;
/// ```
///
/// ```ignore
/// Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(2, Predicate::GreaterThanOrEqual, Value::Int64(18)),
/// )
/// ```
///
/// ```sql
/// -- SELECT * FROM users WHERE name LIKE 'a%';
/// ```
///
/// ```ignore
/// Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(1, Predicate::Like, Value::String("a%".into())),
/// )
/// ```
///
/// ```sql
/// -- SELECT * FROM users WHERE age >= 18 AND name <> 'guest';
/// ```
///
/// ```ignore
/// Filter::new(
///     users_scan,
///     BooleanExpression::And(
///         Box::new(BooleanExpression::col_op_lit(
///             2,
///             Predicate::GreaterThanOrEqual,
///             Value::Int64(18),
///         )),
///         Box::new(BooleanExpression::col_op_lit(
///             1,
///             Predicate::NotEqual,
///             Value::String("guest".into()),
///         )),
///     ),
/// )
/// ```
///
/// # SQL → operator mapping
///
/// ```sql
/// -- SELECT id, name FROM users WHERE age < 30;
/// ```
///
/// ```ignore
/// let filtered = Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(2, Predicate::LessThan, Value::Int64(30)),
/// );
/// let projected = Project::new(Box::new(PlanNode::Filter(filtered)), &[col(0), col(1)])?;
/// ```
///
/// `NULL` in a comparison filters the row out: `WHERE age = NULL` evaluates to
/// `false` for every row in this executor.
#[derive(Debug)]
pub struct Filter<'a> {
    child: Box<PlanNode<'a>>,
    predicate: BooleanExpression,
}

impl<'a> Filter<'a> {
    /// Builds a `WHERE` operator over an already-planned child.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- WHERE age = 30
    /// --   Filter::new(child, BooleanExpression::col_op_lit(2, Predicate::Equals, Value::Int64(30)))
    ///
    /// -- WHERE name LIKE 'a%'
    /// --   Filter::new(child, BooleanExpression::col_op_lit(1, Predicate::Like, Value::String("a%".into())))
    ///
    /// -- WHERE age > id
    /// --   Filter::new(child, BooleanExpression::col_op_col(2, Predicate::GreaterThan, 0))
    /// ```
    ///
    /// # Errors
    ///
    /// Does not validate eagerly. Column resolution errors surface later as
    /// [`ExecutionError::TypeError`] from [`FallibleIterator::next`] when the
    /// predicate references a tuple position outside the child's output.
    pub fn new(child: Box<PlanNode<'a>>, predicate: BooleanExpression) -> Self {
        Self { child, predicate }
    }
}

impl FallibleIterator for Filter<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Returns the next row that qualifies for the SQL `WHERE` predicate.
    ///
    /// Pulls child rows until the predicate evaluates to `true` or the child is
    /// exhausted. `And` / `Or` expressions use Rust's short-circuiting `&&` and
    /// `||`; `NULL` comparisons and non-string `LIKE` checks evaluate to
    /// `false`.
    ///
    /// # Errors
    ///
    /// Propagates child errors and returns [`ExecutionError::TypeError`] when
    /// the predicate references an out-of-bounds column.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(tuple) = self.child.next()? {
            if self.predicate.eval(&tuple)? {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for Filter<'_> {
    /// Exposes the unchanged child schema for `SELECT ... WHERE ...`.
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    /// Rewinds the child so the same `WHERE` predicate can be evaluated again.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's rewind implementation.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

/// One resolved SQL `SELECT` item produced by [`Project`].
///
/// A projection item either copies a column from the child tuple by resolved
/// index, or emits a constant value for every row. It is the bound form of
/// SQL list items like `name`, `age AS years`, or `'guest' AS role`.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- SELECT id FROM users;
/// ```
///
/// ```ignore
/// ProjectItem::Column { idx: 0, alias: None }
/// ```
///
/// ```sql
/// -- SELECT name AS username FROM users;
/// ```
///
/// ```ignore
/// ProjectItem::Column {
///     idx: 1,
///     alias: Some("username".into()),
/// }
/// ```
///
/// ```sql
/// -- SELECT 'active' AS status FROM users;
/// ```
///
/// ```ignore
/// ProjectItem::Literal {
///     value: Value::String("active".into()),
///     name: "status".into(),
/// }
/// ```
#[derive(Debug, Clone)]
pub enum ProjectItem {
    /// A resolved `SELECT col` or `SELECT col AS alias` item.
    Column {
        idx: usize,
        alias: Option<NonEmptyString>,
    },
    /// A resolved `SELECT <literal> AS name` item repeated for every input row.
    Literal { value: Value, name: NonEmptyString },
}

impl ProjectItem {
    /// Builds a `SELECT col` item that keeps the child's field name.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT id FROM users
    /// --   ProjectItem::column(col(0))
    ///
    /// -- SELECT name FROM users
    /// --   ProjectItem::column(col(1))
    /// ```
    ///
    /// # Errors
    ///
    /// Does not validate eagerly. An out-of-bounds column becomes
    /// [`ExecutionError::TypeError`] when [`Project::with_items`] builds the
    /// output schema.
    pub fn column(col_id: primitives::ColumnId, alias: Option<NonEmptyString>) -> Self {
        Self::Column {
            idx: usize::from(col_id),
            alias,
        }
    }

    /// Builds a `SELECT <literal> AS name` item repeated on each output row.
    ///
    /// `NULL` literals default to [`Type::String`] in the output schema because
    /// SQL `NULL` carries no inherent type in this bound form.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT 1 AS one FROM users
    /// --   ProjectItem::literal(Value::Int64(1), "one")
    ///
    /// -- SELECT 'guest' AS role FROM users
    /// --   ProjectItem::literal(Value::String("guest".into()), "role")
    ///
    /// -- SELECT NULL AS missing FROM users
    /// --   ProjectItem::literal(Value::Null, "missing")
    /// ```
    ///
    /// # Errors
    ///
    /// Does not return an error; schema type inference happens later in
    /// [`Project::with_items`] and accepts all literal values.
    pub fn literal(value: Value, name: NonEmptyString) -> Self {
        Self::Literal { value, name }
    }
}

/// Builds the SQL `SELECT` list for each row from one child plan.
///
/// A `Project` maps each input tuple to a new tuple by copying selected child
/// columns and/or appending literal values. The output tuple layout is exactly
/// the `items` order: each [`ProjectItem`] contributes one output column.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- SELECT id, name FROM users;
/// ```
///
/// ```ignore
/// Project::new(users_scan, &[col(0), col(1)])?
/// ```
///
/// ```sql
/// -- SELECT name AS username, age FROM users;
/// ```
///
/// ```ignore
/// Project::with_items(
///     users_scan,
///     vec![
///         ProjectItem::aliased_column(col(1), "username"),
///         ProjectItem::column(col(2)),
///     ],
/// )?
/// ```
///
/// ```sql
/// -- SELECT id, 'active' AS status FROM users;
/// ```
///
/// ```ignore
/// Project::with_items(
///     users_scan,
///     vec![
///         ProjectItem::column(col(0)),
///         ProjectItem::literal(Value::String("active".into()), "status"),
///     ],
/// )?
/// ```
///
/// # SQL → operator mapping
///
/// ```sql
/// -- SELECT name AS username, 1 AS one
/// -- FROM users
/// -- WHERE age >= 18;
/// ```
///
/// ```ignore
/// let filtered = Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(2, Predicate::GreaterThanOrEqual, Value::Int64(18)),
/// );
/// let projected = Project::with_items(
///     Box::new(PlanNode::Filter(filtered)),
///     vec![
///         ProjectItem::aliased_column(col(1), "username"),
///         ProjectItem::literal(Value::Int64(1), "one"),
///     ],
/// )?;
/// ```
///
/// `NULL` values copied from input columns remain `NULL`; literal `NULL`s are
/// emitted unchanged.
#[derive(Debug)]
pub struct Project<'a> {
    child: Box<PlanNode<'a>>,
    items: Vec<ProjectItem>,
    output_schema: TupleSchema,
}

impl<'a> Project<'a> {
    /// Builds a `SELECT col, ...` projection from resolved column indices.
    ///
    /// Every output column is copied straight from the child tuple and keeps the
    /// child's field name.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT id FROM users
    /// --   Project::new(child, &[col(0)])?
    ///
    /// -- SELECT name, age FROM users
    /// --   Project::new(child, &[col(1), col(2)])?
    ///
    /// -- SELECT age, id FROM users
    /// --   Project::new(child, &[col(2), col(0)])?
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any `ColumnId` refers to an
    /// index that does not exist in the child's schema.
    pub fn new(
        child: Box<PlanNode<'a>>,
        col_ids: &[primitives::ColumnId],
    ) -> Result<Self, ExecutionError> {
        let items: Vec<ProjectItem> = col_ids
            .iter()
            .copied()
            .map(|c| ProjectItem::column(c, None))
            .collect();
        Self::with_items(child, items)
    }

    /// Builds a `SELECT` projection with columns, aliases, and literals.
    ///
    /// Use this when the SQL projection contains more than bare column picks,
    /// such as `AS` aliases or constant expressions.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SELECT id AS user_id FROM users
    /// --   Project::with_items(child, vec![ProjectItem::aliased_column(col(0), "user_id")])?
    ///
    /// -- SELECT 1 AS one, name FROM users
    /// --   Project::with_items(child, vec![
    /// --       ProjectItem::literal(Value::Int64(1), "one"),
    /// --       ProjectItem::column(col(1)),
    /// --   ])?
    ///
    /// -- SELECT NULL AS missing FROM users
    /// --   Project::with_items(child, vec![ProjectItem::literal(Value::Null, "missing")])?
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] if any [`ProjectItem::Column`]
    /// refers to an out-of-range index in the child's schema.
    pub fn with_items(
        child: Box<PlanNode<'a>>,
        items: Vec<ProjectItem>,
    ) -> Result<Self, ExecutionError> {
        let child_schema = child.schema();
        let fields = items
            .iter()
            .map(|item| Self::create_field_from_projection(item, child_schema))
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(Self {
            child,
            items,
            output_schema: TupleSchema::new(fields),
        })
    }

    /// Builds one output [`Field`] for a resolved SQL `SELECT` item.
    ///
    /// [`Project::with_items`] uses this helper to turn each [`ProjectItem`]
    /// into the corresponding output schema field before any rows are pulled.
    ///
    /// - [`ProjectItem::Column`] — clones the child's field at `idx`, then applies an optional `AS`
    ///   rename via [`Field::set_name`].
    /// - [`ProjectItem::Literal`] — uses the literal's runtime type (defaulting to [`Type::String`]
    ///   for untyped `NULL`); non-null literals use [`Field::not_null`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] when a [`ProjectItem::Column`] index is
    /// out of range for `schema`.
    fn create_field_from_projection(
        item: &ProjectItem,
        schema: &TupleSchema,
    ) -> Result<Field, ExecutionError> {
        match item {
            ProjectItem::Column { idx, alias } => {
                let mut field = schema
                    .field_or_err(*idx)
                    .map_err(|e| ExecutionError::TypeError(e.to_string()))?
                    .clone();
                if let Some(name) = alias {
                    field
                        .set_name(name.as_str())
                        .map_err(|e| ExecutionError::TypeError(e.to_string()))?;
                }
                Ok(field)
            }
            ProjectItem::Literal { value, name } => {
                let ty = value.get_type().unwrap_or(Type::String);
                let mut field = Field::new_non_empty(name.clone(), ty);
                if !value.is_null() {
                    field = field.not_null();
                }
                Ok(field)
            }
        }
    }
}

impl FallibleIterator for Project<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Returns the next row shaped like the SQL `SELECT` list.
    ///
    /// Copies projected column values by index and clones literal values into
    /// the output tuple. `NULL` values are copied or emitted unchanged; there is
    /// no expression evaluation or type coercion at this layer.
    ///
    /// # Errors
    ///
    /// Propagates child errors and returns [`ExecutionError::TypeError`] if a
    /// projected column is missing from an input tuple.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        let Some(input) = self.child.next()? else {
            return Ok(None);
        };
        let out = self
            .items
            .iter()
            .map(|item| match item {
                ProjectItem::Column { idx, .. } => match input.get(*idx) {
                    Some(v) => Ok(v.clone()),
                    None => Err(ExecutionError::TypeError(format!(
                        "Project: column index {idx} missing from input tuple"
                    ))),
                },
                ProjectItem::Literal { value, .. } => Ok(value.clone()),
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;
        Ok(Some(Tuple::new(out)))
    }
}

impl Executor for Project<'_> {
    /// Exposes the schema produced by the SQL `SELECT` list.
    fn schema(&self) -> &TupleSchema {
        &self.output_schema
    }

    /// Rewinds the child so the same projection can be emitted again.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's rewind implementation.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

/// One resolved SQL `ORDER BY` item: column position plus direction.
///
/// Multiple `SortKey`s are evaluated lexicographically — the first key
/// decides the ordering, and later keys are only consulted to break ties.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- ORDER BY age
/// ```
///
/// ```ignore
/// SortKey::asc(col(2))
/// ```
///
/// ```sql
/// -- ORDER BY name DESC
/// ```
///
/// ```ignore
/// SortKey::desc(col(1))
/// ```
///
/// ```sql
/// -- ORDER BY age ASC, name DESC
/// ```
///
/// ```ignore
/// vec![SortKey::asc(col(2)), SortKey::desc(col(1))]
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SortKey {
    /// Resolved `ORDER BY` column position in the child tuple.
    pub col_id: primitives::ColumnId,
    /// `true` for `ASC`, `false` for `DESC`.
    pub ascending: bool,
}

impl SortKey {
    pub fn asc(col_id: primitives::ColumnId) -> Self {
        Self {
            col_id,
            ascending: true,
        }
    }

    pub fn desc(col_id: primitives::ColumnId) -> Self {
        Self {
            col_id,
            ascending: false,
        }
    }
}

/// Applies a SQL `ORDER BY` clause to all rows from one child plan.
///
/// `Sort` is a blocking operator: on the first call to `next`, it drains the
/// child completely into memory, sorts the collected tuples by the declared
/// keys, then yields those tuples one by one. The output tuple layout is exactly
/// the child layout because sorting only changes row order.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- SELECT * FROM users ORDER BY age;
/// ```
///
/// ```ignore
/// Sort::new(vec![SortKey::asc(col(2))], users_scan)
/// ```
///
/// ```sql
/// -- SELECT * FROM users ORDER BY name DESC;
/// ```
///
/// ```ignore
/// Sort::new(vec![SortKey::desc(col(1))], users_scan)
/// ```
///
/// ```sql
/// -- SELECT * FROM users ORDER BY age ASC, name DESC;
/// ```
///
/// ```ignore
/// Sort::new(
///     vec![SortKey::asc(col(2)), SortKey::desc(col(1))],
///     users_scan,
/// )
/// ```
///
/// # SQL → operator mapping
///
/// ```sql
/// -- SELECT id, name
/// -- FROM users
/// -- WHERE age >= 18
/// -- ORDER BY name DESC;
/// ```
///
/// ```ignore
/// let filtered = Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(2, Predicate::GreaterThanOrEqual, Value::Int64(18)),
/// );
/// let sorted = Sort::new(
///     vec![SortKey::desc(col(1))],
///     Box::new(PlanNode::Filter(filtered)),
/// );
/// let projected = Project::new(Box::new(PlanNode::Sort(sorted)), &[col(0), col(1)])?;
/// ```
///
/// `NULL` sorts before non-`NULL` for ascending keys. Descending keys reverse
/// that per-key ordering.
#[derive(Debug)]
pub struct Sort<'a> {
    keys: Vec<SortKey>,
    child: Box<PlanNode<'a>>,
    sorted: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl<'a> Sort<'a> {
    /// Builds an `ORDER BY` operator over an already-planned child.
    ///
    /// `keys[0]` is the primary `ORDER BY` expression; later keys break ties.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- ORDER BY id
    /// --   Sort::new(vec![SortKey::asc(col(0))], child)
    ///
    /// -- ORDER BY age DESC
    /// --   Sort::new(vec![SortKey::desc(col(2))], child)
    ///
    /// -- ORDER BY age ASC, name DESC
    /// --   Sort::new(vec![SortKey::asc(col(2)), SortKey::desc(col(1))], child)
    /// ```
    ///
    /// # Errors
    ///
    /// Does not validate eagerly. Errors from the child surface from
    /// [`FallibleIterator::next`] during materialization.
    pub fn new(keys: Vec<SortKey>, child: Box<PlanNode<'a>>) -> Self {
        Self {
            keys,
            child,
            sorted: Vec::new(),
            cursor: 0,
            materialized: false,
        }
    }

    /// Drains the child rows for a SQL `ORDER BY`, then sorts them once.
    ///
    /// Subsequent calls return immediately because `self.materialized` is `true`.
    ///
    /// `NULL` values sort before all non-null values for each key. Each key's
    /// `ascending` flag only flips the contribution of that key — it does not
    /// reverse the whole comparator. Incomparable non-null values (which should
    /// not arise for well-typed data) are treated as equal for that key.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's `next`.
    fn materialize_tuples(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        while let Some(tuple) = self.child.next()? {
            self.sorted.push(tuple);
        }

        let keys = self.keys.clone();
        self.sorted.sort_by(|a, b| {
            for key in &keys {
                let ord = Self::compare_by_sort_key(*key, a, b);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        self.materialized = true;
        Ok(())
    }

    /// Compares two rows for one resolved SQL `ORDER BY` item.
    ///
    /// Returns the ordering contribution for this key only; multi-key sorting
    /// calls this repeatedly until one key breaks the tie. Missing values are
    /// treated like `NULL`, `NULL` sorts before non-`NULL` in ascending order,
    /// and incomparable non-`NULL` values compare equal.
    fn compare_by_sort_key(key: SortKey, a: &Tuple, b: &Tuple) -> Ordering {
        let idx = usize::from(key.col_id);
        let ord = match (a.get(idx), b.get(idx)) {
            (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(Ordering::Equal),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if key.ascending { ord } else { ord.reverse() }
    }
}

impl FallibleIterator for Sort<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Returns the next row in SQL `ORDER BY` order.
    ///
    /// The first call drains and sorts the complete child input. Later calls
    /// walk the materialized sorted buffer. `NULL` ordering follows
    /// `Sort::compare_by_sort_key`; type mismatches that cannot be compared
    /// are treated as ties for that key.
    ///
    /// # Errors
    ///
    /// Propagates errors returned while draining the child.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_tuples()?;
        if self.cursor >= self.sorted.len() {
            return Ok(None);
        }
        let tuple = self.sorted[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(tuple))
    }
}

impl Executor for Sort<'_> {
    /// Exposes the unchanged child schema for `SELECT ... ORDER BY ...`.
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    /// Rewinds the sorted output buffer to the first row.
    ///
    /// After materialization, rewind replays the same sorted rows without
    /// rewinding the child. Before materialization, it simply leaves the cursor
    /// at the beginning.
    ///
    /// # Errors
    ///
    /// Does not return an error.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.cursor = 0;
        Ok(())
    }
}

/// Applies a SQL `LIMIT n OFFSET m` row window to one child plan.
///
/// The first `offset` rows from the child are consumed and discarded on the
/// initial `next` call. After that, at most `limit` rows are returned before the
/// operator signals end-of-stream. The output tuple layout is exactly the child
/// layout because limiting only removes rows.
///
/// # SQL examples
///
/// Assume `users(id, name, age)` with binder-resolved indices
/// `id → 0`, `name → 1`, `age → 2`:
///
/// ```sql
/// -- SELECT * FROM users LIMIT 10;
/// ```
///
/// ```ignore
/// Limit::new(users_scan, 10, 0)
/// ```
///
/// ```sql
/// -- SELECT * FROM users LIMIT 10 OFFSET 20;
/// ```
///
/// ```ignore
/// Limit::new(users_scan, 10, 20)
/// ```
///
/// ```sql
/// -- SELECT id, name FROM users ORDER BY age DESC LIMIT 5;
/// ```
///
/// ```ignore
/// let sorted = Sort::new(vec![SortKey::desc(col(2))], users_scan);
/// let limited = Limit::new(Box::new(PlanNode::Sort(sorted)), 5, 0);
/// let projected = Project::new(Box::new(PlanNode::Limit(limited)), &[col(0), col(1)])?;
/// ```
///
/// # SQL → operator mapping
///
/// ```sql
/// -- SELECT *
/// -- FROM users
/// -- WHERE age >= 18
/// -- LIMIT 2 OFFSET 1;
/// ```
///
/// ```ignore
/// let filtered = Filter::new(
///     users_scan,
///     BooleanExpression::col_op_lit(2, Predicate::GreaterThanOrEqual, Value::Int64(18)),
/// );
/// let limited = Limit::new(Box::new(PlanNode::Filter(filtered)), 2, 1);
/// ```
///
/// `LIMIT` and `OFFSET` count rows, so `NULL` values do not receive special
/// handling.
#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct Limit<'a> {
    limit: u64,
    offset: u64,
    count: u64,
    child: Box<PlanNode<'a>>,
    initialized: bool,
}

impl<'a> Limit<'a> {
    /// Builds a `LIMIT limit OFFSET offset` operator over an already-planned child.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- LIMIT 10
    /// --   Limit::new(child, 10, 0)
    ///
    /// -- LIMIT 10 OFFSET 20
    /// --   Limit::new(child, 10, 20)
    ///
    /// -- LIMIT 0
    /// --   Limit::new(child, 0, 0)
    /// ```
    ///
    /// # Errors
    ///
    /// Does not validate eagerly. Errors from the child surface from
    /// [`FallibleIterator::next`] while skipping the offset or returning rows.
    pub fn new(child: Box<PlanNode<'a>>, limit: u64, offset: u64) -> Self {
        Self {
            child,
            limit,
            offset,
            count: 0,
            initialized: false,
        }
    }

    /// Consumes the SQL `OFFSET` rows from the child, but only once.
    ///
    /// If the child is exhausted before the full offset is consumed, the skip
    /// stops early and the operator will immediately return `None` on the next
    /// `next` call.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's `next`.
    fn skip_offset(&mut self) -> Result<(), ExecutionError> {
        if self.initialized {
            return Ok(());
        }
        for _ in 0..self.offset {
            let Some(_) = self.child.next()? else {
                break;
            };
        }
        self.initialized = true;
        Ok(())
    }
}

impl FallibleIterator for Limit<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Returns the next row inside the SQL `LIMIT` / `OFFSET` window.
    ///
    /// The first call skips `offset` rows, then each successful output
    /// increments the `limit` counter. `NULL` values are irrelevant because this
    /// operator counts rows only.
    ///
    /// # Errors
    ///
    /// Propagates errors returned by the child while skipping or reading rows.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.skip_offset()?;
        if self.count >= self.limit {
            return Ok(None);
        }

        let ch = self.child.next()?;
        match ch {
            Some(tup) => {
                self.count += 1;
                Ok(Some(tup))
            }
            None => Ok(None),
        }
    }
}

impl Executor for Limit<'_> {
    /// Exposes the unchanged child schema for `SELECT ... LIMIT ...`.
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    /// Resets the SQL row window and rewinds the child.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the child's rewind implementation.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.count = 0;
        self.initialized = false;
        self.child.rewind()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fallible_iterator::FallibleIterator;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        execution::{PlanNode, scan::SeqScan},
        heap::file::HeapFile,
        primitives::{ColumnId, Predicate},
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn scan_schema() -> TupleSchema {
        TupleSchema::new(vec![field("id", Type::Int32), field("flag", Type::Bool)])
    }

    fn make_scan_tuple(id: i32, flag: bool) -> Tuple {
        Tuple::new(vec![Value::Int32(id), Value::Bool(flag)])
    }

    fn begin_txn(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    fn make_registered_heap_file(existing_pages: u32) -> (HeapFile, Arc<Wal>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let file_id = FileId::new(1);
        let path = dir.path().join("heap.db");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let needed = (existing_pages as usize).max(4) * crate::storage::PAGE_SIZE;
        file.set_len(needed as u64).unwrap();
        drop(file);

        store.register_file(file_id, &path).unwrap();

        let heap = HeapFile::new(
            file_id,
            scan_schema(),
            Arc::clone(&store),
            existing_pages,
            Arc::clone(&wal),
        );
        (heap, wal, dir)
    }

    #[test]
    fn test_filter_next_yields_matching_tuples_only() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(2, false)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(3, true)).unwrap();

        let pred = BooleanExpression::col_op_lit(1, Predicate::Equals, Value::Bool(true));
        let mut filter = Filter::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), pred);

        let mut out = Vec::new();
        while let Some(t) = filter.next().unwrap() {
            out.push(t);
        }
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], make_scan_tuple(1, true));
        assert_eq!(out[1], make_scan_tuple(3, true));
    }

    #[test]
    fn test_project_new_reorders_columns() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(42, false)).unwrap();

        let mut proj = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(1u32).unwrap(),
            ColumnId::try_from(0u32).unwrap(),
        ])
        .unwrap();

        assert_eq!(proj.schema().field(0).unwrap().name, "flag");
        assert_eq!(proj.schema().field(1).unwrap().name, "id");

        let row = proj.next().unwrap().unwrap();
        assert_eq!(row, Tuple::new(vec![Value::Bool(false), Value::Int32(42)]));
    }

    #[test]
    fn test_sort_next_ascending_and_descending() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in [3_i32, 1, 2] {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let col = ColumnId::try_from(0u32).unwrap();

        {
            let mut asc = Sort::new(
                vec![SortKey::asc(col)],
                Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            );
            let mut asc_out = Vec::new();
            while let Some(t) = asc.next().unwrap() {
                asc_out.push(t);
            }
            assert_eq!(
                asc_out
                    .into_iter()
                    .map(|t| t.get(0).unwrap().clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
            );
        }

        // Same txn: a second read txn can block on page locks held until the first
        // scan's resources are fully released; one txn avoids lock handoff races.
        let mut desc = Sort::new(
            vec![SortKey::desc(col)],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut desc_out = Vec::new();
        while let Some(t) = desc.next().unwrap() {
            desc_out.push(t);
        }
        assert_eq!(
            desc_out
                .into_iter()
                .map(|t| t.get(0).unwrap().clone())
                .collect::<Vec<_>>(),
            vec![Value::Int32(3), Value::Int32(2), Value::Int32(1)]
        );
    }

    #[test]
    fn test_sort_multi_key_breaks_ties_with_secondary() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        // Same id (1), different flag — secondary key decides the order.
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(1, false)).unwrap();
        heap.insert_tuple(txn, &make_scan_tuple(2, true)).unwrap();

        // ORDER BY id ASC, flag DESC  =>  (1,true), (1,false), (2,true)
        let mut sort = Sort::new(
            vec![
                SortKey::asc(ColumnId::try_from(0u32).unwrap()),
                SortKey::desc(ColumnId::try_from(1u32).unwrap()),
            ],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut out = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            out.push(t);
        }
        assert_eq!(out, vec![
            make_scan_tuple(1, true),
            make_scan_tuple(1, false),
            make_scan_tuple(2, true),
        ]);
    }

    #[test]
    fn test_sort_null_sorts_before_non_null() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &Tuple::new(vec![Value::Int32(10), Value::Bool(true)]))
            .unwrap();
        heap.insert_tuple(txn, &Tuple::new(vec![Value::Null, Value::Bool(false)]))
            .unwrap();

        let mut sort = Sort::new(
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let first = sort.next().unwrap().unwrap();
        assert_eq!(first.get(0), Some(&Value::Null));
        let second = sort.next().unwrap().unwrap();
        assert_eq!(second.get(0), Some(&Value::Int32(10)));
    }

    #[test]
    fn test_limit_next_offset_and_limit_window() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in 1_i32..=3 {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 1, 1);
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
    }

    // --- edge cases: operators ---

    #[test]
    fn test_filter_next_empty_child_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let mut filter = Filter::new(
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(0)),
        );
        assert_eq!(filter.next().unwrap(), None);
    }

    #[test]
    fn test_project_new_invalid_column_id_returns_type_error() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let err = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(99u32).unwrap(),
        ])
        .unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn test_sort_next_empty_child_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let mut sort = Sort::new(
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        assert_eq!(sort.next().unwrap(), None);
    }

    #[test]
    fn test_limit_next_limit_zero_returns_none_after_skip() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 0, 0);
        assert_eq!(lim.next().unwrap(), None);
    }

    #[test]
    fn test_limit_next_offset_past_end_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, true)).unwrap();

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 5, 10);
        assert_eq!(lim.next().unwrap(), None);
    }

    // --- rewind / invariants ---

    #[test]
    fn test_filter_rewind_delegates_to_child() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(7, true)).unwrap();

        let mut filter = Filter::new(
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
            BooleanExpression::col_op_lit(0, Predicate::Equals, Value::Int32(7)),
        );
        assert_eq!(filter.next().unwrap(), Some(make_scan_tuple(7, true)));
        filter.rewind().unwrap();
        assert_eq!(filter.next().unwrap(), Some(make_scan_tuple(7, true)));
    }

    #[test]
    fn test_project_rewind_delegates_to_child() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_scan_tuple(1, false)).unwrap();

        let mut proj = Project::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), &[
            ColumnId::try_from(0u32).unwrap(),
        ])
        .unwrap();
        assert_eq!(
            proj.next().unwrap(),
            Some(Tuple::new(vec![Value::Int32(1)]))
        );
        proj.rewind().unwrap();
        assert_eq!(
            proj.next().unwrap(),
            Some(Tuple::new(vec![Value::Int32(1)]))
        );
    }

    #[test]
    fn test_sort_rewind_replays_buffer() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in [2_i32, 3, 1] {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut sort = Sort::new(
            vec![SortKey::asc(ColumnId::try_from(0u32).unwrap())],
            Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))),
        );
        let mut first_pass = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            first_pass.push(t);
        }
        sort.rewind().unwrap();
        let mut second_pass = Vec::new();
        while let Some(t) = sort.next().unwrap() {
            second_pass.push(t);
        }
        assert_eq!(first_pass, second_pass);
    }

    #[test]
    fn test_limit_rewind_resets_window() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for id in 1_i32..=3 {
            heap.insert_tuple(txn, &make_scan_tuple(id, true)).unwrap();
        }

        let mut lim = Limit::new(Box::new(PlanNode::SeqScan(SeqScan::new(&heap, txn))), 1, 1);
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
        lim.rewind().unwrap();
        assert_eq!(lim.next().unwrap(), Some(make_scan_tuple(2, true)));
        assert_eq!(lim.next().unwrap(), None);
    }
}
