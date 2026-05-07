//! SQL statement AST — the unbound shape of every command the parser accepts.
//!
//! These types are the bridge between the lexer/parser and the rest of the
//! database. The parser tokenises a SQL string and constructs one of the
//! [`Statement`] variants here; the binder then resolves names against the
//! catalog and lowers the AST into something the executor can run. Nothing in
//! this module touches the catalog, the buffer pool, or the executor — every
//! field is a raw, unresolved fragment of SQL syntax.
//!
//! Every statement type implements [`Display`] and round-trips back into a
//! SQL-like string, which is the format used in tracing, error messages, and
//! debug logs.
//!
//! # SQL coverage
//!
//! The covered surface, by clause:
//!
//! - DDL — `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `SHOW INDEXES`.
//! - DML — `INSERT … VALUES`, `UPDATE … SET … WHERE`, `DELETE FROM … WHERE`.
//! - Queries — `SELECT [DISTINCT] … FROM … [JOIN …] [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …]
//!   [LIMIT n [OFFSET m]]`.
//!
//! Aggregates supported in `SELECT`: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`,
//! `MIN`, `MAX`. `WHERE`/`HAVING` predicates are conjunctions/disjunctions of
//! `<column> <op> <literal>` leaves; `IS NULL` / `BETWEEN` / subqueries are not
//! modeled at this layer.
//!
//! # Shape
//!
//! - [`Statement`] — top-level dispatch enum, one variant per SQL command.
//! - [`ColumnRef`] — possibly-qualified column reference (`t.c` or `c`), shared by every clause
//!   that names a column.
//! - [`WhereCondition`] — predicate tree used by `WHERE`, `HAVING`, and `JOIN … ON`.
//! - [`AggFunc`] — aggregate function tag used by [`Expr::Agg`].
//! - DDL: [`CreateTableStatement`], [`DropStatement`], [`CreateIndexStatement`],
//!   [`DropIndexStatement`], [`ShowIndexesStatement`], [`ColumnDef`].
//! - DML: [`InsertStatement`], [`UpdateStatement`] (with [`Assignment`]), [`DeleteStatement`].
//! - Query: [`SelectStatement`], [`SelectColumns`], [`SelectItem`], [`Expr`], [`TableWithJoins`],
//!   [`TableRef`], [`Join`], [`JoinKind`], [`OrderBy`], [`OrderDirection`], [`LimitClause`].
//!
//! # Fixed schema for examples
//!
//! Throughout the docs in this module, examples use:
//!
//! ```sql
//! CREATE TABLE users  (id INT PRIMARY KEY, name VARCHAR, age INT);
//! CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, total INT);
//! ```
//!
//! Column indices after binding are `users(id → 0, name → 1, age → 2)` and
//! `orders(order_id → 0, user_id → 1, total → 2)`.
//!
//! # NULL semantics
//!
//! The AST itself is value-neutral — `NULL` literals are stored as
//! [`Value::Null`]. Three-valued logic in `WHERE` is
//! the executor's concern; see [`crate::execution::expression`] for how
//! predicates collapse `NULL` to `false`.
//!
//! # File layout
//!
//! 1. **Top-level dispatch** — the [`Statement`] enum and its factories.
//! 2. **Shared AST primitives** — [`ColumnRef`], [`WhereCondition`], [`AggFunc`].
//! 3. **DDL** — `CREATE` / `DROP` / `SHOW` statements.
//! 4. **DML** — `INSERT` / `UPDATE` / `DELETE` statements.
//! 5. **SELECT** — projection, `FROM`, joins, `ORDER BY`, `LIMIT`, and [`SelectStatement`] itself.

use std::fmt::Display;

use crate::{
    Type,
    index::IndexKind,
    primitives::{NonEmptyString, Predicate},
    tuple::{Field, TupleSchema},
    types::Value,
};

/// Top-level SQL statement returned by the parser — one variant per command.
///
/// Each variant wraps a dedicated struct that holds the parsed clause data.
/// Factory methods on `Statement` are `pub(super)` so only the parser module
/// can construct them, keeping AST creation private.
///
/// # SQL → variant mapping
///
/// ```sql
/// -- CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT);
/// --   Statement::CreateTable(CreateTableStatement { … })
///
/// -- DROP TABLE IF EXISTS users;
/// --   Statement::Drop(DropStatement { table_name: "users", if_exists: true })
///
/// -- CREATE INDEX idx_age ON users(age) USING BTREE;
/// --   Statement::CreateIndex(CreateIndexStatement { … })
///
/// -- DROP INDEX idx_age ON users;
/// --   Statement::DropIndex(DropIndexStatement { … })
///
/// -- SHOW INDEXES FROM users;
/// --   Statement::ShowIndexes(ShowIndexesStatement(Some("users")))
///
/// -- INSERT INTO users (id, name, age) VALUES (1, 'a', 30);
/// --   Statement::Insert(InsertStatement { … })
///
/// -- UPDATE users SET age = 31 WHERE id = 1;
/// --   Statement::Update(UpdateStatement { … })
///
/// -- DELETE FROM users WHERE age < 18;
/// --   Statement::Delete(DeleteStatement { … })
///
/// -- SELECT name FROM users WHERE age >= 18;
/// --   Statement::Select(SelectStatement { … })
/// ```
pub enum Statement {
    Drop(DropStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    AlterTable(AlterTableStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Select(SelectStatement),
    ShowIndexes(ShowIndexesStatement),
}

/// Pretty-prints the statement as a SQL-like string.
///
/// # Examples
///
/// ```
/// use storemy::{
///     parser::statements::{ColumnDef, CreateTableStatement, Statement},
///     primitives::NonEmptyString,
///     types::Type,
/// };
///
/// let col = ColumnDef {
///     name: NonEmptyString::new("id").unwrap(),
///     col_type: Type::Int32,
///     nullable: false,
///     primary_key: true,
///     auto_increment: false,
///     default: None,
/// };
/// let inner = CreateTableStatement {
///     table_name: "users".into(),
///     if_not_exists: false,
///     columns: vec![col],
///     primary_key: vec![],
/// };
/// let statement = Statement::CreateTable(inner);
/// assert_eq!(
///     statement.to_string(),
///     "CREATE TABLE users (id INT NOT NULL PRIMARY KEY)"
/// );
/// ```
impl Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Drop(statement) => write!(f, "{statement}"),
            Statement::CreateTable(statement) => write!(f, "{statement}"),
            Statement::CreateIndex(statement) => write!(f, "{statement}"),
            Statement::DropIndex(statement) => write!(f, "{statement}"),
            Statement::AlterTable(statement) => write!(f, "{statement}"),
            Statement::Delete(statement) => write!(f, "{statement}"),
            Statement::Insert(statement) => write!(f, "{statement}"),
            Statement::Update(statement) => write!(f, "{statement}"),
            Statement::Select(statement) => write!(f, "{statement}"),
            Statement::ShowIndexes(statement) => write!(f, "{statement}"),
        }
    }
}

impl Statement {
    /// Short, stable identifier for the variant — used in tracing spans and
    /// metrics where the full `Display` form would be too noisy.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Statement::Drop(_) => "Drop",
            Statement::CreateTable(_) => "CreateTable",
            Statement::CreateIndex(_) => "CreateIndex",
            Statement::DropIndex(_) => "DropIndex",
            Statement::AlterTable(_) => "AlterTable",
            Statement::Delete(_) => "Delete",
            Statement::Insert(_) => "Insert",
            Statement::Update(_) => "Update",
            Statement::Select(_) => "Select",
            Statement::ShowIndexes(_) => "ShowIndexes",
        }
    }

    pub(super) fn delete(
        table_name: impl Into<String>,
        alias: Option<String>,
        where_clause: Option<WhereCondition>,
    ) -> DeleteStatement {
        DeleteStatement {
            table_name: table_name.into(),
            alias,
            where_clause,
        }
    }

    pub(super) fn insert(
        table_name: impl Into<String>,
        columns: Option<Vec<String>>,
        source: InsertSource,
    ) -> InsertStatement {
        InsertStatement {
            table_name: table_name.into(),
            columns,
            source,
        }
    }

    pub(super) fn update(
        table_name: impl Into<String>,
        alias: Option<String>,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereCondition>,
    ) -> UpdateStatement {
        UpdateStatement {
            table_name: table_name.into(),
            alias,
            assignments,
            where_clause,
        }
    }

    pub(super) fn show_indexes(table_name: Option<String>) -> ShowIndexesStatement {
        ShowIndexesStatement(table_name)
    }
}

/// A column reference — `col` or `qualifier.col`.
///
/// Used everywhere a column name appears in SQL: `SELECT` projections,
/// `WHERE` / `HAVING` predicates, `ORDER BY`, `GROUP BY`, and `JOIN … ON`. The
/// `qualifier` is whatever literal token preceded the dot — it might be a
/// table name (`users.age`) or an alias (`u.age`); this struct does not
/// distinguish them. The binder is responsible for resolving the qualifier
/// against the active scope.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT name FROM users
/// --   ColumnRef { qualifier: None,            name: "name" }
///
/// -- SELECT u.age FROM users u
/// --   ColumnRef { qualifier: Some("u"),       name: "age" }
///
/// -- ORDER BY users.id
/// --   ColumnRef { qualifier: Some("users"),   name: "id" }
///
/// -- ON users.id = orders.user_id
/// --   left  = ColumnRef { qualifier: Some("users"),  name: "id" }
/// --   right = ColumnRef { qualifier: Some("orders"), name: "user_id" }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub qualifier: Option<String>, // table name or alias
    pub name: String,
}

impl From<&str> for ColumnRef {
    fn from(s: &str) -> Self {
        let parts = s.split('.').collect::<Vec<&str>>();
        if parts.len() == 1 {
            return Self {
                qualifier: None,
                name: parts[0].to_string(),
            };
        }
        Self {
            qualifier: Some(parts[0].to_string()),
            name: parts[1].to_string(),
        }
    }
}

impl Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(qualifier) = &self.qualifier {
            write!(f, "{qualifier}.")?;
        }
        write!(f, "{}", self.name)
    }
}

/// A `WHERE` / `HAVING` / `JOIN ON` predicate tree.
///
/// Leaves are `<column> <op> <literal>` comparisons; interior nodes combine
/// children with `AND` / `OR`. The parser respects standard SQL precedence —
/// `AND` binds tighter than `OR` — so `a OR b AND c` parses as
/// `Or(a, And(b, c))`.
///
/// Only column-vs-literal leaves are produced at parse time. Column-vs-column
/// comparisons (used by join residuals) are introduced later by the binder
/// when it lowers `JOIN … ON` clauses to executor expressions.
///
/// # SQL examples
///
/// ```sql
/// -- WHERE age = 30
/// --   WhereCondition::Predicate {
/// --       field: ColumnRef { qualifier: None, name: "age" },
/// --       op:    Predicate::Equals,
/// --       value: Value::Int32(30),
/// --   }
///
/// -- WHERE age >= 18 AND name = 'alice'
/// --   And(
/// --       Predicate { field: age, op: GreaterThanOrEqual, value: Int32(18)   },
/// --       Predicate { field: name, op: Equals,            value: String("alice") },
/// --   )
///
/// -- WHERE age < 18 OR age > 65
/// --   Or(
/// --       Predicate { field: age, op: LessThan,    value: Int32(18) },
/// --       Predicate { field: age, op: GreaterThan, value: Int32(65) },
/// --   )
///
/// -- ON users.id = orders.user_id      (post-bind, this becomes the join residual)
/// --   Predicate { field: ColumnRef { qualifier: Some("users"), name: "id" },
/// --               op: Predicate::Equals,
/// --               value: <bound to orders.user_id by the binder> }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum WhereCondition {
    Predicate {
        field: ColumnRef,
        op: Predicate,
        value: Value,
    },
    And(Box<WhereCondition>, Box<WhereCondition>),
    Or(Box<WhereCondition>, Box<WhereCondition>),
}

impl WhereCondition {
    /// Builds a single `<column> <op> <literal>` leaf.
    ///
    /// ```sql
    /// -- WHERE age = 30
    /// --   WhereCondition::predicate("age".into(), Predicate::Equals, Value::Int32(30))
    ///
    /// -- WHERE name <> 'guest'
    /// --   WhereCondition::predicate("name".into(), Predicate::NotEqual, Value::String("guest".into()))
    /// ```
    pub fn predicate(field: ColumnRef, op: Predicate, value: Value) -> Self {
        Self::Predicate { field, op, value }
    }
}

impl Display for WhereCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WhereCondition::Predicate { field, op, value } => {
                write!(f, "{field} {op} {value}")
            }
            WhereCondition::And(l, r) => write!(f, "({l} AND {r})"),
            WhereCondition::Or(l, r) => write!(f, "({l} OR {r})"),
        }
    }
}

/// An SQL aggregate function tag — the function name in `SELECT f(col)`.
///
/// Pairs with a column name (or `*` for `COUNT`) inside [`Expr::Agg`] /
/// [`Expr::CountStar`] to form a complete projection like `SUM(total)`.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT COUNT(*)              FROM users;   --> Expr::CountStar
/// -- SELECT COUNT(name)           FROM users;   --> Expr::Agg(AggFunc::Count, "name")
/// -- SELECT AVG(age)              FROM users;   --> Expr::Agg(AggFunc::Avg,   "age")
/// -- SELECT MIN(age), MAX(age)    FROM users;   --> two Exprs with AggFunc::Min / AggFunc::Max
/// -- SELECT SUM(total) FROM orders GROUP BY user_id;
/// --                                            --> Expr::Agg(AggFunc::Sum,   "total")
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    /// `COUNT(col)` — number of non-null values.
    Count,
    /// `SUM(col)` — total of numeric values.
    Sum,
    /// `AVG(col)` — arithmetic mean of numeric values.
    Avg,
    /// `MIN(col)` — smallest value.
    Min,
    /// `MAX(col)` — largest value.
    Max,
}

impl Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggFunc::Count => write!(f, "COUNT"),
            AggFunc::Sum => write!(f, "SUM"),
            AggFunc::Avg => write!(f, "AVG"),
            AggFunc::Min => write!(f, "MIN"),
            AggFunc::Max => write!(f, "MAX"),
        }
    }
}

impl TryFrom<&str> for AggFunc {
    type Error = String;

    /// Parse an aggregate function name from its uppercase SQL keyword.
    ///
    /// # Errors
    ///
    /// Returns an error string if `s` is not one of the recognized aggregate
    /// names (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`).
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "COUNT" => Ok(AggFunc::Count),
            "SUM" => Ok(AggFunc::Sum),
            "AVG" => Ok(AggFunc::Avg),
            "MIN" => Ok(AggFunc::Min),
            "MAX" => Ok(AggFunc::Max),
            other => Err(format!("unknown aggregate function: {other}")),
        }
    }
}

/// Parsed `DROP TABLE [IF EXISTS] <name>`.
///
/// # SQL examples
///
/// ```sql
/// -- DROP TABLE users;
/// --   DropStatement { table_name: "users", if_exists: false }
///
/// -- DROP TABLE IF EXISTS users;
/// --   DropStatement { table_name: "users", if_exists: true }
/// ```
///
/// With `if_exists = false`, executing the statement against a non-existent
/// table is an error; with `if_exists = true`, it's a no-op.
#[derive(Debug, Clone)]
pub struct DropStatement {
    pub table_name: String,
    pub if_exists: bool,
}

impl Display for DropStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, "{}", self.table_name)
    }
}

/// A single column definition inside `CREATE TABLE`.
///
/// Captures the surface syntax of one column: name, declared type, and any
/// inline modifiers (`NOT NULL`, `PRIMARY KEY`, `AUTO_INCREMENT`, `DEFAULT`).
/// Table-level constraints like `PRIMARY KEY (col)` are stored separately on
/// [`CreateTableStatement::primary_key`].
///
/// # SQL examples
///
/// ```sql
/// -- id INT NOT NULL PRIMARY KEY AUTO_INCREMENT
/// --   ColumnDef {
/// --       name: "id", col_type: Type::Int32,
/// --       nullable: false, primary_key: true,
/// --       auto_increment: true, default: None,
/// --   }
///
/// -- name VARCHAR
/// --   ColumnDef { name: "name", col_type: Type::String,
/// --               nullable: true, primary_key: false,
/// --               auto_increment: false, default: None }
///
/// -- age INT DEFAULT 0
/// --   ColumnDef { name: "age", col_type: Type::Int32,
/// --               nullable: true, primary_key: false,
/// --               auto_increment: false,
/// --               default: Some(Value::Int32(0)) }
/// ```
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: NonEmptyString,
    pub col_type: Type,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub default: Option<Value>,
}

impl Display for ColumnDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.col_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if self.auto_increment {
            write!(f, " AUTO_INCREMENT")?;
        }
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {default}")?;
        }
        Ok(())
    }
}

/// Builds a [`TupleSchema`] from a `CREATE TABLE` column list (same as
/// [`CreateTableStatement::columns`]).
///
/// Maps name, type, and nullability in vector order. A column declared `PRIMARY KEY`
/// is stored as non-nullable regardless of the `nullable` flag, matching SQL semantics.
/// `auto_increment`, `default`, and table-level primary keys are not part of [`Field`]
/// and are ignored here.
impl From<Vec<&ColumnDef>> for TupleSchema {
    fn from(columns: Vec<&ColumnDef>) -> Self {
        TupleSchema::new(
            columns
                .into_iter()
                .map(|col| {
                    let field_nullable = col.nullable && !col.primary_key;
                    let mut field = Field::new_non_empty(col.name.clone(), col.col_type);
                    if !field_nullable {
                        field = field.not_null();
                    }
                    field
                })
                .collect(),
        )
    }
}

/// Parsed `CREATE TABLE [IF NOT EXISTS] name (col_def, ..., [PRIMARY KEY (col)])`.
///
/// `primary_key` is `Some(col)` only when the user wrote a *table-level*
/// constraint like `PRIMARY KEY (id)`. If the primary key was declared
/// inline on a column (`id INT PRIMARY KEY`), it lives on the relevant
/// [`ColumnDef::primary_key`] flag instead and `primary_key` here is `None`.
///
/// # SQL examples
///
/// ```sql
/// -- CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT);
/// --   CreateTableStatement {
/// --       table_name: "users",
/// --       if_not_exists: false,
/// --       columns: vec![ /* id (PK inline), name, age */ ],
/// --       primary_key: None,
/// --   }
///
/// -- CREATE TABLE IF NOT EXISTS orders (
/// --     order_id INT, user_id INT, total INT,
/// --     PRIMARY KEY (order_id)
/// -- );
/// --   CreateTableStatement {
/// --       table_name: "orders",
/// --       if_not_exists: true,
/// --       columns: vec![ /* order_id, user_id, total — none with primary_key=true */ ],
/// --       primary_key: Some("order_id"),
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
}

/// Pretty-prints the statement as a SQL-like string.
impl Display for CreateTableStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} (", self.table_name)?;
        let cols: Vec<String> = self.columns.iter().map(ToString::to_string).collect();
        write!(f, "{}", cols.join(", "))?;
        if !self.primary_key.is_empty() {
            write!(f, ", PRIMARY KEY ({})", self.primary_key.join(", "))?;
        }
        write!(f, ")")
    }
}

/// Parsed `CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<col>, ...) USING <type>`.
///
/// Supports composite (multi-column) indexes. Column order in `columns` is
/// semantically significant — for B-tree indexes it determines the key order
/// used for the leftmost-prefix lookup rule.
///
/// # SQL examples
///
/// ```sql
/// -- CREATE INDEX idx_age ON users (age) USING BTREE;
/// --   CreateIndexStatement {
/// --       index_name: "idx_age", table_name: "users",
/// --       columns: ["age"],
/// --       index_type: IndexKind::Btree, if_not_exists: false,
/// --   }
///
/// -- CREATE INDEX idx_user ON orders (user_id, created_at) USING BTREE;
/// --   CreateIndexStatement {
/// --       index_name: "idx_user", table_name: "orders",
/// --       columns: ["user_id", "created_at"],
/// --       index_type: IndexKind::Btree, if_not_exists: false,
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexKind,
    pub if_not_exists: bool,
}

impl Display for CreateIndexStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let index_type = match self.index_type {
            IndexKind::Hash => "HASH",
            IndexKind::Btree => "BTREE",
        };
        write!(f, "CREATE INDEX")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(
            f,
            " {} ON {} ({}) USING {}",
            self.index_name,
            self.table_name,
            self.columns.join(", "),
            index_type
        )
    }
}

/// Parsed `DROP INDEX [IF EXISTS] <index> ON <table>`.
///
/// # SQL examples
///
/// ```sql
/// -- DROP INDEX idx_age ON users;
/// --   DropIndexStatement {
/// --       table_name: "users", index_name: "idx_age", if_exists: false,
/// --   }
///
/// -- DROP INDEX IF EXISTS idx_age ON users;
/// --   DropIndexStatement {
/// --       table_name: "users", index_name: "idx_age", if_exists: true,
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    pub table_name: String,
    pub index_name: String,
    pub if_exists: bool,
}

impl Display for DropIndexStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {} ON {}", self.index_name, self.table_name)
    }
}

/// Parsed `SHOW INDEXES [FROM <table>]`.
///
/// The inner `Option<String>` holds the table name when `FROM` is specified;
/// `None` means show indexes for all tables.
///
/// # SQL examples
///
/// ```sql
/// -- SHOW INDEXES;
/// --   ShowIndexesStatement(None)
///
/// -- SHOW INDEXES FROM users;
/// --   ShowIndexesStatement(Some("users"))
/// ```
#[derive(Debug, Clone)]
pub struct ShowIndexesStatement(pub Option<String>);

impl Display for ShowIndexesStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW INDEXES")?;
        if let Some(table) = &self.0 {
            write!(f, " FROM {table}")?;
        }
        Ok(())
    }
}

/// Parsed `ALTER TABLE [IF EXISTS] <name> <action>`.
///
/// `if_exists` is the *outer* clause — it means "don't error if the *table*
/// is missing." Per-action `if_exists` flags (e.g. `DROP COLUMN IF EXISTS bio`)
/// live inside the corresponding [`AlterAction`] variant.
///
/// Only a single action per statement is modeled at this stage. Multi-action
/// SQL (`ALTER TABLE u ADD COLUMN x INT, DROP COLUMN y`) would change `action`
/// to `Vec<AlterAction>` without touching the variants themselves.
///
/// # SQL examples
///
/// ```sql
/// -- ALTER TABLE users ADD COLUMN age INT NOT NULL DEFAULT 0;
/// --   AlterTableStatement {
/// --       table_name: "users", if_exists: false,
/// --       action: AlterAction::AddColumn(ColumnDef { name: "age", … }),
/// --   }
///
/// -- ALTER TABLE IF EXISTS users RENAME TO accounts;
/// --   AlterTableStatement {
/// --       table_name: "users", if_exists: true,
/// --       action: AlterAction::RenameTable { to: "accounts" },
/// --   }
///
/// -- ALTER TABLE users DROP COLUMN IF EXISTS bio;
/// --   AlterTableStatement {
/// --       table_name: "users", if_exists: false,
/// --       action: AlterAction::DropColumn { name: "bio", if_exists: true },
/// --   }
///
/// -- ALTER TABLE users RENAME COLUMN name TO full_name;
/// --   AlterTableStatement {
/// --       table_name: "users", if_exists: false,
/// --       action: AlterAction::RenameColumn { from: "name", to: "full_name" },
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub if_exists: bool,
    pub action: AlterAction,
}

/// One mutation applied by an `ALTER TABLE` statement.
///
/// Each variant captures the syntactic shape of one supported action. The
/// parser produces these directly; the binder/executor decides what work each
/// one implies on storage (some are metadata-only, others may require
/// rewriting every page in the table).
///
/// Variants:
///
/// - [`AlterAction::AddColumn`] — append a column. Reuses [`ColumnDef`] so the column tail (`<type>
///   [NOT NULL] [PRIMARY KEY] [AUTO_INCREMENT] [DEFAULT <lit>]`) parses identically to a column
///   inside `CREATE TABLE`.
/// - [`AlterAction::DropColumn`] — remove a column by name. The inner `if_exists` controls whether
///   dropping a missing column is a no-op or an error — independent of the outer table-level
///   `if_exists`.
/// - [`AlterAction::RenameColumn`] — change a column's name. Catalog-only.
/// - [`AlterAction::RenameTable`] — change the table's name. Catalog-only.
/// - [`AlterAction::SetDefault`] — set a column's default value.
/// - [`AlterAction::DropDefault`] — drop a column's default value.
/// - [`AlterAction::DropNotNull`] — drop a column's NOT NULL constraint.
/// - [`AlterAction::AddPrimaryKey`] — add a primary key to one or more columns.
/// - [`AlterAction::DropPrimaryKey`] — drop the table's primary key.
#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn { name: String, if_exists: bool },
    RenameColumn { from: String, to: String },
    RenameTable { to: String },
    SetDefault { column: String, value: Value },
    DropDefault { column: String },
    DropNotNull { column: String },
    AddPrimaryKey { columns: Vec<String> },
    DropPrimaryKey,
}

impl Display for AlterTableStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TABLE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {} {}", self.table_name, self.action)
    }
}

impl Display for AlterAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterAction::AddColumn(col) => write!(f, "ADD COLUMN {col}"),
            AlterAction::DropColumn { name, if_exists } => {
                write!(f, "DROP COLUMN")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")
            }
            AlterAction::RenameColumn { from, to } => {
                write!(f, "RENAME COLUMN {from} TO {to}")
            }
            AlterAction::RenameTable { to } => write!(f, "RENAME TO {to}"),
            AlterAction::SetDefault { column, value } => {
                write!(f, "ALTER COLUMN {column} SET DEFAULT {value}")
            }
            AlterAction::DropDefault { column } => write!(f, "ALTER COLUMN {column} DROP DEFAULT"),
            AlterAction::DropNotNull { column } => write!(f, "ALTER COLUMN {column} DROP NOT NULL"),
            AlterAction::AddPrimaryKey { columns } => {
                write!(f, "ADD PRIMARY KEY ({})", columns.join(", "))
            }
            AlterAction::DropPrimaryKey => write!(f, "DROP PRIMARY KEY"),
        }
    }
}

/// Parsed `INSERT INTO table [(col, …)] VALUES (val, …), …`.
///
/// `columns = None` means the user omitted the column list, so values must
/// match the table's declared column order. `columns = Some(_)` means the
/// user gave an explicit list and the binder will reorder/null-fill against
/// the schema. `values` always carries one inner `Vec<Value>` per row, even
/// for single-row inserts.
///
/// # SQL examples
///
/// ```sql
/// -- INSERT INTO users VALUES (1, 'alice', 30);
/// --   InsertStatement {
/// --       table_name: "users", columns: None,
/// --       values: vec![vec![Int32(1), String("alice"), Int32(30)]],
/// --   }
///
/// -- INSERT INTO users (id, name) VALUES (2, 'bob');
/// --   InsertStatement {
/// --       table_name: "users",
/// --       columns: Some(vec!["id", "name"]),
/// --       values: vec![vec![Int32(2), String("bob")]],
/// --   }
///
/// -- INSERT INTO users VALUES (3, 'c', 25), (4, 'd', 40);
/// --   InsertStatement {
/// --       table_name: "users", columns: None,
/// --       values: vec![
/// --           vec![Int32(3), String("c"), Int32(25)],
/// --           vec![Int32(4), String("d"), Int32(40)],
/// --       ],
/// --   }
/// ```
/// Where the rows for an `INSERT` come from.
///
/// SQL allows three row sources: an explicit `VALUES` list, the special
/// `DEFAULT VALUES` form, or a sub-query (`INSERT … SELECT …`). Modeling
/// these as an enum keeps invalid combinations unrepresentable — the parser
/// can only ever build one shape, and downstream consumers are forced by
/// the compiler to handle every case.
#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Value>>),
    DefaultValues,
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
}

impl Display for InsertStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSERT INTO {}", self.table_name)?;
        if let Some(cols) = &self.columns {
            write!(f, " ({})", cols.join(", "))?;
        }

        match &self.source {
            InsertSource::DefaultValues => write!(f, " DEFAULT VALUES"),
            InsertSource::Select(sel) => write!(f, " {sel}"),
            InsertSource::Values(rows) => {
                let rendered: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(ToString::to_string).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();
                write!(f, " VALUES {}", rendered.join(", "))
            }
        }
    }
}

/// A single `col = val` pair in an `UPDATE … SET` clause.
///
/// Right-hand sides are literal [`Value`]s only — column-to-column or
/// expression assignments (`SET total = total + 1`) are not modeled yet.
///
/// # SQL examples
///
/// ```sql
/// -- SET age = 31
/// --   Assignment { column: "age",  value: Value::Int32(31) }
///
/// -- SET name = 'alice'
/// --   Assignment { column: "name", value: Value::String("alice".into()) }
/// ```
#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

/// Parsed `UPDATE table [alias] SET col = val, … [WHERE …]`.
///
/// `alias` is the optional `AS u` form, which is what `WHERE` predicates can
/// use to qualify column references in self-referential statements.
/// `where_clause = None` means the update applies to *every* row in the
/// table — same as SQL.
///
/// # SQL examples
///
/// ```sql
/// -- UPDATE users SET age = 31 WHERE id = 1;
/// --   UpdateStatement {
/// --       table_name: "users", alias: None,
/// --       assignments: vec![Assignment { column: "age", value: Int32(31) }],
/// --       where_clause: Some(Predicate { id, Equals, Int32(1) }),
/// --   }
///
/// -- UPDATE users u SET name = 'admin', age = 99;
/// --   UpdateStatement {
/// --       table_name: "users", alias: Some("u"),
/// --       assignments: vec![ /* name = 'admin', age = 99 */ ],
/// --       where_clause: None,
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub alias: Option<String>,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<WhereCondition>,
}

impl Display for UpdateStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {}", self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        write!(f, " SET ")?;
        for (i, assignment) in self.assignments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} = {}", assignment.column, assignment.value)?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

/// Parsed `DELETE FROM table [alias] [WHERE …]`.
///
/// `where_clause = None` means *delete every row* — same as SQL.
///
/// # SQL examples
///
/// ```sql
/// -- DELETE FROM users WHERE age < 18;
/// --   DeleteStatement {
/// --       table_name: "users", alias: None,
/// --       where_clause: Some(Predicate { age, LessThan, Int32(18) }),
/// --   }
///
/// -- DELETE FROM users u WHERE u.id = 7;
/// --   DeleteStatement {
/// --       table_name: "users", alias: Some("u"),
/// --       where_clause: Some(Predicate { ColumnRef { qualifier: Some("u"), name: "id" },
/// --                                      Equals, Int32(7) }),
/// --   }
///
/// -- DELETE FROM users;            -- (truncate-everything form)
/// --   DeleteStatement {
/// --       table_name: "users", alias: None, where_clause: None,
/// --   }
/// ```
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub alias: Option<String>,
    pub where_clause: Option<WhereCondition>,
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {}", self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

/// A single expression inside a `SELECT` list.
///
/// `Column` is a plain projection; `Agg` is `f(col)` for any non-`*` aggregate;
/// `CountStar` is the special `COUNT(*)` form (which counts rows rather than
/// non-null values of a specific column).
///
/// A scalar expression — anything that, given a row, evaluates to a single
/// value. `Expr` is the unified shape that every clause that holds an
/// "expression" eventually wants to use (projections, `WHERE`, `HAVING`,
/// `ORDER BY`, `GROUP BY`, join conditions). Today only `SelectItem` consumes
/// it; the other clauses still use their own narrower types and will migrate
/// later.
///
/// The recursive shape is deliberate: a future `Binary { op, left, right }`
/// variant will let any operand be itself an `Expr` (`age + 1 > 18`,
/// `UPPER(name) = 'BOB'`, `SUM(a + 1)`), so `Agg`'s argument is already typed
/// as `Box<Expr>` to avoid a second migration.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT name        -->  Expr::Column(ColumnRef { None, "name" })
/// -- SELECT u.age       -->  Expr::Column(ColumnRef { Some("u"), "age" })
/// -- SELECT 1           -->  Expr::Literal(Value::Int64(1))
/// -- SELECT 'hello'     -->  Expr::Literal(Value::String("hello"))
/// -- SELECT NULL        -->  Expr::Literal(Value::Null)
/// -- SELECT COUNT(*)    -->  Expr::CountStar
/// -- SELECT COUNT(name) -->  Expr::Agg(AggFunc::Count, Box::new(Expr::Column("name".into())))
/// -- SELECT AVG(age)    -->  Expr::Agg(AggFunc::Avg,   Box::new(Expr::Column("age".into())))
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column reference, possibly qualified (`t.c`).
    Column(ColumnRef),
    /// A constant value: number, string, boolean, or `NULL`.
    Literal(Value),
    /// An aggregate call, e.g. `SUM(amount)`. The argument is recursively an
    /// `Expr` so that future syntax like `SUM(a + 1)` requires no AST change.
    Agg(AggFunc, Box<Expr>),
    /// The special `COUNT(*)` form that counts all rows.
    CountStar,
    /// A binary operator applied to two sub-expressions, e.g. `age > 25`,
    /// `a AND b`, `x = y`. Boolean connectives (`AND`, `OR`) and comparisons
    /// share this shape — they differ only by the [`BinOp`] tag and the
    /// types the binder will require of their operands.
    BinaryOp {
        lhs: Box<Expr>,
        op: BinOp,
        rhs: Box<Expr>,
    },
    /// A unary operator applied to a sub-expression, e.g. `NOT (age > 25)`.
    /// The binder enforces the operand's type (e.g. `NOT` requires boolean).
    UnaryOp { op: UnOp, operand: Box<Expr> },
}

impl Expr {
    /// Convenience constructor for `Expr::Agg(func, Box::new(arg))`.
    pub fn agg(func: AggFunc, arg: Expr) -> Self {
        Expr::Agg(func, Box::new(arg))
    }

    /// Convenience constructor for `Expr::BinaryOp { lhs, op, rhs }` that
    /// hides the `Box::new` boilerplate.
    pub fn binary(lhs: Expr, op: BinOp, rhs: Expr) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    /// Convenience constructor for `Expr::UnaryOp { op, operand }` that hides
    /// the `Box::new` boilerplate.
    pub fn unary(op: UnOp, operand: Expr) -> Self {
        Expr::UnaryOp {
            op,
            operand: Box::new(operand),
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(col) => write!(f, "{col}"),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Agg(func, arg) => write!(f, "{func}({arg})"),
            Expr::CountStar => write!(f, "COUNT(*)"),
            Expr::BinaryOp { lhs, op, rhs } => write!(f, "({lhs} {op} {rhs})"),
            Expr::UnaryOp { op, operand } => write!(f, "({op} {operand})"),
        }
    }
}

/// A binary operator usable inside an [`Expr::BinaryOp`].
///
/// Grouped roughly by the type the binder will require of the operands and
/// produce as a result:
///
/// - **Logical**: `And`, `Or` — both operands and the result are boolean.
/// - **Comparison**: `Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq` — operands of compatible scalar
///   types, result is boolean.
///
/// The parser produces these from the SQL surface tokens (`AND`, `OR`, `=`,
/// `!=`/`<>`, `<`, `<=`, `>`, `>=`). Arithmetic operators (`+`, `-`, `*`, `/`,
/// `%`) will land here when expression-level arithmetic is wired through the
/// parser and the runtime evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    And,
    Or,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinOp::And => "AND",
            BinOp::Or => "OR",
            BinOp::Eq => "=",
            BinOp::NotEq => "<>",
            BinOp::Lt => "<",
            BinOp::LtEq => "<=",
            BinOp::Gt => ">",
            BinOp::GtEq => ">=",
        };
        f.write_str(s)
    }
}

/// A unary operator usable inside an [`Expr::UnaryOp`].
///
/// Currently only `NOT` (logical negation, boolean → boolean). Unary minus
/// for arithmetic will join this enum when arithmetic lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
}

impl Display for UnOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            UnOp::Not => "NOT",
        };
        f.write_str(s)
    }
}

/// One projection in a `SELECT` list, with its optional output alias.
///
/// Aliases come from either the explicit `SELECT expr AS name` form or the
/// implicit `SELECT expr name` form. Both are stored the same way; the surface
/// syntax is irrelevant once parsed.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT name
/// --   SelectItem { expr: Expr::Column("name".into()), alias: None }
///
/// -- SELECT 1 AS one
/// --   SelectItem { expr: Expr::Literal(Value::Int64(1)), alias: Some("one") }
///
/// -- SELECT COUNT(*) AS n
/// --   SelectItem { expr: Expr::CountStar, alias: Some("n") }
///
/// -- SELECT AVG(age) avg_age
/// --   SelectItem {
/// --       expr: Expr::Agg(AggFunc::Avg, Box::new(Expr::Column("age".into()))),
/// --       alias: Some("avg_age"),
/// --   }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<NonEmptyString>,
}

impl SelectItem {
    /// Wraps an expression with no alias. Convenient for tests and for callers
    /// that synthesise projections (e.g. `SELECT *` expansion).
    pub fn bare(expr: Expr) -> Self {
        Self { expr, alias: None }
    }
}

impl From<Expr> for SelectItem {
    fn from(expr: Expr) -> Self {
        Self::bare(expr)
    }
}

impl Display for SelectItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// The projection list of a `SELECT` — either `*` or an explicit set of items.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT *           FROM users
/// --   SelectColumns::All
///
/// -- SELECT id, name    FROM users
/// --   SelectColumns::Exprs(vec![
/// --       SelectItem::bare(SelectExpr::Column("id".into())),
/// --       SelectItem::bare(SelectExpr::Column("name".into())),
/// --   ])
///
/// -- SELECT COUNT(*) AS n FROM users
/// --   SelectColumns::Exprs(vec![
/// --       SelectItem { expr: SelectExpr::CountStar, alias: Some("n") },
/// --   ])
/// ```
#[derive(Debug, Clone)]
pub enum SelectColumns {
    All,
    Exprs(Vec<SelectItem>),
}

impl Display for SelectColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectColumns::All => write!(f, "*"),
            SelectColumns::Exprs(items) => {
                let s: Vec<String> = items.iter().map(ToString::to_string).collect();
                write!(f, "{}", s.join(", "))
            }
        }
    }
}

/// Sort direction for an `ORDER BY` clause — `ASC` or `DESC`.
///
/// ```sql
/// -- ORDER BY age ASC          -->  OrderDirection::Asc
/// -- ORDER BY age DESC         -->  OrderDirection::Desc
/// -- ORDER BY age              -->  OrderDirection::Asc      (default when omitted)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// One sort key in an `ORDER BY` clause: a column plus an optional direction.
///
/// `SelectStatement::order_by` holds a `Vec<OrderBy>`, one entry per
/// comma-separated key in left-to-right priority. An empty vec means the
/// clause was absent.
///
/// # SQL examples
///
/// ```sql
/// -- ORDER BY age
/// --   [OrderBy(ColumnRef { qualifier: None, name: "age" }, OrderDirection::Asc)]
///
/// -- ORDER BY users.id DESC, name
/// --   [OrderBy(users.id, Desc), OrderBy(name, Asc)]
/// ```
#[derive(Debug, Clone)]
pub struct OrderBy(pub ColumnRef, pub OrderDirection);

impl Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

/// The type of join between two tables — controls how unmatched rows are handled.
///
/// ```sql
/// -- a INNER JOIN b ON …      -->  JoinKind::Inner   (drop unmatched on both sides)
/// -- a LEFT  JOIN b ON …      -->  JoinKind::Left    (keep unmatched left rows, NULL-pad right)
/// -- a RIGHT JOIN b ON …      -->  JoinKind::Right   (keep unmatched right rows, NULL-pad left)
/// -- a JOIN b ON …            -->  JoinKind::Inner   (default when omitted)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

impl Display for JoinKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinKind::Inner => write!(f, "INNER JOIN"),
            JoinKind::Left => write!(f, "LEFT JOIN"),
            JoinKind::Right => write!(f, "RIGHT JOIN"),
        }
    }
}

/// A table reference in a `FROM` clause: `<name> [alias]`.
///
/// # SQL examples
///
/// ```sql
/// -- FROM users
/// --   TableRef { name: "users", alias: None }
///
/// -- FROM users u
/// --   TableRef { name: "users", alias: Some("u") }
///
/// -- FROM users AS u                 (the AS keyword is optional in the surface syntax)
/// --   TableRef { name: "users", alias: Some("u") }
/// ```
#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

impl Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(alias) = &self.alias {
            write!(f, " {alias}")?;
        }
        Ok(())
    }
}

/// One entry in a comma-separated `FROM` list — a base table plus a chain of
/// `JOIN`s onto it.
///
/// `FROM a JOIN b ON …, c` is two `TableWithJoins` entries. The first carries
/// `a` and one `Join` for `b`; the second carries `c` with no joins. The
/// comma-separated form is treated by the binder as a cross product (an
/// implicit inner join with no `ON` predicate).
///
/// # SQL examples
///
/// ```sql
/// -- FROM users
/// --   TableWithJoins {
/// --       table: TableRef { name: "users", alias: None },
/// --       joins: vec![],
/// --   }
///
/// -- FROM users u INNER JOIN orders o ON u.id = o.user_id
/// --   TableWithJoins {
/// --       table: TableRef { name: "users", alias: Some("u") },
/// --       joins: vec![ Join { kind: JoinKind::Inner,
/// --                           table: "orders", alias: Some("o"),
/// --                           on: Predicate { u.id = o.user_id } } ],
/// --   }
///
/// -- FROM a JOIN b ON …, c        (becomes two entries in SelectStatement::from)
/// --   [ TableWithJoins { table: a, joins: [ Join(b, …) ] },
/// --     TableWithJoins { table: c, joins: []           } ]
/// ```
#[derive(Debug, Clone)]
pub struct TableWithJoins {
    pub table: TableRef,
    pub joins: Vec<Join>,
}

impl Display for TableWithJoins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table)?;
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        Ok(())
    }
}

/// A parsed join clause: `<kind> JOIN <table> [alias] ON <condition>`.
///
/// `on` is required at parse time — implicit / cross joins are expressed at
/// the [`TableWithJoins`] level (separate `FROM` entries), not via an
/// optional join condition.
///
/// # SQL examples
///
/// ```sql
/// -- INNER JOIN orders o ON users.id = o.user_id
/// --   Join {
/// --       kind:  JoinKind::Inner,
/// --       table: "orders", alias: Some("o"),
/// --       on:    WhereCondition::Predicate {
/// --           field: ColumnRef { qualifier: Some("users"), name: "id" },
/// --           op:    Predicate::Equals,
/// --           value: <bound to o.user_id by the binder>,
/// --       },
/// --   }
///
/// -- LEFT JOIN orders ON users.id = orders.user_id
/// --   Join { kind: JoinKind::Left, table: "orders", alias: None, on: <…> }
/// ```
#[derive(Debug, Clone)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: WhereCondition,
}

impl Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.kind, self.table)?;
        write!(f, " ON {}", self.on)
    }
}

/// `LIMIT [n] [OFFSET m]` — row-count cap and skip applied after ordering.
///
/// `limit = None` means *no upper bound* on rows returned (e.g. `OFFSET 5`
/// alone, or no clause at all if the surrounding `Option<LimitClause>` is
/// `Some`). `limit = Some(0)` means *return zero rows* — distinct from "no
/// limit." `offset` defaults to `0` when the user only wrote `LIMIT`.
///
/// # SQL examples
///
/// ```sql
/// -- LIMIT 10
/// --   LimitClause { limit: Some(10), offset: 0 }
///
/// -- LIMIT 10 OFFSET 20
/// --   LimitClause { limit: Some(10), offset: 20 }
///
/// -- OFFSET 5                   (skip 5 rows, no upper bound)
/// --   LimitClause { limit: None, offset: 5 }
///
/// -- LIMIT 0                    (return nothing — useful for schema probes)
/// --   LimitClause { limit: Some(0), offset: 0 }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitClause {
    pub limit: Option<u64>,
    pub offset: u64,
}

impl Display for LimitClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(n) = self.limit {
            write!(f, "LIMIT {n}")?;
            if self.offset != 0 {
                write!(f, " OFFSET {}", self.offset)?;
            }
        } else {
            write!(f, "OFFSET {}", self.offset)?;
        }
        Ok(())
    }
}

/// A full `SELECT` statement — every supported clause in one struct.
///
/// Clause order mirrors the SQL standard:
/// `SELECT [DISTINCT] … FROM … [alias] [JOIN …] [WHERE …] [GROUP BY …]
///  [HAVING …] [ORDER BY …] [LIMIT n [OFFSET n]]`
///
/// # SQL → struct mapping
///
/// ```sql
/// -- 1. SELECT *
/// --
/// --     SELECT * FROM users;
/// --
/// --     SelectStatement {
/// --         distinct: false,
/// --         columns:  SelectColumns::All,
/// --         from:     vec![ TableWithJoins { table: "users", joins: [] } ],
/// --         where_clause: None, group_by: vec![],
/// --         having: None, order_by: vec![], limit: None,
/// --     }
///
/// -- 2. WHERE + ORDER BY + LIMIT
/// --
/// --     SELECT name FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10;
/// --
/// --     SelectStatement {
/// --         distinct: false,
/// --         columns:  SelectColumns::Exprs(vec![ SelectItem::bare(Column("name")) ]),
/// --         from:     vec![ TableWithJoins { table: "users", joins: [] } ],
/// --         where_clause: Some(Predicate { age, GreaterThanOrEqual, Int32(18) }),
/// --         group_by: vec![],
/// --         having: None,
/// --         order_by: vec![ OrderBy(age, OrderDirection::Desc) ],
/// --         limit: Some(LimitClause { limit: Some(10), offset: 0 }),
/// --     }
///
/// -- 3. GROUP BY with HAVING
/// --
/// --     SELECT user_id, SUM(total) FROM orders
/// --     GROUP BY user_id HAVING SUM(total) > 100;
/// --
/// --     SelectStatement {
/// --         distinct: false,
/// --         columns: SelectColumns::Exprs(vec![
/// --             SelectItem::bare(Column("user_id")),
/// --             SelectItem::bare(Agg(AggFunc::Sum, "total")),
/// --         ]),
/// --         from:    vec![ TableWithJoins { table: "orders", joins: [] } ],
/// --         where_clause: None,
/// --         group_by: vec![ ColumnRef { None, "user_id" } ],
/// --         having: Some(Predicate { /* SUM(total) > 100 — modeled as a Predicate node */ }),
/// --         order_by: vec![], limit: None,
/// --     }
///
/// -- 4. JOIN
/// --
/// --     SELECT u.name, o.total
/// --     FROM   users u INNER JOIN orders o ON u.id = o.user_id;
/// --
/// --     SelectStatement {
/// --         columns: SelectColumns::Exprs(vec![
/// --             SelectItem::bare(Column(ColumnRef { Some("u"), "name"  })),
/// --             SelectItem::bare(Column(ColumnRef { Some("o"), "total" })),
/// --         ]),
/// --         from: vec![
/// --             TableWithJoins {
/// --                 table: TableRef { name: "users", alias: Some("u") },
/// --                 joins: vec![ Join { kind: Inner, table: "orders",
/// --                                     alias: Some("o"), on: <u.id = o.user_id> } ],
/// --             },
/// --         ],
/// --         /* other clauses default */
/// --     }
///
/// -- 5. DISTINCT
/// --
/// --     SELECT DISTINCT age FROM users;
/// --
/// --     SelectStatement { distinct: true, columns: …age…, /* … */ }
/// ```
///
/// # NULL semantics
///
/// `NULL` literals in `WHERE`/`HAVING` predicates collapse to `false` per the
/// executor's two-valued model — see [`crate::execution::expression`]. The
/// AST itself just carries [`Value::Null`].
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: SelectColumns,
    pub from: Vec<TableWithJoins>,
    pub where_clause: Option<WhereCondition>,
    pub group_by: Vec<ColumnRef>,
    pub having: Option<WhereCondition>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<LimitClause>,
}

impl SelectStatement {
    /// Renders the comma-separated `FROM` list. Extracted from `Display` to
    /// keep the main format function readable.
    fn fmt_from(&self) -> String {
        self.from
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Renders the comma-separated `GROUP BY` column list.
    fn fmt_group_by(&self) -> String {
        self.group_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Renders the comma-separated `ORDER BY` key list.
    fn fmt_order_by(&self) -> String {
        self.order_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = if self.distinct {
            "SELECT DISTINCT"
        } else {
            "SELECT"
        };
        write!(f, "{prefix} {} FROM {}", self.columns, self.fmt_from())?;

        if let Some(w) = &self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", self.fmt_group_by())?;
        }
        if let Some(h) = &self.having {
            write!(f, " HAVING {h}")?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", self.fmt_order_by())?;
        }
        if let Some(l) = self.limit {
            write!(f, " {l}")?;
        }
        Ok(())
    }
}
