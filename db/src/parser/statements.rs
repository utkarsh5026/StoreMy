//! SQL statement AST types produced by the parser.
//!
//! Each variant of [`Statement`] maps to one SQL command (e.g. `CREATE TABLE`,
//! `SELECT`, `INSERT`). The parser converts raw tokens into these structs, and
//! downstream execution code pattern-matches on them to drive query planning
//! and execution.
//!
//! Every statement type implements [`Display`] so it can be pretty-printed back
//! into a SQL-like string, which is useful for logging and debugging.

use std::fmt::Display;

use crate::{
    primitives::Predicate,
    storage::index::Index,
    tuple::{Field, TupleSchema},
    types::Value,
};

/// Top-level SQL statement returned by the parser.
///
/// Each variant wraps a dedicated struct that holds the parsed clause data.
/// Factory methods on `Statement` are `pub(super)` so only the parser module
/// can construct them, keeping AST creation private.
pub enum Statement {
    Drop(DropStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
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
///     types::Type,
/// };
///
/// let col = ColumnDef {
///     name: "id".into(),
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
///     primary_key: None,
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
            Statement::Delete(statement) => write!(f, "{statement}"),
            Statement::Insert(statement) => write!(f, "{statement}"),
            Statement::Update(statement) => write!(f, "{statement}"),
            Statement::Select(statement) => write!(f, "{statement}"),
            Statement::ShowIndexes(statement) => write!(f, "{statement}"),
        }
    }
}

/// Builds a statement from its components.
impl Statement {
    pub(super) fn drop(table_name: impl Into<String>, if_exists: bool) -> DropStatement {
        DropStatement {
            table_name: table_name.into(),
            if_exists,
        }
    }

    pub(super) fn create_index(
        index_name: impl Into<String>,
        col_name: impl Into<String>,
        index_type: Index,
        if_not_exists: bool,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: index_name.into(),
            col_name: col_name.into(),
            index_type,
            if_not_exists,
        }
    }

    pub(super) fn drop_index(
        table_name: impl Into<String>,
        index_name: impl Into<String>,
        if_exists: bool,
    ) -> DropIndexStatement {
        DropIndexStatement {
            table_name: table_name.into(),
            index_name: index_name.into(),
            if_exists,
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
        values: Vec<Vec<Value>>,
    ) -> InsertStatement {
        InsertStatement {
            table_name: table_name.into(),
            columns,
            values,
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

    pub(super) fn create_table(
        table_name: impl Into<String>,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        primary_key: Option<String>,
    ) -> CreateTableStatement {
        CreateTableStatement {
            table_name: table_name.into(),
            if_not_exists,
            columns,
            primary_key,
        }
    }

    pub(super) fn show_indexes(table_name: Option<String>) -> ShowIndexesStatement {
        ShowIndexesStatement(table_name)
    }
}

/// Parsed `DROP TABLE [IF EXISTS] <name>` statement.
#[derive(Debug, Clone)]
pub struct DropStatement {
    /// Name of the table to drop.
    pub table_name: String,
    /// When `true`, the statement includes `IF EXISTS` and should not error
    /// if the table is missing.
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
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Data type of the column (e.g. `INT`, `VARCHAR(255)`).
    pub col_type: crate::types::Type,
    /// Whether the column accepts `NULL` values.
    pub nullable: bool,
    /// Whether this column is declared as `PRIMARY KEY` inline.
    pub primary_key: bool,
    /// Whether the column auto-increments on insert.
    pub auto_increment: bool,
    /// Optional default value used when no explicit value is provided.
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
                    let mut field = Field::new(col.name.clone(), col.col_type);
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
#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    /// Name of the table to create.
    pub table_name: String,
    /// When `true`, suppress errors if the table already exists.
    pub if_not_exists: bool,
    /// Column definitions in declaration order.
    pub columns: Vec<ColumnDef>,
    /// Optional table-level primary key column name (as opposed to an inline
    /// `PRIMARY KEY` on a [`ColumnDef`]).
    pub primary_key: Option<String>,
}

impl Display for CreateTableStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} (", self.table_name)?;
        let cols: Vec<String> = self.columns.iter().map(ToString::to_string).collect();
        write!(f, "{}", cols.join(", "))?;
        if let Some(pk) = &self.primary_key {
            write!(f, ", PRIMARY KEY ({pk})")?;
        }
        write!(f, ")")
    }
}

/// Parsed `CREATE INDEX [IF NOT EXISTS] <name> ON (<col>) USING <type>`.
#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    /// Name of the index being created.
    index_name: String,
    /// Column the index covers.
    col_name: String,
    /// Index structure — hash or B-tree.
    index_type: Index,
    /// When `true`, suppress errors if the index already exists.
    if_not_exists: bool,
}

impl Display for CreateIndexStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let index_type = match self.index_type {
            Index::Hash => "HASH",
            Index::Btree => "BTREE",
        };
        write!(f, "CREATE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(
            f,
            " INDEX {} ON ({}) USING {}",
            self.index_name, self.col_name, index_type
        )
    }
}

/// Parsed `DROP INDEX [IF EXISTS] <index> ON <table>`.
#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    /// Table the index belongs to.
    table_name: String,
    /// Name of the index to drop.
    index_name: String,
    /// When `true`, suppress errors if the index does not exist.
    if_exists: bool,
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

/// A literal value on the right-hand side of a `WHERE` predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// An integer literal, e.g. `42`.
    Int(i64),
    /// A string literal, e.g. `'hello'`.
    Str(String),
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Int(n) => write!(f, "{n}"),
            Literal::Str(s) => write!(f, "'{s}'"),
        }
    }
}

/// A `WHERE` condition tree.
///
/// Conditions are either a single predicate (`field op value`) or a logical
/// combination via `AND` / `OR`. `AND` binds tighter than `OR`, matching
/// standard SQL precedence.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereCondition {
    Predicate {
        field: String,
        op: Predicate,
        value: Value,
    },
    And(Box<WhereCondition>, Box<WhereCondition>),
    Or(Box<WhereCondition>, Box<WhereCondition>),
}

impl WhereCondition {
    /// Create a leaf predicate node.
    pub fn predicate(field: impl Into<String>, op: Predicate, value: Value) -> Self {
        Self::Predicate {
            field: field.into(),
            op,
            value,
        }
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

/// Parsed `INSERT INTO table [(col, …)] VALUES (val, …), …`.
#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
}

impl Display for InsertStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSERT INTO {}", self.table_name)?;
        if let Some(cols) = &self.columns {
            write!(f, " ({})", cols.join(", "))?;
        }

        let rows: Vec<String> = self
            .values
            .iter()
            .map(|row| {
                let vals: Vec<String> = row.iter().map(ToString::to_string).collect();
                format!("({})", vals.join(", "))
            })
            .collect();
        write!(f, " VALUES {}", rows.join(", "))
    }
}

/// A single `col = val` pair in an `UPDATE … SET` clause.
#[derive(Debug, Clone)]
pub struct Assignment {
    /// Column being assigned to.
    pub column: String,
    /// New value for the column.
    pub value: Value,
}

impl Display for Assignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.column, self.value)
    }
}

/// Parsed `UPDATE table [alias] SET col = val, … [WHERE …]`.
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    /// Target table name.
    pub table_name: String,
    /// Optional table alias (e.g. `UPDATE users u SET …`).
    pub alias: Option<String>,
    /// One or more column assignments.
    pub assignments: Vec<Assignment>,
    /// Optional `WHERE` filter; when `None`, all rows are updated.
    pub where_clause: Option<WhereCondition>,
}

impl Display for UpdateStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {}", self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        let sets: Vec<String> = self.assignments.iter().map(ToString::to_string).collect();
        write!(f, " SET {}", sets.join(", "))?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

/// Parsed `DELETE FROM table [alias] [WHERE …]`.
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    /// Target table name.
    pub table_name: String,
    /// Optional table alias.
    pub alias: Option<String>,
    /// Optional `WHERE` filter; when `None`, all rows are deleted.
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

/// An SQL aggregate function used in a `SELECT` expression.
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

/// A single expression in a `SELECT` list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectExpr {
    /// A plain column reference, e.g. `name`.
    Column(String),
    /// An aggregate call on a named column, e.g. `SUM(amount)` or `COUNT(id)`.
    Agg(AggFunc, String),
    /// The special `COUNT(*)` form that counts all rows.
    CountStar,
}

impl Display for SelectExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectExpr::Column(col) => write!(f, "{col}"),
            SelectExpr::Agg(func, col) => write!(f, "{func}({col})"),
            SelectExpr::CountStar => write!(f, "COUNT(*)"),
        }
    }
}

/// Which columns to project in a `SELECT`.
#[derive(Debug, Clone)]
pub enum SelectColumns {
    /// `SELECT *` — all columns from the source table(s).
    All,
    /// `SELECT expr1, expr2, …` — an explicit list of expressions.
    Exprs(Vec<SelectExpr>),
}

impl Display for SelectColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectColumns::All => write!(f, "*"),
            SelectColumns::Exprs(exprs) => {
                let s: Vec<String> = exprs.iter().map(ToString::to_string).collect();
                write!(f, "{}", s.join(", "))
            }
        }
    }
}

/// Sort direction for an `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    /// Ascending order (smallest first).
    Asc,
    /// Descending order (largest first).
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

/// A single `ORDER BY <column> <direction>` clause.
#[derive(Debug, Clone)]
pub struct OrderBy(pub String, pub OrderDirection);

impl Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

/// The type of join between two tables.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinKind {
    /// `INNER JOIN` — only matching rows from both sides.
    Inner,
    /// `LEFT JOIN` — all rows from the left table, with `NULL` fill for
    /// non-matching right rows.
    Left,
    /// `RIGHT JOIN` — all rows from the right table, with `NULL` fill for
    /// non-matching left rows.
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

/// A parsed join clause: `<kind> JOIN <table> [alias] ON <condition>`.
#[derive(Debug, Clone)]
pub struct Join {
    /// Type of join (inner, left, right).
    pub kind: JoinKind,
    /// Name of the table being joined.
    pub table: String,
    /// Optional alias for the joined table.
    pub alias: Option<String>,
    /// Join condition (the `ON` clause).
    pub on: WhereCondition,
}

impl Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.kind, self.table)?;
        if let Some(alias) = &self.alias {
            write!(f, " {alias}")?;
        }
        write!(f, " ON {}", self.on)
    }
}

/// A full `SELECT` statement.
///
/// Clause order mirrors the SQL standard:
/// `SELECT [DISTINCT] … FROM … [alias] [JOIN …] [WHERE …] [GROUP BY …]
///  [HAVING …] [ORDER BY …] [LIMIT n [OFFSET n]]`
#[derive(Debug, Clone)]
pub struct SelectStatement {
    /// Whether `DISTINCT` was specified.
    pub distinct: bool,
    /// The projection list (`*` or explicit expressions).
    pub columns: SelectColumns,
    /// Primary table in the `FROM` clause.
    pub table_name: String,
    /// Optional alias for the primary table.
    pub alias: Option<String>,
    /// Zero or more `JOIN` clauses.
    pub joins: Vec<Join>,
    /// Optional `WHERE` filter.
    pub where_clause: Option<WhereCondition>,
    /// Columns listed in `GROUP BY`, if any.
    pub group_by: Vec<String>,
    /// Optional `HAVING` filter applied after grouping.
    pub having: Option<WhereCondition>,
    /// Optional `ORDER BY` clause (column + direction).
    pub order_by: Option<OrderBy>,
    /// Optional `LIMIT` and `OFFSET` as `(limit, offset)`.
    pub limit_offset: Option<(u64, u64)>,
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.distinct {
            write!(
                f,
                "SELECT DISTINCT {} FROM {}",
                self.columns, self.table_name
            )?;
        } else {
            write!(f, "SELECT {} FROM {}", self.columns, self.table_name)?;
        }
        if let Some(alias) = &self.alias {
            write!(f, " {alias}")?;
        }
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", self.group_by.join(", "))?;
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        if let Some(order_by) = &self.order_by {
            write!(f, " ORDER BY {order_by}")?;
        }
        if let Some((limit, offset)) = self.limit_offset {
            write!(f, " LIMIT {limit} OFFSET {offset}")?;
        }
        Ok(())
    }
}

/// Parsed `SHOW INDEXES [FROM <table>]`.
///
/// The inner `Option<String>` holds the table name when `FROM` is specified;
/// `None` means show indexes for all tables.
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
