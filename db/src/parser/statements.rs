use std::fmt::Display;

use crate::{primitives::Predicate, storage::index::Index, types::Value};

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

#[derive(Debug, Clone)]
pub struct DropStatement {
    table_name: String,
    if_exists: bool,
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
    pub name: String,
    pub col_type: crate::types::Type,
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

/// Represents `CREATE TABLE [IF NOT EXISTS] name (col_def, ..., [PRIMARY KEY (col)])`.
#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
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

pub struct CreateIndexStatement {
    index_name: String,
    col_name: String,
    index_type: Index,
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

#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    table_name: String,
    index_name: String,
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

/// The right-hand side value in a WHERE condition.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
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

/// A WHERE condition, either a single predicate or a logical combination.
///
/// AND binds tighter than OR, matching standard SQL precedence.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereCondition {
    /// A single predicate: `field op value`, e.g. `id = 1` or `name = 'Alice'`.
    Predicate {
        field: String,
        op: Predicate,
        value: Literal,
    },
    /// `left AND right`
    And(Box<WhereCondition>, Box<WhereCondition>),
    /// `left OR right`
    Or(Box<WhereCondition>, Box<WhereCondition>),
}

impl WhereCondition {
    pub fn predicate(field: impl Into<String>, op: Predicate, value: Literal) -> Self {
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

/// Represents `INSERT INTO table (col, ...) VALUES (val, ...), (val, ...)`.
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

/// A single `col = val` pair in an `UPDATE SET` clause.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

impl Display for Assignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.column, self.value)
    }
}

/// Represents `UPDATE table SET col = val, ... WHERE condition`.
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
        let sets: Vec<String> = self.assignments.iter().map(ToString::to_string).collect();
        write!(f, " SET {}", sets.join(", "))?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

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

/// An aggregate function used in a SELECT expression.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
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

/// A single expression in a SELECT list: a plain column, an aggregate call, or `COUNT(*)`.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectExpr {
    /// A plain column reference, e.g. `name`.
    Column(String),
    /// An aggregate call, e.g. `SUM(amount)` or `COUNT(id)`.
    Agg(AggFunc, String),
    /// The special `COUNT(*)` form.
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

/// Which columns to project in a SELECT.
#[derive(Debug, Clone)]
pub enum SelectColumns {
    /// `SELECT *`
    All,
    /// `SELECT expr1, expr2, ...`
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

/// A single `ORDER BY col ASC/DESC` clause.
#[derive(Debug, Clone)]
pub struct OrderBy(pub String, pub OrderDirection);

impl Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

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

#[derive(Debug, Clone)]
pub struct Join {
    pub kind: JoinKind,
    pub table: String,
    pub alias: Option<String>,
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

/// Represents a full SELECT statement.
///
/// Clause order mirrors SQL standard:
/// `SELECT [DISTINCT] … FROM … [alias] [JOIN …] [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT n [OFFSET n]]`
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: SelectColumns,
    pub table_name: String,
    pub alias: Option<String>,
    pub joins: Vec<Join>,
    pub where_clause: Option<WhereCondition>,
    pub group_by: Vec<String>,
    pub having: Option<WhereCondition>,
    pub order_by: Option<OrderBy>,
    pub limit_offset: Option<(u64, u64)>,
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.distinct {
            write!(f, "SELECT DISTINCT {} FROM {}", self.columns, self.table_name)?;
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

/// Represents `SHOW INDEXES [FROM table]`.
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
