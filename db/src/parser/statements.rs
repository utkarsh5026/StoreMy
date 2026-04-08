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

pub struct CreateTableStatement {}

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

/// A simple WHERE predicate: `field op value`, e.g. `id = 1` or `name = 'Alice'`.
///
/// The field may be unqualified (`id`) or alias-qualified (`u.id`).
#[derive(Debug, Clone, PartialEq)]
pub struct WhereCondition {
    pub field: String,
    pub op: Predicate,
    pub value: Literal,
}

impl Display for WhereCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.field, self.op, self.value)
    }
}

impl WhereCondition {
    pub fn new(field: impl Into<String>, op: Predicate, value: Literal) -> Self {
        Self {
            field: field.into(),
            op,
            value,
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
