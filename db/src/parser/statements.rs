use std::fmt::Display;

use crate::{primitives::Predicate, storage::index::Index};

pub enum Statement {
    Drop(DropStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    Delete(DeleteStatement),
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
