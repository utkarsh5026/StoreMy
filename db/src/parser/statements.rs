use std::fmt::Display;

pub enum Statement {
    Drop(DropStatement),
}

impl Statement {
    pub(super) fn drop(table_name: impl Into<String>, if_exists: bool) -> Self {
        Self::Drop(DropStatement {
            table_name: table_name.into(),
            if_exists,
        })
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
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.table_name)
    }
}
