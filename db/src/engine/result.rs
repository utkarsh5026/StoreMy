use std::fmt;

use crate::{
    FileId,
    index::IndexKind,
    tuple::{Tuple, TupleSchema},
};

/// One row in the result of `SHOW INDEXES`.
#[derive(Debug, Clone)]
pub struct ShownIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub kind: IndexKind,
}

#[derive(Debug)]
pub enum StatementResult {
    /// Successful statement that intentionally performed no changes (typically due to `IF EXISTS`).
    NoOp {
        statement: String,
    },
    TableCreated {
        name: String,
        file_id: FileId,
        already_exists: bool,
    },
    TableDropped {
        name: String,
    },
    IndexCreated {
        name: String,
        table: String,
        already_exists: bool,
    },
    IndexDropped {
        name: String,
    },
    IndexesShown {
        /// `Some(name)` when the user said `SHOW INDEXES FROM <name>`,
        /// `None` for the unfiltered form. Used only for the human-readable
        /// status line.
        scope: Option<String>,
        rows: Vec<ShownIndex>,
    },
    Inserted {
        table: String,
        rows: usize,
    },
    Deleted {
        table: String,
        rows: usize,
    },
    Updated {
        table: String,
        rows: usize,
    },
    Selected {
        table: String,
        schema: TupleSchema,
        rows: Vec<Tuple>,
    },
    ColumnRenamed {
        table: String,
        old_name: String,
        new_name: String,
    },
    ColumnAdded {
        table: String,
        column_name: String,
    },
    ColumnDropped {
        table: String,
        column_name: String,
    },
    TableRenamed {
        old_name: String,
        new_name: String,
    },
    ColumnDefaultSet {
        table: String,
        column: String,
    },
    ColumnDefaultDropped {
        table: String,
        column: String,
    },
    ColumnNotNullDropped {
        table: String,
        column: String,
    },
    PrimaryKeySet {
        table: String,
    },
    PrimaryKeyDropped {
        table: String,
    },
}

impl StatementResult {
    pub(super) fn table_created(
        name: impl Into<String>,
        file_id: FileId,
        already_exists: bool,
    ) -> Self {
        Self::TableCreated {
            name: name.into(),
            file_id,
            already_exists,
        }
    }

    pub(super) fn table_dropped(name: impl Into<String>) -> Self {
        Self::TableDropped { name: name.into() }
    }

    pub(super) fn index_created(
        name: impl Into<String>,
        table: impl Into<String>,
        already_exists: bool,
    ) -> Self {
        Self::IndexCreated {
            name: name.into(),
            table: table.into(),
            already_exists,
        }
    }

    pub(super) fn index_dropped(name: impl Into<String>) -> Self {
        Self::IndexDropped { name: name.into() }
    }

    pub(super) fn indexes_shown(scope: Option<String>, rows: Vec<ShownIndex>) -> Self {
        Self::IndexesShown { scope, rows }
    }

    pub(super) fn inserted(table: impl Into<String>, rows: usize) -> Self {
        Self::Inserted {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn deleted(table: impl Into<String>, rows: usize) -> Self {
        Self::Deleted {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn updated(table: impl Into<String>, rows: usize) -> Self {
        Self::Updated {
            table: table.into(),
            rows,
        }
    }
}

impl fmt::Display for StatementResult {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::NoOp { statement } => write!(f, "{statement} completed: no-op"),
            StatementResult::TableCreated {
                name,
                file_id,
                already_exists,
            } => {
                if *already_exists {
                    write!(
                        f,
                        "CREATE TABLE completed: table '{name}' already exists (IF NOT EXISTS); using existing file {file_id}"
                    )
                } else {
                    write!(
                        f,
                        "CREATE TABLE completed: registered '{name}' in the catalog with backing file {file_id}"
                    )
                }
            }
            StatementResult::TableDropped { name } => write!(
                f,
                "DROP TABLE completed: removed '{name}' from the catalog and released its heap file"
            ),
            StatementResult::IndexCreated {
                name,
                table,
                already_exists,
            } => {
                if *already_exists {
                    write!(
                        f,
                        "CREATE INDEX completed: index '{name}' on '{table}' already exists (IF NOT EXISTS)"
                    )
                } else {
                    write!(
                        f,
                        "CREATE INDEX completed: registered '{name}' on '{table}' in the catalog"
                    )
                }
            }
            StatementResult::IndexDropped { name } => write!(
                f,
                "DROP INDEX completed: removed '{name}' from the catalog and released its file"
            ),
            StatementResult::IndexesShown { scope, rows } => {
                let row_word = if rows.len() == 1 { "index" } else { "indexes" };
                match scope {
                    Some(t) => write!(
                        f,
                        "SHOW INDEXES completed: returned {} {row_word} for table '{t}'",
                        rows.len()
                    ),
                    None => write!(
                        f,
                        "SHOW INDEXES completed: returned {} {row_word} across all tables",
                        rows.len()
                    ),
                }
            }
            StatementResult::Inserted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "INSERT completed: wrote {rows} {row_word} into heap table '{table}'",
                )
            }
            StatementResult::Deleted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "DELETE completed: removed {rows} {row_word} from heap table '{table}'",
                )
            }
            StatementResult::Updated { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "UPDATE completed: modified {rows} {row_word} in heap table '{table}'",
                )
            }
            StatementResult::Selected {
                table,
                schema,
                rows,
            } => {
                let row_word = if rows.len() == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "SELECT completed: returned {} {row_word} from '{table}' with columns {schema:?}",
                    rows.len()
                )
            }
            StatementResult::ColumnRenamed {
                table,
                old_name,
                new_name,
            } => {
                write!(
                    f,
                    "ALTER COLUMN completed: renamed column '{old_name}' to '{new_name}' in table '{table}'"
                )
            }
            StatementResult::ColumnAdded { table, column_name } => {
                write!(
                    f,
                    "ALTER TABLE completed: added column '{column_name}' to table '{table}'"
                )
            }
            StatementResult::ColumnDropped { table, column_name } => {
                write!(
                    f,
                    "ALTER TABLE completed: dropped column '{column_name}' from table '{table}'"
                )
            }
            StatementResult::TableRenamed { old_name, new_name } => {
                write!(
                    f,
                    "RENAME TABLE completed: renamed table '{old_name}' to '{new_name}'"
                )
            }
            StatementResult::ColumnDefaultSet { table, column } => {
                write!(
                    f,
                    "ALTER TABLE completed: set default for column '{column}' in table '{table}'"
                )
            }
            StatementResult::ColumnDefaultDropped { table, column } => {
                write!(
                    f,
                    "ALTER TABLE completed: dropped default for column '{column}' in table '{table}'"
                )
            }
            StatementResult::ColumnNotNullDropped { table, column } => {
                write!(
                    f,
                    "ALTER TABLE completed: dropped NOT NULL constraint on column '{column}' in table '{table}'"
                )
            }
            StatementResult::PrimaryKeySet { table } => {
                write!(
                    f,
                    "ALTER TABLE completed: set primary key on table '{table}'"
                )
            }
            StatementResult::PrimaryKeyDropped { table } => {
                write!(
                    f,
                    "ALTER TABLE completed: dropped primary key from table '{table}'"
                )
            }
        }
    }
}
