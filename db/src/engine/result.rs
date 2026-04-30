use std::fmt;

use crate::{FileId, index::IndexKind, tuple::Tuple};

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
        rows: Vec<Tuple>,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            StatementResult::Selected { table, rows } => {
                let row_word = if rows.len() == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "SELECT completed: returned {} {row_word} from '{table}'",
                    rows.len()
                )
            }
        }
    }
}
