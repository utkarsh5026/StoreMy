use super::EngineError;
use crate::{
    FileId,
    catalog::TableInfo,
    execution::ColumnLookup,
    parser::statements::ColumnRef,
    primitives::{ColumnId, NonEmptyString},
    tuple::{Field, TupleSchema},
};

/// One table participating in a multi-table resolution environment.
///
/// Carries everything name resolution needs: the table's own schema, its
/// alias (if any), and the column offset where this table's fields begin
/// in the joined output row. The qualifier label used for `t.col` lookups
/// is the alias when present, otherwise the table name.
pub(super) struct BoundTable {
    pub name: String,
    pub alias: Option<String>,
    pub schema: TupleSchema,
    pub column_offset: usize,
}

impl BoundTable {
    pub fn new(
        name: String,
        alias: Option<String>,
        schema: TupleSchema,
        column_offset: usize,
    ) -> Self {
        Self {
            name,
            alias,
            schema,
            column_offset,
        }
    }

    fn qualifier_label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Shared name-resolution behavior for both [`Scope`] (multi-table) and
/// [`SingleTableScope`] (DML).
///
/// Used when resolving column references in `SELECT`, `GROUP BY`, `ORDER BY`,
/// and join-equi extraction. WHERE/HAVING/ON predicates are now passed as raw
/// [`Expr`] and evaluated lazily via `eval_expr`.
pub(super) trait ColumnResolver {
    /// Resolves a `ColumnRef` against the in-scope tables.
    ///
    /// Returns `(global_column_index, field, owning_table_name)`.
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError>;
}

/// Multi-table name-resolution environment used while binding `SELECT`.
///
/// Built up incrementally as the binder walks the `FROM` clause: each
/// table (or join right-hand side) is `push`ed in, after which predicates
/// and expressions can be resolved against the accumulated set.
pub(super) struct Scope {
    tables: Vec<BoundTable>,
}

impl Scope {
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn push(&mut self, table: BoundTable) {
        self.tables.push(table);
    }
}

impl ColumnResolver for Scope {
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError> {
        let candidates: Vec<&BoundTable> = match &col.qualifier {
            Some(q) => self
                .tables
                .iter()
                .filter(|t| t.qualifier_label() == q)
                .collect(),
            None => self.tables.iter().collect(),
        };

        if let Some(q) = &col.qualifier {
            if candidates.is_empty() {
                return Err(EngineError::UnknownColumn {
                    table: q.to_string(),
                    column: col.name.to_string(),
                });
            }
            if candidates.len() > 1 {
                return Err(EngineError::AmbiguousColumn {
                    column: q.to_string(),
                });
            }
        }

        let matches: Vec<(usize, &Field, &str)> = candidates
            .iter()
            .filter_map(|t| {
                t.schema.field_by_name(&col.name).map(|(local, fld)| {
                    (t.column_offset + usize::from(local), fld, t.name.as_str())
                })
            })
            .collect();

        match matches.len() {
            0 => {
                let table = col
                    .qualifier
                    .as_ref()
                    .map(ToString::to_string)
                    .or_else(|| self.tables.first().map(|t| t.name.clone()))
                    .unwrap_or_default();

                Err(EngineError::UnknownColumn {
                    table,
                    column: col.name.to_string(),
                })
            }
            1 => Ok(matches[0]),
            _ => Err(EngineError::AmbiguousColumn {
                column: col.name.to_string(),
            }),
        }
    }
}

/// Single-table convenience used by DML (`INSERT`/`UPDATE`/`DELETE`).
///
/// DML's write target is exactly one table, so the qualifier
/// disambiguation a multi-table [`Scope`] needs is dead weight here.
pub(super) struct SingleTableScope {
    pub name: NonEmptyString,
    pub alias: Option<NonEmptyString>,
    pub file_id: FileId,
    pub schema: TupleSchema,
}

impl SingleTableScope {
    pub fn from_info(info: TableInfo, alias: Option<NonEmptyString>) -> Self {
        Self {
            name: info.name,
            alias,
            file_id: info.file_id,
            schema: info.schema,
        }
    }

    fn qualifier_label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

impl ColumnLookup for Scope {
    fn lookup(&self, qualifier: Option<&str>, name: &str) -> Option<ColumnId> {
        let candidates: Vec<&BoundTable> = match qualifier {
            Some(q) => self
                .tables
                .iter()
                .filter(|t| t.qualifier_label() == q)
                .collect(),
            None => self.tables.iter().collect(),
        };

        let matches: Vec<usize> = candidates
            .iter()
            .filter_map(|t| {
                t.schema
                    .field_by_name(name)
                    .map(|(local, _)| t.column_offset + usize::from(local))
            })
            .collect();

        match matches.len() {
            1 => ColumnId::try_from(matches[0]).ok(),
            _ => None,
        }
    }
}

impl ColumnLookup for SingleTableScope {
    fn lookup(&self, qualifier: Option<&str>, name: &str) -> Option<ColumnId> {
        if let Some(q) = qualifier
            && q != self.qualifier_label()
        {
            return None;
        }
        self.schema.field_by_name(name).map(|(id, _)| id)
    }
}

impl ColumnResolver for SingleTableScope {
    fn resolve<'a>(&'a self, col: &ColumnRef) -> Result<(usize, &'a Field, &'a str), EngineError> {
        let ColumnRef {
            name: col_name,
            qualifier,
        } = col;
        if let Some(q) = qualifier
            && q != self.qualifier_label()
        {
            return Err(EngineError::UnknownColumn {
                table: q.to_string(),
                column: col_name.to_string(),
            });
        }

        let (local, fld) =
            self.schema
                .field_by_name(col_name)
                .ok_or_else(|| EngineError::UnknownColumn {
                    table: self.name.to_string(),
                    column: col_name.to_string(),
                })?;

        Ok((usize::from(local), fld, self.name.as_str()))
    }
}
