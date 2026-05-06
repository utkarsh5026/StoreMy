//! Query/result DTOs — the JSON shapes for `POST /api/query` and the
//! catalog endpoints. Heap-page introspection lives in the sibling
//! [`crate::web::dto::heap`] module.

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::{
    catalog::manager::TableInfo,
    engine::{ShownIndex, StatementResult},
    tuple::{Field, Tuple, TupleSchema},
    types::Value,
};

/// One column header in a result set or table description.
#[derive(Debug, Serialize)]
pub struct ColumnDto {
    pub name: String,
    /// SQL type name as rendered by `Display for Type` (e.g. `"INT"`, `"VARCHAR"`).
    pub r#type: String,
    pub nullable: bool,
}

impl From<&Field> for ColumnDto {
    fn from(f: &Field) -> Self {
        ColumnDto {
            name: f.name.clone(),
            r#type: f.field_type.to_string(),
            nullable: f.nullable,
        }
    }
}

/// Convert a [`TupleSchema`] into a list of column DTOs in declaration order.
pub fn columns_from_schema(schema: &TupleSchema) -> Vec<ColumnDto> {
    schema.fields().map(ColumnDto::from).collect()
}

/// Convert a single [`Value`] into a JSON value for transmission.
///
/// Mappings:
/// - `Null` → `null`
/// - integer variants → JSON number (or string if the magnitude doesn't fit a JS-safe number — see
///   below)
/// - `Float64` → JSON number (or `null` for NaN/Inf, since JSON has no representation for those)
/// - `String` → JSON string
/// - `Bool` → JSON bool
///
/// JS numbers are IEEE-754 doubles, so values outside ±2^53 lose precision.
/// For 64-bit integers we encode as a string when they exceed that range, so
/// the frontend doesn't silently truncate. Smaller ints go through as numbers
/// because they're safe and ergonomic.
pub fn value_to_json(v: &Value) -> JsonValue {
    const JS_SAFE_MAX: i64 = 1i64 << 53;
    const JS_SAFE_MIN: i64 = -(1i64 << 53);
    match v {
        Value::Null => JsonValue::Null,
        Value::Int32(n) => JsonValue::from(*n),
        Value::Int64(n) => {
            if (JS_SAFE_MIN..=JS_SAFE_MAX).contains(n) {
                JsonValue::from(*n)
            } else {
                JsonValue::String(n.to_string())
            }
        }
        Value::Uint32(n) => JsonValue::from(*n),
        Value::Uint64(n) => {
            if *n <= (JS_SAFE_MAX as u64) {
                JsonValue::from(*n)
            } else {
                JsonValue::String(n.to_string())
            }
        }
        Value::Float64(f) => {
            if f.is_finite() {
                serde_json::Number::from_f64(*f).map_or(JsonValue::Null, JsonValue::Number)
            } else {
                JsonValue::Null
            }
        }
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Bool(b) => JsonValue::Bool(*b),
    }
}

/// JSON form of a SELECT result.
#[derive(Debug, Serialize)]
pub struct QueryRowsDto {
    pub table: String,
    pub columns: Vec<ColumnDto>,
    pub rows: Vec<Vec<JsonValue>>,
}

impl QueryRowsDto {
    fn from_selected(table: &str, schema: &TupleSchema, rows: &[Tuple]) -> Self {
        let columns = columns_from_schema(schema);
        let rows = rows
            .iter()
            .map(|t| {
                (0..t.len())
                    .map(|i| t.get(i).map_or(JsonValue::Null, value_to_json))
                    .collect::<Vec<_>>()
            })
            .collect();
        Self {
            table: table.to_string(),
            columns,
            rows,
        }
    }
}

/// JSON form of a `SHOW INDEXES` row.
#[derive(Debug, Serialize)]
pub struct ShownIndexDto {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub kind: String,
}

impl From<&ShownIndex> for ShownIndexDto {
    fn from(s: &ShownIndex) -> Self {
        ShownIndexDto {
            name: s.name.clone(),
            table: s.table.clone(),
            columns: s.columns.clone(),
            kind: format!("{:?}", s.kind),
        }
    }
}

/// Top-level response of `POST /api/query`.
///
/// The shape is a tagged union: `kind` selects which sibling field is
/// populated. This is friendlier for the React side than a Rust-style enum
/// with internal tagging, while still being unambiguous.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryResultDto {
    TableCreated {
        name: String,
        file_id: u64,
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
        scope: Option<String>,
        rows: Vec<ShownIndexDto>,
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
    Selected(QueryRowsDto),
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
}

impl From<&StatementResult> for QueryResultDto {
    fn from(r: &StatementResult) -> Self {
        match r {
            StatementResult::TableCreated {
                name,
                file_id,
                already_exists,
            } => QueryResultDto::TableCreated {
                name: name.clone(),
                file_id: u64::from(*file_id),
                already_exists: *already_exists,
            },
            StatementResult::TableDropped { name } => {
                QueryResultDto::TableDropped { name: name.clone() }
            }
            StatementResult::IndexCreated {
                name,
                table,
                already_exists,
            } => QueryResultDto::IndexCreated {
                name: name.clone(),
                table: table.clone(),
                already_exists: *already_exists,
            },
            StatementResult::IndexDropped { name } => {
                QueryResultDto::IndexDropped { name: name.clone() }
            }
            StatementResult::IndexesShown { scope, rows } => QueryResultDto::IndexesShown {
                scope: scope.clone(),
                rows: rows.iter().map(ShownIndexDto::from).collect(),
            },
            StatementResult::Inserted { table, rows } => QueryResultDto::Inserted {
                table: table.clone(),
                rows: *rows,
            },
            StatementResult::Deleted { table, rows } => QueryResultDto::Deleted {
                table: table.clone(),
                rows: *rows,
            },
            StatementResult::Updated { table, rows } => QueryResultDto::Updated {
                table: table.clone(),
                rows: *rows,
            },
            StatementResult::Selected {
                table,
                schema,
                rows,
            } => QueryResultDto::Selected(QueryRowsDto::from_selected(table, schema, rows)),
            StatementResult::ColumnRenamed {
                table,
                old_name,
                new_name,
            } => QueryResultDto::ColumnRenamed {
                table: table.clone(),
                old_name: old_name.clone(),
                new_name: new_name.clone(),
            },
            StatementResult::ColumnAdded { table, column_name } => QueryResultDto::ColumnAdded {
                table: table.clone(),
                column_name: column_name.clone(),
            },
            StatementResult::ColumnDropped { table, column_name } => {
                QueryResultDto::ColumnDropped {
                    table: table.clone(),
                    column_name: column_name.clone(),
                }
            }
            StatementResult::TableRenamed { old_name, new_name } => QueryResultDto::TableRenamed {
                old_name: old_name.clone(),
                new_name: new_name.clone(),
            },
        }
    }
}

/// One entry in `GET /api/tables`.
#[derive(Debug, Serialize)]
pub struct TableSummaryDto {
    pub name: String,
    pub column_count: usize,
    pub file_id: u64,
}

impl From<&TableInfo> for TableSummaryDto {
    fn from(t: &TableInfo) -> Self {
        TableSummaryDto {
            name: t.name.clone(),
            column_count: t.schema.num_fields(),
            file_id: u64::from(t.file_id),
        }
    }
}

/// Full table description for `GET /api/tables/{name}`.
#[derive(Debug, Serialize)]
pub struct TableInfoDto {
    pub name: String,
    pub file_id: u64,
    pub file_path: String,
    pub primary_key: Option<Vec<String>>,
    pub columns: Vec<ColumnDto>,
}

impl From<&TableInfo> for TableInfoDto {
    fn from(t: &TableInfo) -> Self {
        let columns = columns_from_schema(&t.schema);
        let primary_key = t.primary_key.as_ref().map(|cols| {
            cols.iter()
                .filter_map(|c| t.schema.col_name(*c).map(str::to_string))
                .collect::<Vec<_>>()
        });
        TableInfoDto {
            name: t.name.clone(),
            file_id: u64::from(t.file_id),
            file_path: t.file_path.display().to_string(),
            primary_key,
            columns,
        }
    }
}
