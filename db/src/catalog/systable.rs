use std::{fmt, path::PathBuf};

use crate::{
    FileId, Type, Value,
    catalog::{CatalogError, tuple::TupleReader},
    storage::index::Index,
    tuple::{Field, Tuple, TupleSchema},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemTable {
    Tables,
    Columns,
    Indexes,
}

impl SystemTable {
    /// All system tables — iterate for initialization
    pub const ALL: &[SystemTable] = &[
        SystemTable::Tables,
        SystemTable::Columns,
        SystemTable::Indexes,
    ];

    pub const fn file_id(self) -> FileId {
        match self {
            SystemTable::Tables => FileId(1),
            SystemTable::Columns => FileId(2),
            SystemTable::Indexes => FileId(3),
        }
    }

    pub const fn file_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "catalog_tables.dat",
            SystemTable::Columns => "catalog_columns.dat",
            SystemTable::Indexes => "catalog_indexes.dat",
        }
    }

    pub const fn table_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "CATALOG_TABLES",
            SystemTable::Columns => "CATALOG_COLUMNS",
            SystemTable::Indexes => "CATALOG_INDEXES",
        }
    }

    pub fn schema(self) -> TupleSchema {
        use Type::{Bool, Int32, Int64, String, Uint32};

        let field = |name: &'static str, ty| Field::new(name, ty);

        let fields = match self {
            SystemTable::Tables => vec![
                field("table_id", Int64).not_null(),
                field("table_name", String).not_null(),
                field("file_path", String).not_null(),
                field("primary_key", String),
            ],
            SystemTable::Columns => vec![
                field("table_id", Int64).not_null(),
                field("column_name", String).not_null(),
                field("column_type", Uint32).not_null(),
                field("position", Int32).not_null(),
                field("nullable", Bool),
            ],
            SystemTable::Indexes => vec![
                field("index_id", Int64).not_null(),
                field("index_name", String).not_null(),
                field("table_id", Int64).not_null(),
                field("column_name", String).not_null(),
                field("index_type", Int32).not_null(),
            ],
        };

        TupleSchema::new(fields)
    }
}

impl fmt::Display for SystemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}

/// Reads field `$i` from the `t: &Tuple` parameter.
///
/// `read_as!(t, N)` — direct typed read.
/// `read_as!(t, N => Via => Target)` — read as `Via`, then convert to `Target`,
/// mapping the error to `CatalogError`.
macro_rules! read_as {
    ($t:expr, $i:literal) => {
        TupleReader::read($t, $i)?
    };
    ($t:expr, $i:literal => $via:ty => $target:ty) => {
        <$target>::try_from(TupleReader::read::<$via>($t, $i)?)
            .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?
    };
}

pub(super) struct TableRow {
    table_id: i64,
    table_name: String,
    file_path: PathBuf,
    primary_key: Option<String>,
}

impl TryFrom<&Tuple> for TableRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Ok(TableRow {
            table_id: read_as!(t, 0),
            table_name: read_as!(t, 1),
            file_path: PathBuf::from(read_as!(t, 2 => String => String)),
            primary_key: read_as!(t, 3 => String => Option<String>),
        })
    }
}

impl From<&TableRow> for Tuple {
    fn from(r: &TableRow) -> Tuple {
        Tuple::new(vec![
            r.table_id.into(),
            r.table_name.clone().into(),
            r.file_path.to_string_lossy().to_string().into(),
            r.primary_key
                .as_ref()
                .map_or(Value::Null, |s| s.clone().into()),
        ])
    }
}

pub(super) struct ColumnRow {
    table_id: i64,
    column_name: String,
    column_type: Type,
    position: i32,
    nullable: bool,
}

impl TryFrom<&Tuple> for ColumnRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Ok(ColumnRow {
            table_id: read_as!(t, 0),
            column_name: read_as!(t, 1),
            column_type: Type::try_from(read_as!(t, 2 => u32 => u32))
                .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
            position: read_as!(t, 3),
            nullable: read_as!(t, 4),
        })
    }
}

impl From<&ColumnRow> for Tuple {
    fn from(r: &ColumnRow) -> Tuple {
        Tuple::new(vec![
            r.table_id.into(),
            r.column_name.clone().into(),
            u32::from(r.column_type).into(),
            r.position.into(),
            r.nullable.into(),
        ])
    }
}

pub(super) struct IndexRow {
    index_id: i64,
    index_name: String,
    table_id: i64,
    column_name: String,
    index_type: Index,
}

impl TryFrom<&Tuple> for IndexRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Ok(IndexRow {
            index_id: read_as!(t, 0),
            index_name: read_as!(t, 1),
            table_id: read_as!(t, 2),
            column_name: read_as!(t, 3),
            index_type: Index::try_from(read_as!(t, 4 => u32 => u32))
                .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
        })
    }
}

impl From<&IndexRow> for Tuple {
    fn from(r: &IndexRow) -> Tuple {
        Tuple::new(vec![
            r.index_id.into(),
            r.index_name.clone().into(),
            r.table_id.into(),
            r.column_name.clone().into(),
            u32::from(r.index_type).into(),
        ])
    }
}
