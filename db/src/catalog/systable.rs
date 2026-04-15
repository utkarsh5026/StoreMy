use std::fmt;

use crate::{
    FileId, Type,
    tuple::{Field, TupleSchema},
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
        match self {
            SystemTable::Tables => TupleSchema::new(vec![
                Field::new("table_id", Type::Int64).not_null(),
                Field::new("table_name", Type::String).not_null(),
                Field::new("file_path", Type::String).not_null(),
                Field::new("primary_key", Type::String),
            ]),
            SystemTable::Columns => TupleSchema::new(vec![
                Field::new("table_id", Type::Int64).not_null(),
                Field::new("column_name", Type::String).not_null(),
                Field::new("column_type", Type::Int32).not_null(),
                Field::new("position", Type::Int32).not_null(),
                Field::new("nullable", Type::Bool),
            ]),

            SystemTable::Indexes => TupleSchema::new(vec![
                Field::new("index_id", Type::Int64).not_null(),
                Field::new("index_name", Type::String).not_null(),
                Field::new("table_id", Type::Int64).not_null(),
                Field::new("column_name", Type::String).not_null(),
                Field::new("index_type", Type::Int32).not_null(),
            ]),
        }
    }
}

impl fmt::Display for SystemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}
