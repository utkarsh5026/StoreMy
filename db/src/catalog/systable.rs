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
        use Type::{Bool, Int32, Int64, String, Uint32, Uint64};

        let field = |name: &'static str, ty| Field::new(name, ty);

        let fields = match self {
            SystemTable::Tables => vec![
                field("table_id", Uint64).not_null(),
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
                field("table_id", Uint64).not_null(),
                field("column_name", String).not_null(),
                field("index_type", Int32).not_null(),
            ],
        };

        TupleSchema::new(fields)
    }

    /// Validates a single tuple against this system table's schema and semantic constraints.
    ///
    /// Two layers of checks are applied:
    ///
    /// 1. **Structural** — [`TupleSchema::validate`] verifies the field count,
    ///    NOT NULL constraints, and declared types.
    /// 2. **Semantic** — the row-specific `TryFrom<&Tuple>` impl checks that
    ///    stored discriminant values map to known variants (e.g. a `column_type`
    ///    field must be a recognized [`Type`] code).
    ///
    /// # Errors
    ///
    /// - [`CatalogError::Corruption`] — structural validation failed.
    /// - [`CatalogError::InvalidCatalogRow`] — semantic validation failed.
    pub fn validate_row(self, tuple: &Tuple) -> Result<(), CatalogError> {
        self.schema()
            .validate(tuple)
            .map_err(|e| CatalogError::corruption(self.table_name(), e.to_string()))?;

        match self {
            SystemTable::Tables => {
                TableRow::try_from(tuple)?;
            }
            SystemTable::Columns => {
                ColumnRow::try_from(tuple)?;
            }
            SystemTable::Indexes => {
                IndexRow::try_from(tuple)?;
            }
        }

        Ok(())
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

fn non_empty(value: &str, field: &str) -> Result<(), CatalogError> {
    if value.is_empty() {
        return Err(CatalogError::invalid_catalog_row(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

pub(super) struct TableRow {
    pub(super) table_id: FileId,
    pub(super) table_name: String,
    pub(super) file_path: PathBuf,
    pub(super) primary_key: Option<String>,
}

impl TableRow {
    pub(super) fn new(
        table_id: FileId,
        table_name: String,
        file_path: PathBuf,
        primary_key: Option<String>,
    ) -> Result<Self, CatalogError> {
        non_empty(&table_name, "table_name")?;
        non_empty(&file_path.as_os_str().to_string_lossy(), "file_path")?;
        if let Some(ref pk) = primary_key {
            non_empty(pk, "primary_key")?;
        }

        Ok(Self {
            table_id,
            table_name,
            file_path,
            primary_key,
        })
    }
}

impl TryFrom<&Tuple> for TableRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Self::new(
            FileId::from(read_as!(t, 0 => u64 => u64)),
            read_as!(t, 1),
            PathBuf::from(read_as!(t, 2 => String => String)),
            read_as!(t, 3 => String => Option<String>),
        )
    }
}

impl From<&TableRow> for Tuple {
    fn from(r: &TableRow) -> Tuple {
        Tuple::new(vec![
            r.table_id.0.into(),
            r.table_name.clone().into(),
            r.file_path.to_string_lossy().to_string().into(),
            r.primary_key
                .as_ref()
                .map_or(Value::Null, |s| s.clone().into()),
        ])
    }
}

pub(super) struct ColumnRow {
    pub(super) table_id: i64,
    pub(super) column_name: String,
    pub(super) column_type: Type,
    pub(super) position: i32,
    pub(super) nullable: bool,
}

impl ColumnRow {
    pub(super) fn new(
        table_id: i64,
        column_name: String,
        column_type: Type,
        position: i32,
        nullable: bool,
    ) -> Result<Self, CatalogError> {
        non_empty(&column_name, "column_name")?;
        if position < 0 {
            return Err(CatalogError::invalid_catalog_row(format!(
                "position must be non-negative, got {position}"
            )));
        }
        Ok(Self {
            table_id,
            column_name,
            column_type,
            position,
            nullable,
        })
    }

    /// Builds one [`ColumnRow`] per field in `schema` for catalog `CATALOG_COLUMNS`.
    pub(super) fn from_schema(
        table_id: FileId,
        schema: &TupleSchema,
    ) -> Result<Vec<ColumnRow>, CatalogError> {
        schema
            .fields()
            .enumerate()
            .map(|(i, field)| {
                let position = i32::try_from(i).map_err(|_| {
                    CatalogError::invalid_catalog_row(format!(
                        "column position {i} does not fit in i32"
                    ))
                })?;
                Self::new(
                    table_id.0.cast_signed(),
                    field.name.clone(),
                    field.field_type,
                    position,
                    field.nullable,
                )
            })
            .collect()
    }
}

impl TryFrom<&Tuple> for ColumnRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Self::new(
            read_as!(t, 0),
            read_as!(t, 1),
            Type::try_from(read_as!(t, 2 => u32 => u32))
                .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
            read_as!(t, 3),
            read_as!(t, 4),
        )
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

impl From<Vec<ColumnRow>> for TupleSchema {
    fn from(mut rows: Vec<ColumnRow>) -> Self {
        rows.sort_by_key(|r| r.position);
        let fields = rows
            .into_iter()
            .map(|r| {
                let f = Field::new(r.column_name, r.column_type);
                if r.nullable { f } else { f.not_null() }
            })
            .collect();
        TupleSchema::new(fields)
    }
}

pub(super) struct IndexRow {
    pub(super) index_id: i64,
    pub(super) index_name: String,
    pub(super) table_id: FileId,
    pub(super) column_name: String,
    pub(super) index_type: Index,
}

impl IndexRow {
    pub(super) fn new(
        index_id: i64,
        index_name: String,
        table_id: FileId,
        column_name: String,
        index_type: Index,
    ) -> Result<Self, CatalogError> {
        non_empty(&index_name, "index_name")?;
        non_empty(&column_name, "column_name")?;
        Ok(Self {
            index_id,
            index_name,
            table_id,
            column_name,
            index_type,
        })
    }
}

impl TryFrom<&Tuple> for IndexRow {
    type Error = CatalogError;

    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        Self::new(
            read_as!(t, 0),
            read_as!(t, 1),
            FileId::from(read_as!(t, 2 => u64 => u64)),
            read_as!(t, 3),
            Index::try_from(read_as!(t, 4 => u32 => u32))
                .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
        )
    }
}

impl From<&IndexRow> for Tuple {
    fn from(r: &IndexRow) -> Tuple {
        Tuple::new(vec![
            r.index_id.into(),
            r.index_name.clone().into(),
            r.table_id.0.into(),
            r.column_name.clone().into(),
            u32::from(r.index_type).into(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::SystemTable;
    use crate::{Value, catalog::CatalogError, tuple::Tuple};

    fn valid_tables_tuple() -> Tuple {
        // table_id (Uint64 NOT NULL), table_name (String NOT NULL),
        // file_path (String NOT NULL), primary_key (String, nullable)
        Tuple::new(vec![
            Value::Uint64(1),
            Value::String("users".into()),
            Value::String("/data/users.dat".into()),
            Value::Null,
        ])
    }

    fn valid_columns_tuple() -> Tuple {
        // table_id (Int64 NOT NULL), column_name (String NOT NULL),
        // column_type (Uint32 NOT NULL, 5 = Type::String),
        // position (Int32 NOT NULL), nullable (Bool, nullable)
        Tuple::new(vec![
            Value::Int64(1),
            Value::String("email".into()),
            Value::Uint32(5),
            Value::Int32(0),
            Value::Bool(true),
        ])
    }

    // Fully valid Tables row with a null primary_key must pass.
    #[test]
    fn test_validate_tables_row_null_primary_key_ok() {
        assert!(
            SystemTable::Tables
                .validate_row(&valid_tables_tuple())
                .is_ok()
        );
    }

    // Tables row with a non-null primary_key must also pass.
    #[test]
    fn test_validate_tables_row_with_primary_key_ok() {
        let tuple = Tuple::new(vec![
            Value::Uint64(2),
            Value::String("orders".into()),
            Value::String("/data/orders.dat".into()),
            Value::String("order_id".into()),
        ]);
        assert!(SystemTable::Tables.validate_row(&tuple).is_ok());
    }

    // Wrong number of fields must yield Corruption.
    #[test]
    fn test_validate_tables_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![Value::Uint64(1), Value::String("users".into())]);
        let err = SystemTable::Tables.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::Corruption { .. }),
            "expected Corruption, got: {err}"
        );
    }

    // NULL in the NOT NULL table_id column must yield Corruption.
    #[test]
    fn test_validate_tables_row_null_in_not_null_col_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Null, // table_id is NOT NULL
            Value::String("users".into()),
            Value::String("/data/users.dat".into()),
            Value::Null,
        ]);
        let err = SystemTable::Tables.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // Type mismatch on table_id must yield Corruption.
    #[test]
    fn test_validate_tables_row_type_mismatch_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Int32(1), // declared Uint64
            Value::String("users".into()),
            Value::String("/data/users.dat".into()),
            Value::Null,
        ]);
        let err = SystemTable::Tables.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // ── validate_row: Columns — happy path ───────────────────────────────────

    // Fully valid Columns row must pass.
    #[test]
    fn test_validate_columns_row_ok() {
        assert!(
            SystemTable::Columns
                .validate_row(&valid_columns_tuple())
                .is_ok()
        );
    }

    // Every known Type discriminant (0..=6) must be accepted as column_type.
    #[test]
    fn test_validate_columns_row_all_valid_type_discriminants() {
        for disc in 0u32..=6 {
            let tuple = Tuple::new(vec![
                Value::Int64(1),
                Value::String(format!("col_{disc}")),
                Value::Uint32(disc),
                Value::Int32(0),
                Value::Bool(false),
            ]);
            assert!(
                SystemTable::Columns.validate_row(&tuple).is_ok(),
                "discriminant {disc} should be a valid Type"
            );
        }
    }

    // Wrong field count must yield Corruption.
    #[test]
    fn test_validate_columns_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![Value::Int64(1), Value::String("col".into())]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // NULL in the NOT NULL column_name column must yield Corruption.
    #[test]
    fn test_validate_columns_row_null_in_not_null_col_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::Null, // column_name is NOT NULL
            Value::Uint32(0),
            Value::Int32(0),
            Value::Bool(false),
        ]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // An unrecognized column_type discriminant must yield InvalidCatalogRow.
    #[test]
    fn test_validate_columns_row_invalid_type_discriminant_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::String("col".into()),
            Value::Uint32(999), // no such Type variant
            Value::Int32(0),
            Value::Bool(false),
        ]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "expected InvalidCatalogRow, got: {err}"
        );
    }

    // ── validate_row: Indexes — schema/reader inconsistency ──────────────────
    //
    // BUG: The Indexes schema declares `index_type` as Int32, but IndexRow::try_from
    // reads it with TupleReader::read::<u32>, which only accepts Value::Uint32.
    // Neither value type can pass both validation layers simultaneously:
    //
    //   Value::Int32  → passes schema.validate, fails IndexRow::try_from → InvalidCatalogRow
    //   Value::Uint32 → fails schema.validate (type mismatch)            → Corruption
    //
    // Fix: change the Indexes schema's index_type field from Int32 to Uint32
    // (matching the pattern used by ColumnRow's column_type field).

    // Value::Int32 passes structural validation but fails semantic conversion.
    #[test]
    fn test_validate_indexes_row_int32_index_type_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx_email".into()),
            Value::Uint64(1),
            Value::String("email".into()),
            Value::Int32(0), // schema says Int32, reader expects Uint32
        ]);
        let err = SystemTable::Indexes.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "Int32 index_type should fail semantic validation, got: {err}"
        );
    }

    // Value::Uint32 fails structural validation (type mismatch against schema).
    #[test]
    fn test_validate_indexes_row_uint32_index_type_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx_email".into()),
            Value::Uint64(1),
            Value::String("email".into()),
            Value::Uint32(0), // reader needs this, but schema says Int32
        ]);
        let err = SystemTable::Indexes.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::Corruption { .. }),
            "Uint32 index_type should fail structural validation, got: {err}"
        );
    }

    // Wrong field count for Indexes must yield Corruption.
    #[test]
    fn test_validate_indexes_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![Value::Int64(1), Value::String("idx".into())]);
        let err = SystemTable::Indexes.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // ── SystemTable constants ────────────────────────────────────────────────

    // ALL must contain exactly three variants.
    #[test]
    fn test_system_table_all_has_three_variants() {
        assert_eq!(SystemTable::ALL.len(), 3);
        assert!(SystemTable::ALL.contains(&SystemTable::Tables));
        assert!(SystemTable::ALL.contains(&SystemTable::Columns));
        assert!(SystemTable::ALL.contains(&SystemTable::Indexes));
    }

    // file_id values must be unique across all system tables.
    #[test]
    fn test_system_table_file_ids_are_unique() {
        let ids: Vec<_> = SystemTable::ALL.iter().map(|t| t.file_id()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(ids.len(), unique.len());
    }

    // file_name values must be unique across all system tables.
    #[test]
    fn test_system_table_file_names_are_unique() {
        let names: Vec<_> = SystemTable::ALL.iter().map(|t| t.file_name()).collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(names.len(), unique.len());
    }

    // table_name values must be unique across all system tables.
    #[test]
    fn test_system_table_table_names_are_unique() {
        let names: Vec<_> = SystemTable::ALL.iter().map(|t| t.table_name()).collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(names.len(), unique.len());
    }

    // Display output must match table_name.
    #[test]
    fn test_system_table_display_matches_table_name() {
        for table in SystemTable::ALL {
            assert_eq!(table.to_string(), table.table_name());
        }
    }
}
