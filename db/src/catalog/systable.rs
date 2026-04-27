use std::{fmt, path::PathBuf};

use crate::{
    FileId, IndexId, Type,
    catalog::{CatalogError, tuple::TupleReader},
    index::IndexKind,
    primitives::ColumnId,
    tuple::{Field, Tuple, TupleSchema},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemTable {
    Tables,
    Columns,
    Indexes,
    PrimaryKeyColumns,
}

impl SystemTable {
    /// All system tables — iterate for initialization
    pub const ALL: &[SystemTable] = &[
        SystemTable::Tables,
        SystemTable::Columns,
        SystemTable::Indexes,
        SystemTable::PrimaryKeyColumns,
    ];

    pub const fn file_id(self) -> FileId {
        match self {
            SystemTable::Tables => FileId(1),
            SystemTable::Columns => FileId(2),
            SystemTable::Indexes => FileId(3),
            SystemTable::PrimaryKeyColumns => FileId(4),
        }
    }

    pub const fn file_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "catalog_tables.dat",
            SystemTable::Columns => "catalog_columns.dat",
            SystemTable::Indexes => "catalog_indexes.dat",
            SystemTable::PrimaryKeyColumns => "catalog_primary_key_columns.dat",
        }
    }

    pub const fn table_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "CATALOG_TABLES",
            SystemTable::Columns => "CATALOG_COLUMNS",
            SystemTable::Indexes => "CATALOG_INDEXES",
            SystemTable::PrimaryKeyColumns => "CATALOG_PRIMARY_KEY_COLUMNS",
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
            ],
            SystemTable::Columns => vec![
                field("table_id", Uint64).not_null(),
                field("column_name", String).not_null(),
                field("column_type", Uint32).not_null(),
                field("position", Uint32).not_null(),
                field("nullable", Bool),
            ],
            SystemTable::Indexes => vec![
                field("index_id", Int64).not_null(),
                field("index_name", String).not_null(),
                field("table_id", Uint64).not_null(),
                field("column_name", String).not_null(),
                field("column_position", Uint32).not_null(),
                field("index_type", Uint32).not_null(),
                field("index_file_id", Uint64).not_null(),
                field("num_buckets", Uint32).not_null(),
            ],
            SystemTable::PrimaryKeyColumns => vec![
                field("table_id", Uint64).not_null(),
                field("column_id", Uint32).not_null(),
                field("ordinal", Int32).not_null(),
            ],
        };

        TupleSchema::new(fields)
    }

    /// Validates a single tuple against this system table's schema and semantic constraints.
    ///
    /// Two layers of checks are applied:
    ///
    /// 1. **Structural** — [`TupleSchema::validate`] verifies the field count, NOT NULL
    ///    constraints, and declared types.
    /// 2. **Semantic** — the row-specific `TryFrom<&Tuple>` impl checks that stored discriminant
    ///    values map to known variants (e.g. a `column_type` field must be a recognized [`Type`]
    ///    code).
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
            SystemTable::PrimaryKeyColumns => {
                PrimaryKeyColumnRow::try_from(tuple)?;
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

/// Static binding from a row type to the [`SystemTable`] it lives in.
///
/// Every catalog row struct (`TableRow`, `ColumnRow`, …) belongs to exactly
/// one system table. Encoding that relationship as an associated constant lets
/// the catalog helpers (`scan_system_table`, `insert_systable_tuple`, …)
/// recover the table from `T::TABLE` instead of forcing every caller to pass
/// it alongside the type.
///
/// The `for<'a> TryFrom<&'a Tuple>` super-trait is the bound the scan helpers
/// already required — it just lives on the trait now so callers don't have to
/// repeat it in every `where` clause.
pub(super) trait CatalogRow: for<'a> TryFrom<&'a Tuple, Error = CatalogError> {
    const TABLE: SystemTable;
}

/// Generates the on-disk codec and [`CatalogRow`] impl for a catalog row type.
///
/// One invocation produces three impls:
///
/// - `From<&T> for Tuple` — encodes the row to a tuple. The `encode` arm takes a comma-separated
///   list of expressions; the macro wraps each with `.into()` and lifts the result into
///   `Tuple::new(vec![…])`.
/// - `TryFrom<&Tuple> for T` — decodes a tuple back to the row. The `decode` arm takes the argument
///   list to pass to `T::new`; validation lives in `new`, so the codec stays purely mechanical.
/// - `impl CatalogRow for T` — pins the row to its [`SystemTable`].
///
/// **Requires** `T::new` to exist and return `Result<Self, CatalogError>`.
macro_rules! catalog_row_codec {
    (
        $row:ty => $variant:ident,
        encode |$r:ident| [ $($field:expr),* $(,)? ],
        decode |$t:ident| ( $($arg:expr),* $(,)? ) $(,)?
    ) => {
        impl From<&$row> for Tuple {
            fn from($r: &$row) -> Tuple {
                Tuple::new(vec![ $( $field.into() ),* ])
            }
        }

        impl TryFrom<&Tuple> for $row {
            type Error = CatalogError;
            fn try_from($t: &Tuple) -> Result<Self, Self::Error> {
                Self::new( $($arg),* )
            }
        }

        impl CatalogRow for $row {
            const TABLE: SystemTable = SystemTable::$variant;
        }
    };
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

/// Checks that a given string `value` is not empty, returning an error if it is.
///
/// # Arguments
///
/// * `value` - The string to check for emptiness.
/// * `field` - The name of the field (for error messages).
///
/// # Errors
///
/// Returns a [`CatalogError`] if the string is empty, identifying the specific field.
///
/// # Examples
///
/// ```rust,ignore
/// non_empty("foo", "column_name")?; // Ok
/// non_empty("", "column_name")?;    // Err: "column_name must not be empty"
/// ```
fn non_empty(value: &str, field: &'static str) -> Result<(), CatalogError> {
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
}

impl TableRow {
    pub(super) fn new(
        table_id: FileId,
        table_name: String,
        file_path: PathBuf,
    ) -> Result<Self, CatalogError> {
        non_empty(&table_name, "table_name")?;
        non_empty(&file_path.as_os_str().to_string_lossy(), "file_path")?;

        Ok(Self {
            table_id,
            table_name,
            file_path,
        })
    }
}

catalog_row_codec! {
    TableRow => Tables,
    encode |r| [
        u64::from(r.table_id),
        r.table_name.clone(),
        r.file_path.to_string_lossy().to_string(),
    ],
    decode |t| (
        FileId::from(read_as!(t, 0 => u64 => u64)),
        read_as!(t, 1),
        PathBuf::from(read_as!(t, 2 => String => String)),
    ),
}

pub(super) struct ColumnRow {
    pub table_id: FileId,
    pub column_name: String,
    pub column_type: Type,
    pub position: ColumnId,
    pub nullable: bool,
}

impl ColumnRow {
    pub(super) fn new(
        table_id: FileId,
        column_name: String,
        column_type: Type,
        position: ColumnId,
        nullable: bool,
    ) -> Result<Self, CatalogError> {
        non_empty(&column_name, "column_name")?;
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
        let mut rows = Vec::new();

        for (
            i,
            Field {
                name,
                field_type,
                nullable,
            },
        ) in schema.fields().enumerate()
        {
            let position = ColumnId::try_from(i).map_err(|e| {
                CatalogError::invalid_catalog_row(format!(
                    "column position {i} is not a valid ColumnId: {e}"
                ))
            })?;

            let row = Self::new(table_id, name.clone(), *field_type, position, *nullable)?;
            rows.push(row);
        }
        Ok(rows)
    }
}

catalog_row_codec! {
    ColumnRow => Columns,
    encode |r| [
        u64::from(r.table_id),
        r.column_name.clone(),
        u32::from(r.column_type),
        u32::from(r.position),
        r.nullable,
    ],
    decode |t| (
        FileId::from(read_as!(t, 0 => u64 => u64)),
        read_as!(t, 1),
        Type::try_from(read_as!(t, 2 => u32 => u32))
            .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
        read_as!(t, 3 => u32 => ColumnId),
        read_as!(t, 4),
    ),
}

impl From<Vec<ColumnRow>> for TupleSchema {
    fn from(mut rows: Vec<ColumnRow>) -> Self {
        rows.sort_by_key(|r| r.position);
        let mut fields = Vec::new();
        for ColumnRow {
            column_name,
            column_type,
            nullable,
            ..
        } in rows
        {
            let f = Field::new(column_name, column_type);
            fields.push(if nullable { f } else { f.not_null() });
        }
        TupleSchema::new(fields)
    }
}

/// One row in `SystemTable::Indexes`.
///
/// Composite indexes use the multi-row pattern: a 2-column index
/// (`CREATE INDEX … ON t (a, b)`) writes two rows that share every field
/// except `column_name` and `column_position`. The merge from rows to a
/// single in-memory [`crate::catalog::IndexInfo`] happens up the stack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IndexRow {
    pub(super) index_id: IndexId,
    pub(super) index_name: String,
    pub(super) table_id: FileId,
    pub(super) column_name: String,
    /// 0-based ordinal of this column within the index's column list.
    pub(super) column_position: ColumnId,
    pub(super) index_type: IndexKind,
    /// `FileId` of the index's own pages — distinct from `table_id`, which
    /// points at the *table's* heap. Reconstructing the index on database
    /// open requires this file.
    pub(super) index_file_id: FileId,
    /// Static-hash bucket count. Frozen at creation; only meaningful for
    /// `IndexKind::Hash`. Set to 0 for B-tree rows.
    pub(super) num_buckets: u32,
}

impl IndexRow {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        index_id: IndexId,
        index_name: String,
        table_id: FileId,
        column_name: String,
        column_position: ColumnId,
        index_type: IndexKind,
        index_file_id: FileId,
        num_buckets: u32,
    ) -> Result<Self, CatalogError> {
        non_empty(&index_name, "index_name")?;
        non_empty(&column_name, "column_name")?;
        if matches!(index_type, IndexKind::Hash) && num_buckets == 0 {
            return Err(CatalogError::invalid_catalog_row(
                "hash index must declare num_buckets > 0",
            ));
        }
        Ok(Self {
            index_id,
            index_name,
            table_id,
            column_name,
            column_position,
            index_type,
            index_file_id,
            num_buckets,
        })
    }

    /// Returns `true` when both rows describe the same logical index-level
    /// metadata, ignoring per-column fields (`column_name`, `column_position`).
    pub(super) fn same_index_metadata_as(&self, other: &Self) -> bool {
        self.index_id == other.index_id
            && self.index_name == other.index_name
            && self.table_id == other.table_id
            && self.index_type == other.index_type
            && self.index_file_id == other.index_file_id
            && self.num_buckets == other.num_buckets
    }
}

catalog_row_codec! {
    IndexRow => Indexes,
    encode |r| [
        r.index_id.0,
        r.index_name.clone(),
        r.table_id.0,
        r.column_name.clone(),
        u32::from(r.column_position),
        u32::from(r.index_type),
        r.index_file_id.0,
        r.num_buckets,
    ],
    decode |t| (
        IndexId::from(read_as!(t, 0 => i64 => i64)),
        read_as!(t, 1),
        FileId::from(read_as!(t, 2 => u64 => u64)),
        read_as!(t, 3),
        read_as!(t, 4 => u32 => ColumnId),
        IndexKind::try_from(read_as!(t, 5 => u32 => u32))
            .map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))?,
        FileId::from(read_as!(t, 6 => u64 => u64)),
        read_as!(t, 7),
    ),
}

/// One row in `SystemTable::PrimaryKeyColumns`.
///
/// Represents a single column of a table's primary key. A composite PK
/// (`PRIMARY KEY (a, b)`) writes two rows that share `table_id` and differ in
/// `ordinal`, where `ordinal` is the column's position inside the PK list
/// (0-based, in declaration order). Reconstructing the PK on load means
/// scanning all rows for a `table_id` and sorting by `ordinal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PrimaryKeyColumnRow {
    pub(super) table_id: FileId,
    pub(super) column_id: ColumnId,
    /// 0-based position of this column inside the PK list. Distinct from
    /// `column_id`: `column_id` says *which* schema column, `ordinal` says
    /// *which slot* of the PK that column occupies.
    pub(super) ordinal: i32,
}

impl PrimaryKeyColumnRow {
    pub(super) fn new(
        table_id: FileId,
        column_id: ColumnId,
        ordinal: i32,
    ) -> Result<Self, CatalogError> {
        if ordinal < 0 {
            return Err(CatalogError::invalid_catalog_row(format!(
                "ordinal must be non-negative, got {ordinal}"
            )));
        }
        Ok(Self {
            table_id,
            column_id,
            ordinal,
        })
    }
}

catalog_row_codec! {
    PrimaryKeyColumnRow => PrimaryKeyColumns,
    encode |r| [
        r.table_id.0,
        u32::from(r.column_id),
        r.ordinal,
    ],
    decode |t| (
        FileId::from(read_as!(t, 0 => u64 => u64)),
        read_as!(t, 1 => u32 => ColumnId),
        read_as!(t, 2),
    ),
}

#[cfg(test)]
mod tests {
    use super::SystemTable;
    use crate::{Value, catalog::CatalogError, tuple::Tuple};

    fn valid_tables_tuple() -> Tuple {
        // table_id (Uint64 NOT NULL), table_name (String NOT NULL),
        // file_path (String NOT NULL).
        Tuple::new(vec![
            Value::Uint64(1),
            Value::String("users".into()),
            Value::String("/data/users.dat".into()),
        ])
    }

    fn valid_columns_tuple() -> Tuple {
        // table_id (Uint64 NOT NULL), column_name (String NOT NULL),
        // column_type (Uint32 NOT NULL, 5 = Type::String),
        // position (Uint32 NOT NULL), nullable (Bool, nullable)
        Tuple::new(vec![
            Value::Uint64(1),
            Value::String("email".into()),
            Value::Uint32(5),
            Value::Uint32(0),
            Value::Bool(true),
        ])
    }

    // Fully valid Tables row must pass.
    #[test]
    fn test_validate_tables_row_ok() {
        assert!(
            SystemTable::Tables
                .validate_row(&valid_tables_tuple())
                .is_ok()
        );
    }

    // Wrong number of fields must yield Corruption. The PK info now lives
    // in PrimaryKeyColumns, so a Tables row with a trailing primary_key
    // column would now fail — the catch-all field-count check still applies.
    #[test]
    fn test_validate_tables_row_with_extra_pk_field_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Uint64(2),
            Value::String("orders".into()),
            Value::String("/data/orders.dat".into()),
            Value::String("order_id".into()), // legacy field — no longer in schema
        ]);
        let err = SystemTable::Tables.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
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
                Value::Uint64(1),
                Value::String(format!("col_{disc}")),
                Value::Uint32(disc),
                Value::Uint32(0),
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
        let tuple = Tuple::new(vec![Value::Uint64(1), Value::String("col".into())]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // NULL in the NOT NULL column_name column must yield Corruption.
    #[test]
    fn test_validate_columns_row_null_in_not_null_col_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Uint64(1),
            Value::Null, // column_name is NOT NULL
            Value::Uint32(0),
            Value::Uint32(0),
            Value::Bool(false),
        ]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // An unrecognized column_type discriminant must yield InvalidCatalogRow.
    #[test]
    fn test_validate_columns_row_invalid_type_discriminant_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::Uint64(1),
            Value::String("col".into()),
            Value::Uint32(999), // no such Type variant
            Value::Uint32(0),
            Value::Bool(false),
        ]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "expected InvalidCatalogRow, got: {err}"
        );
    }

    // ── validate_row: Indexes ────────────────────────────────────────────────

    fn valid_indexes_tuple() -> Tuple {
        // index_id, index_name, table_id (Uint64), column_name,
        // column_position (Uint32), index_type (Uint32 = IndexKind::Hash),
        // index_file_id (Uint64), num_buckets (Uint32).
        Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx_email".into()),
            Value::Uint64(7),
            Value::String("email".into()),
            Value::Uint32(0),
            Value::Uint32(0), // 0 = IndexKind::Hash
            Value::Uint64(42),
            Value::Uint32(64),
        ])
    }

    #[test]
    fn test_validate_indexes_row_ok() {
        assert!(
            SystemTable::Indexes
                .validate_row(&valid_indexes_tuple())
                .is_ok()
        );
    }

    // Composite index: same row schema, different column_position.
    #[test]
    fn test_validate_indexes_row_composite_position_ok() {
        let t = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx_email".into()),
            Value::Uint64(7),
            Value::String("created_at".into()),
            Value::Uint32(1), // second column of a composite index
            Value::Uint32(0),
            Value::Uint64(42),
            Value::Uint32(64),
        ]);
        assert!(SystemTable::Indexes.validate_row(&t).is_ok());
    }

    // Storing column_position as Int32 (the old format) must now fail
    // structural validation: the schema column type is Uint32. The wrapper
    // type ColumnId encodes its non-negative invariant in the Rust type
    // system, so a separate semantic check would be redundant.
    #[test]
    fn test_validate_indexes_row_int32_position_yields_corruption() {
        let t = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx".into()),
            Value::Uint64(1),
            Value::String("c".into()),
            Value::Int32(0), // wrong: schema declares Uint32
            Value::Uint32(0),
            Value::Uint64(42),
            Value::Uint32(64),
        ]);
        let err = SystemTable::Indexes.validate_row(&t).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    // Hash index with num_buckets == 0 is invalid (frozen bucket count must
    // be positive — zero would imply an empty modulus in hash_to_bucket).
    #[test]
    fn test_validate_indexes_row_hash_zero_buckets_yields_invalid_row() {
        let t = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx".into()),
            Value::Uint64(1),
            Value::String("c".into()),
            Value::Uint32(0),
            Value::Uint32(0), // hash
            Value::Uint64(42),
            Value::Uint32(0), // zero — invalid for hash
        ]);
        let err = SystemTable::Indexes.validate_row(&t).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidCatalogRow { .. }));
    }

    // Schema/reader agreement: index_type as Int32 must now fail structural
    // validation (the schema is Uint32). The previous bug (Int32 schema vs
    // Uint32 reader) is fixed.
    #[test]
    fn test_validate_indexes_row_int32_index_type_yields_corruption() {
        let t = Tuple::new(vec![
            Value::Int64(1),
            Value::String("idx".into()),
            Value::Uint64(1),
            Value::String("c".into()),
            Value::Uint32(0),
            Value::Int32(0), // wrong: schema declares Uint32
            Value::Uint64(42),
            Value::Uint32(64),
        ]);
        let err = SystemTable::Indexes.validate_row(&t).unwrap_err();
        assert!(
            matches!(err, CatalogError::Corruption { .. }),
            "Int32 index_type should fail structural validation, got: {err}"
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
    fn test_system_table_all_has_four_variants() {
        assert_eq!(SystemTable::ALL.len(), 4);
        assert!(SystemTable::ALL.contains(&SystemTable::Tables));
        assert!(SystemTable::ALL.contains(&SystemTable::Columns));
        assert!(SystemTable::ALL.contains(&SystemTable::Indexes));
        assert!(SystemTable::ALL.contains(&SystemTable::PrimaryKeyColumns));
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
