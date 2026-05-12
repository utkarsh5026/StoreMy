use std::{
    fmt,
    path::{Path, PathBuf},
};

use crate::{
    FileId, IndexId, Type, Value,
    catalog::{CatalogError, tuple::CatalogTupleRead},
    index::IndexKind,
    primitives::{ColumnId, NonEmptyString},
    tuple::{Field, Tuple, TupleSchema},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemTable {
    Tables,
    Columns,
    Indexes,
    PrimaryKeyColumns,
    Constraints,
    ConstraintColumns,
    FkConstraints,
}

impl SystemTable {
    /// All system tables — iterate for initialization
    pub const ALL: &[SystemTable] = &[
        SystemTable::Tables,
        SystemTable::Columns,
        SystemTable::Indexes,
        SystemTable::PrimaryKeyColumns,
        SystemTable::Constraints,
        SystemTable::ConstraintColumns,
        SystemTable::FkConstraints,
    ];

    pub const fn file_id(self) -> FileId {
        match self {
            SystemTable::Tables => FileId(1),
            SystemTable::Columns => FileId(2),
            SystemTable::Indexes => FileId(3),
            SystemTable::PrimaryKeyColumns => FileId(4),
            SystemTable::Constraints => FileId(5),
            SystemTable::ConstraintColumns => FileId(6),
            SystemTable::FkConstraints => FileId(7),
        }
    }

    pub const fn file_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "catalog_tables.dat",
            SystemTable::Columns => "catalog_columns.dat",
            SystemTable::Indexes => "catalog_indexes.dat",
            SystemTable::PrimaryKeyColumns => "catalog_primary_key_columns.dat",
            SystemTable::Constraints => "catalog_constraints.dat",
            SystemTable::ConstraintColumns => "catalog_constraint_columns.dat",
            SystemTable::FkConstraints => "catalog_fk_constraints.dat",
        }
    }

    pub const fn table_name(self) -> &'static str {
        match self {
            SystemTable::Tables => "CATALOG_TABLES",
            SystemTable::Columns => "CATALOG_COLUMNS",
            SystemTable::Indexes => "CATALOG_INDEXES",
            SystemTable::PrimaryKeyColumns => "CATALOG_PRIMARY_KEY_COLUMNS",
            SystemTable::Constraints => "CATALOG_CONSTRAINTS",
            SystemTable::ConstraintColumns => "CATALOG_CONSTRAINT_COLUMNS",
            SystemTable::FkConstraints => "CATALOG_FK_CONSTRAINTS",
        }
    }

    pub fn schema(self) -> TupleSchema {
        use Type::{Bool, Int32, Int64, String, Uint32, Uint64};

        // Names are compile-time string literals — Field::new can never fail here.
        let field = |name: &'static str, ty| Field::new(name, ty).expect("static field name");

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
                field("is_dropped", Bool).not_null(),
                // Stored as the raw Value; nullable means "no default". Type
                // validation is skipped for Value::Null so any nullable column
                // works here — revisit when non-null defaults are supported.
                field("missing_default_value", String),
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
            SystemTable::Constraints => vec![
                field("constraint_name", String).not_null(),
                field("table_id", Uint64).not_null(),
                field("constraint_kind", Uint32).not_null(),
                field("expr", String),
                field("backing_index_id", Int64),
            ],
            SystemTable::ConstraintColumns => vec![
                field("constraint_name", String).not_null(),
                field("table_id", Uint64).not_null(),
                field("column_id", Uint32).not_null(),
                field("ordinal", Int32).not_null(),
            ],
            SystemTable::FkConstraints => vec![
                field("constraint_name", String).not_null(),
                field("table_id", Uint64).not_null(),
                field("local_column_id", Uint32).not_null(),
                field("ordinal", Int32).not_null(),
                field("ref_table_id", Uint64).not_null(),
                field("ref_column_id", Uint32).not_null(),
                field("on_delete", Uint32).not_null(),
                field("on_update", Uint32).not_null(),
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
            SystemTable::Constraints => {
                ConstraintRow::try_from(tuple)?;
            }
            SystemTable::ConstraintColumns => {
                ConstraintColumnRow::try_from(tuple)?;
            }
            SystemTable::FkConstraints => {
                FkConstraintRow::try_from(tuple)?;
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

/// Validates a raw string and wraps it in a [`NonEmptyString`], attaching the
/// catalog-field label to any error so the message points at the offending column.
fn validate_name(value: String, field: &'static str) -> Result<NonEmptyString, CatalogError> {
    NonEmptyString::new(value)
        .map_err(|e| CatalogError::invalid_catalog_row(format!("{field}: {e}")))
}

/// Maps a fallible conversion (e.g. [`TryFrom`]) into [`CatalogError::invalid_catalog_row`].
fn catalog_row_try<T, E>(result: Result<T, E>) -> Result<T, CatalogError>
where
    E: fmt::Display,
{
    result.map_err(|e| CatalogError::invalid_catalog_row(e.to_string()))
}

/// Empty-check for a [`PathBuf`]. Paths don't go through [`NonEmptyString`]
/// because the type's NUL/length rules are name-shaped, not path-shaped.
fn non_empty_path(value: &Path, field: &'static str) -> Result<(), CatalogError> {
    if value.as_os_str().is_empty() {
        return Err(CatalogError::invalid_catalog_row(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

pub(super) struct TableRow {
    pub(super) table_id: FileId,
    pub(super) table_name: NonEmptyString,
    pub(super) file_path: PathBuf,
}

impl TableRow {
    pub(super) fn new(
        table_id: FileId,
        table_name: String,
        file_path: PathBuf,
    ) -> Result<Self, CatalogError> {
        let table_name = validate_name(table_name, "table_name")?;
        non_empty_path(&file_path, "file_path")?;

        Ok(Self {
            table_id,
            table_name,
            file_path,
        })
    }
}

impl From<&TableRow> for Tuple {
    fn from(row: &TableRow) -> Tuple {
        Tuple::new(vec![
            u64::from(row.table_id).into(),
            row.table_name.as_str().to_owned().into(),
            row.file_path.to_string_lossy().to_string().into(),
        ])
    }
}

impl TryFrom<&Tuple> for TableRow {
    type Error = CatalogError;

    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        let table_id = FileId::from(tuple.read_field::<u64>(0)?);
        let table_name = tuple.read_field(1)?;
        let file_path = PathBuf::from(tuple.read_field::<String>(2)?);
        Self::new(table_id, table_name, file_path)
    }
}

impl CatalogRow for TableRow {
    const TABLE: SystemTable = SystemTable::Tables;
}

pub(super) struct ColumnRow {
    pub table_id: FileId,
    pub column_name: NonEmptyString,
    pub column_type: Type,
    pub position: ColumnId,
    pub nullable: bool,
    pub is_dropped: bool,
    pub missing_default_value: Option<Value>,
}

impl ColumnRow {
    /// Builds one [`ColumnRow`] per field in `schema` for catalog `CATALOG_COLUMNS`.
    pub(super) fn from_schema(
        table_id: FileId,
        schema: &TupleSchema,
    ) -> Result<Vec<ColumnRow>, CatalogError> {
        let mut rows = Vec::new();

        for (i, f) in schema.fields().enumerate() {
            let position = ColumnId::try_from(i).map_err(|e| {
                CatalogError::invalid_catalog_row(format!(
                    "column position {i} is not a valid ColumnId: {e}"
                ))
            })?;

            let row = ColumnRow {
                table_id,
                column_name: f.name.clone(),
                column_type: f.field_type,
                position,
                nullable: f.nullable,
                is_dropped: f.is_dropped,
                missing_default_value: f.missing_default_value.clone(),
            };
            rows.push(row);
        }
        Ok(rows)
    }
}

/// Encodes an optional default `Value` as a tagged string for catalog storage.
///
/// The catalog's `missing_default_value` column has type `String`, so all
/// non-null defaults are round-tripped through a `"tag:payload"` format.
/// `None` is stored as `Value::Null`.
fn encode_default(v: &Value) -> String {
    match v {
        Value::Int32(n) => format!("i32:{n}"),
        Value::Int64(n) => format!("i64:{n}"),
        Value::Uint32(n) => format!("u32:{n}"),
        Value::Uint64(n) => format!("u64:{n}"),
        Value::Float64(f) => format!("f64:{f}"),
        Value::Bool(b) => format!("bool:{b}"),
        Value::String(s) => format!("str:{s}"),
        Value::Null => "null".to_owned(),
    }
}

/// Decodes a tagged default string back into a `Value`.
///
/// Returns `None` for an empty string (which represents no default), and
/// `Some(Value::Null)` for the literal `"null"` tag.
fn decode_default(s: &str) -> Option<Value> {
    if s.is_empty() {
        return None;
    }
    if s == "null" {
        return Some(Value::Null);
    }
    let (tag, rest) = s.split_once(':')?;
    match tag {
        "i32" => rest.parse::<i32>().ok().map(Value::Int32),
        "i64" => rest.parse::<i64>().ok().map(Value::Int64),
        "u32" => rest.parse::<u32>().ok().map(Value::Uint32),
        "u64" => rest.parse::<u64>().ok().map(Value::Uint64),
        "f64" => rest.parse::<f64>().ok().map(Value::Float64),
        "bool" => match rest {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            _ => None,
        },
        "str" => Some(Value::String(rest.to_owned())),
        _ => None,
    }
}

impl From<&ColumnRow> for Tuple {
    fn from(row: &ColumnRow) -> Tuple {
        let default_val = match &row.missing_default_value {
            None => Value::Null,
            Some(v) => Value::String(encode_default(v)),
        };
        Tuple::new(vec![
            u64::from(row.table_id).into(),
            row.column_name.as_str().to_owned().into(),
            u32::from(row.column_type).into(),
            u32::from(row.position).into(),
            row.nullable.into(),
            row.is_dropped.into(),
            default_val,
        ])
    }
}

impl TryFrom<&Tuple> for ColumnRow {
    type Error = CatalogError;

    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        let table_id = tuple.read_field::<u64>(0)?;
        let column_name = tuple.read_field::<String>(1)?;
        let column_type = catalog_row_try(Type::try_from(tuple.read_field::<u32>(2)?))?;
        let position = catalog_row_try(ColumnId::try_from(tuple.read_field::<u32>(3)?))?;
        let nullable = tuple.read_field(4)?;
        let is_dropped = tuple.read_field(5)?;
        let raw_default: Option<String> = tuple.read_field(6)?;
        let missing_default_value: Option<Value> = raw_default.as_deref().and_then(decode_default);

        Ok(Self {
            table_id: FileId::from(table_id),
            column_name: validate_name(column_name, "column_name")?,
            column_type,
            position,
            nullable,
            is_dropped,
            missing_default_value,
        })
    }
}

impl CatalogRow for ColumnRow {
    const TABLE: SystemTable = SystemTable::Columns;
}

impl From<Vec<ColumnRow>> for TupleSchema {
    fn from(mut rows: Vec<ColumnRow>) -> Self {
        rows.sort_by_key(|r| r.position);
        let mut fields = Vec::new();
        for ColumnRow {
            column_name,
            column_type,
            nullable,
            is_dropped,
            missing_default_value,
            ..
        } in rows
        {
            let f = Field::new_non_empty(column_name, column_type);
            let mut f = if nullable { f } else { f.not_null() };
            let _ = f.set_is_dropped(is_dropped);
            if let Some(v) = missing_default_value {
                let _ = f.set_missing_default_value(v);
            }
            fields.push(f);
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
    pub(super) index_name: NonEmptyString,
    pub(super) table_id: FileId,
    pub(super) column_name: NonEmptyString,
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
        let index_name = validate_name(index_name, "index_name")?;
        let column_name = validate_name(column_name, "column_name")?;
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

    pub(super) fn hash(
        index_id: IndexId,
        index_name: impl Into<String>,
        table_id: FileId,
        column_name: impl Into<String>,
        column_position: ColumnId,
        index_file_id: FileId,
        num_buckets: u32,
    ) -> Result<Self, CatalogError> {
        Self::new(
            index_id,
            index_name.into(),
            table_id,
            column_name.into(),
            column_position,
            IndexKind::Hash,
            index_file_id,
            num_buckets,
        )
    }

    pub(super) fn btree(
        index_id: IndexId,
        index_name: impl Into<String>,
        table_id: FileId,
        column_name: impl Into<String>,
        column_position: ColumnId,
        index_file_id: FileId,
    ) -> Result<Self, CatalogError> {
        Self::new(
            index_id,
            index_name.into(),
            table_id,
            column_name.into(),
            column_position,
            IndexKind::Btree,
            index_file_id,
            // num_buckets=
            0,
        )
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

impl From<&IndexRow> for Tuple {
    fn from(row: &IndexRow) -> Tuple {
        Tuple::new(vec![
            row.index_id.0.into(),
            row.index_name.as_str().to_owned().into(),
            row.table_id.0.into(),
            row.column_name.as_str().to_owned().into(),
            u32::from(row.column_position).into(),
            u32::from(row.index_type).into(),
            row.index_file_id.0.into(),
            row.num_buckets.into(),
        ])
    }
}

impl TryFrom<&Tuple> for IndexRow {
    type Error = CatalogError;

    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        let index_id = IndexId::from(tuple.read_field::<i64>(0)?);
        let index_name = tuple.read_field(1)?;
        let table_id = FileId::from(tuple.read_field::<u64>(2)?);
        let column_name = tuple.read_field(3)?;
        let column_position = catalog_row_try(ColumnId::try_from(tuple.read_field::<u32>(4)?))?;
        let index_type = catalog_row_try(IndexKind::try_from(tuple.read_field::<u32>(5)?))?;
        let index_file_id = FileId::from(tuple.read_field::<u64>(6)?);
        let num_buckets = tuple.read_field(7)?;

        Self::new(
            index_id,
            index_name,
            table_id,
            column_name,
            column_position,
            index_type,
            index_file_id,
            num_buckets,
        )
    }
}

impl CatalogRow for IndexRow {
    const TABLE: SystemTable = SystemTable::Indexes;
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

impl From<&PrimaryKeyColumnRow> for Tuple {
    fn from(row: &PrimaryKeyColumnRow) -> Tuple {
        Tuple::new(vec![
            row.table_id.0.into(),
            u32::from(row.column_id).into(),
            row.ordinal.into(),
        ])
    }
}

impl TryFrom<&Tuple> for PrimaryKeyColumnRow {
    type Error = CatalogError;

    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        let table_id = FileId::from(tuple.read_field::<u64>(0)?);
        let column_id = catalog_row_try(ColumnId::try_from(tuple.read_field::<u32>(1)?))?;
        let ordinal = tuple.read_field(2)?;
        Self::new(table_id, column_id, ordinal)
    }
}

impl CatalogRow for PrimaryKeyColumnRow {
    const TABLE: SystemTable = SystemTable::PrimaryKeyColumns;
}

/// On-disk discriminant for the `constraint_kind` column of [`SystemTable::Constraints`].
///
/// Values `0..=4` are stable catalog codes; any other `u32` fails [`TryFrom`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ConstraintKind {
    NotNull = 0,
    Unique = 1,
    Check = 2,
    PrimaryKey = 3,
    ForeignKey = 4,
}

impl TryFrom<u32> for ConstraintKind {
    type Error = String;
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::NotNull),
            1 => Ok(Self::Unique),
            2 => Ok(Self::Check),
            3 => Ok(Self::PrimaryKey),
            4 => Ok(Self::ForeignKey),
            other => Err(format!("unknown constraint kind: {other}")),
        }
    }
}

impl From<ConstraintKind> for u32 {
    fn from(k: ConstraintKind) -> u32 {
        k as u32
    }
}

/// Referential action codes (`ON DELETE` / `ON UPDATE`), stored as `Uint32` in
/// [`SystemTable::FkConstraints`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum FkAction {
    NoAction = 0,
    Restrict = 1,
    Cascade = 2,
    SetNull = 3,
    SetDefault = 4,
}

impl TryFrom<u32> for FkAction {
    type Error = String;
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::NoAction),
            1 => Ok(Self::Restrict),
            2 => Ok(Self::Cascade),
            3 => Ok(Self::SetNull),
            4 => Ok(Self::SetDefault),
            other => Err(format!("unknown fk action: {other}")),
        }
    }
}

impl From<FkAction> for u32 {
    fn from(a: FkAction) -> u32 {
        a as u32
    }
}

/// One row in `SystemTable::Constraints`.
///
/// Identifies a table-level constraint by name, [`FileId`], and [`ConstraintKind`].
/// `expr` holds a non-empty predicate string when the kind needs one (for example
/// CHECK); otherwise it is `None` and serializes as [`Value::Null`].
///
/// `backing_index_id` links a UNIQUE constraint to the catalog [`IndexId`] of the
/// secondary index that maintains uniqueness for the same column list (when the engine
/// creates or attaches that index). Other constraint kinds keep this unset.
pub(super) struct ConstraintRow {
    pub(super) table_id: FileId,
    pub(super) constraint_name: NonEmptyString,
    pub(super) expr: Option<NonEmptyString>,
    pub(super) constraint_kind: ConstraintKind,
    pub(super) backing_index_id: Option<IndexId>,
}

impl From<&ConstraintRow> for Tuple {
    fn from(c: &ConstraintRow) -> Tuple {
        Tuple::new(vec![
            c.constraint_name.as_str().to_owned().into(),
            c.table_id.0.into(),
            u32::from(c.constraint_kind).into(),
            c.expr
                .clone()
                .map_or(Value::Null, |e| e.as_str().to_owned().into()),
            c.backing_index_id
                .map_or(Value::Null, |id| Value::Int64(id.0)),
        ])
    }
}

impl TryFrom<&Tuple> for ConstraintRow {
    type Error = CatalogError;
    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        let constraint_name = validate_name(tuple.read_field(0)?, "constraint_name")?;
        let table_id = FileId::from(tuple.read_field::<u64>(1)?);

        let constraint_kind =
            catalog_row_try(ConstraintKind::try_from(tuple.read_field::<u32>(2)?))?;
        let expr: Option<String> = tuple.read_field(3)?;
        let backing_index_id: Option<i64> = tuple.read_field(4)?;
        Ok(Self {
            constraint_name,
            table_id,
            constraint_kind,
            expr: expr.map(NonEmptyString::new).transpose()?,
            backing_index_id: backing_index_id.map(IndexId::from),
        })
    }
}

impl CatalogRow for ConstraintRow {
    const TABLE: SystemTable = SystemTable::Constraints;
}

/// One row in `SystemTable::ConstraintColumns`.
///
/// Multi-row pattern: a constraint that spans several columns (composite PK,
/// multi-column UNIQUE, …) writes one tuple per participating column. Rows for
/// the same logical constraint share `constraint_name` and `table_id` and differ
/// by `column_id` and `ordinal` (0-based declaration order within that constraint).
pub(super) struct ConstraintColumnRow {
    pub(super) constraint_name: NonEmptyString,
    pub(super) table_id: FileId,
    pub(super) column_id: ColumnId,
    /// Position of this column inside the constraint's column list (not the table ordinal).
    pub(super) ordinal: i32,
}

impl From<&ConstraintColumnRow> for Tuple {
    fn from(row: &ConstraintColumnRow) -> Tuple {
        Tuple::new(vec![
            row.constraint_name.as_str().to_owned().into(),
            row.table_id.0.into(),
            u32::from(row.column_id).into(),
            row.ordinal.into(),
        ])
    }
}

impl TryFrom<&Tuple> for ConstraintColumnRow {
    type Error = CatalogError;
    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        let constraint_name = validate_name(t.read_field(0)?, "constraint_name")?;
        let table_id = FileId::from(t.read_field::<u64>(1)?);
        let column_id = catalog_row_try(ColumnId::try_from(t.read_field::<u32>(2)?))?;

        let ordinal = t.read_field(3)?;
        if ordinal < 0 {
            return Err(CatalogError::invalid_catalog_row(format!(
                "ordinal must be non-negative, got {ordinal}"
            )));
        }
        Ok(Self {
            constraint_name,
            table_id,
            column_id,
            ordinal,
        })
    }
}

impl CatalogRow for ConstraintColumnRow {
    const TABLE: SystemTable = SystemTable::ConstraintColumns;
}

/// One row in `SystemTable::FkConstraints`.
///
/// Describes one column of a foreign key: the child table/column, the referenced
/// table/column, and optional [`FkAction`] for `ON DELETE` / `ON UPDATE`.
/// Composite FKs follow the same multi-row pattern as [`ConstraintColumnRow`]:
/// shared `constraint_name` and `table_id`, distinct `local_column_id` and
/// `ordinal`.
///
/// [`From`] maps `None` actions to [`Value::Null`]; [`SystemTable::validate_row`]
/// still applies the heap schema (both action columns are declared NOT NULL
/// `Uint32`), so only tuples that pass structural validation reach [`TryFrom`].
pub(super) struct FkConstraintRow {
    pub(super) constraint_name: NonEmptyString,
    pub(super) table_id: FileId,
    pub(super) local_column_id: ColumnId,
    /// 0-based slot of this local column within the FK column list.
    pub(super) ordinal: i32,
    pub(super) ref_table_id: FileId,
    pub(super) ref_column_id: ColumnId,
    pub(super) on_delete: Option<FkAction>,
    pub(super) on_update: Option<FkAction>,
}

impl From<&FkConstraintRow> for Tuple {
    fn from(f: &FkConstraintRow) -> Tuple {
        Tuple::new(vec![
            f.constraint_name.as_str().to_owned().into(),
            f.table_id.0.into(),
            u32::from(f.local_column_id).into(),
            f.ordinal.into(),
            f.ref_table_id.0.into(),
            u32::from(f.ref_column_id).into(),
            Value::Uint32(u32::from(f.on_delete.unwrap_or(FkAction::NoAction))),
            Value::Uint32(u32::from(f.on_update.unwrap_or(FkAction::NoAction))),
        ])
    }
}

impl TryFrom<&Tuple> for FkConstraintRow {
    type Error = CatalogError;
    fn try_from(t: &Tuple) -> Result<Self, Self::Error> {
        let constraint_name = validate_name(t.read_field(0)?, "constraint_name")?;

        let table_id = FileId::from(t.read_field::<u64>(1)?);
        let local_column_id = catalog_row_try(ColumnId::try_from(t.read_field::<u32>(2)?))?;
        let ordinal = t.read_field(3)?;

        let ref_table_id = FileId::from(t.read_field::<u64>(4)?);
        let ref_column_id = catalog_row_try(ColumnId::try_from(t.read_field::<u32>(5)?))?;

        let read_fk_action = |i: usize| -> Result<Option<FkAction>, CatalogError> {
            let raw: Option<u32> = t.read_field(i)?;
            catalog_row_try(raw.map(FkAction::try_from).transpose())
        };

        let on_delete = read_fk_action(6)?;
        let on_update = read_fk_action(7)?;

        if ordinal < 0 {
            return Err(CatalogError::invalid_catalog_row(format!(
                "ordinal must be non-negative, got {ordinal}"
            )));
        }

        Ok(Self {
            constraint_name,
            table_id,
            local_column_id,
            ordinal,
            ref_table_id,
            ref_column_id,
            on_delete,
            on_update,
        })
    }
}

impl CatalogRow for FkConstraintRow {
    const TABLE: SystemTable = SystemTable::FkConstraints;
}

#[cfg(test)]
mod tests {
    use super::{
        ConstraintColumnRow, ConstraintKind, ConstraintRow, FkAction, FkConstraintRow, SystemTable,
    };
    use crate::{
        FileId,
        catalog::CatalogError,
        primitives::{ColumnId, IndexId, NonEmptyString},
        tuple::Tuple,
        types::Value,
    };

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
        Tuple::new(vec![
            Value::Uint64(1),
            Value::String("email".into()),
            Value::Uint32(5),   // column_type = Type::String
            Value::Uint32(0),   // position
            Value::Bool(true),  // nullable
            Value::Bool(false), // is_dropped
            Value::Null,        // missing_default_value
        ])
    }

    fn valid_constraints_tuple() -> Tuple {
        Tuple::new(vec![
            Value::String("pk_orders".into()),
            Value::Uint64(10),
            Value::Uint32(u32::from(ConstraintKind::PrimaryKey)),
            Value::Null, // expr
            Value::Null, // backing_index_id
        ])
    }

    fn valid_constraint_columns_tuple() -> Tuple {
        Tuple::new(vec![
            Value::String("pk_orders".into()),
            Value::Uint64(10),
            Value::Uint32(0),
            Value::Int32(0),
        ])
    }

    fn valid_fk_constraints_tuple() -> Tuple {
        Tuple::new(vec![
            Value::String("fk_orders_users".into()),
            Value::Uint64(10),
            Value::Uint32(0),
            Value::Int32(0),
            Value::Uint64(20),
            Value::Uint32(1),
            Value::Uint32(FkAction::NoAction as u32),
            Value::Uint32(FkAction::Restrict as u32),
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
                Value::Bool(false), // is_dropped
                Value::Null,        // missing_default_value
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
            Value::Bool(false),
            Value::Null,
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
            Value::Bool(false),
            Value::Null,
        ]);
        let err = SystemTable::Columns.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "expected InvalidCatalogRow, got: {err}"
        );
    }

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

    #[test]
    fn test_validate_constraints_row_ok() {
        assert!(
            SystemTable::Constraints
                .validate_row(&valid_constraints_tuple())
                .is_ok()
        );
    }

    #[test]
    fn test_validate_constraints_row_all_constraint_kinds_ok() {
        for kind in [
            ConstraintKind::NotNull,
            ConstraintKind::Unique,
            ConstraintKind::Check,
            ConstraintKind::PrimaryKey,
            ConstraintKind::ForeignKey,
        ] {
            let tuple = Tuple::new(vec![
                Value::String("c_name".into()),
                Value::Uint64(1),
                Value::Uint32(u32::from(kind)),
                Value::Null,
                Value::Null,
            ]);
            assert!(
                SystemTable::Constraints.validate_row(&tuple).is_ok(),
                "kind {kind:?} should validate"
            );
        }
    }

    #[test]
    fn test_validate_constraints_row_unknown_kind_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::String("bad".into()),
            Value::Uint64(1),
            Value::Uint32(99),
            Value::Null,
            Value::Null,
        ]);
        let err = SystemTable::Constraints.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_validate_constraints_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::String("c".into()),
            Value::Uint64(1),
            Value::Uint32(0),
        ]);
        let err = SystemTable::Constraints.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_constraints_row_null_constraint_name_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Null,
            Value::Null,
        ]);
        let err = SystemTable::Constraints.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_constraint_columns_row_ok() {
        assert!(
            SystemTable::ConstraintColumns
                .validate_row(&valid_constraint_columns_tuple())
                .is_ok()
        );
    }

    #[test]
    fn test_validate_constraint_columns_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::String("pk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
        ]);
        let err = SystemTable::ConstraintColumns
            .validate_row(&tuple)
            .unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_constraint_columns_row_null_constraint_name_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(0),
        ]);
        let err = SystemTable::ConstraintColumns
            .validate_row(&tuple)
            .unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_constraint_columns_row_negative_ordinal_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::String("pk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(-1),
        ]);
        let err = SystemTable::ConstraintColumns
            .validate_row(&tuple)
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_validate_constraint_columns_row_column_id_max_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::String("pk".into()),
            Value::Uint64(1),
            Value::Uint32(u32::MAX),
            Value::Int32(0),
        ]);
        let err = SystemTable::ConstraintColumns
            .validate_row(&tuple)
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_validate_fk_constraints_row_ok() {
        assert!(
            SystemTable::FkConstraints
                .validate_row(&valid_fk_constraints_tuple())
                .is_ok()
        );
    }

    #[test]
    fn test_validate_fk_constraints_row_wrong_field_count_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::String("fk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(0),
            Value::Uint64(2),
            Value::Uint32(0),
        ]);
        let err = SystemTable::FkConstraints.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_fk_constraints_row_null_constraint_name_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(0),
            Value::Uint64(2),
            Value::Uint32(0),
            Value::Uint32(0),
            Value::Uint32(0),
        ]);
        let err = SystemTable::FkConstraints.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn test_validate_fk_constraints_row_unknown_on_delete_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::String("fk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(0),
            Value::Uint64(2),
            Value::Uint32(0),
            Value::Uint32(99),
            Value::Uint32(0),
        ]);
        let err = SystemTable::FkConstraints.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_validate_fk_constraints_row_negative_ordinal_yields_invalid_row() {
        let tuple = Tuple::new(vec![
            Value::String("fk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(-1),
            Value::Uint64(2),
            Value::Uint32(0),
            Value::Uint32(0),
            Value::Uint32(0),
        ]);
        let err = SystemTable::FkConstraints.validate_row(&tuple).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_validate_fk_constraints_row_null_on_delete_yields_corruption() {
        let tuple = Tuple::new(vec![
            Value::String("fk".into()),
            Value::Uint64(1),
            Value::Uint32(0),
            Value::Int32(0),
            Value::Uint64(2),
            Value::Uint32(0),
            Value::Null, // on_delete is NOT NULL in schema
            Value::Uint32(0),
        ]);
        let err = SystemTable::FkConstraints.validate_row(&tuple).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }), "got {err}");
    }

    #[test]
    fn test_constraint_kind_try_from_unknown() {
        let err = ConstraintKind::try_from(5u32).unwrap_err();
        assert!(err.contains("unknown constraint kind"));
    }

    #[test]
    fn test_fk_action_try_from_unknown() {
        let err = FkAction::try_from(5u32).unwrap_err();
        assert!(err.contains("unknown fk action"));
    }

    #[test]
    fn test_constraint_row_round_trip() {
        let row = ConstraintRow {
            table_id: FileId(42),
            constraint_name: NonEmptyString::new("pk_t").unwrap(),
            expr: None,
            constraint_kind: ConstraintKind::PrimaryKey,
            backing_index_id: None,
        };
        let tuple = Tuple::from(&row);
        let got = ConstraintRow::try_from(&tuple).unwrap();
        assert_eq!(got.table_id, row.table_id);
        assert_eq!(got.constraint_name.as_str(), row.constraint_name.as_str());
        assert_eq!(got.expr, row.expr);
        assert_eq!(got.constraint_kind, row.constraint_kind);
        assert_eq!(got.backing_index_id, row.backing_index_id);
    }

    #[test]
    fn test_constraint_row_round_trip_with_expr() {
        let expr = NonEmptyString::new("col > 0").unwrap();
        let row = ConstraintRow {
            table_id: FileId(1),
            constraint_name: NonEmptyString::new("chk_t").unwrap(),
            expr: Some(expr.clone()),
            constraint_kind: ConstraintKind::Check,
            backing_index_id: None,
        };
        let tuple = Tuple::from(&row);
        let got = ConstraintRow::try_from(&tuple).unwrap();
        assert_eq!(
            got.expr
                .as_ref()
                .map(crate::primitives::NonEmptyString::as_str),
            Some(expr.as_str())
        );
        assert_eq!(got.constraint_kind, ConstraintKind::Check);
    }

    #[test]
    fn test_constraint_row_round_trip_with_backing_index_id() {
        let row = ConstraintRow {
            table_id: FileId(1),
            constraint_name: NonEmptyString::new("u_email").unwrap(),
            expr: None,
            constraint_kind: ConstraintKind::Unique,
            backing_index_id: Some(IndexId::new(7)),
        };
        let tuple = Tuple::from(&row);
        let got = ConstraintRow::try_from(&tuple).unwrap();
        assert_eq!(got.backing_index_id, Some(IndexId::new(7)));
        assert_eq!(got.constraint_kind, ConstraintKind::Unique);
    }

    #[test]
    fn test_constraint_column_row_round_trip() {
        let row = ConstraintColumnRow {
            constraint_name: NonEmptyString::new("pk_orders").unwrap(),
            table_id: FileId(10),
            column_id: ColumnId::try_from(3u32).unwrap(),
            ordinal: 0,
        };
        let tuple = Tuple::from(&row);
        let got = ConstraintColumnRow::try_from(&tuple).unwrap();
        assert_eq!(got.constraint_name.as_str(), row.constraint_name.as_str());
        assert_eq!(got.table_id, row.table_id);
        assert_eq!(got.column_id, row.column_id);
        assert_eq!(got.ordinal, row.ordinal);
    }

    #[test]
    fn test_fk_constraint_row_round_trip() {
        let row = FkConstraintRow {
            constraint_name: NonEmptyString::new("fk_a").unwrap(),
            table_id: FileId(10),
            local_column_id: ColumnId::try_from(0u32).unwrap(),
            ordinal: 0,
            ref_table_id: FileId(20),
            ref_column_id: ColumnId::try_from(1u32).unwrap(),
            on_delete: Some(FkAction::Cascade),
            on_update: Some(FkAction::NoAction),
        };
        let tuple = Tuple::from(&row);
        let got = FkConstraintRow::try_from(&tuple).unwrap();
        assert_eq!(got.constraint_name.as_str(), row.constraint_name.as_str());
        assert_eq!(got.table_id, row.table_id);
        assert_eq!(got.local_column_id, row.local_column_id);
        assert_eq!(got.ordinal, row.ordinal);
        assert_eq!(got.ref_table_id, row.ref_table_id);
        assert_eq!(got.ref_column_id, row.ref_column_id);
        assert_eq!(got.on_delete, row.on_delete);
        assert_eq!(got.on_update, row.on_update);
    }

    // ALL must contain exactly seven variants.
    #[test]
    fn test_system_table_all_has_seven_variants() {
        assert_eq!(SystemTable::ALL.len(), 7);
        assert!(SystemTable::ALL.contains(&SystemTable::Tables));
        assert!(SystemTable::ALL.contains(&SystemTable::Columns));
        assert!(SystemTable::ALL.contains(&SystemTable::Indexes));
        assert!(SystemTable::ALL.contains(&SystemTable::PrimaryKeyColumns));
        assert!(SystemTable::ALL.contains(&SystemTable::Constraints));
        assert!(SystemTable::ALL.contains(&SystemTable::ConstraintColumns));
        assert!(SystemTable::ALL.contains(&SystemTable::FkConstraints));
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
