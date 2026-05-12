pub mod column;
pub mod constraints;
pub mod index;
pub mod manager;
pub mod systable;
pub mod table;
mod tuple;

use std::path::PathBuf;

pub use constraints::ConstraintDef;
pub use index::{IndexInfo, LiveIndex};
use thiserror::Error;

use crate::{
    FileId,
    catalog::systable::FkAction,
    primitives::{ColumnId, IndexId, NonEmptyString},
    tuple::TupleSchema,
};

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Name(#[from] crate::primitives::NameError),

    #[error(transparent)]
    Heap(#[from] crate::heap::file::HeapError),

    #[error(transparent)]
    Index(#[from] crate::index::IndexError),

    #[error("table not found")]
    TableNotFound { table_name: String },

    #[error("column not found")]
    ColumnNotFound {
        table_name: String,
        column_name: String,
    },

    #[error("column already exists")]
    ColumnAlreadyExists {
        table_name: String,
        column_name: String,
    },

    #[error("cannot alter primary key column")]
    CannotAlterPrimaryKeyColumn {
        table_name: String,
        column_name: String,
    },

    #[error("index not found")]
    IndexNotFound {
        table_name: String,
        index_name: String,
    },

    #[error("index '{0}' not found")]
    IndexNameNotFound(String),

    #[error("table already exists")]
    TableAlreadyExists { table_name: String },

    #[error("constraint '{constraint}' not found on table '{table}'")]
    ConstraintNotFound { table: String, constraint: String },

    #[error("invalid catalog row: {message}")]
    InvalidCatalogRow { message: String },

    #[error("heap file not found for file_id {file_id}")]
    HeapNotFound { file_id: crate::FileId },

    #[error("file too large to fit page count in u32")]
    FileTooLarge,

    #[error("corruption detected in system table '{table}': {message}")]
    Corruption { table: String, message: String },
}

impl CatalogError {
    pub(super) fn invalid_catalog_row(message: impl Into<String>) -> Self {
        Self::InvalidCatalogRow {
            message: message.into(),
        }
    }

    pub(super) fn corruption(table: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Corruption {
            table: table.into(),
            message: message.into(),
        }
    }

    pub(super) fn systable_corruption(
        table: crate::catalog::systable::SystemTable,
        message: impl Into<String>,
    ) -> Self {
        Self::corruption(table.table_name(), message)
    }

    pub(super) fn table_not_found(table_name: impl Into<String>) -> Self {
        Self::TableNotFound {
            table_name: table_name.into(),
        }
    }

    pub(super) fn table_already_exists(table_name: impl Into<String>) -> Self {
        Self::TableAlreadyExists {
            table_name: table_name.into(),
        }
    }

    pub(super) fn heap_not_found(file_id: crate::FileId) -> Self {
        Self::HeapNotFound { file_id }
    }

    pub(super) fn column_not_found(
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self::ColumnNotFound {
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }

    pub(super) fn column_already_exists(
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self::ColumnAlreadyExists {
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }

    pub(super) fn cannot_alter_primary_key_column(
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self::CannotAlterPrimaryKeyColumn {
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

/// Metadata for a single user table held in the catalog's in-memory cache.
#[derive(Clone, Debug)]
pub struct TableInfo {
    /// Logical name of the table (matches the key in the `tables` map).
    pub name: NonEmptyString,
    /// Column layout used to encode and decode tuples in this table's heap.
    pub schema: TupleSchema,
    /// Stable numeric identifier for the backing heap file.
    pub file_id: FileId,
    /// Absolute path to the `.dat` file on disk.
    pub file_path: PathBuf,
    /// Zero-based column identifiers of the primary key, in declaration
    /// order, if one was declared.
    ///
    /// `None` means no primary key. The `Vec` holds one entry per PK column,
    /// so composite keys appear as `Some(vec![…, …])`.
    pub primary_key: Option<Vec<ColumnId>>,
    /// UNIQUE constraints defined on this table, keyed by name.
    ///
    /// Reconstructed from `CATALOG_CONSTRAINTS` + `CATALOG_CONSTRAINT_COLUMNS`
    /// at cache load. Empty vec means no UNIQUE constraints.
    pub unique_constraints: Vec<UniqueConstraint>,
    /// Outgoing foreign keys (this table is the *child*).
    ///
    /// Reconstructed from `CATALOG_CONSTRAINTS` + `CATALOG_FK_CONSTRAINTS` at
    /// cache load. Inbound FKs (this table as parent) are not cached here —
    /// they're queried on demand during `DROP TABLE`.
    pub foreign_keys: Vec<ForeignKey>,
}

/// In-memory shape of one UNIQUE constraint.
#[derive(Clone, Debug)]
pub struct UniqueConstraint {
    pub name: NonEmptyString,
    /// Columns in the UNIQUE list, in declaration order. `(a, b)` ≠ `(b, a)`.
    pub columns: Vec<ColumnId>,
    /// Catalog [`IndexId`] of the secondary index enforcing this UNIQUE, when present.
    pub backing_index_id: Option<IndexId>,
}

/// In-memory shape of one outgoing foreign key.
#[derive(Clone, Debug)]
pub struct ForeignKey {
    pub name: NonEmptyString,
    /// Local columns, in declaration order. Index `i` lines up with `ref_columns[i]`.
    pub local_columns: Vec<ColumnId>,
    pub ref_table_id: FileId,
    pub ref_columns: Vec<ColumnId>,
    pub on_delete: Option<FkAction>,
    pub on_update: Option<FkAction>,
}

/// Descriptor of a foreign key that points INTO a table (the table is the parent).
///
/// Returned by [`Catalog::find_inbound_fks`] to let the engine locate and act
/// on child rows when a parent row is deleted or its referenced columns change.
#[derive(Clone, Debug)]
pub struct InboundFk {
    pub constraint_name: String,
    /// The child table that owns this FK.
    pub child_table_id: FileId,
    /// FK columns in the child, in ordinal order. `child_columns[i]` → `ref_columns[i]`.
    pub child_columns: Vec<ColumnId>,
    /// Referenced columns in the parent, same ordinal alignment.
    pub ref_columns: Vec<ColumnId>,
    pub on_delete: Option<FkAction>,
    pub on_update: Option<FkAction>,
}

impl TableInfo {
    /// Constructs a [`TableInfo`] from its component parts.
    pub(super) fn new(
        name: NonEmptyString,
        schema: TupleSchema,
        file_id: FileId,
        file_path: PathBuf,
        primary_key: Option<Vec<ColumnId>>,
    ) -> Self {
        Self {
            name,
            schema,
            file_id,
            file_path,
            primary_key,
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }
}
