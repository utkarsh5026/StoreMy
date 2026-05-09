pub mod column;
pub mod index;
pub mod manager;
pub mod systable;
pub mod table;
mod tuple;

pub use index::{IndexInfo, LiveIndex};
use thiserror::Error;

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
