pub mod manager;
pub mod systable;
pub mod table;
mod tuple;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Heap(#[from] crate::heap::file::HeapError),

    #[error("table not found")]
    TableNotFound { table_name: String },

    #[error("column not found")]
    ColumnNotFound {
        table_name: String,
        column_name: String,
    },

    #[error("index not found")]
    IndexNotFound {
        table_name: String,
        index_name: String,
    },

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

    pub(super) fn column_not_found(
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self::ColumnNotFound {
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }

    pub(super) fn index_not_found(
        table_name: impl Into<String>,
        index_name: impl Into<String>,
    ) -> Self {
        Self::IndexNotFound {
            table_name: table_name.into(),
            index_name: index_name.into(),
        }
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
}
