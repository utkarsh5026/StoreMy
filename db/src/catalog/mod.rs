pub mod manager;
pub mod systable;
mod tuple;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
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
}

impl CatalogError {
    pub(super) fn invalid_catalog_row(message: impl Into<String>) -> Self {
        Self::InvalidCatalogRow {
            message: message.into(),
        }
    }
}
