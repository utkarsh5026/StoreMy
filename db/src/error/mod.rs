//! Error types for StoreMy database.
//!
//! This module provides a unified error type [`StoremyError`] that covers
//! all error conditions that can occur in the database.
//!
//! Errors are organized into module-specific sub-types for better
//! organization and modularity.

use std::fmt;
use thiserror::Error;

/// A specialized Result type for StoreMy operations.
pub type Result<T> = std::result::Result<T, StoremyError>;

// ==================== Module-Specific Error Types ====================

/// Errors related to file and I/O operations.
#[derive(Error, Debug)]
pub enum IoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("File already exists: {path}")]
    FileAlreadyExists { path: String },
}

/// Errors related to storage layer (pages, buffers).
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Page not found: file_id={file_id}, page_no={page_no}")]
    PageNotFound { file_id: u32, page_no: u32 },

    #[error("Page corrupted: file_id={file_id}, page_no={page_no}, reason={reason}")]
    PageCorrupted {
        file_id: u32,
        page_no: u32,
        reason: String,
    },

    #[error("Page full: file_id={file_id}, page_no={page_no}")]
    PageFull { file_id: u32, page_no: u32 },
}

/// Errors related to transaction management.
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction aborted: txn_id={txn_id}, reason={reason}")]
    Aborted { txn_id: u64, reason: String },

    #[error("Transaction not found: txn_id={txn_id}")]
    NotFound { txn_id: u64 },

    #[error("Invalid transaction state: txn_id={txn_id}, expected={expected}, actual={actual}")]
    InvalidState {
        txn_id: u64,
        expected: String,
        actual: String,
    },
}

/// Errors related to concurrency control and locking.
#[derive(Error, Debug)]
pub enum LockError {
    #[error("Deadlock detected for transaction {txn_id}")]
    DeadlockDetected { txn_id: u64 },

    #[error("Lock timeout for transaction {txn_id} on page {page_id}")]
    Timeout { txn_id: u64, page_id: String },

    #[error("Lock upgrade failed for transaction {txn_id}")]
    UpgradeFailed { txn_id: u64 },
}

/// Errors related to SQL parsing.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Parse error at position {position}: {message}")]
    ParseError { position: usize, message: String },

    #[error("Unexpected token at position {position}: expected {expected}, found {found}")]
    UnexpectedToken {
        position: usize,
        expected: String,
        found: String,
    },

    #[error("Unterminated string at position {position}")]
    UnterminatedString { position: usize },
}

/// Errors related to type system and conversions.
#[derive(Error, Debug)]
pub enum TypeError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    Mismatch { expected: String, actual: String },

    #[error("Cannot convert {from} to {to}")]
    InvalidConversion { from: String, to: String },

    #[error("Null value not allowed for column {column}")]
    NullNotAllowed { column: String },
}

/// Errors related to catalog/schema operations.
#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Table not found: {name}")]
    TableNotFound { name: String },

    #[error("Table already exists: {name}")]
    TableAlreadyExists { name: String },

    #[error("Column not found: {column} in table {table}")]
    ColumnNotFound { table: String, column: String },

    #[error("Index not found: {name}")]
    IndexNotFound { name: String },

    #[error("Index already exists: {name}")]
    IndexAlreadyExists { name: String },
}

/// Errors related to tuple operations.
#[derive(Error, Debug)]
pub enum TupleError {
    #[error("Tuple too large: size={size}, max={max}")]
    TooLarge { size: usize, max: usize },

    #[error("Invalid slot: slot_id={slot_id}")]
    InvalidSlot { slot_id: u16 },

    #[error("Tuple not found: page={page_no}, slot={slot_id}")]
    NotFound { page_no: u32, slot_id: u16 },
}

/// Errors related to serialization and deserialization.
#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("Buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("Deserialization error: {message}")]
    DeserializationError { message: String },
}

/// Errors related to query execution.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Division by zero")]
    DivisionByZero,

    #[error("Aggregate error: {message}")]
    AggregateError { message: String },

    #[error("Join error: {message}")]
    JoinError { message: String },
}

/// The main error type for StoreMy database operations.
///
/// This enum unifies all module-specific errors into a single type,
/// allowing automatic conversion from any module error using the `?` operator.
#[derive(Error, Debug)]
pub enum StoremyError {
    #[error(transparent)]
    Io(#[from] IoError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error(transparent)]
    Lock(#[from] LockError),

    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Type(#[from] TypeError),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error(transparent)]
    Tuple(#[from] TupleError),

    #[error(transparent)]
    Serialization(#[from] SerializationError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl StoremyError {
    pub fn internal(message: impl Into<String>) -> Self {
        StoremyError::Internal {
            message: message.into(),
        }
    }

    pub fn is_retriable(&self) -> bool {
        match self {
            StoremyError::Lock(lock_err) => matches!(
                lock_err,
                LockError::DeadlockDetected { .. } | LockError::Timeout { .. }
            ),
            _ => false,
        }
    }
}

impl ParseError {
    /// Creates a new parse error.
    pub fn new(position: usize, message: impl Into<String>) -> Self {
        ParseError::ParseError {
            position,
            message: message.into(),
        }
    }
}

impl TypeError {
    /// Creates a type mismatch error.
    pub fn mismatch(expected: impl fmt::Display, actual: impl fmt::Display) -> Self {
        TypeError::Mismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_error_display() {
        let err = CatalogError::TableNotFound {
            name: "users".to_string(),
        };
        assert_eq!(err.to_string(), "Table not found: users");
    }

    #[test]
    fn test_storage_error_to_storemy_error() {
        let storage_err = StorageError::PageNotFound {
            file_id: 1,
            page_no: 42,
        };
        let err: StoremyError = storage_err.into();
        assert!(matches!(err, StoremyError::Storage(_)));
    }

    #[test]
    fn test_is_retriable() {
        let deadlock_err = StoremyError::Lock(LockError::DeadlockDetected { txn_id: 1 });
        assert!(deadlock_err.is_retriable());

        let timeout_err = StoremyError::Lock(LockError::Timeout {
            txn_id: 1,
            page_id: "1:42".to_string(),
        });
        assert!(timeout_err.is_retriable());

        let catalog_err = StoremyError::Catalog(CatalogError::TableNotFound {
            name: "t".to_string(),
        });
        assert!(!catalog_err.is_retriable());
    }

    #[test]
    fn test_error_chaining() {
        // Test that we can use ? operator to convert between error types
        fn inner_function() -> std::result::Result<(), IoError> {
            Err(IoError::FileNotFound {
                path: "/tmp/test".to_string(),
            })
        }

        fn outer_function() -> Result<()> {
            inner_function()?;
            Ok(())
        }

        let result = outer_function();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StoremyError::Io(_)));
    }

    #[test]
    fn test_transparent_error_messages() {
        let parse_err = StoremyError::Parse(ParseError::ParseError {
            position: 10,
            message: "unexpected EOF".to_string(),
        });
        assert_eq!(
            parse_err.to_string(),
            "Parse error at position 10: unexpected EOF"
        );
    }
}
