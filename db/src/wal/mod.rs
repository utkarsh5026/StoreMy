pub mod log;
pub mod reader;
pub mod writer;

use thiserror::Error;

use crate::{
    codec::CodecError,
    primitives::{Lsn, PageId, TransactionId},
};

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("unknown transaction: {0}")]
    UnknownTransaction(TransactionId),

    #[error("missing before-image for page {0:?}")]
    MissingBeforeImage(PageId),

    /// The record at `lsn` is a torn (partial) write — the log ends mid-record.
    ///
    /// Returned by [`reader::WalReader::read_at`] when the caller requests a
    /// specific LSN that is not a complete, CRC-valid record.
    #[error("torn record at LSN {0}")]
    TornRecord(Lsn),
}

/// Data-modification WAL record kind for [`crate::wal::writer::Wal::log_page_operation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageLogOp {
    // A new row was inserted into a page.
    Insert,
    // An existing row was updated in a page.
    Update,
    // A row was deleted from a page.
    Delete,
}

/// Outcome of a page mutation — whether the page should be logged and written back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageMutation {
    /// The closure made no durable change; drop the guard without WAL or write.
    Unchanged,
    /// The page was modified; log `op` then [`crate::buffer_pool::page_store::PageGuard::write`].
    Changed,
}
