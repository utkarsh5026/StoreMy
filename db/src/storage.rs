//! Core storage primitives shared across the database.
//!
//! This module pins down the few low-level facts every other storage layer
//! (heap files, B-trees, hash indexes, the buffer pool, the WAL) needs to
//! agree on:
//!
//! - The fixed [`PAGE_SIZE`] used for every on-disk page.
//! - The upper bound [`MAX_TUPLE_SIZE`] on a single tuple's serialized form.
//! - The [`Page`] trait, the minimum interface a page type must expose so the buffer pool and
//!   recovery code can work with it generically.
//! - [`StorageError`], the unified error type returned by storage operations.
//!
//! The nested [`index`] module holds the small enum naming the supported
//! secondary index kinds.

use thiserror::Error;

use crate::primitives::SlotId;

/// Size in bytes of a single on-disk page.
///
/// Every page type in the database — heap, B-tree node, hash bucket — is
/// exactly this many bytes. Keeping the size fixed lets the buffer pool treat
/// pages as interchangeable slots and makes file offsets a simple
/// `page_number * PAGE_SIZE`.
pub const PAGE_SIZE: usize = 4096;

/// Largest serialized tuple, in bytes, that the storage layer will accept.
///
/// Tuples bigger than this are rejected with
/// [`StorageError::TupleTooLarge`] before they ever reach a page. The cap is
/// well below `u16::MAX` so slot offsets and lengths can always fit in two
/// bytes.
pub const MAX_TUPLE_SIZE: usize = 65535;

/// Minimum interface every page type must provide.
///
/// The buffer pool and recovery subsystem are written against this trait so
/// they don't need to know whether they're holding a heap page, an index
/// page, or something else. Implementors are expected to be safe to share
/// across threads — the buffer pool hands out shared references from many
/// worker threads at once, hence the `Send + Sync` bound.
///
/// The "before image" is the snapshot of the page captured at the start of a
/// transaction. Recovery uses it to undo changes if the transaction aborts.
pub trait Page: Send + Sync {
    /// Returns the page's current contents as a fixed-size byte array.
    ///
    /// This is what gets written back to disk when the page is flushed.
    fn page_data(&self) -> [u8; PAGE_SIZE];

    /// Returns the snapshot taken at the start of the current transaction,
    /// or `None` if no snapshot has been taken yet.
    ///
    /// Used by undo recovery to roll the page back to its pre-transaction
    /// state.
    fn before_image(&self) -> Option<[u8; PAGE_SIZE]>;

    /// Captures the current page contents as the new before-image.
    ///
    /// Called once at the start of a transaction's first write to a page,
    /// so a later abort can restore exactly this state.
    fn set_before_image(&mut self);
}

/// Errors that storage-layer operations can return.
///
/// These cover every failure mode a page or tuple operation can hit, from
/// schema mismatches to bad slot indices to oversized tuples. Higher layers
/// usually wrap or convert these into their own error types.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid page size: expected {PAGE_SIZE}, got {got}")]
    InvalidPageSize { got: usize },

    #[error("tuple schema does not match page schema")]
    SchemaMismatch,

    #[error("page is full, no empty slots available")]
    PageFull,

    #[error("tuple size {size} exceeds maximum allowed {max}")]
    TupleTooLarge { size: usize, max: usize },

    #[error("slot index {slot} is out of bounds (page has {num_slots} slots)")]
    SlotOutOfBounds { slot: u16, num_slots: u16 },

    #[error("slot {slot} is already empty")]
    SlotAlreadyEmpty { slot: u16 },

    #[error("tuple has no record id")]
    MissingRecordId,

    #[error("tuple belongs to a different page")]
    WrongPage,

    #[error("failed to parse page data: {0}")]
    ParseError(String),
}

impl StorageError {
    /// Builds a [`StorageError::SlotOutOfBounds`] from a typed [`SlotId`].
    ///
    /// Convenience constructor so callers don't have to manually convert
    /// `SlotId` and slot counts into `u16`.
    pub fn slot_out_of_bounds(slot: SlotId, num_slots: impl Into<u16>) -> Self {
        Self::SlotOutOfBounds {
            slot: slot.into(),
            num_slots: num_slots.into(),
        }
    }

    /// Builds a [`StorageError::SlotAlreadyEmpty`] from a typed [`SlotId`].
    pub fn slot_already_empty(slot: SlotId) -> Self {
        Self::SlotAlreadyEmpty { slot: slot.into() }
    }

    /// Builds a [`StorageError::InvalidPageSize`] for a buffer of the given
    /// size.
    pub fn invalid_page_size(got: usize) -> Self {
        Self::InvalidPageSize { got }
    }

    /// Builds a [`StorageError::TupleTooLarge`] for a tuple of the given
    /// serialized size.
    ///
    /// The `max` field is filled in automatically from [`MAX_TUPLE_SIZE`].
    pub fn tuple_too_large(size: usize) -> Self {
        Self::TupleTooLarge {
            size,
            max: MAX_TUPLE_SIZE,
        }
    }
}

/// Names of the secondary index kinds the database knows about.
///
/// The catalog stores an [`Index`] alongside each declared index so the
/// executor can pick the right access method. Conversions to and from `&str`
/// (for SQL) and `u32` (for the on-disk catalog encoding) are provided.
pub mod index {
    /// Which kind of secondary index to build.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Index {
        /// A hash index — fast equality lookup, no range support.
        Hash,
        /// A B-tree index — supports both equality and range scans.
        Btree,
    }

    impl TryFrom<&str> for Index {
        type Error = &'static str;

        /// Parses an index kind from its SQL spelling.
        ///
        /// Accepts `"hash"` and `"btree"`; anything else returns an error.
        ///
        /// # Errors
        ///
        /// Returns `Err("invalid index type")` for any unrecognised string.
        fn try_from(value: &str) -> Result<Self, Self::Error> {
            match value {
                "hash" => Ok(Index::Hash),
                "btree" => Ok(Index::Btree),
                _ => Err("invalid index type"),
            }
        }
    }

    impl From<Index> for &'static str {
        /// Returns the SQL spelling of the index kind.
        fn from(value: Index) -> Self {
            match value {
                Index::Hash => "hash",
                Index::Btree => "btree",
            }
        }
    }

    impl TryFrom<u32> for Index {
        type Error = &'static str;

        /// Decodes an index kind from its on-disk catalog tag.
        ///
        /// `0` is `Hash`, `1` is `Btree`. Other values are rejected.
        ///
        /// # Errors
        ///
        /// Returns `Err("invalid index type")` if the tag isn't a known
        /// variant — typically a sign of catalog corruption or a version
        /// mismatch.
        fn try_from(value: u32) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(Index::Hash),
                1 => Ok(Index::Btree),
                _ => Err("invalid index type"),
            }
        }
    }

    impl From<Index> for u32 {
        /// Encodes an index kind as its on-disk catalog tag.
        ///
        /// Must stay in sync with the [`TryFrom<u32>`] decoder.
        fn from(value: Index) -> Self {
            match value {
                Index::Hash => 0,
                Index::Btree => 1,
            }
        }
    }
}
