//! Secondary index access methods.
//!
//! This is the home for every index family the database knows about
//! (B+Tree, hash, and any future kinds like LSM or R-Tree). It exposes:
//!
//! - The [`Index`] trait — the abstraction every access method implements. The `IndexManager` and
//!   the executor talk to indexes only through this trait, so adding a new family doesn't ripple
//!   through unrelated code.
//! - [`IndexKind`] — the closed catalog tag (`Hash | Btree`) stored on disk so the executor knows
//!   which `Index` impl to instantiate for a declared index.
//! - [`IndexEntry`] — the leaf-level `(key, rid)` pair shared by all index families.
//! - [`IndexError`] — the unified error type returned by index operations.
//!
//! The per-family submodules ([`btree`], [`hash`]) hold the actual page
//! layouts and algorithms. They depend on this module; nothing in this
//! module depends on them.

use std::io::{Read, Write};

use thiserror::Error;

use crate::{
    TransactionId, Type, Value,
    buffer_pool::page_store::PageStoreError,
    codec::{CodecError, Decode, Encode},
    primitives::RecordId,
    storage::StorageError,
};

pub mod btree;
pub mod hash;

/// Which kind of secondary index to build.
///
/// Closed catalog tag — stored in the on-disk catalog so the executor
/// knows which access method ([`Index`] impl) to instantiate for a given
/// declared index. The set of variants is fixed; adding one is a schema
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexKind {
    Hash,
    Btree,
}

impl TryFrom<&str> for IndexKind {
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
            "hash" => Ok(IndexKind::Hash),
            "btree" => Ok(IndexKind::Btree),
            _ => Err("invalid index type"),
        }
    }
}

impl From<IndexKind> for &'static str {
    fn from(value: IndexKind) -> Self {
        match value {
            IndexKind::Hash => "hash",
            IndexKind::Btree => "btree",
        }
    }
}

impl TryFrom<u32> for IndexKind {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IndexKind::Hash),
            1 => Ok(IndexKind::Btree),
            _ => Err("invalid index type"),
        }
    }
}

impl From<IndexKind> for u32 {
    fn from(value: IndexKind) -> Self {
        match value {
            IndexKind::Hash => 0,
            IndexKind::Btree => 1,
        }
    }
}

/// A single `(key, rid)` pair — the leaf-level unit of storage in any
/// index.
///
/// Both B+Tree leaves and hash buckets store these. The codec is shared
/// because `Value` already implements [`Encode`] / [`Decode`] and
/// [`RecordId`] follows the same pattern.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    pub key: Value,
    pub rid: RecordId,
}

impl IndexEntry {
    pub fn new(key: Value, rid: RecordId) -> Self {
        Self { key, rid }
    }

    pub fn encoded_size(&self) -> usize {
        self.key.encoded_size() + RecordId::ENCODED_SIZE
    }
}

impl Encode for IndexEntry {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.key.encode(w)?;
        self.rid.encode(w)?;
        Ok(())
    }
}

impl Decode for IndexEntry {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let key = Value::decode(r)?;
        let rid = RecordId::decode(r)?;
        Ok(Self { key, rid })
    }
}

/// Errors that index operations can return.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("key type mismatch: expected {expected:?}, got {got:?}")]
    KeyTypeMismatch { expected: Type, got: Option<Type> },

    #[error("duplicate entry: (key, rid) already present")]
    DuplicateEntry,

    #[error("entry not found for delete")]
    NotFound,

    #[error("index structural invariant violated: {0}")]
    CorruptIndex(&'static str),

    #[error(transparent)]
    PageStore(#[from] PageStoreError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Codec(#[from] CodecError),
}

/// A trait representing a secondary index over a table.
///
/// The `Index` trait abstracts any auxiliary structure (hash, B-tree, etc.)
/// that maps from key values to a set of record identifiers (`RecordId`).
///
/// Methods should be concurrency-safe and respect transactional semantics;
/// implementations are responsible for ensuring thread safety and
/// consistency, hence the `Send + Sync` bounds.
pub trait Index: Send + Sync {
    /// Inserts a `(key, rid)` pair into the index.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID under which the insert occurs.
    /// * `key` - The value to be indexed (must match the index's `key_type`).
    /// * `rid` - The record identifier to associate with the key.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if:
    /// - The key type does not match the index's key type.
    /// - The exact `(key, rid)` pair already exists (for non-unique indexes).
    /// - Internal page or storage errors occur.
    fn insert(&self, txn: TransactionId, key: &Value, rid: RecordId) -> Result<(), IndexError>;

    /// Removes the specific `(key, rid)` entry from the index, if present.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID under which the delete occurs.
    /// * `key` - The value whose record is to be removed.
    /// * `rid` - The record identifier to remove.
    ///
    /// # Errors
    ///
    /// Returns an `IndexError` if:
    /// - The `(key, rid)` pair does not exist.
    /// - Storage or page-level errors occur.
    fn delete(&self, txn: TransactionId, key: &Value, rid: RecordId) -> Result<(), IndexError>;

    /// Searches for all record IDs associated with a given key.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID performing the search.
    /// * `key` - The value to look up.
    ///
    /// # Returns
    ///
    /// A vector of `RecordId` matching the key, or an error if the operation fails.
    fn search(&self, txn: TransactionId, key: &Value) -> Result<Vec<RecordId>, IndexError>;

    /// Performs an inclusive range scan from `start` to `end`.
    ///
    /// Only supported on ordered (e.g., B-tree) indexes; unordered
    /// implementations (e.g., hash indexes) may return an error or a
    /// best-effort result containing entries between the bounds.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction ID making the request.
    /// * `start` - The lower bound (inclusive) of the key range.
    /// * `end` - The upper bound (inclusive) of the key range.
    ///
    /// # Returns
    ///
    /// All matching `RecordId` values, or an error.
    fn range_search(
        &self,
        txn: TransactionId,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<RecordId>, IndexError>;

    /// Returns the kind of this index (e.g., hash, B-tree).
    fn kind(&self) -> IndexKind;

    /// Returns the `Type` of key values supported by this index.
    fn key_type(&self) -> Type;
}

pub(crate) fn check_key_type(expected: Type, key: &Value) -> Result<(), IndexError> {
    match key.get_type() {
        Some(t) if t == expected => Ok(()),
        other => Err(IndexError::KeyTypeMismatch {
            expected,
            got: other,
        }),
    }
}
