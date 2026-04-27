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

use thiserror::Error;

use crate::{
    TransactionId, Type, buffer_pool::page_store::PageStoreError, codec::CodecError,
    primitives::RecordId, storage::StorageError,
};

pub mod access;
pub mod btree;
pub mod hash;
pub mod key;

pub use access::{ENVELOPE_HEADER_SIZE, Index, PageKind, decode_index_page, encode_index_page};
pub use key::{CompositeKey, IndexEntry};

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
    /// Returns `Err("invalid index type")` for any unrecognized string.
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

/// Errors that index operations can return.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("key arity mismatch: index expects {expected} columns, got {got}")]
    KeyArityMismatch { expected: usize, got: usize },

    #[error("key type mismatch at position {position}: expected {expected:?}, got {got:?}")]
    KeyTypeMismatch {
        position: usize,
        expected: Type,
        got: Option<Type>,
    },

    #[error("duplicate entry: (key, rid) already present")]
    DuplicateEntry,

    #[error("an index named {0:?} is already registered")]
    DuplicateName(String),

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

pub enum AnyIndex {
    Hash(hash::HashIndex),
    // Btree(btree::tree::BTreeIndex),  // wire up when the B-tree access method lands
}

impl AnyIndex {
    /// The catalog tag for this index — i.e. what gets persisted on disk so
    /// the next database open knows which family to reconstruct.
    pub fn kind(&self) -> IndexKind {
        match self {
            AnyIndex::Hash(_) => IndexKind::Hash,
        }
    }

    /// Per-column declared types, in declaration order. Length is the
    /// index's arity.
    pub fn key_types(&self) -> &[Type] {
        match self {
            AnyIndex::Hash(idx) => idx.key_types(),
        }
    }

    /// See [`Index::insert`].
    pub fn insert(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.insert(txn, key, rid),
        }
    }

    /// See [`Index::delete`].
    pub fn delete(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.delete(txn, key, rid),
        }
    }

    /// See [`Index::search`].
    pub fn search(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.search(txn, key),
        }
    }

    /// See [`Index::range_search`]. For hash indexes this degrades to a full
    /// scan; the planner shouldn't pick a hash index for ranges.
    pub fn range_search(
        &self,
        txn: TransactionId,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.range_search(txn, start, end),
        }
    }
}

/// Ergonomic conversion so callers can write `hash_idx.into()` instead of
/// `AnyIndex::Hash(hash_idx)`. Symmetric impls land alongside future variants.
impl From<hash::HashIndex> for AnyIndex {
    fn from(idx: hash::HashIndex) -> Self {
        AnyIndex::Hash(idx)
    }
}

/// Manual `Debug` so we don't have to cascade `#[derive(Debug)]` through
/// `HashIndex` and its `Arc<PageStore>`. The kind tag is the only useful
/// thing to print at this level — the inner state is buffer-pool resident.
impl std::fmt::Debug for AnyIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyIndex").field(&self.kind()).finish()
    }
}

#[cfg(test)]
mod any_index_tests {
    //! These are deliberately small. The full insert/delete/search behavior
    //! is covered exhaustively in `index::hash::tests` against `HashIndex`
    //! directly; here we only check that wrapping a `HashIndex` in `AnyIndex`
    //! preserves that behavior — i.e. the forwarding match arms don't drop
    //! anything on the floor.
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        FileId, TransactionId, Value,
        buffer_pool::page_store::PageStore,
        index::hash::HashIndex,
        primitives::{PageNumber, RecordId, SlotId},
        storage::PAGE_SIZE,
        wal::writer::Wal,
    };

    struct Fixture {
        any: AnyIndex,
        wal: Arc<Wal>,
        _dir: TempDir,
    }

    fn make_any_hash(num_buckets: u32, key_types: Vec<Type>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(64, Arc::clone(&wal)));
        let file_id = FileId::new(1);
        let path = dir.path().join("hash.db");

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(num_buckets) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);
        store.register_file(file_id, &path).unwrap();

        let hash = HashIndex::new(
            file_id,
            key_types,
            num_buckets,
            Arc::clone(&store),
            num_buckets,
        );

        let init_txn = TransactionId::new(0);
        wal.log_begin(init_txn).unwrap();
        hash.init(init_txn).unwrap();
        store.release_all(init_txn);

        Fixture {
            any: hash.into(),
            wal,
            _dir: dir,
        }
    }

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    fn begin(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    #[test]
    fn from_hash_index_is_hash_kind() {
        let fx = make_any_hash(4, vec![Type::Int32]);
        assert_eq!(fx.any.kind(), IndexKind::Hash);
        assert_eq!(fx.any.key_types(), &[Type::Int32]);
    }

    #[test]
    fn forwarded_insert_search_delete_roundtrip() {
        let fx = make_any_hash(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let key = CompositeKey::single(Value::Int32(42));
        let r = rid(1, 0, 0);

        fx.any.insert(txn, &key, r).unwrap();
        assert_eq!(fx.any.search(txn, &key).unwrap(), vec![r]);

        fx.any.delete(txn, &key, r).unwrap();
        assert!(fx.any.search(txn, &key).unwrap().is_empty());
    }

    #[test]
    fn forwarded_errors_propagate() {
        // Arity check still fires through the wrapper — proves we didn't
        // accidentally short-circuit validation in the forwarding layer.
        let fx = make_any_hash(4, vec![Type::Int32, Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let single = CompositeKey::single(Value::Int32(1));
        let err = fx.any.search(txn, &single).unwrap_err();
        assert!(matches!(err, IndexError::KeyArityMismatch {
            expected: 2,
            got: 1
        }));
    }
}
